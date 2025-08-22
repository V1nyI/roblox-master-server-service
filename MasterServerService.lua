--// Services
local RunService = game:GetService("RunService")
local MessagingService = game:GetService("MessagingService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local DataStoreService = game:GetService("DataStoreService")
local HttpService = game:GetService("HttpService")

--// Variables
local STUDIO_SUFFIX = RunService:IsStudio() and "._STUDIO" or ""

local function ComputeServerKey()
	if game.JobId and game.JobId ~= "" then
		return game.JobId
	end

	if game.PrivateServerId and game.PrivateServerId ~= "" then
		return "PS_"..game.PrivateServerId
	end
	return "SID_"..HttpService:GenerateGUID(false)
end

local ServerKey = ComputeServerKey()
local EntryLifetimeSeconds = 13
local MaxServers = 10

local TopicElection = "MasterServerService.Election"..STUDIO_SUFFIX
local TopicElectionAck = "MasterServerService.ElectionAck"..STUDIO_SUFFIX
local TopicTakeoverConfirm = "MasterServerService.TakeoverConfirm"..STUDIO_SUFFIX
local TopicBroadcastPrefix = "MasterServerService.Broadcast."..STUDIO_SUFFIX

local MasterHashMapKey = "Occupied"..STUDIO_SUFFIX
local MasterHashMapName = "MasterServerServiceMaster"..STUDIO_SUFFIX
local ServerListName = "MasterServerServiceList"..STUDIO_SUFFIX
local AuditDataStoreName = "MasterServerServiceAudit"..STUDIO_SUFFIX
local PassedDataDataStoreName = "MasterServerServicePassedData"..STUDIO_SUFFIX

-- TTL and heartbeat settings
local LeaseSeconds = 5 -- occupied TTL
local HeartbeatInterval = 3 -- master renews every 3s
local HeartbeatTimeout = 6 -- if no heartbeat within 6s, try takeover

local IsInitialized = false
local IsMaster = false
local CurrentMaster = nil
local IsAlive = true
local AuditLogEnabled = false
local _PassedDataSaved = false
local _cache = {}
local ActiveMasterTasks = {}
local PassedData = {
	_data = {},
	_timestamp = nil,
	_from = nil
}

local MasterHashMap
local ServerListSortedMap
local AuditDataStore

local PendingElectionCandidate = nil
local AckReceived = false
local TakeoverConfirmed = false

local OnBecameServerCallback = nil
local OnBecameMasterCallback = nil
local OnServerShutdownCallback = nil

local MasterServerService = {}

local function safe(fn)
	local ok, res = pcall(fn)
	return ok, res
end

--// Service health check
local function AreCoreServicesHealthy()
	local ok, res

	ok, res = safe(function()
		return MemoryStoreService:GetHashMap("__healthcheck__")
	end)
	if not ok or not res then
		warn("[MasterServerService] MemoryStoreService is down:", res)
		return false
	end

	ok, res = safe(function()
		return DataStoreService:GetDataStore("__healthcheck__")
	end)
	if not ok or not res then
		warn("[MasterServerService] DataStoreService is down:", res)
		return false
	end

	ok, res = safe(function()
		local conn = MessagingService:SubscribeAsync("__healthcheck__", function() end)
		if conn then conn:Disconnect() end
		return true
	end)
	if not ok then
		warn("[MasterServerService] MessagingService is down:", res)
		return false
	end

	return true
end

local function IsSerializableForServices(data)
	local function checkType(val, seen)
		local t = typeof(val)
		if t == "function" or t == "userdata" or t == "Instance" or t == "RBXScriptConnection" then
			return false, "Unsupported type: "..t
		elseif t == "table" then
			seen = seen or {}
			if seen[val] then return true end
			seen[val] = true
			for k, v in pairs(val) do
				local ok, err = checkType(k, seen)
				if not ok then return false, "Bad key: "..tostring(err) end
				local ok2, err2 = checkType(v, seen)
				if not ok2 then return false, "Bad value: "..tostring(err2) end
			end
		end
		return true
	end

	local ok, err = checkType(data)
	if not ok then
		return false, err
	end

	local success, encoded = pcall(function()
		return HttpService:JSONEncode(data)
	end)
	if not success then
		return false, "JSONEncode failed: "..tostring(encoded)
	end

	local byteLen = #encoded
	if byteLen > 4096 * 1024 then
		return false, "DataStoreService limit exceeded (4MB)"
	elseif byteLen > 16 * 1024 then
		return false, "MemoryStoreService limit exceeded (16KB)"
	elseif byteLen > 1024 then
		return false, "MessagingService limit exceeded (1KB)"
	end

	return true
end

MasterServerService.PassedData = {
	GetAsync = function(key)
		if key == "data" then
			return PassedData._data
		elseif key == "timestamp" then
			return PassedData._timestamp
		elseif key == "from" then
			return PassedData._from
		else
			return nil
		end
	end,

	SetAsync = function(key, value)
		if key == "data" then
			local ok, err = IsSerializableForServices(value)
			if not ok then
				warn("[PassedData] Data failed sanity check: "..tostring(err))
				return false, err
			end
			PassedData._data = value
		elseif key == "timestamp" then
			PassedData._timestamp = value
		elseif key == "from" then
			PassedData._from = value
		else
			PassedData[key] = value
		end
		return true
	end,

	UpdateAsync = function(key, transformFn)
		local oldValue = MasterServerService.PassedData.GetAsync(key)
		local newValue = transformFn(oldValue)
		local ok, err = MasterServerService.PassedData.SetAsync(key, newValue)
		if not ok then
			return oldValue, err
		end
		return newValue
	end,

	GetAll = function()
		return {data = PassedData._data, timestamp = PassedData._timestamp, from = PassedData._from}
	end
}

local function SubscribeToTopic(topic, callback)
	local success, conn = pcall(function()
		return MessagingService:SubscribeAsync(topic, function(message)
			local data = message.Data
			if typeof(data) == "table" then
				pcall(function() callback(data) end)
			end
		end)
	end)

	if success and conn then
		return conn
	else
		warn("Failed to subscribe to topic:", topic, conn)
		return nil
	end
end

local Subscriptions = {}
local function SubscribeToAllTopics()
	Subscriptions[#Subscriptions + 1] = SubscribeToTopic(TopicElection, function(data)
		if not data then return end
		if data and data.candidate == ServerKey and data.master and data.master ~= ServerKey then
			local ackPayload = { master = data.master, candidate = ServerKey, from = ServerKey, timestamp = os.time() }
			pcall(function() MessagingService:PublishAsync(TopicElectionAck, ackPayload) end)
			local ok, newVal = safe(function()
				return MasterHashMap:UpdateAsync(MasterHashMapKey, function(previous)
					if previous == data.master then
						return ServerKey
					end
					return previous
				end, LeaseSeconds)
			end)

			if ok and newVal == ServerKey then
				CurrentMaster = ServerKey
				IsMaster = true
				if OnBecameMasterCallback then
					OnBecameMasterCallback({reason = "election", from = data.master})
				end
				pcall(function()
					MessagingService:PublishAsync(TopicTakeoverConfirm, { master = data.master, candidate = ServerKey, from = ServerKey, timestamp = os.time() })
				end)
			end
		end
	end)

	Subscriptions[#Subscriptions + 1] = SubscribeToTopic(TopicElectionAck, function(data)
		if data and data.master == ServerKey and PendingElectionCandidate and data.candidate == PendingElectionCandidate then
			AckReceived = true
		end
	end)

	Subscriptions[#Subscriptions + 1] = SubscribeToTopic(TopicTakeoverConfirm, function(data)
		if data and data.master == ServerKey and PendingElectionCandidate and data.candidate == PendingElectionCandidate then
			TakeoverConfirmed = true
		end
	end)
end

local function EnsureStoresInitialized()
	local success1, hashMap = safe(function()
		return MemoryStoreService:GetHashMap(MasterHashMapName)
	end)

	if not success1 then warn("Failed to get MasterHashMap:", hashMap) end

	local success2, sortedMap = safe(function()
		return MemoryStoreService:GetSortedMap(ServerListName)
	end)

	if not success2 then warn("Failed to get ServerListSortedMap:", sortedMap) end

	local success3, dataStore = safe(function()
		return DataStoreService:GetDataStore(AuditDataStoreName)
	end)

	if not success3 then warn("Failed to get AuditDataStore:", dataStore) end

	if not (success1 and success2 and success3) then
		return false
	end

	MasterHashMap = hashMap
	ServerListSortedMap = sortedMap
	AuditDataStore = dataStore
	return true
end

local function RecordAuditLog(event, details)
	if AuditLogEnabled == false then return end
	local success, err = pcall(function()
		AuditDataStore:UpdateAsync("AuditLog", function(prev)
			prev = prev or {}
			prev[#prev + 1] = {ts = os.time(), event = event, details = details, server = ServerKey}
			if #prev > 100 then table.remove(prev, 1) end
			return prev
		end)
	end)
	if not success then warn("Failed to record audit:", err) end
end

local function GetServerList(limit)
	local success, range = pcall(function()
		return ServerListSortedMap:GetRangeAsync(Enum.SortDirection.Ascending, limit)
	end)

	if not success or not range then
		if not success then warn("Failed to get server list:", range) end
		return {}
	end

	local out = {}
	for _, kv in ipairs(range) do
		out[#out + 1] = kv.key
	end

	return out
end

local function AddSelfToServerList()
	local listedServersAmount
	local success, errorMsg = pcall(function()
		listedServersAmount = #ServerListSortedMap:GetRangeAsync(Enum.SortDirection.Ascending, 10)
	end)

	if not success then warn("Failed to get server list:", errorMsg) return end
	if listedServersAmount >= MaxServers then return end
	local success2, err = pcall(function()
		ServerListSortedMap:SetAsync(ServerKey, os.time(), EntryLifetimeSeconds)
	end)

	if not success2 then warn("Failed to add self to server list:", err) end
end

local function StartServerListRefreshLoop()
	task.spawn(function()
		while IsAlive do
			local success, err = pcall(function()
				ServerListSortedMap:SetAsync(ServerKey, os.time(), EntryLifetimeSeconds)
			end)
			if not success then
				warn("[MasterServerService] Failed to refresh server list entry:", err)
			end
			task.wait(8)
		end
	end)
end

local function RemoveSelfFromServerList()
	if ServerKey and ServerKey ~= "" then
		local success, err = pcall(function()
			ServerListSortedMap:RemoveAsync(ServerKey)
		end)
		if not success then warn("Failed to remove self from server list:", err) end
	end
end

local function ReadMasterKey()
	local success, val = pcall(function()
		return MasterHashMap:GetAsync(MasterHashMapKey)
	end)

	if success then
		return val
	else
		warn("Failed to read master key:", val)
		return nil
	end
end

-- BecomeMaster: loads passed data and starts renewal loop that uses UpdateAsync to refresh only if still master
local function BecomeMaster(reason)
	IsMaster = true
	CurrentMaster = ServerKey
	RecordAuditLog("MasterSet", {newMaster = ServerKey, reason = reason})

	local successLoad, loaded = pcall(function()
		local ds = DataStoreService:GetDataStore(PassedDataDataStoreName)
		return ds:GetAsync("data")
	end)

	if successLoad and loaded ~= nil then
		_cache.passedData = loaded
		PassedData._data = loaded
	end
	if OnBecameMasterCallback then
		OnBecameMasterCallback({reason = reason, data = PassedData._data})
	end

	task.spawn(function()
		while IsMaster and IsAlive do
			local ok, newVal = safe(function()
				return MasterHashMap:UpdateAsync(MasterHashMapKey, function(old)
					if old == ServerKey then
						return ServerKey
					end
					return old
				end, LeaseSeconds)
			end)

			if not ok then
				warn("[MasterServerService] Renewal failed (pcall)")
				local cur = ReadMasterKey()
				CurrentMaster = cur
				IsMaster = (cur == ServerKey)
				break
			end

			if newVal ~= ServerKey then
				warn("[MasterServerService] Lost master during renewal. New master:", newVal)
				CurrentMaster = newVal
				IsMaster = (newVal == ServerKey)
				break
			end

			pcall(function()
				MessagingService:PublishAsync("MasterServerHeartbeat"..STUDIO_SUFFIX, {master = ServerKey, ts = os.time()})
			end)
			
			-- print("MasterServerService: Heartbeat sent at " .. os.time())
			task.wait(HeartbeatInterval)
		end
	end)
end

local function TryBecomeMaster(reason)
	task.wait(math.random() * 0.08)
	local maxRetries = 5
	for i = 1, maxRetries do
		local success, newValue = safe(function()
			return MasterHashMap:UpdateAsync(MasterHashMapKey, function(old)
				if old == nil or old == "" then
					return ServerKey
				end
				return old
			end, LeaseSeconds)
		end)

		if success then
			if newValue == ServerKey then
				BecomeMaster(reason)
				return true
			else
				CurrentMaster = newValue
				IsMaster = (newValue == ServerKey)
				if not IsMaster and OnBecameServerCallback then
					OnBecameServerCallback({reason = reason})
				end
				return false
			end
		else
			task.wait(0.05 * i)
		end
	end
	return false
end

-- Heartbeat watcher: listens for heartbeats and triggers takeover when absent
local lastHeartbeat = os.time()
local function StartHeartbeatWatcher()
	SubscribeToTopic("MasterServerHeartbeat"..STUDIO_SUFFIX, function(data)
		if not data or not data.master then return end
		if CurrentMaster == nil or CurrentMaster == "" then
			CurrentMaster = data.master
			lastHeartbeat = os.time()
			return
		end

		if data.master ~= CurrentMaster then
			local success, authoritative = safe(function()
				return MasterHashMap:GetAsync(MasterHashMapKey)
			end)

			if success then
				if authoritative == data.master then

					CurrentMaster = data.master
					IsMaster = (CurrentMaster == ServerKey)
					lastHeartbeat = os.time()
					return
				else
					if authoritative == ServerKey then
						return
					else
						warn("[MasterServerService] Master conflict detected. Clearing current master and attempting takeover.")
						CurrentMaster = nil
						IsMaster = false
						lastHeartbeat = 0
						-- attempt to become master
						task.spawn(function() TryBecomeMaster("master_conflict") end)
						return
					end
				end
			else
				warn("[MasterServerService] Could not read authoritative master during conflict; attempting takeover")
				CurrentMaster = nil
				IsMaster = false
				lastHeartbeat = 0
				task.spawn(function() TryBecomeMaster("master_conflict") end)
				return
			end
		end

		if data.master == CurrentMaster then
			lastHeartbeat = os.time()
		end
	end)

	task.spawn(function()
		while IsAlive do
			task.wait(1)
			if CurrentMaster and not IsMaster and os.time() - lastHeartbeat > HeartbeatTimeout then
				local successGet, current = safe(function()
					return MasterHashMap:GetAsync(MasterHashMapKey)
				end)
				if successGet then
					if current == nil or current == "" then
						TryBecomeMaster("heartbeat_timeout")
					else
						CurrentMaster = current
						lastHeartbeat = os.time()
					end
				else
					TryBecomeMaster("heartbeat_timeout_read_fail")
				end
			end
		end
	end)
end

local function AttemptHandshakeWithCandidate(candidateId)
	local handshakePayload = { type = "HandshakeRequest", candidateId = candidateId, fromServerId = ServerKey, timestamp = os.time() }
	MasterServerService.Publish("ServerHandshake", handshakePayload)
end

local function HandoffMasterOnShutdown()
	RemoveSelfFromServerList()
	if _PassedDataSaved == false then
		PassedData._data = nil
	end

	local serverList = GetServerList(10)
	if #serverList > 1 then
		local selectedServer
		for _, randomServer in ipairs(serverList) do
			if randomServer ~= ServerKey then
				local IsSerializable, reason = IsSerializableForServices(PassedData._data)
				if IsSerializable == false then
					PassedData._data = nil
				end
				pcall(function()
					MessagingService:PublishAsync(randomServer, {
						type = "Handoff",
						previousMasterId = ServerKey,
						newMasterId = randomServer,
						Data = PassedData._data
					})
				end)
				selectedServer = randomServer
				break
			end
		end
	else
		local DataStore = DataStoreService:GetDataStore(PassedDataDataStoreName)
		if _PassedDataSaved == false then
			pcall(function() DataStore:RemoveAsync("data") end)
			return
		end
		if PassedData._data ~= nil then
			local IsSerializable, reason = IsSerializableForServices(PassedData._data)
			if IsSerializable == true then
				pcall(function() DataStore:SetAsync("data", PassedData._data) end)
			end
		end
	end
end

local function InitializeInternal()
	while not AreCoreServicesHealthy() do
		warn("[MasterServerService] One or more core services are down. Will retry in 60 seconds.")
		task.wait(60)
	end

	if not EnsureStoresInitialized() then
		return false
	end

	SubscribeToAllTopics()
	AddSelfToServerList()
	StartServerListRefreshLoop()

	SubscribeToTopic(ServerKey, function(data)
		if not data then return end
		if data.type == "Handoff" then
			local prev = data.previousMasterId
			local newMaster = data.newMasterId
			if newMaster and newMaster ~= "" then
				pcall(function()
					MasterHashMap:UpdateAsync(MasterHashMapKey, function(old)
						if old == prev then
							return newMaster
						end
						return old
					end, LeaseSeconds)
				end)
				CurrentMaster = newMaster
				IsMaster = (newMaster == ServerKey)
			end
		elseif data.type == "PassedDataUpdate" then
			if IsMaster then
				PassedData._data = data.Data
				PassedData._timestamp = data.timestamp or os.time()
				PassedData._from = data.from
				_PassedDataSaved = true
				pcall(function()
					local ds = DataStoreService:GetDataStore(PassedDataDataStoreName)
					ds:SetAsync("data", PassedData._data)
				end)
			end
		end
	end)

	local listcount = #GetServerList(10)
	if listcount == 1 then
		local success, newValue = safe(function()
			return MasterHashMap:UpdateAsync(MasterHashMapKey, function(old)
				if old == nil or old == "" then
					return ServerKey
				end
				return old
			end, LeaseSeconds)
		end)
		if success and newValue == ServerKey then
			BecomeMaster("initial_single")
		else
			local master = ReadMasterKey()
			CurrentMaster = master
			IsMaster = (master == ServerKey)
		end
	else
		local master = ReadMasterKey()
		CurrentMaster = master
		IsMaster = (master == ServerKey)
	end

	StartHeartbeatWatcher()
	return true
end

-- Public API
function MasterServerService.Initialize()
	task.spawn(function()
		if IsInitialized then
			return {success = true, isMaster = IsMaster, masterId = CurrentMaster}
		end
		
		game:BindToClose(function()
			IsAlive = false
			if IsMaster then
				if OnServerShutdownCallback then
					task.spawn(OnServerShutdownCallback)
				end
				task.wait(0.05)
				task.spawn(HandoffMasterOnShutdown)
				local ServerAliveTime = 2
				local startTime = tick()
				while tick() - startTime < ServerAliveTime do
					task.wait(1)
				end
			else
				RemoveSelfFromServerList()
			end
		end)

		local ok = InitializeInternal()
		IsInitialized = true
		print("[MasterServerService] Master server initialized. Is master:", IsMaster, "Master id:", CurrentMaster)
		return {success = ok, isMaster = IsMaster, masterId = CurrentMaster}
	end)
end

function MasterServerService.IsMaster()
	return IsMaster
end

function MasterServerService.GetMasterId()
	return CurrentMaster
end

function MasterServerService.GetServerList()
	return GetServerList(10)
end

function MasterServerService.Publish(topic, payload)
	local success, err = pcall(function()
		MessagingService:PublishAsync(TopicBroadcastPrefix..topic, { from = ServerKey, payload = payload, timestamp = os.time() })
	end)

	if not success then warn("Failed to publish broadcast:", err) end
end

function MasterServerService.Subscribe(topic, callback)
	SubscribeToTopic(TopicBroadcastPrefix..topic, function(data)
		if data then
			callback(data.from, data.payload, data.timestamp)
		end
	end)
end

function MasterServerService.ForceReassign(targetId)
	if not IsMaster or not targetId or targetId == ServerKey then
		return false
	end
	return AttemptHandshakeWithCandidate(targetId)
end

function MasterServerService.Status()
	return { alive = IsAlive, isMaster = IsMaster, masterId = CurrentMaster, listCount = #GetServerList(10) }
end

function MasterServerService.OnBecameMaster(fn)
	OnBecameMasterCallback = fn
end

function MasterServerService.OnBecameServer(fn)
	OnBecameServerCallback = fn
end

function MasterServerService.OnServerShutdown(fn)
	OnServerShutdownCallback = fn
end

function MasterServerService.SetPassedData(data)
	local ok, err = IsSerializableForServices(data)
	if not ok then
		warn("[SetPassedData] Data failed sanity check: "..tostring(err))
		return false, err
	end

	PassedData._data = data
	PassedData._timestamp = os.time()
	PassedData._from = ServerKey
	_PassedDataSaved = true
	RecordAuditLog("PassedDataSet", {from = ServerKey, timestamp = PassedData._timestamp})
	if IsMaster then
		local success, perr = pcall(function()
			local ds = DataStoreService:GetDataStore(PassedDataDataStoreName)
			ds:SetAsync("data", data)
		end)
		if not success then warn("[SetPassedData] Failed to persist PassedData to DataStore:", perr) end
	else
		if CurrentMaster and CurrentMaster ~= "" then
			local success, perr = pcall(function()
				MessagingService:PublishAsync(CurrentMaster, { type = "PassedDataUpdate", Data = data, from = ServerKey, timestamp = PassedData._timestamp })
			end)
			if not success then warn("[SetPassedData] Failed to send PassedData to master:", perr) end
		end
	end

	return true
end

function MasterServerService.AuditService()
	local Audit = {}
	function Audit.GetAuditLogs()
		return AuditDataStore:GetAsync("AuditLogs") or {}
	end
	function Audit.EnableAuditLogging(enable)
		if type(enable) ~= "boolean" then return false, "Argument must be a boolean" end
		AuditLogEnabled = enable
		return true
	end
	function Audit.CleanupAuditLogs()
		local success, err = pcall(function() AuditDataStore:SetAsync("AuditLogs", {}) end)
		if not success then warn("Failed to cleanup audit logs:", err) end
	end
	return Audit
end

-- DebugService: Reset all services data (MemoryStoreService, DataStoreService)
local DebugService = {}

function DebugService.ResetAllServicesData()
	local ok1, hashMap = pcall(function()
		return MemoryStoreService:GetHashMap(MasterHashMapName)
	end)
	if ok1 and hashMap then
		pcall(function()
			hashMap:RemoveAsync(MasterHashMapKey)
		end)
	end

	local ok2, sortedMap = pcall(function()
		return MemoryStoreService:GetSortedMap(ServerListName)
	end)
	if ok2 and sortedMap then
		pcall(function()
			local range = sortedMap:GetRangeAsync(Enum.SortDirection.Ascending, MaxServers)
			for _, kv in ipairs(range) do
				sortedMap:RemoveAsync(kv.key)
			end
		end)
	end
	
	local ok3, auditStore = pcall(function()
		return DataStoreService:GetDataStore(AuditDataStoreName)
	end)
	if ok3 and auditStore then
		pcall(function()
			auditStore:SetAsync("AuditLogs", {})
			auditStore:SetAsync("AuditLog", {})
		end)
	end

	local ok4, passedDataStore = pcall(function()
		return DataStoreService:GetDataStore(PassedDataDataStoreName)
	end)
	if ok4 and passedDataStore then
		pcall(function()
			passedDataStore:SetAsync("data", nil)
		end)
	end
	
	PassedData._data = {}
	PassedData._timestamp = nil
	PassedData._from = nil
	_cache.passedData = nil

	warn("[DebugService] All services data have been reset.")
	return true
end

MasterServerService.DebugService = DebugService

return MasterServerService
