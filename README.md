# MasterServerService v1.0.0 - Master Server Election for Roblox

A Roblox module for **master server coordination** across multiple game servers.  
Features **automatic master election**, **state persistence**, and **cross-server broadcasting**

---

## **Features**
- **Automatic Master Election** - No manual intervention required
- **Fast Failover** - Detects master crashes and elects a new one within seconds
- **Persistent State (`PassedData`)** - New master inherits the last known state
- **Cross-Server Broadcasting** - Publish and subscribe to topics across all servers
- **Graceful Shutdown Handoff** - Transfers master role and data before shutdown
- **Optional Audit Logging** - Record master changes, passed data updates, and more
- **Configurable Parameters** - Heartbeat intervals, timeouts, and max server limits

---

## **Installation**
1. Place `MasterServerService.lua` in `ServerScriptService`.
2. Require it in a server script (Call Initialize() as early as possible in your server’s main script):
```lua
local MasterServerService = require(ServerScriptService.MasterServerService)
MasterServerService.Initialize()
```

# Basic Usage

## Master Election Callback
```lua
MasterServerService.OnBecameMaster(function(passedData)
    print("This server is now the master. Reason:", passedData.reason)
end)
```

## Non-Master Callback
```lua
MasterServerService.OnBecameServer(function(info)
  print("This server is now a regular server. Reason:", info.reason)
end)
```

## Cross-Server Messaging
```lua
-- Publish an event to all servers
MasterServerService.Publish("MyEvent", {foo = "bar"})

-- Subscribe to the topic
MasterServerService.Subscribe("MyEvent", function(from, payload, timestamp)
  print("Message from:", from, "Payload:", payload.foo)
end)
```

## Using PassedData
### `PassedData` is **only** for transferring state between the current master and the next master during a master change
It is **not** a real-time global data store for all servers, for that, use `Publish()`.
```lua
-- Set shared state for the next master server to inherit
MasterServerService.SetPassedData({number = 143})

-- Get shared state (only meaningful if this server is or becomes master)
local data = MasterServerService.PassedData.GetAsync("data")
if data then
    print("Current number:", data.number)
end
```
## API Overview
- Initialize() - Starts the service and participates in master election
- IsMaster() - Returns true if this server is the master
- GetMasterId() - Returns the current master server ID
- GetServerList() - Returns a list of active server IDs
- Publish(topic, payload) - Sends a message to all servers
- Subscribe(topic, callback) - Listens for a topic across all servers
- SetPassedData(data) - Sets shared state for the next master server
- PassedData.GetAsync(key) - Gets a specific field from the shared state
- PassedData.GetAll() - Returns the full shared state table
- OnBecameMaster(fn) - Sets a callback when this server becomes master
- OnBecameServer(fn) - Sets a callback when this server loses master status
- OnServerShutdown(fn) - Sets a callback before this server shuts down
- Status() - Returns {alive, isMaster, masterId, listCount}

---

## License

MIT — see [LICENSE](LICENSE) for details.
