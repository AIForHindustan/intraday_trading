-- Redis Connection Monitoring Lua Script
-- Usage: redis-cli --eval monitor_connections.lua

local clients = redis.call('CLIENT', 'LIST')
local total = 0
local by_name = {}
local by_db = {}
local idle_connections = {}
local old_connections = {}

-- Current time for age calculation
local now = redis.call('TIME')[1]

for line in string.gmatch(clients, "[^\r\n]+") do
    total = total + 1
    
    -- Extract name
    local name = string.match(line, "name=([^%s]+)") or "unnamed"
    if name == "(no" then
        name = "unnamed"
    end
    by_name[name] = (by_name[name] or 0) + 1
    
    -- Extract database
    local db = string.match(line, "db=(%d+)") or "0"
    by_db[db] = (by_db[db] or 0) + 1
    
    -- Extract idle time
    local idle = tonumber(string.match(line, "idle=(%d+)")) or 0
    if idle > 3600 then  -- Idle > 1 hour
        table.insert(idle_connections, {
            name = name,
            db = db,
            idle = idle
        })
    end
    
    -- Extract age
    local age = tonumber(string.match(line, "age=(%d+)")) or 0
    if age > 7200 then  -- Age > 2 hours
        table.insert(old_connections, {
            name = name,
            db = db,
            age = age
        })
    end
end

-- Build result table
local result = {
    total = total,
    by_name = by_name,
    by_db = by_db,
    idle_count = #idle_connections,
    old_count = #old_connections
}

-- Add detailed idle connections (limit to first 10)
if #idle_connections > 0 then
    result.idle_connections = {}
    for i = 1, math.min(10, #idle_connections) do
        table.insert(result.idle_connections, idle_connections[i])
    end
end

-- Add detailed old connections (limit to first 10)
if #old_connections > 0 then
    result.old_connections = {}
    for i = 1, math.min(10, #old_connections) do
        table.insert(result.old_connections, old_connections[i])
    end
end

return result

