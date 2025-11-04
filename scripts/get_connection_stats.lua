-- Redis 8.2 Enhanced Connection Statistics Lua Script
-- Uses Redis 8.2 improved Lua script handling and PUBLISH for zombie connections

local stats = {}
local clients = redis.call('CLIENT', 'LIST')
local total = 0
local by_name = {}
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
    
    -- Extract idle time
    local idle = tonumber(string.match(line, "idle=(%d+)") or "0")
    
    -- Redis 8.2: Track and publish zombie connections (idle > 1 hour)
    if idle > 3600 then
        table.insert(idle_connections, {
            name = name,
            idle = idle,
            line = line
        })
        -- Redis 8.2 improvement: Publish zombie connection info
        redis.call('PUBLISH', 'zombie_connections', line)
    end
    
    -- Track old connections (age > 2 hours)
    local age = tonumber(string.match(line, "age=(%d+)") or "0")
    if age > 7200 then
        table.insert(old_connections, {
            name = name,
            age = age,
            line = line
        })
    end
end

-- Build result table
local result = {
    total = total,
    by_name = by_name,
    idle_count = #idle_connections,
    old_count = #old_connections
}

-- Add detailed idle connections (limit to first 10)
if #idle_connections > 0 then
    result.idle_connections = {}
    for i = 1, math.min(10, #idle_connections) do
        result.idle_connections[i] = {
            name = idle_connections[i].name,
            idle = idle_connections[i].idle
        }
    end
end

-- Add detailed old connections (limit to first 10)
if #old_connections > 0 then
    result.old_connections = {}
    for i = 1, math.min(10, #old_connections) do
        result.old_connections[i] = {
            name = old_connections[i].name,
            age = old_connections[i].age
        }
    end
end

return result

