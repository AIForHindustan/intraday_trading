-- Create ohlc_stats keys from ohlc_daily sorted sets
-- Calculates 30-day statistics: high, low, avg_close, avg_volume, total_volume

local keys = redis.call('KEYS', 'ohlc_daily:*')
local created_count = 0

for _, key in ipairs(keys) do
    local stats_key = string.gsub(key, 'ohlc_daily:', 'ohlc_stats:')
    
    -- Get all entries (sorted by timestamp)
    local entries = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    
    if #entries > 0 then
        local highs = {}
        local lows = {}
        local closes = {}
        local volumes = {}
        
        -- Parse all entries
        for i = 1, #entries, 2 do
            local payload = entries[i]
            local success, json_data = pcall(cjson.decode, payload)
            
            if success and json_data then
                local high = tonumber(json_data.h or json_data.high or 0)
                local low = tonumber(json_data.l or json_data.low or 0)
                local close = tonumber(json_data.c or json_data.close or 0)
                local volume = tonumber(json_data.v or json_data.volume or 0)
                
                if high > 0 then table.insert(highs, high) end
                if low > 0 then table.insert(lows, low) end
                if close > 0 then table.insert(closes, close) end
                if volume > 0 then table.insert(volumes, volume) end
            end
        end
        
        -- Calculate statistics
        local function max_value(t)
            if #t == 0 then return 0.0 end
            local max = t[1]
            for i = 2, #t do
                if t[i] > max then max = t[i] end
            end
            return max
        end
        
        local function min_value(t)
            if #t == 0 then return 0.0 end
            local min = t[1]
            for i = 2, #t do
                if t[i] < min then min = t[i] end
            end
            return min
        end
        
        local function average(t)
            if #t == 0 then return 0.0 end
            local sum = 0.0
            for i = 1, #t do
                sum = sum + t[i]
            end
            return sum / #t
        end
        
        local function sum_values(t)
            local sum = 0.0
            for i = 1, #t do
                sum = sum + t[i]
            end
            return sum
        end
        
        -- Build stats hash
        local high_30d = max_value(highs)
        local low_30d = min_value(lows)
        local avg_close_30d = average(closes)
        local avg_volume_30d = average(volumes)
        local total_volume_30d = sum_values(volumes)
        local records = #entries / 2  -- Each entry has score, so divide by 2
        
        -- Store in hash
        redis.call('HSET', stats_key,
            'records', records,
            'high_30d', string.format('%.2f', high_30d),
            'low_30d', string.format('%.2f', low_30d),
            'avg_close_30d', string.format('%.2f', avg_close_30d),
            'avg_volume_30d', string.format('%.0f', avg_volume_30d),
            'total_volume_30d', string.format('%.0f', total_volume_30d)
        )
        
        created_count = created_count + 1
    end
end

return created_count

