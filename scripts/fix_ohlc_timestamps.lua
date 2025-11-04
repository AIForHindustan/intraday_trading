-- Fix timestamps in ohlc_daily sorted sets
-- Reads date from JSON payload and converts to correct timestamp (milliseconds)

local keys = redis.call('KEYS', 'ohlc_daily:*')
local fixed_count = 0
local total_entries = 0

-- Helper function to parse date string (YYYY-MM-DD) to timestamp in milliseconds
local function date_to_timestamp_ms(date_str)
    if not date_str then return nil end
    local year, month, day = string.match(date_str, "(%d+)-(%d+)-(%d+)")
    if not year or not month or not day then return nil end
    
    year = tonumber(year)
    month = tonumber(month)
    day = tonumber(day)
    
    -- Calculate timestamp using Lua's os.time equivalent
    -- For better accuracy, we'll use a formula that accounts for leap years
    local function is_leap_year(y)
        return (y % 4 == 0 and y % 100 ~= 0) or (y % 400 == 0)
    end
    
    -- Days per month
    local days_in_month = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
    if is_leap_year(year) then
        days_in_month[2] = 29
    end
    
    -- Calculate days since 1970-01-01
    local days = 0
    -- Years
    for y = 1970, year - 1 do
        days = days + (is_leap_year(y) and 366 or 365)
    end
    -- Months
    for m = 1, month - 1 do
        days = days + days_in_month[m]
    end
    -- Days
    days = days + (day - 1)
    
    -- Convert to milliseconds
    return math.floor(days * 86400000 + 12 * 3600000)  -- Add 12 hours for IST offset (UTC+5:30 = ~12 hours from midnight)
end

for _, key in ipairs(keys) do
    -- Get all entries with scores
    local entries = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
    local updates = {}
    local updated = false
    
    for i = 1, #entries, 2 do
        local payload = entries[i]
        local old_score = tonumber(entries[i+1])
        
        -- Parse JSON payload
        local success, json_data = pcall(cjson.decode, payload)
        if success and json_data then
            local date_str = json_data.date or json_data.d
            
            if date_str then
                local new_timestamp_ms = date_to_timestamp_ms(date_str)
                
                if new_timestamp_ms and new_timestamp_ms ~= old_score then
                    -- Store for batch update
                    table.insert(updates, new_timestamp_ms)
                    table.insert(updates, payload)
                    updated = true
                    total_entries = total_entries + 1
                else
                    -- Keep original if parsing failed
                    table.insert(updates, old_score)
                    table.insert(updates, payload)
                end
            else
                -- Keep original if no date field
                table.insert(updates, old_score)
                table.insert(updates, payload)
            end
        else
            -- Keep original if JSON parse failed
            table.insert(updates, old_score)
            table.insert(updates, payload)
        end
    end
    
    -- Batch update with new scores
    if updated and #updates > 0 then
        redis.call('DEL', key)  -- Clear old entries
        redis.call('ZADD', key, unpack(updates))
        fixed_count = fixed_count + 1
    end
end

return {fixed_count, total_entries}

