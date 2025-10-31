-- redis_calculations.lua
-- Consolidated trading indicator calculations for Redis 8.x using EVAL scripts.

local function abs(x)
    return x < 0 and -x or x
end

local function max3(a, b, c)
    local m = a
    if b > m then m = b end
    if c > m then m = c end
    return m
end

local function fast_atr(highs, lows, closes, period)
    if #highs < period + 1 then return 0 end
    local sum = 0
    for i = 2, math.min(#highs, period + 1) do
        local high = highs[i] or 0
        local low = lows[i] or 0
        local prev_close = closes[i - 1] or 0
        local tr1 = high - low
        local tr2 = abs(high - prev_close)
        local tr3 = abs(low - prev_close)
        sum = sum + max3(tr1, tr2, tr3)
    end
    return sum / period
end

local function fast_ema(prices, period)
    if #prices < period then return prices[#prices] or 0 end
    local alpha = 2.0 / (period + 1.0)
    local ema = prices[1] or 0
    for i = 2, #prices do
        ema = (prices[i] or 0) * alpha + ema * (1 - alpha)
    end
    return ema
end

local function fast_ema_all_windows(prices)
    local periods = {5, 10, 20, 50, 100, 200}
    local result = {}
    for _, period in ipairs(periods) do
        result["ema_" .. period] = fast_ema(prices, period)
    end
    return result
end

local function fast_rsi(prices, period)
    if #prices < period + 1 then return 50 end

    local avg_gain, avg_loss = 0, 0
    for i = 2, period + 1 do
        local change = (prices[i] or 0) - (prices[i - 1] or 0)
        if change > 0 then
            avg_gain = avg_gain + change
        else
            avg_loss = avg_loss - change
        end
    end
    avg_gain = avg_gain / period
    avg_loss = avg_loss / period

    for i = period + 2, #prices do
        local change = (prices[i] or 0) - (prices[i - 1] or 0)
        if change > 0 then
            avg_gain = (avg_gain * (period - 1) + change) / period
            avg_loss = (avg_loss * (period - 1)) / period
        else
            avg_gain = (avg_gain * (period - 1)) / period
            avg_loss = (avg_loss * (period - 1) - change) / period
        end
    end

    if avg_loss == 0 then return 100 end
    local rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
end

local function fast_vwap(tick_data)
    local total_volume, price_volume_sum = 0, 0
    for i = 1, #tick_data do
        local tick = tick_data[i] or {}
        -- Try different volume fields in order of preference
        local volume = (tonumber(tick.zerodha_cumulative_volume) or 
                       tonumber(tick.bucket_incremental_volume) or 
                       tonumber(tick.bucket_cumulative_volume) or 
                       tonumber(tick.volume) or 0)
        local price = tonumber(tick.last_price) or tonumber(tick.price) or 0
        if volume > 0 and price > 0 then
            total_volume = total_volume + volume
            price_volume_sum = price_volume_sum + (volume * price)
        end
    end
    if total_volume == 0 then return 0 end
    return price_volume_sum / total_volume
end

local function fast_indicator_batch(prices, highs, lows, volumes)
    local result = {
        ema_5 = fast_ema(prices, 5),
        ema_10 = fast_ema(prices, 10),
        ema_20 = fast_ema(prices, 20),
        ema_50 = fast_ema(prices, 50),
        ema_100 = fast_ema(prices, 100),
        ema_200 = fast_ema(prices, 200),
        rsi = fast_rsi(prices, 14),
        atr = fast_atr(highs, lows, prices, 14),
        vwap = 0.0
    }

    local total_volume, price_volume_sum = 0, 0
    local limit = math.min(#prices, #volumes)
    for i = 1, limit do
        local price = tonumber(prices[i]) or 0
        local volume = tonumber(volumes[i]) or 0
        if price > 0 and volume > 0 then
            total_volume = total_volume + volume
            price_volume_sum = price_volume_sum + price * volume
        end
    end
    result.vwap = total_volume > 0 and (price_volume_sum / total_volume) or 0
    return result
end

local command = ARGV[1] or ''

if command == 'ATR' then
    local highs = cjson.decode(ARGV[2] or '[]')
    local lows = cjson.decode(ARGV[3] or '[]')
    local closes = cjson.decode(ARGV[4] or '[]')
    local period = tonumber(ARGV[5]) or 14
    return tostring(fast_atr(highs, lows, closes, period))

elseif command == 'EMA' then
    local prices = cjson.decode(ARGV[2] or '[]')
    local period = tonumber(ARGV[3]) or 20
    return tostring(fast_ema(prices, period))

elseif command == 'EMA_ALL' then
    local prices = cjson.decode(ARGV[2] or '[]')
    return cjson.encode(fast_ema_all_windows(prices))

elseif command == 'RSI' then
    local prices = cjson.decode(ARGV[2] or '[]')
    local period = tonumber(ARGV[3]) or 14
    return tostring(fast_rsi(prices, period))

elseif command == 'VWAP' then
    local tick_data = cjson.decode(ARGV[2] or '[]')
    return tostring(fast_vwap(tick_data))


elseif command == 'ATR_BATCH' then
    local highs = cjson.decode(ARGV[2] or '[]')
    local lows = cjson.decode(ARGV[3] or '[]')
    local closes = cjson.decode(ARGV[4] or '[]')
    local period = tonumber(ARGV[5]) or 14
    return tostring(fast_atr(highs, lows, closes, period))

elseif command == 'BATCH' then
    local prices = cjson.decode(ARGV[2] or '[]')
    local highs = cjson.decode(ARGV[3] or '[]')
    local lows = cjson.decode(ARGV[4] or '[]')
    local volumes = cjson.decode(ARGV[5] or '[]')
    return cjson.encode(fast_indicator_batch(prices, highs, lows, volumes))

end

return '0'
