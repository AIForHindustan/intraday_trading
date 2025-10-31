
# Redis Key Structure Standards
==============================

## Standardized Key Formats

### Volume Data
- **Volume Buckets**: `volume:bucket:{symbol}:{session}:{hour}:{minute}`
- **Volume Aggregates**: `volume:aggregate:{symbol}:{session}`
- **Volume History**: `volume:history:{symbol}:{date}`

### Session Data
- **Session Data**: `session:{symbol}:{session}`
- **Cumulative Data**: `cumulative:{symbol}:{session}`
- **Session Metrics**: `metrics:{symbol}:{session}`

### OHLC Market Data
- **Daily Time Series**: `ohlc:{symbol}:1d`
- **Hourly Time Series**: `ohlc:{symbol}:1h`
- **5-Minute Time Series**: `ohlc:{symbol}:5m`
- **Daily Sorted Set**: `ohlc_daily:{symbol}`
- **Hourly Sorted Set**: `ohlc_hourly:{symbol}`
- **Latest Snapshot Hash**: `ohlc_latest:{symbol}`
- **Statistics Hash**: `ohlc_stats:{symbol}`
- **Realtime Stream**: `ohlc_updates:{symbol}`

### Data Types
- **data_type**: volume, price, indicators, metrics
- **session**: YYYY-MM-DD format
- **timestamp**: Unix timestamp or ISO format
- **symbol**: Trading symbol (e.g., NIFTY, RELIANCE, NFO:RELIANCE)

## Key Parsing Functions

```python
def parse_volume_bucket_key(key):
    """Parse volume bucket key: volume:bucket:{symbol}:{session}:{hour}:{minute}"""
    if not key.startswith('volume:bucket:'):
        return None
    
    parts = key.split(':')
    if len(parts) < 6:
        return None
    
    return {
        'symbol': ':'.join(parts[2:-3]),
        'session': parts[-3],
        'hour': parts[-2],
        'minute': parts[-1]
    }

def parse_session_key(key):
    """Parse session key: session:{symbol}:{session}"""
    if not key.startswith('session:'):
        return None
    
    parts = key.split(':')
    if len(parts) < 3:
        return None
    
    return {
        'symbol': ':'.join(parts[1:-1]),
        'session': parts[-1]
    }
```

## Migration Guide

### Old Formats → New Formats
- `tick:bucket:{symbol}:{timestamp}` → `volume:bucket:{symbol}:{session}:{hour}:{minute}`
- `volume:{symbol}:{session}` → `volume:aggregate:{symbol}:{session}`
- `market_data:ohlc_30d:{symbol}` → `ohlc:{symbol}:1d` / `ohlc_daily:{symbol}`
- `market_data:ohlc_20d:{symbol}` → `ohlc:{symbol}:1h` / `ohlc_hourly:{symbol}`
- `market_data:current_ohlc:{symbol}` → `ohlc_latest:{symbol}`

### Benefits
1. **Consistent Structure**: All keys follow the same pattern
2. **Easy Parsing**: Standardized parsing functions
3. **Better Performance**: Optimized key patterns for Redis
4. **Maintainable**: Clear naming conventions
