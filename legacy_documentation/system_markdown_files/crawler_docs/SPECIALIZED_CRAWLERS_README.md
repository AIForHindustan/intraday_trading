# Specialized Binary Crawlers

This document describes the specialized binary crawlers designed for different trading and data mining needs.

## Overview

We have created two specialized binary crawlers to optimize data collection based on use case:

1. **Intraday Trading Crawler** - High-frequency data for real-time trading decisions
2. **Data Mining Crawler** - Comprehensive market data for research and analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Specialized Crawlers                     │
├─────────────────────────────────────────────────────────────┤
│  Intraday Trading Crawler    │    Data Mining Crawler      │
│  • 136 instruments          │    • 1,632 instruments       │
│  • 100ms update frequency   │    • 1000ms update frequency │
│  • NIFTY 50 + Futures       │    • All asset classes       │
│  • Real-time trading focus  │    • Research focus          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Redis Streams                            │
├─────────────────────────────────────────────────────────────┤
│  ticks:raw:binary:intraday  │  ticks:raw:binary:mining     │
│  market_data.ticks          │  ticks:category:*            │
│  market_data.equity_*       │  ts:*:* (time-series)        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                Pattern Detection & Alerts                   │
│  • Scanner processes intraday data                         │
│  • Real-time pattern detection                             │
│  • macOS + Telegram notifications                          │
└─────────────────────────────────────────────────────────────┘
```

## Configuration Files

### Generated Files

| File | Purpose | Instruments |
|------|---------|-------------|
| `config/intraday_trading_crawler_v2.json` | Intraday crawler config | 136 |
| `config/data_mining_crawler_v2.json` | Data mining crawler config | 1,632 |
| `config/intraday_trading_tokens_v2.json` | Token mapping for intraday | 136 |
| `config/data_mining_tokens_v2.json` | Token mapping for mining | 1,632 |
| `config/crawler_summary.json` | Summary statistics | - |

### Instrument Distribution

#### Intraday Trading Crawler (136 instruments)
- **NIFTY 50 Equities**: 100 instruments
- **Futures Contracts**: 26 instruments
  - NIFTY futures (current + next month)
  - BANKNIFTY futures (current + next month) 
  - SENSEX futures (current + next month)
  - Top 20 NIFTY 50 stock futures
- **Key Indices**: 6 instruments
- **Major ETFs**: 5 instruments

#### Data Mining Crawler (1,632 instruments)
- **Equity**: 300+ instruments
- **Futures**: 200+ instruments
- **Options**: 300+ instruments
- **ETFs**: 50+ instruments
- **Indices**: 20+ instruments
- **Commodities**: 50+ instruments
- **Currencies**: 20+ instruments
- **NIFTY 50 Complete**: All equities, futures, options

## Usage

### 1. Generate Configurations

```bash
# Generate specialized crawler configurations
python3 config/build_specialized_crawlers.py
```

### 2. Start Crawlers

```bash
# Start all specialized crawlers
python3 start_specialized_crawlers.py

# Or start individually
python3 config/intraday_binary_crawler.py
python3 config/data_mining_binary_crawler.py
python3 config/intraday_binary_integration.py
```

### 3. Monitor Status

```bash
# Check running processes
ps aux | grep -E "(intraday|mining|crawler)"

# Check Redis streams
redis-cli XINFO STREAMS ticks:raw:binary:intraday
redis-cli XINFO STREAMS ticks:raw:binary:mining
```

## Crawler Details

### Intraday Trading Crawler

**Purpose**: High-frequency data collection for real-time trading decisions

**Key Features**:
- 100ms update frequency
- Priority instrument handling
- Real-time symbol resolution
- Optimized for low latency

**Target Instruments**:
- NIFTY 50 equities (all 50)
- Key index futures (NIFTY, BANKNIFTY, SENSEX)
- Most volatile NIFTY 50 stock futures
- Major ETFs (NIFTYBEES, BANKBEES, etc.)

**Redis Streams**:
- `ticks:raw:binary:intraday` - Raw binary data
- `market_data.ticks` - Processed tick data
- `ticks:{symbol}:{date}` - Individual symbol streams

### Data Mining Crawler

**Purpose**: Comprehensive market data collection for research and analysis

**Key Features**:
- 1000ms update frequency
- Category-based data organization
- Time-series data storage
- Historical data retention

**Target Instruments**:
- All NIFTY 50 instruments (equity, futures, options)
- All index instruments
- All ETFs
- Sample of other asset classes
- Commodities and currencies

**Redis Streams**:
- `ticks:raw:binary:mining` - Raw binary data
- `ticks:category:{category}` - Category-specific streams
- `ts:{category}:{symbol}` - Time-series data
- `ticks:{symbol}:{date}` - Individual symbol streams

## Integration with Existing System

### Pattern Detection
The intraday crawler data flows into the existing pattern detection system:

1. **Binary Integration** (`config/intraday_binary_integration.py`)
   - Consumes from `ticks:raw:binary:intraday`
   - Decodes binary data to readable format
   - Publishes to `market_data.ticks`

2. **Scanner** (`scanner/production/main.py`)
   - Subscribes to `market_data.ticks`
   - Processes through pattern detector
   - Sends alerts via alert manager

3. **Alert System**
   - macOS notifications
   - Telegram alerts
   - Pattern logging

### Data Flow

```
Intraday Crawler → Redis Stream → Integration → Scanner → Alerts
Data Mining Crawler → Redis Streams → Analysis Tools
```

## Performance Considerations

### Intraday Trading Crawler
- **Memory**: ~50MB for 136 instruments
- **CPU**: Low (100ms intervals)
- **Network**: ~1MB/min per instrument
- **Redis**: ~10MB/hour storage

### Data Mining Crawler
- **Memory**: ~200MB for 1,632 instruments
- **CPU**: Medium (1000ms intervals)
- **Network**: ~5MB/min total
- **Redis**: ~100MB/hour storage

## Monitoring and Logging

### Log Files
- `logs/intraday_crawler_YYYYMMDD.log`
- `logs/data_mining_crawler_YYYYMMDD.log`
- `logs/intraday_integration_YYYYMMDD.log`

### Key Metrics
- Tick processing rate
- Symbol resolution success rate
- Redis stream lag
- Memory usage
- Error rates

## Troubleshooting

### Common Issues

1. **Configuration Missing**
   ```bash
   python3 config/build_specialized_crawlers.py
   ```

2. **Redis Connection Failed**
   ```bash
   redis-server --daemonize yes
   ```

3. **WebSocket Authentication**
   - Update access token in crawler scripts
   - Ensure token has market data permissions

4. **High Memory Usage**
   - Check Redis memory usage: `redis-cli INFO memory`
   - Clear old streams: `redis-cli FLUSHDB`

### Debug Commands

```bash
# Check Redis streams
redis-cli XINFO STREAMS ticks:raw:binary:intraday
redis-cli XINFO STREAMS ticks:raw:binary:mining

# Monitor live data
redis-cli XREAD STREAMS ticks:raw:binary:intraday $

# Check symbol resolution
redis-cli HGETALL latest_prices | head -10
```

## Future Enhancements

1. **Authentication Management**
   - Automatic token refresh
   - Multiple broker support

2. **Data Compression**
   - ZSTD compression for historical data
   - Efficient storage formats

3. **Advanced Filtering**
   - Volume-based filtering
   - Price movement thresholds

4. **Real-time Analytics**
   - Live dashboard
   - Performance metrics

5. **Backup and Recovery**
   - Data persistence
   - Crash recovery

## Support

For issues or questions:
1. Check log files for errors
2. Verify Redis connectivity
3. Ensure proper authentication
4. Review configuration files

---

**Note**: These crawlers are designed to work with the existing trading system. Ensure the main scanner and alert system are running for complete functionality.
