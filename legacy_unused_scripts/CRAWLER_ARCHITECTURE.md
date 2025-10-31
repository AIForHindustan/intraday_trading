# Crawler Architecture Documentation

## Overview

This document describes the crawler architecture for the intraday trading system, including which crawlers are active, deprecated, and their respective purposes.

## Active Crawlers

### 1. IntradayCrawler (`crawlers/zerodha_websocket/intraday_crawler.py`)
**Status**: ✅ ACTIVE (Production)

**Purpose**: Real-time intraday trading data collection and processing

**Features**:
- Publishes parsed tick data to Redis for real-time pattern detection
- Writes binary data to disk for historical analysis
- Uses multi-database Redis storage via `store_by_data_type()`
- Optimized for low-latency processing

**Redis Channels**:
- `market_data.ticks` - Main channel for scanner consumption
- `ticks:{asset_class}` - Asset-class specific channels
- `ticks:intraday:processed` - Intraday-specific stream
- `ticks:raw:binary` - Raw binary data for decoders

**Redis Databases**:
- **DB 4**: Continuous market data (primary tick storage)
- **Streaming Data**: Binary data storage
- **Real-time Data**: Processed tick data
- **5s Ticks**: Intraday-specific data

**File Writing**: ✅ Yes (binary data to disk)

**Scanner Compatibility**: ✅ Full compatibility

## Deprecated Crawlers

### 2. TradingCrawler (`crawlers/trading_crawler/trading_crawler.py`)
**Status**: ❌ DEPRECATED (Do Not Use)

**Reason for Deprecation**:
- Redundant functionality with IntradayCrawler
- Different Redis channel structure incompatible with scanner
- No file writing capabilities
- Potential Redis client errors

**Alternative**: Use `IntradayCrawler` instead

### 3. DataMiningCrawler (`crawlers/zerodha_websocket/data_mining_crawler.py`)
**Status**: ✅ ACTIVE (Specialized)

**Purpose**: Historical data collection only

**Features**:
- Writes raw binary data to disk for historical analysis
- No Redis publishing (file writing only)
- Optimized for data integrity and storage efficiency

**Redis Publishing**: ❌ None

**File Writing**: ✅ Yes (historical data only)

### 4. ResearchCrawler (`crawlers/zerodha_websocket/research_crawler.py`)
**Status**: ✅ ACTIVE (Specialized)

**Purpose**: Cross-venue analysis and research

**Features**:
- Writes raw binary data to disk for research and analysis
- No Redis publishing (file writing only)
- Supports SSO-XVENUE cross-venue analysis
- Optimized for data completeness

**Redis Publishing**: ❌ None

**File Writing**: ✅ Yes (research data only)

## Data Flow

### Production Data Flow
```
Zerodha WebSocket → IntradayCrawler → Redis (DB 4) → Scanner → Alerts
                              ↓
                        File Storage (Historical)
```

### Specialized Data Flows
```
Zerodha WebSocket → DataMiningCrawler → File Storage (Historical Analysis)
Zerodha WebSocket → ResearchCrawler → File Storage (Cross-Venue Research)
```

## Redis Channel Architecture

### Scanner-Consumed Channels (from IntradayCrawler)
- `market_data.ticks` - Primary tick data for pattern detection
- `ticks:equity_cash` - Equity cash market data
- `ticks:equity_futures` - Equity futures data
- `ticks:equity_options` - Equity options data
- `ticks:indices` - Index data
- `ticks:currency_futures` - Currency futures data
- `ticks:commodity_futures` - Commodity futures data

### Internal Processing Channels
- `ticks:raw:binary` - Raw binary data for binary decoder
- `ticks:intraday:processed` - Processed intraday data

## Configuration

### Launch Script
- **File**: `launch_crawlers.py`
- **Active Crawlers**: IntradayCrawler, DataMiningCrawler, ResearchCrawler
- **Deprecated**: TradingCrawler (not included in launch)

### Redis Client
- **IntradayCrawler**: Uses `RobustRedisClient` with `store_by_data_type()`
- **Other Crawlers**: Use basic Redis client for connection management

## Troubleshooting

### Common Issues

1. **Redis Client Errors**
   - Symptom: `'Redis' object has no attribute 'store_by_data_type'`
   - Solution: Ensure IntradayCrawler uses `RobustRedisClient`

2. **Scanner Not Receiving Data**
   - Check: IntradayCrawler is publishing to `market_data.ticks`
   - Verify: Scanner is subscribed to correct channels

3. **File Writing Issues**
   - Check: Data directory permissions
   - Verify: Crawler has write access to data directories

### Monitoring

- Check crawler logs for initialization warnings
- Monitor Redis stream activity
- Verify file creation in data directories
- Watch for deprecation warnings from TradingCrawler

## Future Enhancements

1. **Consolidate Crawlers**: Consider merging specialized crawlers
2. **Enhanced Monitoring**: Add health checks and metrics
3. **Configuration Management**: Centralize crawler configuration
4. **Error Recovery**: Improve reconnection and error handling

## Summary

- **Use IntradayCrawler** for real-time trading data
- **Avoid TradingCrawler** - it's deprecated and incompatible
- **Use specialized crawlers** for specific data collection needs
- **Monitor Redis channels** for data flow verification