# 🚀 Intraday Trading System - How to Use

## Overview
This is a comprehensive intraday trading system with real-time data processing, pattern detection, and automated alerts. The system uses Polars/DuckDB for high-performance analytics and Redis for real-time data storage.

## 🏗️ System Architecture

### Core Components
- **Data Pipeline**: Real-time tick data processing from NSE/BSE
- **Pattern Detector**: Advanced algorithmic pattern recognition
- **Alert Manager**: Multi-channel notification system (macOS + Telegram)
- **Risk Manager**: Position sizing and risk controls
- **Polars/DuckDB Integration**: High-performance columnar analytics

### Directory Structure
```
intraday_trading/
├── config/                 # Configuration files
│   ├── data_patterns.py    # Polars/DuckDB processing patterns
│   ├── data_definitions.py # Schema definitions
│   ├── telegram_config.json # Alert notifications
│   └── unified_schema.py   # Data schemas
├── scanner/                # Trading scanner system
│   ├── production/         # Production-ready components
│   ├── alert_manager.py    # Alert filtering & notifications
│   └── pattern_detector.py # Pattern recognition
├── data/                   # Data storage
├── logs/                   # System logs
└── .venv_new/             # Python virtual environment
```

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Navigate to project directory
cd /Users/apple/Desktop/intraday_trading

# Activate virtual environment
source .venv_new/bin/activate

# Verify Polars/DuckDB installation
python3 -c "import polars as pl; import duckdb; print('✅ Ready!')"
```

### 2. Configuration Setup

#### Telegram Alerts (Optional)
Edit `alerts/config/telegram_config.json`:
```json
{
  "bot_token": "YOUR_TELEGRAM_BOT_TOKEN_HERE",
  "chat_ids": ["YOUR_TELEGRAM_CHAT_ID_HERE"]
}
```

#### Environment Variables
Your `.env` file should contain:
```bash
ZERODHA_API_KEY="your_api_key"
ZERODHA_API_SECRET="your_api_secret"
KITE_ACCESS_TOKEN="your_access_token"
```

### 3. Start the System

#### Option A: Full Production System
```bash
cd /Users/apple/Desktop/intraday_trading
source .venv_new/bin/activate
python3 scanner/production/main.py
```

#### Option B: Test Integration
```bash
cd /Users/apple/Desktop/intraday_trading
source .venv_new/bin/activate
python3 test_integration.py
```

## 📊 Data Processing Features

### Polars/DuckDB Integration
The system uses optimized columnar processing for real-time analytics:

```python
from config.data_patterns import DataPatterns
from config.data_definitions import get_data_definitions

# Initialize components
dp = DataPatterns()
dd = get_data_definitions()

# Process tick data with vectorized operations
df = dp.process_tick_batch_polars(tick_data)

# Run analytical queries
results = dp.query_with_duckdb(df, "SELECT symbol, AVG(last_price) FROM df GROUP BY symbol")
```

### Key Features:
- **10-100x faster** than traditional pandas
- **Vectorized operations** for technical indicators
- **Real-time pattern detection** with DuckDB SQL
- **Memory-efficient** columnar storage
- **Redis integration** for cumulative volume/price data

## 🎯 Alert System Configuration

### Alert Manager Settings
The alert system filters and prioritizes trading signals:

#### Confidence Thresholds
```python
# In alert_manager.py
DERIVATIVE_CONF_THRESHOLD = 0.80  # 80% for derivatives
HIGH_CONF_THRESHOLD = 0.80        # 80% high confidence
MEDIUM_CONF_THRESHOLD = 0.75      # 75% medium confidence
```

#### Price Filters (Retail Trading)
```python
RETAIL_MIN_PRICE = 18      # ₹18 minimum
RETAIL_MAX_PRICE = 5000    # ₹5000 maximum
```

#### Cooldown Periods (Anti-Spam)
```python
DERIVATIVE_COOLDOWN = 60   # 60 seconds for derivatives
EQUITY_COOLDOWN = 10       # 10 seconds for equities
```

### Notification Channels

#### 1. macOS Notifications
- **Critical**: Submarine sound
- **High**: Glass sound
- **Info**: No sound

#### 2. Telegram Integration
- Multi-recipient support
- HTML-formatted messages
- Automatic retry on failure

### Alert Types
- **Volume Spikes**: 1.5x average volume
- **Price Momentum**: >2% price change
- **Pattern Detection**: Railway, PSU Dump, Breakdown patterns
- **Sector Cascade**: Multi-stock sector movements

## 🔧 Advanced Configuration

### Redis Database Segmentation
```python
REDIS_DATABASES = {
    0: 'system_general',      # System configs
    1: 'spoofing_alerts',     # Spoofing detections
    2: 'asset_price_data',    # Price data by category
    3: 'premarket_volume',    # Premarket data
    4: 'continuous_market',   # Real-time 5s intervals
    5: 'cumulative_volume',   # Daily cumulative data
    6: 'alert_system',        # Pattern alerts
    7: 'alert_validation'     # Alert outcome tracking
}
```

### Risk Management
```python
PROFIT_THRESHOLDS = {
    'PSU_DUMP': 0.10,      # 0.10% minimum profit
    'RAILWAY': 0.10,       # 0.10% minimum profit
    'BREAKDOWN': 0.10,     # 0.10% minimum profit
    'DEFAULT': 0.05        # 0.05% minimum profit
}
```

## 📈 Monitoring & Logs

### Log Files
- `scanner/production/logs/alerts_sent.log` - Alert history
- `logs/` - System operation logs
- `data/migration_summary.json` - Data processing logs

### Health Checks
```bash
# Check Redis connection
python3 -c "from scanner.production.utils.redis_client import get_redis_client; print('Redis:', get_redis_client().ping())"

# Check Polars/DuckDB
python3 -c "import polars as pl; import duckdb; print('Polars:', pl.__version__, 'DuckDB:', duckdb.__version__)"
```

## 🛠️ Development & Testing

### Running Tests
```bash
# Integration test
python3 test_integration.py

# Individual component tests
python3 -m pytest tests/ -v
```

### Adding New Patterns
1. Define pattern logic in `scanner/pattern_detector.py`
2. Add confidence thresholds in `alert_manager.py`
3. Test with sample data
4. Update documentation

### Performance Optimization
- Use Polars for vectorized operations
- Leverage DuckDB for analytical queries
- Implement Redis caching for frequently accessed data
- Monitor memory usage with large datasets

## 🚨 Troubleshooting

### Common Issues

#### 1. Redis Connection Failed
```bash
# Check Redis status
redis-cli ping

# Restart Redis
brew services restart redis
```

#### 2. Polars/DuckDB Import Error
```bash
# Reinstall dependencies
source .venv_new/bin/activate
pip install --upgrade polars duckdb pyarrow
```

#### 3. Telegram Notifications Not Working
- Verify `telegram_config.json` has correct bot token
- Check bot permissions in Telegram
- Ensure chat IDs are correct

#### 4. Memory Issues
- Reduce batch sizes in data processing
- Use streaming processing for large datasets
- Monitor Redis memory usage

### Debug Mode
```bash
# Enable debug logging
export PYTHONPATH=/Users/apple/Desktop/intraday_trading
python3 scanner/production/main.py --debug
```

## 📚 API Reference

### DataPatterns Class
```python
dp = DataPatterns()
df = dp.process_tick_batch_polars(ticks)  # Process with Polars
results = dp.query_with_duckdb(df, query) # Analytical queries
patterns = dp.detect_patterns_duckdb(df)  # Pattern detection
```

### AlertManager Class
```python
am = AlertManager()
am.send_alert(symbol, pattern, confidence)  # Send alert
am.filter_retail_alerts(alerts)            # Filter for retail
```

### RedisClient Class
```python
redis = get_redis_client()
redis.set('key', 'value')
redis.get('key')
```

## 🔒 Security & Best Practices

### API Key Management
- Store API keys in `.env` file
- Never commit sensitive data to git
- Rotate keys regularly

### Data Validation
- Validate all input data
- Use schema validation
- Implement rate limiting

### Error Handling
- Graceful degradation on failures
- Comprehensive logging
- Automatic retry mechanisms

## 📞 Support

### Getting Help
1. Check logs in `logs/` directory
2. Run `python3 test_integration.py` for diagnostics
3. Review configuration files for common issues
4. Check Redis connectivity

### Performance Tuning
- Monitor system resources
- Adjust batch sizes based on hardware
- Optimize Redis memory usage
- Use appropriate data partitioning

---

**Last Updated**: September 2025
**System Version**: Enterprise Grade Intraday Trading Platform
**Performance**: 10-100x faster with Polars/DuckDB integration</content>
<parameter name="filePath">/Users/apple/Desktop/intraday_trading/HOW_TO_USE.md