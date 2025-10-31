# Trading System Troubleshooting Guide

## 🎯 UNIFIED SYSTEM - 8 CORE PATTERNS WITH VOLUME ARCHITECTURE

### **Current System State (Post-Audit):**
- ✅ **8 Core Patterns**: volume_spike, volume_breakout, volume_price_divergence, upside_momentum, downside_momentum, breakout, reversal, hidden_accumulation
- ✅ **Unified Volume Architecture**: TimeAwareVolumeBaseline + CorrectVolumeCalculator + VolumeResolver
- ✅ **Field Mapping System**: All components use `utils.field_mapping` utilities
- ✅ **VIX-Aware Thresholds**: Dynamic, market-aware threshold system with Indian market optimization
- ✅ **Redis Infrastructure**: Fixed configuration, Lua script optimization, all databases operational
- ✅ **End-to-End Integration**: Complete data flow from WebSocket to alerts with unified volume calculations

### **🚀 MODERN CLI TOOLS AVAILABLE**

```bash
# Fast file search
fd "*.py" --type f | head -10

# Fast text search  
rg "import redis" --type py

# Modern directory listing
eza -la --git --header

# Syntax-highlighted file viewing
bat config/zerodha_config.py

# Redis operations
redis-cli --scan --pattern "*" | head -10
```

This document captures the end‑to‑end alert data flow (with key variable names) and a quick checklist of commands to isolate issues at each stage.

## 🐍 PYTHON ENVIRONMENT

### Using .venv (Recommended)
```bash
# Always activate the virtual environment first
cd /Users/lokeshgupta/Desktop/intraday_trading
source .venv/bin/activate

# Verify environment
python -c "import redis, pandas, numpy; print('✅ All dependencies OK')"

# Run scripts with activated environment
python scanner_main.py
python core/utils/update_all_20day_averages.py
python research/bayesian_data_extractor.py
```

### Environment Status
- **Conda**: ❌ Removed (all environments deleted)
- **Virtual Environment**: ✅ `.venv` (Python 3.13.7)
- **Dependencies**: ✅ All packages installed in `.venv`

---

## 🎯 UNIFIED DATA FLOW - 8 CORE PATTERNS WITH VOLUME ARCHITECTURE

```
┌─────────────────────────────┐
│ Zerodha WebSocket Feed      │
│ · last_traded_quantity      │
│ · volume_traded (cumulative)│
│ · price, timestamp          │
└──────────────┬──────────────┘
               │ tick_data
               ▼
┌────────────────────────────────────────────┐
│ core/data/data_pipeline.py                 │
│ · Clean tick data with field mapping       │
│ · Store in Redis time buckets (TTL: 1hr)  │
│ · Use unified volume architecture          │
│ · Generate indicators dict                 │
└──────────────┬─────────────────────────────┘
               │ indicators: {volume_ratio, price_change, rsi, ...}
               ▼
┌────────────────────────────────────────────┐
│ patterns/pattern_detector.py               │
│ · 8 Core Patterns with VIX-aware thresholds│
│   - volume_spike, volume_breakout          │
│   - volume_price_divergence               │
│   - upside_momentum, downside_momentum     │
│   - breakout, reversal                    │
│   - hidden_accumulation                   │
│ · Uses TimeAwareVolumeBaseline             │
│ · Uses CorrectVolumeCalculator            │
└──────────────┬─────────────────────────────┘
               │ patterns: [{symbol, pattern, confidence, ...}]
               ▼
┌────────────────────────────────────────────┐
│ alerts/alert_manager.py                    │
│ · ProductionAlertManager                   │
│ · VIX-aware confidence thresholds          │
│ · Deduplication & conflict resolution      │
│ · Risk metrics calculation                 │
└──────────────┬─────────────────────────────┘
               │ alert payload
               ▼
┌────────────────────────────────────────────┐
│ alerts/notifiers.py                       │
│ · Human-readable templates                │
│ · Telegram + macOS notifications         │
└────────────────────────────────────────────┘

┌────────────────────────────────────────────┐
│ alert_validation/alert_validator.py        │
│ · Standalone validation system            │
│ · Uses VolumeResolver + CorrectVolumeCalc  │
│ · Redis pub/sub monitoring                │
└────────────────────────────────────────────┘
```

## 🗄️ REDIS DATABASE LAYOUT (SIMPLIFIED)

| DB | Purpose | Key Patterns | Usage |
|----|---------|--------------|-------|
| 0  | **System** | `session:*`, `system_config:*` | Health checks, configs |
| 2  | **Prices** | `price_history:{symbol}`, `ticks:{symbol}` | Real-time price data |
| 3  | **Premarket** | `premarket:*` | Pre-market order flow |
| 4  | **Continuous Market** | `ticks:*`, `patterns:*` | Live market streams |
| 5  | **Volume Data** | `volume_ratio:{symbol}:*`, `bucket:*` | **Pre-calculated volume ratios** |
| 6  | **Alerts** | `alerts:*`, `validation:*` | Alert storage & validation |
| 8  | **Microstructure** | Depth/flow analytics | Advanced analysis |
| 10 | **Patterns** | `pattern_candidates:*` | Pattern detection cache |
| 11 | **News** | `news:{symbol}:*` | News sentiment data |
| 13 | **Metrics** | Performance metrics | System monitoring |

### **🎯 Key Redis Operations:**
- **Volume Ratios**: Calculated using TimeAwareVolumeBaseline and stored in DB5 with TTL
- **Time Buckets**: Rolling 5-minute windows with auto-expire
- **Field Mapping**: All components use `utils.field_mapping` utilities
- **VIX Thresholds**: Dynamic thresholds from `config/thresholds.py`
- **Lua Script**: Performance optimization with `data/redis_calculations.lua`

---

## 🔧 TROUBLESHOOTING CHECKLIST - 8 CORE PATTERNS

### 1. **System Health Check**
```bash
# Check Redis health and volume pipeline
python core/data/redis_health.py

# Expected output:
# ✅ Redis connections: stable
# ✅ Volume bucket updates: consistent  
# ✅ Pattern detection: active
# ✅ Alert system: active
```

### 2. **Volume Architecture Issues**
- **Symptom:** Volume ratios always 1.0 or missing
- **Check:**
  ```bash
  # Test volume architecture components
  python -c "
  from utils.correct_volume_calculator import CorrectVolumeCalculator, VolumeResolver
  from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
  from core.data.redis_client import get_redis_client
  
  redis_client = get_redis_client()
  baseline = TimeAwareVolumeBaseline(redis_client)
  calculator = CorrectVolumeCalculator(redis_client)
  
  test_data = {'zerodha_last_traded_quantity': 1000, 'zerodha_cumulative_volume': 50000}
  incremental = VolumeResolver.get_incremental_volume(test_data)
  cumulative = VolumeResolver.get_cumulative_volume(test_data)
  print(f'Volume resolver: incremental={incremental}, cumulative={cumulative}')
  "
  
  # Check Redis volume data
  redis-cli --scan --pattern "volume_ratio:*" | wc -l
  redis-cli --scan --pattern "bucket:*" | wc -l
  redis-cli --scan --pattern "volume_baseline:*" | wc -l
  ```

### 3. **Pattern Detection Issues**
- **Symptom:** No patterns detected
- **Check:**
  ```bash
  # Test pattern detector with VIX-aware thresholds
  python -c "
  from patterns.pattern_detector import PatternDetector
  from core.data.redis_client import get_redis_client
  from config.thresholds import get_volume_threshold, get_confidence_threshold
  
  detector = PatternDetector(redis_client=get_redis_client())
  test_indicators = {
      'symbol': 'NIFTY',
      'last_price': 19500,
      'volume_ratio': 2.5,
      'price_change': 0.02,
      'rsi': 65
  }
  patterns = detector.detect_patterns(test_indicators)
  print(f'Patterns detected: {len(patterns)}')
  
  # Test VIX-aware thresholds
  volume_threshold = get_volume_threshold('volume_spike', 'banking', 'NORMAL')
  confidence_threshold = get_confidence_threshold('NORMAL')
  print(f'VIX-aware thresholds: volume={volume_threshold}, confidence={confidence_threshold}')
  "
  ```

### 4. **Alert System Issues**
- **Symptom:** No alerts being sent
- **Check:**
  ```bash
  # Check alert manager
  python -c "
  from alerts.alert_manager import ProductionAlertManager
  from core.data.redis_client import get_redis_client
  
  manager = ProductionAlertManager(redis_client=get_redis_client())
  print('Alert manager initialized successfully')
  "
  
  # Check Redis alert data
  redis-cli --scan --pattern "alerts:*" | wc -l
  ```

### 5. **Redis Connection Issues**
- **Symptom:** Redis connection errors
- **Check:**
  ```bash
  # Test Redis connection
  redis-cli ping
  
  # Check Redis configuration (fixed)
  python -c "
  from core.data.redis_client import get_redis_client
  client = get_redis_client()
  print('Redis client:', type(client))
  print('Ping result:', client.ping())
  print('All databases connected:', len(client.clients))
  "
  
  # Check Lua script
  ls -la data/redis_calculations.lua
  
  # Check field mapping system
  python -c "
  from utils.field_mapping import resolve_session_field
  last_price = resolve_session_field('last_price')
  volume = resolve_session_field('zerodha_cumulative_volume')
  print(f'Field mapping: last_price={last_price}, volume={volume}')
  "
  ```

---

## 🚀 QUICK START COMMANDS

### **Start Trading System:**
```bash
# 1. Start the main scanner
python scanner_main.py

# 2. Monitor system health
python core/data/redis_health.py

# 3. Check volume pipeline health
python -c "
from scanner_main import MarketScanner
import asyncio
scanner = MarketScanner()
asyncio.run(scanner.check_volume_pipeline_health())
"
```

### **System Monitoring:**
```bash
# Check Redis health
redis-cli ping

# Count volume data
redis-cli --scan --pattern "volume_ratio:*" | wc -l
redis-cli --scan --pattern "bucket:*" | wc -l

# Check alerts
redis-cli --scan --pattern "alerts:*" | wc -l

# Monitor logs
tail -f logs/scanner.log
```

### **Testing Components:**
```bash
# Test volume architecture
python -c "
from utils.correct_volume_calculator import CorrectVolumeCalculator, VolumeResolver
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from core.data.redis_client import get_redis_client
import asyncio

redis_client = get_redis_client()
baseline = TimeAwareVolumeBaseline(redis_client)
calculator = CorrectVolumeCalculator(redis_client)

test_data = {'zerodha_last_traded_quantity': 1000, 'zerodha_cumulative_volume': 50000}
volume_ratio = asyncio.run(calculator.calculate_volume_ratio('RELIANCE', test_data))
print(f'✅ Volume architecture ready: ratio={volume_ratio}')
"

# Test pattern detector with VIX thresholds
python -c "
from patterns.pattern_detector import PatternDetector
from core.data.redis_client import get_redis_client
from config.thresholds import get_volume_threshold
detector = PatternDetector(redis_client=get_redis_client())
volume_threshold = get_volume_threshold('volume_spike', 'banking', 'NORMAL')
print(f'✅ Pattern detector ready: volume_threshold={volume_threshold}')
"

# Test alert manager  
python -c "
from alerts.alert_manager import ProductionAlertManager
from core.data.redis_client import get_redis_client
manager = ProductionAlertManager(redis_client=get_redis_client())
print('✅ Alert manager ready')
"
```

---

## 🎯 **CURRENT SYSTEM ARCHITECTURE (POST-AUDIT)**

### **Volume Architecture Components:**
- **TimeAwareVolumeBaseline**: Calculates volume baselines based on time-of-day patterns
- **CorrectVolumeCalculator**: Single source of truth for volume calculations
- **VolumeResolver**: Unified volume field resolution across all components

### **Field Mapping System:**
- **Canonical Source**: `config/optimized_field_mapping.yaml`
- **Utilities**: `utils.field_mapping` functions used across all components
- **Consistency**: No more hardcoded field names

### **VIX-Aware Thresholds:**
- **Centralized**: `config/thresholds.py` with Indian market optimization
- **Dynamic**: VIX regime-based multipliers (PANIC, HIGH, NORMAL, LOW)
- **Sector-Aware**: All 8 core patterns have sector-specific adjustments

### **Redis Infrastructure:**
- **Fixed Configuration**: No more `max_connections` parameter errors
- **Lua Script**: `data/redis_calculations.lua` for performance optimization
- **All Databases**: 0-13 operational with connection pooling

### **Component Integration:**
- **scanner_main.py**: Uses TimeAwareVolumeBaseline + CorrectVolumeCalculator
- **alert_validator.py**: Uses VolumeResolver + CorrectVolumeCalculator  
- **pattern_detector.py**: Uses TimeAwareVolumeBaseline + CorrectVolumeCalculator
- **data_pipeline.py**: Uses field mapping utilities

---

## ✅ **SYSTEM READY FOR TRADING**

**The unified system is ready to use:**
- ✅ **8 Core Patterns** with VIX-aware thresholds
- ✅ **Unified Volume Architecture** with time-aware baselines
- ✅ **Field Mapping System** for consistent field names
- ✅ **Redis Infrastructure** with Lua script optimization
- ✅ **End-to-end integration** with unified volume calculations
- ✅ **Indian Market Optimization** for Nifty50 reality

**Key Improvements:**
- **Volume Calculations**: Now use TimeAwareVolumeBaseline + CorrectVolumeCalculator
- **Field Consistency**: All components use utils.field_mapping utilities
- **VIX Thresholds**: Dynamic, market-aware threshold system
- **Redis Configuration**: Fixed and optimized with Lua script
- **No Hardcoded Values**: Everything uses dynamic data from existing infrastructure

**No historical data population needed** - the system works with real-time data!

---

*Last updated: January 15, 2025 - Unified system with volume architecture and field mapping*
