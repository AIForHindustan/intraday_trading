# High-Performance Intraday Trading System
*Last Updated: November 4, 2025 - React Dashboard with 2FA & Brokerage Integration*

## 🧭 End-to-End Audit Playbook (Nightly Reference)
1. **Environment Baseline** — Activate the project venv and refresh dependencies: `python -m pip install -r requirements_apple_silicon.txt`; confirm `.env`/config secrets align with `config/optimized_field_mapping.yaml`.
2. **Redis & Data Spine** — Run `python core/data/redis_health.py` (uses consolidated client) to verify DB availability, bucket TTLs, and pattern/alert counts. Cross-check DB layouts and key patterns in `docs/redis/README.md`.
3. **Market Data Ingestion Dry Run** — Start the scanner for a bounded session: `python scanner_main.py --debug --duration 120`. Ensure the startup banner reports successful volume baselines, math dispatcher wiring, and Redis connectivity; abort if any single-source-of-truth check fails.
4. **Pattern & Math Integrity Sweep** — During the scanner dry run, tail logs for `MathDispatcher` warnings (search for “fallback math”) to confirm every detector stays on the shared engine. Any fallback hit requires a docstring + implementation review in `patterns/pattern_mathematics.py` or `core/math_dispatcher.py`.
5. **Risk & Alerts Validation** — Launch `python -m alert_validation.alert_validator` after market open; confirm the market-hours guard halts processing post 15:30 IST and that the generated report writes to `alert_validator_report_*.md`. Review the risk outputs in `alerts/risk_manager.py` for dispatcher usage.
6. **Data Quality & Reporting** — Execute `python quality_dashboard.py` for the full snapshot and `python quality_monitor.py` for the quick quality check. Archive the dashboards in `logs/audit/` with timestamps.
7. **Alert Dashboard Verification** — Start `python aion_alert_dashboard/alert_dashboard.py` and verify:
   - Alerts are loading from `alerts:stream` (check console output)
   - Price charts display full trading session data
   - Technical indicators overlay correctly (VWAP, EMAs, RSI, MACD)
   - Volume Profile shows POC and Value Area lines
   - Market indices (VIX, NIFTY 50, BANKNIFTY) refresh every 30s
   - News feed displays with 180-minute TTL filtering
   - Stop loss/target are correctly oriented for SELL vs BUY actions
   - **Statistical Model Section** displays validation performance metrics
   - Pattern performance charts show success rates and confidence scores
   - Forward validation window performance is tracked (1m, 2m, 5m, 10m, 30m, 60m)
7. **Docstring & Standards Pass** — Run `pydocstyle` (or `ruff check --select D` if available). Update docstrings immediately where violations are flagged, keeping canonical field references consistent with YAML mappings.

> ✅ Log findings in `audit.md` after each nightly run so the subsequent session knows which modules require corrective action.

## ✍️ Docstring Standardization Guardrails
- Prefer Google-style docstrings (`Args`, `Returns`, `Raises`) and explicitly reference canonical field names resolved via `resolve_session_field`.
- Every dispatcher-aware detector must describe the context payload it sends to `MathDispatcher`; reference pattern identifiers exactly as listed in `patterns/data/pattern_registry_config.json`.
- Redis touch points must document DB numbers, key prefixes, and expiry behaviour. If logic depends on a Redis script or Lua helper, cite the module path (for example `core/data/redis_storage.py` or `alert_validation/alert_streams.py`).
- Update docstrings in tandem with configuration changes so automated audit scripts can diff behaviour vs documentation without manual intervention.

## 🎯 **SYSTEM OVERVIEW**

**Comprehensive Pattern Detection System with Mathematical Integrity & Risk Manager Integration**
- **20 Total Patterns**: 8 Core + 6 ICT + 4 Straddle Strategies + 1 Kow Signal Straddle + 1 Volume Profile Advanced
- **Mathematical Integrity**: PatternMathematics class with confidence bounds (0.0-1.0) and outlier protection
- **Risk Manager Integration**: Dynamic expected_move, position_size, and stop_loss calculation
- **Volume Baseline Integration**: Centralized volume system with dynamic thresholds
- **Mathematical Volume Engine**: Time-aware baselines with session multipliers
- **Bucket Resolution Mathematics**: Dynamic scaling between time granularities
- **VIX Integration**: Market regime-aware threshold adjustments
- **Redis Publishing**: Full streaming architecture with pattern/indicator channels
- **Sectoral Analysis**: NSE sectoral indices integration for backtesting and correlation analysis (update using `utils/update_sectoral_volatility.py`)
- **Greek Calculations**: Complete options pricing with Delta, Gamma, Theta, Vega, Rho for F&O (Library used: `pandas-market-calendars`, `scipy.stats.norm`, `numpy`)

## 🏛️ **SINGLE SOURCES OF TRUTH**

### **📋 Field Name Standardization**
- **Primary Source**: `config/optimized_field_mapping.yaml`
- **Loader**: `utils/yaml_field_loader.py` (direct YAML access with caching)
- **Usage**: All components use `resolve_session_field()` and `resolve_calculated_field()`
- **Volume Fields**: Centralized volume state management in `core/data/volume_state_manager.py`

### **🔧 Data Schema Standardization**
- **Primary Source**: `config/schemas.py` (End-to-end data standardization)
- **Integration**: Field mapping + Volume state manager integration
- **Purpose**: Unified data processing pipeline with backward compatibility
- **Features**: Tick data normalization, session/indicator record creation, validation
- **Usage**: All scripts use standardized schemas for consistent data flow

### **🔢 Volume State Management**
- **Primary Source**: `core/data/volume_state_manager.py`
- **Class**: `VolumeStateManager` - session-aware incremental volume calculation with Redis persistence
- **Integration**: VolumeProfileManager integration for enhanced pattern detection
- **Usage**: Single calculation point in WebSocket Parser only
- **Fields**: `zerodha_cumulative_volume`, `bucket_incremental_volume`, `zerodha_last_traded_quantity`
- **Volume Profile**: Automatic POC/VA calculation from incremental volume data
- **Volume Profile Detector**: Advanced pattern detection with historical trend analysis

### **📈 Volume Averages (20-Day & 55-Day)**
- **Primary Source**: `config/volume_averages_20d.json` (345KB, 12,145 instruments)
- **Redis Storage**: Redis DB 5 with key pattern `volume_averages:NSE_SYMBOL`
- **Data Generation**: `utils/update_all_20day_averages.py` (daily before market open)
- **Redis Loading**: `load_volume_averages_to_redis.py` (759 symbols loaded)
- **Access Pattern**: Redis-first with JSON fallback across all scripts
- **Fields**: `avg_volume_20d`, `avg_volume_55d`, `avg_price_20d`, `avg_price_55d`, `turtle_ready`
- **Usage**: Pattern detection thresholds, volume ratio calculations, market analysis

### **🔗 Token Resolution**
- **Primary Source**: `core/data/token_lookup_enriched.json` (enriched metadata)
- **Resolver**: `crawlers/utils/instrument_mapper.py` (InstrumentMapper.token_to_symbol)
- **Usage**: All components use `InstrumentMapper().token_to_symbol()`

### **📊 Pattern Detection**
- **Primary Source**: `patterns/pattern_detector.py` (20 patterns with mathematical integrity)
- **Base Class**: `patterns/base_detector.py` (BasePatternDetector with common functionality)
- **Registry**: `patterns/data/pattern_registry_config.json` (20 total patterns)
- **Mathematical Integrity**: `patterns/pattern_mathematics.py` (PatternMathematics class)
- **Risk Manager Integration**: Dynamic risk metrics for all patterns
- **ICT Patterns**: 6 specialized ICT detectors with mathematical integrity
- **Straddle Strategies**: 4 options trading patterns + 1 Kow Signal Straddle
- **Volume Baseline Integration**: Centralized volume system with dynamic thresholds
- **Support/Resistance**: Statistical significance-based S/R level detection
- **Stop Hunt Zones**: Dynamic liquidity cluster identification with ATR-based distances
- **Volume Profile Detector**: Advanced pattern detection with historical trend analysis
- **Math Dispatcher**: `core/math_dispatcher.py` routes confidence, price targets, position sizing, and news boosts through `PatternMathematics` and the risk manager so every detector uses the same signatures and field names.

### **📊 Technical Indicators Engine**
- **Primary Library**: `ta-lib` (150+ technical indicators)
- **Custom Indicators**: VWAP, Volume Profile, Support/Resistance levels
- **Mathematical Engine**: `patterns/pattern_mathematics.py` for confidence calculations
- **Redis Integration**: All indicators published to Redis streams for real-time access
- **Pattern Integration**: All 20 patterns use technical indicators for signal generation
- **Indicator Categories**:
  - **Trend**: EMA (20, 50), SMA, MACD, ADX, Aroon, Parabolic SAR
  - **Momentum**: RSI (14), Stochastic, Williams %R, CCI, ROC
  - **Volatility**: Bollinger Bands (20, 2), ATR (14), Keltner Channels
  - **Volume**: OBV, AD Line, Chaikin Money Flow, Money Flow Index
  - **Custom**: VWAP, Volume Profile POC/VA, Support/Resistance levels

### **📈 Sectoral Analysis**
- **Primary Source**: `config/sector_volatility.json` (9 NSE sectoral indices)
- **Update Script**: `utils/update_sectoral_volatility.py` (NSE API integration)
- **Coverage**: 126+ stocks across 9 sectors (NIFTY_50, NIFTY_BANK, NIFTY_AUTO, NIFTY_IT, NIFTY_PHARMA, NIFTY_FMCG, NIFTY_METAL, NIFTY_ENERGY, NIFTY_REALTY)
- **Use Cases**: Backtesting, sector correlation analysis, risk management
- **Scheduling**: Weekly updates (Sundays at 6:00 PM IST)
- **Independent**: Separate from intraday crawler (174 instruments)

### **🗄️ Redis Configuration**
- **Primary Source**: `config/redis_config.py` (multi-database configuration)
- **Client**: `redis_files/redis_client.py` (RobustRedisClient with circuit breakers)
- **Storage**: `redis_files/redis_storage.py` (standardized field mapping)

### **📊 Mathematical Volume Engine**
- **Primary Source**: `utils/time_aware_volume_baseline.py` (TimeAwareVolumeBaseline)
- **Mathematical Engine**: `patterns/volume_thresholds.py` (VolumeThresholdCalculator)
- **Session Multipliers**: Market session-aware volume adjustments (opening: 1.8x, closing: 2.0x)
- **Bucket Resolution**: Dynamic scaling between 1min, 5min, 10min, 15min, 30min, 60min
- **VIX Integration**: Market regime-aware sensitivity adjustments (LOW: 0.8x, HIGH: 1.4x, PANIC: 1.8x)
- **Straddle Volume Tracking**: Combined CE/PE volume analysis for options strategies
- **Usage**: Pattern detection with mathematical precision

### **🎯 Greek Calculations Engine**
- **Primary Source**: `utils/calculations.py` (EnhancedGreekCalculator, ExpiryCalculator)
- **Libraries**: `pandas-market-calendars`, `scipy.stats.norm`, `numpy`
- **Features**: Black-Scholes model with NSE trading calendar awareness
- **Greeks**: Delta, Gamma, Theta, Vega, Rho with proper DTE calculations
- **Integration**: Automatic calculation for F&O options with Redis publishing
- **Usage**: Options pricing and risk management for pattern detection

### **📊 Volume Profile Engine**
- **Primary Source**: `patterns/volume_profile_manager.py` (VolumeProfileManager)
- **Advanced Detection**: `patterns/volume_profile_detector.py` (VolumeProfileDetector)
- **Libraries**: `market_profile` (MarketProfile class), `pandas`, `numpy`
- **Features**: Point of Control (POC), Value Area (70% volume), High/Low volume zones
- **Advanced Patterns**: Volume nodes, gaps, POC shifts, Value Area changes, profile strength
- **Integration**: Volume-based support/resistance levels for pattern detection
- **Usage**: Price level volume analysis and gravitational effect zones
- **Historical Analysis**: Trend analysis with 24-hour lookback for pattern detection

### **📊 Alert Validation Dashboard**
- **Primary Source**: `aion_alert_dashboard/alert_dashboard.py` (AlertValidationDashboard)
- **Framework**: Dash (Plotly) with Bootstrap components
- **Port**: 53056 (fixed for external consumer access)
- **External Access**: ngrok tunnel (https://jere-unporous-magan.ngrok-free.dev)
- **Features**:
  - Real-time alert visualization with interactive charts
  - Technical indicators overlay (VWAP, EMAs, Bollinger Bands, RSI, MACD)
  - Volume Profile visualization (POC, Value Area, distribution histogram)
  - Options Greeks display (Delta, Gamma, Theta, Vega, Rho)
  - News enrichment integration
  - **Statistical Model Integration**: Real-time validation performance metrics from `alert_validator.py`
    - Validation performance overview (total validations, success rate, avg confidence)
    - Pattern performance analysis (success rates and confidence by pattern type)
    - Forward validation window performance (1m, 2m, 5m, 10m, 30m, 60m success rates)
    - Data source: `validation_results:recent` (Redis DB 1) and `forward_validation:results` (Redis DB 0)
  - Market indices (VIX, NIFTY 50, BANKNIFTY) with 30s auto-refresh
  - Full trading session time series data
  - Action-aware stop loss/target correction (BUY vs SELL)
- **Data Sources**:
  - Redis DB 0: Alert storage (`alert:*`, `forward_validation:alert:*`)
  - Redis DB 1: Real-time data (`alerts:stream`, `ohlc_updates:*`, `ohlc_daily:*`, indicators, Greeks)
  - Redis DB 2: Analytics (`volume_profile:poc:*`, `volume_profile:distribution:*`)
  - Redis DB 5: Primary indicator storage (indicators_cache)
- **Integration Points**:
  - Scanner: Loads alerts from `alerts:stream` (primary source)
  - Redis Storage: Reads indicators/Greeks from multiple DBs
  - Volume Profile: Integrates with `VolumeProfileManager` for POC/VA data
  - News System: Enriches alerts with news from `news:MARKET_NEWS:*` keys
  - Instrument Mapper: Resolves tokens to symbols for display

## 🔄 **HOW THE SYSTEM WORKS**

### **1. Data Ingestion Flow**
```
Zerodha WebSocket → WebSocket Parser → Volume State Manager → Redis Streams
     ↓                    ↓                    ↓                    ↓
Raw Binary Data → Field Standardization → Single Calculation → Multi-DB Storage
```

**Note**: Order flow crawler has been removed for simplified architecture. All data processing now flows through the main WebSocket parser.

### **2. Volume Processing (Single Source of Truth)**
```
Zerodha Cumulative → VolumeStateManager → Incremental Volume → VolumeProfileManager
     ↓                        ↓                    ↓                    ↓
volume_traded_for_the_day → calculate_incremental() → bucket_incremental_volume → POC/VA Calculation
     ↓                        ↓                    ↓                    ↓
Pattern Detection → Volume Analysis → Price-Level Volume → Support/Resistance Levels
     ↓                        ↓                    ↓                    ↓
WebSocket Parser → Automatic Integration → Real-time Updates → Volume Profile Patterns
```

### **3. Volume Averages System (20-Day & 55-Day)**
```
Daily OHLC Data → update_all_20day_averages.py → config/volume_averages_20d.json
     ↓                        ↓                              ↓
Historical Analysis → 20d/55d Calculations → JSON Storage (345KB, 12,145 instruments)
     ↓                        ↓                              ↓
load_volume_averages_to_redis.py → Redis DB 5 → volume_averages:NSE_SYMBOL
     ↓                        ↓                              ↓
Script Access → Redis-First Pattern → JSON Fallback → Pattern Detection Thresholds
```

**Volume Averages Features:**
- **Data Generation**: Daily calculation before market open
- **Redis Storage**: 759 symbols loaded into Redis DB 5
- **Access Pattern**: Redis-first with JSON fallback for reliability
- **Fields**: 20-day/55-day volume & price averages, turtle breakout readiness
- **Usage**: Volume ratio calculations, pattern detection thresholds, market analysis

## ✅ Wiring Guidelines
- Always resolve field names through `utils.yaml_field_loader` (`resolve_session_field`, `resolve_calculated_field`) instead of hardcoding Zerodha keys.
- Obtain incremental volume and volume ratios from `core/data/volume_state_manager.py` and `utils.correct_volume_calculator`; do not recalculate ratios or apply manual caps.
- Import ICT components from `patterns.ict` and let `patterns/pattern_detector.py` orchestrate Redis access, killzone timing, and risk wiring.
- Run alert logic through `alerts/alert_manager.py`/`alerts/notifiers.py` so action text, position sizing, and stop-loss calculations remain centralized. Alert formatting uses streamlined actionable format only (legacy format removed).
- When you need historical candles or session stats, call `redis_client.get_ohlc_buckets()` or `redis_client.get_cumulative_data()`—those helpers already return standardized payloads.

## ⏰ Daily Pre-Market Checklist
- Regenerate averages: `python utils/update_all_20day_averages.py` then `python load_volume_averages_to_redis.py`.
- Refresh OHLC/sector volatility JSON if required.
- Start the live crawlers (Zerodha websocket, index feeds) so Redis buckets begin updating.
- Warm-test the pipeline:
  ```bash
  .venv/bin/python - <<'PY'
  import asyncio
  from scanner_main import MarketScanner
  scanner = MarketScanner(config={"continuous": False})
  loop = asyncio.get_event_loop()
  loop.run_until_complete(scanner.check_volume_pipeline_health())
  scanner.shutdown()
  PY
  ```
- Confirm all four health checks are `True` before enabling live alerts.

### **4. Field Name Resolution**
```
Component Request → yaml_field_loader.py → optimized_field_mapping.yaml → Canonical Field
     ↓                        ↓                        ↓                        ↓
resolve_session_field() → Direct YAML Access → Field Mapping → Standardized Field Name
```

### **5. Schema Standardization Pipeline**
```
Raw Tick Data → schemas.py → Field Mapping → Volume Manager → Standardized Output
     ↓              ↓              ↓              ↓              ↓
WebSocket Data → normalize_zerodha_tick_data() → Field Resolution → Volume Integration → Consistent Schema
     ↓              ↓              ↓              ↓              ↓
Pattern Detection → process_tick_data_end_to_end() → Validation → Enhanced Data → All Scripts
```

**Schema Integration Features:**
- **End-to-End Processing**: `process_tick_data_end_to_end()` - Complete pipeline processing
- **Session Records**: `create_session_record_end_to_end()` - Standardized session data
- **Indicator Records**: `create_indicator_record_end_to_end()` - Standardized indicator data
- **Data Validation**: `validate_data_with_field_mapping()` - Schema compliance checking
- **Volume Integration**: Automatic volume profile data enhancement
- **Backward Compatibility**: Graceful fallbacks when components unavailable

### **6. Pattern Detection Pipeline**
```
Redis Streams → Data Pipeline → Field Mapping → Pattern Detector → Alert Manager
     ↓              ↓              ↓              ↓              ↓
Tick Data → Standardized Fields → Volume State → Pattern Analysis → Trading Alerts
```

### **7. Redis Multi-Database Architecture**
```
DB 0: System → system_config, metadata, session_data, health_checks, volume baselines
DB 1: Realtime → alerts:stream, ticks:*, patterns:*, news:* (NO indicators - indicators moved to DB 5)
DB 2: Analytics → volume_profile:poc:*, volume_profile:distribution:*, ohlc_daily:*, metrics
DB 3: Independent Validator → signal_quality, pattern_performance (isolated from main system)
DB 5: Indicators Cache → indicators:*, Greeks (PRIMARY location per redis_config.py)
```

**⚠️ CRITICAL: Redis Storage Schema for Indicators, Greeks, and Volume Profile**

All assistants must know where indicators, Greeks, and volume profile are stored in Redis:

#### **📊 Technical Indicators (DB 5 - PRIMARY) ✅ SINGLE SOURCE OF TRUTH**
- **Key Pattern**: `indicators:{symbol}:{indicator_name}`
- **Examples**:
  - `indicators:NFO:NIFTY25DEC26000CE:rsi` → RSI value (float as string)
  - `indicators:NFO:NIFTY25DEC26000CE:atr` → ATR value (float as string)
  - `indicators:NFO:NIFTY25DEC26000CE:vwap` → VWAP value (float as string)
  - `indicators:NFO:NIFTY25DEC26000CE:ema_20` → EMA 20 value (float as string)
  - `indicators:NFO:NIFTY25DEC26000CE:ema_50` → EMA 50 value (float as string)
  - `indicators:NFO:NIFTY25DEC26000CE:macd` → MACD dict (JSON: `{'value': {'macd': ..., 'signal': ..., 'histogram': ...}, 'timestamp': ..., 'symbol': ...}`)
  - `indicators:NFO:NIFTY25DEC26000CE:bollinger_bands` → BB dict (JSON: `{'value': {'upper': ..., 'middle': ..., 'lower': ...}, ...}`)
- **Storage Method**: Both `redis_storage.publish_indicators_to_redis()` and `data_pipeline._store_calculated_indicators()` store via `indicators_cache` data type (DB 5)
- **Data Type**: `indicators_cache` → maps to DB 5 per `redis_config.py`
- **TTL**: 300 seconds (5 minutes) to match calculation cache
- **Format**: 
  - Simple indicators (RSI, EMA, ATR, VWAP): Stored as string/number
  - Complex indicators (MACD, BB, Volume Profile): Stored as JSON with `{'value': {...}, 'timestamp': ..., 'symbol': ...}`
- **Fallback**: Dashboard checks DB 5 first, then DB 1 (for backward compatibility during migration)
- **Calculation**: Pure Python libraries (pandas_ta → pandas → polars → TA-Lib fallback)

#### **📈 Options Greeks (DB 5 - PRIMARY) ✅ SINGLE SOURCE OF TRUTH**
- **Key Patterns**:
  - `indicators:{symbol}:greeks` → Combined Greeks dict (JSON: `{'value': {'delta': ..., 'gamma': ..., 'theta': ..., 'vega': ..., 'rho': ...}, 'timestamp': ..., 'symbol': ...}`)
  - `indicators:{symbol}:delta` → Delta value (float as string)
  - `indicators:{symbol}:gamma` → Gamma value (float as string)
  - `indicators:{symbol}:theta` → Theta value (float as string)
  - `indicators:{symbol}:vega` → Vega value (float as string)
  - `indicators:{symbol}:rho` → Rho value (float as string)
- **Examples**:
  - `indicators:NFO:NIFTY25DEC26000CE:greeks` → All Greeks combined
  - `indicators:NFO:NIFTY25DEC26000CE:delta` → Individual Delta
- **Storage Method**: Both `data_pipeline._store_calculated_indicators()` and `redis_storage.publish_indicators_to_redis()` store via `indicators_cache` data type (DB 5)
- **Data Type**: `indicators_cache` → maps to DB 5 per `redis_config.py`
- **TTL**: 300 seconds (5 minutes) to match calculation cache
- **Format**: 
  - Combined: JSON with nested `{'value': {'delta': ..., 'gamma': ..., ...}, ...}`
  - Individual: Float as string
- **Fallback**: Dashboard checks DB 5 first, then DB 1 (for backward compatibility during migration)
- **Calculation**: `EnhancedGreekCalculator.black_scholes_greeks()` using `scipy.stats.norm` (pure Python, NO TA-Lib)

#### **📊 Volume Profile (DB 2 - Analytics, DB 1 - Fallback)**
- **Primary Storage**: Redis DB 2 (analytics), key: `volume_profile:poc:{symbol}` (Hash)
- **Key Patterns**:
  - `volume_profile:poc:{symbol}` → Hash with POC/VA data (`poc_price`, `poc_volume`, `value_area_high`, `value_area_low`, `profile_strength`, `exchange_timestamp`)
  - `volume_profile:session:{symbol}:{YYYY-MM-DD}` → Hash with full profile data including `price_volume_distribution` (bin volumes)
  - `volume_profile:distribution:{symbol}:{YYYY-MM-DD}` → Hash with price-volume bins (`{price: volume, ...}`)
  - `volume_profile:nodes:{symbol}` → Hash with support/resistance levels
  - `volume_profile:patterns:{symbol}:daily` → Sorted Set with historical patterns
  - `volume_profile:historical:{symbol}` → List with last 24 hours (FIFO queue)
  - `indicators:{symbol}:poc_price` → POC price indicator (float as string, stored in DB 1)
- **Examples**:
  - `volume_profile:poc:NFO:NIFTY25DECFUT` → Hash with POC data
  - `indicators:NFO:NIFTY25DECFUT:poc_price` → POC price as indicator
- **Storage Method**: 
  - Primary: `VolumeProfileManager` stores in DB 2 (analytics), TTL: 24 hours
  - Update Frequency: Every 100 ticks or 1 minute (whichever comes first)
  - Secondary: `redis_storage.publish_indicators_to_redis()` stores POC/VA in DB 1 as indicators
- **Format**: Hash (for poc/distribution) or string/JSON (for indicators)
- **Calculation**: `HybridCalculations.calculate_volume_profile()` using Polars/pandas
- **Output Signature**: `get_profile_data()` returns dict with `price_volume_distribution` field (bin volumes)
- **Documentation**: See `patterns/VOLUME_PROFILE_AUDIT.md`, `patterns/VOLUME_PROFILE_FIXES_APPLIED.md`, `patterns/VOLUME_PROFILE_OVERRIDE_ANALYSIS.md`

#### **🔑 Key Access Patterns**
- **Dashboard**: `alert_dashboard.py` loads from DB 1 (realtime), DB 5 (indicators), DB 2 (volume profile)
- **Alert Templates**: `alerts/notifiers.py` extracts from alert payload (`signal['indicators']`, `signal['delta']`, etc.)
- **Alert Validator**: `alert_validation/alert_validator.py` can load from Redis if needed
- **Alert Manager**: `alerts/alert_manager.py` merges indicators from Redis into alert payload via `_fetch_indicators_from_redis()`

#### **📝 Important Notes**
- All indicators/Greeks calculated by `HybridCalculations` (pure Python: pandas_ta/pandas/scipy)
- Greeks only exist for F&O instruments (options/futures with CE/PE/FUT suffix)
- Volume Profile POC stored both as hash (DB 2) and indicator (DB 1) for easy access
- Dashboard checks multiple DBs and key variants for maximum compatibility

### **8. Mathematical Volume Engine**
```
Time-Aware Baselines → Session Multipliers → Bucket Resolution → VIX Integration
     ↓                        ↓                    ↓                    ↓
1min baseline → Market sessions → Dynamic scaling → Threshold adjustment
     ↓                        ↓                    ↓                    ↓
Redis storage → Opening: 1.8x → 1min→5min→10min → LOW: 0.8x, HIGH: 1.4x
```

**Mathematical Engine Examples:**
- **Base Calculation**: 1min baseline = 64.4M (from 5min: 322M ÷ 5)
- **Session Adjustment**: Opening frenzy = 64.4M × 1.8 = 116M
- **Bucket Scaling**: 10min baseline = 64.4M × 10 = 644M
- **VIX Integration**: High volatility = 644M × 1.4 = 901M threshold

### **9. Greek Calculations Engine**
```
F&O Options → Spot Price Detection → Black-Scholes Model → Greeks Calculation
     ↓                    ↓                    ↓                    ↓
NFO:SYMBOL → underlying_price → Strike/Expiry/Vol → Delta/Gamma/Theta/Vega/Rho
     ↓                    ↓                    ↓                    ↓
Redis Publishing → Pattern Detection → Risk Management → Trading Alerts
```

**Greek Calculations Examples:**
- **Delta**: Price sensitivity (0.0-1.0 for calls, -1.0-0.0 for puts)
- **Gamma**: Delta sensitivity (highest near ATM, lowest for ITM/OTM)
- **Theta**: Time decay (negative for long options, positive for short)
- **Vega**: Volatility sensitivity (highest for ATM options)
- **Rho**: Interest rate sensitivity (minimal for short-term options)

### **10. Volume Profile Engine**
```
Raw Ticks → VolumeStateManager → VolumeProfileManager → VolumeProfileDetector
     ↓                    ↓                    ↓                    ↓
Price-Volume Data → Incremental Volume → POC/VA Calculation → Advanced Pattern Detection
     ↓                    ↓                    ↓                    ↓
Pattern Detection → Support/Resistance → Historical Analysis → Enhanced Trading Signals
```

**Volume Profile Examples:**
- **POC (Point of Control)**: Price level with highest traded volume
- **Value Area**: Price range containing 70% of total volume
- **Support Levels**: High volume nodes below current price
- **Resistance Levels**: High volume nodes above current price
- **Profile Strength**: Volume concentration metric (0.0-1.0)
- **Volume Nodes**: High volume areas (support/resistance) and low volume gaps (breakout zones)
- **POC Shifts**: Significant Point of Control movements indicating trend changes
- **Value Area Changes**: Expansion/contraction of the 70% volume range

### **9. Redis Storage Schema for Volume Profile**
```
Volume Profile Storage Keys (Complete Implementation - Redis DB 2):
├── volume_profile:poc:SYMBOL → Hash with POC/VA for quick access (PRIMARY POC STORAGE)
│   └─> Fields: poc_price, poc_volume, value_area_high, value_area_low, profile_strength, exchange_timestamp
├── volume_profile:session:SYMBOL:YYYY-MM-DD → Hash with full profile data (includes price_volume_distribution)
├── volume_profile:distribution:SYMBOL:YYYY-MM-DD → Hash with price-volume distribution buckets
│   └─> Format: {price: volume, price: volume, ...} for histogram/chart rendering
├── volume_profile:nodes:SYMBOL → Hash with support/resistance levels
├── volume_profile:patterns:SYMBOL:daily → Sorted Set with historical patterns (by timestamp)
└── volume_profile:historical:SYMBOL → List with historical profile data (24h lookback, FIFO queue)

Pattern Detection Keys:
├── patterns:volume_profile:SYMBOL → Hash with detected volume profile patterns
└── patterns:volume_profile_advanced:SYMBOL → Hash with advanced pattern detection results

Note: All keys have 24-hour TTL (86400 seconds). Dashboard reads from DB 2 first, falls back to DB 1.

Indicator Stream Keys (Real-time):
├── indicators:poc_price:SYMBOL → POC price indicator stream
├── indicators:poc_volume:SYMBOL → POC volume indicator stream
├── indicators:value_area_high:SYMBOL → Value Area high indicator stream
├── indicators:value_area_low:SYMBOL → Value Area low indicator stream
├── indicators:profile_strength:SYMBOL → Profile strength indicator stream
├── indicators:support_levels:SYMBOL → Support levels indicator stream
└── indicators:resistance_levels:SYMBOL → Resistance levels indicator stream

Data Structure Examples:
├── Price-Volume Distribution: {"19500.00": "12500", "19500.05": "9800", "19500.10": "15600"}
├── Session Profile Summary: {"poc_price": "19500.50", "poc_volume": "45600", "value_area_high": "19550.25"}
└── Historical Patterns: {"2024-01-15T15:30:00": '{"poc_price": 19500.5, "profile_strength": 0.85}'}

Query Interface for Pattern Detection:
```python
# Get historical volume profiles for pattern detection
def get_historical_profiles(symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
    patterns_key = f"volume_profile:patterns:{symbol}:daily"
    start_ts = start_date.timestamp()
    end_ts = end_date.timestamp()
    profile_data = redis_client.zrangebyscore(patterns_key, start_ts, end_ts)
    return sorted([json.loads(data) for data in profile_data], key=lambda x: x.get('session_date', ''))

# Get price-volume distribution buckets
def get_price_volume_distribution(symbol: str, date: str = None) -> Dict:
    distribution_key = f"volume_profile:distribution:{symbol}:{date or datetime.now().strftime('%Y-%m-%d')}"
    return redis_client.hgetall(distribution_key)

# Get session profile summary
def get_session_profile_summary(symbol: str, date: str = None) -> Dict:
    session_key = f"volume_profile:session:{symbol}:{date or datetime.now().strftime('%Y-%m-%d')}"
    return redis_client.hgetall(session_key)
```

## 🔧 **KEY COMPONENTS & THEIR ROLES**

### **🔌 Data Ingestion**
- **`crawlers/websocket_message_parser.py`**: Single calculation point for volume processing using VolumeStateManager
- **`crawlers/zerodha_websocket/intraday_crawler.py`**: Main data ingestion from Zerodha
- **`core/data/volume_state_manager.py`**: VolumeStateManager for session-aware calculations

### **📊 Data Processing**
- **`core/data/data_pipeline.py`**: Main data processing pipeline (uses pre-calculated volume data)
- **`core/data/redis_client.py`**: RobustRedisClient with circuit breakers
- **`core/data/redis_storage.py`**: Standardized field mapping for storage with Greek calculations and Volume Profile publishing
- **`core/data/volume_profile_manager.py`**: Volume profile calculations with POC/VA analysis
- **`core/data/volume_profile_detector.py`**: Advanced volume profile pattern detection with historical analysis

### **🔧 Schema Standardization**
- **`config/schemas.py`**: End-to-end data standardization with field mapping and volume manager integration
- **Purpose**: Unified data processing pipeline ensuring consistency across all scripts
- **Key Functions**:
  - `process_tick_data_end_to_end()`: Complete pipeline processing with validation
  - `create_session_record_end_to_end()`: Standardized session data creation
  - `create_indicator_record_end_to_end()`: Standardized indicator data creation
  - `normalize_zerodha_tick_data()`: WebSocket data normalization with field mapping
  - `validate_data_with_field_mapping()`: Schema compliance validation
  - `get_volume_state_data()`: Volume profile integration for pattern detection
  - `track_straddle_volume()`: Kow Signal Straddle strategy support
- **Integration**: Field mapping + Volume state manager + Backward compatibility
- **Usage**: All scripts use schemas.py for consistent data flow and validation

### **🎯 Pattern Detection**
- **`patterns/pattern_detector.py`**: 8 core patterns + 2 volume profile patterns with VIX integration
- **`patterns/data/pattern_registry_config.json`**: 36 total patterns configuration
- **`alerts/alert_manager.py`**: Production alert system

### **📊 Mathematical Volume Engine**
- **`utils/time_aware_volume_baseline.py`**: TimeAwareVolumeBaseline with session multipliers
- **`core/data/volume_thresholds.py`**: VolumeThresholdCalculator with mathematical precision
- **`utils/correct_volume_calculator.py`**: Single source of truth for volume calculations
- **`patterns/pattern_detector.py`**: Mathematical engine integration for pattern detection
- **`strategies/kow_signal_straddle.py`**: VWAP-based options trading strategy
- **Redis Storage**: Dynamic baseline calculation with session and VIX awareness

### **🔧 Configuration & Utilities**
- **`config/optimized_field_mapping.yaml`**: Single source of truth for field names
- **`utils/yaml_field_loader.py`**: Direct YAML access with caching
- **`core/data/token_lookup_enriched.json`**: Master token resolution (enriched metadata)
- **`intraday_scanner/calculations.py`**: HybridCalculations with Greeks/expiry helpers

### **⚡ Ultra-Fast EMA Calculations**
- **`utils/calculations.py`**: Enhanced with Numba-accelerated EMA calculations
- **FastEMA Class**: Ultra-fast EMA calculation using Numba optimization
- **CircularEMA Class**: Pre-allocated circular buffer EMA for maximum performance
- **Performance**: O(1) single tick updates, vectorized batch processing
- **EMA Windows**: All periods (5, 10, 20, 50, 100, 200) calculated ultra-fast
- **Integration**: Seamlessly integrated with existing TA-Lib/Polars fallbacks
- **Redis Caching**: All EMA windows cached in Redis DB 1 (realtime) for pattern access
- **Pattern Access**: Patterns now have access to complete EMA suite with Greeks

### **📚 Available Libraries & Dependencies**

#### **Core Data Processing**
- **`redis`**: Redis client for data storage and streaming
- **`pandas`**: Data manipulation and analysis
- **`numpy`**: Numerical computing and mathematical operations
- **`polars`**: High-performance DataFrame operations
- **`numba`**: JIT compilation for ultra-fast numerical calculations (EMA acceleration)

#### **Technical Analysis Indicators**
- **`ta-lib`**: Primary technical analysis library with 150+ indicators
  - **Trend Indicators**: EMA, SMA, MACD, ADX, Aroon, Parabolic SAR
  - **Momentum Indicators**: RSI, Stochastic, Williams %R, CCI, ROC
  - **Volatility Indicators**: Bollinger Bands, ATR, Keltner Channels, Donchian Channels
  - **Volume Indicators**: OBV, AD Line, Chaikin Money Flow, Money Flow Index
  - **Pattern Recognition**: Candlestick patterns, Doji, Hammer, Engulfing patterns
  - **Custom Indicators**: VWAP, Volume Profile, Support/Resistance levels

#### **Mathematical & Statistical Libraries**
- **`scipy`**: Scientific computing (stats.norm for Black-Scholes, statistical functions)
- **`scipy.stats`**: Statistical distributions and hypothesis testing
- **`scipy.optimize`**: Optimization algorithms for parameter tuning

#### **Options & Derivatives**
- **`py_vollib`**: Options pricing and Greeks calculations (Black-Scholes, Binomial)
- **`py_lets_be_rational`**: Advanced options pricing models (Implied Volatility)
- **`pandas-market-calendars`**: NSE trading calendar for DTE calculations

#### **Volume Profile & Market Microstructure**
- **`market_profile`**: Volume profile analysis (MarketProfile class)
- **Custom Implementation**: Point of Control (POC), Value Area (70% volume), High/Low volume zones

#### **Data Sources & APIs**
- **`youtube-transcript-api`**: YouTube transcript extraction for educational content
- **NSE API Integration**: Sectoral volatility data via `utils/update_sectoral_volatility.py`

#### **Sectoral Analysis Scripts**
- **`utils/update_sectoral_volatility.py`**: Updates NSE sectoral indices data
  - **Coverage**: 9 sectors (NIFTY_50, NIFTY_BANK, NIFTY_AUTO, NIFTY_IT, NIFTY_PHARMA, NIFTY_FMCG, NIFTY_METAL, NIFTY_ENERGY, NIFTY_REALTY)
  - **Schedule**: Weekly updates (Sundays at 6:00 PM IST)
  - **Data Source**: NSE official API
  - **Output**: `config/sector_volatility.json` with 126+ stocks

## 🏗️ **SYSTEM ARCHITECTURE**

### **Volume State Management System**
```
WebSocket Parser (Single Calculation Point)
         ↓
VolumeStateManager (Session-Aware)
         ↓
Redis Persistence + Local Cache
         ↓
All Components (Consistent Volume Data)
```

### **Field Standardization System**
```
Component Request
         ↓
yaml_field_loader.py
         ↓
optimized_field_mapping.yaml
         ↓
Canonical Field Name
```

### **Pattern Detection System**
```
📊 21 TOTAL PATTERNS WITH MATHEMATICAL INTEGRITY:
├── 8 Core Patterns (Volume, Momentum, Breakout, Microstructure)
├── 6 ICT Patterns (Liquidity, FVG, OTE, Premium/Discount, Killzone, Momentum)
├── 4 Straddle Strategies (IV Crush, Range Bound, MM Traps, Premium Collection)
├── 1 Kow Signal Straddle (VWAP-based options strategy)
└── 2 Volume Profile Patterns (Breakout, Advanced Detection)

MATHEMATICAL INTEGRITY FEATURES:
├── PatternMathematics Class (Confidence bounds 0.0-1.0)
├── Risk Manager Integration (Dynamic expected_move, position_size, stop_loss)
├── Volume Baseline Integration (Centralized volume system)
└── Outlier Protection (Volume ratio protection against extreme values)
```

### **Symbol Processing System**
```
Raw Tokens → Token Resolver → Prefixed Symbols → Data Pipeline → 
Robust Parsing → Indicator Calculation → Greeks (for options)
     ↓                    ↓                    ↓                    ↓
Numeric IDs → NSE:/NFO: prefixes → Symbol extraction → Option/Future detection
     ↓                    ↓                    ↓                    ↓
Redis Storage → Pattern Detection → Risk Management → Trading Alerts
```

**Symbol Processing Features:**
- **Token Resolution**: Converts numeric tokens to prefixed symbols (NSE:, NFO:)
- **Robust Parsing**: Validates expiry format (DDMMM), strike price, CE/PE/FUT suffixes
- **Option Detection**: NFO:SYMBOL + DDMMM + STRIKE + CE/PE (e.g., NFO:NIFTY25NOV19500CE)
- **Future Detection**: NFO:SYMBOL + DDMMM + FUT (e.g., NFO:NIFTY25NOVFUT)
- **Edge Case Handling**: No false positives, malformed symbol rejection
- **Greeks Integration**: Automatic calculation for F&O options only

### **Mathematical Volume Engine**
- **Time-Aware Baselines**: Market session multipliers (opening: 1.8x, closing: 2.0x)
- **Bucket Resolution**: Dynamic scaling between time granularities (1min→5min→10min)
- **VIX Integration**: Market regime-aware sensitivity (LOW: 0.8x, HIGH: 1.4x, PANIC: 1.8x)
- **Statistical Methods**: Percentile-based anomaly detection for real-world data
- **Mathematical Precision**: Formula-based calculations with documented foundations

## 🔄 **RECENT INTEGRATIONS (Latest Updates)**

### **✅ Volume Profile Pattern Detection (January 15, 2025)**
- **Volume Profile Breakout**: Detection of breakouts above Value Area High or below Value Area Low with volume confirmation
- **Volume Node Test**: Detection of price testing key support/resistance levels with volume validation
- **POC Rejection**: Detection of price rejection at Point of Control with volume confirmation
- **Mathematical Integrity**: All volume profile patterns use PatternMathematics class for confidence bounds
- **VolumeStateManager Integration**: Seamless access to volume profile data from VolumeStateManager
- **WebSocket Parser Integration**: Automatic volume profile updates during tick processing - no additional code needed
- **Pattern Categories**: New "volume_profile" category for enhanced pattern classification
- **Result**: Complete volume profile-based pattern detection system integrated with existing patterns

### **✅ Greek Calculations Integration (January 15, 2025)**
- **Options Pricing**: Complete Black-Scholes model with NSE trading calendar awareness
- **Greeks Calculation**: Delta, Gamma, Theta, Vega, Rho with proper DTE calculations
- **F&O Integration**: Automatic calculation for F&O options with Redis publishing
- **Libraries**: `pandas-market-calendars`, `scipy.stats.norm`, `numpy` for mathematical precision
- **Redis Streams**: Individual and combined Greek indicators published to Redis
- **Pattern Detection**: Greeks available for options trading pattern detection
- **Result**: Complete options pricing system integrated with existing technical analysis

### **✅ Sectoral Volatility Integration (January 15, 2025)**
- **NSE Sectoral Indices**: 9 sectors with 126+ stocks (NIFTY_50, NIFTY_BANK, NIFTY_AUTO, NIFTY_IT, NIFTY_PHARMA, NIFTY_FMCG, NIFTY_METAL, NIFTY_ENERGY, NIFTY_REALTY)
- **Update Script**: `utils/update_sectoral_volatility.py` for NSE API integration
- **Independent System**: Separate from intraday crawler (174 instruments)
- **Use Cases**: Backtesting, sector correlation analysis, risk management
- **Scheduling**: Weekly updates (Sundays at 6:00 PM IST)
- **Result**: Complete sectoral analysis system for backtesting and correlation analysis

### **✅ Mathematical Integrity & Risk Manager Integration (January 15, 2025)**
- **PatternMathematics Class**: Centralized mathematical functions with confidence bounds (0.0-1.0)
- **Outlier Protection**: Volume ratio protection against extreme values (max 10.0)
- **Risk Manager Integration**: Dynamic expected_move, position_size, and stop_loss calculation
- **Volume Baseline Integration**: Centralized volume system with dynamic thresholds
- **All 19 Patterns**: Mathematical integrity applied to all pattern detection methods
- **Result**: Complete mathematical precision with Risk Manager integration for all patterns

### **✅ ICT Pattern Mathematical Integrity (January 15, 2025)**
- **6 ICT Patterns**: All ICT patterns now use PatternMathematics class
- **Mathematical Bounds**: All confidence values properly bounded (0.0-1.0)
- **Risk Manager Integration**: All ICT patterns get dynamic risk metrics
- **Volume Baseline Integration**: All ICT patterns use centralized volume system
- **Individual Methods**: Each ICT pattern has dedicated detection method with mathematical integrity
- **Result**: All ICT patterns now follow expected structure template with mathematical precision

### **✅ Robust Symbol Parsing & End-to-End Extraction (October 26, 2025)**
- **Robust Option Detection**: Validates NFO prefix, expiry format (DDMMM), strike price, CE/PE suffix
- **Robust Future Detection**: Validates NFO prefix, expiry format (DDMMM), FUT suffix
- **End-to-End Flow**: Token resolution → Symbol extraction → Robust parsing → Indicator calculation → Greeks
- **No False Positives**: Correctly rejects NSE:BAJAJ FINANCE (contains CE but not option)
- **Complete Validation**: Handles all month abbreviations, edge cases, malformed symbols
- **Greeks Integration**: Automatic calculation for F&O options with Redis publishing
- **Result**: Complete symbol processing system with mathematical precision and robust validation

### **✅ Token Resolution Fix & Redis Data Verification (October 27, 2025)**
- **Token Resolution Fix**: Fixed `core/data/token_resolver.py` to correctly extract full symbols with expiry
- **28OCT Futures Data**: Successfully resolved missing 28OCT futures data issue
- **Redis Data Verification**: Confirmed all data is stored in correct databases (DB 2, 5, 0)
- **Data Structure Compliance**: Verified data matches `redis_storage.py` and `redis_config.py` design
- **Consumer Access**: Confirmed all consumers can access data correctly
- **Volume Averages**: 20-day and 55-day averages properly stored and accessible
- **OHLC Data**: Current OHLC data properly stored in DB 2 for indicators
- **Market Data Averages**: Comprehensive averages data properly stored in DB 5
- **Result**: Complete data flow verification with proper Redis storage architecture

### **✅ Kow Signal Straddle Strategy (October 25, 2025)**
- **VWAP-Based Strategy**: Uses underlying VWAP for entry/exit signals
- **Premium Collection**: Focuses on premium decay and time value erosion
- **Risk Management**: Stop loss at 100% of premium, profit target at 50%
- **Position Sizing**: Turtle trading methodology for position sizing
- **Volume Tracking**: Combined CE/PE volume analysis for signal confirmation
- **Result**: Complete options trading strategy with mathematical precision

### **✅ Mathematical Volume Engine (October 25, 2025)**
- **Time-Aware Baselines**: TimeAwareVolumeBaseline with market session multipliers
- **Bucket Resolution Mathematics**: Dynamic scaling between time granularities
- **VIX Integration**: Market regime-aware threshold adjustments
- **Statistical Methods**: Percentile-based anomaly detection for real-world data
- **Result**: Mathematical precision in volume threshold calculations

### **✅ Volume State Management (October 25, 2025)**
- **Centralized**: VolumeStateManager in `core/data/volume_state_manager.py`
- **Session-Aware**: Redis persistence with local cache for performance
- **Single Calculation Point**: WebSocket Parser is the ONLY place where incremental volume is calculated
- **Consistent Fields**: All volume fields use canonical field names
- **Result**: Eliminated volume field conflicts and ensured data consistency

### **✅ Field Mapping Standardization (October 25, 2025)**
- **Direct YAML Access**: Replaced field_mapping.py with yaml_field_loader.py
- **Performance**: Added caching with `@lru_cache` for better performance
- **Compatibility**: Maintained backward compatibility with mock objects
- **Result**: Eliminated intermediate layer while maintaining all functionality

### **✅ VIX Multiplier Corrections (October 25, 2025)**
- **Fixed**: VIX regime logic now correctly adjusts pattern detection thresholds
- **HIGH VIX**: Higher thresholds (1.4x multiplier) = Less sensitive, reduces noise
- **LOW VIX**: Lower thresholds (0.8x multiplier) = More sensitive, catches weaker signals
- **Market Logic**: High volatility requires stronger signals, low volatility allows weaker signals
- **Result**: Pattern detection now properly adapts to market volatility conditions

## ✅ **SYSTEM STATUS VERIFICATION**

### **End-to-End Integration Status**
- ✅ **Mathematical Integrity**: PatternMathematics class with confidence bounds (0.0-1.0)
- ✅ **Risk Manager Integration**: Dynamic risk metrics for all 19 patterns
- ✅ **Volume Baseline Integration**: Centralized volume system with dynamic thresholds
- ✅ **Mathematical Volume Engine**: TimeAwareVolumeBaseline with session multipliers
- ✅ **Bucket Resolution Mathematics**: Dynamic scaling between time granularities
- ✅ **VIX Integration**: Market regime-aware threshold adjustments
- ✅ **Volume State Management**: VolumeStateManager is the single source of truth
- ✅ **Field Standardization**: All components use `optimized_field_mapping.yaml`
- ✅ **Pattern Detection**: 20 patterns (8 core + 6 ICT + 4 straddle + 1 Kow Signal + 1 Volume Profile Advanced)
- ✅ **Kow Signal Straddle**: VWAP-based options trading strategy integrated
- ✅ **Sectoral Analysis**: NSE sectoral indices integration for backtesting and correlation analysis
- ✅ **Greek Calculations**: Complete options pricing with Delta, Gamma, Theta, Vega, Rho
- ✅ **F&O Integration**: Automatic Greek calculations for F&O options with Redis publishing
- ✅ **Volume Profile**: Point of Control (POC), Value Area, High/Low volume zones
- ✅ **Publishing Channels**: All Redis streams properly wired
- ✅ **Ultra-Fast EMA Calculations**: Numba-accelerated EMA with O(1) single tick updates
- ✅ **Complete EMA Suite**: All windows (5, 10, 20, 50, 100, 200) available to patterns
- ✅ **Indicator Access Fixed**: Patterns now have access to complete indicator suite with Greeks
- ✅ **Order Flow Crawler Removed**: Legacy order flow crawler and all references cleaned up
- ✅ **Alert Formatting Streamlined**: Legacy alert format removed, all alerts now use actionable format with graceful handling for informational alerts

### **Ultra-Fast EMA System Verification**
```bash
# Test ultra-fast EMA calculations
python -c "
from utils.calculations import HybridCalculations, FastEMA, CircularEMA
import numpy as np

# Initialize fast EMA system
calc = HybridCalculations()

# Test single tick EMA update (O(1) operation)
symbol = 'NIFTY50'
price = 19500.0
fast_emas = calc.get_all_emas_fast(symbol, price)
print('✅ Fast EMAs calculated:', list(fast_emas.keys()))

# Test batch processing
prices = [19500.0, 19510.0, 19505.0, 19520.0, 19515.0]
batch_indicators = calc.batch_calculate_indicators([
    {'last_price': p, 'zerodha_cumulative_volume': 1000} for p in prices
], 'NIFTY50')
print('✅ Batch EMAs calculated:', [k for k in batch_indicators.keys() if 'ema' in k])

# Test circular buffer EMA
circular_ema = CircularEMA(20)
for price in prices:
    ema_value = circular_ema.add_value(price)
print(f'✅ Circular EMA final value: {ema_value:.2f}')
"

# Expected output:
# ✅ Fast EMAs calculated: ['ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200']
# ✅ Batch EMAs calculated: ['ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200', 'ema_crossover']
# ✅ Circular EMA final value: 19510.00
```

### **Mathematical Volume Engine Verification**
```bash
# Test mathematical volume engine
python -c "
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from core.data.volume_thresholds import VolumeThresholdCalculator
from redis_files.redis_client import get_redis_client
from datetime import datetime

# Initialize mathematical engine
baseline = TimeAwareVolumeBaseline(get_redis_client())
calculator = VolumeThresholdCalculator(baseline)

# Test with BANKNIFTY data
symbol = 'BANKNIFTY28OCTFUT'
current_time = datetime.now().replace(hour=9, minute=30)

# Calculate dynamic threshold
threshold = calculator.calculate_volume_spike_threshold(
    symbol, '1min', current_time, 'NORMAL'
)
print(f'✅ Mathematical engine: {threshold:,.0f} threshold')
"
```

### **Field Mapping Verification**
```bash
# Test field mapping consistency
python -c "
from utils.yaml_field_loader import resolve_session_field, resolve_calculated_field
print('✅ Session Field:', resolve_session_field('bucket_incremental_volume'))
print('✅ Indicator Field:', resolve_calculated_field('volume_ratio'))
"
```

### **Schema Standardization Verification**
```bash
# Test schemas.py integration
python -c "
from config.schemas import (
    process_tick_data_end_to_end,
    create_session_record_end_to_end,
    create_indicator_record_end_to_end,
    get_integration_status,
    normalize_session_data,
    validate_data_with_field_mapping
)
from datetime import datetime

# Test integration status
status = get_integration_status()
print(f'✅ Schema Integration: Field Mapping {status[\"field_mapping_available\"]}, Volume Manager {status[\"volume_manager_available\"]}')

# Test sample data processing
sample_tick = {
    'instrument_token': 12345,
    'tradingsymbol': 'NIFTY50',
    'last_price': 19500.0,
    'volume_traded': 1000000,
    'timestamp': datetime.now().isoformat()
}

# Test end-to-end processing
processed = process_tick_data_end_to_end(sample_tick, 'equity')
print(f'✅ End-to-end processing: {len(processed)} fields')

# Test session record creation
session_record = create_session_record_end_to_end('NIFTY50', processed)
print(f'✅ Session record: {len(session_record)} fields')

# Test validation
validation = validate_data_with_field_mapping(processed, 'equity')
print(f'✅ Data validation: {\"PASSED\" if validation[\"valid\"] else \"FAILED\"}')
"
```

### **Symbol Processing Verification**
```bash
# Test robust symbol parsing system
python -c "
from utils.calculations import HybridCalculations
from core.data.token_resolver import resolve_token_to_symbol

calc = HybridCalculations()

# Test token resolution
test_tokens = [6401, 40193, 60417]
print('\\n1. TOKEN RESOLUTION:')
for token in test_tokens:
    symbol = resolve_token_to_symbol(token)
    print(f'  Token {token} → {symbol}')

# Test robust parsing
test_symbols = [
    'NSE:ADANI ENTERPRISES',      # Equity
    'NFO:NIFTY25NOV19500CE',     # Option
    'NFO:NIFTY25NOVFUT',         # Future
    'NSE:BAJAJ FINANCE',         # Edge case (contains CE)
    'NFO:RELIANCE',              # Invalid F&O format
]

print('\\n2. ROBUST PARSING:')
for symbol in test_symbols:
    is_option = calc._is_option_symbol(symbol)
    is_future = calc._is_future_symbol(symbol)
    is_fno = calc._is_fno_symbol(symbol)
    print(f'  {symbol}: Option={is_option}, Future={is_future}, F&O={is_fno}')

print('\\n3. VERIFICATION:')
print('✅ Token resolution: Working')
print('✅ Robust parsing: Working')
print('✅ Option detection: Working')
print('✅ Future detection: Working')
print('✅ Edge case handling: Working')
print('✅ No false positives: Working')
"

### **Volume Averages Verification**
```bash
# Test volume averages system
python -c "
import json
import os
from redis_files.redis_client import get_redis_client

# Test JSON file
volume_file = 'config/volume_averages_20d.json'
if os.path.exists(volume_file):
    with open(volume_file, 'r') as f:
        data = json.load(f)
    print(f'✅ Volume Averages JSON: {len(data)} instruments loaded')
    print(f'✅ Sample: NSE:HDFCBANK avg_volume_20d = {data.get(\"NSE:HDFCBANK\", {}).get(\"avg_volume_20d\", \"N/A\")}')
else:
    print('❌ Volume averages JSON file not found')

# Test Redis storage
try:
    redis_client = get_redis_client(db=5)
    redis_client.ping()
    
    # Check Redis keys
    keys = redis_client.keys('volume_averages:*')
    print(f'✅ Redis DB 5: {len(keys)} volume averages keys')
    
    # Test specific symbol
    hdfc_data = redis_client.hgetall('volume_averages:NSE_HDFCBANK')
    if hdfc_data:
        print(f'✅ Redis Sample: NSE_HDFCBANK avg_volume_20d = {hdfc_data.get(b\"avg_volume_20d\", b\"N/A\").decode()}')
    else:
        print('⚠️ Redis data not found for NSE_HDFCBANK')
        
except Exception as e:
    print(f'❌ Redis connection failed: {e}')
"
```

### **Mathematical Integrity & Risk Manager Integration**
```bash
# Test mathematical integrity and Risk Manager integration
python -c "
from patterns.pattern_detector import PatternDetector
from patterns.pattern_mathematics import PatternMathematics
from redis_files.redis_client import get_redis_client
from datetime import datetime

# Initialize pattern detector with mathematical integrity
detector = PatternDetector({}, get_redis_client())

# Test mathematical functions
confidence = PatternMathematics.calculate_bounded_confidence(0.8, 0.1)
outlier_protection = PatternMathematics.protect_outliers(15.0, 10.0)

print(f'✅ Mathematical integrity: Confidence {confidence:.2f}, Outlier protection {outlier_protection:.2f}')

# Test Risk Manager integration
if hasattr(detector, 'risk_manager') and detector.risk_manager:
    print('✅ Risk Manager integration: Active')
else:
    print('⚠️ Risk Manager integration: Using emergency fallback')
"
```

### **📊 Expected Move Calculation System**

The system uses a **centralized expected move calculation** implemented in `alerts/risk_manager.py`:

#### **How Expected Move is Calculated**

1. **Primary Source**: Expected move comes from pattern detection data
2. **Normalization**: `_normalise_expected_move()` converts values to decimal format
3. **Fallback Logic**: If expected move is 0.0, uses `price_change` as fallback
4. **Target Price Calculation**: `target_price = price * (1 + direction_factor * expected_move_pct)`

#### **Expected Move Integration**

```python
# Risk Manager calculates expected move for all patterns
def calculate_risk_metrics(self, pattern_data: Dict) -> Dict:
    # Get expected move from pattern data
    expected_move_pct = self._normalise_expected_move(pattern_data.get('expected_move', 0.0))
    
    # Fallback to price change if expected move is 0
    if expected_move_pct == 0.0:
        expected_move_pct = self._normalise_expected_move(pattern_data.get('price_change'))
    
    # Calculate target price based on expected move
    direction_factor = -1 if signal in ('BEARISH', 'SELL', 'SHORT') else 1
    target_price = price * (1 + direction_factor * expected_move_pct)
    
    # Calculate risk/reward ratio
    risk_amount = quantity * stop_distance
    reward_amount = quantity * reward_distance
    rr_ratio = reward_amount / risk_amount if risk_amount > 0 else 0
```

#### **Expected Move Features**

- ✅ **Centralized Calculation**: All patterns use the same expected move logic
- ✅ **Automatic Fallback**: Uses price change when expected move is unavailable
- ✅ **Direction Aware**: Handles both bullish and bearish signals
- ✅ **Risk Integration**: Combined with position sizing and stop loss calculation
- ✅ **Real-time Updates**: Uses current market data from Redis

#### **Usage in Pattern Detection**

Expected move is automatically calculated for all 20 patterns:
- **8 Core Patterns**: Volume spike, momentum, breakout, reversal, etc.
- **6 ICT Patterns**: Liquidity grab, FVG retest, OTE, premium/discount zones
- **4 Straddle Strategies**: Options trading patterns
- **1 Kow Signal Straddle**: VWAP-based options strategy
- **1 Volume Profile Advanced**: Breakout and advanced detection

#### **Expected Move Verification**
```bash
# Test expected move calculation
python -c "
from alerts.risk_manager import RiskManager
from redis_files.redis_client import get_redis_client

# Initialize risk manager
risk_manager = RiskManager(redis_client=get_redis_client())

# Test pattern with expected move
pattern_data = {
    'symbol': 'NSE:NIFTY 50',
    'pattern': 'volume_spike',
    'signal': 'BULLISH',
    'confidence': 0.85,
    'last_price': 19500.0,
    'expected_move': 1.5,  # 1.5% expected move
    'volume_ratio': 2.5
}

# Calculate risk metrics including expected move
risk_metrics = risk_manager.calculate_risk_metrics(pattern_data)
print(f'✅ Expected move: {risk_metrics.get(\"risk_metrics\", {}).get(\"expected_move_percent\", 0):.2f}%')
print(f'✅ Target price: ₹{risk_metrics.get(\"risk_metrics\", {}).get(\"target_price\", 0):.2f}')
print(f'✅ Risk/Reward ratio: {risk_metrics.get(\"risk_metrics\", {}).get(\"rr_ratio\", 0):.2f}x')
"
```

### **Greek Calculations Verification**
```bash
# Test Greek calculations integration
python -c "
from utils.calculations import greek_calculator, expiry_calculator
from datetime import datetime

# Test expiry calculation
dte_info = expiry_calculator.calculate_dte(
    expiry_date=datetime(2024, 1, 25),
    symbol='NFO:BANKNIFTY25JAN54900CE'
)
print(f'✅ Expiry calculation: {dte_info.get(\"trading_dte\", 0)} trading days')

# Test Greek calculation
test_tick = {
    'underlying_price': 18500.0,
    'strike_price': 18500.0,
    'expiry_date': datetime(2024, 1, 25),
    'option_type': 'call'
}
greeks = greek_calculator.calculate_greeks_for_tick_data(test_tick)
print(f'✅ Greek calculation: Delta {greeks.get(\"delta\", 0):.3f}')
"
```

### **Kow Signal Straddle Strategy Verification**
```bash
# Test straddle strategy integration
python -c "
from strategies.kow_signal_straddle import VWAPStraddleStrategy
from redis_files.redis_client import get_redis_client

# Initialize straddle strategy
config = {
    'entry_time': '09:30',
    'exit_time': '15:15',
    'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
    'max_risk_per_trade': 0.03
}
strategy = VWAPStraddleStrategy(get_redis_client(), config)
print('✅ Kow Signal Straddle strategy initialized')
"
```

## 🚀 **CRAWLER USAGE & MANAGEMENT**

### **Available Crawlers**

#### **1. Intraday Crawler (Main Trading System)**
```bash
# Start intraday crawler (Real-time trading data)
source .venv/bin/activate
python crawlers/zerodha_websocket/intraday_crawler.py

# Features:
# - Real-time WebSocket data from Zerodha
# - Redis publishing (DB 2, 4, 5)
# - Parquet file writing (DuckDB compatible)
# - 174 instruments (NIFTY 50 + Futures + BANKNIFTY)
# - Pattern detection integration
```

#### **2. Data Mining Crawler (Historical Collection)**
```bash
# Start data mining crawler (Historical data only)
source .venv/bin/activate
python crawlers/run_data_mining_crawler.py

# Features:
# - Pure file writing (no external data publishing)
# - Historical data collection
# - Parquet files with metadata
# - No Redis publishing
```

#### **3. Research Crawler (Cross-venue Analysis)**
```bash
# Start research crawler (Research data only)
source .venv/bin/activate
python crawlers/run_research_crawler.py

# Features:
# - Pure file writing (no external data publishing)
# - Cross-venue analysis
# - Research data collection
# - No Redis publishing
```

#### **4. GIFT NIFTY Gap Crawler (Market Context)**
```bash
# Start GIFT NIFTY gap analyzer (Continuous mode)
source .venv/bin/activate
python crawlers/gift_nifty_gap.py --continuous

# Features:
# - 30-second interval updates
# - NIFTY 50, BANK NIFTY, SGX GIFT NIFTY, INDIA VIX
# - News collection from Zerodha Pulse
# - Market context publishing
# - Gap analysis
# - Redis publishing (DB 2, 5, 11)
```

### **Crawler Management Commands**

#### **Start All Crawlers**
```bash
# Start main trading system
source .venv/bin/activate
python crawlers/launch_crawlers.py

# This launches:
# - Intraday Crawler (Redis + Parquet)
# - Data Mining Crawler (Parquet only)
# - Research Crawler (Parquet only)
```

#### **Stop All Crawlers**
```bash
# Stop all running crawlers
pkill -f "intraday_crawler"
pkill -f "data_mining_crawler"
pkill -f "research_crawler"
pkill -f "gift_nifty_gap"
```

#### **Check Crawler Status**
```bash
# Check running processes
ps aux | grep -E "(intraday|mining|research|gift)"

# Check Redis data
redis-cli select 2
redis-cli keys "ohlc_latest:*" | wc -l

redis-cli select 5
redis-cli keys "volume_averages:*" | wc -l
```

## 🚨 **TROUBLESHOOTING GUIDE**

### **Common Issues & Solutions**

#### **1. Token Resolution Issues**
```bash
# Problem: Missing 28OCT futures data
# Solution: Check token resolution logic
python -c "
from core.data.token_resolver import resolve_token_to_symbol
# Test token resolution
token = 6401  # NIFTY token
symbol = resolve_token_to_symbol(token)
print(f'Token {token} → {symbol}')
"

# Expected: NSE:NIFTY 50
# If wrong: Check token_lookup.json and token_resolver.py
```

#### **2. Redis Connection Issues**
```bash
# Problem: Redis connection failed
# Solution: Check Redis status
redis-cli ping
redis-cli info memory

# Check Redis configuration
python -c "
from redis_files.redis_config import get_redis_config
config = get_redis_config()
print(f'Redis Config: {config}')
"
```

#### **3. Volume Averages Missing**
```bash
# Problem: No volume averages data
# Solution: Update volume averages
source .venv/bin/activate
python utils/update_all_20day_averages.py

# Check if data was updated
redis-cli select 5
redis-cli keys "volume_averages:*28OCT*" | head -5
```

#### **4. OHLC Data Missing**
```bash
# Problem: No OHLC data in Redis
# Solution: Check intraday crawler and update script
redis-cli select 2
redis-cli keys "ohlc_latest:*28OCT*" | head -5

# If missing, restart intraday crawler
pkill -f "intraday_crawler"
python crawlers/zerodha_websocket/intraday_crawler.py
```

#### **5. Parquet Files Not Created**
```bash
# Problem: No Parquet files generated
# Solution: Check crawler status and data flow
ls -la crawlers/parquet_data/
ls -la binary_to_parquet/

# Check if crawlers are writing data
tail -f logs/intraday_crawler.log
tail -f logs/data_mining_crawler.log
```

### **Python Environment & Dependencies**
```bash
# Activate virtual environment
source .venv/bin/activate

# Test core dependencies
python -c "import redis, pandas, numpy; print('✅ All dependencies OK')"

# Test Redis connectivity
python -c "from redis_files.redis_client import get_redis_client; r = get_redis_client(); print('✅ Redis connected:', r.ping())"

# Test mathematical volume engine
python -c "from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline; from redis_files.redis_client import get_redis_client; baseline = TimeAwareVolumeBaseline(get_redis_client()); print('✅ Mathematical volume engine loaded')"
```

## ✅ **DO'S AND DON'TS**

### **✅ DO'S**

#### **System Management**
- ✅ **Always use virtual environment**: `source .venv/bin/activate`
- ✅ **Check Redis health before starting crawlers**: `redis-cli ping`
- ✅ **Update volume averages daily**: `python utils/update_all_20day_averages.py`
- ✅ **Monitor crawler logs**: `tail -f logs/intraday_crawler.log`
- ✅ **Verify data flow**: Check Redis keys and Parquet files
- ✅ **Use proper crawler separation**: Data mining = file only, Intraday = Redis + file

#### **Data Management**
- ✅ **Check token resolution**: Verify symbols are correctly resolved
- ✅ **Validate Redis data**: Ensure data is in correct databases
- ✅ **Monitor memory usage**: Keep Redis memory < 10GB
- ✅ **Use proper field mapping**: Always use `optimized_field_mapping.yaml`
- ✅ **Check data consistency**: Verify 20-day/55-day averages are available

#### **Troubleshooting**
- ✅ **Check logs first**: Always start with log files
- ✅ **Verify Redis connectivity**: Test Redis connection before debugging
- ✅ **Test token resolution**: Verify symbols are correctly mapped
- ✅ **Check data flow**: Verify data is flowing through the pipeline
- ✅ **Monitor system resources**: Check memory, CPU, and disk usage

### **❌ DON'TS**

#### **System Management**
- ❌ **Don't run crawlers without virtual environment**
- ❌ **Don't start multiple instances of the same crawler**
- ❌ **Don't ignore Redis memory warnings**
- ❌ **Don't run crawlers during market holidays without checking**
- ❌ **Don't modify crawler configurations without understanding impact**

#### **Data Management**
- ❌ **Don't mix data mining and intraday crawlers** (different purposes)
- ❌ **Don't ignore token resolution errors** (causes missing data)
- ❌ **Don't skip volume averages updates** (required for pattern detection)
- ❌ **Don't modify Redis data directly** (use proper APIs)
- ❌ **Don't ignore field mapping errors** (causes data inconsistency)

#### **Troubleshooting**
- ❌ **Don't restart everything at once** (identify specific issue first)
- ❌ **Don't ignore error messages** (they indicate specific problems)
- ❌ **Don't modify core files without backup**
- ❌ **Don't run crawlers without proper Zerodha credentials**
- ❌ **Don't ignore Redis connection failures**

### **🔧 Emergency Procedures**

#### **System Recovery**
```bash
# If Redis is down
sudo systemctl restart redis
redis-cli ping

# If crawlers are stuck
pkill -f "crawler"
python crawlers/zerodha_websocket/intraday_crawler.py

# If data is corrupted
redis-cli flushdb  # CAUTION: This deletes all data
python utils/update_all_20day_averages.py
```

#### **Data Recovery**
```bash
# If volume averages are missing
python utils/update_all_20day_averages.py

# If OHLC data is missing
python utils/update_all_20day_averages.py

# If token resolution fails
python -c "
from core.data.token_resolver import _load_token_mapping
mapping = _load_token_mapping()
print(f'Loaded {len(mapping)} tokens')
"
```

#### **Performance Issues**
```bash
# If Redis is slow
redis-cli info memory
redis-cli --stat

# If crawlers are slow
ps aux | grep crawler
top -p $(pgrep -f crawler)

# If memory is high
redis-cli info memory | grep used_memory_human
```

### **Redis Health & Data Flow**
```bash
# Check Redis memory (should be < 10GB)
redis-cli info memory | grep used_memory_human

# Check active streams
redis-cli xinfo stream "ticks:raw:binary"
redis-cli xinfo stream "ticks:intraday:processed"

# Monitor real-time data
redis-cli xread streams "ticks:intraday:processed" 0 count 3

# Check database distribution
redis-cli info keyspace
```

### **System Status & Performance**
```bash
# Run comprehensive health check
python core/data/redis_health.py

# Test end-to-end pipeline
python -c "
from patterns.pattern_detector import PatternDetector
from redis_files.redis_client import get_redis_client
detector = PatternDetector({}, get_redis_client())
print('✅ Pattern detector initialized')
"

# Check alert system
python -c "
from alerts.alert_manager import ProductionAlertManager
from redis_files.redis_client import get_redis_client
am = ProductionAlertManager(redis_client=get_redis_client())
print('✅ Alert manager initialized')
"
```

## 🚀 **REAL-TIME TRADING ARCHITECTURE**

A sophisticated Redis 8.2.2-powered trading system with real-time WebSocket data ingestion, centralized volume management, and comprehensive pattern detection.

### **🏗️ System Architecture**

```
WebSocket Data → Intraday Crawler → Redis Streams → 
Multi-DB Storage → Enhanced Pattern Detection → 
TA-Lib Calculations → Alert Generation
```

### **🎯 COMPREHENSIVE PATTERN DETECTION SYSTEM**

**11 Pattern Detectors with 36 Total Patterns:**

#### **ICT Patterns (6 Detectors)**
- **ICTPatternDetector** - Momentum patterns and trend analysis
- **ICTLiquidityDetector** - Liquidity pools and stop-hunt zones  
- **ICTFVGDetector** - Fair Value Gaps from OHLC candles
- **ICTOTECalculator** - Optimal Trade Entry retracement zones
- **ICTPremiumDiscountDetector** - Premium/Discount zones using VWAP
- **ICTKillzoneDetector** - Time-of-day killzone detection

#### **Straddle Strategies (2 Detectors)**
- **MMExploitationStrategies** - IV Crush Play with volatility detection
- **RangeBoundAnalyzer** - Strangle Strategy for range-bound markets

#### **Additional Pattern Detectors (2 Detectors)**
- **MarketMakerTrapDetector** - Market maker manipulation patterns
- **PremiumCollectionStrategy** - Premium collection trades

#### **Core Pattern Detector (1 Detector - 8 Patterns)**
- **PatternDetector** - Core patterns from registry:
  - `volume_spike` - Sudden volume increase > 2x average
  - `volume_breakout` - Price breakout with volume confirmation
  - `volume_price_divergence` - Price and volume moving opposite
  - `upside_momentum` - Strong upward momentum with volume
  - `downside_momentum` - Strong downward momentum with volume
  - `breakout` - Price breaks resistance with volume
  - `reversal` - Price reversal with volume confirmation
  - `hidden_accumulation` - Hidden liquidity patterns

## 📊 **Active Crawler Architecture**

### **Current Crawlers**
- **Intraday Crawler**: Main real-time data ingestion (174 instruments)
- **Research Crawler**: Cross-venue analysis and research
- **Data Mining Crawler**: Historical data collection
- **Gift Nifty Gap Crawler**: Gap analysis and news correlation

### **Removed Components**
- **Order Flow Crawler**: Removed for simplified architecture (October 26, 2025)

### **1. WebSocket Ingestion**
- **Source**: Zerodha WebSocket API (`wss://ws.kite.trade`)
- **Mode**: Full mode for complete market data
- **Instruments**: 174 NSE/NFO instruments (binary_crawler1.json)
- **Frequency**: Real-time tick-by-tick data

### **2. Data Processing Pipeline**
```python
# Intraday Crawler (crawlers/zerodha_websocket/intraday_crawler.py)
WebSocket Binary Data → ZerodhaWebSocketMessageParser → 
VolumeStateManager → Multiple Redis Publishing Paths → Storage & Calculations
```

### **3. Redis Publishing Mechanisms**

#### **A. Binary Stream Publishing**
```python
# Stream: "ticks:raw:binary" (DB 1 - realtime)
# Contains: 4,411,873+ entries (massive real-time data)
fields = {
    "binary_data": base64_encoded_data,
    "instrument_token": str(token),
    "timestamp": str(time.time()),
    "mode": "full",
    "source": "intraday_crawler"
}
redis_client.xadd("ticks:raw:binary", fields)
```

#### **B. Processed Stream Publishing**
```python
# Stream: "ticks:intraday:processed" (DB 1 - realtime)
# Normalized Zerodha field names with timestamp conversion
intraday_tick = {
    "symbol": symbol,
    "instrument_token": token,
    "last_price": price,
    "zerodha_cumulative_volume": cumulative_volume,
    "bucket_incremental_volume": incremental_volume,
    "exchange_timestamp": iso_timestamp,
    "exchange_timestamp_ms": epoch_ms,
    "best_bid_price": bid_price,
    "best_ask_price": ask_price,
    "sequence": tick_count,
    "is_tradable": True,
    "open_interest": oi,
    "change": price_change,
    "net_change": net_change
}
redis_client.xadd("ticks:intraday:processed", {"data": json.dumps(intraday_tick)})
```

## 🗄️ **Redis Storage Architecture (8.2.2)**

### **Multi-Database Design**
| Database | Purpose | Data Types | Usage |
|----------|---------|------------|-------|
| **DB 0** | System | system_config, metadata, session_data, health_checks | System operations |
| **DB 2** | Prices | equity_prices, futures_prices, options_prices | OHLC data storage |
| **DB 3** | Premarket | premarket_trades, incremental_volume, opening_data | Pre-market analysis |
| **DB 1** | Realtime | Ticks, alerts, patterns, prices, news (consolidated from DBs 2,3,4,6,8,10,11) | Live tick processing, streaming data |
| **DB 2** | Analytics | Volume profiles, OHLC, historical data, metrics (consolidated from DBs 5,13) | Volume analysis, analytics |
| **DB 3** | Independent Validator | Signal quality, pattern performance (isolated) | Pattern validation (isolated from main system) |
| **DB 5** | Indicators Cache | Technical indicators, Greeks | Indicator storage (300s TTL) |

### **Data Structures**

#### **1. Streams (Real-time Processing)**
- **`ticks:raw:binary`**: 4.4M+ entries - Raw WebSocket binary data
- **`ticks:intraday:processed`**: 1.9M+ entries - Processed tick data
- **Auto-expiry**: TTL-based cleanup (16 hours)

#### **2. Hash Storage (Session Data)**
```json
// session:HDFCBANK:2025-10-23
{
  "zerodha_cumulative_volume": 43526526,
  "bucket_incremental_volume": 1000,
  "last_price": 1008.8,
  "high_price": 1020.35,
  "low_price": 1006.0,
  "update_count": 18712,
  "last_update": 1761215723
}
```

#### **3. Volume Buckets (Time-series)**
```json
// volume:volume:bucket1:SUNPHARMA25NOVFUT:buckets:12:54
{
  "bucket_incremental_volume": 6650,
  "incremental_volume": 6650,
  "volume": 6650,
  "count": 19,
  "first_timestamp": "2025-10-23T12:54:02+05:30",
  "last_timestamp": "2025-10-23T12:54:56+05:30",
  "last_price": 1711.5
}
```

## ⚡ **Redis Calculations Engine**

### **Lua Scripts (redis_calculations.lua)**
- **ATR**: Average True Range calculation
- **EMA**: Exponential Moving Average
- **RSI**: Relative Strength Index
- **VWAP**: Volume Weighted Average Price
- **Batch Processing**: Multiple indicators in single call

### **Python Fallbacks (redis_calculations.py)**
- Automatic fallback when Redis unavailable
- Circuit breaker pattern for resilience
- Optimized for high-frequency trading data

## 🔍 **System Monitoring**

### **Pattern Detection Health**
```bash
# Check pattern detector stats
python -c "from patterns.pattern_detector import PatternDetector; pd = PatternDetector(); print(pd.get_stats())"

# Verify base detector initialization
python -c "from patterns.base_detector import BasePatternDetector; bd = BasePatternDetector(); print('Base detector initialized successfully')"

# Test VIX integration
python -c "from utils.vix_utils import VIXUtils; vix = VIXUtils(); print(f'VIX: {vix.get_current_vix()}')"
```

### **Redis Health Check**
```bash
# Memory usage (10GB limit)
redis-cli info memory

# Stream analysis
redis-cli xinfo stream "ticks:raw:binary"

# Database status
redis-cli info keyspace

# Real-time monitoring
redis-cli --stat
```

### **Data Verification**
```bash
# Check intraday data
redis-cli select 4
redis-cli keys "ticks:*"

# Check session data
redis-cli select 0
redis-cli keys "session:*"

# Check volume buckets
redis-cli keys "volume:*" | head -5
```

## 📈 **Performance Metrics**

### **Current System Status**
- **Memory Usage**: 1.88GB (2GB limit) - 94% utilization
- **Total Keys**: 57,813 across all databases
- **Database Distribution**: DB 0 (system), DB 1 (realtime), DB 2 (analytics), DB 5 (indicators_cache)
- **Volume Buckets**: 48,877 accessible buckets
- **Redis Connections**: 10 stable connections
- **Alert System**: 1 active alert
- **Pattern Detection**: 19 patterns with mathematical integrity
- **Base Detector**: Robust error handling with VIX integration
- **Support/Resistance**: Statistical significance-based detection

### **Data Volume Analysis**
- **Volume Buckets**: 48,877 buckets with `volume:volume:bucket:*` pattern
- **Session Data**: Stored in DB 0 with `session:*` pattern
- **Volume Baselines**: 1,425 baselines in DB 5 for pattern detection
- **Memory Efficiency**: 94% utilization with proper database segmentation

## 🚀 **Quick Start**

### **1. System Requirements**
- **Redis**: 8.2.2+ with 2GB memory (expandable to 10GB)
- **Python**: 3.8+ with asyncio support
- **WebSocket**: Zerodha API access
- **Memory**: 2GB+ available for Redis operations
- **Dependencies**: Dash, Plotly, dash-bootstrap-components (for dashboard)

### **2. Start Intraday Crawler**
```bash
# Start the intraday crawler
python crawlers/zerodha_websocket/intraday_crawler.py

# Monitor Redis health
python core/data/redis_health.py

# Check system status
redis-cli --stat
```

### **3. Start Alert Dashboard**
```bash
# Start the dashboard (local access)
python aion_alert_dashboard/alert_dashboard.py

# Dashboard will be available at:
# - Local: http://localhost:53056
# - Network: http://<local-ip>:53056
# - External: Requires ngrok tunnel (see below)

# For external access, start ngrok tunnel:
ngrok http 53056

# Dashboard features:
# - Real-time alert visualization
# - Technical indicators (VWAP, EMAs, RSI, MACD, Bollinger Bands)
# - Volume Profile (POC, Value Area, distribution)
# - Options Greeks (Delta, Gamma, Theta, Vega, Rho)
# - Market indices (VIX, NIFTY 50, BANKNIFTY)
# - News enrichment with sentiment analysis
# - Interactive price charts with full trading session data
```

### **3. Verify Data Flow**
```bash
# Check streams are active
redis-cli xinfo stream "ticks:raw:binary"

# Update sectoral volatility data
python utils/update_sectoral_volatility.py

# Check sectoral data
python -c "
import json
with open('config/sector_volatility.json', 'r') as f:
    data = json.load(f)
print(f'Sectors: {len(data.get(\"sectors\", {}))}')
print(f'Total stocks: {sum(len(stocks) for stocks in data.get(\"sectors\", {}).values())}')
"

# Manual sectoral data update (if needed)
python -c "
from utils.update_sectoral_volatility import update_sectoral_volatility
update_sectoral_volatility()
print('✅ Sectoral volatility data updated successfully')
"
redis-cli xinfo stream "ticks:intraday:processed"

# Monitor real-time data
redis-cli xread streams "ticks:intraday:processed" 0 count 5
```

## 🎯 **System Capabilities**

### **Real-time Processing**
- ✅ **WebSocket Ingestion**: Live market data
- ✅ **Stream Processing**: 4.4M+ entries
- ✅ **Multi-DB Storage**: 8+ specialized databases
- ✅ **Lua Calculations**: Server-side processing
- ✅ **TTL Management**: Automatic cleanup
- ✅ **Circuit Breakers**: Fault tolerance
- ✅ **Ultra-Fast EMA**: Numba-accelerated calculations with O(1) single tick updates
- ✅ **Vectorized Processing**: Batch EMA calculations for maximum throughput
- ✅ **Memory Efficient**: Circular buffers prevent memory leaks
- ✅ **Complete Indicator Suite**: All EMA windows (5, 10, 20, 50, 100, 200) + Greeks

### **Data Management**
- ✅ **Mathematical Integrity**: PatternMathematics class with confidence bounds (0.0-1.0)
- ✅ **Risk Manager Integration**: Dynamic risk metrics for all 20 patterns
- ✅ **Volume Baseline Integration**: Centralized volume system with dynamic thresholds
- ✅ **Mathematical Volume Engine**: Time-aware baselines with session multipliers
- ✅ **Bucket Resolution Mathematics**: Dynamic scaling between time granularities
- ✅ **VIX Integration**: Market regime-aware threshold adjustments
- ✅ **Schema Standardization**: End-to-end data processing with field mapping and volume manager integration
- ✅ **Data Validation**: Schema compliance checking with backward compatibility
- ✅ **Unified Processing**: `process_tick_data_end_to_end()` for consistent data flow
- ✅ **Session/Indicator Records**: Standardized record creation across all scripts
- ✅ **Base Pattern Detector**: Robust error handling with graceful fallbacks
- ✅ **Support/Resistance Detection**: Statistical significance-based S/R levels
- ✅ **Stop Hunt Zones**: Dynamic liquidity cluster identification
- ✅ **Session Tracking**: JSON-based storage in DB 0
- ✅ **Volume Analysis**: 48,877 time-bucketed volume buckets
- ✅ **Pattern Detection**: Real-time alerts with mathematical precision
- ✅ **Kow Signal Straddle**: VWAP-based options trading strategy
- ✅ **Greek Calculations**: Complete options pricing with Delta, Gamma, Theta, Vega, Rho
- ✅ **Alert Validation**: Real-time validation with 90%+ confidence threshold
- ✅ **Community Distribution**: Telegram and Reddit bot integration

## 🤖 **Community Bots & Distribution**

### **Community Manager Architecture**
The `community_bots/community_manager.py` orchestrates multi-platform distribution of alerts and validation results:

```python
# Community Manager Components
- Telegram Bot: Real-time alert distribution
- Reddit Bot: Educational content and validation results
- Validation Consumer: Redis pub/sub for high-confidence alerts
- Multi-platform Sync: Coordinated content distribution
```

### **Alert Filtering System**
- **Kow Signal Straddle**: Filtered to `@kow_signal_bot` channel only
- **NIFTY50/BANKNIFTY Futures**: November 25th expiry contracts only
- **High-Confidence Validation**: 90%+ confidence results to all communities
- **Complete Transparency**: Full validation details with all indicators

### **Telegram Integration**
```python
# Telegram Bot Features
- Kow Signal Straddle filtering
- Validation result distribution
- Performance updates
- Educational content sharing
- Market updates
```

**Configuration**: `alerts/config/telegram_config.json`
```json
{
    "bot_token": "YOUR_BOT_TOKEN",
    "chat_ids": ["@kow_signal_bot"],
    "signal_bot": {
        "bot_token": "SIGNAL_BOT_TOKEN",
        "chat_ids": ["@kow_signal_bot"]
    }
}
```

### **Reddit Integration**
```python
# Reddit Bot Features
- Validation result posts
- Educational content
- Performance reports
- Market analysis
```

**Configuration**: `alerts/config/reddit_config.json`
```json
{
    "client_id": "YOUR_CLIENT_ID",
    "client_secret": "YOUR_CLIENT_SECRET",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD",
    "subreddit": "AIONAlgoTrading"
}
```

### **Validation Results Publishing**
High-confidence validation results (90%+) are automatically published to:
- **Telegram Channels**: `telegram:validation_results`, `telegram:high_confidence_alerts`
- **Reddit Channel**: `reddit:validation_results`
- **Complete Details**: Volume analysis, expected move accuracy, rolling windows

### **Community Manager Usage**
```bash
# Start Community Manager
python community_bots/community_manager.py

# Test Telegram Bot
python community_bots/telegram_bot.py

# Test Reddit Bot
python community_bots/reddit_bot.py
```

### **Redis Channels**
The community manager subscribes to these Redis channels:
- `telegram:validation_results`
- `telegram:high_confidence_alerts`
- `telegram:community_updates`
- `reddit:validation_results`

### **Alert Flow**
```
Kow Signal Straddle Alerts → @kow_signal_bot (Filtered)
     ↓
Alert Validator (90%+ confidence)
     ↓
All Communities (Telegram + Reddit)
```
- ✅ **F&O Integration**: Automatic Greek calculations for F&O options with Redis publishing
- ✅ **Volume Profile**: Point of Control (POC), Value Area, High/Low volume zones
- ✅ **Memory Efficiency**: 94% utilization with proper database segmentation
- ✅ **Auto-expiry**: 16-hour TTL

### **System Monitoring**
- ✅ **Redis Health**: Full monitoring with memory, connections, and keyspace info
- ✅ **Volume Buckets**: 48,877 accessible buckets with proper key patterns
- ✅ **Database Segmentation**: Proper data organization across Redis databases
- ✅ **Alert System**: 1 active alert with real-time processing

---

---

## 📚 **MODULE DOCUMENTATION**

### **Volume Profile Module** (`patterns/`)
- **`patterns/VOLUME_PROFILE_AUDIT.md`** - Comprehensive audit of volume profile logic, data flow, and consumer usage
- **`patterns/VOLUME_PROFILE_FIXES_APPLIED.md`** - Complete list of fixes applied to volume profile calculations
- **`patterns/VOLUME_PROFILE_OVERRIDE_ANALYSIS.md`** - Detailed override analysis and verification of single source of truth

### **Redis Client Module** (`redis_files/`)
- **`redis_files/REDIS_CLIENT_ANALYSIS.md`** - Analysis of Redis client structure, circuit breakers, and data flow
- **`redis_files/REDIS_CLIENT_FIXES_NEEDED.md`** - Identified issues and fixes needed in Redis client
- **`redis_files/REDIS_STORAGE_ORPHANED_FUNCTIONS.md`** - Audit of orphaned functions in Redis storage module
- **`redis_files/WEBSOCKET_PARSER_DATA_FLOW_VERIFIED.md`** - Verified data flow from WebSocket parser to Redis storage
- **`redis_files/CUMULATIVE_VOLUME_CLARIFICATIONS.md`** - Clarifications on cumulative volume data handling
- **`redis_files/CUMULATIVE_VOLUME_DATA_FLOW.md`** - Complete data flow documentation for cumulative volume

### **Field Mapping & Utilities** (`utils/`)
- **`utils/FIELD_MAPPING_AUDIT_GUIDE.md`** - Guide for manual field mapping audits and best practices
- **`utils/FIELD_MISMATCH_ANALYSIS.md`** - Analysis of field name mismatches and mapping overrides

### **Dashboard Module** (`aion_alert_dashboard/`)
- **`aion_alert_dashboard/DASHBOARD_INTEGRATION.md`** - Dashboard integration guide and API documentation
- **`aion_alert_dashboard/DASHBOARD_ISSUES_ANALYSIS.md`** - Analysis of dashboard issues and resolutions
- **`aion_alert_dashboard/NGROK_DASHBOARD_INTEGRATION.md`** - Ngrok tunnel management and dashboard coordination
- **`STATISTICAL_MODEL_INTEGRATION.md`** - Statistical model integration with alert validator (validation performance, pattern analysis)

### **Alert System** (`alerts/`)
- **`alerts/DATA_STORAGE_STRATEGY.md`** - Alert data storage strategy and Redis key patterns

---

---

## 🖥️ **REACT DASHBOARD & AUTHENTICATION**

### **Modern React Dashboard**

The system includes a modern React + TypeScript dashboard with real-time updates, comprehensive alert management, and advanced security features.

**Tech Stack:**
- **Frontend**: React 18 + TypeScript + Vite
- **UI**: Material-UI (MUI)
- **State Management**: Zustand
- **Charts**: Lightweight Charts (price), ECharts (volume profile)
- **Real-time**: Socket.IO Client
- **Routing**: React Router

**Features:**
- ✅ Real-time alerts table with filtering
- ✅ Summary statistics cards
- ✅ Market indices display (NIFTY, BANKNIFTY, VIX)
- ✅ Recent news sidebar
- ✅ Alert detail pages with charts
- ✅ Technical indicators overlay
- ✅ Volume profile visualization
- ✅ JWT authentication with 2FA
- ✅ Brokerage integration

### **Authentication System**

#### **User Management**
- Users stored in Redis DB 0: `user:{username}` → JSON
- Password hashing with bcrypt (passlib)
- JWT tokens for API access (24-hour expiry)
- Default users: `admin/admin123`, `user/user123`

#### **2FA (Two-Factor Authentication)**
- **TOTP Support**: Google Authenticator / Microsoft Authenticator compatible
- **Setup**: POST `/api/auth/2fa/setup` - Generates QR code
- **Verify**: POST `/api/auth/2fa/verify` - Enables 2FA after code verification
- **Disable**: POST `/api/auth/2fa/disable` - Requires password confirmation
- **Login Flow**: Username/Password → 2FA Code (if enabled) → JWT Token

#### **Brokerage Integration**
- **Zerodha Kite Connect**: Connect user brokerage accounts
- **Connect**: POST `/api/brokerage/connect` - Link Zerodha account
- **List**: GET `/api/brokerage/connections` - View connected accounts
- **Disconnect**: POST `/api/brokerage/disconnect` - Remove connection
- Credentials stored securely in Redis per-user

#### **API Endpoints**

**Authentication:**
- `POST /api/auth/login` - Login (supports 2FA)
- `POST /api/auth/register` - Register new user (requires auth)
- `POST /api/auth/create-user` - Admin create user
- `POST /api/auth/refresh` - Refresh JWT token

**2FA:**
- `POST /api/auth/2fa/setup` - Generate QR code
- `POST /api/auth/2fa/verify` - Enable 2FA
- `POST /api/auth/2fa/disable` - Disable 2FA

**Brokerage:**
- `POST /api/brokerage/connect` - Connect Zerodha
- `GET /api/brokerage/connections` - List connections
- `POST /api/brokerage/disconnect` - Disconnect account

**Alerts:**
- `GET /api/alerts` - Get paginated alerts
- `GET /api/alerts/{alert_id}` - Get alert details
- `GET /api/alerts/stats/summary` - Alert statistics

**Market Data:**
- `GET /api/market/indices` - Market indices
- `GET /api/charts/{symbol}` - OHLC chart data
- `GET /api/indicators/{symbol}` - Technical indicators
- `GET /api/greeks/{symbol}` - Options Greeks
- `GET /api/volume-profile/{symbol}` - Volume profile
- `GET /api/news/{symbol}` - Symbol news
- `GET /api/news/market/latest` - Latest market news

**Validation:**
- `GET /api/validation/{alert_id}` - Validation results
- `GET /api/validation/stats` - Validation statistics

### **External Access**

**LocalTunnel (Recommended):**
```bash
# Install LocalTunnel
npm install -g localtunnel

# Expose backend
lt --port 5001

# Expose frontend
lt --port 3000
```

**Security:**
- Rate limiting (100 req/min)
- CORS protection
- Origin validation
- Security headers
- JWT token authentication

---

*Last updated: November 4, 2025 - React Dashboard with 2FA & Brokerage Integration*
