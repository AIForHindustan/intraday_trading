# 🔔 INTRADAY TRADING ALERT SYSTEM - COMPREHENSIVE ANALYSIS

**Generated:** 2025-01-XX  
**Location:** `/Users/lokeshgupta/Desktop/intraday_trading/`

---

## 1. 📋 ALERT STRUCTURE & SOURCES

### **Core Alert Object Structure**

```python
{
    # PRIMARY IDENTIFIERS
    "symbol": "NSE:RELIANCE",          # Trading symbol
    "pattern": "volume_spike",         # Pattern type (see patterns below)
    "pattern_type": "volume_spike",     # Alias for pattern
    "confidence": 0.85,                 # Confidence score (0.0-1.0)
    
    # TIMING
    "timestamp": "2025-01-15T10:30:00+05:30",  # ISO format
    "timestamp_ms": 1736923200000,     # Epoch milliseconds
    "published_at": "2025-01-15T10:30:00+05:30",
    "exchange_timestamp": 1736923200000,
    
    # DIRECTION & SIGNAL
    "direction": "long",               # long/short/neutral
    "signal": "BUY",                   # BUY/SELL/WATCH/HOLD
    "action": "BUY-FOLIO",             # Trading action
    "directional_action": "BUY",       # Directional hint
    
    # PRICE & MOVEMENT
    "last_price": 2456.75,             # Current price
    "entry_price": 2456.75,            # Entry price
    "target": 2480.00,                 # Target price
    "stop_loss": 2440.00,              # Stop loss
    "expected_move": 0.95,              # Expected move percentage
    
    # VOLUME METRICS
    "volume_ratio": 2.3,               # Volume spike multiplier
    "bucket_incremental_volume": 125000,  # Current volume
    "zerodha_cumulative_volume": 5000000, # Session volume
    
    # INDICATORS (from Redis or calculated)
    "indicators": {
        "rsi": 65.5,
        "atr": 12.34,
        "vwap": 2450.00,
        "ema_20": 2445.00,
        "ema_50": 2430.00,
        "macd": 2.5,
        "volume_ratio": 2.3,
        "price_change": 0.5
    },
    
    # METADATA
    "pattern_title": "📊 Volume Spike",
    "pattern_description": "Unusual volume activity detected",
    "trading_instruction": "WATCH CLOSELY - Be ready to follow volume",
    "move_type": "VOLUME_SPIKE",
    "urgency": "medium",
    
    # NEWS CONTEXT (if available)
    "news_context": {
        "title": "Company announces earnings",
        "sentiment": "positive",
        "impact": "HIGH",
        "source": "Financial Express"
    },
    "has_news": True,
    
    # RISK METRICS
    "risk_metrics": {
        "stop_loss": 2440.00,
        "target_price": 2480.00,
        "position_size": 100,
        "risk_reward_ratio": 2.5
    },
    
    # TIME FRAME
    "timeframe": "5min",               # Detection timeframe
    
    # VALIDATION
    "qualified_path": "PATH_1",        # Which qualification path passed
    "pre_validation_score": 0.85,      # Pre-validation score
    "risk_category": "MEDIUM_RISK",     # LOW_RISK/MEDIUM_RISK/HIGH_RISK
    
    # SESSION CONTEXT
    "allow_premarket": False,          # Can alert be sent premarket?
    "market_session": "MORNING",       # Current market session
    "vix_value": 15.5,                 # Current VIX
    "vix_regime": "NORMAL"             # PANIC/NORMAL/COMPLACENT
}
```

### **Pattern Types Found**

#### **Core Patterns (8):**
- `volume_spike` - Unusual volume activity
- `volume_breakout` - Volume explosion with price move
- `volume_price_divergence` - Volume/price mismatch
- `reversal` - Trend reversal
- `breakout` - Price breaking resistance/support
- `upside_momentum` - Strong upward momentum
- `downside_momentum` - Strong downward momentum
- `hidden_accumulation` - Stealth institutional buying

#### **ICT Patterns:**
- `ict_liquidity_pools` - ICT liquidity grab
- `ict_fair_value_gaps` - FVG detection
- `ict_optimal_trade_entry` - OTE zones
- `ict_premium_discount` - Premium/discount zones
- `ict_killzone` - High-probability time windows
- `ict_momentum` - ICT-based momentum

#### **Straddle Strategy Patterns:**
- `kow_signal_straddle` - VWAP-based straddle strategy
- `iv_crush_play_straddle` - IV crush opportunities
- `range_bound_strangle` - Range-bound markets
- `market_maker_trap_detection` - MM trap patterns
- `premium_collection_strategy` - Premium collection

#### **Other Patterns:**
- `scalper_opportunity` - Quick scalping setups
- `spring_coil` - Accumulation pattern
- `fake_bid_wall_basic` - Fake bid wall detection
- `fake_ask_wall_basic` - Fake ask wall detection
- `NEWS_ALERT` - High-impact news alerts

### **Instruments**
- **Equity:** NIFTY 50 stocks, Mid-cap, Small-cap
- **Indices:** NIFTY 50, BANKNIFTY, NIFTY FIN SERVICE
- **Derivatives:** Futures, Options (CE/PE)
- **VIX:** INDIA VIX for regime detection

### **Timeframes**
- `5min` - Primary intraday timeframe
- `15min` - Secondary timeframe
- `1min` - Scalping timeframe
- `INTRADAY` - General intraday classification

### **Priority Levels**
Based on `AlertConflictResolver.PATTERN_PRIORITIES`:
- `reversal`: 90 (highest)
- `scalper_opportunity`: 85
- `volume_breakout`: 80
- `breakout`: 80
- `volume_spike`: 70
- `volume_price_divergence`: 60
- `hidden_accumulation`: 60
- `upside_momentum`: 50
- `downside_momentum`: 50

---

## 2. 🔄 CURRENT ALERT PIPELINE

### **Alert Generation**

**Location:** `scanner_main.py` → `PatternDetector.detect_patterns()`

**Flow:**
```
1. Market Data (Redis/WebSocket)
   ↓
2. PatternDetector (patterns/pattern_detector.py)
   - Detects patterns from indicators
   - Calculates confidence scores
   ↓
3. AlertManager (alerts/alert_manager.py)
   - Enhanced validation (6-path qualification)
   - Conflict resolution
   - Pre-validation (multi-layer)
   ↓
4. RetailAlertFilter (alerts/filters.py)
   - 6-path qualification system
   - Profitability checks
   - Cooldown management
   ↓
5. ProductionAlertManager.send_alert()
   - Prepares alert payload
   - Enriches with indicators/news
   ↓
6. Notifiers (alerts/notifiers.py)
   - TelegramNotifier
   - RedisNotifier
   - MacOSNotifier
```

### **Storage & Queuing**

#### **Redis Storage:**
- **Deduplication:** `alerts:dedup:{symbol}:{pattern_type}` (TTL: 300s/5min)
- **Notification Channel:** `alerts:notifications` (PUB/SUB)
- **Alert History:** In-memory (max 1000 alerts) in `ProductionAlertManager`
- **Channel Publishing:** `channel:NSEAlgoTrading` (main signals channel)

#### **In-Memory Queuing:**
- **Alert Queue:** `alert_queue` (max 5000 alerts) in `RetailAlertFilter`
- **Cooldown Tracker:** `cooldown_tracker` dictionary (symbol:pattern → timestamp)
- **Alert History:** `alert_history` dictionary (for spam prevention)

### **Sending Triggers**

1. **Immediate:** When pattern qualifies through 6-path system
2. **Batch:** Queue processing (disabled by default)
3. **Scheduled:** None currently
4. **Cooldown Gates:**
   - Derivatives: 8 seconds between same symbol/pattern
   - Equity: 4 seconds between same symbol/pattern
   - Index: 3 seconds between same symbol/pattern

### **Deduplication Logic**

#### **Redis Deduplication:**
```python
# Location: alerts/alert_manager.py (EnhancedAlertValidator)
dedup_key = f"alerts:dedup:{symbol}:{pattern_type}"
if redis_client.exists(dedup_key):
    return False  # Duplicate alert, skip

redis_client.setex(dedup_key, 300, "1")  # 5 minute TTL
```

#### **In-Memory Cooldown:**
```python
# Location: alerts/filters.py (RetailAlertFilter)
cooldown_key = f"{symbol}:{pattern}"
last_alert_time = cooldown_tracker.get(cooldown_key)
if time_since_last < cooldown_period:
    return False  # Still in cooldown
```

#### **Conflict Resolution:**
```python
# Location: alerts/alert_manager.py (AlertConflictResolver)
# Groups alerts by symbol, keeps highest priority pattern
# Priority determined by PATTERN_PRIORITIES dictionary
```

---

## 3. ⏰ TIME-BASED GROUPING NEEDS

### **Current Time Windows**

**Recommended Grouping Windows:**
- **1 minute:** Too aggressive, may group dissimilar alerts
- **5 minutes:** GOOD - Default intraday timeframe
- **15 minutes:** GOOD - Secondary timeframe
- **30 minutes:** Too long, may miss distinct patterns

### **Session Boundaries**

**Trading Sessions:**
- **PRE_OPEN:** 9:00-9:15 (alerts suppressed by default)
- **OPENING:** 9:15-9:45
- **MORNING:** 9:45-11:00
- **MID_MORNING:** 11:00-12:30
- **MIDDAY:** 12:30-14:00
- **AFTERNOON:** 14:00-15:00
- **CLOSING:** 15:00-15:30
- **POST_CLOSE:** 15:30+

**Recommendation:** Group alerts within same market session only.

### **Cross-Instrument Grouping**

**Current Behavior:** Alerts are grouped by symbol first, then pattern type.

**Recommendation:** 
- **DO NOT group across instruments** - Each symbol needs separate alerts
- Exception: Sector-wide alerts (like banking sector momentum)

---

## 4. 📊 LOGICAL GROUPING DIMENSIONS

### **Priority Order (Recommended)**

1. **Instrument** (NIFTY, BANKNIFTY, RELIANCE, etc.) - MUST separate
2. **Pattern Type** (kow, volume_spike, ict, etc.) - Logical grouping
3. **Direction** (long/short) - Separate opposing directions
4. **Timeframe** (5min, 15min) - Keep timeframes separate
5. **Time Window** (within X minutes) - Final grouping criterion

### **Grouping Logic Example**

```python
# Group alerts by:
group_key = f"{instrument}:{pattern_type}:{direction}:{timeframe}"

# Then apply time window:
time_window = 5 * 60  # 5 minutes in seconds
if abs(alert1.timestamp - alert2.timestamp) <= time_window:
    # Group them
```

---

## 5. 📈 ALERT VOLUME ANALYSIS

### **Pattern Distribution** (Estimated)

Based on code analysis:

```python
alert_counts = {
    'by_pattern': {
        'volume_spike': 'HIGH (~2000/day)',        # Most common
        'volume_breakout': 'MEDIUM (~1000/day)',   # High priority
        'kow_signal_straddle': 'LOW (~50/day)',   # Specific strategy
        'ict_liquidity_pools': 'MEDIUM (~500/day)',
        'upside_momentum': 'MEDIUM (~800/day)',
        'reversal': 'LOW (~200/day)',            # High priority when occurs
        'hidden_accumulation': 'LOW (~150/day)',
        'NEWS_ALERT': 'VARIES (~10-100/day)'     # Event-driven
    },
    'by_instrument': {
        'NIFTY': 'HIGH (~1500/day)',             # Index futures
        'BANKNIFTY': 'HIGH (~1200/day)',         # Index futures
        'NIFTY50_STOCKS': 'MEDIUM (~500/day)',   # Individual stocks
        'DERIVATIVES': 'HIGH (~2000/day)'        # F&O instruments
    },
    'by_timeframe': {
        '5min': 'HIGH (~4000/day)',              # Primary timeframe
        '15min': 'MEDIUM (~1000/day)',           # Secondary
        '1min': 'LOW (~500/day)'                 # Scalping
    },
    'by_confidence': {
        '0.90-1.0': 'MEDIUM (~500/day)',         # High confidence
        '0.85-0.90': 'HIGH (~2000/day)',         # Medium-high
        '0.80-0.85': 'HIGH (~2500/day)',        # Medium
        '0.70-0.80': 'MEDIUM (~1000/day)',      # Lower threshold
        '0.0-0.70': 'FILTERED OUT'               # Rejected
    }
}
```

**Note:** These are estimates based on code thresholds. Actual volumes require Redis analysis.

---

## 6. 📤 DESIRED OUTPUT FORMAT

### **Option A: Grouped Summary Alert** (RECOMMENDED)

```python
{
    "type": "grouped_summary",
    "instrument": "BANKNIFTY",
    "pattern": "volume_spike",
    "direction": "long",
    "timeframe": "5min",
    
    # GROUPING METRICS
    "count": 15,                    # 15 individual alerts grouped
    "window_start": "2025-01-15T10:30:00+05:30",
    "window_end": "2025-01-15T10:42:00+05:30",
    "duration_minutes": 12,
    
    # CONFIDENCE AGGREGATION
    "max_confidence": 0.92,
    "avg_confidence": 0.87,
    "min_confidence": 0.82,
    
    # PRICE AGGREGATION
    "first_price": 45200.0,
    "last_price": 45350.0,
    "avg_price": 45275.0,
    "price_range": (45200.0, 45350.0),
    
    # VOLUME AGGREGATION
    "max_volume_ratio": 3.2,
    "avg_volume_ratio": 2.5,
    "total_volume": 15000000,
    
    # TRIGGER REASONS
    "trigger_reasons": [
        "vwap_break",
        "volume_spike",
        "ict_liquidity_grab",
        "multi_timeframe_confirmation"
    ],
    
    # QUALIFICATION PATHS
    "qualified_paths": ["PATH_1", "PATH_2"],  # Multiple paths qualified
    
    # REPRESENTATIVE ALERT (highest confidence)
    "representative_alert": {
        "timestamp": "2025-01-15T10:38:00+05:30",
        "confidence": 0.92,
        "last_price": 45300.0,
        "volume_ratio": 3.0
    },
    
    # CONTEXT
    "market_session": "MORNING",
    "vix_regime": "NORMAL",
    "has_news": True,
    "news_count": 2
}
```

### **Option B: Representative Alert**

Use highest confidence alert from group, but add metadata:
```python
{
    ...original_alert...,
    "grouped_count": 15,
    "group_duration": 12,  # minutes
    "group_avg_confidence": 0.87
}
```

### **Option C: Frequency-Based**

Alert every X duplicates:
```python
# Alert on 1st, 5th, 10th, 15th occurrence
if count % 5 == 0 or count == 1:
    send_alert(representative_alert)
```

---

## 7. 🚨 CURRENT ALERT DESTINATIONS

### **Telegram**

**Configuration:** `alerts/config/telegram_config.json`

**Bots:**
1. **Main Bot:** All alerts with confidence >= 90%
2. **Signal Bot:** High-priority only (KOW Signal Straddle, NIFTY/BANKNIFTY futures)

**Rate Limits:**
- **Telegram API:** 30 messages/second per bot
- **Current Implementation:** 0.5 seconds between messages (self-imposed)
- **Rate Limit Handling:** Automatic backoff with retry (3 attempts)

**Channels:**
- Main chat IDs configured in `telegram_config.json`
- Signal bot chat IDs for high-priority alerts

### **Redis (PUB/SUB)**

**Channels:**
- `alerts:notifications` - Main alert channel
- `channel:NSEAlgoTrading` - Structured channel for external consumers

**Format:** JSON string published to Redis

### **macOS Notifications**

**Trigger:** Confidence >= 90%  
**Location:** Uses `osascript` for native macOS notifications

### **File Logs**

**Log Files:**
- `logs/scanner_main.log` - Main scanner logs
- `logs/alert_validator.log` - Alert validation logs
- `logs/scanner_detailed.log` - Detailed pattern detection logs

**Alert Storage:** No dedicated alert log file. Alerts are logged as part of scanner logs.

---

## 8. 🎯 IMPORTANCE HIERARCHY - NEVER GROUP THESE

**CRITICAL - Never Group:**

1. **KOW Signal Straddle** (`kow_signal_straddle`)
   - Strategy-specific, precision-critical
   - Sent to separate signal bot
   
2. **Reversal Patterns** (`reversal`)
   - High priority (90), time-sensitive
   - Each reversal is distinct setup

3. **News Alerts** (`NEWS_ALERT`)
   - Event-driven, non-repetitive
   - Each news item is unique

4. **High Confidence (>0.95)**
   - Rare, high-impact alerts
   - Each deserves individual attention

5. **Different Directions** (long vs short)
   - Opposing signals should never group
   - Separate trade setups

6. **Different Instruments**
   - Always separate by symbol
   - Each symbol is independent trade

---

## 9. 🔍 ALERT LOG LOCATION

### **Current Alert Logs**

**No dedicated alert log file exists.** Alerts are logged within:

1. **`logs/scanner_main.log`**
   - Main scanner execution logs
   - Pattern detection results
   - Alert processing decisions

2. **`logs/alert_validator.log`**
   - Alert validation results
   - Forward validation tracking

3. **Redis Channels:**
   - `alerts:notifications` - All sent alerts
   - `channel:NSEAlgoTrading` - Structured alerts

### **Retrieving Alert History**

**From Redis:**
```python
# Get recent alerts from Redis channel
redis_client = get_redis_client()
recent_alerts = redis_client.lrange("alerts:notifications", 0, -1)

# Parse JSON alerts
alerts = [json.loads(alert) for alert in recent_alerts]
```

**From Log Files:**
```bash
# Search for alerts in logs
grep -r "Alert approved\|Alert rejected" logs/scanner_main.log

# Find pattern detections
grep -r "pattern" logs/scanner_main.log
```

---

## 10. 📝 RECOMMENDATIONS FOR GROUPING IMPLEMENTATION

### **Grouping Strategy**

```python
# Recommended grouping configuration
GROUPING_CONFIG = {
    'time_window_seconds': 300,  # 5 minutes
    'min_group_size': 2,         # Minimum alerts to group
    'max_group_size': 50,        # Maximum alerts per group
    
    'never_group_patterns': [
        'kow_signal_straddle',
        'NEWS_ALERT',
        'reversal'  # Consider carefully
    ],
    
    'always_separate_directions': True,
    'always_separate_instruments': True,
    'group_within_session_only': True,
    
    'priority_threshold': 0.95,  # Never group alerts above this
}
```

### **Implementation Priority**

1. **Phase 1:** Implement time-based grouping (5min window)
2. **Phase 2:** Add pattern-type grouping
3. **Phase 3:** Add direction-based separation
4. **Phase 4:** Add summary alert generation
5. **Phase 5:** Add frequency-based alerting option

---

## 11. 🔗 KEY FILES FOR ALERT SYSTEM

| File | Purpose |
|------|---------|
| `alerts/alert_manager.py` | Core alert processing, validation |
| `alerts/filters.py` | 6-path qualification, filtering |
| `alerts/notifiers.py` | Telegram, Redis, macOS notifications |
| `scanner_main.py` | Main orchestration, pattern detection |
| `alert_validation/alert_validator.py` | Forward validation, backtesting |
| `alerts/config/telegram_config.json` | Telegram bot configuration |
| `config/thresholds.py` | Alert thresholds, VIX regimes |

---

## 📌 SUMMARY

The alert system is **well-structured** with:
- ✅ Comprehensive 6-path qualification system
- ✅ Multi-layer pre-validation
- ✅ Redis-based deduplication
- ✅ Multiple notification channels
- ✅ Pattern-specific thresholds
- ❌ **No dedicated alert log file** (alerts in general logs)
- ❌ **No alert grouping currently implemented**
- ✅ Strong conflict resolution
- ✅ VIX-aware thresholds
- ✅ Session-based filtering

**Next Steps for Grouping:**
1. Implement time-window grouping (5 minutes)
2. Add pattern-type grouping logic
3. Create grouped summary alert format
4. Test with high-volume days
5. Add grouping statistics/metrics

---

**Document Version:** 1.0  
**Last Updated:** 2025-01-XX  
**Author:** AI Assistant (based on codebase analysis)
