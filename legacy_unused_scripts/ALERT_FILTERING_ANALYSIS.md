# Alert Filtering Logic Analysis

## 🎯 Overview

The alert filtering system uses a **multi-layered approach** with **6-path qualification paths** plus **Redis-based deduplication** to ensure only high-quality, actionable alerts are sent to users.

## 🏗️ System Architecture

```
Pattern Detection → Alert Manager → 6-Path Filter → Redis Deduplication → Notification
```

## 📊 Alert Filtering Components

### 1. **Schema Pre-Gate** (Optional)
- **Location**: `_passes_schema_pre_gate()` in `alert_manager.py`
- **Purpose**: Basic pattern structure validation
- **Status**: Disabled by default (`enable_schema_pre_gate = False`)

### 2. **Redis Deduplication** (Primary)
- **Location**: `should_send_alert_enhanced()` lines 4418-4432
- **Key**: `alert_cooldown:{symbol}_{pattern_name}`
- **Cooldown**: **5 seconds** for identical symbol+pattern combinations
- **Purpose**: Prevent duplicate alerts for same symbol+pattern within 5 seconds

### 3. **Dynamic Expiry Filtering**
- **Location**: Lines 4434-4443
- **Logic**: 
  - Expired contracts (DTE ≤ 0): **REJECTED**
  - Expiry day (DTE ≤ 1): **Higher thresholds** via `_check_expiry_day_conditions()`

### 4. **6-Path Qualification System**

#### **PATH 1: High Confidence**
- **Thresholds**: VIX-aware via `config.volume_thresholds`
- **Requirements**:
  - Confidence ≥ `high_confidence` threshold
  - Expected move ≥ `min_move` threshold  
  - Volume ratio ≥ `path1_min_vol` threshold
  - Net profit ≥ 0.05% (after costs)
- **Cooldown**: Redis 5-second cooldown after success

#### **PATH 2: Volume Spike**
- **Thresholds**: VIX-aware volume spike thresholds
- **Requirements**:
  - Volume ratio ≥ `volume_spike_min_vol` threshold
  - Expected move ≥ `volume_spike_min_move` threshold
  - Confidence ≥ `high_confidence` threshold
  - Net profit ≥ 0.05% (after costs)
- **Cooldown**: Redis 5-second cooldown after success

#### **PATH 3: Sector-Specific** (Not Implemented)
- **Status**: Placeholder for sector-specific thresholds**

#### **PATH 4: Balanced Play**
- **Thresholds**: VIX-aware balanced thresholds
- **Requirements**:
  - Confidence ≥ `balanced_confidence` threshold
  - Volume ratio ≥ `balanced_volume` threshold
  - Expected move ≥ `balanced_move` threshold
  - Net profit ≥ 0.05% (after costs)
- **Cooldown**: Local cooldown tracking

#### **PATH 5: Proven Patterns**
- **Patterns**: 28 proven patterns including:
  - Volume patterns: `volume_spike`, `volume_accumulation`, `volume_dump`
  - Momentum patterns: `upside_momentum`, `downside_momentum`
  - Pressure patterns: `buy_pressure`, `sell_pressure`
  - Breakout patterns: `breakout`, `breakdown`, `reversal`
  - ICT patterns: `ict_liquidity_grab_fvg_retest_long`, etc.
  - MM patterns: `hidden_accumulation`, `fake_bid_wall_basic`
- **Requirements**:
  - Pattern must be in `PROVEN_PATTERNS` set
  - Confidence ≥ `proven_patterns_confidence` threshold
  - Volume ratio ≥ `proven_patterns_ratio` threshold
  - Expected move ≥ `proven_patterns_min_move` threshold
  - Volume ratio < 5.0 (avoid extreme spikes)
  - Net profit ≥ 0.05% (after costs)
- **Cooldown**: Redis 5-second cooldown after success

#### **PATH 6: Composite Scoring**
- **Logic**: Weighted score calculation based on:
  - Pattern priority (higher = more important)
  - Confidence level
  - Volume ratio
  - Expected move
- **Requirements**:
  - Composite score ≥ 35/100
  - Net profit ≥ 0.04% (after costs)
- **Cooldown**: Local cooldown tracking

#### **PATH 7: Pressure Patterns**
- **Patterns**: `buy_pressure`, `sell_pressure`
- **Requirements**:
  - Confidence ≥ `pressure_patterns_confidence` threshold
  - Volume ratio ≥ `pressure_patterns_ratio` threshold
  - Expected move ≥ `pressure_patterns_min_move` threshold
  - Net profit ≥ 0.01% (lower threshold for pressure patterns)
- **Cooldown**: Redis 5-second cooldown after success

## 🔧 Key Configuration

### **Threshold Sources**
- **Primary**: `config.volume_thresholds.py` with VIX-aware multipliers
- **VIX Regimes**: Complacent (0.7x), Normal (1.0x), Panic (1.2x)
- **Dynamic**: Time-based adjustments for market sessions

### **Cooldown Systems**
1. **Redis Deduplication**: 5-second cooldown for same symbol+pattern
2. **Local Cooldown**: Longer-term tracking for some paths
3. **Expiry Filtering**: Dynamic thresholds based on days to expiry

### **Cost Structure**
- **Total Costs**: 0.15% (brokerage + slippage + taxes)
- **Minimum Net Profit**: 0.05% for most paths, 0.01% for pressure patterns
- **Profit Calculation**: `abs(expected_move) - TOTAL_COSTS`

## 📈 Alert Flow

### **Main Scanner** (`scanner/production/main.py`)
1. **Pattern Detection**: Receives pattern from detector
2. **Individual Alert**: Calls `_send_individual_alert()`
3. **Redis Check**: 5-second cooldown check for same symbol+pattern
4. **Alert Manager**: Delegates to `alert_manager.send_alert()`

### **Alert Manager** (`scanner/alert_manager.py`)
1. **Schema Pre-Gate**: Optional basic validation
2. **6-Path Filter**: `should_send_alert_enhanced()` qualification
3. **Redis Cooldown**: Set 5-second cooldown after successful alert
4. **Publish**: Send to macOS, Telegram, Console

## 🎯 Filtering Effectiveness

### **Current Status**
- ✅ **Redis Deduplication**: Working (5-second cooldown)
- ✅ **6-Path Qualification**: Active with VIX-aware thresholds
- ✅ **Expiry Filtering**: Dynamic DTE-based filtering
- ✅ **Cost-Aware**: All paths ensure profitability after costs

### **Key Improvements Made**
1. **Removed `/375` division** from volume thresholds (was too restrictive)
2. **Consolidated cooldown logic** to Redis-only deduplication
3. **Added VIX-aware thresholds** for dynamic market conditions
4. **Implemented expiry filtering** for F&O contracts
5. **Enhanced pattern coverage** with 28 proven patterns

## 📊 Performance Metrics

### **Filtering Rates**
- **Total Processed**: Tracked in `stats["total_processed"]`
- **Schema Rejections**: Tracked in `stats["schema_rejections"]`
- **Alerts Sent**: Tracked in `stats["alerts_sent_schema_approved"]`
- **Expired Contracts**: Tracked in `stats["expired_contract_rejections"]`

### **Cooldown Effectiveness**
- **Redis Cooldown**: 5-second window prevents duplicates
- **Local Tracking**: Longer-term pattern analysis
- **Memory Management**: Automatic cleanup of old entries

## 🔍 Debugging

### **Debug Logs**
- **Threshold Debug**: `🔍 DEBUG: {symbol} thresholds: {thresholds}`
- **Redis Cooldown**: `🚫 Redis cooldown active: {symbol} {pattern_name}`
- **Path Qualification**: `✅ PRESSURE_PATTERN_QUALIFIED: {symbol} {pattern_name}`
- **Alert Validation**: `🔍 ALERT VALIDATION: Filter={should_send}, reason={reason}`

### **Key Log Messages**
- **Alert Sent**: `✅ ALERT SENT SUCCESSFULLY: {symbol} {pattern_name}`
- **Alert Rejected**: `❌ ALERT REJECTED BY FILTER: {symbol} {pattern_name}`
- **Redis Deduplication**: `🚫 REDIS DEDUPLICATION: Blocked duplicate alert`

## 🎯 Summary

The alert filtering system is **highly sophisticated** with:
- **6-path qualification** ensuring only actionable alerts
- **Redis deduplication** preventing spam (5-second cooldown)
- **VIX-aware thresholds** adapting to market conditions
- **Expiry filtering** for F&O contracts
- **Cost-aware filtering** ensuring profitability
- **28 proven patterns** with specialized thresholds

The system successfully balances **alert quality** with **actionability**, ensuring users receive only high-probability trading opportunities while preventing alert spam through intelligent deduplication and multi-path qualification.
