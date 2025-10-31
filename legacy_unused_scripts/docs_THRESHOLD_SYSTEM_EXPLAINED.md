# THRESHOLD SYSTEM EXPLAINED

## Overview

Your alert filtering system uses **dynamic, multi-factor thresholds** that adjust based on:
1. **Time of Day** (Market session phases)
2. **VIX Level** (Market volatility regime)
3. **Sector Volatility** (Sector-specific behavior)
4. **Pattern Type** (Pattern-specific confidence boosts)

This is NOT a static system - thresholds adapt in real-time to market conditions.

---

## 🎯 THRESHOLD CALCULATION FLOW

```
Base Thresholds (Class Constants)
    ↓
Apply Time-of-Day Multiplier (1.0 - 1.5x)
    ↓
Apply VIX Regime Multiplier (0.7 - 1.3x)
    ↓
Apply Sector Volatility Multiplier (0.9 - 1.2x)
    ↓
Final Dynamic Thresholds
```

---

## 1. BASE THRESHOLDS (Starting Point)

### Defined in `RetailAlertFilter.__init__()` (Lines 1510-1565)

```python
# CONFIDENCE THRESHOLDS
DERIVATIVE_CONF_THRESHOLD = 0.85  # 85% for derivatives
HIGH_CONF_THRESHOLD = 0.85        # 85% high confidence
MEDIUM_CONF_THRESHOLD = 0.80      # 80% medium confidence
PATH1_CONF_THRESHOLD = 0.85       # 85% for primary filtering path

# VOLUME THRESHOLDS
PATH1_MIN_VOL = 2.0 / 375         # 2.0x average volume (per-minute adjusted)
VOLUME_SPIKE_MIN_VOL = 2.0 / 375  # 2.0x for volume spikes

# MOVE THRESHOLDS
PATH1_MIN_MOVE = 0.30             # 0.30% minimum expected price move
VOLUME_SPIKE_MIN_MOVE = 0.30      # 0.30% for volume spikes
BALANCED_MIN_MOVE = 0.30          # 0.30% balanced minimum

# COOLDOWN PERIODS
DERIVATIVE_COOLDOWN = 45          # 45 seconds for derivatives
EQUITY_COOLDOWN = 30              # 30 seconds for equities
```

**Key Insight:** These are BASE values. They get multiplied by dynamic factors.

---

## 2. TIME-OF-DAY MULTIPLIERS (Market Session Phases)

### Implemented in `_get_current_time_multiplier()` (Lines 2184-2240)

The system recognizes that different market sessions have different noise levels:

| Time Period | Multiplier | Threshold Effect | Rationale |
|-------------|-----------|------------------|-----------|
| **Pre-market** (Before 9:15 AM) | **1.3x** | 🔴 30% STRICTER | Thin liquidity, prone to manipulation |
| **Opening** (9:15-9:30 AM) | **1.2x** | 🟠 20% STRICTER | High volatility, false breakouts |
| **Golden Hour** (9:30-10:30 AM) | **1.0x** | 🟢 NORMAL | Best trading conditions |
| **Mid-Morning** (10:30-11:30 AM) | **1.1x** | 🟡 10% STRICTER | Momentum slowing |
| **Late Morning** (11:30-12:30 PM) | **1.2x** | 🟠 20% STRICTER | Pre-lunch positioning |
| **Lunch Hour** (12:30-1:30 PM) | **1.4x** | 🔴 40% STRICTER | Lowest volume, highest noise |
| **Afternoon** (1:30-2:30 PM) | **1.2x** | 🟠 20% STRICTER | Caution period |
| **Final Hour** (2:30-3:30 PM) | **1.1x** | 🟡 10% STRICTER | Closing volatility |
| **After hours** | **1.3x** | 🔴 30% STRICTER | Market closed |

**Example Calculation:**
```python
# Base confidence threshold: 85%
# At 9:45 AM (Golden Hour): 85% × 1.0 = 85%
# At 1:00 PM (Lunch Hour): 85% × 1.4 = 119% → capped at 95%

# Base move threshold: 0.30%
# At 9:45 AM: 0.30% × 1.0 = 0.30%
# At 1:00 PM: 0.30% × 1.4 = 0.42%
```

**Caching:** Multiplier is cached for 30 seconds to avoid redundant calculations.

---

## 3. VIX REGIME MULTIPLIERS (Market Volatility)

### Implemented in `_get_vix_multiplier()` (Lines 2056-2064)

VIX measures market fear/volatility. The system adjusts thresholds inversely:

| VIX Level | Regime | Multiplier | Threshold Effect | Strategy |
|-----------|--------|-----------|------------------|----------|
| **VIX < 12** | 🟢 COMPLACENT | **0.7x** | 30% EASIER | Market too calm, need to act |
| **VIX 12-22** | 🟡 NORMAL | **1.0x** | NORMAL | Standard conditions |
| **VIX > 22** | 🔴 PANIC | **1.3x** | 30% STRICTER | High volatility, more false signals |

**Rationale:**
- **Low VIX:** Market is calm, opportunities are scarce → lower thresholds to catch moves
- **High VIX:** Market is chaotic, many false breakouts → higher thresholds to filter noise

**Example:**
```python
# Pattern has 82% confidence, 0.35% expected move
# Base threshold: 80% confidence, 0.30% move

# VIX = 10 (COMPLACENT):
#   Required: 80% × 0.7 = 56% confidence ✅
#   Required: 0.30% × 0.7 = 0.21% move ✅
#   Result: ALERT SENT

# VIX = 25 (PANIC):
#   Required: 80% × 1.3 = 104% → capped at 95% ❌
#   Required: 0.30% × 1.3 = 0.39% move ❌
#   Result: REJECTED
```

**Implementation Note:** Current VIX fetched from `get_current_vix()` - defaults to 15.0 if unavailable.

---

## 4. SECTOR VOLATILITY MULTIPLIERS (Sector Behavior)

### Implemented in `_get_sector_volatility_multiplier()` (Lines 2066-2074)

Different sectors have different volatility profiles:

| Sector Volatility | Multiplier | Threshold Effect | Examples |
|-------------------|-----------|------------------|----------|
| **HIGH** | **1.2x** | 20% STRICTER | Small-cap, PSU, Metals, Energy |
| **MEDIUM** | **1.0x** | NORMAL | Large-cap, Banking, IT |
| **LOW** | **0.9x** | 10% EASIER | Utilities, FMCG, Pharma |

**Current Status:** Placeholder implementation returns "medium" for all symbols.

**TODO for Enhancement:**
```python
def _get_sector_volatility(self, symbol):
    """Map symbol to sector volatility classification"""
    
    # High volatility sectors
    HIGH_VOL_SECTORS = {
        "PSU", "ENERGY", "METALS", "SMALLCAP", "MIDCAP"
    }
    
    # Low volatility sectors
    LOW_VOL_SECTORS = {
        "FMCG", "PHARMA", "UTILITIES", "TELECOM"
    }
    
    sector = self._get_symbol_sector(symbol)
    
    if sector in HIGH_VOL_SECTORS:
        return "high"
    elif sector in LOW_VOL_SECTORS:
        return "low"
    else:
        return "medium"
```

---

## 5. COMBINED MULTIPLIER CALCULATION

### Implemented in `get_market_adjusted_thresholds()` (Lines 2028-2057)

All multipliers are combined multiplicatively:

```python
final_multiplier = time_multiplier × vix_multiplier × sector_multiplier
```

**Example Scenario:**
```
Time: 1:00 PM (Lunch Hour) → 1.4x
VIX: 25 (Panic)            → 1.3x
Sector: PSU (High Vol)     → 1.2x

Combined Multiplier = 1.4 × 1.3 × 1.2 = 2.184x

Base Thresholds:
- Confidence: 85%
- Volume: 2.0x
- Move: 0.30%

Adjusted Thresholds:
- Confidence: 85% × 2.184^0.7 = 85% × 1.67 = 142% → capped at 95%
- Volume: 2.0x × 2.184 = 4.37x
- Move: 0.30% × 2.184 = 0.66%
```

**Note:** Confidence uses `multiplier^0.7` to prevent extreme values (Line 2017).

**Safety Caps:**
- Minimum confidence: 80% (never lower)
- Maximum confidence: 95% (never higher)

---

## 6. PATTERN-SPECIFIC CONFIDENCE BOOSTS

### Defined in `PATTERN_CONFIDENCE_BOOSTS` (Lines 23-31)

Proven patterns get confidence boosts based on historical win rates:

```python
PATTERN_CONFIDENCE_BOOSTS = {
    "PSU_DUMP": 0.72,              # 72% win rate
    "SPRING_COIL": 0.75,           # 75% win rate
    "COORDINATED_MOVE": 0.68,      # 68% win rate
    "STEALTH_ACCUMULATION": 0.70,  # 70% win rate
    "DISTRIBUTION": 0.65,          # 65% win rate
    "BREAKOUT": 0.60,              # 60% win rate
    "REVERSAL": 0.55,              # 55% win rate
}
```

**Applied in `_apply_pattern_confidence_boost()`:**
```python
boosted_confidence = max(original_confidence, pattern_boost)
```

**This is a FLOOR, not a multiplier** - ensures proven patterns never fall below their historical win rate.

---

## 7. REAL-WORLD EXAMPLE: ALERT DECISION

Let's trace a complete alert decision:

### Scenario: BankNifty Future Pattern

**Input Pattern:**
```json
{
  "symbol": "NFO:BANKNIFTY28OCTFUT",
  "pattern_type": "ict_liquidity_grab_fvg_retest_short",
  "confidence": 0.82,
  "expected_move": 0.45,
  "volume_ratio": 2.8,
  "timestamp": "2024-01-15 13:15:00"  // Lunch hour
}
```

**Step 1: Time Multiplier**
- Time: 1:15 PM → Lunch hour → **1.4x**

**Step 2: VIX Multiplier**
- VIX: 18 (Normal) → **1.0x**

**Step 3: Sector Multiplier**
- Sector: Index Future (Medium) → **1.0x**

**Step 4: Combined Multiplier**
```
Combined = 1.4 × 1.0 × 1.0 = 1.4x
```

**Step 5: Calculate Required Thresholds**
```
Base confidence: 85%
Required: 85% × min(1.4^0.7, 1.12) = 85% × 1.12 = 95.2% → capped at 95%

Base volume: 2.0x
Required: 2.0 × 1.4 = 2.8x

Base move: 0.30%
Required: 0.30% × 1.4 = 0.42%
```

**Step 6: Check Pattern Against Thresholds**
```
Pattern confidence: 82% < 95% ❌ FAILS
Pattern volume: 2.8x = 2.8x ✅ PASSES
Pattern move: 0.45% > 0.42% ✅ PASSES
```

**Result:** ❌ **REJECTED** (fails confidence threshold)

**If this was at 10:00 AM instead:**
```
Time multiplier: 1.0x
Required confidence: 85% × 1.0 = 85%
Pattern confidence: 82% < 85% ❌ Still fails (close though!)
```

**If pattern had 86% confidence at 10:00 AM:**
```
Required: 85% ✅
Volume: 2.8x > 2.0x ✅
Move: 0.45% > 0.30% ✅
```

**Result:** ✅ **ALERT SENT**

---

## 8. ICT PATTERNS: SPECIAL HANDLING

### Implemented in `should_send_alert()` from `pattern_schema.py`

ICT patterns have **stricter baseline requirements**:

```python
# Standard patterns
if confidence < 0.70:
    return False  # 70% minimum

# ICT patterns (pattern_type starts with "ict_")
if pattern_type.startswith("ict_"):
    if confidence < 0.75:
        return False  # 75% minimum (5% higher)
```

**Additionally, ICT patterns check cumulative volume:**
```python
if cumulative_delta is not None and cumulative_delta < 1000:
    return False  # Insufficient session volume
```

**Rationale:** ICT concepts (liquidity grabs, FVG retests, OTE zones) are sophisticated setups requiring stronger confirmation.

---

## 9. PROFITABILITY GATING (Cost-Adjusted)

### Implemented throughout filtering paths

Every alert must pass profitability check:

```python
TOTAL_COSTS = 0.0025  # 0.25% (brokerage + STT + charges)

net_profit = abs(expected_move) - TOTAL_COSTS

if net_profit < 0.08:  # 0.08% minimum net profit
    return False
```

**Example:**
- Expected move: 0.35%
- Total costs: 0.25%
- Net profit: 0.35% - 0.25% = 0.10% ✅ Passes (> 0.08%)

**Pattern-Specific Profit Thresholds:**
```python
PROFIT_THRESHOLDS = {
    "coordinated_manipulation": 0.12,  # Higher bar
    "volume_spike": 0.08,
    "market_maker": 0.04,              # Lower bar (frequent)
    "DEFAULT": 0.05
}
```

---

## 10. COOLDOWN SYSTEM (Anti-Spam)

### Prevents Alert Flooding

**Per-Symbol Cooldowns:**
```python
DERIVATIVE_COOLDOWN = 45  # seconds
EQUITY_COOLDOWN = 30      # seconds
```

**Logic:**
```python
last_alert = self.last_alert_time.get(symbol, 0)
cooldown_period = 45 if "NFO:" in symbol else 30

if time.time() - last_alert < cooldown_period:
    return False  # Too soon
```

**Global Rate Limit:**
```python
self.global_alert_rate_limit = 1.0  # Minimum 1 second between ANY alerts
```

---

## 🎯 SUMMARY: WHAT MAKES THRESHOLDS DYNAMIC?

| Factor | Multiplier Range | Impact | Update Frequency |
|--------|-----------------|--------|------------------|
| **Time of Day** | 1.0x - 1.5x | High | Every 30 seconds (cached) |
| **VIX Regime** | 0.7x - 1.3x | High | Real-time |
| **Sector Volatility** | 0.9x - 1.2x | Medium | Static (TODO: make dynamic) |
| **Pattern Type** | Floor boost | Medium | Per-pattern |
| **Profitability** | Binary gate | High | Per-alert |
| **Cooldown** | Binary gate | High | Real-time |

---

## 📊 CURRENT STATUS

### ✅ Fully Implemented
- Time-of-day multipliers
- VIX regime multipliers
- Pattern confidence boosts
- Profitability gating
- Cooldown system
- Combined threshold calculation

### 🚧 Partially Implemented
- Sector volatility (placeholder returns "medium")
- VIX fetching (defaults to 15.0 if unavailable)

### 💡 Enhancement Opportunities

1. **Real-time Sector Volatility:**
   - Calculate 14-day ATR for each sector
   - Update classification dynamically
   - Store in Redis for fast lookup

2. **Adaptive Learning:**
   - Track alert success rates by time/VIX/sector
   - Adjust multipliers based on outcomes
   - Machine learning for optimal thresholds

3. **Symbol-Specific Calibration:**
   - Individual stock volatility profiles
   - Historical win rates per symbol
   - Personalized thresholds

4. **Market Breadth Integration:**
   - Advance/decline ratio
   - Sector rotation detection
   - Risk-on/risk-off regimes

---

## 🔬 DEBUGGING THRESHOLDS

When alerts are rejected, check the logs:

```python
logger.info(f"🔍 DEBUG: {symbol} thresholds: {thresholds}")
logger.info(f"🔍 DEBUG: confidence: {confidence}, required: {thresholds['min_confidence']}")
```

**Threshold Breakdown Includes:**
```json
{
  "min_confidence": 0.95,
  "min_volume": 0.0112,
  "min_move": 0.42,
  "multiplier_breakdown": {
    "time_base": {...},
    "vix_multiplier": 1.3,
    "sector_multiplier": 1.2,
    "combined_multiplier": 1.56
  }
}
```

---

## 🚀 BEST PRACTICES

1. **Don't override thresholds lightly** - they're calibrated for profitability
2. **Monitor rejection rates** - if > 95%, thresholds may be too strict
3. **Time your trades** - Golden Hour (9:30-10:30 AM) has easiest thresholds
4. **Trust the VIX adjustment** - it adapts to market conditions
5. **Pattern confidence matters** - proven patterns get automatic boosts

---

## 📝 CONFIGURATION

To adjust threshold behavior, modify `config` in `RetailAlertFilter.__init__()`:

```python
self.config = {
    "enable_schema_pre_gate": True,         # Schema validation gate
    "total_costs": 0.0025,                  # 0.25% trading costs
    "global_alert_rate_limit": 1.0,         # 1 second between alerts
    "alert_cooldown_seconds": 45,           # Per-symbol cooldown
    "min_confidence_threshold": 0.8,        # Baseline confidence
    "max_alert_age_minutes": 1.0,           # Stale alert cutoff
}
```

---

**Last Updated:** October 2024  
**Maintainer:** Your Trading System  
**Related Files:**
- `scanner/alert_manager.py` - Main implementation
- `patterns/pattern_schema.py` - Schema validation and ICT gating
- `patterns/ict_pattern_detector.py` - ICT pattern generation