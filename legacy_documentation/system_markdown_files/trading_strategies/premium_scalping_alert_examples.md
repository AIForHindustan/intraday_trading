# Premium Scalping Alert Templates - How They Will Appear

## 🎯 Alert Display Formats

### 1. macOS Notifications (Clean & Simple)
```
Title: RELIANCE - High Gamma Scalping
Subtitle: BUY
Message: Alert Time: 14:30 IST
```

### 2. Telegram Alerts (Detailed)
```
📈 BUY RELIANCE @ ₹2,500.00

📊 Pattern: ⚡ High Gamma Scalping
💡 What's happening: ATM option with high gamma - rapid premium moves expected
🔍 Why it matters: SCALP NOW: High gamma = fast premium changes, quick profit opportunity

🔥 Confidence: 75% (Very High Confidence)

💰 Current: ₹2,500.00
🎯 Target: ₹2,550.00 (+2.00%)
🛑 Stop Loss: ₹2,475.00

⚡ Trading Instruction: Enter immediately, target 2% premium move in 1-5 minutes
📊 Volume: 2.3x average
⏰ Timeframe: 1-5min
🎯 Risk Level: MEDIUM
```

### 3. Console Logs (Detailed)
```
[14:30:15] 🎯 PREMIUM SCALPING ALERT: RELIANCE
Pattern: high_gamma_scalping | Action: BUY | Confidence: 75%
Current: ₹2,500.00 | Target: ₹2,550.00 (+2.00%) | Stop: ₹2,475.00
Description: ATM option with high gamma - rapid premium moves expected
Trading: Enter immediately, target 2% premium move in 1-5 minutes
```

## 🎯 All 6 Premium Scalping Patterns

### 1. High Gamma Scalping ⚡
- **Trigger**: ATM options (delta 0.45-0.55) with gamma > 0.1
- **Action**: BUY (scalp premium moves)
- **Timeframe**: 1-5 minutes
- **Expected Move**: 2% premium move
- **Risk Level**: MEDIUM
- **Urgency**: HIGH

### 2. Theta Decay Scalping ⏰
- **Trigger**: ≤3 days to expiry with high theta > 0.05
- **Action**: SELL (sell premium for time decay)
- **Timeframe**: Intraday
- **Expected Move**: Time decay profit
- **Risk Level**: HIGH
- **Urgency**: HIGH

### 3. IV Crush Scalping 💥
- **Trigger**: Post-event IV > 40% expecting crush
- **Action**: SELL (sell volatility)
- **Timeframe**: 15-30 minutes
- **Expected Move**: 15% IV crush
- **Risk Level**: MEDIUM
- **Urgency**: MEDIUM

### 4. Premium Mean Reversion 📊
- **Trigger**: Premium 20% above historical average
- **Action**: SELL (sell overpriced premium)
- **Timeframe**: 30min-2hr
- **Expected Move**: 8% mean reversion
- **Risk Level**: MEDIUM
- **Urgency**: MEDIUM

### 5. Delta-Neutral Scalping 🎯
- **Trigger**: Low delta (<0.2) with high gamma > 0.08
- **Action**: BUY (gamma-driven moves)
- **Timeframe**: 5-15 minutes
- **Expected Move**: 1.5% gamma move
- **Risk Level**: LOW
- **Urgency**: MEDIUM

### 6. Vega Scalping 🌊
- **Trigger**: High vega > 0.1 with volatility event expected
- **Action**: BUY (volatility expansion)
- **Timeframe**: 1-4 hours
- **Expected Move**: 10% vega impact
- **Risk Level**: HIGH
- **Urgency**: HIGH

## 🎯 Alert Data Structure

Each premium scalping alert will contain:
```json
{
  "symbol": "RELIANCE",
  "pattern": "high_gamma_scalping",
  "confidence": 0.75,
  "action": "BUY",
  "last_price": 2500.00,
  "premium": 2500.00,
  "underlying_price": 2500.00,
  "strike_price": 2500.00,
  "implied_volatility": 0.25,
  "delta": 0.50,
  "gamma": 0.15,
  "theta": -0.05,
  "vega": 0.10,
  "days_to_expiry": 5,
  "timeframe": "1-5min",
  "risk_level": "MEDIUM",
  "scalping_type": "premium",
  "description": "High Gamma 0.150 for ATM option",
  "expected_move": 0.02,
  "volume_ratio": 2.3,
  "timestamp": "2025-09-30T14:30:15.123456"
}
```

## 🎯 Key Features

### Pattern-Specific Information:
- **Greeks Data**: Delta, Gamma, Theta, Vega
- **Options Metrics**: Premium, Strike, IV, Days to Expiry
- **Risk Management**: Risk level, Timeframe, Position sizing
- **Trading Instructions**: Specific entry/exit guidance

### Alert Customization:
- **macOS**: Clean symbol-pattern-action format
- **Telegram**: Rich formatting with emojis and details
- **Console**: Comprehensive logging for debugging

### Risk-Based Filtering:
- **Premium Filter**: Minimum ₹0.50 premium
- **Volume Filter**: Minimum 0.8x volume ratio
- **Time Filter**: Far OTM restrictions for short-term patterns

## 🎯 Integration Status

✅ **Pattern Detection**: Fully integrated in `pattern_detector.py`
✅ **Alert Templates**: Added to `alert_manager.py`
✅ **Helper Methods**: All 6 helper functions implemented
✅ **Risk Management**: Position sizing and filtering complete
✅ **Error Handling**: Robust exception handling
✅ **Logging**: Comprehensive debug and error logging

The premium scalping patterns are now fully integrated and ready to generate professional-grade options trading alerts! 🚀
