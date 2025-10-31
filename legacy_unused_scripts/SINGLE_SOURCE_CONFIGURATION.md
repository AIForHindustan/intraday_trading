# Single Source Configuration - @NSEAlgoTrading

## 📡 **TELEGRAM CHANNEL SETUP**

### **Channel Details:**
- **Channel**: `@NSEAlgoTrading`
- **Purpose**: Single source of truth for all algorithmic trading alerts
- **Bot Token**: `7586453352:AAGuAnng91UTtG1081FwSzYOhO3rwIrvvCA`

### **Configuration Updates:**

#### 1. **Telegram Config** (`config/telegram_config.json`)
```json
{
  "bot_token": "7586453352:AAGuAnng91UTtG1081FwSzYOhO3rwIrvvCA",
  "chat_ids": ["@NSEAlgoTrading"],
  "single_source": true,
  "channel_name": "NSE Algo Trading",
  "description": "Single source of truth for algorithmic trading alerts"
}
```

#### 2. **Community Bot** (`community_bots/telegram_bot.py`)
```python
'channels': {
    'main_signals': '@NSEAlgoTrading',
    'news_alerts': '@NSEAlgoTrading'
}
```

#### 3. **Redis Notifier** (`alerts/notifiers.py`)
```python
self.CHANNEL_STRUCTURE = {
    'main_signals': 'NSEAlgoTrading',           # Single source for all alerts
    'news_alerts': 'NSEAlgoTrading',           # Same channel for news
}
```

## 🎯 **SINGLE SOURCE BENEFITS**

### **1. Centralized Alert Distribution**
- All alerts go to one channel: `@NSEAlgoTrading`
- No duplicate messages across multiple channels
- Consistent message formatting

### **2. Simplified Management**
- One channel to monitor
- Single point of configuration
- Easier to maintain and update

### **3. Better User Experience**
- Users subscribe to one channel
- No confusion about which channel to follow
- All content in one place

## 🔧 **NEXT STEPS**

### **1. Bot Setup**
1. Add the bot to `@NSEAlgoTrading` channel
2. Give bot admin permissions to post messages
3. Test the bot connection

### **2. Channel Configuration**
1. Set up channel description
2. Configure channel settings
3. Add channel rules and guidelines

### **3. Testing**
1. Send test alert to verify single source
2. Confirm no duplicate messages
3. Validate alert formatting

## 📊 **ALERT TYPES**

All alerts will be published to `@NSEAlgoTrading`:

- **Pattern Alerts**: Volume spikes, breakouts, reversals
- **News Alerts**: Market news with sentiment analysis
- **Educational Content**: Trading education and insights
- **Performance Updates**: System performance metrics

## ✅ **CONFIGURATION COMPLETE**

The system is now configured for **single source publishing** to `@NSEAlgoTrading` channel. All alerts, news, and educational content will be centralized in this one channel.
