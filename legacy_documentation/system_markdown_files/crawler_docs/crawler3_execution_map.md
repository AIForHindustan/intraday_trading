# 🚀 CRAWLER 3 EXECUTION MAP - SSO-XVENUE

## 📋 **OVERVIEW**
- **Name**: SSO-XVENUE (Single-Stock F&O + BSE Cross-Venue)
- **Total Instruments**: 320
- **Active Hours**: 09:05 - 15:35 IST
- **Mode**: WebSocket Binary (FULL mode, 184-byte packets)
- **Architecture**: Raw binary capture only, no processing

## 🗂️ **FILE STRUCTURE & STORAGE**

### **Data Storage Directory**
```
raw_ticks/sso_xvenue/
├── sso_xvenue_20251005_0900.bin  # Hourly rotation
├── sso_xvenue_20251005_1000.bin
├── sso_xvenue_20251005_1100.bin
└── ...
```

### **Binary File Format**
```
Header (12 bytes): [timestamp_ms(8)][length(4)]
Payload: Raw WebSocket binary packet (variable length)
```

## 🔧 **EXECUTION FLOW**

### **1. Initialization**
```python
# Load configuration
config_path = "config/crawlers/crawler3_sso_xvenue/crawler3_sso_xvenue.json"

# Load tokens from ZerodhaConfig
token_data = ZerodhaConfig.get_token_data()
api_key = token_data['api_key']
access_token = token_data['access_token']

# Extract instruments from config
instruments = [inst['token'] for inst in config['instruments'].values()]
instrument_info = {inst['token']: inst for inst in config['instruments'].values()}
```

### **2. WebSocket Connection**
```python
# Connect to Zerodha WebSocket
ws_url = f"wss://ws.kite.trade/?api_key={api_key}&access_token={access_token}"

# Send subscription message
subscribe_msg = {"a": "subscribe", "v": instruments}
ws.send(json.dumps(subscribe_msg))

# Set mode to FULL
mode_msg = {"a": "mode", "v": ["full", instruments]}
ws.send(json.dumps(mode_msg))
```

### **3. Data Capture Process**
```python
def _handle_binary_message(self, binary_data: bytes):
    # 1. Write raw binary to disk immediately
    self._write_raw_binary(binary_data)
    
    # 2. Publish to Redis stream
    self.redis_client.xadd("ticks:raw:binary", {"data": binary_data})
    
    # 3. Parse for statistics (optional)
    ticks = self.parser.parse_websocket_message(binary_data)
```

## 📊 **INSTRUMENT BREAKDOWN**

### **NSE Stock Futures (140 instruments)**
- **Focus**: Single-stock futures (near + next month)
- **Examples**: 360ONE25OCTFUT, ABB25OCTFUT, ADANIENT25OCTFUT
- **Asset Class**: `equity_futures`
- **Price Divisor**: 100

### **NSE Stock Options (120 instruments)**
- **Focus**: Top 60 F&O stocks, ATM ±3 weekly
- **Examples**: ADANIENT25OCT2520CE, ADANIPORTS25OCT1360CE
- **Asset Class**: `equity_options`
- **Price Divisor**: 100

### **BSE Index Derivatives (30 instruments)**
- **Focus**: SENSEX/BANKEX options + futures
- **Examples**: BANKEX25OCTFUT, SENSEX25OCTFUT
- **Asset Class**: `indices` (BSE)
- **Price Divisor**: 100

### **BSE Cash/ETF (30 instruments)**
- **Focus**: SENSEX 30 cash + ETFs
- **Examples**: BAJFINANCE, SBIN, TITAN
- **Asset Class**: `equity_cash` (BSE)
- **Price Divisor**: 100

## 🔍 **DATA FIELDS CAPTURED**

### **FULL Mode (184-byte packets)**
```python
# Core fields for all asset classes
{
    'timestamp': datetime.now(),
    'symbol': 'ADANIENT25OCTFUT',
    'instrument_token': 13356546,
    'asset_class': 'equity_futures',
    'last_price': 2520.50,
    'last_quantity': 100,
    'volume': 1500000,
    'oi': 250000,
    'open': 2510.00,
    'high': 2530.00,
    'low': 2505.00,
    'close': 2520.50,
    'best_bid_price': 2520.00,
    'best_bid_quantity': 500,
    'best_ask_price': 2521.00,
    'best_ask_quantity': 300,
    'underlying': 'ADANIENT',
    'expiry': '2025-10-28',
    'last_trade_time': '2025-10-05T09:30:00',
    'average_price': 2520.25,
    'buy_quantity': 750000,
    'sell_quantity': 750000,
    'net_change': 10.50,
    'lower_circuit_limit': 2268.45,
    'upper_circuit_limit': 2772.55,
    'last_traded_timestamp': '2025-10-05T09:30:00',
    'exchange_timestamp': '2025-10-05T09:30:00',
    'oi_day_high': 300000,
    'oi_day_low': 200000,
    'market_depth': {...},  # 5 levels
    'ohlc': {...},
    'depth': {...}
}
```

## 🔐 **AUTHENTICATION & TOKEN LOADING**

### **Token Source**
```python
# Primary: config/api_configs/zerodha_token.json
{
    "api_key": "v7ettoea5c...",
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
    "api_secret": "secret_key",
    "created_at": "2025-10-05T09:00:00",
    "user_id": "AB1234"
}
```

### **Fallback: Environment Variables**
```bash
export ZERODHA_API_KEY="v7ettoea5c..."
export KITE_ACCESS_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
```

## 📝 **LOGGING & MONITORING**

### **Log File Location**
```
logs/crawler3_sso_xvenue.log
```

### **Log Format**
```
2025-10-05 09:05:00 - INFO - Crawler 3 started
2025-10-05 09:05:01 - INFO - WebSocket connected, subscribing to 320 instruments
2025-10-05 09:05:02 - INFO - Sent subscription and mode setting messages
2025-10-05 09:05:03 - INFO - Opened new binary file: raw_ticks/sso_xvenue/sso_xvenue_20251005_0900.bin
2025-10-05 09:05:04 - INFO - Processed 1000 packets, 184000 bytes
```

### **Statistics Tracking**
- **Packets Processed**: Real-time count
- **Bytes Received**: Total data volume
- **File Rotation**: Hourly binary files
- **Redis Publishing**: Stream "ticks:raw:binary"

## 🚨 **ERROR HANDLING**

### **Connection Issues**
```python
# Auto-reconnection with exponential backoff
def _run_forever(self):
    while not self.connected:
        try:
            self.ws.run_forever(ping_interval=10, ping_timeout=5)
        except Exception as e:
            print(f"WebSocket error: {e}, reconnecting in 5 seconds...")
            time.sleep(5)
```

### **File Writing Errors**
```python
# Graceful handling of disk space issues
try:
    self.current_binary_file.write(header)
    self.current_binary_file.write(binary_data)
    self.current_binary_file.flush()
except Exception as e:
    print(f"Error writing binary data: {e}")
```

## 🔄 **DATA FLOW ARCHITECTURE**

```
Zerodha WebSocket → Binary Client → Raw Binary Files
                                    ↓
                              Redis Stream → Consumer Processes
```

### **No Processing in Crawler**
- ✅ Raw binary capture only
- ✅ Hourly file rotation
- ✅ Redis stream publishing
- ❌ No tick parsing
- ❌ No JSONL files
- ❌ No real-time processing

## 🎯 **EXECUTION COMMAND**

```bash
# Start Crawler 3
python3 -c "
from config.api_configs.zerodha_config import ZerodhaConfig
from config.crawler.binary_websocket_client import ZerodhaBinaryWebSocketClient
import json

# Load config
with open('config/crawlers/crawler3_sso_xvenue/crawler3_sso_xvenue.json') as f:
    config = json.load(f)

# Load tokens
token_data = ZerodhaConfig.get_token_data()
instruments = [inst['token'] for inst in config['instruments'].values()]
instrument_info = {inst['token']: inst for inst in config['instruments'].values()}

# Start crawler
client = ZerodhaBinaryWebSocketClient(
    api_key=token_data['api_key'],
    access_token=token_data['access_token'],
    instruments=instruments,
    instrument_info=instrument_info,
    data_directory='raw_ticks/sso_xvenue'
)
client.connect()
"
```

## ✅ **VERIFICATION CHECKLIST**

- [ ] Config file exists: `config/crawlers/crawler3_sso_xvenue/crawler3_sso_xvenue.json`
- [ ] Token file exists: `config/api_configs/zerodha_token.json`
- [ ] Binary directory created: `raw_ticks/sso_xvenue/`
- [ ] Redis connection working
- [ ] WebSocket connection established
- [ ] 320 instruments subscribed
- [ ] Binary files being created hourly
- [ ] Redis stream publishing data
- [ ] No processing/parsing errors
