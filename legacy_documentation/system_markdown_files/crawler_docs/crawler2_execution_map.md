# 🚀 CRAWLER 2 EXECUTION MAP - VOLATILITY & EXTENDED-HOURS

## 📋 **OVERVIEW**
- **Name**: Volatility & Extended-Hours (Data Mining)
- **Total Instruments**: 1,498
- **Active Hours**: 08:55 - 20:00 IST (Extended hours for Currency & Cross-asset)
- **Mode**: WebSocket Binary (FULL mode, 184-byte packets)
- **Architecture**: Raw binary capture only, no processing
- **Focus**: Index Options, Currency Derivatives, IRD, High-liquidity ETFs

## 🗂️ **FILE STRUCTURE & STORAGE**

### **Data Storage Directory**
```
raw_ticks/crawler2_volatility/
├── volatility_ticks_20251005_0855.bin  # Hourly rotation
├── volatility_ticks_20251005_0955.bin
├── volatility_ticks_20251005_1055.bin
├── volatility_ticks_20251005_1155.bin
├── volatility_ticks_20251005_1255.bin
├── volatility_ticks_20251005_1355.bin
├── volatility_ticks_20251005_1455.bin
├── volatility_ticks_20251005_1555.bin
├── volatility_ticks_20251005_1655.bin
├── volatility_ticks_20251005_1755.bin
├── volatility_ticks_20251005_1855.bin
└── volatility_ticks_20251005_1955.bin
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
config_path = "config/crawlers/crawler2_volatility/crawler2_volatility.json"

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
    
    # 2. NO Redis publishing (raw binary capture only)
    # 3. Parse for statistics (optional)
    ticks = self.parser.parse_websocket_message(binary_data)
```

## 📊 **INSTRUMENT BREAKDOWN**

### **NSE Index Options (276 CE + 280 PE = 556 total)**
- **Focus**: NIFTY/BANKNIFTY/FINNIFTY ATM ±10 strikes
- **Expiry**: Current week + Current month (auto-rollover 2 days before expiry)
- **Examples**: NIFTY25OCT24000CE, BANKNIFTY25OCT52000PE
- **Asset Class**: `equity_options`
- **Price Divisor**: 100

### **NSE Stock Futures (217 instruments)**
- **Focus**: Single-stock futures (nearest expiry only)
- **Examples**: RELIANCE25OCTFUT, TCS25OCTFUT
- **Asset Class**: `equity_futures`
- **Price Divisor**: 100

### **Currency Derivatives (157 instruments)**
- **Focus**: USDINR, EURINR, GBPINR, JPYINR (current week + monthly expiry)
- **Examples**: USDINR25OCTFUT, EURINR25OCTCE
- **Asset Class**: `currency_futures`
- **Price Divisor**: 10,000,000 (4 decimal places)

### **BSE Cash Stocks (217 instruments)**
- **Focus**: SENSEX 30 + other liquid stocks
- **Examples**: RELIANCE, TCS, HDFCBANK, INFY
- **Asset Class**: `equity_cash`
- **Price Divisor**: 100

### **BSE Index Derivatives (109 instruments)**
- **Focus**: SENSEX/BANKEX options + futures
- **Examples**: SENSEX25OCTFUT, BANKEX25OCTCE
- **Asset Class**: `indices`
- **Price Divisor**: 100

### **High-Liquidity ETFs (10 instruments)**
- **Focus**: NIFTYBEES, BANKBEES, LIQUID, etc.
- **Examples**: NIFTYBEES, BANKBEES, LIQUIDBEES
- **Asset Class**: `etf`
- **Price Divisor**: 100

### **MCX Commodities (10 instruments)**
- **Focus**: Gold, Silver, Crude Oil futures
- **Examples**: GOLD25OCTFUT, SILVER25OCTFUT
- **Asset Class**: `commodity_futures`
- **Price Divisor**: 100

## 🔍 **DATA FIELDS CAPTURED**

### **FULL Mode (184-byte packets)**
```python
# Core fields for all asset classes
{
    'timestamp': datetime.now(),
    'symbol': 'NIFTY25OCT24000CE',
    'instrument_token': 12345678,
    'asset_class': 'equity_options',
    'last_price': 150.50,
    'last_quantity': 100,
    'volume': 500000,
    'oi': 250000,
    'open': 145.00,
    'high': 155.00,
    'low': 140.00,
    'close': 150.50,
    'best_bid_price': 150.00,
    'best_bid_quantity': 500,
    'best_ask_price': 151.00,
    'best_ask_quantity': 300,
    'underlying': 'NIFTY',
    'expiry': '2025-10-28',
    'strike_price': 24000,
    'option_type': 'CE',
    'last_trade_time': '2025-10-05T09:30:00',
    'average_price': 150.25,
    'buy_quantity': 250000,
    'sell_quantity': 250000,
    'net_change': 5.50,
    'lower_circuit_limit': 135.45,
    'upper_circuit_limit': 165.55,
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
logs/crawler2_volatility.log
```

### **Log Format**
```
2025-10-05 08:55:00 - INFO - Crawler 2 started
2025-10-05 08:55:01 - INFO - WebSocket connected, subscribing to 1498 instruments
2025-10-05 08:55:02 - INFO - Sent subscription and mode setting messages
2025-10-05 08:55:03 - INFO - Opened new binary file: raw_ticks/crawler2_volatility/volatility_ticks_20251005_0855.bin
2025-10-05 08:55:04 - INFO - Processed 1000 packets, 184000 bytes
2025-10-05 20:00:00 - INFO - Market closed, stopping crawler
```

### **Statistics Tracking**
- **Packets Processed**: Real-time count
- **Bytes Received**: Total data volume
- **File Rotation**: Hourly binary files
- **NO Redis Publishing**: Raw binary capture only

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
                              NO Redis Stream (raw capture only)
                                    ↓
                              Consumer Processes (offline)
```

### **No Processing in Crawler**
- ✅ Raw binary capture only
- ✅ Hourly file rotation
- ❌ NO Redis stream publishing
- ❌ No tick parsing
- ❌ No JSONL files
- ❌ No real-time processing

## 🎯 **EXECUTION COMMAND**

```bash
# Start Crawler 2
python3 -c "
from config.api_configs.zerodha_config import ZerodhaConfig
from config.crawler.binary_websocket_client import ZerodhaBinaryWebSocketClient
import json

# Load config
with open('config/crawlers/crawler2_volatility/crawler2_volatility.json') as f:
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
    data_directory='raw_ticks/crawler2_volatility'
)
client.connect()
"
```

## ⏰ **ACTIVE HOURS BREAKDOWN**

### **08:55 - 09:15**: Pre-market
- **Focus**: Index options, currency futures
- **Data**: Opening prices, pre-market activity

### **09:15 - 15:30**: Regular Market Hours
- **Focus**: All instruments (NSE, BSE, Currency, Commodities)
- **Data**: Full market depth, volume, OI

### **15:30 - 19:30**: Extended Hours
- **Focus**: Currency derivatives, cross-currency pairs
- **Data**: USDINR, EURINR, GBPINR, JPYINR activity

### **19:30 - 20:00**: Post-market
- **Focus**: Currency derivatives, international markets
- **Data**: Closing prices, final settlements

## ✅ **VERIFICATION CHECKLIST**

- [ ] Config file exists: `config/crawlers/crawler2_volatility/crawler2_volatility.json`
- [ ] Token file exists: `config/api_configs/zerodha_token.json`
- [ ] Binary directory created: `raw_ticks/crawler2_volatility/`
- [ ] WebSocket connection established
- [ ] 1,498 instruments subscribed
- [ ] Binary files being created hourly
- [ ] NO Redis stream publishing (raw capture only)
- [ ] No processing/parsing errors
- [ ] Extended hours coverage (08:55-20:00)
