# Instrument Master Configuration System

## Overview
This system provides a centralized way to manage instrument data for the millisecond crawler using a single JSON file instead of hardcoded lists.

## Files

### 1. `millisecond_master.json`
- **Purpose**: Stores all instrument data including tokens, symbols, and metadata
- **Structure**:
  ```json
  {
    "metadata": { ... },
    "instruments": {
      "equity": { "SYMBOL": { token, name, etc. } },
      "futures": { ... },
      "options": { ... },
      "indices": { ... }
    },
    "categories": {
      "focus_stocks": [...],
      "sectors": { ... }
    }
  }
  ```

### 2. `instrument_master_loader.py`
- **Purpose**: Python module to load and access instrument data
- **Usage**:
  ```python
  from config.instrument_master_loader import get_equity_instruments, get_instrument_token

  equities = get_equity_instruments()
  token = get_instrument_token("NSE:RELIANCE")
  ```

### 3. `update_instrument_master.py`
- **Purpose**: Utility script to fetch fresh data from Zerodha API
- **Usage**: `python config/update_instrument_master.py`

## How to Use

### Initial Setup
1. **Update the master file** with your desired instruments:
   ```bash
   python config/update_instrument_master.py
   ```

2. **Edit categories** in `millisecond_master.json`:
   ```json
   "categories": {
     "focus_stocks": ["RELIANCE", "TCS", "HDFCBANK"],
     "sectors": {
       "IT": ["TCS", "INFY"],
       "Banking": ["HDFCBANK", "ICICIBANK"]
     }
   }
   ```

### Integration with Crawler
The `millisecond_crawler_config.py` automatically loads instruments from the master file:

```python
# Automatically loads from millisecond_master.json
from config.instrument_master_loader import get_all_fno_instruments
NFO_OPTIONS_FUTURES = get_all_fno_instruments()
```

## Benefits

### ✅ **Single Source of Truth**
- All instrument data in one JSON file
- No more hardcoded lists scattered across config files

### ✅ **Easy Updates**
- Run `update_instrument_master.py` to fetch latest data from Zerodha
- Edit JSON file directly for customizations

### ✅ **Token Management**
- Automatic token lookup for any instrument
- No need to manually maintain token numbers

### ✅ **Flexible Categories**
- Group instruments by sectors, volatility, volume, etc.
- Easy to create custom watchlists

## API Reference

### InstrumentMasterLoader Functions
- `get_equity_instruments()` - List of equity symbols
- `get_futures_instruments()` - List of futures symbols
- `get_options_instruments()` - List of options symbols
- `get_indices_instruments()` - List of index symbols
- `get_focus_stocks()` - Focus stocks from categories
- `get_instrument_token(symbol)` - Get token for symbol
- `get_instrument_info(symbol)` - Get full instrument details

## Migration from Old System

### Before (Broken)
```python
# This import fails
from config.nfo_options_config import ALL_NFO_INSTRUMENTS
```

### After (Working)
```python
# This works automatically
from config.instrument_master_loader import get_all_fno_instruments
NFO_OPTIONS_FUTURES = get_all_fno_instruments()
```

## Troubleshooting

### Master File Not Found
- Run `python config/update_instrument_master.py` to create it
- Check file permissions

### Kite API Connection Issues
- Ensure your Zerodha credentials are set up
- Check `zerodha_config.py` for authentication

### Empty Instrument Lists
- Verify `millisecond_master.json` has data
- Check the JSON structure matches expected format

## Future Enhancements

- **Auto-update**: Schedule regular updates from Zerodha API
- **Filtering**: Add filters for active/expired instruments
- **Validation**: Check for duplicate tokens/symbols
- **Backup**: Automatic backup of master file before updates
