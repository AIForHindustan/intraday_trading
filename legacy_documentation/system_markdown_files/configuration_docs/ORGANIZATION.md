# Config Directory Organization

**Last Updated**: October 4, 2025

## Overview

The config directory has been reorganized for better maintainability and clarity. All configuration files are now grouped by their purpose into subdirectories.

## Directory Structure

```
config/
├── api_configs/          # API credentials & connection configs
├── crawlers/             # Crawler configurations & scripts
├── market_data/          # Market reference data
├── schemas/              # Data schemas & field mappings
├── utils/                # Config utilities & helpers
├── sector_data/          # Sector volatility data
├── binary_parser/        # Binary data parsing
├── crawler/              # WebSocket client
└── integrations/         # External integrations
```

## Subdirectory Details

### 1. `api_configs/` - API Credentials & Connection Configs
**Purpose**: All API keys, tokens, and connection configurations

**Files**:
- `zerodha_config.py` - Zerodha API config loader
- `zerodha_token.json` - Zerodha API credentials
- `hdfc_config.json` - HDFC API configuration
- `hdfc_sky_config.py` - HDFC Sky config loader
- `telegram_config.json` - Telegram bot credentials

**Import Path**:
```python
from config.api_configs.zerodha_config import ZerodhaConfig
```

---

### 2. `market_data/` - Market Reference Data
**Purpose**: Static market data and reference configurations

**Files**:
- `market_holidays_2025.json` - Trading holidays calendar
- `nse_sector_mapping.json` - NSE sector classifications
- `manipulation_config.json` - Market manipulation detection rules

**Usage**:
```python
import json
with open('config/market_data/market_holidays_2025.json') as f:
    holidays = json.load(f)
```

---

### 3. `schemas/` - Data Schemas & Field Mappings
**Purpose**: Data structure definitions and field mappings

**Files**:
- `unified_schema.py` - Unified tick data schema
- `data_definitions.py` - Data type definitions
- `asset_class_schema.py` - Asset class schemas
- `asset_class_schemas.py` - Extended asset schemas
- `asset_specific_schema.py` - Specific asset schemas
- `data_patterns.py` - Data processing patterns
- `field_map.yaml` - Field mapping configuration
- `optimized_field_mapping.yaml` - Optimized mappings

**Import Path**:
```python
from config.schemas.unified_schema import map_kite_to_unified
from config.schemas.data_definitions import get_data_definitions
```

---

### 4. `utils/` - Config Utilities
**Purpose**: Helper utilities for configuration management

**Files**:
- `holiday_aware_expiry.py` - Expiry date calculations with holidays
- `holiday_pattern_integration.py` - Holiday pattern detection
- `holiday_utils.py` - Holiday utility functions
- `crawlers/gift_nifty_gap.py` - GIFT Nifty gap calculations
- `sector_volatility_analyzer.py` - Sector volatility analysis

**Usage**:
```python
from config.utils.holiday_utils import is_trading_day
```

---

### 5. `crawlers/` - Crawler Configurations
**Purpose**: All crawler-related configurations and scripts

**Subdirectories**:
- `binary_crawler1/` - Main binary crawler config
- `crawler2_volatility/` - Volatility & extended-hours crawler
- `crawler3_sso_xvenue/` - Single-stock F&O + BSE crawler

**Files**:
- `crawler_config.py` - Base crawler configuration
- `crawler_token_loader.py` - Token loader for crawlers
- `millisecond_crawler_config.py` - Millisecond crawler config
- `total_crawler_config.py` - Total market crawler config
- `total_market_data_crawler.py` - Total market crawler script
- `hdfc_websocket_crawler.json` - HDFC crawler config

**Import Path**:
```python
from config.crawlers.crawler_token_loader import load_crawler_tokens
```

---

### 6. `sector_data/` - Sector Volatility Data
**Purpose**: Sector-specific volume and volatility data

**Files**:
- `sector_volatility.json` - Sector volatility metrics
- `README.md` - Documentation

**Used By**: `core/utils/calculations.py` for volume normalization

---

## Migration Guide

### Old Import Paths → New Import Paths

| Old | New |
|-----|-----|
| `from config.zerodha_config import ...` | `from config.api_configs.zerodha_config import ...` |
| `from config.unified_schema import ...` | `from config.schemas.unified_schema import ...` |
| `from config.data_definitions import ...` | `from config.schemas.data_definitions import ...` |
| `from config.hdfc_sky_config import ...` | `from config.api_configs.hdfc_sky_config import ...` |

### File Paths

| Old | New |
|-----|-----|
| `config/zerodha_token.json` | `config/api_configs/zerodha_token.json` |
| `config/market_holidays_2025.json` | `config/market_data/market_holidays_2025.json` |
| `config/field_map.yaml` | `config/schemas/field_map.yaml` |

---

## Updated Files

The following files have been updated with new import paths:

✅ **Core Utilities**:
- `core/volume_utils/daily_volume_updater.py`
- `core/utils/update_all_20day_averages.py`

✅ **Scanners**:
- `scanner/data_pipeline.py`
- `scanner/production/main.py`
- `scanner/enhanced_spoofing_detector.py`

✅ **Config Files**:
- `config/api_configs/zerodha_config.py`
- `config/schemas/data_patterns.py`
- `config/integrations/hdfc_sky_websocket_crawler.py`
- `config/crawlers/total_market_data_crawler.py`

---

## Benefits

### Before
```
config/
├── zerodha_config.py
├── zerodha_token.json
├── hdfc_config.json
├── telegram_config.json
├── market_holidays_2025.json
├── unified_schema.py
├── data_definitions.py
├── crawler_config.py
├── ... (30+ files in root)
```

### After
```
config/
├── api_configs/        (5 files)
├── market_data/        (3 files)
├── schemas/            (8 files)
├── utils/              (5 files)
├── crawlers/           (organized)
└── ... (only 4 files in root)
```

### Improvements

✅ **Cleaner Root**: Config root now only has essential directories  
✅ **Logical Grouping**: Files grouped by purpose  
✅ **Easier Navigation**: Find files by category  
✅ **Better Maintainability**: Clear organization structure  
✅ **Scalable**: Easy to add new configs in appropriate directories  

---

## Root Level Files

Only essential files remain in the config root:

- `__init__.py` - Package initialization
- `README.md` - Main config documentation
- `INSTRUMENT_MASTER_README.md` - Instrument master docs
- `ORGANIZATION.md` - This file
- `master_token_mapping.json` - Symlink to master token mapping

---

## Testing

All imports have been verified working:

```bash
cd /Users/apple/Desktop/intraday_trading

# Test imports
python3 -c "
from config.api_configs.zerodha_config import ZerodhaConfig
from config.schemas.unified_schema import map_kite_to_unified
from config.schemas.data_definitions import get_data_definitions
print('✅ All imports working!')
"
```

---

## Future Organization

When adding new files:

- **API credentials** → `api_configs/`
- **Market data** → `market_data/`
- **Schemas** → `schemas/`
- **Utilities** → `utils/`
- **Crawler configs** → `crawlers/`

---

**For questions or issues, see individual README files in each subdirectory.**

