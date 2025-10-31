#!/usr/bin/env python3
"""
CRAWLER CONFIGURATION - INTRADAY TRADING SYSTEM
===============================================

Configuration for millisecond and total market crawlers.
Defines file paths, instrument lists, and settings for data collection.
"""

import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import re

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent  # intraday_trading directory
RAW_TICKS_DIR = PROJECT_ROOT / "raw_ticks"

# Directory paths (created lazily when needed)
MILLISECOND_DIR = RAW_TICKS_DIR / "millisecond_crawler"
TOTAL_MARKET_DIR = RAW_TICKS_DIR / "total_market_crawler"

def ensure_millisecond_dirs():
    """Ensure millisecond crawler directories exist"""
    RAW_TICKS_DIR.mkdir(exist_ok=True)
    MILLISECOND_DIR.mkdir(exist_ok=True)
    print(f"✅ Millisecond crawler directories created:")
    print(f"  Raw ticks: {RAW_TICKS_DIR}")
    print(f"  Millisecond: {MILLISECOND_DIR}")

def ensure_total_market_dirs():
    """Ensure total market crawler directories exist"""
    RAW_TICKS_DIR.mkdir(exist_ok=True)
    TOTAL_MARKET_DIR.mkdir(exist_ok=True)
    print(f"✅ Total market crawler directories created:")
    print(f"  Raw ticks: {RAW_TICKS_DIR}")
    print(f"  Total Market: {TOTAL_MARKET_DIR}")

class FilePaths:
    """File path configuration for crawlers - DAT files only"""
    
    def __init__(self):
        self.base_dir = RAW_TICKS_DIR

    def get_intraday_data_file(self) -> str:
        """Get intraday data file path (DAT format)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(RAW_TICKS_DIR / f"intraday_data_{timestamp}.dat")

    def get_data_mining_file(self) -> str:
        """Get data mining file path (DAT format)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return str(RAW_TICKS_DIR / f"data_mining_{timestamp}.dat")

    def get_research_data_file(self, session_id: str) -> str:
        """Get research data file path (DAT format)"""
        return str(RAW_TICKS_DIR / session_id / "research_data.dat")

    @property
    def websocket_status_file(self) -> str:
        """Get websocket status log file path"""
        date_str = datetime.now().strftime("%Y%m%d")
        crawler_log_dir = PROJECT_ROOT / "logs" / f"crawler_{date_str}"
        crawler_log_dir.mkdir(exist_ok=True)
        return str(crawler_log_dir / "websocket_status.log")

# Initialize file paths
FILE_PATHS = FilePaths()

class Settings:
    """Crawler settings"""
    RECONNECT_BASE_DELAY = 1
    RECONNECT_MAX_DELAY = 60
    MAX_RECONNECT_ATTEMPTS = 10
    TICK_BUFFER_SIZE = 1000
    WRITE_INTERVAL = 1  # seconds
    ORDER_WINDOW_MS = 100  # Order tracking window in milliseconds
    # Threshold for flagging very large orders in depth (number of shares)
    VERY_LARGE_ORDER_THRESHOLD = 10000

class TokenConfig:
    """Token configuration"""
    def __init__(self):
        # Use consistent environment variable names with zerodha_config.py
        self.api_key = os.getenv('ZERODHA_API_KEY', '')
        self.api_secret = os.getenv('ZERODHA_API_SECRET', '')
        self.access_token = os.getenv('ZERODHA_ACCESS_TOKEN', '')

    def get_token_data(self):
        """Load token data from zerodha_token.json - CONSISTENT with zerodha_config.py"""
        try:
            # Use the same path as zerodha_config.py
            token_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'zerodha_token.json')
            with open(token_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Failed to load token data from file: {e}")
            print("Please run 'python config/zerodha_config.py login' to generate token file.")
            raise  # Surface the real error instead of returning None

# Sample instrument lists (you can expand these)
ALL_MARKET_INSTRUMENTS = [
    {"instrument_token": 256265, "tradingsymbol": "RELIANCE", "name": "Reliance Industries", "exchange": "NSE"},
    {"instrument_token": 738561, "tradingsymbol": "TCS", "name": "Tata Consultancy Services", "exchange": "NSE"},
    {"instrument_token": 2953217, "tradingsymbol": "HDFCBANK", "name": "HDFC Bank", "exchange": "NSE"},
    {"instrument_token": 3045, "tradingsymbol": "NIFTY50", "name": "NIFTY 50", "exchange": "NSE"},
]

SCANNER_FOCUS_STOCKS = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK"]
FOCUS_INSTRUMENT_TOKENS = [256265, 738561, 2953217]  # Corresponding tokens
ALL_MARKET_TOKENS = [256265, 738561, 2953217, 3045]
HIGH_PRIORITY_STOCKS = ["RELIANCE", "TCS"]

SECTOR_MAPPING = {
    "RELIANCE": "Energy",
    "TCS": "IT",
    "HDFCBANK": "Banking",
    "INFY": "IT",
    "ICICIBANK": "Banking",
    "NIFTY50": "Index"
}

SETTINGS = Settings()
TokenConfig = TokenConfig()

# ---- Cached metadata loader (symbols -> exchange/instrument_type) ----
_VOLUME_META_CACHE: Optional[Dict[str, Dict[str, Any]]] = None

def _get_current_and_next_month() -> Dict[str, str]:
    """Get current month and next month in the format used by F&O symbols"""
    now = datetime.now()
    current_month = now.strftime("%b").upper()  # e.g., "OCT"
    current_year = now.strftime("%y")  # e.g., "25"
    
    # Next month
    next_month_date = now.replace(day=1) + timedelta(days=32)
    next_month = next_month_date.strftime("%b").upper()
    next_year = next_month_date.strftime("%y")
    
    # F&O expiry dates for 2025 (last Thursday of each month)
    fno_expiry_dates = {
        'JAN': '30', 'FEB': '27', 'MAR': '27', 'APR': '24', 'MAY': '29', 'JUN': '26',
        'JUL': '31', 'AUG': '28', 'SEP': '25', 'OCT': '28', 'NOV': '28', 'DEC': '26'
    }
    
    # Use actual F&O expiry dates instead of current date
    current_expiry_day = fno_expiry_dates.get(current_month, '28')
    next_expiry_day = fno_expiry_dates.get(next_month, '25')
    
    return {
        'current': f"{current_year}{current_month}",  # e.g., "25OCT" -> "28OCT"
        'next': f"{next_year}{next_month}",           # e.g., "25NOV" -> "25NOV"
        'current_expiry': f"{current_year}{current_month}{current_expiry_day}",  # e.g., "28OCT"
        'next_expiry': f"{next_year}{next_month}{next_expiry_day}",              # e.g., "25NOV"
    }

def _is_fno_current_or_next_month(symbol: str) -> bool:
    """Check if F&O symbol is for current or next month"""
    months = _get_current_and_next_month()
    # Check for both old format (25OCT) and correct format (28OCT)
    return (months['current'] in symbol or months['next'] in symbol or 
            months['current_expiry'] in symbol or months['next_expiry'] in symbol)

def _is_gold_or_silver_bullion(symbol: str) -> bool:
    """Check if commodity symbol is Gold or Silver bullion (for millisecond crawler)"""
    clean_symbol = symbol.upper()
    # Remove exchange prefix if present
    if ':' in clean_symbol:
        clean_symbol = clean_symbol.split(':', 1)[1]
    # Check for Gold and Silver bullion patterns
    return (clean_symbol.startswith('GOLD') and 'FUT' in clean_symbol) or \
           (clean_symbol.startswith('SILVER') and 'FUT' in clean_symbol)

def _load_volume_meta() -> Dict[str, Dict[str, Any]]:
    global _VOLUME_META_CACHE
    if _VOLUME_META_CACHE is not None:
        return _VOLUME_META_CACHE
    meta: Dict[str, Dict[str, Any]] = {}
    try:
        volume_file = PROJECT_ROOT / "config" / "volume_averages_20d.json"
        if volume_file.exists():
            data = json.loads(volume_file.read_text())
            # Normalize keys to un-prefixed symbols (e.g., 'NSE:RELIANCE' -> 'RELIANCE')
            for k, v in data.items():
                clean = k.split(":", 1)[-1].upper()
                # Keep the last seen (NSE over NFO if duplicates), but store exchange and type
                meta[clean] = {
                    "exchange": v.get("exchange"),
                    "instrument_type": v.get("instrument_type"),
                }
    except Exception as e:
        print(f"⚠️ Could not load volume_averages_20d.json for metadata: {e}")
    _VOLUME_META_CACHE = meta
    return meta

def get_instrument_token_mapping() -> Dict[str, int]:
    """Get mapping of trading symbols to instrument tokens for millisecond crawler with dynamic monthly F&O filtering and NIFTY 50 price segregation"""
    try:
        # Load NIFTY 50 segregation data
        segregation_file = PROJECT_ROOT / "config" / "nifty_50_segregation.json"
        expensive_nifty_50 = set()
        if segregation_file.exists():
            with open(segregation_file, 'r') as f:
                segregation_data = json.load(f)
            expensive_nifty_50 = set(segregation_data['nifty_50_segregation']['total_crawler']['symbols'])
        
        # Load volume data to get the list of instruments the crawler should monitor
        volume_file = PROJECT_ROOT / "config" / "volume_averages_20d.json"
        if volume_file.exists():
            with open(volume_file, 'r') as f:
                volume_data = json.load(f)
            
            # Get symbols from volume data with dynamic monthly filtering for F&O and Gold/Silver only for commodities
            target_symbols = set()
            fno_count = 0
            equity_count = 0
            commodity_count = 0
            excluded_expensive = 0
            
            for symbol_key in volume_data.keys():
                # Always include NSE equities, but exclude expensive NIFTY 50 stocks
                if symbol_key.startswith('NSE:') and not any(x in symbol_key for x in ['CE', 'PE', 'FUT', 'BEES', 'ETF']):
                    symbol = symbol_key.replace('NSE:', '')
                    # Exclude expensive NIFTY 50 stocks (>= ₹4,000)
                    if symbol in expensive_nifty_50:
                        excluded_expensive += 1
                        continue
                    target_symbols.add(symbol)
                    equity_count += 1
                # For MCX commodities, only include Gold and Silver bullion
                elif symbol_key.startswith('MCX:') and _is_gold_or_silver_bullion(symbol_key):
                    symbol = symbol_key.replace('MCX:', '')
                    target_symbols.add(symbol)
                    commodity_count += 1
                # For F&O, only include current and next month
                elif symbol_key.startswith('NFO:') and _is_fno_current_or_next_month(symbol_key):
                    symbol = symbol_key.replace('NFO:', '')
                    target_symbols.add(symbol)
                    fno_count += 1
            
            # Load comprehensive token mapping
            mapping_file = PROJECT_ROOT / "core" / "data" / "token_lookup.json"
            if mapping_file.exists():
                with open(mapping_file, 'r') as f:
                    token_data = json.load(f)
                
                # Build mapping only for symbols that are in our target list
                mapping = {}
                for key, data in token_data.items():
                    token = data.get('token')
                    symbol = data.get('symbol', key.split(':', 1)[-1] if ':' in key else key)
                    
                    # Check if this symbol is in our target list
                    if symbol in target_symbols and token:
                        mapping[symbol] = token
                
                if mapping:
                    months = _get_current_and_next_month()
                    print(f"✅ Loaded {len(mapping)} instruments with dynamic monthly F&O filtering and NIFTY 50 price segregation")
                    print(f"   • NSE Equities: {equity_count}")
                    print(f"   • F&O Derivatives (current/next month): {fno_count}")
                    print(f"   • MCX Commodities (Gold/Silver only): {commodity_count}")
                    print(f"   • Excluded expensive NIFTY 50 (>= ₹4,000): {excluded_expensive}")
                    print(f"   • Current month: {months['current']}, Next month: {months['next']}")
                    return mapping
    except Exception as e:
        print(f"⚠️ Could not load instrument mappings: {e}")
    
    # Final fallback to the static list
    return {instr["tradingsymbol"]: instr["instrument_token"] for instr in ALL_MARKET_INSTRUMENTS}

def get_stock_sector(symbol: str) -> str:
    """Get sector for a stock symbol"""
    return SECTOR_MAPPING.get(symbol, "Unknown")

def validate_millisecond_configuration() -> bool:
    """Validate millisecond crawler configuration"""
    ensure_millisecond_dirs()
    return True

def validate_total_market_configuration() -> bool:
    """Validate total market crawler configuration"""
    ensure_total_market_dirs()
    return True

def validate_configuration() -> bool:
    """Legacy function - validates both configurations"""
    ensure_millisecond_dirs()
    ensure_total_market_dirs()
    return True

def get_asset_class(symbol: str) -> dict:
    """Return asset classification for a tradingsymbol.

    Asset classes: equity, futures, options, commodity, currency, index, etf, debt.
    Category: sector for equities/derivatives when known; otherwise a generic category.
    """
    # Clean any exchange prefix if present
    clean = str(symbol).split(":", 1)[-1].upper()

    # Load metadata (exchange/instrument_type) if available
    meta = _load_volume_meta().get(clean, {})
    exch = (meta.get("exchange") or "").upper()
    inst_type = (meta.get("instrument_type") or "").upper()

    # Quick derivative detection by naming convention
    is_option = clean.endswith("CE") or clean.endswith("PE") or " OPT" in clean or "-OPT" in clean
    is_future = ("FUT" in clean) and not is_option

    # Currency and commodity primarily by exchange
    if exch == "CDS":
        if is_option:
            cls = "options"
            category = "Currency"
        elif is_future or inst_type == "FUTURES":
            cls = "futures"
            category = "Currency"
        else:
            cls = "currency"
            category = "Currency"
        return {"asset_class": cls, "category": category}

    if exch == "MCX":
        if is_option:
            cls = "options"
            category = "Commodity"
        elif is_future or inst_type == "FUTURES":
            cls = "futures"
            category = "Commodity"
        else:
            cls = "commodity"
            category = "Commodity"
        return {"asset_class": cls, "category": category}

    # NSE/NFO (equities, indices and their derivatives)
    # Known pure index spot names (not exhaustive)
    pure_index_names = {
        "NIFTY", "NIFTY50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYIT",
        "NIFTYMIDCAP", "NIFTYBANK"
    }

    # Classify derivatives first for NFO/NSE
    if is_option:
        return {"asset_class": "equity_options", "category": get_stock_sector(clean) or "Equity"}
    if is_future:
        return {"asset_class": "equity_futures", "category": get_stock_sector(clean) or "Equity"}

    # ETF heuristic
    if clean.endswith("ETF") or clean.endswith("BEES"):
        return {"asset_class": "equity_etf", "category": "ETF"}

    # Pure index spot (non-derivative)
    if clean in pure_index_names:
        return {"asset_class": "indices", "category": "Index"}

    # Debt heuristic (very limited; extend as needed)
    if clean.endswith("NCD") or clean.endswith("BOND"):
        return {"asset_class": "government_securities", "category": "Debt"}

    # Default: equity
    return {"asset_class": "equity_cash", "category": get_stock_sector(clean) or "Equity"}

def get_correlation_tag(symbol: str) -> str:
    """Get correlation tag for a symbol"""
    sector = get_stock_sector(symbol)
    asset_info = get_asset_class(symbol)
    asset_class = asset_info["asset_class"]
    return f"{sector}_{asset_class}"

def get_total_crawler_commodities() -> Dict[str, int]:
    """Get all commodities for total crawler (including non-Gold/Silver)"""
    try:
        volume_file = PROJECT_ROOT / "config" / "volume_averages_20d.json"
        if volume_file.exists():
            with open(volume_file, 'r') as f:
                volume_data = json.load(f)
            
            # Get all MCX commodities (not just Gold/Silver)
            target_symbols = set()
            for symbol_key in volume_data.keys():
                if symbol_key.startswith('MCX:'):
                    symbol = symbol_key.replace('MCX:', '')
                    target_symbols.add(symbol)
            
            # Load comprehensive token mapping
            mapping_file = PROJECT_ROOT / "core" / "data" / "token_lookup.json"
            if mapping_file.exists():
                with open(mapping_file, 'r') as f:
                    token_data = json.load(f)
                
                # Build mapping for all commodities
                mapping = {}
                for key, data in token_data.items():
                    token = data.get('token')
                    symbol = data.get('symbol', key.split(':', 1)[-1] if ':' in key else key)
                    
                    if symbol in target_symbols and token:
                        mapping[symbol] = token
                
                if mapping:
                    print(f"✅ Loaded {len(mapping)} commodities for total crawler")
                    return mapping
    except Exception as e:
        print(f"⚠️ Could not load commodity mappings: {e}")
    
    return {}
