#!/usr/bin/env python3
"""
Live Intraday Crawler - Optimized for NIFTY & BANKNIFTY
=======================================================

Focused on live trading with proper historical baseline (20-55 days OHLC)
for indicators like MACD, RSI, ATR, Bollinger Bands.

Maintains:
- NIFTY & BANKNIFTY: Current + Next month for pattern strategies
- Reduced strike ranges for live trading efficiency
- High liquidity focus
- Same Redis key structure
"""

from redis_files.redis_client import RedisManager82
import json
import re
import redis
import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from calendar import monthrange
from collections import defaultdict

# Import tier definitions
from crawlers.market_activity_tiers import TIER_2_SYMBOLS
from utils.market_calendar  import MarketCalendar

# Import Zerodha API for fetching spot prices
try:
    from config.zerodha_config import ZerodhaConfig
    ZERODHA_AVAILABLE = True
except ImportError:
    ZERODHA_AVAILABLE = False

def get_zerodha_spot_price(symbol: str, kite, token_lookup: Dict) -> Optional[float]:
    """Fetch spot price using Zerodha API - IMPROVED VERSION"""
    if not kite:
        return None
    
    try:
        symbol_upper = symbol.upper()
        
        # Try direct symbol first
        try:
            quote = kite.quote([symbol_upper])
            if quote and symbol_upper in quote:
                data = quote[symbol_upper]
                last_price = data.get('last_price', 0)
                if last_price and last_price > 0:
                    return float(last_price)
        except:
            pass
        
        # Try with NSE: prefix for equities
        if 'FUT' not in symbol_upper and 'CE' not in symbol_upper and 'PE' not in symbol_upper:
            nse_symbol = f"NSE:{symbol_upper}"
            try:
                quote = kite.quote([nse_symbol])
                if quote and nse_symbol in quote:
                    data = quote[nse_symbol]
                    last_price = data.get('last_price', 0)
                    if last_price and last_price > 0:
                        return float(last_price)
            except:
                pass
        
        # Try with NFO: prefix for futures
        if 'FUT' in symbol_upper:
            nfo_symbol = f"NFO:{symbol_upper}"
            try:
                quote = kite.quote([nfo_symbol])
                if quote and nfo_symbol in quote:
                    data = quote[nfo_symbol]
                    last_price = data.get('last_price', 0)
                    if last_price and last_price > 0:
                        return float(last_price)
            except:
                pass
        
        return None
    except Exception as e:
        return None



def get_redis_spot_price(symbol: str, redis_client) -> Optional[float]:
    """
    Fetch spot price from Redis DB1 using correct key format.
    
    Uses actual Redis key standards:
    - ticks:{canonical_symbol} (hash with last_price field)
    - ohlc_latest:{normalized_symbol} (hash with close/last_price field)
    """
    if not redis_client:
        return None

    
    
    # ✅ FIXED: Use actual Redis key standards from redis_key_standards.py
    from redis_files.redis_key_standards import RedisKeyStandards, get_key_builder, normalize_symbol
    
    # Get canonical symbol for ticks hash (matches redis_storage.py format)
    canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
    
    # Try ticks hash first (most up-to-date) - matches redis_storage.py storage format
    key_builder = get_key_builder()
    ticks_key = key_builder.live_ticks_hash(canonical_sym)
    
    try:
        tick_data = redis_client.hgetall(ticks_key)
        if tick_data:
            # Handle both bytes and string keys (redis_storage.py stores as strings)
            for field in [b'last_price', 'last_price', b'price', 'price', b'ltp', 'ltp']:
                if field in tick_data:
                    value = tick_data[field]
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                    try:
                        last_price = float(value)
                        if last_price > 0:
                            return last_price
                    except (ValueError, TypeError):
                        continue
    except Exception:
        pass

    
    # Try OHLC latest (fallback) - uses normalize_symbol function (not method)
    normalized_sym = normalize_symbol(symbol)
    ohlc_key = key_builder.live_ohlc_latest(normalized_sym)

    try:
        symbol_upper = symbol.upper()
        
        # Try multiple key patterns
        patterns = [
            f"index:NSE:{symbol_upper}",
            f"equity:NSE:{symbol_upper}",
            f"tick:{symbol_upper}",
            f"price:{symbol_upper}",
            f"ltp:{symbol_upper}",
            symbol_upper,
        ]
        
        # Add NIFTY specific patterns
        if 'NIFTY' in symbol_upper:
            patterns.extend([
                "index:NSE:NIFTY 50",
                "index:NSE:NIFTY_50", 
                "index:NSENIFTY_50",
                "index:NSE:NIFTY",
            ])
        elif 'BANKNIFTY' in symbol_upper:
            patterns.extend([
                "index:NSE:NIFTY BANK", 
                "index:NSE:NIFTY_BANK",
                "index:NSENIFTY_BANK",
            ])
        
        for pattern in patterns:
            try:
                data = redis_client.get(pattern)
                if data:
                    # Try to parse as JSON
                    try:
                        price_data = json.loads(data)
                        if 'last_price' in price_data:
                            price = float(price_data['last_price'])
                            if price > 0:
                                return price
                        elif 'last' in price_data:
                            price = float(price_data['last'])
                            if price > 0:
                                return price
                        elif 'ltp' in price_data:
                            price = float(price_data['ltp'])
                            if price > 0:
                                return price
                    except:
                        # If not JSON, try to parse as float directly
                        try:
                            price = float(data)
                            if price > 0:
                                return price
                        except:
                            pass
            except:
                continue
        
        return None
    except Exception as e:
        return None

def get_equity_spot_price(equity: str, redis_client, kite, token_lookup: Dict, current_month: str) -> float:
    """Get equity spot price with multiple fallback methods"""
    # Method 1: Try Redis for equity cash
    spot_price = get_redis_spot_price(equity, redis_client)
    if spot_price:
        return spot_price
    
    # Method 2: Try Zerodha for equity cash
    if kite:
        spot_price = get_zerodha_spot_price(equity, kite, token_lookup)
        if spot_price:
            return spot_price
    
    # Method 3: Try current month future price
    fut_symbol = f"{equity}{current_month}FUT"
    spot_price = get_redis_spot_price(fut_symbol, redis_client)
    if spot_price:
        return spot_price
    
    # Method 4: Try Zerodha for future
    if kite:
        spot_price = get_zerodha_spot_price(fut_symbol, kite, token_lookup)
        if spot_price:
            return spot_price
        else:
            return None
    return None


@dataclass
class ExpiryWindow:
    code: str  # Zerodha format (e.g., "25DEC") - use for token lookup
    date: datetime  # Actual expiry date (e.g., 2025-12-30)
    
    @property
    def normalized_code(self) -> str:
        """Generate normalized code based on actual expiry date (e.g., "30DEC" instead of "25DEC")"""
        return self.date.strftime("%d%b").upper()


def _compute_monthly_expiry(month_offset: int = 0) -> datetime:
    """
    Calculate the actual monthly expiry date (last Thursday, adjusted for holidays)
    for the month offset relative to the current month.
    """
    now = datetime.now()
    target_month_index = now.month - 1 + month_offset
    target_year = now.year + (target_month_index // 12)
    target_month = (target_month_index % 12) + 1

    last_day = monthrange(target_year, target_month)[1]
    expiry_candidate = datetime(target_year, target_month, last_day)

    # Step back to the last Thursday of the month
    while expiry_candidate.weekday() != 3:  # 3 = Thursday
        expiry_candidate -= timedelta(days=1)

    # NSE F&O expiry is always the last Thursday of the month
    # The symbol uses this date even if it's a holiday (exchange adjusts settlement)
    # So we return the last Thursday regardless of trading day status
    # The calendar helper is available if needed for DTE calculations elsewhere
    return expiry_candidate


def get_expiry_months(token_lookup: Optional[Dict] = None) -> Tuple[ExpiryWindow, ExpiryWindow]:
    """
    Get current and next month expiry windows.
    
    ✅ FIXED: Reads actual expiry dates from token_lookup metadata instead of calculating.
    This ensures we use the real exchange settlement dates (e.g., Dec 30, not Dec 25).

    code -> matches YYMMM format expected by Zerodha/NFO symbols (e.g., 25DEC)
    date -> actual calendar date from token metadata (e.g., 2025-12-30)
    """
    # Calculate theoretical last Thursday for symbol code
    current_expiry_date = _compute_monthly_expiry(month_offset=0)
    next_expiry_date = _compute_monthly_expiry(month_offset=1)
    
    current_code = current_expiry_date.strftime("%y%b").upper()
    next_code = next_expiry_date.strftime("%y%b").upper()
    
    # ✅ FIXED: Look up actual expiry dates from token metadata
    # Note: After normalization, keys may have normalized codes (30DEC) instead of Zerodha codes (25DEC)
    # So we match by expiry date month/year, not by code in key
    if token_lookup:
        # Find actual expiry dates from representative contracts
        # Match by expiry date month/year (works with both normalized and original codes)
        for token_str, data in token_lookup.items():
            key = data.get('key', '').upper()
            expiry_str = data.get('expiry', '')
            
            # Check for current month contract - match by expiry date
            if 'FUT' in key and expiry_str:
                try:
                    actual_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                    if actual_date.month == current_expiry_date.month and actual_date.year == current_expiry_date.year:
                        current_expiry_date = actual_date
                        break
                except (ValueError, TypeError):
                    pass
        
        # Find next month actual expiry
        for token_str, data in token_lookup.items():
            key = data.get('key', '').upper()
            expiry_str = data.get('expiry', '')
            
            # Check for next month contract - match by expiry date month/year
            if 'FUT' in key and expiry_str:
                try:
                    actual_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                    if actual_date.month == next_expiry_date.month and actual_date.year == next_expiry_date.year:
                        next_expiry_date = actual_date
                        break
                except (ValueError, TypeError):
                    pass
    
    return ExpiryWindow(current_code, current_expiry_date), ExpiryWindow(next_code, next_expiry_date)


def calculate_atm_strike(spot_price: float, strike_interval: float = 50) -> float:
    """Calculate ATM strike by rounding to nearest strike interval"""
    return round(round(spot_price / strike_interval) * strike_interval, 2)


def generate_strikes(atm_strike: float, range_strikes: int, strike_interval: float) -> List[float]:
    """Generate strikes around ATM (±range_strikes)"""
    strikes = []
    for i in range(-range_strikes, range_strikes + 1):
        strike = atm_strike + (i * strike_interval)
        if strike > 0:
            strikes.append(round(strike, 2))
    return sorted(strikes)


def get_strike_interval(symbol: str) -> int:
    """Get strike interval based on symbol type"""
    symbol_upper = symbol.upper()
    
    if 'NIFTY' in symbol_upper and 'BANK' not in symbol_upper:
        return 50
    elif 'BANKNIFTY' in symbol_upper:
        return 100
    else:
        return 5


def find_token_for_symbol_robust(symbol: str, token_lookup: Dict) -> Optional[int]:
    """Robust token finder with multiple matching strategies"""
    symbol_upper = symbol.upper()
    is_derivative = 'FUT' in symbol_upper or 'CE' in symbol_upper or 'PE' in symbol_upper
    
    # ✅ FIXED: Prioritize NSE: prefix for equities (before contained matches)
    # This prevents matching "INFY" with "NFO:INFY25NOVFUT"
    if not is_derivative:
        nse_symbol = f"NSE:{symbol_upper}"
        for token_str, data in token_lookup.items():
            key = data.get('key', '').upper()
            if key == nse_symbol:
                return int(token_str)
    
    # Try exact match
    for token_str, data in token_lookup.items():
        key = data.get('key', '').upper()
        name = data.get('name', '').upper()
        
        # Remove exchange prefix for matching
        key_clean = key.split(':', 1)[-1] if ':' in key else key
        
        # Exact match
        if symbol_upper == key_clean or symbol_upper == name:
            return int(token_str)
    
    # Try with NFO: prefix for derivatives
    if is_derivative:
        nfo_symbol = f"NFO:{symbol_upper}"
        for token_str, data in token_lookup.items():
            key = data.get('key', '').upper()
            if key == nfo_symbol:
                return int(token_str)
    
    # Try contained match (only for derivatives, or if NSE match failed)
    # ✅ FIXED: Skip contained match for plain equity symbols to avoid matching futures
    if is_derivative:
        for token_str, data in token_lookup.items():
            key = data.get('key', '').upper()
            name = data.get('name', '').upper()
            
            key_clean = key.split(':', 1)[-1] if ':' in key else key
            
            # Contained match
            if symbol_upper in key_clean or symbol_upper in key:
                return int(token_str)
    
    return None


def generate_option_symbols(underlying: str, expiry: str, strikes: List[int]) -> List[str]:
    """Generate option symbols (CE and PE) for given strikes"""
    symbols = []
    for strike in strikes:
        symbols.append(f"{underlying}{expiry}{strike}CE")
        symbols.append(f"{underlying}{expiry}{strike}PE")
    return symbols


def normalize_instrument_names(token_lookup: Dict, expiry_mapping: Dict[str, str]) -> Dict:
    """
    Normalize instrument names by replacing Zerodha expiry codes with actual expiry dates.
    
    Args:
        token_lookup: Original token lookup dict
        expiry_mapping: Mapping of Zerodha codes to normalized codes (e.g., {"25DEC": "30DEC"})
    
    Returns:
        Token lookup with normalized 'key' fields
    """
    normalized = {}
    for token_str, data in token_lookup.items():
        # Create a copy to avoid modifying original
        normalized_data = data.copy()
        key = data.get('key', '')
        
        # Replace expiry codes in key field
        normalized_key = key
        for zerodha_code, normalized_code in expiry_mapping.items():
            if zerodha_code in normalized_key:
                normalized_key = normalized_key.replace(zerodha_code, normalized_code)
                break
        
        normalized_data['key'] = normalized_key
        normalized[token_str] = normalized_data
    
    return normalized


def load_configs():
    """Load current configuration and token lookup"""
    config_path = Path("crawlers/binary_crawler1/binary_crawler1.json")
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    token_lookup_path = Path("zerodha_token_list/zerodha_instrument_token.json")

    if not token_lookup_path.exists():
        raise FileNotFoundError(f"Token lookup file not found. Tried: {token_lookup_path}")
    
    with open(token_lookup_path, 'r') as f:
        token_lookup = json.load(f)
    print(f"✅ Loaded token lookup from: {token_lookup_path}")
    
    return config, token_lookup


def find_atm_options_from_api(kite, underlying: str, expiry_str: str, current_price: float, strike_range: int, nfo_instruments_cache: Optional[List[Dict]] = None) -> List[int]:
    """
    Find ATM ±strike_range options from actual Zerodha API instruments
    Returns list of tokens for valid, existing options
    """
    if not kite:
        return []
    
    try:
        # Use cache if provided, otherwise fetch
        if nfo_instruments_cache is None:
            nfo_instruments = kite.instruments('NFO')
        else:
            nfo_instruments = nfo_instruments_cache
        
        # Parse expiry string (e.g., "25NOV") to date
        # Format: YYMMM -> 2025-11-XX (last Thursday)
        expiry_date = None
        for inst in nfo_instruments:
            if inst.get('name') == underlying and inst.get('instrument_type') in ['CE', 'PE']:
                inst_expiry_str = inst['expiry'].strftime('%y%b').upper()
                if inst_expiry_str == expiry_str.upper():
                    expiry_date = inst['expiry'].date()
                    break
        
        if not expiry_date:
            return []
        
        # Filter for underlying, expiry, and option type
        options = []
        for inst in nfo_instruments:
            if (inst.get('name') == underlying and 
                inst.get('expiry').date() == expiry_date and
                inst.get('instrument_type') in ['CE', 'PE']):
                options.append(inst)
        
        if not options:
            return []
        
        # Get unique strikes and find ATM
        strikes = sorted(set([opt['strike'] for opt in options]))
        atm_strike = min(strikes, key=lambda x: abs(x - current_price))
        atm_idx = strikes.index(atm_strike)
        
        # Get ±strike_range strikes
        start_idx = max(0, atm_idx - strike_range)
        end_idx = min(len(strikes), atm_idx + strike_range + 1)
        target_strikes = strikes[start_idx:end_idx]
        
        # Get tokens for all matching options
        tokens = []
        for strike in target_strikes:
            for opt_type in ['CE', 'PE']:
                matching = [o for o in options if o['strike'] == strike and o['instrument_type'] == opt_type]
                if matching:
                    tokens.append(matching[0]['instrument_token'])
        
        return tokens
        
    except Exception as e:
        # Silently fail - API lookup is optional
        return []


def get_liquid_equities() -> List[str]:
    """
    Get most liquid Nifty 50 equities for live trading
    Focus on high volume stocks that provide good indicator baselines
    Uses TIER_2_SYMBOLS and filters out futures/options
    """
    # Get equity symbols from TIER_2_SYMBOLS (exclude futures/options)
    equities = []
    for symbol in TIER_2_SYMBOLS:
        # Skip futures and options (they contain 'FUT', 'CE', 'PE', or dates)
        if 'FUT' in symbol or 'CE' in symbol or 'PE' in symbol:
            continue
        # Skip any symbol with numbers (likely expiry dates)
        if any(char.isdigit() for char in symbol):
            continue
        equities.append(symbol)
    
    return sorted(equities)


def main():
    """Main function optimized for live intraday trading"""
    parser = argparse.ArgumentParser(
        description='Live Intraday Crawler - Optimized for NIFTY & BANKNIFTY',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Live Trading Focus:
    NIFTY & BANKNIFTY: Current + Next month for pattern strategies
    Reduced strikes: Focus on liquid near-ATM options
    Historical baseline: 20-55 days OHLC for indicators (MACD, RSI, ATR, Bollinger)
        """
    )
    parser.add_argument(
        '--tier1-range', type=int, default=8,
        help='Strike range for Tier 1 indices (NIFTY, BANKNIFTY). Default: 8 (±8 ATM)'
    )
    parser.add_argument(
        '--tier2-range', type=int, default=5,
        help='Strike range for Tier 2 equities. Default: 5 (±5 ATM)'
    )
    parser.add_argument(
        '--include-forex', action='store_true',
        help='Include USDINR (disabled by default for live trading)'
    )
    parser.add_argument(
        '--include-gold', action='store_true', 
        help='Include GOLD (disabled by default for live trading)'
    )
    parser.add_argument(
        '--banknifty-spot', type=float, default=59347.0,
        help='Manual BANKNIFTY spot override for strike generation (default: 59347.0)'
    )
    
    args = parser.parse_args()
    
    # Live trading optimized ranges
    TIER_1_STRIKE_RANGE = args.tier1_range  # ±8 for indices (balance between strategies and efficiency)
    TIER_2_STRIKE_RANGE = args.tier2_range  # ±5 for equities
    
    print("🎯 LIVE INTRADAY CRAWLER - Optimized for NIFTY & BANKNIFTY")
    print("=" * 70)
    print("📊 Strategy: Live trading with 20-55 day OHLC baseline for indicators")
    print("📈 Focus: NIFTY, BANKNIFTY + Liquid Nifty 50 equities")
    
    # Connect to Redis
    try:
        redis_client = RedisManager82.get_client(process_name="update_live_crawler", db=1)
        redis_client.ping()
        print("✅ Connected to Redis DB1")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return
    
    # Initialize Zerodha API (optional)
    kite = None
    if ZERODHA_AVAILABLE:
        try:
            kite = ZerodhaConfig.get_kite_instance()
            print("✅ Connected to Zerodha API")
        except Exception as e:
            print(f"⚠️  Zerodha API not available: {e}")
            kite = None
    
    # Load configurations
    config, token_lookup = load_configs()
    print(f"📊 Loaded token lookup: {len(token_lookup)} instruments")
    
    # Get expiry months (with actual dates from token metadata)
    current_expiry, next_expiry = get_expiry_months(token_lookup=token_lookup)
    current_month = current_expiry.code  # Zerodha code (e.g., "25DEC") - for token lookup
    next_month = next_expiry.code  # Zerodha code (e.g., "25DEC") - for token lookup
    current_month_normalized = current_expiry.normalized_code  # Normalized (e.g., "30DEC") - for symbol names
    next_month_normalized = next_expiry.normalized_code  # Normalized (e.g., "30DEC") - for symbol names
    print(
        f"📅 Expiry months: {current_month}→{current_month_normalized} ({current_expiry.date.strftime('%Y-%m-%d')}) | "
        f"{next_month}→{next_month_normalized} ({next_expiry.date.strftime('%Y-%m-%d')})"
    )
    print(f"🎯 Strike ranges: Tier 1 ±{TIER_1_STRIKE_RANGE} ATM, Tier 2 ±{TIER_2_STRIKE_RANGE} ATM")
    
    # Collect all tokens
    all_tokens = set()
    
    # ===== 🥇 TIER 1: NIFTY & BANKNIFTY (Current + Next Month) =====
    print(f"\n🥇 Tier 1: NIFTY & BANKNIFTY (±{TIER_1_STRIKE_RANGE} ATM)")
    print("-" * 60)
    
    indices = ['NIFTY', 'BANKNIFTY']
    index_options_added = 0
    index_futures_added = 0
    
    for index in indices:
        # Fetch spot price
        spot_price = None
        for symbol_variant in [index, f"{index} 50", f"{index} BANK", "NIFTY 50" if index == "NIFTY" else "NIFTY BANK"]:
            spot_price = get_redis_spot_price(symbol_variant, redis_client)
            if spot_price:
                break
        
        if index == 'BANKNIFTY' and args.banknifty_spot:
            spot_price = float(args.banknifty_spot)
            print(f"✅ {index}: Using manual override spot price ₹{spot_price:,.2f}")
        elif not spot_price:
            # Try Zerodha API as fallback
            if kite:
                # Zerodha API uses specific symbols for indices
                if index == 'NIFTY':
                    api_symbol = "NSE:NIFTY 50"
                else:  # BANKNIFTY
                    api_symbol = "NSE:NIFTY BANK"
                
                spot_price = get_zerodha_spot_price(api_symbol, kite, token_lookup)
                if spot_price:
                    print(f"✅ {index}: Using Zerodha API price: ₹{spot_price:,.2f}")
                else:
                    # Last resort: Use approximate fallback (only if API fails)
                    if index == 'NIFTY':
                        spot_price = 25890.0
                    else:  # BANKNIFTY
                        spot_price = 58340.0
                    print(f"⚠️  {index}: Using approximate fallback price: ₹{spot_price:,.2f} (Zerodha API unavailable)")
            else:
                # Fallback prices if Zerodha API not connected
                if index == 'NIFTY':
                    spot_price = 25890.0
                else:  # BANKNIFTY
                    spot_price = 58340.0
                print(f"⚠️  {index}: Using approximate fallback price: ₹{spot_price:,.2f} (Zerodha API not connected)")
        else:
            print(f"✅ {index}: Spot = ₹{spot_price:,.2f}")
        
        # Calculate strikes
        strike_interval = get_strike_interval(index)
        index_strike_range = TIER_1_STRIKE_RANGE
        if index == 'BANKNIFTY':
            index_strike_range = max(index_strike_range, 10)
        atm_strike = calculate_atm_strike(spot_price, strike_interval)
        strikes = generate_strikes(atm_strike, index_strike_range, strike_interval)
        
        print(f"   ATM: {atm_strike}, Strikes: {strikes[0]} to {strikes[-1]} (±{index_strike_range})")
        
        # Add options for BOTH current and next month (for pattern strategies)
        # ✅ Use normalized codes (30DEC) - token_lookup should be normalized first
        for expiry in [current_month_normalized, next_month_normalized]:
            option_symbols = generate_option_symbols(index, expiry, strikes)
            for opt_symbol in option_symbols:
                token = find_token_for_symbol_robust(opt_symbol, token_lookup)
                if token:
                    all_tokens.add(token)
                    index_options_added += 1
            
            # Add futures for both months
            fut_symbol = f"{index}{expiry}FUT"
            token = find_token_for_symbol_robust(fut_symbol, token_lookup)
            if token:
                all_tokens.add(token)
                index_futures_added += 1
    
    print(f"   ✅ Added: {index_options_added} options, {index_futures_added} futures")
    
    # ===== 📈 TIER 2: Liquid Equities (Current Month Only) =====
    print(f"\n📈 Tier 2: Liquid Nifty 50 Equities (±{TIER_2_STRIKE_RANGE} ATM)")
    print("-" * 60)
    
    # Cache NFO instruments to avoid repeated API calls
    nfo_instruments_cache = None
    if kite:
        try:
            nfo_instruments_cache = kite.instruments('NFO')
            print(f"   ✅ Cached {len(nfo_instruments_cache)} NFO instruments")
        except Exception as e:
            print(f"   ⚠️  Could not cache NFO instruments: {e}")
    
    liquid_equities = get_liquid_equities()
    equity_cash_added = 0
    equity_futures_added = 0
    equity_options_added = 0
    
    print(f"  Processing {len(liquid_equities)} liquid equities...")
    
    for equity in liquid_equities:
        # Add equity cash token (for baseline indicators)
        equity_token = find_token_for_symbol_robust(equity, token_lookup)
        if equity_token and equity_token not in all_tokens:
            all_tokens.add(equity_token)
            equity_cash_added += 1
        
        # Fetch spot price
        spot_price = get_redis_spot_price(equity, redis_client)
        if not spot_price:
            # Try future price as fallback
            fut_symbol = f"{equity}{current_month}FUT"
            spot_price = get_redis_spot_price(fut_symbol, redis_client)
            if spot_price:
                print(f"   ℹ️  {equity}: Using future price {spot_price:.2f}")
            else:
                # Try Zerodha API as fallback
                if kite:
                    spot_price = get_zerodha_spot_price(equity, kite, token_lookup)
                    if spot_price:
                        print(f"   ✅ {equity}: Using Zerodha API price {spot_price:.2f}")
                    else:
                        # Last resort: Try future via API
                        fut_symbol = f"{equity}{current_month}FUT"
                        spot_price = get_zerodha_spot_price(fut_symbol, kite, token_lookup)
                        if spot_price:
                            print(f"   ✅ {equity}: Using Zerodha API future price {spot_price:.2f}")
                        else:
                            print(f"   ⚠️  {equity}: Could not fetch price from Redis or Zerodha API - skipping options generation")
                            spot_price = None
                else:
                    print(f"   ⚠️  {equity}: Could not fetch price (Redis unavailable, Zerodha API not connected) - skipping options generation")
                    spot_price = None
        
        # Add CURRENT MONTH future only (for live trading)
        # ✅ Use normalized code (30DEC) - token_lookup should be normalized first
        fut_symbol = f"{equity}{current_month_normalized}FUT"
        token = find_token_for_symbol_robust(fut_symbol, token_lookup)
        if token and token not in all_tokens:
            all_tokens.add(token)
            equity_futures_added += 1
        
        # Add CURRENT MONTH options only - fetch actual available options from API
        if spot_price and kite:
            # Fetch actual available options from Zerodha API
            # Note: API still uses Zerodha code, but we'll normalize the symbols after
            option_tokens = find_atm_options_from_api(
                kite, equity, current_month, spot_price, TIER_2_STRIKE_RANGE, nfo_instruments_cache
            )
            for token in option_tokens:
                if token not in all_tokens:
                    all_tokens.add(token)
                    equity_options_added += 1
        elif spot_price:
            # Fallback to old method if API not available
            strike_interval = get_strike_interval(equity)
            atm_strike = calculate_atm_strike(spot_price, strike_interval)
            strikes = generate_strikes(atm_strike, TIER_2_STRIKE_RANGE, strike_interval)
            
            # ✅ Use normalized code (30DEC) - token_lookup should be normalized first
            option_symbols = generate_option_symbols(equity, current_month_normalized, strikes)
            for opt_symbol in option_symbols:
                token = find_token_for_symbol_robust(opt_symbol, token_lookup)
                if token and token not in all_tokens:
                    all_tokens.add(token)
                    equity_options_added += 1
    
    print(f"   ✅ Added: {equity_cash_added} cash, {equity_futures_added} futures, {equity_options_added} options")
    
    # ===== 💰 OPTIONAL: USDINR (Only if explicitly enabled) =====
    if args.include_forex:
        print(f"\n💰 USDINR (±5 ATM)")
        print("-" * 60)
        
        currency_base = 'USDINR'
        spot_price = get_redis_spot_price(currency_base, redis_client)
        if not spot_price:
            spot_price = 83.5
        
        print(f"  {currency_base}: Spot = ₹{spot_price:.2f}")
        
        # Add current month only for live trading
        strike_interval = 0.25
        atm_strike = calculate_atm_strike(spot_price, strike_interval)
        strikes = generate_strikes(atm_strike, 5, strike_interval)
        
        # Options
        options_added = 0
        for strike in strikes:
            for opt_type in ['CE', 'PE']:
                opt_symbol = f"{currency_base}{current_month}{strike}{opt_type}"
                token = find_token_for_symbol_robust(opt_symbol, token_lookup)
                if token:
                    all_tokens.add(token)
                    options_added += 1
        
        # Future
        fut_symbol = f"{currency_base}{current_month}FUT"
        token = find_token_for_symbol_robust(fut_symbol, token_lookup)
        if token:
            all_tokens.add(token)
        
        print(f"   ✅ Added {options_added} options + 1 future")
    
    # ===== 🥇 OPTIONAL: GOLD (Only if explicitly enabled) =====
    if args.include_gold:
        print(f"\n🥇 GOLD (±5 ATM)")
        print("-" * 60)
        
        gold_base = 'GOLD'
        spot_price = get_redis_spot_price(gold_base, redis_client)
        if not spot_price:
            spot_price = 125163.0  # NSE spot for 10g
        
        print(f"  {gold_base}: Spot = ₹{spot_price:,.2f}")
        
        # Add next month only (gold typically has quarterly expiries)
        strike_interval = 100
        atm_strike = calculate_atm_strike(spot_price, strike_interval)
        strikes = generate_strikes(atm_strike, 5, strike_interval)
        
        # Options
        options_added = 0
        for strike in strikes:
            for opt_type in ['CE', 'PE']:
                opt_symbol = f"{gold_base}{next_month}{strike}{opt_type}"
                token = find_token_for_symbol_robust(opt_symbol, token_lookup)
                if token:
                    all_tokens.add(token)
                    options_added += 1
        
        # Future
        fut_symbol = f"{gold_base}{next_month}FUT"
        token = find_token_for_symbol_robust(fut_symbol, token_lookup)
        if token:
            all_tokens.add(token)
        
        print(f"   ✅ Added {options_added} options + 1 future")
    
    # Update configuration
    print(f"\n✅ Total tokens collected: {len(all_tokens)}")
    config["tokens"] = sorted(list(all_tokens))
    config["total_instruments"] = len(all_tokens)
    
    # Update expiry policy
    config["expiry_policy"] = {
        "current_expiry": current_expiry.date.strftime("%Y-%m-%d"),
        "next_expiry": next_expiry.date.strftime("%Y-%m-%d"),
        "included_patterns": [current_month, next_month],
        "live_trading_optimized": True
    }
    
    # Update instrument breakdown
    # Count from actual tokens in all_tokens set, not just token_lookup (which may be NFO-only)
    futures_count = 0
    options_count = 0
    equity_count = 0
    
    for token in all_tokens:
        token_str = str(token)
        if token_str in token_lookup:
            inst_type = token_lookup[token_str].get('instrument_type', '').upper()
            if inst_type == 'FUT':
                futures_count += 1
            elif inst_type in ['CE', 'PE']:
                options_count += 1
            elif inst_type == 'EQ':
                equity_count += 1
    
    config["instrument_breakdown"] = {
        "equity_cash": equity_count,
        "equity_futures": futures_count,
        "index_options": options_count,
        "total": len(all_tokens),
        "live_trading_focus": "NIFTY+BANKNIFTY+20_LIQUID_EQUITIES"
    }
    
    # Add live trading note
    if "notes" not in config:
        config["notes"] = []
    
    config["notes"].append(
        f"LIVE_TRADING {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: "
        f"NIFTY/BANKNIFTY ±{TIER_1_STRIKE_RANGE}ATM ({current_month}+{next_month}), "
        f"Equities ±{TIER_2_STRIKE_RANGE}ATM ({current_month} only), "
        f"20-liquid stocks, Historical OHLC baseline"
    )
    
    # Save updated configuration
    config_path = Path("crawlers/binary_crawler1/binary_crawler1.json")
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"💾 Configuration saved to {config_path}")
    
    # Final summary
    print(f"\n📋 LIVE TRADING SUMMARY:")
    print(f"   Equity Cash: {equity_count} (for 20-55 day OHLC baseline)")
    print(f"   Futures: {futures_count}")
    print(f"   Options: {options_count}")
    print(f"   Total Instruments: {len(all_tokens)}")
    print(f"\n🎯 Optimized for:")
    print(f"   - NIFTY & BANKNIFTY: Both months for pattern strategies")
    print(f"   - 20 liquid equities: Current month only")
    print(f"   - Reduced strikes: Focus on liquid near-ATM")
    print(f"   - Historical baseline: 20-55 days OHLC for indicators")


if __name__ == "__main__":
    main()