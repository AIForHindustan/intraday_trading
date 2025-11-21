"""
Market Activity Analysis - Tier-Based Asset Classification
===========================================================

Categorizes trading assets by activity level and volatility for optimized pattern detection.

Tiers:
- Tier 1: Core Active Derivatives (30-40 symbols) - Always active, perfect for patterns
- Tier 2: Liquid Equities + Derivatives (50-60 symbols) - High volume ratio, strong trading
- Tier 3: Nifty 50 Equities (Monitor Only) - Process only if unusual activity detected

✅ WIRED UP: Used by crawlers/intraday_crawler/update_intraday_crawler.py
   - Updates binary_crawler1.json based on tier definitions
   - Generates instrument lists for Tier 1, Tier 2, Tier 3
   - Used to configure crawler instrument selection
"""

from redis_files.redis_key_standards import is_fno_instrument

# 🥇 Tier 1 - Core Active Derivatives (30-40 symbols)
TIER_1_SYMBOLS = {
    # Gold Futures (VERY ACTIVE - 3,592 price updates)
    'GOLD25DECFUT',
    'GOLDM25DECFUT',
    'GOLDPETAL25DECFUT',
    
    # Currency Futures (ACTIVE - 930 price updates)
    'USDINR25NOVFUT',
    'GBPINR25NOVFUT',
    'EURINR25NOVFUT',
    'JPYINR25NOVFUT',
    
    # Index Futures (Always Active)
    'NIFTY25NOVFUT',
    'BANKNIFTY25NOVFUT',
    'NIFTY25DECFUT',
    'BANKNIFTY25DECFUT',
    
    # Active NIFTY Options Strikes (ATM and near-ATM)
    'NIFTY25NOV26000CE', 'NIFTY25NOV26000PE',
    'NIFTY25NOV26100CE', 'NIFTY25NOV26100PE',
    'NIFTY25NOV26200CE', 'NIFTY25NOV26200PE',
    'NIFTY25NOV26300CE', 'NIFTY25NOV26300PE',
    'NIFTY25DEC26000CE', 'NIFTY25DEC26000PE',
    'NIFTY25DEC26100CE', 'NIFTY25DEC26100PE',
    
    # Active BANKNIFTY Options Strikes (ATM and near-ATM)
    'BANKNIFTY25NOV58000CE', 'BANKNIFTY25NOV58000PE',
    'BANKNIFTY25NOV58100CE', 'BANKNIFTY25NOV58100PE',
    'BANKNIFTY25NOV58200CE', 'BANKNIFTY25NOV58200PE',
    'BANKNIFTY25NOV58300CE', 'BANKNIFTY25NOV58300PE',
    'BANKNIFTY25DEC58000CE', 'BANKNIFTY25DEC58000PE',
    'BANKNIFTY25DEC58100CE', 'BANKNIFTY25DEC58100PE',
}

# Legacy alias for backward compatibility
TIER_1_ASSETS = TIER_1_SYMBOLS

# 📈 Tier 2 - Liquid Equities + Derivatives (50-60 symbols)
TIER_2_SYMBOLS = {
    # High Volume Ratio Stocks (From Redis Analysis)
    'WIPRO',
    'TECHM',
    'JSWSTEEL',
    'INDIGO',
    'TATACONSUM',
    'POWERGRID',
    'SBILIFE',
    'TATASTEEL',
    'ADANIENT',
    'NTPC',
    'AXISBANK',
    
    # Traditional Bluechips (High Market Cap, Always Liquid)
    'RELIANCE',
    'HDFCBANK',
    'ICICIBANK',
    'INFY',
    'TCS',
    'SBIN',
    'KOTAKBANK',
    'BAJFINANCE',
    'HINDUNILVR',
    'ITC',
    'LT',
    'MARUTI',
    'ASIANPAINT',
    'DMART',
    
    # Additional Liquid Equities
    'BHARTIARTL',
    'SUNPHARMA',
    'ULTRACEMCO',
    'NESTLEIND',
    'TITAN',
    'ONGC',
    'COALINDIA',
    'M&M',
    'HEROMOTOCO',
    'EICHERMOT',
    'DABUR',
    'BRITANNIA',
    'HCLTECH',
    'WIPRO',
    'TECHM',
    
    # Futures for Tier 2 Equities (Active contracts)
    'RELIANCE25NOVFUT', 'RELIANCE25DECFUT',
    'HDFCBANK25NOVFUT', 'HDFCBANK25DECFUT',
    'ICICIBANK25NOVFUT', 'ICICIBANK25DECFUT',
    'INFY25NOVFUT', 'INFY25DECFUT',
    'TCS25NOVFUT', 'TCS25DECFUT',
    'SBIN25NOVFUT', 'SBIN25DECFUT',
    'KOTAKBANK25NOVFUT', 'KOTAKBANK25DECFUT',
    'BAJFINANCE25NOVFUT', 'BAJFINANCE25DECFUT',
    'HINDUNILVR25NOVFUT', 'HINDUNILVR25DECFUT',
    'ITC25NOVFUT', 'ITC25DECFUT',
    'TATASTEEL25NOVFUT', 'TATASTEEL25DECFUT',
    'JSWSTEEL25NOVFUT', 'JSWSTEEL25DECFUT',
    'ADANIENT25NOVFUT', 'ADANIENT25DECFUT',
    'WIPRO25NOVFUT', 'WIPRO25DECFUT',
    'TECHM25NOVFUT', 'TECHM25DECFUT',
}

# Legacy alias for backward compatibility
TIER_2_EQUITIES = TIER_2_SYMBOLS

# 📊 Tier 3 - Remaining Nifty 50 Equities (Monitor Only)
# All Nifty 50 equities NOT in Tier 2
# Monitor only, no pre-market seeding
# Process only if unusual activity detected
TIER_3_NIFTY50_EQUITIES = {
    # Remaining Nifty 50 stocks (not in Tier 2)
    # These are the Nifty 50 equities that are NOT in Tier 2
    'ADANIPORTS',
    'APOLLOHOSP',
    'BAJAJ-AUTO',
    'BAJAJFINSV',
    'BEL',
    'CIPLA',
    'DRREDDY',
    'ETERNAL',
    'GRASIM',
    'HDFCLIFE',
    'HINDALCO',
    'JIOFIN',
    'MAXHEALTH',
    'SHRIRAMFIN',
    'TRENT',
    # Note: ULTRACEMCO is in Tier 2, so excluded from Tier 3
    # Tier 2 takes priority - these are the remaining Nifty 50 stocks (17 total)
}

# Note: Only 3 tiers - no Tier 4


def get_tier_1_assets() -> set:
    """Get Tier 1 high activity assets"""
    return TIER_1_SYMBOLS.copy()


def get_tier_1_symbols() -> set:
    """Get Tier 1 core active derivatives (30-40 symbols)"""
    return TIER_1_SYMBOLS.copy()


def get_tier_2_equities() -> set:
    """Get Tier 2 high volatility equities (legacy alias)"""
    return TIER_2_SYMBOLS.copy()


def get_tier_2_symbols() -> set:
    """Get Tier 2 liquid equities + derivatives (50-60 symbols)"""
    return TIER_2_SYMBOLS.copy()


def get_tier_3_nifty50() -> set:
    """Get Tier 3 Nifty 50 equities (monitor only)"""
    return TIER_3_NIFTY50_EQUITIES.copy()


def get_all_priority_assets() -> set:
    """Get all Tier 1 and Tier 2 assets combined"""
    return TIER_1_SYMBOLS.union(TIER_2_SYMBOLS)


def get_all_monitored_assets() -> set:
    """Get all Tier 1, 2, and 3 assets (everything we monitor)"""
    return TIER_1_SYMBOLS.union(TIER_2_SYMBOLS).union(TIER_3_NIFTY50_EQUITIES)


def is_tier_1_asset(symbol: str) -> bool:
    """Check if symbol is a Tier 1 asset"""
    # Check exact match
    if symbol in TIER_1_SYMBOLS:
        return True
    
    # Check if symbol contains tier 1 asset name (for options/futures)
    symbol_upper = symbol.upper()
    for tier1_symbol in TIER_1_SYMBOLS:
        if tier1_symbol.upper() in symbol_upper:
            return True
    
    return False


def is_tier_2_equity(symbol: str) -> bool:
    """Check if symbol is a Tier 2 equity (legacy alias)"""
    return is_tier_2_symbol(symbol)


def is_tier_2_symbol(symbol: str) -> bool:
    """Check if symbol is a Tier 2 symbol"""
    # Check exact match
    if symbol in TIER_2_SYMBOLS:
        return True
    
    # Check if symbol contains tier 2 equity name (for options/futures)
    symbol_upper = symbol.upper()
    for tier2_symbol in TIER_2_SYMBOLS:
        if tier2_symbol.upper() in symbol_upper:
            return True
    
    return False


def is_tier_3_nifty50(symbol: str) -> bool:
    """Check if symbol is a Tier 3 Nifty 50 equity"""
    # Check exact match
    if symbol in TIER_3_NIFTY50_EQUITIES:
        return True
    
    # Check if symbol contains tier 3 equity name (for options/futures)
    symbol_upper = symbol.upper()
    for tier3_equity in TIER_3_NIFTY50_EQUITIES:
        if tier3_equity.upper() in symbol_upper:
            return True
    
    return False


def get_asset_tier(symbol: str) -> int:
    """
    Get the tier for a given symbol.
    
    Returns:
        1: Tier 1 (Core Active Derivatives - 30-40 symbols)
        2: Tier 2 (Liquid Equities + Derivatives - 50-60 symbols)
        3: Tier 3 (Remaining Nifty 50 Equities - Monitor Only)
    """
    if is_tier_1_asset(symbol):
        return 1
    elif is_tier_2_symbol(symbol):
        return 2
    elif is_tier_3_nifty50(symbol):
        return 3
    else:
        # Not in any tier - default to Tier 3 (monitor only)
        return 3


def should_premarket_seed(symbol: str) -> bool:
    """
    Check if symbol should be pre-market seeded.
    
    Only Tier 1 and Tier 2 symbols get pre-market seeding.
    Tier 3 (Nifty 50) is monitor only - no pre-market seeding.
    """
    tier = get_asset_tier(symbol)
    return tier in [1, 2]


def should_process_unusual_activity_only(symbol: str) -> bool:
    """
    Check if symbol should only be processed when unusual activity is detected.
    
    Tier 3 (Nifty 50) is monitor only - process only if unusual activity detected.
    """
    tier = get_asset_tier(symbol)
    return tier == 3


# Activity Analysis Metadata
ACTIVITY_METADATA = {
    'tier_1': {
        'description': 'High Activity Assets - Always active, perfect for pattern detection',
        'priority': 'HIGH',
        'examples': {
            'gold_futures': '3,592 price updates - Extremely active',
            'currency_futures': '930 price updates - Good activity (GBPINR, EURINR most active)',
            'index_derivatives': 'Always active - Core trading instruments'
        }
    },
    'tier_2': {
        'description': 'High Volatility Equities - High volume ratio, strong trading',
        'priority': 'MEDIUM-HIGH',
        'examples': {
            'wipro': '6.10x volume ratio - Massive activity',
            'techm': '4.20x volume ratio - Very active',
            'high_volume_stocks': '11 stocks with >1.0x ratio - Real trading happening'
        }
    }
}

