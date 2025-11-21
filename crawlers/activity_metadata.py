"""
Market Activity Metadata
========================

Activity analysis metadata and statistics for different asset categories.

⚠️ NOTE: This file is NOT wired up yet - for future integration
"""
from typing import List
# Activity Analysis by Category
ACTIVITY_BY_CATEGORY = {
    'gold_futures': {
        'description': 'Gold Futures - Extremely Active',
        'price_updates': 3592,
        'activity_level': 'VERY_HIGH',
        'tier': 1,
        'priority': 'HIGH',
        'notes': 'Perfect for pattern detection - 3,592 price updates',
        'symbols': ['GOLD25DECFUT', 'GOLDM25DECFUT', 'GOLDPETAL25DECFUT']
    },
    'currency_futures': {
        'description': 'Currency Futures - Good Activity',
        'price_updates': 930,
        'activity_level': 'HIGH',
        'tier': 1,
        'priority': 'HIGH',
        'notes': '930 price updates - GBPINR, EURINR most active',
        'symbols': ['GBPINR25NOVFUT', 'EURINR25NOVFUT', 'JPYINR25NOVFUT', 'USDINR25NOVFUT']
    },
    'index_derivatives': {
        'description': 'Index Derivatives - Always Active',
        'price_updates': 'continuous',
        'activity_level': 'ALWAYS_ACTIVE',
        'tier': 1,
        'priority': 'HIGH',
        'notes': 'Core trading instruments - Always active',
        'symbols': ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']
    },
    'high_volatility_equities': {
        'description': 'High Volatility Equities - Strong Trading',
        'volume_ratio_range': '>1.0x',
        'activity_level': 'HIGH',
        'tier': 2,
        'priority': 'MEDIUM_HIGH',
        'notes': '11 stocks with >1.0x volume ratio - Real trading happening',
        'top_volatile': [
            {'symbol': 'WIPRO', 'volume_ratio': 6.10, 'note': 'Massive activity'},
            {'symbol': 'TECHM', 'volume_ratio': 4.20, 'note': 'Very active'},
            {'symbol': 'JSWSTEEL', 'volume_ratio': 3.36, 'note': 'High activity'},
            {'symbol': 'INDIGO', 'volume_ratio': 3.26, 'note': 'High activity'},
            {'symbol': 'TATACONSUM', 'volume_ratio': 2.57, 'note': 'Good activity'}
        ]
    },
    'traditional_liquids': {
        'description': 'Traditional Liquid Equities - High Market Cap',
        'activity_level': 'CONSISTENT',
        'tier': 2,
        'priority': 'MEDIUM_HIGH',
        'notes': 'High market cap stocks - Always liquid',
        'symbols': [
            'RELIANCE', 'HDFCBANK', 'ICICIBANK', 'INFY', 'TCS', 'HDFC',
            'SBIN', 'KOTAKBANK', 'BAJFINANCE', 'HINDUNILVR', 'ITC'
        ]
    }
}


def get_category_metadata(category: str) -> dict:
    """Get metadata for a specific category"""
    return ACTIVITY_BY_CATEGORY.get(category, {})


def get_all_tier_1_categories() -> List[str]:
    """Get all Tier 1 category names"""
    return [
        cat for cat, data in ACTIVITY_BY_CATEGORY.items()
        if data.get('tier') == 1
    ]


def get_all_tier_2_categories() -> List[str]:
    """Get all Tier 2 category names"""
    return [
        cat for cat, data in ACTIVITY_BY_CATEGORY.items()
        if data.get('tier') == 2
    ]


def get_category_symbols(category: str) -> List[str]:
    """Get all symbols for a category"""
    metadata = get_category_metadata(category)
    if 'symbols' in metadata:
        return metadata['symbols']
    elif 'top_volatile' in metadata:
        return [item['symbol'] for item in metadata['top_volatile']]
    return []


def get_activity_summary() -> dict:
    """Get summary of all activity categories"""
    summary = {
        'tier_1_categories': {},
        'tier_2_categories': {},
        'total_tier_1_symbols': 0,
        'total_tier_2_symbols': 0
    }
    
    for category, metadata in ACTIVITY_BY_CATEGORY.items():
        tier = metadata.get('tier')
        if tier == 1:
            summary['tier_1_categories'][category] = metadata
            summary['total_tier_1_symbols'] += len(get_category_symbols(category))
        elif tier == 2:
            summary['tier_2_categories'][category] = metadata
            if 'top_volatile' in metadata:
                summary['total_tier_2_symbols'] += len(metadata['top_volatile'])
            else:
                summary['total_tier_2_symbols'] += len(get_category_symbols(category))
    
    return summary

