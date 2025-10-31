"""
Volume and Confidence Thresholds
================================

Centralized configuration for all volume-based calculations and confidence thresholds
with VIX regime-based adjustments.
"""

from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Base thresholds for Indian Nifty50 market
BASE_THRESHOLDS = {
    # 8 Core Pattern thresholds (adjusted for Indian market reality)
    'volume_spike': 2.2,          # Was 2.0 - Indian stocks need higher volume spikes
    'volume_breakout': 1.8,       # Was 1.5
    'volume_price_divergence': 1.5, # Was 1.3
    'upside_momentum': 1.3,       # Was 1.2
    'downside_momentum': 1.3,     # Was 1.2
    'breakout': 1.8,              # Was 1.5
    'reversal': 1.6,              # Was 1.3
    'hidden_accumulation': 1.6,   # Was 1.3
    
    # ICT Pattern thresholds (6 patterns with mathematical integrity)
    'ict_liquidity_pools': 1.8,        # Volume threshold for liquidity pools
    'ict_fair_value_gaps': 1.5,        # Volume threshold for FVG detection
    'ict_optimal_trade_entry': 1.6,    # Volume threshold for OTE zones
    'ict_premium_discount': 1.4,       # Volume threshold for premium/discount zones
    'ict_killzone': 1.2,               # Volume threshold for killzone detection
    'ict_momentum': 1.5,               # Volume threshold for momentum patterns
    
    # ICT Confidence thresholds
    'ict_liquidity_confidence': 0.75,
    'ict_fvg_confidence': 0.80,
    'ict_ote_confidence': 0.70,
    'ict_premium_discount_confidence': 0.75,
    'ict_killzone_confidence': 0.85,
    'ict_momentum_confidence': 0.70,
    
    # Straddle Strategy thresholds (4 patterns + 1 Kow Signal Straddle with mathematical integrity)
    'iv_crush_play_straddle': 1.8,     # Volume threshold for IV crush detection
    'range_bound_strangle': 1.4,       # Volume threshold for range-bound strangle
    'market_maker_trap_detection': 2.0, # Volume threshold for MM trap detection
    'premium_collection_strategy': 1.6, # Volume threshold for premium collection
    'kow_signal_straddle': 1.7,        # Volume threshold for Kow Signal Straddle
    
    # Straddle Confidence thresholds
    'straddle_entry_confidence': 0.85,
    'straddle_vwap_threshold': 0.95,  # Combined premium below 95% of VWAP
    'straddle_volume_threshold': 1.5,  # 1.5x average volume for entry
    'straddle_profit_target': 0.5,     # 50% profit target
    'straddle_stop_loss': 1.0,        # 100% stop loss (premium loss)
    'straddle_max_hold_hours': 4,     # Maximum 4 hours hold time
    
    # Kow Signal Straddle Strategy Configuration
    'kow_signal_entry_time': '09:30',
    'kow_signal_exit_time': '15:15',
    'kow_signal_confirmation_periods': 1,
    'kow_signal_max_reentries': 2,
    'kow_signal_vwap_period': 20,
    'kow_signal_underlying_symbols': ['NIFTY', 'BANKNIFTY'],
    'kow_signal_strike_selection': 'atm',
    'kow_signal_expiry_preference': 'weekly',
    'kow_signal_max_risk_per_trade': 0.03,
    'kow_signal_position_sizing': 'turtle_trading',
    'kow_signal_confidence_threshold': 0.85,
    'kow_signal_telegram_routing': True,
    
    # Alert filter thresholds - FIXED: Removed erroneous division by 375
    'path1_min_vol': 2.2,  # 2.2x average volume (adjusted for Indian market)
    'proven_patterns_ratio': 1.8,
    'pressure_patterns_ratio': 0.8,
    'balanced_play_ratio': 2.0,
    
    # Confidence thresholds (adjusted for public channel)
    'min_confidence': 0.70,       # Was 0.60 - higher for public signals
    'high_confidence': 0.85,
    'medium_confidence': 0.80,
    'news_enhanced_confidence': 0.80,
    'proven_patterns_confidence': 0.80,
    'pressure_patterns_confidence': 0.80,
    
    # News enhancement additive boosts (CONSERVATIVE - Prevent False Signals)
    'news_high_impact_boost': 0.05,          # Maximum 5% boost for high-impact news
    'news_medium_impact_boost': 0.02,        # 2% boost for medium-impact news
    'news_volume_trigger_boost': 0.02,      # 2% boost for volume-triggering news
    'news_positive_sentiment_boost': 0.01,   # 1% boost for positive sentiment
    'news_negative_sentiment_boost': -0.01, # 1% reduction for negative sentiment
    'news_max_total_boost': 0.08,            # HARD CAP: Never exceed 8% total boost
    'news_max_negative_boost': -0.03,       # Maximum negative boost
    
    # Move thresholds (realistic for Indian stocks)
    'min_move': 0.30,             # Was 0.25 - account for tick size
    'volume_spike_min_move': 0.35,
    'proven_patterns_min_move': 0.20,
    
    # Pattern Risk Categories (for risk-adjusted position sizing)
    'pattern_risk_categories': {
        'LOW_RISK': ['volume_breakout', 'breakout'],
        'MEDIUM_RISK': ['volume_spike', 'reversal'], 
        'HIGH_RISK': ['hidden_accumulation', 'volume_price_divergence']
    },
    
    # Risk-adjusted position sizing multipliers
    'risk_position_multipliers': {
        'LOW_RISK': 1.0,    # Full position size
        'MEDIUM_RISK': 0.7,  # 70% position size
        'HIGH_RISK': 0.4,    # 40% position size
    },
    
    # Risk-adjusted confidence requirements
    'risk_confidence_requirements': {
        'LOW_RISK': 0.65,   # Lower confidence threshold for low-risk patterns
        'MEDIUM_RISK': 0.70, # Standard confidence threshold
        'HIGH_RISK': 0.80,   # Higher confidence threshold for high-risk patterns
    },
    'pressure_patterns_min_move': 0.04,
    'balanced_play_min_move': 0.45,
    
    # Volume duration thresholds
    'volume_spike_min_duration': 3,    # Must sustain for 3+ minutes
    'volume_breakout_min_duration': 5, # 5+ minutes for breakouts
    
    # Legacy patterns removed - using 8 core patterns only
    
    # Legacy sector-specific thresholds (kept for backward compatibility)
    'banking_volume_spike': 2.8,
    'it_volume_spike': 2.2,
    'pharma_volume_spike': 2.0,
    'auto_volume_spike': 2.5,
    'energy_volume_spike': 2.3,
    'metal_volume_spike': 2.7,
    'cement_volume_spike': 2.1,
}

# Expand sector thresholds for all patterns
SECTOR_SPECIFIC_THRESHOLDS = {
    'banking': {
        'volume_spike': 2.8,    # Banking stocks have higher average volume
        'volume_breakout': 2.0,
        'volume_price_divergence': 1.8,
        'upside_momentum': 1.5,
        'downside_momentum': 1.5,
        'breakout': 2.0,
        'reversal': 1.8,
        'hidden_accumulation': 2.2,  # Banking often has hidden accumulation
    },
    'it_technology': {
        'volume_spike': 2.2,    # IT stocks moderate volume
        'volume_breakout': 1.8,
        'volume_price_divergence': 1.5,
        'upside_momentum': 1.3,
        'downside_momentum': 1.3,
        'breakout': 1.8,
        'reversal': 1.5,
        'hidden_accumulation': 1.8,
    },
    'pharma_healthcare': {
        'volume_spike': 2.0,    # Pharma typically lower volume
        'volume_breakout': 1.6,
        'volume_price_divergence': 1.4,
        'upside_momentum': 1.2,
        'downside_momentum': 1.2,
        'breakout': 1.6,
        'reversal': 1.4,
        'hidden_accumulation': 1.6,
    },
    'auto_automobile': {
        'volume_spike': 2.5,    # Auto sector moderate-high volume
        'volume_breakout': 1.9,
        'volume_price_divergence': 1.6,
        'upside_momentum': 1.4,
        'downside_momentum': 1.4,
        'breakout': 1.9,
        'reversal': 1.6,
        'hidden_accumulation': 2.0,
    },
    'energy_power': {
        'volume_spike': 2.3,    # Energy sector moderate volume
        'volume_breakout': 1.7,
        'volume_price_divergence': 1.5,
        'upside_momentum': 1.3,
        'downside_momentum': 1.3,
        'breakout': 1.7,
        'reversal': 1.5,
        'hidden_accumulation': 1.8,
    },
    'metal_mining': {
        'volume_spike': 2.7,    # Metals have high volatility
        'volume_breakout': 2.1,
        'volume_price_divergence': 1.8,
        'upside_momentum': 1.6,
        'downside_momentum': 1.6,
        'breakout': 2.1,
        'reversal': 1.8,
        'hidden_accumulation': 2.3,
    },
    'cement_infrastructure': {
        'volume_spike': 2.1,    # Cement moderate volume
        'volume_breakout': 1.7,
        'volume_price_divergence': 1.4,
        'upside_momentum': 1.2,
        'downside_momentum': 1.2,
        'breakout': 1.7,
        'reversal': 1.4,
        'hidden_accumulation': 1.7,
    },
    'fmcg_consumer': {
        'volume_spike': 1.9,    # FMCG typically lower volume
        'volume_breakout': 1.5,
        'volume_price_divergence': 1.3,
        'upside_momentum': 1.1,
        'downside_momentum': 1.1,
        'breakout': 1.5,
        'reversal': 1.3,
        'hidden_accumulation': 1.5,
    },
}

# Realistic Indian VIX ranges (10-25 typical, rarely below 10 or above 35)
INDIAN_VIX_RANGES = {
    'LOW': (10, 15),      # 10-15 (not <10 - that's extremely rare)
    'NORMAL': (15, 20),   # 15-20 
    'HIGH': (20, 25),     # 20-25
    'PANIC': (25, 50)     # 25+ (market crisis)
}

# Indian VIX-aware multipliers (CORRECTED - Market Microstructure Reality)
VIX_REGIME_MULTIPLIERS = {
    'PANIC': {  # VIX 25+ - High noise, require STRONGER signals
        'volume_spike': 1.8,      # HIGHER threshold (1.8x base) - need stronger signals in chaos
        'volume_breakout': 2.0,   # HIGHER threshold
        'volume_price_divergence': 1.8,
        'upside_momentum': 1.8,
        'downside_momentum': 1.8,
        'breakout': 2.0,
        'reversal': 1.8,
        'hidden_accumulation': 1.8,
        'min_confidence': 0.6,    # LOWER confidence (market is chaotic)
        'min_move': 1.5,          # HIGHER move required
        # Straddle strategy adjustments for panic
        'straddle_entry_confidence': 0.95,   # Very high confidence needed
        'straddle_vwap_threshold': 0.80,    # More lenient VWAP threshold
        'straddle_volume_threshold': 1.0,   # Lower volume requirement
    },
    'HIGH': {   # VIX 20-25 - Elevated noise, moderately higher thresholds
        'volume_spike': 1.3,      # Moderately higher threshold
        'volume_breakout': 1.4,
        'volume_price_divergence': 1.3,
        'upside_momentum': 1.3,
        'downside_momentum': 1.3,
        'breakout': 1.4,
        'reversal': 1.3,
        'hidden_accumulation': 1.3,
        'min_confidence': 0.65,   # Slightly lower confidence
        'min_move': 1.2,          # Moderately higher move required
        # Straddle strategy adjustments for high volatility
        'straddle_entry_confidence': 0.88,
        'straddle_vwap_threshold': 0.92,
        'straddle_volume_threshold': 1.3,
    },
    'NORMAL': { # VIX 15-20 - Base thresholds
        'volume_spike': 1.0,     # Base thresholds
        'volume_breakout': 1.0,
        'volume_price_divergence': 1.0,
        'upside_momentum': 1.0,
        'downside_momentum': 1.0,
        'breakout': 1.0,
        'reversal': 1.0,
        'hidden_accumulation': 1.0,
        'min_confidence': 0.7,
        'min_move': 1.0,
        # Straddle strategy base thresholds
        'straddle_entry_confidence': 0.85,
        'straddle_vwap_threshold': 0.95,
        'straddle_volume_threshold': 1.5,
    },
    'LOW': {    # VIX 10-15 - Low noise, can use LOWER thresholds
        'volume_spike': 0.7,      # LOWER threshold (easier to detect in quiet markets)
        'volume_breakout': 0.8,
        'volume_price_divergence': 0.7,
        'upside_momentum': 0.7,
        'downside_momentum': 0.7,
        'breakout': 0.8,
        'reversal': 0.7,
        'hidden_accumulation': 0.7,
        'min_confidence': 0.75,   # HIGHER confidence (markets are predictable)
        'min_move': 0.8,          # SMALLER moves significant
        # Straddle strategy adjustments
        'straddle_entry_confidence': 0.9,    # Higher confidence needed in low vol
        'straddle_vwap_threshold': 0.90,     # Stricter VWAP threshold
        'straddle_volume_threshold': 2.0,    # Higher volume requirement
    }
}

def classify_indian_vix_regime(vix_value: float) -> str:
    """
    Classify VIX value according to realistic Indian market ranges.
    
    Args:
        vix_value: Current VIX value
        
    Returns:
        VIX regime: LOW, NORMAL, HIGH, or PANIC
    """
    if vix_value >= 25:
        return 'PANIC'
    elif vix_value >= 20:
        return 'HIGH'
    elif vix_value >= 15:
        return 'NORMAL'
    elif vix_value >= 10:
        return 'LOW'
    else:
        # VIX < 10 is extremely rare in Indian market
        return 'LOW'  # Treat as low volatility

def normalize_vix_regime(regime: str) -> str:
    """
    Normalize any VIX regime name to the standard 4-regime system.
    
    Args:
        regime: Raw regime name from any source
        
    Returns:
        Normalized regime name (PANIC, HIGH, NORMAL, LOW)
    """
    regime = regime.upper()
    
    # Map all variations to 4 standard regimes
    regime_mapping = {
        'COMPLACENT': 'LOW',      # VIX < 12
        'LOW_VOL': 'LOW',         # VIX < 12  
        'ELEVATED': 'HIGH',       # VIX 15-20
        'HIGH_VOL': 'HIGH',       # VIX 15-20
        'PANIC': 'PANIC',         # VIX > 25
        'CRASH': 'PANIC',         # VIX > 25
    }
    
    normalized = regime_mapping.get(regime, regime)
    
    # Only these 4 regimes are valid
    valid_regimes = ['PANIC', 'HIGH', 'NORMAL', 'LOW']
    if normalized not in valid_regimes:
        logger.warning(f"Unknown VIX regime '{regime}', defaulting to NORMAL")
        return 'NORMAL'
    
    return normalized

# Pattern-specific confidence boosts for 8 core patterns (REALISTIC - Based on Indian Market Backtesting)
PATTERN_CONFIDENCE_BOOSTS = {
    # High-probability patterns in India
    'volume_breakout': 0.08,      # Volume + breakout = reliable
    'breakout': 0.06,             # Clean breakouts work
    
    # Medium-probability  
    'volume_spike': 0.04,         # Needs strong confirmation
    'reversal': 0.03,             # Reversals are tricky in India
    
    # Lower-probability (penalize these)
    'upside_momentum': 0.02,
    'downside_momentum': 0.02,
    'hidden_accumulation': 0.00,  # No boost - too hard to detect
    'volume_price_divergence': -0.02, # 🚨 NEGATIVE boost - often false
}

def get_volume_threshold(pattern_name: str, sector: str = None, vix_regime: str = 'NORMAL') -> float:
    """Get volume threshold for a pattern adjusted by VIX regime and sector."""
    normalized_regime = normalize_vix_regime(vix_regime)
    
    # Map pattern names to actual BASE_THRESHOLDS keys (8 core patterns)
    pattern_mapping = {
        'volume_spike': 'volume_spike',
        'volume_breakout': 'volume_breakout',
        'volume_price_divergence': 'volume_price_divergence',
        'upside_momentum': 'upside_momentum',
        'downside_momentum': 'downside_momentum',
        'breakout': 'breakout',
        'reversal': 'reversal',
        'hidden_accumulation': 'hidden_accumulation',
    }
    
    # Get the actual key from BASE_THRESHOLDS
    actual_key = pattern_mapping.get(pattern_name, pattern_name)
    base_threshold = BASE_THRESHOLDS.get(actual_key, 1.0)
    
    # Apply sector-specific adjustment if sector provided
    if sector:
        sector_key = f"{sector.lower()}_volume_spike"
        if sector_key in BASE_THRESHOLDS and pattern_name == 'volume_spike':
            base_threshold = BASE_THRESHOLDS[sector_key]
    
    # Get regime multiplier for the pattern
    regime_multiplier = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {}).get(pattern_name, 1.0)
    
    return base_threshold * regime_multiplier

def get_confidence_threshold(pattern_name: str, vix_regime: str = 'NORMAL') -> float:
    """Get confidence threshold for a pattern adjusted by VIX regime."""
    normalized_regime = normalize_vix_regime(vix_regime)
    base_confidence = BASE_THRESHOLDS.get('min_confidence', 0.6)
    regime_multiplier = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {}).get('min_confidence', 1.0)
    
    return base_confidence * regime_multiplier

def get_move_threshold(pattern_name: str, vix_regime: str = 'NORMAL') -> float:
    """Get move threshold for a pattern adjusted by VIX regime."""
    normalized_regime = normalize_vix_regime(vix_regime)
    base_move = BASE_THRESHOLDS.get('min_move', 0.25)
    regime_multiplier = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {}).get('min_move', 1.0)
    
    return base_move * regime_multiplier

def get_all_thresholds_for_regime(vix_regime: str = 'NORMAL') -> Dict[str, float]:
    """Get all thresholds adjusted for a specific VIX regime."""
    normalized_regime = normalize_vix_regime(vix_regime)
    regime_multipliers = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {})
    
    adjusted_thresholds = {}
    for pattern, base_value in BASE_THRESHOLDS.items():
        # Only apply multipliers to numeric values
        if isinstance(base_value, (int, float)):
            multiplier = regime_multipliers.get(pattern, 1.0)
            adjusted_thresholds[pattern] = base_value * multiplier
        else:
            # Keep non-numeric values as-is (strings, lists, etc.)
            adjusted_thresholds[pattern] = base_value
    
    return adjusted_thresholds

def get_pattern_confidence_boost(pattern_name: str) -> float:
    """
    Get confidence boost for a pattern based on historical performance.
    
    Args:
        pattern_name: Name of the pattern
        
    Returns:
        Confidence boost value (0.0 to 1.0)
    """
    return PATTERN_CONFIDENCE_BOOSTS.get(pattern_name, 0.0)

def get_sector_threshold(sector: str, pattern_name: str, vix_regime: str = 'NORMAL') -> float:
    """
    Get sector-specific threshold for a pattern adjusted by VIX regime.
    
    Args:
        sector: Sector name (banking, it_technology, pharma_healthcare, etc.)
        pattern_name: Pattern name
        vix_regime: VIX regime for adjustment
        
    Returns:
        Sector and VIX-adjusted threshold
    """
    normalized_regime = normalize_vix_regime(vix_regime)
    
    # Get sector base threshold
    sector_patterns = SECTOR_SPECIFIC_THRESHOLDS.get(sector, {})
    base_threshold = sector_patterns.get(pattern_name, BASE_THRESHOLDS.get(pattern_name, 1.0))
    
    # Apply VIX regime multiplier
    regime_multiplier = VIX_REGIME_MULTIPLIERS.get(normalized_regime, {}).get(pattern_name, 1.0)
    
    return base_threshold * regime_multiplier

def is_high_confidence_pattern(pattern_name: str) -> bool:
    """
    Check if a pattern is considered high-confidence (8 core patterns).
    
    Args:
        pattern_name: Name of the pattern
        
    Returns:
        True if high-confidence pattern
    """
    high_confidence_patterns = [
        'reversal',
        'volume_breakout',
        'breakout',
    ]
    
    return pattern_name in high_confidence_patterns

def get_news_aware_confidence(pattern: Dict, news_context: Optional[Dict]) -> float:
    """
    Adjust confidence based on news alignment and sentiment - SAFE ADDITIVE BOOSTS.
    
    Args:
        pattern: Pattern dictionary with confidence and action
        news_context: News context with sentiment and impact data
        
    Returns:
        News-adjusted confidence (capped at 0.95)
    """
    base_confidence = pattern.get('confidence', 0.0)
    
    if not news_context:
        return base_confidence  # No news, no adjustment
    
    # Get news enhancement additive boosts from BASE_THRESHOLDS
    high_impact_boost = BASE_THRESHOLDS.get('news_high_impact_boost', 0.06)
    medium_impact_boost = BASE_THRESHOLDS.get('news_medium_impact_boost', 0.03)
    volume_trigger_boost = BASE_THRESHOLDS.get('news_volume_trigger_boost', 0.03)
    positive_sentiment_boost = BASE_THRESHOLDS.get('news_positive_sentiment_boost', 0.02)
    negative_sentiment_boost = BASE_THRESHOLDS.get('news_negative_sentiment_boost', -0.02)
    max_total_boost = BASE_THRESHOLDS.get('news_max_total_boost', 0.10)
    max_negative_boost = BASE_THRESHOLDS.get('news_max_negative_boost', -0.05)
    
    # SEPARATE NEWS ALERTS FROM PATTERN BOOSTS
    # 1. Send independent news alert (unchanged)
    if news_context.get('impact') == 'HIGH':
        # News alert is handled separately by alert_manager
        pass
    
    # 2. Apply SAFE boost to pattern (if pattern exists and is decent)
    if base_confidence >= 0.60:  # Only boost patterns that are already decent
        safe_boost = calculate_news_boost(news_context, high_impact_boost, medium_impact_boost, volume_trigger_boost, positive_sentiment_boost, negative_sentiment_boost, max_total_boost, max_negative_boost)
        enhanced_confidence = apply_news_boost(base_confidence, safe_boost)
        return enhanced_confidence
    else:
        return base_confidence  # Don't boost weak patterns


def calculate_news_boost(news_context: dict, high_impact_boost: float, medium_impact_boost: float, volume_trigger_boost: float, positive_sentiment_boost: float, negative_sentiment_boost: float, max_total_boost: float, max_negative_boost: float) -> float:
    """Calculate safe, capped news confidence boost"""
    base_boost = 0.0
    
    # Additive boosts (not multiplicative)
    impact = news_context.get('impact', 'LOW')
    if impact == 'HIGH':
        base_boost += high_impact_boost
    elif impact == 'MEDIUM':
        base_boost += medium_impact_boost
    
    if news_context.get('volume_trigger'):
        base_boost += volume_trigger_boost
    
    # Sentiment-based adjustment
    sentiment = news_context.get('sentiment', 0)
    if isinstance(sentiment, str):
        if sentiment == 'positive':
            base_boost += positive_sentiment_boost
        elif sentiment == 'negative':
            base_boost += negative_sentiment_boost
    elif isinstance(sentiment, (int, float)):
        if sentiment > 0.2:  # Positive sentiment
            base_boost += positive_sentiment_boost
        elif sentiment < -0.2:  # Negative sentiment
            base_boost += negative_sentiment_boost
    
    # HARD CAP: Never exceed max boost limits
    return max(max_negative_boost, min(max_total_boost, base_boost))


def get_pattern_risk_category(pattern_type: str) -> str:
    """
    Get risk category for a pattern type
    
    Args:
        pattern_type: Pattern type (e.g., 'volume_breakout', 'reversal')
        
    Returns:
        Risk category: 'LOW_RISK', 'MEDIUM_RISK', or 'HIGH_RISK'
    """
    pattern_risk_categories = BASE_THRESHOLDS.get('pattern_risk_categories', {})
    
    for risk_category, patterns in pattern_risk_categories.items():
        if pattern_type in patterns:
            return risk_category
    
    # Default to medium risk for unknown patterns
    return 'MEDIUM_RISK'


def get_risk_adjusted_position_multiplier(pattern_type: str) -> float:
    """
    Get risk-adjusted position sizing multiplier for a pattern
    
    Args:
        pattern_type: Pattern type
        
    Returns:
        Position size multiplier (0.4 to 1.0)
    """
    risk_category = get_pattern_risk_category(pattern_type)
    risk_multipliers = BASE_THRESHOLDS.get('risk_position_multipliers', {})
    
    return risk_multipliers.get(risk_category, 0.7)  # Default to medium risk


def get_risk_adjusted_confidence_requirement(pattern_type: str) -> float:
    """
    Get risk-adjusted confidence requirement for a pattern
    
    Args:
        pattern_type: Pattern type
        
    Returns:
        Minimum confidence requirement (0.65 to 0.80)
    """
    risk_category = get_pattern_risk_category(pattern_type)
    confidence_requirements = BASE_THRESHOLDS.get('risk_confidence_requirements', {})
    
    return confidence_requirements.get(risk_category, 0.70)  # Default to medium risk


def apply_risk_adjustments(pattern_data: dict) -> dict:
    """
    Apply risk adjustments to pattern data
    
    Args:
        pattern_data: Pattern detection data
        
    Returns:
        Pattern data with risk adjustments applied
    """
    pattern_type = pattern_data.get('pattern', 'unknown')
    
    # Get risk category and adjustments
    risk_category = get_pattern_risk_category(pattern_type)
    position_multiplier = get_risk_adjusted_position_multiplier(pattern_type)
    confidence_requirement = get_risk_adjusted_confidence_requirement(pattern_type)
    
    # Apply position size adjustment
    if 'position_size' in pattern_data:
        original_size = pattern_data['position_size']
        pattern_data['position_size'] = original_size * position_multiplier
        pattern_data['risk_adjusted_size'] = True
        pattern_data['original_position_size'] = original_size
    
    # Add risk metadata
    pattern_data['risk_category'] = risk_category
    pattern_data['risk_position_multiplier'] = position_multiplier
    pattern_data['risk_confidence_requirement'] = confidence_requirement
    
    # Check if pattern meets risk-adjusted confidence requirement
    current_confidence = pattern_data.get('confidence', 0.0)
    pattern_data['meets_risk_requirement'] = current_confidence >= confidence_requirement
    
    return pattern_data


def apply_news_boost(pattern_confidence: float, news_boost: float) -> float:
    """Apply boost without distortion"""
    boosted_confidence = pattern_confidence + news_boost
    return max(0.10, min(0.95, boosted_confidence))  # Keep within reasonable bounds

def get_kow_signal_strategy_config() -> Dict[str, Any]:
    """
    Get Kow Signal Straddle strategy configuration from thresholds.
    
    Returns:
        Dictionary with Kow Signal strategy configuration
    """
    return {
        'entry_time': BASE_THRESHOLDS.get('kow_signal_entry_time', '09:30'),
        'exit_time': BASE_THRESHOLDS.get('kow_signal_exit_time', '15:15'),
        'confirmation_periods': BASE_THRESHOLDS.get('kow_signal_confirmation_periods', 1),
        'max_reentries': BASE_THRESHOLDS.get('kow_signal_max_reentries', 2),
        'vwap_period': BASE_THRESHOLDS.get('kow_signal_vwap_period', 20),
        'underlying_symbols': BASE_THRESHOLDS.get('kow_signal_underlying_symbols', ['NIFTY', 'BANKNIFTY']),
        'strike_selection': BASE_THRESHOLDS.get('kow_signal_strike_selection', 'atm'),
        'expiry_preference': BASE_THRESHOLDS.get('kow_signal_expiry_preference', 'weekly'),
        'max_risk_per_trade': BASE_THRESHOLDS.get('kow_signal_max_risk_per_trade', 0.03),
        'position_sizing': BASE_THRESHOLDS.get('kow_signal_position_sizing', 'turtle_trading'),
        'confidence_threshold': BASE_THRESHOLDS.get('kow_signal_confidence_threshold', 0.85),
        'telegram_routing': BASE_THRESHOLDS.get('kow_signal_telegram_routing', True)
    }

def get_threshold_summary() -> Dict[str, Any]:
    """
    Get a summary of all threshold configurations.
    
    Returns:
        Dictionary with threshold summary
    """
    return {
        'base_thresholds': BASE_THRESHOLDS,
        'vix_regime_multipliers': VIX_REGIME_MULTIPLIERS,
        'pattern_confidence_boosts': PATTERN_CONFIDENCE_BOOSTS,
        'kow_signal_strategy': get_kow_signal_strategy_config(),
        'total_patterns': len(BASE_THRESHOLDS),
        'vix_regimes': list(VIX_REGIME_MULTIPLIERS.keys()),
        'high_confidence_patterns': [
            pattern for pattern in BASE_THRESHOLDS.keys() 
            if is_high_confidence_pattern(pattern)
        ]
    }
