"""
Pattern normalisation helpers for the alert dashboard.

Centralises the mapping between raw pattern names coming from the
pipeline and the snake_case keys we use for filtering and lookups.

This module is synchronized with patterns/data/pattern_registry_config.json
to ensure consistency across the system.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional, Dict, List, Set

# Path to pattern registry config
# Updated for aion_trading_dashboard structure: backend/services/patterns.py
# Need to go up to aion_algo_trading root, then into intraday_trading/patterns
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # Go up to aion_algo_trading
PATTERN_REGISTRY_PATH = PROJECT_ROOT / "intraday_trading" / "patterns" / "data" / "pattern_registry_config.json"


@lru_cache(maxsize=1)
def _load_pattern_registry() -> Dict:
    """Load pattern registry config from JSON file."""
    try:
        with open(PATTERN_REGISTRY_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"⚠️ Warning: Could not load pattern registry: {e}")
        return {}


@lru_cache(maxsize=256)
def normalize_pattern_name(pattern: Optional[str]) -> str:
    """Normalise any pattern identifier to snake_case."""
    if not pattern:
        return "unknown"
    normalized = pattern.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized or "unknown"


@lru_cache(maxsize=256)
def pattern_display_label(pattern_key: str, original: Optional[str] = None) -> str:
    """Return a human-friendly label for a pattern key.
    
    Uses description from pattern registry if available, otherwise
    generates a label from the pattern key.
    """
    registry = _load_pattern_registry()
    pattern_configs = registry.get("pattern_configs", {})
    
    # Normalize the pattern key for lookup
    normalized_key = normalize_pattern_name(pattern_key)
    
    # Try to get description from registry
    if normalized_key in pattern_configs:
        description = pattern_configs[normalized_key].get("description", "")
        if description:
            return description
    
    # Fallback to generating label from key or original
    source = original or pattern_key or "unknown"
    text = source.replace("_", " ").strip()
    if not text:
        text = "unknown"
    words = []
    for word in text.split():
        if word.isupper():
            words.append(word)
        else:
            words.append(word.title())
    return " ".join(words)


def get_all_patterns() -> List[str]:
    """Get list of all enabled pattern keys from registry."""
    registry = _load_pattern_registry()
    pattern_configs = registry.get("pattern_configs", {})
    
    enabled_patterns = [
        key for key, config in pattern_configs.items()
        if config.get("enabled", True)
    ]
    return sorted(enabled_patterns)


def get_patterns_by_category(category: Optional[str] = None) -> Dict[str, List[str]]:
    """Get patterns grouped by category.
    
    Args:
        category: Optional category name to filter. If None, returns all categories.
        
    Returns:
        Dictionary mapping category names to lists of pattern keys.
    """
    registry = _load_pattern_registry()
    categories = registry.get("categories", {})
    pattern_configs = registry.get("pattern_configs", {})
    
    result = {}
    
    for cat_name, cat_data in categories.items():
        if not cat_data.get("enabled", True):
            continue
        if category and cat_name != category:
            continue
            
        patterns_in_category = []
        for pattern_key in cat_data.get("patterns", []):
            pattern_config = pattern_configs.get(pattern_key, {})
            if pattern_config.get("enabled", True):
                patterns_in_category.append(pattern_key)
        
        if patterns_in_category:
            result[cat_name] = sorted(patterns_in_category)
    
    return result


def get_pattern_metadata(pattern_key: str) -> Optional[Dict]:
    """Get metadata for a specific pattern.
    
    Returns:
        Dictionary with pattern metadata (enabled, category, description, etc.)
        or None if pattern not found.
    """
    registry = _load_pattern_registry()
    pattern_configs = registry.get("pattern_configs", {})
    
    normalized_key = normalize_pattern_name(pattern_key)
    return pattern_configs.get(normalized_key)


def is_pattern_enabled(pattern_key: str) -> bool:
    """Check if a pattern is enabled in the registry."""
    metadata = get_pattern_metadata(pattern_key)
    if metadata is None:
        return False
    return metadata.get("enabled", True)


def get_pattern_category(pattern_key: str) -> Optional[str]:
    """Get the category for a pattern."""
    metadata = get_pattern_metadata(pattern_key)
    if metadata is None:
        return None
    return metadata.get("category")


def get_pattern_description(pattern_key: str) -> Optional[str]:
    """Get the description for a pattern."""
    metadata = get_pattern_metadata(pattern_key)
    if metadata is None:
        return None
    return metadata.get("description")


def get_emergency_filter_tier(pattern_key: str) -> Optional[str]:
    """Get the emergency filter tier for a pattern (TIER_1, TIER_2, TIER_3, or None)."""
    metadata = get_pattern_metadata(pattern_key)
    if metadata is None:
        return None
    return metadata.get("emergency_filter_tier")


def get_emergency_filter_group_size(pattern_key: str) -> Optional[int]:
    """Get the emergency filter group size for a pattern."""
    metadata = get_pattern_metadata(pattern_key)
    if metadata is None:
        return None
    return metadata.get("emergency_filter_group_size")


def get_all_categories() -> List[str]:
    """Get list of all enabled category names."""
    registry = _load_pattern_registry()
    categories = registry.get("categories", {})
    
    return sorted([
        name for name, data in categories.items()
        if data.get("enabled", True)
    ])


def get_category_description(category: str) -> Optional[str]:
    """Get description for a category."""
    registry = _load_pattern_registry()
    categories = registry.get("categories", {})
    
    cat_data = categories.get(category, {})
    return cat_data.get("description")


# Registry metadata
def get_registry_metadata() -> Dict:
    """Get metadata about the pattern registry."""
    registry = _load_pattern_registry()
    return registry.get("metadata", {})


# ✅ UPDATED: Pattern lists aligned with pattern_registry_config.json (10-pattern registry)
CORE_PATTERNS = [
    "volume_spike",
    "volume_breakout",
    "volume_price_divergence",
    "momentum_continuation",  # ✅ UPDATED: Unified momentum pattern
    "breakout_pattern",  # ✅ UPDATED: Registry pattern name
    "reversal_pattern",  # ✅ UPDATED: Registry pattern name
    "spring_pattern",  # ✅ UPDATED: Wyckoff spring
    "coil_pattern",  # ✅ UPDATED: Wyckoff coil
    "hidden_accumulation"
]

# ✅ DEPRECATED: ICT Patterns (not in 10-pattern registry, kept for backward compatibility)
ICT_PATTERNS = [
    "ict_fair_value_gaps",
    "ict_optimal_trade_entry",
    "ict_premium_discount"
]

STRADDLE_PATTERNS = [
    "iv_crush_play_straddle",
    "range_bound_strangle",
    "market_maker_trap_detection",
    "premium_collection_strategy",
    "kow_signal_straddle"
]

OPTION_PATTERNS = [
    "delta_hedge_opportunity",
    "gamma_squeeze",
    "theta_decay_alert"
]

ALL_REGISTERED_PATTERNS = CORE_PATTERNS + ICT_PATTERNS + STRADDLE_PATTERNS + OPTION_PATTERNS
