"""
Trading System Utilities
========================

This package contains utilities for the intraday trading system.

Modules:
- calculations: Main calculations engine with Greek calculations
- yaml_field_loader: Direct YAML field mapping utilities
- market_calendar: Market calendar for trading days
- redis_calculations: Redis-based calculations
- time_utils: Time utilities
- timestamp_normalizer: Timestamp normalization
- update_all_20day_averages: 20-day average calculations
- validation_logger: Validation logging
- vix_regimes: VIX regime management
- vix_utils: VIX utilities
- volume_baselines: Volume baseline management
"""

# Import commonly used utilities
try:
    from intraday_scanner.calculations import HybridCalculations, get_tick_processor
except Exception:
    # Provide dummies if calculations module is unavailable at import-time
    HybridCalculations = None
    def get_tick_processor(*args, **kwargs):  # type: ignore
        return None
from .vix_utils import get_vix_value, get_vix_regime
from .vix_regimes import VIXRegimeManager

__all__ = [
    'HybridCalculations', 
    'get_tick_processor',
    'get_vix_value', 
    'get_vix_regime',
    'VIXRegimeManager'
]
