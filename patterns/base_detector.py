"""
Base Pattern Detection Class
============================

Base class for all pattern detection functionality.
Consolidates common pattern detection logic from existing pattern_detector.py.
"""

import logging
import math
import sys
import os
import json
from typing import Dict, Any, List, Optional, Set
from collections import deque
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import time

# Import VIX utilities for volatility-aware thresholds
try:
    from utils.vix_utils import get_vix_value, get_vix_regime
except ImportError:
    # Fallback for when scanner module is not available
    def get_vix_value():
        return None

    def get_vix_regime():
        return "UNKNOWN"

# MM patterns removed - using 8 core patterns only

# Import optional pattern detectors
# ICT patterns removed - using 8 core patterns only

try:
    from utils.vix_regimes import VIXRegimeManager
except ImportError:
    VIXRegimeManager = None

# Pattern schema functions (simplified fallbacks)
def create_pattern(pattern_data, indicators):
    """Create pattern with basic validation"""
    return pattern_data

def validate_pattern(pattern):
    """Basic pattern validation"""
    return True

# Import calculations utilities
# Removed legacy imports - using simplified pattern detection

logger = logging.getLogger(__name__)

class BasePatternDetector:
    """
    Base class for pattern detection with common functionality.
    Consolidates shared logic from existing pattern detection files.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None):
        """
        Initialize base pattern detector

        Args:
            config: Configuration dictionary
            redis_client: Redis client for market data
        """
        self.config = config or {}
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
        # 🎯 Data source tracking initialization
        self.logger.info("🎯 BasePatternDetector initialized - setting up data source tracking")

        # Load pattern configuration
        self.pattern_config = self._load_pattern_config()

        # Initialize pattern tracking
        self.pattern_counts = defaultdict(int)
        self.pattern_success_rates = defaultdict(float)

        # Load thresholds from config
        self.thresholds = self._load_thresholds()

        # Load dynamic volume baselines (20d/55d)
        self.volume_baselines = self._load_volume_baselines()
        
        # Historical data tracking for Wyckoff patterns (Spring/Coil)
        self.price_history = {}  # symbol -> list of prices
        self.high_history = {}   # symbol -> list of highs  
        self.low_history = {}    # symbol -> list of lows
        self.volume_history = {} # symbol -> list of volumes
        
        # Volume profile data (required for _find_support_resistance)
        self.price_volume = {}  # price -> volume mapping for volume profile

        # Load sector correlation groups for position sizing
        self.correlation_groups = self._load_correlation_groups()

        # Initialize VIX utilities for threshold updates
        self.vix_utils = None
        try:
            from utils.vix_utils import VIXUtils
            self.vix_utils = VIXUtils(redis_client)
            self.logger.debug("✅ VIX utilities initialized")
        except Exception as e:
            self.logger.warning(f"Could not initialize VIX utilities: {e}")
        
        # VIX-aware threshold adjustment will be handled by child classes

        # Stats for monitoring
        self.stats = {
            "invocations": 0,
            "patterns_found": 0,
            "missing_indicator_calls": 0,
            "errors": 0,
        }

    def _load_pattern_config(self) -> Dict[str, Any]:
        """Load pattern configuration from existing config files"""
        try:
            config_path = Path(__file__).parent / "data" / "pattern_registry_config.json"
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"Could not load pattern config: {e}")
        return {}

    def _load_thresholds(self) -> Dict[str, Any]:
        """Load thresholds from existing configuration"""
        try:
            from config.thresholds import get_all_thresholds_for_regime
            return get_all_thresholds_for_regime("NORMAL")
        except ImportError:
            return {}

    def _load_volume_baselines(self) -> Dict[str, Any]:
        """Load volume baselines from existing data (overridden by subclasses)"""
        # This method is overridden by PatternDetector which uses volume_averages_20d.json
        # No need to load sector_volatility.json as it's legacy
        return {}

    def _load_correlation_groups(self) -> Dict[str, List[str]]:
        """Load sector correlation groups from existing data"""
        try:
            # Try config directory first
            sector_path = Path(__file__).parent.parent / "config" / "nse_sector_mapping.json"
            if sector_path.exists():
                with open(sector_path, 'r') as f:
                    return json.load(f)
            else:
                self.logger.warning(f"Sector mapping file not found: {sector_path}")
        except Exception as e:
            self.logger.warning(f"Could not load correlation groups: {e}")
        return {}

    def _update_vix_aware_thresholds(self):
        """Update thresholds using centralized configuration"""
        try:
            from config.thresholds import get_all_thresholds_for_regime
            
            vix_val, vix_regime = self._get_vix_from_redis()
            
            if not vix_val:
                self.logger.debug("No VIX data available, using NORMAL regime")
                vix_regime = "NORMAL"

            # Get VIX-aware thresholds
            self.thresholds = get_all_thresholds_for_regime(vix_regime)
            self.logger.debug(f"Updated thresholds for VIX regime: {vix_regime}")
            
        except Exception as e:
            self.logger.warning(f"Could not update VIX-aware thresholds: {e}")
            # Fallback to NORMAL regime
            try:
                from config.thresholds import get_all_thresholds_for_regime
                self.thresholds = get_all_thresholds_for_regime("NORMAL")
            except Exception:
                self.logger.error("Failed to load even fallback thresholds")

    def _get_vix_from_redis(self) -> tuple[Optional[float], str]:
        """Get VIX value and regime from Redis"""
        try:
            if not self.redis_client:
                return None, "UNKNOWN"
            
            vix_data = self.redis_client.get('vix:current')
            if vix_data:
                import json
                vix_info = json.loads(vix_data)
                vix_value = vix_info.get('value', 0)
                vix_regime = vix_info.get('regime', 'NORMAL')
                return vix_value, vix_regime
        except Exception as e:
            self.logger.debug(f"Could not get VIX from Redis: {e}")
        
        return None, "UNKNOWN"

    def detect_patterns(self, symbol: str, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Base pattern detection method to be overridden by subclasses
        
        Args:
            symbol: Trading symbol
            indicators: Market indicators and data
            
        Returns:
            List of detected patterns
        """
        self.stats["invocations"] += 1
        
        try:
            # Base pattern detection logic would go here
            # This is a placeholder for the actual implementation
            patterns = []
            
            self.stats["patterns_found"] += len(patterns)
            return patterns
            
        except Exception as e:
            self.logger.error(f"Error in pattern detection for {symbol}: {e}")
            self.stats["errors"] += 1
            return []

    def get_profile_data(self) -> Dict[str, Any]:
        """Get volume profile data (required for _find_support_resistance)"""
        # This is a fallback implementation - child classes should override
        return {
            'poc_price': 0.0,
            'value_area_high': 0.0,
            'value_area_low': 0.0,
            'support_levels': [],
            'resistance_levels': []
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get detection statistics"""
        return self.stats.copy()
