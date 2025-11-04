"""
VIX Utilities - India VIX data access for retail validation
Provides easy access to current VIX values and market regime detection.

This module is designed to be resilient in environments where the
`redis` package is not installed or a Redis server is not available.
In such cases, it degrades gracefully and simply returns None/UNKNOWN
for VIX queries rather than raising ImportError at import time.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Import field mapping utilities for canonical field names
try:
    from utils.yaml_field_loader import resolve_session_field, get_field_mapping_manager
    FIELD_MAPPING_AVAILABLE = True
except ImportError:
    FIELD_MAPPING_AVAILABLE = False
    def resolve_session_field(field_name: str) -> str:
        return field_name  # Fallback to original field name

# Optional imports: redis and duckdb may not be installed in all environments
try:
    import redis  # type: ignore
    _REDIS_AVAILABLE = True
except Exception:  # ImportError or any other failure
    redis = None  # type: ignore
    _REDIS_AVAILABLE = False

try:
    import duckdb  # type: ignore
    _DUCKDB_AVAILABLE = True
except Exception:  # ImportError or any other failure
    duckdb = None  # type: ignore
    _DUCKDB_AVAILABLE = False

logger = logging.getLogger(__name__)

class VIXUtils:
    """Utility class for accessing India VIX data"""
    
    def __init__(self, redis_client=None):
        """Initialize VIX utilities with optional Redis client.

        If a client is not provided and the `redis` package is unavailable or a
        connection cannot be established, the instance will operate in a
        no-Redis mode where `get_current_vix()` returns None.
        """
        self.redis_client = redis_client
        if not self.redis_client and _REDIS_AVAILABLE:
            try:
                # Use centralized Redis configuration
                from redis_files.redis_client import get_redis_client
                self.redis_client = get_redis_client()
                # Test connection
                if self.redis_client:
                    # Get client for realtime DB (where indices are stored)
                    try:
                        from redis_files.redis_config import get_database_for_data_type
                        indices_db = get_database_for_data_type("equity_prices")  # Indices stored with prices in realtime DB
                        test_client = self.redis_client.get_client(indices_db)
                        if test_client:
                            test_client.ping()
                    except Exception:
                        # Fallback to default client
                        if hasattr(self.redis_client, 'ping'):
                            self.redis_client.ping()
            except Exception as e:
                logger.debug(f"Redis connection failed (VIX utils will operate in no-Redis mode): {e}")
                self.redis_client = None
    
    def get_current_vix(self) -> Optional[Dict[str, Any]]:
        """
        Get current India VIX value and regime from Redis database
        
        Returns:
            Dict with 'value', 'regime', 'timestamp' or None if unavailable
        """
        try:
            if not self.redis_client:
                logger.debug("No Redis client available for VIX data (operating in no-Redis mode)")
                return None
            
            # Try to get from Redis using actual keys from the system
            # Based on system analysis of how VIX data is stored
            vix_data = None
            key_used = None
            
            # Get appropriate client for realtime DB (where indices are stored)
            try:
                from redis_files.redis_config import get_database_for_data_type
                indices_db = get_database_for_data_type("equity_prices")  # Indices stored in realtime DB
                client = self.redis_client.get_client(indices_db) if hasattr(self.redis_client, 'get_client') else self.redis_client
            except Exception:
                client = self.redis_client
            
            for key in [
                "index:NSE:INDIA VIX",  # Primary key from IndexDataUpdater
                "market_data:indices:nse_india_vix",  # Secondary key from IndexDataUpdater
                "index_data:indiavix",  # normalized fallback
                "index_data:india_vix",  # fallback
                "market_data:indices:nse_india vix",  # legacy with space
            ]:
                try:
                    vix_data = client.get(key) if hasattr(client, 'get') else None
                    if vix_data:
                        key_used = key
                        logger.debug(f"Found VIX data in Redis key: {key}")
                        break
                except Exception as e:
                    logger.debug(f"Error checking Redis key {key}: {e}")
                    continue
            
            if vix_data:
                try:
                    data = json.loads(vix_data)
                except Exception as e:
                    logger.error(f"Error parsing VIX data from Redis: {e}")
                    return None
                
                # Extract VIX value from various possible fields (using canonical field names)
                vix_value = (
                    data.get(resolve_session_field('last_price')) or 
                    data.get('value') or  # Legacy field
                    data.get('close') or  # Legacy field
                    data.get('price') or  # Legacy field
                    0
                )
                
                if vix_value == 0:
                    logger.warning(f"VIX value is 0 from Redis key {key_used}")
                    return None
                
                return {
                    'value': float(vix_value),
                    'regime': self._classify_vix_regime(float(vix_value)),
                    'timestamp': data.get('timestamp', datetime.now().isoformat()),
                    'source': 'redis',
                    'key_used': key_used
                }
            else:
                logger.debug("No VIX data found in any Redis keys")
                return None
            
        except Exception as e:
            logger.error(f"Error getting current VIX from Redis: {e}")
            return None
    
    def _classify_vix_regime(self, vix_value: float) -> str:
        """Classify VIX value into market regime (4 regimes) - CONSISTENT with config/thresholds.py"""
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
    
    def is_vix_panic(self) -> bool:
        """Check if VIX indicates panic mode (>22)"""
        vix_data = self.get_current_vix()
        return vix_data and vix_data['regime'] == 'PANIC'
    
    def is_vix_complacent(self) -> bool:
        """Check if VIX indicates complacent mode (<10)"""
        vix_data = self.get_current_vix()
        return vix_data and vix_data['regime'] == 'LOW'
    
    def get_vix_regime(self) -> str:
        """Get current VIX regime"""
        vix_data = self.get_current_vix()
        return vix_data['regime'] if vix_data else 'UNKNOWN'
    
    def get_vix_value(self) -> Optional[float]:
        """Get current VIX value"""
        vix_data = self.get_current_vix()
        return vix_data['value'] if vix_data else None


# Global VIX utilities instance
_vix_utils = None

def get_vix_utils(redis_client=None) -> VIXUtils:
    """Get global VIX utilities instance, optionally with Redis client"""
    global _vix_utils
    if _vix_utils is None or (redis_client is not None and _vix_utils.redis_client is None):
        _vix_utils = VIXUtils(redis_client=redis_client)
    return _vix_utils

def get_current_vix() -> Optional[Dict[str, Any]]:
    """Convenience function to get current VIX data"""
    return get_vix_utils().get_current_vix()

def is_vix_panic() -> bool:
    """Convenience function to check if VIX indicates panic"""
    return get_vix_utils().is_vix_panic()

def is_vix_complacent() -> bool:
    """Convenience function to check if VIX indicates complacency"""
    return get_vix_utils().is_vix_complacent()

def get_vix_regime() -> str:
    """Convenience function to get VIX regime"""
    return get_vix_utils().get_vix_regime()

def get_vix_value() -> Optional[float]:
    """Convenience function to get VIX value"""
    return get_vix_utils().get_vix_value()

