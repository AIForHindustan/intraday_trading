"""
Unified Data Storage - Source of Truth for All Redis Storage Operations

This module provides a single, consistent interface for storing and retrieving
all trading data in Redis. All downstream code MUST use this class.

Features:
- Uses UniversalSymbolParser for symbol normalization (source of truth)
- Uses RedisKeyStandards for key generation (consistent key format)
- Unified structure for tick data, indicators, and Greeks
- Automatic TTL management
- Thread-safe operations
- Compatible with existing redis_gateway pattern
"""

import json
import time
import logging
from typing import Dict, Any, Optional, List

from redis_files.redis_client import RedisManager82

from redis_files.redis_key_standards import (
    RedisKeyStandards,
    get_symbol_parser,
    get_key_builder,
    _infer_exchange_prefix,
)

try:
    from redis_files.redis_client import redis_gateway
    REDIS_GATEWAY_AVAILABLE = True
except ImportError:
    REDIS_GATEWAY_AVAILABLE = False
    redis_gateway = None

logger = logging.getLogger(__name__)


class UnifiedDataStorage:
    """
    Unified Data Storage - Source of Truth for All Redis Storage Operations.
    
    All Redis storage operations MUST go through this class to ensure:
    - Consistent symbol normalization (via UniversalSymbolParser)
    - Consistent key format (via RedisKeyStandards)
    - Unified data structure across the system
    """
    
    def __init__(self, redis_url='redis://localhost:6379/1', redis_client=None, db: int = 1):
        """
        Initialize UnifiedDataStorage.
        
        Args:
            redis_url: Redis connection URL (default: redis://localhost:6379/1) - DEPRECATED, use redis_client
            redis_client: Optional Redis client instance (if provided, redis_url is ignored)
            db: Redis database number (default: 1 for live data)
        """
        if redis_client:
            self.redis = redis_client
        else:
            # ✅ SOURCE OF TRUTH: Use RedisManager82 for client creation
            self.redis = RedisManager82.get_client(process_name="unified_data_storage", db=db)
        
        self.parser = get_symbol_parser()
        self.key_builder = get_key_builder()
        
        # Use redis_gateway if available (for compatibility with existing system)
        self.redis_gateway = redis_gateway if REDIS_GATEWAY_AVAILABLE else None
        
    def _normalize_symbol(self, symbol: str) -> str:
        """
        Normalize symbol using RedisKeyStandards.canonical_symbol (source of truth).
        
        Returns canonical format: {exchange}{normalized} (e.g., NFOINDIGO25NOV5900CE)
        
        ✅ FIXED: Uses canonical_symbol() for consistency with rest of pipeline.
        """
        if not symbol:
            return symbol
        
        try:
            # ✅ FIXED: Use RedisKeyStandards.canonical_symbol() for consistency
            # This ensures the same canonical format is used throughout the pipeline
            return RedisKeyStandards.canonical_symbol(symbol)
        except Exception as e:
            logger.warning(f"Symbol normalization failed for {symbol}: {e}, using original")
            # Fallback: try basic normalization
            try:
                parsed = self.parser.parse_symbol(symbol)
                exchange = _infer_exchange_prefix(symbol)
                return f"{exchange}{parsed.normalized}"
            except Exception:
                return symbol
    
    def store_tick_data(self, symbol: str, data: Dict[str, Any], ttl: int = 3600):
        """
        Store tick data with unified structure.
        
        Uses UniversalSymbolParser for symbol normalization.
        For real-time tick streaming, use RedisStorage.queue_tick_for_storage() instead.
        This method stores simple tick snapshots.
        
        Key format: tick:{normalized_symbol}
        
        Args:
            symbol: Trading symbol (e.g., INDIGO25NOV5900CE)
            data: Tick data dictionary
            ttl: Time to live in seconds (default: 3600 = 1 hour)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        tick_key = f"tick:{normalized_symbol}"
        
        try:
            # Store current tick snapshot (for simple lookups)
            # Note: For real-time streaming, use RedisStorage.queue_tick_for_storage()
            self.redis.hset(tick_key, mapping=data)
            self.redis.expire(tick_key, ttl)
            
            # Add to recent ticks list (keep last 100)
            recent_key = f"recent_ticks:{normalized_symbol}"
            self.redis.lpush(recent_key, json.dumps(data))
            self.redis.ltrim(recent_key, 0, 99)
            self.redis.expire(recent_key, ttl)
            
            logger.debug(f"Stored tick data snapshot for {normalized_symbol}")
        except Exception as e:
            logger.error(f"Failed to store tick data for {symbol}: {e}")
            raise
    
    def store_indicators(self, symbol: str, indicators: Dict[str, float], ttl: int = 86400):
        """
        Store all indicators in one operation.
        
        ✅ WIRED: Follows redis_storage.py pattern exactly:
        - Uses RedisManager82.get_client() with explicit DB 1
        - Uses key_builder.live_indicator() for key generation
        - Uses key_builder.get_database() to verify DB assignment
        - All indicators stored in DB 1 (live data)
        
        Args:
            symbol: Trading symbol
            indicators: Dictionary of indicator_name -> value
            ttl: Time to live in seconds (default: 86400 = 24 hours)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ WIRED: Get client for DB 1 (matches redis_storage.py pattern)
        try:
            if RedisManager82:
                client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
            else:
                client = self.redis
        except Exception as e:
            logger.error(f"Failed to get Redis client for indicator storage: {e}")
            raise
        
        try:
            # ✅ WIRED: Use key_builder.live_indicator() for each indicator (matches redis_storage.py pattern)
            for indicator_name, value in indicators.items():
                # Generate key using key_builder
                key = self.key_builder.live_indicator(normalized_symbol, indicator_name)
                
                # ✅ WIRED: Verify DB assignment (matches redis_storage.py pattern)
                from redis_files.redis_key_standards import RedisDatabase
                db = self.key_builder.get_database(key)
                if db != RedisDatabase.DB1_LIVE_DATA:
                    logger.warning("Indicator key %s assigned to wrong DB: %s (expected DB1)", key, db)
                
                # Store using setex (matches redis_storage.py pattern)
                value_str = str(value) if not isinstance(value, str) else value
                client.setex(key, ttl, value_str)
            
            logger.debug(f"Stored {len(indicators)} indicators for {normalized_symbol}")
        except Exception as e:
            logger.error(f"Failed to store indicators for {symbol}: {e}")
            raise
    
    def store_greeks(self, symbol: str, greeks: Dict[str, float], ttl: int = 86400):
        """
        Store option Greeks.
        
        ✅ WIRED: Follows redis_storage.py pattern exactly:
        - Uses RedisManager82.get_client() with explicit DB 1
        - Uses key_builder.live_greeks() for key generation
        - Uses key_builder.get_database() to verify DB assignment
        - All Greeks stored in DB 1 (live data)
        
        Args:
            symbol: Trading symbol
            greeks: Dictionary of greek_name -> value
            ttl: Time to live in seconds (default: 86400 = 24 hours)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ WIRED: Get client for DB 1 (matches redis_storage.py pattern)
        try:
            if RedisManager82:
                client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
            else:
                client = self.redis
        except Exception as e:
            logger.error(f"Failed to get Redis client for Greeks storage: {e}")
            raise
        
        try:
            # ✅ WIRED: Use key_builder.live_greeks() for each Greek (matches redis_storage.py pattern)
            for greek_name, value in greeks.items():
                # Generate key using key_builder
                key = self.key_builder.live_greeks(normalized_symbol, greek_name)
                
                # ✅ WIRED: Verify DB assignment (matches redis_storage.py pattern)
                from redis_files.redis_key_standards import RedisDatabase
                db = self.key_builder.get_database(key)
                if db != RedisDatabase.DB1_LIVE_DATA:
                    logger.warning("Greeks key %s assigned to wrong DB: %s (expected DB1)", key, db)
                
                # Store using setex (matches redis_storage.py pattern)
                value_str = str(value) if not isinstance(value, str) else value
                client.setex(key, ttl, value_str)
            
            logger.debug(f"Stored {len(greeks)} Greeks for {normalized_symbol}")
        except Exception as e:
            logger.error(f"Failed to store Greeks for {symbol}: {e}")
            raise
    
    def get_all_data(self, symbol: str) -> Dict[str, Any]:
        """
        Get all data for a symbol in one call.
        
        Uses UniversalSymbolParser for symbol normalization.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with keys: 'tick', 'indicators', 'greeks', 'recent_ticks'
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        try:
            # Get tick data
            tick_key = f"tick:{normalized_symbol}"
            tick_data = self.redis.hgetall(tick_key) or {}
            
            # ✅ SOURCE OF TRUTH: Get indicators using key builder
            indicators = {}
            variants = RedisKeyStandards.get_indicator_symbol_variants(normalized_symbol)
            # Common indicators to fetch
            indicator_names = ['rsi', 'macd', 'bollinger_upper', 'bollinger_lower', 'volume_ratio']
            for variant in variants:
                for indicator_name in indicator_names:
                    # ✅ WIRED: Use key_builder.live_indicator() (matches redis_storage.py pattern)
                    # category is auto-determined by live_indicator() if not provided
                    key = self.key_builder.live_indicator(variant, indicator_name)
                    value = self.redis.get(key)
                    if value and indicator_name not in indicators:
                        try:
                            indicators[indicator_name] = float(value) if '.' in str(value) else int(value)
                        except (ValueError, TypeError):
                            indicators[indicator_name] = value
            
            # ✅ SOURCE OF TRUTH: Get Greeks using key builder
            greeks = {}
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
            for variant in variants:
                for greek_name in greek_names:
                    # ✅ SOURCE OF TRUTH: Use key builder method for Greeks
                    key = self.key_builder.live_greeks(variant, greek_name)
                    value = self.redis.get(key)
                    if value and greek_name not in greeks:
                        try:
                            greeks[greek_name] = float(value) if '.' in str(value) else int(value)
                        except (ValueError, TypeError):
                            greeks[greek_name] = value
            
            # Get recent ticks
            recent_key = f"recent_ticks:{normalized_symbol}"
            recent_ticks = []
            for tick_json in self.redis.lrange(recent_key, 0, 9):
                try:
                    recent_ticks.append(json.loads(tick_json))
                except (json.JSONDecodeError, TypeError):
                    continue
            
            return {
                'tick': tick_data,
                'indicators': indicators,
                'greeks': greeks,
                'recent_ticks': recent_ticks
            }
        except Exception as e:
            logger.error(f"Failed to get all data for {symbol}: {e}")
            return {
                'tick': {},
                'indicators': {},
                'greeks': {},
                'recent_ticks': []
            }
    
    def get_tick_data(self, symbol: str) -> Dict[str, Any]:
        """Get current tick data for a symbol."""
        normalized_symbol = self._normalize_symbol(symbol)
        tick_key = f"tick:{normalized_symbol}"
        return self.redis.hgetall(tick_key) or {}
    
    def get_indicators(self, symbol: str, indicator_names: List[str] = None) -> Dict[str, Any]:
        """
        Get indicators following redis_storage.py pattern.
        
        ✅ WIRED: Matches redis_storage.py signature:
        - Uses key_builder.live_indicator() for key generation
        - Uses RedisManager82.get_client() with explicit DB 1
        - Uses key_builder.get_database() to verify DB assignment
        
        Args:
            symbol: Trading symbol
            indicator_names: List of indicator names to fetch (if None, fetches common ones)
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        if indicator_names is None:
            indicator_names = ['rsi', 'macd', 'bollinger_upper', 'bollinger_lower', 'volume_ratio']
        
        # ✅ WIRED: Get client for DB 1 (matches redis_storage.py pattern)
        try:
            if RedisManager82:
                client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
            else:
                client = self.redis
        except Exception as e:
            logger.error(f"Failed to get Redis client for indicator fetch: {e}")
            return {}
        
        indicators = {}
        variants = RedisKeyStandards.get_indicator_symbol_variants(normalized_symbol)
        
        for variant in variants:
            for indicator_name in indicator_names:
                if indicator_name in indicators:
                    continue
                # ✅ WIRED: Use key_builder.live_indicator() (matches redis_storage.py pattern)
                key = self.key_builder.live_indicator(variant, indicator_name)
                
                # ✅ WIRED: Verify DB assignment (matches redis_storage.py pattern)
                from redis_files.redis_key_standards import RedisDatabase
                db = self.key_builder.get_database(key)
                if db != RedisDatabase.DB1_LIVE_DATA:
                    logger.debug("Indicator key %s assigned to wrong DB: %s (expected DB1)", key, db)
                
                value = client.get(key)
                if value:
                    if isinstance(value, bytes):
                        value = value.decode("utf-8")
                    try:
                        indicators[indicator_name] = float(value) if '.' in str(value) else int(value)
                    except (ValueError, TypeError):
                        indicators[indicator_name] = value
        
        return indicators
    
    def get_greeks(self, symbol: str) -> Dict[str, float]:
        """
        Get option Greeks following redis_storage.py pattern.
        
        ✅ WIRED: Matches redis_storage.py signature:
        - Uses key_builder.live_greeks() for key generation
        - Uses RedisManager82.get_client() with explicit DB 1
        - Uses key_builder.get_database() to verify DB assignment
        """
        normalized_symbol = self._normalize_symbol(symbol)
        
        # ✅ WIRED: Get client for DB 1 (matches redis_storage.py pattern)
        try:
            if RedisManager82:
                client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
            else:
                client = self.redis
        except Exception as e:
            logger.error(f"Failed to get Redis client for Greeks fetch: {e}")
            return {}
        
        greeks = {}
        greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
        variants = RedisKeyStandards.get_indicator_symbol_variants(normalized_symbol)
        
        for variant in variants:
            for greek_name in greek_names:
                if greek_name in greeks:
                    continue
                
                # ✅ WIRED: Use key_builder.live_greeks() (matches redis_storage.py pattern)
                key = self.key_builder.live_greeks(variant, greek_name)
                
                # ✅ WIRED: Verify DB assignment (matches redis_storage.py pattern)
                from redis_files.redis_key_standards import RedisDatabase
                db = self.key_builder.get_database(key)
                if db != RedisDatabase.DB1_LIVE_DATA:
                    logger.debug("Greeks key %s assigned to wrong DB: %s (expected DB1)", key, db)
                
                value = client.get(key)
                if value:
                    try:
                        greeks[greek_name] = float(value)
                    except (ValueError, TypeError):
                        pass
        
        return greeks


# ============================================================================
# Singleton instance for global use
# ============================================================================

_unified_storage_instance: Optional[UnifiedDataStorage] = None


def get_unified_storage(redis_client=None) -> UnifiedDataStorage:
    """
    Get singleton instance of UnifiedDataStorage.
    
    This is the source of truth for all Redis storage operations.
    All downstream code should use this function to get the storage instance.
    
    Args:
        redis_client: Optional Redis client (if None, uses redis_gateway or creates new client)
    
    Returns:
        UnifiedDataStorage instance
    """
    global _unified_storage_instance
    
    if _unified_storage_instance is None:
        # ✅ SOURCE OF TRUTH: Use RedisManager82 if no client provided
        if redis_client is None:
            try:
                from redis_files.redis_client import redis_gateway
                if redis_gateway and hasattr(redis_gateway, 'redis_client'):
                    redis_client = redis_gateway.redis_client
                elif redis_gateway and hasattr(redis_gateway, 'redis'):
                    redis_client = redis_gateway.redis
                else:
                    # ✅ SOURCE OF TRUTH: Fallback to RedisManager82
                    redis_client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
            except ImportError:
                # ✅ SOURCE OF TRUTH: Use RedisManager82 as fallback
                redis_client = RedisManager82.get_client(process_name="unified_data_storage", db=1)
        
        _unified_storage_instance = UnifiedDataStorage(redis_client=redis_client)
    
    return _unified_storage_instance

