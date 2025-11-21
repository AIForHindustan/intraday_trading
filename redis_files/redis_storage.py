"""
Focused tick storage - ONLY stores raw ticks and OHLC data.

No calculations, no indicators, no patterns.

Redis Database Architecture:
- DB 1: Historical OHLC + current tick data + rolling windows
- DB 2: Pattern validation results only

Storage Pattern:
1. Queue ticks for batch storage (async, batched)
2. Store tick data in Redis hashes (ticks:{symbol}) using hset with mapping
3. Add to processed stream (ticks:intraday:processed) with maxlen trimming
4. Store latest tick metadata for quick lookup

All tick data is stored in DB 1 (ohlc_and_ticks).

MIGRATION NOTE: Replace direct redis.Redis(...) calls with RedisManager82.get_client()
Example:
  BEFORE: r = redis.Redis(host='localhost', port=6379, db=1)
  AFTER:  from redis_files.redis_client import RedisManager82
          r = RedisManager82.get_client(process_name="your_process", db=1)
          # Uses process-specific connection pools from PROCESS_POOL_CONFIG
"""

import json
import time
import logging
import threading
from typing import Dict, List, Any, Optional
from config.utils.timestamp_normalizer import TimestampNormalizer
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
)

logger = logging.getLogger(__name__)


class RedisStorage:
    """
    Focused tick storage - ONLY stores raw ticks and OHLC data.
    
    No calculations, no indicators, no patterns.
    
    Features:
    - Batches ticks for efficient Redis pipeline execution (50 ticks or 100ms threshold)
    - Automatic flushing with Redis 8.2 optimized hset with mapping parameter
    - Thread-safe operations with locking
    - Stream management with maxlen and approximate trimming
    - TTL management for tick data
    
    Storage Pattern:
    1. Queue ticks for batch storage (async, batched)
    2. Store tick data in Redis hashes (ticks:{symbol}) using hset with mapping
    3. Add to processed stream (ticks:intraday:processed) with maxlen trimming
    4. Store latest tick metadata for quick lookup
    """
    
    def __init__(self, redis_client):
        """
        Initialize Redis storage for tick data.
        
        Args:
            redis_client: Redis client instance (RobustRedisClient or compatible)
        """
        self.redis = redis_client
        self.field_mapping_manager = get_field_mapping_manager()
        
        # ✅ BATCHING: Initialize tick batching for optimized storage (Redis 8.2)
        # Migrated from Dragonfly OptimizedTickClient
        self.tick_batch = []
        self.max_batch_size = 50
        self.last_flush = time.time()
        self.flush_interval = 0.1  # 100ms max
        self._lock = threading.Lock() if hasattr(threading, 'Lock') else None
        
        # Resolve canonical field names from optimized_field_mapping.yaml
        self.FIELD_LAST_PRICE = resolve_session_field("last_price")
        self.FIELD_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_ZERODHA_CUMULATIVE_VOLUME = resolve_session_field("zerodha_cumulative_volume")
        self.FIELD_ZERODHA_LAST_TRADED_QUANTITY = resolve_session_field("zerodha_last_traded_quantity")
        self.FIELD_BUCKET_INCREMENTAL_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_BUCKET_CUMULATIVE_VOLUME = resolve_session_field("bucket_cumulative_volume")
        self.FIELD_HIGH = resolve_session_field("high")
        self.FIELD_LOW = resolve_session_field("low")
        self.FIELD_SESSION_DATE = resolve_session_field("session_date")
        self.FIELD_UPDATE_COUNT = resolve_session_field("update_count")
        self.FIELD_LAST_UPDATE_TIMESTAMP = resolve_session_field("last_update_timestamp")
        self.FIELD_FIRST_UPDATE = resolve_session_field("first_update")
        self.FIELD_AVERAGE_PRICE = resolve_session_field("average_price")
        self.FIELD_OHLC = resolve_session_field("ohlc")
        self.FIELD_NET_CHANGE = resolve_session_field("net_change")
        
        # ✅ Initialize KowSignalStraddleStrategy (IV + VWAP based pattern)
        # This strategy fetches Greeks for IV calculations
        try:
            from patterns.kow_signal_straddle import KowSignalStraddleStrategy
            self.kow_signal = KowSignalStraddleStrategy(redis_client=redis_client)
            logger.info("✅ KowSignalStraddleStrategy initialized in RedisStorage")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize KowSignalStraddleStrategy: {e}")
            self.kow_signal = None
    
    def queue_tick_for_storage(self, tick_data: dict):
        """Queue tick for batch Redis storage (matches Dragonfly pattern)
        
        This method queues tick data for batch storage, following the same pattern
        as Dragonfly OptimizedTickClient. Ticks are batched and flushed when:
        - Batch size reaches max_batch_size (50 ticks)
        - Time threshold exceeds flush_interval (100ms)
        
        Args:
            tick_data: Tick data dictionary with upstream field names:
                - symbol: Symbol identifier
                - last_price: Last traded price (or 'price')
                - bucket_incremental_volume: Incremental volume
                - bucket_cumulative_volume: Cumulative volume
                - timestamp_ms: Timestamp in milliseconds
                - exchange_timestamp: Exchange timestamp
                - All other fields from upstream (preserved as-is)
        
        Storage Pattern (matches Dragonfly):
        1. Queue tick in batch buffer
        2. Flush when threshold reached (batch size or time)
        3. Store in Redis hash (ticks:{symbol}) using hset with mapping
        4. Add to processed stream (ticks:intraday:processed) with maxlen trimming
        5. Update latest price metadata
        """
        if not tick_data:
            return
        
        # Extract symbol from tick_data (upstream field names)
        symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or "UNKNOWN"
        if not symbol or symbol == "UNKNOWN":
            logger.warning(f"⚠️ Skipping tick storage - invalid symbol: {symbol}")
            return
        
        # ✅ SOURCE OF TRUTH: Canonicalize symbol for consistent storage
        from redis_files.redis_key_standards import RedisKeyStandards
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        # Thread-safe batch append
        if self._lock:
            with self._lock:
                self.tick_batch.append(tick_data)
                batch_size = len(self.tick_batch)
                time_since_flush = time.time() - self.last_flush
                
                # Flush if batch size or time threshold reached
                if batch_size >= self.max_batch_size or time_since_flush >= self.flush_interval:
                    self._flush_batch()
        else:
            # Fallback if threading not available
            self.tick_batch.append(tick_data)
            if len(self.tick_batch) >= self.max_batch_size:
                self._flush_batch()
    
    def _flush_batch(self):
        """Flush batch to Redis using pipeline (Redis 8.2 optimized, matches Dragonfly pattern)
        
        Storage operations (matches Dragonfly write_tick_to_redis):
        1. Store tick data in Redis hash (ticks:{symbol}) using hset with mapping
        2. Add to processed stream (ticks:intraday:processed) with maxlen trimming
        3. Update latest price metadata for quick lookup
        
        Uses Redis 8.2 optimizations:
        - hset with mapping parameter for batch hash updates
        - Stream maxlen with approximate trimming
        - Pipeline for single round-trip execution
        """
        if not self.tick_batch:
            return
        
        try:
            # Get Redis client (DB 1 - realtime)
            from redis_files.redis_client import RedisManager82
            client = RedisManager82.get_client(process_name="redis_storage", db=1)
        except Exception as e:
            logger.error(f"Failed to get Redis client for batch flush: {e}")
            self.tick_batch.clear()
            return
        
        try:
            # Use pipeline for efficient batch operations (matches Dragonfly pattern)
            with client.pipeline() as pipe:
                for tick_data in self.tick_batch:
                    # Extract symbol (upstream field names)
                    symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or "UNKNOWN"
                    if not symbol or symbol == "UNKNOWN":
                        continue
                    
                    # Resolve token to symbol if needed (respects upstream/downstream flow)
                    symbol = self._resolve_token_to_symbol_for_storage(symbol)
                    if not symbol:
                        continue
                    
                    # ✅ SOURCE OF TRUTH: Canonicalize symbol for consistent storage
                    from redis_files.redis_key_standards import RedisKeyStandards
                    symbol = RedisKeyStandards.canonical_symbol(symbol)
                    
                    # 1. Store tick data in hash (Redis 8.2 hset with mapping - matches Dragonfly)
                    # ✅ SOURCE OF TRUTH: Use key builder for hash key
                    from redis_files.redis_key_standards import get_key_builder
                    tick_hash_key = get_key_builder().live_ticks_hash(symbol)
                    # Convert tick_data to mapping format (preserve all upstream fields)
                    tick_mapping = {str(k): str(v) for k, v in tick_data.items() if v is not None}
                    if tick_mapping:
                        pipe.hset(tick_hash_key, mapping=tick_mapping)  # Redis 8.2 batch update
                        pipe.expire(tick_hash_key, 300)  # 5min TTL
                    
                    # 2. Add to processed stream (matches Dragonfly stream pattern)
                    # ✅ SOURCE OF TRUTH: Use key builder for stream key
                    stream_key = get_key_builder().live_processed_stream()
                    # Convert tick_data to stream format (all values must be strings for Redis streams)
                    # Preserve all upstream field names (symbol, last_price, bucket_incremental_volume, etc.)
                    stream_fields = {str(k): str(v) for k, v in tick_data.items() if v is not None}
                    pipe.xadd(
                        stream_key,
                        stream_fields,  # All values converted to strings (Redis stream requirement)
                        maxlen=10000,  # Keep last 10k ticks (Redis 8.2 approximate trimming)
                        approximate=True  # Redis 8.2 optimization
                    )
                    
                    # 3. Update latest price (matches Dragonfly pattern)
                    # Use upstream field names: last_price or price
                    price = float(tick_data.get('last_price', tick_data.get('price', 0)))
                    if price > 0:
                        # ✅ SOURCE OF TRUTH: Use canonical symbol for price key
                        from redis_files.redis_key_standards import RedisKeyStandards
                        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                        price_key = f"price:latest:{canonical_symbol}"  # Note: price:latest format is standard
                        pipe.set(price_key, price, ex=300)  # 5min TTL
                    
                    # 🆕 NEW: Archive to historical tick time series (1-hour retention)
                    # Uses existing time series pattern from your architecture
                    # Note: Historical archiving needs to fetch existing bar before pipeline
                    self._archive_tick_to_historical_series(pipe, client, symbol, tick_data)
                
                # Execute all operations in single round-trip (matches Dragonfly pattern)
                pipe.execute()
            
            # Clear batch and update flush time
            self.tick_batch.clear()
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"Error flushing tick batch: {e}")
            # Clear batch on error to prevent memory buildup
            self.tick_batch.clear()
    
    def _archive_tick_to_historical_series(self, pipe, client, symbol: str, tick_data: Dict[str, Any]):
        """
        Archive tick data to historical time series using existing patterns
        Integrates with your current ClickHouse bridge and Redis standards
        
        Storage Pattern (matches your existing architecture):
        - ticks:historical:{symbol}:1min - 1-minute aggregated bars (24h TTL)
        - ticks:historical:{symbol}:raw   - Raw ticks (8h TTL, 1000 max entries) 
        - Uses existing key builder standards
        """
        try:
            from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards
            from datetime import datetime
            
            key_builder = get_key_builder()
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            
            # Extract timestamp (use existing field names from your architecture)
            timestamp_ms = tick_data.get('exchange_timestamp_ms') or tick_data.get('timestamp_ms') or int(time.time() * 1000)
            timestamp = timestamp_ms / 1000.0
            
            # 🆕 1. Archive to 1-minute aggregated bars (matches your OHLC pattern)
            minute_timestamp = int(timestamp // 60) * 60  # Round to minute
            minute_key = key_builder.live_ohlc_timeseries(canonical_symbol, "1min")
            
            # Get existing bar BEFORE pipeline (pipeline.get() doesn't return value immediately)
            existing_bar = client.get(minute_key)
            current_price = float(tick_data.get('last_price', tick_data.get('price', 0)))
            current_volume = float(tick_data.get('volume', tick_data.get('bucket_incremental_volume', 0)))
            
            if existing_bar:
                try:
                    bar_data = json.loads(existing_bar)
                    # Update OHLC
                    bar_data['high'] = max(bar_data['high'], current_price)
                    bar_data['low'] = min(bar_data['low'], current_price)
                    bar_data['close'] = current_price
                    bar_data['volume'] += current_volume
                    bar_data['tick_count'] += 1
                except:
                    bar_data = self._create_new_minute_bar(minute_timestamp, current_price, current_volume)
            else:
                bar_data = self._create_new_minute_bar(minute_timestamp, current_price, current_volume)
            
            # Store 1-minute bar with 24-hour TTL (matches your OHLC pattern)
            pipe.setex(minute_key, 86400, json.dumps(bar_data))  # 24 hours
            
            # 🆕 2. Archive raw ticks to sorted set (8-hour retention for intraday analysis)
            # Uses your existing sorted set pattern from ohlc_daily:
            raw_ticks_key = f"ticks:historical:raw:{canonical_symbol}"
            tick_payload = {
                'p': current_price,  # price
                'v': current_volume, # volume
                'ts': timestamp_ms,  # timestamp
                's': symbol          # symbol
            }
            
            # Add to sorted set with timestamp as score (your existing pattern)
            pipe.zadd(raw_ticks_key, {json.dumps(tick_payload): timestamp_ms})
            
            # 🆕 3. Maintain rolling window of last 1000 ticks (8-hour TTL for intraday queries)
            # Increased from 1 hour to 8 hours to support HistoricalTickQueries
            pipe.zremrangebyrank(raw_ticks_key, 0, -1001)  # Keep last 1000 ticks
            # Also trim old entries beyond 8 hours
            eight_hours_ago_ms = int(time.time() * 1000) - (8 * 60 * 60 * 1000)
            pipe.zremrangebyscore(raw_ticks_key, 0, eight_hours_ago_ms)
            pipe.expire(raw_ticks_key, 28800)  # 8-hour TTL (28800 seconds)
            
            # 🆕 4. Update volume profile in real-time (enhances your existing volume_profile_manager)
            self._update_realtime_volume_profile(pipe, canonical_symbol, current_price, current_volume)
            
        except Exception as e:
            logger.error(f"Historical tick archiving failed for {symbol}: {e}")
    
    def _create_new_minute_bar(self, minute_timestamp: int, price: float, volume: float) -> Dict:
        """Create new 1-minute OHLC bar - matches your existing OHLC structure"""
        return {
            'timestamp': minute_timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
            'tick_count': 1
        }
    
    def _update_realtime_volume_profile(self, pipe, symbol: str, price: float, volume: float):
        """
        Update real-time volume profile using your existing VolumeProfileManager pattern
        Enhances the pre-calculated volume profiles from your premarket script
        """
        try:
            # Check if volume profile manager is available
            try:
                from patterns.volume_profile_manager import VolumeProfileManager
                VOLUME_PROFILE_AVAILABLE = True
            except ImportError:
                VOLUME_PROFILE_AVAILABLE = False
            
            if VOLUME_PROFILE_AVAILABLE:
                # Use your existing volume profile key pattern
                vp_key = f"volume_profile:realtime:{symbol}"
                
                # Round price to nearest tick size for volume aggregation
                tick_size = self._get_tick_size(symbol)
                rounded_price = round(price / tick_size) * tick_size
                
                # Increment volume at price level
                pipe.hincrbyfloat(vp_key, str(rounded_price), volume)
                pipe.expire(vp_key, 28800)  # 8-hour TTL for intraday session analysis
                
        except Exception as e:
            logger.debug(f"Volume profile update failed for {symbol}: {e}")
    
    def _get_tick_size(self, symbol: str) -> float:
        """Get tick size for symbol (default: 0.05 for most instruments)"""
        # Default tick size - can be enhanced with symbol-specific lookup
        return 0.05
    
    def flush(self):
        """Manually flush any pending ticks in batch"""
        if self._lock:
            with self._lock:
                self._flush_batch()
        else:
            self._flush_batch()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        if self._lock:
            with self._lock:
                return {
                    'batch_size': len(self.tick_batch),
                    'max_batch_size': self.max_batch_size,
                    'time_since_flush': time.time() - self.last_flush,
                    'flush_interval': self.flush_interval
                }
        else:
            return {
                'batch_size': len(self.tick_batch),
                'max_batch_size': self.max_batch_size,
                'time_since_flush': time.time() - self.last_flush,
                'flush_interval': self.flush_interval
            }
    
    async def store_tick(self, symbol: str, tick_data: dict):
        """Store normalized tick data using Zerodha field names
        
        Args:
            symbol: Trading symbol
            tick_data: Tick data dictionary with Zerodha field names
        """
        # Resolve token to symbol BEFORE storing
        # ✅ Delegate to redis client's cumulative_tracker if available
        if hasattr(self.redis, 'cumulative_tracker') and self.redis.cumulative_tracker:
            symbol = self.redis.cumulative_tracker._resolve_token_to_symbol_for_storage(symbol)
        else:
            # Use RedisStorage's own method as fallback
            symbol = self._resolve_token_to_symbol_for_storage(symbol)
        
        # ✅ SOURCE OF TRUTH: Canonicalize symbol for consistent storage
        from redis_files.redis_key_standards import RedisKeyStandards
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        # Normalize timestamp FIRST using Zerodha field names
        normalized_tick = tick_data.copy()
        
        # Zerodha timestamp priority: exchange_timestamp > timestamp_ns > timestamp
        if 'exchange_timestamp' in tick_data and tick_data['exchange_timestamp']:
            normalized_tick['exchange_timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['exchange_timestamp'])
        elif 'timestamp_ns' in tick_data and tick_data['timestamp_ns']:
            normalized_tick['timestamp_ns_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp_ns'])
        elif 'timestamp' in tick_data and tick_data['timestamp']:
            normalized_tick['timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp'])
        
        # Last trade time - Zerodha specific field
        if 'last_trade_time' in tick_data and tick_data['last_trade_time']:
            normalized_tick['last_trade_time_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['last_trade_time'])
        
        # Store in Redis Streams using Zerodha field names in DB 1 (realtime)
        # ✅ SOURCE OF TRUTH: Use key builder for stream key
        from redis_files.redis_key_standards import get_key_builder
        stream_key = get_key_builder().live_ticks_stream(symbol)
        # Use process-specific client if available
        try:
            from redis_files.redis_client import RedisManager82
            # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
            realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
        except Exception:
            # Fallback to existing method
            realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
        
        # Convert dict to proper stream format
        stream_data = {
            'data': json.dumps(normalized_tick, default=str),
            'timestamp': str(int(time.time() * 1000)),
            'symbol': symbol
        }
        # ✅ FIXED: ALWAYS use maxlen to prevent unbounded stream growth
        realtime_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)
    
    async def get_recent_ticks(self, symbol: str, count: int = 100) -> List[dict]:
        """Get recent ticks - already normalized timestamps using Zerodha field names
        
        Args:
            symbol: Trading symbol
            count: Number of recent ticks to retrieve (default: 100)
            
        Returns:
            List of tick data dictionaries
        """
        # ✅ SOURCE OF TRUTH: Canonicalize symbol before retrieval
        from redis_files.redis_key_standards import RedisKeyStandards, get_key_builder
        symbol = RedisKeyStandards.canonical_symbol(symbol)
        stream_key = get_key_builder().live_ticks_stream(symbol)
        # Use process-specific client if available
        try:
            from redis_files.redis_client import RedisManager82
            # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
            realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
        except Exception:
            # Fallback to existing method
            realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
        ticks = realtime_client.xrevrange(stream_key, count=count)
        return [tick_data for tick_id, tick_data in ticks]
    
    def _resolve_token_to_symbol_for_storage(self, symbol):
        """Resolve token to symbol for Redis storage using hot_token_mapper (no Redis dependency)
        
        Args:
            symbol: Symbol or token identifier
            
        Returns:
            Resolved symbol string or None if invalid
        """
        try:
            symbol_str = str(symbol) if symbol else ""
            
            # If already a proper symbol (contains :), return as-is
            if ":" in symbol_str and not symbol_str.isdigit() and not symbol_str.startswith("UNKNOWN_"):
                return symbol
            
            # Skip empty or pure UNKNOWN symbols
            if not symbol_str or symbol_str == "UNKNOWN":
                return None  # Don't store with invalid keys
            
            # Try to resolve token to symbol using hot_token_mapper
            if symbol_str.isdigit() or symbol_str.startswith("TOKEN_") or symbol_str.startswith("UNKNOWN_"):
                try:
                    # Extract token number
                    if symbol_str.startswith("TOKEN_"):
                        token_str = symbol_str.replace("TOKEN_", "")
                    elif symbol_str.startswith("UNKNOWN_"):
                        token_str = symbol_str.replace("UNKNOWN_", "")
                    else:
                        token_str = symbol_str
                    
                    token = int(token_str)
                    
                    # ✅ Use hot_token_mapper (no Redis dependency)
                    from crawlers.hot_token_mapper import get_hot_token_mapper
                    mapper = get_hot_token_mapper()
                    resolved = mapper.token_to_symbol(token)
                    
                    # Only return if it's not still UNKNOWN
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        return resolved
                except (ValueError, ImportError) as e:
                    pass
            
            # Fallback: return original if resolution not available
            return symbol if symbol_str and ":" in symbol_str else None
        except Exception as e:
            return symbol if ":" in str(symbol) else None


class HistoricalTickQueries:
    """
    Query historical tick data from Redis time series
    Uses your existing Redis key patterns and standards
    
    Data Availability:
    - Raw ticks: 8-hour retention (ticks:historical:raw:{symbol})
    - 1-minute bars: 24-hour retention (ohlc:{symbol}:1min)
    - Volume profiles: 8-hour retention (volume_profile:realtime:{symbol})
    """
    
    def __init__(self, redis_client=None):
        from redis_files.redis_client import RedisManager82
        self.redis_client = redis_client or RedisManager82.get_client(process_name="historical_queries", db=1)
    
    def get_historical_ticks(self, symbol: str, start_time: int, end_time: int, max_ticks: int = 1000) -> List[Dict]:
        """
        Get historical ticks for a symbol in time range
        Uses your existing sorted set pattern
        
        Note: Data is available for up to 8 hours (TTL: 28800 seconds)
        Maximum 1000 ticks per symbol (rolling window)
        """
        from redis_files.redis_key_standards import RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        raw_ticks_key = f"ticks:historical:raw:{canonical_symbol}"
        
        try:
            # Use ZRANGEBYSCORE with your timestamp scores
            tick_data = self.redis_client.zrangebyscore(
                raw_ticks_key, start_time, end_time, 
                start=0, num=max_ticks, withscores=True
            )
            
            ticks = []
            for tick_json, timestamp in tick_data:
                try:
                    tick_obj = json.loads(tick_json)
                    tick_obj['timestamp'] = timestamp  # Add the score as timestamp
                    ticks.append(tick_obj)
                except json.JSONDecodeError:
                    continue
            
            return ticks
            
        except Exception as e:
            logger.error(f"Historical tick query failed for {symbol}: {e}")
            return []
    
    def get_ohlc_bars(self, symbol: str, timeframe: str, start_time: int, end_time: int) -> List[Dict]:
        """
        Get OHLC bars for a symbol and timeframe
        Uses your existing ohlc_timeseries pattern
        """
        from redis_files.redis_key_standards import get_key_builder, RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        key_builder = get_key_builder()
        
        # Generate keys for the time range
        bars = []
        current_time = start_time
        
        while current_time <= end_time:
            bar_key = key_builder.live_ohlc_timeseries(canonical_symbol, timeframe)
            # For simplicity, get latest bar - extend for historical range as needed
            bar_data = self.redis_client.get(bar_key)
            
            if bar_data:
                try:
                    bars.append(json.loads(bar_data))
                except json.JSONDecodeError:
                    pass
            
            # Move to next timeframe (implementation depends on your timeframe logic)
            if timeframe == "1min":
                current_time += 60
            elif timeframe == "5min":
                current_time += 300
        
        return bars
    
    def get_volume_profile(self, symbol: str, period: str = "1h") -> Dict:
        """
        Get volume profile for a symbol
        Uses your existing volume profile pattern
        """
        from redis_files.redis_key_standards import RedisKeyStandards
        
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        
        if period == "realtime":
            vp_key = f"volume_profile:realtime:{canonical_symbol}"
        else:
            vp_key = f"volume_profile:{period}:{canonical_symbol}"
        
        try:
            profile_data = self.redis_client.hgetall(vp_key)
            # Convert bytes to string keys and float values
            decoded_profile = {}
            for price_bytes, volume_bytes in profile_data.items():
                price_str = price_bytes.decode('utf-8') if isinstance(price_bytes, bytes) else str(price_bytes)
                volume_str = volume_bytes.decode('utf-8') if isinstance(volume_bytes, bytes) else str(volume_bytes)
                decoded_profile[price_str] = float(volume_str)
            
            return decoded_profile
            
        except Exception as e:
            logger.error(f"Volume profile query failed for {symbol}: {e}")
            return {}
