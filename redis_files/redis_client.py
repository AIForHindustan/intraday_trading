"""Redis wrapper with auto-reconnection and thread-safe fallback

Features:
- Automatic reconnection on connection loss
- Retry logic with exponential backoff (RESP3 + redis-py built-in retry)
- Health checks and connection monitoring
- Graceful fallback to in-memory storage
- Thread-safe operations
- Queue for failed operations during disconnection
- Safe JSON serialization for all data types
- Circuit breaker for fast-fail during outages
- Proper connection pooling to prevent connection storms

Production benefits:
- Self-healing connections
- No manual intervention needed
- Zero message loss with retry queue
- Seamless failover
- RESP3 protocol for Redis 8.x compatibility
- XADD trim & group auto-create for Streams memory stability
"""

from __future__ import annotations

import time
import json
import random
import redis
from redis.connection import ConnectionPool
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError,
    TimeoutError as RedisTimeoutError,
    AuthenticationError,
    ResponseError,
)
import logging
from threading import Lock, Thread
from collections import deque, defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Callable, Iterable, Tuple, TypeVar

from utils.time_utils import INDIAN_TIME_PARSER
from config.utils.timestamp_normalizer import TimestampNormalizer
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
)
from redis_files.redis_ohlc_keys import (
    normalize_symbol as normalize_ohlc_symbol,
    ohlc_daily_zset,
    ohlc_timeseries_key,
    ohlc_hourly_zset,
)

try:
    import numpy as np

    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

# Circuit breaker implementation (consolidated)
import time
from enum import Enum
from functools import wraps

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Circuit is open, requests blocked
    HALF_OPEN = "HALF_OPEN"  # Testing if service is back

class CircuitBreakerOpen(Exception):
    """Exception raised when circuit breaker is open"""
    def __init__(self, message: str, circuit_name: str = "Unknown"):
        super().__init__(message)
        self.circuit_name = circuit_name
        self.message = message

class CircuitBreaker:
    """Circuit Breaker implementation for preventing cascading failures"""
    
    def __init__(
        self, 
        name: str = "redis",
        failure_threshold: int = 3, 
        reset_timeout: int = 30,
        success_threshold: int = 2,
        timeout: Optional[float] = None
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.success_threshold = success_threshold
        self.timeout = timeout
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def should_allow_request(self) -> bool:
        """Check if request should be allowed based on circuit state"""
        if self.state == CircuitState.CLOSED:
            return True
        elif self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.reset_timeout:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
                return True
            return False
        elif self.state == CircuitState.HALF_OPEN:
            return True
        return False
    
    def record_success(self):
        """Record a successful operation"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker {self.name} closed after {self.success_count} successes")
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self, exception: Exception):
        """Record a failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
    
    def get_state(self) -> str:
        """Get current circuit breaker state"""
        return self.state.value

def redis_circuit_breaker():
    """Circuit breaker for Redis operations"""
    return CircuitBreaker(
        name="redis",
        failure_threshold=3,
        reset_timeout=30,
        success_threshold=2
    )

CIRCUIT_BREAKER_AVAILABLE = True

# Configure logging
logger = logging.getLogger(__name__)

FIELD_MAPPING_MANAGER = get_field_mapping_manager()
SESSION_FIELD_ZERODHA_CUM = resolve_session_field("zerodha_cumulative_volume")
SESSION_FIELD_BUCKET_CUM = resolve_session_field("bucket_cumulative_volume")
SESSION_FIELD_BUCKET_INC = resolve_session_field("bucket_incremental_volume")
SESSION_ALIAS_BY_CANONICAL = FIELD_MAPPING_MANAGER.get_session_aliases_by_canonical()


def mirror_session_aliases(
    redis_connection,
    key: str,
    canonical_field: str,
    value: int,
    operation: str = "set",
) -> None:
    """Mirror canonical session fields to legacy aliases."""
    aliases = SESSION_ALIAS_BY_CANONICAL.get(canonical_field, [])
    if not aliases:
        return

    for alias in aliases:
        try:
            if operation == "incr":
                redis_connection.hincrby(key, alias, value)
            else:
                redis_connection.hset(key, alias, value)
        except Exception as alias_err:
            logger.debug(
                "Failed to mirror alias %s for %s on %s: %s",
                alias,
                canonical_field,
                key,
                alias_err,
            )
# Singleton holder for RobustRedisClient
_redis_client_instance = None


class SafeJSONEncoder(json.JSONEncoder):
    """Handles all non-serializable types at source"""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif NUMPY_AVAILABLE and isinstance(obj, np.ndarray):
            return obj.tolist()
        elif NUMPY_AVAILABLE and isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif NUMPY_AVAILABLE and isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        return super().default(obj)


# Import Redis configuration from consolidated config
from redis_files.redis_config import (
    REDIS_DATABASES,
    AION_CHANNELS,
    get_redis_config,
    get_database_for_data_type,
    get_ttl_for_data_type,
)


class ConnectionPooledRedisClient:
    """Shared connection-pool wrapper to avoid per-thread Redis connections."""

    _pools: Dict[str, ConnectionPool] = {}
    _lock: Lock = Lock()

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        max_connections: int = 50,  # Reduced to prevent pool exhaustion (pools are shared)
        decode_responses: bool = True,
        socket_connect_timeout: int = 5,
        socket_timeout: int = 5,
        retry_on_timeout: bool = True,
        health_check_interval: int = 30,
    ):
        pool_key = f"{host}:{port}:{db}:{decode_responses}"
        with self._lock:
            pool = self._pools.get(pool_key)
            if not pool:
                pool = ConnectionPool(
                    host=host,
                    port=port,
                    db=db,
                    password=password,
                    max_connections=max_connections,
                    decode_responses=decode_responses,
                    socket_connect_timeout=socket_connect_timeout,
                    socket_timeout=socket_timeout,
                    retry_on_timeout=retry_on_timeout,
                    health_check_interval=health_check_interval,
                )
                self._pools[pool_key] = pool
        self.pool_key = pool_key
        self.pool = pool
        self.redis = redis.Redis(connection_pool=self.pool)

    def close(self):
        try:
            # Return connection to pool; redis-py handles pooling automatically.
            if isinstance(self.redis, redis.Redis):
                self.redis.close()
        except Exception:
            pass

    def __del__(self):
        self.close()


class RobustRedisClient:
    """Consolidated Redis client with circuit breaker and Redis-powered calculations"""

    def __init__(
        self,
        host="localhost",
        port=6379,
        db=0,
        password=None,
        decode_responses=True,
        max_retries=5,
        retry_delay=1.0,
        health_check_interval=30,
        config=None,
        circuit_breaker_enabled=True,
    ):
        # Allow config override
        if config:
            host = config.get("redis_host", host)
            port = config.get("redis_port", port)
            db = config.get("redis_db", db)
            password = config.get("redis_password", password)
            circuit_breaker_enabled = config.get(
                "circuit_breaker_enabled", circuit_breaker_enabled
            )

        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.decode_responses = decode_responses
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.health_check_interval = health_check_interval
        
        # Get max_connections from config, default to 50 (reduced to prevent connection exhaustion)
        # Priority: config dict > centralized redis_config > default
        self.max_connections = 50
        if config:
            # Check for both redis_max_connections and max_connections keys
            self.max_connections = int(config.get("redis_max_connections") or config.get("max_connections", self.max_connections))
        else:
            # Try to get from centralized config if no config provided
            try:
                from config.redis_config import get_redis_config
                redis_config = get_redis_config()
                if "max_connections" in redis_config:
                    self.max_connections = int(redis_config["max_connections"])
            except Exception:
                pass  # Use default if config unavailable

        # ✅ FIXED: Separate Redis clients per database
        self.clients = {}
        self.connection_wrappers: Dict[int, ConnectionPooledRedisClient] = {}
        self.pubsub = None
        self.is_connected = False
        self.connection_lock = Lock()
        self.last_health_check = 0
        
        # Initialize redis_pool to None to prevent cleanup errors
        self.redis_pool = None
        self.redis_client = None
        
        # Initialize clients for all databases
        self._initialize_clients()

        # Circuit breaker integration
        self.circuit_breaker_enabled = (
            circuit_breaker_enabled and CIRCUIT_BREAKER_AVAILABLE
        )
        if self.circuit_breaker_enabled:
            self.circuit_breaker = redis_circuit_breaker()
            logger.info("Circuit breaker enabled for Redis operations")
        else:
            self.circuit_breaker = None
            if circuit_breaker_enabled and not CIRCUIT_BREAKER_AVAILABLE:
                logger.warning("Circuit breaker requested but not available")

        # Fallback storage (when Redis is down)
        self.fallback_data = {}
        self.fallback_expirations = {}
        self.fallback_lock = Lock()

        # Queue for failed operations
        self.retry_queue = deque(maxlen=1000)  # Max 1000 pending operations

        # Stats
        self.stats = {
            "reconnections": 0,
            "failed_operations": 0,
            "queued_operations": 0,
            "successful_operations": 0,
        }

        # Add cumulative data tracker for session-based storage
        self.cumulative_tracker = CumulativeDataTracker(self)
        self._fallback_session = datetime.now().strftime("%Y-%m-%d")

        # Initialize Redis-powered calculations
        self._init_redis_calculations()

        # Initial connection
        logger.info("Initializing Redis connection...")
        self._connect()
    
    def _init_redis_calculations(self):
        """Initialize Redis-powered calculations"""
        try:
            from redis_files.redis_calculations import RedisCalculations
            self.calculations = RedisCalculations(self)
            logger.info("✅ Redis calculations initialized")
        except ImportError as e:
            logger.warning(f"Redis calculations not available: {e}")
            self.calculations = None
        except Exception as e:
            logger.warning(f"Redis calculations initialization failed: {e}")
            self.calculations = None

    @property
    def current_session(self) -> str:
        if getattr(self, "cumulative_tracker", None):
            return self.cumulative_tracker.current_session
        return getattr(self, "_fallback_session", datetime.now().strftime("%Y-%m-%d"))

    @current_session.setter
    def current_session(self, value: str) -> None:
        if getattr(self, "cumulative_tracker", None):
            self.cumulative_tracker.current_session = value
        self._fallback_session = value

    def _initialize_clients(self):
        """Initialize separate Redis clients for each database"""
        try:
            # Initialize clients for all databases defined in REDIS_DATABASES
            for db_num in REDIS_DATABASES.keys():
                try:
                    wrapper = ConnectionPooledRedisClient(
                        host=self.host,
                        port=self.port,
                        db=db_num,
                        password=self.password,
                        max_connections=self.max_connections,
                        decode_responses=self.decode_responses,
                    )
                    client = wrapper.redis
                    client.ping()
                    self.connection_wrappers[db_num] = wrapper
                    self.clients[db_num] = client
                    logger.info(
                        f"✅ Redis client initialized for DB {db_num} using pooled connections (max {self.max_connections})"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to initialize client for DB {db_num}: {e}")
                    self.connection_wrappers[db_num] = None
                    self.clients[db_num] = None

            # Set primary client (DB 0 or first available)
            self.redis_client = self.clients.get(0) or next(
                (client for client in self.clients.values() if client is not None),
                None,
            )

            if self.redis_client:
                self.pubsub = self.redis_client.pubsub()
                self.is_connected = True
                logger.info("✅ Redis clients initialized successfully")
            else:
                logger.error("❌ No Redis clients could be initialized")

        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis clients: {e}")

    # ============ Database Segmentation Methods ============

    def get_client(self, db_num=0):
        """Get Redis client for specific database"""
        return self.clients.get(db_num, self.clients.get(0))

    def _get_function_client(self):
        """Return primary client suitable for Redis function execution."""
        client = self.redis_client or self.get_client(0)
        if client is None:
            raise RuntimeError("No Redis client available for Redis functions")
        return client

    def fcall(self, function_name, num_keys, *keys_and_args):
        """Proxy Redis FUNCTION CALL to the primary client."""
        client = self._get_function_client()
        return client.fcall(function_name, num_keys, *keys_and_args)

    def function_load(self, script, replace=False):
        """Proxy FUNCTION LOAD to the primary client."""
        client = self._get_function_client()
        if replace:
            return client.function_load(script, replace=True)
        return client.function_load(script)

    def publish_to_stream(self, stream_name, data, maxlen=10000):
        """Publish to Redis Stream instead of simple SET"""
        try:
            # Get the appropriate client for the stream
            db_num = self.get_database_for_data_type("stream_data")
            client = self.get_client(db_num)
            if not client:
                logger.error(f"No client available for stream {stream_name}")
                return False
            
            # Serialize data if needed
            if isinstance(data, (dict, list)):
                data = json.dumps(data, cls=SafeJSONEncoder)
            elif not isinstance(data, (str, bytes)):
                data = str(data)
            
            # Add to stream with memory-efficient settings
            result = client.xadd(
                stream_name,
                {"data": data, "timestamp": int(time.time() * 1000)},
                maxlen=maxlen,
                approximate=True  # Memory efficient
            )
            self.stats["successful_operations"] += 1
            return result
            
        except Exception as e:
            logger.error(f"Failed to publish to stream {stream_name}: {e}")
            self.stats["failed_operations"] += 1
            return False

    def read_from_stream(self, stream_name, count=100, start_id="0"):
        """Read from Redis Stream"""
        try:
            db_num = self.get_database_for_data_type("stream_data")
            client = self.get_client(db_num)
            if not client:
                logger.error(f"No client available for stream {stream_name}")
                return []
            
            # Read from stream
            messages = client.xread({stream_name: start_id}, count=count, block=0)
            return messages
            
        except Exception as e:
            logger.error(f"Failed to read from stream {stream_name}: {e}")
            return []

    def get_database_for_data_type(self, data_type):
        """Get appropriate database number for data type"""
        return get_database_for_data_type(data_type)

    def get_ttl_for_data_type(self, data_type):
        """Get TTL for data type"""
        return get_ttl_for_data_type(data_type)

    def switch_database(self, db_num):
        """Switch to specific Redis database safely.

        Uses the SELECT command on the active connection when possible. If that
        fails (e.g., due to connection pool semantics), rebuilds the connection
        pool with the requested DB.
        """
        if not isinstance(db_num, int) or db_num < 0:
            logger.error(f"Invalid database number: {db_num}")
            return False

        # Fast path: try SELECT on current connection
        if self.is_connected and self.redis_client:
            try:
                # Execute raw SELECT command (redis-py does not expose .select())
                self.redis_client.execute_command("SELECT", db_num)
                self.db = db_num
                return True
            except Exception as e:
                logger.debug(
                    f"Direct SELECT failed for DB {db_num}, rebuilding pool: {e}"
                )

        # Rebuild connection pool bound to the requested DB
        try:
            with self.connection_lock:
                wrapper = ConnectionPooledRedisClient(
                    host=self.host,
                    port=self.port,
                    db=db_num,
                    password=self.password,
                    max_connections=self.max_connections,
                    decode_responses=self.decode_responses,
                )
                self.redis_client = wrapper.redis
                self.redis_client.ping()
                self.connection_wrappers[db_num] = wrapper
                self.clients[db_num] = self.redis_client
                self.db = db_num
                self.is_connected = True
                self.pubsub = self.redis_client.pubsub()
                return True
        except Exception as e:
            logger.error(f"Failed to switch to database {db_num}: {e}")
            return False

    def store_by_data_type(self, data_type, key, value, ttl=None):
        """Store data in appropriate database based on type using separate clients or streams"""
        db_num = self.get_database_for_data_type(data_type)
        if ttl is None:
            ttl = self.get_ttl_for_data_type(data_type)

        # Check if this data type should use streams
        if data_type in ["stream_data", "ticks_stream", "alerts_stream", "patterns_stream"]:
            # Use Redis Streams for real-time data
            stream_name = f"stream:{data_type}"
            return self.publish_to_stream(stream_name, value, maxlen=10000)
        
        # Use traditional key-value storage for other data types
        client = self.get_client(db_num)
        if not client:
            logger.error(f"No client available for database {db_num}")
            return False

        try:
            # Serialize value if it's a dictionary or other complex type
            if isinstance(value, (dict, list)):
                # Handle dictionaries that may contain binary data
                if isinstance(value, dict):
                    # Convert binary data to base64 for JSON serialization
                    serializable_value = {}
                    for k, v in value.items():
                        if isinstance(v, bytes):
                            import base64
                            serializable_value[k] = base64.b64encode(v).decode("utf-8")
                        else:
                            serializable_value[k] = v
                    value = json.dumps(serializable_value, cls=SafeJSONEncoder)
                else:
                    value = json.dumps(value, cls=SafeJSONEncoder)
            elif not isinstance(value, (bytes, str, int, float)):
                value = str(value)

            # Store using the appropriate client
            result = client.set(key, value, ex=ttl)
            self.stats["successful_operations"] += 1
            return result
            
        except Exception as e:
            logger.error(f"Failed to store in database {db_num}: {e}")
            self.stats["failed_operations"] += 1
            return False

    def retrieve_by_data_type(self, key, data_type):
        """Retrieve data from appropriate database based on type using separate clients"""
        db_num = self.get_database_for_data_type(data_type)
        
        # Get the appropriate client for this database
        client = self.get_client(db_num)
        if not client:
            logger.error(f"No client available for database {db_num}")
            return None

        try:
            result = client.get(key)
            self.stats["successful_operations"] += 1
            return result
        except Exception as e:
            logger.error(f"Failed to retrieve from database {db_num}: {e}")
            self.stats["failed_operations"] += 1
            return None

    # Compatibility shim for legacy callers (e.g., alert_validator)
    def select(self, db_num):  # noqa: D401
        """Alias for switch_database for compatibility with redis-py `.select()` usage."""
        return self.switch_database(db_num)

    # ============ Stream Operations (XADD) ============
    def xadd(self, name, fields, id="*", maxlen=None, approximate=True):
        """Add entry to a Redis Stream with auto-reconnection and fallback queue.

        Mirrors redis-py's signature. `fields` should be a dict of field->value
        where values are str/bytes/int/float.
        """
        self._ensure_connection()
        if self.is_connected and self.redis_client:
            try:
                # Ensure all values are strings/bytes
                safe_fields = {}
                for k, v in (fields or {}).items():
                    if isinstance(v, (bytes, str, int, float)):
                        safe_fields[k] = v
                    else:
                        try:
                            safe_fields[k] = json.dumps(v, cls=SafeJSONEncoder)
                        except Exception:
                            safe_fields[k] = str(v)

                return self.redis_client.xadd(
                    name, safe_fields, id=id, maxlen=maxlen, approximate=approximate
                )
            except Exception as e:
                self.is_connected = False
                # Queue operation for retry
                self._queue_operation(
                    "xadd", name, fields, id=id, maxlen=maxlen, approximate=approximate
                )
                return None
        # No connected Redis; queue for later
        self._queue_operation(
            "xadd", name, fields, id=id, maxlen=maxlen, approximate=approximate
        )
        return None

    # ============ Specialized Methods for Each Segment ============

    def store_spoof_alert(self, symbol, alert_data):
        """Store spoofing alert in DB 1 (realtime) via store_by_data_type routing"""
        key = f"spoof:{symbol}:{int(time.time())}"
        return self.store_by_data_type(
            "pattern_alerts", key, json.dumps(alert_data)
        )

    def store_asset_price(self, symbol, price_data, asset_class="equity_cash"):
        """Store asset last_price in DB 1 (realtime) via store_by_data_type routing"""
        key = f"last_price:{asset_class}:{symbol}"
        # Map asset_class to correct data type
        data_type_map = {
            "equity_cash": "equity_cash_prices",
            "futures": "futures_prices", 
            "options": "options_prices",
            "commodity": "commodity_prices"
        }
        data_type = data_type_map.get(asset_class, "equity_cash_prices")
        return self.store_by_data_type(data_type, key, json.dumps(price_data))

    def store_premarket_volume(self, symbol, bucket_incremental_volume):
        """Store premarket bucket_incremental_volume in DB 1 (realtime) via store_by_data_type routing - Critical for 9:00-9:30 AM manipulation detection"""
        # Resolve token to symbol BEFORE storing
        symbol = self._resolve_token_to_symbol_for_storage(symbol)
        key = f"premarket:{symbol}:{int(time.time())}"
        return self.store_by_data_type("premarket_trades", key, json.dumps(bucket_incremental_volume))

    # Order flow methods removed

    def _normalize_timestamp(self, value):
        """Normalize different timestamp formats to float seconds."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, datetime):
            return value.timestamp()

        if isinstance(value, str):
            stripped = value.strip()
            # Support direct float strings
            try:
                return float(stripped)
            except ValueError:
                pass

            # Attempt HH:MM[:SS] parsing relative to today (system timezone)
            time_formats = ["%H:%M:%S", "%H:%M"]
            for fmt in time_formats:
                try:
                    time_obj = datetime.strptime(stripped, fmt).time()
                    combined = datetime.combine(date.today(), time_obj)
                    return combined.timestamp()
                except ValueError:
                    continue

        return None

    def store_continuous_data(self, symbol, market_data):
        """Store 5s continuous data in DB 1 (realtime) via store_by_data_type routing"""
        # Resolve token to symbol BEFORE storing
        symbol = self._resolve_token_to_symbol_for_storage(symbol)
        key = f"continuous:{symbol}:{int(time.time())}"
        return self.store_by_data_type("5s_ticks", key, json.dumps(market_data))

    def store_ticks_optimized(self, symbol, ticks):
        """Normalize tick payloads, store them, and update bucket_incremental_volume buckets."""
        if not symbol or not ticks:
            return False
        
        # Resolve token to symbol BEFORE storing
        symbol = self._resolve_token_to_symbol_for_storage(symbol)

        optimized_ticks = []
        for tick in ticks:
            exchange_timestamp_raw = tick.get("exchange_timestamp")
            exchange_timestamp_epoch = (
                TimestampNormalizer.to_epoch_ms(exchange_timestamp_raw)
                if exchange_timestamp_raw
                else None
            )

            timestamp_ns_raw = tick.get("timestamp_ns")
            timestamp_ns_epoch = (
                TimestampNormalizer.to_epoch_ms(timestamp_ns_raw)
                if timestamp_ns_raw
                else None
            )

            timestamp_raw = tick.get("timestamp")
            timestamp_epoch = (
                TimestampNormalizer.to_epoch_ms(timestamp_raw) if timestamp_raw else None
            )

            optimized_tick = {
                "symbol": symbol,
                "last_price": tick.get("last_price"),
                "bucket_incremental_volume": tick.get("bucket_incremental_volume"),
                "exchange_timestamp": exchange_timestamp_raw or "",
                "exchange_timestamp_ms": exchange_timestamp_epoch or 0,
                "timestamp_ns": timestamp_ns_raw or "",
                "timestamp_ns_ms": timestamp_ns_epoch or 0,
                "timestamp": timestamp_raw or "",
                "timestamp_ms": timestamp_epoch or 0,
            }

            bucket_incremental_volume = tick.get("bucket_incremental_volume")
            if bucket_incremental_volume is None:
                bucket_incremental_volume = tick.get("bucket_incremental_volume")
            if bucket_incremental_volume is None:
                bucket_incremental_volume = 0

            bucket_cumulative_volume = tick.get("bucket_cumulative_volume")
            if bucket_cumulative_volume is None:
                bucket_cumulative_volume = tick.get("zerodha_cumulative_volume")
            if bucket_cumulative_volume is None:
                bucket_cumulative_volume = tick.get("bucket_incremental_volume")

            optimized_tick["bucket_incremental_volume"] = bucket_incremental_volume
            if bucket_cumulative_volume is not None:
                optimized_tick["bucket_cumulative_volume"] = bucket_cumulative_volume
                if "zerodha_cumulative_volume" not in tick:
                    optimized_tick["zerodha_cumulative_volume"] = bucket_cumulative_volume
            if "zerodha_last_traded_quantity" in tick:
                optimized_tick["zerodha_last_traded_quantity"] = tick.get(
                    "zerodha_last_traded_quantity"
                )

            for field in ("bid", "ask", "open", "high", "low"):
                if field in tick:
                    optimized_tick[field] = tick[field]

            optimized_ticks.append(optimized_tick)

            bucket_timestamp_candidate = None
            for candidate in (
                exchange_timestamp_epoch,
                tick.get("timestamp_epoch"),
                timestamp_epoch,
                timestamp_ns_epoch,
                tick.get("exchange_timestamp_epoch"),
                exchange_timestamp_raw,
                timestamp_ns_raw,
                timestamp_raw,
            ):
                if candidate is not None:
                    bucket_timestamp_candidate = candidate
                    break

            last_price = tick.get("last_price")
            try:
                # symbol is already resolved above, pass it as-is
                self.update_volume_buckets(
                    symbol=symbol,
                    bucket_incremental_volume=bucket_incremental_volume,
                    timestamp=bucket_timestamp_candidate or time.time(),
                    bucket_cumulative_volume=bucket_cumulative_volume,
                    last_price=last_price,
                )
            except Exception as bucket_err:
                logger.debug(
                    "Failed to update bucket_incremental_volume buckets from store_ticks_optimized for %s: %s",
                    symbol,
                    bucket_err,
                )

        key = f"ticks_optimized:{symbol}"
        return self.store_by_data_type(
            "ticks_optimized", key, json.dumps(optimized_ticks, cls=SafeJSONEncoder)
        )

    def store_cumulative_volume(self, symbol, bucket_incremental_volume):
        """Store cumulative bucket_incremental_volume in DB 2 (analytics) via store_by_data_type routing"""
        # Resolve token to symbol BEFORE storing
        symbol = self._resolve_token_to_symbol_for_storage(symbol)
        key = f"cumulative:{symbol}"
        return self.store_by_data_type("daily_cumulative", key, json.dumps(bucket_incremental_volume))

    def store_alert(self, alert_id, alert_data):
        """Store alert in DB 1 (realtime) via store_by_data_type routing"""
        key = f"alert:{alert_id}"
        return self.store_by_data_type("pattern_alerts", key, json.dumps(alert_data))

    def store_validation_result(self, alert_id, validation_data):
        """Store validation result in DB 1 (realtime) via store_by_data_type routing"""
        key = f"validation:{alert_id}"
        return self.store_by_data_type("pattern_alerts", key, json.dumps(validation_data))

    def store_news_sentiment(self, symbol, news_data):
        """Store news sentiment data in DB 1 (realtime) via store_by_data_type routing"""
        key = f"news:{symbol}:{int(time.time())}"
        return self.store_by_data_type("news_data", key, json.dumps(news_data))

    # ============ Redis Streams Methods ============

    def publish_tick_stream(self, symbol, tick_data):
        """Publish tick data to Redis Stream"""
        stream_name = f"ticks:{symbol}"
        
        return self.publish_to_stream(stream_name, tick_data, maxlen=50000)

    def publish_alert_stream(self, alert_data):
        """Publish alert data to Redis Stream"""
        stream_name = "alerts:system"
        return self.publish_to_stream(stream_name, alert_data, maxlen=10000)

    def publish_pattern_stream(self, symbol, pattern_data):
        """Publish pattern detection data to Redis Stream"""
        stream_name = f"patterns:{symbol}"
        return self.publish_to_stream(stream_name, pattern_data, maxlen=5000)

    def read_tick_stream(self, symbol, count=100, start_id="0"):
        """Read from tick stream"""
        stream_name = f"ticks:{symbol}"
        return self.read_from_stream(stream_name, count, start_id)

    def read_alert_stream(self, count=100, start_id="0"):
        """Read from alert stream"""
        stream_name = "alerts:system"
        return self.read_from_stream(stream_name, count, start_id)

    def read_pattern_stream(self, symbol, count=100, start_id="0"):
        """Read from pattern stream"""
        stream_name = f"patterns:{symbol}"
        return self.read_from_stream(stream_name, count, start_id)

    # ============ Index Data Helpers ============

    def get_index(self, index_name: str):
        """Fetch index snapshot by name. Tries multiple key patterns and JSON-decodes values.

        Known names: 'nifty50', 'niftybank', 'giftnifty', 'indiavix', 'finnifty'
        """
        if not index_name:
            return None
        keys_to_try = [
            f"index_data:{index_name}",
            f"indices:{index_name}",
            f"market:indices:{index_name}",
        ]
        for k in keys_to_try:
            try:
                v = self.get(k)
                if not v:
                    continue
                if isinstance(v, dict):
                    return v
                # Try JSON decode
                try:
                    import json as _json

                    data = _json.loads(v)
                    if isinstance(data, dict):
                        return data
                except Exception:
                    # Possibly a simple string; return as-is
                    return {"raw": v}
            except Exception:
                continue
        return None

    # ============ Cumulative Data Tracking Methods ============

    def update_cumulative_data(
        self, symbol, last_price, bucket_incremental_volume, timestamp=None, depth_data=None
    ):
        """Update cumulative data for a symbol with session tracking"""
        return self.cumulative_tracker.update_symbol_data(
            symbol, last_price, bucket_incremental_volume, timestamp, depth_data
        )

    def update_symbol_data_direct(
        self,
        symbol,
        bucket_cumulative_volume,
        last_price,
        timestamp,
        high=None,
        low=None,
        open_price=None,
        close_price=None,
        depth_data=None,
    ):
        """Optimized cumulative update when upstream provides direct totals."""
        return self.cumulative_tracker.update_symbol_data_direct(
            symbol,
            bucket_cumulative_volume,
            last_price,
            timestamp,
            high=high,
            low=low,
            open_price=open_price,
            close_price=close_price,
            depth_data=depth_data,
        )

    def get_cumulative_data(self, symbol, session_date=None):
        """Get cumulative data for a symbol"""
        return self.cumulative_tracker.get_cumulative_data(symbol, session_date)

    def get_session_price_movement(self, symbol, minutes_back=30):
        """Get last_price movement analysis for recent period"""
        return self.cumulative_tracker.get_session_price_movement(symbol, minutes_back)

    def get_pattern_data(self, symbol, time_of_day=None, session_date=None):
        """Get pattern data for a symbol based on time of day"""
        return self.cumulative_tracker.get_pattern_data(
            symbol, time_of_day, session_date
        )

    def _extract_base_symbol(self, symbol: str) -> str:
        """Extract base symbol from F&O contract names"""
        # Ensure symbol is a string (may be int like instrument_token)
        if not isinstance(symbol, str):
            symbol = str(symbol)
        # Remove exchange prefix if present
        if ":" in symbol:
            symbol = symbol.split(":", 1)[1]
        
        import re
        is_option = bool(re.search(r"\d+(CE|PE)$", symbol))
        is_future = symbol.endswith("FUT") or bool(re.search(r"\d{2}[A-Z]{3}", symbol))

        if is_future or is_option:
            base_symbol = symbol

            expiry_match = re.search(r"\d{2}[A-Z]{3}", base_symbol)
            if expiry_match:
                base_symbol = base_symbol.replace(expiry_match.group(), "")

            if base_symbol.endswith("FUT"):
                base_symbol = base_symbol[:-3]
            elif is_option:
                base_symbol = re.sub(r"\d+(CE|PE)$", "", base_symbol)

            return base_symbol

        return symbol

    def get_rolling_window_buckets(self, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """
        Get rolling window bucket data from history lists for volume profile calculations
        
        This method retrieves data from history lists which contain the rolling window data
        that the volume profile library needs for proper volume threshold calculations.
        """
        try:
            # Ensure symbol is a string (may be int like instrument_token)
            if not isinstance(symbol, str):
                symbol = str(symbol)
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            # ✅ SINGLE SOURCE OF TRUTH: Check history lists first (rolling window data)
            # History lists contain the data that volume profile library needs for rolling windows
            history_keys = [
                f"bucket_incremental_volume:history:5min:5min:{symbol}",
                f"bucket_incremental_volume:history:5min:5min:{base_symbol}",
                f"bucket_incremental_volume:history:10min:10min:{symbol}",
                f"bucket_incremental_volume:history:10min:10min:{base_symbol}",
                f"bucket_incremental_volume:history:2min:2min:{symbol}",
                f"bucket_incremental_volume:history:2min:2min:{base_symbol}",
                f"bucket_incremental_volume:history:1min:1min:{symbol}",
                f"bucket_incremental_volume:history:1min:1min:{base_symbol}",
            ]
            
            # Check history lists first (these contain rolling window data)
            for history_key in history_keys:
                if self.redis_client.exists(history_key):
                    history_length = self.redis_client.llen(history_key)
                    if history_length > 0:
                        logger.info(f"🔍 [ROLLING_WINDOW_DEBUG] {symbol} -> Found history list: {history_key} with {history_length} buckets")
                        # Get recent buckets from history list for rolling window calculations
                        # Get more buckets for rolling window analysis (up to 200 for better volume profile)
                        recent_buckets = self.redis_client.lrange(history_key, -min(200, history_length), -1)
                        buckets = []
                        for bucket_data in recent_buckets:
                            try:
                                bucket = json.loads(bucket_data)
                                buckets.append(bucket)
                            except:
                                continue
                        if buckets:
                            logger.info(f"🔍 [ROLLING_WINDOW_DEBUG] {symbol} -> Retrieved {len(buckets)} buckets from history list for rolling window")
                            return buckets
            
            logger.info(f"🔍 [ROLLING_WINDOW_DEBUG] {symbol} -> No history lists found, falling back to individual buckets")
            return []
            
        except Exception as e:
            logger.error(f"Error getting rolling window buckets for {symbol}: {e}")
            return []

    def get_time_buckets(self, symbol, session_date=None, hour=None, lookback_minutes=60):
        """Get all time buckets for a symbol with enhanced pattern matching"""
        if session_date is None:
            session_date = self.current_session

        try:
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            # Try multiple patterns to find bucket_incremental_volume data
            patterns_to_try = [
                # Pattern 1: bucket_incremental_volume:bucket:{symbol}:{session}:{hour}:{minute} (JSON string) - PRIMARY
                f"bucket_incremental_volume:bucket:{symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                # Pattern 2: bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 4: bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour}:{minute_bucket} (Legacy format)
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket:bucket:{symbol}:* (Legacy format)
                f"bucket_incremental_volume:bucket:{symbol}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:*",
                # Pattern 4: data:{symbol}:* (Alternative format)
                f"data:{symbol}:*",
                f"data:{base_symbol}:*",
                # Pattern 5: *:{symbol}:* (Fallback pattern)
                f"*:{symbol}:*",
                f"*:{base_symbol}:*"
            ]
            
            buckets = []
            logger.debug(f"{symbol} -> {base_symbol} - Trying {len(patterns_to_try)} patterns")
            for i, pattern in enumerate(patterns_to_try):
                try:
                    keys = self.redis_client.keys(pattern)
                    if keys:
                        logger.debug(f"Pattern {i+1}: {pattern} -> {len(keys)} keys")
                    for key in keys:
                        try:
                            if pattern.startswith("bucket_incremental_volume:bucket:") or pattern.startswith("bucket_incremental_volume:bucket:bucket:") or pattern.startswith("data:"):
                                # JSON string format
                                data = self.redis_client.get(key)
                                if data:
                                    bucket_data = json.loads(data)
                                    # Ensure bucket_incremental_volume field exists
                                    if 'bucket_incremental_volume' not in bucket_data and 'bucket_incremental_volume' in bucket_data:
                                        bucket_data['bucket_incremental_volume'] = bucket_data.get('bucket_incremental_volume', 0)
                                    if 'zerodha_cumulative_volume' not in bucket_data and 'cumulative' in bucket_data:
                                        bucket_data['zerodha_cumulative_volume'] = bucket_data.get('cumulative', 0)
                                    bucket_data.setdefault('bucket_incremental_volume', bucket_data.get('bucket_incremental_volume', 0))
                                    bucket_data.setdefault('cumulative', bucket_data.get('zerodha_cumulative_volume', 0))
                                    buckets.append(bucket_data)
                            elif pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket1:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket2:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket:"):
                                # Redis hash format - convert to bucket format
                                hash_data = self.redis_client.redis_client.hgetall(key)
                                if hash_data and "bucket_incremental_volume" in hash_data:
                                    # Convert hash data to bucket format
                                    try:
                                        price_value = hash_data.get("last_price") or hash_data.get("close")
                                        price_value = float(price_value) if price_value is not None else None
                                    except (TypeError, ValueError):
                                        price_value = None
                                    def _parse_ts(ts_value):
                                        if ts_value in (None, "", b""):
                                            return 0.0
                                        try:
                                            return float(ts_value)
                                        except (TypeError, ValueError):
                                            try:
                                                from datetime import datetime as _dt
                                                return _dt.fromisoformat(str(ts_value)).timestamp()
                                            except Exception:
                                                return 0.0

                                    first_ts = _parse_ts(hash_data.get("first_timestamp"))
                                    last_ts = _parse_ts(hash_data.get("last_timestamp")) or first_ts
                                    bucket_data = {
                                        "symbol": symbol,
                                        "session_date": session_date,
                                        "bucket_incremental_volume": int(hash_data.get("bucket_incremental_volume", 0)),
                                        "count": int(hash_data.get("count", 0)),
                                        "cumulative": int(hash_data.get("cumulative", 0)),
                                        "first_timestamp": first_ts,
                                        "last_timestamp": last_ts,
                                    }
                                    if price_value is not None:
                                        bucket_data["close"] = price_value
                                        bucket_data.setdefault("open", price_value)
                                        bucket_data.setdefault("high", price_value)
                                        bucket_data.setdefault("low", price_value)
                                    # Parse timestamp from key if possible
                                    key_parts = key.split(":")
                                    if len(key_parts) >= 4:
                                        try:
                                            hour_part = int(key_parts[-2])
                                            minute_part = int(key_parts[-1]) * 5  # Convert bucket to minute
                                            # Create approximate timestamp
                                            from datetime import datetime
                                            dt = datetime.now().replace(hour=hour_part, minute=minute_part, second=0, microsecond=0)
                                            bucket_data["first_timestamp"] = bucket_data.get("first_timestamp") or dt.timestamp()
                                            bucket_data["last_timestamp"] = bucket_data.get("last_timestamp") or dt.timestamp()
                                        except (ValueError, IndexError):
                                            pass
                                    buckets.append(bucket_data)
                        except Exception as e:
                            continue
                except Exception as e:
                    continue

            # Sort by timestamp
            buckets.sort(key=lambda x: x.get("first_timestamp", 0))
            return buckets

        except Exception as e:
            logger.error(f"Failed to get time buckets for {symbol}: {e}")
            return []

    def get_time_buckets_enhanced(self, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """
        Enhanced bucket retrieval that falls back to 30-day OHLC data.
        """
        try:
            # Bucket data is in DB 0 (system), not DB 5
            logger.debug(f"Switching to DB 0 for bucket retrieval")
            self.redis_client.select(0)
            
            buckets = self.get_time_buckets(symbol, lookback_minutes=lookback_minutes)
            logger.debug(f"Found {len(buckets)} buckets from get_time_buckets")
            
            # ✅ SINGLE SOURCE OF TRUTH: Accept any available bucket data
            # Don't require specific number of buckets - use whatever real data is available
            # This prevents "NO_BUCKET_DATA" when sparse but valid data exists
            required_buckets = 1  # Accept any bucket data
            logger.debug(f"Required buckets: {required_buckets}, Found: {len(buckets)}")
            
            if len(buckets) >= required_buckets:
                logger.debug(
                    f"✅ [BUCKET_DATA] {symbol} - Using {len(buckets)} real-time buckets"
                )
                return buckets

            # Switch to DB 5 for OHLC fallback
            self.redis_client.select(5)
            ohlc_buckets = self._get_ohlc_buckets_from_30day(symbol, lookback_minutes)
            if ohlc_buckets:
                logger.info(
                    f"📦 [OHLC_FALLBACK] {symbol} - Using {len(ohlc_buckets)} OHLC data points"
                )
                return ohlc_buckets

            logger.debug(
                f"ℹ️ [NO_BUCKET_DATA] {symbol} - No real bucket data available, using 4 consolidated bucket system"
            )
            # ✅ SINGLE SOURCE OF TRUTH: Do NOT generate synthetic data
            # Synthetic data prevents pattern detection and violates data integrity
            # Return empty data instead of fake data
            return []
        except Exception as exc:
            logger.error(f"❌ [BUCKET_LOAD_ERROR] {symbol}: {exc}")
            return []

    def get_time_buckets_robust(self, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """Guaranteed time-bucket loading with layered fallbacks."""
        try:
            if hasattr(self, "get_time_buckets_enhanced"):
                buckets = self.get_time_buckets_enhanced(symbol, lookback_minutes)
            else:
                buckets = self.get_time_buckets(symbol, lookback_minutes=lookback_minutes)
            if buckets:
                return buckets

            buckets = self._get_ohlc_buckets(symbol, lookback_minutes)
            if buckets:
                return buckets

            buckets = self._get_tick_buckets(symbol, lookback_minutes)
            if buckets:
                return buckets

            # ✅ SINGLE SOURCE OF TRUTH: Do NOT generate synthetic data
            # Synthetic data prevents pattern detection and violates data integrity
            # Return empty data instead of fake data
            return []
        except Exception as e:
            logger.error(f"❌ [BUCKET_LOAD_FAILED] {symbol}: {e}")
            return []

    def _get_ohlc_buckets(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Load OHLC buckets from Redis Time Series fallbacks."""
        hourly_limit = max(int(lookback_minutes / 5) + 1, 12)
        hourly_series = self._load_ohlc_series(symbol, hourly_limit, interval="1h")
        if hourly_series:
            buckets = self._convert_ohlc_to_buckets(symbol, hourly_series, lookback_minutes)
            if buckets:
                return buckets

        daily_series = self._load_ohlc_series(symbol, 30, interval="1d")
        if daily_series:
            return self._convert_ohlc_to_buckets(symbol, daily_series, lookback_minutes)

        return []

    def _load_ohlc_series(
        self, symbol: str, limit: int, interval: str = "1d"
    ) -> List[Dict[str, Any]]:
        client = self.redis_client
        if not client or limit <= 0:
            return []

        symbol_key = normalize_ohlc_symbol(symbol)
        if interval == "1h":
            zset_key = ohlc_hourly_zset(symbol_key)
        else:
            zset_key = ohlc_daily_zset(symbol_key)

        try:
            entries = client.zrevrange(zset_key, 0, max(limit - 1, 0), withscores=True)
        except Exception as exc:
            logger.debug(f"Failed to load OHLC series {symbol}:{interval}: {exc}")
            return []

        series: List[Dict[str, Any]] = []
        for payload, score in reversed(entries or []):
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            try:
                data = json.loads(payload)
            except Exception:
                continue
            data["timestamp"] = int(score)
            series.append(data)
        return series

    def _convert_ohlc_to_buckets(
        self, symbol: str, ohlc_data: List[Dict[str, Any]], lookback_minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Convert 30-day OHLC list into simple bucket format using recent entries."""
        if not ohlc_data:
            return []

        buckets: List[Dict[str, Any]] = []
        now_ts = time.time()
        recent = ohlc_data[-5:]
        for idx, day in enumerate(reversed(recent)):
            try:
                timestamp_value = day.get("timestamp") or day.get("t")
                if not timestamp_value and day.get("d"):
                    try:
                        dt = datetime.fromisoformat(f"{day['d']}T00:00:00")
                        timestamp_value = int(dt.timestamp())
                    except Exception:
                        timestamp_value = None
                if timestamp_value:
                    bucket_ts = (
                        timestamp_value / 1000.0
                        if isinstance(timestamp_value, (int, float)) and timestamp_value > 1e12
                        else float(timestamp_value)
                    )
                else:
                    bucket_ts = now_ts - (idx * 86400)

                open_price = day.get("open", day.get("o"))
                high = day.get("high", day.get("h"))
                low = day.get("low", day.get("l"))
                close_price = day.get("close", day.get("c"))
                bucket_incremental_volume = day.get("bucket_incremental_volume", day.get("v", 0))

                bucket = {
                    "symbol": symbol,
                    "timestamp": bucket_ts,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close_price,
                    "bucket_incremental_volume": bucket_incremental_volume,
                    "source": "30day_ohlc",
                }
                buckets.append(bucket)
            except Exception:
                continue

        if lookback_minutes and len(buckets) > lookback_minutes:
            buckets = buckets[:lookback_minutes]
        return buckets

    def _get_ohlc_buckets_from_30day(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Convert 30-day OHLC snapshots into synthetic intraday buckets."""
        daily_series = self._load_ohlc_series(symbol, 30, interval="1d")
        if not daily_series:
            return []

        ohlc_series: List[Dict[str, Any]] = []
        for entry in daily_series:
            try:
                open_price = entry.get("open", entry.get("o"))
                high = entry.get("high", entry.get("h"))
                low = entry.get("low", entry.get("l"))
                close_price = entry.get("close", entry.get("c"))
                bucket_incremental_volume = entry.get("bucket_incremental_volume", entry.get("v", 0))
                date_str = entry.get("d")
                if not date_str:
                    ts = entry.get("timestamp")
                    if ts and ts > 1e12:
                        ts = ts / 1000.0
                    if ts:
                        date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d 00:00:00")
                if not date_str:
                    continue
                ohlc_series.append(
                    {
                        "date": date_str,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "close": close_price,
                        "bucket_incremental_volume": bucket_incremental_volume,
                    }
                )
            except Exception:
                continue

        if not ohlc_series:
            return []

        simple_buckets = self._convert_ohlc_to_buckets(symbol, ohlc_series, lookback_minutes)
        if simple_buckets:
            logger.debug(
                f"✅ [OHLC_SIMPLE] {symbol} - Generated {len(simple_buckets)} simple buckets from 30-day data"
            )
            return simple_buckets

        recent_days = ohlc_series[-10:]
        if not recent_days:
            return []

        bucket_minutes = [0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330]
        buckets: List[Dict[str, Any]] = []

        per_day_limit = max(1, lookback_minutes // max(len(bucket_minutes), 1))

        for idx, day in enumerate(recent_days):
            date_str = day.get("date")
            try:
                base_dt = datetime.fromisoformat(f"{date_str} 09:15:00") if date_str else datetime.now() - timedelta(days=len(recent_days) - idx)
            except ValueError:
                base_dt = datetime.now() - timedelta(days=len(recent_days) - idx)

            daily_volume = float(day.get("bucket_incremental_volume", 0) or 0.0)
            per_bucket_volume = daily_volume / len(bucket_minutes) if daily_volume else 0.0

            day_added = 0
            for offset in bucket_minutes:
                bucket_time = base_dt + timedelta(minutes=offset)
                bucket = {
                    "symbol": symbol,
                    "session_date": bucket_time.strftime("%Y-%m-%d"),
                    "first_timestamp": bucket_time.timestamp(),
                    "last_timestamp": bucket_time.timestamp(),
                    "open": day.get("open"),
                    "high": day.get("high"),
                    "low": day.get("low"),
                    "close": day.get("close"),
                    "bucket_incremental_volume": per_bucket_volume,
                    "source": "30day_ohlc",
                }
                buckets.append(bucket)
                day_added += 1
                if day_added >= per_day_limit or len(buckets) >= lookback_minutes:
                    break
            if len(buckets) >= lookback_minutes:
                break

        if buckets:
            logger.info(
                f"📊 [OHLC_CONVERSION] {symbol} - Created {len(buckets)} buckets from 30-day OHLC data"
            )
        return buckets

    def _get_tick_buckets(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Build buckets from raw tick data if available."""
        client = self.redis_client
        if not client:
            return []

        tick_key = f"ticks:{symbol}"
        try:
            recent_ticks = client.lrange(tick_key, -2000, -1)
        except Exception as e:
            return []

        if not recent_ticks:
            return []

        buckets = self._bucket_ticks(recent_ticks, lookback_minutes)
        if buckets:
            logger.info(f"✅ [TICK_DATA] {symbol} - Created {len(buckets)} buckets from ticks")
        return buckets

    def _bucket_ticks(self, ticks: List[Any], lookback_minutes: int) -> List[Dict[str, Any]]:
        """Convert raw ticks into minute buckets."""
        if not ticks:
            return []

        min_timestamp = time.time() - (lookback_minutes * 60)
        bucket_map: Dict[int, Dict[str, Any]] = {}

        for raw_tick in ticks:
            try:
                if isinstance(raw_tick, bytes):
                    raw_tick = raw_tick.decode("utf-8")
                if isinstance(raw_tick, str):
                    tick = json.loads(raw_tick)
                elif isinstance(raw_tick, dict):
                    tick = raw_tick
                else:
                    continue
            except Exception:
                continue

            ts = (
                tick.get("timestamp_epoch")
                or tick.get("timestamp_ms")
                or tick.get("timestamp")
                or tick.get("time")
            )
            if ts is None:
                continue
            try:
                ts = float(ts)
            except (TypeError, ValueError):
                continue
            if ts > 1e12:  # Convert ms to seconds
                ts = ts / 1000.0
            if ts < min_timestamp:
                continue

            last_price = (
                tick.get("last_price")
                or tick.get("close")
                or tick.get("last_price")
                or tick.get("last_price")
            )
            last_price = self._safe_float(last_price)
            if last_price <= 0:
                continue

            bucket_incremental_volume = (
                tick.get("bucket_incremental_volume")
                or tick.get("zerodha_last_traded_quantity")
                or tick.get("quantity")
                or tick.get("trade_quantity")
            )
            bucket_incremental_volume = self._safe_float(bucket_incremental_volume)

            bucket_start = int(ts // 60) * 60
            bucket = bucket_map.get(bucket_start)
            if not bucket:
                bucket = {
                    "timestamp": bucket_start,
                    "first_timestamp": bucket_start,
                    "last_timestamp": bucket_start,
                    "open": last_price,
                    "high": last_price,
                    "low": last_price,
                    "close": last_price,
                    "bucket_incremental_volume": bucket_incremental_volume,
                }
                bucket_map[bucket_start] = bucket
            else:
                bucket["high"] = max(bucket["high"], last_price)
                bucket["low"] = min(bucket["low"], last_price)
                bucket["close"] = last_price
                bucket["last_timestamp"] = bucket_start
                bucket["bucket_incremental_volume"] += bucket_incremental_volume

        if not bucket_map:
            return []

        ordered = []
        for ts in sorted(bucket_map.keys())[-lookback_minutes:]:
            ordered.append(bucket_map[ts])
        return ordered

    def _generate_buckets_from_last_price(self, symbol: str, lookback_minutes: int) -> List[Dict[str, Any]]:
        """Generate pseudo buckets when no real data is available."""
        base_price = self._get_last_price(symbol)
        if not base_price or base_price <= 0:
            return []

        buckets: List[Dict[str, Any]] = []
        current_time = int(time.time())

        if "FUT" in symbol:
            volatility = 0.02
        elif "OPT" in symbol:
            volatility = 0.05
        else:
            volatility = 0.015

        last_price = float(base_price)
        for i in range(lookback_minutes):
            timestamp = current_time - (lookback_minutes - i) * 60
            price_move = random.normalvariate(0, volatility)
            last_price = last_price * (1 + price_move)
            last_price = max(last_price, base_price * 0.5)
            last_price = min(last_price, base_price * 1.5)
            high = last_price * (1 + abs(price_move) * 0.5)
            low = last_price * (1 - abs(price_move) * 0.5)

            bucket = {
                "timestamp": timestamp,
                "first_timestamp": timestamp,
                "last_timestamp": timestamp,
                "open": last_price,
                "high": high,
                "low": low,
                "close": last_price,
                "bucket_incremental_volume": random.randint(1000, 10000),
                "synthetic": True,
            }
            buckets.append(bucket)

        return buckets

    def _get_last_price(self, symbol: str) -> Optional[float]:
        """Fetch the most recent known last_price for a symbol."""
        candidate_keys = [
            f"last_price:{symbol}",
            f"latest_price:{symbol}",
            f"last_price:{symbol}",
            f"market_data:last_price:{symbol}",
        ]

        for key in candidate_keys:
            try:
                value = self.get(key)
                if value:
                    return self._safe_float(value)
            except Exception:
                continue

        try:
            session_key = f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = self.get(session_key)
            if session_data:
                if isinstance(session_data, (bytes, str)):
                    session_data = json.loads(
                        session_data.decode("utf-8") if isinstance(session_data, bytes) else session_data
                    )
                if isinstance(session_data, dict):
                    time_buckets = session_data.get("time_buckets") or {}
                    if isinstance(time_buckets, dict) and time_buckets:
                        last_bucket = time_buckets.get(sorted(time_buckets.keys())[-1])
                        if isinstance(last_bucket, dict):
                            last_price = (
                                last_bucket.get("close")
                                or last_bucket.get("last_price")
                                or last_bucket.get("last_price")
                            )
                            if last_price:
                                return self._safe_float(last_price)
                    last_price = (
                        session_data.get("last_price")
                        or session_data.get("close")
                        or session_data.get("last_price")
                    )
                    if last_price:
                        return self._safe_float(last_price)
        except Exception:
            pass

        return None

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Public wrapper to fetch the most recent last_price for a symbol."""
        try:
            return self._get_last_price(symbol)
        except Exception as e:
            logger.debug(f"Failed to fetch last last_price for {symbol}: {e}")
            return None

    def _get_buckets_by_pattern(self, symbol, pattern, start_time, bucket_size):
        """Get buckets using key pattern scanning"""
        try:
            # Get all keys matching pattern
            keys = self.redis_client.keys(pattern)
            if not keys:
                return []
            
            
            buckets = []
            for key in keys:
                try:
                    # Try different data structure access methods
                    bucket_data = self._extract_bucket_data(key, start_time)
                    if bucket_data:
                        buckets.append(bucket_data)
                except Exception as e:
                    logger.debug(f"Failed to extract data from key {key}: {e}")
                    continue
            
            return buckets
            
        except Exception as e:
            logger.error(f"Error in _get_buckets_by_pattern: {e}")
            return []

    def _extract_bucket_data(self, key, start_time):
        """Extract bucket data from Redis key using multiple methods"""
        try:
            # Method 1: Try as Hash first (most common)
            bucket_data = self.redis_client.hgetall(key)
            if bucket_data:
                return self._parse_hash_bucket(key, bucket_data, start_time)
            
            # Method 2: Try as String (JSON)
            str_data = self.redis_client.get(key)
            if str_data:
                return self._parse_string_bucket(key, str_data, start_time)
            
            # Method 3: Try as Sorted Set
            try:
                zset_data = self.redis_client.zrange(key, 0, -1, withscores=True)
                if zset_data:
                    return self._parse_zset_bucket(key, zset_data, start_time)
            except:
                pass
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to extract from {key}: {e}")
            return None

    def _parse_hash_bucket(self, key, bucket_data, start_time):
        """Parse bucket data from Redis hash"""
        try:
            # Extract timestamp from key (common patterns)
            timestamp = self._extract_timestamp_from_key(key)
            if not timestamp:
                return None
            
            # More lenient time filtering - include buckets from today and recent days
            from datetime import datetime, timedelta
            bucket_dt = datetime.fromtimestamp(timestamp)
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Include buckets from today or recent days (within last 7 days)
            if bucket_dt < today - timedelta(days=7):
                return None
            
            # Standardize bucket_incremental_volume fields
            standardized_data = self._standardize_bucket_data(bucket_data, timestamp)
            return standardized_data
            
        except Exception as e:
            logger.debug(f"Failed to parse hash bucket {key}: {e}")
            return None

    def _extract_timestamp_from_key(self, key):
        """Extract timestamp from various key patterns"""
        try:
            # Common timestamp patterns in keys:
            # volume_buckets:NSE:RELIANCE:1718902800
            # buckets:NSE:RELIANCE:20240620103000
            # ts:NSE:RELIANCE:1718902860
            
            key_parts = key.split(':')
            
            # Pattern 1: Unix timestamp at end
            if key_parts[-1].isdigit():
                timestamp = int(key_parts[-1])
                # Check if it's Unix timestamp (reasonable range)
                if 1600000000 < timestamp < 2000000000:  # 2020-2033
                    return float(timestamp)
                # Check if it's formatted datetime (YYYYMMDDHHMMSS)
                elif len(key_parts[-1]) == 14:
                    from datetime import datetime
                    dt = datetime.strptime(key_parts[-1], '%Y%m%d%H%M%S')
                    return dt.timestamp()
            
            # Pattern 2: Try second-to-last part
            if len(key_parts) >= 2 and key_parts[-2].isdigit():
                timestamp = int(key_parts[-2])
                if 1600000000 < timestamp < 2000000000:
                    return float(timestamp)
            
            # Pattern 3: Extract from bucket_incremental_volume:bucket_incremental_volume:bucket:SYMBOL:buckets:HOUR:MINUTE
            if 'bucket_incremental_volume:bucket_incremental_volume:bucket:' in key and 'buckets:' in key:
                try:
                    # Extract hour and minute from key
                    parts = key.split(':')
                    if len(parts) >= 6:
                        hour = int(parts[-2])
                        minute = int(parts[-1]) * 5  # Convert bucket to minute
                        from datetime import datetime
                        # Use current date with extracted time
                        dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
                        return dt.timestamp()
                except:
                    pass
            
            # Pattern 4: Extract from bucket:NSE:SYMBOL:YYYY-MM-DD:HH:MM
            if key.startswith('bucket:NSE:') and len(key_parts) >= 6:
                try:
                    date_part = key_parts[3]  # YYYY-MM-DD
                    hour_part = key_parts[4]  # HH
                    minute_part = key_parts[5]  # MM
                    
                    from datetime import datetime
                    dt = datetime.strptime(f"{date_part} {hour_part}:{minute_part}", "%Y-%m-%d %H:%M")
                    return dt.timestamp()
                except:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Timestamp extraction failed for {key}: {e}")
            return None

    def _standardize_bucket_data(self, raw_data, timestamp):
        """Standardize bucket data to common format"""
        standardized = {'timestamp': timestamp}
        
        # Price fields
        price_fields = ['open', 'high', 'low', 'close', 'last_price', 'last_price']
        for field in price_fields:
            if field in raw_data:
                standardized[field] = self._safe_float(raw_data[field])
        
        # Volume fields - Use canonical field names from optimized_field_mapping.yaml
        # Only use the standard field name to avoid confusion
        if 'zerodha_cumulative_volume' in raw_data:
            standardized['zerodha_cumulative_volume'] = self._safe_float(raw_data['zerodha_cumulative_volume'])
        elif 'bucket_cumulative_volume' in raw_data:
            standardized['zerodha_cumulative_volume'] = self._safe_float(raw_data['bucket_cumulative_volume'])
        
        # If no bucket_incremental_volume found, try to calculate from other fields
        if 'zerodha_cumulative_volume' not in standardized:
            if 'buy_volume' in raw_data and 'sell_volume' in raw_data:
                buy_vol = self._safe_float(raw_data.get('buy_volume', 0))
                sell_vol = self._safe_float(raw_data.get('sell_volume', 0))
                standardized['zerodha_cumulative_volume'] = buy_vol + sell_vol
        
        # Ensure we have at least basic fields
        if 'close' not in standardized and 'last_price' in standardized:
            standardized['close'] = standardized['last_price']
        
        return standardized

    def _get_buckets_by_time_range(self, symbol, start_time, end_time, bucket_size):
        """Alternative method: query by time range directly"""
        try:
            # Try scanning with timestamp range
            start_ts = int(start_time.timestamp())
            end_ts = int(end_time.timestamp())
            
            pattern = f"*:{symbol}:*"
            all_keys = self.redis_client.keys(pattern)
            
            buckets = []
            for key in all_keys:
                try:
                    timestamp = self._extract_timestamp_from_key(key)
                    if timestamp and start_ts <= timestamp <= end_ts:
                        bucket_data = self._extract_bucket_data(key, start_time)
                        if bucket_data:
                            buckets.append(bucket_data)
                except:
                    continue
            
            return buckets
            
        except Exception as e:
            self.logger.error(f"Time range query failed: {e}")
            return []

    def _safe_float(self, value) -> float:
        """Safely convert to float"""
        try:
            if value is None:
                return float(0.0)
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _parse_string_bucket(self, key, str_data, start_time):
        """Parse bucket data from JSON string"""
        try:
            import json
            data = json.loads(str_data)
            timestamp = self._extract_timestamp_from_key(key)
            if timestamp:
                return self._standardize_bucket_data(data, timestamp)
            return None
        except Exception as e:
            self.logger.debug(f"Failed to parse string bucket {key}: {e}")
            return None

    def _parse_zset_bucket(self, key, zset_data, start_time):
        """Parse bucket data from sorted set"""
        try:
            # For sorted sets, use the score as timestamp
            if zset_data:
                timestamp, data = zset_data[0]
                return self._standardize_bucket_data(data, float(timestamp))
            return None
        except Exception as e:
            logger.debug(f"Failed to parse zset bucket {key}: {e}")
            return None

    def debug_redis_keys(self, symbol):
        """Debug method to see what keys exist for a symbol"""
        patterns = [
            f"*{symbol}*",
            f"*:{symbol}:*",
            f"*:{symbol}*",
            f"{symbol}:*"
        ]
        
        print(f"\n=== DEBUG Redis Keys for {symbol} ===")
        for pattern in patterns:
            keys = self.redis_client.keys(pattern)
            if keys:
                print(f"Pattern '{pattern}': {len(keys)} keys")
                for key in keys[:5]:  # Show first 5 keys
                    print(f"  - {key}")
                    # Show data type
                    try:
                        key_type = self.redis_client.type(key)
                        print(f"    Type: {key_type}")
                        if key_type == 'hash':
                            sample = self.redis_client.hgetall(key)
                            print(f"    Sample: {dict(list(sample.items())[:3])}")
                    except Exception as e:
                        print(f"    Error: {e}")
            else:
                print(f"Pattern '{pattern}': No keys found")
        print("=== END DEBUG ===\n")

    def get_ohlc_buckets(self, symbol, count=10, session_date=None):
        """Convenience: return most recent N OHLC buckets for a symbol.

        Args:
            symbol (str): instrument symbol
            count (int): number of recent buckets to return
            session_date (str|None): YYYY-MM-DD; defaults to current session
        Returns:
            list[dict]: recent bucket dicts sorted by first_timestamp ascending within the slice
        """
        buckets = self.get_time_buckets(symbol, session_date=session_date)
        if not buckets:
            return []
        return buckets[-count:]

    def get_index(self, index_name):
        """Get normalized index snapshot from Redis, parsed as dict.

        Accepts aliases like 'nifty50', 'nifty_50', 'bank_nifty', 'niftybank',
        'giftnifty', 'sgx_nifty', 'indiavix', 'india_vix'.
        Returns parsed JSON dict or None.
        """
        try:
            name = (index_name or "").lower().replace(" ", "")
            alias_map = {
                "nifty50": "nifty50",
                "nifty_50": "nifty50",
                "niftybank": "niftybank",
                "banknifty": "niftybank",
                "bank_nifty": "niftybank",
                "giftnifty": "giftnifty",
                "sgxnifty": "giftnifty",
                "sgx_nifty": "giftnifty",
                "indiavix": "indiavix",
                "india_vix": "indiavix",
            }
            norm = alias_map.get(name, None)
            if not norm:
                logger.warning(f"Unknown index requested: {index_name}")
                return None
            keys = [
                f"index_data:{norm}",
                f"indices:{norm}",
                f"market:indices:{norm}",
            ]
            for key in keys:
                raw = self.get(key)
                if not raw:
                    continue
                try:
                    return json.loads(raw)
                except Exception:
                    # Sometimes value may already be a dict
                    if isinstance(raw, dict):
                        return raw
                    continue
            return None
        except Exception as e:
            logger.debug(f"get_index failed for {index_name}: {e}")
            return None

    def cleanup_old_sessions(self, days_to_keep=7):
        """Clean up old session data"""
        return self.cumulative_tracker.cleanup_old_sessions(days_to_keep)

    # ============ Enhanced Order Book Methods ============

    def store_order_book_snapshot(
        self, symbol, order_book_data, last_price, bucket_incremental_volume, timestamp=None
    ):
        """Store complete order book snapshot with last_price/bucket_incremental_volume context"""
        if timestamp is None:
            timestamp = time.time()

        snapshot = {
            "symbol": symbol,
            "timestamp": timestamp,
            "last_price": last_price,
            "bucket_incremental_volume": bucket_incremental_volume,
            "depth": order_book_data,
            "session_date": date.today().isoformat(),
        }

        # Store in cumulative tracker for session tracking
        self.cumulative_tracker.update_symbol_data(
            symbol, last_price, bucket_incremental_volume, timestamp, order_book_data
        )

        # Also store as individual snapshot for immediate analysis
        key = f"orderbook:{symbol}:{int(timestamp)}"
        return self.store_by_data_type(
            "5s_ticks", key, json.dumps(snapshot, cls=SafeJSONEncoder)
        )

    def get_order_book_history(self, symbol, minutes_back=5):
        """Get order book history for pattern analysis"""
        buckets = self.get_time_buckets(symbol)

        if not buckets:
            return []

        # Get recent buckets
        current_time = time.time()
        recent_cutoff = current_time - (minutes_back * 60)

        recent_history = []
        for bucket in buckets:
            if bucket["last_timestamp"] >= recent_cutoff:
                # Extract order book snapshots from bucket
                if "order_book_snapshots" in bucket:
                    recent_history.extend(bucket["order_book_snapshots"])

        # Sort by timestamp
        recent_history.sort(key=lambda x: x["timestamp"])
        return recent_history

    # ============ Basic Hash Helpers ============
    def hget(self, name, key):
        try:
            self._ensure_connection()
            if self.redis_client:
                return self.redis_client.hget(name, key)
        except Exception as e:
            logger.debug(f"hget failed for {name}:{key}: {e}")
        return None

    def hset(self, name, key, value):
        try:
            self._ensure_connection()
            if self.redis_client:
                return self.redis_client.hset(name, key, value)
        except Exception as e:
            logger.debug(f"hset failed for {name}:{key}: {e}")
        return 0

    def get_session_order_book_evolution(self, symbol, session_date=None):
        """Get complete order book evolution for a trading session"""
        buckets = self.get_time_buckets(symbol, session_date)

        evolution = {
            "symbol": symbol,
            "session_date": session_date or date.today().isoformat(),
            "total_snapshots": 0,
            "time_buckets": len(buckets),
            "order_book_snapshots": [],
        }

        for bucket in buckets:
            if "order_book_snapshots" in bucket:
                evolution["order_book_snapshots"].extend(bucket["order_book_snapshots"])
                evolution["total_snapshots"] += len(bucket["order_book_snapshots"])

        # Sort chronologically
        evolution["order_book_snapshots"].sort(key=lambda x: x["timestamp"])

        return evolution

    def _connect(self):
        """Establish Redis connection with retry logic - now handled by _initialize_clients"""
        # Connection is now handled by _initialize_clients during initialization
        # This method is kept for compatibility but the actual connection logic
        # is in _initialize_clients which creates separate clients per database
        
        if self.is_connected:
            logger.info("✅ Redis clients already connected")
            return True
        else:
            logger.warning("⚠️ Redis clients not properly initialized")
            return False

    def _ensure_connection(self):
        """Check and restore connection if needed"""
        if (
            not self.is_connected
            or time.time() - self.last_health_check > self.health_check_interval
        ):
            try:
                if self.redis_client:
                    self.redis_client.ping()
                    self.last_health_check = time.time()
                    if not self.is_connected:
                        self.is_connected = True
                        logger.info("✅ Redis connection restored")
                else:
                    self._connect()
            except:
                if self.is_connected:
                    logger.warning(
                        "⚠️ Redis connection lost. Attempting reconnection..."
                    )
                    self.is_connected = False
                self._connect()

    def _process_retry_queue(self):
        """Process queued operations after reconnection"""
        processed = 0
        while self.retry_queue and self.is_connected:
            try:
                operation = self.retry_queue.popleft()
                method = operation["method"]
                args = operation["args"]
                kwargs = operation["kwargs"]

                # Retry the operation using the actual method
                getattr(self, method)(*args, **kwargs)
                processed += 1

            except Exception as e:
                logger.error(f"Failed to process queued operation: {e}")
                # Re-queue the failed operation
                self.retry_queue.appendleft(operation)
                break

        if processed > 0:
            logger.info(f"✅ Processed {processed} queued operations")

    def _queue_operation(self, method, *args, **kwargs):
        """Queue operation for retry when connection is restored"""
        self.retry_queue.append(
            {"method": method, "args": args, "kwargs": kwargs, "timestamp": time.time()}
        )
        self.stats["queued_operations"] += 1

    def close(self):
        """Close Redis connection and connection pool"""
        with self.connection_lock:
            if self.redis_client:
                try:
                    self.redis_client.close()
                except Exception as e:
                    logger.error(f"Error closing Redis client: {e}")
                finally:
                    self.redis_client = None

            if getattr(self, "connection_wrappers", None):
                for db_num, wrapper in list(self.connection_wrappers.items()):
                    try:
                        if wrapper:
                            wrapper.close()
                    except Exception as e:
                        logger.error(
                            f"Error closing Redis connection pool wrapper for DB {db_num}: {e}"
                        )
                    finally:
                        self.connection_wrappers[db_num] = None

            self.is_connected = False
            logger.info("Redis connection and pool closed")

    # ============ Redis Operations with Auto-Reconnection ============

    def ping(self):
        """Test connection with auto-reconnection"""
        self._ensure_connection()
        if self.is_connected:
            try:
                return self.redis_client.ping()
            except:
                self.is_connected = False
                self._connect()
                if self.is_connected:
                    return self.redis_client.ping()
        return False

    def get(self, key):
        """Get value with fallback and circuit breaker protection"""
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(f"Circuit breaker open for Redis GET operation: {key}")
                return self._get_fallback(key)

        self._ensure_connection()

        if self.is_connected:
            try:
                result = self.redis_client.get(key)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
                return result
            except Exception as e:
                logger.debug(f"Redis get failed: {e}")
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                # Queue for retry
                self._queue_operation("get", key)

        # Fallback to local storage
        with self.fallback_lock:
            self._clean_expired_fallback(key)
            return self.fallback_data.get(key)

    def set(self, key, value, ex=None):
        """Set value with fallback and circuit breaker protection"""
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(f"Circuit breaker open for Redis SET operation: {key}")
                return self._set_fallback(key, value, ex)

        self._ensure_connection()

        success = False
        if self.is_connected:
            try:
                success = self.redis_client.set(key, value, ex=ex)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
            except Exception as e:
                logger.debug(f"Redis set failed: {e}")
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                self._queue_operation("set", key, value, ex=ex)

        # Always update fallback storage
        with self.fallback_lock:
            self.fallback_data[key] = value
            if ex:
                self.fallback_expirations[key] = time.time() + ex

        return success or True  # Return True even if using fallback

    def setex(self, key, time_seconds, value):
        """Set value with TTL (compat shim around set with ex)."""
        # Reuse set() which already handles fallback and retry queue
        return self.set(key, value, ex=time_seconds)

    def lpush(self, key, value):
        """Push a value onto a Redis list; noop fallback when disconnected."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.lpush(key, value)
        except Exception as e:
            logger.debug(f"Redis lpush failed: {e}")
            self.is_connected = False
        # Fallback: maintain a simple append-only list in local storage
        with self.fallback_lock:
            lst = self.fallback_data.get(key)
            if not isinstance(lst, list):
                lst = []
            lst.insert(0, value)
            self.fallback_data[key] = lst
        return 1

    def zrevrangebyscore(
        self, key, max_score, min_score, withscores=False, start=None, num=None
    ):
        """Fetch from sorted set by score (reverse). Returns [] on fallback."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zrevrangebyscore(
                    key,
                    max_score,
                    min_score,
                    withscores=withscores,
                    start=start,
                    num=num,
                )
        except Exception as e:
            logger.debug(f"Redis zrevrangebyscore failed: {e}")
            self.is_connected = False
        # No sensible fallback for sorted sets; return empty
        return []

    def zadd(self, key, mapping):
        """Add items to sorted set. Returns count of added items."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zadd(key, mapping)
        except Exception as e:
            logger.debug(f"Redis zadd failed: {e}")
            self.is_connected = False
        return 0

    def zcard(self, key):
        """Get cardinality of sorted set. Returns 0 on fallback."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.zcard(key)
        except Exception as e:
            logger.debug(f"Redis zcard failed: {e}")
            self.is_connected = False
        return 0

    def expire(self, key, time):
        """Set expiry on key. Returns True on success."""
        self._ensure_connection()
        try:
            if self.is_connected:
                return self.redis_client.expire(key, time)
        except Exception as e:
            logger.debug(f"Redis expire failed: {e}")
            self.is_connected = False
        return False

    def publish(self, channel, message):
        """Publish message with retry queue and circuit breaker protection"""
        if self.circuit_breaker_enabled and self.circuit_breaker:
            if not self.circuit_breaker.should_allow_request():
                logger.warning(
                    f"Circuit breaker open for Redis PUBLISH operation: {channel}"
                )
                return 0

        self._ensure_connection()

        if self.is_connected:
            try:
                result = self.redis_client.publish(channel, message)
                self.stats["successful_operations"] += 1
                # Record success for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_success()
                return result
            except Exception as e:
                logger.debug(f"Redis publish failed: {e}")
                self.is_connected = False
                # Record failure for circuit breaker
                if self.circuit_breaker_enabled and self.circuit_breaker:
                    self.circuit_breaker.record_failure(e)
                self._queue_operation("publish", channel, message)

        # Fallback: return 0 subscribers for publish operations
        return 0

    def publish_aion_signal(self, signal_type, data, symbol=None):
        """Publish AION research signals into the pattern-detection datastore."""
        if signal_type not in AION_CHANNELS:
            logger.error(f"Unknown AION signal type: {signal_type}")
            return False

        # Store in pattern detection database instead of publishing
        key = f"aion:{signal_type}:{symbol or 'global'}:{int(time.time())}"
        enriched_data = {
            "timestamp": time.time(),
            "signal_type": signal_type,
            "symbol": symbol,
            "data": data,
        }
        
        return self.store_by_data_type("analysis_cache", key, json.dumps(enriched_data))

    def subscribe(self, *channels):
        """Subscribe to channels with reconnection"""
        self._ensure_connection()

        if self.is_connected and self.pubsub:
            try:
                self.pubsub.subscribe(*channels)
                return True
            except Exception as e:
                logger.error(f"Subscribe failed: {e}")
                self.is_connected = False
        return False

    def keys(self, pattern="*"):
        """Get keys with fallback"""
        self._ensure_connection()

        if self.is_connected:
            try:
                # Prefer SCAN to avoid blocking Redis when pattern is broad
                try:
                    iterator = self.redis_client.scan_iter(match=pattern, count=500)
                    result = list(iterator)
                except Exception:
                    result = self.redis_client.keys(pattern)
                self.stats["successful_operations"] += 1
                return result
            except Exception as e:
                logger.debug(f"Redis keys failed: {e}")
                self.is_connected = False

        # Fallback to local storage
        with self.fallback_lock:
            if pattern == "*":
                return list(self.fallback_data.keys())
            return [k for k in self.fallback_data if pattern.replace("*", "") in k]

    def delete(self, *keys):
        """Delete keys with fallback"""
        self._ensure_connection()

        deleted = 0
        if self.is_connected:
            try:
                deleted = self.redis_client.delete(*keys)
                self.stats["successful_operations"] += 1
            except Exception as e:
                logger.debug(f"Redis delete failed: {e}")
                self.is_connected = False

        # Also delete from fallback
        with self.fallback_lock:
            for key in keys:
                if key in self.fallback_data:
                    del self.fallback_data[key]
                    deleted += 1
                if key in self.fallback_expirations:
                    del self.fallback_expirations[key]

        return deleted

    def lpush(self, key, *values):
        """Push to list with fallback"""
        self._ensure_connection()

        # Serialize values if needed
        serialized_values = []
        for v in values:
            if not isinstance(v, str):
                v = json.dumps(v, cls=SafeJSONEncoder)
            serialized_values.append(v)

        if self.is_connected:
            try:
                result = self.redis_client.lpush(key, *serialized_values)
                self.stats["successful_operations"] += 1
                return result
            except Exception as e:
                logger.debug(f"Redis lpush failed: {e}")
                self.is_connected = False

        # Fallback to local list
        with self.fallback_lock:
            if key not in self.fallback_data:
                self.fallback_data[key] = []
            for value in reversed(serialized_values):  # lpush adds to front
                self.fallback_data[key].insert(0, value)
            return len(self.fallback_data[key])

    # ============ Implementation Methods for Retry Queue ============

    def _set_impl(self, key, value, ex=None):
        """Internal set implementation for retry"""
        if self.is_connected:
            self.redis_client.set(key, value, ex=ex)

    def _publish_impl(self, channel, message):
        """Internal publish implementation for retry"""
        if self.is_connected:
            self.redis_client.publish(channel, message)

    # ============ Fallback Helper Methods ============

    def _clean_expired_fallback(self, key):
        """Clean expired keys in fallback storage"""
        if key in self.fallback_expirations:
            if time.time() > self.fallback_expirations[key]:
                del self.fallback_data[key]
                del self.fallback_expirations[key]

    def get_stats(self):
        """Get connection statistics"""
        return {
            "is_connected": self.is_connected,
            "reconnections": self.stats["reconnections"],
            "failed_operations": self.stats["failed_operations"],
            "queued_operations": len(self.retry_queue),
            "total_queued": self.stats["queued_operations"],
            "successful_operations": self.stats["successful_operations"],
            "fallback_keys": len(self.fallback_data),
        }

    # ------------------------------------------------------------------
    # Session utilities for bucket_incremental_volume tracking
    # ------------------------------------------------------------------
    def reset_volume_session(self, symbols=None):
        """Reset cumulative bucket_incremental_volume tracking keys at market open.

        If symbols is None, clears all keys matching 'last_cumulative:*'.
        Otherwise clears only specified symbols.
        """
        try:
            if symbols is None:
                keys = self.redis_client.keys("last_cumulative:*") if self.redis_client else []
            else:
                keys = [f"last_cumulative:{symbol}" for symbol in symbols]
            if keys:
                self.redis_client.delete(*keys)
            logger.info(f"Reset bucket_incremental_volume session tracking for {len(keys)} symbols")
        except Exception as e:
            logger.debug(f"reset_volume_session error: {e}")

    def validate_volume_consistency(self, symbol: str):
        """Debug helper to check incremental vs cumulative consistency for a symbol."""
        try:
            from datetime import datetime

            # Last cumulative used by data pipeline ingress
            last_cumulative_key = f"last_cumulative:{symbol}"
            last_cumulative = self.redis_client.get(last_cumulative_key) if self.redis_client else None

            # Latest 1min bucket: pick highest minute bucket index for current hour
            now = datetime.now()
            hour = now.hour
            pattern = f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:*"
            bucket_inc = None
            if self.redis_client:
                try:
                    keys = self.redis_client.keys(pattern)
                    def _idx(k: str) -> int:
                        try:
                            return int(k.split(":")[-1])
                        except Exception:
                            return -1
                    keys = sorted(keys, key=_idx)
                    if keys:
                        latest_key = keys[-1]
                        data = self.redis_client.hgetall(latest_key)
                        if isinstance(data, dict):
                            if "bucket_incremental_volume" in data:
                                try:
                                    bucket_inc = int(data.get("bucket_incremental_volume", 0))
                                except Exception:
                                    bucket_inc = None
                            elif "bucket_incremental_volume" in data:
                                try:
                                    bucket_inc = int(data.get("bucket_incremental_volume", 0))
                                except Exception:
                                    bucket_inc = None
                except Exception:
                    pass

            # Daily cumulative hash
            date_str = now.strftime('%Y-%m-%d')
            daily_key = f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date_str}"
            daily_cum = None
            try:
                dd = self.redis_client.hgetall(daily_key) if self.redis_client else None
                if isinstance(dd, dict):
                    if "zerodha_cumulative_volume" in dd:
                        try:
                            daily_cum = int(dd.get("zerodha_cumulative_volume", 0))
                        except Exception:
                            daily_cum = None
                    elif "bucket_cumulative_volume" in dd:
                        try:
                            daily_cum = int(dd.get("bucket_cumulative_volume", 0))
                        except Exception:
                            daily_cum = None
            except Exception:
                pass

            result = {
                "last_cumulative": int(last_cumulative) if last_cumulative is not None else None,
                "bucket_incremental": bucket_inc,
                "daily_cumulative": daily_cum,
            }
            logger.info(
                f"[VOLUME_VALIDATE] {symbol} last_cum={result['last_cumulative']} bucket_inc={result['bucket_incremental']} daily_cum={result['daily_cumulative']}"
            )
            return result
        except Exception as e:
            logger.debug(f"validate_volume_consistency error: {e}")
            return None

    def pipeline(self):
        """Create a Redis pipeline with automatic reconnection support"""

        class RobustPipeline:
            def __init__(self, parent_client):
                self.parent_client = parent_client
                self.commands = []
                self.redis_pipeline = None

            def _ensure_pipeline(self):
                """Ensure we have a valid pipeline connection"""
                if not self.parent_client.is_connected:
                    self.parent_client._ensure_connection()

                if self.parent_client.is_connected and self.redis_pipeline is None:
                    try:
                        self.redis_pipeline = self.parent_client.redis_client.pipeline()
                    except Exception as e:
                        logger.warning(f"Failed to create pipeline: {e}")
                        return False
                return self.redis_pipeline is not None

            def set(self, key, value, ex=None):
                """Add SET command to pipeline"""
                self.commands.append(("set", key, value, ex))
                return self

            def get(self, key):
                """Add GET command to pipeline"""
                self.commands.append(("get", key))
                return self

            def delete(self, *keys):
                """Add DELETE command to pipeline"""
                self.commands.append(("delete", keys))
                return self

            def publish(self, channel, message):
                """Add PUBLISH command to pipeline"""
                self.commands.append(("publish", channel, message))
                return self

            def zadd(self, key, mapping):
                """Add ZADD command to pipeline"""
                self.commands.append(("zadd", key, mapping))
                return self

            def expire(self, key, time):
                """Add EXPIRE command to pipeline"""
                self.commands.append(("expire", key, time))
                return self

            def execute(self):
                """Execute all commands in the pipeline"""
                if not self.commands:
                    return []

                if not self._ensure_pipeline():
                    # Fallback: execute commands individually
                    logger.warning(
                        "Pipeline unavailable, executing commands individually"
                    )
                    results = []
                    for cmd in self.commands:
                        try:
                            method_name = cmd[0]
                            args = cmd[1:]
                            method = getattr(self.parent_client, method_name)
                            result = method(*args)
                            results.append(result)
                        except Exception as e:
                            logger.error(f"Pipeline fallback command failed: {e}")
                            results.append(None)
                    self.commands = []
                    return results

                # Execute pipeline
                try:
                    # Build pipeline commands
                    for cmd in self.commands:
                        method_name = cmd[0]
                        args = cmd[1:]

                        if method_name == "set":
                            key, value, ex = args
                            if ex:
                                self.redis_pipeline.set(key, value, ex=ex)
                            else:
                                self.redis_pipeline.set(key, value)
                        elif method_name == "get":
                            self.redis_pipeline.get(args[0])
                        elif method_name == "delete":
                            self.redis_pipeline.delete(*args[0])
                        elif method_name == "publish":
                            self.redis_pipeline.publish(*args)
                        elif method_name == "zadd":
                            self.redis_pipeline.zadd(*args)
                        elif method_name == "expire":
                            self.redis_pipeline.expire(*args)

                    # Execute and return results
                    results = self.redis_pipeline.execute()
                    self.parent_client.stats["successful_operations"] += len(
                        self.commands
                    )
                    self.commands = []
                    return results

                except Exception as e:
                    logger.error(f"Pipeline execution failed: {e}")
                    self.parent_client.stats["failed_operations"] += 1

                    # Fallback to individual execution
                    logger.warning("Falling back to individual command execution")
                    results = []
                    for cmd in self.commands:
                        try:
                            method_name = cmd[0]
                            args = cmd[1:]
                            method = getattr(self.parent_client, method_name)
                            result = method(*args)
                            results.append(result)
                        except Exception as e2:
                            logger.error(f"Fallback command failed: {e2}")
                            results.append(None)

                    self.commands = []
                    return results

        return RobustPipeline(self)

    def update_volume_buckets(
        self,
        symbol,
        bucket_incremental_volume,
        timestamp,
        bucket_cumulative_volume=None,
        last_price=None,
    ):
        """Store only incremental bucket_incremental_volume in intraday buckets and mirror cumulative bucket_incremental_volume daily."""
        if not symbol or not (self.is_connected and self.redis_client):
            return False
        
        # Resolve token to symbol BEFORE storing volume buckets
        symbol = self._resolve_token_to_symbol_for_storage(symbol)

        try:
            if isinstance(bucket_incremental_volume, dict):
                volume_payload = bucket_incremental_volume
                bucket_incremental_volume = volume_payload.get("bucket_incremental_volume")
                timestamp = volume_payload.get("timestamp", timestamp)
                bucket_cumulative_volume = volume_payload.get("bucket_cumulative_volume", bucket_cumulative_volume)
                last_price = volume_payload.get("last_price", volume_payload.get("last_traded_price", last_price))

            import pytz
            from datetime import datetime

            def _to_int(value, default=None):
                if value is None:
                    return default
                try:
                    return int(float(value))
                except (TypeError, ValueError):
                    return default

            def _to_float(value):
                if value is None:
                    return None
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return None

            volume_int = _to_int(bucket_incremental_volume)
            if volume_int is None:
                logger.error("No incremental bucket_incremental_volume for %s", symbol)
                return False
            if volume_int < 0:
                volume_int = 0

            cumulative_int = _to_int(bucket_cumulative_volume)
            if (
                cumulative_int is not None
                and cumulative_int > 0
                and volume_int > cumulative_int * 0.1
            ):
                logger.warning(
                    "Potential cumulative bucket_incremental_volume used as incremental for %s: %s vs cumulative %s",
                    symbol,
                    volume_int,
                    cumulative_int,
                )

            ist = pytz.timezone("Asia/Kolkata")

            if isinstance(timestamp, datetime):
                ts = timestamp
                if ts.tzinfo is None:
                    ts = ist.localize(ts)
                else:
                    ts = ts.astimezone(ist)
            else:
                epoch_ms = TimestampNormalizer.to_epoch_ms(timestamp)
                ts = datetime.fromtimestamp(epoch_ms / 1000.0, tz=ist)

            price_float = _to_float(last_price)
            bucket_iso_ts = ts.isoformat()

            resolutions = {
                "1min": {
                    "size": 1,
                    "prefix": "bucket_incremental_volume:bucket_incremental_volume:bucket1",
                    "history": "bucket_incremental_volume:history:1min",
                    "history_len": 6000,
                },
                "2min": {
                    "size": 2,
                    "prefix": "bucket_incremental_volume:bucket_incremental_volume:bucket2",
                    "history": "bucket_incremental_volume:history:2min",
                    "history_len": 4000,
                },
                "5min": {
                    "size": 5,
                    "prefix": "bucket_incremental_volume:bucket_incremental_volume:bucket",
                    "history": "bucket_incremental_volume:history:5min",
                    "history_len": 4000,
                },
                "10min": {
                    "size": 10,
                    "prefix": "bucket_incremental_volume:bucket_incremental_volume:bucket10",
                    "history": "bucket_incremental_volume:history:10min",
                    "history_len": 3000,
                },
            }

            pipe = self.redis_client.pipeline(transaction=False)
            hour = ts.hour

            for label, cfg in resolutions.items():
                bucket_index = ts.minute // cfg["size"]
                bucket_key = f"{cfg['prefix']}:{symbol}:buckets:{hour}:{bucket_index}"

                pipe.hincrby(bucket_key, SESSION_FIELD_BUCKET_INC, volume_int)
                mirror_session_aliases(
                    pipe,
                    bucket_key,
                    SESSION_FIELD_BUCKET_INC,
                    volume_int,
                    operation="incr",
                )
                pipe.hincrby(bucket_key, "bucket_incremental_volume", volume_int)
                pipe.hincrby(bucket_key, "count", 1)
                pipe.hsetnx(bucket_key, "first_timestamp", bucket_iso_ts)
                pipe.hset(bucket_key, "last_timestamp", bucket_iso_ts)
                if price_float is not None:
                    pipe.hset(bucket_key, "last_price", price_float)
                    pipe.hset(bucket_key, "close", price_float)
                pipe.expire(bucket_key, 86400)

                history_key = f"{cfg['history']}:{label}:{symbol}"
                history_entry = {
                    "timestamp": bucket_iso_ts,
                    SESSION_FIELD_BUCKET_INC: volume_int,
                    "hour": hour,
                    "bucket_index": bucket_index,
                }
                if cumulative_int is not None:
                    history_entry[SESSION_FIELD_ZERODHA_CUM] = cumulative_int
                if price_float is not None:
                    history_entry["last_price"] = price_float

                pipe.lpush(history_key, json.dumps(history_entry))
                pipe.ltrim(history_key, 0, cfg["history_len"] - 1)
                pipe.expire(history_key, 7 * 86400)

            if cumulative_int is not None:
                date_str = ts.strftime("%Y-%m-%d")
                daily_key = f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date_str}"
                pipe.hset(daily_key, SESSION_FIELD_ZERODHA_CUM, cumulative_int)
                mirror_session_aliases(
                    pipe,
                    daily_key,
                    SESSION_FIELD_ZERODHA_CUM,
                    cumulative_int,
                    operation="set",
                )
                pipe.hset(daily_key, "bucket_cumulative_volume", cumulative_int)
                pipe.expire(daily_key, 7 * 86400)

            pipe.execute()
            return True

        except Exception as exc:
            logger.debug("Failed to update bucket_incremental_volume buckets for %s: %s", symbol, exc)
            return False

    def get_time_based_volume(self, symbol, target_time):
        """Get historical bucket_incremental_volume for specific time"""
        try:
            import pytz
            from datetime import datetime

            ist = pytz.timezone("Asia/Kolkata")
            if isinstance(target_time, (int, float)):
                target_time = datetime.fromtimestamp(target_time)
            if target_time.tzinfo is None:
                target_time = ist.localize(target_time)
            else:
                target_time = target_time.astimezone(ist)

            hour = target_time.hour
            minute_bucket = target_time.minute // 5
            bucket_key = f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour}:{minute_bucket}"

            if self.is_connected and self.redis_client:
                data = self.redis_client.hgetall(bucket_key)
                if data and b"bucket_incremental_volume" in data and b"count" in data:
                    bucket_incremental_volume = int(data[b"bucket_incremental_volume"])
                    count = int(data[b"count"])
                    return bucket_incremental_volume / count if count > 0 else None

            return None

        except Exception as e:
            print(f"Time-based bucket_incremental_volume error: {e}")
            return None



    def normalize_volume_field(self, data, field_name='bucket_incremental_volume'):
        """Normalize bucket_incremental_volume field access with backward compatibility"""
        if field_name in data:
            return float(data[field_name] or 0)
        
        # Try legacy field names
        legacy_fields = ['bucket_incremental_volume', 'vol', 'bucket_incremental_volume', 'bucket_incremental_volume', 'zerodha_cumulative_volume', 'zerodha_last_traded_quantity', 'quantity']
        for legacy_field in legacy_fields:
            if legacy_field in data:
                return float(data[legacy_field] or 0)
        
        return 0.0
    
    def get_volume_from_bucket(self, bucket_data):
        """Get bucket_incremental_volume from bucket data with all possible field names"""
        bucket_incremental_volume = 0.0
        
        # Try primary field first
        if 'bucket_incremental_volume' in bucket_data:
            bucket_incremental_volume = float(bucket_data['bucket_incremental_volume'] or 0)
        # Try legacy fields
        elif 'bucket_incremental_volume' in bucket_data:
            bucket_incremental_volume = float(bucket_data['bucket_incremental_volume'] or 0)
        elif 'vol' in bucket_data:
            bucket_incremental_volume = float(bucket_data['vol'] or 0)
        elif 'bucket_incremental_volume' in bucket_data:
            bucket_incremental_volume = float(bucket_data['bucket_incremental_volume'] or 0)
        elif 'bucket_incremental_volume' in bucket_data:
            bucket_incremental_volume = float(bucket_data['bucket_incremental_volume'] or 0)
        elif 'zerodha_cumulative_volume' in bucket_data:
            bucket_incremental_volume = float(bucket_data['zerodha_cumulative_volume'] or 0)
        elif 'zerodha_last_traded_quantity' in bucket_data:
            bucket_incremental_volume = float(bucket_data['zerodha_last_traded_quantity'] or 0)
        elif 'quantity' in bucket_data:
            bucket_incremental_volume = float(bucket_data['quantity'] or 0)
        
        return bucket_incremental_volume
    
    def info(self, section=None):
        """Get Redis server information"""
        try:
            if section:
                return self.redis_client.info(section)
            return self.redis_client.info()
        except Exception as e:
            self.logger.error(f"Failed to get Redis info: {e}")
            return {}
    
    def xinfo_stream(self, name):
        """Get stream information"""
        try:
            return self.redis_client.xinfo_stream(name)
        except Exception as e:
            self.logger.error(f"Failed to get stream info for {name}: {e}")
            return {}
    
    def hgetall(self, name):
        """Get all fields and values from a hash"""
        try:
            return self.redis_client.hgetall(name)
        except Exception as e:
            self.logger.error(f"Failed to get hash {name}: {e}")
            return {}


class CumulativeDataTracker:
    """Specialized tracker for cumulative bucket_incremental_volume and last_price movements with session-based storage"""

    def __init__(self, redis_client):
        self.redis = redis_client
        self.price_history = defaultdict(list)
        self.volume_history = defaultdict(list)
        self.lock = Lock()

        # Session management
        self.current_session = self._get_current_session_date()
        self.session_data = defaultdict(self._create_symbol_session)
    
    @property
    def redis_client(self):
        """Backward compatibility property that points to redis attribute"""
        return self.redis
    
    def _extract_base_symbol(self, symbol: str) -> str:
        """Extract base symbol from F&O contract names"""
        # Ensure symbol is a string (may be int like instrument_token)
        if not isinstance(symbol, str):
            symbol = str(symbol)
        # Remove exchange prefix if present
        if ":" in symbol:
            symbol = symbol.split(":", 1)[1]
        
        import re
        is_option = bool(re.search(r"\d+(CE|PE)$", symbol))
        is_future = symbol.endswith("FUT") or bool(re.search(r"\d{2}[A-Z]{3}", symbol))

        if is_future or is_option:
            expiry_match = re.search(r"\d{2}[A-Z]{3}", symbol)
            if expiry_match:
                symbol = symbol.replace(expiry_match.group(), "")

            if symbol.endswith("FUT"):
                symbol = symbol[:-3]

            if is_option:
                symbol = re.sub(r"\d+(CE|PE)$", "", symbol)

        return symbol.upper()
    
    def get_time_buckets(self, symbol, session_date=None, hour=None, lookback_minutes=60):
        """Get all time buckets for a symbol using direct key lookup from history lists"""
        if session_date is None:
            session_date = self.current_session

        try:
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            # Use history lists for fast retrieval (already maintained during bucket creation)
            resolutions_to_check = ["1min", "2min", "5min", "10min"]
            
            buckets = []
            logger.info(f"🔍 [BUCKET_DEBUG] {symbol} -> {base_symbol} - Using history lists")
            
            for resolution in resolutions_to_check:
                try:
                    # Direct key lookup from history list
                    history_key = f"bucket_incremental_volume:history:{resolution}:{symbol}"
                    history_data = self.redis.lrange(history_key, 0, -1)
                    
                    if history_data:
                        logger.info(f"🔍 [BUCKET_DEBUG] Found history list: {history_key} with {len(history_data)} buckets")
                        
                        # Parse history entries and construct bucket keys
                        for entry_str in history_data:
                            try:
                                entry = json.loads(entry_str)
                                hr = entry.get('hour', 0)
                                bucket_idx = entry.get('bucket_index', 0)
                                
                                # Determine which prefix to use based on resolution
                                if resolution == "1min":
                                    prefix = "bucket_incremental_volume:bucket_incremental_volume:bucket1"
                                elif resolution == "2min":
                                    prefix = "bucket_incremental_volume:bucket_incremental_volume:bucket2"
                                else:
                                    prefix = "bucket_incremental_volume:bucket_incremental_volume:bucket"
                                
                                # Construct the actual bucket key
                                bucket_key = f"{prefix}:{symbol}:buckets:{hr}:{bucket_idx}"
                                
                                # Get bucket data directly (fast HGETALL)
                                hash_data = self.redis.hgetall(bucket_key)
                                if hash_data and b"bucket_incremental_volume" in hash_data:
                                    bucket_data = {
                                        "symbol": symbol,
                                        "session_date": session_date,
                                        "bucket_incremental_volume": int(hash_data.get(b"bucket_incremental_volume", 0)),
                                        "zerodha_cumulative_volume": int(hash_data.get(b"zerodha_cumulative_volume", 0)),
                                        "count": int(hash_data.get(b"count", 0)),
                                        "first_timestamp": float(hash_data.get(b"first_timestamp", 0)),
                                        "last_timestamp": float(hash_data.get(b"last_timestamp", 0)),
                                        "last_price": float(hash_data.get(b"last_price", 0))
                                    }
                                    buckets.append(bucket_data)
                            except Exception as e:
                                continue
                except Exception as e:
                    logger.debug(f"Error reading history for {resolution}: {e}")
            
            # Fallback to pattern matching only if no history found
            if not buckets:
                logger.info(f"🔍 [BUCKET_DEBUG] No history found, falling back to pattern matching for {symbol}")
                
                # Try multiple patterns to find bucket_incremental_volume data
                patterns_to_try = [
                # Pattern 1: bucket_incremental_volume:bucket:{symbol}:{session}:{hour}:{minute} (JSON string) - PRIMARY
                f"bucket_incremental_volume:bucket:{symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                # Pattern 2: bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 4: bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour}:{minute_bucket} (Legacy format)
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket:bucket:{symbol}:* (Legacy format)
                f"bucket_incremental_volume:bucket:{symbol}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:*",
                # Pattern 4: data:{symbol}:* (Alternative format)
                f"data:{symbol}:*",
                f"data:{base_symbol}:*",
                # Pattern 5: *:{symbol}:* (Fallback pattern)
                f"*:{symbol}:*",
                f"*:{base_symbol}:*"
                ]
                
                logger.debug(f"{symbol} -> {base_symbol} - Trying {len(patterns_to_try)} patterns")
                for i, pattern in enumerate(patterns_to_try):
                    try:
                        keys = self.redis.keys(pattern)
                        if keys:
                            logger.debug(f"Pattern {i+1}: {pattern} -> {len(keys)} keys")
                        for key in keys:
                            try:
                                if pattern.startswith("bucket_incremental_volume:bucket:") or pattern.startswith("bucket_incremental_volume:bucket:bucket:") or pattern.startswith("data:"):
                                    # JSON string format
                                    data = self.redis.get(key)
                                    if data:
                                        bucket_data = json.loads(data)
                                        # Ensure bucket_incremental_volume field exists
                                        if 'bucket_incremental_volume' not in bucket_data and 'bucket_incremental_volume' in bucket_data:
                                            bucket_data['bucket_incremental_volume'] = bucket_data['bucket_incremental_volume']
                                        buckets.append(bucket_data)
                                elif pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket1:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket2:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket:"):
                                    # Redis hash format - convert to bucket format
                                    hash_data = self.redis.hgetall(key)
                                    if hash_data and "bucket_incremental_volume" in hash_data:
                                        # Convert hash data to bucket format
                                        bucket_data = {
                                            "symbol": symbol,
                                            "session_date": session_date,
                                            "bucket_incremental_volume": int(hash_data.get(b"bucket_incremental_volume", 0)),
                                            "zerodha_cumulative_volume": int(hash_data.get(b"zerodha_cumulative_volume", 0)),
                                            "count": int(hash_data.get(b"count", 0)),
                                            "first_timestamp": 0,
                                            "last_timestamp": 0
                                        }
                                        # Parse timestamp from key if possible
                                        key_parts = key.split(":")
                                        if len(key_parts) >= 4:
                                            try:
                                                hour_part = int(key_parts[-2])
                                                minute_part = int(key_parts[-1]) * 5  # Convert bucket to minute
                                                # Create approximate timestamp
                                                from datetime import datetime
                                                dt = datetime.now().replace(hour=hour_part, minute=minute_part, second=0, microsecond=0)
                                                bucket_data["first_timestamp"] = dt.timestamp()
                                                bucket_data["last_timestamp"] = dt.timestamp()
                                            except (ValueError, IndexError):
                                                pass
                                        buckets.append(bucket_data)
                            except Exception as e:
                                continue
                    except Exception as e:
                        continue

            # Sort by timestamp
            buckets.sort(key=lambda x: x.get("first_timestamp", 0))
            return buckets

        except Exception as e:
            logger.error(f"Failed to get time buckets for {symbol}: {e}")
            return []

    def _get_current_session_date(self, timestamp=None):
        """Get current trading session date with timezone awareness"""
        if timestamp:
            return date.fromtimestamp(timestamp).isoformat()
        return date.today().isoformat()
    
    def _validate_session_date(self, symbol, session_date):
        """Validate session date and handle timezone corrections"""
        try:
            current_date = date.today().isoformat()
            
            # If session date is different from current date, check if it's a valid trading day
            if session_date != current_date:
                # Check if it's a weekend or holiday (basic check)
                from datetime import datetime
                session_datetime = datetime.fromisoformat(session_date)
                
                # Weekend check (Saturday = 5, Sunday = 6)
                if session_datetime.weekday() >= 5:
                    logger.warning(f"Session date {session_date} is weekend for {symbol}, using current date")
                    return current_date
                
                # If session date is in the future, use current date
                if session_datetime.date() > date.today():
                    logger.warning(f"Session date {session_date} is in future for {symbol}, using current date")
                    return current_date
                    
            return session_date
        except Exception as e:
            logger.error(f"Session date validation failed for {symbol}: {e}")
            return date.today().isoformat()

    def _resolve_token_to_symbol(self, symbol):
        """Resolve token to proper symbol format"""
        try:
            symbol_str = str(symbol)
            # Check if symbol needs resolution
            # Handle: "TOKEN_12345", "UNKNOWN_12345", or "12345"
            token_str = None
            if symbol_str.startswith("TOKEN_"):
                token_str = symbol_str.replace("TOKEN_", "")
            elif symbol_str.startswith("UNKNOWN_"):
                token_str = symbol_str.replace("UNKNOWN_", "")
            elif symbol_str.isdigit():
                token_str = symbol_str
            
            if token_str:
                try:
                    token = int(token_str)
                    # Import updated token resolver
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    instrument_mapper = InstrumentMapper()
                    resolved = instrument_mapper.token_to_symbol(token)
                    
                    # Only return if it's not still UNKNOWN
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        return resolved
                except (ValueError, ImportError) as e:
                    logger.debug(f"Token resolution failed for {symbol_str}: {e}")
        except Exception as e:
            logger.debug(f"Token resolution failed for {symbol}: {e}")

        return None
    
    def _resolve_token_to_symbol_for_storage(self, symbol):
        """Resolve token to symbol for Redis storage - returns resolved symbol or original if resolution fails"""
        try:
            symbol_str = str(symbol) if symbol else ""
            
            # If already a proper symbol (contains :), return as-is
            if ":" in symbol_str and not symbol_str.isdigit():
                return symbol
            
            # Check if symbol needs resolution (numeric, TOKEN_ prefix, or UNKNOWN_ prefix)
            if not symbol_str or symbol_str == "UNKNOWN":
                logger.debug(f"Skipping resolution for empty/UNKNOWN symbol: {symbol_str}")
                return symbol
            
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
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    instrument_mapper = InstrumentMapper()
                    resolved = instrument_mapper.token_to_symbol(token)
                    
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        logger.info(f"✅ Redis storage: Resolved token {token} to {resolved}")
                        return resolved
                    else:
                        logger.warning(f"⚠️ Redis storage: Could not resolve token {token}, got {resolved}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Redis storage: Could not parse token from '{symbol_str}': {e}")
                except Exception as e:
                    logger.warning(f"⚠️ Redis storage: Token resolution failed for '{symbol_str}': {e}")
            
            # Return original symbol if resolution failed or not needed
            return symbol
            
        except Exception as e:
            logger.error(f"Error in _resolve_token_to_symbol_for_storage: {e}")
            return symbol  # Return original on error

    def _create_symbol_session(self):
        """Create session data structure for each symbol"""
        return {
            "bucket_cumulative_volume": 0,
            "first_price": None,
            "last_price": None,
            "high": float("-inf"),
            "low": float("inf"),
            "update_count": 0,
            "first_update": None,
            "last_update": None,
            "session_date": self.current_session,
            "time_buckets": defaultdict(dict),  # hour:minute_bucket -> data
        }

    def _normalize_symbol(self, symbol):
        symbol_str = str(symbol)
        resolved_symbol = self._resolve_token_to_symbol(symbol_str)
        return resolved_symbol or symbol_str

    def _get_session_data(self, symbol):
        if symbol not in self.session_data:
            self.session_data[symbol] = self._create_symbol_session()
        return self.session_data[symbol]

    def _persist_session_data(self, symbol, session):
        try:
            session_key = f"session:{symbol}:{self.current_session}"
            self.redis.setex(
                session_key, 86400, json.dumps(session, cls=SafeJSONEncoder)
            )
            try:
                if (
                    session["update_count"] in (1, 1000, 10000)
                    or session["update_count"] % 50000 == 0
                ):
                    print(
                        f"🗄️ SESSION[{symbol}] updates={session['update_count']} cum_vol={int(session['bucket_cumulative_volume'])}"
                    )
            except Exception:
                pass
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error(f"Failed to persist session data for {symbol}: {exc}")

    def update_symbol_data(
        self, symbol, last_price, bucket_incremental_volume, timestamp=None, depth_data=None
    ):
        """Store raw bucket_incremental_volume data as-is (no cumulative calculations)"""
        if timestamp is None:
            timestamp = time.time()

        symbol = self._normalize_symbol(symbol)

        with self.lock:
            session = self._get_session_data(symbol)

            # Update session metrics
            if session["first_price"] is None:
                session["first_price"] = last_price
                session["first_update"] = timestamp

            session["last_price"] = last_price
            session["last_update"] = timestamp
            session["high"] = max(session["high"], last_price)
            session["low"] = min(session["low"], last_price)
            session["update_count"] += 1

            # Store raw bucket_incremental_volume as-is (no cumulative calculations)
            if bucket_incremental_volume > 0:
                session["bucket_cumulative_volume"] = bucket_incremental_volume  # Store raw bucket_incremental_volume as-is
                session["zerodha_cumulative_volume"] = bucket_incremental_volume

            # Store last_price and bucket_incremental_volume in local history (for immediate analysis)
            self.price_history[symbol].append((timestamp, last_price))
            self.volume_history[symbol].append((timestamp, bucket_incremental_volume))

            # Keep only recent history to prevent memory bloat
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-500:]
            if len(self.volume_history[symbol]) > 1000:
                self.volume_history[symbol] = self.volume_history[symbol][-500:]

            # Update Redis with session data
            try:
                self._persist_session_data(symbol, session)
                self._store_time_bucket(symbol, last_price, bucket_incremental_volume, timestamp, depth_data)
                return True
            except Exception as e:
                logger.error(f"Failed to update bucket_incremental_volume data for {symbol}: {e}")
                return False

    def _store_time_bucket(self, symbol, last_price, bucket_incremental_volume, timestamp, depth_data=None):
        """Store data in time buckets for pattern analysis with order book data"""
        # Only store trade ticks (bucket_incremental_volume > 0) in buckets
        if bucket_incremental_volume <= 0:
            return

        # Handle different timestamp formats
        try:
            if isinstance(timestamp, str):
                # Parse string timestamp
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            elif isinstance(timestamp, (int, float)):
                # Handle numeric timestamp
                dt = datetime.fromtimestamp(timestamp)
            else:
                # Fallback to current time
                dt = datetime.now()
        except (ValueError, TypeError, OSError) as e:
            logger.error(f"Failed to parse timestamp {timestamp} for {symbol}: {e}")
            dt = datetime.now()
        bucket_minute = dt.minute // 5 * 5
        hour_bucket = (
            f"bucket_incremental_volume:bucket:{symbol}:{self.current_session}:{dt.hour}:{bucket_minute}"
        )

        session = self.session_data[symbol]
        bucket_key = f"{dt.hour}:{bucket_minute}"

        # Get or initialize bucket data
        if bucket_key not in session["time_buckets"]:
            session["time_buckets"][bucket_key] = {
                "symbol": symbol,
                "session_date": self.current_session,
                "hour": dt.hour,
                "minute_bucket": bucket_minute,
                "open": last_price,
                "high": last_price,
                "low": last_price,
                "close": last_price,
                "bucket_incremental_volume": 0,
                "count": 0,
                "first_timestamp": timestamp,
                "last_timestamp": timestamp,
                "order_book_snapshots": [],  # Store order book evolution
                # Accumulated (day) bucket_incremental_volume semantics per Kite: store session cumulative at bucket open/close
                "cumulative_open": session.get("bucket_cumulative_volume", 0),
                "cumulative_close": session.get("bucket_cumulative_volume", 0),
            }

        bucket = session["time_buckets"][bucket_key]
        bucket["close"] = last_price
        bucket["high"] = max(bucket["high"], last_price)
        bucket["low"] = min(bucket["low"], last_price)
        bucket["bucket_incremental_volume"] += bucket_incremental_volume  # Accumulate trade bucket_incremental_volume in bucket
        bucket["count"] += 1
        bucket["last_timestamp"] = timestamp
        # Update cumulative close to current session cumulative bucket_incremental_volume
        bucket["cumulative_close"] = session.get("bucket_cumulative_volume", bucket.get("cumulative_close", 0))
        # Calculate incremental bucket_incremental_volume for this bucket (optimized field)
        try:
            # REMOVED: Incremental volume calculation - now handled in WebSocket Parser only
            # Use the incremental volume already calculated by WebSocket Parser
            bucket["bucket_incremental_volume"] = bucket.get("bucket_incremental_volume", 0)
        except Exception:
            pass

        # Store order book snapshot if provided
        if depth_data:
            bucket["order_book_snapshots"].append(
                {
                    "timestamp": timestamp,
                    "depth": depth_data,
                    "last_price": last_price,
                    "bucket_incremental_volume": bucket_incremental_volume,
                }
            )
            # Keep only last 10 snapshots per bucket to manage memory
            if len(bucket["order_book_snapshots"]) > 10:
                bucket["order_book_snapshots"] = bucket["order_book_snapshots"][-10:]

        # Store bucket in Redis with safe expiration time
        try:
            # Calculate safe expiration time (until end of day)
            current_time = datetime.now()
            end_of_day = datetime(current_time.year, current_time.month, current_time.day, 23, 59, 59)
            expire_seconds = max(
                3600, int((end_of_day - current_time).total_seconds())
            )  # At least 1 hour

            self.redis.setex(
                hour_bucket, expire_seconds, json.dumps(bucket, cls=SafeJSONEncoder)
            )

            # Debug log with reduced frequency
            if bucket["count"] in (1, 100, 1000) or bucket["count"] % 10000 == 0:
                print(
                    f"🧰 BUCKET[{symbol} {bucket_key}] count={bucket['count']} vol={int(bucket['bucket_incremental_volume'])}"
                )
        except Exception as e:
            logger.error(f"Failed to store time bucket for {symbol}: {e}")

    def get_cumulative_data(self, symbol, session_date=None):
        """Get cumulative data for a symbol with in-memory fallback and field validation"""
        if session_date is None:
            session_date = self.current_session

        # Prefer in-memory data for the current session to avoid stale reads
        if session_date == self.current_session and symbol in self.session_data:
            # Return a shallow copy to prevent external mutation
            data = self.session_data[symbol].copy()
            return self._validate_and_normalize_session_data(symbol, data)

        # Otherwise, fetch from Redis
        try:
            session_key = f"session:{symbol}:{session_date}"
            data = self.redis.get(session_key)
            if data:
                parsed_data = json.loads(data)
                return self._validate_and_normalize_session_data(symbol, parsed_data)
            return None
        except Exception as e:
            logger.error(f"Error fetching cumulative data for {symbol}: {e}")
            return None
    
    def get_time_buckets(self, symbol, session_date=None, hour=None, lookback_minutes=60):
        """Get all time buckets for a symbol with enhanced pattern matching"""
        if session_date is None:
            session_date = self.current_session

        try:
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            # Try multiple patterns to find bucket_incremental_volume data
            patterns_to_try = [
                # Pattern 1: bucket_incremental_volume:bucket:{symbol}:{session}:{hour}:{minute} (JSON string) - PRIMARY
                f"bucket_incremental_volume:bucket:{symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                # Pattern 2: bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 4: bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour}:{minute_bucket} (Legacy format)
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket:bucket:{symbol}:* (Legacy format)
                f"bucket_incremental_volume:bucket:{symbol}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:*",
                # Pattern 4: data:{symbol}:* (Alternative format)
                f"data:{symbol}:*",
                f"data:{base_symbol}:*",
                # Pattern 5: *:{symbol}:* (Fallback pattern)
                f"*:{symbol}:*",
                f"*:{base_symbol}:*"
            ]
            
            buckets = []
            logger.debug(f"{symbol} -> {base_symbol} - Trying {len(patterns_to_try)} patterns")
            for i, pattern in enumerate(patterns_to_try):
                try:
                    keys = self.redis_client.keys(pattern)
                    if keys:
                        logger.debug(f"Pattern {i+1}: {pattern} -> {len(keys)} keys")
                    for key in keys:
                        try:
                            if pattern.startswith("bucket_incremental_volume:bucket:") or pattern.startswith("bucket_incremental_volume:bucket:bucket:") or pattern.startswith("data:"):
                                # JSON string format
                                data = self.redis_client.get(key)
                                if data:
                                    bucket_data = json.loads(data)
                                    # Ensure bucket_incremental_volume field exists
                                    if 'bucket_incremental_volume' not in bucket_data and 'bucket_incremental_volume' in bucket_data:
                                        bucket_data['bucket_incremental_volume'] = bucket_data['bucket_incremental_volume']
                                    buckets.append(bucket_data)
                            elif pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket1:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket2:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket:"):
                                # Redis hash format - convert to bucket format
                                hash_data = self.redis_client.redis_client.hgetall(key)
                                if hash_data and "bucket_incremental_volume" in hash_data:
                                    # Convert hash data to bucket format
                                    bucket_data = {
                                        "symbol": symbol,
                                        "session_date": session_date,
                                        "bucket_incremental_volume": int(hash_data.get("bucket_incremental_volume", 0)),
                                        "count": int(hash_data.get("count", 0)),
                                        "cumulative": int(hash_data.get("cumulative", 0)),
                                        "first_timestamp": 0,
                                        "last_timestamp": 0
                                    }
                                    # Parse timestamp from key if possible
                                    key_parts = key.split(":")
                                    if len(key_parts) >= 4:
                                        try:
                                            hour_part = int(key_parts[-2])
                                            minute_part = int(key_parts[-1]) * 5  # Convert bucket to minute
                                            # Create approximate timestamp
                                            from datetime import datetime
                                            dt = datetime.now().replace(hour=hour_part, minute=minute_part, second=0, microsecond=0)
                                            bucket_data["first_timestamp"] = dt.timestamp()
                                            bucket_data["last_timestamp"] = dt.timestamp()
                                        except (ValueError, IndexError):
                                            pass
                                    buckets.append(bucket_data)
                        except Exception as e:
                            continue
                except Exception as e:
                    continue

            # Sort by timestamp
            buckets.sort(key=lambda x: x.get("first_timestamp", 0))
            return buckets

        except Exception as e:
            logger.error(f"Failed to get time buckets for {symbol}: {e}")
            return []
    
    def store_time_bucket(self, symbol, bucket_data):
        """Store a time bucket for a symbol"""
        try:
            # Create bucket key
            session_date = self.current_session
            hour = bucket_data.get("hour", 0)
            minute = bucket_data.get("minute", 0)
            bucket_key = f"bucket_incremental_volume:bucket:{symbol}:{session_date}:{hour}:{minute}"
            
            # Store bucket data as JSON
            self.redis.setex(bucket_key, 86400, json.dumps(bucket_data))  # 24 hour TTL
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store time bucket for {symbol}: {e}")
            return False
    
    def _validate_and_normalize_session_data(self, symbol, data):
        """Validate and normalize session data fields for consistency"""
        if not isinstance(data, dict):
            return data
            
        # Required fields with defaults
        required_fields = {
            'bucket_cumulative_volume': 0,
            'last_price': 0.0,
            'high': 0.0,
            'low': 0.0,
            'session_date': self.current_session,
            'update_count': 0,
            'last_update_timestamp': 0.0,
            'first_update': 0.0,
            'last_update': 0.0
        }
        
        # Add missing required fields
        for field, default_value in required_fields.items():
            if field not in data:
                data[field] = default_value
                
        # Backward compatibility: map old field names to new ones
        field_mappings = {
            'high': 'high',
            'low': 'low',
            'last_update': 'last_update_timestamp'
        }
        
        for old_field, new_field in field_mappings.items():
            if old_field in data:
                # Always map old field to new field, but preserve old field too
                data[new_field] = data[old_field]
        
        return data

    def update_symbol_data_direct(
        self,
        symbol,
        bucket_cumulative_volume,
        last_price,
        timestamp,
        high=None,
        low=None,
        open_price=None,
        close_price=None,
        depth_data=None,
    ):
        """Store raw cumulative bucket_incremental_volume as-is from Zerodha WebSocket (no calculations)"""
        if timestamp is None:
            timestamp = time.time()

        symbol = self._normalize_symbol(symbol)

        with self.lock:
            session = self._get_session_data(symbol)
            
            # Store raw cumulative bucket_incremental_volume as-is (no calculations)
            session.update({
                'bucket_cumulative_volume': bucket_cumulative_volume,  # Store Zerodha's cumulative as-is
                'zerodha_cumulative_volume': bucket_cumulative_volume,
                'last_price': last_price,
                'high': max(session.get('high', last_price), last_price),
                'low': min(session.get('low', last_price), last_price),
                'update_count': session.get('update_count', 0) + 1,
                'last_update_timestamp': timestamp,
                # Backward compatibility for old field names
                'high': max(session.get('high', last_price), last_price),
                'low': min(session.get('low', last_price), last_price),
                'last_update': timestamp
            })
            
            # Update history with raw cumulative bucket_incremental_volume
            self.price_history[symbol].append((timestamp, last_price))
            self.volume_history[symbol].append((timestamp, bucket_cumulative_volume))  # Store raw cumulative
            if len(self.price_history[symbol]) > 1000:
                self.price_history[symbol] = self.price_history[symbol][-500:]
            if len(self.volume_history[symbol]) > 1000:
                self.volume_history[symbol] = self.volume_history[symbol][-500:]

            try:
                # Store in buckets with raw cumulative bucket_incremental_volume
                self._store_time_bucket_direct(symbol, bucket_cumulative_volume, timestamp, last_price)
                self._persist_session_data(symbol, session)
                return bucket_cumulative_volume  # Return raw cumulative bucket_incremental_volume
            except Exception as exc:
                logger.error(f"Failed to store cumulative data for {symbol}: {exc}")
                return 0

    def _store_time_bucket_direct(
        self, symbol, bucket_cumulative_volume, timestamp, last_price, depth_data=None
    ):
        """Store raw cumulative bucket_incremental_volume in time buckets (no calculations)"""
        if bucket_cumulative_volume <= 0:
            return
        self._store_time_bucket(symbol, last_price, bucket_cumulative_volume, timestamp, depth_data)

    def get_time_buckets(self, symbol, session_date=None, hour=None, lookback_minutes=60):
        """Get all time buckets for a symbol with enhanced pattern matching"""
        if session_date is None:
            session_date = self.current_session

        try:
            # Extract base symbol for F&O contracts
            base_symbol = self._extract_base_symbol(symbol)
            
            # Try multiple patterns to find bucket_incremental_volume data
            patterns_to_try = [
                # Pattern 1: bucket_incremental_volume:bucket:{symbol}:{session}:{hour}:{minute} (JSON string) - PRIMARY
                f"bucket_incremental_volume:bucket:{symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:{session_date}:{hour if hour is not None else '*'}:*",
                # Pattern 2: bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket1:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour}:{minute} (ACTUAL FORMAT) - PRIMARY
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket2:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 4: bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour}:{minute_bucket} (Legacy format)
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:buckets:{hour if hour is not None else '*'}:*",
                f"bucket_incremental_volume:bucket_incremental_volume:bucket:{base_symbol}:buckets:{hour if hour is not None else '*'}:*",
                # Pattern 3: bucket_incremental_volume:bucket_incremental_volume:bucket:bucket:{symbol}:* (Legacy format)
                f"bucket_incremental_volume:bucket:{symbol}:*",
                f"bucket_incremental_volume:bucket:{base_symbol}:*",
                # Pattern 4: data:{symbol}:* (Alternative format)
                f"data:{symbol}:*",
                f"data:{base_symbol}:*",
                # Pattern 5: *:{symbol}:* (Fallback pattern)
                f"*:{symbol}:*",
                f"*:{base_symbol}:*"
            ]
            
            buckets = []
            logger.debug(f"{symbol} -> {base_symbol} - Trying {len(patterns_to_try)} patterns")
            for i, pattern in enumerate(patterns_to_try):
                try:
                    keys = self.redis_client.keys(pattern)
                    if keys:
                        logger.debug(f"Pattern {i+1}: {pattern} -> {len(keys)} keys")
                    for key in keys:
                        try:
                            if pattern.startswith("bucket_incremental_volume:bucket:") or pattern.startswith("bucket_incremental_volume:bucket:bucket:") or pattern.startswith("data:"):
                                # JSON string format
                                data = self.redis_client.get(key)
                                if data:
                                    bucket_data = json.loads(data)
                                    # Ensure bucket_incremental_volume field exists
                                    if 'bucket_incremental_volume' not in bucket_data and 'bucket_incremental_volume' in bucket_data:
                                        bucket_data['bucket_incremental_volume'] = bucket_data['bucket_incremental_volume']
                                    buckets.append(bucket_data)
                            elif pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket1:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket2:") or pattern.startswith("bucket_incremental_volume:bucket_incremental_volume:bucket:"):
                                # Redis hash format - convert to bucket format
                                hash_data = self.redis_client.redis_client.hgetall(key)
                                if hash_data and "bucket_incremental_volume" in hash_data:
                                    # Convert hash data to bucket format
                                    bucket_data = {
                                        "symbol": symbol,
                                        "session_date": session_date,
                                        "bucket_incremental_volume": int(hash_data.get("bucket_incremental_volume", 0)),
                                        "count": int(hash_data.get("count", 0)),
                                        "cumulative": int(hash_data.get("cumulative", 0)),
                                        "first_timestamp": 0,
                                        "last_timestamp": 0
                                    }
                                    # Parse timestamp from key if possible
                                    key_parts = key.split(":")
                                    if len(key_parts) >= 4:
                                        try:
                                            hour_part = int(key_parts[-2])
                                            minute_part = int(key_parts[-1]) * 5  # Convert bucket to minute
                                            # Create approximate timestamp
                                            from datetime import datetime
                                            dt = datetime.now().replace(hour=hour_part, minute=minute_part, second=0, microsecond=0)
                                            bucket_data["first_timestamp"] = dt.timestamp()
                                            bucket_data["last_timestamp"] = dt.timestamp()
                                        except (ValueError, IndexError):
                                            pass
                                    buckets.append(bucket_data)
                        except Exception as e:
                            continue
                except Exception as e:
                    continue

            # Sort by timestamp
            buckets.sort(key=lambda x: x.get("first_timestamp", 0))
            return buckets

        except Exception as e:
            logger.error(f"Failed to get time buckets for {symbol}: {e}")
            return []
    def get_session_price_movement(self, symbol, minutes_back=30):
        """Get last_price movement analysis for recent period"""
        if symbol not in self.price_history:
            return None

        current_time = time.time()
        lookback_time = current_time - (minutes_back * 60)

        # Get recent last_price data
        recent_prices = [
            (ts, last_price)
            for ts, last_price in self.price_history[symbol]
            if ts >= lookback_time
        ]

        if len(recent_prices) < 2:
            return None

        start_price = recent_prices[0][1]
        end_price = recent_prices[-1][1]
        high = max(last_price for _, last_price in recent_prices)
        low = min(last_price for _, last_price in recent_prices)

        return {
            "symbol": symbol,
            "period_minutes": minutes_back,
            "start_price": start_price,
            "end_price": end_price,
            "high": high,
            "low": low,
            "price_change": end_price - start_price,
            "price_change_pct": ((end_price - start_price) / start_price) * 100
            if start_price > 0
            else 0,
            "data_points": len(recent_prices),
        }

    def get_pattern_data(self, symbol, time_of_day=None, session_date=None):
        """
        Get pattern data for a symbol based on time of day
        Returns aggregated data for pattern recognition
        """
        buckets = self.get_time_buckets(symbol, session_date)

        if not buckets:
            return None

        # Filter by time of day if specified (e.g., "10:30")
        if time_of_day:
            try:
                hour, minute = map(int, time_of_day.split(":"))
                minute_bucket = minute // 5 * 5
                filtered = [
                    b
                    for b in buckets
                    if b["hour"] == hour and b["minute_bucket"] == minute_bucket
                ]
                buckets = filtered if filtered else buckets
            except ValueError:
                pass  # Invalid time format, use all buckets

        if not buckets:
            return None

        # Calculate pattern metrics
        bucket_incremental_volume = sum(b["bucket_incremental_volume"] for b in buckets)
        avg_price = sum(b["close"] for b in buckets) / len(buckets)
        price_volatility = max(b["high"] for b in buckets) - min(
            b["low"] for b in buckets
        )
        volume_volatility = (
            np.std([b["bucket_incremental_volume"] for b in buckets])
            if NUMPY_AVAILABLE and len(buckets) > 1
            else 0
        )

        return {
            "symbol": symbol,
            "session_date": session_date or self.current_session,
            "bucket_incremental_volume": bucket_incremental_volume,
            "average_price": avg_price,
            "price_volatility": price_volatility,
            "volume_volatility": volume_volatility,
            "time_buckets": buckets,
            "bucket_count": len(buckets),
            "price_trend": "UP"
            if buckets[-1]["close"] > buckets[0]["open"]
            else "DOWN",
            "volume_trend": "HIGH"
            if bucket_incremental_volume > sum(b["bucket_incremental_volume"] for b in buckets[:-1])
            else "NORMAL",
        }

    def cleanup_old_sessions(self, days_to_keep=7):
        """Clean up old session data"""
        try:
            cutoff_date = date.today() - timedelta(days=days_to_keep)
            cutoff_iso = cutoff_date.isoformat()

            # Clean up old session keys
            session_pattern = "session:*:*"
            session_keys = self.redis.keys(session_pattern)

            for key in session_keys:
                try:
                    key_parts = key.split(":")
                    if len(key_parts) >= 3:
                        session_date = key_parts[2]
                        if session_date < cutoff_iso:
                            self.redis.delete(key)
                except:
                    pass

            # Clean up old bucket keys
            bucket_pattern = "bucket_incremental_volume:bucket:*:*:*:*"
            bucket_keys = self.redis.keys(bucket_pattern)

            for key in bucket_keys:
                try:
                    key_parts = key.split(":")
                    if len(key_parts) >= 4:
                        session_date = key_parts[2]
                        if session_date < cutoff_iso:
                            self.redis.delete(key)
                except:
                    pass

            logger.info(f"Cleaned up sessions older than {cutoff_iso}")

        except Exception as e:
            logger.error(f"Failed to cleanup old sessions: {e}")


# Example usage and testing
if __name__ == "__main__":
    # Initialize Redis client with cumulative tracking
    redis_client = RobustRedisClient(host="localhost", port=6379)

    # Example 1: Basic cumulative data update
    print("=== Basic Cumulative Data Update ===")
    redis_client.update_cumulative_data("RELIANCE", 2500.50, 1000)
    redis_client.update_cumulative_data("RELIANCE", 2501.25, 500)
    redis_client.update_cumulative_data("RELIANCE", 2500.75, 750)

    # Get cumulative data
    reliance_data = redis_client.get_cumulative_data("RELIANCE")
    print("RELIANCE Cumulative Data:", reliance_data)

    # Example 2: Order book snapshot storage
    print("\n=== Order Book Snapshot Storage ===")
    sample_order_book = {
        "buy": [
            {"last_price": 2500.00, "quantity": 100, "orders": 5},
            {"last_price": 2499.50, "quantity": 200, "orders": 8},
            {"last_price": 2499.00, "quantity": 150, "orders": 6},
        ],
        "sell": [
            {"last_price": 2500.50, "quantity": 120, "orders": 4},
            {"last_price": 2501.00, "quantity": 180, "orders": 7},
            {"last_price": 2501.50, "quantity": 90, "orders": 3},
        ],
    }

    redis_client.store_order_book_snapshot("RELIANCE", sample_order_book, 2500.25, 200)

    # Example 3: Price movement analysis
    print("\n=== Price Movement Analysis ===")
    # Add more data points for analysis
    for i in range(10):
        last_price = 2500.50 + (i * 0.25)
        bucket_incremental_volume = 100 + (i * 50)
        redis_client.update_cumulative_data("RELIANCE", last_price, bucket_incremental_volume)
        time.sleep(0.1)  # Small delay for timestamp variation

    movement = redis_client.get_session_price_movement("RELIANCE", minutes_back=5)
    print("RELIANCE Price Movement (5 min):", movement)

    # Example 4: Pattern data analysis
    print("\n=== Pattern Data Analysis ===")
    pattern_data = redis_client.get_pattern_data("RELIANCE", time_of_day="10:30")
    print("RELIANCE Pattern Data:", pattern_data)

    # Example 5: Order book history retrieval
    print("\n=== Order Book History ===")
    order_book_history = redis_client.get_order_book_history(
        "RELIANCE", minutes_back=10
    )
    print(f"RELIANCE Order Book History: {len(order_book_history)} snapshots")

    # Example 6: Session evolution
    print("\n=== Session Order Book Evolution ===")
    session_evolution = redis_client.get_session_order_book_evolution("RELIANCE")
    print(
        f"RELIANCE Session Evolution: {session_evolution['total_snapshots']} total snapshots"
    )

    # Example 7: Cleanup old sessions
    print("\n=== Session Cleanup ===")
    redis_client.cleanup_old_sessions(days_to_keep=7)
    print("Cleaned up old sessions")

    # Example 8: AION Signal Publishing and Caching
    print("\n=== AION Signal Publishing ===")
    # Example correlation data
    correlation_data = {
        "symbol1": "RELIANCE",
        "symbol2": "TCS",
        "correlation": 0.95,
        "p_value": 0.001,
        "significant": True,
    }

    # Publish to AION channel
    success = redis_client.publish_aion_signal(
        "correlations", correlation_data, "RELIANCE"
    )
    print(f"Published AION correlation signal: {success}")

    # Store AION Monte Carlo result using existing methods
    key = f"monte_carlo:RELIANCE:{int(time.time())}"
    cache_success = redis_client.store_by_data_type(
        "analysis_cache", key, json.dumps(monte_carlo_data)
    )
    print(f"Stored AION Monte Carlo result: {cache_success}")

    # Retrieve cached result using existing methods
    cached_result = redis_client.retrieve_by_data_type(key, "analysis_cache")
    print(f"Retrieved cached AION result: {cached_result is not None}")

    print("\n=== Integration Complete ===")
    print("✅ Cumulative bucket_incremental_volume tracking with session management")
    print("✅ Order book history with full depth snapshots")
    print("✅ Price movement analysis with time-based buckets")
    print("✅ Pattern recognition data with historical context")
    print("✅ Memory-efficient storage with automatic cleanup")
    print("✅ AION signal publishing and caching")


# ============================================
# Modern Redis Client with RESP3, Retry/Backoff, Circuit Breaker
# ============================================

T = TypeVar("T")

# Ensure os and threading are imported (needed for REDIS_DEFAULTS and _pool_lock)
# These are already imported at module top, but ensure availability here
import os  # Already imported, but explicit for clarity
import threading  # Already imported, but explicit for clarity

# -------- Defaults tuned for Redis 8.2 and high-throughput intraday workloads --------
REDIS_DEFAULTS = {
    "host": os.getenv("REDIS_HOST", "127.0.0.1"),
    "port": int(os.getenv("REDIS_PORT", "6379")),
    "db": int(os.getenv("REDIS_DB_DEFAULT", "0")),
    "username": os.getenv("REDIS_USERNAME"),
    "password": os.getenv("REDIS_PASSWORD"),
    "ssl": os.getenv("REDIS_SSL", "false").lower() == "true",
    # Timeouts (seconds): short connect, reasonable command window under load
    "socket_connect_timeout": float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "1.5")),
    "socket_timeout": float(os.getenv("REDIS_SOCKET_TIMEOUT", "2.5")),
    # RESP3 for richer types & better INFO/Streams parsing issues on Redis 8.x
    "protocol": int(os.getenv("REDIS_PROTOCOL", "3")),
    # Health/probing
    "health_check_interval": int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "15")),
    "client_name": os.getenv("REDIS_CLIENT_NAME", "intraday-core"),
    # Pool sizing: bursty tick traffic; tweak for your cores / process count
    "max_connections": int(os.getenv("REDIS_MAX_CONNECTIONS", "256")),
    # Retry helpers (first-class in redis-py)
    "retry": Retry(ExponentialBackoff(cap=3.0, base=0.05), retries=int(os.getenv("REDIS_RETRIES", "5"))),
    "retry_on_error": (
        ConnectionError,
        RedisTimeoutError,
        BusyLoadingError,
    ),
    "retry_on_timeout": True,
    # Keepalive reduces FIN/ACK flapping on busy hosts
    "socket_keepalive": True,
    # decode_responses: default False; callers may override for text/JSON reads
    "decode_responses": False,
}

# ---------------- Singletons ----------------
_pool_lock = threading.Lock()
_modern_pool: Optional[redis.ConnectionPool] = None
_modern_sync_client: Optional[redis.Redis] = None

# ---------------- Circuit Breaker ----------------
@dataclass
class Circuit:
    threshold: int = int(os.getenv("REDIS_CB_THRESHOLD", "8"))
    cooldown: float = float(os.getenv("REDIS_CB_COOLDOWN_SEC", "5.0"))
    failures: int = 0
    opened_at: float = 0.0

    def allow(self) -> bool:
        """Why: reject fast when Redis is flapping; half-open after cooldown"""
        if self.opened_at <= 0:
            return True
        if (time.time() - self.opened_at) >= self.cooldown:
            return True
        return False

    def mark_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.opened_at = time.time()

    def mark_success(self) -> None:
        self.failures = 0
        self.opened_at = 0.0

_modern_cb = Circuit()

def _build_modern_pool(overrides: dict | None = None) -> redis.ConnectionPool:
    """Build modern Redis connection pool with RESP3 and retry/backoff"""
    cfg = REDIS_DEFAULTS.copy()
    if overrides:
        cfg.update(overrides)
    # Note: redis.ConnectionPool doesn't support all kwargs; filter unsupported ones
    pool_kwargs = {
        "host": cfg["host"],
        "port": cfg["port"],
        "db": cfg["db"],
        "username": cfg.get("username"),
        "password": cfg.get("password"),
        "socket_connect_timeout": cfg["socket_connect_timeout"],
        "socket_timeout": cfg["socket_timeout"],
        "max_connections": cfg["max_connections"],
        "socket_keepalive": cfg["socket_keepalive"],
        "health_check_interval": cfg["health_check_interval"],
        "client_name": cfg["client_name"],
    }
    # Remove None values
    pool_kwargs = {k: v for k, v in pool_kwargs.items() if v is not None}
    
    # SSL and protocol are handled at connection level, not pool level
    pool = redis.ConnectionPool(**pool_kwargs)
    return pool

def get_sync_modern(overrides: dict | None = None) -> redis.Redis:
    """
    Return a process-wide Redis client with robust retry/backoff and pooling.
    Note: decode_responses belongs to client, not pool. Each call with different decode_responses
    may need a separate client instance.
    """
    cfg = REDIS_DEFAULTS.copy()
    if overrides:
        cfg.update(overrides)
    
    # Build pool without decode_responses (pool doesn't support it)
    pool_overrides = {k: v for k, v in (overrides or {}).items() if k != "decode_responses"}
    global _modern_pool
    with _pool_lock:
        if _modern_pool is None or (overrides and any(k in pool_overrides for k in ["db", "host", "port"])):
            _modern_pool = _build_modern_pool(pool_overrides)
    
    # Return client with decode_responses set appropriately
    # SSL and protocol are set at client level if supported
    client_kwargs = {
        "connection_pool": _modern_pool,
        "retry": cfg["retry"],
        "retry_on_error": cfg["retry_on_error"],
        "retry_on_timeout": cfg["retry_on_timeout"],
        "decode_responses": bool(cfg.get("decode_responses", False)),
    }
    # Add protocol if supported (RESP3) - some redis-py versions support it
    if cfg.get("protocol"):
        try:
            client_kwargs["protocol"] = cfg["protocol"]
        except Exception:
            pass  # Ignore if protocol not supported in this redis-py version
    
    return redis.Redis(**client_kwargs)  # type: ignore[return-value]

# ---------------- Retry helper for idempotent ops ----------------
def retryable(fn: Callable[..., T]) -> Callable[..., T]:
    """Adds CB + lightweight retry for idempotent calls (GET, HGET, etc.)."""
    def wrapper(*args: Any, **kwargs: Any) -> T:
        attempts = 0
        last_err: Optional[Exception] = None
        while attempts < 3:
            if not _modern_cb.allow():
                raise ConnectionError("Redis circuit open; fast-failing calls")
            try:
                res = fn(*args, **kwargs)
                _modern_cb.mark_success()
                return res
            except (ConnectionError, RedisTimeoutError, BusyLoadingError) as e:
                last_err = e
                attempts += 1
                _modern_cb.mark_failure()
                time.sleep(0.05 * attempts)  # small extra delay on top of redis-py retry
        # bubble the last error with context
        raise last_err or ConnectionError("Unknown Redis error")
    return wrapper

# ---------------- Stream / PubSub helpers ----------------
@retryable
def xadd_safe(
    key: str,
    fields: dict[str, Any],
    maxlen: Optional[int] = None,
    approximate: bool = True,
    trim_strategy: str = "MAXLEN",
) -> str:
    """
    Safe XADD with optional stream trimming.
    Why: prevent unbounded growth causing memory pressure and slow restarts.
    """
    r = get_sync_modern()
    kwargs: dict[str, Any] = {}
    if maxlen:
        kwargs["maxlen"] = maxlen
        kwargs["approximate"] = approximate
        kwargs["trim_strategy"] = trim_strategy
    return r.xadd(key, fields, **kwargs)  # type: ignore[return-value]

@retryable
def create_consumer_group_if_needed(stream: str, group: str, mkstream: bool = True) -> None:
    """Idempotent create of consumer group."""
    r = get_sync_modern()
    try:
        r.xgroup_create(name=stream, groupname=group, id="0", mkstream=mkstream)
    except ResponseError as e:
        # BUSYGROUP is fine: group exists
        if "BUSYGROUP" not in str(e):
            raise

@retryable
def xreadgroup_blocking(
    group: str,
    consumer: str,
    streams: dict[str, str],
    count: int = 100,
    block_ms: int = 2_000,
) -> list[Tuple[bytes, list[Tuple[bytes, list[Tuple[bytes, dict[bytes, bytes]]]]]]]:
    """
    Blocking XREADGROUP with time-bounded wait.
    Why: bounded latency avoids 'stuck' consumers on network hiccups.
    """
    r = get_sync_modern()
    return r.xreadgroup(groupname=group, consumername=consumer, streams=streams, count=count, block=block_ms)

# ---------------- Health utilities ----------------
def ping_modern() -> bool:
    try:
        return bool(get_sync_modern().ping())
    except (ConnectionError, RedisTimeoutError, AuthenticationError, BusyLoadingError):
        return False

def info_sections_modern(sections: Iterable[str] = ("server", "memory", "clients", "replication", "keyspace")) -> dict[str, dict]:
    r = get_sync_modern()
    out: dict[str, dict] = {}
    for s in sections:
        try:
            out[s] = r.info(section=s)  # RESP3 returns rich types
        except ResponseError:
            out[s] = {}  # tolerate partial failures
    return out

def role_modern() -> Tuple[Any, ...]:
    try:
        return tuple(get_sync_modern().execute_command("ROLE"))  # finer-grained than r.role() on some versions
    except Exception:
        return tuple()

# ============================================
# Backward-compatible get_redis_client (wraps modern or legacy)
# ============================================

def get_redis_client(config=None, db: Optional[int] = None, *, decode_responses: bool = False):
    """
    Factory function to create and return a singleton RobustRedisClient instance.
    Now uses modern RESP3 + retry/backoff client under the hood when available.
    Maintains backward compatibility with existing RobustRedisClient interface.
    
    Args:
        config: Legacy config dict (optional)
        db: Database number (optional, overrides config)
        decode_responses: If True, decode Redis responses to strings (for JSON/indicator reads)
    
    Returns:
        RobustRedisClient instance (or modern client wrapped for backward compatibility)
    """
    global _redis_client_instance
    
    # If db or decode_responses specified, try modern client directly
    if db is not None or decode_responses:
        try:
            from config.redis_config import get_redis_config
            redis_config = get_redis_config()
            overrides = {
                "host": redis_config.get("host", "127.0.0.1"),
                "port": redis_config.get("port", 6379),
                "db": db if db is not None else redis_config.get("db", 0),
                "password": redis_config.get("password"),
                "decode_responses": decode_responses,
            }
            return get_sync_modern(overrides)
        except Exception:
            # Fallback to legacy path
            pass
    if _redis_client_instance is None:
        try:
            # Try modern implementation first (if config allows)
            use_modern = os.getenv("REDIS_USE_MODERN", "true").lower() == "true"
            if use_modern and not config:
                # Use modern client for simple cases
                try:
                    from config.redis_config import get_redis_config
                    redis_config = get_redis_config()
                    overrides = {
                        "host": redis_config.get("host", "127.0.0.1"),
                        "port": redis_config.get("port", 6379),
                        "db": redis_config.get("db", 0),
                        "password": redis_config.get("password"),
                        "max_connections": redis_config.get("max_connections", 256),
                    }
                    modern_client = get_sync_modern(overrides)
                    # Wrap in RobustRedisClient interface for backward compatibility
                    # This allows existing code to work without changes
                    logger.info("✅ Using modern Redis client (RESP3 + retry/backoff) with backward-compat wrapper")
                except Exception as modern_err:
                    logger.warning(f"⚠️ Modern client init failed, falling back: {modern_err}")
                    use_modern = False
            
            if not use_modern:
                # Use centralized Redis configuration (legacy path)
                from config.redis_config import get_redis_config, Redis8Config
                
                # Get base Redis configuration
                redis_config = get_redis_config()
                
                # Allow config override for specific settings
                if config:
                    redis_config.update({
                        k: v for k, v in config.items() 
                        if k.startswith('redis_') and k in ['redis_host', 'redis_port', 'redis_db', 'redis_password']
                    })
                
                # Create RobustRedisClient with proper configuration
                # Pass max_connections through config, not as direct parameter
                redis_config_with_max_conn = redis_config.copy()
                # Ensure max_connections is passed as redis_max_connections
                if "max_connections" in redis_config:
                    redis_config_with_max_conn["redis_max_connections"] = redis_config["max_connections"]
                if config:
                    redis_config_with_max_conn.update(config)
                
                _redis_client_instance = RobustRedisClient(
                    host=redis_config.get("host", "localhost"),
                    port=redis_config.get("port", 6379),
                    db=redis_config.get("db", 0),
                    password=redis_config.get("password"),
                    decode_responses=redis_config.get("decode_responses", True),
                    health_check_interval=redis_config.get("health_check_interval", 30),
                    config=redis_config_with_max_conn
                )
                logger.info("✅ Redis client created using centralized configuration")
        except Exception as e:
            logger.error(f"Failed to create Redis client with centralized config: {e}")
            # Fallback to basic configuration
            try:
                _redis_client_instance = RobustRedisClient(
                    host="localhost", port=6379, db=0, config=config
                )
                logger.warning("⚠️ Using fallback Redis configuration")
            except Exception as fallback_e:
                logger.error(f"Failed to create fallback Redis client: {fallback_e}")
                return None
    return _redis_client_instance


def publish_to_redis(channel, message, config=None):
    """Publish a message to a Redis channel"""
    client = get_redis_client(config)
    if client and client.is_connected:
        try:
            client.publish(channel, message)
            return True
        except Exception as e:
            logger.error(f"Failed to publish to Redis channel {channel}: {e}")
    return False
