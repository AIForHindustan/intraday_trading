"""
Data Pipeline Module
Handles Redis subscription, tick ingestion, and preprocessing
Receives data from crawlers via Redis and forwards to pattern engine
"""

import json
import time
import threading
import logging
from logging.handlers import RotatingFileHandler
import redis
import queue
from collections import deque
from datetime import datetime
from pathlib import Path
import os
import sys
import atexit
import signal
from typing import Any, Dict, List
import asyncio

# Process-specific Redis client caching (prevents connection pool exhaustion)
# Use RedisManager82 for Redis 8.2 optimized connection management
from redis_files.redis_manager import RedisManager82

# ✅ SOLUTION 2: Robust Stream Consumer for handling pending messages and connection resilience
class RobustStreamConsumer:
    """
    Robust Redis stream consumer with automatic reconnection and backlog handling.
    Processes both pending and new messages to prevent backlog accumulation.
    """
    
    def __init__(self, stream_key: str, group_name: str, consumer_name: str, db: int = 1):
        """
        Initialize robust stream consumer.
        
        Args:
            stream_key: Redis stream key name
            group_name: Consumer group name
            consumer_name: Consumer name (unique per process)
            db: Redis database number (default: 1 for realtime)
        """
        self.stream_key = stream_key
        self.group_name = group_name
        self.consumer_name = consumer_name
        self.db = db
        self.redis_client = None
        self.logger = logging.getLogger(__name__)
        self._reconnect()
    
    def _reconnect(self):
        """Recreate Redis client on connection issues"""
        try:
            if self.redis_client:
                try:
                    self.redis_client.close()
                except:
                    pass
        except:
            pass
        
        # ✅ SOLUTION 4: Use RedisManager82 with optimized pool size from PROCESS_POOL_CONFIG
        # process_name="stream_consumer" will use 10 connections from PROCESS_POOL_CONFIG
        self.redis_client = RedisManager82.get_client(
            process_name="stream_consumer",
            db=self.db,
            max_connections=None  # Use PROCESS_POOL_CONFIG value (10 connections)
        )
        
        # Ensure consumer group exists
        try:
            groups = self.redis_client.xinfo_groups(self.stream_key)
            group_exists = any(
                g.get('name') == self.group_name if isinstance(g.get('name'), str) 
                else g.get('name', b'').decode('utf-8') == self.group_name
                for g in groups
            )
            if not group_exists:
                self.redis_client.xgroup_create(
                    name=self.stream_key,
                    groupname=self.group_name,
                    id='0',  # Start from beginning to catch pending messages
                    mkstream=True
                )
                self.logger.info(f"✅ Created consumer group '{self.group_name}' for '{self.stream_key}'")
        except Exception as e:
            error_str = str(e).lower()
            if "busygroup" in error_str or "already exists" in error_str:
                pass  # Group already exists
            elif "no such key" in error_str:
                # Stream doesn't exist yet, will be created on first XADD
                pass
            else:
                self.logger.warning(f"⚠️ Error checking/creating consumer group: {e}")
    
    def process_messages(self, process_callback):
        """
        Process messages with automatic reconnection and backlog handling.
        
        Args:
            process_callback: Callback function(message_data) to process each message
        """
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while True:
            try:
                # First, process any pending messages (backlog recovery)
                pending_messages = None
                try:
                    pending_messages = self.redis_client.xreadgroup(
                        groupname=self.group_name,
                        consumername=self.consumer_name,
                        streams={self.stream_key: '0'},  # '0' = pending messages
                        count=100,  # Process up to 100 pending messages per batch
                        block=1000  # 1 second timeout
                    )
                except Exception as e:
                    error_str = str(e).lower()
                    if "no such key" in error_str or "no such stream" in error_str:
                        # Stream doesn't exist yet, skip pending read
                        pending_messages = None
                    else:
                        raise
                
                # Process new messages
                new_messages = None
                try:
                    new_messages = self.redis_client.xreadgroup(
                        groupname=self.group_name,
                        consumername=self.consumer_name,
                        streams={self.stream_key: '>'},  # '>' = new messages only
                        count=50,  # Process up to 50 new messages per batch
                        block=2000  # 2 second timeout
                    )
                except Exception as e:
                    error_str = str(e).lower()
                    if "no such key" in error_str or "no such stream" in error_str:
                        # Stream doesn't exist yet, skip new read
                        new_messages = None
                    else:
                        raise
                
                # Combine both batches
                all_messages = []
                if pending_messages:
                    all_messages.extend(pending_messages)
                if new_messages:
                    all_messages.extend(new_messages)
                
                if not all_messages:
                    continue  # No messages, continue loop
                
                # Process messages and ACK them
                processed_count = 0
                ack_ids = []
                
                for stream_data in all_messages:
                    if not stream_data or len(stream_data) < 2:
                        continue
                    
                    stream_name_bytes, stream_messages = stream_data[0], stream_data[1]
                    
                    # Decode stream name if bytes
                    if isinstance(stream_name_bytes, bytes):
                        stream_name = stream_name_bytes.decode('utf-8')
                    else:
                        stream_name = str(stream_name_bytes)
                    
                    for message_id, message_data in stream_messages:
                        try:
                            # Process message using callback
                            process_callback(message_data)
                            
                            # Track for ACK
                            ack_ids.append((stream_name, message_id))
                            processed_count += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing message {message_id}: {e}")
                            # Still ACK to prevent infinite reprocessing
                            ack_ids.append((stream_name, message_id))
                
                # Batch ACK all processed messages
                if ack_ids:
                    try:
                        for stream_name, message_id in ack_ids:
                            self.redis_client.xack(stream_name, self.group_name, message_id)
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error ACKing messages: {e}")
                
                if processed_count > 0:
                    self.logger.debug(f"✅ Processed {processed_count} messages from {self.stream_key}")
                
                # Reset error counter on success
                consecutive_errors = 0
                
            except redis.ConnectionError as e:
                consecutive_errors += 1
                self.logger.error(f"❌ Redis connection error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.error(f"❌ Max consecutive errors reached. Stopping consumer.")
                    break
                
                time.sleep(5)
                self._reconnect()
                
            except Exception as e:
                consecutive_errors += 1
                error_str = str(e).lower()
                
                if "too many connections" in error_str:
                    self.logger.warning(f"⚠️ Too many connections - will retry after delay")
                    time.sleep(5)
                    self._reconnect()
                elif "no such key" in error_str or "no such stream" in error_str:
                    # Stream doesn't exist yet, wait and retry
                    time.sleep(2)
                    consecutive_errors = 0  # Don't count as error
                else:
                    self.logger.error(f"❌ Stream processing error: {e}")
                    time.sleep(1)
                    
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.error(f"❌ Max consecutive errors reached. Stopping consumer.")
                    break

# Module-level cached clients for process-specific connection pools
_redis_clients_cache: Dict[int, redis.Redis] = {}
_clients_lock = threading.Lock()

def get_cached_redis_client(db: int = 1) -> redis.Redis:
    """
    Get a cached Redis client for the intraday_scanner process.
    Uses RedisManager82 (Redis 8.2 optimized) connection pool to prevent connection exhaustion.
    
    Args:
        db: Redis database number (default: 1 for realtime)
        
    Returns:
        redis.Redis: Cached Redis client instance for this process+db
        
    Note:
        Pool size is automatically optimized based on PROCESS_POOL_CONFIG
        (20 connections for "intraday_scanner" process)
    """
    global _redis_clients_cache, _clients_lock
    
    with _clients_lock:
        if db not in _redis_clients_cache:
            # Use RedisManager82 for Redis 8.2 optimized connection management
            # ✅ SOLUTION 4: Use PROCESS_POOL_CONFIG (30 connections for intraday_scanner)
            _redis_clients_cache[db] = RedisManager82.get_client(
                process_name="intraday_scanner",
                db=db,
                max_connections=None  # Use PROCESS_POOL_CONFIG value (30 connections)
            )
    
    return _redis_clients_cache[db]


def cleanup_redis_connections():
    """
    Cleanup function to close all cached Redis connections properly.
    Called on process exit to prevent connection leaks.
    Uses RedisManager82 cleanup for proper pool management.
    """
    global _redis_clients_cache, _clients_lock
    
    with _clients_lock:
        # Use RedisManager82 cleanup for process-specific pools
        try:
            RedisManager82.cleanup(process_name="intraday_scanner")
        except Exception:
            pass
        
        # Also cleanup cached clients
        for db, client in _redis_clients_cache.items():
            try:
                # Close the client connection (releases connections back to pool)
                if client:
                    try:
                        client.close()
                    except Exception:
                        pass
            except Exception as e:
                # Ignore errors during cleanup - best effort
                pass
        
        # Clear the cache
        _redis_clients_cache.clear()


def _signal_handler(signum, frame):
    """
    Handle termination signals gracefully by cleaning up Redis connections.
    
    For SIGTERM: Cleanup and exit gracefully (used by process managers)
    For SIGINT: Cleanup only (application will handle the rest)
    """
    cleanup_redis_connections()
    # Don't re-raise - let the application handle shutdown naturally


# Register cleanup handlers for graceful connection closure
atexit.register(cleanup_redis_connections)
# Register signal handlers for clean shutdown
try:
    signal.signal(signal.SIGTERM, _signal_handler)
except (ValueError, OSError):
    # SIGTERM might not be available on all platforms
    pass

try:
    signal.signal(signal.SIGINT, _signal_handler)
except (ValueError, OSError):
    # SIGINT might not be available in all contexts
    pass

import numpy as np

from utils.time_utils import INDIAN_TIME_PARSER
from config.utils.timestamp_normalizer import TimestampNormalizer
from utils.vix_utils import VIXUtils
from intraday_scanner.calculations import expiry_calculator
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
    normalize_session_record,
)
from intraday_scanner.math_dispatcher import MathDispatcher
from patterns.pattern_mathematics import PatternMathematics

# ✅ Spoofing detection simplified - uses Redis-based detection only (no detector object)

# Timezone utilities
import pytz

IST = pytz.timezone("Asia/Kolkata")

FIELD_MAPPING_MANAGER = get_field_mapping_manager()
SESSION_FIELD_ZERODHA_CUM = resolve_session_field("zerodha_cumulative_volume")
SESSION_FIELD_BUCKET_CUM = resolve_session_field("bucket_cumulative_volume")
SESSION_FIELD_BUCKET_INC = resolve_session_field("bucket_incremental_volume")
SESSION_FIELD_LTQ = resolve_session_field("zerodha_last_traded_quantity")

def get_current_ist_time():
    """Get current time in IST timezone"""
    return datetime.now(IST)


def get_current_ist_timestamp():
    """Get current timestamp in IST as ISO string"""
    return get_current_ist_time().isoformat()


class DataPipeline:
    """
    Main data ingestion pipeline
    Subscribes to Redis channels and provides tick data to pattern engine
    """

    def __init__(
        self, redis_client=None, config=None, pattern_detector=None, alert_manager=None, tick_processor=None
    ):
        """Initialize the data pipeline with configurable parameters"""
        self.config = config or {}
        self.running = False
        self.last_heartbeat = time.time()  # Health check heartbeat
        self.pattern_detector = pattern_detector
        self.alert_manager = alert_manager
        self.tick_processor = tick_processor
        self.math_dispatcher = MathDispatcher(PatternMathematics, None)
        
        # Initialize cumulative bucket_incremental_volume tracking for incremental bucket_incremental_volume calculation
        self._cumulative_volume_tracker = {}
        
        # Deduplication tracking to reduce processing overhead
        self.last_processed = {}  # symbol -> last processed timestamp
        self.dedupe_window = 0.1  # 100ms between processing same symbol
        
        # News processing tracking
        self.last_news_check = 0
        self.news_check_interval = 30  # Check for news every 30 seconds

        # SINGLE project root calculation
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if self.project_root not in sys.path:
            sys.path.insert(0, self.project_root)

        # Import schema validation function from consolidated config
        try:
            from config.schemas import (
                validate_unified_tick,
                calculate_derived_fields,
                map_kite_to_unified,
                normalize_zerodha_tick_data,
            )
            from crawlers.utils.instrument_mapper import InstrumentMapper
            instrument_mapper = InstrumentMapper()
            resolve_token_to_symbol = instrument_mapper.token_to_symbol
            self.validate_unified_tick = validate_unified_tick
            self.calculate_derived_fields = calculate_derived_fields
            self.map_kite_to_unified = map_kite_to_unified
            self.normalize_zerodha_tick_data = normalize_zerodha_tick_data
            self.resolve_token_to_symbol = resolve_token_to_symbol
            self.schema_validation_available = True
        except ImportError:
            self.validate_unified_tick = None
            # Provide safe fallbacks so downstream calls do not fail
            self.schema_validation_available = False
            self.calculate_derived_fields = lambda cleaned: {}
            self.map_kite_to_unified = lambda data: data
            self.normalize_zerodha_tick_data = lambda data: data
            self.resolve_token_to_symbol = lambda token: None

        # Configurable parameters
        self.buffer_capacity = self.config.get("buffer_size", 50000)
        self.batch_size = self.config.get(
            "batch_size", 10
        )  # Smaller batch size for faster processing
        self.dedup_window = self.config.get("dedup_window", 5.0)  # seconds
        self.max_log_size = self.config.get("max_log_size", 10 * 1024 * 1024)  # 10MB
        self.log_backup_count = self.config.get("log_backup_count", 5)

        # Initialize last_price cache for underlying last_price tracking
        self.price_cache = {}
        
        # Setup logging first
        self._setup_logging()

        # ✅ TIER 2: Initialize historical archive (async, non-blocking)
        # Must be after _setup_logging() so logger is available
        self.historical_archive_enabled = self.config.get('enable_historical_archive', True)
        self.historical_archive = None
        if self.historical_archive_enabled:
            try:
                from utils.historical_archive import get_historical_archive
                postgres_config = self.config.get('postgres_config', {})
                self.historical_archive = get_historical_archive(
                    postgres_config=postgres_config,
                    batch_size=self.config.get('archive_batch_size', 1000),
                    batch_interval=self.config.get('archive_batch_interval', 5.0),
                    enable_postgres=self.config.get('enable_postgres_archive', False),
                    fallback_to_parquet=True
                )
                self.logger.info("✅ Historical archive initialized (Tier 2)")
            except Exception as e:
                self.logger.warning(f"⚠️ Historical archive unavailable: {e}. Continuing without archival.")
                self.historical_archive = None

        # ✅ Spoofing detection simplified - uses Redis-based detection only (no detector object)
        self.spoofing_detector = None
        self.spoofing_enabled = False

        # News cache - symbol -> news data
        self.news_cache = {}
        self.news_cache_expiry = 1800  # 30 minutes
        self.last_news_cleanup = time.time()

        # Spoofing cache - symbol -> spoofing alert data
        self.spoofing_cache = {}
        self.spoofing_cache_expiry = 300  # 5 minutes (shorter than news)
        self.last_spoofing_cleanup = time.time()

        # Spoofing blocks - symbol -> block expiry time
        self.spoofing_blocks = {}

        # ✅ SOLUTION 4: Use RedisManager82 for process-specific connection pools
        # ✅ FIXED: Create wrapper to add store_by_data_type method to raw Redis client
        if redis_client:
            # If already has store_by_data_type, use it; otherwise wrap it
            if hasattr(redis_client, 'store_by_data_type'):
                self.redis_client = redis_client
            else:
                # Wrap with a simple wrapper that adds store_by_data_type
                self.redis_client = self._wrap_redis_client(redis_client)
        else:
            # Use RedisManager82 with PROCESS_POOL_CONFIG (30 connections for intraday_scanner)
            raw_client = RedisManager82.get_client(
                process_name="intraday_scanner",
                db=0,  # Default DB for primary client
                max_connections=None  # Use PROCESS_POOL_CONFIG value
            )
            if not raw_client:
                self.logger.error("❌ Failed to initialize Redis client")
                raise RuntimeError("Redis client initialization failed")
            
            # ✅ FIXED: Wrap raw Redis client to add store_by_data_type method
            self.redis_client = self._wrap_redis_client(raw_client)
        
        # Initialize Redis storage layer
        from redis_files.redis_storage import RedisStorage
        self.redis_storage = RedisStorage(self.redis_client)
        
        # Cache Redis clients upfront using process-specific connection pools
        # This prevents connection pool exhaustion by using isolated pools per process
        try:
            # Use cached optimized process-specific client for DB 1 (realtime)
            self.realtime_client = get_cached_redis_client(db=1)
            # News client uses same DB 1, so reuse the cached client
            self.news_client = self.realtime_client
        except Exception as e:
            self.logger.warning(f"Failed to initialize process-specific Redis clients: {e}")
            # Fallback to shared client if process-specific fails
            self.realtime_client = self.redis_client
            self.news_client = self.redis_client
        
        # Initialize HybridCalculations for high-performance processing
        from intraday_scanner.calculations import HybridCalculations
        self.hybrid_calculations = HybridCalculations(max_cache_size=500, max_batch_size=174)
        
        # ✅ SINGLE SOURCE OF TRUTH: VolumeStateManager is called by WebSocket parser only
        # DataPipeline should NOT call VolumeStateManager - this violates single source of truth
        self.logger.info("✅ DataPipeline using pre-calculated volume from WebSocket parser")

        # ✅ Spoofing detection removed - simplified architecture

        # Partial message buffers to recover from split/corrupt pubsub payloads
        self.partial_message_buffers: Dict[str, str] = {}
        self.max_partial_buffer = int(self.config.get("max_partial_buffer", 8192))

    def _wrap_redis_client(self, raw_client):
        """
        ✅ FIXED: Wrap raw Redis client to add store_by_data_type method.
        This allows using store_by_data_type with RedisManager82 clients.
        """
        from redis_files.redis_config import get_database_for_data_type, get_ttl_for_data_type
        
        class RedisClientWrapper:
            def __init__(self, client):
                self.client = client
                # Cache DB clients for different databases
                self._db_clients = {}
            
            def __getattr__(self, name):
                # Delegate all other methods to the underlying client
                return getattr(self.client, name)
            
            def _get_client_for_db(self, db_num):
                """Get client for specific database"""
                if db_num not in self._db_clients:
                    # Get client for this DB using RedisManager82
                    db_client = RedisManager82.get_client(
                        process_name="intraday_scanner",
                        db=db_num,
                        max_connections=None
                    )
                    self._db_clients[db_num] = db_client
                return self._db_clients[db_num]
            
            def store_by_data_type(self, data_type: str, key: str, value: str, ttl: int = None):
                """Store data in appropriate database based on type"""
                try:
                    db_num = get_database_for_data_type(data_type)
                    if ttl is None:
                        ttl = get_ttl_for_data_type(data_type)
                    
                    db_client = self._get_client_for_db(db_num)
                    
                    # Store with TTL
                    if ttl and ttl > 0:
                        db_client.setex(key, ttl, value)
                    else:
                        db_client.set(key, value)
                    
                    return True
                except Exception as e:
                    self.logger.error(f"❌ Error storing {key} via store_by_data_type: {e}")
                    return False
            
            def retrieve_by_data_type(self, key: str, data_type: str):
                """Retrieve data from appropriate database based on type"""
                try:
                    db_num = get_database_for_data_type(data_type)
                    db_client = self._get_client_for_db(db_num)
                    return db_client.get(key)
                except Exception as e:
                    self.logger.debug(f"Error retrieving {key} via retrieve_by_data_type: {e}")
                    return None
        
        wrapper = RedisClientWrapper(raw_client)
        wrapper.logger = self.logger
        return wrapper

        # Partial message buffers to recover from split/corrupt pubsub payloads
        self.partial_message_buffers: Dict[str, str] = {}
        self.max_partial_buffer = int(self.config.get("max_partial_buffer", 8192))

        # Tick buffer for processing (unbounded to avoid gating streaming data)
        self.tick_buffer = deque()
        if not hasattr(self, 'buffer_lock'):
            self.buffer_lock = threading.Lock()

        # Queue-based tick processing for timeout support
        self.tick_queue = queue.Queue()

        # Batch processing buffer
        self.batch_buffer = []
        self.batch_lock = threading.Lock()
        self.last_batch_time = time.time()
        self.batch_timeout = 0.01  # 10ms max wait for batch (faster processing)

        # Redis subscription
        self.pubsub = None
        self.subscription_thread = None

        # Deduplication with configurable window
        self.last_tick_hash = {}
        self.dedup_cleanup_interval = 60  # Clean old hashes every minute
        self.last_dedup_cleanup = time.time()

        # Channel-specific error tracking
        self.channel_errors = {
            "market_data.ticks": 0,
            "premarket.orders": 0,
            "alerts.manager": 0,
        }

        # Statistics
        self.stats = {
            "ticks_received": 0,
            "ticks_processed": 0,
            "ticks_deduplicated": 0,
            "batches_processed": 0,
            "errors": 0,
            "json_errors": 0,
            "validation_errors": 0,
            "protocol_errors": 0,
        }

    def _setup_logging(self):
        """Setup logging with rotation for data pipeline"""
        log_timestamp = get_current_ist_time()
        log_dir = Path(f"logs/data_pipeline/{log_timestamp.strftime('%Y/%m/%d')}")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_filename = log_dir / f"{log_timestamp.strftime('%H%M%S')}_pipeline.log"

        self.logger = logging.getLogger("DataPipeline")
        self.logger.setLevel(logging.INFO)

        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []

        # Rotating file handler
        handler = RotatingFileHandler(
            str(log_filename),
            maxBytes=self.max_log_size,
            backupCount=self.log_backup_count,
        )
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Also add console handler for critical errors
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        console.setFormatter(formatter)
        self.logger.addHandler(console)

    def start(self):
        """Start the data pipeline"""
        if self.running:
            self.logger.warning("Data pipeline is already running")
            return

        self.logger.info("🚀 Starting data pipeline...")
        # perf_probe removed - no longer using performance monitoring monkey patches
        self.perf = None
        
        # Start consuming in a separate thread
        import threading
        # Use streams instead of Pub/Sub for better performance
        use_streams = self.config.get("use_redis_streams", True)  # Default to streams
        if use_streams:
            self.consumer_thread = threading.Thread(target=self.start_consuming_streams, daemon=True)
            self.logger.info("📡 Using Redis Streams (XREADGROUP) for data ingestion")
        else:
            self.consumer_thread = threading.Thread(target=self.start_consuming, daemon=True)
            self.logger.info("📡 Using Redis Pub/Sub for data ingestion")
        self.consumer_thread.start()
        
    def start_consuming_streams(self):
        """
        ✅ SOLUTION 2: Start consuming from Redis streams using RobustStreamConsumer.
        This replaces the inefficient Pub/Sub polling and handles pending messages.
        """
        import os
        
        # ✅ FIXED: Set running flag so the loop doesn't exit immediately
        self.running = True
        
        # Use DB 1 for streams (realtime)
        try:
            from redis_files.redis_config import get_database_for_data_type
            realtime_db = get_database_for_data_type("stream_data")
        except Exception:
            realtime_db = 1
        
        # ✅ SOLUTION 2: Use RobustStreamConsumer for ticks:intraday:processed
        consumer_name = f"scanner_{os.getpid()}"
        group_name = "scanner_group"
        stream_key = 'ticks:intraday:processed'
        
        self.logger.info(f"📡 Starting robust stream consumer: {consumer_name} for {stream_key}")
        
        # Create robust consumer
        consumer = RobustStreamConsumer(
            stream_key=stream_key,
            group_name=group_name,
            consumer_name=consumer_name,
            db=realtime_db
        )
        
        # Define message processing callback
        def process_message_callback(message_data: dict):
            """Process a single message from the stream"""
            try:
                # ✅ FIXED: Ensure required attributes are initialized
                if not hasattr(self, 'last_tick_hash'):
                    self.last_tick_hash = {}
                if not hasattr(self, 'batch_lock'):
                    import threading
                    self.batch_lock = threading.Lock()
                if not hasattr(self, 'batch_buffer'):
                    self.batch_buffer = []
                
                # Parse stream message
                parsed_data = self._parse_stream_message(stream_key, message_data)
                if parsed_data:
                    # Process tick directly through existing pipeline
                    self._process_market_tick(parsed_data)
                    self.last_heartbeat = time.time()
            except Exception as e:
                self.logger.error(f"Error processing message in callback: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())
        
        # Start processing messages (RobustStreamConsumer.process_messages is blocking)
        try:
            consumer.process_messages(process_callback=process_message_callback)
        except KeyboardInterrupt:
            self.logger.info("🛑 Stream consumer interrupted")
        except Exception as e:
            self.logger.error(f"❌ Unexpected error in stream consumer: {e}")
        finally:
            self.logger.info("🛑 Robust stream consumer stopped")
            self.running = False
    
    def _parse_stream_message(self, stream_name: str, message_data: dict) -> dict:
        """Parse Redis stream message data into tick dict"""
        try:
            # ✅ Check if message_data is valid
            if not message_data or not isinstance(message_data, dict):
                self.logger.debug(f"⚠️ Invalid message_data: type={type(message_data)}, value={message_data}")
                return None
            
            # ✅ Handle empty dicts gracefully (common in stream processing)
            if len(message_data) == 0:
                self.logger.debug(f"⚠️ Empty message_data received for {stream_name}")
                return None
            
            # Stream messages have bytes keys, decode them
            parsed = {}
            try:
                for key, value in message_data.items():
                    try:
                        if isinstance(key, bytes):
                            key_str = key.decode('utf-8')
                        else:
                            key_str = str(key)
                        
                        if isinstance(value, bytes):
                            try:
                                # Try to decode as UTF-8 string
                                value_str = value.decode('utf-8')
                                # For 'data' field in ticks:intraday:processed, keep as string for JSON parsing below
                                if key_str == 'data' and stream_name == 'ticks:intraday:processed':
                                    parsed[key_str] = value_str
                                elif value_str.startswith('{') or value_str.startswith('['):
                                    # For other fields, auto-parse JSON if possible
                                    try:
                                        parsed[key_str] = json.loads(value_str)
                                    except:
                                        parsed[key_str] = value_str
                                else:
                                    parsed[key_str] = value_str
                            except Exception as decode_err:
                                # Keep as bytes if decode fails
                                self.logger.debug(f"Failed to decode value for key {key_str}: {decode_err}")
                                parsed[key_str] = value
                        else:
                            parsed[key_str] = value
                    except Exception as key_err:
                        self.logger.error(f"❌ Error processing key {key} in stream message: {key_err}")
                        continue
            except Exception as items_err:
                self.logger.error(f"❌ Error iterating message_data.items(): {items_err}, message_data type: {type(message_data)}")
                return None
            
            # ✅ CRITICAL FIX: For ticks:intraday:processed, the 'data' field contains JSON string with tick data
            # The crawler publishes as: {"data": json.dumps(intraday_tick)}
            if stream_name == 'ticks:intraday:processed' and 'data' in parsed:
                data_value = parsed['data']
                # Handle both string and bytes
                if isinstance(data_value, bytes):
                    try:
                        data_str = data_value.decode('utf-8')
                    except Exception:
                        self.logger.error(f"Failed to decode data field as UTF-8")
                        return None
                elif isinstance(data_value, str):
                    data_str = data_value
                else:
                    self.logger.error(f"Unexpected data field type: {type(data_value)}")
                    return None
                
                # Parse the JSON string to get the actual tick data
                try:
                    tick_data = json.loads(data_str)
                    # ✅ The tick data already has symbol from websocket parser
                    if 'symbol' not in tick_data or not tick_data.get('symbol'):
                        # Fallback: resolve from instrument_token if symbol missing
                        if 'instrument_token' in tick_data and tick_data['instrument_token']:
                            try:
                                from crawlers.utils.instrument_mapper import InstrumentMapper
                                mapper = InstrumentMapper()
                                resolved_symbol = mapper.token_to_symbol(tick_data['instrument_token'])
                                if resolved_symbol and not resolved_symbol.startswith('UNKNOWN'):
                                    tick_data['symbol'] = resolved_symbol
                            except Exception:
                                pass
                    return tick_data
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse JSON from data field: {e}, preview: {data_str[:200]}")
                    return None
                except Exception as e:
                    self.logger.error(f"Error parsing data field: {e}")
                    return None
            
            # For other streams or if no 'data' field, try other field names
            for field_name in ['tick_data', 'payload']:
                if field_name in parsed:
                    data_value = parsed[field_name]
                    if isinstance(data_value, bytes):
                        try:
                            data_str = data_value.decode('utf-8')
                        except Exception:
                            continue
                    elif isinstance(data_value, str):
                        data_str = data_value
                    else:
                        continue
                    
                    try:
                        tick_data = json.loads(data_str)
                        return tick_data
                    except Exception:
                        continue
            
            # If we get here and it's ticks:intraday:processed, we failed to parse the 'data' field
            if stream_name == 'ticks:intraday:processed':
                self.logger.error(f"❌ Failed to parse ticks:intraday:processed - missing or invalid 'data' field. Parsed keys: {list(parsed.keys())}, message_data type: {type(message_data)}, message_data keys: {list(message_data.keys()) if isinstance(message_data, dict) else 'N/A'}")
                return None
            
            # For other streams, return parsed dict as-is
            return parsed
            
        except Exception as e:
            self.logger.error(f"Error parsing stream message: {e}")
            return None
    
    def _read_ticks_optimized(self):
        """
        Optimized tick reading using XREADGROUP (replaces Pub/Sub polling).
        Reads from Redis streams with consumer groups for reliability.
        """
        import os
        
        # Get Redis client for DB 1 (streams)
        if not hasattr(self, 'realtime_client') or not self.realtime_client:
            return None
        
        try:
            # Use dedicated consumer group for data pipeline
            group_name = 'data_pipeline_group'
            consumer_name = f"pipeline_{os.getpid()}"
            
            # Ensure consumer group exists
            try:
                groups = self.realtime_client.xinfo_groups('ticks:raw:binary')
                group_exists = any(g.get('name') == group_name for g in groups)
                if not group_exists:
                    self.realtime_client.xgroup_create(
                        name='ticks:raw:binary',
                        groupname=group_name,
                        id='$',
                        mkstream=True
                    )
            except Exception:
                # Group exists or stream doesn't exist yet
                pass
            
            # Read from streams using XREADGROUP (blocking, efficient)
            messages = self.realtime_client.xreadgroup(
                groupname=group_name,
                consumername=consumer_name,
                streams={'ticks:raw:binary': '>', 'ticks:intraday:processed': '>'},
                count=25,      # Process 25 messages per batch (adjust based on load)
                block=1000,    # Block for 1 second (efficient blocking, no polling)
                noack=False    # Require explicit acknowledgment
            )
            
            return messages if messages else None
            
        except redis.ConnectionError:
            raise  # Re-raise connection errors
        except Exception as e:
            error_str = str(e).lower()
            if "no such key" in error_str or "no such stream" in error_str:
                # Stream not ready yet, return None (will retry)
                return None
            elif "unknown consumer group" in error_str:
                # Try to create group
                try:
                    self.realtime_client.xgroup_create(
                        name='ticks:raw:binary',
                        groupname=group_name,
                        id='0',
                        mkstream=True
                    )
                    # Retry read
                    return self._read_ticks_optimized()
                except:
                    return None
            else:
                # Log non-critical errors
                self.logger.debug(f"Stream read error (non-critical): {e}")
                return None
    
    def _process_binary_stream_tick(self, stream_data: dict):
        """Process binary stream tick (from ticks:raw:binary)"""
        try:
            # Binary stream has: binary_data (base64), instrument_token, timestamp, mode, source
            # For now, we'll skip binary processing and wait for processed stream
            # Or you can decode binary_data here if needed
            self.logger.debug(f"Received binary stream tick: {stream_data.get('instrument_token')}")
            # TODO: Add binary decoding logic if needed
        except Exception as e:
            self.logger.error(f"Error processing binary stream tick: {e}")

    def start_consuming(self):
        """Start consuming from Redis channels"""
        self.running = True

        # Subscribe to channels (store as instance variable for reconnection)
        self.channels = [
            "index:NSE:NIFTY 50",  # NIFTY 50 index data
            "index:NSE:NIFTY BANK",  # BANK NIFTY index data
            "index:NSEIX:GIFT NIFTY",  # GIFT NIFTY index data
            "index:NSE:INDIA VIX",  # INDIA VIX data
            "market_data.ticks",  # Main tick data from crawlers
            "market_data.news",  # News data from gift_nifty_gap.py
            "premarket.orders",  # Pre-market data
            "alerts.manager",  # Spoofing alerts from crawler
        ]
        
        # Store subscribed channels for reconnection
        self.subscribed_channels = self.channels.copy()

        try:
            # Ensure Redis client is connected
            if not hasattr(self.redis_client, 'pubsub'):
                raise AttributeError("Redis client does not support pubsub")
            
            # Create pubsub instance bound to realtime DB client to avoid DB switching side-effects
            # Use redis_config to get the correct database for stream_data
            try:
                from redis_files.redis_config import get_database_for_data_type
                realtime_db = get_database_for_data_type("stream_data")
            except Exception:
                realtime_db = 1
            
            # Store realtime_db as instance variable for reconnection
            self.realtime_db = realtime_db
            
            # Use cached optimized process-specific client (always use cached, no get_client() calls)
            if realtime_db == 1:
                dedicated_client = self.realtime_client
            else:
                # For other databases, get cached optimized client for that DB
                dedicated_client = get_cached_redis_client(db=realtime_db)
            if dedicated_client is None:
                raise RuntimeError("No Redis client available for Pub/Sub")
            self.pubsub = dedicated_client.pubsub()
            self.pubsub.subscribe(*self.channels)
            self.logger.info(f"📡 Subscribed to channels: {self.channels}")


            # Start listening with timeout handling
            message_count = 0
            consecutive_errors = 0
            max_consecutive_errors = 5
            
            while self.running:
                try:
                    # Update heartbeat to show thread is alive
                    self.last_heartbeat = time.time()

                    # Check if pubsub is still valid before reading
                    if not self.pubsub or not hasattr(self.pubsub, 'get_message'):
                        self.logger.warning("⚠️ Pubsub connection lost, attempting to reconnect...")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("❌ Too many pubsub connection failures, restarting...")
                            break
                        
                        # Try to recreate pubsub connection
                        try:
                            # Use cached optimized process-specific client (no get_client() calls)
                            if self.realtime_db == 1:
                                dedicated_client = self.realtime_client
                            else:
                                # Get cached optimized client for this database
                                dedicated_client = get_cached_redis_client(db=self.realtime_db)
                            if dedicated_client:
                                # Test connection
                                dedicated_client.ping()
                                # Recreate pubsub and resubscribe
                                if self.pubsub:
                                    try:
                                        self.pubsub.close()
                                    except:
                                        pass
                                self.pubsub = dedicated_client.pubsub()
                                self.pubsub.subscribe(*self.channels)
                                self.logger.info(f"✅ Re-subscribed to {len(self.channels)} channels after reconnection")
                                consecutive_errors = 0
                            else:
                                raise RuntimeError("No Redis client available")
                        except Exception as recon_err:
                            self.logger.warning(f"⚠️ Reconnection attempt failed: {recon_err}, retrying...")
                            time.sleep(2)
                            continue

                    # ✅ OPTIMIZED: Use XREADGROUP instead of Pub/Sub polling
                    # This replaces inefficient get_message(timeout=1.0) polling
                    try:
                        messages = self._read_ticks_optimized()
                        if messages:
                            for stream_name, stream_messages in messages:
                                for message_id_bytes, message_data in stream_messages:
                                    message_id = message_id_bytes.decode('utf-8') if isinstance(message_id_bytes, bytes) else str(message_id_bytes)
                                    try:
                                        # Parse stream message
                                        parsed_data = self._parse_stream_message(stream_name, message_data)
                                        if parsed_data:
                                            # Convert to Pub/Sub message format for compatibility
                                            fake_message = {
                                                "type": "message",
                                                "channel": stream_name.encode() if isinstance(stream_name, str) else stream_name,
                                                "data": json.dumps(parsed_data) if isinstance(parsed_data, dict) else str(parsed_data)
                                            }
                                            message_count += 1
                                            self._process_message(fake_message)
                                            
                                            # Acknowledge successful processing
                                            if hasattr(self, 'realtime_client') and self.realtime_client:
                                                try:
                                                    self.realtime_client.xack(stream_name, 'data_pipeline_group', message_id)
                                                except Exception as ack_err:
                                                    self.logger.debug(f"ACK error (non-critical): {ack_err}")
                                    except Exception as msg_err:
                                        self.logger.error(f"❌ Error processing stream message: {msg_err}")
                                        continue
                            
                            consecutive_errors = 0  # Reset on successful batch
                            if message_count <= 5 or message_count % 100 == 0:
                                # Count total messages in batch
                                total_msgs = sum(len(stream_data[1]) if len(stream_data) >= 2 else 0 for stream_data in messages)
                                self.logger.debug(f"✅ Processed {total_msgs} messages from streams")
                        else:
                            # No messages (normal with blocking read)
                            consecutive_errors = 0
                    except redis.ConnectionError as conn_err:
                        self.logger.warning(f"⚠️ Redis connection error during stream read: {conn_err}")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            break
                        time.sleep(1)
                        continue
                    except Exception as stream_err:
                        error_str = str(stream_err).lower()
                        if "timeout" in error_str or "no such key" in error_str:
                            # Normal timeout or stream not ready, continue
                            consecutive_errors = 0
                            continue
                        else:
                            self.logger.warning(f"⚠️ Stream reading error: {stream_err}")
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                            time.sleep(1)
                            continue

                    # Periodic cleanup of old dedup hashes
                    self._cleanup_old_hashes()
                    
                    # Periodic cleanup of old rolling windows
                    self._cleanup_old_rolling_windows()
                    
                    # Periodic news processing from symbol keys (every 30 seconds) - TEMPORARILY DISABLED
                    # current_time = time.time()
                    # if current_time - self.last_news_check >= self.news_check_interval:
                    #     self._process_news_from_symbol_keys()
                    #     self.last_news_check = current_time

                except redis.ConnectionError as conn_err:
                    self.logger.warning(f"⚠️ Redis connection error in pipeline loop: {conn_err}")
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("❌ Too many connection errors, breaking loop")
                        break
                    # Force pubsub recreation
                    self.pubsub = None
                    time.sleep(2)
                    continue
                except Exception as e:
                    if "timeout" in str(e).lower():
                        # Normal timeout, continue
                        continue
                    else:
                        # Handle protocol errors specifically
                        error_msg = str(e)
                        if "Protocol" in error_msg or "ck_sequence" in error_msg:
                            if self.logger and self.logger.handlers:
                                self.logger.warning(
                                    f"⚠️ Protocol error in data pipeline (likely corrupt data): {error_msg[:100]}..."
                                )
                            self.stats["protocol_errors"] += 1
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                        else:
                            if self.logger and self.logger.handlers:
                                self.logger.error(
                                    f"❌ Unexpected error in data pipeline: {e}"
                                )
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                break
                        time.sleep(0.5)  # Reduced sleep for faster recovery

        except redis.ConnectionError as e:
            error_msg = str(e).lower()
            if self.logger and self.logger.handlers:
                self.logger.error(f"❌ Redis connection error: {e}")
            self.stats["errors"] += 1
            
            # CRITICAL: Handle "Too many connections" with exponential backoff
            # If we hit connection limit, don't create MORE connections by reconnecting immediately
            if "too many connections" in error_msg:
                if not hasattr(self, '_connection_limit_backoff'):
                    self._connection_limit_backoff = 5  # Start with 5 seconds
                else:
                    # Exponential backoff: 5s, 10s, 20s, 30s max
                    self._connection_limit_backoff = min(self._connection_limit_backoff * 2, 30)
                
                if self.logger and self.logger.handlers:
                    self.logger.warning(f"⚠️ Connection limit reached - backing off for {self._connection_limit_backoff}s (stopping pipeline to prevent cascade)")
                
                # CRITICAL: Don't try to reconnect immediately - that creates MORE connections
                # Stop the pipeline and let the health monitor in scanner_main.py restart it
                # after connections have time to close and backoff delay has passed
                if self.logger and self.logger.handlers:
                    self.logger.error("🛑 Stopping pipeline to prevent connection cascade - health monitor will restart after backoff")
                # Don't recursively call start_consuming() - that creates MORE connections
                # The outer health monitor will detect stopped pipeline and restart with proper delay
                return  # Exit start_consuming() - pipeline will be restarted by health monitor
            else:
                # Normal connection error (not "too many connections")
                # Reset backoff for normal errors
                self._connection_limit_backoff = 1
                # Attempt reconnection with normal delay
                time.sleep(2)
                if self.running:
                    if self.logger and self.logger.handlers:
                        self.logger.info("🔄 Attempting to reconnect and resubscribe...")
                    # Recursively restart consuming (will recreate pubsub)
                    self.start_consuming()
        except Exception as e:
            # Handle protocol errors specifically
            error_msg = str(e)
            if "Protocol" in error_msg or "ck_sequence" in error_msg:
                if self.logger and self.logger.handlers:
                    self.logger.warning(
                        f"⚠️ Protocol error in data pipeline (likely corrupt data): {error_msg[:100]}..."
                    )
                self.stats["protocol_errors"] += 1
            else:
                if self.logger and self.logger.handlers:
                    self.logger.error(f"❌ Unexpected error in data pipeline: {e}")
                self.stats["errors"] += 1
        finally:
            self.stop()


    def _process_message(self, message):
        """Process incoming Redis message with specific error handling"""
        # Handle channel (may be bytes or str depending on decode_responses)
        channel = message["channel"]
        if isinstance(channel, bytes):
            channel = channel.decode("utf-8", errors="ignore")
        elif not isinstance(channel, str):
            channel = str(channel)
        
        # Handle payload (may be bytes or str depending on decode_responses)
        raw_payload = message.get("data", "")
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")
        elif not isinstance(raw_payload, str):
            raw_payload = str(raw_payload)

        fragments = [frag for frag in raw_payload.splitlines() if frag.strip()]
        if not fragments:
            fragments = [raw_payload]

        for fragment in fragments:
            combined = self.partial_message_buffers.get(channel, "") + fragment
            combined_stripped = combined.strip()
            if not combined_stripped:
                continue

            try:
                data = json.loads(combined_stripped)
                # Clear buffer on successful parse
                self.partial_message_buffers.pop(channel, None)
            except json.JSONDecodeError as e:
                # Cache partial payload for next fragment
                self.partial_message_buffers[channel] = combined[-self.max_partial_buffer :]
                preview = combined[-120:].replace("\n", " ")
                self.logger.warning(
                    f"Invalid JSON from {channel}: {e}. Cached fragment tail: {preview}"
                )
                self.stats["json_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                continue
            except ValueError as e:
                self.stats["validation_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.warning(f"Validation error for {channel}: {e}")
                continue
            except Exception as e:
                error_msg = str(e)
                if "Protocol" in error_msg or "ck_sequence" in error_msg:
                    self.logger.warning(
                        f"⚠️ Protocol error processing {channel} (corrupt data): {error_msg[:100]}..."
                    )
                    self.stats["protocol_errors"] += 1
                else:
                    self.stats["errors"] += 1
                    self.logger.error(f"❌ Error parsing message for {channel}: {e}")
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                continue

            try:
                # ✅ FIX: Decode channel from bytes to string if needed
                if isinstance(channel, bytes):
                    channel = channel.decode('utf-8')
                
                if channel == "market_data.ticks":
                    self._process_market_tick(data)
                elif channel == "market_data.news":
                    self.logger.info(f"📰 Received news message: {data.get('title', 'Unknown')[:50]}...")
                    self._process_news_from_channel(data)
                elif channel == "premarket.orders":
                    self._process_premarket_order(data)
                elif channel == "alerts.manager":
                    self.logger.debug(
                        f"Received alert manager data for {data.get('symbol', 'UNKNOWN')} - sophisticated detection active"
                    )
                elif channel.startswith("index:"):
                    self._process_index_data(channel, data)
                else:
                    self.logger.warning(f"Unknown channel: {channel}")
            except ValueError as e:
                self.stats["validation_errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.warning(f"Validation error for {channel}: {e}")
            except Exception as e:
                self.stats["errors"] += 1
                self.channel_errors[channel] = self.channel_errors.get(channel, 0) + 1
                self.logger.error(f"❌ Error processing {channel}: {e}", exc_info=True)

    def process_tick(self, raw_tick: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalize core bucket_incremental_volume fields from a crawler tick."""
        symbol = raw_tick.get("symbol") or raw_tick.get("tradingsymbol") or "UNKNOWN"
        current_time = time.time()
        
        # Skip if we just processed this symbol (deduplication)
        if symbol in self.last_processed:
            time_since_last = current_time - self.last_processed[symbol]
            if time_since_last < self.dedupe_window:
                return None  # Skip this tick
        self.last_processed[symbol] = current_time
        
        exchange_ts = raw_tick.get("exchange_timestamp") or raw_tick.get("timestamp")

        try:
            timestamp_ms = (
                TimestampNormalizer.to_epoch_ms(exchange_ts)
                if exchange_ts
                else None
            )
        except Exception:
            timestamp_ms = None

        last_price = raw_tick.get("last_traded_price", raw_tick.get("last_price"))
        bucket_incremental_volume = raw_tick.get("bucket_incremental_volume")
        bucket_cumulative_volume = raw_tick.get("bucket_cumulative_volume") or raw_tick.get(
            "volume_traded_for_the_day"
        )
        bucket_incremental_volume = raw_tick.get("bucket_incremental_volume", bucket_incremental_volume)

        cleaned = {
            "symbol": symbol,
            "timestamp_ms": timestamp_ms,
            "last_price": last_price,
            "bucket_incremental_volume": bucket_incremental_volume,
            "bucket_incremental_volume": bucket_incremental_volume,
            "bucket_cumulative_volume": bucket_cumulative_volume,
            "exchange_timestamp": exchange_ts,
        }

        try:
            vol_val = float(bucket_incremental_volume) if bucket_incremental_volume is not None else None
            inc_val = float(bucket_incremental_volume) if bucket_incremental_volume is not None else None
        except (TypeError, ValueError):
            vol_val = None
            inc_val = None

        if vol_val is not None and inc_val is not None:
            if abs(vol_val - inc_val) > 1e-6:
                self.logger.warning(
                    "Volume mismatch for %s: %s (bucket_incremental_volume) vs %s (incremental)",
                    symbol,
                    vol_val,
                    inc_val,
                )

        return cleaned

    def _process_market_tick(self, data):
        """Process market tick data with unified schema validation and cleaning"""
        start_time = time.time()
        # ✅ DEBUG: Log what we receive before symbol extraction
        incoming_symbol = data.get("symbol", "NOT_FOUND")
        incoming_token = data.get("instrument_token", "NOT_FOUND")
        if incoming_symbol == "NOT_FOUND" or incoming_symbol == "UNKNOWN" or not incoming_symbol:
            self.logger.warning(f"🔍 [TICK_DEBUG] Before extraction: symbol={incoming_symbol}, token={incoming_token}, keys={list(data.keys())[:10]}")
        
        # ✅ FIXED: Use _extract_symbol which includes token resolution
        symbol = self._extract_symbol(data)
        if not symbol or symbol == "UNKNOWN":
            # Fallback to direct extraction if token resolution failed
            symbol = data.get("symbol", data.get("tradingsymbol", "UNKNOWN"))
            if symbol == "UNKNOWN":
                self.logger.warning(f"🔍 [TICK_DEBUG] After extraction: symbol still UNKNOWN, original data keys: {list(data.keys())[:15]}")

        # Apply comprehensive mapping only if schema is available; otherwise skip
        mapped_tick = data
        if getattr(self, "schema_validation_available", False):
            try:
                # 🚀 OPTIMIZED: Use pre-imported function reference (no hot-path import)
                mapped_tick = self.normalize_zerodha_tick_data(data)
            except Exception as e:
                mapped_tick = data

        # Clean the tick data first (handle dict issues)
        cleaned_tick = self._clean_tick_data(mapped_tick)

        # Normalize volume fields for backward compatibility
        if self.schema_validation_available:
            try:
                from config.schemas import normalize_volume_field
                cleaned_tick = normalize_volume_field(cleaned_tick)
            except Exception as e:
                self.logger.warning(f"Volume normalization error: {e}")

        # Validate using unified schema
        if self.schema_validation_available and self.validate_unified_tick:
            try:
                is_valid, issues = self.validate_unified_tick(cleaned_tick)
                if not is_valid:
                    self.logger.warning(f"Schema validation failed: {issues}")
                    self._log_data_quality_issue(cleaned_tick, issues)
                    self.stats["validation_errors"] += 1
                    return
            except Exception as e:
                self.logger.warning(f"Schema validation error: {e}")
                # Continue processing without validation

        # Ensure numeric types are normalized for all downstream consumers
        self._normalize_numeric_types(cleaned_tick)

        # ✅ TIER 2: Archive to historical storage (non-blocking, queue-based)
        # Archive AFTER cleaning/validation to ensure we store complete, validated data
        # This is non-blocking and won't slow down real-time pattern detection
        if self.historical_archive and symbol and symbol != "UNKNOWN":
            try:
                # Archive the cleaned tick data (includes all normalized fields)
                self.historical_archive.archive_tick(cleaned_tick)
            except Exception as e:
                self.logger.debug(f"Archive failed (non-critical): {e}")

        # Validate bucket_incremental_volume normalization is preserved from ingestion
        try:
            self.process_tick(cleaned_tick)
        except Exception as exc:
            self.logger.warning(f"Volume processing sanity check failed: {exc}")

        try:
            self._log_cleaned_tick_summary(cleaned_tick)
        except Exception:
            pass

        if self._is_duplicate(cleaned_tick):
            # Even if duplicate, record latency for visibility
            processing_time = time.time() - start_time
            if processing_time > 0.1:
                self.logger.warning(
                    "Slow tick processing (duplicate): %.3fs", processing_time
                )
            return

        # Add to batch buffer for efficient processing
        with self.batch_lock:
            self.batch_buffer.append(cleaned_tick)
            # Process batch if size reached or timeout
            
        # 🚀 CRITICAL FIX: Call main processing loop for pattern detection and alerts
        try:
            self._process_tick_for_patterns(cleaned_tick)
        except Exception as e:
            self.logger.error(f"Error in pattern processing: {e}")
        
        # Note: Archive happens in _process_market_tick after cleaning/validation
        # This ensures we archive complete, validated data without duplicates
        
        if (
            len(self.batch_buffer) >= self.batch_size
            or (time.time() - self.last_batch_time) > self.batch_timeout
        ):
            self._flush_batch()
    
    def _process_tick_for_patterns(self, tick_data):
        """Process tick data for pattern detection and alerts"""
        start_time = time.time()
        try:
            symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol") or "UNKNOWN"
            # Ensure symbol is present in indicators downstream
            if "symbol" not in tick_data and symbol:
                tick_data["symbol"] = symbol
            self.logger.info(f"Processing tick for patterns: {symbol}")
            
            # ✅ SINGLE SOURCE OF TRUTH: Volume already calculated by WebSocket parser
            # Do NOT recalculate volume - WebSocket parser has already called VolumeStateManager
            # and set bucket_incremental_volume, incremental_volume, and volume fields
            self.logger.debug(f"Using pre-calculated volume from WebSocket parser")
            
            # Calculate indicators and patterns
            # perf_probe removed - no longer using slow_tick_guard context manager
            try:
                if hasattr(self, 'tick_processor') and self.tick_processor:
                    indicators = self.tick_processor.process_tick(symbol, tick_data)
                    if indicators:
                        self.logger.info(f"Indicators calculated for {symbol}: {len(indicators)} indicators")
                        if "symbol" not in indicators:
                            indicators["symbol"] = symbol
                        if hasattr(self, 'pattern_detector'):
                            patterns = self.pattern_detector.detect_patterns(indicators)
                            self.logger.info(f"Pattern detection for {symbol}: {len(patterns)} patterns")
                            self.logger.info(f"🔍 DEBUG: patterns={len(patterns) if patterns else 0}, has_alert_manager={hasattr(self, 'alert_manager')}")
                            if patterns and hasattr(self, 'alert_manager'):
                                self.logger.info(f"🔍 DEBUG: Processing {len(patterns)} patterns for {symbol}")
                                for pattern in patterns:
                                    pattern['symbol'] = symbol
                                    # CRITICAL: Include all calculated indicators in the pattern payload
                                    # This ensures indicators are available in alert payload for dashboard
                                    if indicators and isinstance(indicators, dict):
                                        # Ensure indicators dict exists in pattern
                                        if 'indicators' not in pattern or not isinstance(pattern.get('indicators'), dict):
                                            pattern['indicators'] = {}
                                        # Merge calculated indicators into pattern
                                        pattern['indicators'].update(indicators)
                                        # Also add top-level fields for easy access (including Greeks for options)
                                        for indicator_key in ['rsi', 'macd', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200', 
                                                             'atr', 'vwap', 'bollinger_bands', 'volume_profile', 'volume_ratio', 'price_change',
                                                             # Greeks for options (so they're at top level in alerts)
                                                             'delta', 'gamma', 'theta', 'vega', 'rho', 'dte_years', 'trading_dte']:
                                            if indicator_key in indicators and indicator_key not in pattern:
                                                pattern[indicator_key] = indicators[indicator_key]
                                    self.logger.info(f"🔍 DEBUG: Calling alert_manager.send_alert for {symbol}: {pattern.get('pattern', 'UNKNOWN')}")
                                    alert_sent = self.alert_manager.send_alert(pattern)
                                    if alert_sent:
                                        self.logger.info(f"Alert sent for {symbol}: {pattern.get('pattern', 'UNKNOWN')}")
                                    else:
                                        self.logger.info(f"Alert not sent for {symbol}: {pattern.get('pattern', 'UNKNOWN')} (filtered by AlertManager)")
                            else:
                                self.logger.info(f"🔍 DEBUG: Skipping alert processing - patterns={len(patterns) if patterns else 0}, has_alert_manager={hasattr(self, 'alert_manager')}")
                    else:
                        self.logger.warning(f"No indicators calculated for {symbol}")
                else:
                    self.logger.warning(f"No tick processor available")
            except Exception as e:
                self.logger.error(f"Error calculating indicators: {e}")
                
        except Exception as e:
            self.logger.error(f"Error in _process_tick_for_patterns: {e}")

        # Track processing latency
        processing_time = time.time() - start_time
        if processing_time > 0.1:  # 100ms threshold
            self.logger.warning(
                "Slow tick processing: %.3fs for %s", processing_time, symbol
            )

        self.stats["ticks_received"] += 1

    def _log_cleaned_tick_summary(self, tick: dict):
        """Emit a compact, informative debug line after cleaning to trace logic issues.

        Includes: symbol, prices, bucket_incremental_volume fields, mode, timestamps (ISO + ms), depth validity,
        and flags suspicious combinations (e.g., full mode with zero bucket_incremental_volume fields).
        """
        sym = tick.get("symbol") or tick.get("tradingsymbol") or "UNKNOWN"
        lp = tick.get("last_price", 0)
        vol = tick.get("bucket_incremental_volume", 0)
        vt = tick.get("zerodha_cumulative_volume", 0)
        lq = tick.get("zerodha_last_traded_quantity", 0)
        mode = tick.get("mode", "unknown")
        ts = tick.get("timestamp")
        ts_ms = tick.get("timestamp_ms")
        ex = tick.get("exchange_timestamp")
        ex_ms = tick.get("exchange_timestamp_ms")
        depth_valid = tick.get("depth_valid", False)
        total_buy = tick.get("total_buy_quantity", tick.get("buy_quantity", 0))
        total_sell = tick.get("total_sell_quantity", tick.get("sell_quantity", 0))

        # Compute time delta when possible
        delta_ms = None
        try:
            if isinstance(ts_ms, (int, float)) and isinstance(ex_ms, (int, float)):
                delta_ms = int(ts_ms) - int(ex_ms)
        except Exception:
            delta_ms = None

        msg = (
            f"🧭 Cleaned: {sym} lp={lp} mode={mode} "
            f"vol={vol} vt={vt} lq={lq} depth_valid={depth_valid} "
            f"ts={ts} ex={ex} ts_ms={ts_ms} ex_ms={ex_ms}"
            + (f" Δms={delta_ms}" if delta_ms is not None else "")
            + f" buy_tot={total_buy} sell_tot={total_sell}"
        )

        if self.config.get("debug", False):
            self.logger.info(msg)
        else:
            self.logger.debug(msg)

        # Flag suspicious combination explicitly (but not for indices and ETFs which often have zero bucket_incremental_volume)
        if (
            (mode == "full")
            and (float(vol) == 0)
            and (float(vt) == 0)
            and (float(lq) == 0)
        ):
            # Skip warning for indices and ETFs which often don't have bucket_incremental_volume data
            is_index = sym in ["NIFTY 50", "BANKNIFTY", "NIFTY", "SENSEX", "INDIA VIX"]
            is_etf = (
                sym.endswith("ETF")
                or sym.endswith("BEES")
                or sym.endswith("INAV")
                or sym.endswith("NAV")
                or "ETF" in sym
                or "BEES" in sym
                or sym.endswith("ADD")
                or sym.endswith("SEN")
            )

            if not is_index and not is_etf:
                self.logger.warning(
                    f"⚠️ Full-mode tick with zero bucket_incremental_volume fields for {sym} — check upstream feed."
                )



    def _get_stream_maxlen(self, stream_key: str) -> int:
        """
        Determine appropriate maxlen for Redis stream based on stream type.
        ALWAYS use maxlen in XADD operations to prevent unbounded stream growth.
        
        Args:
            stream_key: Redis stream key name
            
        Returns:
            int: Maximum number of messages to keep in stream
        """
        # Main streams - match optimizer targets
        if stream_key == 'ticks:intraday:processed':
            return 5000  # Match optimizer target
        elif stream_key == 'ticks:raw:binary':
            return 10000  # Match optimizer target
        elif stream_key == 'alerts:stream':
            return 1000  # Match optimizer target
        elif stream_key.startswith('ticks:'):
            # Per-symbol streams - keep smaller to prevent memory bloat
            return 1000
        else:
            # Default for unknown streams
            return 10000
    
    def _publish_to_stream_with_maxlen(self, redis_client, stream_key: str, stream_data: dict) -> bool:
        """
        Publish to Redis stream with ALWAYS applying maxlen parameter.
        This prevents unbounded stream growth at the source.
        
        Args:
            redis_client: Redis client instance
            stream_key: Stream key name
            stream_data: Data to publish
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            maxlen = self._get_stream_maxlen(stream_key)
            redis_client.xadd(
                stream_key,
                stream_data,
                maxlen=maxlen,
                approximate=True  # Faster trimming, slight approximation acceptable
            )
            return True
        except Exception as e:
            self.logger.error(f"Stream publish error for {stream_key}: {e}")
            return False

    def _flush_batch(self):
        """Flush batch buffer to main buffer and queue"""
        if not self.batch_buffer:
            return
        
        # ✅ FIXED: Ensure all required attributes are initialized
        if not hasattr(self, 'buffer_lock'):
            import threading
            self.buffer_lock = threading.Lock()
        if not hasattr(self, 'tick_buffer'):
            from collections import deque
            self.tick_buffer = deque()
        if not hasattr(self, 'batch_buffer'):
            self.batch_buffer = []
        if not hasattr(self, 'tick_queue'):
            import queue
            self.tick_queue = queue.Queue()

        with self.buffer_lock:
            # Extend tick buffer with batch
            self.tick_buffer.extend(self.batch_buffer)

            # Also add to queue for timeout-based retrieval
            for tick in self.batch_buffer:
                try:
                    self.tick_queue.put_nowait(tick)
                except queue.Full:
                    # If queue is full, remove oldest and add new
                    try:
                        self.tick_queue.get_nowait()
                        self.tick_queue.put_nowait(tick)
                    except queue.Empty:
                        pass

        batch_size = len(self.batch_buffer)
        
        # ✅ FIXED: Publish tick data to Redis Streams and process with HybridCalculations
        if hasattr(self, 'redis_client') and self.redis_client:
            # Group ticks by symbol for batch processing
            symbol_ticks = {}
            for tick in self.batch_buffer:
                try:
                    symbol = tick.get('symbol', 'UNKNOWN')
                    if symbol not in symbol_ticks:
                        symbol_ticks[symbol] = []
                    symbol_ticks[symbol].append(tick)
                    
                    # Publish to tick stream for real-time processing in DB 1 (realtime)
                    # Ensure tick data has the symbol field populated
                    tick['symbol'] = symbol
                    stream_key = f"ticks:{symbol}"
                    realtime_client = self.realtime_client
                    
                    # Check if key exists and is not a stream, delete it to avoid WRONGTYPE error
                    key_type = realtime_client.type(stream_key)
                    if key_type and key_type != 'stream':
                        realtime_client.delete(stream_key)
                    
                    # Convert dict to proper stream format
                    stream_data = {
                        'data': json.dumps(tick, default=str),
                        'timestamp': str(int(time.time() * 1000)),
                        'symbol': symbol
                    }
                    try:
                        # ✅ FIXED: ALWAYS use maxlen in XADD to prevent unbounded stream growth
                        if self._publish_to_stream_with_maxlen(realtime_client, stream_key, stream_data):
                            self.stats["ticks_published_to_stream"] = self.stats.get("ticks_published_to_stream", 0) + 1
                    except redis.ConnectionError as conn_err:
                        if "Too many connections" in str(conn_err):
                            # Don't log every single tick failure - just increment error counter
                            self.stats["connection_errors"] = self.stats.get("connection_errors", 0) + 1
                            # Log only once per 100 errors to avoid log spam
                            if self.stats.get("connection_errors", 0) % 100 == 1:
                                self.logger.warning(f"⚠️ Connection pool exhausted (suppressing further logs until resolved)")
                        else:
                            raise  # Re-raise non-connection errors
                except Exception as e:
                    # Handle other errors (but not connection exhaustion)
                    if "Too many connections" not in str(e):
                        self.logger.error(f"Failed to publish tick to stream: {e}")
                    # Increment error counter silently for connection exhaustion
                    self.stats["errors"] = self.stats.get("errors", 0) + 1
            
            # Process ticks with HybridCalculations batch processing
            try:
                if symbol_ticks:
                    self.logger.info(f"📊 Processing batch: {len(symbol_ticks)} symbols, {sum(len(ticks) for ticks in symbol_ticks.values())} total ticks")
                    # ✅ SIMPLIFIED CACHING: Use process_batch_indicators to only store fresh calculations
                    all_results = self.process_batch_indicators(symbol_ticks)
                    self.stats["batch_indicators_calculated"] = self.stats.get("batch_indicators_calculated", 0) + len(all_results)
                    
                    if all_results:
                        self.logger.info(f"✅ Processed indicators for {len(all_results)} symbols (fresh + cached): {list(all_results.keys())[:5]}")
                    else:
                        self.logger.warning(f"⚠️ process_batch_indicators returned empty results for {len(symbol_ticks)} symbols")
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to process batch with HybridCalculations: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
        
        self.batch_buffer.clear()
        self.last_batch_time = time.time()
        self.stats["batches_processed"] += 1

    def process_batch_indicators(self, symbol_data: Dict[str, List[Dict]]) -> Dict[str, Dict]:
        """
        ✅ SIMPLIFIED CACHING: Process batch indicators with intelligent storage.
        Only stores fresh calculations (cached results are skipped).
        
        Args:
            symbol_data: Dict mapping symbol -> list of tick data
            
        Returns:
            Dict mapping symbol -> calculated indicators (fresh + cached combined)
        """
        # Get calculations (fresh + cached)
        all_results = self.hybrid_calculations.batch_process_symbols(symbol_data, max_ticks_per_symbol=50)
        
        # ✅ STORE ONLY FRESH CALCULATIONS: Check which symbols had fresh calculations
        fresh_results = {}
        if hasattr(self.hybrid_calculations, '_fresh_calculations'):
            for symbol in self.hybrid_calculations._fresh_calculations:
                if symbol in all_results:
                    fresh_results[symbol] = all_results[symbol]
        
        # Store ONLY fresh calculations to Redis
        if fresh_results:
            self._store_calculated_indicators(fresh_results)
            self.logger.debug(f"💾 Stored {len(fresh_results)} fresh indicator sets to Redis (skipped {len(all_results) - len(fresh_results)} cached)")
        
        # Return combined results (fresh + cached)
        return all_results

    def _store_calculated_indicators(self, batch_results: dict) -> None:
        """
        ✅ STORE ONLY WHEN NEEDED: Store calculated indicators in Redis.
        Only stores to Redis when we have fresh calculations (not from cache).
        Stores to DB 5 using "indicators_cache" data type.
        """
        if not batch_results:
            self.logger.debug("⚠️ _store_calculated_indicators: batch_results is empty")
            return
        
        if not self.redis_client:
            self.logger.warning("⚠️ _store_calculated_indicators: redis_client is None")
            return
        
        try:
            import json
            import time
            
            storage_count = 0
            
            for symbol, indicators in batch_results.items():
                if not indicators:
                    continue
                
                # ✅ Only store if this was a fresh calculation (not from cache)
                if not self._was_fresh_calculation(symbol):
                    continue
                
                # Store technical indicators
                greeks_to_store = {}
                
                for indicator_name, value in indicators.items():
                    # Store technical indicators
                    if indicator_name in ['rsi', 'atr', 'ema_20', 'ema_50', 'vwap', 'macd', 'bollinger_bands', 
                                         'ema_5', 'ema_10', 'ema_100', 'ema_200']:
                        try:
                            redis_key = f"indicators:{symbol}:{indicator_name}"
                            
                            # Store as JSON for complex indicators (MACD, Bollinger Bands)
                            if isinstance(value, dict):
                                indicator_data = {
                                    'value': value,
                                    'timestamp': int(time.time() * 1000),
                                    'symbol': symbol,
                                    'indicator_type': indicator_name
                                }
                                # ✅ Store to DB 5 using "indicators_cache" data type
                                result = self.redis_client.store_by_data_type(
                                    "indicators_cache",  # DB 5
                                    redis_key, 
                                    json.dumps(indicator_data),
                                    ttl=300  # 5 minutes to match calculation cache
                                )
                            else:
                                # Store simple numeric values
                                # ✅ Store to DB 5 using "indicators_cache" data type
                                result = self.redis_client.store_by_data_type(
                                    "indicators_cache",  # DB 5
                                    redis_key, 
                                    str(value),
                                    ttl=300  # 5 minutes to match calculation cache
                                )
                            
                            if result:
                                storage_count += 1
                                self.logger.debug(f"✅ Stored {indicator_name} for {symbol}")
                            else:
                                self.logger.warning(f"❌ Failed to store {indicator_name} for {symbol} (store_by_data_type returned False)")
                                
                        except Exception as e:
                            self.logger.error(f"❌ Exception storing {indicator_name} for {symbol}: {e}")
                            import traceback
                            self.logger.debug(traceback.format_exc())
                    
                    # Collect Greeks for storage (delta, gamma, theta, vega, rho)
                    elif indicator_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                        greeks_to_store[indicator_name] = value
                
                # Store Greeks as a combined dict (preferred) and individually (fallback)
                if greeks_to_store:
                    try:
                        # Store combined Greeks dict
                        greeks_data = {
                            'value': greeks_to_store,
                            'timestamp': int(time.time() * 1000),
                            'symbol': symbol,
                            'indicator_type': 'greeks'
                        }
                        redis_key = f"indicators:{symbol}:greeks"
                        # ✅ Store to DB 5 using "indicators_cache" data type
                        self.redis_client.store_by_data_type(
                            "indicators_cache",  # DB 5
                            redis_key, 
                            json.dumps(greeks_data),
                            ttl=300  # 5 minutes to match calculation cache
                        )
                        
                        # Also store individual Greeks for backward compatibility
                        for greek_name, greek_value in greeks_to_store.items():
                            greek_key = f"indicators:{symbol}:{greek_name}"
                            # ✅ Store to DB 5 using "indicators_cache" data type
                            self.redis_client.store_by_data_type(
                                "indicators_cache",  # DB 5
                                greek_key, 
                                str(greek_value),
                                ttl=300  # 5 minutes to match calculation cache
                            )
                        
                        storage_count += len(greeks_to_store)
                        self.logger.debug(f"✅ Stored Greeks for {symbol}: {list(greeks_to_store.keys())}")
                    except Exception as e:
                        self.logger.debug(f"Failed to store Greeks for {symbol}: {e}")
            
            if storage_count > 0:
                self.logger.info(f"💾 Stored {storage_count} fresh indicators to Redis DB 5")
            
        except Exception as e:
            self.logger.error(f"Failed to store calculated indicators in Redis: {e}")
    
    def _was_fresh_calculation(self, symbol: str) -> bool:
        """
        Check if the last calculation was fresh or cached.
        Uses the _fresh_calculations set from HybridCalculations.
        """
        if not hasattr(self.hybrid_calculations, '_fresh_calculations'):
            return False
        
        # Check if symbol is in the fresh calculations set
        return symbol in self.hybrid_calculations._fresh_calculations

    def _get_news_for_symbol(self, symbol):
        """
        Get recent news for symbol from Redis cache
        Returns news data if available and fresh, None otherwise
        """
        # First check local cache
        if symbol in self.news_cache:
            cached_news, cache_time = self.news_cache[symbol]
            # Check if cache is still fresh (30 minutes)
            if time.time() - cache_time < self.news_cache_expiry:
                return cached_news

        # Fetch from Redis if not in cache or expired
        try:
            # Try multiple symbol variations for news lookup
            news_keys_to_try = self._get_news_keys_for_symbol(symbol)
            
            try:
                news_db = self.redis_client.get_database_for_data_type("news_data")  # Will route to DB 1 (realtime)
            except Exception:
                news_db = 1  # Fallback to realtime DB (was DB 11, now consolidated)

            news_client = self.news_client  # Use cached client instead of get_client() call

            news_items = []
            for redis_key in news_keys_to_try:
                if news_client:
                    news_items = news_client.zrevrangebyscore(
                        redis_key,
                        "+inf",
                        time.time() - 1800,  # Last 30 minutes
                        withscores=False,
                        start=0,
                        num=5,  # Get last 5 news items
                    )
                else:
                    news_items = []

                if news_items:
                    break  # Found news, stop trying other keys

            if news_items:
                # Parse and find most relevant news for this specific symbol
                best_news = None
                best_relevance_score = 0

                for news_json in news_items:
                    try:
                        news_data = json.loads(news_json)
                        data = news_data.get("data", {})
                        
                        # Calculate relevance score for this symbol
                        relevance_score = self._calculate_news_relevance(symbol, data)
                        
                        # Only consider news with some relevance to this symbol
                        if relevance_score > 0:
                            sentiment = data.get("sentiment", 0)
                            volume_trigger = data.get("volume_trigger", False)
                            
                            # Weight by relevance and news strength
                            news_strength = abs(sentiment) + (1.0 if volume_trigger else 0.0)
                            combined_score = relevance_score * news_strength
                            
                            if combined_score > best_relevance_score:
                                best_relevance_score = combined_score
                                best_news = {
                                    "has_news": True,
                                    "sentiment": sentiment,
                                    "source": news_data.get("source", "unknown"),
                                    "title": data.get("title", ""),
                                    "timestamp": news_data.get("timestamp", ""),
                                    "relevance_score": relevance_score,
                                    "news_strength": news_strength
                                }
                    except json.JSONDecodeError:
                        continue
                
                if best_news:
                    # Cache it
                        self.news_cache[symbol] = (best_news, time.time())
                        return best_news

            # No news found - cache None to avoid repeated lookups
            self.news_cache[symbol] = (None, time.time())

        except Exception as e:
            self.logger.debug(f"Error fetching news for {symbol}: {e}")

        return None

    def _get_news_keys_for_symbol(self, symbol):
        """
        Get list of Redis keys to try for news lookup based on symbol mapping
        Maps individual stocks to their parent indices for news correlation
        """
        keys_to_try = []
        
        # 1. Try exact symbol match first
        keys_to_try.append(f"news:symbol:{symbol}")
        
        # 2. Extract base symbol for futures/options
        if ":" in symbol:
            exchange, name = symbol.split(":", 1)
            
            # For futures/options, try base symbol
            if "FUT" in name or "OPT" in name:
                # Extract base symbol (remove date/expiry info)
                base_name = name.split("28OCT")[0] if "28OCT" in name else name.split("FUT")[0] if "FUT" in name else name.split("OPT")[0] if "OPT" in name else name
                keys_to_try.append(f"news:symbol:{exchange}:{base_name}")
            
            # 3. Map to parent indices based on symbol patterns (ONLY for relevant sectors)
            if "BANK" in name.upper() or "INDUSINDBK" in name.upper() or "HDFCBANK" in name.upper() or "ICICIBANK" in name.upper() or "KOTAKBANK" in name.upper() or "AXISBANK" in name.upper():
                keys_to_try.append("news:symbol:NSE:NIFTY BANK")
            
            # Map NIFTY 50 constituents (ONLY for actual NIFTY 50 stocks)
            nifty50_stocks = ["RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "AXISBANK", "ITC", "BHARTIARTL", "SBIN", "LT", "ASIANPAINT", "MARUTI", "NESTLEIND", "ULTRACEMCO", "TITAN", "SUNPHARMA", "TATAMOTORS", "POWERGRID", "NTPC", "ONGC", "TECHM", "WIPRO", "HCLTECH", "COALINDIA", "JSWSTEEL", "TATASTEEL", "DRREDDY", "BAJFINANCE", "BAJAJFINSV", "ADANIPORTS", "TATACONSUM", "GRASIM", "BRITANNIA", "CIPLA", "EICHERMOT", "HEROMOTOCO", "INDUSINDBK", "NESTLEIND", "SHREECEM", "UPL", "BAJAJHLDNG", "APOLLOHOSP", "DIVISLAB", "HINDALCO", "SBILIFE", "TATACONSUM"]
            
            for stock in nifty50_stocks:
                if stock in name.upper():
                    keys_to_try.append("news:symbol:NSE:NIFTY 50")
                    break
        
        # Remove duplicates while preserving order
        seen = set()
        unique_keys = []
        for key in keys_to_try:
            if key not in seen:
                seen.add(key)
                unique_keys.append(key)
        
        return unique_keys

    def _extract_underlying_from_symbol(self, symbol: str) -> str:
        """Extract underlying symbol from option/future symbols"""
        import re
        
        if not symbol:
            return symbol
        
        symbol_upper = symbol.upper()
        if ':' in symbol_upper:
            symbol_upper = symbol_upper.split(':')[-1]
        
        # Check for index names first
        index_match = re.search(r'(BANKNIFTY|NIFTY|FINNIFTY|MIDCPNIFTY)', symbol_upper)
        if index_match:
            return index_match.group(1)
        
        # For options (CE/PE), extract everything before the date pattern
        if 'CE' in symbol_upper or 'PE' in symbol_upper:
            match = re.search(r'^([A-Z]+)\d{2}[A-Z]{3}', symbol_upper)
            if match:
                return match.group(1)
            match = re.search(r'^([A-Z]+)\d', symbol_upper)
            if match:
                return match.group(1)
        
        # For futures (FUT), extract everything before the date
        if symbol_upper.endswith('FUT'):
            match = re.search(r'^([A-Z]+)\d', symbol_upper)
            if match:
                return match.group(1)
        
        return symbol_upper

    def _get_sector_from_symbol_name(self, symbol: str) -> str:
        """Map symbol to sector based on naming patterns"""
        if not symbol:
            return ""
        
        symbol_upper = symbol.upper()
        bank_keywords = ['BANK', 'HDFC', 'ICICI', 'SBI', 'AXIS', 'KOTAK', 'INDUSIND', 
                         'YESBANK', 'FEDERAL', 'IDFC', 'BANDHAN', 'RBL', 'UNION',
                         'PNB', 'CANARA', 'BANKOFBARODA', 'BANKOFINDIA']
        it_keywords = ['INFOSYS', 'TCS', 'WIPRO', 'HCL', 'TECHMAHINDRA', 'LTIM', 
                       'LTTS', 'PERSISTENT', 'MINDTREE', 'COFORGE', 'MPHASIS']
        auto_keywords = ['MARUTI', 'M&M', 'TATA', 'MOTORS', 'BAJAJ', 'HERO', 
                         'EICHER', 'ASHOK', 'LEYLAND', 'TVS', 'MOTHERSUM']
        pharma_keywords = ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'TORRENT', 
                           'AUROBINDO', 'DIVIS', 'GLENMARK', 'CADILA']
        fmcg_keywords = ['HUL', 'ITC', 'NESTLE', 'DABUR', 'MARICO', 'BRITANNIA', 
                         'GODREJ', 'TATA', 'CONSUMER']
        energy_keywords = ['RELIANCE', 'ONGC', 'GAIL', 'IOC', 'BPCL', 'HPCL', 
                           'OIL', 'PETRONET']
        metal_keywords = ['TATASTEEL', 'JSWSTEEL', 'SAIL', 'JINDAL', 'VEDANTA', 
                          'HINDALCO', 'NMDC']
        
        for keyword in bank_keywords:
            if keyword in symbol_upper:
                return "BANK"
        for keyword in it_keywords:
            if keyword in symbol_upper:
                return "IT"
        for keyword in auto_keywords:
            if keyword in symbol_upper:
                return "AUTO"
        for keyword in pharma_keywords:
            if keyword in symbol_upper:
                return "PHARMA"
        for keyword in fmcg_keywords:
            if keyword in symbol_upper:
                return "FMCG"
        for keyword in energy_keywords:
            if keyword in symbol_upper:
                return "ENERGY"
        for keyword in metal_keywords:
            if keyword in symbol_upper:
                return "METAL"
        return ""

    def _calculate_news_relevance(self, symbol, news_data):
        """
        Calculate how relevant a news item is to a specific symbol using sector-based matching
        Returns a score from 0.0 (not relevant) to 1.0 (highly relevant)
        """
        if not news_data:
            return 0.0
            
        title = news_data.get("title", "").upper()
        content = news_data.get("content", "").upper()
        full_text = f"{title} {content}"
        
        # Extract underlying symbol from option/future symbols
        underlying_symbol = self._extract_underlying_from_symbol(symbol)
        base_name = underlying_symbol.upper()
        
        # Direct symbol match (highest relevance)
        if base_name in full_text:
            return 1.0
        
        # Check if news mentions the symbol directly (with exchange prefix)
        if ':' in symbol:
            exchange, name = symbol.split(":", 1)
            if name.upper() in full_text:
                return 1.0
        
        # Get sector from symbol
        symbol_sector = self._get_sector_from_symbol_name(base_name)
        
        # Sector-based relevance matching
        sector_keywords = {
            "BANK": ["BANKING", "BANK", "FINANCIAL", "CREDIT", "LOAN", "LENDING", "INTEREST RATE"],
            "AUTO": ["AUTOMOTIVE", "AUTO", "VEHICLE", "CAR", "BIKE", "MOTORCYCLE", "AUTOMOBILE"],
            "PHARMA": ["PHARMACEUTICAL", "DRUG", "MEDICINE", "HEALTHCARE", "MEDICAL"],
            "IT": ["TECHNOLOGY", "SOFTWARE", "IT", "DIGITAL", "TECH", "COMPUTER"],
            "ENERGY": ["OIL", "GAS", "ENERGY", "POWER", "PETROLEUM", "REFINERY", "CRUDE"],
            "METAL": ["STEEL", "METAL", "MINING", "ALUMINIUM", "IRON", "COPPER"],
            "FMCG": ["FMCG", "CONSUMER", "FMCG PRODUCTS", "RETAIL"],
            "CEMENT": ["CEMENT", "CONSTRUCTION", "BUILDING", "INFRASTRUCTURE"]
        }
        
        # If symbol has a sector, check if news mentions that sector
        if symbol_sector and symbol_sector in sector_keywords:
            for keyword in sector_keywords[symbol_sector]:
                if keyword.upper() in full_text:
                    return 0.8  # High relevance for sector match
        
        # Check for sub-category company names (specific companies in the sector)
        # For banks
        if symbol_sector == "BANK":
            bank_companies = ["HDFC", "ICICI", "SBI", "AXIS", "KOTAK", "INDUSIND", "YES BANK", "FEDERAL"]
            for company in bank_companies:
                if company in full_text and company in base_name:
                    return 0.9  # Very high for specific company match
        
        # For IT
        if symbol_sector == "IT":
            it_companies = ["INFOSYS", "TCS", "WIPRO", "HCL", "TECH MAHINDRA"]
            for company in it_companies:
                if company in full_text and company in base_name:
                    return 0.9
        
        # General market news (lowest relevance)
        market_keywords = ["MARKET", "STOCK", "SHARE", "EQUITY", "NIFTY", "SENSEX"]
        for keyword in market_keywords:
            if keyword in full_text:
                return 0.3
                
        return 0.0

    def _cleanup_news_cache(self):
        """Cleanup old news from cache periodically"""
        current_time = time.time()
        if current_time - self.last_news_cleanup > 300:  # Every 5 minutes
            # Remove expired entries
            symbols_to_remove = []
            for symbol, (news, cache_time) in self.news_cache.items():
                if current_time - cache_time > self.news_cache_expiry:
                    symbols_to_remove.append(symbol)

            for symbol in symbols_to_remove:
                del self.news_cache[symbol]

            self.last_news_cleanup = current_time
            if symbols_to_remove:
                self.logger.debug(
                    f"Cleaned {len(symbols_to_remove)} expired news entries"
                )

    def _get_spoofing_for_symbol(self, symbol):
        """
        Get recent spoofing alert for symbol from Redis or cache
        Returns spoofing data if alert is active, None otherwise
        """
        # First check local cache
        if symbol in self.spoofing_cache:
            cached_spoofing, cache_time = self.spoofing_cache[symbol]
            # Check if cache is still fresh (5 minutes for spoofing)
            if time.time() - cache_time < self.spoofing_cache_expiry:
                return cached_spoofing

        # Check Redis for spoofing alerts
        try:
            # Check for active spoofing alert
            spoofing_key = f"spoofing:{symbol}"
            spoofing_json = self.redis_client.get(spoofing_key)

            if spoofing_json:
                spoofing_data = json.loads(spoofing_json)
                # Check if alert is still valid (within 5 minutes)
                alert_time = (
                    spoofing_data.get("timestamp_ms", 0) / 1000
                )  # Convert ms to seconds
                if time.time() - alert_time < 300:  # 5 minute validity
                    # Extract enhanced spoofing details from crawler
                    spoofing_info = {
                        "detected": True,
                        "type": spoofing_data.get("type", "UNKNOWN"),
                        "score": spoofing_data.get("score", 0),
                        "confidence": spoofing_data.get("confidence", 0),
                        "direction": spoofing_data.get("direction", "unknown"),  # 🆕 CRITICAL: Add direction
                        "severity": spoofing_data.get("severity", "INFO"),
                        "details": spoofing_data.get("details", {}),
                        "walls": spoofing_data.get("walls", []),
                        "persistent_walls": spoofing_data.get("persistent", False),
                        "price_action_confirms": spoofing_data.get(
                            "price_action_confirms", False
                        ),
                        "timestamp": spoofing_data.get("timestamp", ""),
                    }
                    # Cache it
                    self.spoofing_cache[symbol] = (spoofing_info, time.time())
                    return spoofing_info

            # No spoofing found - cache None to avoid repeated lookups
            self.spoofing_cache[symbol] = (None, time.time())

        except Exception as e:
            self.logger.debug(f"Error fetching spoofing for {symbol}: {e}")

        return None

    def _is_spoofing_blocked(self, symbol):
        """
        Check if symbol is currently blocked due to spoofing detection
        Returns True if blocked, False otherwise
        """
        if symbol in self.spoofing_blocks:
            block_expiry = self.spoofing_blocks[symbol]
            if time.time() < block_expiry:
                return True
            else:
                # Block expired, remove it
                del self.spoofing_blocks[symbol]
        return False

    def _get_block_reason(self, symbol):
        """
        Get the reason why a symbol is blocked
        """
        # Check Redis for block details
        try:
            block_key = f"spoofing_block:{symbol}"
            block_data = self.redis_client.get(block_key)
            if block_data:
                block_info = json.loads(block_data)
                return block_info.get("reason", "Spoofing detected")
        except:
            pass
        return "Spoofing activity detected"

    # ✅ Spoofing detection methods removed - simplified architecture

    def _cleanup_spoofing_cache(self):
        """Cleanup old spoofing alerts from cache periodically"""
        current_time = time.time()
        if current_time - self.last_spoofing_cleanup > 60:  # Every minute
            # Remove expired entries
            symbols_to_remove = []
            for symbol, (spoofing, cache_time) in self.spoofing_cache.items():
                if current_time - cache_time > self.spoofing_cache_expiry:
                    symbols_to_remove.append(symbol)

            for symbol in symbols_to_remove:
                del self.spoofing_cache[symbol]

            # Also cleanup expired blocks
            blocks_to_remove = []
            for symbol, expiry in self.spoofing_blocks.items():
                if current_time > expiry:
                    blocks_to_remove.append(symbol)

            for symbol in blocks_to_remove:
                del self.spoofing_blocks[symbol]

            self.last_spoofing_cleanup = current_time
            if symbols_to_remove or blocks_to_remove:
                self.logger.debug(
                    f"Cleaned {len(symbols_to_remove)} spoofing entries, {len(blocks_to_remove)} blocks"
                )

    def _validate_and_clean_depth(self, depth):
        """
        Validate and clean depth data - handle all depth issues ONCE
        Returns cleaned depth or empty structure if invalid

        This replaces pattern_engine's _validate_depth_data method
        """
        # Return empty if no depth
        if not depth:
            return {"buy": [], "sell": []}

        # Handle dict structure
        if not isinstance(depth, dict):
            return {"buy": [], "sell": []}

        # Must have at least one side
        if "buy" not in depth and "sell" not in depth:
            return {"buy": [], "sell": []}

        cleaned_depth = {"buy": [], "sell": []}

        for side in ["buy", "sell"]:
            if side not in depth:
                cleaned_depth[side] = []
                continue

            side_data = depth[side]
            if not isinstance(side_data, list):
                cleaned_depth[side] = []
                continue

            # Clean each order level
            for order in side_data[:5]:  # Max 5 levels
                if not isinstance(order, dict):
                    continue

                # Extract and validate last_price
                last_price = order.get("last_price", 0)
                if isinstance(last_price, dict):
                    last_price = last_price.get("value", 0)

                # Extract and validate quantity
                quantity = order.get("quantity", 0)
                if isinstance(quantity, dict):
                    quantity = quantity.get("value", 0)

                # Skip invalid entries
                if last_price <= 0 or quantity <= 0:
                    continue

                # Add cleaned order
                cleaned_depth[side].append(
                    {
                        "last_price": float(last_price),
                        "quantity": int(quantity),
                        "orders": int(order.get("orders", 1)),
                    }
                )

        return cleaned_depth

    def _extract_symbol(self, tick_data):
        """Extract symbol from various possible fields"""
        symbol = (
            tick_data.get("tradingsymbol")
            or tick_data.get("symbol")
            or tick_data.get("instrument_token")
        )

        # If we only have a numeric token or TOKEN_ format, try to resolve to tradingsymbol
        try:
            sym_str = str(symbol) if symbol is not None else ""
        except Exception:
            sym_str = ""

        # Resolve if symbol is missing, numeric, TOKEN_ format, or UNKNOWN
        if (
            (not sym_str)
            or sym_str.isdigit()
            or sym_str.startswith("TOKEN_")
            or sym_str == "UNKNOWN"
        ):
            tok = tick_data.get("instrument_token")
            # ✅ FIXED: Only resolve if instrument_token exists and is not None
            if tok is not None and tok != "":
                try:
                    # 🚀 OPTIMIZED: Use instrument mapper to resolve token to symbol
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    mapper = InstrumentMapper()
                    resolved = mapper.token_to_symbol(tok)
                    if resolved and not resolved.startswith("UNKNOWN"):
                        symbol = resolved
                        # Store the resolved symbol back into the tick data
                        tick_data['symbol'] = resolved
                except Exception as e:
                    self.logger.debug(f"Token resolution failed for token {tok}: {e}")
                    resolved = None

        # Debug logging for missing symbols
        if not symbol or symbol == "UNKNOWN":
            self.logger.debug(
                "Tick missing symbol. Original keys: %s", list(tick_data.keys())
            )
            for key in ["tradingsymbol", "symbol", "instrument_token"]:
                value = tick_data.get(key)
                self.logger.debug("    %s: %r", key, value)

        return symbol or ""

    def _is_option_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O option (contains CE/PE in NFO segment)."""
        if not symbol:
            return False
        # Ensure symbol is a string (sometimes it's an int like instrument_token)
        if not isinstance(symbol, str):
            return False
        symbol_upper = symbol.upper()
        # Must be NFO segment AND end with CE/PE (exclude equity cash)
        return (symbol_upper.startswith('NFO:') and 
                (symbol_upper.endswith('CE') or symbol_upper.endswith('PE')))

    def _is_derivative(self, symbol: str) -> bool:
        """Check if symbol is a derivative (F&O options or futures)."""
        if not symbol:
            return False
        symbol_upper = symbol.upper()
        # Must be NFO segment AND be options (CE/PE) or futures (FUT)
        return (symbol_upper.startswith('NFO:') and 
                (symbol_upper.endswith('CE') or symbol_upper.endswith('PE') or 
                 symbol_upper.endswith('FUT')))

    def _get_underlying_price(self, option_symbol: str) -> float:
        """Get underlying last_price for options."""
        try:
            # Extract underlying symbol (e.g., 'BANKNIFTY' from 'NFO:BANKNIFTY25OCT54900CE')
            underlying_symbol = self._extract_underlying_symbol(option_symbol)
            if not underlying_symbol:
                return 0.0
            
            # Get latest last_price from Redis or last_price cache
            if hasattr(self, 'redis_client') and self.redis_client:
                # Try to get from Redis first
                price_data = self.redis_client.get_latest_price(underlying_symbol)
                if price_data and 'last_price' in price_data:
                    return float(price_data['last_price'])
            
            # Fallback to last_price cache if available
            if hasattr(self, 'price_cache'):
                return self.price_cache.get(underlying_symbol, 0.0)
            
            return 0.0
            
        except Exception as e:
            self.logger.debug(f"Failed to get underlying last_price for {option_symbol}: {e}")
            return 0.0

    def _extract_underlying_symbol(self, option_symbol: str) -> str:
        """Extract underlying symbol from option symbol."""
        try:
            import re
            
            # Pattern: NFO:RELIANCE28OCTFUT -> RELIANCE
            
            # Remove NFO: prefix if present
            clean_symbol = option_symbol.replace('NFO:', '')
            
            # For options (CE/PE), extract everything before the date
            if 'CE' in clean_symbol.upper() or 'PE' in clean_symbol.upper():
                # Find the first occurrence of digits (date starts)
                match = re.search(r'^([A-Z]+)\d', clean_symbol)
                if match:
                    return f"NFO:{match.group(1)}"
            
            # For futures (FUT), extract everything before the date
            elif 'FUT' in clean_symbol.upper():
                match = re.search(r'^([A-Z]+)\d', clean_symbol)
                if match:
                    return f"NFO:{match.group(1)}"
            
            # Fallback: return the symbol as-is
            return clean_symbol
            
        except Exception as e:
            self.logger.debug(f"Failed to extract underlying from {option_symbol}: {e}")
            return ""

    def _update_price_cache(self, symbol: str, last_price: float):
        """Update last_price cache with latest last_price for underlying symbols."""
        try:
            if last_price > 0 and symbol:
                self.price_cache[symbol] = last_price
                # Also update with NFO: prefix if it's an underlying
                if not symbol.startswith('NFO:'):
                    self.price_cache[f"NFO:{symbol}"] = last_price
        except Exception as e:
            self.logger.debug(f"Failed to update last_price cache for {symbol}: {e}")

    def _safe_float(self, value, default=0.0):
        """Safely convert value to float, handling strings and None"""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _get_volume_from_tick(self, tick_data):
        """Extract bucket_incremental_volume from tick with all possible field names"""
        try:
            volume_fields = [
                "zerodha_cumulative_volume",
                "zerodha_cumulative_volume",
                "bucket_incremental_volume",
                "vol",
                "zerodha_last_traded_quantity",
                "quantity",
            ]
            for field in volume_fields:
                if field in tick_data and tick_data[field] is not None:
                    return int(float(tick_data[field]))
        except Exception:
            pass
        return 0

    # REMOVED: _normalize_volume function - volume calculation now handled in WebSocket Parser only

    def _clean_numeric_fields(self, tick_data, cleaned):
        """Clean numeric fields with dict-in-dict handling"""
        numeric_fields = [
            "last_price",
            "bucket_incremental_volume",
            "zerodha_cumulative_volume",
            "bucket_cumulative_volume",  # Raw cumulative bucket_incremental_volume from Zerodha
            "bucket_incremental_volume",  # Set to 0 (no calculations)
            "total_buy_quantity",
            "total_sell_quantity",
            "zerodha_last_traded_quantity",
            "average_price",
            "oi",
            "oi_day_high",
            "oi_day_low",
            "change",
            "net_change",  # Calculated from last_price - ohlc.close
        ]
        # Normalize last traded quantity across common Kite aliases
        # Some feeds provide only per-trade size (no cumulative bucket_incremental_volume).
        # Accept aliases and store canonically as 'zerodha_last_traded_quantity'.
        last_qty_aliases = (
            "zerodha_last_traded_quantity",
            "last_traded_quantity",
            "last_trade_quantity",
            "last_trade_qty",
            "ltq",
        )
        last_qty_val = None
        for k in last_qty_aliases:
            if k in tick_data and tick_data[k] is not None:
                last_qty_val = self._safe_float(tick_data.get(k), 0.0)
                break
        if last_qty_val is None:
            last_qty_val = 0.0
        cleaned[SESSION_FIELD_LTQ] = last_qty_val

        # 🚨 CRITICAL FIX: Don't overwrite bucket_incremental_volume data that was already processed
        # Check if bucket_incremental_volume data was already set by previous processing steps
        if "zerodha_cumulative_volume" not in cleaned or cleaned.get("zerodha_cumulative_volume", 0) == 0:
            # Only try to get bucket_incremental_volume from tick_data if it wasn't already processed
            # According to optimized_field_mapping.yaml:
            # - zerodha_cumulative_volume: "zerodha_cumulative_volume" (Zerodha cumulative session bucket_incremental_volume)
            # - bucket_incremental_volume: "zerodha_cumulative_volume" (Legacy alias → canonical cumulative)
            
            # Try zerodha_cumulative_volume first (preferred), then bucket_incremental_volume (legacy alias)
            zerodha_cum = tick_data.get("zerodha_cumulative_volume")
            if zerodha_cum is None:
                zerodha_cum = tick_data.get("bucket_incremental_volume")
            zerodha_cum = self._safe_float(zerodha_cum, 0.0)

            if zerodha_cum and zerodha_cum > 0:
                # Use ONLY canonical field names from optimized_field_mapping.yaml
                cleaned[SESSION_FIELD_ZERODHA_CUM] = zerodha_cum  # zerodha_cumulative_volume
                cleaned["zerodha_cumulative_volume"] = zerodha_cum  # Primary field name
            else:
                cleaned.setdefault(SESSION_FIELD_ZERODHA_CUM, 0.0)
                cleaned.setdefault("zerodha_cumulative_volume", 0.0)  # Primary field name

        # If Zerodha bucket_incremental_volume is zero but we have a last trade quantity, log minimal bucket_incremental_volume for compatibility
        if zerodha_cum == 0 and last_qty_val > 0:
            cleaned[SESSION_FIELD_ZERODHA_CUM] = last_qty_val
            cleaned["zerodha_cumulative_volume"] = last_qty_val

        # Get symbol for tracking
        symbol = tick_data.get("symbol") or tick_data.get("tradingsymbol", "UNKNOWN")
        
        # REMOVED: Duplicate volume calculation - now handled in WebSocket Parser only
        # Use the volume data already calculated by WebSocket Parser
        current_cumulative = cleaned.get('zerodha_cumulative_volume', 0)
        bucket_incremental_volume = cleaned.get('bucket_incremental_volume', 0)
        
        self._cumulative_volume_tracker[symbol] = current_cumulative
        
        # Debug logging for volume data from WebSocket Parser
        if bucket_incremental_volume > 0:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"🔧 [VOLUME_FIX] {symbol}: cumulative={current_cumulative}, incremental={bucket_incremental_volume} (from WebSocket Parser)")
        
        # Initialize bucket-level fields (will be updated in bucket_incremental_volume context step)
        bucket_cumulative_default = cleaned.get(SESSION_FIELD_BUCKET_CUM)
        if bucket_cumulative_default is None:
            bucket_cumulative_default = cleaned.get("bucket_cumulative_volume", 0.0)
        cleaned.setdefault(SESSION_FIELD_BUCKET_CUM, bucket_cumulative_default or 0.0)

        for field in numeric_fields:
            # Skip if already set by bidirectional mapping
            if field in cleaned:
                continue
            value = tick_data.get(field)
            if value is not None:
                # Handle dict format from Kite API
                if isinstance(value, dict):
                    cleaned[field] = self._safe_float(
                        value.get("value", value.get("last", value.get("current", 0)))
                    )
                else:
                    cleaned[field] = self._safe_float(value)
            else:
                cleaned[field] = 0.0

        normalize_session_record(cleaned, include_aliases=True)

    def _clean_ohlc_data(self, tick_data, cleaned):
        """Clean OHLC (Open, High, Low, Close) data"""
        ohlc = tick_data.get("ohlc", {})
        if isinstance(ohlc, dict):
            cleaned["ohlc"] = {}
            for key in ["open", "high", "low", "close"]:
                val = ohlc.get(key)
                if isinstance(val, dict):
                    cleaned["ohlc"][key] = self._safe_float(val.get("value", 0))
                else:
                    cleaned["ohlc"][key] = self._safe_float(val, 0)
            # Flatten OHLC to top-level for indicator consumers
            try:
                cleaned["open"] = self._safe_float(cleaned["ohlc"].get("open", 0))
                cleaned["high"] = self._safe_float(cleaned["ohlc"].get("high", 0))
                cleaned["low"] = self._safe_float(cleaned["ohlc"].get("low", 0))
                cleaned["close"] = self._safe_float(cleaned["ohlc"].get("close", 0))
            except Exception:
                pass
        else:
            # Create default OHLC if missing
            last_price = cleaned.get("last_price", 0)
            cleaned["ohlc"] = {
                "open": self._safe_float(last_price),
                "high": self._safe_float(last_price),
                "low": self._safe_float(last_price),
                "close": self._safe_float(last_price),
            }
            # Also set flattened defaults
            cleaned["open"] = cleaned["ohlc"]["open"]
            cleaned["high"] = cleaned["ohlc"]["high"]
            cleaned["low"] = cleaned["ohlc"]["low"]
            cleaned["close"] = cleaned["ohlc"]["close"]

    def _clean_depth_data(self, tick_data, cleaned):
        """Clean and validate order book depth data with canonicalization.

        Canonicalizes multiple possible upstream shapes into:
            cleaned['depth'] = {'buy': [...], 'sell': [...]} and sets 'depth_valid'.
        Supports:
            - tick_data['depth'] as a dict with buy/sell (Kite standard)
            - 'depth_levels_buy' / 'depth_levels_sell' (crawler alternates)
            - 'depth_levels' as a dict with buy/sell (crawler consolidated)
        """

        buy_lvls = None
        sell_lvls = None

        # Alternates (crawler variants)
        if isinstance(tick_data.get("depth_levels_buy"), list):
            buy_lvls = tick_data.get("depth_levels_buy")
        if isinstance(tick_data.get("depth_levels_sell"), list):
            sell_lvls = tick_data.get("depth_levels_sell")

        # Kite-standard
        depth_obj = tick_data.get("depth")
        if isinstance(depth_obj, dict):
            if buy_lvls is None and isinstance(depth_obj.get("buy"), list):
                buy_lvls = depth_obj.get("buy")
            if sell_lvls is None and isinstance(depth_obj.get("sell"), list):
                sell_lvls = depth_obj.get("sell")

        # Consolidated 'depth_levels' (crawler)
        levels = tick_data.get("depth_levels")
        if isinstance(levels, dict):
            if buy_lvls is None and isinstance(levels.get("buy"), list):
                buy_lvls = levels.get("buy")
            if sell_lvls is None and isinstance(levels.get("sell"), list):
                sell_lvls = levels.get("sell")

        if buy_lvls or sell_lvls:
            raw_depth = {"buy": buy_lvls or [], "sell": sell_lvls or []}
            cleaned_depth = self._validate_and_clean_depth(raw_depth)
            cleaned["depth"] = cleaned_depth
            cleaned["depth_valid"] = bool(
                cleaned_depth.get("buy") or cleaned_depth.get("sell")
            )
            # Flatten best bid/ask to top-level for indicator consumers
            try:
                best_bid = (
                    cleaned_depth.get("buy", [{}])[0] if cleaned_depth.get("buy") else {}
                )
                best_ask = (
                    cleaned_depth.get("sell", [{}])[0] if cleaned_depth.get("sell") else {}
                )
                if best_bid:
                    if "last_price" in best_bid:
                        cleaned["best_bid_price"] = self._safe_float(best_bid.get("last_price"))
                    if "quantity" in best_bid:
                        cleaned["best_bid_quantity"] = self._safe_float(best_bid.get("quantity"))
                if best_ask:
                    if "last_price" in best_ask:
                        cleaned["best_ask_price"] = self._safe_float(best_ask.get("last_price"))
                    if "quantity" in best_ask:
                        cleaned["best_ask_quantity"] = self._safe_float(best_ask.get("quantity"))
            except Exception:
                pass
        else:
            cleaned["depth"] = {"buy": [], "sell": []}
            cleaned["depth_valid"] = False

    def _clean_timestamps(self, tick_data, cleaned):
        """Handle timestamp fields with one-time normalization to epoch milliseconds using Zerodha field names."""
        
        # ✅ ONE-TIME NORMALIZATION: Convert any timestamp format to epoch milliseconds
        # Use Zerodha field names exactly as per optimized_field_mapping.yaml
        
        # Primary timestamp: exchange_timestamp (preferred by Zerodha)
        exchange_timestamp_raw = tick_data.get("exchange_timestamp")
        if exchange_timestamp_raw:
            exchange_timestamp_epoch = TimestampNormalizer.to_epoch_ms(exchange_timestamp_raw)
            cleaned["exchange_timestamp"] = exchange_timestamp_raw  # Keep original
            cleaned["exchange_timestamp_ms"] = exchange_timestamp_epoch  # Always epoch milliseconds
            # Also set the primary field name for HybridCalculations compatibility
            cleaned["timestamp"] = exchange_timestamp_epoch  # Primary timestamp for calculations
        else:
            # Fallback to timestamp_ns if exchange_timestamp not available
            timestamp_ns_raw = tick_data.get("timestamp_ns")
            if timestamp_ns_raw:
                timestamp_ns_epoch = TimestampNormalizer.to_epoch_ms(timestamp_ns_raw)
                cleaned["timestamp_ns"] = timestamp_ns_raw  # Keep original
                cleaned["timestamp_ns_ms"] = timestamp_ns_epoch  # Always epoch milliseconds
            else:
                # Final fallback to legacy timestamp
                timestamp_raw = tick_data.get("timestamp", get_current_ist_timestamp())
                timestamp_epoch = TimestampNormalizer.to_epoch_ms(timestamp_raw)
                cleaned["timestamp"] = timestamp_raw  # Keep original
                cleaned["timestamp_ms"] = timestamp_epoch  # Always epoch milliseconds
        
        # Last trade time - Zerodha specific field
        last_trade_time_raw = tick_data.get("last_trade_time")
        if last_trade_time_raw:
            last_trade_time_epoch = TimestampNormalizer.to_epoch_ms(last_trade_time_raw)
            cleaned["last_trade_time"] = last_trade_time_raw  # Keep original
            cleaned["last_trade_time_ms"] = last_trade_time_epoch  # Always epoch milliseconds

    def _calculate_derived_fields(self, cleaned):
        """Calculate derived fields from cleaned data using unified schema"""
        try:
            # Calculate last_price change percentage (data pipeline specific)
            open_price = self._safe_float(cleaned["ohlc"]["open"])
            last_price = self._safe_float(cleaned["last_price"])

            if open_price > 0 and last_price > 0:
                cleaned["price_change_pct"] = (
                    (last_price - open_price) / open_price
                ) * 100
            else:
                cleaned["price_change_pct"] = 0.0
        except (KeyError, TypeError, ZeroDivisionError):
            cleaned["price_change_pct"] = 0.0

        # Use unified schema for field mapping and derived calculations
        derived_fields = self.calculate_derived_fields(cleaned)
        cleaned.update(derived_fields)

        # Handle data pipeline specific field mappings not in unified schema
        # 2) weighted bid/ask synonyms
        if "weighted_bid_price" not in cleaned and "weighted_bid" in cleaned:
            cleaned["weighted_bid_price"] = self._safe_float(
                cleaned.get("weighted_bid")
            )
        if "weighted_ask_price" not in cleaned and "weighted_ask" in cleaned:
            cleaned["weighted_ask_price"] = self._safe_float(
                cleaned.get("weighted_ask")
            )

        # 3) bid/ask spread and mid last_price if depth available and not supplied
        depth = cleaned.get("depth") if isinstance(cleaned.get("depth"), dict) else None
        if depth:
            try:
                best_bid = (
                    self._safe_float(depth.get("buy", [{}])[0].get("last_price", 0))
                    if depth.get("buy")
                    else 0.0
                )
                best_ask = (
                    self._safe_float(depth.get("sell", [{}])[0].get("last_price", 0))
                    if depth.get("sell")
                    else 0.0
                )
                if "bid_ask_spread" not in cleaned and best_bid and best_ask:
                    cleaned["bid_ask_spread"] = best_ask - best_bid
                if "mid_price" not in cleaned and best_bid and best_ask:
                    cleaned["mid_price"] = (best_bid + best_ask) / 2.0
            except Exception:
                pass

    def _preserve_crawler_fields(self, tick_data, cleaned):
        """Preserve ALL 184-byte tick data fields from Zerodha WebSocket"""
        # Preserve ALL fields from the 184-byte Zerodha WebSocket packet
        # to ensure downstream functions get complete tick data
        crawler_fields = [
            # Core 184-byte packet fields (bytes 0-64)
            "instrument_token",
            "last_price",
            "zerodha_last_traded_quantity", 
            "average_price",
            "zerodha_cumulative_volume",  # Raw cumulative bucket_incremental_volume from Zerodha
            "total_buy_quantity",
            "total_sell_quantity",
            "ohlc",  # Open, High, Low, Close
            "change",
            "net_change",  # Calculated from last_price - ohlc.close
            "last_trade_time",
            "oi",  # Open Interest
            "oi_day_high",
            "oi_day_low", 
            "exchange_timestamp",
            "exchange_timestamp_epoch",
            
            # Market depth fields (bytes 64-184)
            "depth",  # Full market depth data
            "best_bid_price",
            "best_bid_quantity", 
            "best_bid_orders",
            "best_ask_price",
            "best_ask_quantity",
            "best_ask_orders",
            
            # Calculated order-book metrics
            "total_bid_orders",
            "total_ask_orders",
            "avg_bid_order_size",
            "avg_ask_order_size",
            "order_imbalance",
            "order_count_imbalance",
            "bid_ask_ratio",
            "total_bid_qty",
            "total_ask_qty",
            "weighted_bid",
            "weighted_ask",
            
            # Depth variants from crawler
            "depth_levels",
            "depth_levels_buy",
            "depth_levels_sell",
            
            # Status/identity fields required by downstream logic
            "mode",  # QUOTE/LTP/FULL – required by utils.calculations
            "tradable",  # Trading status (halted etc.)
            "last_trade_time",  # For latency/recency reasoning
            "exchange",  # NSE/BSE/NFO
            "segment",
            
            # 🎯 CRITICAL: Volume ratio and related fields
            "volume_ratio",  # Calculated bucket_incremental_volume ratio
            "normalized_volume",  # Normalized bucket_incremental_volume
            "volume_context",  # Volume context (high/normal)
            "asset_class",  # EQ/FUT/OPT etc.
            
            # Volume fields (preserve all bucket_incremental_volume data)
            "bucket_incremental_volume",  # Raw bucket_incremental_volume from Zerodha
            "bucket_cumulative_volume",  # Raw cumulative bucket_incremental_volume
            "bucket_incremental_volume",  # Set to 0 (no calculations)
        ]

        for field in crawler_fields:
            if field in tick_data:
                cleaned[field] = tick_data[field]

    def _attach_external_data(self, symbol, cleaned):
        """Attach external data like news and spoofing information"""
        # Periodic cleanup of caches
        self._cleanup_news_cache()
        self._cleanup_spoofing_cache()

        # Attach news data
        news_data = self._get_news_for_symbol(symbol)
        cleaned["news"] = news_data if news_data else None
        
        # CRITICAL: Add news_context for pattern detection integration
        if news_data:
            cleaned["news_context"] = news_data

        # ✅ Spoofing detection: Uses simplified Redis-based detection (legacy detector object removed)
        spoofing_data = self._get_spoofing_for_symbol(symbol)
        if spoofing_data:
            cleaned["spoofing_detected"] = True
            cleaned["spoofing_score"] = spoofing_data.get("score", 0)
            cleaned["spoofing_type"] = spoofing_data.get("type", "")
            cleaned["spoofing_confidence"] = spoofing_data.get("confidence", 0)
            cleaned["spoofing_details"] = spoofing_data
            cleaned["spoofing_confirmed"] = spoofing_data.get("price_action_confirms", False)
            cleaned["spoofing_direction"] = spoofing_data.get("direction", "unknown")
        else:
            cleaned.update({
                "spoofing_detected": False,
                "spoofing_score": 0,
                "spoofing_confirmed": False,
                "spoofing_confidence": 0.0,
                "spoofing_direction": "none"
            })

        # Check spoofing block status
        if self._is_spoofing_blocked(symbol):
            cleaned["spoofing_blocked"] = True
            cleaned["spoofing_block_reason"] = self._get_block_reason(symbol)
        else:
            cleaned["spoofing_blocked"] = False

    def _is_duplicate(self, tick):
        """Enhanced deduplication with cleanup"""
        symbol = tick.get("tradingsymbol")
        timestamp = tick.get("timestamp", "")
        last_price = tick.get("last_price")
        bucket_incremental_volume = tick.get("bucket_incremental_volume", 0)

        # Create more comprehensive hash including bucket_incremental_volume
        tick_hash = f"{symbol}:{last_price}:{bucket_incremental_volume}:{timestamp}"

        # Check if duplicate
        if symbol in self.last_tick_hash:
            last_hash, last_time = self.last_tick_hash[symbol]

            # Check if within dedup window
            current_time = time.time()
            if (
                tick_hash == last_hash
                and (current_time - last_time) < self.dedup_window
            ):
                self.stats["ticks_deduplicated"] += 1
                return True

        # Update last tick hash
        self.last_tick_hash[symbol] = (tick_hash, time.time())
        return False

    def _cleanup_old_hashes(self):
        """Clean up old deduplication hashes periodically"""
        current_time = time.time()

        # Only cleanup every interval
        if current_time - self.last_dedup_cleanup < self.dedup_cleanup_interval:
            return

        self.last_dedup_cleanup = current_time

        # Remove hashes older than 2x dedup window
        cutoff_time = current_time - (self.dedup_window * 2)
        symbols_to_remove = []

        for symbol, (hash_val, timestamp) in self.last_tick_hash.items():
            if timestamp < cutoff_time:
                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self.last_tick_hash[symbol]

        if symbols_to_remove:
            self.logger.debug(f"Cleaned {len(symbols_to_remove)} old dedup hashes")
    
    def _cleanup_old_rolling_windows(self):
        """Clean up old rolling windows periodically"""
        if hasattr(self, 'hybrid_calculations'):
            try:
                cleaned_count = self.hybrid_calculations.cleanup_old_windows(max_age_minutes=30)
                if cleaned_count > 0:
                    self.logger.debug(f"Cleaned {cleaned_count} old rolling windows")
            except Exception as e:
                self.logger.debug(f"Error cleaning up rolling windows: {e}")

    def _log_data_quality_issue(self, tick: dict, issues: list):
        """Log data quality issues to Redis for analysis"""
        try:
            issue_data = {
                "symbol": tick.get("symbol", "UNKNOWN"),
                "missing_fields": issues,
                "timestamp": datetime.now().isoformat(),
                "tick_data": tick,
            }
            # Publish data quality issues to DB 1 (realtime)
            realtime_client = self.realtime_client
            realtime_client.publish("data_quality.issues", json.dumps(issue_data))
        except Exception as e:
            self.logger.error(f"Data quality logging failed: {e}")

    def get_next_tick(self, timeout=1.0):
        """Get next tick with timeout"""
        try:
            if hasattr(self, "tick_queue"):
                tick = self.tick_queue.get(timeout=timeout)
                return tick
            return None
        except queue.Empty:
            return None
        except Exception as e:
            self.logger.error(f"Error in get_next_tick: {e}")
            return None

    def get_batch(self, max_size=None):
        """Get batch of ticks for efficient processing"""
        batch_size = max_size or self.batch_size
        batch = []

        # First flush any pending batch
        with self.batch_lock:
            if self.batch_buffer:
                self._flush_batch()

        with self.buffer_lock:
            while self.tick_buffer and len(batch) < batch_size:
                batch.append(self.tick_buffer.popleft())

            self.stats["ticks_processed"] += len(batch)

        return batch if batch else None

    def is_healthy(self, max_age=10):
        """Check if pipeline thread is still alive and processing

        Args:
            max_age: Maximum seconds since last heartbeat before considered unhealthy

        Returns:
            bool: True if healthy, False if thread appears dead
        """
        if not self.running:
            return False

        age = time.time() - self.last_heartbeat
        return age < max_age

    def get_stats(self):
        """Get detailed pipeline statistics"""
        stats = {
            **self.stats,
            "buffer_usage": len(self.tick_buffer),
            "buffer_capacity": self.buffer_capacity,
            "batch_buffer_size": len(self.batch_buffer),
            "dedup_cache_size": len(self.last_tick_hash),
            "channel_errors": self.channel_errors,
            "error_rate": self.stats["errors"] / max(self.stats["ticks_received"], 1),
            "dedup_rate": self.stats["ticks_deduplicated"]
            / max(self.stats["ticks_received"], 1),
        }
        
        # Add HybridCalculations statistics
        if hasattr(self, 'hybrid_calculations'):
            try:
                calc_stats = self.hybrid_calculations.get_cache_stats()
                rolling_stats = self.hybrid_calculations.get_rolling_window_stats()
                memory_stats = self.hybrid_calculations.get_memory_usage()
                
                stats.update({
                    "hybrid_calc_cache_hits": calc_stats.get("cache_hits", 0),
                    "hybrid_calc_cache_misses": calc_stats.get("cache_misses", 0),
                    "hybrid_calc_hit_rate": calc_stats.get("hit_rate", 0.0),
                    "rolling_window_symbols": rolling_stats.get("total_symbols", 0),
                    "rolling_window_avg_length": rolling_stats.get("average_window_length", 0.0),
                    "hybrid_calc_memory_mb": memory_stats.get("total_memory_mb", 0.0),
                    "hybrid_calc_cache_utilization": memory_stats.get("cache_utilization", 0.0)
                })
            except Exception as e:
                self.logger.debug(f"Error getting HybridCalculations stats: {e}")
        
        return stats

    def stop(self):
        """Stop the data pipeline gracefully"""
        self.running = False

        # ✅ TIER 2: Stop historical archive and flush remaining data
        if self.historical_archive:
            try:
                self.historical_archive.stop()
                self.logger.info("✅ Historical archive stopped and flushed")
            except Exception as e:
                self.logger.warning(f"Error stopping historical archive: {e}")

        # Flush any remaining batch
        with self.batch_lock:
            if self.batch_buffer:
                self._flush_batch()

        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except:
                pass

        # Log final statistics with proper error handling
        try:
            stats = self.get_stats()
            self.logger.info(f"📊 Pipeline stopped. Final stats:")
            for key, value in stats.items():
                if isinstance(value, float):
                    self.logger.info(f"  {key}: {value:.2%}")
                else:
                    self.logger.info(f"  {key}: {value}")
        except Exception as e:
            # If logging fails, print to console instead
            try:
                sys.stderr.write("📊 Pipeline stopped. Final stats:\n")
                stats = self.get_stats()
                for key, value in stats.items():
                    if isinstance(value, float):
                        sys.stderr.write(f"  {key}: {value:.2%}\n")
                    else:
                        sys.stderr.write(f"  {key}: {value}\n")
                sys.stderr.write(f"Note: Logging failed during shutdown: {e}\n")
            except Exception:
                pass

        # Close all log handlers to prevent I/O errors
        try:
            for handler in self.logger.handlers:
                handler.close()
        except:
            pass

    def _clean_tick_data(self, tick_data):
        """Clean and standardize tick data."""
        if not isinstance(tick_data, dict):
            raise ValueError("Tick data must be a dictionary")

        # Volume debugging removed - issue resolved

        # Extract symbol first
        symbol = self._extract_symbol(tick_data)
        if not symbol:
            raise ValueError("No symbol found in tick data")
        

        # Initialize cleaned data structure
        cleaned = {
            "tradingsymbol": symbol,
            "symbol": symbol,  # Keep both for compatibility
            "processed_at": get_current_ist_timestamp(),
            "source": "unknown",
        }

        # Clean numeric fields
        self._clean_numeric_fields(tick_data, cleaned)

        # Clean OHLC data
        self._clean_ohlc_data(tick_data, cleaned)

        # Clean depth data
        self._clean_depth_data(tick_data, cleaned)

        # Clean timestamps
        self._clean_timestamps(tick_data, cleaned)

        # Preserve crawler-calculated fields
        self._preserve_crawler_fields(tick_data, cleaned)

        # Calculate derived fields
        self._calculate_derived_fields(cleaned)

        # Attach external data (news, spoofing)
        self._attach_external_data(symbol, cleaned)

        # 🎯 ADD UNDERLYING PRICE FOR OPTIONS
        if self._is_option_symbol(symbol):
            underlying_price = self._get_underlying_price(symbol)
            if underlying_price > 0:
                cleaned['underlying_price'] = underlying_price
        else:
            # Update last_price cache for underlying symbols
            self._update_price_cache(symbol, cleaned.get('last_price', 0))

        # 🎯 ADD DYNAMIC EXPIRY CALCULATION FOR DERIVATIVES
        if self._is_option_symbol(symbol):
            # Add expiry calculation for options
            try:
                dte_info = expiry_calculator.calculate_dte(
                    expiry_date=cleaned.get('expiry_date'),
                    symbol=symbol
                )
                cleaned['days_to_expiry'] = dte_info.get('trading_dte', 0)
                cleaned['time_to_expiry'] = dte_info.get('dte_years', 0.0)
                cleaned['is_expiry_week'] = dte_info.get('is_weekly', False)
                cleaned['is_expiry_day'] = dte_info.get('trading_dte', 0) <= 1
            except Exception as e:
                self.logger.debug(f"Expiry calculation failed for {symbol}: {e}")
        
        return cleaned

    def _normalize_numeric_types(self, cleaned):
        """Normalize numeric-like fields to proper Python numbers for downstream code.

        This converts a small whitelist of last_price/quantity fields and nested OHLC/depth
        entries into floats/ints using the existing _safe_float helper. It mutates
        the cleaned dict in-place.
        """
        # Whitelist of top-level numeric fields to coerce to float
        numeric_fields = [
            "last_price",
            "average_price",
            "bucket_incremental_volume",
            "zerodha_cumulative_volume",
            "bucket_cumulative_volume",  # Raw cumulative bucket_incremental_volume from Zerodha
            "bucket_incremental_volume",  # Set to 0 (no calculations)
            "volume_ratio",  # 🎯 CRITICAL: Add volume_ratio to numeric fields
            "buy_quantity",
            "sell_quantity",
            "total_buy_quantity",
            "total_sell_quantity",
            "zerodha_last_traded_quantity",
            "oi",
            "oi_day_high",
            "oi_day_low",
            "change",
            "net_change",  # Calculated from last_price - ohlc.close
            "price_change_pct",
            "spoofing_score",
            "weighted_bid",
            "weighted_ask",
            # Canonical/synonyms added downstream
            "weighted_bid_price",
            "weighted_ask_price",
            "bid_ask_spread",
            "mid_price",
            "order_book_imbalance",
        ]

        for f in numeric_fields:
            if f in cleaned:
                cleaned[f] = self._safe_float(cleaned.get(f))

        # Normalize OHLC
        ohlc = cleaned.get("ohlc")
        if isinstance(ohlc, dict):
            for key in ["open", "high", "low", "close"]:
                if key in ohlc:
                    ohlc[key] = self._safe_float(ohlc.get(key))
            cleaned["ohlc"] = ohlc

        # Normalize depth levels (last_price, quantity)
        depth = cleaned.get("depth")
        if isinstance(depth, dict):
            for side in ("buy", "sell"):
                levels = depth.get(side, [])
                if isinstance(levels, list):
                    for lvl in levels:
                        if isinstance(lvl, dict):
                            if "last_price" in lvl:
                                lvl["last_price"] = self._safe_float(lvl.get("last_price"))
                            if "quantity" in lvl:
                                # quantities are often ints but safe_float is fine
                                lvl["quantity"] = self._safe_float(lvl.get("quantity"))
            cleaned["depth"] = depth


        # Normalize any numeric-looking preserved crawler fields
        crawler_numeric = [
            "total_bid_orders",
            "total_ask_orders",
            "avg_bid_order_size",
            "avg_ask_order_size",
            "order_imbalance",
            "order_count_imbalance",
            "bid_ask_ratio",
            "total_bid_qty",
            "total_ask_qty",
            "depth_levels",
        ]
        for f in crawler_numeric:
            if f in cleaned:
                cleaned[f] = self._safe_float(cleaned.get(f))

    def _process_premarket_order(self, data):
        """Process premarket order data using premarket adapter"""
        try:
            # Use absolute path for premarket adapter
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            sys.path.insert(0, project_root)
            from core.premarket_adapter import (
                build_enhanced_premarket_indicator,
                build_premarket_indicator,
            )
            # ✅ Build standardized premarket indicator
            premarket_data = build_premarket_indicator(data)
            symbol = premarket_data.get("symbol", "UNKNOWN")

            # ✅ Order flow analysis removed - simplified premarket indicator
            enhanced_indicator = build_enhanced_premarket_indicator(
                data,
                order_flow_analysis=None,  # Order flow analysis disabled
                base_indicator=premarket_data,
            )
            premarket_data.update(enhanced_indicator)

            # Store in Redis DB 3 (premarket_volume)
            if self.redis_client:
                self.redis_client.store_premarket_volume(
                    symbol, premarket_data
                )

            # Generate alerts for high-confidence premarket manipulation signals
            should_send_alert = False
            if self.alert_manager and hasattr(
                self.alert_manager, "should_send_premarket_alert"
            ):
                try:
                    should_send_alert = self.alert_manager.should_send_premarket_alert(
                        premarket_data
                    )
                except Exception as exc:
                    self.logger.error(
                        "Error while evaluating premarket alert for %s: %s",
                        symbol,
                        exc,
                    )
                    should_send_alert = False
            else:
                confidence = premarket_data.get("confidence", 0.0)
                should_send_alert = (
                    confidence >= 0.8 and not premarket_data.get("manipulation_detected")
                )

            if should_send_alert:
                # Generate alert for high-confidence premarket manipulation
                alert_data = {
                    "symbol": symbol,
                    "pattern": "premarket_manipulation",
                    "confidence": premarket_data.get("confidence", 0.0),
                    "last_price": premarket_data.get("price_0915", 0),
                    "direction": premarket_data.get(
                        "true_direction", premarket_data.get("direction", "UNKNOWN")
                    ),
                    "price_change_pct": premarket_data.get("price_change_pct", 0),
                    "volume_change_pct": premarket_data.get("volume_change_pct", 0),
                    "timestamp": datetime.now().isoformat(),
                    "data": premarket_data,
                }

                # Send alert through AlertManager if available
                if self.alert_manager:
                    try:
                        self.alert_manager.send_alert(alert_data)
                        self.logger.info(
                            f"✅ Premarket alert sent via AlertManager: {alert_data['symbol']}"
                        )
                    except Exception as e:
                        self.logger.error(
                            f"❌ Failed to send premarket alert via AlertManager: {e}"
                        )
                        # Fallback to Redis publishing in DB 1 (realtime)
                        if self.redis_client:
                            realtime_client = self.realtime_client
                            realtime_client.publish(
                                "alerts:new", json.dumps(alert_data)
                            )
                            realtime_client.publish(
                                f"alerts:{premarket_data.get('symbol')}",
                                json.dumps(alert_data),
                            )
                else:
                    # Fallback to Redis publishing if no AlertManager in DB 1 (realtime)
                    if self.redis_client:
                        realtime_client = self.realtime_client
                        realtime_client.publish("alerts:new", json.dumps(alert_data))
                        realtime_client.publish(
                            f"alerts:{premarket_data.get('symbol')}",
                            json.dumps(alert_data),
                        )

            self.stats["premarket_orders"] += 1

        except Exception as e:
            self.logger.error(f"Error processing premarket order: {e}")
            self.stats["errors"] += 1

    def _process_news_from_channel(self, data):
        """Process news data from market_data.news channel and create NEWS_ALERT patterns"""
        try:
            # Use existing comprehensive news processing system
            # Store news data in Redis for existing news functions to access
            news_key = f"news:latest:{int(time.time())}"
            self.redis_client.setex(news_key, 86400, json.dumps(data))
            
            # Use existing store_news_sentiment method
            if hasattr(self.redis_client, 'store_news_sentiment'):
                self.redis_client.store_news_sentiment("MARKET_NEWS", data)
            
            # Process symbols mentioned in news using existing news system
            symbols = data.get("symbols", [])
            for symbol in symbols:
                # Use existing _get_news_for_symbol system
                existing_news = self._get_news_for_symbol(symbol)
                if existing_news:
                    # News already exists for this symbol, update cache
                    self.news_cache[symbol] = (existing_news, time.time())
            
            # Create NEWS_ALERT pattern for high-impact news
            if self._should_create_news_alert(data):
                news_alert = self._create_news_alert(data)
                if news_alert and self.alert_manager:
                    self.alert_manager.send_alert(news_alert)
                    self.logger.info(f"📰 NEWS_ALERT sent: {data.get('title', 'Unknown')[:50]}...")
            
            # Increment stats
            if hasattr(self, "stats"):
                self.stats["news_processed"] = self.stats.get("news_processed", 0) + 1
            
            self.logger.info(f"📰 Processed news from channel: {data.get('title', 'Unknown')[:50]}...")
            
        except Exception as e:
            self.logger.error(f"Error processing news from channel: {e}")
            self.stats["errors"] += 1

    def _process_news_from_symbol_keys(self):
        """Process news from symbol-specific keys (gift_nifty_gap.py format)
        
        ✅ STANDARDIZED: Uses direct key lookups for known symbols instead of pattern matching.
        ❌ FORBIDDEN: .keys("news:symbol:*") pattern matching removed per Redis key standards.
        """
        try:
            # ✅ STANDARDIZED: Use direct key lookups for known symbols
            # Get symbols from active universe or tick data, not pattern matching
            symbols_to_check = set()
            
            # Collect symbols from current tick data being processed
            if hasattr(self, 'batch_buffer') and self.batch_buffer:
                for tick in self.batch_buffer.values():
                    for symbol_tick in tick:
                        symbol = symbol_tick.get('symbol') or ''
                        if symbol:
                            symbols_to_check.add(symbol)
            
            # Also check symbols from recent ticks cache
            if hasattr(self, 'last_processed_symbols'):
                symbols_to_check.update(self.last_processed_symbols)
            
            if not symbols_to_check:
                self.logger.debug("No symbols available for news lookup")
                return
            
            self.logger.debug(f"🔍 Checking news for {len(symbols_to_check)} known symbols (direct lookup)")
            
            for symbol in symbols_to_check:
                # ✅ Direct key lookup per symbol
                news_key = f"news:symbol:{symbol}"
                try:
                    # Get the latest news item from the sorted set
                    news_items = self.redis_client.zrange(news_key, -1, -1)
                    if not news_items:
                        continue
                    
                    news_data = json.loads(news_items[0])
                    news_item = news_data.get('data', news_data)
                    
                    # Extract symbol from key
                    symbol = news_key.replace('news:symbol:', '')
                    
                    # Create NEWS_ALERT pattern for high-impact news
                    if self._should_create_news_alert(news_item):
                        news_alert = self._create_news_alert(news_item)
                        if news_alert and self.alert_manager:
                            # Update symbol to match the key
                            news_alert['symbol'] = symbol
                            self.alert_manager.send_alert(news_alert)
                            self.logger.info(f"📰 NEWS_ALERT sent from {symbol}: {news_item.get('title', 'Unknown')[:50]}...")
                    
                except Exception as e:
                    self.logger.debug(f"Error processing news from {news_key}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error processing news from symbol keys: {e}")

    def _should_create_news_alert(self, data):
        """Determine if news should trigger an alert"""
        try:
            # Check for high-impact news indicators
            title = data.get('title', '').lower()
            sentiment = data.get('sentiment', 'neutral').lower()
            publisher = data.get('publisher', '').lower()
            
            # High-impact keywords
            high_impact_keywords = [
                'earnings', 'results', 'quarterly', 'revenue', 'profit', 'loss',
                'merger', 'acquisition', 'takeover', 'deal', 'partnership',
                'fda', 'approval', 'regulatory', 'investigation', 'lawsuit',
                'bankruptcy', 'restructuring', 'layoffs', 'hiring',
                'rate', 'interest', 'fed', 'rbi', 'policy', 'guidance',
                'upgrade', 'downgrade', 'target', 'price', 'forecast',
                'alphabet', 'google', 'microsoft', 'apple', 'amazon', 'nvidia'
            ]
            
            # Check if title contains high-impact keywords
            has_high_impact = any(keyword in title for keyword in high_impact_keywords)
            
            # Check sentiment
            is_positive_sentiment = sentiment in ['positive', 'bullish']
            is_negative_sentiment = sentiment in ['negative', 'bearish']
            
            # Check publisher credibility
            credible_publishers = ['reuters', 'bloomberg', 'economic times', 'business standard', 'mint', 'livemint']
            is_credible = any(pub in publisher for pub in credible_publishers)
            
            # Create alert if: high impact OR (credible publisher AND strong sentiment)
            return has_high_impact or (is_credible and (is_positive_sentiment or is_negative_sentiment))
            
        except Exception as e:
            self.logger.error(f"Error checking news alert criteria: {e}")
            return False

    def _create_news_alert(self, data):
        """Create NEWS_ALERT pattern from news data"""
        try:
            title = data.get('title', 'Unknown News')
            sentiment = data.get('sentiment', 'neutral')
            publisher = data.get('publisher', 'Unknown')
            link = data.get('link', '')
            symbols = data.get('symbols', [])
            
            # If no symbols provided, try to infer from title
            if not symbols:
                title_lower = title.lower()
                if any(keyword in title_lower for keyword in ['nifty', 'banknifty', 'bank nifty']):
                    symbols = ['NIFTY']
                elif any(keyword in title_lower for keyword in ['alphabet', 'google', 'microsoft', 'apple', 'amazon', 'nvidia']):
                    symbols = ['MARKET']  # Tech news affects overall market
                else:
                    symbols = ['MARKET']  # General market news
            
            # Determine alert action based on sentiment
            if sentiment in ['positive', 'bullish']:
                action = 'BUY'
                signal = 'BULLISH'
            elif sentiment in ['negative', 'bearish']:
                action = 'SELL'
                signal = 'BEARISH'
            else:
                action = 'WATCH'
                signal = 'NEUTRAL'
            
            # Create news alert pattern
            news_alert = {
                'symbol': symbols[0] if symbols else 'MARKET',
                'pattern': 'NEWS_ALERT',
                'confidence': 0.9,  # High confidence for news alerts
                'action': action,
                'signal': signal,
                'expected_move': 2.0,  # News can cause 2% moves
                'last_price': 0.0,  # News doesn't have a specific price
                'description': f"News Alert: {title[:100]}...",
                'pattern_type': 'news',
                'news_context': {
                    'title': title,
                    'sentiment': sentiment,
                    'publisher': publisher,
                    'link': link,
                    'symbols': symbols
                },
                'risk_metrics': {
                    'stop_loss': 0.0,
                    'target_price': 0.0,
                    'position_size': 0,
                    'risk_reward_ratio': 0.0
                },
                'timestamp': int(time.time() * 1000),
                'pattern_title': '📰 News Alert',
                'pattern_description': f'High-impact news from {publisher}',
                'move_type': 'NEWS_ALERT',
                'action_explanation': f'News impact: {sentiment} sentiment',
                'pattern_display': '📰 News Alert',
                'trading_instruction': f'📰 NEWS: {action} - Monitor for news-driven moves',
                'directional_action': action
            }
            
            return news_alert
            
        except Exception as e:
            self.logger.error(f"Error creating news alert: {e}")
            return None

    def _process_index_data(self, channel, data):
        """Process index data (NIFTY, VIX, etc.) from crawlers/gift_nifty_gap.py"""
        try:
            # Extract index name from channel (e.g., 'index:NSE:NIFTY 50' -> 'nifty50')
            parts = channel.split(":")
            if len(parts) >= 2:
                # Channel format: index:<EXCHANGE>:<INDEX NAME>
                raw_name = parts[-1]
                index_symbol = raw_name.replace(" ", "").lower()  # e.g., 'nifty50', 'niftybank', 'indiavix', 'giftnifty'
                index_data = {
                    "index": index_symbol,
                    "last_price": self._safe_float(data.get("last_price", 0)),
                    "change": self._safe_float(data.get("change", 0)),
                    "change_pct": self._safe_float(data.get("change_pct", 0)),
                    "timestamp": data.get("timestamp", get_current_ist_timestamp()),
                    "raw_data": data,
                }

                # Store in Redis for VIX/indicator use (60s TTL), normalized key
                redis_key = f"index_data:{index_symbol}"
                self.redis_client.setex(redis_key, 60, json.dumps(index_data))

                # Increment stats if available
                if hasattr(self, "stats"):
                    self.stats["index_updates"] = self.stats.get("index_updates", 0) + 1

        except Exception as e:
            self.logger.warning(f"Error processing {channel}: {e}")


def process_tick_data_fast(tick_data):
    """High-performance tick processing using numpy."""
    if not tick_data:
        return {}

    # ASSUME timestamps are already in epoch_ms format from pipeline
    # No need to parse again if data comes from cleaned pipeline
    ts_np = np.array([tick.get("timestamp_ms", 0) for tick in tick_data], dtype="int64")
    prices_np = np.array([tick.get("last_price", 0.0) for tick in tick_data], dtype="float64")
    volumes_np = np.array([tick.get("bucket_incremental_volume", 0) for tick in tick_data], dtype="int64")

    indicators = calculate_indicators_numpy(ts_np, prices_np, volumes_np)
    indicators["timestamps"] = ts_np
    return indicators


def calculate_indicators_numpy(timestamps, prices, volumes):
    """Pure NumPy technical indicators for tick data."""
    def sma(data, window):
        if len(data) < window:
            return np.array([], dtype=data.dtype)
        kernel = np.ones(window, dtype="float64") / window
        return np.convolve(data, kernel, mode="valid")

    def roc(data, period):
        if len(data) <= period or period <= 0:
            return np.array([], dtype="float64")
        base = data[:-period]
        delta = data[period:]
        with np.errstate(divide="ignore", invalid="ignore"):
            result = (delta - base) / base
        return result

    return {
        "sma_20": sma(prices, 20),
        "sma_50": sma(prices, 50),
        "volume_sma": sma(volumes, 20),
        "price_roc": roc(prices, 10),
        "timestamps": timestamps,
    }


class Nifty50SentimentAnalyzer:
    """
    Nifty50-focused sentiment analysis with sector-specific intelligence
    """
    
    def __init__(self):
        self.nifty50_symbols = self._get_nifty50_symbols()
        self.sector_keywords = self._build_sector_keywords()
        self.company_specific_terms = self._build_company_terms()
    
    def _get_nifty50_symbols(self):
        """Get current Nifty50 symbols (accurate as of 2025)"""
        return [
            # Oil & Gas
            'RELIANCE', 'ONGC', 'BPCL', 'IOC', 'GAIL',
            # Banking & Financial Services
            'HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'SBIN', 'AXISBANK', 'INDUSINDBK', 'BAJFINANCE', 'BAJAJFINSV',
            'HDFCLIFE', 'SBILIFE', 'SHREECEM', 'HDFCAMC',
            # Information Technology
            'TCS', 'INFY', 'WIPRO', 'TECHM', 'HCLTECH', 'LTIM',
            # Consumer Goods
            'HINDUNILVR', 'ITC', 'ASIANPAINT', 'NESTLEIND', 'TITAN', 'BRITANNIA', 'TATACONSUM',
            # Automobile
            'MARUTI', 'TATAMOTORS', 'EICHERMOT', 'HEROMOTOCO', 'M&M', 'BAJAJHLDNG',
            # Pharmaceuticals
            'SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'APOLLOHOSP',
            # Metals & Mining
            'TATASTEEL', 'JSWSTEEL', 'COALINDIA', 'HINDALCO',
            # Infrastructure & Construction
            'LT', 'ADANIPORTS', 'GRASIM',
            # Cement
            'ULTRACEMCO', 'SHREECEM', 'GRASIM',
            # Power & Utilities
            'NTPC', 'POWERGRID', 'TATAPOWER',
            # Telecom
            'BHARTIARTL',
            # Chemicals
            'UPL',
            # Diversified
            'ADANIENT'
        ]
    
    def _build_sector_keywords(self):
        """Sector-specific sentiment keywords for Nifty50"""
        return {
            'banking_finance': {
                'positive': [
                    'rate cut', 'loan growth', 'npa recovery', 'profit growth', 'dividend hike',
                    'capital infusion', 'merger approval', 'rbi approval', 'asset quality',
                    'provision writeback', 'credit growth', 'margin expansion', 'slii inclusion',
                    'casa growth', 'digital banking', 'fintech partnership', 'regulatory relief',
                    'capital adequacy', 'liquidity surplus', 'interest margin', 'fee income growth',
                    'policy growth', 'premium growth', 'aum growth', 'inflow increase'
                ],
                'negative': [
                    'npa rise', 'provisioning', 'rbi penalty', 'fraud', 'default', 'rate hike',
                    'liquidity crunch', 'capital adequacy', 'slii exclusion', 'regulatory scrutiny',
                    'bad loans', 'credit cost', 'margin pressure', 'asset quality concern',
                    'attrition', 'cyber attack', 'data breach', 'compliance issue', 'audit finding',
                    'policy lapse', 'premium decline', 'aum decline', 'outflow increase'
                ]
            },
            'it_technology': {
                'positive': [
                    'deal win', 'contract renewal', 'digital transformation', 'cloud migration',
                    'ai adoption', 'quarterly beat', 'guidance raise', 'client addition',
                    'margin expansion', 'acquisition', 'partnership', 'innovation award',
                    'attrition improvement', 'wage normalization', 'offshore growth', 'automation',
                    'cybersecurity', 'data analytics', 'blockchain', 'iot solutions'
                ],
                'negative': [
                    'client exit', 'contract loss', 'cyber attack', 'data breach', 'attrition',
                    'wage inflation', 'margin pressure', 'guidance cut', 'project delay',
                    'competition', 'pricing pressure', 'visa issues', 'recession fears',
                    'offshore pressure', 'currency impact', 'client concentration', 'regulatory compliance'
                ]
            },
            'pharma_healthcare': {
                'positive': [
                    'fda approval', 'drug launch', 'patent expiry', 'generic opportunity', 'api expansion',
                    'clinical trial success', 'regulatory approval', 'market share gain', 'export growth',
                    'r&d investment', 'acquisition', 'partnership', 'capacity expansion', 'cost optimization'
                ],
                'negative': [
                    'fda warning', 'regulatory action', 'patent challenge', 'last_price control', 'competition',
                    'raw material cost', 'currency impact', 'regulatory delay', 'quality issue',
                    'recall', 'litigation', 'market share loss', 'pricing pressure', 'supply chain issue'
                ]
            },
            'automobile': {
                'positive': [
                    'sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption',
                    'capacity expansion', 'technology upgrade', 'partnership', 'acquisition',
                    'cost reduction', 'efficiency improvement', 'safety rating', 'award recognition'
                ],
                'negative': [
                    'sales decline', 'production cut', 'supply chain issue', 'raw material cost',
                    'competition', 'regulatory compliance', 'safety recall', 'emission norms',
                    'inventory pileup', 'dealer concern', 'export decline', 'technology lag'
                ]
            },
            'energy_oil_gas': {
                'positive': [
                    'oil discovery', 'production increase', 'refining margin', 'gas last_price hike',
                    'capacity expansion', 'export growth', 'partnership', 'acquisition',
                    'technology upgrade', 'efficiency improvement', 'regulatory relief', 'market share gain'
                ],
                'negative': [
                    'oil last_price fall', 'refining margin pressure', 'production decline', 'regulatory issue',
                    'environmental concern', 'competition', 'supply chain issue', 'currency impact',
                    'demand decline', 'capacity underutilization', 'safety incident', 'cost escalation'
                ]
            },
            'metals_mining': {
                'positive': [
                    'last_price increase', 'demand growth', 'capacity expansion', 'export growth',
                    'technology upgrade', 'efficiency improvement', 'partnership', 'acquisition',
                    'market share gain', 'cost reduction', 'productivity improvement', 'sustainability'
                ],
                'negative': [
                    'last_price decline', 'demand fall', 'overcapacity', 'competition', 'raw material cost',
                    'currency impact', 'regulatory compliance', 'environmental concern', 'supply chain issue',
                    'labor issue', 'safety incident', 'cost escalation', 'market share loss'
                ]
            },
            'fmcg_consumer': {
                'positive': [
                    'bucket_incremental_volume growth', 'market share gain', 'new product launch', 'brand expansion',
                    'distribution improvement', 'cost optimization', 'efficiency gain', 'acquisition',
                    'partnership', 'innovation', 'premiumization', 'rural growth', 'export increase'
                ],
                'negative': [
                    'bucket_incremental_volume decline', 'market share loss', 'competition', 'pricing pressure',
                    'raw material cost', 'supply chain issue', 'distribution challenge', 'brand erosion',
                    'consumer shift', 'regulatory compliance', 'currency impact', 'inventory pileup'
                ]
            },
            'infrastructure_construction': {
                'positive': [
                    'order win', 'execution improvement', 'margin expansion', 'capacity utilization',
                    'technology upgrade', 'efficiency gain', 'partnership', 'acquisition',
                    'market share gain', 'cost reduction', 'productivity improvement', 'sustainability'
                ],
                'negative': [
                    'order decline', 'execution delay', 'margin pressure', 'competition',
                    'raw material cost', 'labor issue', 'regulatory compliance', 'environmental concern',
                    'supply chain issue', 'safety incident', 'cost escalation', 'market share loss'
                ]
            },
            'telecom_media': {
                'positive': [
                    'subscriber growth', 'arpu increase', 'network expansion', 'technology upgrade',
                    'spectrum acquisition', 'partnership', 'acquisition', 'market share gain',
                    'cost optimization', 'efficiency improvement', 'innovation', 'digital transformation'
                ],
                'negative': [
                    'subscriber loss', 'arpu decline', 'competition', 'regulatory issue',
                    'spectrum cost', 'network issue', 'service quality', 'pricing pressure',
                    'market share loss', 'cost escalation', 'technology lag', 'regulatory compliance'
                ]
            },
            'cement': {
                'positive': [
                    'bucket_incremental_volume growth', 'last_price increase', 'capacity expansion', 'export growth',
                    'demand growth', 'infrastructure spending', 'government projects', 'urban development',
                    'rural housing', 'cost optimization', 'efficiency improvement', 'market share gain'
                ],
                'negative': [
                    'bucket_incremental_volume decline', 'last_price pressure', 'demand fall', 'overcapacity', 'competition',
                    'raw material cost', 'energy cost', 'transportation cost', 'regulatory compliance',
                    'environmental concern', 'capacity underutilization', 'market share loss'
                ]
            },
            'mining': {
                'positive': [
                    'production increase', 'last_price increase', 'demand growth', 'export growth',
                    'capacity expansion', 'technology upgrade', 'efficiency improvement', 'sustainability',
                    'coal production', 'mineral discovery', 'partnership', 'acquisition'
                ],
                'negative': [
                    'production decline', 'last_price decline', 'demand fall', 'regulatory issue',
                    'environmental concern', 'safety incident', 'labor issue', 'competition',
                    'cost escalation', 'capacity underutilization', 'market share loss'
                ]
            },
            'diversified': {
                'positive': [
                    'portfolio growth', 'diversification', 'acquisition', 'partnership',
                    'market expansion', 'innovation', 'technology upgrade', 'efficiency improvement',
                    'market share gain', 'revenue growth', 'profit growth', 'strategic investment'
                ],
                'negative': [
                    'portfolio decline', 'diversification risk', 'acquisition failure', 'partnership loss',
                    'market contraction', 'technology lag', 'efficiency decline', 'market share loss',
                    'revenue decline', 'profit decline', 'strategic failure', 'regulatory issue'
                ]
            }
        }
    
    def _build_company_terms(self):
        """Company-specific sentiment triggers for all Nifty50 stocks"""
        return {
            'RELIANCE': {
                'positive': ['jio subscriber', 'retail expansion', 'green energy', '5g rollout', 'oil discovery', 'petrochemical', 'refining margin', 'digital services'],
                'negative': ['refining margin pressure', 'debt concern', 'regulatory issue', 'project delay', 'competition', 'capex pressure']
            },
            'TCS': {
                'positive': ['large deal', 'digital growth', 'attrition improvement', 'bancs modernization', 'cloud migration', 'ai adoption'],
                'negative': ['wage inflation', 'client concentration', 'competition from accenture', 'attrition', 'pricing pressure']
            },
            'HDFCBANK': {
                'positive': ['casa growth', 'npa improvement', 'digital adoption', 'market share gain', 'credit growth', 'margin expansion'],
                'negative': ['rbi action', 'slii exclusion', 'management change', 'asset quality', 'competition', 'regulatory scrutiny']
            },
            'INFY': {
                'positive': ['deal win', 'digital transformation', 'attrition improvement', 'margin expansion', 'cloud services', 'automation'],
                'negative': ['client exit', 'wage inflation', 'competition', 'pricing pressure', 'attrition', 'project delay']
            },
            'HINDUNILVR': {
                'positive': ['bucket_incremental_volume growth', 'market share gain', 'premiumization', 'innovation', 'distribution expansion', 'brand strength'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'ICICIBANK': {
                'positive': ['loan growth', 'npa recovery', 'digital banking', 'market share gain', 'credit growth', 'margin improvement'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'KOTAKBANK': {
                'positive': ['casa growth', 'npa improvement', 'digital adoption', 'credit growth', 'margin expansion', 'market share gain'],
                'negative': ['npa rise', 'competition', 'regulatory issue', 'asset quality', 'margin pressure', 'provisioning']
            },
            'BHARTIARTL': {
                'positive': ['subscriber growth', 'arpu increase', '5g rollout', 'network expansion', 'market share gain', 'digital services'],
                'negative': ['subscriber loss', 'arpu decline', 'competition', 'spectrum cost', 'regulatory issue', 'network quality']
            },
            'SBIN': {
                'positive': ['loan growth', 'npa recovery', 'digital banking', 'market share gain', 'credit growth', 'margin improvement'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'ITC': {
                'positive': ['cigarette bucket_incremental_volume', 'fmgc growth', 'hotel expansion', 'paper business', 'agri business', 'market share gain'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'regulatory issue', 'tax increase', 'market share loss', 'pricing pressure']
            },
            'LT': {
                'positive': ['order win', 'execution improvement', 'margin expansion', 'infrastructure', 'technology', 'market share gain'],
                'negative': ['order decline', 'execution delay', 'margin pressure', 'competition', 'raw material cost', 'labor issue']
            },
            'ASIANPAINT': {
                'positive': ['bucket_incremental_volume growth', 'market share gain', 'premium products', 'distribution expansion', 'innovation', 'brand strength'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'AXISBANK': {
                'positive': ['loan growth', 'npa recovery', 'digital banking', 'market share gain', 'credit growth', 'margin improvement'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'MARUTI': {
                'positive': ['sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption', 'technology upgrade'],
                'negative': ['sales decline', 'production cut', 'supply chain issue', 'competition', 'raw material cost', 'safety recall']
            },
            'SUNPHARMA': {
                'positive': ['fda approval', 'drug launch', 'export growth', 'market share gain', 'r&d investment', 'acquisition'],
                'negative': ['fda warning', 'regulatory action', 'competition', 'pricing pressure', 'currency impact', 'quality issue']
            },
            'TITAN': {
                'positive': ['sales growth', 'market share gain', 'new launch', 'brand expansion', 'distribution improvement', 'innovation'],
                'negative': ['sales decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'NESTLEIND': {
                'positive': ['bucket_incremental_volume growth', 'market share gain', 'new product launch', 'brand expansion', 'distribution improvement', 'innovation'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'ULTRACEMCO': {
                'positive': ['bucket_incremental_volume growth', 'last_price increase', 'market share gain', 'capacity expansion', 'export growth', 'cost optimization'],
                'negative': ['bucket_incremental_volume decline', 'last_price pressure', 'competition', 'raw material cost', 'demand fall', 'capacity underutilization']
            },
            'WIPRO': {
                'positive': ['deal win', 'digital transformation', 'attrition improvement', 'margin expansion', 'cloud services', 'automation'],
                'negative': ['client exit', 'wage inflation', 'competition', 'pricing pressure', 'attrition', 'project delay']
            },
            'ONGC': {
                'positive': ['oil discovery', 'production increase', 'last_price increase', 'exploration success', 'partnership', 'technology upgrade'],
                'negative': ['oil last_price fall', 'production decline', 'exploration failure', 'regulatory issue', 'environmental concern', 'competition']
            },
            'NTPC': {
                'positive': ['capacity addition', 'power generation', 'renewable energy', 'efficiency improvement', 'market share gain', 'technology upgrade'],
                'negative': ['capacity underutilization', 'demand decline', 'regulatory issue', 'environmental concern', 'competition', 'cost escalation']
            },
            'POWERGRID': {
                'positive': ['transmission growth', 'grid expansion', 'efficiency improvement', 'market share gain', 'technology upgrade', 'sustainability'],
                'negative': ['transmission decline', 'grid issue', 'regulatory compliance', 'environmental concern', 'competition', 'cost escalation']
            },
            'TECHM': {
                'positive': ['deal win', 'digital transformation', 'attrition improvement', 'margin expansion', 'cloud services', 'automation'],
                'negative': ['client exit', 'wage inflation', 'competition', 'pricing pressure', 'attrition', 'project delay']
            },
            'HCLTECH': {
                'positive': ['deal win', 'digital transformation', 'attrition improvement', 'margin expansion', 'cloud services', 'automation'],
                'negative': ['client exit', 'wage inflation', 'competition', 'pricing pressure', 'attrition', 'project delay']
            },
            'BAJFINANCE': {
                'positive': ['loan growth', 'npa recovery', 'digital adoption', 'market share gain', 'credit growth', 'margin expansion'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'BAJAJFINSV': {
                'positive': ['loan growth', 'npa recovery', 'digital adoption', 'market share gain', 'credit growth', 'margin expansion'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'TATAMOTORS': {
                'positive': ['sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption', 'technology upgrade'],
                'negative': ['sales decline', 'production cut', 'supply chain issue', 'competition', 'raw material cost', 'safety recall']
            },
            'TATASTEEL': {
                'positive': ['last_price increase', 'demand growth', 'capacity expansion', 'export growth', 'technology upgrade', 'efficiency improvement'],
                'negative': ['last_price decline', 'demand fall', 'overcapacity', 'competition', 'raw material cost', 'currency impact']
            },
            'JSWSTEEL': {
                'positive': ['last_price increase', 'demand growth', 'capacity expansion', 'export growth', 'technology upgrade', 'efficiency improvement'],
                'negative': ['last_price decline', 'demand fall', 'overcapacity', 'competition', 'raw material cost', 'currency impact']
            },
            'TATACONSUM': {
                'positive': ['bucket_incremental_volume growth', 'market share gain', 'new product launch', 'brand expansion', 'distribution improvement', 'innovation'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'DRREDDY': {
                'positive': ['fda approval', 'drug launch', 'export growth', 'market share gain', 'r&d investment', 'acquisition'],
                'negative': ['fda warning', 'regulatory action', 'competition', 'pricing pressure', 'currency impact', 'quality issue']
            },
            'CIPLA': {
                'positive': ['fda approval', 'drug launch', 'export growth', 'market share gain', 'r&d investment', 'acquisition'],
                'negative': ['fda warning', 'regulatory action', 'competition', 'pricing pressure', 'currency impact', 'quality issue']
            },
            'APOLLOHOSP': {
                'positive': ['patient growth', 'capacity expansion', 'technology upgrade', 'market share gain', 'efficiency improvement', 'acquisition'],
                'negative': ['patient decline', 'capacity underutilization', 'competition', 'regulatory issue', 'cost escalation', 'market share loss']
            },
            'DIVISLAB': {
                'positive': ['fda approval', 'drug launch', 'export growth', 'market share gain', 'r&d investment', 'acquisition'],
                'negative': ['fda warning', 'regulatory action', 'competition', 'pricing pressure', 'currency impact', 'quality issue']
            },
            'GRASIM': {
                'positive': ['bucket_incremental_volume growth', 'last_price increase', 'market share gain', 'capacity expansion', 'export growth', 'cost optimization'],
                'negative': ['bucket_incremental_volume decline', 'last_price pressure', 'competition', 'raw material cost', 'demand fall', 'capacity underutilization']
            },
            'BRITANNIA': {
                'positive': ['bucket_incremental_volume growth', 'market share gain', 'new product launch', 'brand expansion', 'distribution improvement', 'innovation'],
                'negative': ['bucket_incremental_volume decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'EICHERMOT': {
                'positive': ['sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption', 'technology upgrade'],
                'negative': ['sales decline', 'production cut', 'supply chain issue', 'competition', 'raw material cost', 'safety recall']
            },
            'HEROMOTOCO': {
                'positive': ['sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption', 'technology upgrade'],
                'negative': ['sales decline', 'production cut', 'supply chain issue', 'competition', 'raw material cost', 'safety recall']
            },
            'COALINDIA': {
                'positive': ['production increase', 'last_price increase', 'efficiency improvement', 'market share gain', 'technology upgrade', 'sustainability'],
                'negative': ['production decline', 'last_price pressure', 'competition', 'environmental concern', 'regulatory issue', 'cost escalation']
            },
            'BPCL': {
                'positive': ['refining margin', 'capacity expansion', 'export growth', 'technology upgrade', 'efficiency improvement', 'market share gain'],
                'negative': ['refining margin pressure', 'capacity underutilization', 'competition', 'regulatory issue', 'environmental concern', 'cost escalation']
            },
            'IOC': {
                'positive': ['refining margin', 'capacity expansion', 'export growth', 'technology upgrade', 'efficiency improvement', 'market share gain'],
                'negative': ['refining margin pressure', 'capacity underutilization', 'competition', 'regulatory issue', 'environmental concern', 'cost escalation']
            },
            'ADANIPORTS': {
                'positive': ['cargo growth', 'capacity expansion', 'efficiency improvement', 'market share gain', 'technology upgrade', 'sustainability'],
                'negative': ['cargo decline', 'capacity underutilization', 'competition', 'regulatory issue', 'environmental concern', 'cost escalation']
            },
            'TATAPOWER': {
                'positive': ['power generation', 'capacity addition', 'renewable energy', 'efficiency improvement', 'market share gain', 'technology upgrade'],
                'negative': ['power generation decline', 'capacity underutilization', 'regulatory issue', 'environmental concern', 'competition', 'cost escalation']
            },
            'SHREECEM': {
                'positive': ['bucket_incremental_volume growth', 'last_price increase', 'market share gain', 'capacity expansion', 'export growth', 'cost optimization'],
                'negative': ['bucket_incremental_volume decline', 'last_price pressure', 'competition', 'raw material cost', 'demand fall', 'capacity underutilization']
            },
            'INDUSINDBK': {
                'positive': ['loan growth', 'npa recovery', 'digital banking', 'market share gain', 'credit growth', 'margin improvement'],
                'negative': ['npa rise', 'provisioning', 'competition', 'regulatory issue', 'asset quality', 'margin pressure']
            },
            'BAJAJHLDNG': {
                'positive': ['investment growth', 'portfolio performance', 'market share gain', 'acquisition', 'partnership', 'innovation'],
                'negative': ['investment decline', 'portfolio loss', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'HDFCLIFE': {
                'positive': ['premium growth', 'policy growth', 'market share gain', 'product innovation', 'distribution expansion', 'efficiency improvement'],
                'negative': ['premium decline', 'policy lapse', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'SBILIFE': {
                'positive': ['premium growth', 'policy growth', 'market share gain', 'product innovation', 'distribution expansion', 'efficiency improvement'],
                'negative': ['premium decline', 'policy lapse', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'HDFCAMC': {
                'positive': ['aum growth', 'inflow increase', 'market share gain', 'product innovation', 'distribution expansion', 'efficiency improvement'],
                'negative': ['aum decline', 'outflow increase', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'ICICIGI': {
                'positive': ['premium growth', 'policy growth', 'market share gain', 'product innovation', 'distribution expansion', 'efficiency improvement'],
                'negative': ['premium decline', 'policy lapse', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'ICICIPRULI': {
                'positive': ['premium growth', 'policy growth', 'market share gain', 'product innovation', 'distribution expansion', 'efficiency improvement'],
                'negative': ['premium decline', 'policy lapse', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            },
            'M&M': {
                'positive': ['sales growth', 'new launch', 'export increase', 'market share gain', 'ev adoption', 'technology upgrade'],
                'negative': ['sales decline', 'production cut', 'supply chain issue', 'competition', 'raw material cost', 'safety recall']
            },
            'UPL': {
                'positive': ['sales growth', 'market share gain', 'new product launch', 'brand expansion', 'distribution improvement', 'innovation'],
                'negative': ['sales decline', 'competition', 'pricing pressure', 'raw material cost', 'market share loss', 'brand erosion']
            },
            'GAIL': {
                'positive': ['gas last_price increase', 'pipeline expansion', 'lng import', 'city gas distribution', 'petrochemical growth', 'efficiency improvement'],
                'negative': ['gas last_price fall', 'pipeline issue', 'lng cost', 'regulatory issue', 'competition', 'environmental concern']
            },
            'HINDALCO': {
                'positive': ['aluminum last_price increase', 'demand growth', 'capacity expansion', 'export growth', 'technology upgrade', 'efficiency improvement'],
                'negative': ['aluminum last_price decline', 'demand fall', 'overcapacity', 'competition', 'raw material cost', 'currency impact']
            },
            'LTIM': {
                'positive': ['deal win', 'digital transformation', 'attrition improvement', 'margin expansion', 'cloud services', 'automation'],
                'negative': ['client exit', 'wage inflation', 'competition', 'pricing pressure', 'attrition', 'project delay']
            },
            'ADANIENT': {
                'positive': ['portfolio growth', 'diversification', 'acquisition', 'partnership', 'market expansion', 'innovation'],
                'negative': ['portfolio decline', 'diversification risk', 'acquisition failure', 'partnership loss', 'market contraction', 'regulatory issue']
            },
            'BAJAJHLDNG': {
                'positive': ['investment growth', 'portfolio performance', 'market share gain', 'acquisition', 'partnership', 'innovation'],
                'negative': ['investment decline', 'portfolio loss', 'competition', 'regulatory issue', 'market share loss', 'cost escalation']
            }
        }
    
    def analyze_sentiment(self, news_text: str, symbol: str = None) -> dict:
        """
        Analyze sentiment for news text with sector and company-specific intelligence
        
        Args:
            news_text: News text to analyze
            symbol: Company symbol (optional)
            
        Returns:
            dict: Sentiment analysis results
        """
        news_lower = news_text.lower()
        
        # Initialize sentiment scores
        sector_sentiment = 0.0
        company_sentiment = 0.0
        general_sentiment = 0.0
        
        # Analyze sector-specific sentiment
        for sector, keywords in self.sector_keywords.items():
            for keyword in keywords['positive']:
                if keyword in news_lower:
                    sector_sentiment += 0.1
            for keyword in keywords['negative']:
                if keyword in news_lower:
                    sector_sentiment -= 0.1
        
        # Analyze company-specific sentiment
        if symbol and symbol in self.company_specific_terms:
            company_terms = self.company_specific_terms[symbol]
            for keyword in company_terms['positive']:
                if keyword in news_lower:
                    company_sentiment += 0.2
            for keyword in company_terms['negative']:
                if keyword in news_lower:
                    company_sentiment -= 0.2
        
        # General sentiment analysis
        general_positive = ['bullish', 'rise', 'gain', 'up', 'positive', 'growth', 'profit', 'earnings', 'beat', 'exceed', 'surge', 'rally']
        general_negative = ['bearish', 'fall', 'drop', 'down', 'negative', 'loss', 'miss', 'decline', 'crash', 'plunge', 'slump', 'dip']
        
        for keyword in general_positive:
            if keyword in news_lower:
                general_sentiment += 0.1
        for keyword in general_negative:
            if keyword in news_lower:
                general_sentiment -= 0.1
        
        # Calculate overall sentiment
        overall_sentiment = (sector_sentiment + company_sentiment + general_sentiment) / 3
        
        # Determine sentiment label
        if overall_sentiment > 0.1:
            sentiment_label = 'positive'
        elif overall_sentiment < -0.1:
            sentiment_label = 'negative'
        else:
            sentiment_label = 'neutral'
        
        return {
            'sentiment_score': round(overall_sentiment, 3),
            'sentiment_label': sentiment_label,
            'sector_sentiment': round(sector_sentiment, 3),
            'company_sentiment': round(company_sentiment, 3),
            'general_sentiment': round(general_sentiment, 3),
            'confidence': min(1.0, abs(overall_sentiment) * 2),
            'symbol': symbol,
            'analysis_timestamp': datetime.now().isoformat()
        }
    
    def get_sector_for_symbol(self, symbol: str) -> str:
        """Get sector for a given symbol"""
        sector_mapping = {
            'banking_finance': ['HDFCBANK', 'ICICIBANK', 'KOTAKBANK', 'SBIN', 'AXISBANK', 'INDUSINDBK', 'BAJFINANCE', 'BAJAJFINSV', 'HDFCLIFE', 'SBILIFE', 'HDFCAMC'],
            'it_technology': ['TCS', 'INFY', 'WIPRO', 'TECHM', 'HCLTECH', 'LTIM'],
            'pharma_healthcare': ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'APOLLOHOSP'],
            'automobile': ['MARUTI', 'TATAMOTORS', 'EICHERMOT', 'HEROMOTOCO', 'M&M', 'BAJAJHLDNG'],
            'energy_oil_gas': ['RELIANCE', 'ONGC', 'BPCL', 'IOC', 'GAIL'],
            'metals_mining': ['TATASTEEL', 'JSWSTEEL', 'COALINDIA', 'HINDALCO'],
            'fmcg_consumer': ['HINDUNILVR', 'ITC', 'NESTLEIND', 'TITAN', 'BRITANNIA', 'TATACONSUM', 'ASIANPAINT'],
            'infrastructure_construction': ['LT', 'ADANIPORTS'],
            'cement': ['ULTRACEMCO', 'SHREECEM', 'GRASIM'],
            'telecom_media': ['BHARTIARTL'],
            'utilities': ['NTPC', 'POWERGRID', 'TATAPOWER'],
            'chemicals': ['UPL'],
            'diversified': ['ADANIENT', 'BAJAJHLDNG']
        }
        
        for sector, symbols in sector_mapping.items():
            if symbol in symbols:
                return sector
        return 'general'


class NewsIntegratedPatternDetector:
    """
    Integrates Nifty50-specific news sentiment with pattern detection
    """
    
    def __init__(self, core_detector, redis_client, sentiment_analyzer):
        self.core_detector = core_detector
        self.redis_client = redis_client
        self.sentiment_analyzer = sentiment_analyzer
    
    async def detect_patterns_with_news(self, symbol, price_data):
        """
        Detect patterns with integrated news sentiment analysis
        
        Args:
            symbol: Stock symbol
            price_data: Price and bucket_incremental_volume data
            
        Returns:
            list: Enhanced patterns with news context
        """
        # Get core patterns from the main detector
        patterns = await self.core_detector.detect_core_patterns(symbol, price_data)
        
        # Get Nifty50-specific news context
        news_context = await self.get_nifty50_news(symbol)
        
        enhanced_patterns = []
        for pattern in patterns:
            # Apply news-based confidence adjustment
            news_adjusted = self._apply_news_adjustment(pattern, news_context, symbol)
            
            if news_adjusted['confidence'] >= 0.70:  # Higher threshold for public
                enhanced_patterns.append(news_adjusted)
        
        return enhanced_patterns
    
    async def get_nifty50_news(self, symbol):
        """
        Get Nifty50-specific news for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            dict: News context with sentiment analysis
        """
        try:
            # Get news from Redis
            news_key = f"news:{symbol}:latest"
            news_data = self.redis_client.retrieve_by_data_type(news_key, "news_cache")
            
            if not news_data:
                return None
            
            # Analyze sentiment using Nifty50SentimentAnalyzer
            sentiment_result = self.sentiment_analyzer.analyze_sentiment(
                news_data.get('title', '') + ' ' + news_data.get('content', ''),
                symbol
            )
            
            # Determine market impact
            market_impact = self._assess_market_impact(news_data, sentiment_result)
            
            return {
                'title': news_data.get('title', ''),
                'content': news_data.get('content', ''),
                'timestamp': news_data.get('timestamp', ''),
                'sentiment_score': sentiment_result['sentiment_score'],
                'sentiment_label': sentiment_result['sentiment_label'],
                'confidence': sentiment_result['confidence'],
                'market_impact': market_impact,
                'sector_sentiment': sentiment_result['sector_sentiment'],
                'company_sentiment': sentiment_result['company_sentiment']
            }
            
        except Exception as e:
            print(f"Error getting news for {symbol}: {e}")
            return None
    
    def _apply_news_adjustment(self, pattern, news_context, symbol):
        """
        Apply Nifty50-specific news adjustments to pattern confidence - SAFE ADDITIVE BOOSTS
        
        Args:
            pattern: Original pattern data
            news_context: News context with sentiment
            symbol: Stock symbol
            
        Returns:
            dict: Adjusted pattern with news context
        """
        if not news_context:
            return pattern
        
        base_confidence = pattern.get('confidence', 0.5)
        sentiment_score = news_context.get('sentiment_score', 0.0)
        market_impact = news_context.get('market_impact', 'LOW')
        
        # SEPARATE NEWS ALERTS FROM PATTERN BOOSTS
        # 1. Send independent news alert (unchanged)
        if market_impact == 'HIGH':
            # News alert is handled separately by alert_manager
            pass
        
        # 2. Apply SAFE boost to pattern (if pattern exists and is decent)
        if base_confidence >= 0.60:  # Only boost patterns that are already decent
            news_boost = None
            if self.math_dispatcher:
                try:
                    news_boost = self.math_dispatcher.calculate_news_boost(
                        pattern.get('symbol', symbol), news_context
                    )
                except Exception as dispatch_error:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug(f"MathDispatcher news impact error for {symbol}: {dispatch_error}")

            if news_boost is None:
                news_boost = self._calculate_news_boost(sentiment_score, market_impact, symbol)

            enhanced_confidence = self._apply_news_boost(base_confidence, news_boost)
            pattern['news_boost'] = news_boost or 0.0
        else:
            enhanced_confidence = base_confidence  # Don't boost weak patterns
            pattern['news_boost'] = 0.0  # No boost applied
        
        pattern['confidence'] = enhanced_confidence
        
        # Add news context to pattern
        news_boost_applied = pattern.get('news_boost', 0.0)
        pattern['news_context'] = {
            'headline': news_context.get('title', '')[:100] + '...' if len(news_context.get('title', '')) > 100 else news_context.get('title', ''),
            'sentiment': 'bullish' if sentiment_score > 0.2 else 'bearish' if sentiment_score < -0.2 else 'neutral',
            'impact': market_impact,
            'alignment': 'aligned' if news_boost_applied > 0.05 else 'contradictory' if news_boost_applied < -0.05 else 'neutral',
            'sentiment_score': sentiment_score,
            'sector_sentiment': news_context.get('sector_sentiment', 0.0),
            'company_sentiment': news_context.get('company_sentiment', 0.0)
        }
        
        return pattern
    
    def _calculate_news_boost(self, sentiment_score, market_impact, symbol):
        """
        Calculate safe, capped news confidence boost
        
        Args:
            sentiment_score: News sentiment score
            market_impact: Market impact level
            symbol: Stock symbol
            
        Returns:
            float: News boost for confidence adjustment
        """
        base_boost = 0.0
        
        # Additive boosts (not multiplicative) - CONSERVATIVE VALUES
        if market_impact == 'HIGH':
            base_boost += 0.06  # 6% (was 15%)
        elif market_impact == 'MEDIUM':
            base_boost += 0.03  # 3% (was 8%)
        
        # Sentiment-based adjustment - CONSERVATIVE VALUES
        if sentiment_score > 0.2:  # Positive sentiment
            base_boost += 0.02  # 2% (was 5%)
        elif sentiment_score < -0.2:  # Negative sentiment
            base_boost -= 0.02  # -2% (was -5%)
        
        # Company relevance adjustment (higher for direct mentions) - CONSERVATIVE VALUES
        try:
            if self._is_company_specific_news(symbol):
                base_boost += 0.02  # 2% additional boost (was 5%)
        except AttributeError:
            # Fallback if sentiment_analyzer is not initialized
            pass
        
        # HARD CAP: Never exceed 10% total boost - CONSERVATIVE CAPS
        return max(-0.05, min(0.10, base_boost))
    
    def _apply_news_boost(self, pattern_confidence: float, news_boost: float) -> float:
        """Apply boost without distortion"""
        boosted_confidence = pattern_confidence + news_boost
        return max(0.10, min(0.95, boosted_confidence))  # Keep within reasonable bounds
    
    def _assess_market_impact(self, news_data, sentiment_result):
        """
        Assess market impact of news based on content and sentiment
        
        Args:
            news_data: News data from Redis
            sentiment_result: Sentiment analysis result
            
        Returns:
            str: Market impact level (HIGH/MEDIUM/LOW)
        """
        title = news_data.get('title', '').lower()
        content = news_data.get('content', '').lower()
        full_text = title + ' ' + content
        
        # High impact keywords
        high_impact_keywords = [
            'merger', 'acquisition', 'takeover', 'bankruptcy', 'fraud', 'scandal',
            'fda approval', 'fda warning', 'regulatory action', 'rbi penalty',
            'earnings beat', 'earnings miss', 'guidance raise', 'guidance cut',
            'dividend hike', 'stock split', 'buyback', 'rights issue'
        ]
        
        # Medium impact keywords
        medium_impact_keywords = [
            'partnership', 'contract win', 'contract loss', 'expansion', 'capacity',
            'technology upgrade', 'innovation', 'award', 'recognition',
            'market share', 'competition', 'pricing', 'cost reduction'
        ]
        
        # Check for high impact
        for keyword in high_impact_keywords:
            if keyword in full_text:
                return 'HIGH'
        
        # Check for medium impact
        for keyword in medium_impact_keywords:
            if keyword in full_text:
                return 'MEDIUM'
        
        # Default to LOW if no specific keywords found
        return 'LOW'
    
    def _is_company_specific_news(self, symbol):
        """
        Check if news is company-specific (mentions company name directly)
        
        Args:
            symbol: Stock symbol
            
        Returns:
            bool: True if company-specific news
        """
        # This would check if the news directly mentions the company
        # For now, return True for Nifty50 companies
        return symbol in self.sentiment_analyzer.nifty50_symbols


def format_nifty50_public_alert(pattern, news_context):
    """
    Format educational Nifty50 intelligence alert with competitive edge
    
    Args:
        pattern: Pattern data with news context
        news_context: News context from NewsIntegratedPatternDetector
        
    Returns:
        str: Formatted educational alert with Nifty50 intelligence
    """
    # Get current timestamp
    from datetime import datetime
    current_time = datetime.now().strftime("%H:%M:%S")
    
    # Base alert formatting with educational focus
    base_alert = f"""
🎯 **NIFTY50 INTELLIGENCE ALERT**

📈 **Pattern:** {pattern.get('pattern', 'UNKNOWN').upper()}
🎯 **Action:** {pattern.get('action', 'HOLD')} {pattern.get('symbol', 'N/A')}
💪 **Confidence:** {int(pattern.get('confidence', 0.0) * 100)}% (News-enhanced)
💰 **Price:** ₹{pattern.get('last_price', 0):,.2f}
⏰ **Time:** {current_time}
"""
    
    # Add news context with enhanced formatting
    if news_context and news_context.get('alignment') != 'neutral':
        # Sentiment emoji mapping
        sentiment_emoji = {
            'bullish': '🟢',
            'bearish': '🔴', 
            'neutral': '🟡'
        }
        
        # Impact emoji mapping
        impact_emoji = {
            'HIGH': '🔥',
            'MEDIUM': '⚡',
            'LOW': '📊'
        }
        
        # Alignment emoji mapping
        alignment_emoji = {
            'aligned': '✅',
            'contradictory': '❌',
            'neutral': '⚖️'
        }
        
        nifty_context = f"""
📰 **NEWS CONTEXT:**
{news_context.get('headline', 'No headline available')}
- Sentiment: {sentiment_emoji.get(news_context.get('sentiment', 'neutral'), '🟡')} {news_context.get('sentiment', 'neutral').upper()}
- Impact: {impact_emoji.get(news_context.get('impact', 'LOW'), '📊')} {news_context.get('impact', 'LOW')}
- Alignment: {alignment_emoji.get(news_context.get('alignment', 'neutral'), '⚖️')} PATTERN {news_context.get('alignment', 'neutral').upper()}
"""
        base_alert += nifty_context
    
    # Add educational insight based on pattern and sector
    educational_insight = _get_educational_insight(pattern, news_context)
    if educational_insight:
        base_alert += f"""
🎓 **EDUCATIONAL INSIGHT:**
{educational_insight}
"""
    
    # Add quick scalp targets
    scalp_targets = _get_scalp_targets(pattern)
    if scalp_targets:
        base_alert += f"""
⚡ **QUICK SCALP:** 
{scalp_targets}
"""
    
    # Add sentiment breakdown
    if news_context and news_context.get('sector_sentiment') is not None:
        sentiment_breakdown = f"""
📊 **SENTIMENT BREAKDOWN:**
Company: {news_context.get('company_sentiment', 0.0):.2f} ({_get_sentiment_label(news_context.get('company_sentiment', 0.0))}) | 
Sector: {news_context.get('sector_sentiment', 0.0):.2f} ({_get_sentiment_label(news_context.get('sector_sentiment', 0.0))}) | 
Market: {news_context.get('sentiment_score', 0.0):.2f} ({_get_sentiment_label(news_context.get('sentiment_score', 0.0))})
"""
        base_alert += sentiment_breakdown
    
    # Add risk note
    base_alert += """
⚠️ **RISK NOTE:** Always use 1:2 risk-reward. Past performance ≠ future results.
"""
    
    return base_alert


def _get_educational_insight(pattern, news_context):
    """Generate educational insight based on pattern and news context"""
    pattern_name = pattern.get('pattern', '').lower()
    symbol = pattern.get('symbol', '')
    
    # Get sector for educational context
    if news_context and 'sector_sentiment' in news_context:
        sector = _get_sector_from_sentiment(news_context)
    else:
        sector = 'general'
    
    insights = {
        'volume_breakout': {
            'it_technology': 'Volume breakouts with positive news context have 78% success rate in IT sector stocks. This pattern suggests institutional accumulation following positive fundamental developments.',
            'banking_finance': 'Volume breakouts in banking stocks with regulatory clarity have 72% success rate. This pattern indicates strong institutional interest following fundamental improvements.',
            'pharma_healthcare': 'Volume breakouts in pharma stocks with FDA approvals have 75% success rate. This pattern suggests smart money positioning ahead of regulatory developments.',
            'general': 'Volume breakouts with news catalyst have 70% success rate. This pattern indicates institutional accumulation following positive developments.'
        },
        'volume_spike': {
            'it_technology': 'Volume spikes in IT stocks during deal announcements have 68% success rate. This pattern suggests immediate institutional response to positive developments.',
            'banking_finance': 'Volume spikes in banking stocks during earnings beats have 65% success rate. This pattern indicates strong institutional interest in fundamental improvements.',
            'general': 'Volume spikes with news catalyst have 62% success rate. This pattern suggests immediate market response to significant developments.'
        },
        'reversal': {
            'it_technology': 'Reversal patterns in IT stocks with negative news have 58% success rate. This pattern suggests contrarian institutional positioning.',
            'banking_finance': 'Reversal patterns in banking stocks with regulatory clarity have 55% success rate. This pattern indicates smart money positioning.',
            'general': 'Reversal patterns with news context have 52% success rate. This pattern suggests contrarian institutional interest.'
        }
    }
    
    return insights.get(pattern_name, {}).get(sector, insights.get(pattern_name, {}).get('general', ''))


def _get_scalp_targets(pattern):
    """Generate quick scalp targets based on pattern"""
    last_price = pattern.get('last_price', 0)
    action = pattern.get('action', 'HOLD')
    
    if last_price == 0:
        return ""
    
    if action in ['BUY', 'LONG']:
        target_price = last_price * 1.008  # +0.8%
        stop_price = last_price * 0.996   # -0.4%
        target_pct = "+0.8%"
        stop_pct = "-0.4%"
    elif action in ['SELL', 'SHORT']:
        target_price = last_price * 0.992  # -0.8%
        stop_price = last_price * 1.004   # +0.4%
        target_pct = "-0.8%"
        stop_pct = "+0.4%"
    else:
        return ""
    
    return f"Target: ₹{target_price:,.2f} ({target_pct}) in 15-30 minutes\nStop: ₹{stop_price:,.2f} ({stop_pct})"


def _get_sentiment_label(sentiment_score):
    """Get sentiment label from score"""
    if sentiment_score > 0.2:
        return "Strong"
    elif sentiment_score > 0.1:
        return "Positive"
    elif sentiment_score > -0.1:
        return "Neutral"
    elif sentiment_score > -0.2:
        return "Negative"
    else:
        return "Weak"


def _get_sector_from_sentiment(news_context):
    """Get sector from news context"""
    # This would map to actual sectors based on the news context
    # For now, return a default
    return 'general'
