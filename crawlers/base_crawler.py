# base_crawler.py - Redis 8.2 Optimized
import asyncio
import atexit
import logging
import os
import signal
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
from websocket import WebSocketApp, WebSocketConnectionClosedException

logger = logging.getLogger(__name__)


class CrawlerState(Enum):
    """Crawler operational states"""
    INITIALIZING = "initializing"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class CrawlerConfig:
    """Configuration for crawler instances - Redis 8.2 Optimized"""
    name: str
    websocket_url: str
    tokens: List[int]
    redis_host: Optional[str] = None  # Only for intraday crawler (publishes to Redis)
    redis_port: Optional[int] = None  # Only for intraday crawler (publishes to Redis)
    redis_db: int = 1  # Only for intraday crawler (DB 1 - realtime for tick data)
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 5.0
    heartbeat_interval: float = 30.0
    connection_timeout: float = 10.0
    max_threads: int = 5
    buffer_size: int = 1000
    enable_compression: bool = True
    
    def __post_init__(self):
        """Set defaults from environment variables if not provided"""
        # ✅ Only set Redis defaults if this is an intraday crawler (publishes to Redis)
        # Write-only crawlers (data_mining, research) don't need Redis config
        if "intraday" in self.name.lower():
            self.redis_host = self.redis_host or os.environ.get('REDIS_HOST', '127.0.0.1')
            self.redis_port = self.redis_port or int(os.environ.get('REDIS_PORT', '6379'))


class BaseCrawler(ABC):
    """
    Base crawler class - Redis 8.2 Optimized
    
    Design:
    - Intraday crawler: Publishes to Redis 8.2 + writes to disk
    - Data mining crawler: Writes to disk only (no Redis)
    - Research crawler: Writes to disk only (no Redis)
    
    Uses RedisManager82 for process-specific connection pools.
    
    ⚠️ TROUBLESHOOTING & DOCUMENTATION:
    - Read `data_feeding.md` for architecture overview and troubleshooting guide
    - Always read method docstrings in this class before making assumptions
    - Method signatures, parameters, return values, and data formats are documented in docstrings
    - Redis initialization, buffer configuration, and write behavior are documented in docstrings
    - Don't assume crawler behavior - check docstrings for exact implementation details
    """

    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.state = CrawlerState.INITIALIZING
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()

        # Thread management
        self._main_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._processing_thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(max_workers=config.max_threads)

        # Connection tracking
        self._reconnect_attempts = 0
        self._last_message_time = 0
        self._message_count = 0

        # ✅ Redis 8.2: Use RedisManager82 for process-specific connection pools (optional - only for intraday crawler)
        self._init_redis()
        self._init_websocket()
        self._register_cleanup()

        logger.info(f"Initialized {self.config.name} crawler with Redis 8.2")

    def _init_redis(self):
        """Initialize Redis 8.2 connection using RedisManager82
        
        Note: Only intraday_crawler will use this. Data mining and research
        crawlers will have redis_client = None (write-only mode).
        """
        # Check if this crawler should use Redis
        # Only intraday_crawler publishes to Redis
        if "intraday" not in self.config.name.lower():
            self.redis_client = None
            logger.info(f"Redis disabled for {self.config.name} (write-only mode)")
            return
        
        try:
            # ✅ CRITICAL FIX: Use centralized RedisConnectionManager singleton to prevent connection leaks
            from redis_files.redis_client import redis_manager
            
            # Get the singleton Redis client (DB 1 - unified structure)
            self.redis_client = redis_manager.get_client()
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"✅ Redis connection established for {self.config.name} using singleton (DB {self.config.redis_db})")

        except Exception as e:
            logger.error(f"Failed to connect to Redis at {self.config.redis_host}:{self.config.redis_port}: {e}")
            self.redis_client = None

    def _init_websocket(self):
        """Initialize WebSocket client"""
        self.websocket: Optional[WebSocketApp] = None
    
    def _register_cleanup(self):
        """Register cleanup handlers"""
        try:
            atexit.register(self._cleanup_connections)
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)
        except Exception as e:
            logger.warning(f"Could not register cleanup handlers: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals gracefully"""
        logger.info(f"Received signal {signum}, cleaning up {self.config.name}...")
        self._cleanup_connections()
    
    def _cleanup_connections(self):
        """Cleanup Redis connections - Redis 8.2"""
        try:
            # Close Redis connection (RedisManager82 handles connection pooling)
            if hasattr(self, 'redis_client') and self.redis_client:
                try:
                    # RedisManager82 uses connection pools, so we just clear the reference
                    # The pool will be cleaned up automatically
                    self.redis_client = None
                    logger.info(f"✅ Closed Redis connection for {self.config.name}")
                except Exception as e:
                    logger.warning(f"Error closing Redis: {e}")
            
            # Close WebSocket if open
            if hasattr(self, 'websocket') and self.websocket:
                try:
                    self.websocket.close()
                except Exception:
                    pass
                    
        except Exception as e:
            logger.warning(f"Error in _cleanup_connections: {e}")

    def start(self):
        """Start the crawler with proper thread management"""
        with self._lock:
            if self.state in [CrawlerState.RUNNING, CrawlerState.CONNECTING]:
                logger.warning(f"Crawler {self.config.name} already running")
                return

            self.state = CrawlerState.CONNECTING
            self._stop_event.clear()
            self._reconnect_event.clear()

            # Start main processing thread
            self._main_thread = threading.Thread(
                target=self._run_main_loop, name=f"{self.config.name}_main", daemon=True
            )
            self._main_thread.start()

            # Start heartbeat thread
            self._heartbeat_thread = threading.Thread(
                target=self._run_heartbeat,
                name=f"{self.config.name}_heartbeat",
                daemon=True,
            )
            self._heartbeat_thread.start()

            logger.info(f"Started {self.config.name} crawler")

    def stop(self, timeout: float = 10.0):
        """Stop the crawler gracefully"""
        with self._lock:
            if self.state in [CrawlerState.STOPPING, CrawlerState.STOPPED]:
                return

            self.state = CrawlerState.STOPPING
            self._stop_event.set()
            self._reconnect_event.set()

            # Close WebSocket connection
            if self.websocket:
                try:
                    self.websocket.close()
                except Exception as e:
                    logger.warning(f"Error closing WebSocket: {e}")

            # Shutdown executor
            self._executor.shutdown(wait=False)
            
            # Cleanup connections
            self._cleanup_connections()

            # Wait for threads to terminate
            threads_to_join = []
            if self._main_thread and self._main_thread.is_alive():
                threads_to_join.append(self._main_thread)
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                threads_to_join.append(self._heartbeat_thread)

            for thread in threads_to_join:
                thread.join(timeout=timeout)
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} did not terminate gracefully")

            self.state = CrawlerState.STOPPED
            logger.info(f"Stopped {self.config.name} crawler")

    def _run_main_loop(self):
        """Main processing loop with robust error handling"""
        logger.info(f"Starting main loop for {self.config.name}")

        while not self._stop_event.is_set():
            try:
                if self.state == CrawlerState.CONNECTING:
                    self._connect_websocket()

                elif self.state == CrawlerState.CONNECTED:
                    self.state = CrawlerState.RUNNING
                    logger.info(f"{self.config.name} entered running state")

                elif self.state == CrawlerState.RECONNECTING:
                    self._handle_reconnection()

                elif self.state == CrawlerState.ERROR:
                    self._handle_error_state()

                # Small sleep to prevent busy waiting
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                self.state = CrawlerState.ERROR
                time.sleep(1.0)  # Prevent rapid error loops

        logger.info(f"Main loop terminated for {self.config.name}")

    def _connect_websocket(self):
        """Establish WebSocket connection with timeout handling"""
        try:
            logger.info(f"Connecting to WebSocket: {self.config.websocket_url}")

            # Prepare WebSocket URL with query parameters for Zerodha authentication
            websocket_url = self.config.websocket_url
            if hasattr(self, "api_key") and hasattr(self, "access_token"):
                if self.api_key and self.access_token:
                    websocket_url = f"{self.config.websocket_url}?api_key={self.api_key}&access_token={self.access_token}"
                    logger.info(
                        f"🔐 Using WebSocket authentication: {self.api_key[:10]}...:{self.access_token[:10]}..."
                    )

            self.websocket = WebSocketApp(
                websocket_url,
                on_open=self._on_websocket_open,
                on_message=self._on_websocket_message,
                on_error=self._on_websocket_error,
                on_close=self._on_websocket_close,
            )

            # Run WebSocket in a separate thread with timeout
            def run_websocket():
                try:
                    self.websocket.run_forever(
                        ping_interval=20,
                        ping_timeout=10,
                        reconnect=5,  # Automatic reconnection attempts
                    )
                except Exception as e:
                    logger.error(f"WebSocket run_forever error: {e}")

            websocket_thread = threading.Thread(
                target=run_websocket, name=f"{self.config.name}_websocket", daemon=True
            )
            websocket_thread.start()

            # Wait for connection with timeout
            start_time = time.time()
            while (time.time() - start_time) < self.config.connection_timeout:
                if self.state == CrawlerState.CONNECTED:
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError("WebSocket connection timeout")

        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self.state = CrawlerState.RECONNECTING

    def _handle_reconnection(self):
        """Handle reconnection logic with exponential backoff"""
        if self._reconnect_attempts >= self.config.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts reached for {self.config.name}")
            self.state = CrawlerState.ERROR
            return

        delay = self.config.reconnect_delay * (2**self._reconnect_attempts)
        logger.info(
            f"Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts + 1})"
        )

        # Wait for reconnect delay or stop signal
        if self._reconnect_event.wait(delay):
            return  # Stop signal received

        self._reconnect_attempts += 1
        self.state = CrawlerState.CONNECTING

    def _handle_error_state(self):
        """Handle error state with recovery attempts"""
        logger.warning(f"{self.config.name} in error state, attempting recovery")
        time.sleep(5.0)  # Wait before recovery attempt
        self.state = CrawlerState.RECONNECTING
        self._reconnect_attempts = 0

    def _run_heartbeat(self):
        """Monitor crawler health and Redis connection"""
        while not self._stop_event.is_set():
            try:
                current_time = time.time()

                # Check message freshness
                if (
                    self.state == CrawlerState.RUNNING
                    and current_time - self._last_message_time
                    > self.config.heartbeat_interval * 2
                ):
                    logger.warning(
                        f"No messages received for {current_time - self._last_message_time:.1f}s"
                    )
                    self.state = CrawlerState.RECONNECTING

                # ✅ Redis 8.2: Check Redis connection (only if enabled)
                if self.redis_client:
                    try:
                        self.redis_client.ping()
                    except Exception as e:
                        logger.error(f"Redis heartbeat failed: {e}")
                        self._init_redis()  # Reinitialize connection

                time.sleep(self.config.heartbeat_interval)

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                time.sleep(5.0)

    def _on_websocket_open(self, ws):
        """Handle WebSocket connection open"""
        with self._lock:
            logger.info(f"WebSocket connected for {self.config.name}")
            self.state = CrawlerState.CONNECTED
            self._reconnect_attempts = 0
            self._last_message_time = time.time()

            # Subscribe to tokens
            self._subscribe_to_tokens()

    def _on_websocket_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            self._last_message_time = time.time()
            self._message_count += 1

            # Process message asynchronously
            self._executor.submit(self._process_message, message)

        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

    def _on_websocket_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error in {self.config.name}: {error}")
        self.state = CrawlerState.RECONNECTING

    def _on_websocket_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        logger.info(
            f"WebSocket closed for {self.config.name}: {close_status_code} - {close_msg}"
        )
        if not self._stop_event.is_set():
            self.state = CrawlerState.RECONNECTING

    @abstractmethod
    def _subscribe_to_tokens(self):
        """Subscribe to specific tokens - to be implemented by subclasses"""
        pass

    @abstractmethod
    def _process_message(self, message):
        """Process incoming message - to be implemented by subclasses
        
        Recommended debug pattern for subclasses to verify upstream data extraction:
        
        def _process_message(self, message):
            # Log raw message to verify nothing is lost upstream
            logger.debug(f"RAW MESSAGE: {message}")
            
            # Process message...
            tick_data = self._extract_tick_data(message)
            symbol = tick_data.get('symbol', 'UNKNOWN')
            
            # Log extracted data before Redis write
            logger.debug(f"EXTRACTED TICK [{symbol}]: keys={list(tick_data.keys())}")
            if 'strike_price' in tick_data:
                logger.debug(f"  strike_price: {tick_data.get('strike_price')}")
            if 'underlying_price' in tick_data:
                logger.debug(f"  underlying_price: {tick_data.get('underlying_price')}")
            if 'option_type' in tick_data:
                logger.debug(f"  option_type: {tick_data.get('option_type')}")
            if 'greeks' in tick_data:
                logger.debug(f"  greeks: {tick_data['greeks']}")
            
            # Write to Redis
            self.write_tick_to_redis(symbol, tick_data)
        """
        pass

    def write_tick_to_redis(self, symbol: str, tick_data: dict):
        """Write COMPLETE tick data to Redis 8.2 - Preserves all fields
        
        Enhanced version that stores complete tick data in multiple formats:
        1. Symbol-specific stream (ticks:raw:{symbol})
        2. Unified stream for all ticks (ticks:unified:raw)
        3. Latest tick hash for quick access (ticks:latest:{symbol})
        4. Real-time price metric (price:realtime:{symbol})
        
        This method preserves ALL fields from tick_data without filtering.
        """
        if not self.redis_client:
            return
        
        import json
        
        try:
            # Get underlying Redis client
            if hasattr(self.redis_client, 'redis'):
                redis_client = self.redis_client.redis
            else:
                redis_client = self.redis_client
            
            with redis_client.pipeline() as pipe:
                # 1. Store in primary tick stream (symbol-specific)
                stream_key = f"ticks:raw:{symbol}"
                stream_data = self._prepare_redis_data(tick_data)
                
                pipe.xadd(
                    stream_key,
                    stream_data,
                    maxlen=10000,  # Keep more ticks for analysis
                    approximate=True
                )
                
                # 2. Store in unified stream for all ticks
                # Convert datetime objects to ISO strings for JSON serialization
                from datetime import datetime, date
                def _json_default(obj):
                    """Default JSON encoder for non-serializable types"""
                    if isinstance(obj, (datetime, date)):
                        return obj.isoformat()
                    elif hasattr(obj, '__dict__'):
                        return obj.__dict__
                    else:
                        return str(obj)
                
                unified_stream_data = {
                    'symbol': symbol,
                    'data': json.dumps(tick_data, default=_json_default)  # Store complete data as JSON
                }
                pipe.xadd(
                    "ticks:unified:raw",
                    unified_stream_data,
                    maxlen=50000,
                    approximate=True
                )
                
                # 3. Update latest tick in hash for quick access
                latest_key = f"ticks:latest:{symbol}"
                # Convert to Redis-compatible types
                latest_data = {}
                for k, v in tick_data.items():
                    if v is not None:
                        if isinstance(v, (dict, list)):
                            latest_data[k] = json.dumps(v)
                        else:
                            latest_data[k] = str(v)
                
                pipe.delete(latest_key)  # Ensure correct type
                if latest_data:
                    pipe.hset(latest_key, mapping=latest_data)
                    pipe.expire(latest_key, 3600)  # 1 hour TTL
                
                # 4. Update real-time metrics
                if 'last_price' in tick_data and tick_data['last_price']:
                    price = float(tick_data['last_price'])
                    pipe.set(f"price:realtime:{symbol}", price, ex=60)
                
                # Execute pipeline
                pipe.execute()
                
                logger.debug(f"✅ Published COMPLETE tick for {symbol} with {len(tick_data)} fields")
                
        except Exception as e:
            logger.error(f"Redis publish error for {symbol}: {e}", exc_info=True)
    
    def _prepare_redis_data(self, tick_data: dict) -> dict:
        """Convert tick data to Redis-compatible format
        
        Preserves all fields, converting complex types to JSON strings.
        """
        import json
        
        redis_data = {}
        
        for key, value in tick_data.items():
            if value is None:
                continue
                
            if isinstance(value, (str, int, float)):
                redis_data[key] = str(value)
            elif isinstance(value, bool):
                redis_data[key] = "1" if value else "0"
            elif isinstance(value, (dict, list)):
                redis_data[key] = json.dumps(value)
            else:
                redis_data[key] = str(value)
        
        return redis_data

    def get_status(self) -> Dict[str, Any]:
        """Get current crawler status"""
        with self._lock:
            return {
                "name": self.config.name,
                "state": self.state.value,
                "message_count": self._message_count,
                "reconnect_attempts": self._reconnect_attempts,
                "last_message_time": self._last_message_time,
                "redis_connected": self.redis_client is not None,
                "threads_alive": {
                    "main": self._main_thread.is_alive()
                    if self._main_thread
                    else False,
                    "heartbeat": self._heartbeat_thread.is_alive()
                    if self._heartbeat_thread
                    else False,
                },
            }

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()

    def __del__(self):
        """Destructor for cleanup"""
        if hasattr(self, "_stop_event") and not self._stop_event.is_set():
            self.stop()
