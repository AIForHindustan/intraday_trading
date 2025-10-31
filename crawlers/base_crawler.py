import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
import redis
from websocket import WebSocketApp, WebSocketConnectionClosedException

# Import custom Redis client with store_by_data_type method
try:
    from redis_files.redis_client import RobustRedisClient

    REDIS_CLIENT_AVAILABLE = True
except ImportError:
    REDIS_CLIENT_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning(
        "RobustRedisClient not available, falling back to basic Redis client"
    )

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
    """Configuration for crawler instances"""

    name: str
    websocket_url: str
    tokens: List[int]
    redis_host: Optional[str] = None  # Optional - None means no Redis
    redis_port: int = 6379
    redis_db: int = 0
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 5.0
    heartbeat_interval: float = 30.0
    connection_timeout: float = 10.0
    max_threads: int = 5
    buffer_size: int = 1000
    enable_compression: bool = True


class BaseCrawler(ABC):
    """
    Base crawler class with robust error handling and thread management
    for Zerodha Kite WebSocket integration
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

        # Initialize components
        self._init_redis()
        self._init_websocket()

        logger.info(f"Initialized {self.config.name} crawler")

    def _init_redis(self):
        """Initialize Redis connection with error handling"""
        # Skip Redis initialization if redis_host is None (file-writing only crawlers)
        if self.config.redis_host is None:
            self.redis_client = None
            logger.info("Redis disabled - file-writing only mode")
            return
            
        try:
            # Use RobustRedisClient if available, otherwise fall back to basic Redis client
            if REDIS_CLIENT_AVAILABLE:
                self.redis_client = RobustRedisClient(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    db=self.config.redis_db,
                    health_check_interval=30,
                )
                logger.info("RobustRedisClient connection established")
                logger.info(f"Redis client type: {type(self.redis_client).__name__}")
                logger.info(
                    f"Has store_by_data_type: {hasattr(self.redis_client, 'store_by_data_type')}"
                )
            else:
                self.redis_client = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    db=self.config.redis_db,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30,
                )
                logger.info("Basic Redis connection established")
                logger.info(f"Redis client type: {type(self.redis_client).__name__}")
                logger.info(
                    f"Has store_by_data_type: {hasattr(self.redis_client, 'store_by_data_type')}"
                )

            # Test connection
            self.redis_client.ping()
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def _init_websocket(self):
        """Initialize WebSocket client"""
        self.websocket: Optional[WebSocketApp] = None

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
        """Monitor crawler health and connection status"""
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

                # Check Redis connection (only if Redis is enabled)
                if self.redis_client is not None:
                    try:
                        self.redis_client.ping()
                    except Exception as e:
                        logger.error(f"Redis heartbeat failed: {e}")
                        self._init_redis()  # Reinitialize Redis

                time.sleep(self.config.heartbeat_interval)

            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                time.sleep(5.0)  # Wait before retry

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
        """Process incoming message - to be implemented by subclasses"""
        pass

    def get_status(self) -> Dict[str, Any]:
        """Get current crawler status"""
        with self._lock:
            return {
                "name": self.config.name,
                "state": self.state.value,
                "message_count": self._message_count,
                "reconnect_attempts": self._reconnect_attempts,
                "last_message_time": self._last_message_time,
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
