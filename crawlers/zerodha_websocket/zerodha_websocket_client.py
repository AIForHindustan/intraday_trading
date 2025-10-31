import json
import logging
import struct
import threading
import time
from typing import Dict, List, Any, Optional
import redis
from websocket import WebSocketApp

from crawlers.base_crawler import BaseCrawler, CrawlerConfig
from crawlers.websocket_message_parser import ZerodhaWebSocketMessageParser

logger = logging.getLogger(__name__)


class ZerodhaWebSocketClient(BaseCrawler):
    """
    Zerodha Kite WebSocket Client for full mode data capture

    Connects to wss://ws.kite.trade and subscribes to instruments in full mode
    according to Kite WebSocket documentation:
    https://kite.trade/docs/connect/v3/websocket/
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        instrument_info: Dict[int, Dict],
        websocket_url: str = "wss://ws.kite.trade",
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        name: str = "zerodha_websocket_client",
    ):
        """
        Initialize Zerodha WebSocket client

        Args:
            api_key: Zerodha API key
            access_token: Zerodha access token
            instruments: List of instrument tokens to subscribe to
            instrument_info: Mapping of token -> instrument details
            websocket_url: WebSocket endpoint URL
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database
            name: Crawler name
        """
        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.instrument_info = instrument_info

        # Configure crawler
        config = CrawlerConfig(
            name=name,
            websocket_url=websocket_url,
            tokens=instruments,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            max_reconnect_attempts=10,
            reconnect_delay=2.0,
            heartbeat_interval=30.0,
            connection_timeout=15.0,
            max_threads=10,
            buffer_size=1000,
            enable_compression=True,
        )

        super().__init__(config)

        # Debug Redis client
        logger.info(f"Redis client type: {type(self.redis_client).__name__}")
        logger.info(f"Redis client is None: {self.redis_client is None}")
        logger.info(f"Redis client bool: {bool(self.redis_client)}")

        # Initialize parser with Redis client
        self.parser = ZerodhaWebSocketMessageParser(instrument_info, self.redis_client)

        # Additional state tracking
        self._subscription_sent = False
        self._binary_message_count = 0
        self._last_heartbeat_time = time.time()

        logger.info(
            f"Initialized Zerodha WebSocket client for {len(instruments)} instruments"
        )

    def _subscribe_to_tokens(self):
        """Subscribe to instruments in full mode"""
        try:
            if not self.websocket:
                logger.error("WebSocket not connected, cannot subscribe")
                return

            # Prepare subscription message according to Kite documentation
            subscription_message = {
                "a": "subscribe",  # action: subscribe
                "v": self.instruments,  # tokens to subscribe
                "mode": "full",  # full mode for 184-byte packets
            }

            # Send subscription message
            self.websocket.send(json.dumps(subscription_message))
            self._subscription_sent = True

            logger.info(
                f"Sent subscription for {len(self.instruments)} instruments in full mode"
            )

            # Send heartbeat to keep connection alive
            self._send_heartbeat()

        except Exception as e:
            logger.error(f"Error subscribing to tokens: {e}")
            self._subscription_sent = False

    def _process_message(self, message):
        """Process incoming WebSocket messages"""
        try:
            self._binary_message_count += 1

            # Handle binary messages (tick data)
            if isinstance(message, bytes):
                self._process_binary_message(message)
            # Handle text messages (heartbeats, errors, etc.)
            elif isinstance(message, str):
                self._process_text_message(message)
            else:
                logger.warning(f"Unknown message type: {type(message)}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _process_binary_message(self, binary_data: bytes):
        """Process binary WebSocket message containing tick data"""
        try:
            # Parse the binary message using our parser
            ticks = self.parser.parse_websocket_message(binary_data)

            if not ticks:
                logger.debug("No ticks parsed from binary message")
                return

            # Process each tick
            for tick in ticks:
                self._process_tick(tick)

            # Log processing stats periodically
            if self._binary_message_count % 1000 == 0:
                logger.info(f"Processed {self._binary_message_count} binary messages")

        except Exception as e:
            logger.error(f"Error processing binary message: {e}")

    def _process_tick(self, tick_data: Dict[str, Any]):
        """Process individual tick data"""
        try:
            # Add metadata
            tick_data.update(
                {
                    "processed_timestamp": time.time(),
                    "crawler_name": self.config.name,
                    "message_count": self._binary_message_count,
                }
            )

            # Publish to Redis for downstream processing
            self._publish_to_redis(tick_data)

            # Log important ticks (first tick, mode changes, etc.)
            self._log_tick_if_important(tick_data)

        except Exception as e:
            logger.error(f"Error processing tick: {e}")

    def _publish_to_redis(self, tick_data: Dict[str, Any]):
        """Publish tick data to Redis streams"""
        try:
            # Publish to raw binary stream for binary decoder
            self._publish_to_binary_stream(tick_data)

            # Publish to processed tick stream for pattern detection
            self._publish_to_processed_stream(tick_data)

        except Exception as e:
            logger.error(f"Error publishing to Redis: {e}")

    def _publish_to_binary_stream(self, tick_data: Dict[str, Any]):
        """Publish to raw binary stream for binary decoder"""
        try:
            stream_key = "ticks:raw:binary"

            # Extract binary data if available, otherwise create minimal binary representation
            binary_data = tick_data.get("raw_data")
            if not binary_data and "instrument_token" in tick_data:
                # Create minimal binary representation for the binary decoder
                binary_data = struct.pack(">I", tick_data["instrument_token"])

            if binary_data:
                message_data = {
                    "binary_data": binary_data,
                    "instrument_token": str(tick_data.get("instrument_token", 0)),
                    "timestamp": str(time.time()),
                    "mode": tick_data.get("mode", "unknown"),
                }

                self.redis_client.xadd(stream_key, message_data)

        except Exception as e:
            logger.error(f"Error publishing to binary stream: {e}")

    def _publish_to_processed_stream(self, tick_data: Dict[str, Any]):
        """Publish processed tick data for pattern detection"""
        try:
            # Remove binary data to reduce message size
            processed_tick = {k: v for k, v in tick_data.items() if k != "raw_data"}

            # Publish to main tick channel
            self.redis_client.publish(
                "market_data.ticks", json.dumps(processed_tick, default=str)
            )

            # Publish to asset-class specific channel
            asset_class = tick_data.get("asset_class", "unknown")
            self.redis_client.publish(
                f"ticks:{asset_class}", json.dumps(processed_tick, default=str)
            )

        except Exception as e:
            logger.error(f"Error publishing to processed stream: {e}")

    def _process_text_message(self, text_message: str):
        """Process text WebSocket messages (heartbeats, errors, etc.)"""
        try:
            message_data = json.loads(text_message)
            message_type = message_data.get("type")

            if message_type == "order":
                logger.debug(f"Order update: {message_data}")
            elif message_type == "error":
                logger.error(f"WebSocket error: {message_data}")
            elif "ping" in text_message.lower():
                self._handle_ping(message_data)
            else:
                logger.debug(f"Unknown text message: {text_message}")

        except json.JSONDecodeError:
            # Handle non-JSON text messages (like heartbeats)
            if "ping" in text_message.lower():
                self._handle_ping({"type": "ping"})
            else:
                logger.debug(f"Non-JSON text message: {text_message}")

    def _handle_ping(self, ping_data: Dict[str, Any]):
        """Handle ping messages to keep connection alive"""
        try:
            self._last_heartbeat_time = time.time()

            # Send pong response if required
            if self.websocket and ping_data.get("type") == "ping":
                pong_message = {"type": "pong"}
                self.websocket.send(json.dumps(pong_message))

            logger.debug("Handled ping message")

        except Exception as e:
            logger.error(f"Error handling ping: {e}")

    def _send_heartbeat(self):
        """Send periodic heartbeat to keep WebSocket connection alive"""
        try:
            if self.websocket and self.state.name == "RUNNING":
                heartbeat_message = {"type": "ping"}
                self.websocket.send(json.dumps(heartbeat_message))
                logger.debug("Sent heartbeat")

        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")

    def _log_tick_if_important(self, tick_data: Dict[str, Any]):
        """Log important ticks for monitoring"""
        instrument_token = tick_data.get("instrument_token")
        mode = tick_data.get("mode")

        # Log first tick for each instrument
        if self._binary_message_count <= len(self.instruments):
            logger.info(f"First tick for token {instrument_token}: mode={mode}")

        # Log mode changes
        if hasattr(self, "_last_modes"):
            last_mode = self._last_modes.get(instrument_token)
            if last_mode and last_mode != mode:
                logger.info(
                    f"Mode change for token {instrument_token}: {last_mode} -> {mode}"
                )
            self._last_modes[instrument_token] = mode
        else:
            self._last_modes = {instrument_token: mode}

    def get_detailed_status(self) -> Dict[str, Any]:
        """Get detailed status including WebSocket-specific metrics"""
        base_status = super().get_status()

        detailed_status = {
            **base_status,
            "binary_message_count": self._binary_message_count,
            "subscription_sent": self._subscription_sent,
            "instruments_subscribed": len(self.instruments),
            "last_heartbeat_time": self._last_heartbeat_time,
            "time_since_last_heartbeat": time.time() - self._last_heartbeat_time,
        }

        return detailed_status

    def _on_websocket_open(self, ws):
        """Override to add WebSocket-specific initialization"""
        super()._on_websocket_open(ws)

        # Reset WebSocket-specific state
        self._subscription_sent = False
        self._last_heartbeat_time = time.time()

        logger.info("Zerodha WebSocket connection established")

    def _on_websocket_error(self, ws, error):
        """Override to handle WebSocket-specific errors"""
        super()._on_websocket_error(ws, error)

        # Reset subscription state on error
        self._subscription_sent = False

        logger.error(f"Zerodha WebSocket error: {error}")

    def _on_websocket_close(self, ws, close_status_code, close_msg):
        """Override to handle WebSocket close"""
        super()._on_websocket_close(ws, close_status_code, close_msg)

        # Reset subscription state on close
        self._subscription_sent = False

        logger.info(f"Zerodha WebSocket closed: {close_status_code} - {close_msg}")


def create_zerodha_websocket_client(
    api_key: str,
    access_token: str,
    instruments: List[int],
    instrument_info: Dict[int, Dict],
    **kwargs,
) -> ZerodhaWebSocketClient:
    """
    Factory function to create Zerodha WebSocket client

    Args:
        api_key: Zerodha API key
        access_token: Zerodha access token
        instruments: List of instrument tokens
        instrument_info: Instrument token mapping
        **kwargs: Additional arguments for ZerodhaWebSocketClient

    Returns:
        Configured ZerodhaWebSocketClient instance
    """
    return ZerodhaWebSocketClient(
        api_key=api_key,
        access_token=access_token,
        instruments=instruments,
        instrument_info=instrument_info,
        **kwargs,
    )
