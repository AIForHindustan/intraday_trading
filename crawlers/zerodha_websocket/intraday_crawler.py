import json
import logging
import struct
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from crawlers.base_crawler import BaseCrawler, CrawlerConfig
from crawlers.websocket_message_parser import ZerodhaWebSocketMessageParser
from config.utils.timestamp_normalizer import TimestampNormalizer
from crawlers.metadata_resolver import metadata_resolver
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
    resolve_calculated_field as resolve_indicator_field,
)

logger = logging.getLogger(__name__)


class IntradayCrawler(BaseCrawler):
    """
    Intraday Trading Crawler (Crawler 1)

    Purpose:
    --------
    Specialized crawler for real-time intraday trading that:
    - Connects to Zerodha WebSocket in full mode
    - Publishes parsed tick data to Redis for real-time pattern detection
    - Writes Parquet files to disk for historical analysis (DuckDB compatible)
    - Optimized for low-latency processing

    Dependent Scripts:
    -----------------
    - crawlers/metadata_resolver.py: Provides instrument metadata resolution
    - crawlers/websocket_message_parser.py: WebSocket message parsing
    - core/data/redis_storage.py: Redis data storage
    - config/schemas.py: Data normalization and field mapping
    - utils/yaml_field_loader.py: Field name standardization

    Important Aspects:
    -----------------
    - Uses metadata_resolver for centralized instrument metadata
    - Publishes to Redis DB 1 (realtime - consolidated) for real-time access
    - Writes Parquet files to crawlers/raw_data/intraday_data directory (DuckDB compatible)
    - Integrates with real-time pattern detection system
    - Handles 218+ intraday data files
    - Supports field mapping standardization
    - Optimized for low-latency processing
    - Optimized buffer size (500 records) for 176 instruments
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        instrument_info: Dict[int, Dict],
        data_directory: str = "crawlers/raw_data/intraday_data",
        websocket_url: str = "wss://ws.kite.trade",
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 1,  # Use realtime database (DB 1) for tick data (consolidated from DB 4)
        name: str = "intraday_crawler",
    ):
        """
        Initialize Intraday Crawler

        Args:
            api_key: Zerodha API key
            access_token: Zerodha access token
            instruments: List of instrument tokens to subscribe to
            instrument_info: Mapping of token -> instrument details
            data_directory: Directory to store Parquet data files
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

        # Initialize data directory
        self.data_directory = Path(data_directory)
        self.data_directory.mkdir(parents=True, exist_ok=True)

        # Parquet writing state
        self.buffer = []
        self.buffer_size = 500  # Optimized for 176 instruments (low-latency real-time)
        self.parquet_writer = None
        self._records_written = 0

        # Configure crawler for intraday trading
        config = CrawlerConfig(
            name=name,
            websocket_url=websocket_url,
            tokens=instruments,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            max_reconnect_attempts=10,  # More aggressive reconnection for intraday
            reconnect_delay=1.0,  # Faster reconnection
            heartbeat_interval=15.0,  # More frequent heartbeats
            connection_timeout=10.0,
            max_threads=20,  # More threads for high-frequency data
            buffer_size=500,  # Smaller buffer for lower latency
            enable_compression=True,
        )

        super().__init__(config)

        # Initialize parser for full mode data (after redis_client is available)
        self.parser = ZerodhaWebSocketMessageParser(instrument_info)
        if getattr(self, "redis_client", None):
            self.parser.redis_client = self.redis_client
        
        # Initialize Redis storage layer
        from redis_files.redis_storage import RedisStorage
        self.redis_storage = RedisStorage(self.redis_client)
        
        # Initialize metadata resolver for symbol resolution
        self.metadata_resolver = metadata_resolver
        
        # Initialize field mapping manager for standardized field names
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field

        # Intraday-specific state tracking
        self._subscription_sent = False
        self._binary_message_count = 0
        self._tick_count = 0
        self._last_heartbeat_time = time.time()
        self._start_time = time.time()
        self._last_tick_time = time.time()

        logger.info(
            f"Initialized Intraday Crawler for {len(instruments)} instruments (Redis publishing + Parquet writing to {self.data_directory})"
        )

    def _subscribe_to_tokens(self):
        """Subscribe to instruments in full mode for intraday trading"""
        try:
            if not self.websocket:
                logger.error("WebSocket not connected, cannot subscribe")
                return

            # Prepare subscription message (defaults to quote mode)
            subscription_message = {
                "a": "subscribe",  # action: subscribe
                "v": self.instruments,  # tokens to subscribe
            }

            # Send subscription message
            self.websocket.send(json.dumps(subscription_message))

            # Prepare full mode message for all instruments
            mode_message = {
                "a": "mode",  # action: set mode
                "v": ["full", self.instruments],  # full mode for all instruments
            }

            # Send mode message to set full mode
            self.websocket.send(json.dumps(mode_message))

            self._subscription_sent = True

            # Initialize Parquet buffer
            self.buffer = []

            logger.info(
                f"Intraday Crawler subscribed to {len(self.instruments)} instruments in full mode"
            )

            # Send initial heartbeat
            self._send_heartbeat()

        except Exception as e:
            logger.error(f"Error subscribing to tokens: {e}")
            self._subscription_sent = False

    def _process_message(self, message):
        """Process incoming WebSocket messages for intraday trading"""
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

            # Process each tick for intraday trading
            for tick in ticks:
                self._process_intraday_tick(tick)

            # Log processing stats for monitoring
            if (
                self._binary_message_count % 100 == 0
            ):  # More frequent logging for intraday
                elapsed = time.time() - self._start_time
                ticks_per_second = self._tick_count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Intraday: {self._binary_message_count} messages, "
                    f"{self._tick_count} ticks, {ticks_per_second:.1f} ticks/sec, "
                    f"{self._records_written} records written to Parquet"
                )

        except Exception as e:
            error_msg = str(e)
            if "Protocol" in error_msg or "ck_sequence" in error_msg:
                logger.warning(
                    f"⚠️ Protocol error in binary message (likely corrupt data): {error_msg[:100]}..."
                )
            else:
                logger.error(f"Error processing binary message: {e}")

    def _process_intraday_tick(self, tick_data: Dict[str, Any]):
        """Process individual tick data for intraday trading"""
        try:
            self._tick_count += 1
            self._last_tick_time = time.time()

            # Add intraday-specific metadata
            tick_data.update(
                {
                    "processed_timestamp": time.time(),
                    "crawler_name": self.config.name,
                    "tick_sequence": self._tick_count,
                    "source": "intraday_crawler",
                }
            )

            if self.redis_client:
                exchange_timestamp_dt = tick_data.get("exchange_timestamp")
                if hasattr(exchange_timestamp_dt, "isoformat"):
                    timestamp_str = exchange_timestamp_dt.isoformat()
                else:
                    timestamp_str = str(exchange_timestamp_dt or "")

                # Use canonical field names from optimized_field_mapping.yaml
                optimized_tick = {
                    "symbol": tick_data.get("symbol"),
                    "timestamp": timestamp_str,
                    "timestamp_epoch": tick_data.get("exchange_timestamp_epoch"),
                    "last_price": tick_data.get("last_price"),
                    # Canonical volume fields - use the fields already set by WebSocket parser
                    "zerodha_cumulative_volume": tick_data.get("zerodha_cumulative_volume", 0),
                    "bucket_cumulative_volume": tick_data.get("bucket_cumulative_volume", 0),
                    "bucket_incremental_volume": tick_data.get("bucket_incremental_volume", 0),
                    "zerodha_last_traded_quantity": tick_data.get("zerodha_last_traded_quantity", 0),
                    # OHLC data
                    "open": tick_data.get("ohlc", {}).get("open"),
                    "high": tick_data.get("ohlc", {}).get("high"),
                    "low": tick_data.get("ohlc", {}).get("low"),
                }

                try:
                    self.redis_client.store_ticks_optimized(
                        tick_data.get("symbol"), [optimized_tick]
                    )
                except Exception as exc:
                    logger.debug(
                        "Failed to store optimized ticks for %s: %s",
                        tick_data.get("symbol"),
                        exc,
                    )

            # Publish to Redis for real-time pattern detection
            self._publish_to_redis(tick_data)

            # Write to Parquet for historical analysis
            self._write_to_parquet(tick_data)

            # Log important ticks for intraday monitoring
            self._log_intraday_tick(tick_data)

        except Exception as e:
            error_msg = str(e)
            if "Protocol" in error_msg or "ck_sequence" in error_msg:
                logger.warning(
                    f"⚠️ Protocol error in intraday tick (likely corrupt data): {error_msg[:100]}..."
                )
            else:
                logger.error(f"Error processing intraday tick: {e}")

    def _publish_to_redis(self, tick_data: Dict[str, Any]):
        """Publish tick data to Redis streams for intraday pattern detection"""
        try:
            # Publish to raw binary stream for binary decoder
            self._publish_to_binary_stream(tick_data)

            # Publish to processed tick stream for pattern detection
            self._publish_to_processed_stream(tick_data)

            # Publish to intraday-specific stream
            self._publish_to_intraday_stream(tick_data)

        except Exception as e:
            error_msg = str(e)
            if "Protocol" in error_msg or "ck_sequence" in error_msg:
                logger.warning(
                    f"⚠️ Protocol error publishing to Redis (likely corrupt data): {error_msg[:100]}..."
                )
            else:
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
                # Base64-encode binary for stream field safety
                try:
                    import base64
                    if isinstance(binary_data, bytes):
                        encoded = base64.b64encode(binary_data).decode("utf-8")
                    else:
                        encoded = str(binary_data)
                except Exception:
                    encoded = str(binary_data)

                fields = {
                    "binary_data": encoded,
                    "instrument_token": str(tick_data.get("instrument_token", 0)),
                    "timestamp": str(time.time()),
                    "mode": tick_data.get("mode", "unknown"),
                    "source": "intraday_crawler",
                }

                # Append to Redis Stream in DB 1 (realtime)
                realtime_client = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') else self.redis_client
                realtime_client.xadd(stream_key, fields)

        except Exception as e:
            logger.error(f"Error publishing to binary stream: {e}")

    def _publish_to_processed_stream(self, tick_data: Dict[str, Any]):
        """Publish processed tick data for pattern detection"""
        try:
            # Check if redis_client is available
            if not self.redis_client:
                logger.warning("⚠️ Redis client not available, skipping publish")
                return
            
            # Remove binary data to reduce message size
            processed_tick = {k: v for k, v in tick_data.items() if k != "raw_data"}

            # Get realtime client (DB 1) for pub/sub publishing
            if hasattr(self.redis_client, 'get_client'):
                realtime_client = self.redis_client.get_client(1)
            else:
                realtime_client = self.redis_client
            
            if not realtime_client:
                logger.warning("⚠️ Realtime client not available, skipping publish")
                return

            # Publish to main tick channel for pattern detectors (use pub/sub)
            subscribers = realtime_client.publish(
                "market_data.ticks",
                json.dumps(processed_tick, default=str),
            )
            
            # Log first few publishes for debugging
            if not hasattr(self, '_publish_count'):
                self._publish_count = 0
            self._publish_count += 1
            if self._publish_count <= 5 or self._publish_count % 100 == 0:
                symbol = tick_data.get('symbol', 'unknown')
                logger.debug(f"📡 Published tick {self._publish_count}: {symbol} to market_data.ticks ({subscribers} subscribers)")

            # Publish to asset-class specific channel
            asset_class = tick_data.get("asset_class", "unknown")
            realtime_client.publish(
                f"ticks:{asset_class}",
                json.dumps(processed_tick, default=str),
            )

        except Exception as e:
            logger.error(f"Error publishing to processed stream: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _publish_to_intraday_stream(self, tick_data: Dict[str, Any]):
        """Publish to intraday-specific Redis stream"""
        try:
            stream_key = "ticks:intraday:processed"

            # ✅ ONE-TIME NORMALIZATION: Convert any timestamp format to epoch milliseconds
            # Use Zerodha field names exactly as per optimized_field_mapping.yaml
            
            def _normalize_ts(raw_value):
                if not raw_value:
                    return None, ""
                ms = TimestampNormalizer.to_epoch_ms(raw_value)
                iso_value = TimestampNormalizer.to_iso_string(ms)
                return ms, iso_value

            exchange_timestamp_ms, exchange_timestamp_iso = _normalize_ts(
                tick_data.get("exchange_timestamp")
            )
            timestamp_ns_ms, timestamp_ns_iso = _normalize_ts(
                tick_data.get("timestamp_ns")
            )
            timestamp_ms, timestamp_iso = _normalize_ts(tick_data.get("timestamp"))

            if exchange_timestamp_ms is None:
                if timestamp_ns_ms is not None:
                    exchange_timestamp_ms, exchange_timestamp_iso = (
                        timestamp_ns_ms,
                        timestamp_ns_iso,
                    )
                elif timestamp_ms is not None:
                    exchange_timestamp_ms, exchange_timestamp_iso = (
                        timestamp_ms,
                        timestamp_iso,
                    )

            if timestamp_ms is None:
                if timestamp_ns_ms is not None:
                    timestamp_ms, timestamp_iso = timestamp_ns_ms, timestamp_ns_iso
                elif exchange_timestamp_ms is not None:
                    timestamp_ms, timestamp_iso = (
                        exchange_timestamp_ms,
                        exchange_timestamp_iso,
                    )

            last_trade_time_ms, last_trade_time_iso = _normalize_ts(
                tick_data.get("last_trade_time")
            )

            # ✅ FIXED: Use comprehensive field mapping for Zerodha data
            from config.schemas import normalize_zerodha_tick_data
            
            # Apply comprehensive field mapping to normalize all field names
            normalized_tick = normalize_zerodha_tick_data(tick_data)
            
            # Resolve symbol from token if not available
            symbol = normalized_tick.get("symbol", "")
            if not symbol or symbol == "UNKNOWN":
                token = normalized_tick.get("instrument_token")
                if token:
                    resolved_symbol = self.metadata_resolver.token_to_symbol(token)
                    if resolved_symbol:
                        symbol = resolved_symbol
            
            # Create intraday-optimized tick data with normalized timestamps using canonical field names
            intraday_tick = {
                "symbol": symbol,
                "instrument_token": normalized_tick.get("instrument_token", 0),
                "last_price": normalized_tick.get("last_price", 0),
                # Use canonical field names from optimized_field_mapping.yaml
                "zerodha_cumulative_volume": normalized_tick.get("zerodha_cumulative_volume", 0),
                "bucket_cumulative_volume": normalized_tick.get("bucket_cumulative_volume", 0),
                "bucket_incremental_volume": normalized_tick.get("bucket_incremental_volume", 0),
                "zerodha_last_traded_quantity": normalized_tick.get("zerodha_last_traded_quantity", 0),
                "volume_ratio": normalized_tick.get("volume_ratio", 0.0),
                # Zerodha timestamp fields with normalization
                "exchange_timestamp": exchange_timestamp_iso,
                "exchange_timestamp_ms": exchange_timestamp_ms or 0,
                "timestamp_ns": tick_data.get("timestamp_ns") or "",
                "timestamp_ns_ms": timestamp_ns_ms or 0,
                "timestamp": timestamp_iso,
                "timestamp_ms": timestamp_ms or 0,
                "last_trade_time": last_trade_time_iso,
                "last_trade_time_ms": last_trade_time_ms or 0,
                "processed_timestamp": tick_data.get("processed_timestamp", time.time()),
                "mode": normalized_tick.get("mode", "unknown"),
                "best_bid_price": tick_data.get("best_bid_price", 0),
                "best_ask_price": tick_data.get("best_ask_price", 0),
                "sequence": self._tick_count,
                "is_tradable": normalized_tick.get("is_tradable", True),
                "open_interest": normalized_tick.get("open_interest", 0),
                "change": normalized_tick.get("change", 0.0),
                "net_change": normalized_tick.get("net_change", 0.0),
            }

            # Convert any remaining None values to empty strings
            for key, value in intraday_tick.items():
                if value is None:
                    intraday_tick[key] = ""

            # Store as a Redis Stream entry (JSON blob for compactness) in DB 1 (realtime)
            realtime_client = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') else self.redis_client
            realtime_client.xadd(
                stream_key,
                {"data": json.dumps(intraday_tick, default=str)},
            )

        except Exception as e:
                logger.error(f"Error publishing to intraday stream: {e}")

    def reset_session_volumes(self):
        """Reset cumulative volume tracking at market session boundaries."""
        if hasattr(self.parser, "on_market_open"):
            self.parser.on_market_open()
        elif hasattr(self.parser, "reset_volume_session"):
            self.parser.reset_volume_session()
        logger.info("Market open - volume tracking reset")

    def on_market_open(self):
        """External hook to mark market open."""
        self.reset_session_volumes()

    def on_market_close(self):
        """External hook to mark market close."""
        if hasattr(self.parser, "on_market_close"):
            self.parser.on_market_close()
        tracked = len(getattr(self.parser, "last_cumulative_volumes", {}))
        logger.info("Session ended - tracked %s symbols", tracked)

    def validate_volume_flow(self, symbol: str):
        """Validate volume flow from raw websocket to baselines."""
        raw_volume = None
        normalized_volume = None
        bucket_volume = None
        baseline = None

        # Use canonical field names for volume tracking
        if hasattr(self.parser, "last_cumulative_volumes"):
            raw_volume = self.parser.last_cumulative_volumes.get(symbol)
        if hasattr(self.parser, "last_incremental_volumes"):
            normalized_volume = self.parser.last_incremental_volumes.get(symbol)

        if hasattr(self.redis_client, "get_time_buckets"):
            try:
                buckets = self.redis_client.get_time_buckets(symbol, lookback_minutes=5)
                if buckets:
                    bucket = buckets[0]
                    # Use canonical field names for bucket volume lookup
                    bucket_volume = (
                        bucket.get("bucket_incremental_volume")
                        or bucket.get("zerodha_cumulative_volume")
                        or bucket.get("bucket_cumulative_volume")
                    )
            except Exception as exc:
                logger.debug("Volume bucket lookup failed for %s: %s", symbol, exc)

        try:
            if not hasattr(self, "_historical_baseline_helper"):
                from utils.historical_volume_baseline import HistoricalVolumeBaseline

                self._historical_baseline_helper = HistoricalVolumeBaseline(
                    self.redis_client
                )
            baseline = self._historical_baseline_helper.get_session_aware_baseline(
                symbol, datetime.now()
            )
        except Exception as exc:
            logger.debug("Baseline computation failed for %s: %s", symbol, exc)

        def _fmt(value):
            try:
                if value in (None, "", 0, 0.0):
                    return "0"
                return f"{float(value):,.0f}"
            except (TypeError, ValueError):
                return "0"

        print(f"Volume Flow Validation - {symbol}:")
        print(f"  Raw Cumulative: {_fmt(raw_volume)}")
        print(f"  Normalized Incremental: {_fmt(normalized_volume)}")
        print(f"  Redis Bucket: {_fmt(bucket_volume)}")
        print(f"  Current Baseline: {_fmt(baseline)}")

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

    def _log_intraday_tick(self, tick_data: Dict[str, Any]):
        """Log important ticks for intraday monitoring"""
        instrument_token = tick_data.get("instrument_token")
        mode = tick_data.get("mode")
        symbol = tick_data.get("symbol", f"TOKEN_{instrument_token}")

        # Log first tick for each instrument
        if self._tick_count <= len(self.instruments):
            logger.info(f"First intraday tick: {symbol} (token {instrument_token})")

        # Log mode changes
        if hasattr(self, "_last_modes"):
            last_mode = self._last_modes.get(instrument_token)
            if last_mode and last_mode != mode:
                logger.info(f"Mode change for {symbol}: {last_mode} -> {mode}")
            self._last_modes[instrument_token] = mode
        else:
            self._last_modes = {instrument_token: mode}

        # Log high-volume ticks for intraday using canonical field names
        zerodha_cumulative_volume = tick_data.get("zerodha_cumulative_volume", 0)
        bucket_incremental_volume = tick_data.get("bucket_incremental_volume", 0)
        volume = bucket_incremental_volume or zerodha_cumulative_volume
        
        if volume > 10000:  # Adjust threshold as needed
            logger.info(
                f"High volume tick: {symbol} - {volume} shares @ {tick_data.get('last_price', 0)}"
            )



    def get_intraday_status(self) -> Dict[str, Any]:
        """Get intraday-specific status metrics"""
        base_status = super().get_status()

        elapsed = time.time() - self._start_time
        ticks_per_second = self._tick_count / elapsed if elapsed > 0 else 0

        intraday_status = {
            **base_status,
            "binary_message_count": self._binary_message_count,
            "tick_count": self._tick_count,
            "ticks_per_second": round(ticks_per_second, 2),
            "subscription_sent": self._subscription_sent,
            "instruments_subscribed": len(self.instruments),
            "last_heartbeat_time": self._last_heartbeat_time,
            "time_since_last_heartbeat": time.time() - self._last_heartbeat_time,
            "uptime_seconds": round(elapsed, 2),
            "data_directory": str(self.data_directory),
            "records_written": self._records_written,
            "buffer_size": len(self.buffer),
            "buffer_capacity": self.buffer_size,
        }

        return intraday_status

    def _on_websocket_open(self, ws):
        """Override to add intraday-specific initialization"""
        super()._on_websocket_open(ws)

        # Reset intraday-specific state
        self._subscription_sent = False
        self._last_heartbeat_time = time.time()
        self._start_time = time.time()

        logger.info("Intraday WebSocket connection established")

    def _on_websocket_error(self, ws, error):
        """Override to handle intraday-specific errors"""
        super()._on_websocket_error(ws, error)

        # Reset subscription state on error
        self._subscription_sent = False

        logger.error(f"Intraday WebSocket error: {error}")

    def _on_websocket_close(self, ws, close_status_code, close_msg):
        """Override to handle WebSocket close"""
        super()._on_websocket_close(ws, close_status_code, close_msg)

        # Flush any remaining Parquet buffer on connection close
        if self.buffer:
            self._flush_buffer()

        # Reset subscription state on close
        self._subscription_sent = False

        logger.info(f"Intraday WebSocket closed: {close_status_code} - {close_msg}")

    def _write_to_parquet(self, tick_data):
        """Write tick data to Parquet with metadata"""
        try:
            # Add metadata to each tick
            metadata = metadata_resolver.get_metadata(tick_data.get('instrument_token', 0))
            tick_data_with_meta = {
                **tick_data,
                'symbol': metadata['symbol'],
                'exchange': metadata['exchange'],
                'instrument_type': metadata['instrument_type'],
                'segment': metadata['segment'],
                'tradingsymbol': metadata['tradingsymbol'],
                'processing_timestamp': datetime.now().isoformat()
            }
            
            self.buffer.append(tick_data_with_meta)
            
            # Write when buffer reaches size
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()
                
        except Exception as e:
            logger.error(f"Error writing to Parquet buffer: {e}")

    def _flush_buffer(self):
        """Flush buffer to Parquet file"""
        if not self.buffer:
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"intraday_{timestamp}.parquet"
            filepath = self.data_directory / filename
            
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Parquet
            df.to_parquet(filepath, index=False, compression='snappy')
            
            logger.info(f"✅ Written {len(self.buffer)} records to {filepath}")
            self._records_written += len(self.buffer)
            self.buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ Error writing Parquet: {e}")

    def stop(self, timeout: float = 10.0):
        """Override stop to flush remaining buffer"""
        try:
            if self.buffer:
                self._flush_buffer()
            logger.info(f"Intraday Crawler stopped. Total records written: {self._records_written}")
        except Exception as e:
            logger.error(f"Error during stop: {e}")
        finally:
            super().stop(timeout)


def create_intraday_crawler(
    api_key: str,
    access_token: str,
    instruments: List[int],
    instrument_info: Dict[int, Dict],
    **kwargs,
) -> IntradayCrawler:
    """
    Factory function to create Intraday Crawler

    Args:
        api_key: Zerodha API key
        access_token: Zerodha access token
        instruments: List of instrument tokens
        instrument_info: Instrument token mapping
        **kwargs: Additional arguments for IntradayCrawler

    Returns:
        Configured IntradayCrawler instance
    """
    return IntradayCrawler(
        api_key=api_key,
        access_token=access_token,
        instruments=instruments,
        instrument_info=instrument_info,
        **kwargs,
    )
