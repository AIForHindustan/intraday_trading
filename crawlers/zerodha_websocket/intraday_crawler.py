import json
import logging
import random
import struct
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from crawlers.base_crawler import BaseCrawler, CrawlerConfig
from crawlers.websocket_message_parser import ZerodhaWebSocketMessageParser, ParserHealthMonitor
from crawlers.enhanced_tick_parser import EnhancedTickParser
from config.utils.timestamp_normalizer import TimestampNormalizer
from crawlers.metadata_resolver import metadata_resolver
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
    resolve_calculated_field as resolve_indicator_field,
)
from utils.update_all_20day_averages import correct_symbol_expiry_date

logger = logging.getLogger(__name__)


class IntradayCrawler(BaseCrawler):
    """
    Intraday Trading Crawler (Crawler 1)

    Purpose:
    --------
    Specialized crawler for real-time intraday trading that:
    - Connects to Zerodha WebSocket in full mode
    - Publishes parsed tick data to Redis 8.2 for real-time pattern detection
    - Writes Parquet files to disk for historical analysis (DuckDB compatible)
    - Optimized for low-latency processing

    Dependent Scripts:
    -----------------
    - crawlers/metadata_resolver.py: Provides instrument metadata resolution
    - crawlers/websocket_message_parser.py: WebSocket message parsing
    - redis_files/redis_storage.py: Redis data storage
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
    - Optimized buffer size (2000 records) for 176 instruments
    
    ⚠️ TROUBLESHOOTING & DOCUMENTATION:
    - Read `data_feeding.md` for architecture overview and troubleshooting guide
    - Always read method docstrings in this class before making assumptions
    - Method signatures, parameters, return values, and data formats are documented in docstrings
    - Buffer configuration, Redis publishing, and disk writing behavior are documented in docstrings
    - Don't assume crawler behavior - check docstrings for exact implementation details
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        instrument_info: Dict[int, Dict],
        data_directory: str = "crawlers/raw_data/intraday_data",
        websocket_url: str = "wss://ws.kite.trade",
        redis_host: str = None,  # Redis 8.2 host
        redis_port: int = None,  # Redis 8.2 port
        redis_db: int = 1,  # Redis database (DB 1 for realtime tick data)
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
            redis_host: Redis 8.2 host
            redis_port: Redis 8.2 port
            redis_db: Redis database (DB 1 for realtime tick data)
            name: Crawler name
        """
        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.instrument_info = instrument_info

        # Initialize data directory
        self.data_directory = Path(data_directory)
        self.data_directory.mkdir(parents=True, exist_ok=True)

        # Configure crawler for intraday trading
        # ✅ Redis 8.2: Use RedisManager82 for process-specific connection pools
        final_redis_host = redis_host or "127.0.0.1"
        final_redis_port = redis_port if redis_port is not None else 6379
        
        config = CrawlerConfig(
            name=name,
            websocket_url=websocket_url,
            tokens=instruments,
            redis_host=final_redis_host,  # Redis 8.2 host
            redis_port=final_redis_port,  # Redis 8.2 port
            redis_db=redis_db,  # DB 1 (realtime) for tick data
            max_reconnect_attempts=10,  # More aggressive reconnection for intraday
            reconnect_delay=1.0,  # Faster reconnection
            heartbeat_interval=15.0,  # More frequent heartbeats
            connection_timeout=10.0,
            max_threads=20,  # More threads for high-frequency data
            buffer_size=2000,  # ✅ UPDATED: Increased from 500 to 2000 for I/O efficiency
            enable_compression=True,
        )

        super().__init__(config)

        # Parquet writing state
        # ✅ FIXED: Use config.buffer_size instead of hardcoded 500
        self.buffer = []
        self.buffer_size = config.buffer_size  # Use config value (2000) for consistency
        self.parquet_writer = None
        self._records_written = 0

        # Ensure Redis client from BaseCrawler is available (needed for parser + downstream storage)
        if not getattr(self, "redis_client", None):
            try:
                from redis_files.redis_client import redis_manager
                self.redis_client = redis_manager.get_client()
                logger.info("✅ Intraday crawler reconnected to Redis singleton (DB 1)")
            except Exception as redis_exc:
                logger.error(f"❌ Unable to initialize Redis client for intraday crawler: {redis_exc}")
                self.redis_client = None

        # ✅ Redis 8.2: Use redis_client from base_crawler (RedisManager82)
        # BULLETPROOF PARSER: Initialize robust parser with health monitoring
        # Pass redis_client to constructor so volume_calculator is properly initialized
        parser_redis_client = getattr(self.redis_client, "redis", self.redis_client)
        self.parser = ZerodhaWebSocketMessageParser(instrument_info, parser_redis_client)
        
        # ✅ ENHANCED PARSER: Initialize enhanced tick parser for complete field preservation
        self.enhanced_parser = EnhancedTickParser(instrument_info)
        
        # Add health monitoring
        self.health_monitor = ParserHealthMonitor(self.parser)
        
        # ✅ Redis 8.2: Using write_tick_to_redis from base_crawler
        # Uses RedisManager82 for process-specific connection pools
        
        # Initialize metadata resolver for symbol resolution
        self.metadata_resolver = metadata_resolver
        
        # Initialize field mapping manager for standardized field names
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field
        
        # Initialize symbol parser for option field enrichment
        from redis_files.redis_key_standards import get_symbol_parser
        self.symbol_parser = get_symbol_parser()

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
        """Process incoming WebSocket messages with COMPLETE data extraction
        
        Enhanced version that:
        - Preserves ALL fields from Zerodha WebSocket
        - Logs data completeness for monitoring
        - Publishes complete data to Redis
        """
        try:
            self._binary_message_count += 1

            # Handle binary messages (tick data)
            if isinstance(message, bytes):
                # Process binary message with complete data extraction
                self._process_binary_message(message)
            
            # Handle text messages (heartbeats, errors, etc.)
            elif isinstance(message, str):
                self._process_text_message(message)
            else:
                logger.warning(f"Unknown message type: {type(message)}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    def _process_binary_message(self, binary_data: bytes):
        """Process binary WebSocket message with COMPLETE data extraction
        
        Enhanced version that:
        - Preserves ALL fields from Zerodha WebSocket
        - Logs data completeness for monitoring
        - Publishes complete data to Redis
        """
        try:
            # ✅ BULLETPROOF PARSER: Use health-monitored parsing
            # Parse with enhanced parser that preserves all fields
            ticks = self.health_monitor.monitor_parse_health(binary_data)

            if not ticks:
                return

            # Process each tick with complete data
            for tick_data in ticks:
                if not tick_data:
                    continue
                
                # Validate tick before processing
                if not self._validate_tick(tick_data):
                    logger.warning(f"Invalid tick: {tick_data.get('symbol', 'unknown')}")
                    continue
                
                symbol = tick_data.get('symbol', 'unknown')
                
                # Log data completeness
                field_count = len(tick_data)
                logger.info(f"📦 Processing {symbol} - {field_count} fields extracted")
                
                # Process tick with complete data
                self._process_intraday_tick(tick_data)
                
                # Optional: Log sample of what we're publishing (0.1% of ticks)
                if random.random() < 0.001:
                    sample_data = {k: v for k, v in list(tick_data.items())[:10]}
                    logger.debug(f"🧪 SAMPLE TICK: {symbol} - {sample_data}")

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
                logger.error(f"Error processing binary message: {e}", exc_info=True)
            # Don't crash - continue processing
    
    def _enrich_option_fields(self, symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich option fields from symbol if missing (strike_price, underlying_price, option_type)
        
        WebSocket packet doesn't contain these fields - must be extracted from symbol name.
        See: https://kite.trade/docs/connect/v3/market-quotes/
        """
        try:
            parsed = self.symbol_parser.parse_symbol(symbol)
            
            # If it's an option, extract and add option-specific fields
            if parsed.instrument_type == 'OPT':
                # Extract strike_price from parsed.strike
                if not tick_data.get('strike_price') or tick_data.get('strike_price') == 0:
                    if parsed.strike and parsed.strike > 0:
                        tick_data['strike_price'] = float(parsed.strike)
                        logger.info(f"✅ [CRAWLER_ENRICH] {symbol} - Added strike_price={parsed.strike}")
                    else:
                        logger.warning(f"⚠️ [CRAWLER_ENRICH] {symbol} - parsed.strike is {parsed.strike}, cannot add strike_price")
                
                # Extract option_type from parsed.option_type (CE/PE -> call/put)
                if not tick_data.get('option_type'):
                    if parsed.option_type:
                        opt_type_lower = parsed.option_type.lower()
                        if opt_type_lower in ['ce', 'c']:
                            tick_data['option_type'] = 'call'
                        elif opt_type_lower in ['pe', 'p']:
                            tick_data['option_type'] = 'put'
                        else:
                            tick_data['option_type'] = opt_type_lower
                        logger.info(f"✅ [CRAWLER_ENRICH] {symbol} - Added option_type={tick_data['option_type']}")
                    else:
                        logger.warning(f"⚠️ [CRAWLER_ENRICH] {symbol} - parsed.option_type is {parsed.option_type}, cannot add option_type")
                
                # Extract expiry_date if missing
                if not tick_data.get('expiry_date') and parsed.expiry:
                    tick_data['expiry_date'] = parsed.expiry.strftime('%Y-%m-%d')
                    logger.debug(f"[CRAWLER_ENRICH] {symbol} - Added expiry_date={tick_data['expiry_date']}")
                
                # Extract instrument_type if missing
                if not tick_data.get('instrument_type'):
                    tick_data['instrument_type'] = parsed.instrument_type
                    logger.debug(f"[CRAWLER_ENRICH] {symbol} - Added instrument_type={parsed.instrument_type}")
        
        except Exception as enrich_err:
            logger.warning(f"[CRAWLER_ENRICH] {symbol} - Failed to enrich option fields: {enrich_err}")
        
        return tick_data
    
    def _validate_tick(self, tick: Dict) -> bool:
        """Validate tick data before processing"""
        required_fields = ['symbol', 'instrument_token', 'last_price']
        
        for field in required_fields:
            if not tick.get(field):
                return False
        
        # Validate price is reasonable
        price = tick.get('last_price')
        if price is None or price <= 0 or price > 1000000:  # Adjust based on your instruments
            return False
        
        # Validate symbol is not unknown
        symbol = tick.get('symbol', '')
        if not symbol or symbol.startswith('UNKNOWN_'):
            return False
            
        return True

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

            # ✅ CRITICAL: Enrich option fields from symbol BEFORE writing to Redis
            # WebSocket packet doesn't contain strike_price, underlying_price, option_type
            # See: https://kite.trade/docs/connect/v3/market-quotes/
            symbol = tick_data.get('symbol', 'UNKNOWN')
            
            # TEMPORARY: Log tick_data keys before enrichment
            if 'CE' in symbol or 'PE' in symbol:
                logger.info(f"🔍 [ENRICH_BEFORE] {symbol} - Keys before enrichment: {list(tick_data.keys())[:20]}")
                logger.info(f"🔍 [ENRICH_BEFORE] {symbol} - strike_price={tick_data.get('strike_price')}, option_type={tick_data.get('option_type')}")
            
            tick_data = self._enrich_option_fields(symbol, tick_data)
            
            # TEMPORARY: Log tick_data keys after enrichment
            if 'CE' in symbol or 'PE' in symbol:
                logger.info(f"🔍 [ENRICH_AFTER] {symbol} - Keys after enrichment: {list(tick_data.keys())[:20]}")
                logger.info(f"🔍 [ENRICH_AFTER] {symbol} - strike_price={tick_data.get('strike_price')}, option_type={tick_data.get('option_type')}")
            
            # TEMPORARY: Validate Greek data before Redis write (expected to be missing - crawler doesn't calculate Greeks)
            greek_fields = ['delta', 'gamma', 'theta', 'vega', 'iv', 'implied_volatility', 'oi']
            missing = [g for g in greek_fields if g not in tick_data or tick_data.get(g) is None]
            if missing and ('CE' in symbol or 'PE' in symbol):
                logger.debug(f"[GREEK_CHECK] {symbol} - Missing Greeks {missing} (expected - crawler doesn't calculate)")
                # Log what Greeks are present
                present_greeks = {g: tick_data.get(g) for g in greek_fields if g in tick_data and tick_data.get(g) is not None}
                if present_greeks:
                    logger.debug(f"[GREEK_CHECK] {symbol} - Present Greeks: {present_greeks}")
            
            # ✅ Redis 8.2: Publish to Redis for real-time pattern detection
            # Use optimized write_tick_to_redis from base_crawler (RedisManager82)
            # Override with validation for intraday crawler
            self.write_tick_to_redis(symbol, tick_data)
            
            # ✅ CRITICAL: Also publish to ticks:intraday:processed stream for scanner consumption
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
            from redis_files.redis_key_standards import get_key_builder
            stream_key = get_key_builder().live_raw_binary_stream()

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
                # ✅ CRITICAL FIX: Use singleton for DB 1
                try:
                    from redis_files.redis_client import redis_manager
                    realtime_client = redis_manager.get_client()  # DB 1 singleton
                except Exception:
                    # Fallback: Use self.redis_client if it's already DB 1 singleton
                    realtime_client = self.redis_client
                
                if not realtime_client:
                    logger.warning(f"⚠️ No Redis client available for publishing to {stream_key}")
                    return
                
                # ✅ TIER 1: Keep stream trimmed for performance (last 10k ticks)
                realtime_client.xadd(stream_key, fields, maxlen=10000, approximate=True)

        except Exception as e:
            logger.error(f"Error publishing to binary stream: {e}")

    def _publish_to_processed_stream(self, tick_data: Dict[str, Any]):
        """Publish processed tick data for pattern detection"""
        try:
            # Remove binary data to reduce message size
            processed_tick = {k: v for k, v in tick_data.items() if k != "raw_data"}

            # Get realtime client (DB 1) for pub/sub publishing
            # Use process-specific client if available, otherwise use wrapper's get_client
            # ✅ CRITICAL FIX: Use singleton for DB 1
            try:
                from redis_files.redis_client import redis_manager
                realtime_client = redis_manager.get_client()  # DB 1 singleton
            except Exception:
                # Fallback: Use self.redis_client if it's already DB 1 singleton
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
            from redis_files.redis_key_standards import get_key_builder
            stream_key = get_key_builder().live_processed_stream()

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
            
            # ✅ DISABLED: Symbol expiry correction disabled to maintain consistency with JSON file format
            # The token_lookup_enriched.json uses 25DEC format, so we keep symbols as-is
            # If expiry correction is needed, it should be done at the JSON file level, not at runtime
            # if symbol and symbol != "UNKNOWN":
            #     corrected_symbol = correct_symbol_expiry_date(symbol)
            #     if corrected_symbol != symbol:
            #         logger.debug(f"🔄 Corrected symbol expiry: {symbol} -> {corrected_symbol}")
            #         symbol = corrected_symbol
            
            # Create intraday-optimized tick data with normalized timestamps using canonical field names
            # ✅ CRITICAL: Include ALL fields from Zerodha WebSocket API (184-byte full quote packet)
            # Reference: https://kite.trade/docs/connect/v3/websocket/
            intraday_tick = {
                "symbol": symbol,
                "instrument_token": normalized_tick.get("instrument_token", 0),
                # Quote packet fields (bytes 0-44)
                "last_price": normalized_tick.get("last_price", 0),
                "last_traded_quantity": tick_data.get("last_quantity") or tick_data.get("last_traded_quantity") or normalized_tick.get("zerodha_last_traded_quantity", 0),
                "average_traded_price": tick_data.get("average_price") or tick_data.get("average_traded_price") or normalized_tick.get("average_traded_price", 0),
                # Use canonical field names from optimized_field_mapping.yaml
                "zerodha_cumulative_volume": normalized_tick.get("zerodha_cumulative_volume", 0),
                "bucket_cumulative_volume": normalized_tick.get("bucket_cumulative_volume", 0),
                "bucket_incremental_volume": normalized_tick.get("bucket_incremental_volume", 0),
                "zerodha_last_traded_quantity": normalized_tick.get("zerodha_last_traded_quantity", 0),
                # ✅ CRITICAL FIX: Check tick_data first for volume_ratio (from parser), then normalized_tick as fallback
                "volume_ratio": tick_data.get("volume_ratio") or normalized_tick.get("volume_ratio", 0.0),
                # OHLC fields (bytes 28-44) - explicitly include from tick_data
                "open_price": tick_data.get("open") or (tick_data.get("ohlc", {}).get("open") if isinstance(tick_data.get("ohlc"), dict) else None) or 0,
                "high_price": tick_data.get("high") or (tick_data.get("ohlc", {}).get("high") if isinstance(tick_data.get("ohlc"), dict) else None) or 0,
                "low_price": tick_data.get("low") or (tick_data.get("ohlc", {}).get("low") if isinstance(tick_data.get("ohlc"), dict) else None) or 0,
                "close_price": tick_data.get("close") or (tick_data.get("ohlc", {}).get("close") if isinstance(tick_data.get("ohlc"), dict) else None) or 0,
                # Zerodha timestamp fields with normalization (bytes 44-48, 60-64)
                "exchange_timestamp": exchange_timestamp_iso,
                "exchange_timestamp_ms": exchange_timestamp_ms or 0,
                "last_traded_timestamp": tick_data.get("last_traded_timestamp") or 0,  # bytes 44-48
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
                # Open Interest fields (bytes 48-60)
                "open_interest": tick_data.get("open_interest") or tick_data.get("oi") or normalized_tick.get("open_interest", 0),
                "oi_day_high": tick_data.get("oi_day_high", 0),  # bytes 52-56
                "oi_day_low": tick_data.get("oi_day_low", 0),   # bytes 56-60
                "change": normalized_tick.get("change", 0.0),
                "net_change": normalized_tick.get("net_change", 0.0),
            }
            
            # ✅ CRITICAL FIX: Include option-specific fields from tick_data
            option_fields = ['strike_price', 'option_type', 'expiry_date', 'instrument_type', 'underlying_price']
            for field in option_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include Greeks if present in tick_data
            greek_fields = ['delta', 'gamma', 'theta', 'vega', 'rho', 'iv', 'implied_volatility', 'dte_years', 'trading_dte', 'expiry_series', 'option_price']
            for field in greek_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include Zerodha market depth and additional fields explicitly
            # Market depth fields (from _parse_market_depth)
            market_depth_fields = [
                'depth',  # Full order book (5 buy + 5 sell levels)
                'best_bid_quantity', 'best_bid_orders',
                'best_ask_quantity', 'best_ask_orders'
            ]
            for field in market_depth_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include Zerodha volume fields
            volume_fields = [
                'total_buy_quantity', 'total_sell_quantity',
                'average_traded_price', 'average_price'
            ]
            for field in volume_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include OI fields
            oi_fields = ['oi', 'oi_day_high', 'oi_day_low']
            for field in oi_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include circuit limit fields
            circuit_fields = ['lower_circuit_limit', 'upper_circuit_limit']
            for field in circuit_fields:
                if field in tick_data and tick_data[field] is not None:
                    intraday_tick[field] = tick_data[field]
            
            # ✅ CRITICAL FIX: Include all other fields from tick_data that aren't already included
            # This ensures nothing is lost
            # IMPORTANT: Include depth dict even if > 1000 chars (valuable order book data)
            for key, value in tick_data.items():
                if key not in intraday_tick and value is not None:
                    # For depth dict, always include (valuable order book data)
                    if key == 'depth' and isinstance(value, dict):
                        intraday_tick[key] = value
                    # For other complex objects, include if reasonable size
                    elif not isinstance(value, (dict, list)) or (isinstance(value, (dict, list)) and len(str(value)) < 2000):
                        intraday_tick[key] = value

            # Convert any remaining None values to empty strings
            for key, value in intraday_tick.items():
                if value is None:
                    intraday_tick[key] = ""

            # Store as a Redis Stream entry (JSON blob for compactness) in DB 1 (realtime)
            # ✅ CRITICAL FIX: Use singleton for DB 1
            try:
                from redis_files.redis_client import redis_manager
                realtime_client = redis_manager.get_client()  # DB 1 singleton
            except Exception:
                # Fallback: Use self.redis_client if it's already DB 1 singleton
                realtime_client = self.redis_client
            
            if not realtime_client:
                logger.warning(f"⚠️ No Redis client available for publishing to {stream_key}")
                return
            
            # ✅ DEBUG: Log what we're publishing to the stream
            current_time = time.time()
            exchange_ts_sec = exchange_timestamp_ms / 1000.0 if exchange_timestamp_ms else 0
            tick_age = current_time - exchange_ts_sec if exchange_ts_sec > 0 else 0
            
            if tick_age > 300:  # Log if older than 5 minutes
                logger.warning(f"⚠️ [CRAWLER_PUBLISH] {symbol} - Publishing STALE tick to stream: exchange_ts_ms={exchange_timestamp_ms}, age={tick_age:.1f}s")
            else:
                logger.debug(f"✅ [CRAWLER_PUBLISH] {symbol} - Publishing fresh tick to stream: age={tick_age:.1f}s")
            
            # ✅ TIER 1: Keep stream trimmed for performance (last 5k processed ticks)
            realtime_client.xadd(
                stream_key,
                {"data": json.dumps(intraday_tick, default=str)},
                maxlen=5000,
                approximate=True
            )

        except Exception as e:
            logger.error(f"Error publishing to intraday stream: {e}")
            import traceback
            logger.error(traceback.format_exc())

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
                pass

        try:
            if not hasattr(self, "_historical_baseline_helper"):
                from utils.time_aware_volume_baseline import HistoricalVolumeBaseline

                self._historical_baseline_helper = HistoricalVolumeBaseline(
                    self.redis_client
                )
            baseline = self._historical_baseline_helper.get_session_aware_baseline(
                symbol, datetime.now()
            )
        except Exception as exc:
            pass

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
                pass
            elif message_type == "error":
                logger.error(f"WebSocket error: {message_data}")
            elif "ping" in text_message.lower():
                self._handle_ping(message_data)
            else:
                pass

        except json.JSONDecodeError:
            # Handle non-JSON text messages (like heartbeats)
            if "ping" in text_message.lower():
                self._handle_ping({"type": "ping"})
            else:
                pass

    def _handle_ping(self, ping_data: Dict[str, Any]):
        """Handle ping messages to keep connection alive"""
        try:
            self._last_heartbeat_time = time.time()

            # Send pong response if required
            if self.websocket and ping_data.get("type") == "ping":
                pong_message = {"type": "pong"}
                self.websocket.send(json.dumps(pong_message))


        except Exception as e:
            logger.error(f"Error handling ping: {e}")

    def _send_heartbeat(self):
        """Send periodic heartbeat to keep WebSocket connection alive"""
        try:
            if self.websocket and self.state.name == "RUNNING":
                heartbeat_message = {"type": "ping"}
                self.websocket.send(json.dumps(heartbeat_message))

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



    def write_tick_to_redis(self, symbol: str, tick_data: dict):
        """Override base method to add data validation before writing to Redis"""
        # Validate option fields for options
        from redis_files.redis_key_standards import get_symbol_parser
        parser = get_symbol_parser()
        try:
            parsed = parser.parse_symbol(symbol)
            if parsed.instrument_type == 'OPT':
                # Validate critical option fields
                expected_option_fields = ['strike_price', 'option_type']
                missing_option_fields = [f for f in expected_option_fields if not tick_data.get(f) or tick_data.get(f) == 0]
                
                if missing_option_fields:
                    logger.warning(f"[VALIDATION] {symbol} - Missing option fields: {missing_option_fields}")
                
                # Validate Greek data before writing
                expected_greek_fields = ['delta', 'gamma', 'theta', 'vega']
                missing_greeks = [greek for greek in expected_greek_fields if greek not in tick_data or tick_data.get(greek) is None or tick_data.get(greek) == 0]
                
                if missing_greeks:
                    logger.warning(f"[VALIDATION] {symbol} - Missing Greek fields: {missing_greeks}")
                else:
                    # Log if Greeks are present but all zeros (indicates calculation issue)
                    greek_values = {g: tick_data.get(g) for g in expected_greek_fields}
                    if all(v == 0 or v is None for v in greek_values.values()):
                        logger.warning(f"[VALIDATION] {symbol} - All Greeks are zero: {greek_values}")
        except Exception as e:
            logger.debug(f"[VALIDATION] {symbol} - Could not validate (not an option?): {e}")
        
        # Continue with existing write logic from base class
        super().write_tick_to_redis(symbol, tick_data)
    
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
        """Write tick data to Parquet with normalized timestamps (epoch milliseconds)"""
        try:
            # Add metadata to each tick
            metadata = metadata_resolver.get_metadata(tick_data.get('instrument_token', 0))
            
            # Normalize all timestamps to epoch milliseconds for consistent downstream processing
            current_time_ms = int(time.time() * 1000)
            
            # Normalize exchange_timestamp
            exchange_ts_ms = None
            if tick_data.get('exchange_timestamp_ms'):
                exchange_ts_ms = int(tick_data.get('exchange_timestamp_ms'))
            elif tick_data.get('exchange_timestamp'):
                exchange_ts_ms = TimestampNormalizer.to_epoch_ms(tick_data.get('exchange_timestamp'))
            else:
                exchange_ts_ms = current_time_ms
            
            # Normalize timestamp
            timestamp_ms = None
            if tick_data.get('timestamp_ms'):
                timestamp_ms = int(tick_data.get('timestamp_ms'))
            elif tick_data.get('timestamp'):
                timestamp_ms = TimestampNormalizer.to_epoch_ms(tick_data.get('timestamp'))
            else:
                timestamp_ms = exchange_ts_ms or current_time_ms
            
            # Normalize last_trade_time
            last_trade_time_ms = None
            if tick_data.get('last_trade_time_ms'):
                last_trade_time_ms = int(tick_data.get('last_trade_time_ms'))
            elif tick_data.get('last_trade_time'):
                last_trade_time_ms = TimestampNormalizer.to_epoch_ms(tick_data.get('last_trade_time'))
            
            # Normalize timestamp_ns (convert nanoseconds to milliseconds)
            timestamp_ns_ms = None
            if tick_data.get('timestamp_ns_ms'):
                timestamp_ns_ms = int(tick_data.get('timestamp_ns_ms'))
            elif tick_data.get('timestamp_ns'):
                # If it's a large number, assume nanoseconds and convert to ms
                ts_ns = tick_data.get('timestamp_ns')
                if isinstance(ts_ns, (int, float)) and ts_ns > 1e12:
                    timestamp_ns_ms = int(ts_ns / 1_000_000)
                else:
                    timestamp_ns_ms = TimestampNormalizer.to_epoch_ms(ts_ns)
            
            # Normalize processed_timestamp
            processed_ts_ms = None
            if tick_data.get('processed_timestamp'):
                processed_ts = tick_data.get('processed_timestamp')
                if isinstance(processed_ts, (int, float)) and processed_ts < 1e12:
                    # Unix timestamp in seconds
                    processed_ts_ms = int(processed_ts * 1000)
                elif isinstance(processed_ts, (int, float)) and processed_ts > 1e12:
                    # Already in milliseconds
                    processed_ts_ms = int(processed_ts)
                else:
                    processed_ts_ms = TimestampNormalizer.to_epoch_ms(processed_ts)
            else:
                processed_ts_ms = current_time_ms
            
            tick_data_with_meta = {
                **tick_data,
                'symbol': metadata['symbol'],
                'exchange': metadata['exchange'],
                'instrument_type': metadata['instrument_type'],
                'segment': metadata['segment'],
                'tradingsymbol': metadata['tradingsymbol'],
                # Normalized timestamps (all in epoch milliseconds as int64)
                'exchange_timestamp_ms': exchange_ts_ms,
                'timestamp_ms': timestamp_ms,
                'timestamp_ns_ms': timestamp_ns_ms or timestamp_ms,
                'last_trade_time_ms': last_trade_time_ms or 0,
                'processed_timestamp_ms': processed_ts_ms,
                # Keep ISO strings for readability (optional, can be removed)
                'exchange_timestamp': TimestampNormalizer.to_iso_string(exchange_ts_ms),
                'timestamp': TimestampNormalizer.to_iso_string(timestamp_ms),
                'processing_timestamp': TimestampNormalizer.to_iso_string(processed_ts_ms),
            }
            
            self.buffer.append(tick_data_with_meta)
            
            # Write when buffer reaches size
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()
                
        except Exception as e:
            logger.error(f"Error writing to Parquet buffer: {e}")

    def _flush_buffer(self):
        """Flush buffer to Parquet file with atomic write and validation"""
        if not self.buffer:
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Ensure all timestamp columns are int64 (epoch milliseconds)
            timestamp_columns = [
                'exchange_timestamp_ms', 'timestamp_ms', 'timestamp_ns_ms',
                'last_trade_time_ms', 'processed_timestamp_ms'
            ]
            for col in timestamp_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"intraday_{timestamp}.parquet"
            filepath = self.data_directory / filename
            
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Atomic write: write to temp file first, then validate and rename
            temp_file = filepath.with_suffix('.tmp')
            try:
                # Write to temp file
                df.to_parquet(temp_file, index=False, compression='snappy', engine='pyarrow')
                
                # Validate the file before renaming
                try:
                    pq.ParquetFile(temp_file)
                    # Check file size > 0
                    if temp_file.stat().st_size == 0:
                        raise ValueError("Written file is empty")
                    
                    # Atomic rename
                    temp_file.replace(filepath)
                    logger.info(f"✅ Written {len(self.buffer)} records to {filepath} (validated)")
                    self._records_written += len(self.buffer)
                except Exception as validation_error:
                    # Validation failed, remove temp file
                    if temp_file.exists():
                        temp_file.unlink()
                    raise ValueError(f"Parquet validation failed: {validation_error}")
                    
            except Exception as write_error:
                # Clean up temp file on error
                if temp_file.exists():
                    temp_file.unlink()
                raise write_error
            
            self.buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ Error writing Parquet: {e}")
            # Don't clear buffer on error - allow retry

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


def main():
    """Entry point for running intraday crawler - merged from run_intraday_crawler.py"""
    import sys
    from pathlib import Path
    
    # Add project root to Python path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    from config.zerodha_config import ZerodhaConfig
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Intraday Crawler with Redis 8.2")
    logger.info("=" * 60)
    
    try:
        # Load Zerodha credentials
        logger.info("🔑 Loading Zerodha credentials...")
        token_data = ZerodhaConfig.get_token_data()
        if not token_data:
            raise ValueError("No Zerodha token data found")
        
        api_key = token_data.get("api_key")
        access_token = token_data.get("access_token")
        
        if not api_key or not access_token:
            raise ValueError("Missing API key or access token")
        
        logger.info("✅ Zerodha credentials loaded")
        
        # Load instrument configuration
        logger.info("📋 Loading intraday instrument configuration...")
        intraday_config_path = project_root / "crawlers" / "binary_crawler1" / "binary_crawler1.json"
        
        if not intraday_config_path.exists():
            raise FileNotFoundError(f"Intraday config not found: {intraday_config_path}")
        
        with open(intraday_config_path, "r") as f:
            intraday_config = json.load(f)
            # The config has a direct 'tokens' array
            raw_tokens = intraday_config.get("tokens", [])
            instruments = []
            for token in raw_tokens:
                try:
                    instruments.append(int(token))
                except (TypeError, ValueError):
                    continue
        
        logger.info(f"📊 Loaded {len(instruments)} intraday instruments")
        
        # Create instrument info mapping using metadata_resolver
        logger.info("🔧 Creating instrument info mapping using metadata_resolver...")
        instrument_info = {}
        for token in instruments:
            instrument_info[token] = metadata_resolver.get_instrument_info(token)
        
        logger.info(f"✅ Created instrument info for {len(instrument_info)} instruments")
        
        # ✅ Redis 8.2: Create crawler with RedisManager82
        logger.info("🔧 Creating intraday crawler with Redis 8.2...")
        
        crawler = create_intraday_crawler(
            api_key=api_key,
            access_token=access_token,
            instruments=instruments,
            instrument_info=instrument_info,
            data_directory="crawlers/raw_data/intraday_data",  # Optional backup
            name="intraday_crawler",
            # ✅ Redis 8.2 configuration (uses RedisManager82)
            redis_host="127.0.0.1",
            redis_port=6379,
            redis_db=1,  # DB 1 (realtime) for tick data
        )
        
        logger.info("🚀 Starting intraday crawler with Redis 8.2...")
        crawler.start()
        
        # Keep running until interrupted
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping intraday crawler...")
            crawler.stop()
            logger.info("✅ Intraday crawler stopped")
    
    except Exception as e:
        logger.error(f"❌ Failed to start intraday crawler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
