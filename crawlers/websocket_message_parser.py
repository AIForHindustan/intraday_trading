# binary_parser/websocket_message_parser.py
import logging
import struct
import time
import zlib
from datetime import datetime
from typing import List, Dict, Any, Optional

from crawlers.utils.instrument_mapper import InstrumentMapper
# ✅ UPDATED: Use VolumeManager (removed incomplete UnifiedVolumeEngine)
from redis_files.volume_manager import get_volume_manager


class ZerodhaBinaryParser:
    """Low-level parser for Zerodha WebSocket binary packet structures."""

    def __init__(
        self,
        instrument_mapper: InstrumentMapper,
        redis_client=None,
    ):
        self.logger = logging.getLogger(__name__)
        # ✅ Use the client directly, no wrapping
        self.redis_client = redis_client
        if instrument_mapper:
            self.instrument_mapper = instrument_mapper
        else:
            from crawlers.hot_token_mapper import get_redis_token_mapper
            if redis_client:
                self.instrument_mapper = get_redis_token_mapper(redis_client)
            else:
                from crawlers.hot_token_mapper import get_hot_token_mapper
                self.instrument_mapper = get_hot_token_mapper()

    def parse_binary_message(self, binary_data: bytes) -> List[Dict[str, Any]]:
        """Parse Zerodha WebSocket binary message structure into packet blobs."""
        packets: List[Dict[str, Any]] = []

        if not binary_data or len(binary_data) < 4:
            return packets

        try:
            num_packets = int.from_bytes(binary_data[0:2], byteorder="big", signed=False)
        except Exception:
            return packets

        offset = 2

        for _ in range(num_packets):
            if offset + 2 > len(binary_data):
                break

            packet_length = int.from_bytes(
                binary_data[offset : offset + 2], byteorder="big", signed=False
            )
            offset += 2

            if packet_length <= 0 or offset + packet_length > len(binary_data):
                break

            packet_data = binary_data[offset : offset + packet_length]
            offset += packet_length

            parsed: Optional[Dict[str, Any]] = None
            packet_type = None

            if packet_length == 184:
                parsed = self._parse_quote_packet(packet_data)
                packet_type = "full_quote"
            elif packet_length == 32:
                parsed = self._parse_index_packet(packet_data)
                packet_type = "index"
            elif packet_length == 8:
                parsed = self._parse_ltp_packet(packet_data)
                packet_type = "ltp"
            else:
                packet_type = "unknown"

            packets.append(
                {
                    "packet_length": packet_length,
                    "packet_data": packet_data,
                    "packet_type": packet_type,
                    "parsed": parsed,
                }
            )

        return packets

    def _parse_quote_packet(self, packet_data: bytes) -> Dict[str, Any]:
        """Parse 184-byte full quote packet into raw field dictionary."""
        instrument_token = int.from_bytes(packet_data[0:4], "big", signed=True)
        parsed = {
            "instrument_token": instrument_token,
            "last_traded_price": int.from_bytes(packet_data[4:8], "big", signed=True),
            "zerodha_last_traded_quantity": int.from_bytes(packet_data[8:12], "big", signed=True),
            "average_traded_price": int.from_bytes(packet_data[12:16], "big", signed=True),
            "zerodha_cumulative_volume": int.from_bytes(
                packet_data[16:20], "big", signed=True
            ),
            "total_buy_quantity": int.from_bytes(packet_data[20:24], "big", signed=True),
            "total_sell_quantity": int.from_bytes(packet_data[24:28], "big", signed=True),
            "open_price": int.from_bytes(packet_data[28:32], "big", signed=True),
            "high_price": int.from_bytes(packet_data[32:36], "big", signed=True),
            "low_price": int.from_bytes(packet_data[36:40], "big", signed=True),
            "close_price": int.from_bytes(packet_data[40:44], "big", signed=True),
            "last_traded_timestamp": int.from_bytes(packet_data[44:48], "big", signed=True),
            "exchange_timestamp": int.from_bytes(packet_data[60:64], "big", signed=True),
        }
        parsed["symbol"] = self.token_to_symbol(instrument_token)
        return parsed

    def _parse_index_packet(self, packet_data: bytes) -> Dict[str, Any]:
        """Parse 32-byte index packet."""
        instrument_token = int.from_bytes(packet_data[0:4], "big", signed=True)
        parsed = {
            "instrument_token": instrument_token,
            "last_traded_price": int.from_bytes(packet_data[4:8], "big", signed=True),
            "high_price": int.from_bytes(packet_data[8:12], "big", signed=True),
            "low_price": int.from_bytes(packet_data[12:16], "big", signed=True),
            "open_price": int.from_bytes(packet_data[16:20], "big", signed=True),
            "close_price": int.from_bytes(packet_data[20:24], "big", signed=True),
            "net_change": int.from_bytes(packet_data[24:28], "big", signed=True),
            "exchange_timestamp": int.from_bytes(packet_data[28:32], "big", signed=True),
        }
        parsed["symbol"] = self.token_to_symbol(instrument_token)
        return parsed

    def _parse_ltp_packet(self, packet_data: bytes) -> Dict[str, Any]:
        """Parse 8-byte LTP packet."""
        instrument_token = int.from_bytes(packet_data[0:4], "big", signed=True)
        parsed = {
            "instrument_token": instrument_token,
            "last_traded_price": int.from_bytes(packet_data[4:8], "big", signed=True),
        }
        parsed["symbol"] = self.token_to_symbol(instrument_token)
        return parsed

    def token_to_symbol(self, instrument_token: int) -> str:
        """Resolve instrument token to canonical symbol and log unknown tokens."""
        symbol = self.instrument_mapper.token_to_symbol(instrument_token)
        if symbol.startswith("UNKNOWN_"):
            self.log_unknown_token(instrument_token)
        return symbol

    def log_unknown_token(self, token: int):
        """Track unknown tokens for debugging and visibility."""
        # For file-only crawlers (no Redis), just log silently at debug level
        if not self.redis_client:
            return

        # Only log to Redis for crawlers that use Redis (like intraday crawler)
        try:
            key = f"unknown_tokens:{datetime.now().strftime('%Y-%m-%d')}"
            self.redis_client.sadd(key, token)
            self.redis_client.expire(key, 86400)
        except Exception as exc:
            pass


class ZerodhaWebSocketMessageParser:
    """
    Official WebSocket binary packet parser for Zerodha Kite Connect
    Matches the exact structure from: https://kite.trade/docs/connect/v3/websocket/

    Full mode packet structure (184 bytes):
    - Bytes 0-64: Basic quote data + timestamps + OI
    - Bytes 64-184: Market depth (5 buy levels + 5 sell levels)

    Price conversion:
    - Currencies: Divide by 10000000 (4 decimal places)
    - Everything else: Divide by 100 (2 decimal places)
    """

    def __init__(self, instrument_info: Dict[int, Dict], redis_client=None):
        self.instrument_info = instrument_info
        
        # ✅ Use the client directly, no wrapping
        self.redis_client = redis_client
        
        self.logger = logging.getLogger(__name__)
        # Use hot token mapper for performance (falls back to full mapper if needed)
        from crawlers.hot_token_mapper import get_redis_token_mapper, get_hot_token_mapper
        if redis_client:
            self.instrument_mapper = get_redis_token_mapper(redis_client)
        else:
            self.instrument_mapper = get_hot_token_mapper()
        # Note: instrument_info is handled by hot_token_mapper's fallback mechanism
        
        # ✅ Remove the redis_for_parser duplication
        self.binary_parser = ZerodhaBinaryParser(
            self.instrument_mapper, 
            redis_client  # Pass the same client
        )
        
        # ✅ UPDATED: Use VolumeManager (removed incomplete UnifiedVolumeEngine)
        if redis_client:
            self.volume_manager = get_volume_manager(redis_client=redis_client)
            self.volume_calculator = self.volume_manager
        else:
            self.volume_manager = None
            self.volume_calculator = None
        
        # ✅ ADDED: Redis key standards, key builder, and symbol parser for Greek storage
        try:
            from redis_files.redis_key_standards import (
                RedisKeyStandards,
                get_key_builder,
                get_symbol_parser,
            )
            self.key_builder = get_key_builder()
            self.redis_key_standards = RedisKeyStandards
            self.symbol_parser = get_symbol_parser()
            self.logger.debug("✅ [PARSER] Initialized redis_key_standards and key_builder for Greek storage")
        except Exception as e:
            self.logger.warning(f"⚠️ [PARSER] Failed to initialize redis_key_standards: {e}")
            self.key_builder = None
            self.redis_key_standards = None
            self.symbol_parser = None
        
        # ✅ ADDED: ExpiryCalculator for DTE calculation (uses pandas_market_calendars)
        try:
            from intraday_scanner.calculations import ExpiryCalculator
            self.expiry_calculator = ExpiryCalculator('NSE')
            self.logger.debug("✅ [PARSER] Initialized ExpiryCalculator for expiry date parsing")
        except Exception as e:
            self.logger.warning(f"⚠️ [PARSER] Failed to initialize ExpiryCalculator: {e}")
            self.expiry_calculator = None
        
        # ✅ ADDED: Greek calculator (lazy import to avoid circular dependencies)
        self._greek_calculator = None
        
        # ✅ MERGED: Error tracking from bulletproof_parser
        self.error_counts = {}
        self.last_error_time = {}
        self.max_errors_per_minute = 10

    @staticmethod
    def _is_option_symbol(symbol: Optional[str]) -> bool:
        """Return True if symbol string looks like an option (CE/PE suffix)."""
        if not symbol or not isinstance(symbol, str):
            return False
        symbol_upper = symbol.upper()
        if ":" in symbol_upper:
            symbol_upper = symbol_upper.split(":", 1)[-1]
        return symbol_upper.endswith(("CE", "PE"))

    def _update_price_change_fields(self, tick_data: Dict[str, Any]) -> None:
        """Populate price change metrics using last_price vs close."""
        last_price = tick_data.get("last_price")
        ohlc = tick_data.get("ohlc")
        close_price = None
        if isinstance(ohlc, dict):
            close_price = ohlc.get("close")
        if close_price is None:
            close_price = tick_data.get("close_price") or tick_data.get("close")
        try:
            last_price = float(last_price)
            close_price = float(close_price) if close_price is not None else None
        except (TypeError, ValueError):
            last_price = None
            close_price = None

        if not last_price or close_price in (None, 0):
            tick_data.setdefault("price_change", 0.0)
            tick_data.setdefault("price_change_pct", 0.0)
            tick_data.setdefault("price_change_display", 0.0)
            tick_data.setdefault("price_change_abs", 0.0)
            tick_data.setdefault("price_move", 0.0)
            tick_data.setdefault("price_change_pct_decimal", 0.0)
            return

        price_delta = last_price - close_price
        price_pct = (price_delta / close_price) * 100.0 if close_price else 0.0
        tick_data["price_change"] = round(price_pct, 6)
        tick_data["price_change_pct"] = round(price_pct, 6)
        tick_data["price_change_display"] = round(price_pct, 2)
        tick_data["price_move"] = round(abs(price_pct), 6)
        tick_data["price_change_abs"] = round(price_delta, 6)
        tick_data["price_change_pct_decimal"] = round(price_pct / 100.0, 8)

    def parse_websocket_message(self, message_data: bytes) -> List[Dict[str, Any]]:
        """
        Parse complete WebSocket message containing multiple packets with error resilience.
        
        ✅ MERGED: Enhanced with error rate limiting and packet-level isolation from bulletproof_parser.

        Message structure:
        - First 2 bytes: Number of packets (SHORT/int16)
        - Then for each packet: 2 bytes length + packet data
        """
        if not message_data:
            return []
        
        # ✅ MERGED: Check error rate limit before parsing
        if self._error_rate_exceeded():
            self.logger.warning("🚨 Error rate exceeded, skipping message")
            return []
        
        ticks = []

        try:
            # Check if message is compressed
            if self._is_compressed(message_data):
                try:
                    message_data = zlib.decompress(message_data)
                except zlib.error as e:
                    self._record_error("decompression")
                    self.logger.error(f"❌ Decompression failed: {e}")
                    return ticks

            # Parse message header with packet-level error isolation
            if len(message_data) < 4:
                return ticks

            # ✅ MERGED: Parse with packet-level error isolation
            ticks = self._parse_message_structure_with_isolation(message_data)

        except Exception as e:
            self._record_error("general")
            self.logger.error(f"❌ Critical parse error: {e}")

        return ticks
    
    def _parse_message_structure_with_isolation(self, message_data: bytes) -> List[Dict[str, Any]]:
        """Parse message structure with packet-level error isolation (merged from bulletproof_parser)"""
        ticks = []
        
        try:
            num_packets = int.from_bytes(message_data[0:2], byteorder="big", signed=False)
            offset = 2
            
            for packet_index in range(num_packets):
                if offset + 2 > len(message_data):
                    break
                
                # Parse each packet in isolation - errors in one packet don't affect others
                packet_ticks = self._parse_single_packet_safe(message_data, offset, packet_index)
                ticks.extend(packet_ticks)
                
                # Update offset safely
                try:
                    packet_length = int.from_bytes(
                        message_data[offset:offset+2], byteorder="big", signed=False
                    )
                    offset += 2 + packet_length
                except Exception:
                    break
                    
        except Exception as e:
            self._record_error("structure")
            self.logger.error(f"❌ Message structure error: {e}")
        
        return ticks
    
    def _parse_single_packet_safe(self, message_data: bytes, offset: int, packet_index: int) -> List[Dict]:
        """Parse single packet safely - errors in one packet don't affect others (merged from bulletproof_parser)"""
        try:
            packet_length = int.from_bytes(
                message_data[offset:offset+2], byteorder="big", signed=False
            )
            
            if packet_length <= 0 or offset + 2 + packet_length > len(message_data):
                return []
            
            packet_data = message_data[offset+2:offset+2+packet_length]
            
            # Route to appropriate parser based on packet length
            if packet_length == 184:
                # Full packet - use existing parse_zerodha_packet
                wrapper = {"packet_data": packet_data, "packet_length": 184}
                tick = self.parse_zerodha_packet(wrapper)
                if tick:
                    tick["packet_index"] = packet_index
                    return [tick]
            elif packet_length == 32:
                # Index packet
                tick = self._parse_index_packet_safe(packet_data, packet_index)
                return [tick] if tick else []
            elif packet_length == 8:
                # LTP packet
                tick = self._parse_ltp_packet_safe(packet_data, packet_index)
                return [tick] if tick else []
            else:
                # Unknown packet type - try to parse with binary parser
                try:
                    parsed = self.binary_parser._parse_quote_packet(packet_data) if packet_length >= 64 else None
                    if parsed:
                        tick = self.parse_zerodha_packet({"packet_data": packet_data, "parsed": parsed})
                        if tick:
                            tick["packet_index"] = packet_index
                            return [tick]
                except Exception:
                    pass
                
        except Exception as e:
            self._record_error(f"packet_{packet_index}")
            self.logger.error(f"❌ Packet {packet_index} parse error: {e}")
        
        return []
    
    def _parse_index_packet_safe(self, packet_data: bytes, packet_index: int) -> Optional[Dict]:
        """Safe index packet parsing (32 bytes) - merged from bulletproof_parser"""
        try:
            if len(packet_data) != 32:
                return None
            
            instrument_token = self._parse_int32_safe(packet_data, 0, 4)
            if not instrument_token:
                return None
            
            symbol = self._token_to_symbol_safe(instrument_token)
            segment = instrument_token & 0xFF
            divisor = self._get_price_divisor(segment)
            
            tick_data = {
                "instrument_token": instrument_token,
                "symbol": symbol,
                "last_price": self._parse_price_safe(packet_data, 4, 8, instrument_token, divisor),
                "high": self._parse_price_safe(packet_data, 8, 12, instrument_token, divisor),
                "low": self._parse_price_safe(packet_data, 12, 16, instrument_token, divisor),
                "open": self._parse_price_safe(packet_data, 16, 20, instrument_token, divisor),
                "close": self._parse_price_safe(packet_data, 20, 24, instrument_token, divisor),
                "timestamp": datetime.now().isoformat(),
                "packet_index": packet_index,
                "mode": "index"
            }
            
            if not tick_data.get("last_price") or not tick_data.get("symbol"):
                return None
            
            close_val = tick_data.get("close")
            last_val = tick_data.get("last_price")
            if close_val not in (None, 0) and last_val not in (None, 0):
                price_delta = last_val - close_val
                pct = (price_delta / close_val) * 100.0
                tick_data["price_change"] = round(pct, 6)
                tick_data["price_change_pct"] = round(pct, 6)
                tick_data["price_change_abs"] = round(price_delta, 6)
                tick_data["price_move"] = round(abs(pct), 6)
                tick_data["price_change_display"] = round(pct, 2)
                tick_data["price_change_pct_decimal"] = round(pct / 100.0, 8)

            return tick_data
        except Exception as e:
            self._record_error("index_packet")
            self.logger.error(f"❌ Index packet parse error: {e}")
            return None
    
    def _parse_ltp_packet_safe(self, packet_data: bytes, packet_index: int) -> Optional[Dict]:
        """Safe LTP packet parsing (8 bytes) - merged from bulletproof_parser"""
        try:
            if len(packet_data) != 8:
                return None
            
            instrument_token = self._parse_int32_safe(packet_data, 0, 4)
            if not instrument_token:
                return None
            
            symbol = self._token_to_symbol_safe(instrument_token)
            segment = instrument_token & 0xFF
            divisor = self._get_price_divisor(segment)
            
            tick_data = {
                "instrument_token": instrument_token,
                "symbol": symbol,
                "last_price": self._parse_price_safe(packet_data, 4, 8, instrument_token, divisor),
                "timestamp": datetime.now().isoformat(),
                "packet_index": packet_index,
                "mode": "ltp"
            }
            
            if not tick_data.get("last_price") or not tick_data.get("symbol"):
                return None
            
            return tick_data
        except Exception as e:
            self._record_error("ltp_packet")
            self.logger.error(f"❌ LTP packet parse error: {e}")
            return None
    
    def _parse_int32_safe(self, data: bytes, start: int, end: int) -> Optional[int]:
        """Safe integer parsing with bounds checking - merged from bulletproof_parser"""
        try:
            if end > len(data):
                return None
            # ✅ Use signed=True - Zerodha uses signed integers for instrument_token
            return int.from_bytes(data[start:end], byteorder="big", signed=True)
        except:
            return None
    
    def _parse_price_safe(self, data: bytes, start: int, end: int, instrument_token: int, divisor: float) -> Optional[float]:
        """Safe price parsing with divisor - merged from bulletproof_parser"""
        try:
            raw_price = self._parse_int32_safe(data, start, end)
            if raw_price is None:
                return None
            return raw_price / divisor if divisor else None
        except:
            return None
    
    def _token_to_symbol_safe(self, instrument_token: int) -> str:
        """Safe symbol resolution - merged from bulletproof_parser"""
        try:
            symbol = self.instrument_mapper.token_to_symbol(instrument_token)
            return symbol if not symbol.startswith("UNKNOWN_") else f"UNKNOWN_{instrument_token}"
        except:
            return f"UNKNOWN_{instrument_token}"
    
    def _error_rate_exceeded(self) -> bool:
        """Check if we're getting too many errors too fast - merged from bulletproof_parser"""
        current_minute = int(time.time() / 60)
        
        # Clean old error counts
        self.error_counts = {
            k: v for k, v in self.error_counts.items() 
            if self.last_error_time.get(k, 0) > current_minute - 2
        }
        
        total_errors = sum(self.error_counts.values())
        return total_errors > self.max_errors_per_minute
    
    def _record_error(self, error_type: str):
        """Track error rates - merged from bulletproof_parser"""
        current_minute = int(time.time() / 60)
        
        if error_type not in self.error_counts or self.last_error_time.get(error_type, 0) < current_minute:
            self.error_counts[error_type] = 0
        
        self.error_counts[error_type] += 1
        self.last_error_time[error_type] = current_minute

    def _is_compressed(self, data: bytes) -> bool:
        """Check if data is zlib compressed"""
        if len(data) < 2:
            return False
        # Check for zlib header (78 01, 78 9C, 78 DA)
        return data[0] == 0x78 and data[1] in [0x01, 0x9C, 0xDA]

    def parse_zerodha_packet(self, binary_data: Any) -> Dict[str, Any]:
        """Parse Zerodha binary packet with proper volume handling."""
        try:
            packet_bytes = binary_data
            pre_parsed: Optional[Dict[str, Any]] = None

            if isinstance(binary_data, dict):
                packet_bytes = binary_data.get("packet_data")
                pre_parsed = binary_data.get("parsed") or {}

            packet = self._parse_binary_packet(packet_bytes)
            if packet is None and pre_parsed:
                packet = pre_parsed.copy()
                packet["instrument_token"] = packet.get("instrument_token")
            if not packet:
                return {}

            instrument_token = packet.get("instrument_token") or packet.get("token")
            symbol = self._token_to_symbol(instrument_token) if instrument_token else None

            packet.setdefault("instrument_token", instrument_token)
            if symbol:
                packet["symbol"] = symbol

            last_traded_price = packet.get("last_price") or packet.get("last_traded_price")
            cumulative_volume = (
                packet.get("zerodha_cumulative_volume")
                or packet.get("volume_traded_for_the_day")
                or packet.get("cumulative_volume")
            )

            volume_data = {
                "instrument_token": instrument_token,
                "symbol": symbol,
                "last_traded_price": last_traded_price,
                "zerodha_last_traded_quantity": packet.get("zerodha_last_traded_quantity")
                or packet.get("last_traded_quantity")
                or packet.get("last_quantity"),
                "zerodha_cumulative_volume": cumulative_volume,
                "total_buy_quantity": packet.get("total_buy_quantity"),
                "total_sell_quantity": packet.get("total_sell_quantity"),
                "exchange_timestamp": packet.get("exchange_timestamp"),
                "timestamp": datetime.now().isoformat(),
            }

            packet.update({k: v for k, v in volume_data.items() if v is not None})

            tick = self._normalize_volume_at_ingestion(packet)

            # ✅ REMOVED: Backward compatibility aliases - normalization method already sets canonical fields
            # The _normalize_volume_at_ingestion() method sets all canonical field names
            # Downstream code should use canonical field names from field mapping

            return tick
        except Exception as e:
            self.logger.error(f"Packet parsing error: {e}")
            return {}

    def _parse_binary_packet(self, binary_data: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """Parse binary packet based on length."""
        if not binary_data:
            return None

        packet_size = len(binary_data)
        if packet_size == 184:
            return self._parse_full_packet(binary_data)

        # Unsupported packet sizes fall back to None for now
        return None

    def _normalize_volume_at_ingestion(
        self, tick_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        SINGLE CALCULATION POINT: Convert cumulative volume to incremental using centralized volume state manager.
        
        Volume Profile Integration:
        - VolumeManager automatically integrates with VolumeProfileManager
        - Volume profile updates happen automatically during incremental volume calculation
        - POC, Value Area, and support/resistance levels are calculated in real-time
        - No additional code needed here - integration is seamless
        
        NOTE: For file-only crawlers (data_mining, research), this method skips Redis operations
        and only sets basic volume fields to avoid unnecessary Redis connections.
        """
        # SKIP volume normalization for file-only crawlers (no Redis client)
        if self.redis_client is None:
            return tick_data  # Skip all Redis operations
        
        try:
            # ✅ Use unified volume manager (already initialized in __init__)
            if not self.volume_manager:
                return tick_data
            
            symbol = tick_data.get("symbol")
            if not symbol:
                instrument_token = tick_data.get("instrument_token")
                symbol = f"TOKEN_{instrument_token}" if instrument_token else None

            if symbol is None:
                return tick_data

            # Get exchange timestamp for session-aware calculation
            exchange_timestamp = tick_data.get("exchange_timestamp")
            if not exchange_timestamp:
                exchange_timestamp = datetime.now()
            elif isinstance(exchange_timestamp, str):
                try:
                    exchange_timestamp = datetime.fromisoformat(exchange_timestamp.replace('Z', '+00:00'))
                except:
                    exchange_timestamp = datetime.now()

            # Get cumulative volume from tick data
            cumulative = tick_data.get("zerodha_cumulative_volume")
            if cumulative is None:
                cumulative = tick_data.get("volume_traded_for_the_day")
            if cumulative is None:
                cumulative = tick_data.get("cumulative_volume")
            
            if cumulative is None:
                return tick_data

            # Ensure cumulative is numeric
            try:
                cumulative_int = int(float(cumulative))
            except (TypeError, ValueError):
                cumulative_int = 0

            # ✅ Use unified volume manager
            volume_manager = self.volume_manager
            instrument_token_str = str(tick_data.get('instrument_token', symbol))
            self.logger.info(f"🔍 [VOLUME_FLOW] Calling calculate_incremental: symbol={symbol}, token={instrument_token_str}, cumulative={cumulative_int}")
            incremental = volume_manager.calculate_incremental(
                instrument_token=instrument_token_str,
                current_cumulative=cumulative_int,
                exchange_timestamp=exchange_timestamp,  # Must be datetime
                symbol=symbol  # ✅ Pass already-resolved symbol to avoid re-resolution
            )
            self.logger.info(f"🔍 [VOLUME_FLOW] calculate_incremental returned: incremental={incremental} for {symbol}")

            # Set ALL volume fields consistently using canonical field names
            tick_data["zerodha_cumulative_volume"] = cumulative_int  # Canonical: Zerodha cumulative
            tick_data["volume_traded_for_the_day"] = cumulative_int  # Legacy alias
            tick_data["cumulative_volume"] = cumulative_int          # Legacy cumulative  
            tick_data["bucket_incremental_volume"] = incremental    # Canonical: Pattern incremental
            tick_data["incremental_volume"] = incremental           # Canonical: Explicit incremental
            tick_data["volume"] = incremental                       # Canonical: Generic incremental
            try:
                if self.volume_calculator and symbol:
                    volume_metrics = self.volume_calculator.calculate_volume_metrics(symbol, tick_data)
                    tick_data["volume_ratio"] = volume_metrics.get("volume_ratio", 0.0)
                    tick_data["normalized_volume"] = tick_data.get("volume_ratio", 0.0)
                    
                    # Verify volume consistency at WebSocket ingestion stage
                    from utils.correct_volume_calculator import VolumeResolver  # VolumeResolver still in correct_volume_calculator
                    verification = VolumeResolver.verify_volume_consistency(tick_data, "websocket_ingestion")
                    if not verification["consistent"]:
                        self.logger.warning(f"Volume consistency issue at WebSocket ingestion for {symbol}: {verification['issues']}")
            except Exception as calc_exc:
                pass

            return tick_data

        except Exception as e:
            self.logger.error(f"❌ Volume normalization error: {e}")
            # Fallback to original data
            return tick_data

    
    def _parse_full_packet(self, packet_data: bytes) -> Dict[str, Any]:
        """
        Parse full mode packet (184 bytes) according to official documentation

        Official structure:
        - Bytes 0-64: Basic quote data + timestamps + OI (16 integers)
        - Bytes 64-184: Market depth (120 bytes)
        """
        if len(packet_data) != 184:
            return None

        try:
            # ✅ FIX: Use signed integer format (">i") instead of unsigned (">I")
            # Zerodha uses signed integers for instrument_token
            instrument_token = struct.unpack(">i", packet_data[0:4])[0]
        except struct.error as e:
            print(f"⚠️ Protocol error parsing instrument token: {e}")
            return None

        # Get instrument info for metadata
        inst_info = self.instrument_info.get(instrument_token, {})
        mapped_symbol = self.binary_parser.token_to_symbol(instrument_token)
        if mapped_symbol.startswith("UNKNOWN"):
            mapped_symbol = (
                inst_info.get("symbol")
                or inst_info.get("tradingsymbol")
                or mapped_symbol
            )

        # Determine price divisor based on segment
        # Extract segment from instrument_token (last byte)
        segment = instrument_token & 0xFF
        divisor = self._get_price_divisor(segment)

        # Determine tradable status (indices are not tradable)
        # Segment 9 = indices, all others are tradable
        tradable = segment != 9

        # Parse the full packet
        tick_data = {
            "instrument_token": instrument_token,
            "mode": "full",
            "tradable": tradable,
            "symbol": mapped_symbol,
            "asset_class": inst_info.get("asset_class", "unknown"),
            "segment": segment,
            "timestamp": datetime.now(),
        }
        
        # ✅ CRITICAL: Add option/futures metadata from instrument_info (from instrument list CSV)
        # These fields are NOT in the WebSocket packet but are in the instrument list
        # See: https://kite.trade/docs/connect/v3/market-quotes/
        if inst_info:
            # Add strike for options (from instrument list CSV)
            if inst_info.get("strike"):
                tick_data["strike_price"] = float(inst_info["strike"])
            
            # Add expiry for derivatives (from instrument list CSV)
            if inst_info.get("expiry"):
                tick_data["expiry_date"] = inst_info["expiry"]
            
            # Add instrument_type (EQ, FUT, CE, PE) from instrument list CSV
            inst_type = inst_info.get("instrument_type", "")
            if inst_type:
                tick_data["instrument_type"] = inst_type
                # Convert CE/PE to call/put for option_type field
                if inst_type in ("CE", "PE"):
                    tick_data["option_type"] = "call" if inst_type == "CE" else "put"
        
        # ✅ CRITICAL: Parse expiry_date from symbol if missing (using ExpiryCalculator with pandas_market_calendars)
        # This ensures expiry_date is available for Greek calculations even if instrument_info doesn't have it
        if not tick_data.get("expiry_date") and self.expiry_calculator:
            try:
                parsed_expiry = self.expiry_calculator.parse_expiry_from_symbol(mapped_symbol)
                if parsed_expiry:
                    tick_data["expiry_date"] = parsed_expiry
                    self.logger.debug(f"✅ [PARSER] Parsed expiry_date={parsed_expiry} from symbol {mapped_symbol}")
            except Exception as e:
                self.logger.debug(f"⚠️ [PARSER] Failed to parse expiry from symbol {mapped_symbol}: {e}")

        # ✅ FALLBACK: Derive option metadata from symbol if instrument_info is incomplete
        parsed_symbol = None
        if self.symbol_parser:
            needs_option_metadata = any(
                tick_data.get(field) in (None, "", 0)
                for field in ("instrument_type", "option_type", "strike_price")
            )
            if needs_option_metadata:
                try:
                    parsed_symbol = self.symbol_parser.parse_symbol(mapped_symbol)
                    if parsed_symbol and parsed_symbol.instrument_type in ("OPT", "CE", "PE"):
                        strike = getattr(parsed_symbol, "strike", None)
                        option_hint = getattr(parsed_symbol, "option_type", "")
                        expiry_obj = getattr(parsed_symbol, "expiry", None)

                        if strike and not tick_data.get("strike_price"):
                            tick_data["strike_price"] = float(strike)
                            self.logger.debug(f"✅ [PARSER] Derived strike_price={strike} from symbol {mapped_symbol}")
                        
                        if option_hint:
                            if not tick_data.get("instrument_type"):
                                tick_data["instrument_type"] = option_hint.upper()
                            if not tick_data.get("option_type"):
                                tick_data["option_type"] = "call" if option_hint.upper().startswith("C") else "put"
                        
                        if expiry_obj and not tick_data.get("expiry_date"):
                            tick_data["expiry_date"] = expiry_obj.strftime("%Y-%m-%d")
                            self.logger.debug(f"✅ [PARSER] Derived expiry_date={tick_data['expiry_date']} from symbol {mapped_symbol}")
                except Exception as parse_err:
                    self.logger.debug(f"⚠️ [PARSER] Unable to derive option metadata from symbol {mapped_symbol}: {parse_err}")

        # Parse basic quote data (bytes 0-44)
        self._parse_quote_data(packet_data, tick_data, divisor)

        # Parse additional full mode data (bytes 44-64)
        self._parse_full_mode_data(packet_data, tick_data, divisor)

        # Extract circuit limits from instrument info or packet data
        self._extract_circuit_limits(packet_data, tick_data, divisor, inst_info)

        # Parse market depth (bytes 64-184)
        self._parse_market_depth(packet_data, tick_data, divisor)

        # Store raw cumulative volume from Zerodha as-is (no calculations)
        # Official field: volume (also check legacy alias volume_traded)
        cumulative_volume = tick_data.get("volume", 0) or tick_data.get("volume_traded", 0)
        tick_data["cumulative_volume"] = cumulative_volume
        # Ensure official field name is set
        if "volume" not in tick_data or tick_data["volume"] == 0:
            tick_data["volume"] = cumulative_volume
        # Store raw volume in Redis buckets as-is (respecting field mapping)
        if self.redis_client and hasattr(
            self.redis_client, "update_symbol_data_direct"
        ):
            exchange_epoch = tick_data.get("exchange_timestamp_epoch")
            if not exchange_epoch:
                exchange_epoch = int(time.time())

            # Store raw cumulative volume as-is in Redis
            self.redis_client.update_symbol_data_direct(
                symbol=tick_data["symbol"],
                bucket_cumulative_volume=cumulative_volume,  # Store Zerodha's cumulative as-is
                last_price=tick_data.get("last_price", 0.0),
                timestamp=exchange_epoch,
                high=tick_data.get("ohlc", {}).get("high"),
                low=tick_data.get("ohlc", {}).get("low"),
                open_price=tick_data.get("ohlc", {}).get("open"),
                close_price=tick_data.get("ohlc", {}).get("close"),
                depth_data=tick_data.get("depth"),
            )

        # ✅ CRITICAL: Calculate and store Greeks for options BEFORE sending downstream
        # This ensures Greeks are available in Redis with proper TTL before scanner processes the tick
        # Check for options: instrument_type can be "CE", "PE", or "OPT", or option_type field exists
        instrument_type = tick_data.get("instrument_type", "")
        option_type = tick_data.get("option_type")
        symbol_for_greeks = tick_data.get("symbol")
        if instrument_type in ("CE", "PE", "OPT") or option_type or self._is_option_symbol(symbol_for_greeks):
            if not option_type and self._is_option_symbol(symbol_for_greeks):
                suffix = symbol_for_greeks.upper().split(":")[-1]
                tick_data["option_type"] = "call" if suffix.endswith("CE") else "put"
            self._calculate_and_store_greeks(tick_data)

        # ✅ Add metadata fields (matching enhanced_tick_parser structure)
        tick_data["_parsed_at"] = time.time()
        tick_data["_source"] = "kite_websocket"

        return tick_data
    
    def _calculate_and_store_greeks(self, tick_data: Dict[str, Any]):
        """
        Calculate and store Greeks for options in Redis with proper TTL.
        
        ✅ STORAGE SPECIFICATION:
        - Key pattern: ind:greeks:{symbol}:{greek_name} (via key_builder.live_greeks())
        - TTL: 86400 seconds (24 hours) - matches unified_data_storage.py
        - Database: DB 1 (live data)
        - Stores: delta, gamma, theta, vega, rho, iv, implied_volatility, dte_years, trading_dte, expiry_series, option_price
        
        Greeks are stored BEFORE sending downstream so scanner can access them immediately.
        """
        if not self.key_builder or not self.redis_client:
            return
        
        symbol = tick_data.get("symbol")
        if not symbol:
            return
        
        # Check if it's an option
        instrument_type = tick_data.get("instrument_type", "")
        option_type = tick_data.get("option_type", "")
        # ✅ CRITICAL FIX: Include "OPT" as valid instrument_type (matches line 788)
        if instrument_type not in ("CE", "PE", "OPT") and not option_type:
            self.logger.debug(f"⚠️ [PARSER] Skipping Greeks for {symbol}: instrument_type={instrument_type}, option_type={option_type}")
            return
        
        self.logger.info(f"🔍 [PARSER] Calculating Greeks for {symbol}: instrument_type={instrument_type}, option_type={option_type}")
        
        # ✅ CRITICAL: Ensure expiry_date is parsed from symbol if missing (using ExpiryCalculator)
        # This must happen BEFORE Greek calculation
        if not tick_data.get("expiry_date") and self.expiry_calculator:
            try:
                parsed_expiry = self.expiry_calculator.parse_expiry_from_symbol(symbol)
                if parsed_expiry:
                    tick_data["expiry_date"] = parsed_expiry
                    self.logger.debug(f"✅ [PARSER] Parsed expiry_date={parsed_expiry} from symbol {symbol} for Greek calculation")
            except Exception as e:
                self.logger.debug(f"⚠️ [PARSER] Failed to parse expiry from symbol {symbol}: {e}")
        
        # ✅ CRITICAL: Calculate DTE using ExpiryCalculator (pandas_market_calendars)
        # This ensures accurate trading_dte calculation for Greek calculations
        if self.expiry_calculator and tick_data.get("expiry_date"):
            try:
                dte_info = self.expiry_calculator.calculate_dte(
                    expiry_date=tick_data.get("expiry_date"),
                    symbol=symbol
                )
                # Add DTE fields to tick_data for Greek calculation
                tick_data["trading_dte"] = dte_info.get("trading_dte", 0)
                tick_data["calendar_dte"] = dte_info.get("calendar_dte", 0.0)
                tick_data["dte_years"] = dte_info.get("trading_dte", 0) / 365.0
                tick_data["expiry_series"] = dte_info.get("expiry_series", "UNKNOWN")
                self.logger.debug(f"✅ [PARSER] Calculated DTE for {symbol}: trading_dte={dte_info.get('trading_dte')}, expiry_series={dte_info.get('expiry_series')}")
            except Exception as e:
                self.logger.warning(f"⚠️ [PARSER] Failed to calculate DTE for {symbol}: {e}")
        
        try:
            # Lazy import to avoid circular dependencies
            if self._greek_calculator is None:
                try:
                    from intraday_scanner.calculations import HybridCalculations
                    self._greek_calculator = HybridCalculations()
                    self.logger.debug("✅ [PARSER] Initialized Greek calculator")
                except Exception as e:
                    self.logger.warning(f"⚠️ [PARSER] Failed to initialize Greek calculator: {e}")
                    return

            # ✅ Derive strike/option_type/underlying_price if missing
            parsed_symbol = None
            if self.symbol_parser:
                try:
                    parsed_symbol = self.symbol_parser.parse_symbol(symbol)
                except Exception:
                    parsed_symbol = None

            if parsed_symbol and parsed_symbol.instrument_type in ("OPT", "CE", "PE"):
                strike_val = getattr(parsed_symbol, "strike", None)
                if strike_val and (not tick_data.get("strike_price") or tick_data.get("strike_price") == 0):
                    tick_data["strike_price"] = float(strike_val)
                    self.logger.debug(f"✅ [PARSER] Derived strike_price={strike_val} for {symbol} inside Greek calc")
                option_hint = getattr(parsed_symbol, "option_type", "")
                if option_hint and not tick_data.get("option_type"):
                    tick_data["option_type"] = "call" if option_hint.upper().startswith("C") else "put"
                if getattr(parsed_symbol, "expiry", None) and not tick_data.get("expiry_date"):
                    tick_data["expiry_date"] = parsed_symbol.expiry.strftime("%Y-%m-%d")

            # ✅ Resolve underlying/spot price
            spot_price = tick_data.get("underlying_price") or tick_data.get("spot_price")
            if (not spot_price or spot_price <= 0) and parsed_symbol and parsed_symbol.base_symbol:
                try:
                    spot_price = self._greek_calculator._get_underlying_price_from_redis(
                        parsed_symbol.base_symbol, symbol
                    )
                except Exception as spot_err:
                    self.logger.debug(f"⚠️ [PARSER] Underlying lookup failed for {symbol}: {spot_err}")
            if spot_price and spot_price > 0:
                tick_data["underlying_price"] = float(spot_price)
                tick_data["spot_price"] = float(spot_price)
            else:
                self.logger.warning(f"⚠️ [PARSER] Missing underlying price for {symbol}, Greeks may fallback")
            
            # Calculate Greeks using HybridCalculations
            greeks = self._greek_calculator.calculate_greeks_for_tick_data(tick_data)
            
            if not greeks:
                return
            
            # Extract required Greek fields
            greek_fields = {
                'delta', 'gamma', 'theta', 'vega', 'rho', 
                'iv', 'implied_volatility', 'dte_years', 
                'trading_dte', 'expiry_series', 'option_price'
            }
            
            # Filter to only valid Greeks (non-None, non-zero for main Greeks)
            valid_greeks = {}
            for greek_name, value in greeks.items():
                if greek_name in greek_fields and value is not None:
                    # Allow zero values for rho, iv, implied_volatility, dte_years, trading_dte
                    if greek_name in ('delta', 'gamma', 'theta', 'vega') and value == 0.0:
                        continue  # Skip zero main Greeks (calculation likely failed)
                    valid_greeks[greek_name] = value
            
            # ✅ CRITICAL: Ensure IV is always included in valid_greeks (even if 0.0) for downstream consumption
            # IV is critical for option patterns and must be available
            if 'iv' not in valid_greeks and 'implied_volatility' in valid_greeks:
                valid_greeks['iv'] = valid_greeks['implied_volatility']
            elif 'implied_volatility' not in valid_greeks and 'iv' in valid_greeks:
                valid_greeks['implied_volatility'] = valid_greeks['iv']
            
            if not valid_greeks:
                self.logger.debug(f"⚠️ [PARSER] No valid Greeks to store for {symbol}")
                return
            
            # ✅ STORAGE: Store Greeks using key_builder with TTL 86400 (24 hours)
            # Matches unified_data_storage.py store_greeks() pattern
            canonical_symbol = self.redis_key_standards.canonical_symbol(symbol) if self.redis_key_standards else symbol
            stored_count = 0
            
            # Get Redis client for DB 1
            try:
                from redis_files.redis_client import RedisManager82
                redis_client = RedisManager82.get_client(process_name="websocket_parser", db=1)
            except Exception:
                redis_client = self.redis_client
            
            for greek_name, value in valid_greeks.items():
                try:
                    # Generate key using key_builder.live_greeks()
                    key = self.key_builder.live_greeks(canonical_symbol, greek_name)
                    
                    # Store with TTL 86400 (24 hours) - matches unified_data_storage.py
                    value_str = str(value) if not isinstance(value, str) else value
                    redis_client.setex(key, 86400, value_str)
                    stored_count += 1
                except Exception as e:
                    self.logger.warning(f"⚠️ [PARSER] Failed to store Greek {greek_name} for {symbol}: {e}")
            
            if stored_count > 0:
                # Add Greeks to tick_data so they're available downstream
                tick_data.update(valid_greeks)
                self.logger.debug(f"✅ [PARSER] Stored {stored_count} Greeks for {symbol} in Redis (TTL: 86400s)")
            
        except Exception as e:
            self.logger.warning(f"⚠️ [PARSER] Error calculating/storing Greeks for {symbol}: {e}", exc_info=True)

    def _get_price_divisor(self, segment: int) -> float:
        """
        Determine price divisor based on segment
        Official documentation:
        - Currencies: Divide by 10000000 (4 decimal places)
        - Everything else: Divide by 100 (2 decimal places)
        """
        # Currency segments: CDS (3), BCD (6)
        if segment in [3, 6]:
            return 10000000.0
        else:
            return 100.0

    def _parse_quote_data(self, data: bytes, tick_data: Dict, divisor: float):
        """Parse quote data (bytes 0-44) - 11 integers"""
        # Unpack first 11 integers (44 bytes)
        fields = struct.unpack(">11I", data[0:44])

        # Parse quote data according to official Zerodha documentation
        # Official field names: last_price, last_quantity, average_price, volume, buy_quantity, sell_quantity
        tick_data.update(
            {
                "last_price": fields[1] / divisor,
                # Official field: last_quantity (also keep legacy alias for backward compatibility)
                "last_quantity": fields[2],
                "last_traded_quantity": fields[2],  # Legacy alias
                # Official field: average_price (also keep legacy alias)
                "average_price": fields[3] / divisor,
                "average_traded_price": fields[3] / divisor,  # Legacy alias
                # Official field: volume (also keep legacy alias)
                "volume": fields[4],
                "volume_traded": fields[4],  # Legacy alias
                # Official field: buy_quantity (also keep legacy alias)
                "buy_quantity": fields[5],
                "total_buy_quantity": fields[5],  # Legacy alias
                # Official field: sell_quantity (also keep legacy alias)
                "sell_quantity": fields[6],
                "total_sell_quantity": fields[6],  # Legacy alias
                "ohlc": {
                    "open": fields[7] / divisor,
                    "high": fields[8] / divisor,
                    "low": fields[9] / divisor,
                    "close": fields[10] / divisor,
                },
            }
        )

        # Calculate percentage change from close price
        if tick_data["ohlc"]["close"] != 0:
            change_pct = (
                (tick_data["last_price"] - tick_data["ohlc"]["close"])
                * 100
                / tick_data["ohlc"]["close"]
            )
            tick_data["change"] = change_pct
            # ✅ Also store as net_change (Zerodha official field name)
            # net_change is the absolute change from yesterday's close to last traded price
            tick_data["net_change"] = change_pct
        else:
            tick_data["change"] = 0.0
            tick_data["net_change"] = 0.0

        # Populate canonical price change fields for downstream detectors
        self._update_price_change_fields(tick_data)

    def _parse_full_mode_data(self, data: bytes, tick_data: Dict, divisor: float):
        """Parse additional full mode data (bytes 44-64) - 5 integers"""
        # Unpack next 5 integers (20 bytes)
        fields = struct.unpack(">5I", data[44:64])

        raw_last_trade = fields[0]
        raw_exchange_timestamp = fields[4]

        # Parse timestamps
        try:
            last_trade_time = datetime.fromtimestamp(raw_last_trade) if raw_last_trade > 0 else None
        except Exception:
            last_trade_time = None

        try:
            exchange_timestamp = datetime.fromtimestamp(raw_exchange_timestamp) if raw_exchange_timestamp > 0 else None
        except Exception:
            exchange_timestamp = None

        # Parse full mode data according to official Zerodha documentation
        # Official fields: open_interest (oi), oi_day_high, oi_day_low
        tick_data.update(
            {
                "last_trade_time": last_trade_time.isoformat() if last_trade_time else "",
                "last_traded_timestamp": raw_last_trade,
                "last_traded_timestamp_ms": raw_last_trade * 1000 if raw_last_trade else 0,
                "last_trade_time_ms": raw_last_trade * 1000 if raw_last_trade else 0,
                # Official field: open_interest (also keep oi as alias for backward compatibility)
                "open_interest": fields[1],
                "oi": fields[1],  # Legacy alias
                # Official fields: oi_day_high, oi_day_low
                "oi_day_high": fields[2],
                "oi_day_low": fields[3],
                "exchange_timestamp": exchange_timestamp.isoformat() if exchange_timestamp else "",
                "exchange_timestamp_epoch": raw_exchange_timestamp,
                "exchange_timestamp_ms": raw_exchange_timestamp * 1000 if raw_exchange_timestamp else 0,
            }
        )

    def _parse_market_depth(self, data: bytes, tick_data: Dict, divisor: float):
        """
        Parse market depth structure (bytes 64-184) - 120 bytes total

        Structure:
        - 5 buy levels (bytes 64-124): 5 × 12 bytes
        - 5 sell levels (bytes 124-184): 5 × 12 bytes
        Each level: 4 bytes quantity + 4 bytes price + 2 bytes orders + 2 bytes padding
        """
        depth = {"buy": [], "sell": []}

        # Parse 5 buy levels (bytes 64-124)
        for i in range(5):
            offset = 64 + (i * 12)
            if offset + 12 <= len(data):
                quantity = struct.unpack(">I", data[offset : offset + 4])[0]
                price = struct.unpack(">I", data[offset + 4 : offset + 8])[0]
                orders = struct.unpack(">H", data[offset + 8 : offset + 10])[0]
                # Skip 2 bytes padding

                if price > 0:
                    depth["buy"].append(
                        {
                            "level": i + 1,
                            "price": price / divisor,
                            "quantity": quantity,
                            "orders": orders,
                        }
                    )

        # Parse 5 sell levels (bytes 124-184)
        for i in range(5):
            offset = 124 + (i * 12)
            if offset + 12 <= len(data):
                quantity = struct.unpack(">I", data[offset : offset + 4])[0]
                price = struct.unpack(">I", data[offset + 4 : offset + 8])[0]
                orders = struct.unpack(">H", data[offset + 8 : offset + 10])[0]
                # Skip 2 bytes padding

                if price > 0:
                    depth["sell"].append(
                        {
                            "level": i + 1,
                            "price": price / divisor,
                            "quantity": quantity,
                            "orders": orders,
                        }
                    )

        tick_data["depth"] = depth

        # Extract best bid/ask for easy access
        if depth["buy"]:
            best_bid = depth["buy"][0]
            tick_data["best_bid_price"] = best_bid["price"]
            tick_data["best_bid_quantity"] = best_bid["quantity"]
            tick_data["best_bid_orders"] = best_bid["orders"]

        if depth["sell"]:
            best_ask = depth["sell"][0]
            tick_data["best_ask_price"] = best_ask["price"]
            tick_data["best_ask_quantity"] = best_ask["quantity"]
            tick_data["best_ask_orders"] = best_ask["orders"]

    def _extract_circuit_limits(self, data: bytes, tick_data: Dict, divisor: float, inst_info: Dict):
        """
        Extract upper circuit and lower circuit limits from Zerodha full mode packet.
        
        According to official Zerodha documentation:
        - lower_circuit_limit: The current lower circuit limit
        - upper_circuit_limit: The current upper circuit limit
        
        Circuit limits are typically provided in instrument info or can be extracted
        from the packet data. They represent the maximum and minimum price limits
        for the day.
        
        Args:
            data: Full packet bytes (184 bytes)
            tick_data: Tick data dictionary to update
            divisor: Price divisor for conversion
            inst_info: Instrument info dictionary from instrument_info
        """
        try:
            # Try to get circuit limits from instrument info first (most reliable)
            # Official field names per Zerodha documentation
            upper_circuit = inst_info.get("upper_circuit_limit") or inst_info.get("upper_circuit")
            lower_circuit = inst_info.get("lower_circuit_limit") or inst_info.get("lower_circuit")
            
            # If not in instrument info, try to extract from packet
            # Note: Zerodha full mode packet structure may vary - check if circuits are in bytes 44-64
            # or if they need to be calculated from base price and percentage
            if upper_circuit is None or lower_circuit is None:
                # Some packet structures include circuit limits in additional fields
                # If available in packet, extract here (adjust byte positions based on actual structure)
                # For now, we'll rely on instrument_info which Zerodha provides
                pass
            
            # Convert to float and apply divisor if they're raw integers
            if upper_circuit is not None:
                if isinstance(upper_circuit, (int, float)) and upper_circuit > 1000:
                    # Likely raw price value, apply divisor
                    upper_circuit = float(upper_circuit) / divisor
                else:
                    upper_circuit = float(upper_circuit) if upper_circuit else None
                    
            if lower_circuit is not None:
                if isinstance(lower_circuit, (int, float)) and lower_circuit > 1000:
                    # Likely raw price value, apply divisor
                    lower_circuit = float(lower_circuit) / divisor
                else:
                    lower_circuit = float(lower_circuit) if lower_circuit else None
            
            # Store circuit limits using official Zerodha field names
            # Official field names: lower_circuit_limit, upper_circuit_limit
            if upper_circuit is not None:
                tick_data["upper_circuit_limit"] = upper_circuit
                tick_data["upper_circuit"] = upper_circuit  # Legacy alias for backward compatibility
            if lower_circuit is not None:
                tick_data["lower_circuit_limit"] = lower_circuit
                tick_data["lower_circuit"] = lower_circuit  # Legacy alias for backward compatibility
                
        except Exception as e:
            # Don't fail the entire parse if circuit extraction fails
            pass

    def _token_to_symbol(self, instrument_token: int) -> str:
        return self.binary_parser.token_to_symbol(instrument_token)

    @staticmethod
    def _parse_int32(buffer: bytes) -> int:
        return struct.unpack(">I", buffer)[0]

    def parse_quote_packet_optimized(
        self,
        packet_data: bytes,
        instrument_token: int,
        redis_client=None,
    ) -> Optional[Dict[str, Any]]:
        if len(packet_data) < 64:
            return None

        symbol = self._token_to_symbol(instrument_token)
        segment = instrument_token & 0xFF
        divisor = self._get_price_divisor(segment)

        last_price = self._parse_int32(packet_data[4:8]) / divisor
        high_price = self._parse_int32(packet_data[32:36]) / divisor
        low_price = self._parse_int32(packet_data[36:40]) / divisor
        open_price = self._parse_int32(packet_data[28:32]) / divisor
        close_price = self._parse_int32(packet_data[40:44]) / divisor

        cumulative_volume = self._parse_int32(packet_data[16:20])
        exchange_timestamp = self._parse_int32(packet_data[60:64])
        last_traded_timestamp = self._parse_int32(packet_data[44:48])

        parser_redis = redis_client or self.redis_client

        incremental_volume = None
        if parser_redis and hasattr(parser_redis, "update_symbol_data_direct"):
            incremental_volume = parser_redis.update_symbol_data_direct(
                symbol=symbol,
                cumulative_volume=cumulative_volume,
                last_price=last_price,
                timestamp=exchange_timestamp or int(time.time()),
                high=high_price,
                low=low_price,
                open_price=open_price,
                close_price=close_price,
            )

        tick_data = {
            "symbol": symbol,
            "instrument_token": instrument_token,
            "last_price": last_price,
            "last_quantity": self._parse_int32(packet_data[8:12]),
            "average_price": self._parse_int32(packet_data[12:16]) / divisor,
            "cumulative_volume": cumulative_volume,
            "total_buy_quantity": self._parse_int32(packet_data[20:24]),
            "total_sell_quantity": self._parse_int32(packet_data[24:28]),
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "last_traded_timestamp": last_traded_timestamp,
            "open_interest": self._parse_int32(packet_data[48:52]),
            "exchange_timestamp": exchange_timestamp,
            "incremental_volume": incremental_volume,
        }

        return tick_data


class ParserHealthMonitor:
    """Monitor parser health and provide validation - merged from bulletproof_parser"""
    
    def __init__(self, parser: ZerodhaWebSocketMessageParser):
        self.parser = parser
        self.logger = logging.getLogger(__name__)
        self.parse_stats = {
            'total_messages': 0,
            'successful_parses': 0,
            'failed_parses': 0,
            'invalid_ticks': 0,
            'valid_ticks': 0
        }
    
    def monitor_parse_health(self, message_data: bytes) -> List[Dict[str, Any]]:
        """Monitor parsing health and return validated ticks"""
        self.parse_stats['total_messages'] += 1
        
        try:
            ticks = self.parser.parse_websocket_message(message_data)
            
            if ticks:
                self.parse_stats['successful_parses'] += 1
                
                # Validate each tick
                valid_ticks = []
                for tick in ticks:
                    if self._validate_tick(tick):
                        valid_ticks.append(tick)
                        self.parse_stats['valid_ticks'] += 1
                    else:
                        self.parse_stats['invalid_ticks'] += 1
                        self.logger.warning(f"Invalid tick: {tick.get('symbol', 'unknown')}")
                
                return valid_ticks
            else:
                self.parse_stats['failed_parses'] += 1
                return []
                
        except Exception as e:
            self.parse_stats['failed_parses'] += 1
            self.logger.error(f"❌ Health monitor error: {e}")
            return []
    
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
    
    def get_health_stats(self) -> Dict[str, Any]:
        """Get parser health statistics"""
        total = self.parse_stats['total_messages']
        success_rate = (self.parse_stats['successful_parses'] / total * 100) if total > 0 else 0
        valid_rate = (self.parse_stats['valid_ticks'] / (self.parse_stats['valid_ticks'] + self.parse_stats['invalid_ticks']) * 100) if (self.parse_stats['valid_ticks'] + self.parse_stats['invalid_ticks']) > 0 else 0
        
        return {
            **self.parse_stats,
            'success_rate': round(success_rate, 2),
            'valid_tick_rate': round(valid_rate, 2),
            'error_rate_exceeded': self.parser._error_rate_exceeded() if hasattr(self.parser, '_error_rate_exceeded') else False
        }
