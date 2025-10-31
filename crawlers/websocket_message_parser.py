# binary_parser/websocket_message_parser.py
import logging
import struct
import time
import zlib
from datetime import datetime
from typing import List, Dict, Any, Optional

from crawlers.utils.instrument_mapper import InstrumentMapper
from utils.correct_volume_calculator import CorrectVolumeCalculator


class ZerodhaBinaryParser:
    """Low-level parser for Zerodha WebSocket binary packet structures."""

    def __init__(
        self,
        instrument_mapper: InstrumentMapper,
        redis_client=None,
    ):
        self.logger = logging.getLogger(__name__)
        self.instrument_mapper = instrument_mapper or InstrumentMapper()
        self.redis_client = getattr(redis_client, "redis_client", redis_client)

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
            self.logger.debug("Unknown instrument token %s (file-only crawler, no Redis)", token)
            return

        # Only log to Redis for crawlers that use Redis (like intraday crawler)
        try:
            key = f"unknown_tokens:{datetime.now().strftime('%Y-%m-%d')}"
            self.redis_client.sadd(key, token)
            self.redis_client.expire(key, 86400)
        except Exception as exc:
            self.logger.debug("Failed to log unknown token %s: %s", token, exc)


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
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        self.instrument_mapper = InstrumentMapper(instrument_info)
        # Binary parser only needs Redis for intraday crawlers (for logging unknown tokens)
        # File-only crawlers (data_mining, research) should pass redis_client=None
        redis_for_parser = None
        if redis_client:
            redis_for_parser = getattr(redis_client, "redis_client", redis_client)
        self.binary_parser = ZerodhaBinaryParser(
            self.instrument_mapper, redis_for_parser
        )
        # Only initialize volume calculator with Redis for intraday crawlers
        if redis_client:
            self.volume_calculator = CorrectVolumeCalculator(getattr(redis_client, "redis_client", redis_client))
        else:
            self.volume_calculator = None
        # ✅ LEGACY REMOVED: Volume tracking now handled by VolumeStateManager (single source of truth)
        # self.last_cumulative_volumes and self.last_incremental_volumes removed
        # All volume calculations now go through VolumeStateManager.calculate_incremental()

    def parse_websocket_message(self, message_data: bytes) -> List[Dict[str, Any]]:
        """
        Parse complete WebSocket message containing multiple packets

        Message structure:
        - First 2 bytes: Number of packets (SHORT/int16)
        - Then for each packet: 2 bytes length + packet data
        """
        ticks = []

        try:
            # Check if message is compressed
            if self._is_compressed(message_data):
                try:
                    message_data = zlib.decompress(message_data)
                except zlib.error as e:
                    print(f"⚠️ Zlib decompression failed: {e}")
                    return ticks

            # Parse message header
            if len(message_data) < 4:
                return ticks

            packet_wrappers = self.binary_parser.parse_binary_message(message_data)

            for wrapper in packet_wrappers:
                if wrapper.get("packet_length") == 184:
                    tick = self.parse_zerodha_packet(wrapper)
                    if tick:
                        ticks.append(tick)

        except Exception as e:
            print(f"⚠️ Protocol error in WebSocket message parsing: {e}")

        return ticks

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

            # Provide canonical aliases for downstream compatibility
            tick["last_price"] = tick.get("last_price", last_traded_price)
            tick["last_traded_price"] = tick.get("last_price")
            tick["cumulative_volume"] = tick.get("cumulative_volume", cumulative_volume)
            tick["zerodha_cumulative_volume"] = tick.get("cumulative_volume")

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
        - VolumeStateManager automatically integrates with VolumeProfileManager
        - Volume profile updates happen automatically during incremental volume calculation
        - POC, Value Area, and support/resistance levels are calculated in real-time
        - No additional code needed here - integration is seamless
        """
        try:
            from redis_files.volume_state_manager import get_volume_manager
            
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

            # SINGLE CALCULATION POINT: WebSocket Parser ONLY - Calculate incremental volume
            volume_manager = get_volume_manager()
            incremental = volume_manager.calculate_incremental(
                instrument_token=str(tick_data.get('instrument_token', symbol)),
                current_cumulative=cumulative_int,
                exchange_timestamp=exchange_timestamp  # Must be datetime
            )

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
                    from utils.correct_volume_calculator import VolumeResolver
                    verification = VolumeResolver.verify_volume_consistency(tick_data, "websocket_ingestion")
                    if not verification["consistent"]:
                        self.logger.warning(f"Volume consistency issue at WebSocket ingestion for {symbol}: {verification['issues']}")
            except Exception as calc_exc:
                self.logger.debug(f"Volume metric calculation skipped for {symbol}: {calc_exc}")

            return tick_data

        except Exception as e:
            self.logger.error(f"❌ Volume normalization error: {e}")
            # Fallback to original data
            return tick_data

    def reset_volume_session(self):
        """✅ LEGACY REMOVED: Volume session reset now handled by VolumeStateManager"""
        # VolumeStateManager handles session resets automatically
        pass

    def on_market_open(self):
        """✅ LEGACY REMOVED: Market open handling now managed by VolumeStateManager"""
        # VolumeStateManager handles market session boundaries automatically
        self.logger.info("Market open - volume tracking managed by VolumeStateManager")

    def on_market_close(self):
        """✅ LEGACY REMOVED: Market close handling now managed by VolumeStateManager"""
        # VolumeStateManager handles market session boundaries automatically
        self.logger.info("Market close - volume tracking managed by VolumeStateManager")
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
            instrument_token = struct.unpack(">I", packet_data[0:4])[0]
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

        # Parse basic quote data (bytes 0-44)
        self._parse_quote_data(packet_data, tick_data, divisor)

        # Parse additional full mode data (bytes 44-64)
        self._parse_full_mode_data(packet_data, tick_data, divisor)

        # Parse market depth (bytes 64-184)
        self._parse_market_depth(packet_data, tick_data, divisor)

        # Store raw cumulative volume from Zerodha as-is (no calculations)
        cumulative_volume = tick_data.get("volume_traded", 0)
        tick_data["cumulative_volume"] = cumulative_volume
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

        return tick_data

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

        tick_data.update(
            {
                "last_price": fields[1] / divisor,
                "last_traded_quantity": fields[2],
                "average_traded_price": fields[3] / divisor,
                "volume_traded": fields[4],
                "total_buy_quantity": fields[5],
                "total_sell_quantity": fields[6],
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
            tick_data["change"] = (
                (tick_data["last_price"] - tick_data["ohlc"]["close"])
                * 100
                / tick_data["ohlc"]["close"]
            )
        else:
            tick_data["change"] = 0.0

    def _parse_full_mode_data(self, data: bytes, tick_data: Dict, divisor: float):
        """Parse additional full mode data (bytes 44-64) - 5 integers"""
        # Unpack next 5 integers (20 bytes)
        fields = struct.unpack(">5I", data[44:64])

        raw_last_trade = fields[0]
        raw_exchange_timestamp = fields[4]

        # Parse timestamps
        try:
            last_trade_time = (
                datetime.fromtimestamp(raw_last_trade) if raw_last_trade > 0 else None
            )
        except:
            last_trade_time = None

        try:
            exchange_timestamp = (
                datetime.fromtimestamp(raw_exchange_timestamp)
                if raw_exchange_timestamp > 0
                else None
            )
        except:
            exchange_timestamp = None

        tick_data.update(
            {
                "last_trade_time": last_trade_time,
                "oi": fields[1],
                "oi_day_high": fields[2],
                "oi_day_low": fields[3],
                "exchange_timestamp": exchange_timestamp,
                "exchange_timestamp_epoch": raw_exchange_timestamp,
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
