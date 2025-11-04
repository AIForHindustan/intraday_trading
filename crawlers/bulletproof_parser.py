"""
Bulletproof Zerodha WebSocket Parser with comprehensive error handling
"""
import logging
import time
import zlib
from datetime import datetime
from typing import List, Dict, Any, Optional

from crawlers.utils.instrument_mapper import InstrumentMapper
from utils.correct_volume_calculator import CorrectVolumeCalculator


class BulletproofZerodhaParser:
    def __init__(self, instrument_info: Dict[int, Dict], redis_client=None):
        self.instrument_info = instrument_info
        self.redis_client = redis_client
        self.logger = logging.getLogger(__name__)
        
        # Initialize instrument mapper for symbol resolution
        self.instrument_mapper = InstrumentMapper(instrument_info)
        
        # Initialize volume calculator if Redis is available
        if redis_client:
            self.volume_calculator = CorrectVolumeCalculator(redis_client)
        else:
            self.volume_calculator = None
        
        # Error tracking
        self.error_counts = {}
        self.last_error_time = {}
        self.max_errors_per_minute = 10
        
        # Use existing parser for volume normalization (delegate to it)
        from crawlers.websocket_message_parser import ZerodhaWebSocketMessageParser
        self._legacy_parser = ZerodhaWebSocketMessageParser(instrument_info, redis_client)
        
    def parse_websocket_message(self, message_data: bytes) -> List[Dict[str, Any]]:
        """Parse messages with comprehensive error handling"""
        if not message_data:
            return []
        
        # Check error rate limit
        if self._error_rate_exceeded():
            self.logger.warning("🚨 Error rate exceeded, skipping message")
            return []
        
        try:
            # Decompress if needed
            if self._is_compressed(message_data):
                try:
                    message_data = zlib.decompress(message_data)
                except zlib.error as e:
                    self._record_error("decompression")
                    self.logger.error(f"❌ Decompression failed: {e}")
                    return []
            
            return self._parse_message_structure(message_data)
            
        except Exception as e:
            self._record_error("general")
            self.logger.error(f"❌ Critical parse error: {e}")
            return []
    
    def _parse_message_structure(self, message_data: bytes) -> List[Dict[str, Any]]:
        """Parse message structure with packet-level error isolation"""
        ticks = []
        
        try:
            # Parse header
            if len(message_data) < 4:
                return []
            
            num_packets = int.from_bytes(message_data[0:2], byteorder="big", signed=False)
            offset = 2
            
            for packet_index in range(num_packets):
                if offset + 2 > len(message_data):
                    break
                
                # Parse each packet in isolation
                packet_ticks = self._parse_single_packet(message_data, offset, packet_index)
                ticks.extend(packet_ticks)
                
                # Update offset safely
                packet_length = int.from_bytes(
                    message_data[offset:offset+2], byteorder="big", signed=False
                )
                offset += 2 + packet_length
                
        except Exception as e:
            self._record_error("structure")
            self.logger.error(f"❌ Message structure error: {e}")
        
        return ticks
    
    def _parse_single_packet(self, message_data: bytes, offset: int, packet_index: int) -> List[Dict]:
        """Parse single packet - errors in one packet don't affect others"""
        try:
            packet_length = int.from_bytes(
                message_data[offset:offset+2], byteorder="big", signed=False
            )
            
            if packet_length <= 0 or offset + 2 + packet_length > len(message_data):
                return []
            
            packet_data = message_data[offset+2:offset+2+packet_length]
            
            # Route to appropriate parser
            if packet_length == 184:
                tick = self._parse_full_packet_safe(packet_data, packet_index)
                return [tick] if tick else []
            elif packet_length == 32:
                tick = self._parse_index_packet_safe(packet_data, packet_index)
                return [tick] if tick else []
            elif packet_length == 8:
                tick = self._parse_ltp_packet_safe(packet_data, packet_index)
                return [tick] if tick else []
            else:
                # For other packet types, delegate to legacy parser
                try:
                    # Create a wrapper dict for legacy parser compatibility
                    wrapper = {"packet_data": packet_data, "packet_length": packet_length}
                    tick = self._legacy_parser.parse_zerodha_packet(wrapper)
                    return [tick] if tick else []
                except Exception as e:
                    self.logger.debug(f"Legacy parser fallback failed for packet {packet_index}: {e}")
                    return []
                
        except Exception as e:
            self._record_error(f"packet_{packet_index}")
            self.logger.error(f"❌ Packet {packet_index} parse error: {e}")
            return []
    
    def _parse_full_packet_safe(self, packet_data: bytes, packet_index: int) -> Optional[Dict]:
        """Safe full packet parsing with validation"""
        try:
            if len(packet_data) != 184:
                self.logger.warning(f"Invalid full packet length: {len(packet_data)}")
                return None
            
            # Parse with bounds checking
            instrument_token = self._parse_int32_safe(packet_data, 0, 4)
            if not instrument_token:
                return None
            
            # Get symbol safely
            symbol = self._token_to_symbol_safe(instrument_token)
            
            # Parse all fields with validation using legacy parser for full packet structure
            # Delegate to legacy parser for complete field parsing (OHLC, depth, etc.)
            try:
                wrapper = {"packet_data": packet_data, "packet_length": 184}
                tick_data = self._legacy_parser.parse_zerodha_packet(wrapper)
                
                if not tick_data:
                    return None
                
                # Validate required fields
                if not tick_data.get("last_price") or not tick_data.get("symbol"):
                    return None
                
                # Add packet index for tracking
                tick_data["packet_index"] = packet_index
                return tick_data
            except Exception as e:
                self._record_error("full_packet_delegation")
                self.logger.error(f"❌ Legacy parser delegation failed: {e}")
                return None
            
        except Exception as e:
            self._record_error("full_packet")
            self.logger.error(f"❌ Full packet parse error: {e}")
            return None
    
    def _parse_int32_safe(self, data: bytes, start: int, end: int) -> Optional[int]:
        """Safe integer parsing with bounds checking"""
        try:
            if end > len(data):
                return None
            return int.from_bytes(data[start:end], byteorder="big", signed=False)
        except:
            return None
    
    def _parse_price_safe(self, data: bytes, start: int, end: int, instrument_token: int) -> Optional[float]:
        """Safe price parsing with divisor detection"""
        try:
            raw_price = self._parse_int32_safe(data, start, end)
            if raw_price is None:
                return None
                
            divisor = self._get_price_divisor_safe(instrument_token)
            return raw_price / divisor if divisor else None
        except:
            return None
    
    def _get_price_divisor_safe(self, instrument_token: int) -> float:
        """Safe divisor detection"""
        try:
            segment = instrument_token & 0xFF
            return 10000000.0 if segment in [3, 6] else 100.0
        except:
            return 100.0  # Fallback
    
    def _token_to_symbol_safe(self, instrument_token: int) -> str:
        """Safe symbol resolution"""
        try:
            symbol = self.instrument_mapper.token_to_symbol(instrument_token)
            return symbol if not symbol.startswith("UNKNOWN_") else f"UNKNOWN_{instrument_token}"
        except:
            return f"UNKNOWN_{instrument_token}"
    
    def _is_compressed(self, data: bytes) -> bool:
        """Check if data is zlib compressed"""
        if len(data) < 2:
            return False
        # Check for zlib header (78 01, 78 9C, 78 DA)
        return data[0] == 0x78 and data[1] in [0x01, 0x9C, 0xDA]
    
    def _parse_index_packet_safe(self, packet_data: bytes, packet_index: int) -> Optional[Dict]:
        """Safe index packet parsing (32 bytes)"""
        try:
            if len(packet_data) != 32:
                return None
            
            instrument_token = self._parse_int32_safe(packet_data, 0, 4)
            if not instrument_token:
                return None
            
            symbol = self._token_to_symbol_safe(instrument_token)
            
            tick_data = {
                "instrument_token": instrument_token,
                "symbol": symbol,
                "last_price": self._parse_price_safe(packet_data, 4, 8, instrument_token),
                "high": self._parse_price_safe(packet_data, 8, 12, instrument_token),
                "low": self._parse_price_safe(packet_data, 12, 16, instrument_token),
                "open": self._parse_price_safe(packet_data, 16, 20, instrument_token),
                "close": self._parse_price_safe(packet_data, 20, 24, instrument_token),
                "timestamp": datetime.now().isoformat(),
                "packet_index": packet_index,
                "mode": "index"
            }
            
            if not tick_data.get("last_price") or not tick_data.get("symbol"):
                return None
            
            return tick_data
        except Exception as e:
            self._record_error("index_packet")
            self.logger.error(f"❌ Index packet parse error: {e}")
            return None
    
    def _parse_ltp_packet_safe(self, packet_data: bytes, packet_index: int) -> Optional[Dict]:
        """Safe LTP packet parsing (8 bytes)"""
        try:
            if len(packet_data) != 8:
                return None
            
            instrument_token = self._parse_int32_safe(packet_data, 0, 4)
            if not instrument_token:
                return None
            
            symbol = self._token_to_symbol_safe(instrument_token)
            
            tick_data = {
                "instrument_token": instrument_token,
                "symbol": symbol,
                "last_price": self._parse_price_safe(packet_data, 4, 8, instrument_token),
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
    
    def _error_rate_exceeded(self) -> bool:
        """Check if we're getting too many errors too fast"""
        current_minute = int(time.time() / 60)
        
        # Clean old error counts
        self.error_counts = {
            k: v for k, v in self.error_counts.items() 
            if self.last_error_time.get(k, 0) > current_minute - 2
        }
        
        total_errors = sum(self.error_counts.values())
        return total_errors > self.max_errors_per_minute
    
    def _record_error(self, error_type: str):
        """Track error rates"""
        current_minute = int(time.time() / 60)
        
        if error_type not in self.error_counts or self.last_error_time.get(error_type, 0) < current_minute:
            self.error_counts[error_type] = 0
        
        self.error_counts[error_type] += 1
        self.last_error_time[error_type] = current_minute


class ParserHealthMonitor:
    """Monitor parser health and provide validation"""
    
    def __init__(self, parser: BulletproofZerodhaParser):
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