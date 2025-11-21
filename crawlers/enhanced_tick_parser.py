# enhanced_tick_parser.py
import logging
import time
from typing import Dict, Any, Optional
import json

logger = logging.getLogger(__name__)

class EnhancedTickParser:
    """
    Enhanced tick parser that preserves ALL fields from Zerodha WebSocket
    
    This parser is designed to work alongside ZerodhaWebSocketMessageParser
    and ensures complete data preservation for Redis storage.
    """
    
    def __init__(self, instrument_info: Optional[Dict[int, Dict]] = None):
        """
        Initialize enhanced parser
        
        Args:
            instrument_info: Optional mapping of instrument_token -> instrument details
                           for symbol resolution
        """
        self.instrument_info = instrument_info or {}
        self.required_fields = [
            'instrument_token', 'timestamp', 'last_price', 'volume', 'average_price',
            'buy_quantity', 'sell_quantity', 'open_interest', 'last_quantity',
            'ohlc', 'net_change', 'oi', 'oi_day_high', 'oi_day_low',
            'depth', 'lower_circuit_limit', 'upper_circuit_limit'
        ]
    
    def parse_raw_message(self, raw_message: Any) -> Dict[str, Any]:
        """
        Parse raw WebSocket message and preserve ALL fields
        
        Args:
            raw_message: Raw message (can be bytes, str, or dict)
            
        Returns:
            Dict with all tick data fields preserved
        """
        try:
            # Handle different input types
            if isinstance(raw_message, bytes):
                # Binary message - should be handled by ZerodhaWebSocketMessageParser
                logger.warning("EnhancedTickParser received bytes - use ZerodhaWebSocketMessageParser for binary parsing")
                return {}
            elif isinstance(raw_message, str):
                tick_data = json.loads(raw_message)
            elif isinstance(raw_message, dict):
                tick_data = raw_message
            else:
                logger.warning(f"Unknown message type: {type(raw_message)}")
                return {}
            
            # Log structure for debugging
            if 'instrument_token' in tick_data:
                logger.debug(f"📊 RAW TICK KEYS: {list(tick_data.keys())}")
            
            # Preserve ALL fields - don't filter
            return self._structure_tick_data(tick_data)
            
        except Exception as e:
            logger.error(f"Failed to parse raw message: {e}", exc_info=True)
            return {}
    
    def _structure_tick_data(self, raw_data: Dict) -> Dict[str, Any]:
        """
        Structure the tick data preserving ALL fields
        
        This method preserves all fields from raw_data without filtering.
        """
        # Start with all raw data (preserve everything)
        structured = raw_data.copy()
        
        # Ensure required nested structures exist
        if 'ohlc' in raw_data and isinstance(raw_data['ohlc'], dict):
            structured['ohlc'] = raw_data['ohlc']
        elif 'ohlc' not in structured:
            structured['ohlc'] = {}
        
        # Ensure depth structure exists
        if 'depth' in raw_data and isinstance(raw_data['depth'], dict):
            structured['depth'] = raw_data['depth']
        elif 'depth' not in structured:
            structured['depth'] = {'buy': [], 'sell': []}
        
        # Add symbol if missing (try to resolve from instrument_token)
        if 'symbol' not in structured or not structured['symbol']:
            instrument_token = structured.get('instrument_token')
            if instrument_token:
                structured['symbol'] = self._map_instrument_to_symbol(instrument_token)
        
        # Add metadata
        structured['_parsed_at'] = time.time()
        structured['_source'] = 'kite_websocket'
        
        return structured
    
    def _map_instrument_to_symbol(self, instrument_token: int) -> str:
        """
        Map instrument_token to symbol name
        
        Uses instrument_info if available, otherwise returns token-based name.
        """
        # Try to get symbol from instrument_info
        if self.instrument_info and instrument_token in self.instrument_info:
            inst_info = self.instrument_info[instrument_token]
            symbol = inst_info.get('symbol') or inst_info.get('tradingsymbol')
            if symbol:
                return symbol
        
        # Fallback to token-based name
        return f"TOKEN_{instrument_token}"