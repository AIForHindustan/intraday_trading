"""
Unified Schema Definitions
=========================

Single source of truth for data structure across:
- WebSocket Crawlers
- Market Scanner  
- Pattern Detectors
- Parquet Storage
- Redis Publishing
- Field Mapping Integration
- Volume State Management
"""

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

# Import field mapping and volume state manager for integration
try:
    from utils.yaml_field_loader import get_field_mapping_manager, normalize_session_record, normalize_indicator_record
    from redis_files.volume_state_manager import get_volume_manager
    FIELD_MAPPING_AVAILABLE = True
    VOLUME_MANAGER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Field mapping or volume manager not available: {e}")
    FIELD_MAPPING_AVAILABLE = False
    VOLUME_MANAGER_AVAILABLE = False

# ============================================
# KITE FIELD MAPPING (What Kite sends → What we save)
# Standardized to match Kite Connect API documentation exactly
# ============================================
KITE_TO_UNIFIED_MAPPING = {
    # ===== CORE IDENTIFIERS =====
    "instrument_token": "instrument_token",
    "tradingsymbol": "tradingsymbol",
    "timestamp": "timestamp",
    "last_trade_time": "last_trade_time",
    "last_traded_timestamp": "last_trade_time",  # ✅ FIXED: Map legacy field to correct one
    "exchange_timestamp": "exchange_timestamp",
    # ===== PRICE DATA =====
    # WebSocket uses these exact field names
    "last_price": "last_price",
    "last_traded_quantity": "zerodha_last_traded_quantity",
    "last_quantity": "zerodha_last_traded_quantity",  # Legacy alias → canonical field
    "average_traded_price": "average_price",  # WebSocket field → unified field
    "net_change": "net_change",
    "change": "change",
    # ===== VOLUME DATA =====
    # WebSocket uses these exact field names
    "volume_traded": "zerodha_cumulative_volume",
    "volume": "zerodha_cumulative_volume",  # Legacy alias → canonical cumulative volume
    "total_buy_quantity": "buy_quantity",  # WebSocket field → unified field
    "total_sell_quantity": "sell_quantity",  # WebSocket field → unified field
    "buy_quantity": "buy_quantity",  # Binary crawler field → unified field
    "sell_quantity": "sell_quantity",  # Binary crawler field → unified field
    # ===== OHLC DATA =====
    "ohlc": "ohlc",
    # ===== DEPTH DATA =====
    "depth": "depth",
    # ===== OPEN INTEREST (F&O) =====
    "oi": "oi",
    "oi_day_high": "oi_day_high",
    "oi_day_low": "oi_day_low",
}

# ============================================
# UNIFIED TICK SCHEMA (PyArrow Schema)
# Single source of truth for all tick data
# ============================================
if PYARROW_AVAILABLE:
    try:
        UNIFIED_TICK_SCHEMA = pa.schema([
        # Core identifiers
        pa.field("instrument_token", pa.int64()),
        pa.field("tradingsymbol", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("timestamp", pa.string()),
        pa.field("timestamp_ms", pa.int64()),
        pa.field("last_trade_time", pa.string()),
        pa.field("exchange_timestamp", pa.string()),
        pa.field("exchange_timestamp_ms", pa.int64()),
        
        # Price data
        pa.field("last_price", pa.float64()),
        pa.field("last_quantity", pa.int64()),
        pa.field("average_price", pa.float64()),
        pa.field("net_change", pa.float64()),
        pa.field("change", pa.float64()),
        pa.field("price_change_pct", pa.float64()),
        
        # Volume data
        pa.field("zerodha_cumulative_volume", pa.int64()),
        pa.field("zerodha_last_traded_quantity", pa.int64()),
        pa.field("bucket_cumulative_volume", pa.int64()),
        pa.field("bucket_incremental_volume", pa.int64()),
        pa.field("volume", pa.int64()),  # Legacy alias → canonical cumulative
        pa.field("volume_traded", pa.int64()),  # Legacy alias → canonical cumulative
        pa.field("cumulative_volume", pa.int64()),  # Legacy alias → canonical bucket cumulative
        pa.field("incremental_volume", pa.int64()),  # Legacy alias → canonical bucket incremental
        pa.field("buy_quantity", pa.int64()),
        pa.field("sell_quantity", pa.int64()),
        pa.field("total_buy_quantity", pa.int64()),
        pa.field("total_sell_quantity", pa.int64()),
        
        # OHLC data
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        
        # Market depth
        pa.field("depth", pa.string()),  # JSON string
        pa.field("depth_valid", pa.bool_()),
        pa.field("best_bid_price", pa.float64()),
        pa.field("best_bid_quantity", pa.int64()),
        pa.field("best_ask_price", pa.float64()),
        pa.field("best_ask_quantity", pa.int64()),
        
        # Open Interest (F&O)
        pa.field("oi", pa.int64()),
        pa.field("oi_day_high", pa.int64()),
        pa.field("oi_day_low", pa.int64()),
        
        # Calculated fields
        pa.field("volume_ratio", pa.float64()),
        pa.field("normalized_volume", pa.float64()),
        pa.field("volume_context", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("mode", pa.string()),
        pa.field("tradable", pa.bool_()),
        pa.field("exchange", pa.string()),
        pa.field("segment", pa.string()),
        
        # External data
        pa.field("news", pa.string()),  # JSON string
        pa.field("news_context", pa.string()),  # JSON string
        pa.field("spoofing_detected", pa.bool_()),
        pa.field("spoofing_score", pa.float64()),
        pa.field("spoofing_type", pa.string()),
        pa.field("spoofing_confidence", pa.float64()),
        pa.field("spoofing_direction", pa.string()),
        pa.field("spoofing_details", pa.string()),  # JSON string
        pa.field("spoofing_confirmed", pa.bool_()),
        pa.field("spoofing_blocked", pa.bool_()),
        pa.field("spoofing_block_reason", pa.string()),
        
        # Derivatives specific
        pa.field("underlying_price", pa.float64()),
        pa.field("days_to_expiry", pa.int64()),
        pa.field("time_to_expiry", pa.float64()),
        pa.field("is_expiry_week", pa.bool_()),
        pa.field("is_expiry_day", pa.bool_()),
        
        # Metadata
        pa.field("processed_at", pa.string()),
        pa.field("source", pa.string()),
    ])
    except AttributeError:
        # Fallback if PyArrow is not available or has different API
        UNIFIED_TICK_SCHEMA = None
else:
    # PyArrow not available
    UNIFIED_TICK_SCHEMA = None

# ============================================
# REDIS SESSION DATA SCHEMA (Standardized across all components)
# ============================================
REDIS_SESSION_SCHEMA = {
    # Core session data - standardized field names
    "zerodha_cumulative_volume": int,   # Total session volume from Zerodha
    "bucket_cumulative_volume": int,    # Session cumulative maintained by scanner
    "bucket_incremental_volume": int,   # Volume delta per update
    "cumulative_volume": int,           # Legacy alias → bucket_cumulative_volume
    "incremental_volume": int,          # Legacy alias → bucket_incremental_volume
    "last_price": float,                # Last traded price
    "high": float,                      # Session high (standardized)
    "low": float,                       # Session low (standardized)
    "session_date": str,               # Trading session date (YYYY-MM-DD)
    "update_count": int,                # Number of updates in session
    "last_update_timestamp": float,    # Last update time (standardized)
    "first_update": float,             # First update time
    "last_update": float,               # Legacy timestamp field
    
    # Backward compatibility fields
    "high_price": float,                # Legacy high field (mapped to 'high')
    "low_price": float,                 # Legacy low field (mapped to 'low')
    
    # Price movement tracking
    "first_price": float,               # Opening price for session
    "close_price": float,               # Closing price (if available)
}

# ============================================
# SCHEMA VALIDATION FUNCTIONS
# ============================================

def validate_unified_tick(tick_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate tick data against unified schema.
    
    Args:
        tick_data: Tick data dictionary
        
    Returns:
        Tuple of (is_valid, issues_list)
    """
    issues = []
    
    # Required fields - instrument_token is optional if symbol is present
    required_fields = ["last_price"]
    for field in required_fields:
        if field not in tick_data:
            issues.append(f"Missing required field: {field}")
    
    # Check for symbol or instrument_token
    if "symbol" not in tick_data and "instrument_token" not in tick_data:
        issues.append("Missing required field: symbol or instrument_token")
    
    # Check for at least one volume field - bucket_incremental_volume is the primary method
    volume_fields = ["bucket_incremental_volume", "bucket_cumulative_volume", "zerodha_cumulative_volume", "volume"]
    has_volume = any(field in tick_data for field in volume_fields)
    if not has_volume:
        issues.append("Missing required field: volume (or any volume field)")
    
    # Numeric field validation - check all volume fields
    numeric_fields = ["last_price", "last_quantity"]
    volume_numeric_fields = ["volume", "bucket_incremental_volume", "bucket_cumulative_volume", "zerodha_cumulative_volume"]
    
    for field in numeric_fields:
        if field in tick_data:
            try:
                float(tick_data[field])
            except (ValueError, TypeError):
                issues.append(f"Invalid numeric value for {field}: {tick_data[field]}")
    
    # Validate volume fields if present
    for field in volume_numeric_fields:
        if field in tick_data:
            try:
                float(tick_data[field])
            except (ValueError, TypeError):
                issues.append(f"Invalid numeric value for {field}: {tick_data[field]}")
    
    # Timestamp validation - handle both epoch milliseconds and ISO strings
    if "timestamp" in tick_data:
        timestamp = tick_data["timestamp"]
        try:
            # Check if it's epoch milliseconds (integer or string that converts to int)
            if isinstance(timestamp, (int, str)) and str(timestamp).isdigit():
                # Convert epoch milliseconds to datetime to validate
                epoch_ms = int(timestamp)
                if epoch_ms > 0:  # Basic validation - must be positive
                    datetime.fromtimestamp(epoch_ms / 1000)
                else:
                    issues.append(f"Invalid timestamp value: {timestamp}")
            else:
                # Try ISO format
                datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except (ValueError, AttributeError, OSError):
            issues.append(f"Invalid timestamp format: {timestamp}")
    
    return len(issues) == 0, issues

def normalize_volume_field(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize volume fields - ensure 'volume' field exists for backward compatibility.
    Uses bucket_incremental_volume as the primary volume method (as per README.md design).
    
    Args:
        tick_data: Tick data dictionary
        
    Returns:
        Tick data with normalized volume field
    """
    # Priority order for volume fields - bucket_incremental_volume is PRIMARY
    volume_priority = [
        "bucket_incremental_volume",  # PRIMARY METHOD (per README.md)
        "bucket_cumulative_volume", 
        "zerodha_cumulative_volume",
        "volume"  # Legacy alias
    ]
    
    # Find the first available volume field and set 'volume' as legacy alias
    for field in volume_priority:
        if field in tick_data and tick_data[field] is not None:
            # Set 'volume' field for backward compatibility with legacy code
            tick_data["volume"] = tick_data[field]
            break
    
    return tick_data

def calculate_derived_fields(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculate derived fields from tick data.
    
    Args:
        tick_data: Cleaned tick data
        
    Returns:
        Dictionary of derived fields
    """
    derived = {}
    
    # Price change percentage
    if "open" in tick_data and "last_price" in tick_data:
        open_price = float(tick_data.get("open", 0))
        last_price = float(tick_data.get("last_price", 0))
        if open_price > 0:
            derived["price_change_pct"] = ((last_price - open_price) / open_price) * 100
        else:
            derived["price_change_pct"] = 0.0
    
    # Volume ratio (if we have historical data)
    if "volume" in tick_data and "normalized_volume" in tick_data:
        volume = float(tick_data.get("volume", 0))
        normalized = float(tick_data.get("normalized_volume", 0))
        if normalized > 0:
            derived["volume_ratio"] = volume / normalized
        else:
            derived["volume_ratio"] = 1.0
    
    # Asset class detection
    symbol = str(tick_data.get("tradingsymbol", ""))
    if "CE" in symbol or "PE" in symbol:
        derived["asset_class"] = "OPT"
    elif "FUT" in symbol:
        derived["asset_class"] = "FUT"
    else:
        derived["asset_class"] = "EQ"
    
    return derived

def map_kite_to_unified(kite_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Kite API data to unified schema.
    
    Args:
        kite_data: Raw data from Kite API
        
    Returns:
        Mapped data dictionary
    """
    mapped = {}
    
    for kite_field, unified_field in KITE_TO_UNIFIED_MAPPING.items():
        if kite_field in kite_data:
            # For volume fields, prioritize volume_traded over volume to avoid 0 override
            if unified_field == "zerodha_cumulative_volume" and kite_field == "volume":
                # Only use volume if volume_traded is not available
                if "volume_traded" not in kite_data or kite_data.get("volume_traded", 0) == 0:
                    mapped[unified_field] = kite_data[kite_field]
            else:
                mapped[unified_field] = kite_data[kite_field]
    
    # Handle special cases
    if "tradingsymbol" in mapped:
        mapped["symbol"] = mapped["tradingsymbol"]
    
    # Add metadata
    mapped["processed_at"] = datetime.now().isoformat()
    mapped["source"] = "kite_api"
    
    return mapped

def normalize_session_data(session_data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
    """
    Normalize session data using field mapping integration.
    
    Args:
        session_data: Raw session data from Redis
        include_aliases: Whether to include legacy aliases
        
    Returns:
        Normalized session data
    """
    if FIELD_MAPPING_AVAILABLE:
        return normalize_session_record(session_data, include_aliases)
    else:
        # Fallback normalization without field mapping
        normalized = session_data.copy()
        
        # Apply basic field mappings
        if 'high_price' in normalized and 'high' not in normalized:
            normalized['high'] = normalized['high_price']
        if 'low_price' in normalized and 'low' not in normalized:
            normalized['low'] = normalized['low_price']
        if 'last_update' in normalized and 'last_update_timestamp' not in normalized:
            normalized['last_update_timestamp'] = normalized['last_update']
        
        return normalized

def normalize_indicator_data(indicator_data: Dict[str, Any], include_aliases: bool = True) -> Dict[str, Any]:
    """
    Normalize indicator data using field mapping integration.
    
    Args:
        indicator_data: Raw indicator data
        include_aliases: Whether to include legacy aliases
        
    Returns:
        Normalized indicator data
    """
    if FIELD_MAPPING_AVAILABLE:
        return normalize_indicator_record(indicator_data, include_aliases)
    else:
        return indicator_data.copy()

def validate_data_with_field_mapping(data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """
    Validate data using field mapping manager.
    
    Args:
        data: Data to validate
        asset_type: Type of asset (equity, futures, options)
        
    Returns:
        Validation result with errors and warnings
    """
    if FIELD_MAPPING_AVAILABLE:
        field_manager = get_field_mapping_manager()
        return field_manager.validate_data(data, asset_type)
    else:
        # Fallback validation
        errors = []
        warnings = []
        
        # Basic required field validation
        required_fields = ["instrument_token", "last_price", "volume"]
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }

def get_standardized_session_key(symbol: str, date: str = None) -> str:
    """
    Get standardized session key using field mapping.
    
    Args:
        symbol: Trading symbol
        date: Session date (YYYY-MM-DD format)
        
    Returns:
        Standardized session key
    """
    if FIELD_MAPPING_AVAILABLE:
        field_manager = get_field_mapping_manager()
        return field_manager.get_session_key_pattern(symbol, date)
    else:
        # Fallback session key
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return f"session:{symbol}:{date}"

def resolve_field_name(field_name: str, field_type: str = 'session') -> str:
    """
    Resolve field name using field mapping.
    
    Args:
        field_name: Field name to resolve
        field_type: Type of field (session, indicator)
        
    Returns:
        Resolved canonical field name
    """
    if FIELD_MAPPING_AVAILABLE:
        field_manager = get_field_mapping_manager()
        if field_type == 'session':
            return field_manager.resolve_redis_session_field(field_name)
        elif field_type == 'indicator':
            return field_manager.resolve_calculated_field(field_name)
    
    return field_name

def get_volume_state_data(symbol: str) -> Dict[str, Any]:
    """
    Get volume state data using volume state manager.
    
    Args:
        symbol: Trading symbol
        
    Returns:
        Volume state data including profile information
    """
    if VOLUME_MANAGER_AVAILABLE:
        try:
            volume_manager = get_volume_manager()
            return volume_manager.get_volume_profile_data(symbol)
        except Exception as e:
            logger.debug(f"Volume state data retrieval failed for {symbol}: {e}")
            return {}
    return {}

def track_straddle_volume(underlying_symbol: str, ce_symbol: str, pe_symbol: str, 
                         ce_volume: int, pe_volume: int, exchange_timestamp: datetime) -> Dict[str, int]:
    """
    Track straddle volume using volume state manager.
    
    Args:
        underlying_symbol: Underlying symbol
        ce_symbol: Call option symbol
        pe_symbol: Put option symbol
        ce_volume: Call option volume
        pe_volume: Put option volume
        exchange_timestamp: Exchange timestamp
        
    Returns:
        Straddle volume metrics
    """
    if VOLUME_MANAGER_AVAILABLE:
        try:
            volume_manager = get_volume_manager()
            return volume_manager.track_straddle_volume(
                underlying_symbol, ce_symbol, pe_symbol, ce_volume, pe_volume, exchange_timestamp
            )
        except Exception as e:
            logger.error(f"Straddle volume tracking failed: {e}")
            return {
                'ce_incremental': 0,
                'pe_incremental': 0,
                'combined_incremental': 0,
                'ce_cumulative': ce_volume,
                'pe_cumulative': pe_volume,
                'combined_cumulative': ce_volume + pe_volume
            }
    else:
        # Fallback calculation
        return {
            'ce_incremental': 0,
            'pe_incremental': 0,
            'combined_incremental': 0,
            'ce_cumulative': ce_volume,
            'pe_cumulative': pe_volume,
            'combined_cumulative': ce_volume + pe_volume
        }

def normalize_zerodha_tick_data(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    ENHANCED VERSION: Normalize Zerodha tick data with field mapping and volume state integration.
    
    Args:
        tick_data: Raw tick data from Zerodha
        
    Returns:
        Normalized tick data with field mapping applied
    """
    try:
        # Start with mapped data
        normalized = map_kite_to_unified(tick_data)

        # Apply field mapping if available
        if FIELD_MAPPING_AVAILABLE:
            field_manager = get_field_mapping_manager()
            normalized = field_manager.apply_field_mapping(normalized, 'equity')
            
            # Normalize session record with field mapping
            normalized = normalize_session_record(normalized, include_aliases=True)

        # Preserve pre-calculated volume ratio if present
        if 'volume_ratio' in tick_data:
            try:
                normalized['volume_ratio'] = float(tick_data.get('volume_ratio'))
            except (TypeError, ValueError):
                normalized['volume_ratio'] = None
        if 'normalized_volume' in tick_data:
            try:
                normalized['normalized_volume'] = float(tick_data.get('normalized_volume'))
            except (TypeError, ValueError):
                normalized['normalized_volume'] = None
        
        # Volume state management integration
        symbol = normalized.get("symbol", "UNKNOWN")
        current_cumulative = normalized.get("zerodha_cumulative_volume", 0)
        
        if current_cumulative > 0 and VOLUME_MANAGER_AVAILABLE:
            try:
                # Get volume manager
                volume_manager = get_volume_manager()
                
                # Calculate incremental volume using volume state manager
                instrument_token = normalized.get("instrument_token")
                exchange_timestamp = datetime.fromisoformat(normalized.get("exchange_timestamp", datetime.now().isoformat()))
                
                if instrument_token:
                    incremental_volume = volume_manager.calculate_incremental(
                        instrument_token, current_cumulative, exchange_timestamp
                    )
                    
                    # Store incremental volume
                    normalized["incremental_volume"] = incremental_volume
                    normalized["bucket_incremental_volume"] = incremental_volume
                    
                    # Get volume profile data if available
                    volume_profile_data = volume_manager.get_volume_profile_data(symbol)
                    if volume_profile_data:
                        normalized.update(volume_profile_data)
                        
            except Exception as e:
                logger.debug(f"Volume state manager integration failed: {e}")
                # Fallback to existing logic
                incremental_volume = tick_data.get("bucket_incremental_volume", 0)
                normalized["incremental_volume"] = incremental_volume
                normalized["bucket_incremental_volume"] = incremental_volume
        else:
            # Fallback when volume manager not available
            incremental_volume = tick_data.get("bucket_incremental_volume", 0)
            normalized["incremental_volume"] = incremental_volume
            normalized["bucket_incremental_volume"] = incremental_volume
        
        # Handle nested structures
        if "ohlc" in tick_data and isinstance(tick_data["ohlc"], dict):
            ohlc = tick_data["ohlc"]
            normalized["open"] = float(ohlc.get("open", 0))
            normalized["high"] = float(ohlc.get("high", 0))
            normalized["low"] = float(ohlc.get("low", 0))
            normalized["close"] = float(ohlc.get("close", 0))
        
        # Handle depth data
        if "depth" in tick_data and isinstance(tick_data["depth"], dict):
            import json
            normalized["depth"] = json.dumps(tick_data["depth"])
            normalized["depth_valid"] = True
        else:
            normalized["depth"] = "{}"
            normalized["depth_valid"] = False
        
        # Calculate derived fields
        derived = calculate_derived_fields(normalized)
        normalized.update(derived)
        
        return normalized
        
    except Exception as e:
        logger.error(f"Error normalizing Zerodha tick data: {e}")
        return tick_data

# ============================================
# INDICATOR SCHEMA (From indicator_schema.py)
# ============================================

# Unified indicator schema for trading system
UNIFIED_INDICATOR_SCHEMA = {
    # Core identifiers
    "symbol": str,
    "timestamp": str,
    "timestamp_ms": float,
    "exchange_timestamp": str,
    "last_trade_time": str,
    "last_price": float,
    "volume": int,
    
    # Technical indicators
    "rsi": float,
    "rsi_period": int,
    "rsi_signal": str,
    "macd": float,
    "macd_signal": float,
    "macd_histogram": float,
    "macd_fast_period": int,
    "macd_slow_period": int,
    "macd_signal_period": int,
    "macd_crossover": str,
    "bb_upper": float,
    "bb_middle": float,
    "bb_lower": float,
    "bb_period": int,
    "bb_std": int,
    "bb_position": str,
}

# Indicator constants
INDICATOR_CONSTANTS = {
    "RSI_PERIOD": 14,
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "BB_PERIOD": 20,
    "BB_STD": 2,
    "SMA_PERIODS": [20, 50, 200],
    "EMA_PERIODS": [12, 26, 50],
    "VOLUME_SMA_PERIOD": 20,
    "ATR_PERIOD": 14,
    "VOLATILITY_PERIOD": 20,
}

# Indicator thresholds
INDICATOR_THRESHOLDS = {
    "RSI_OVERSOLD": 30,
    "RSI_OVERBOUGHT": 70,
    "MACD_CROSSOVER_THRESHOLD": 0.001,
    "BB_POSITION_THRESHOLD": 0.05,
    "VOLUME_RATIO_SIGNIFICANT": 2.0,
    "VOLUME_ACCUMULATION": 1.8,
    "VOLUME_BREAKOUT": 2.0,
    "VOLUME_DUMP": 2.2,
    "VOLATILITY_LOW": 0.1,
    "VOLATILITY_HIGH": 0.3,
    "VOLATILITY_EXTREME": 0.5,
    "CONFIDENCE_HIGH": 0.85,
    "CONFIDENCE_MEDIUM": 0.80,
    "MOVE_MIN": 0.25,
}

def create_indicator_data(
    symbol: str,
    timestamp: str,
    timestamp_ms: float,
    last_price: float,
    volume: int,
    exchange_timestamp: Optional[str] = None,
    last_trade_time: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """
    Create indicator data following unified schema.
    """
    indicator_data = {
        "symbol": symbol,
        "timestamp": timestamp,
        "timestamp_ms": timestamp_ms,
        "last_price": last_price,
        "volume": volume,
        "exchange_timestamp": exchange_timestamp or "",
        "last_trade_time": last_trade_time or "",
    }
    
    # Add any additional indicator values
    indicator_data.update(kwargs)
    
    return indicator_data

def validate_indicator_data(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate indicator data against schema.
    """
    issues = []
    
    # Required fields
    required_fields = ["symbol", "timestamp", "last_price", "volume"]
    for field in required_fields:
        if field not in data:
            issues.append(f"Missing required field: {field}")
    
    # Numeric validation
    numeric_fields = ["last_price", "volume", "timestamp_ms"]
    for field in numeric_fields:
        if field in data:
            try:
                float(data[field])
            except (ValueError, TypeError):
                issues.append(f"Invalid numeric value for {field}: {data[field]}")
    
    return len(issues) == 0, issues

def enrich_indicator_signals(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich indicator data with signal information.
    """
    enriched = data.copy()
    
    # RSI signals
    if "rsi" in data:
        rsi = float(data["rsi"])
        if rsi <= 30:
            enriched["rsi_signal"] = "OVERSOLD"
        elif rsi >= 70:
            enriched["rsi_signal"] = "OVERBOUGHT"
        else:
            enriched["rsi_signal"] = "NEUTRAL"
    
    # MACD signals
    if "macd" in data and "macd_signal" in data:
        macd = float(data["macd"])
        signal = float(data["macd_signal"])
        if macd > signal:
            enriched["macd_crossover"] = "BULLISH"
        elif macd < signal:
            enriched["macd_crossover"] = "BEARISH"
        else:
            enriched["macd_crossover"] = "NONE"
    
    return enriched

# ============================================
# ASSET-SPECIFIC SCHEMA (From asset_specific_schema.py)
# ============================================

# Market phase definitions
MARKET_PHASES = {
    "pre_market": {"start": "08:59", "end": "09:08"},
    "market_hours": {"start": "09:15", "end": "15:30"},
    "post_market": {"start": "15:30", "end": "16:00"},
    "closed": {"start": "16:00", "end": "08:59"},
}

# Asset-specific field availability
ASSET_SPECIFIC_FIELDS = {
    "equity": {
        "always_available": [
            "instrument_token", "tradingsymbol", "last_price", "last_quantity",
            "average_price", "net_change", "volume", "ohlc", "mode", "tradable"
        ],
        "market_hours_only": ["buy_quantity", "sell_quantity", "depth"],
    },
    "futures": {
        "always_available": [
            "instrument_token", "tradingsymbol", "last_price", "last_quantity",
            "average_price", "net_change", "volume", "ohlc", "oi", "oi_day_high", "oi_day_low"
        ],
        "market_hours_only": ["buy_quantity", "sell_quantity", "depth"],
    },
    "options": {
        "always_available": [
            "instrument_token", "tradingsymbol", "last_price", "last_quantity",
            "average_price", "net_change", "volume", "ohlc", "oi", "strike_price", "option_type"
        ],
        "market_hours_only": ["buy_quantity", "sell_quantity", "depth"],
    },
}

def get_asset_class(symbol: str) -> str:
    """Determine asset class from symbol."""
    if "CE" in symbol or "PE" in symbol:
        return "options"
    elif "FUT" in symbol:
        return "futures"
    else:
        return "equity"

def enhance_tick_for_asset_class(tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """Enhance tick data with asset-specific fields."""
    enhanced = tick_data.copy()
    asset_class = get_asset_class(tick_data.get("tradingsymbol", ""))
    enhanced["asset_class"] = asset_class
    return enhanced

# ============================================
# PATTERN SCHEMA (From pattern_schema.py)
# ============================================

PATTERN_SCHEMA = {
    "symbol": str,
    "pattern": str,
    "pattern_type": str,
    "timestamp": float,
    "timestamp_ms": float,
    "signal": str,
    "confidence": float,
    "last_price": float,
    "price_change": float,
    "volume": float,
    "volume_ratio": float,
    "vix_level": float,
    "market_regime": str,
    "pattern_category": str,
    "expected_move": float,
    "target_price": float,
    "stop_loss": float,
    "risk_reward": float,
}

# ============================================
# END-TO-END INTEGRATION FUNCTIONS
# ============================================

def process_tick_data_end_to_end(tick_data: Dict[str, Any], asset_type: str = 'equity') -> Dict[str, Any]:
    """
    Process tick data through the complete end-to-end pipeline with field mapping and volume state integration.
    
    Args:
        tick_data: Raw tick data from WebSocket
        asset_type: Type of asset (equity, futures, options)
        
    Returns:
        Fully processed tick data ready for pattern detection and storage
    """
    try:
        # Step 1: Normalize tick data with field mapping
        normalized = normalize_zerodha_tick_data(tick_data)
        
        # Step 2: Apply field mapping normalization
        if FIELD_MAPPING_AVAILABLE:
            field_manager = get_field_mapping_manager()
            normalized = field_manager.apply_field_mapping(normalized, asset_type)
        
        # Step 3: Validate data
        validation_result = validate_data_with_field_mapping(normalized, asset_type)
        if not validation_result['valid']:
            logger.warning(f"Data validation issues: {validation_result['errors']}")
        
        # Step 4: Enhance with asset-specific fields
        enhanced = enhance_tick_for_asset_class(normalized)
        
        # Step 5: Add volume profile data if available
        symbol = enhanced.get("symbol", "UNKNOWN")
        volume_profile_data = get_volume_state_data(symbol)
        if volume_profile_data:
            enhanced.update(volume_profile_data)
        
        # Step 6: Add metadata
        enhanced["processed_at"] = datetime.now().isoformat()
        enhanced["schema_version"] = "2.0"
        enhanced["integration_status"] = {
            "field_mapping": FIELD_MAPPING_AVAILABLE,
            "volume_manager": VOLUME_MANAGER_AVAILABLE,
            "validation_passed": validation_result['valid']
        }
        
        return enhanced
        
    except Exception as e:
        logger.error(f"End-to-end processing failed: {e}")
        return tick_data

def create_session_record_end_to_end(symbol: str, tick_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a standardized session record using field mapping and volume state integration.
    
    Args:
        symbol: Trading symbol
        tick_data: Processed tick data
        
    Returns:
        Standardized session record
    """
    try:
        # Get standardized session key
        session_key = get_standardized_session_key(symbol)
        
        # Create base session record
        session_record = {
            "symbol": symbol,
            "session_key": session_key,
            "last_price": tick_data.get("last_price", 0.0),
            "zerodha_cumulative_volume": tick_data.get("zerodha_cumulative_volume", 0),
            "bucket_cumulative_volume": tick_data.get("bucket_cumulative_volume", 0),
            "bucket_incremental_volume": tick_data.get("bucket_incremental_volume", 0),
            "session_date": datetime.now().strftime("%Y-%m-%d"),
            "last_update_timestamp": datetime.now().timestamp(),
            "update_count": 1
        }
        
        # Add OHLC data if available
        if "open" in tick_data:
            session_record["open"] = tick_data["open"]
        if "high" in tick_data:
            session_record["high"] = tick_data["high"]
        if "low" in tick_data:
            session_record["low"] = tick_data["low"]
        if "close" in tick_data:
            session_record["close"] = tick_data["close"]
        
        # Normalize using field mapping
        normalized_session = normalize_session_data(session_record, include_aliases=True)
        
        return normalized_session
        
    except Exception as e:
        logger.error(f"Session record creation failed: {e}")
        return {}

def create_indicator_record_end_to_end(symbol: str, tick_data: Dict[str, Any], 
                                     indicator_values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a standardized indicator record using field mapping integration.
    
    Args:
        symbol: Trading symbol
        tick_data: Processed tick data
        indicator_values: Calculated indicator values
        
    Returns:
        Standardized indicator record
    """
    try:
        # Create base indicator record
        indicator_record = {
            "symbol": symbol,
            "timestamp": tick_data.get("timestamp", datetime.now().isoformat()),
            "timestamp_ms": tick_data.get("timestamp_ms", datetime.now().timestamp() * 1000),
            "last_price": tick_data.get("last_price", 0.0),
            "volume": tick_data.get("bucket_incremental_volume", 0),
            "exchange_timestamp": tick_data.get("exchange_timestamp", ""),
            "last_trade_time": tick_data.get("last_trade_time", "")
        }
        
        # Add indicator values
        indicator_record.update(indicator_values)
        
        # Normalize using field mapping
        normalized_indicator = normalize_indicator_data(indicator_record, include_aliases=True)
        
        # Enrich with signals
        enriched_indicator = enrich_indicator_signals(normalized_indicator)
        
        return enriched_indicator
        
    except Exception as e:
        logger.error(f"Indicator record creation failed: {e}")
        return {}

def get_integration_status() -> Dict[str, Any]:
    """
    Get the current integration status of field mapping and volume state manager.
    
    Returns:
        Integration status information
    """
    status = {
        "field_mapping_available": FIELD_MAPPING_AVAILABLE,
        "volume_manager_available": VOLUME_MANAGER_AVAILABLE,
        "pyarrow_available": PYARROW_AVAILABLE,
        "integration_version": "2.0",
        "last_updated": datetime.now().isoformat()
    }
    
    if FIELD_MAPPING_AVAILABLE:
        try:
            field_manager = get_field_mapping_manager()
            status["field_mapping_config"] = {
                "config_loaded": field_manager.mapping_config is not None,
                "session_fields": len(field_manager.get_redis_session_fields()),
                "indicator_fields": len(field_manager.get_calculated_indicator_fields())
            }
        except Exception as e:
            status["field_mapping_error"] = str(e)
    
    if VOLUME_MANAGER_AVAILABLE:
        try:
            volume_manager = get_volume_manager()
            status["volume_manager"] = {
                "available": True,
                "volume_profile_integrated": volume_manager.volume_profile_manager is not None
            }
        except Exception as e:
            status["volume_manager_error"] = str(e)
    
    return status
