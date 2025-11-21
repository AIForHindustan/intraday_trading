"""
Redis Key Lookup Standards - ENFORCED THROUGHOUT CODEBASE

CRITICAL RULE: ALL Redis operations MUST use direct key lookups.
Pattern matching (KEYS, SCAN) is FORBIDDEN except in admin/debug scripts.

DATABASE ASSIGNMENT:
- DB1: Live ticks, OHLC data, volume data, indicators, session data
- DB2: Analytics, alert validations, performance metrics, pattern history
- Pub/Sub: Alert publishing (channel-based, no keys)

This ensures:
- O(1) performance instead of O(N) blocking scans
- Non-blocking Redis operations  
- Predictable performance characteristics
- No connection pool exhaustion from slow scans
- Clear separation of concerns between databases

✅ UPDATED: All keys now use unified structure with clear database assignment
✅ CONSOLIDATED: SINGLE source of truth for ALL Redis keys
"""

from __future__ import annotations

import logging
from typing import Optional, List, Dict, Any, Union, Final, Tuple
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)

class RedisDatabase(Enum):
    """Redis database assignments for clear separation"""
    DB1_LIVE_DATA = 1      # Live ticks, OHLC, volume, indicators, session data
    DB2_ANALYTICS = 2      # Analytics, alert validations, performance metrics
    PUBSUB = None          # Alert publishing (channels, not keys)

# ============================================================================
# Database-Aware Key Builder
# ============================================================================

class DatabaseAwareKeyBuilder:
    """
    Builds Redis keys with explicit database assignment and validation.
    Ensures consistent key structure across all databases.
    """
    
    @staticmethod
    def get_database(key: str) -> RedisDatabase:
        """
        Determine which database a key belongs to based on its prefix.
        This ensures operations use the correct Redis connection.
        """
        if not key:
            return RedisDatabase.DB1_LIVE_DATA  # Default to live data
        
        # DB1: Live data prefixes
        live_data_prefixes = {
            'ohlc:', 'session:', 'vol:', 'ind:', 'ticks:', 
            'underlying_price:', 'options:', 'bucket_incremental_volume:'
        }
        
        # DB2: Analytics prefixes  
        analytics_prefixes = {
            'pattern_history:', 'scanner:', 'alert_performance:', 
            'signal_quality:', 'pattern_performance:', 'pattern_metrics:',
            'forward_validation:', 'analysis_cache:',
            'time_window_performance:', 'pattern_window_aggregate:',
            'sharpe_inputs:', 'alert_timeline:', 'final_validation:'
        }
        
        for prefix in live_data_prefixes:
            if key.startswith(prefix):
                return RedisDatabase.DB1_LIVE_DATA
                
        for prefix in analytics_prefixes:
            if key.startswith(prefix):
                return RedisDatabase.DB2_ANALYTICS
                
        # Default to live data for unknown keys
        return RedisDatabase.DB1_LIVE_DATA

    # ============================================================================
    # DB1: LIVE DATA KEYS (Ticks, OHLC, Volume, Indicators)
    # ============================================================================
    
    @staticmethod
    def live_ohlc_latest(symbol: str) -> str:
        """Latest OHLC data - DB1"""
        normalized = normalize_symbol(symbol)
        return f"ohlc_latest:{normalized}"
    
    @staticmethod
    def live_ohlc_timeseries(symbol: str, interval: str = "1d") -> str:
        """OHLC time series - DB1"""
        normalized = normalize_symbol(symbol)
        return f"ohlc:{normalized}:{interval}"
    
    @staticmethod
    def live_ohlc_daily(symbol: str) -> str:
        """OHLC daily data - DB1"""
        normalized = normalize_symbol(symbol)
        return f"ohlc_daily:{normalized}"
    
    @staticmethod
    def live_session(symbol: str, date: str) -> str:
        """Trading session data - DB1"""
        return f"session:{symbol}:{date}"
    
    @staticmethod
    def live_underlying_price(symbol: str) -> str:
        """Underlying price data - DB1"""
        return f"underlying_price:{symbol}"
    
    @staticmethod
    def live_ticks_hash(symbol: str) -> str:
        """Ticks hash storage - DB1 (ticks:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:{canonical_sym}"
    
    @staticmethod
    def live_ticks_stream(symbol: str) -> str:
        """Ticks stream - DB1 (ticks:stream:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:stream:{canonical_sym}"
    
    @staticmethod
    def live_processed_stream() -> str:
        """Global processed tick stream - DB1 (ticks:intraday:processed)"""
        return "ticks:intraday:processed"
    
    @staticmethod
    def live_raw_binary_stream() -> str:
        """Raw binary tick stream - DB1 (ticks:raw:binary)"""
        return "ticks:raw:binary"
    
    @staticmethod
    def live_historical_ticks_raw(symbol: str) -> str:
        """Historical raw ticks sorted set - DB1 (ticks:historical:raw:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ticks:historical:raw:{canonical_sym}"
    
    @staticmethod 
    def live_volume_state(instrument_token: str) -> str:
        """Volume state - DB1"""
        return f"vol:state:{instrument_token}"
    
    @staticmethod
    def live_volume_baseline(symbol: str) -> str:
        """Volume baseline - DB1"""
        return f"vol:baseline:{symbol}"
    
    @staticmethod
    def live_volume_profile(symbol: str, profile_type: str, date: Optional[str] = None) -> str:
        """Volume profile data - DB1"""
        if date:
            return f"vol:profile:{symbol}:{profile_type}:{date}"
        return f"vol:profile:{symbol}:{profile_type}"
    
    @staticmethod
    def live_volume_profile_realtime(symbol: str) -> str:
        """Real-time volume profile - DB1 (volume_profile:realtime:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"volume_profile:realtime:{canonical_sym}"
    
    @staticmethod
    def live_indicator(symbol: str, indicator_name: str, category: Optional[str] = None) -> str:
        """Indicator data - DB1"""
        if not category:
            category = RedisKeyStandards._categorize_indicator(indicator_name)
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"ind:{category}:{canonical_sym}:{indicator_name}"
    
    @staticmethod
    def live_greeks(symbol: str, greek_name: Optional[str] = None) -> str:
        """Greeks data - DB1"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        if greek_name:
            return f"ind:greeks:{canonical_sym}:{greek_name}"
        return f"ind:greeks:{canonical_sym}:greeks"
    
    @staticmethod
    def live_bucket_history(symbol: str, resolution: str = "5min") -> str:
        """Bucket history - DB1"""
        return f"bucket_incremental_volume:history:{resolution}:{symbol}"
    
    @staticmethod
    def live_option_chain(underlying: str, strike: int, option_type: str, field: str) -> str:
        """Option chain data - DB1"""
        underlying_lower = underlying.lower()
        return f"options:{underlying_lower}:{strike}{option_type}:{field}"

    # ============================================================================
    # DB2: ANALYTICS & VALIDATION KEYS (Patterns, Performance, Alerts)
    # ============================================================================
    
    @staticmethod
    def analytics_pattern_history(pattern_type: str, symbol: str, field: str) -> str:
        """Pattern history data - DB2"""
        return f"pattern_history:{pattern_type}:{symbol}:{field}"
    
    @staticmethod
    def analytics_scanner_performance(metric: str, timeframe: str) -> str:
        """Scanner performance - DB2"""
        return f"scanner:performance:{metric}:{timeframe}"
    
    @staticmethod
    def analytics_alert_performance_stats() -> str:
        """Alert performance stats - DB2"""
        return "alert_performance:stats"
    
    @staticmethod
    def analytics_alert_performance_pattern(pattern: str) -> str:
        """Pattern-specific alert performance - DB2"""
        return f"alert_performance:stats:{pattern}"
    
    @staticmethod
    def analytics_signal_quality(symbol: str, pattern_type: str) -> str:
        """Signal quality metrics - DB2"""
        return f"signal_quality:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_pattern_performance(symbol: str, pattern_type: str) -> str:
        """Pattern performance - DB2"""
        return f"pattern_performance:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_pattern_metrics(symbol: str, pattern_type: str) -> str:
        """Pattern metrics - DB2"""
        return f"pattern_metrics:{symbol}:{pattern_type}"
    
    @staticmethod
    def analytics_validation(alert_id: str) -> str:
        """Alert validation data - DB2"""
        return f"forward_validation:alert:{alert_id}"
    
    @staticmethod
    def analytics_legacy_cache(symbol: str, indicator_name: str) -> str:
        """Legacy analysis cache - DB2 (for migration)"""
        return f"analysis_cache:indicators:{symbol}:{indicator_name}"
    
    @staticmethod
    def analytics_final_validation(pattern_type: str, symbol: str) -> str:
        """Final validation results with time-window data - DB2"""
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        return f"final_validation:{pattern_type}:{canonical_symbol}"
    
    @staticmethod
    def analytics_pattern_window_aggregate(pattern_type: str, window_key: str) -> str:
        """Pattern-window aggregate statistics - DB2"""
        return f"pattern_window_aggregate:{pattern_type}:{window_key}"
    
    @staticmethod
    def analytics_time_window_performance(pattern_type: str, symbol: str, window_key: str) -> str:
        """Time-window performance data - DB2"""
        canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
        return f"time_window_performance:{pattern_type}:{canonical_symbol}:{window_key}"
    
    @staticmethod
    def analytics_alert_timeline(alert_id: str) -> str:
        """Alert validation timeline - DB2"""
        return f"alert_timeline:{alert_id}"
    
    @staticmethod
    def analytics_sharpe_inputs(pattern_type: str, window_key: str) -> str:
        """Sharpe ratio calculation inputs - DB2"""
        return f"sharpe_inputs:{pattern_type}:{window_key}"

    # ============================================================================
    # DB1: Alert Streams (Live Data)
    # ============================================================================
    
    @staticmethod
    def live_alerts_stream() -> str:
        """Main alert stream - DB1 (alerts:stream)"""
        return "alerts:stream"
    
    @staticmethod
    def live_alerts_telegram() -> str:
        """Telegram alert stream - DB1 (alerts:telegram)"""
        return "alerts:telegram"
    
    @staticmethod
    def live_pattern_stream(pattern_type: str, symbol: str) -> str:
        """Pattern-specific stream - DB1 (patterns:{pattern_type}:{symbol})"""
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        return f"patterns:{pattern_type}:{canonical_sym}"
    
    # ============================================================================
    # DB2: Alert Validation Storage (Analytics)
    # ============================================================================
    
    @staticmethod
    def analytics_alert_storage(alert_id: str) -> str:
        """Alert storage key - DB2 (alert:{alert_id})"""
        return f"alert:{alert_id}"
    
    @staticmethod
    def analytics_validation_storage(alert_id: str) -> str:
        """Validation storage key - DB2 (validation:{alert_id})"""
        return f"validation:{alert_id}"
    
    @staticmethod
    def analytics_validation_stream() -> str:
        """Validation results stream - DB2 (alerts:validation:results)"""
        return "alerts:validation:results"
    
    @staticmethod
    def analytics_validation_recent() -> str:
        """Recent validation results list - DB2 (validation_results:recent)"""
        return "validation_results:recent"
    
    # ============================================================================
    # Pub/Sub Channels (No database - channel-based)
    # ============================================================================
    
    @staticmethod
    def pubsub_alerts_system() -> str:
        """System alerts channel - Pub/Sub"""
        return "alerts:system"
    
    @staticmethod
    def pubsub_validation_results() -> str:
        """Validation results channel - Pub/Sub"""
        return "alerts:validation:results"
    
    @staticmethod
    def pubsub_ohlc_updates(symbol: str) -> str:
        """OHLC updates channel - Pub/Sub"""
        return f"ohlc_updates:{symbol}"

# ============================================================================
# Symbol Normalization & OHLC Key Utilities 
# ============================================================================

@dataclass
class ParsedSymbol:
    raw_symbol: str
    normalized: str
    asset_class: str  # equity, futures, options, currency, commodity
    instrument_type: str  # EQ, FUT, OPT, CUR, COM
    base_symbol: str  # Underlying symbol
    expiry: Optional[datetime] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None  # CE, PE
    lot_size: int = 1

class UniversalSymbolParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Comprehensive regex patterns
        self.patterns = {
            'equity': [
                re.compile(r'^[A-Z]{1,20}$'),  # RELIANCE, TCS
                re.compile(r'^NSE:([A-Z0-9_-]+)$'),  # NSE:RELIANCE
            ],
            'futures': [
                # Stock futures: RELIANCE25NOVFUT, BANKNIFTY25NOVFUT
                re.compile(r'^([A-Z]+)(\d{2})([A-Z]{3})FUT$'),
                # Index futures: NIFTY25NOVFUT
                re.compile(r'^([A-Z]{5,10})(\d{2})([A-Z]{3})FUT$'),
            ],
            'options': [
                # Stock options: RELIANCE25NOV1540CE, BANKNIFTY25NOV15400CE
                re.compile(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d+)([CP]E)$'),
                # Index options: NIFTY25NOV15400CE
                re.compile(r'^([A-Z]{5,10})(\d{2})([A-Z]{3})(\d+)([CP]E)$'),
            ],
            'currency': [
                # USDINR25NOVFUT, EURINR25NOVFUT
                re.compile(r'^([A-Z]{6})(\d{2})([A-Z]{3})FUT$'),
                # USDINR25NOV76.50CE, EURINR25NOV88.00PE
                re.compile(r'^([A-Z]{6})(\d{2})([A-Z]{3})(\d+\.?\d*)([CP]E)$'),
            ],
            'commodity': [
                # GOLD25NOVFUT, SILVER25NOVFUT
                re.compile(r'^([A-Z]+)(\d{2})([A-Z]{3})FUT$'),
                # GOLD25NOV52000CE, SILVER25NOV72000PE
                re.compile(r'^([A-Z]+)(\d{2})([A-Z]{3})(\d+)([CP]E)$'),
            ]
        }

    def parse_symbol(self, symbol: str, segment: str = "") -> ParsedSymbol:
        """Universal symbol parser for all asset classes"""
        if not symbol:
            raise ValueError("Symbol cannot be empty")
        
        clean_symbol = self._clean_symbol(symbol)
        
        # Try to detect asset class automatically
        asset_class = self._detect_asset_class(clean_symbol, segment)
        
        if asset_class == 'equity':
            return self._parse_equity(clean_symbol)
        elif asset_class == 'futures':
            return self._parse_futures(clean_symbol)
        elif asset_class == 'options':
            return self._parse_options(clean_symbol)
        elif asset_class == 'currency':
            return self._parse_currency(clean_symbol)
        elif asset_class == 'commodity':
            return self._parse_commodity(clean_symbol)
        else:
            # Fallback to equity
            return ParsedSymbol(
                raw_symbol=symbol,
                normalized=clean_symbol,
                asset_class='equity',
                instrument_type='EQ',
                base_symbol=clean_symbol
            )

    def _detect_asset_class(self, symbol: str, segment: str) -> str:
        """Auto-detect asset class from symbol pattern and segment"""
        segment = segment.lower() if segment else ""
        
        # Check segment hints first
        segment_map = {
            'equity': 'equity',
            'futures': 'futures', 
            'options': 'options',
            'currency': 'currency',
            'commodity': 'commodity'
        }
        
        if segment in segment_map:
            return segment_map[segment]
        
        # ✅ FIXED: Check currency FIRST (most specific - 6 char pairs like USDINR, EURINR)
        if any(pattern.match(symbol) for pattern in self.patterns['currency']):
            return 'currency'
        
        # ✅ FIXED: Check options/futures BEFORE commodity
        known_commodities = {'GOLD', 'SILVER', 'CRUDE', 'NATURALGAS', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'CRUDEOIL', 'NATURALGAS', 'GOLDM', 'SILVERM'}
        
        # Check options patterns
        for pattern in self.patterns['options']:
            match = pattern.match(symbol)
            if match:
                base_symbol = match.group(1)
                if base_symbol in known_commodities:
                    return 'commodity'
                return 'options'
        
        # Check futures patterns
        for pattern in self.patterns['futures']:
            match = pattern.match(symbol)
            if match:
                base_symbol = match.group(1)
                if base_symbol in known_commodities:
                    return 'commodity'
                return 'futures'
        
        # Check commodity patterns last
        if any(pattern.match(symbol) for pattern in self.patterns['commodity']):
            return 'commodity'
        
        # Check equity last (broadest pattern)
        if any(pattern.match(symbol) for pattern in self.patterns['equity']):
            return 'equity'
        
        return 'equity'  # Default fallback

    def _parse_equity(self, symbol: str) -> ParsedSymbol:
        """Parse equity symbols"""
        return ParsedSymbol(
            raw_symbol=symbol,
            normalized=symbol,
            asset_class='equity',
            instrument_type='EQ',
            base_symbol=symbol
        )

    def _parse_futures(self, symbol: str) -> ParsedSymbol:
        """Parse futures symbols"""
        match = self.patterns['futures'][0].match(symbol) or self.patterns['futures'][1].match(symbol)
        if match:
            base_symbol, day, month = match.groups()
            expiry = self._parse_expiry(day, month)
            return ParsedSymbol(
                raw_symbol=symbol,
                normalized=f"{base_symbol}{day}{month}FUT",
                asset_class='futures',
                instrument_type='FUT',
                base_symbol=base_symbol,
                expiry=expiry
            )
        raise ValueError(f"Invalid futures symbol: {symbol}")

    def _parse_options(self, symbol: str) -> ParsedSymbol:
        """Parse options symbols"""
        match = self.patterns['options'][0].match(symbol) or self.patterns['options'][1].match(symbol)
        if match:
            base_symbol, day, month, strike, option_type = match.groups()
            expiry = self._parse_expiry(day, month)
            return ParsedSymbol(
                raw_symbol=symbol,
                normalized=f"{base_symbol}{day}{month}{strike}{option_type}",
                asset_class='options',
                instrument_type='OPT',
                base_symbol=base_symbol,
                expiry=expiry,
                strike=float(strike),
                option_type=option_type
            )
        raise ValueError(f"Invalid options symbol: {symbol}")

    def _parse_currency(self, symbol: str) -> ParsedSymbol:
        """Parse currency derivatives"""
        match = self.patterns['currency'][0].match(symbol) or self.patterns['currency'][1].match(symbol)
        if match:
            if len(match.groups()) == 3:  # Futures
                base_symbol, day, month = match.groups()
                expiry = self._parse_expiry(day, month)
                return ParsedSymbol(
                    raw_symbol=symbol,
                    normalized=f"{base_symbol}{day}{month}FUT",
                    asset_class='currency',
                    instrument_type='FUT',
                    base_symbol=base_symbol,
                    expiry=expiry
                )
            else:  # Options
                base_symbol, day, month, strike, option_type = match.groups()
                expiry = self._parse_expiry(day, month)
                return ParsedSymbol(
                    raw_symbol=symbol,
                    normalized=f"{base_symbol}{day}{month}{strike}{option_type}",
                    asset_class='currency',
                    instrument_type='OPT',
                    base_symbol=base_symbol,
                    expiry=expiry,
                    strike=float(strike),
                    option_type=option_type
                )
        raise ValueError(f"Invalid currency symbol: {symbol}")

    def _parse_commodity(self, symbol: str) -> ParsedSymbol:
        """Parse commodity derivatives"""
        match = self.patterns['commodity'][0].match(symbol) or self.patterns['commodity'][1].match(symbol)
        if match:
            if len(match.groups()) == 3:  # Futures
                base_symbol, day, month = match.groups()
                expiry = self._parse_expiry(day, month)
                return ParsedSymbol(
                    raw_symbol=symbol,
                    normalized=f"{base_symbol}{day}{month}FUT",
                    asset_class='commodity',
                    instrument_type='FUT',
                    base_symbol=base_symbol,
                    expiry=expiry
                )
            else:  # Options
                base_symbol, day, month, strike, option_type = match.groups()
                expiry = self._parse_expiry(day, month)
                return ParsedSymbol(
                    raw_symbol=symbol,
                    normalized=f"{base_symbol}{day}{month}{strike}{option_type}",
                    asset_class='commodity',
                    instrument_type='OPT',
                    base_symbol=base_symbol,
                    expiry=expiry,
                    strike=float(strike),
                    option_type=option_type
                )
        raise ValueError(f"Invalid commodity symbol: {symbol}")

    def _parse_expiry(self, day: str, month: str) -> datetime:
        """Parse expiry date from day and month codes"""
        year = datetime.now().year
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        
        if month not in month_map:
            raise ValueError(f"Invalid month code: {month}")
        
        return datetime(year, month_map[month], int(day))

    def _clean_symbol(self, symbol: str) -> str:
        """Clean symbol string"""
        return symbol.strip().upper().replace(' ', '')

# Global instances
_parser_instance: Optional[UniversalSymbolParser] = None
_key_builder_instance: Optional[DatabaseAwareKeyBuilder] = None

def get_symbol_parser() -> UniversalSymbolParser:
    """Get singleton instance of UniversalSymbolParser."""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = UniversalSymbolParser()
    return _parser_instance

def get_key_builder() -> DatabaseAwareKeyBuilder:
    """Get singleton instance of DatabaseAwareKeyBuilder."""
    global _key_builder_instance
    if _key_builder_instance is None:
        _key_builder_instance = DatabaseAwareKeyBuilder()
    return _key_builder_instance

def normalize_symbol(symbol: str | None) -> str:
    """
    Normalize symbols used for Redis OHLC keys.
    Returns format without colon (e.g., "NSEWIPRO" not "NSE:WIPRO").
    """
    if not symbol:
        return ""
    
    symbol = symbol.strip()
    if not symbol:
        return ""
    
    # Remove colon if present
    upper_symbol = symbol.upper()
    if ':' in upper_symbol:
        parts = upper_symbol.split(':', 1)
        if len(parts) == 2:
            return f"{parts[0]}{parts[1]}"
        return upper_symbol.replace(':', '')
    
    # Use parser to determine correct exchange
    try:
        parser = get_symbol_parser()
        symbol_without_prefix = upper_symbol
        known_prefixes = ('NSE', 'BSE', 'NFO', 'BFO', 'MCX', 'CDS')
        for pref in known_prefixes:
            if upper_symbol.startswith(pref):
                symbol_without_prefix = upper_symbol[len(pref):]
                break
        
        parsed = parser.parse_symbol(symbol_without_prefix)
        correct_exchange = _infer_exchange_prefix(symbol_without_prefix)
        return f"{correct_exchange}{parsed.normalized}"
    except (ValueError, Exception):
        # Fallback to basic normalization
        _ALLOWED_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
        normalized = (symbol.upper().replace(" ", "_")).replace("-", "_")
        normalized = "".join(ch for ch in normalized if ch in _ALLOWED_CHARS)
        if not any(normalized.startswith(pref) for pref in ('NSE', 'BSE', 'NFO', 'BFO', 'MCX', 'CDS')):
            exchange = _infer_exchange_prefix(normalized)
            return f"{exchange}{normalized}"
        return normalized

def is_fno_instrument(symbol: str) -> bool:
    """Check if symbol is a Futures/Options instrument."""
    if not symbol:
        return False
    
    try:
        parser = get_symbol_parser()
        parsed = parser.parse_symbol(symbol)
        return parsed.instrument_type in ('FUT', 'OPT')
    except (ValueError, Exception):
        sym_upper = symbol.upper()
        if sym_upper.endswith('FUT'):
            return True
        if sym_upper.endswith(('CE', 'PE')):
            if sym_upper.startswith('NFO'):
                return True
            if re.search(r'\d+(NOV|DEC|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT)(CE|PE)$', sym_upper):
                return True
            if re.search(r'\d{4,}(CE|PE)$', sym_upper):
                return True
            return False
        if re.search(r'\d{2}(NOV|DEC|JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT)', sym_upper):
            return True
        return False

def _infer_exchange_prefix(symbol: str) -> str:
    """Infer exchange prefix for symbols without explicit exchange metadata."""
    if not symbol:
        return ""
    
    sym_upper = symbol.upper()
    if ':' in sym_upper:
        return sym_upper.split(':', 1)[0]
    
    try:
        parser = get_symbol_parser()
        parsed = parser.parse_symbol(symbol)
        # ✅ FIXED: Prioritize instrument type (FUT/OPT) over asset_class
        # If it's a future or option, it's NFO regardless of asset_class
        if parsed.instrument_type in ('FUT', 'OPT'):
            return 'NFO'
        elif parsed.asset_class == 'currency':
            return 'CDS'
        elif parsed.asset_class == 'commodity':
            return 'MCX'
        elif parsed.asset_class == 'equity':
            if sym_upper.startswith('BSE'):
                return 'BSE'
            return 'NSE'
        else:
            return 'NSE'
    except (ValueError, Exception):
        if is_fno_instrument(symbol):
            return 'NFO'
        if sym_upper.startswith('BSE'):
            return 'BSE'
        return 'NSE'

class RedisKeyStandards:
    """
    Standardized Redis key lookup utilities with database awareness.
    
    ✅ UPDATED: All keys now explicitly assigned to DB1 or DB2
    ✅ ENFORCED: Clear separation between live data and analytics
    """
    
    @staticmethod
    def get_redis_client(key: str, redis_clients: Dict[RedisDatabase, Any]) -> Any:
        """
        Get the correct Redis client based on key prefix.
        
        Args:
            key: Redis key to determine database for
            redis_clients: Dict mapping RedisDatabase to client instances
            
        Returns:
            Appropriate Redis client for the key's database
        """
        builder = get_key_builder()
        db = builder.get_database(key)
        return redis_clients.get(db)
    
    @staticmethod
    def canonical_symbol(symbol: str) -> str:
        """Canonical Redis symbol format (matches OHLC/indicator storage)."""
        if not symbol:
            return ""
        symbol = symbol.strip()
        if not symbol:
            return ""
        
        upper_symbol = symbol.upper()
        base_symbol = upper_symbol
        
        # Strip existing exchange prefix
        if ':' in base_symbol:
            parts = base_symbol.split(':', 1)
            if len(parts) == 2:
                base_symbol = parts[1]
        
        known_prefixes = ('NSE', 'BSE', 'NFO', 'BFO', 'MCX', 'CDS')
        for prefix in known_prefixes:
            if base_symbol.startswith(prefix):
                remaining = base_symbol[len(prefix):]
                if len(remaining) >= 6 and remaining[:6] in ['USDINR', 'EURINR', 'GBPINR', 'JPYINR']:
                    base_symbol = remaining
                    break
                elif len(remaining) > 0 and remaining[0].isalpha():
                    base_symbol = remaining
                    break
        
        # Parse and add correct prefix
        try:
            parser = get_symbol_parser()
            parsed = parser.parse_symbol(base_symbol)
            normalized = parsed.normalized
            if any(normalized.startswith(pref) for pref in known_prefixes):
                return normalized
            exchange = _infer_exchange_prefix(base_symbol)
            return f"{exchange}{normalized}"
        except (ValueError, Exception):
            for pref in known_prefixes:
                if base_symbol.startswith(pref):
                    return base_symbol
            prefix = _infer_exchange_prefix(base_symbol)
            if prefix:
                return f"{prefix}{base_symbol}"
            return base_symbol

    @staticmethod
    def get_indicator_symbol_variants(symbol: str) -> List[str]:
        """Return prioritized symbol variants for indicator storage/retrieval."""
        if not symbol:
            return []
        
        variants: List[str] = []
        canonical_sym = RedisKeyStandards.canonical_symbol(symbol)
        variants.append(canonical_sym)
        
        try:
            parser = get_symbol_parser()
            parsed = parser.parse_symbol(symbol)
            exchange = _infer_exchange_prefix(symbol)
            
            candidates = [
                parsed.normalized,
                parsed.base_symbol,
            ]
            
            if not parsed.normalized.startswith(exchange):
                candidates.append(f"{exchange}{parsed.normalized}")
            if not parsed.base_symbol.startswith(exchange):
                candidates.append(f"{exchange}{parsed.base_symbol}")
            
            if '-' in parsed.base_symbol:
                candidates.append(parsed.base_symbol.replace('-', '_'))
            if '_' in parsed.base_symbol:
                candidates.append(parsed.base_symbol.replace('_', '-'))
            
            for candidate in candidates:
                if candidate and candidate not in variants:
                    variants.append(candidate)
            
        except (ValueError, Exception):
            base_symbol = (symbol or "").split(':', 1)[-1].upper()
            original_base = base_symbol
            for pref in ('NSE', 'BSE', 'NFO', 'BFO'):
                if base_symbol.startswith(pref):
                    stripped = base_symbol[len(pref):]
                    base_symbol = stripped.lstrip(':')
                    break
            if not base_symbol:
                base_symbol = original_base
            
            inferred_prefix = _infer_exchange_prefix(base_symbol)
            candidates = [
                RedisKeyStandards.canonical_symbol(symbol),
                normalize_symbol(base_symbol),
                base_symbol,
            ]
            if '-' in base_symbol:
                candidates.append(base_symbol.replace('-', '_'))
            if '_' in base_symbol:
                candidates.append(base_symbol.replace('_', '-'))
            if inferred_prefix:
                candidates.extend([
                    f"{inferred_prefix}:{base_symbol}",
                    f"{inferred_prefix}:{normalize_symbol(base_symbol)}",
                    f"{inferred_prefix}{base_symbol}",
                    f"{inferred_prefix}{normalize_symbol(base_symbol)}",
                ])
            for candidate in candidates:
                if candidate and candidate not in variants:
                    variants.append(candidate)
        
        return variants

    @staticmethod
    def _categorize_indicator(indicator: str) -> str:
        """Categorize indicator for unified key structure."""
        indicator_lower = indicator.lower()
        if indicator_lower in ['rsi', 'macd', 'bollinger', 'ema', 'sma', 'vwap', 'atr'] or \
           indicator_lower.startswith('ema_') or indicator_lower.startswith('sma_') or \
           indicator_lower.startswith('bollinger_'):
            return 'ta'
        elif indicator_lower in ['delta', 'gamma', 'theta', 'vega', 'rho', 
                                  'dte_years', 'trading_dte', 'expiry_series', 'option_price',
                                  'iv', 'implied_volatility']:
            # ✅ FIX: All Greek-related fields (including DTE and IV) should be categorized as 'greeks'
            return 'greeks'
        elif 'volume' in indicator_lower:
            return 'volume'
        else:
            return 'custom'
    
    @staticmethod
    def get_indicator_key(symbol: str, indicator: str, category: Optional[str] = None, use_cache: bool = True) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        This method is maintained for backward compatibility with existing code.
        **New code should NOT use this method.**
        
        **For new code, use:**
        ```python
        from redis_files.redis_key_standards import get_key_builder
        builder = get_key_builder()
        category = RedisKeyStandards._categorize_indicator(indicator)
        key = builder.live_indicator(symbol, indicator, category)
        ```
        
        Args:
            symbol: Trading symbol
            indicator: Indicator name (e.g., 'rsi', 'atr', 'gamma')
            category: Optional category override (auto-detected if not provided)
            use_cache: Ignored (kept for compatibility)
            
        Returns:
            Canonical Redis key for indicator (DB1)
        """
        builder = get_key_builder()
        if not category:
            category = RedisKeyStandards._categorize_indicator(indicator)
        return builder.live_indicator(symbol, indicator, category)
    
    @staticmethod
    def get_session_key(symbol: str, date: Optional[str] = None) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.live_session(symbol, date)
        """
        builder = get_key_builder()
        if date:
            return builder.live_session(symbol, date)
        from datetime import datetime
        date = datetime.now().strftime('%Y-%m-%d')
        return builder.live_session(symbol, date)
    
    @staticmethod
    def get_ohlc_key(symbol: str, interval: str = "latest") -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use:
        - builder.live_ohlc_latest(symbol) for latest OHLC
        - builder.live_ohlc_daily(symbol) for daily OHLC
        - builder.live_ohlc_timeseries(symbol, interval) for timeseries
        """
        builder = get_key_builder()
        if interval == "latest":
            return builder.live_ohlc_latest(symbol)
        elif interval == "daily":
            return builder.live_ohlc_daily(symbol)
        else:
            return builder.live_ohlc_timeseries(symbol, interval)
    
    @staticmethod
    def get_alert_performance_stats_key() -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_alert_performance_stats()
        """
        builder = get_key_builder()
        return builder.analytics_alert_performance_stats()
    
    @staticmethod
    def get_alert_performance_pattern_key(pattern: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_alert_performance_pattern(pattern)
        """
        builder = get_key_builder()
        return builder.analytics_alert_performance_pattern(pattern)
    
    @staticmethod
    def get_alert_performance_patterns_set_key() -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        Returns the key for the set of all patterns with performance data.
        For new code, construct directly: "alert_performance:patterns"
        """
        return "alert_performance:patterns"
    
    @staticmethod
    def get_signal_quality_key(symbol: str, pattern_type: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_signal_quality(symbol, pattern_type)
        """
        builder = get_key_builder()
        return builder.analytics_signal_quality(symbol, pattern_type)
    
    @staticmethod
    def get_pattern_performance_key(symbol: str, pattern_type: str) -> str:
        """
        ⚠️ DEPRECATED: Compatibility method for backward compatibility only.
        
        For new code, use: builder.analytics_pattern_performance(symbol, pattern_type)
        """
        builder = get_key_builder()
        return builder.analytics_pattern_performance(symbol, pattern_type)

# ============================================================================
# Debug Utilities
# ============================================================================

def debug_symbol_parsing(symbol: str):
    """Debug function to see what's happening with symbol parsing"""
    print(f"🔍 [SYMBOL_DEBUG] Parsing: {symbol}")
    
    parser = get_symbol_parser()
    try:
        parsed = parser.parse_symbol(symbol)
        print(f"   Parsed: {parsed}")
        print(f"   Normalized: {parsed.normalized}")
        print(f"   Base: {parsed.base_symbol}")
        print(f"   Type: {parsed.instrument_type}")
    except Exception as e:
        print(f"   Parse error: {e}")
    
    # Test canonical symbol
    canonical = RedisKeyStandards.canonical_symbol(symbol)
    print(f"   Canonical: {canonical}")
    
    # Test variants
    variants = RedisKeyStandards.get_indicator_symbol_variants(symbol)
    print(f"   Variants: {variants}")

# ============================================================================
# Usage Examples with Database Awareness
# ============================================================================

EXAMPLES = {
    "BAD - Pattern Matching": """
    # ❌ FORBIDDEN - Blocking operations
    alert_keys = redis_client.keys("alert:*")
    for key in alert_keys:
        data = redis_client.get(key)
    """,
    
    "GOOD - Direct Lookup with Database Awareness": """
    # ✅ CORRECT - Direct lookup with correct database
    from redis_files.redis_key_standards import get_key_builder, RedisDatabase
    
    builder = get_key_builder()
    
    # Live data (DB1)
    ohlc_key = builder.live_ohlc_latest("NIFTY")
    data = redis_db1_client.get(ohlc_key)  # Explicit DB1 client
    
    # Analytics (DB2) 
    pattern_key = builder.analytics_pattern_history("high", "BANKNIFTY", "values")
    data = redis_db2_client.get(pattern_key)  # Explicit DB2 client
    """,
    
    "GOOD - Automatic Client Selection": """
    # ✅ CORRECT - Automatic database selection
    from redis_files.redis_key_standards import RedisKeyStandards
    
    # Clients mapped by database
    redis_clients = {
        RedisDatabase.DB1_LIVE_DATA: redis_db1_client,
        RedisDatabase.DB2_ANALYTICS: redis_db2_client
    }
    
    # Get correct client automatically
    ohlc_key = get_key_builder().live_ohlc_latest("NIFTY")
    client = RedisKeyStandards.get_redis_client(ohlc_key, redis_clients)
    data = client.get(ohlc_key)
    """,
    
    "GOOD - Pub/Sub Channels": """
    # ✅ CORRECT - Pub/Sub (no database)
    from redis_files.redis_key_standards import get_key_builder
    
    builder = get_key_builder()
    alert_channel = builder.pubsub_alerts_system()
    
    # Pub/Sub uses separate connection, not database-specific
    redis_pubsub = redis_client.pubsub()
    redis_pubsub.subscribe(alert_channel)
    """,
    
    "DATABASE ASSIGNMENT EXAMPLES": """
    # DB1 - Live Data Examples:
    builder.live_ohlc_latest("NIFTY")                    # → "ohlc_latest:NIFTY"
    builder.live_ticks_stream("RELIANCE")               # → "ticks:stream:RELIANCE" 
    builder.live_underlying_price("BANKNIFTY")          # → "underlying_price:BANKNIFTY"
    builder.live_volume_state("256265")                 # → "vol:state:256265"
    builder.live_indicator("NIFTY", "rsi")              # → "ind:ta:NIFTY:rsi"
    
    # DB2 - Analytics Examples:
    builder.analytics_pattern_history("high", "NIFTY", "values")  # → "pattern_history:high:NIFTY:values"
    builder.analytics_scanner_performance("metrics", "daily")     # → "scanner:performance:metrics:daily"
    builder.analytics_alert_performance_stats()                   # → "alert_performance:stats"
    builder.analytics_signal_quality("BANKNIFTY", "breakout")     # → "signal_quality:BANKNIFTY:breakout"
    
    # Pub/Sub Channels:
    builder.pubsub_alerts_system()                       # → "alerts:system"
    builder.pubsub_validation_results()                  # → "alerts:validation:results"
    """
}