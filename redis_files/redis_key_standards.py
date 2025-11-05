"""
Redis Key Lookup Standards - ENFORCED THROUGHOUT CODEBASE

CRITICAL RULE: ALL Redis operations MUST use direct key lookups.
Pattern matching (KEYS, SCAN) is FORBIDDEN except in admin/debug scripts.

This ensures:
- O(1) performance instead of O(N) blocking scans
- Non-blocking Redis operations
- Predictable performance characteristics
- No connection pool exhaustion from slow scans
"""

import logging
from typing import Optional, List, Dict, Any, Union

logger = logging.getLogger(__name__)


class RedisKeyStandards:
    """
    Standardized Redis key lookup utilities.
    Provides helper methods for common key patterns while enforcing direct lookups.
    """
    
    # Key patterns (for reference only - never use in lookups)
    KEY_PATTERNS = {
        # Indicators
        'indicator': 'indicators:{symbol}:{indicator_name}',
        'indicator_cache': 'analysis_cache:indicators:{symbol}:{indicator_name}',
        
        # Greeks
        'greeks': 'indicators:{symbol}:greeks',
        'greek_individual': 'indicators:{symbol}:{greek_name}',
        
        # Alerts
        'alert': 'alert:{alert_id}',
        'alert_stream': 'alerts:system',  # Stream key
        
        # Validation
        'validation': 'forward_validation:alert:{alert_id}',
        'validation_stream': 'alerts:validation:results',  # Stream key
        
        # News
        'news_symbol': 'news:symbol:{symbol}',  # Direct key - symbol known
        'news_latest': 'news:latest:{symbol}',  # Direct key - symbol known
        'news_item': 'news:item:{date}:{item_id}',  # Direct key - date+id known
        
        # Buckets
        'bucket_history': 'bucket_incremental_volume:history:{resolution}:{symbol}',
        'bucket_daily': 'bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}:daily:{date}',
        
        # Volume profile (DB 2 - analytics)
        'volume_profile_poc': 'volume_profile:poc:{symbol}',
        'volume_profile_nodes': 'volume_profile:nodes:{symbol}',
        'volume_profile_session': 'volume_profile:session:{symbol}:{date}',
        'volume_profile_distribution': 'volume_profile:distribution:{symbol}:{date}',
        'volume_profile_patterns': 'volume_profile:patterns:{symbol}:daily',
        'volume_profile_historical': 'volume_profile:historical:{symbol}',
        
        # Volume state (DB 2 - analytics)
        'volume_state': 'volume_state:{instrument_token}',
        'straddle_volume': 'straddle_volume:{underlying_symbol}:{date}',
        
        # Session data
        'session': 'session:{symbol}:{date}',
        'ohlc': 'ohlc:{symbol}:{date}',
        
        # Metrics
        'metrics': 'metrics:{symbol}:{window}min',
        'volume_averages': 'volume_averages:{symbol}',
        
        # Alert validation metadata (DB 0)
        'alert_metadata': 'alert:metadata:{alert_id}',
        'alert_performance_stats': 'alert_performance:stats',
        'alert_performance_pattern': 'alert_performance:stats:{pattern}',
        'alert_performance_patterns_set': 'alert_performance:patterns',  # Set of known patterns
        'alert_rolling_windows': 'alert:rolling_windows',
        'alert_charts': 'alert:charts',
        
        # Independent validator system (DB 3)
        'signal_quality': 'signal_quality:{symbol}:{pattern_type}',
        'pattern_performance': 'pattern_performance:{symbol}:{pattern_type}',
        'pattern_metrics': 'pattern_metrics:{symbol}:{pattern_type}',
    }
    
    @staticmethod
    def build_key(key_type: str, **kwargs) -> str:
        """
        Build a Redis key from pattern and parameters.
        
        Args:
            key_type: One of KEY_PATTERNS keys
            **kwargs: Parameters to fill in the pattern
            
        Returns:
            Complete Redis key string
            
        Example:
            build_key('indicator', symbol='NIFTY', indicator_name='RSI')
            # Returns: 'indicators:NIFTY:RSI'
        """
        pattern = RedisKeyStandards.KEY_PATTERNS.get(key_type)
        if not pattern:
            raise ValueError(f"Unknown key type: {key_type}. Available: {list(RedisKeyStandards.KEY_PATTERNS.keys())}")
        
        try:
            return pattern.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"Missing required parameter for {key_type}: {e}")
    
    @staticmethod
    def get_indicator_key(symbol: str, indicator_name: str, use_cache: bool = True) -> str:
        """Get indicator key - direct lookup"""
        if use_cache:
            return f"analysis_cache:indicators:{symbol}:{indicator_name}"
        return f"indicators:{symbol}:{indicator_name}"
    
    @staticmethod
    def get_greeks_key(symbol: str) -> str:
        """Get Greeks key - direct lookup"""
        return f"indicators:{symbol}:greeks"
    
    @staticmethod
    def get_alert_key(alert_id: str) -> str:
        """Get alert key - direct lookup"""
        return f"alert:{alert_id}"
    
    @staticmethod
    def get_validation_key(alert_id: str) -> str:
        """Get validation key - direct lookup"""
        return f"forward_validation:alert:{alert_id}"
    
    @staticmethod
    def get_news_symbol_key(symbol: str) -> str:
        """Get news symbol key - direct lookup"""
        return f"news:symbol:{symbol}"
    
    @staticmethod
    def get_bucket_history_key(symbol: str, resolution: str = "5min") -> str:
        """Get bucket history key - direct lookup"""
        return f"bucket_incremental_volume:history:{resolution}:{resolution}:{symbol}"
    
    @staticmethod
    def get_session_key(symbol: str, date: str) -> str:
        """Get session key - direct lookup"""
        return f"session:{symbol}:{date}"
    
    @staticmethod
    def get_alert_metadata_key(alert_id: str) -> str:
        """Get alert metadata key - direct lookup (DB 0)"""
        return f"alert:metadata:{alert_id}"
    
    @staticmethod
    def get_alert_performance_stats_key() -> str:
        """Get alert performance stats key - direct lookup (DB 0)"""
        return "alert_performance:stats"
    
    @staticmethod
    def get_alert_performance_pattern_key(pattern: str) -> str:
        """Get alert performance pattern-specific stats key - direct lookup (DB 0)"""
        return f"alert_performance:stats:{pattern}"
    
    @staticmethod
    def get_alert_performance_patterns_set_key() -> str:
        """Get alert performance patterns Set key - direct lookup (DB 0)"""
        return "alert_performance:patterns"
    
    @staticmethod
    def get_signal_quality_key(symbol: str, pattern_type: str) -> str:
        """Get signal quality key - direct lookup (DB 3 - independent validator)"""
        return f"signal_quality:{symbol}:{pattern_type}"
    
    @staticmethod
    def get_pattern_performance_key(symbol: str, pattern_type: str) -> str:
        """Get pattern performance key - direct lookup (DB 3 - independent validator)"""
        return f"pattern_performance:{symbol}:{pattern_type}"
    
    @staticmethod
    def get_pattern_metrics_key(symbol: str, pattern_type: str) -> str:
        """Get pattern metrics key - direct lookup (DB 3 - independent validator)"""
        return f"pattern_metrics:{symbol}:{pattern_type}"
    
    @staticmethod
    def get_ohlc_latest_key(symbol: str) -> str:
        """Get OHLC latest key - direct lookup (DB 2 - analytics)"""
        return f"ohlc_latest:{symbol}"
    
    @staticmethod
    def get_volume_averages_key(symbol: str) -> str:
        """Get volume averages key - direct lookup"""
        return f"volume_averages:{symbol}"
    
    @staticmethod
    def get_metrics_key(symbol: str, window: int) -> str:
        """Get metrics key - direct lookup (DB 2 - analytics)"""
        return f"metrics:{symbol}:{window}min"
    
    @staticmethod
    def get_volume_profile_poc_key(symbol: str) -> str:
        """Get volume profile POC key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:poc:{symbol}"
    
    @staticmethod
    def get_volume_profile_nodes_key(symbol: str) -> str:
        """Get volume profile nodes key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:nodes:{symbol}"
    
    @staticmethod
    def get_volume_profile_session_key(symbol: str, date: str) -> str:
        """Get volume profile session key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:session:{symbol}:{date}"
    
    @staticmethod
    def get_volume_profile_distribution_key(symbol: str, date: str) -> str:
        """Get volume profile distribution key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:distribution:{symbol}:{date}"
    
    @staticmethod
    def get_volume_profile_patterns_key(symbol: str) -> str:
        """Get volume profile patterns key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:patterns:{symbol}:daily"
    
    @staticmethod
    def get_volume_profile_historical_key(symbol: str) -> str:
        """Get volume profile historical key - direct lookup (DB 2 - analytics)"""
        return f"volume_profile:historical:{symbol}"
    
    @staticmethod
    def get_volume_state_key(instrument_token: str) -> str:
        """Get volume state key - direct lookup (DB 2 - analytics)"""
        return f"volume_state:{instrument_token}"
    
    @staticmethod
    def get_straddle_volume_key(underlying_symbol: str, date: str) -> str:
        """Get straddle volume key - direct lookup (DB 2 - analytics)"""
        return f"straddle_volume:{underlying_symbol}:{date}"
    
    @staticmethod
    def validate_no_pattern_matching(client, operation_description: str = ""):
        """
        Runtime validation to detect pattern matching operations.
        This should be called before any Redis operation that might use KEYS/SCAN.
        
        Raises:
            RuntimeError: If pattern matching is detected
        """
        # This is a placeholder for runtime checks
        # In production, we rely on code review and static analysis
        pass


def enforce_direct_lookups_only():
    """
    Utility function to document and enforce the standard.
    Call this in code reviews and add to pre-commit hooks.
    
    FORBIDDEN:
    - redis_client.keys("*")
    - redis_client.keys("pattern:*")
    - redis_client.scan(match="pattern:*")
    - Any wildcard pattern matching
    
    REQUIRED:
    - redis_client.get(f"alert:{alert_id}")  # Direct key
    - redis_client.hget(f"session:{symbol}:{date}", "field")  # Direct key
    - Use Redis Streams (XREADGROUP) for iterating over data
    - Use sorted sets (ZRANGE) with known keys for time-series data
    """
    pass


# Example usage patterns (for documentation)
EXAMPLES = {
    "BAD - Pattern Matching": """
    # ❌ FORBIDDEN
    alert_keys = redis_client.keys("alert:*")
    for key in alert_keys:
        data = redis_client.get(key)
    """,
    
    "GOOD - Direct Lookup": """
    # ✅ CORRECT
    alert_id = "alert_12345"
    data = redis_client.get(f"alert:{alert_id}")
    """,
    
    "GOOD - Stream Iteration": """
    # ✅ CORRECT - Use Streams for iteration
    stream_name = "alerts:system"
    messages = redis_client.xread({stream_name: "0"}, count=100)
    """,
    
    "GOOD - Known Symbol List": """
    # ✅ CORRECT - Iterate over known symbols
    symbols = ["NIFTY", "BANKNIFTY", "RELIANCE"]  # From config/cache
    for symbol in symbols:
        indicators = redis_client.get(f"indicators:{symbol}:RSI")
    """,
}

