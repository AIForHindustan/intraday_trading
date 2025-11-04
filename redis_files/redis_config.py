"""
Redis 8.0 Configuration
=======================

Single source of truth for Redis connection settings, database segmentation,
and feature flags (client tracking, RedisJSON readiness).
"""

from __future__ import annotations

import os
import time
import threading
from typing import Any, Dict, Optional
import redis
from redis.connection import ConnectionPool

# ---------------------------------------------------------------------------
# Base connection configuration (Redis 8.x)
# ---------------------------------------------------------------------------

REDIS_8_CONFIG: Dict[str, Any] = {
    "host": os.environ.get("REDIS_HOST", "localhost"),
    "port": int(os.environ.get("REDIS_PORT", 6379)),
    "password": os.environ.get("REDIS_PASSWORD") or None,
    "decode_responses": True,  # Decode to strings for easier processing
    "socket_connect_timeout": 5,
    "socket_keepalive": True,
    "retry_on_timeout": True,
    "max_connections": 50,  # Increased for high-frequency trading data         
    "health_check_interval": 30,  # Redis 8.0 enhancement
    "socket_timeout": 2,  # Reduced from default for faster response
}

# Redis 8.2 low-latency optimizations for tick/crawler workload
REDIS_8_LOW_LATENCY_CONFIG: Dict[str, Any] = {
    # Persistence (avoid per-write fsync stalls)
    "appendonly": True,
    "appendfsync": "everysec",
    "no-appendfsync-on-rewrite": True,
    "rdb-save-incremental-fsync": True,
    "save": "",  # Disable RDB saves if AOF is sufficient
    
    # Latency monitoring (8.x adds per-command percentiles)
    "latency-tracking": True,
    "latency-monitor-threshold": 100,  # Record spikes >=100ms
    
    # Networking / event loop
    "tcp-keepalive": 60,
    "tcp-backlog": 511,
    
    # I/O threads (helpful for read-heavy Pub/Sub / Streams)
    "io-threads": 4,  # min(CPU cores/2, 4)
    "io-threads-do-reads": True,  # Only reads (writes remain single-threaded)
    
    # Memory & defrag (avoid stop-the-world malloc)
    "maxmemory": "4gb",
    "maxmemory-policy": "allkeys-lru",
    "activedefrag": True,
    "active-defrag-ignore-bytes": 1048576,
    "active-defrag-threshold-lower": 10,
    "active-defrag-threshold-upper": 100,
    
    # Lazy frees (reduce big-key delete stalls)
    "lazyfree-lazy-eviction": True,
    "lazyfree-lazy-expire": True,
    "lazyfree-lazy-server-del": True,
    
    # Pub/Sub buffers (protect server under bursts)
    "client-output-buffer-limit": "pubsub 64mb 16mb 60",
    "client-output-buffer-limit-normal": "0 0 0",
    
    # Streams (prefer over LPUSH/LTRIM; cheap trimming)
    "stream-node-max-bytes": 4096,
    "stream-node-max-entries": 100,
    
    # Server loop
    "hz": 10,  # Default; avoid cranking up unless measured
}

# Optional global feature flags (e.g., client tracking, RedisJSON support)
REDIS_8_FEATURE_FLAGS: Dict[str, bool] = {
    "client_tracking": True,
    "use_json": False,  # set to True when RedisJSON module is loaded
}

# ---------------------------------------------------------------------------
# Database segmentation (single source of truth)
# Consolidated to 3 databases for simplicity and performance
# ---------------------------------------------------------------------------

REDIS_DATABASES: Dict[int, Dict[str, Any]] = {
    # System / metadata
    0: {
        "name": "system",
        "ttl": 57_600,
        "data_types": ["system_config", "metadata", "session_data", "health_checks"],
        "client_tracking": False,
        "use_json": False,
    },
    # Real-time data (consolidated from DBs 2,3,4,6,8,10,11)
    # All live/streaming data: ticks, alerts, patterns, prices, news, etc.
    1: {
        "name": "realtime",
        "ttl": 57_600,
        "data_types": [
            # Prices (from DB 2)
            "equity_prices",
            "equity_cash_prices",
            "futures_prices",
            "options_prices",
            "commodity_prices",
            # Premarket (from DB 3)
            "premarket_trades",
            "bucket_incremental_volume",  # Canonical field name (replaces legacy "incremental_volume")
            "incremental_volume",  # Legacy identifier - kept for backward compatibility
            "opening_data",
            # Continuous market / ticks (from DB 4)
            "5s_ticks",
            "ticks_optimized",
            "real_time_data",
            "streaming_data",
            "stream_data",
            "ticks_stream",
            "alerts_stream",
            "patterns_stream",
            # Alerts (from DB 6)
            "pattern_alerts",
            "trading_alerts",
            "system_alerts",
            # Microstructure (from DB 8)
            "depth_analysis",
            "flow_data",
            "microstructure_metrics",
            # Patterns (from DB 10)
            "pattern_candidates",
            "detection_data",
            "analysis_cache",
            # News (from DB 11)
            "news_data",
            "sentiment_scores",
            "market_news",
        ],
        "client_tracking": True,  # Enable for pattern detection cache invalidation
        "use_json": True,  # Enable for complex alert/pattern structures
    },
    # Analytics / Historical data (consolidated from DBs 5,13)
    # Historical data, metrics, volume profiles
    2: {
        "name": "analytics",
        "ttl": 57_600,
        "data_types": [
            # Cumulative volume (from DB 5)
            "daily_cumulative",
            "symbol_volume",
            "historical_data",
            # Metrics (from DB 13)
            "performance_data",
            "metrics_cache",
            "analytics_data",
        ],
        "client_tracking": True,  # ideal for local cache invalidation
        "use_json": False,
    },
    # Indicators and Greeks cache (DB 5)
    # Per user requirement: indicators and Greeks stored in DB 5
    5: {
        "name": "indicators_cache_db",
        "ttl": 300,  # 5 minutes to match calculation cache
        "data_types": [
            "indicators_cache",  # Primary storage for indicators and Greeks
        ],
        "client_tracking": False,
        "use_json": True,
    },
}


# ---------------------------------------------------------------------------
# Redis 8.0 configuration helper
# ---------------------------------------------------------------------------

class Redis8Config:
    """Unified Redis configuration with feature awareness."""

    def __init__(self, environment: Optional[str] = None):
        self.environment = environment or os.environ.get("ENVIRONMENT", "dev")
        self.base_config = REDIS_8_CONFIG.copy()
        self.global_features = REDIS_8_FEATURE_FLAGS.copy()

        # Environment tuning
        if self.environment == "prod":
            self.base_config.update({"max_connections": 50, "health_check_interval": 60})
        elif self.environment == "dev":
            self.base_config.update({"max_connections": 10, "health_check_interval": 10})

    def apply_low_latency_optimizations(self, redis_client) -> bool:
        """
        Apply Redis 8.2 low-latency optimizations for tick/crawler workload.
        
        Args:
            redis_client: Connected Redis client instance
            
        Returns:
            bool: True if optimizations applied successfully
        """
        try:
            import redis
            
            # Apply low-latency configurations
            for config_key, config_value in REDIS_8_LOW_LATENCY_CONFIG.items():
                try:
                    if isinstance(config_value, str) and config_value == "":
                        # Handle empty string values (like save "")
                        redis_client.config_set(config_key, "")
                    elif isinstance(config_value, bool):
                        # Convert boolean to string
                        redis_client.config_set(config_key, "yes" if config_value else "no")
                    else:
                        redis_client.config_set(config_key, config_value)
                except redis.exceptions.ResponseError as e:
                    # Some configs might not be available or require restart
                    print(f"⚠️  Config {config_key}={config_value} failed: {e}")
                    continue
            
            print("✅ Redis 8.2 low-latency optimizations applied successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to apply Redis optimizations: {e}")
            return False

    def _resolve_db_number(self, db_name: str) -> int:
        for number, spec in REDIS_DATABASES.items():
            if spec["name"] == db_name:
                return number
        raise KeyError(f"Unknown Redis DB name: {db_name}")

    def get_db_config(self, db_name: str) -> Dict[str, Any]:
        """Return base connection kwargs for a named database."""
        config = self.base_config.copy()
        config["db"] = self._resolve_db_number(db_name)
        return config

    def get_db_features(self, db_name: str) -> Dict[str, bool]:
        """Return feature flags (client tracking / JSON) for a named database."""
        db_num = self._resolve_db_number(db_name)
        spec = REDIS_DATABASES[db_num]
        return {
            "client_tracking": bool(spec.get("client_tracking", False)),
            "use_json": bool(spec.get("use_json", False)),
        }


# ---------------------------------------------------------------------------
# Compatibility helpers (legacy API)
# ---------------------------------------------------------------------------

def get_redis_config(environment: Optional[str] = None) -> Dict[str, Any]:
    """
    Backwards-compatible accessor that returns the base connection configuration.
    (Note: client-tracking must be enabled explicitly by the caller.)
    """
    cfg = Redis8Config(environment)
    return cfg.base_config.copy()


def get_database_for_data_type(data_type: str) -> int:
    """Map a logical data type to its Redis database number."""
    for db_num, spec in REDIS_DATABASES.items():
        if data_type in spec.get("data_types", []):
            return db_num
    return 0


def get_ttl_for_data_type(data_type: str) -> int:
    """Return TTL (seconds) for a given data type."""
    for spec in REDIS_DATABASES.values():
        if data_type in spec.get("data_types", []):
            return spec.get("ttl", 3600)
    return 3600


def apply_redis_low_latency_optimizations(redis_client) -> bool:
    """
    Convenience function to apply Redis 8.2 low-latency optimizations.
    
    Args:
        redis_client: Connected Redis client instance
        
    Returns:
        bool: True if optimizations applied successfully
    """
    config = Redis8Config()
    return config.apply_low_latency_optimizations(redis_client)


# ---------------------------------------------------------------------------
# Connection Pool Configuration - Process-Specific Pools
# ---------------------------------------------------------------------------

# Process-specific pool size configurations (prevents over-allocation)
# ✅ SOLUTION 4: Increased pool sizes to prevent connection exhaustion
# 
# IMPORTANT: RedisManager82.get_client() will automatically use PROCESS_POOL_CONFIG values
# if available, so these values override any hardcoded max_connections parameter.
# 
# Pool sizes are optimized for high-throughput processes:
# - High-frequency processes (scanner, crawler) get larger pools
# - Moderate processes (consumers, validators) get moderate pools
# - Low-frequency processes (dashboard, cleanup) get smaller pools
PROCESS_POOL_CONFIG: Dict[str, int] = {
    # ✅ SOLUTION 4: Increased for high-throughput processes
    "intraday_scanner": 30,      # Main scanner needs more connections for high-frequency processing
    "intraday_crawler": 15,      # Crawler needs dedicated pool for high-frequency publishing
    "stream_consumer": 10,       # Stream consumers need dedicated connections
    "data_pipeline": 10,         # Data pipeline consumer group
    "dashboard": 8,              # Dashboard needs fewer (mostly read operations)
    "validator": 10,            # Moderate needs (validation processing)
    "alert_validator": 10,       # Alert validator consumer
    "cleanup": 3,                # Maintenance tasks (minimal connections)
    "monitor": 3,                # Stream monitor (proactive monitoring)
    "ngrok": 5,                  # Minimal needs (tunnel management)
    # Legacy/fallback values
    "crawler": 15,               # Generic crawler pool size (matches intraday_crawler)
    "gift_nifty_crawler": 2,     # Index updater needs minimal connections (30s intervals)
    "gift_nifty_gap": 2,         # Alias for gift_nifty_crawler (backward compatibility)
    "default": 10,               # Default for unknown processes
}

# ⚠️ DEPRECATED FUNCTIONS REMOVED:
# - create_process_specific_pool() -> Use RedisManager82.get_client()
# - get_redis_connection_pool() -> Use RedisManager82.get_client()
# - get_redis_client_from_pool() -> Use RedisManager82.get_client()
#
# Migration guide:
# - Old: get_redis_client_from_pool(db=1)
# - New: RedisManager82.get_client(process_name="your_process", db=1)


# ---------------------------------------------------------------------------
# Redis Optimization Examples (REMOVED - move to documentation if needed)
# ---------------------------------------------------------------------------
# optimize_redis_usage_examples() removed - contained examples only


class RedisConfig(Redis8Config):
    """
    Legacy wrapper retained for import compatibility.
    Exposes a ``databases`` property (name -> db number) matching prior usage.
    """

    def __init__(self, environment: Optional[str] = None):
        super().__init__(environment)
        self.databases = {spec["name"]: db for db, spec in REDIS_DATABASES.items()}


# ---------------------------------------------------------------------------
# AION analytics channels (replaces legacy DeepSeek exports)
# ---------------------------------------------------------------------------

AION_CHANNELS: Dict[str, str] = {
    "correlations": "aion.correlations",
    "divergences": "aion.divergences",
    "validations": "aion.validated",
    "alerts": "aion.alerts",
    "monte_carlo": "aion.monte_carlo",
    "granger": "aion.granger",
    "dbscan": "aion.dbscan",
}

