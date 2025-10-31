"""
Redis 8.0 Configuration
=======================

Single source of truth for Redis connection settings, database segmentation,
and feature flags (client tracking, RedisJSON readiness).
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

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
    "max_connections": 200,  # High limit for high-frequency trading data (multiple scanners, dashboards, crawlers)
    "health_check_interval": 30,  # Redis 8.0 enhancement
    "socket_timeout": 2,  # Reduced from default for faster response
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
            "incremental_volume",
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

        # Environment tuning - use same high limits for all environments
        # High-frequency trading requires many concurrent connections regardless of environment
        if self.environment == "prod":
            self.base_config.update({"max_connections": 500, "health_check_interval": 60})
        elif self.environment == "dev":
            # Dev also needs high connections for testing multiple components
            self.base_config.update({"max_connections": 200, "health_check_interval": 10})

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

