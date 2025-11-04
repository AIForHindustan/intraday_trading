"""
Alerts Configuration Package
============================

Centralized configuration for alert management, filtering, and notifications.
Contains telegram configs, Redis settings, and alert thresholds.

Files:
- telegram_config.json: Basic telegram bot configuration
- telegram_api_config.json: Extended telegram API configuration with user data
- redis_config.py: Redis configuration for alert publishing
- thresholds.py: Alert filtering thresholds and VIX regime-aware rules (single source of truth)

Created: October 9, 2025
"""

# Import Redis configuration from core
try:
    from redis_files.redis_config import REDIS_DATABASES, get_redis_config, get_database_for_data_type, get_ttl_for_data_type
    REDIS_CONFIG_AVAILABLE = True
except ImportError:
    REDIS_CONFIG_AVAILABLE = False
    REDIS_DATABASES = {}
    def get_redis_config():
        return {}
    def get_database_for_data_type(data_type):
        return 0
    def get_ttl_for_data_type(data_type):
        return 3600

__all__ = [
    "REDIS_DATABASES",
    "get_redis_config", 
    "get_database_for_data_type",
    "get_ttl_for_data_type",
    "REDIS_CONFIG_AVAILABLE",
]
