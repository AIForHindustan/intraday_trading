"""
Core Data Package
=================

Centralized data processing components for the intraday trading system.

Modules:
- redis_client: Enhanced Redis client with auto-reconnection and database segmentation
- data_pipeline: Clean data processing pipeline for tick ingestion and preprocessing
- token_resolver: Single source for token-to-symbol resolution
"""

from redis_files.redis_client import RobustRedisClient, get_redis_client, publish_to_redis
from intraday_scanner.data_pipeline import DataPipeline
from crawlers.utils.instrument_mapper import InstrumentMapper
instrument_mapper = InstrumentMapper()
resolve_token_to_symbol = instrument_mapper.token_to_symbol

# Health monitoring
def check_redis_health(host="localhost", port=6379):
    """Check Redis health using the consolidated health script"""
    from redis_files.redis_health import main as health_main
    import sys
    # Use centralized Redis configuration
    from config.redis_config import get_redis_config
    redis_config = get_redis_config()
    host = redis_config.get("host", host)
    port = redis_config.get("port", port)
    sys.argv = ["redis_health", "--host", host, "--port", str(port)]
    return health_main()

__all__ = [
    # Redis client
    "RobustRedisClient",
    "get_redis_client", 
    "publish_to_redis",
    
    # Data pipeline
    "DataPipeline",
    
    # Token resolver
    "resolve_token_to_symbol",
    
    # Health monitoring
    "check_redis_health",
]
