"""
Redis 8.2 Optimized Connection Manager
Process-specific connection pooling with Redis 8.2 enhancements
"""

import redis
import os
import time
import threading
from typing import Dict, Optional
from redis.connection import ConnectionPool

class RedisManager82:
    """
    Redis 8.2 optimized connection manager with process-specific pools.
    
    Features:
    - Process-specific connection pools
    - Redis 8.2 health check intervals
    - Automatic client naming (process_PID_timestamp)
    - Connection cleanup methods
    - Thread-safe pool management
    """
    
    _pools: Dict[str, Dict[int, redis.ConnectionPool]] = {}  # process_name -> {db -> pool}
    _lock = threading.Lock()
    
    @classmethod
    def get_client(
        cls, 
        process_name: str, 
        db: int = 0,
        max_connections: int = None,  # ✅ SOLUTION 4: Default to None to force config lookup
        host: str = 'localhost',
        port: int = 6379,
        password: Optional[str] = None,
        decode_responses: bool = True
    ) -> redis.Redis:
        """
        Get a Redis client with process-specific connection pool.
        
        ✅ SOLUTION 4: Uses PROCESS_POOL_CONFIG for optimal connection pool sizes.
        Connection pools are cached per process+db to enable connection reuse.
        
        Args:
            process_name: Unique identifier for the process
            db: Redis database number (default: 0)
            max_connections: Max connections for this pool (if None, uses PROCESS_POOL_CONFIG)
            host: Redis host (default: 'localhost')
            port: Redis port (default: 6379)
            password: Redis password (optional)
            decode_responses: If True, decode responses to strings (default: True)
            
        Returns:
            redis.Redis: Redis client instance with process-specific pool
        """
        # Check for environment or config overrides
        host = os.getenv('REDIS_HOST', host)
        port = int(os.getenv('REDIS_PORT', port))
        password = os.getenv('REDIS_PASSWORD') or password
        
        # ✅ SOLUTION 4: Get optimized pool size from PROCESS_POOL_CONFIG
        # Priority: PROCESS_POOL_CONFIG > provided max_connections > default (10)
        if max_connections is None:
            try:
                from redis_files.redis_config import PROCESS_POOL_CONFIG
                max_connections = PROCESS_POOL_CONFIG.get(
                    process_name, 
                    PROCESS_POOL_CONFIG.get('default', 10)
                )
            except Exception:
                max_connections = 10  # Fallback default
        else:
            # If max_connections provided, still check config for override
            try:
                from redis_files.redis_config import PROCESS_POOL_CONFIG
                config_max = PROCESS_POOL_CONFIG.get(process_name)
                if config_max is not None:
                    max_connections = config_max  # Use config value (overrides provided)
            except Exception:
                pass  # Use provided max_connections if config lookup fails
        
        # Thread-safe pool creation/caching
        with cls._lock:
            pool_key = f"{process_name}:{db}"
            
            if process_name not in cls._pools:
                cls._pools[process_name] = {}
            
            if db not in cls._pools[process_name]:
                # Create process-specific pool for this db
                pool_config = {
                    'host': host,
                    'port': port,
                    'db': db,
                    'password': password,
                    'max_connections': max_connections,
                    'socket_keepalive': True,
                    'retry_on_timeout': True,
                    'health_check_interval': 30,  # Redis 8.2 enhancement
                    'socket_timeout': 10,
                    'socket_connect_timeout': 5,
                }
                
                # Remove None values
                pool_config = {k: v for k, v in pool_config.items() if v is not None}
                
                pool = ConnectionPool(**pool_config)
                cls._pools[process_name][db] = pool
        
        # Get cached pool
        pool = cls._pools[process_name][db]
        
        # Create client with connection pool
        client = redis.Redis(
            connection_pool=pool,
            decode_responses=decode_responses
        )
        
        # Set descriptive client name (process_PID) for monitoring
        try:
            client_id = f"{process_name}_{os.getpid()}"
            client.client_setname(client_id)
        except Exception:
            pass  # Ignore errors setting name
        
        return client
    
    @classmethod
    def cleanup(cls, process_name: Optional[str] = None):
        """
        Cleanup connection pools.
        
        Args:
            process_name: If provided, cleanup only this process's pools.
                         If None, cleanup all pools.
        """
        with cls._lock:
            if process_name:
                # Cleanup specific process
                if process_name in cls._pools:
                    for pool in cls._pools[process_name].values():
                        try:
                            pool.disconnect()
                        except Exception:
                            pass
                    del cls._pools[process_name]
            else:
                # Cleanup all processes
                for process_pools in cls._pools.values():
                    for pool in process_pools.values():
                        try:
                            pool.disconnect()
                        except Exception:
                            pass
                cls._pools.clear()
    
    # get_crawler_client() removed - use get_client() directly with process_name="crawler_{crawler_name}"
    
    @classmethod
    def get_pool_stats(cls) -> Dict:
        """Get statistics about connection pools"""
        with cls._lock:
            stats = {}
            for process_name, db_pools in cls._pools.items():
                stats[process_name] = {
                    'databases': list(db_pools.keys()),
                    'total_pools': len(db_pools),
                    'total_connections': sum(
                        pool.connection_kwargs.get('max_connections', 0) 
                        for pool in db_pools.values()
                    )
                }
            return stats


# Pre-configured convenience wrappers removed - use RedisManager82.get_client() directly
# Example: RedisManager82.get_client(process_name="intraday_scanner", db=0)

