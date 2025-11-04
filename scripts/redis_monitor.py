"""
Redis Connection Monitor - Emergency Connection Cleanup
Uses RedisManager82 for centralized connection management
"""

import time
import psutil


def monitor_redis_connections(poll_interval: int = 30, idle_timeout: int = 300):
    """
    Emergency Redis connection monitor with auto cleanup for idle clients.
    
    ✅ Uses RedisManager82 for centralized connection management to avoid connection storms.
    """
    while True:
        try:
            # ✅ Use RedisManager82 for centralized connection management
            from redis_files.redis_manager import RedisManager82
            
            client = RedisManager82.get_client(
                process_name="redis_monitor",
                db=0,  # Default DB for monitoring
                max_connections=1  # Monitor only needs one connection
            )
            
            if not client:
                print("⚠️ [REDIS_MONITOR] Failed to get Redis client from RedisManager82")
                time.sleep(poll_interval)
                continue
            
            info = client.info("clients")
            connected = info.get("connected_clients", 0)
            process = psutil.Process()
            threads = process.num_threads()

            print(f"🔍 [REDIS_MONITOR] Connections: {connected}, Threads: {threads}")

            if connected > 50:
                print(f"🚨 [REDIS_CRITICAL] {connected} connections - killing idle clients (> {idle_timeout}s)")
                try:
                    client.client_kill_filter(_idle=idle_timeout)
                except Exception as kill_err:
                    print(f"⚠️ [REDIS_MONITOR] Failed to kill idle clients: {kill_err}")

            time.sleep(poll_interval)
        except ImportError:
            # Fallback if RedisManager82 not available
            print("⚠️ [REDIS_MONITOR] RedisManager82 not available, using fallback")
            try:
                import redis
                client = redis.Redis(max_connections=1)
                info = client.info("clients")
                connected = info.get("connected_clients", 0)
                print(f"🔍 [REDIS_MONITOR] Connections: {connected} (fallback mode)")
                time.sleep(poll_interval)
            except Exception as exc:
                print(f"❌ [MONITOR_ERROR] {exc}")
                time.sleep(max(poll_interval, 60))
        except Exception as exc:
            print(f"❌ [MONITOR_ERROR] {exc}")
            time.sleep(max(poll_interval, 60))
