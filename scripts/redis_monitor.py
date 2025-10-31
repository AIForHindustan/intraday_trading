import time
import redis
import psutil


def monitor_redis_connections(poll_interval: int = 30, idle_timeout: int = 300):
    """Emergency Redis connection monitor with auto cleanup for idle clients."""
    while True:
        try:
            client = redis.Redis(max_connections=1)
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
        except Exception as exc:
            print(f"❌ [MONITOR_ERROR] {exc}")
            time.sleep(max(poll_interval, 60))
