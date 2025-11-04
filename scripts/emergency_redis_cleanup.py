#!/usr/bin/env python3
"""
Emergency Redis Connection Cleanup Script
=========================================

Periodically cleans up unnamed/idle Redis connections to prevent "Too many connections" errors.

This script:
- Only targets connections without names (unnamed/zombie connections)
- Only closes connections idle for > 10 minutes
- Runs in a daemon thread so it doesn't block
- Does NOT kill crawler/scanner/dashboard connections (they have names)

Usage:
    python scripts/emergency_redis_cleanup.py
    
Or import and use in your application:
    from scripts.emergency_redis_cleanup import start_cleanup_thread
    start_cleanup_thread()
"""

import redis
import time
import threading
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def cleanup_unnamed_connections(
    host: str = 'localhost',
    port: int = 6379,
    idle_threshold_seconds: int = 600,  # 10 minutes
    check_interval_seconds: int = 300,  # Check every 5 minutes
    stop_event: Optional[threading.Event] = None
):
    """
    Periodically clean up unnamed/idle Redis connections.
    
    Args:
        host: Redis host (default: 'localhost')
        port: Redis port (default: 6379)
        idle_threshold_seconds: Close connections idle for this long (default: 600 = 10 min)
        check_interval_seconds: How often to check (default: 300 = 5 min)
        stop_event: Optional threading.Event to stop the loop
    """
    try:
        client = redis.Redis(host=host, port=6379, decode_responses=False)
        client.ping()  # Test connection
        logger.info("✅ Emergency cleanup script connected to Redis")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        return
    
    cleanup_count = 0
    
    while True:
        try:
            # Check if we should stop
            if stop_event and stop_event.is_set():
                logger.info("🛑 Emergency cleanup thread stopping...")
                break
            
            # Get all client connections
            clients = client.client_list()
            
            killed_count = 0
            for c in clients:
                try:
                    # Parse client info (can be dict or string)
                    if isinstance(c, dict):
                        client_name = c.get('name', '') or ''
                        client_addr = c.get('addr', '')
                        client_idle = int(c.get('idle', 0))
                    else:
                        # Parse from string format if needed
                        client_name = ''
                        client_addr = ''
                        client_idle = 0
                        # Skip string parsing for now - focus on dict format
                        continue
                    
                    # Only target unnamed connections (empty name or None)
                    # AND only if idle for more than threshold
                    if not client_name and client_idle > idle_threshold_seconds:
                        try:
                            # Kill the connection by address
                            if client_addr:
                                # Parse address format (host:port)
                                client.client_kill(client_addr)
                                killed_count += 1
                                cleanup_count += 1
                                logger.info(f"🧹 Closed unnamed idle connection: {client_addr} (idle: {client_idle}s)")
                        except Exception as kill_error:
                            logger.warning(f"⚠️ Could not kill connection {client_addr}: {kill_error}")
                            
                except Exception as parse_error:
                    logger.debug(f"Error parsing client info: {parse_error}")
                    continue
            
            if killed_count > 0:
                logger.info(f"✅ Cleaned up {killed_count} unnamed idle connections (total: {cleanup_count})")
            
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        # Wait before next check
        if stop_event:
            # Use wait with timeout so we can check stop_event
            stop_event.wait(check_interval_seconds)
        else:
            time.sleep(check_interval_seconds)


def start_cleanup_thread(
    host: str = 'localhost',
    port: int = 6379,
    idle_threshold_seconds: int = 600,
    check_interval_seconds: int = 300,
    daemon: bool = True
) -> tuple[threading.Thread, threading.Event]:
    """
    Start emergency cleanup in a daemon thread.
    
    Returns:
        Tuple of (thread, stop_event)
        Use stop_event.set() to stop the cleanup thread
    """
    stop_event = threading.Event()
    
    cleanup_thread = threading.Thread(
        target=cleanup_unnamed_connections,
        args=(host, port, idle_threshold_seconds, check_interval_seconds, stop_event),
        name="EmergencyRedisCleanup",
        daemon=daemon
    )
    cleanup_thread.start()
    
    logger.info(f"🚀 Emergency Redis cleanup thread started (checking every {check_interval_seconds}s)")
    
    return cleanup_thread, stop_event


def main():
    """Run cleanup as standalone script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=" * 60)
    logger.info("🚀 Emergency Redis Connection Cleanup")
    logger.info("=" * 60)
    logger.info("This script will:")
    logger.info("  - Clean up unnamed/idle connections (>10 min idle)")
    logger.info("  - Run every 5 minutes")
    logger.info("  - NOT affect named connections (crawlers/scanner/dashboard)")
    logger.info("=" * 60)
    
    try:
        # Run cleanup in main thread (blocking)
        cleanup_unnamed_connections()
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down emergency cleanup...")


if __name__ == "__main__":
    main()

