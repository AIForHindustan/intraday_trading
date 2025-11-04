"""
Redis Stream Health Monitor
Monitors stream performance, consumer lag, and group metrics.
"""

import sys
import time
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from redis_files.redis_manager import RedisManager82

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def monitor_stream_health(streams_to_monitor=None, check_interval=60):
    """
    Monitor stream performance and consumer lag
    
    ⚠️ DEPRECATED: This function is replaced by utils.stream_monitor.StreamMonitor
    Use utils.stream_monitor.StreamMonitor for proactive monitoring instead.
    
    Args:
        streams_to_monitor: List of stream names to monitor. If None, monitors default streams.
        check_interval: How often to check (seconds). Default 60 seconds.
    """
    if streams_to_monitor is None:
        streams_to_monitor = [
            'ticks:raw:binary',
            'ticks:intraday:processed',
            'alerts:stream'
        ]
    
    try:
        # ✅ SOLUTION 4: Use RedisManager82 with PROCESS_POOL_CONFIG
        redis_client = RedisManager82.get_client(
            process_name="stream_monitor_legacy",
            db=1,  # DB 1 for streams
            max_connections=None  # Use PROCESS_POOL_CONFIG (default: 10)
        )
        
        if not redis_client:
            logger.error("❌ Failed to get Redis client for stream monitoring")
            return
        
        # Test connection
        redis_client.ping()
        logger.info("✅ Connected to Redis DB 1 for stream monitoring")
        logger.info(f"📊 Monitoring {len(streams_to_monitor)} streams every {check_interval} seconds")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                logger.info("\n" + "="*80)
                logger.info(f"📊 Stream Health Report - {time.strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info("="*80)
                
                total_pending = 0
                total_consumers = 0
                streams_found = 0
                
                for stream_name in streams_to_monitor:
                    try:
                        # Check if stream exists
                        stream_info = redis_client.xinfo_stream(stream_name)
                        streams_found += 1
                        
                        stream_length = stream_info.get('length', 0)
                        first_entry_id = stream_info.get('first-entry', [None])[0]
                        last_entry_id = stream_info.get('last-entry', [None])[0]
                        
                        logger.info(f"\n📡 Stream: {stream_name}")
                        logger.info(f"   Length: {stream_length:,} messages")
                        if first_entry_id:
                            logger.info(f"   First entry: {first_entry_id}")
                        if last_entry_id:
                            logger.info(f"   Last entry: {last_entry_id}")
                        
                        # Get consumer groups
                        try:
                            consumer_groups = redis_client.xinfo_groups(stream_name)
                            
                            if consumer_groups:
                                logger.info(f"   Consumer Groups: {len(consumer_groups)}")
                                
                                for group in consumer_groups:
                                    group_name = group.get('name', 'unknown')
                                    pending = group.get('pending', 0)
                                    consumers = group.get('consumers', 0)
                                    last_delivered_id = group.get('last-delivered-id', 'none')
                                    
                                    total_pending += pending
                                    total_consumers += consumers
                                    
                                    # Health status
                                    if pending > 1000:
                                        status = "⚠️  HIGH LAG"
                                    elif pending > 100:
                                        status = "⚠️  MODERATE LAG"
                                    else:
                                        status = "✅ HEALTHY"
                                    
                                    logger.info(f"      Group: {group_name}")
                                    logger.info(f"         Status: {status}")
                                    logger.info(f"         Pending: {pending:,} messages")
                                    logger.info(f"         Consumers: {consumers}")
                                    logger.info(f"         Last delivered: {last_delivered_id}")
                                    
                                    # Get consumer details
                                    try:
                                        consumers_info = redis_client.xinfo_consumers(stream_name, group_name)
                                        if consumers_info:
                                            logger.info(f"         Consumer Details:")
                                            for consumer in consumers_info:
                                                consumer_name = consumer.get('name', 'unknown')
                                                consumer_pending = consumer.get('pending', 0)
                                                idle_time = consumer.get('idle', 0) / 1000  # Convert to seconds
                                                
                                                logger.info(f"            {consumer_name}:")
                                                logger.info(f"               Pending: {consumer_pending:,}")
                                                logger.info(f"               Idle: {idle_time:.1f}s")
                                    except Exception as e:
                                        logger.debug(f"         Could not get consumer details: {e}")
                            else:
                                logger.warning(f"   ⚠️  No consumer groups found for {stream_name}")
                                
                        except Exception as e:
                            error_str = str(e).lower()
                            if "no such key" in error_str or "no such stream" in error_str:
                                logger.warning(f"   ⚠️  Stream {stream_name} does not exist or has no groups")
                            else:
                                logger.warning(f"   ⚠️  Error getting consumer groups: {e}")
                                
                    except Exception as e:
                        error_str = str(e).lower()
                        if "no such key" in error_str:
                            logger.warning(f"⚠️  Stream '{stream_name}' does not exist")
                        else:
                            logger.error(f"❌ Error monitoring stream '{stream_name}': {e}")
                
                # Summary
                logger.info("\n" + "-"*80)
                logger.info("📊 Summary:")
                logger.info(f"   Streams found: {streams_found}/{len(streams_to_monitor)}")
                logger.info(f"   Total pending messages: {total_pending:,}")
                logger.info(f"   Total active consumers: {total_consumers}")
                
                if total_pending > 5000:
                    logger.warning("⚠️  WARNING: High total pending messages across all streams!")
                elif total_pending > 1000:
                    logger.warning("⚠️  CAUTION: Moderate pending messages detected")
                else:
                    logger.info("✅ All streams appear healthy")
                
                consecutive_errors = 0  # Reset on successful check
                
            except redis.ConnectionError as conn_err:
                logger.error(f"❌ Redis connection error: {conn_err}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("❌ Too many consecutive errors, stopping monitor")
                    break
                time.sleep(30)
                continue
            except Exception as e:
                logger.error(f"❌ Stream monitoring error: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("❌ Too many consecutive errors, stopping monitor")
                    break
                time.sleep(30)
                continue
            
            # Wait before next check
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Stream monitor stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error in stream monitor: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor Redis stream health')
    parser.add_argument(
        '--streams',
        nargs='+',
        default=None,
        help='Stream names to monitor (default: ticks:raw:binary, ticks:intraday:processed, alerts:stream)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=60,
        help='Check interval in seconds (default: 60)'
    )
    
    args = parser.parse_args()
    
    monitor_stream_health(
        streams_to_monitor=args.streams,
        check_interval=args.interval
    )

