"""
Redis Streams Setup Script
Run once at startup to create streams and consumer groups for the scanner.
Also optimizes stream lengths for performance.
"""

import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from redis_files.redis_manager import RedisManager82

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_streams():
    """
    Create Redis streams and consumer groups for scanner.
    Streams are in DB 1 (realtime database).
    """
    try:
        # ✅ SOLUTION 4: Use RedisManager82 with PROCESS_POOL_CONFIG
        redis_client = RedisManager82.get_client(
            process_name="stream_setup",
            db=1,  # DB 1 for realtime streams
            max_connections=None  # Use PROCESS_POOL_CONFIG (default: 10)
        )
        
        if not redis_client:
            logger.error("❌ Failed to get Redis client")
            return False
        
        # Test connection
        redis_client.ping()
        logger.info("✅ Connected to Redis DB 1")
        
        # Streams to create
        streams_config = [
            {
                'stream': 'ticks:raw:binary',
                'groups': ['scanner_group', 'data_pipeline_group'],
                'description': 'Raw binary tick data from crawler'
            },
            {
                'stream': 'ticks:intraday:processed',
                'groups': ['scanner_group', 'data_pipeline_group'],
                'description': 'Processed intraday tick data'
            },
            {
                'stream': 'alerts:stream',
                'groups': ['scanner_group', 'alert_validator_group'],
                'description': 'Alert stream for scanner processing and validator'
            }
        ]
        
        created_count = 0
        existing_count = 0
        
        for config in streams_config:
            stream_name = config['stream']
            group_names = config.get('groups', [config.get('group')])  # Support both old and new format
            
            # Create all consumer groups for this stream
            for group_name in group_names:
                try:
                    # Create consumer group (creates stream if it doesn't exist)
                    # '$' means start from new messages only
                    redis_client.xgroup_create(
                        name=stream_name,
                        groupname=group_name,
                        id='$',  # Start from new messages
                        mkstream=True  # Create stream if it doesn't exist
                    )
                    logger.info(f"✅ Created stream '{stream_name}' and consumer group '{group_name}'")
                    created_count += 1
                    
                except Exception as e:
                    error_msg = str(e).lower()
                    if "busygroup" in error_msg or "already exists" in error_msg:
                        logger.info(f"ℹ️  Consumer group '{group_name}' already exists for '{stream_name}'")
                        existing_count += 1
                    elif "no such key" in error_msg:
                        # Stream doesn't exist, try creating it first
                        try:
                            # Create stream by adding a dummy message then deleting it
                            dummy_id = redis_client.xadd(stream_name, {'setup': 'dummy'}, maxlen=1)
                            redis_client.xdel(stream_name, dummy_id)
                            # Now create the group
                            redis_client.xgroup_create(
                                name=stream_name,
                                groupname=group_name,
                                id='0',  # Start from beginning since stream exists
                                mkstream=False
                            )
                            logger.info(f"✅ Created consumer group '{group_name}' for existing stream '{stream_name}'")
                            created_count += 1
                        except Exception as e2:
                            logger.warning(f"⚠️  Failed to create group '{group_name}' for '{stream_name}': {e2}")
                    else:
                        logger.warning(f"⚠️  Error setting up '{stream_name}' group '{group_name}': {e}")
        
        logger.info(f"\n📊 Summary:")
        logger.info(f"   Created: {created_count} streams/groups")
        logger.info(f"   Already exists: {existing_count} groups")
        logger.info(f"✅ Stream setup complete")
        
        # ✅ OPTIMIZATION 2: Optimize stream lengths after setup
        try:
            from utils.redis_stream_optimizer import optimize_redis_streams
            logger.info("\n🔧 Optimizing Redis stream lengths...")
            results = optimize_redis_streams(db=1)
            
            for stream, success in results.items():
                if success:
                    logger.info(f"   ✅ {stream}: optimized")
                else:
                    logger.warning(f"   ⚠️  {stream}: optimization failed")
        except Exception as opt_error:
            logger.warning(f"⚠️  Stream optimization skipped: {opt_error}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to setup streams: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = setup_streams()
    sys.exit(0 if success else 1)

