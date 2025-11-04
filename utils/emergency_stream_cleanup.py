#!/usr/bin/env python3
"""
Emergency Stream Cleanup Script
One-time cleanup of overloaded Redis streams to clear backlog.

⚠️ WARNING: This will DELETE and recreate streams. Run only once when needed.
"""

import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from redis_files.redis_manager import RedisManager82

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def emergency_stream_cleanup(db: int = 1):
    """
    One-time cleanup of overloaded streams.
    
    ⚠️ WARNING: This will DELETE and recreate streams. Use only when streams are severely overloaded.
    
    Args:
        db: Redis database number (default: 1 for realtime streams)
    """
    logger.warning("="*70)
    logger.warning("⚠️  EMERGENCY STREAM CLEANUP")
    logger.warning("="*70)
    logger.warning("This will DELETE and recreate overloaded streams!")
    logger.warning("Only run this if streams are severely overloaded (500k+ messages)")
    logger.warning("="*70)
    
    try:
        # Get Redis client with minimal connections
        redis_client = RedisManager82.get_client(
            process_name="cleanup",
            db=db,
            max_connections=1  # Minimal connections for cleanup
        )
        
        if not redis_client:
            logger.error("❌ Failed to get Redis client")
            return False
        
        # Test connection
        redis_client.ping()
        logger.info("✅ Connected to Redis")
        
        # Streams to clean with target sizes
        streams_to_clean = {
            'ticks:raw:binary': 10000,
            'ticks:intraday:processed': 5000
        }
        
        cleanup_results = {}
        
        for stream, target_size in streams_to_clean.items():
            try:
                # Check if stream exists
                if not redis_client.exists(stream):
                    logger.info(f"ℹ️  Stream '{stream}' does not exist, skipping")
                    cleanup_results[stream] = {'status': 'skipped', 'reason': 'not_exists'}
                    continue
                
                # Get current length
                current_length = redis_client.xlen(stream)
                logger.info(f"📊 Stream '{stream}': {current_length:,} messages (target: {target_size:,})")
                
                if current_length > target_size * 2:  # Only clean if way over target
                    # Get consumer groups before deletion
                    try:
                        groups = redis_client.xinfo_groups(stream)
                        group_names = []
                        for group in groups:
                            name = group.get('name', b'')
                            if isinstance(name, bytes):
                                name = name.decode('utf-8')
                            group_names.append(name)
                        logger.info(f"   Consumer groups: {group_names}")
                    except Exception as e:
                        logger.warning(f"   Could not get consumer groups: {e}")
                        group_names = []
                    
                    # Delete the overloaded stream
                    redis_client.delete(stream)
                    logger.warning(f"🗑️  Deleted overloaded stream: {stream} (was {current_length:,} messages)")
                    
                    # Recreate consumer groups
                    for group_name in group_names:
                        try:
                            redis_client.xgroup_create(
                                name=stream,
                                groupname=group_name,
                                id='$',  # Start from new messages
                                mkstream=True
                            )
                            logger.info(f"   ✅ Recreated consumer group '{group_name}'")
                        except Exception as e:
                            error_str = str(e).lower()
                            if "busygroup" in error_str or "already exists" in error_str:
                                logger.debug(f"   Group '{group_name}' already exists (expected)")
                            else:
                                logger.warning(f"   ⚠️  Could not recreate group '{group_name}': {e}")
                    
                    # If no groups existed, create default scanner group
                    if not group_names:
                        try:
                            redis_client.xgroup_create(
                                name=stream,
                                groupname='scanner_group',
                                id='$',
                                mkstream=True
                            )
                            logger.info(f"   ✅ Created default 'scanner_group'")
                        except Exception as e:
                            logger.warning(f"   ⚠️  Could not create default group: {e}")
                    
                    cleanup_results[stream] = {
                        'status': 'cleaned',
                        'previous_length': current_length,
                        'target_size': target_size,
                        'groups_recreated': len(group_names)
                    }
                else:
                    logger.info(f"   ✅ Stream is within acceptable range (not cleaning)")
                    cleanup_results[stream] = {
                        'status': 'skipped',
                        'reason': 'within_range',
                        'current_length': current_length
                    }
                    
            except Exception as e:
                logger.error(f"❌ Failed to cleanup '{stream}': {e}")
                cleanup_results[stream] = {'status': 'error', 'error': str(e)}
        
        # Clean up per-symbol streams (they'll be recreated as needed)
        logger.info("\n🧹 Cleaning up per-symbol streams...")
        try:
            # Get all tick streams
            symbol_streams = []
            for key in redis_client.scan_iter(match='ticks:*'):
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                # Skip main streams
                if key_str not in streams_to_clean.keys():
                    symbol_streams.append(key_str)
            
            deleted_count = 0
            for stream in symbol_streams:
                try:
                    length = redis_client.xlen(stream)
                    redis_client.delete(stream)
                    deleted_count += 1
                    logger.debug(f"   Deleted symbol stream: {stream} (was {length} messages)")
                except Exception as e:
                    logger.warning(f"   ⚠️  Could not delete {stream}: {e}")
            
            logger.info(f"✅ Deleted {deleted_count} per-symbol streams")
            cleanup_results['symbol_streams'] = {'deleted': deleted_count}
            
        except Exception as e:
            logger.error(f"❌ Error cleaning symbol streams: {e}")
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("CLEANUP SUMMARY")
        logger.info("="*70)
        for stream, result in cleanup_results.items():
            if isinstance(result, dict):
                status = result.get('status', 'unknown')
                if status == 'cleaned':
                    logger.info(f"✅ {stream}: Cleaned (was {result.get('previous_length', 0):,} messages)")
                elif status == 'skipped':
                    reason = result.get('reason', 'unknown')
                    logger.info(f"⏭️  {stream}: Skipped ({reason})")
                elif status == 'error':
                    logger.error(f"❌ {stream}: Error - {result.get('error', 'unknown')}")
        
        logger.info("="*70)
        logger.info("✅ Emergency cleanup complete!")
        logger.info("Streams will be recreated with proper maxlen on next XADD")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Emergency cleanup failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    import sys
    
    # Safety check - require confirmation
    if len(sys.argv) > 1 and sys.argv[1] == '--confirm':
        logger.warning("⚠️  Running emergency cleanup with confirmation...")
        success = emergency_stream_cleanup()
        sys.exit(0 if success else 1)
    else:
        print("="*70)
        print("⚠️  EMERGENCY STREAM CLEANUP")
        print("="*70)
        print("This script will DELETE and recreate overloaded Redis streams.")
        print("Only run this if streams are severely overloaded (500k+ messages).")
        print()
        print("To run, execute:")
        print("  python utils/emergency_stream_cleanup.py --confirm")
        print()
        print("Or import and call:")
        print("  from utils.emergency_stream_cleanup import emergency_stream_cleanup")
        print("  emergency_stream_cleanup()")
        print("="*70)
        sys.exit(1)

