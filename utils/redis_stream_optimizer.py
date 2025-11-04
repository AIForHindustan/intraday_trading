#!/usr/bin/env python3
"""
Redis Stream Optimization
Configures and trims Redis streams for optimal performance
"""

import logging
from typing import Dict, Optional
from redis_files.redis_manager import RedisManager82

logger = logging.getLogger(__name__)

# Stream configurations for real-time processing (keep them lean)
STREAM_CONFIGS = {
    'ticks:raw:binary': 10000,        # Keep last 10k messages
    'ticks:intraday:processed': 5000,  # Keep last 5k messages
    'alerts:stream': 1000,             # Keep last 1k alerts
}

def optimize_redis_streams(db: int = 1, stream_configs: Optional[Dict[str, int]] = None) -> Dict[str, bool]:
    """
    Configure Redis streams for optimal performance by trimming to specified max lengths
    
    Args:
        db: Redis database number (default: 1 for realtime streams)
        stream_configs: Custom stream configurations (default: uses STREAM_CONFIGS)
    
    Returns:
        Dictionary mapping stream names to success status
    """
    if stream_configs is None:
        stream_configs = STREAM_CONFIGS
    
    results = {}
    
    try:
        # Get Redis client for the specified database
        # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
        client = RedisManager82.get_client(process_name="stream_optimizer", db=db)
        
        for stream, maxlen in stream_configs.items():
            try:
                # Check if stream exists
                try:
                    stream_info = client.xinfo_stream(stream)
                    current_length = stream_info.get('length', 0)
                    
                    if current_length > maxlen:
                        # Trim stream to maxlen (iterative for large streams)
                        total_trimmed = 0
                        iterations = 0
                        max_iterations = 10  # Safety limit
                        
                        while current_length > maxlen * 1.1 and iterations < max_iterations:  # Trim until within 10% of target
                            trimmed = client.xtrim(stream, maxlen=maxlen, approximate=True)
                            total_trimmed += trimmed
                            iterations += 1
                            
                            # Check new length
                            try:
                                stream_info = client.xinfo_stream(stream)
                                current_length = stream_info.get('length', 0)
                            except:
                                break
                            
                            # If approximate trimming isn't working well, try exact trimming once
                            if iterations == 3 and current_length > maxlen * 2:
                                try:
                                    # Exact trim (slower but more accurate)
                                    trimmed_exact = client.xtrim(stream, maxlen=maxlen, approximate=False)
                                    total_trimmed += trimmed_exact
                                    stream_info = client.xinfo_stream(stream)
                                    current_length = stream_info.get('length', 0)
                                    logger.debug(f"Applied exact trim to {stream}: removed {trimmed_exact}")
                                except:
                                    pass
                        
                        results[stream] = True
                        final_length = current_length
                        logger.info(
                            f"✅ Trimmed {stream}: {stream_info.get('length', 0)} messages "
                            f"(removed ~{total_trimmed} messages in {iterations} iterations)"
                        )
                    else:
                        results[stream] = True
                        logger.debug(
                            f"ℹ️  {stream} already optimized: {current_length} <= {maxlen} messages"
                        )
                        
                except Exception as e:
                    if "no such key" in str(e).lower() or "not found" in str(e).lower():
                        logger.debug(f"Stream {stream} doesn't exist yet (will be created on first use)")
                        results[stream] = True  # Not an error, just doesn't exist yet
                    else:
                        raise
                        
            except Exception as e:
                logger.warning(f"⚠️  Could not optimize stream {stream}: {e}")
                results[stream] = False
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Error optimizing Redis streams: {e}")
        return {stream: False for stream in stream_configs.keys()}


def configure_stream_maxlen(stream: str, maxlen: int, db: int = 1) -> bool:
    """
    Configure a single stream's max length
    
    Args:
        stream: Stream name
        maxlen: Maximum number of messages to keep
        db: Redis database number
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
        client = RedisManager82.get_client(process_name="stream_optimizer", db=db)
        
        # Check current length
        try:
            stream_info = client.xinfo_stream(stream)
            current_length = stream_info.get('length', 0)
            
            if current_length > maxlen:
                trimmed = client.xtrim(stream, maxlen=maxlen, approximate=True)
                logger.info(
                    f"✅ Configured {stream}: trimmed to {maxlen} messages "
                    f"(removed ~{trimmed} messages)"
                )
            else:
                logger.debug(f"ℹ️  {stream} already at optimal size: {current_length} messages")
            
            return True
            
        except Exception as e:
            if "no such key" in str(e).lower():
                logger.debug(f"Stream {stream} doesn't exist yet")
                return True  # Not an error
            else:
                raise
                
    except Exception as e:
        logger.error(f"❌ Error configuring stream {stream}: {e}")
        return False


def get_stream_info(stream: str, db: int = 1) -> Optional[Dict]:
    """
    Get information about a Redis stream
    
    Args:
        stream: Stream name
        db: Redis database number
    
    Returns:
        Stream info dictionary or None if stream doesn't exist
    """
    try:
        # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
        client = RedisManager82.get_client(process_name="stream_optimizer", db=db)
        stream_info = client.xinfo_stream(stream)
        return {
            'length': stream_info.get('length', 0),
            'first_entry': stream_info.get('first-entry'),
            'last_entry': stream_info.get('last-entry'),
            'groups': len(client.xinfo_groups(stream)) if hasattr(client, 'xinfo_groups') else 0,
        }
    except Exception as e:
        if "no such key" in str(e).lower():
            return None
        logger.error(f"Error getting stream info for {stream}: {e}")
        return None


def optimize_all_streams() -> Dict[str, Dict]:
    """
    Optimize all configured streams and return status report
    
    Returns:
        Dictionary with optimization results for each stream
    """
    results = {}
    
    for stream, maxlen in STREAM_CONFIGS.items():
        stream_info_before = get_stream_info(stream)
        success = configure_stream_maxlen(stream, maxlen)
        stream_info_after = get_stream_info(stream)
        
        results[stream] = {
            'success': success,
            'configured_maxlen': maxlen,
            'before': stream_info_before,
            'after': stream_info_after,
        }
    
    return results


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("🔧 Optimizing Redis streams...")
    results = optimize_all_streams()
    
    print("\n📊 Optimization Results:")
    print("=" * 80)
    for stream, result in results.items():
        status = "✅" if result['success'] else "❌"
        before_len = result['before']['length'] if result['before'] else 0
        after_len = result['after']['length'] if result['after'] else 0
        maxlen = result['configured_maxlen']
        
        print(f"{status} {stream}:")
        print(f"   Before: {before_len} messages")
        print(f"   After: {after_len} messages")
        print(f"   Target: {maxlen} messages")
        print()

