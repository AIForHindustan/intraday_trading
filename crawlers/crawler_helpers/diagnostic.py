from redis_files.redis_client import RedisManager82
#!/usr/bin/env python3
"""
Comprehensive System Diagnostic Script
Analyzes WebSocket sources, Redis data flow, and consumer architecture
"""
import redis
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any


# Fix for stream reading - add this decoding logic
def decode_stream_data(stream_data):
    """Decode bytes to strings in stream data"""
    decoded = {}
    for key, value in stream_data.items():
        if isinstance(key, bytes):
            key = key.decode('utf-8')
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        decoded[key] = value
    return decoded

def diagnose_system():
    print("🔍 SYSTEM DIAGNOSTIC REPORT")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # Connect to Redis
    try:
        r = r = RedisManager82.get_client(process_name="diagnostic", db=1)
        r.ping()
        print("✅ Redis Connection: SUCCESS (DB 1 - realtime)")
    except Exception as e:
        print(f"❌ Redis Connection: FAILED - {e}")
        return
    
    # Redis Info
    info = r.info()
    print(f"\n📊 Redis Server Info:")
    print(f"  Memory Used: {info.get('used_memory_human', 'unknown')}")
    print(f"  Connected Clients: {info.get('connected_clients', 0)}")
    print(f"  Operations/sec: {info.get('instantaneous_ops_per_sec', 0)}")
    print(f"  Total Keys: {info.get('db1', {}).get('keys', 'unknown')}")
    
    # Key Patterns Analysis
    print(f"\n🔑 Redis Key Patterns (DB 1 - realtime):")
    patterns = {
        '*ws*': 'WebSocket related',
        '*tick*': 'Tick data',
        '*alert*': 'Alert data',
        '*volume*': 'Volume data',
        '*bucket*': 'Volume buckets',
        'ticks:raw:binary': 'Raw binary stream',
        'ticks:intraday:processed': 'Processed intraday stream',
        'market_data.ticks': 'Market data channel (Pub/Sub)',
        'patterns:*': 'Pattern detection',
        'indicators:*': 'Technical indicators'
    }
    
    key_stats = {}
    sample_data = {}
    
    for pattern, description in patterns.items():
        try:
            if pattern.startswith('*') or ':' in pattern:
                # Use KEYS for wildcards, different approach for exact streams
                if '*' in pattern:
                    keys = r.keys(pattern)
                else:
                    # For exact stream names, check if it exists
                    try:
                        stream_info = r.xinfo_stream(pattern)
                        keys = [pattern] if stream_info else []
                    except:
                        keys = r.keys(pattern)
            else:
                keys = r.keys(pattern)
            
            count = len(keys)
            key_stats[pattern] = {'count': count, 'description': description}
            
            # Get sample data
            if keys and count > 0:
                sample_key = keys[0]
                key_type = r.type(sample_key)
                
                if key_type == 'stream':
                    # Get last entry from stream
                    try:
                        entries = r.xrevrange(sample_key, count=1)
                        if entries:
                            sample_data[pattern] = {
                                'type': 'stream',
                                'sample_entry': dict(entries[0][1])
                            }
                    except:
                        pass
                elif key_type == 'list':
                    length = r.llen(sample_key)
                    if length > 0:
                        sample = r.lrange(sample_key, 0, 0)
                        sample_data[pattern] = {
                            'type': 'list',
                            'length': length,
                            'sample': sample[0][:200] if sample else None
                        }
                elif key_type == 'string':
                    value = r.get(sample_key)
                    sample_data[pattern] = {
                        'type': 'string',
                        'sample': value[:200] if value else None
                    }
                elif key_type == 'hash':
                    sample_data[pattern] = {
                        'type': 'hash',
                        'sample_keys': list(r.hkeys(sample_key))[:5]
                    }
                    
        except Exception as e:
            key_stats[pattern] = {'count': 0, 'error': str(e)}
    
    # Print key statistics
    for pattern, stats in key_stats.items():
        print(f"\n  {pattern}:")
        print(f"    Description: {stats.get('description', 'N/A')}")
        print(f"    Count: {stats.get('count', 0)} keys")
        if 'error' in stats:
            print(f"    Error: {stats['error']}")
        if pattern in sample_data:
            sample = sample_data[pattern]
            print(f"    Type: {sample.get('type', 'unknown')}")
            if 'sample' in sample:
                print(f"    Sample: {str(sample['sample'])[:150]}...")
            if 'sample_keys' in sample:
                print(f"    Sample Keys: {sample['sample_keys']}")
    
    # Check Pub/Sub channels (these don't show up in KEYS, need different approach)
    print(f"\n📡 Pub/Sub Channels (checking common channels):")
    common_channels = [
        'market_data.ticks',
        'ticks:equity',
        'ticks:futures',
        'ticks:options',
        'alerts:new',
        'alerts:system',
        'patterns:global'
    ]
    
    # Pub/Sub channels don't persist, so we can't query them directly
    # But we can check if there are subscribers by looking at client info
    print("  Note: Pub/Sub channels are ephemeral - active channels only exist when subscribers are listening")
    
    # Check Streams
    print(f"\n🌊 Redis Streams:")
    stream_patterns = ['ticks:raw:binary', 'ticks:intraday:processed', 'alerts:system', 'patterns:global']
    for stream_name in stream_patterns:
        try:
            stream_info = r.xinfo_stream(stream_name)
            length = stream_info.get('length', 0)
            first_entry = r.xrange(stream_name, count=1)
            last_entry = r.xrevrange(stream_name, count=1)
            
            print(f"\n  {stream_name}:")
            print(f"    Length: {length} entries")
            if last_entry:
                sample = dict(last_entry[0][1])
                print(f"    Latest Entry: {json.dumps(sample, default=str)[:200]}...")
        except redis.ResponseError as e:
            if "no such key" in str(e).lower():
                print(f"\n  {stream_name}: NOT FOUND")
            else:
                print(f"\n  {stream_name}: ERROR - {e}")
        except Exception as e:
            print(f"\n  {stream_name}: ERROR - {e}")
    
    # Check multiple databases
    print(f"\n🗄️  Multi-Database Check (Consolidated Structure):")
    databases = {
        0: 'system',
        1: 'realtime',
        2: 'analytics'
    }
    
    for db_num, db_name in databases.items():
        try:
            db_client = db_client = RedisManager82.get_client(process_name="diagnostic", db=1)
            db_info = db_client.info(f'db{db_num}')
            key_count = db_info.get('keys', 0) if db_info else 0
            # Try to get actual count
            try:
                actual_keys = len(db_client.keys('*'))
                print(f"  DB {db_num} ({db_name}): ~{actual_keys} keys")
            except:
                print(f"  DB {db_num} ({db_name}): {key_count} keys (from info)")
        except Exception as e:
            print(f"  DB {db_num} ({db_name}): ERROR - {e}")
    
    print(f"\n✅ Diagnostic Complete")
    print("=" * 70)

if __name__ == "__main__":
    diagnose_system()

