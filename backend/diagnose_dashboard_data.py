#!/usr/bin/env python3
"""
Dashboard Data Pipeline Diagnostic

Checks Redis data availability and backend API connectivity
"""

import json
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from redis_files.redis_client import RedisClientFactory

def diagnose_dashboard_data():
    print("🔍 DASHBOARD DATA PIPELINE DIAGNOSTIC")
    print("=" * 50)
    
    # ✅ Use RedisClientFactory for process-specific connection pools
    # ✅ DB 2 (analytics) is for pattern metrics, not DB 3
    redis_clients = {}
    for db in [0, 1, 2]:  # ✅ Changed DB 3 to DB 2 (analytics)
        try:
            redis_clients[db] = RedisClientFactory.get_trading_client()
            redis_clients[db].ping()
            print(f"✅ Redis DB {db}: Connected")
        except Exception as e:
            print(f"❌ Redis DB {db}: Failed - {e}")
            return
    
    # Check key data availability
    test_keys = {
        1: [
            "ohlc_latest:NIFTY",
            "ohlc_latest:NIFTY 50",
            "ohlc_latest:BANKNIFTY",
            "indicators:NIFTY", 
            "indicators:NIFTY 50",
            "alerts:stream",
            "validation_results:recent"
        ],
        2: [  # ✅ DB 2 (analytics) is for pattern metrics
            "pattern_metrics:NIFTY:bullish_engulfing",
            "pattern_metrics:NIFTY:volume_breakout"
        ]
    }
    
    for db, keys in test_keys.items():
        print(f"\n📊 Checking DB {db}:")
        for key in keys:
            try:
                exists = redis_clients[db].exists(key)
                key_type = redis_clients[db].type(key)
                status = '✅ EXISTS' if exists else '❌ MISSING'
                print(f"   {key}: {status} ({key_type})")
                
                if exists and key_type == 'string':
                    data = redis_clients[db].get(key)
                    if data:
                        try:
                            parsed = json.loads(data) if isinstance(data, str) else data
                            print(f"      Sample: {str(parsed)[:100]}...")
                        except:
                            print(f"      Sample: {str(data)[:100]}...")
                elif exists and key_type == 'list':
                    length = redis_clients[db].llen(key)
                    print(f"      Length: {length}")
                    if length > 0:
                        sample = redis_clients[db].lindex(key, 0)
                        print(f"      First item: {str(sample)[:100]}...")
                elif exists and key_type == 'stream':
                    length = redis_clients[db].xlen(key)
                    print(f"      Stream length: {length}")
                elif exists and key_type == 'hash':
                    size = redis_clients[db].hlen(key)
                    print(f"      Hash size: {size}")
                    if size > 0:
                        sample_key = list(redis_clients[db].hkeys(key))[0]
                        print(f"      Sample key: {sample_key}")
                    
            except Exception as e:
                print(f"   {key}: ❌ ERROR - {e}")
    
    # Check for any OHLC or indicator keys
    print(f"\n📈 Checking for OHLC data (DB 1):")
    try:
        ohlc_keys = redis_clients[1].keys("ohlc_latest:*")
        print(f"   Found {len(ohlc_keys)} ohlc_latest keys")
        if ohlc_keys:
            print(f"   Sample keys: {list(ohlc_keys[:5])}")
    except Exception as e:
        print(f"   Error: {e}")
    
    print(f"\n📊 Checking for indicator data (DB 1):")
    try:
        indicator_keys = redis_clients[1].keys("indicators:*")
        print(f"   Found {len(indicator_keys)} indicator keys")
        if indicator_keys:
            print(f"   Sample keys: {list(indicator_keys[:5])}")
    except Exception as e:
        print(f"   Error: {e}")
    
    print(f"\n🔔 Checking alerts stream (DB 1):")
    try:
        stream_length = redis_clients[1].xlen("alerts:stream")
        print(f"   alerts:stream length: {stream_length}")
        if stream_length > 0:
            # Get last entry
            last_entries = redis_clients[1].xrevrange("alerts:stream", count=1)
            if last_entries:
                print(f"   Last entry ID: {last_entries[0][0]}")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "=" * 50)
    print("✅ Diagnostic complete")

if __name__ == "__main__":
    diagnose_dashboard_data()

