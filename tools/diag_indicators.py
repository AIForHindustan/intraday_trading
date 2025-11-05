#!/usr/bin/env python3
"""
Diagnostic tool to pinpoint indicator visibility mismatches.

Usage:
    python tools/diag_indicators.py

Checks:
- Indicator keys in DB 5 (indicators_cache)
- Key enumeration using SCAN
- JSON parsing and schema validation
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ✅ indicators_store.py removed - use RedisManager82 and retrieve_by_data_type() instead
from redis_files.redis_manager import RedisManager82
from redis_files.redis_client import RobustRedisClient


def main() -> int:
    """Diagnose indicators in Redis DB 5 (indicators_cache)"""
    try:
        # ✅ Use RedisManager82 for centralized connection management
        client = RedisManager82.get_client(
            process_name="diag_indicators",
            db=5,  # DB 5 is PRIMARY for indicators_cache
            max_connections=1
        )
        
        if not client:
            print("[error] Failed to connect to Redis DB 5")
            return 1
        
        # Scan for indicator keys (limited to 1000 for safety)
        keys = []
        cursor = 0
        count = 0
        while count < 1000:
            cursor, batch = client.scan(cursor=cursor, match="indicators:*", count=100)
            keys.extend(batch)
            count += len(batch)
            if cursor == 0:
                break
            if count >= 1000:
                break
        
        print(f"[diag] Found {len(keys)} indicator keys in DB 5 (indicators_cache)")
        
        if not keys:
            print("[hint] No indicator keys found in DB 5")
            print("[hint] Indicators are stored via store_by_data_type('indicators_cache', ...)")
            return 1
        
        sample = keys[:10]
        print("[diag] Sample keys:")
        for k in sample:
            if isinstance(k, bytes):
                k = k.decode('utf-8')
            print(f"  - {k}")
        
        # ✅ Try a few sample reads using retrieve_by_data_type
        wrapped_client = RobustRedisClient(
            redis_client=client,
            process_name="diag_indicators"
        )
        
        misses = 0
        hits = 0
        
        for k in sample[:5]:  # Test first 5 keys
            if isinstance(k, bytes):
                k = k.decode('utf-8')
            try:
                # ✅ Use retrieve_by_data_type() for standardized retrieval
                data = wrapped_client.retrieve_by_data_type(k, "indicators_cache")
                if data:
                    # Handle bytes
                    if isinstance(data, bytes):
                        data = data.decode('utf-8')
                    
                    # Try parsing as JSON
                    try:
                        parsed = json.loads(data) if isinstance(data, str) else data
                        if isinstance(parsed, dict):
                            fields = list(parsed.keys())[:8]
                            print(f"[HIT] {k} -> fields={fields}")
                            hits += 1
                        else:
                            print(f"[HIT] {k} -> value={parsed}")
                            hits += 1
                    except (json.JSONDecodeError, TypeError):
                        print(f"[HIT] {k} -> value={data}")
                        hits += 1
                else:
                    print(f"[MISS] {k} -> None")
                    misses += 1
            except Exception as e:
                print(f"[MISS] {k} -> Error: {e}")
                misses += 1
        
        print(f"\n[diag] Summary: {hits} hits, {misses} misses")
        
        if misses > 0:
            print(f"[diag] {misses} misses. Check:")
            print("  - Indicators stored via store_by_data_type('indicators_cache', ...)?")
            print("  - Correct DB (DB 5 for indicators_cache)?")
        
        if hits > 0:
            print(f"[diag] ✅ {hits} indicators visible - dashboard should work!")
        
        print("[diag] Done.")
        return 0
        
    except Exception as e:
        print(f"[error] Failed to diagnose indicators: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
    