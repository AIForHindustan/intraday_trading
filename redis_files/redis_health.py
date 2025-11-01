#!/usr/bin/env python3
"""
Redis Health and Bucket Checks (Enhanced with modern client support)

Usage:
  .venv/bin/python redis_files/redis_health.py --host localhost --port 6379
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import os
from pathlib import Path
from typing import Any

# Ensure os is available
import os as _os

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def _fmt_bytes(n: int) -> str:
    """Format bytes to human-readable string"""
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.getenv("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    args = parser.parse_args()

    print("== Redis Health ==")
    print(f"Target: {args.host}:{args.port} db={os.getenv('REDIS_DB_DEFAULT','0')}")
    
    # Try modern health utilities first
    try:
        from redis_files.redis_client import (
            ping_modern,
            info_sections_modern,
            role_modern,
            create_consumer_group_if_needed,
            xadd_safe,
            get_redis_client,
        )
        
        ok = ping_modern()
        print(f"PING: {'OK' if ok else 'FAIL'}")
        if not ok:
            return 2

        data = info_sections_modern()
        mem = data.get("memory", {})
        clients = data.get("clients", {})
        keyspace = data.get("keyspace", {})
        server = data.get("server", {})

        print(f"Redis {server.get('redis_version','?')} mode={server.get('redis_mode','?')} proto={server.get('proto','?')}")
        used = int(mem.get("used_memory", 0))
        peak = int(mem.get("used_memory_peak", 0))
        print(f"Memory used={_fmt_bytes(used)} peak={_fmt_bytes(peak)} clients={clients.get('connected_clients','?')}")

        # Key patterns sanity
        r0 = get_redis_client()
        r2 = get_redis_client() if hasattr(get_redis_client(), 'get_client') else r0
        if hasattr(r2, 'get_client'):
            r2 = r2.get_client(2)
        else:
            r2 = r0
        
        # Try to get clients for different DBs
        try:
            if hasattr(r0, 'get_client'):
                r0_db = r0.get_client(0) if hasattr(r0, 'get_client') else r0
                r2_db = r0.get_client(2) if hasattr(r0, 'get_client') else r0
                r4_db = r0.get_client(4) if hasattr(r0, 'get_client') else r0
                r5_db = r0.get_client(5) if hasattr(r0, 'get_client') else r0
            else:
                r0_db = r0
                r2_db = r0
                r4_db = r0
                r5_db = r0
        except:
            r0_db = r0
            r2_db = r0
            r4_db = r0
            r5_db = r0

        counts = {
            "db0:session:*": len(r0_db.keys("session:*")) if hasattr(r0_db, 'keys') else 0,
            "db2:ohlc_latest:*": len(r2_db.keys("ohlc_latest:*")) if hasattr(r2_db, 'keys') else 0,
            "db4:ticks:*": len(r4_db.keys("ticks:*")) if hasattr(r4_db, 'keys') else 0,
            "db5:volume_averages:*": len(r5_db.keys("volume_averages:*")) if hasattr(r5_db, 'keys') else 0,
        }
        print("Keyspace subset:", json.dumps(counts, indent=2))

        # Streams: create + write a tiny test message (auto-trim)
        stream = os.getenv("HEALTH_STREAM", "health:smoke")
        group = os.getenv("HEALTH_GROUP", "ops")
        create_consumer_group_if_needed(stream, group)
        xid = xadd_safe(stream, {"ts": int(time.time()), "msg": "ok"}, maxlen=1000)
        print(f"Stream write ok: {xid}")

        # Role
        print("ROLE:", role_modern())

        print("OK")
        return 0
        
    except ImportError:
        # Fallback to legacy health check
        pass
    except Exception as e:
        print(f"Modern health check failed: {e}, falling back to legacy")
    
    # Legacy fallback
    try:
        # Use consolidated Redis client
        from redis_files.redis_client import get_redis_client
        r = get_redis_client()
    except Exception as e:
        print(f"Consolidated Redis client not available: {e}")
        # Fallback to basic redis client
        try:
            import redis
            r = redis.Redis(host=args.host, port=args.port)
        except Exception as e2:
            print(f"redis-py not available: {e2}")
            return 1
    try:
        pong = r.ping()
        print(f"PING: {'OK' if pong else 'FAIL'}")
    except Exception as e:
        print(f"Cannot connect to Redis at {args.host}:{args.port}: {e}")
        return 2

    try:
        mem = r.info(section="memory")
        ks = r.info(section="keyspace")
        print("\n=== Memory ===")
        print(json.dumps({
            "used_memory_human": mem.get("used_memory_human"),
            "used_memory_peak_human": mem.get("used_memory_peak_human"),
            "maxmemory_human": mem.get("maxmemory_human"),
        }, indent=2))
        print("\n=== Keyspace ===")
        print(json.dumps(ks, indent=2))
    except Exception as e:
        print(f"Failed to fetch info: {e}")

    try:
        bucket_keys = r.keys("bucket:*")
        volume_keys = r.keys("volume:*:buckets:*")
        volume_ratio_keys = r.keys("volume_ratio:*")
        pattern_keys = r.keys("patterns:*")
        alert_keys = r.keys("alerts:*")
        
        print("\n=== Trading System Health ===")
        print(f"📊 OHLC buckets: {len(bucket_keys)}")
        print(f"📈 Volume buckets: {len(volume_keys)}")
        print(f"⚡ Volume ratios: {len(volume_ratio_keys)}")
        print(f"🎯 Pattern data: {len(pattern_keys)}")
        print(f"🚨 Alert data: {len(alert_keys)}")
        
        # Health indicators
        print("\n=== Health Indicators ===")
        
        # Check Redis connections
        try:
            clients_info = r.info("clients")
            connected_clients = clients_info.get("connected_clients", 0)
            print(f"✅ Redis connections: {connected_clients} (stable: {connected_clients > 0})")
        except:
            print("❌ Redis connections: Unable to check")
        
        # Check volume bucket updates (recent activity)
        try:
            recent_buckets = r.keys("bucket:*")
            if recent_buckets:
                # Check TTL of a few buckets to see if they're being updated
                sample_bucket = recent_buckets[0]
                ttl = r.ttl(sample_bucket)
                if ttl > 0:
                    print(f"✅ Volume bucket updates: Consistent (TTL: {ttl}s)")
                else:
                    print("⚠️ Volume bucket updates: Some buckets expired")
            else:
                print("❌ Volume bucket updates: No buckets found")
        except:
            print("❌ Volume bucket updates: Unable to check")
        
        # Check pattern detection latency (approximate)
        try:
            pattern_data = r.keys("patterns:*")
            if pattern_data:
                print("✅ Pattern detection: Active (patterns found)")
            else:
                print("⚠️ Pattern detection: No recent patterns")
        except:
            print("❌ Pattern detection: Unable to check")
        
        # Check alert accuracy (recent alerts)
        try:
            recent_alerts = r.keys("alerts:*")
            if recent_alerts:
                print(f"✅ Alert system: Active ({len(recent_alerts)} alerts)")
            else:
                print("⚠️ Alert system: No recent alerts")
        except:
            print("❌ Alert system: Unable to check")
            
        # Memory efficiency check
        print("\n=== Memory Efficiency ===")
        try:
            # Check if time buckets have TTL (auto-expire)
            if bucket_keys:
                sample_bucket = bucket_keys[0]
                ttl = r.ttl(sample_bucket)
                if ttl > 0:
                    print(f"✅ Time buckets auto-expire: TTL {ttl}s (efficient)")
                else:
                    print("⚠️ Time buckets: Some may not have TTL")
            else:
                print("ℹ️ Time buckets: None found")
        except:
            print("❌ Memory efficiency: Unable to check")
            
    except Exception as e:
        print(f"Failed to scan system keys: {e}")


from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_latest_hash


def check_ohlc_data_health():
    """Monitor OHLC data completeness."""
    try:
        from core.redis_config import ohlc_redis
        client = ohlc_redis.client
    except Exception as exc:
        print(f"Unable to create OHLC Redis client: {exc}")
        return

    try:
        with open(project_root / "nifty_50_official_list.json", "r", encoding="utf-8") as handle:
            data = json.load(handle)
            symbols = data.get("nifty_50_constituents", [])
    except Exception as exc:
        print(f"Failed to load symbol list: {exc}")
        symbols = []

    normalized_symbols = [normalize_symbol(sym) for sym in symbols]
    symbols.append("NIFTY 50")
    normalized_symbols.append(normalize_symbol("NIFTY 50"))

    now_ms = time.time() * 1000
    for raw_symbol, symbol in zip(symbols, normalized_symbols):
        latest_key = ohlc_latest_hash(symbol)
        latest = client.hgetall(latest_key)
        if not latest:
            print(f"Missing latest data for {raw_symbol}")
            continue

        updated_at = latest.get("updated_at")
        if updated_at:
            try:
                age_days = (now_ms - float(updated_at)) / (1000 * 3600 * 24)
                if age_days > 1:
                    print(f"Stale data for {raw_symbol}: {age_days:.1f} days old")
            except (TypeError, ValueError):
                print(f"Invalid updated_at timestamp for {raw_symbol}: {updated_at}")
        else:
            print(f"No updated_at field for {raw_symbol}")


if __name__ == "__main__":
    main()
