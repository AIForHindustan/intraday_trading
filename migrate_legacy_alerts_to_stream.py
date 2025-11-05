#!/usr/bin/env python3
"""
Migration script to populate alerts:stream with legacy alerts from Redis.

This script:
1. Reads alerts from validation metadata (DB 0: alert:metadata:*)
2. Reads alerts from validation results (DB 0: forward_validation:alert:*)
3. Reads alerts from alert hashes if they exist (DB 1: alert:*)
4. Generates alert_id for each alert
5. Enriches with required fields (signal, pattern_label, base_symbol)
6. Publishes to alerts:stream (DB 1)

Usage:
    python migrate_legacy_alerts_to_stream.py
"""

import json
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import re

try:
    from redis_files.redis_manager import RedisManager82
    from redis_files.redis_key_standards import RedisKeyStandards
except ImportError:
    print("❌ Error: Could not import Redis modules")
    sys.exit(1)


def generate_alert_id(symbol: str, timestamp: Optional[int] = None) -> str:
    """Generate alert_id from symbol and timestamp"""
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    elif timestamp < 1e10:  # If in seconds, convert to ms
        timestamp = int(timestamp * 1000)
    return f"{symbol}_{timestamp}"


def extract_base_symbol(symbol: str) -> str:
    """Extract base symbol from full symbol (e.g., 'NIFTY25DEC26000CE' -> 'NIFTY')"""
    if not symbol:
        return 'UNKNOWN'
    # Try regex match for base symbol
    base_match = re.match(r'^([A-Z]+)', symbol)
    if base_match:
        return base_match.group(1)
    # Fallback: split by colon if present
    if ':' in symbol:
        return symbol.split(':')[-1].split('_')[0]
    return symbol.split('_')[0] if '_' in symbol else symbol


def extract_symbol_from_alert_id(alert_id: str) -> str:
    """Extract symbol from alert_id (e.g., 'BANKNIFTY25NOV59300PE_kow_signal_straddle_1762160328' -> 'BANKNIFTY25NOV59300PE')"""
    if not alert_id:
        return 'UNKNOWN'
    # Alert ID format: {symbol}_{pattern}_{timestamp}
    parts = alert_id.split('_')
    if len(parts) >= 3:
        # Symbol is everything before the last 2 parts (pattern and timestamp)
        symbol = '_'.join(parts[:-2])
        return symbol
    return alert_id.split('_')[0]


def enrich_alert(alert: Dict) -> Dict:
    """Enrich alert with required fields for frontend"""
    # Ensure alert_id
    if 'alert_id' not in alert or not alert.get('alert_id'):
        timestamp = alert.get('timestamp_ms') or alert.get('timestamp')
        symbol = alert.get('symbol', 'UNKNOWN')
        alert['alert_id'] = generate_alert_id(symbol, timestamp)
    
    # Extract symbol from alert_id if missing
    if 'symbol' not in alert or not alert.get('symbol'):
        alert['symbol'] = extract_symbol_from_alert_id(alert.get('alert_id', ''))
    
    # Ensure signal
    if 'signal' not in alert:
        alert['signal'] = alert.get('direction', alert.get('action', 'NEUTRAL'))
    
    # Extract pattern from alert_id if missing
    if 'pattern_label' not in alert or alert.get('pattern_label') == 'Unknown Pattern':
        pattern = alert.get('pattern', alert.get('pattern_type', ''))
        if not pattern and alert.get('alert_id'):
            # Try to extract from alert_id (format: {symbol}_{pattern}_{timestamp})
            parts = alert.get('alert_id', '').split('_')
            if len(parts) >= 3:
                pattern = '_'.join(parts[-2:-1])  # Second-to-last part
        alert['pattern_label'] = pattern or 'Unknown Pattern'
    
    # Ensure base_symbol
    if 'base_symbol' not in alert or alert.get('base_symbol') == 'UNKNOWN':
        symbol = alert.get('symbol', '')
        if symbol:
            alert['base_symbol'] = extract_base_symbol(symbol)
        else:
            alert['base_symbol'] = extract_base_symbol(extract_symbol_from_alert_id(alert.get('alert_id', '')))
    
    # Ensure pattern field (for compatibility)
    if 'pattern' not in alert:
        alert['pattern'] = alert.get('pattern_label', 'Unknown Pattern')
    
    # Ensure confidence
    if 'confidence' not in alert:
        alert['confidence'] = alert.get('confidence_score', 0.0)
    
    # Ensure last_price
    if 'last_price' not in alert:
        alert['last_price'] = alert.get('current_price', alert.get('price', 0.0))
    
    # Normalize timestamp
    if 'timestamp' in alert and isinstance(alert['timestamp'], str):
        try:
            dt = datetime.fromisoformat(alert['timestamp'].replace('Z', '+00:00'))
            alert['timestamp'] = int(dt.timestamp() * 1000)
        except:
            pass
    
    if 'timestamp_ms' in alert and 'timestamp' not in alert:
        alert['timestamp'] = alert['timestamp_ms']
    
    # Ensure timestamp is in milliseconds
    if 'timestamp' in alert and alert['timestamp'] < 1e10:
        alert['timestamp'] = int(alert['timestamp'] * 1000)
    
    # If timestamp is still missing, try to extract from alert_id
    if 'timestamp' not in alert or not alert.get('timestamp'):
        alert_id = alert.get('alert_id', '')
        if alert_id:
            parts = alert_id.split('_')
            if len(parts) >= 1:
                try:
                    # Last part might be timestamp
                    ts = int(parts[-1])
                    if ts < 1e10:  # If in seconds, convert to ms
                        ts = ts * 1000
                    alert['timestamp'] = ts
                except:
                    pass
    
    return alert


def read_validation_metadata_alerts(db0_client) -> List[Dict]:
    """Read alerts from alert:metadata:* keys (DB 0)"""
    alerts = []
    try:
        # Use SCAN for migration (one-time admin operation)
        cursor = 0
        pattern = "alert:metadata:*"
        while True:
            cursor, keys = db0_client.scan(cursor, match=pattern, count=100)
            for key in keys:
                try:
                    data = db0_client.hgetall(key)
                    if data:
                        alert = {}
                        for k, v in data.items():
                            key_str = k.decode() if isinstance(k, bytes) else k
                            val_str = v.decode() if isinstance(v, bytes) else v
                            try:
                                alert[key_str] = json.loads(val_str)
                            except:
                                alert[key_str] = val_str
                        
                        # Extract alert_id from key (alert:metadata:{alert_id})
                        if 'alert_id' not in alert:
                            alert_id = key.decode().split(':')[-1] if isinstance(key, bytes) else key.split(':')[-1]
                            alert['alert_id'] = alert_id
                        
                        alerts.append(alert)
                except Exception as e:
                    print(f"⚠️  Error reading {key}: {e}")
                    continue
            
            if cursor == 0:
                break
    except Exception as e:
        print(f"⚠️  Error scanning validation metadata: {e}")
    
    return alerts


def read_validation_results_alerts(db0_client) -> List[Dict]:
    """Read alerts from forward_validation:alert:* keys (DB 0)"""
    alerts = []
    try:
        # Use SCAN for migration
        cursor = 0
        pattern = "forward_validation:alert:*"
        while True:
            cursor, keys = db0_client.scan(cursor, match=pattern, count=100)
            for key in keys:
                try:
                    data = db0_client.get(key)
                    if data:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        alert = json.loads(data) if isinstance(data, str) else data
                        
                        # Extract alert_id from key
                        if 'alert_id' not in alert:
                            alert_id = key.decode().split(':')[-1] if isinstance(key, bytes) else key.split(':')[-1]
                            alert['alert_id'] = alert_id
                        
                        alerts.append(alert)
                except Exception as e:
                    print(f"⚠️  Error reading {key}: {e}")
                    continue
            
            if cursor == 0:
                break
    except Exception as e:
        print(f"⚠️  Error scanning validation results: {e}")
    
    return alerts


def read_alert_hashes(db1_client) -> List[Dict]:
    """Read alerts from alert:* keys (DB 1)"""
    alerts = []
    try:
        # Use SCAN for migration
        cursor = 0
        pattern = "alert:*"
        while True:
            cursor, keys = db1_client.scan(cursor, match=pattern, count=100)
            for key in keys:
                # Skip if it's a metadata key
                if b'alert:metadata:' in key if isinstance(key, bytes) else 'alert:metadata:' in key:
                    continue
                
                try:
                    data = db1_client.get(key)
                    if data:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        alert = json.loads(data) if isinstance(data, str) else data
                        
                        # Extract alert_id from key
                        if 'alert_id' not in alert:
                            alert_id = key.decode().split(':')[-1] if isinstance(key, bytes) else key.split(':')[-1]
                            alert['alert_id'] = alert_id
                        
                        alerts.append(alert)
                except Exception as e:
                    print(f"⚠️  Error reading {key}: {e}")
                    continue
            
            if cursor == 0:
                break
    except Exception as e:
        print(f"⚠️  Error scanning alert hashes: {e}")
    
    return alerts


def read_validation_stream_alerts(db0_client) -> List[Dict]:
    """Read alerts from alerts:validation:results stream (DB 0)"""
    alerts = []
    try:
        # Read from validation stream
        stream_name = "alerts:validation:results"
        messages = db0_client.xrevrange(stream_name, count=1000)
        
        for msg_id, msg_data in messages:
            try:
                alert = {}
                for k, v in msg_data.items():
                    key_str = k.decode() if isinstance(k, bytes) else k
                    val_str = v.decode() if isinstance(v, bytes) else v
                    try:
                        alert[key_str] = json.loads(val_str)
                    except:
                        alert[key_str] = val_str
                
                if alert.get('alert_id'):
                    alerts.append(alert)
            except Exception as e:
                print(f"⚠️  Error reading stream message {msg_id}: {e}")
                continue
    except Exception as e:
        print(f"⚠️  Error reading validation stream: {e}")
    
    return alerts


def get_existing_alert_ids(db1_client) -> Set[str]:
    """Get set of alert_ids already in alerts:stream to avoid duplicates"""
    existing_ids = set()
    try:
        stream_name = "alerts:stream"
        messages = db1_client.xrevrange(stream_name, count=10000)
        
        for msg_id, msg_data in messages:
            try:
                data_field = msg_data.get('data') or msg_data.get(b'data')
                if data_field:
                    if isinstance(data_field, bytes):
                        try:
                            import orjson
                            alert = orjson.loads(data_field)
                        except:
                            alert = json.loads(data_field.decode('utf-8'))
                    else:
                        alert = json.loads(data_field) if isinstance(data_field, str) else data_field
                    
                    if 'alert_id' in alert:
                        existing_ids.add(alert['alert_id'])
            except:
                continue
    except Exception as e:
        print(f"⚠️  Error reading existing stream: {e}")
    
    return existing_ids


def publish_alert_to_stream(db1_client, alert: Dict) -> bool:
    """Publish enriched alert to alerts:stream"""
    try:
        # Serialize to binary using orjson if available
        try:
            import orjson
            binary_data = orjson.dumps(alert)
        except ImportError:
            binary_data = json.dumps(alert).encode('utf-8')
        
        # Publish to stream
        db1_client.xadd(
            'alerts:stream',
            {'data': binary_data},
            maxlen=1000,
            approximate=True
        )
        return True
    except Exception as e:
        print(f"⚠️  Error publishing alert {alert.get('alert_id')}: {e}")
        return False


def main():
    print("🔄 Starting migration of legacy alerts to alerts:stream...")
    print("")
    
    # Get Redis clients
    db0_client = RedisManager82.get_client(process_name="migration", db=0, decode_responses=False)
    db1_client = RedisManager82.get_client(process_name="migration", db=1, decode_responses=False)
    
    # Get existing alert IDs to avoid duplicates
    print("📊 Checking existing alerts in stream...")
    existing_ids = get_existing_alert_ids(db1_client)
    print(f"   Found {len(existing_ids)} existing alerts in stream")
    print("")
    
    all_alerts = []
    
    # Read from various sources
    print("📖 Reading alerts from validation metadata (DB 0: alert:metadata:*)...")
    metadata_alerts = read_validation_metadata_alerts(db0_client)
    print(f"   Found {len(metadata_alerts)} alerts in metadata")
    all_alerts.extend(metadata_alerts)
    
    print("📖 Reading alerts from validation results (DB 0: forward_validation:alert:*)...")
    validation_alerts = read_validation_results_alerts(db0_client)
    print(f"   Found {len(validation_alerts)} alerts in validation results")
    all_alerts.extend(validation_alerts)
    
    print("📖 Reading alerts from alert hashes (DB 1: alert:*)...")
    hash_alerts = read_alert_hashes(db1_client)
    print(f"   Found {len(hash_alerts)} alerts in hashes")
    all_alerts.extend(hash_alerts)
    
    print("📖 Reading alerts from validation stream (DB 0: alerts:validation:results)...")
    stream_alerts = read_validation_stream_alerts(db0_client)
    print(f"   Found {len(stream_alerts)} alerts in validation stream")
    all_alerts.extend(stream_alerts)
    
    print("")
    print(f"📦 Total alerts collected: {len(all_alerts)}")
    
    # Deduplicate by alert_id
    alerts_by_id = {}
    for alert in all_alerts:
        alert = enrich_alert(alert)
        alert_id = alert.get('alert_id')
        if alert_id:
            # Keep the most complete version
            if alert_id not in alerts_by_id or len(str(alert)) > len(str(alerts_by_id[alert_id])):
                alerts_by_id[alert_id] = alert
    
    print(f"📦 Unique alerts after deduplication: {len(alerts_by_id)}")
    print("")
    
    # Filter out alerts that already exist in stream
    new_alerts = [alert for alert in alerts_by_id.values() if alert.get('alert_id') not in existing_ids]
    print(f"📦 New alerts to migrate: {len(new_alerts)}")
    print(f"⏭️  Skipping {len(existing_ids)} existing alerts")
    print("")
    
    if not new_alerts:
        print("✅ No new alerts to migrate!")
        return
    
    # Publish alerts to stream
    print("📤 Publishing alerts to alerts:stream...")
    published = 0
    failed = 0
    
    for i, alert in enumerate(new_alerts, 1):
        alert = enrich_alert(alert)
        if publish_alert_to_stream(db1_client, alert):
            published += 1
            if i % 10 == 0:
                print(f"   Published {published}/{len(new_alerts)} alerts...")
        else:
            failed += 1
    
    print("")
    print("✅ Migration complete!")
    print(f"   Published: {published}")
    print(f"   Failed: {failed}")
    print(f"   Skipped (already exists): {len(existing_ids)}")
    print("")
    print("🎉 Legacy alerts have been migrated to alerts:stream!")


if __name__ == "__main__":
    main()

