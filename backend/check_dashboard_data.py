# Fixed import

#!/usr/bin/env python3
"""
Check Dashboard Data Availability

Verifies that validation and performance data exists in Redis
and can be accessed by the dashboard.
"""

import redis
import json
import sys

def check_dashboard_data():
    print("🔍 CHECKING DASHBOARD DATA AVAILABILITY")
    print("=" * 60)
    
    # Connect to Redis
    try:
        r_db0 = r_db0 = RedisClientFactory.get_trading_client()
        r_db1 = r_db1 = RedisClientFactory.get_trading_client()
        r_db2 = r_db2 = RedisClientFactory.get_trading_client()
        
        # Test connections
        r_db0.ping()
        r_db1.ping()
        r_db2.ping()
        print("✅ Redis connections: OK")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return
    
    # Check scanner performance stats (DB 0)
    print("\n📊 SCANNER PERFORMANCE (DB 0):")
    stats_key = "scanner:performance:stats"
    stats_data = r_db0.get(stats_key)
    if stats_data:
        try:
            stats = json.loads(stats_data) if isinstance(stats_data, str) else stats_data
            print(f"   ✅ Found scanner stats:")
            print(f"      Total Ticks: {stats.get('total_ticks', 0):,}")
            print(f"      Throughput: {stats.get('throughput_ticks_per_second', 0):.1f} ticks/sec")
            print(f"      Uptime: {stats.get('uptime_seconds', 0):.0f}s")
        except Exception as e:
            print(f"   ⚠️ Error parsing stats: {e}")
    else:
        print(f"   ❌ No scanner stats found (key: {stats_key})")
        print(f"      Dashboard will show: 'Scanner stats will appear after processing 1000+ ticks...'")
    
    # Check validation results (DB 1)
    print("\n✅ VALIDATION RESULTS (DB 1):")
    validation_key = "validation_results:recent"
    validation_count = r_db1.llen(validation_key)
    print(f"   Found {validation_count} validation results")
    
    if validation_count > 0:
        sample = r_db1.lrange(validation_key, 0, 0)[0]
        try:
            sample_data = json.loads(sample) if isinstance(sample, str) else sample
            print(f"   ✅ Sample validation:")
            print(f"      Alert ID: {sample_data.get('alert_id', 'N/A')}")
            print(f"      Symbol: {sample_data.get('symbol', 'N/A')}")
            print(f"      Is Valid: {sample_data.get('validation_result', {}).get('is_valid', 'N/A')}")
            print(f"      Confidence: {sample_data.get('validation_result', {}).get('confidence_score', 'N/A')}")
        except Exception as e:
            print(f"   ⚠️ Error parsing sample: {e}")
    else:
        print(f"   ❌ No validation results found")
        print(f"      Dashboard will show: 'No Validation Data Available'")
    
    # Check pattern metrics (DB 2 - analytics)
    print("\n📈 PATTERN METRICS (DB 2 - analytics):")
    pattern_keys = r_db2.keys("pattern_metrics:*")
    print(f"   Found {len(pattern_keys)} pattern metrics")
    
    if pattern_keys:
        # Show top 5 by Sharpe ratio
        pattern_data = []
        for key in pattern_keys[:20]:  # Check first 20
            try:
                data = r_db2.hgetall(key)
                if data:
                    key_parts = key.split(":")
                    if len(key_parts) >= 3:
                        symbol = key_parts[-2]
                        pattern = key_parts[-1]
                        sharpe = float(data.get('sharpe_ratio', 0) or 0)
                        total_return = float(data.get('total_return', 0) or 0)
                        trades = int(data.get('total_trades', 0) or 0)
                        pattern_data.append({
                            'key': f"{symbol}:{pattern}",
                            'sharpe': sharpe,
                            'return': total_return,
                            'trades': trades
                        })
            except Exception as e:
                print(f"   ⚠️ Error processing {key}: {e}")
                continue
        
        if pattern_data:
            # Sort by Sharpe
            pattern_data.sort(key=lambda x: x['sharpe'], reverse=True)
            print(f"   ✅ Top 5 patterns by Sharpe ratio:")
            for i, p in enumerate(pattern_data[:5], 1):
                print(f"      {i}. {p['key']}: Sharpe={p['sharpe']:.2f}, Return={p['return']:.3f}, Trades={p['trades']}")
        else:
            print(f"   ⚠️ No valid pattern data found in keys")
    else:
        print(f"   ❌ No pattern metrics found")
        print(f"      Dashboard will show: 'No pattern metrics found'")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY:")
    has_scanner = stats_data is not None
    has_validation = validation_count > 0
    has_patterns = len(pattern_keys) > 0
    
    if has_scanner and has_validation and has_patterns:
        print("✅ All data available - Dashboard should display:")
        print("   ✅ Scanner performance stats")
        print("   ✅ Validation results")
        print("   ✅ Pattern Sharpe ratios")
    else:
        print("⚠️ Missing data:")
        if not has_scanner:
            print("   ❌ Scanner stats (run scanner_main.py to generate)")
        if not has_validation:
            print("   ❌ Validation results (run alert_validator to generate)")
        if not has_patterns:
            print("   ❌ Pattern metrics (run test_pattern_metrics.py or wait for live data)")

if __name__ == "__main__":
    check_dashboard_data()

