#!/usr/bin/env python3
"""
Retrieve Alert Validation Report from Redis
Based on README.md structure and Redis DB 1 (realtime - consolidated from DB 6 alerts)

✅ Uses RedisManager82 for centralized connection management:
- Connection pooling (process-specific pools)
- Proper DB routing per redis_config.py
- No raw redis.Redis() clients
"""

import json
from datetime import datetime
from typing import Dict, List, Any
from collections import defaultdict

class AlertValidationReport:
    def __init__(self):
        # ✅ Use RedisManager82 for centralized connection management
        from redis_files.redis_manager import RedisManager82
        
        # Connect to Redis DB 1 (realtime database - consolidated from DB 6) for validation results
        self.redis_client = RedisManager82.get_client(
            process_name="alert_validation_report",
            db=1,  # DB 1 for realtime data
            max_connections=1,  # Report script only needs one connection
            decode_responses=True
        )
        
        # Connect to Redis DB 0 for forward validation alerts and schedule
        self.redis_db0 = RedisManager82.get_client(
            process_name="alert_validation_report",
            db=0,  # DB 0 for system/metadata
            max_connections=1,  # Report script only needs one connection
            decode_responses=True
        )
        
        if not self.redis_client or not self.redis_db0:
            raise RuntimeError("Failed to initialize Redis clients via RedisManager82")
        
    def get_validation_results(self) -> List[Dict]:
        """Get recent validation results from Redis"""
        try:
            # Get from validation_results:recent list
            recent_results = self.redis_client.lrange('validation_results:recent', 0, 99)
            validation_data = []
            
            for result_str in recent_results:
                try:
                    result = json.loads(result_str) if isinstance(result_str, str) else result_str
                    validation_data.append(result)
                except json.JSONDecodeError:
                    continue
            
            return validation_data
        except Exception as e:
            print(f"Error getting validation results: {e}")
            return []
    
    def get_forward_validation_alerts(self) -> List[Dict]:
        """Get forward validation alerts from DB 0"""
        try:
            # ✅ Use limited SCAN instead of KEYS to avoid blocking Redis
            def safe_scan_keys(client, pattern, max_keys=1000):
                """Safely scan keys with limit"""
                try:
                    keys = []
                    cursor = 0
                    count = 0
                    while count < max_keys:
                        cursor, batch = client.scan(cursor=cursor, match=pattern, count=100)
                        keys.extend(batch)
                        count += len(batch)
                        if cursor == 0:
                            break
                        if count >= max_keys:
                            break
                    return keys[:max_keys]
                except Exception as e:
                    print(f"⚠️ SCAN failed for pattern {pattern}: {e}")
                    return []
            
            keys = safe_scan_keys(self.redis_db0, 'forward_validation:alert:*', max_keys=1000)
            alerts = []
            
            for key in keys:
                try:
                    data = self.redis_db0.get(key)
                    if data:
                        alert = json.loads(data)
                        alert['redis_key'] = key
                        alerts.append(alert)
                except Exception as e:
                    print(f"Error parsing alert {key}: {e}")
                    continue
            
            return alerts
        except Exception as e:
            print(f"Error getting forward validation alerts: {e}")
            return []
    
    def get_forward_validation_schedule(self) -> Dict:
        """Get forward validation schedule from DB 0"""
        try:
            # Check if schedule is a sorted set or list
            schedule_key = 'forward_validation:schedule'
            schedule_data = {}
            
            # Try as sorted set first
            try:
                members = self.redis_db0.zrange(schedule_key, 0, -1, withscores=True)
                if members:
                    schedule_data['type'] = 'sorted_set'
                    schedule_data['count'] = len(members)
                    schedule_data['items'] = []
                    for member, score in members[:20]:  # Show first 20
                        try:
                            item_data = json.loads(member) if isinstance(member, str) else member
                            schedule_data['items'].append({
                                'data': item_data,
                                'score': score
                            })
                        except:
                            schedule_data['items'].append({
                                'data': member,
                                'score': score
                            })
            except:
                pass
            
            # Try as list
            try:
                items = self.redis_db0.lrange(schedule_key, 0, 19)  # First 20 items
                if items:
                    schedule_data['type'] = 'list'
                    schedule_data['count'] = self.redis_db0.llen(schedule_key)
                    schedule_data['items'] = []
                    for item in items:
                        try:
                            item_data = json.loads(item) if isinstance(item, str) else item
                            schedule_data['items'].append(item_data)
                        except:
                            schedule_data['items'].append(item)
            except:
                pass
            
            return schedule_data if schedule_data else {'type': 'none', 'count': 0, 'items': []}
        except Exception as e:
            print(f"Error getting forward validation schedule: {e}")
            return {'type': 'error', 'count': 0, 'items': [], 'error': str(e)}
    
    def get_validation_by_key(self, pattern: str = 'validation:*') -> List[Dict]:
        """Get validation results by key pattern"""
        try:
            # ✅ Use limited SCAN instead of KEYS to avoid blocking Redis
            def safe_scan_keys(client, pattern, max_keys=1000):
                """Safely scan keys with limit"""
                try:
                    keys = []
                    cursor = 0
                    count = 0
                    while count < max_keys:
                        cursor, batch = client.scan(cursor=cursor, match=pattern, count=100)
                        keys.extend(batch)
                        count += len(batch)
                        if cursor == 0:
                            break
                        if count >= max_keys:
                            break
                    return keys[:max_keys]
                except Exception as e:
                    print(f"⚠️ SCAN failed for pattern {pattern}: {e}")
                    return []
            
            keys = safe_scan_keys(self.redis_client, pattern, max_keys=1000)
            results = []
            
            for key in keys:
                try:
                    data = self.redis_client.get(key)
                    if data:
                        result = json.loads(data)
                        result['redis_key'] = key
                        results.append(result)
                except Exception as e:
                    print(f"Error parsing validation {key}: {e}")
                    continue
            
            return results
        except Exception as e:
            print(f"Error getting validation by key: {e}")
            return []
    
    def get_validation_stream(self, count: int = 100) -> List[Dict]:
        """Get validation results from Redis stream"""
        try:
            stream_name = 'alerts:validation:results'
            messages = self.redis_client.xrevrange(stream_name, count=count)
            
            results = []
            for msg_id, fields in messages:
                result = dict(fields)
                result['stream_id'] = msg_id
                results.append(result)
            
            return results
        except Exception as e:
            print(f"Error getting validation stream: {e}")
            return []
    
    def generate_report(self) -> Dict:
        """Generate comprehensive validation report"""
        print("🔍 RETRIEVING ALERT VALIDATION REPORT FROM REDIS")
        print("=" * 70)
        
        # Get all validation data sources
        recent_results = self.get_validation_results()
        forward_alerts = self.get_forward_validation_alerts()
        validation_keys = self.get_validation_by_key('validation:*')
        stream_results = self.get_validation_stream()
        schedule_info = self.get_forward_validation_schedule()
        
        print(f"\n📊 DATA SUMMARY:")
        print(f"  Recent validation results: {len(recent_results)}")
        print(f"  Forward validation alerts: {len(forward_alerts)}")
        print(f"  Forward validation schedule: {schedule_info.get('count', 0)} items ({schedule_info.get('type', 'none')})")
        print(f"  Validation keys: {len(validation_keys)}")
        print(f"  Stream results: {len(stream_results)}")
        
        # Aggregate statistics
        stats = {
            'total_validations': len(recent_results) + len(validation_keys),
            'total_alerts': len(forward_alerts),
            'valid_validations': 0,
            'invalid_validations': 0,
            'average_confidence': 0.0,
            'pattern_distribution': defaultdict(int),
            'symbol_distribution': defaultdict(int),
            'timestamp': datetime.now().isoformat()
        }
        
        # Process recent results
        all_results = recent_results + validation_keys
        confidence_scores = []
        
        for result in all_results:
            if result.get('is_valid'):
                stats['valid_validations'] += 1
            else:
                stats['invalid_validations'] += 1
            
            confidence = result.get('confidence_score', 0.0)
            if confidence:
                confidence_scores.append(confidence)
            
            pattern = result.get('pattern', result.get('pattern_type', 'unknown'))
            stats['pattern_distribution'][pattern] += 1
            
            symbol = result.get('symbol', 'unknown')
            stats['symbol_distribution'][symbol] += 1
        
        # Calculate average confidence
        if confidence_scores:
            stats['average_confidence'] = sum(confidence_scores) / len(confidence_scores)
        
        # Generate report
        report = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'data_sources': {
                    'recent_results': len(recent_results),
                    'forward_alerts': len(forward_alerts),
                    'forward_schedule': schedule_info,
                    'validation_keys': len(validation_keys),
                    'stream_results': len(stream_results)
                }
            },
            'statistics': stats,
            'recent_validations': recent_results[:20],  # Top 20 recent
            'forward_alerts': forward_alerts[:20],  # Top 20 alerts
            'forward_schedule': schedule_info,  # Schedule details
            'validation_stream': stream_results[:20]  # Top 20 from stream
        }
        
        return report
    
    def print_report(self):
        """Print formatted validation report"""
        report = self.generate_report()
        
        print("\n" + "=" * 70)
        print("📋 ALERT VALIDATION REPORT")
        print("=" * 70)
        
        stats = report['statistics']
        print(f"\n📊 STATISTICS:")
        print(f"  Total Validations: {stats['total_validations']}")
        print(f"  Valid Signals: {stats['valid_validations']}")
        print(f"  Invalid Signals: {stats['invalid_validations']}")
        print(f"  Average Confidence: {stats['average_confidence']:.2%}")
        
        print(f"\n📈 PATTERN DISTRIBUTION:")
        for pattern, count in sorted(stats['pattern_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {pattern}: {count}")
        
        print(f"\n🎯 TOP SYMBOLS:")
        for symbol, count in sorted(stats['symbol_distribution'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {symbol}: {count}")
        
        print(f"\n📝 RECENT VALIDATION RESULTS (Top 5):")
        for i, result in enumerate(report['recent_validations'][:5], 1):
            print(f"\n  {i}. {result.get('symbol', 'N/A')} - {result.get('pattern', 'N/A')}")
            print(f"     Valid: {result.get('is_valid', False)}")
            print(f"     Confidence: {result.get('confidence_score', 0):.2%}")
            print(f"     Timestamp: {result.get('timestamp', result.get('processed_at', 'N/A'))}")
        
        print(f"\n⏰ FORWARD VALIDATION ALERTS (Top 5):")
        forward_alerts = report.get('forward_alerts', [])
        schedule_info = report.get('forward_schedule', {})
        if forward_alerts:
            for i, alert in enumerate(forward_alerts[:5], 1):
                alert_id = alert.get('alert_id', alert.get('redis_key', 'N/A'))
                symbol = alert.get('symbol', alert.get('trading_symbol', 'N/A'))
                status = alert.get('status', alert.get('current_status', 'UNKNOWN'))
                windows = alert.get('windows', alert.get('completed_windows', []))
                print(f"\n  {i}. {symbol} (ID: {alert_id[:8]}...)")
                print(f"     Status: {status}")
                print(f"     Completed windows: {len(windows)}/{len(alert.get('target_windows', []))}")
        elif schedule_info.get('count', 0) > 0:
            print(f"  Schedule contains {schedule_info['count']} pending items")
            for i, item in enumerate(schedule_info.get('items', [])[:5], 1):
                if isinstance(item, dict) and 'score' in item:
                    data = item.get('data', {})
                    score = item.get('score', 0)
                    print(f"  {i}. Scheduled at: {datetime.fromtimestamp(score).isoformat()}")
                    print(f"     Alert: {data.get('alert_id', data.get('symbol', 'N/A'))}")
                else:
                    print(f"  {i}. {item}")
        else:
            print("  No forward validation alerts or schedule items found")
        
        # Save to file
        filename = f"alert_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n💾 Full report saved to: {filename}")
        print("=" * 70)

if __name__ == '__main__':
    reporter = AlertValidationReport()
    reporter.print_report()

