#!/usr/bin/env python3
"""
Alert Validation Report Generator
=================================

Generates reports on alert validation results from Redis.
✅ Uses streams and direct lookups (no SCAN operations)
"""

import json
import sys
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from redis_files.redis_manager import RedisManager82

class AlertValidationReport:
    def __init__(self):
        # ✅ Use RedisManager82 for centralized connection management
        
        try:
            self.redis_client = RedisManager82.get_client(
                process_name="validation_report",
                db=0,  # DB 0 for validation metadata
                max_connections=1
            )
        except Exception as e:
            print(f"❌ Failed to connect to Redis: {e}")
            self.redis_client = None
    
    def get_forward_validation_schedule(self) -> Dict:
        """Get forward validation schedule from DB 0"""
        try:
            if not self.redis_client:
                return {'type': 'error', 'count': 0, 'items': [], 'error': 'No Redis client'}
            
            # ✅ Use direct lookup for schedule key (if standardized)
            # Otherwise, return empty - schedule should be in stream or known key
            schedule_key = "forward_validation:schedule"
            try:
                schedule_data_raw = self.redis_client.get(schedule_key)
                if schedule_data_raw:
                    if isinstance(schedule_data_raw, bytes):
                        schedule_data_raw = schedule_data_raw.decode('utf-8')
                    schedule_data = json.loads(schedule_data_raw)
                    return schedule_data if schedule_data else {'type': 'none', 'count': 0, 'items': []}
            except:
                pass
            
            return schedule_data if schedule_data else {'type': 'none', 'count': 0, 'items': []}
        except Exception as e:
            print(f"Error getting forward validation schedule: {e}")
            return {'type': 'error', 'count': 0, 'items': [], 'error': str(e)}
    
    def get_validation_by_key(self, pattern: str = 'validation:*') -> List[Dict]:
        """✅ Get validation results from stream (no SCAN) - admin script only"""
        try:
            # ✅ CRITICAL: Use stream instead of SCAN for validation results
            # Validation results are stored in 'alerts:validation:results' stream
            # This is an admin script, but we still avoid SCAN for consistency
            
            # If pattern is 'validation:*', read from stream instead
            if pattern == 'validation:*':
                return self.get_validation_stream(count=1000)
            
            # For specific alert_id, use direct lookup
            if pattern.startswith('forward_validation:alert:'):
                alert_id = pattern.replace('forward_validation:alert:', '')
                from redis_files.redis_key_standards import RedisKeyStandards
                validation_key = RedisKeyStandards.get_validation_key(alert_id)
                try:
                    data = self.redis_client.get(validation_key)
                    if data:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        result = json.loads(data)
                        result['redis_key'] = validation_key
                        return [result]
                except Exception as e:
                    print(f"Error parsing validation {validation_key}: {e}")
                    return []
            
            # Unknown pattern - return empty (no SCAN allowed)
            print(f"⚠️ Unknown validation pattern: {pattern}. Use stream or direct lookup.")
            return []
        except Exception as e:
            print(f"Error getting validation by key: {e}")
            return []
    
    def get_validation_stream(self, count: int = 100) -> List[Dict]:
        """Get validation results from Redis stream"""
        try:
            if not self.redis_client:
                return []
            
            # ✅ Use stream for iteration (no SCAN)
            from redis_files.redis_key_standards import RedisKeyStandards
            stream_name = 'alerts:validation:results'
            messages = self.redis_client.xrevrange(stream_name, count=count)
            
            results = []
            for msg_id, fields in messages:
                try:
                    # Parse stream message
                    validation_data = {}
                    for key, value in fields.items():
                        if isinstance(key, bytes):
                            key = key.decode('utf-8')
                        if isinstance(value, bytes):
                            try:
                                value = json.loads(value.decode('utf-8'))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                value = value.decode('utf-8')
                        validation_data[key] = value
                    
                    validation_data['redis_key'] = f"{stream_name}:{msg_id}"
                    validation_data['message_id'] = msg_id
                    results.append(validation_data)
                except Exception as e:
                    print(f"Error parsing stream message {msg_id}: {e}")
                    continue
            
            return results
        except Exception as e:
            print(f"Error getting validation stream: {e}")
            return []

if __name__ == "__main__":
    report = AlertValidationReport()
    
    print("Forward Validation Schedule:")
    schedule = report.get_forward_validation_schedule()
    print(json.dumps(schedule, indent=2))
    
    print("\nValidation Results (from stream):")
    validations = report.get_validation_stream(count=10)
    print(f"Found {len(validations)} validation results")
    for val in validations[:5]:
        print(f"  - {val.get('alert_id', 'unknown')}: {val.get('status', 'unknown')}")
