#!/usr/bin/env python3
"""
Redis Connection Monitor
========================

Monitors Redis connections to identify connection leaks and connection pool usage.
Helps diagnose "Too many connections" errors.

Usage:
    python scripts/monitor_redis_connections.py
    python scripts/monitor_redis_connections.py --interval 5  # Check every 5 seconds
"""

import sys
import os
import time
import argparse
from datetime import datetime
from collections import defaultdict

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    import redis
    from redis_files.redis_manager import RedisManager82
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def get_redis_info():
    """Get Redis connection statistics"""
    try:
        # ✅ STANDARDIZED: Use RedisManager82 instead of deprecated get_redis_client_from_pool
        client = RedisManager82.get_client(process_name="redis_monitor", db=0)
        info = client.info('clients')
        stats = client.info('stats')
        return {
            'connected_clients': info.get('connected_clients', 0),
            'client_recent_max_input_buffer': info.get('client_recent_max_input_buffer', 0),
            'client_recent_max_output_buffer': info.get('client_recent_max_output_buffer', 0),
            'blocked_clients': info.get('blocked_clients', 0),
            'total_connections_received': stats.get('total_connections_received', 0),
            'rejected_connections': stats.get('rejected_connections', 0),
        }
    except Exception as e:
        print(f"❌ Error getting Redis info: {e}")
        return None


def get_client_list():
    """Get list of connected clients"""
    try:
        # ✅ STANDARDIZED: Use RedisManager82 instead of deprecated get_redis_client_from_pool
        client = RedisManager82.get_client(process_name="redis_monitor", db=0)
        clients = client.execute_command('CLIENT', 'LIST')
        
        if isinstance(clients, bytes):
            clients = clients.decode('utf-8')
        
        client_info = []
        for line in clients.split('\n'):
            if line.strip():
                info = {}
                for part in line.split(' '):
                    if '=' in part:
                        key, value = part.split('=', 1)
                        info[key] = value
                if info:
                    client_info.append(info)
        
        return client_info
    except Exception as e:
        print(f"❌ Error getting client list: {e}")
        return []


def analyze_clients(client_list):
    """Analyze client connections"""
    analysis = {
        'by_db': defaultdict(int),
        'by_name': defaultdict(int),
        'idle_times': [],
        'total': len(client_list),
    }
    
    for client in client_list:
        db = client.get('db', '0')
        name = client.get('name', 'unknown')
        idle = int(client.get('idle', 0))
        
        analysis['by_db'][db] += 1
        analysis['by_name'][name] += 1
        analysis['idle_times'].append(idle)
    
    return analysis


def print_report(redis_info, client_analysis, previous_total=None):
    """Print monitoring report"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"\n{'='*60}")
    print(f"Redis Connection Monitor - {timestamp}")
    print(f"{'='*60}")
    
    if redis_info:
        print(f"\n📊 Connection Statistics:")
        print(f"  Connected Clients:     {redis_info['connected_clients']}")
        print(f"  Blocked Clients:       {redis_info['blocked_clients']}")
        print(f"  Total Connections:     {redis_info['total_connections_received']:,}")
        print(f"  Rejected Connections:  {redis_info['rejected_connections']}")
        
        if previous_total:
            new_connections = redis_info['total_connections_received'] - previous_total
            print(f"  New Connections:       {new_connections} (since last check)")
    
    if client_analysis:
        print(f"\n🔍 Client Analysis:")
        print(f"  Total Active Clients:  {client_analysis['total']}")
        
        if client_analysis['by_db']:
            print(f"\n  By Database:")
            for db, count in sorted(client_analysis['by_db'].items()):
                print(f"    DB {db}: {count} clients")
        
        if client_analysis['by_name']:
            print(f"\n  By Client Name:")
            for name, count in sorted(client_analysis['by_name'].items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"    {name or '(no name)'}: {count} clients")
        
        if client_analysis['idle_times']:
            avg_idle = sum(client_analysis['idle_times']) / len(client_analysis['idle_times'])
            max_idle = max(client_analysis['idle_times'])
            print(f"\n  Idle Time (seconds):")
            print(f"    Average: {avg_idle:.1f}s")
            print(f"    Maximum: {max_idle:.1f}s")
    
    # Warnings
    if redis_info:
        if redis_info['connected_clients'] > 40:
            print(f"\n⚠️  WARNING: High number of connected clients ({redis_info['connected_clients']})")
            print(f"   Consider checking for connection leaks!")
        
        if redis_info['rejected_connections'] > 0:
            print(f"\n❌ ERROR: {redis_info['rejected_connections']} connections were rejected!")
            print(f"   Redis maxclients limit may be too low.")


def main():
    parser = argparse.ArgumentParser(description='Monitor Redis connections')
    parser.add_argument('--interval', type=int, default=10, help='Check interval in seconds (default: 10)')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    parser.add_argument('--clients', action='store_true', help='Show detailed client list')
    
    args = parser.parse_args()
    
    previous_total = None
    
    try:
        while True:
            redis_info = get_redis_info()
            client_list = get_client_list()
            client_analysis = analyze_clients(client_list) if client_list else None
            
            print_report(redis_info, client_analysis, previous_total)
            
            if args.clients and client_list:
                print(f"\n📋 Client List (showing first 20):")
                for i, client in enumerate(client_list[:20], 1):
                    print(f"  {i}. DB={client.get('db', '?')} "
                          f"name={client.get('name', '?')[:30]} "
                          f"idle={client.get('idle', '?')}s")
            
            if args.once:
                break
            
            previous_total = redis_info['total_connections_received'] if redis_info else None
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n\n✅ Monitoring stopped by user")


if __name__ == "__main__":
    main()

