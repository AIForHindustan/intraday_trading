#!/usr/bin/env python3
"""
Kow Signal Straddle Strategy Analysis
Analyzes real alerts and tick data to find correlations
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis
import duckdb
import pandas as pd
import numpy as np
from redis_files.redis_config import get_redis_config

def get_kow_signal_alerts(redis_client, limit=50):
    """Extract Kow Signal alerts from Redis"""
    alerts = []
    
    # Check alerts:stream
    try:
        stream_messages = redis_client.xrevrange('alerts:stream', count=1000)
        for msg_id, data in stream_messages:
            if isinstance(data, dict):
                alert_type = data.get('pattern_type', '') or data.get('alert_type', '')
                if 'kow_signal' in str(alert_type).lower():
                    alerts.append({
                        'msg_id': msg_id,
                        'symbol': data.get('symbol', 'UNKNOWN'),
                        'pattern_type': alert_type,
                        'confidence': float(data.get('confidence', 0)),
                        'last_price': float(data.get('last_price', 0)),
                        'volume_ratio': float(data.get('volume_ratio', 0)),
                        'timestamp': data.get('timestamp', ''),
                        'alert_time': data.get('alert_time', ''),
                        'strike_price': data.get('strike_price'),
                        'raw_data': data
                    })
    except Exception as e:
        print(f"Error reading alerts stream: {e}")
    
    return alerts[:limit]

def analyze_price_movement(conn, symbol, alert_time, time_windows=[60, 300, 900]):
    """Analyze price movement after alert in DuckDB"""
    try:
        # Parse alert time
        if isinstance(alert_time, str):
            alert_dt = pd.to_datetime(alert_time)
        else:
            alert_dt = alert_time
        
        # Query price data around alert time
        query = f"""
        SELECT 
            timestamp,
            last_price,
            volume,
            high_price,
            low_price
        FROM tick_data_corrected
        WHERE symbol LIKE '%{symbol.split(":")[-1].split(".")[-1]}%'
            AND timestamp >= '{alert_dt - timedelta(minutes=5)}'
            AND timestamp <= '{alert_dt + timedelta(minutes=30)}'
        ORDER BY timestamp
        """
        
        df = conn.execute(query).df()
        
        if df.empty:
            return None
        
        # Find alert price
        alert_idx = df['timestamp'].searchsorted(alert_dt)
        if alert_idx >= len(df):
            alert_idx = len(df) - 1
        
        alert_price = df.iloc[alert_idx]['last_price']
        
        # Calculate price movements
        movements = {}
        for window in time_windows:
            target_time = alert_dt + timedelta(seconds=window)
            target_idx = df['timestamp'].searchsorted(target_time)
            if target_idx < len(df):
                target_price = df.iloc[target_idx]['last_price']
                pct_change = ((target_price - alert_price) / alert_price) * 100
                movements[f'{window}s'] = {
                    'price': target_price,
                    'pct_change': pct_change,
                    'max_gain': ((df.iloc[alert_idx:target_idx+1]['high_price'].max() - alert_price) / alert_price * 100) if target_idx > alert_idx else 0,
                    'max_loss': ((df.iloc[alert_idx:target_idx+1]['low_price'].min() - alert_price) / alert_price * 100) if target_idx > alert_idx else 0,
                }
        
        return {
            'alert_price': alert_price,
            'alert_time': alert_dt,
            'data_points': len(df),
            'movements': movements
        }
    except Exception as e:
        print(f"Error analyzing {symbol}: {e}")
        return None

def calculate_vwap(conn, symbol, before_minutes=20):
    """Calculate VWAP for a symbol before alert"""
    try:
        query = f"""
        SELECT 
            SUM(last_price * volume) / NULLIF(SUM(volume), 0) as vwap,
            AVG(last_price) as avg_price,
            SUM(volume) as total_volume
        FROM tick_data_corrected
        WHERE symbol LIKE '%{symbol.split(":")[-1].split(".")[-1]}%'
            AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '{before_minutes} minutes'
        """
        
        result = conn.execute(query).fetchone()
        if result and result[0]:
            return {
                'vwap': result[0],
                'avg_price': result[1],
                'total_volume': result[2]
            }
    except Exception as e:
        print(f"Error calculating VWAP for {symbol}: {e}")
    
    return None

def generate_analysis_report(alerts, correlations):
    """Generate educational post content"""
    
    if not alerts:
        return None
    
    # Filter successful alerts (positive price movement)
    successful = [a for a in correlations if a and any(
        m.get('pct_change', 0) > 0 for m in a['movements'].values()
    )]
    
    success_rate = len(successful) / len(correlations) * 100 if correlations else 0
    avg_confidence = np.mean([a['confidence'] for a in alerts if a.get('confidence')])
    
    report = {
        'total_alerts': len(alerts),
        'analyzed': len(correlations),
        'successful': len(successful),
        'success_rate': success_rate,
        'avg_confidence': avg_confidence,
        'alerts': alerts[:5],  # Top 5 for examples
        'correlations': correlations[:5]
    }
    
    return report

def main():
    """Main analysis function"""
    print("Analyzing Kow Signal Straddle Strategy...")
    
    # Connect to Redis
    redis_config = get_redis_config()
    redis_client = redis.Redis(
        host=redis_config['host'],
        port=redis_config['port'],
        db=1,
        decode_responses=True
    )
    
    # Get Kow Signal alerts
    print("Fetching Kow Signal alerts from Redis...")
    alerts = get_kow_signal_alerts(redis_client, limit=20)
    print(f"Found {len(alerts)} Kow Signal alerts")
    
    if not alerts:
        print("No Kow Signal alerts found. Using sample data for educational post.")
        return None
    
    # Connect to DuckDB
    db_path = project_root / "nse_tick_data.duckdb"
    if not db_path.exists():
        print(f"DuckDB database not found at {db_path}")
        return None
    
    conn = duckdb.connect(str(db_path))
    
    # Analyze correlations
    print("Analyzing price movements...")
    correlations = []
    for alert in alerts:
        symbol = alert['symbol']
        alert_time = alert.get('alert_time') or alert.get('timestamp')
        if alert_time:
            analysis = analyze_price_movement(conn, symbol, alert_time)
            if analysis:
                analysis['alert_data'] = alert
                correlations.append(analysis)
    
    conn.close()
    
    # Generate report
    report = generate_analysis_report(alerts, correlations)
    return report

if __name__ == "__main__":
    report = main()
    if report:
        print(f"\nAnalysis Complete:")
        print(f"Total Alerts: {report['total_alerts']}")
        print(f"Success Rate: {report['success_rate']:.1f}%")
        print(f"Avg Confidence: {report['avg_confidence']:.1f}%")
        
        # Save report
        output_file = project_root / "community_bots" / "kow_signal_analysis_report.json"
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"Report saved to {output_file}")
    else:
        print("Could not generate analysis report")

