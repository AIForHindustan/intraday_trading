#!/usr/bin/env python3
"""
Simple Alert Validation Report
- Find top 20 alerts (90%+ confidence) from logs
- Check price movements from parquet using pyarrow
- 5 windows: 1min, 2min, 5min, 10min, 30min
- Simple success/failure report
"""

import sys
import os
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the working parser from existing script
from scripts.analyze_high_confidence_alerts import HighConfidenceAlertAnalyzer

def find_alerts_in_logs():
    """Use the working parser from HighConfidenceAlertAnalyzer"""
    print("🔍 Searching logs for high-confidence alerts...")
    
    analyzer = HighConfidenceAlertAnalyzer()
    all_alerts = analyzer.parse_alerts_from_logs()
    
    # Filter 90%+ and sort
    high_conf = [a for a in all_alerts if float(a.get('confidence', a.get('confidence_score', 0))) >= 0.90]
    high_conf.sort(key=lambda x: float(x.get('confidence', 0)), reverse=True)
    
    print(f"✅ Found {len(high_conf)} alerts with 90%+ confidence")
    return high_conf[:20]  # Top 20

def get_price_from_parquet(symbol, alert_time, window_minutes):
    """Get price movement from parquet files using pyarrow"""
    try:
        alert_ts_ms = int(alert_time.timestamp() * 1000)
        window_end = alert_time + timedelta(minutes=window_minutes)
        window_end_ts_ms = int(window_end.timestamp() * 1000)
        
        # Extract underlying symbol (NIFTY, BANKNIFTY)
        clean_symbol = symbol.replace('NSE:', '').replace('NFO:', '').strip().upper()
        underlying = None
        if 'NIFTY' in clean_symbol and 'NOV' in clean_symbol:
            underlying = 'NIFTY'
        elif 'BANKNIFTY' in clean_symbol:
            underlying = 'BANKNIFTY'
        elif clean_symbol in ['NIFTY', 'BANKNIFTY']:
            underlying = clean_symbol
        
        if not underlying:
            return None
        
        # Find parquet files from Oct 29 - files are time-ordered: intraday_YYYYMMDD_HHMMSS.parquet
        intraday_dir = Path("crawlers/raw_data/intraday_data")
        all_prices = []
        
        if not intraday_dir.exists():
            return None
        
        # Calculate which files we need based on alert time and window
        alert_date_str = alert_time.strftime('%Y%m%d')
        alert_hour_min = alert_time.strftime('%H%M%S')
        window_end = alert_time + timedelta(minutes=window_minutes)
        window_end_str = window_end.strftime('%H%M%S')
        
        # Find files in the time range (files are named with timestamp)
        # Format: intraday_20251029_HHMMSS.parquet
        parquet_files = []
        for pf in intraday_dir.glob(f"intraday_{alert_date_str}_*.parquet"):
            # Extract timestamp from filename: intraday_20251029_HHMMSS.parquet -> HHMMSS
            file_time_str = pf.name.replace(f'intraday_{alert_date_str}_', '').replace('.parquet', '')
            if alert_hour_min <= file_time_str <= window_end_str:
                parquet_files.append(pf)
        
        # If no exact match, use alert time to find nearest files (within ±2 minutes)
        if not parquet_files:
            all_oct29_files = sorted(intraday_dir.glob(f"intraday_{alert_date_str}_*.parquet"))
            if all_oct29_files:
                # Binary search for closest file
                alert_sec = int(alert_time.strftime('%H%M%S'))
                closest_idx = min(range(len(all_oct29_files)), 
                    key=lambda i: abs(int(all_oct29_files[i].name.split('_')[2].split('.')[0]) - alert_sec))
                # Get files around the alert time (±30 files ≈ ±1 minute at ~1 file/sec)
                start_idx = max(0, closest_idx - 30)
                end_idx = min(len(all_oct29_files), closest_idx + 30 + (window_minutes * 60))
                parquet_files = all_oct29_files[start_idx:end_idx]
        
        for pf in parquet_files:
                try:
                    table = pq.read_table(pf)
                    if table.num_rows == 0:
                        continue
                    
                    df = table.to_pandas()
                    
                    # Find columns
                    symbol_col = next((c for c in df.columns if 'symbol' in c.lower() and 'trading' in c.lower()), None)
                    if not symbol_col:
                        symbol_col = next((c for c in df.columns if 'symbol' in c.lower()), None)
                    
                    ts_col = next((c for c in df.columns if 'exchange_timestamp' in c.lower()), None)
                    if not ts_col:
                        ts_col = next((c for c in df.columns if 'timestamp' in c.lower() and ('epoch' in c.lower() or 'exchange' in c.lower())), None)
                    
                    price_col = next((c for c in df.columns if 'last_price' in c.lower()), None)
                    if not price_col:
                        price_col = next((c for c in df.columns if 'close' in c.lower()), None)
                    
                    if not all([symbol_col, ts_col, price_col]):
                        continue
                    
                    # Filter by underlying symbol and time window
                    symbol_series = df[symbol_col].astype(str).str.upper()
                    symbol_mask = symbol_series.str.contains(underlying, na=False)
                    
                    # Filter by symbol first (faster)
                    filtered = df[symbol_mask]
                    
                    if filtered.empty:
                        continue
                    
                    # Handle timestamp filtering
                    try:
                        # Try exchange_timestamp_epoch first (it's epoch seconds)
                        if 'exchange_timestamp_epoch' in df.columns:
                            ts_col = 'exchange_timestamp_epoch'
                            ts_values = pd.to_numeric(filtered[ts_col], errors='coerce')
                            # Convert alert time to seconds (not ms)
                            alert_ts_sec = int(alert_time.timestamp())
                            window_end_ts_sec = int(window_end.timestamp())
                            time_mask = (ts_values >= alert_ts_sec) & (ts_values <= window_end_ts_sec)
                        else:
                            # Fallback: use exchange_timestamp as datetime string or epoch
                            ts_values = pd.to_numeric(filtered[ts_col], errors='coerce')
                            if ts_values.max() < 1e12:  # Likely seconds
                                alert_ts_sec = int(alert_time.timestamp())
                                window_end_ts_sec = int(window_end.timestamp())
                                time_mask = (ts_values >= alert_ts_sec) & (ts_values <= window_end_ts_sec)
                            else:  # Likely milliseconds
                                time_mask = (ts_values >= alert_ts_ms) & (ts_values <= window_end_ts_ms)
                        
                        filtered = filtered[time_mask] if hasattr(time_mask, '__len__') else filtered
                    except Exception as e:
                        # If timestamp filtering fails, use all data from file (file already time-filtered)
                        pass
                    
                    if filtered.empty:
                        continue
                    
                    # Get price data
                    prices = pd.to_numeric(filtered[price_col], errors='coerce').dropna()
                    if len(prices) > 0:
                        all_prices.extend([float(p) for p in prices if p > 0])
                    
                except Exception as e:
                    continue
        
        if all_prices:
            entry = all_prices[0]
            exit = all_prices[-1]
            return {
                'entry_price': entry,
                'exit_price': exit,
                'high': max(all_prices),
                'low': min(all_prices),
                'price_change_pct': ((exit - entry) / entry) * 100 if entry > 0 else 0
            }
        
        return None
        
    except Exception as e:
        return None

def check_alert_success(alert, price_data):
    """Simple: price moved in expected direction >0.5% = SUCCESS"""
    if not price_data:
        return 'NO_DATA', 0
    
    price_change = price_data['price_change_pct']
    direction = alert.get('direction', 0)
    signal = str(alert.get('signal', '')).upper()
    
    if direction > 0 or 'BUY' in signal or 'LONG' in signal:
        if price_change > 0.5:
            return 'SUCCESS', price_change
        elif price_change < -0.5:
            return 'FAILURE', price_change
    elif direction < 0 or 'SELL' in signal or 'SHORT' in signal:
        if price_change < -0.5:
            return 'SUCCESS', price_change
        elif price_change > 0.5:
            return 'FAILURE', price_change
    
    return 'INCONCLUSIVE', price_change

def main():
    print("="*80)
    print("SIMPLE ALERT VALIDATION REPORT")
    print("="*80)
    
    # 1. Find top 20 alerts
    alerts = find_alerts_in_logs()
    if not alerts:
        print("❌ No alerts found")
        return
    
    print(f"\n📊 Analyzing {len(alerts)} alerts...")
    
    # 2. Windows
    windows = {
        '1min': 1,
        '2min': 2,
        '5min': 5,
        '10min': 10,
        '30min': 30,
    }
    
    # 3. Analyze each alert
    results = []
    pattern_stats = defaultdict(lambda: {'success': 0, 'failure': 0, 'no_data': 0})
    
    for i, alert in enumerate(alerts):
        symbol = alert.get('symbol', 'UNKNOWN')
        pattern = alert.get('pattern', 'unknown')
        confidence = float(alert.get('confidence', 0))
        
        # Parse timestamp
        alert_time_str = alert.get('timestamp', alert.get('published_at', ''))
        try:
            if isinstance(alert_time_str, str):
                alert_time = datetime.fromisoformat(alert_time_str.replace('Z', '+00:00'))
                alert_time = alert_time.astimezone(pytz.timezone("Asia/Kolkata"))
            else:
                alert_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        except:
            alert_time = datetime.now(pytz.timezone("Asia/Kolkata"))
        
        print(f"\n[{i+1}/{len(alerts)}] {symbol} - {pattern} ({confidence*100:.1f}%)")
        
        window_results = {}
        for win_name, win_mins in windows.items():
            price_data = get_price_from_parquet(symbol, alert_time, win_mins)
            outcome, price_change = check_alert_success(alert, price_data)
            window_results[win_name] = {
                'outcome': outcome,
                'price_change': price_change
            }
            
            if outcome == 'SUCCESS':
                pattern_stats[pattern]['success'] += 1
            elif outcome == 'FAILURE':
                pattern_stats[pattern]['failure'] += 1
            else:
                pattern_stats[pattern]['no_data'] += 1
        
        results.append({
            'symbol': symbol,
            'pattern': pattern,
            'confidence': confidence,
            'timestamp': alert_time_str,
            'windows': window_results
        })
    
    # 4. Print report
    print("\n" + "="*80)
    print("VALIDATION REPORT")
    print("="*80)
    
    print("\n📊 PATTERN STATISTICS (by trigger count):")
    pattern_counts = defaultdict(int)
    for alert in alerts:
        pattern_counts[alert.get('pattern', 'unknown')] += 1
    
    for pattern, count in sorted(pattern_counts.items(), key=lambda x: x[1], reverse=True):
        stats = pattern_stats[pattern]
        total = stats['success'] + stats['failure'] + stats['no_data']
        success_rate = (stats['success'] / total * 100) if total > 0 else 0
        print(f"   {pattern}: {count} alerts | Success: {stats['success']}/{total} ({success_rate:.1f}%)")
    
    print("\n📈 ALERT RESULTS:")
    for i, result in enumerate(results, 1):
        print(f"\n#{i} {result['symbol']} - {result['pattern']} ({result['confidence']*100:.1f}%)")
        for win_name, win_data in result['windows'].items():
            outcome = win_data['outcome']
            change = win_data['price_change']
            icon = '✅' if outcome == 'SUCCESS' else ('❌' if outcome == 'FAILURE' else '⚪')
            print(f"   {win_name}: {icon} {outcome} ({change:+.2f}%)")
    
    # Save report
    report_file = f"simple_validation_report_{int(datetime.now().timestamp())}.json"
    with open(report_file, 'w') as f:
        json.dump({
            'alerts': results,
            'pattern_stats': dict(pattern_stats),
            'pattern_counts': dict(pattern_counts)
        }, f, indent=2, default=str)
    
    print(f"\n✅ Report saved: {report_file}")

if __name__ == "__main__":
    main()

