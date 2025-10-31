#!/usr/bin/env python3
"""
Signal Provider Results Report
Shows unbiased alert performance for public disclosure
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import redis
import warnings
warnings.filterwarnings('ignore')

class SignalProviderReport:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.alerts_data = []
        self.price_data = []
        
    def extract_data(self):
        """Extract alert and price data"""
        print("Extracting alert and price data...")
        
        # Extract alerts
        keys = self.redis_client.keys("forward_validation:alert:*")
        for key in keys:
            try:
                data = json.loads(self.redis_client.get(key))
                alert_info = {
                    'symbol': data.get('symbol'),
                    'pattern': data.get('pattern'),
                    'signal': data.get('signal'),
                    'confidence': data.get('confidence'),
                    'entry_price': data.get('entry_price'),
                    'expected_move': data.get('expected_move'),
                    'timestamp': data.get('timestamp'),
                    'alert_time': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')),
                    'time_of_day': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')).strftime('%H:%M:%S')
                }
                self.alerts_data.append(alert_info)
            except Exception as e:
                print(f"Error loading alert {key}: {e}")
        
        # Extract price data
        symbols = list(set([alert['symbol'] for alert in self.alerts_data]))
        for symbol in symbols:
            try:
                # Try different bucket patterns for different instrument types
                bucket_patterns = [
                    f"bucket_incremental_volume:bucket:{symbol}:2025-10-28:*",  # Regular stocks
                    f"bucket_incremental_volume:bucket_incremental_volume:bucket*:{symbol}:buckets:*"  # NFO instruments
                ]
                
                bucket_keys = []
                for pattern in bucket_patterns:
                    keys = self.redis_client.keys(pattern)
                    bucket_keys.extend(keys)
                
                for key in bucket_keys:
                    try:
                        data = json.loads(self.redis_client.get(key))
                        price_info = {
                            'symbol': data.get('symbol'),
                            'hour': data.get('hour'),
                            'minute_bucket': data.get('minute_bucket'),
                            'open': data.get('open'),
                            'high': data.get('high'),
                            'low': data.get('low'),
                            'close': data.get('close'),
                            'volume': data.get('bucket_incremental_volume'),
                            'timestamp': datetime(2025, 10, 28, data.get('hour'), data.get('minute_bucket'), 0),
                            'time_str': f"{data.get('hour')}:{data.get('minute_bucket'):02d}"
                        }
                        self.price_data.append(price_info)
                    except Exception as e:
                        print(f"Error loading price {key}: {e}")
            except Exception as e:
                print(f"Error processing symbol {symbol}: {e}")
        
        print(f"Loaded {len(self.alerts_data)} alerts and {len(self.price_data)} price records")
        
    def create_signal_provider_report(self):
        """Create the signal provider results report"""
        print("Creating signal provider report...")
        
        # Convert to DataFrames
        df_alerts = pd.DataFrame(self.alerts_data)
        df_prices = pd.DataFrame(self.price_data)
        
        if len(df_alerts) == 0 or len(df_prices) == 0:
            print("No data available")
            return
        
        # Merge alerts with price data
        df_results = self._merge_alerts_with_prices(df_alerts, df_prices)
        
        if len(df_results) == 0:
            print("No matching alert-price pairs found")
            return
        
        # Create individual alert charts
        self._create_individual_alert_charts(df_results)
        
        # Create summary report
        self._create_summary_report(df_results)
        
    def _merge_alerts_with_prices(self, df_alerts, df_prices):
        """Merge alerts with price data to calculate actual performance"""
        merged_data = []
        
        for _, alert in df_alerts.iterrows():
            symbol_prices = df_prices[df_prices['symbol'] == alert['symbol']].copy()
            symbol_prices = symbol_prices.sort_values('timestamp')
            
            # Find price at alert time and forward prices
            alert_time = alert['alert_time']
            alert_price = alert['entry_price']
            
            # Get prices at different forward windows
            performance = {}
            for window_minutes in [1, 5, 15, 30]:
                target_time = alert_time + timedelta(minutes=window_minutes)
                
                # Find closest price after target time
                future_prices = symbol_prices[symbol_prices['timestamp'] >= target_time]
                if len(future_prices) > 0:
                    future_price = future_prices.iloc[0]['close']
                    price_change = future_price - alert_price
                    price_change_pct = (price_change / alert_price) * 100
                    performance[f'price_{window_minutes}m'] = future_price
                    performance[f'change_{window_minutes}m'] = price_change
                    performance[f'change_pct_{window_minutes}m'] = price_change_pct
                else:
                    performance[f'price_{window_minutes}m'] = np.nan
                    performance[f'change_{window_minutes}m'] = np.nan
                    performance[f'change_pct_{window_minutes}m'] = np.nan
            
            # Add to merged data
            merged_alert = alert.to_dict()
            merged_alert.update(performance)
            merged_data.append(merged_alert)
        
        return pd.DataFrame(merged_data)
    
    def _create_individual_alert_charts(self, df_results):
        """Create individual charts for each alert showing actual performance"""
        print("Creating individual alert performance charts...")
        
        # Clean symbol names - group all variants of the same stock
        df_results['clean_symbol'] = df_results['symbol'].str.replace('25NOVFUT', '').str.replace('28OCTFUT', '')
        
        # Create charts for top 20 alerts by confidence
        top_alerts = df_results.nlargest(20, 'confidence')
        
        for idx, alert in top_alerts.iterrows():
            self._create_single_alert_chart(alert)
    
    def _create_single_alert_chart(self, alert):
        """Create a chart for a single alert"""
        symbol = alert['clean_symbol']
        pattern = alert['pattern']
        signal = alert['signal']
        confidence = alert['confidence']
        entry_price = alert['entry_price']
        expected_move = alert['expected_move']
        alert_time = alert['time_of_day']
        
        # Get actual performance
        windows = [1, 5, 15, 30]
        actual_changes = []
        actual_prices = []
        
        for window in windows:
            change_pct = alert[f'change_pct_{window}m']
            price = alert[f'price_{window}m']
            actual_changes.append(change_pct if not pd.isna(change_pct) else 0)
            actual_prices.append(price if not pd.isna(price) else entry_price)
        
        # Create the chart
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Chart 1: Price Movement Over Time
        ax1.plot(windows, actual_prices, 'o-', linewidth=3, markersize=8, color='blue', label='Actual Price')
        ax1.axhline(y=entry_price, color='red', linestyle='--', linewidth=2, label=f'Entry Price: ₹{entry_price:.2f}')
        
        # Add expected move lines
        if expected_move and not pd.isna(expected_move):
            expected_up = entry_price * (1 + expected_move/100)
            expected_down = entry_price * (1 - expected_move/100)
            ax1.axhline(y=expected_up, color='green', linestyle=':', alpha=0.7, label=f'Expected Up: ₹{expected_up:.2f}')
            ax1.axhline(y=expected_down, color='orange', linestyle=':', alpha=0.7, label=f'Expected Down: ₹{expected_down:.2f}')
        
        ax1.set_title(f'{symbol} - {pattern} Alert\nSignal: {signal} | Confidence: {confidence:.1%} | Time: {alert_time}', 
                     fontsize=16, fontweight='bold')
        ax1.set_xlabel('Minutes After Alert', fontsize=12)
        ax1.set_ylabel('Price (₹)', fontsize=12)
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)
        
        # Chart 2: Percentage Change Over Time
        colors = ['green' if x > 0 else 'red' for x in actual_changes]
        bars = ax2.bar(windows, actual_changes, color=colors, alpha=0.7, edgecolor='black')
        
        # Add value labels on bars
        for i, (bar, change) in enumerate(zip(bars, actual_changes)):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + (0.1 if height > 0 else -0.1),
                    f'{change:+.1f}%', ha='center', va='bottom' if height > 0 else 'top', fontweight='bold')
        
        ax2.set_title('Actual Price Movement (%)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Minutes After Alert', fontsize=12)
        ax2.set_ylabel('Price Change (%)', fontsize=12)
        ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        ax2.grid(True, alpha=0.3)
        
        # Add expected move annotation
        if expected_move and not pd.isna(expected_move):
            ax2.axhline(y=expected_move, color='blue', linestyle='--', alpha=0.7, label=f'Expected: {expected_move:+.1f}%')
            ax2.axhline(y=-expected_move, color='blue', linestyle='--', alpha=0.7)
            ax2.legend(fontsize=10)
        
        plt.tight_layout()
        
        # Save chart
        filename = f"alert_{symbol}_{pattern}_{alert_time.replace(':', '')}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_summary_report(self, df_results):
        """Create a summary report of all alerts"""
        print("Creating summary report...")
        
        # Calculate summary statistics
        total_alerts = len(df_results)
        
        # Win rates for different windows
        win_rates = {}
        for window in [1, 5, 15, 30]:
            col = f'change_pct_{window}m'
            positive_moves = df_results[df_results[col] > 0]
            win_rates[f'{window}m'] = len(positive_moves) / total_alerts
        
        # Average returns
        avg_returns = {}
        for window in [1, 5, 15, 30]:
            col = f'change_pct_{window}m'
            avg_returns[f'{window}m'] = df_results[col].mean()
        
        # Create summary chart
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Win Rate Chart
        windows = list(win_rates.keys())
        win_rate_values = list(win_rates.values())
        bars1 = ax1.bar(windows, win_rate_values, color='lightblue', edgecolor='navy')
        ax1.set_title('Win Rate by Time Window\n(Percentage of Profitable Alerts)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Win Rate (%)', fontsize=12)
        ax1.set_ylim(0, 1)
        
        # Add value labels
        for bar, rate in zip(bars1, win_rate_values):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{rate:.1%}', ha='center', va='bottom', fontweight='bold')
        
        # Average Return Chart
        avg_return_values = list(avg_returns.values())
        colors = ['green' if x > 0 else 'red' for x in avg_return_values]
        bars2 = ax2.bar(windows, avg_return_values, color=colors, alpha=0.7, edgecolor='black')
        ax2.set_title('Average Return by Time Window', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Average Return (%)', fontsize=12)
        ax2.axhline(y=0, color='black', linestyle='-', alpha=0.5)
        
        # Add value labels
        for bar, ret in zip(bars2, avg_return_values):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + (0.01 if height > 0 else -0.01),
                    f'{ret:+.2f}%', ha='center', va='bottom' if height > 0 else 'top', fontweight='bold')
        
        # Pattern Performance
        pattern_performance = df_results.groupby('pattern').agg({
            'change_pct_15m': ['mean', 'count']
        }).round(2)
        pattern_performance.columns = ['avg_return', 'count']
        pattern_performance = pattern_performance.sort_values('avg_return', ascending=False)
        
        bars3 = ax3.bar(range(len(pattern_performance)), pattern_performance['avg_return'], 
                       color='skyblue', edgecolor='navy')
        ax3.set_title('Average 15min Return by Pattern Type', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Average Return (%)', fontsize=12)
        ax3.set_xticks(range(len(pattern_performance)))
        ax3.set_xticklabels(pattern_performance.index, rotation=45, ha='right')
        ax3.axhline(y=0, color='red', linestyle='--', alpha=0.7)
        
        # Symbol Performance (top 10)
        symbol_performance = df_results.groupby('clean_symbol').agg({
            'change_pct_15m': ['mean', 'count']
        }).round(2)
        symbol_performance.columns = ['avg_return', 'count']
        symbol_performance = symbol_performance[symbol_performance['count'] >= 2]  # At least 2 alerts
        symbol_performance = symbol_performance.sort_values('avg_return', ascending=False).head(10)
        
        bars4 = ax4.bar(range(len(symbol_performance)), symbol_performance['avg_return'], 
                       color='lightgreen', edgecolor='darkgreen')
        ax4.set_title('Top 10 Symbols by Average 15min Return\n(Minimum 2 alerts)', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Average Return (%)', fontsize=12)
        ax4.set_xticks(range(len(symbol_performance)))
        ax4.set_xticklabels(symbol_performance.index, rotation=45, ha='right')
        ax4.axhline(y=0, color='red', linestyle='--', alpha=0.7)
        
        # Add count annotations
        for i, (bar, count) in enumerate(zip(bars4, symbol_performance['count'])):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + (0.01 if height > 0 else -0.01),
                    f'n={count}', ha='center', va='bottom' if height > 0 else 'top', fontsize=8)
        
        plt.suptitle(f'Signal Provider Results Summary - October 28, 2025\nTotal Alerts: {total_alerts}', 
                    fontsize=18, fontweight='bold')
        plt.tight_layout()
        plt.savefig('signal_provider_summary.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Create text summary
        self._create_text_summary(df_results, total_alerts, win_rates, avg_returns)
    
    def _create_text_summary(self, df_results, total_alerts, win_rates, avg_returns):
        """Create a text summary report"""
        print("Creating text summary...")
        
        summary = []
        summary.append("# Signal Provider Results Report")
        summary.append(f"## Date: October 28, 2025")
        summary.append(f"## Total Alerts Sent: {total_alerts}")
        summary.append("")
        
        summary.append("## Performance Summary")
        summary.append("")
        
        for window in [1, 5, 15, 30]:
            win_rate = win_rates[f'{window}m']
            avg_return = avg_returns[f'{window}m']
            summary.append(f"### {window} Minute Window")
            summary.append(f"- **Win Rate**: {win_rate:.1%}")
            summary.append(f"- **Average Return**: {avg_return:+.2f}%")
            summary.append("")
        
        summary.append("## Pattern Performance")
        pattern_performance = df_results.groupby('pattern').agg({
            'change_pct_15m': ['mean', 'count']
        }).round(2)
        pattern_performance.columns = ['avg_return', 'count']
        pattern_performance = pattern_performance.sort_values('avg_return', ascending=False)
        
        for pattern, row in pattern_performance.iterrows():
            summary.append(f"- **{pattern}**: {row['avg_return']:+.2f}% (n={row['count']})")
        
        summary.append("")
        summary.append("## Top Performing Symbols")
        symbol_performance = df_results.groupby('clean_symbol').agg({
            'change_pct_15m': ['mean', 'count']
        }).round(2)
        symbol_performance.columns = ['avg_return', 'count']
        symbol_performance = symbol_performance[symbol_performance['count'] >= 2]
        symbol_performance = symbol_performance.sort_values('avg_return', ascending=False).head(10)
        
        for symbol, row in symbol_performance.iterrows():
            summary.append(f"- **{symbol}**: {row['avg_return']:+.2f}% (n={row['count']})")
        
        summary.append("")
        summary.append("## Individual Alert Results")
        summary.append("(See individual PNG files for detailed charts)")
        
        # Save text summary
        with open('signal_provider_report.md', 'w') as f:
            f.write('\n'.join(summary))
        
        print("Text summary saved to 'signal_provider_report.md'")

if __name__ == "__main__":
    report = SignalProviderReport()
    report.extract_data()
    report.create_signal_provider_report()
