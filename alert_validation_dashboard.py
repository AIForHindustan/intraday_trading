#!/usr/bin/env python3
"""
Alert Validation Dashboard - Hard Evidence of Alert Quality
Creates static, social-friendly validation charts for Reddit/Telegram
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import redis
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import precision_recall_curve, roc_auc_score, brier_score_loss
from sklearn.calibration import calibration_curve
import warnings
warnings.filterwarnings('ignore')

class AlertValidationDashboard:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.price_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.alerts_data = []
        self.price_data = []
        
    def extract_data(self):
        """Extract and process alert and price data"""
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
                bucket_keys = self.redis_client.keys(f"bucket_incremental_volume:bucket:{symbol}:2025-10-28:*")
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
        
    def create_validation_dashboard(self):
        """Create the comprehensive validation dashboard"""
        print("Creating validation dashboard...")
        
        # Convert to DataFrames
        df_alerts = pd.DataFrame(self.alerts_data)
        df_prices = pd.DataFrame(self.price_data)
        
        if len(df_alerts) == 0 or len(df_prices) == 0:
            print("No data available for validation")
            return
        
        # Merge alerts with price data
        df_corr = self._merge_alerts_with_prices(df_alerts, df_prices)
        
        if len(df_corr) == 0:
            print("No matching alert-price pairs found")
            return
        
        # Data transformations
        df_corr = self._transform_data(df_corr)
        
        # Create the dashboard
        self._create_event_study(df_corr)
        self._create_calibration_plot(df_corr)
        self._create_precision_recall(df_corr)
        self._create_lift_analysis(df_corr)
        self._create_time_volume_heatmap(df_corr)
        self._create_pnl_summary(df_corr)
        
        # Create combined dashboard
        self._create_combined_dashboard(df_corr)
        
    def _merge_alerts_with_prices(self, df_alerts, df_prices):
        """Merge alerts with price data to calculate forward returns"""
        merged_data = []
        
        for _, alert in df_alerts.iterrows():
            symbol_prices = df_prices[df_prices['symbol'] == alert['symbol']].copy()
            symbol_prices = symbol_prices.sort_values('timestamp')
            
            # Find price at alert time and forward prices
            alert_time = alert['alert_time']
            alert_price = alert['entry_price']
            
            # Get prices at different forward windows
            forward_returns = {}
            for window_minutes in [1, 5, 15, 30]:
                target_time = alert_time + timedelta(minutes=window_minutes)
                
                # Find closest price after target time
                future_prices = symbol_prices[symbol_prices['timestamp'] >= target_time]
                if len(future_prices) > 0:
                    future_price = future_prices.iloc[0]['close']
                    forward_returns[f'return_{window_minutes}m'] = 100 * (future_price - alert_price) / alert_price
                else:
                    forward_returns[f'return_{window_minutes}m'] = np.nan
            
            # Add volume data from the alert time bucket
            alert_bucket = symbol_prices[symbol_prices['timestamp'] <= alert_time]
            if len(alert_bucket) > 0:
                volume_at_alert = alert_bucket.iloc[-1]['volume']
            else:
                volume_at_alert = 0
            
            # Add to merged data
            merged_alert = alert.to_dict()
            merged_alert.update(forward_returns)
            merged_alert['volume'] = volume_at_alert
            merged_data.append(merged_alert)
        
        return pd.DataFrame(merged_data)
    
    def _transform_data(self, df_corr):
        """Apply data transformations for proper analysis"""
        # Remove duplicates (one alert per symbol per 5 minutes)
        df_corr = df_corr.sort_values(['symbol', 'alert_time'])
        df_corr['time_diff'] = df_corr.groupby('symbol')['alert_time'].diff()
        df_corr = df_corr[(df_corr['time_diff'].isna()) | (df_corr['time_diff'] >= timedelta(minutes=5))]
        
        # Create time buckets
        df_corr['hour'] = df_corr['alert_time'].dt.hour
        df_corr['minute'] = df_corr['alert_time'].dt.minute
        df_corr['time_bucket'] = df_corr['hour'].astype(str) + ':' + df_corr['minute'].astype(str).str.zfill(2)
        
        # Create broader time buckets
        def get_time_period(hour, minute):
            if hour < 9 or (hour == 9 and minute < 30):
                return 'Pre-Market'
            elif hour < 10:
                return 'Open'
            elif hour < 12:
                return 'Morning'
            elif hour < 14:
                return 'Lunch'
            elif hour < 15:
                return 'Afternoon'
            else:
                return 'Close'
        
        df_corr['time_period'] = df_corr.apply(lambda x: get_time_period(x['hour'], x['minute']), axis=1)
        
        # Create success labels
        df_corr['success_15m'] = (df_corr['return_15m'].abs() >= 0.5).astype(int)  # 50 bps threshold
        df_corr['success_30m'] = (df_corr['return_30m'].abs() >= 0.5).astype(int)
        
        # Direction accuracy
        df_corr['direction_correct_15m'] = ((df_corr['return_15m'] > 0) & (df_corr['signal'] == 'BUY')).astype(int) | \
                                          ((df_corr['return_15m'] < 0) & (df_corr['signal'] == 'SELL')).astype(int)
        
        return df_corr
    
    def _create_event_study(self, df_corr):
        """A) Event Study - Forward returns with confidence intervals"""
        print("Creating event study...")
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        windows = [1, 5, 15, 30]
        returns = []
        ci_lower = []
        ci_upper = []
        
        for window in windows:
            col = f'return_{window}m'
            valid_returns = df_corr[col].dropna()
            
            if len(valid_returns) > 0:
                mean_ret = valid_returns.mean()
                std_ret = valid_returns.std()
                n = len(valid_returns)
                
                # 95% CI
                ci = 1.96 * std_ret / np.sqrt(n)
                
                returns.append(mean_ret)
                ci_lower.append(mean_ret - ci)
                ci_upper.append(mean_ret + ci)
            else:
                returns.append(0)
                ci_lower.append(0)
                ci_upper.append(0)
        
        # Plot
        ax.plot(windows, returns, 'o-', linewidth=2, markersize=8, color='blue', label='Mean Return')
        ax.fill_between(windows, ci_lower, ci_upper, alpha=0.3, color='blue', label='95% CI')
        ax.axhline(y=0, color='red', linestyle='--', alpha=0.7, label='Zero Line')
        
        ax.set_xlabel('Minutes After Alert', fontsize=14)
        ax.set_ylabel('Forward Return (bps)', fontsize=14)
        ax.set_title(f'Event Study: Forward Returns\nN={len(df_corr)} alerts', fontsize=16, fontweight='bold')
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Add annotations
        for i, (window, ret) in enumerate(zip(windows, returns)):
            ax.annotate(f'{ret:.1f}bps', (window, ret), 
                       textcoords="offset points", xytext=(0,10), ha='center', fontsize=10)
        
        plt.tight_layout()
        plt.savefig('event_study.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_calibration_plot(self, df_corr):
        """B) Calibration - Predicted vs Empirical hit rate"""
        print("Creating calibration plot...")
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Bin by confidence
        df_corr['confidence_bin'] = pd.cut(df_corr['confidence'], bins=10, labels=False)
        
        bin_stats = []
        for bin_id in range(10):
            bin_data = df_corr[df_corr['confidence_bin'] == bin_id]
            if len(bin_data) > 0:
                predicted = bin_data['confidence'].mean()
                empirical = bin_data['success_15m'].mean()
                bin_stats.append((predicted, empirical, len(bin_data)))
        
        if bin_stats:
            predicted, empirical, counts = zip(*bin_stats)
            
            ax.plot(predicted, empirical, 'o-', linewidth=2, markersize=8, color='blue')
            ax.plot([0, 1], [0, 1], 'r--', alpha=0.7, label='Perfect Calibration')
            
            # Add sample size annotations
            for i, (p, e, n) in enumerate(bin_stats):
                ax.annotate(f'n={n}', (p, e), textcoords="offset points", xytext=(0,10), ha='center')
            
            # Calculate Brier Score
            brier = brier_score_loss(df_corr['success_15m'], df_corr['confidence'])
            
            ax.set_xlabel('Predicted Probability', fontsize=14)
            ax.set_ylabel('Empirical Hit Rate', fontsize=14)
            ax.set_title(f'Calibration: Brier Score = {brier:.3f}\nN={len(df_corr)} alerts', fontsize=16, fontweight='bold')
            ax.legend(fontsize=12)
            ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('calibration_plot.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_precision_recall(self, df_corr):
        """C) Precision-Recall curve"""
        print("Creating precision-recall curve...")
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        # Use confidence as prediction score
        precision, recall, thresholds = precision_recall_curve(df_corr['success_15m'], df_corr['confidence'])
        
        ax.plot(recall, precision, linewidth=2, color='blue')
        ax.set_xlabel('Recall', fontsize=14)
        ax.set_ylabel('Precision', fontsize=14)
        ax.set_title('Precision-Recall Curve\n(Success = |Return| ≥ 50 bps in 15min)', fontsize=16, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Calculate AUC-PR
        auc_pr = np.trapz(precision, recall)
        
        # Find best F1 threshold
        f1_scores = 2 * (precision * recall) / (precision + recall)
        best_f1_idx = np.argmax(f1_scores)
        best_f1 = f1_scores[best_f1_idx]
        best_threshold = thresholds[best_f1_idx] if best_f1_idx < len(thresholds) else 0.5
        
        ax.annotate(f'AUC-PR = {auc_pr:.3f}\nBest F1 = {best_f1:.3f} @ τ = {best_threshold:.3f}', 
                   xy=(0.5, 0.5), xytext=(0.3, 0.3), fontsize=12,
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7))
        
        plt.tight_layout()
        plt.savefig('precision_recall.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_lift_analysis(self, df_corr):
        """D) Lift analysis by confidence deciles"""
        print("Creating lift analysis...")
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create deciles
        df_corr['confidence_decile'] = pd.qcut(df_corr['confidence'], q=10, labels=False, duplicates='drop')
        
        decile_stats = []
        for decile in range(10):
            decile_data = df_corr[df_corr['confidence_decile'] == decile]
            if len(decile_data) > 0:
                win_rate = decile_data['success_15m'].mean()
                avg_return = decile_data['return_15m'].mean()
                count = len(decile_data)
                decile_stats.append((decile, win_rate, avg_return, count))
        
        if decile_stats:
            deciles, win_rates, avg_returns, counts = zip(*decile_stats)
            
            # Create bar chart
            bars = ax.bar(deciles, win_rates, alpha=0.7, color='skyblue', edgecolor='navy')
            
            # Add return line
            ax2 = ax.twinx()
            ax2.plot(deciles, avg_returns, 'ro-', linewidth=2, markersize=6, label='Avg Return (bps)')
            
            # Add sample size annotations
            for i, (decile, win_rate, avg_return, count) in enumerate(decile_stats):
                ax.annotate(f'n={count}', (decile, win_rate), 
                           textcoords="offset points", xytext=(0,10), ha='center', fontsize=8)
            
            ax.set_xlabel('Confidence Decile', fontsize=14)
            ax.set_ylabel('Win Rate', fontsize=14)
            ax2.set_ylabel('Average Return (bps)', fontsize=14)
            ax.set_title('Lift Analysis: Win Rate & Returns by Confidence Decile', fontsize=16, fontweight='bold')
            ax.grid(True, alpha=0.3)
            ax2.legend(loc='upper right')
        
        plt.tight_layout()
        plt.savefig('lift_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_time_volume_heatmap(self, df_corr):
        """E) Time-of-Day × Volume-Ratio heatmap"""
        print("Creating time-volume heatmap...")
        
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Create time buckets (30-minute intervals)
        df_corr['time_bucket_30m'] = df_corr['alert_time'].dt.floor('30min').dt.strftime('%H:%M')
        
        # Create volume ratio (simplified - using raw volume for now)
        df_corr['volume_ratio'] = df_corr['volume'] / df_corr['volume'].mean()
        
        # Create pivot table
        pivot_data = df_corr.groupby(['time_bucket_30m', 'volume_ratio']).agg({
            'return_15m': 'median'
        }).reset_index()
        
        # Create heatmap
        pivot_table = pivot_data.pivot(index='time_bucket_30m', columns='volume_ratio', values='return_15m')
        
        sns.heatmap(pivot_table, annot=True, fmt='.1f', cmap='RdYlGn', center=0, ax=ax)
        ax.set_title('Time-of-Day × Volume-Ratio Heatmap\n(Median 15min Return in bps)', fontsize=16, fontweight='bold')
        ax.set_xlabel('Volume Ratio (vs Mean)', fontsize=14)
        ax.set_ylabel('Time of Day', fontsize=14)
        
        plt.tight_layout()
        plt.savefig('time_volume_heatmap.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_pnl_summary(self, df_corr):
        """F) Micro P&L Summary"""
        print("Creating P&L summary...")
        
        # Calculate P&L metrics
        total_alerts = len(df_corr)
        hit_rate_15m = df_corr['success_15m'].mean()
        avg_gain = df_corr[df_corr['return_15m'] > 0]['return_15m'].mean() if len(df_corr[df_corr['return_15m'] > 0]) > 0 else 0
        avg_loss = df_corr[df_corr['return_15m'] < 0]['return_15m'].mean() if len(df_corr[df_corr['return_15m'] < 0]) > 0 else 0
        gain_loss_ratio = abs(avg_gain / avg_loss) if avg_loss != 0 else np.inf
        
        # Simple execution simulation
        df_corr['pnl'] = df_corr['return_15m'] - 0.01  # 1 bps cost
        cumulative_pnl = df_corr['pnl'].cumsum()
        max_drawdown = (cumulative_pnl - cumulative_pnl.expanding().max()).min()
        
        # Create summary table
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.axis('tight')
        ax.axis('off')
        
        summary_data = [
            ['Sample Size', f'{total_alerts:,}'],
            ['Hit Rate @ 15min', f'{hit_rate_15m:.1%}'],
            ['Avg Gain', f'{avg_gain:.1f} bps'],
            ['Avg Loss', f'{avg_loss:.1f} bps'],
            ['Gain/Loss Ratio', f'{gain_loss_ratio:.2f}'],
            ['Max Drawdown', f'{max_drawdown:.1f} bps'],
            ['Total P&L', f'{df_corr["pnl"].sum():.1f} bps']
        ]
        
        table = ax.table(cellText=summary_data, colLabels=['Metric', 'Value'],
                        cellLoc='center', loc='center', bbox=[0, 0, 1, 1])
        table.auto_set_font_size(False)
        table.set_fontsize(14)
        table.scale(1, 2)
        
        ax.set_title('Micro P&L Summary\n(1 bps execution cost)', fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        plt.savefig('pnl_summary.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _create_combined_dashboard(self, df_corr):
        """Create the final combined dashboard"""
        print("Creating combined dashboard...")
        
        fig = plt.figure(figsize=(20, 12))
        
        # Create subplots
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # A) Event Study (top-left)
        ax1 = fig.add_subplot(gs[0, 0])
        windows = [1, 5, 15, 30]
        returns = [df_corr[f'return_{w}m'].mean() for w in windows]
        ax1.plot(windows, returns, 'o-', linewidth=2, markersize=8, color='blue')
        ax1.axhline(y=0, color='red', linestyle='--', alpha=0.7)
        ax1.set_title(f'Event Study\n+{returns[2]:.1f}bps @ 15m, N={len(df_corr)}', fontweight='bold')
        ax1.set_xlabel('Minutes After Alert')
        ax1.set_ylabel('Forward Return (bps)')
        ax1.grid(True, alpha=0.3)
        
        # B) Calibration (top-right)
        ax2 = fig.add_subplot(gs[0, 1])
        df_corr['confidence_bin'] = pd.cut(df_corr['confidence'], bins=5, labels=False)
        bin_stats = []
        for bin_id in range(5):
            bin_data = df_corr[df_corr['confidence_bin'] == bin_id]
            if len(bin_data) > 0:
                predicted = bin_data['confidence'].mean()
                empirical = bin_data['success_15m'].mean()
                bin_stats.append((predicted, empirical))
        
        if bin_stats:
            predicted, empirical = zip(*bin_stats)
            ax2.plot(predicted, empirical, 'o-', linewidth=2, markersize=8, color='blue')
            ax2.plot([0, 1], [0, 1], 'r--', alpha=0.7)
            brier = brier_score_loss(df_corr['success_15m'], df_corr['confidence'])
            ax2.set_title(f'Calibration\nBrier={brier:.3f}', fontweight='bold')
            ax2.set_xlabel('Predicted Probability')
            ax2.set_ylabel('Empirical Hit Rate')
            ax2.grid(True, alpha=0.3)
        
        # C) Precision-Recall (middle-left)
        ax3 = fig.add_subplot(gs[1, 0])
        precision, recall, thresholds = precision_recall_curve(df_corr['success_15m'], df_corr['confidence'])
        ax3.plot(recall, precision, linewidth=2, color='blue')
        auc_pr = np.trapz(precision, recall)
        ax3.set_title(f'Precision-Recall\nAUC-PR={auc_pr:.3f}', fontweight='bold')
        ax3.set_xlabel('Recall')
        ax3.set_ylabel('Precision')
        ax3.grid(True, alpha=0.3)
        
        # D) Lift Analysis (middle-right)
        ax4 = fig.add_subplot(gs[1, 1])
        df_corr['confidence_decile'] = pd.qcut(df_corr['confidence'], q=5, labels=False, duplicates='drop')
        decile_stats = []
        for decile in range(5):
            decile_data = df_corr[df_corr['confidence_decile'] == decile]
            if len(decile_data) > 0:
                win_rate = decile_data['success_15m'].mean()
                decile_stats.append(win_rate)
        
        if decile_stats:
            ax4.bar(range(5), decile_stats, alpha=0.7, color='skyblue', edgecolor='navy')
            ax4.set_title('Lift Analysis\nWin Rate by Decile', fontweight='bold')
            ax4.set_xlabel('Confidence Decile')
            ax4.set_ylabel('Win Rate')
            ax4.grid(True, alpha=0.3)
        
        # E) Time Heatmap (bottom-left)
        ax5 = fig.add_subplot(gs[2, 0])
        df_corr['time_period'] = df_corr['alert_time'].dt.hour
        time_stats = df_corr.groupby('time_period')['return_15m'].mean()
        ax5.bar(time_stats.index, time_stats.values, alpha=0.7, color='lightgreen')
        ax5.set_title('Time-of-Day Analysis\nAvg Return by Hour', fontweight='bold')
        ax5.set_xlabel('Hour of Day')
        ax5.set_ylabel('Avg Return (bps)')
        ax5.grid(True, alpha=0.3)
        
        # F) P&L Summary (bottom-right)
        ax6 = fig.add_subplot(gs[2, 1])
        hit_rate = df_corr['success_15m'].mean()
        avg_return = df_corr['return_15m'].mean()
        total_pnl = df_corr['return_15m'].sum()
        
        summary_text = f"""P&L Summary
Hit Rate: {hit_rate:.1%}
Avg Return: {avg_return:.1f} bps
Total P&L: {total_pnl:.1f} bps
N: {len(df_corr):,}"""
        
        ax6.text(0.1, 0.5, summary_text, transform=ax6.transAxes, fontsize=12,
                verticalalignment='center', bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue"))
        ax6.set_title('Performance Summary', fontweight='bold')
        ax6.axis('off')
        
        # Add watermark
        fig.text(0.95, 0.05, '@intraday_trading', fontsize=10, alpha=0.5, ha='right')
        
        plt.suptitle('Alert Validation Dashboard - October 28, 2025', fontsize=20, fontweight='bold', y=0.95)
        plt.savefig('alert_validation_dashboard.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        print("Dashboard saved as 'alert_validation_dashboard.png'")

if __name__ == "__main__":
    dashboard = AlertValidationDashboard()
    dashboard.extract_data()
    dashboard.create_validation_dashboard()
