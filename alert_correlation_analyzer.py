#!/usr/bin/env python3
"""
Alert Data Correlation Analysis and Visualization
Analyzes correlations between alert patterns, timing, and price movements
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import redis
from scipy.stats import pearsonr, spearmanr
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import warnings
warnings.filterwarnings('ignore')

class AlertCorrelationAnalyzer:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.price_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        self.alerts_data = []
        self.price_data = []
        
    def extract_alert_data(self):
        """Extract alert data from Redis forward validation keys"""
        print("Extracting alert data from Redis...")
        
        # Get all forward validation keys
        keys = self.redis_client.keys("forward_validation:alert:*")
        
        for key in keys:
            try:
                data = json.loads(self.redis_client.get(key))
                
                # Extract alert information
                alert_info = {
                    'symbol': data.get('symbol'),
                    'pattern': data.get('pattern'),
                    'signal': data.get('signal'),
                    'confidence': data.get('confidence'),
                    'entry_price': data.get('entry_price'),
                    'expected_move': data.get('expected_move'),
                    'timestamp': data.get('timestamp'),
                    'alert_time': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')),
                    'hour': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')).hour,
                    'minute': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')).minute,
                    'time_of_day': datetime.fromisoformat(data.get('timestamp').replace('Z', '+00:00')).strftime('%H:%M')
                }
                
                self.alerts_data.append(alert_info)
                
            except Exception as e:
                print(f"Error processing key {key}: {e}")
                
        print(f"Extracted {len(self.alerts_data)} alerts")
        return pd.DataFrame(self.alerts_data)
    
    def extract_price_data(self, symbols=None):
        """Extract price data from Redis bucket data"""
        print("Extracting price data from Redis buckets...")
        
        if symbols is None:
            symbols = list(set([alert['symbol'] for alert in self.alerts_data]))
        
        for symbol in symbols:
            try:
                # Get bucket keys for the symbol
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
                            'timestamp': f"2025-10-28 {data.get('hour')}:{data.get('minute_bucket')}:00"
                        }
                        
                        self.price_data.append(price_info)
                        
                    except Exception as e:
                        print(f"Error processing price key {key}: {e}")
                        
            except Exception as e:
                print(f"Error processing symbol {symbol}: {e}")
                
        print(f"Extracted {len(self.price_data)} price records")
        return pd.DataFrame(self.price_data)
    
    def analyze_correlations(self, df_alerts, df_prices):
        """Analyze correlations between different variables"""
        print("Analyzing correlations...")
        
        # Prepare data for correlation analysis
        correlation_data = []
        
        for _, alert in df_alerts.iterrows():
            symbol = alert['symbol']
            alert_time = alert['alert_time']
            
            # Find price data around alert time
            symbol_prices = df_prices[df_prices['symbol'] == symbol].copy()
            
            if len(symbol_prices) > 0:
                # Calculate price movements after alert
                alert_minute = alert_time.minute
                alert_hour = alert_time.hour
                
                # Find price buckets after alert
                future_prices = symbol_prices[
                    (symbol_prices['hour'] > alert_hour) | 
                    ((symbol_prices['hour'] == alert_hour) & (symbol_prices['minute_bucket'] > alert_minute))
                ].sort_values(['hour', 'minute_bucket'])
                
                if len(future_prices) > 0:
                    # Calculate 5-minute, 10-minute, 30-minute movements
                    entry_price = alert['entry_price']
                    
                    for _, future_price in future_prices.head(6).iterrows():  # First 6 buckets (30 minutes)
                        time_diff = (future_price['hour'] - alert_hour) * 60 + (future_price['minute_bucket'] - alert_minute)
                        
                        if time_diff > 0:
                            price_change = future_price['close'] - entry_price
                            price_change_pct = (price_change / entry_price) * 100
                            
                            correlation_data.append({
                                'symbol': symbol,
                                'pattern': alert['pattern'],
                                'confidence': alert['confidence'],
                                'entry_price': entry_price,
                                'time_of_day': alert['time_of_day'],
                                'hour': alert['hour'],
                                'minute': alert['minute'],
                                'time_diff_minutes': time_diff,
                                'price_change': price_change,
                                'price_change_pct': price_change_pct,
                                'volume': future_price['volume']
                            })
        
        return pd.DataFrame(correlation_data)
    
    def create_visualizations(self, df_corr):
        """Create correlation visualizations"""
        print("Creating visualizations...")
        
        # Set up the plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        # 1. Correlation Heatmap
        plt.figure(figsize=(12, 8))
        numeric_cols = ['confidence', 'entry_price', 'time_diff_minutes', 'price_change_pct', 'volume']
        correlation_matrix = df_corr[numeric_cols].corr()
        
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', center=0,
                    square=True, linewidths=0.5)
        plt.title('Correlation Matrix: Alert Variables vs Price Movements')
        plt.tight_layout()
        plt.savefig('correlation_heatmap.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 2. Pattern vs Price Movement
        plt.figure(figsize=(14, 8))
        pattern_movement = df_corr.groupby('pattern')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        pattern_movement = pattern_movement.sort_values('mean', ascending=False)
        
        bars = plt.bar(pattern_movement['pattern'], pattern_movement['mean'], 
                      yerr=pattern_movement['std'], capsize=5, alpha=0.7)
        plt.title('Average Price Movement by Pattern Type')
        plt.xlabel('Pattern Type')
        plt.ylabel('Average Price Change (%)')
        plt.xticks(rotation=45)
        
        # Add count labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            count = pattern_movement.iloc[i]['count']
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'n={count}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('pattern_movement.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 3. Time of Day Analysis
        plt.figure(figsize=(14, 8))
        time_analysis = df_corr.groupby('time_of_day')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        time_analysis = time_analysis.sort_values('time_of_day')
        
        plt.errorbar(range(len(time_analysis)), time_analysis['mean'], 
                    yerr=time_analysis['std'], marker='o', capsize=5, capthick=2)
        plt.title('Price Movement by Time of Day')
        plt.xlabel('Time of Day')
        plt.ylabel('Average Price Change (%)')
        plt.xticks(range(len(time_analysis)), time_analysis['time_of_day'], rotation=45)
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('time_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # 4. Interactive Plotly Visualization - Clean Version
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Confidence vs Price Movement', 'Entry Price vs Movement',
                          'Volume vs Price Movement', 'Time vs Movement'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Clean symbol names for hover tooltips only
        df_corr['symbol_label'] = df_corr['symbol'].str.replace('25NOVFUT', '').str.replace('28OCTFUT', '')
        
        # Confidence vs Price Movement
        fig.add_trace(
            go.Scatter(x=df_corr['confidence'], y=df_corr['price_change_pct'],
                      mode='markers', name='Confidence vs Movement',
                      text=df_corr['symbol_label'],
                      hovertemplate='<b>%{text}</b><br>Confidence: %{x:.2f}<br>Movement: %{y:.2f}%<br>Pattern: ' + df_corr['pattern'] + '<br>Time: ' + df_corr['time_of_day'] + '<extra></extra>',
                      marker=dict(size=8, opacity=0.7)),
            row=1, col=1
        )
        
        # Entry Price vs Movement
        fig.add_trace(
            go.Scatter(x=df_corr['entry_price'], y=df_corr['price_change_pct'],
                      mode='markers', name='Entry Price vs Movement',
                      text=df_corr['symbol_label'],
                      hovertemplate='<b>%{text}</b><br>Entry Price: ₹%{x:.2f}<br>Movement: %{y:.2f}%<br>Pattern: ' + df_corr['pattern'] + '<br>Time: ' + df_corr['time_of_day'] + '<extra></extra>',
                      marker=dict(size=8, opacity=0.7)),
            row=1, col=2
        )
        
        # Volume vs Movement
        fig.add_trace(
            go.Scatter(x=df_corr['volume'], y=df_corr['price_change_pct'],
                      mode='markers', name='Volume vs Movement',
                      text=df_corr['symbol_label'],
                      hovertemplate='<b>%{text}</b><br>Volume: %{x:,.0f}<br>Movement: %{y:.2f}%<br>Pattern: ' + df_corr['pattern'] + '<br>Time: ' + df_corr['time_of_day'] + '<extra></extra>',
                      marker=dict(size=8, opacity=0.7)),
            row=2, col=1
        )
        
        # Time vs Movement
        fig.add_trace(
            go.Scatter(x=df_corr['time_diff_minutes'], y=df_corr['price_change_pct'],
                      mode='markers', name='Time vs Movement',
                      text=df_corr['symbol_label'],
                      hovertemplate='<b>%{text}</b><br>Time Diff: %{x}min<br>Movement: %{y:.2f}%<br>Pattern: ' + df_corr['pattern'] + '<br>Time: ' + df_corr['time_of_day'] + '<extra></extra>',
                      marker=dict(size=8, opacity=0.7)),
            row=2, col=2
        )
        
        fig.update_layout(height=800, title_text="Interactive Alert Correlation Analysis")
        fig.write_html('interactive_correlation_analysis.html')
        fig.show()
        
        # 5. Symbol Performance Analysis
        plt.figure(figsize=(16, 10))
        symbol_performance = df_corr.groupby('symbol')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        symbol_performance = symbol_performance.sort_values('mean', ascending=False).head(15)
        
        bars = plt.bar(range(len(symbol_performance)), symbol_performance['mean'],
                      yerr=symbol_performance['std'], capsize=5, alpha=0.7)
        plt.title('Top 15 Symbols by Average Price Movement')
        plt.xlabel('Symbols')
        plt.ylabel('Average Price Change (%)')
        plt.xticks(range(len(symbol_performance)), symbol_performance['symbol'], rotation=45)
        
        # Add count labels
        for i, bar in enumerate(bars):
            height = bar.get_height()
            count = symbol_performance.iloc[i]['count']
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'n={count}', ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('symbol_performance.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        # Interactive Symbol Performance Chart - Actual vs Expected Movement
        symbol_performance_interactive = df_corr.groupby('symbol').agg({
            'price_change_pct': ['mean', 'std', 'count'],
            'expected_move': 'mean',
            'confidence': 'mean',
            'volume': 'mean'
        }).round(2)
        
        symbol_performance_interactive.columns = ['actual_movement', 'movement_std', 'alert_count', 'expected_movement', 'avg_confidence', 'avg_volume']
        symbol_performance_interactive = symbol_performance_interactive.sort_values('actual_movement', ascending=False)
        
        # Clean symbol names for display
        symbol_performance_interactive['display_name'] = symbol_performance_interactive.index.str.replace('25NOVFUT', '').str.replace('28OCTFUT', '')
        
        fig_symbols = go.Figure()
        
        # Actual movement bars
        fig_symbols.add_trace(go.Bar(
            x=symbol_performance_interactive['display_name'],
            y=symbol_performance_interactive['actual_movement'],
            name='Actual Movement',
            text=symbol_performance_interactive['actual_movement'].round(2),
            textposition='auto',
            hovertemplate='<b>%{x}</b><br>Actual: %{y:.2f}%<br>Expected: ' + symbol_performance_interactive['expected_movement'].round(2).astype(str) + '%<br>Alerts: ' + symbol_performance_interactive['alert_count'].astype(str) + '<extra></extra>',
            marker_color=['green' if x > 0 else 'red' for x in symbol_performance_interactive['actual_movement']]
        ))
        
        # Expected movement line
        fig_symbols.add_trace(go.Scatter(
            x=symbol_performance_interactive['display_name'],
            y=symbol_performance_interactive['expected_movement'],
            mode='markers+lines',
            name='Expected Movement',
            line=dict(color='blue', width=2),
            marker=dict(size=8, symbol='diamond'),
            hovertemplate='<b>%{x}</b><br>Expected: %{y:.2f}%<extra></extra>'
        ))
        
        fig_symbols.update_layout(
            title='Symbol Performance: Actual vs Expected Movement (%)',
            xaxis_title='Instrument',
            yaxis_title='Price Movement (%)',
            height=600,
            xaxis_tickangle=-45,
            barmode='group'
        )
        
        fig_symbols.write_html('symbol_performance_interactive.html')
        fig_symbols.show()
    
    def generate_correlation_report(self, df_corr):
        """Generate a detailed correlation report"""
        print("Generating correlation report...")
        
        report = []
        report.append("# Alert Correlation Analysis Report")
        report.append(f"## Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Basic statistics
        report.append("## Basic Statistics")
        report.append(f"- Total alert-price pairs analyzed: {len(df_corr)}")
        report.append(f"- Unique symbols: {df_corr['symbol'].nunique()}")
        report.append(f"- Unique patterns: {df_corr['pattern'].nunique()}")
        report.append(f"- Average price movement: {df_corr['price_change_pct'].mean():.3f}%")
        report.append(f"- Price movement std: {df_corr['price_change_pct'].std():.3f}%")
        report.append("")
        
        # Correlation analysis
        report.append("## Correlation Analysis")
        numeric_cols = ['confidence', 'entry_price', 'time_diff_minutes', 'price_change_pct', 'volume']
        
        for col1 in numeric_cols:
            for col2 in numeric_cols:
                if col1 != col2:
                    corr, p_value = pearsonr(df_corr[col1], df_corr[col2])
                    report.append(f"- {col1} vs {col2}: r={corr:.3f}, p={p_value:.3f}")
        
        report.append("")
        
        # Pattern analysis
        report.append("## Pattern Performance")
        pattern_stats = df_corr.groupby('pattern')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        pattern_stats = pattern_stats.sort_values('mean', ascending=False)
        
        for _, row in pattern_stats.iterrows():
            report.append(f"- {row['pattern']}: {row['mean']:.3f}% ± {row['std']:.3f}% (n={row['count']})")
        
        report.append("")
        
        # Top performing symbols
        report.append("## Top 10 Performing Symbols")
        symbol_stats = df_corr.groupby('symbol')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        symbol_stats = symbol_stats.sort_values('mean', ascending=False).head(10)
        
        for _, row in symbol_stats.iterrows():
            report.append(f"- {row['symbol']}: {row['mean']:.3f}% ± {row['std']:.3f}% (n={row['count']})")
        
        report.append("")
        
        # Time analysis
        report.append("## Time-based Analysis")
        time_stats = df_corr.groupby('time_of_day')['price_change_pct'].agg(['mean', 'std', 'count']).reset_index()
        time_stats = time_stats.sort_values('time_of_day')
        
        for _, row in time_stats.iterrows():
            report.append(f"- {row['time_of_day']}: {row['mean']:.3f}% ± {row['std']:.3f}% (n={row['count']})")
        
        # Save report
        with open('correlation_analysis_report.md', 'w') as f:
            f.write('\n'.join(report))
        
        print("Correlation report saved to 'correlation_analysis_report.md'")
        
        return '\n'.join(report)
    
    def run_analysis(self):
        """Run the complete correlation analysis"""
        print("Starting Alert Correlation Analysis...")
        
        # Extract data
        df_alerts = self.extract_alert_data()
        df_prices = self.extract_price_data()
        
        if len(df_alerts) == 0 or len(df_prices) == 0:
            print("No data available for analysis")
            return
        
        # Analyze correlations
        df_corr = self.analyze_correlations(df_alerts, df_prices)
        
        if len(df_corr) == 0:
            print("No correlation data available")
            return
        
        print(f"Analyzed {len(df_corr)} alert-price pairs")
        
        # Create visualizations
        self.create_visualizations(df_corr)
        
        # Generate report
        report = self.generate_correlation_report(df_corr)
        
        print("Analysis complete! Check the generated files:")
        print("- correlation_heatmap.png")
        print("- pattern_movement.png") 
        print("- time_analysis.png")
        print("- symbol_performance.png")
        print("- interactive_correlation_analysis.html")
        print("- correlation_analysis_report.md")
        
        return df_corr, report

if __name__ == "__main__":
    analyzer = AlertCorrelationAnalyzer()
    df_corr, report = analyzer.run_analysis()
