#!/usr/bin/env python3
"""
Generate comprehensive report package with visualizations, PDF, and CSV
"""

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
import sys

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10

class ComprehensiveReportGenerator:
    """Generate comprehensive report package with visualizations"""
    
    def __init__(self, report_json_path: str):
        self.report_json_path = report_json_path
        self.alerts = []
        self.date_str = datetime.now().strftime('%Y-%m-%d')
        
    def _is_fno_symbol(self, symbol: str) -> bool:
        """Check if symbol is F&O"""
        if not symbol or not isinstance(symbol, str):
            return False
        import re
        symbol_upper = symbol.upper()
        clean_symbol = symbol_upper.split(':')[-1] if ':' in symbol_upper else symbol_upper
        
        if clean_symbol.endswith('FUT'):
            return True
        if clean_symbol.endswith('CE') or clean_symbol.endswith('PE'):
            before_ce_pe = clean_symbol[:-2]
            if re.search(r'\d+$', before_ce_pe):
                return True
        if any(idx in clean_symbol for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY']):
            if re.search(r'\d{2}[A-Z]{3}', clean_symbol):
                return True
        return False
    
    def load_data(self):
        """Load all alerts from JSON and Redis"""
        try:
            import redis
            redis_db0 = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            redis_db1 = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
            
            all_alerts = []
            
            # Load from Redis stream
            try:
                stream_messages = redis_db1.xrevrange('alerts:stream', count=1000)
                for msg_id, fields in stream_messages:
                    try:
                        data_field = fields.get('data', b'')
                        if not data_field:
                            continue
                        try:
                            import orjson
                            alert = orjson.loads(data_field)
                        except (ImportError, orjson.JSONDecodeError):
                            if isinstance(data_field, bytes):
                                alert_str = data_field.decode('utf-8')
                            else:
                                alert_str = data_field
                            alert = json.loads(alert_str)
                        
                        alert['source'] = 'alerts_stream'
                        all_alerts.append(alert)
                    except:
                        continue
            except:
                pass
            
            # Load from JSON
            try:
                with open(self.report_json_path, 'r') as f:
                    report = json.load(f)
                recent_validations = report.get('recent_validations', [])
                for alert in recent_validations:
                    alert['source'] = 'json_file'
                    all_alerts.append(alert)
            except:
                pass
            
            # Deduplicate
            seen = set()
            for alert in all_alerts:
                alert_id = alert.get('alert_id', '')
                if not alert_id:
                    symbol = alert.get('symbol', '')
                    timestamp = alert.get('timestamp', '')
                    alert_id = f"{symbol}_{timestamp}"
                
                if alert_id and alert_id not in seen:
                    seen.add(alert_id)
                    validation_result = alert.get('validation_result', {})
                    forward_validation = validation_result.get('forward_validation', {})
                    summary = forward_validation.get('summary', {})
                    
                    # Determine if alert was successful based on forward validation
                    # PRIMARY METRIC: Forward validation success rate (actual price movement)
                    forward_success_ratio = summary.get('success_ratio', 0)
                    total_windows = summary.get('total_windows', 0)
                    success_count = summary.get('success_count', 0)
                    
                    # Classification logic:
                    # - If has validation windows: classify as successful/unsuccessful based on results
                    # - If NO validation windows: mark as "pending validation" (not included in success/failure counts)
                    has_validation_data = total_windows > 0
                    
                    if has_validation_data:
                        # Alert is successful if forward validation success ratio > 50% OR has any successful windows
                        is_successful = forward_success_ratio > 0.5 or success_count > 0
                    else:
                        # No forward validation data yet - mark as pending (will be excluded from success/failure classification)
                        is_successful = None  # None = pending validation
                    
                    self.alerts.append({
                        'symbol': alert.get('symbol', 'N/A'),
                        'alert_type': alert.get('alert_type', alert.get('pattern', 'unknown')),
                        'confidence': alert.get('confidence', 0.0),
                        'is_successful': is_successful,  # None = pending validation, True = successful, False = unsuccessful
                        'has_validation_data': has_validation_data,  # Track if forward validation data exists
                        'is_fno': self._is_fno_symbol(alert.get('symbol', '')),
                        'success_ratio': forward_success_ratio,
                        'success_count': summary.get('success_count', 0),
                        'failure_count': summary.get('failure_count', 0),
                        'inconclusive_count': summary.get('inconclusive_count', 0),
                        'total_windows': total_windows,
                        'max_move_pct': summary.get('max_directional_move_pct', 0)
                    })
            
            return True
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def generate_statistics(self) -> Dict:
        """Generate statistics"""
        if not self.alerts:
            return {}
        
        equity_alerts = [a for a in self.alerts if not a['is_fno']]
        fno_alerts = [a for a in self.alerts if a['is_fno']]
        
        def calc_stats(alert_list):
            # Only count alerts that have forward validation data
            validated_alerts = [a for a in alert_list if a.get('has_validation_data', False)]
            pending_alerts = [a for a in alert_list if not a.get('has_validation_data', False)]
            
            # Count successful/unsuccessful only among validated alerts
            successful = sum(1 for a in validated_alerts if a['is_successful'] is True)
            unsuccessful = sum(1 for a in validated_alerts if a['is_successful'] is False)
            
            # Forward validation window statistics (only from validated alerts)
            total_success = sum(a['success_count'] for a in validated_alerts)
            total_failure = sum(a['failure_count'] for a in validated_alerts)
            total_inconclusive = sum(a['inconclusive_count'] for a in validated_alerts)
            total_windows = sum(a['total_windows'] for a in validated_alerts)
            
            # Alert-level success rate (percentage of VALIDATED alerts that were successful)
            validated_count = len(validated_alerts)
            alert_success_rate = successful / validated_count if validated_count > 0 else 0
            
            # Window-level success rate (percentage of windows that succeeded) - PRIMARY METRIC
            window_success_rate = total_success / total_windows if total_windows > 0 else 0
            
            return {
                'total': len(alert_list),
                'validated': validated_count,
                'pending_validation': len(pending_alerts),
                'successful': successful,
                'unsuccessful': unsuccessful,
                'alert_success_rate': alert_success_rate,
                'total_success': total_success,
                'total_failure': total_failure,
                'total_inconclusive': total_inconclusive,
                'total_windows': total_windows,
                'window_success_rate': window_success_rate
            }
        
        overall = calc_stats(self.alerts)
        equity = calc_stats(equity_alerts)
        fno = calc_stats(fno_alerts)
        
        # Pattern distribution
        pattern_dist = defaultdict(int)
        for alert in self.alerts:
            pattern_dist[alert['alert_type']] += 1
        
        # Symbol distribution
        symbol_dist = defaultdict(int)
        for alert in self.alerts:
            symbol_dist[alert['symbol']] += 1
        
        return {
            'overall': overall,
            'equity': equity,
            'fno': fno,
            'pattern_distribution': dict(pattern_dist),
            'top_symbols': dict(sorted(symbol_dist.items(), key=lambda x: x[1], reverse=True)[:10])
        }
    
    def plot_overall_valid_invalid(self, stats: Dict, ax):
        """Plot overall successful vs unsuccessful - PRIMARY METRIC is Forward Validation Window Success Rate"""
        overall = stats['overall']
        
        # PRIMARY METRIC: Window-level success rate (actual price movement validation)
        window_sr = overall['window_success_rate'] * 100
        
        # Alert-level classification (only validated alerts)
        categories = ['Successful Alerts', 'Unsuccessful Alerts']
        if overall['pending_validation'] > 0:
            categories.append('Pending Validation')
        
        counts = [overall['successful'], overall['unsuccessful']]
        if overall['pending_validation'] > 0:
            counts.append(overall['pending_validation'])
        
        alert_sr = overall['alert_success_rate'] * 100
        validated_count = overall['validated']
        
        colors = ['#2ecc71', '#e74c3c', '#f39c12'][:len(categories)]
        bars = ax.bar(categories, counts, color=colors, alpha=0.8)
        
        # Add percentage labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            height = bar.get_height()
            if i == 0:
                # Successful: % of validated alerts
                pct = alert_sr
            elif i == 1:
                # Unsuccessful: % of validated alerts
                pct = 100 - alert_sr
            else:
                # Pending: % of total alerts
                pct = (count / overall['total']) * 100 if overall['total'] > 0 else 0
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(count)}\n({pct:.1f}%)',
                   ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # PROMINENTLY DISPLAY PRIMARY METRIC
        ax.text(0.5, 0.98, f'PRIMARY METRIC: Forward Validation Window Success Rate = {window_sr:.1f}%',
               transform=ax.transAxes, ha='center', fontsize=14, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8, edgecolor='darkgreen', linewidth=2))
        ax.text(0.5, 0.88, f'This measures actual price movement success across {overall["total_windows"]} validation windows',
               transform=ax.transAxes, ha='center', fontsize=10, style='italic')
        ax.text(0.5, 0.82, f'Validated Alerts: {overall["successful"]} successful / {validated_count} total ({alert_sr:.1f}%)',
               transform=ax.transAxes, ha='center', fontsize=11,
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        if overall['pending_validation'] > 0:
            ax.text(0.5, 0.76, f'Pending Validation: {overall["pending_validation"]} alerts (excluded from success/failure)',
                   transform=ax.transAxes, ha='center', fontsize=10, style='italic',
                   bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
        
        ax.set_title(f'Alert Success Classification\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.set_ylabel('Alert Count', fontsize=12)
        ax.set_ylim(0, max(counts) * 1.4 if counts else 100)
        ax.grid(axis='y', alpha=0.3)
    
    def plot_equity_vs_fno(self, stats: Dict, ax):
        """Plot Equity vs F&O successful/unsuccessful breakdown (only validated alerts)"""
        equity = stats['equity']
        fno = stats['fno']
        
        x = ['Equity', 'F&O']
        successful_counts = [equity['successful'], fno['successful']]
        unsuccessful_counts = [equity['unsuccessful'], fno['unsuccessful']]
        pending_counts = [equity['pending_validation'], fno['pending_validation']]
        has_pending = any(pending_counts)
        
        x_pos = [0, 1]
        if has_pending:
            width = 0.25
            bars1 = ax.bar([x - width for x in x_pos], successful_counts, width, 
                          label='Successful', color='#2ecc71', alpha=0.8)
            bars2 = ax.bar(x_pos, unsuccessful_counts, width,
                          label='Unsuccessful', color='#e74c3c', alpha=0.8)
            bars3 = ax.bar([x + width for x in x_pos], pending_counts, width,
                          label='Pending Validation', color='#f39c12', alpha=0.8)
            
            # Add labels for pending
            for i, (bar3, count) in enumerate(zip(bars3, pending_counts)):
                if count > 0:
                    ax.text(bar3.get_x() + bar3.get_width()/2., bar3.get_height(),
                           f'{int(count)}',
                           ha='center', va='bottom', fontsize=9, fontweight='bold')
        else:
            width = 0.35
            bars1 = ax.bar([x - width/2 for x in x_pos], successful_counts, width, 
                          label='Successful', color='#2ecc71', alpha=0.8)
            bars2 = ax.bar([x + width/2 for x in x_pos], unsuccessful_counts, width,
                          label='Unsuccessful', color='#e74c3c', alpha=0.8)
        
        # Add percentage labels (only for validated alerts)
        for i, (bar1, bar2) in enumerate(zip(bars1, bars2)):
            validated_total = successful_counts[i] + unsuccessful_counts[i]
            if validated_total > 0:
                pct1 = successful_counts[i] / validated_total * 100
                pct2 = unsuccessful_counts[i] / validated_total * 100
                ax.text(bar1.get_x() + bar1.get_width()/2., bar1.get_height(),
                       f'{int(successful_counts[i])}\n({pct1:.1f}%)',
                       ha='center', va='bottom', fontsize=10, fontweight='bold')
                ax.text(bar2.get_x() + bar2.get_width()/2., bar2.get_height(),
                       f'{int(unsuccessful_counts[i])}\n({pct2:.1f}%)',
                       ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # Add forward validation rates (PRIMARY METRIC)
        equity_fv = equity['window_success_rate'] * 100
        fno_fv = fno['window_success_rate'] * 100
        ax.text(0.5, 0.95, f'Forward Validation Window Success: Equity {equity_fv:.1f}% | F&O {fno_fv:.1f}%',
               transform=ax.transAxes, ha='center', fontsize=11, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
        
        ax.set_xlabel('Segment', fontsize=12)
        ax.set_ylabel('Alert Count', fontsize=12)
        ax.set_title(f'Equity vs F&O (Validated Alerts Only)\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x)
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
    
    def plot_forward_validation(self, stats: Dict, ax):
        """Plot forward validation results"""
        overall = stats['overall']
        
        categories = ['Success', 'Failure', 'Inconclusive']
        counts = [overall['total_success'], overall['total_failure'], overall['total_inconclusive']]
        total = overall['total_windows']
        
        percentages = [c/total*100 if total > 0 else 0 for c in counts]
        
        colors = ['#2ecc71', '#e74c3c', '#f39c12']
        bars = ax.bar(categories, counts, color=colors, alpha=0.8)
        
        # Add labels
        for i, (bar, pct) in enumerate(zip(bars, percentages)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{int(counts[i])}\n({pct:.1f}%)',
                   ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        ax.set_title(f'Forward Validation Results\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.set_ylabel('Windows', fontsize=12)
        ax.set_ylim(0, max(counts) * 1.2 if counts else 100)
        ax.grid(axis='y', alpha=0.3)
    
    def plot_pattern_distribution(self, stats: Dict, ax):
        """Plot pattern distribution"""
        pattern_dist = stats['pattern_distribution']
        
        if not pattern_dist:
            ax.text(0.5, 0.5, 'No patterns found', ha='center', va='center', transform=ax.transAxes)
            return
        
        patterns = list(pattern_dist.keys())
        counts = list(pattern_dist.values())
        
        colors = sns.color_palette("husl", len(patterns))
        bars = ax.barh(patterns, counts, color=colors, alpha=0.8)
        
        # Add count labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                   f' {int(count)}',
                   ha='left', va='center', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Count', fontsize=12)
        ax.set_title(f'Pattern Distribution\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
    
    def plot_top_symbols(self, stats: Dict, ax):
        """Plot top symbols"""
        top_symbols = stats['top_symbols']
        
        if not top_symbols:
            ax.text(0.5, 0.5, 'No symbols found', ha='center', va='center', transform=ax.transAxes)
            return
        
        symbols = list(top_symbols.keys())[:10]
        counts = [top_symbols[s] for s in symbols]
        
        colors = sns.color_palette("coolwarm", len(symbols))
        bars = ax.barh(symbols, counts, color=colors, alpha=0.8)
        
        # Add count labels
        for bar, count in zip(bars, counts):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                   f' {int(count)}',
                   ha='left', va='center', fontsize=10, fontweight='bold')
        
        ax.set_xlabel('Alert Count', fontsize=12)
        ax.set_title(f'Top Symbols\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
    
    def plot_heatmap_segment_validation(self, stats: Dict, ax):
        """Heatmap: Segment Success % (only validated alerts, based on forward validation)"""
        equity = stats['equity']
        fno = stats['fno']
        
        # Only include validated alerts in success rate calculation
        equity_sr = equity['alert_success_rate'] * 100 if equity['validated'] > 0 else 0
        fno_sr = fno['alert_success_rate'] * 100 if fno['validated'] > 0 else 0
        
        data = {
            'Equity': {
                'Successful': equity_sr,
                'Unsuccessful': 100 - equity_sr if equity['validated'] > 0 else 0
            },
            'F&O': {
                'Successful': fno_sr,
                'Unsuccessful': 100 - fno_sr if fno['validated'] > 0 else 0
            }
        }
        
        df = pd.DataFrame(data).T
        
        sns.heatmap(df, annot=True, fmt='.1f', cmap='RdYlGn', vmin=0, vmax=100,
                   cbar_kws={'label': 'Percentage (%)'}, ax=ax, linewidths=0.5)
        
        subtitle = ""
        if equity['pending_validation'] > 0 or fno['pending_validation'] > 0:
            subtitle = f"\n(Only validated alerts included)"
        
        ax.set_title(f'Segment Success % (Validated Alerts Only)\nDate: {self.date_str}{subtitle}', 
                    fontsize=14, fontweight='bold')
        ax.set_ylabel('Segment', fontsize=12)
    
    def plot_heatmap_forward_validation(self, stats: Dict, ax):
        """Heatmap: Forward Validation %"""
        equity = stats['equity']
        fno = stats['fno']
        
        data = {
            'Equity': {
                'Success': equity['total_success'] / equity['total_windows'] * 100 if equity['total_windows'] > 0 else 0,
                'Failure': equity['total_failure'] / equity['total_windows'] * 100 if equity['total_windows'] > 0 else 0,
                'Inconclusive': equity['total_inconclusive'] / equity['total_windows'] * 100 if equity['total_windows'] > 0 else 0
            },
            'F&O': {
                'Success': fno['total_success'] / fno['total_windows'] * 100 if fno['total_windows'] > 0 else 0,
                'Failure': fno['total_failure'] / fno['total_windows'] * 100 if fno['total_windows'] > 0 else 0,
                'Inconclusive': fno['total_inconclusive'] / fno['total_windows'] * 100 if fno['total_windows'] > 0 else 0
            }
        }
        
        df = pd.DataFrame(data).T
        
        sns.heatmap(df, annot=True, fmt='.1f', cmap='RdYlGn', vmin=0, vmax=100,
                   cbar_kws={'label': 'Percentage (%)'}, ax=ax, linewidths=0.5)
        
        ax.set_title(f'Forward Validation %\nDate: {self.date_str}', fontsize=14, fontweight='bold')
        ax.set_ylabel('Segment', fontsize=12)
    
    def plot_kpi_snapshot(self, stats: Dict, ax):
        """KPI Snapshot - Highlight Forward Validation as PRIMARY metric"""
        overall = stats['overall']
        equity = stats['equity']
        fno = stats['fno']
        
        # PRIMARY METRIC first
        primary_fv = overall['window_success_rate'] * 100
        
        kpis = [
            f"⭐ PRIMARY METRIC: Forward Validation Window Success Rate",
            f"   {primary_fv:.1f}% ({overall['total_success']} success / {overall['total_windows']} windows)",
            "",
            f"Total Alerts: {overall['total']}",
            f"Validated Alerts: {overall['validated']}",
            f"Pending Validation: {overall['pending_validation']} (excluded from success/failure)",
            f"Successful (Validated): {overall['successful']} ({overall['alert_success_rate']*100:.1f}%)",
            f"Equity Forward Validation: {equity['window_success_rate']*100:.1f}% ({equity['total_success']}/{equity['total_windows']} windows)",
            f"F&O Forward Validation: {fno['window_success_rate']*100:.1f}% ({fno['total_success']}/{fno['total_windows']} windows)"
        ]
        
        ax.axis('off')
        ax.text(0.5, 0.95, 'KPI SNAPSHOT', ha='center', va='top', 
               transform=ax.transAxes, fontsize=20, fontweight='bold')
        ax.text(0.5, 0.90, f'Date: {self.date_str}', ha='center', va='top',
               transform=ax.transAxes, fontsize=12)
        
        y_start = 0.80
        for i, kpi in enumerate(kpis):
            if kpi.startswith('⭐'):
                # Highlight primary metric
                ax.text(0.5, y_start - i*0.07, kpi, ha='center', va='top',
                       transform=ax.transAxes, fontsize=15, fontweight='bold', color='darkgreen',
                       bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8, edgecolor='darkgreen', linewidth=2))
            elif kpi == "":
                # Skip empty lines (spacing)
                continue
            else:
                ax.text(0.5, y_start - i*0.07, kpi, ha='center', va='top',
                       transform=ax.transAxes, fontsize=13, fontweight='bold',
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    def generate_csv_summary(self, stats: Dict):
        """Generate CSV summary"""
        overall = stats['overall']
        equity = stats['equity']
        fno = stats['fno']
        
        csv_data = {
            'Metric': [
                'Total Alerts',
                'Alerts with Forward Validation Data',
                'Alerts Pending Validation',
                'Successful Alerts (Validated Only)',
                'Unsuccessful Alerts (Validated Only)',
                'Alert Success Rate % (Validated Only)',
                '⭐ PRIMARY METRIC: Forward Validation Window Success Rate %',
                'Equity Alerts',
                'Equity Validated',
                'Equity Pending',
                'Equity Successful',
                'Equity Unsuccessful',
                'Equity Alert Success Rate %',
                'Equity Forward Validation Window Success %',
                'F&O Alerts',
                'F&O Validated',
                'F&O Pending',
                'F&O Successful',
                'F&O Unsuccessful',
                'F&O Alert Success Rate %',
                'F&O Forward Validation Window Success %',
                'Total Validation Windows',
                'Success Windows',
                'Failure Windows',
                'Inconclusive Windows'
            ],
            'Value': [
                overall['total'],
                overall['validated'],
                overall['pending_validation'],
                overall['successful'],
                overall['unsuccessful'],
                overall['alert_success_rate']*100,
                overall['window_success_rate']*100,
                equity['total'],
                equity['validated'],
                equity['pending_validation'],
                equity['successful'],
                equity['unsuccessful'],
                equity['alert_success_rate']*100,
                equity['window_success_rate']*100,
                fno['total'],
                fno['validated'],
                fno['pending_validation'],
                fno['successful'],
                fno['unsuccessful'],
                fno['alert_success_rate']*100,
                fno['window_success_rate']*100,
                overall['total_windows'],
                overall['total_success'],
                overall['total_failure'],
                overall['total_inconclusive']
            ]
        }
        
        df = pd.DataFrame(csv_data)
        # Save to everyday_validation_reports directory
        report_dir = Path("everyday_validation_reports")
        report_dir.mkdir(exist_ok=True)
        csv_file = report_dir / f"alert_report_{self.date_str}_summary.csv"
        df.to_csv(csv_file, index=False)
        print(f"CSV summary saved: {csv_file}")
        return str(csv_file)
    
    def generate_text_report(self, stats: Dict):
        """Generate comprehensive text report (same as PDF but in text format)"""
        overall = stats['overall']
        equity = stats['equity']
        fno = stats['fno']
        
        report_dir = Path("everyday_validation_reports")
        report_dir.mkdir(exist_ok=True)
        text_file = report_dir / f"alert_report_{self.date_str}_comprehensive.txt"
        
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write(f"COMPREHENSIVE ALERT VALIDATION REPORT\n")
            f.write(f"Date: {self.date_str}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n")
            f.write("="*80 + "\n\n")
            
            # PRIMARY METRIC
            f.write("⭐ PRIMARY METRIC: FORWARD VALIDATION WINDOW SUCCESS RATE\n")
            f.write("-"*80 + "\n")
            f.write(f"Overall Forward Validation Success: {overall['window_success_rate']*100:.1f}%\n")
            f.write(f"  Success Windows: {overall['total_success']}\n")
            f.write(f"  Failure Windows: {overall['total_failure']}\n")
            f.write(f"  Inconclusive Windows: {overall['total_inconclusive']}\n")
            f.write(f"  Total Windows Evaluated: {overall['total_windows']}\n")
            f.write("\nThis metric measures actual price movement success across all validation windows.\n")
            f.write("It is the PRIMARY indicator of algorithm performance.\n\n")
            
            # Overall Statistics
            f.write("="*80 + "\n")
            f.write("OVERALL STATISTICS\n")
            f.write("="*80 + "\n\n")
            f.write(f"Total Alerts: {overall['total']}\n")
            f.write(f"  Alerts with Forward Validation Data: {overall['validated']}\n")
            f.write(f"  Alerts Pending Validation: {overall['pending_validation']} (excluded from success/failure)\n\n")
            f.write(f"Validated Alerts Classification:\n")
            f.write(f"  Successful Alerts: {overall['successful']} ({overall['alert_success_rate']*100:.1f}%)\n")
            f.write(f"  Unsuccessful Alerts: {overall['unsuccessful']} ({(1-overall['alert_success_rate'])*100:.1f}%)\n\n")
            
            # Equity vs F&O Breakdown
            f.write("="*80 + "\n")
            f.write("EQUITY vs F&O BREAKDOWN\n")
            f.write("="*80 + "\n\n")
            
            f.write("EQUITY ALERTS:\n")
            f.write("-"*80 + "\n")
            f.write(f"  Total Equity Alerts: {equity['total']}\n")
            f.write(f"  Validated: {equity['validated']}\n")
            f.write(f"  Pending Validation: {equity['pending_validation']}\n")
            f.write(f"  Successful (Validated): {equity['successful']} ({equity['alert_success_rate']*100:.1f}%)\n")
            f.write(f"  Unsuccessful (Validated): {equity['unsuccessful']} ({(1-equity['alert_success_rate'])*100:.1f}%)\n")
            f.write(f"  Forward Validation Window Success Rate: {equity['window_success_rate']*100:.1f}%\n")
            f.write(f"    Success Windows: {equity['total_success']}\n")
            f.write(f"    Failure Windows: {equity['total_failure']}\n")
            f.write(f"    Total Windows: {equity['total_windows']}\n\n")
            
            f.write("F&O ALERTS (FUTURES & OPTIONS):\n")
            f.write("-"*80 + "\n")
            f.write(f"  Total F&O Alerts: {fno['total']}\n")
            f.write(f"  Validated: {fno['validated']}\n")
            f.write(f"  Pending Validation: {fno['pending_validation']}\n")
            f.write(f"  Successful (Validated): {fno['successful']} ({fno['alert_success_rate']*100:.1f}%)\n")
            f.write(f"  Unsuccessful (Validated): {fno['unsuccessful']} ({(1-fno['alert_success_rate'])*100:.1f}%)\n")
            f.write(f"  Forward Validation Window Success Rate: {fno['window_success_rate']*100:.1f}%\n")
            f.write(f"    Success Windows: {fno['total_success']}\n")
            f.write(f"    Failure Windows: {fno['total_failure']}\n")
            f.write(f"    Total Windows: {fno['total_windows']}\n\n")
            
            # Forward Validation Results
            f.write("="*80 + "\n")
            f.write("FORWARD VALIDATION RESULTS (PRIMARY METRIC)\n")
            f.write("="*80 + "\n\n")
            f.write(f"Total Windows Evaluated: {overall['total_windows']}\n")
            f.write(f"  Success: {overall['total_success']} ({overall['window_success_rate']*100:.1f}%)\n")
            f.write(f"  Failure: {overall['total_failure']} ({(overall['total_failure']/overall['total_windows']*100) if overall['total_windows'] > 0 else 0:.1f}%)\n")
            f.write(f"  Inconclusive: {overall['total_inconclusive']} ({(overall['total_inconclusive']/overall['total_windows']*100) if overall['total_windows'] > 0 else 0:.1f}%)\n\n")
            
            # Pattern Distribution
            f.write("="*80 + "\n")
            f.write("PATTERN DISTRIBUTION\n")
            f.write("="*80 + "\n\n")
            pattern_dist = stats['pattern_distribution']
            sorted_patterns = sorted(pattern_dist.items(), key=lambda x: x[1], reverse=True)
            for pattern, count in sorted_patterns:
                f.write(f"  {pattern}: {count} alerts\n")
            f.write("\n")
            
            # Top Symbols
            f.write("="*80 + "\n")
            f.write("TOP SYMBOLS\n")
            f.write("="*80 + "\n\n")
            top_symbols = stats['top_symbols']
            for symbol, count in list(top_symbols.items())[:10]:
                f.write(f"  {symbol}: {count} alerts\n")
            f.write("\n")
            
            # Heatmap Summary
            f.write("="*80 + "\n")
            f.write("SEGMENT VALIDATION SUMMARY (HEATMAP DATA)\n")
            f.write("="*80 + "\n\n")
            
            f.write("Segment Success % (Validated Alerts Only):\n")
            f.write("-"*80 + "\n")
            equity_sr = equity['alert_success_rate'] * 100 if equity['validated'] > 0 else 0
            fno_sr = fno['alert_success_rate'] * 100 if fno['validated'] > 0 else 0
            f.write(f"  Equity:\n")
            f.write(f"    Successful: {equity_sr:.1f}%\n")
            f.write(f"    Unsuccessful: {100-equity_sr:.1f}%\n")
            f.write(f"  F&O:\n")
            f.write(f"    Successful: {fno_sr:.1f}%\n")
            f.write(f"    Unsuccessful: {100-fno_sr:.1f}%\n\n")
            
            f.write("Forward Validation % (Window-Level):\n")
            f.write("-"*80 + "\n")
            equity_fv = equity['window_success_rate'] * 100
            fno_fv = fno['window_success_rate'] * 100
            f.write(f"  Equity:\n")
            f.write(f"    Success: {equity_fv:.1f}%\n")
            f.write(f"    Failure: {100-equity_fv:.1f}%\n")
            f.write(f"  F&O:\n")
            f.write(f"    Success: {fno_fv:.1f}%\n")
            f.write(f"    Failure: {100-fno_fv:.1f}%\n\n")
            
            # KPI Snapshot
            f.write("="*80 + "\n")
            f.write("KPI SNAPSHOT\n")
            f.write("="*80 + "\n\n")
            f.write(f"⭐ PRIMARY METRIC: Forward Validation Window Success Rate\n")
            f.write(f"   {overall['window_success_rate']*100:.1f}% ({overall['total_success']} success / {overall['total_windows']} windows)\n\n")
            f.write(f"Total Alerts: {overall['total']}\n")
            f.write(f"Validated Alerts: {overall['validated']}\n")
            f.write(f"Pending Validation: {overall['pending_validation']} (excluded from success/failure)\n")
            f.write(f"Successful (Validated): {overall['successful']} ({overall['alert_success_rate']*100:.1f}%)\n")
            f.write(f"Equity Forward Validation: {equity_fv:.1f}% ({equity['total_success']}/{equity['total_windows']} windows)\n")
            f.write(f"F&O Forward Validation: {fno_fv:.1f}% ({fno['total_success']}/{fno['total_windows']} windows)\n\n")
            
            # Footer
            f.write("="*80 + "\n")
            f.write("NOTES\n")
            f.write("="*80 + "\n\n")
            f.write("- Alerts without forward validation data are marked as 'Pending Validation'\n")
            f.write("  and are excluded from success/failure classification.\n\n")
            f.write("- PRIMARY METRIC is Forward Validation Window Success Rate, which measures\n")
            f.write("  actual price movement success across all validation windows.\n\n")
            f.write("- Alert-level success rate is calculated only from validated alerts.\n\n")
            f.write(f"Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}\n")
            f.write("="*80 + "\n")
        
        print(f"Text report saved: {text_file}")
        return str(text_file)
    
    def generate_pdf_report(self, stats: Dict):
        """Generate all-in-one PDF report"""
        # Save to everyday_validation_reports directory
        report_dir = Path("everyday_validation_reports")
        report_dir.mkdir(exist_ok=True)
        pdf_file = report_dir / f"alert_report_{self.date_str}.pdf"
        
        with PdfPages(pdf_file) as pdf:
            # Page 1: Overall Valid vs Invalid
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_overall_valid_invalid(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 2: Equity vs F&O
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_equity_vs_fno(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 3: Forward Validation Results
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_forward_validation(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 4: Pattern Distribution
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_pattern_distribution(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 5: Top Symbols
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_top_symbols(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 6: Heatmap Segment Validation
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_heatmap_segment_validation(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 7: Heatmap Forward Validation
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_heatmap_forward_validation(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
            
            # Page 8: KPI Snapshot
            fig, ax = plt.subplots(figsize=(10, 6))
            self.plot_kpi_snapshot(stats, ax)
            pdf.savefig(fig, bbox_inches='tight')
            plt.close()
        
        print(f"PDF report saved: {pdf_file}")
        return str(pdf_file)
    
    def generate(self):
        """Generate complete report package"""
        print(f"Generating comprehensive report package for {self.date_str}...")
        
        if not self.load_data():
            print("Error: Failed to load data")
            return False
        
        if not self.alerts:
            print("Warning: No alerts found")
            return False
        
        stats = self.generate_statistics()
        
        # Generate PDF
        pdf_file = self.generate_pdf_report(stats)
        
        # Generate CSV
        csv_file = self.generate_csv_summary(stats)
        
        # Generate Text Report
        text_file = self.generate_text_report(stats)
        
        print(f"\nReport package generated successfully!")
        print(f"  PDF: {pdf_file}")
        print(f"  CSV: {csv_file}")
        print(f"  Text: {text_file}")
        
        return True


if __name__ == "__main__":
    report_path = sys.argv[1] if len(sys.argv) > 1 else "alert_validation_report_20251031_154513.json"
    
    if not Path(report_path).exists():
        print(f"Error: Report file not found: {report_path}")
        sys.exit(1)
    
    generator = ComprehensiveReportGenerator(report_path)
    generator.generate()

