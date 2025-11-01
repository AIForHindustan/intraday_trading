#!/usr/bin/env python3
"""
Generate non-biased daily report for 95%+ confidence alerts
and publish to Telegram bots and Reddit community
"""

import json
import asyncio
import logging
from datetime import datetime, date
from typing import Dict, List
from collections import defaultdict
from pathlib import Path

# Import bot modules
import sys
sys.path.append(str(Path(__file__).parent))
from community_bots.telegram_bot import AIONTelegramBot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Daily95PercentReport:
    """Generate and publish daily 95% confidence alert report"""
    
    def __init__(self, report_json_path: str):
        self.report_json_path = report_json_path
        self.telegram_bot = AIONTelegramBot()
        self.alerts_95pct = []
        
    def load_indicators_from_redis(self, symbol: str) -> Dict:
        """Fetch indicators from Redis for a symbol"""
        try:
            import redis
            redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
            
            indicators = {}
            
            # Try to fetch indicators using the standard format
            indicator_keys = [
                'rsi', 'macd', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                'atr', 'vwap', 'volume_ratio', 'bb_upper', 'bb_middle', 'bb_lower',
                'delta', 'gamma', 'theta', 'vega', 'rho'  # Greeks for options
            ]
            
            for ind_name in indicator_keys:
                key = f"indicators:{symbol}:{ind_name}"
                try:
                    data = redis_client.get(key)
                    if data:
                        # Try parsing as JSON first (for complex indicators)
                        try:
                            parsed = json.loads(data)
                            if isinstance(parsed, dict) and 'value' in parsed:
                                indicators[ind_name] = parsed['value']
                            else:
                                indicators[ind_name] = parsed
                        except (json.JSONDecodeError, TypeError):
                            # Simple numeric value
                            try:
                                indicators[ind_name] = float(data)
                            except (ValueError, TypeError):
                                pass
                except Exception:
                    pass
            
            return indicators
            
        except Exception as e:
            logger.debug(f"Could not load indicators for {symbol}: {e}")
            return {}
    
    def _is_fno_symbol(self, symbol: str) -> bool:
        """Check if symbol is F&O (futures, options, or index derivatives)"""
        if not symbol or not isinstance(symbol, str):
            return False
        
        import re
        symbol_upper = symbol.upper()
        clean_symbol = symbol_upper.split(':')[-1] if ':' in symbol_upper else symbol_upper
        
        # Futures
        if clean_symbol.endswith('FUT'):
            return True
        
        # Options (ends with CE/PE with digits before)
        if clean_symbol.endswith('CE') or clean_symbol.endswith('PE'):
            before_ce_pe = clean_symbol[:-2]
            if re.search(r'\d+$', before_ce_pe):
                return True
        
        # Index derivatives (NIFTY, BANKNIFTY, FINNIFTY)
        if any(idx in clean_symbol for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY']):
            if re.search(r'\d{2}[A-Z]{3}', clean_symbol):
                return True
        
        return False
    
    def load_alerts_from_redis(self, include_all_alerts=False, confidence_threshold=0.95):
        """Load ALL alerts from Redis (not just validated ones)"""
        try:
            import redis
            redis_db0 = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            redis_db1 = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
            
            all_alerts = []
            
            # Source 1: Forward validation alerts (DB 0)
            fv_keys = redis_db0.keys('forward_validation:alert:*')
            logger.info(f"Found {len(fv_keys)} forward validation alerts in Redis")
            
            for key in fv_keys:
                try:
                    data = redis_db0.get(key)
                    if data:
                        alert = json.loads(data)
                        confidence = alert.get('confidence', 0.0)
                        if include_all_alerts or (isinstance(confidence, (int, float)) and confidence >= confidence_threshold):
                            alert['source'] = 'forward_validation'
                            all_alerts.append(alert)
                except Exception as e:
                    logger.debug(f"Error parsing alert {key}: {e}")
                    continue
            
            # Source 2: alerts:stream (Redis Stream - where all alerts are published)
            try:
                # Read from stream (last 1000 entries)
                stream_messages = redis_db1.xrevrange('alerts:stream', count=1000)
                logger.info(f"Found {len(stream_messages)} messages in alerts:stream")
                
                for msg_id, fields in stream_messages:
                    try:
                        # The stream stores data in 'data' field as binary JSON
                        data_field = fields.get('data', b'')
                        if not data_field:
                            continue
                        
                        # Try to decode binary data
                        try:
                            # Try orjson format first
                            import orjson
                            alert = orjson.loads(data_field)
                        except (ImportError, orjson.JSONDecodeError):
                            # Fallback to standard JSON
                            if isinstance(data_field, bytes):
                                alert_str = data_field.decode('utf-8')
                            else:
                                alert_str = data_field
                            alert = json.loads(alert_str)
                        
                        confidence = alert.get('confidence', 0.0)
                        if include_all_alerts or (isinstance(confidence, (int, float)) and confidence >= confidence_threshold):
                            alert['source'] = 'alerts_stream'
                            alert['stream_id'] = msg_id
                            all_alerts.append(alert)
                    except Exception as e:
                        logger.debug(f"Error parsing stream message {msg_id}: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Could not read from alerts:stream: {e}")
            
            # Source 3: Validation results (DB 1)
            try:
                validation_results = redis_db1.lrange('validation_results:recent', 0, 199)  # Last 200
                logger.info(f"Found {len(validation_results)} validation results in Redis")
                
                for result_str in validation_results:
                    try:
                        result = json.loads(result_str) if isinstance(result_str, str) else result_str
                        # Check if we have confidence from the alert itself, not validation
                        confidence = result.get('confidence', result.get('alert_confidence', 0.0))
                        if include_all_alerts or (isinstance(confidence, (int, float)) and confidence >= confidence_threshold):
                            result['source'] = 'validation_result'
                            all_alerts.append(result)
                    except Exception as e:
                        logger.debug(f"Error parsing validation result: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Could not read validation_results:recent: {e}")
            
            return all_alerts
            
        except Exception as e:
            logger.error(f"Error loading alerts from Redis: {e}")
            return []
    
    def load_report_data(self, include_all_alerts=False, confidence_threshold=0.95, use_redis=True):
        """Load alert validation report from JSON AND Redis, fetch indicators"""
        try:
            all_alerts = []
            
            # Load from Redis if requested (gets ALL alerts, not just validated)
            if use_redis:
                redis_alerts = self.load_alerts_from_redis(include_all_alerts, confidence_threshold)
                all_alerts.extend(redis_alerts)
                logger.info(f"Loaded {len(redis_alerts)} alerts from Redis")
            
            # Also load from JSON file
            try:
                with open(self.report_json_path, 'r') as f:
                    report = json.load(f)
                
                recent_validations = report.get('recent_validations', [])
                
                # Filter alerts by confidence threshold
                for alert in recent_validations:
                    confidence = alert.get('confidence', 0.0)
                    if include_all_alerts or (isinstance(confidence, (int, float)) and confidence >= confidence_threshold):
                        alert['source'] = 'json_file'
                        all_alerts.append(alert)
                
                logger.info(f"Loaded {len(recent_validations)} alerts from JSON file")
            except Exception as e:
                logger.warning(f"Could not load from JSON file: {e}")
            
            # Deduplicate alerts by alert_id or symbol+timestamp
            seen_alerts = set()
            unique_alerts = []
            
            for alert in all_alerts:
                # Create unique key for alert
                alert_id = alert.get('alert_id', '')
                if not alert_id:
                    # Fallback: use symbol + timestamp
                    symbol = alert.get('symbol', '')
                    timestamp = alert.get('timestamp', '')
                    alert_id = f"{symbol}_{timestamp}"
                
                if alert_id and alert_id not in seen_alerts:
                    seen_alerts.add(alert_id)
                    unique_alerts.append(alert)
            
            logger.info(f"Total unique alerts: {len(unique_alerts)} (after deduplication)")
            
            # Process all unique alerts
            for alert in unique_alerts:
                validation_result = alert.get('validation_result', {})
                forward_validation = validation_result.get('forward_validation', {})
                summary = forward_validation.get('summary', {})
                
                symbol = alert.get('symbol', 'N/A')
                
                # Fetch indicators from Redis
                indicators = self.load_indicators_from_redis(symbol)
                
                self.alerts_95pct.append({
                    'symbol': symbol,
                    'alert_type': alert.get('alert_type', alert.get('pattern', 'unknown')),
                    'timestamp': alert.get('timestamp', ''),
                    'entry_price': alert.get('entry_price', 0),
                    'signal': alert.get('signal', 'NEUTRAL'),
                    'confidence': alert.get('confidence', 0.0),
                    'expected_move': alert.get('expected_move', 0),
                    'is_valid': validation_result.get('is_valid', False),
                    'validation_confidence': validation_result.get('confidence_score', 0),
                    'success_ratio': summary.get('success_ratio', 0),
                    'success_count': summary.get('success_count', 0),
                    'failure_count': summary.get('failure_count', 0),
                    'inconclusive_count': summary.get('inconclusive_count', 0),
                    'total_windows': summary.get('total_windows', 0),
                    'max_move_pct': summary.get('max_directional_move_pct', 0),
                    'alert_id': alert.get('alert_id', ''),
                    'indicators': indicators,  # Add indicators data
                    'is_fno': self._is_fno_symbol(symbol)  # Mark F&O vs Equity
                })
            
            threshold_text = "all" if include_all_alerts else f"{confidence_threshold*100:.0f}%+"
            logger.info(f"Loaded {len(self.alerts_95pct)} alerts ({threshold_text} confidence)")
            
            # Log breakdown
            fno_count = sum(1 for a in self.alerts_95pct if a.get('is_fno', False))
            equity_count = len(self.alerts_95pct) - fno_count
            logger.info(f"   Breakdown: {equity_count} Equity, {fno_count} F&O")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading report data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def generate_statistics(self) -> Dict:
        """Generate non-biased statistics with separate Equity vs F&O breakdown"""
        if not self.alerts_95pct:
            return {}
        
        # Separate Equity and F&O alerts
        equity_alerts = [a for a in self.alerts_95pct if not a.get('is_fno', False)]
        fno_alerts = [a for a in self.alerts_95pct if a.get('is_fno', False)]
        
        def calculate_stats(alert_list):
            pattern_dist = defaultdict(int)
            symbol_dist = defaultdict(int)
            valid_count = 0
            invalid_count = 0
            total_success = 0
            total_failure = 0
            total_inconclusive = 0
            total_windows = 0
            success_ratios = []
            max_moves = []
            
            for alert in alert_list:
                pattern_dist[alert['alert_type']] += 1
                symbol_dist[alert['symbol']] += 1
                
                if alert['is_valid']:
                    valid_count += 1
                else:
                    invalid_count += 1
                
                total_success += alert['success_count']
                total_failure += alert['failure_count']
                total_inconclusive += alert['inconclusive_count']
                total_windows += alert['total_windows']
                
                if alert['success_ratio'] > 0:
                    success_ratios.append(alert['success_ratio'])
                
                if alert['max_move_pct']:
                    max_moves.append(alert['max_move_pct'])
            
            avg_success_ratio = sum(success_ratios) / len(success_ratios) if success_ratios else 0
            avg_max_move = sum(max_moves) / len(max_moves) if max_moves else 0
            overall_success_rate = total_success / total_windows if total_windows > 0 else 0
            
            return {
                'total_alerts': len(alert_list),
                'valid_alerts': valid_count,
                'invalid_alerts': invalid_count,
                'pattern_distribution': dict(pattern_dist),
                'top_symbols': dict(sorted(symbol_dist.items(), key=lambda x: x[1], reverse=True)[:10]),
                'forward_validation': {
                    'total_success': total_success,
                    'total_failure': total_failure,
                    'total_inconclusive': total_inconclusive,
                    'total_windows': total_windows,
                    'overall_success_rate': overall_success_rate,
                    'avg_success_ratio_per_alert': avg_success_ratio,
                    'avg_max_move_pct': avg_max_move
                }
            }
        
        # Overall stats (all alerts combined)
        overall_stats = calculate_stats(self.alerts_95pct)
        equity_stats = calculate_stats(equity_alerts)
        fno_stats = calculate_stats(fno_alerts)
        
        return {
            **overall_stats,
            'equity': equity_stats,
            'fno': fno_stats,
            'generated_at': datetime.now().isoformat()
        }
    
    def format_telegram_report(self, stats: Dict) -> str:
        """Format report for Telegram (HTML format)"""
        report_date = datetime.now().strftime('%Y-%m-%d')
        dashboard_url = "https://jere-unporous-magan.ngrok-free.dev"
        
        # Main statistics
        msg = f"""📊 <b>DAILY 95% CONFIDENCE ALERT REPORT</b>
📅 <b>Date:</b> {report_date}

<b>📈 OVERALL STATISTICS</b>
• Total Alerts (95%+): {stats['total_alerts']}
• Equity Alerts: {stats['equity']['total_alerts']}
• F&O Alerts (Futures/Options): {stats['fno']['total_alerts']}
• Valid Signals: {stats['valid_alerts']} ({stats['valid_alerts']/stats['total_alerts']*100:.1f}%)
• Invalid Signals: {stats['invalid_alerts']} ({stats['invalid_alerts']/stats['total_alerts']*100:.1f}%)

<b>📊 EQUITY vs F&O BREAKDOWN</b>

<b>Equity Alerts:</b>
• Total: {stats['equity']['total_alerts']}
• Valid: {stats['equity']['valid_alerts']} ({stats['equity']['valid_alerts']/max(1, stats['equity']['total_alerts'])*100:.1f}%)
• Invalid: {stats['equity']['invalid_alerts']} ({stats['equity']['invalid_alerts']/max(1, stats['equity']['total_alerts'])*100:.1f}%)
• Forward Validation Success: {stats['equity']['forward_validation']['overall_success_rate']*100:.1f}%
• Success Windows: {stats['equity']['forward_validation']['total_success']} | Failure: {stats['equity']['forward_validation']['total_failure']}

<b>F&O Alerts (Futures & Options):</b>
• Total: {stats['fno']['total_alerts']}
• Valid: {stats['fno']['valid_alerts']} ({stats['fno']['valid_alerts']/max(1, stats['fno']['total_alerts'])*100:.1f}%)
• Invalid: {stats['fno']['invalid_alerts']} ({stats['fno']['invalid_alerts']/max(1, stats['fno']['total_alerts'])*100:.1f}%)
• Forward Validation Success: {stats['fno']['forward_validation']['overall_success_rate']*100:.1f}%
• Success Windows: {stats['fno']['forward_validation']['total_success']} | Failure: {stats['fno']['forward_validation']['total_failure']}

<b>⏰ OVERALL FORWARD VALIDATION RESULTS</b>
• Total Windows Evaluated: {stats['forward_validation']['total_windows']}
• ✅ Success: {stats['forward_validation']['total_success']} ({stats['forward_validation']['overall_success_rate']*100:.1f}%)
• ❌ Failure: {stats['forward_validation']['total_failure']} ({stats['forward_validation']['total_failure']/max(1, stats['forward_validation']['total_windows'])*100:.1f}%)
• ⚠️ Inconclusive: {stats['forward_validation']['total_inconclusive']} ({stats['forward_validation']['total_inconclusive']/max(1, stats['forward_validation']['total_windows'])*100:.1f}%)
• Average Success Ratio: {stats['forward_validation']['avg_success_ratio_per_alert']*100:.1f}%
• Average Max Move: {stats['forward_validation']['avg_max_move_pct']:.2f}%

<b>📊 TOP PATTERNS</b>
"""
        
        # Top patterns
        pattern_dist = stats['pattern_distribution']
        sorted_patterns = sorted(pattern_dist.items(), key=lambda x: x[1], reverse=True)[:5]
        for pattern, count in sorted_patterns:
            msg += f"• {pattern}: {count}\n"
        
        msg += f"\n<b>🎯 TOP SYMBOLS</b>\n"
        # Top symbols
        for symbol, count in list(stats['top_symbols'].items())[:5]:
            msg += f"• {symbol}: {count}\n"
        
        # Alert details (top 10)
        msg += f"\n<b>📋 ALERT DETAILS (Top 10)</b>\n"
        for i, alert in enumerate(self.alerts_95pct[:10], 1):
            status_emoji = "✅" if alert['is_valid'] else "❌"
            msg += f"\n{i}. {status_emoji} <b>{alert['symbol']}</b> - {alert['alert_type']}\n"
            msg += f"   Confidence: {alert['confidence']*100:.0f}%\n"
            msg += f"   Entry: ₹{alert['entry_price']:.2f}\n"
            msg += f"   Forward Success: {alert['success_ratio']*100:.0f}% ({alert['success_count']}/{alert['total_windows']})\n"
            msg += f"   Max Move: {alert['max_move_pct']:.2f}%\n"
        
        msg += f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>🔗 LIVE DASHBOARD ACCESS</b>
📊 <a href="{dashboard_url}">View Real-Time Dashboard</a>

Monitor live alerts, technical indicators, Greeks, and validation results in real-time!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<b>🤝 HELP US GROW!</b>

If you find these reports valuable, please:
• 👥 Share this channel with fellow traders
• 📢 Invite friends who are interested in algo trading
• 💬 Help build our community of serious traders

Together, we can create a powerful trading community!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<i>Report generated at {datetime.now().strftime('%H:%M:%S IST')}</i>
<i>Non-biased analysis • Zero human intervention</i>
"""
        
        return msg
    
    def format_reddit_report(self, stats: Dict) -> tuple:
        """Format professional Reddit report without markdown decorators"""
        report_date = datetime.now().strftime('%Y-%m-%d')
        
        title = f"Daily 95% Confidence Alert Report - {report_date}"
        
        body = f"""Daily 95% Confidence Alert Report
Date: {report_date}
Report Type: Non-biased automated analysis
Generated At: {datetime.now().strftime('%H:%M:%S IST')}

═══════════════════════════════════════════════════════════════

OVERALL STATISTICS

Total Alerts (95%+ confidence): {stats['total_alerts']}
Equity Alerts: {stats['equity']['total_alerts']}
F&O Alerts (Futures/Options): {stats['fno']['total_alerts']}
Valid Signals: {stats['valid_alerts']} ({stats['valid_alerts']/stats['total_alerts']*100:.1f}%)
Invalid Signals: {stats['invalid_alerts']} ({stats['invalid_alerts']/stats['total_alerts']*100:.1f}%)

═══════════════════════════════════════════════════════════════

EQUITY vs F&O BREAKDOWN

EQUITY ALERTS

Total Equity Alerts: {stats['equity']['total_alerts']}
Valid: {stats['equity']['valid_alerts']} ({stats['equity']['valid_alerts']/max(1, stats['equity']['total_alerts'])*100:.1f}%)
Invalid: {stats['equity']['invalid_alerts']} ({stats['equity']['invalid_alerts']/max(1, stats['equity']['total_alerts'])*100:.1f}%)
Forward Validation Success Rate: {stats['equity']['forward_validation']['overall_success_rate']*100:.1f}%
Total Windows Evaluated: {stats['equity']['forward_validation']['total_windows']}
Success Windows: {stats['equity']['forward_validation']['total_success']}
Failure Windows: {stats['equity']['forward_validation']['total_failure']}
Average Max Move: {stats['equity']['forward_validation']['avg_max_move_pct']:.2f}%

F&O ALERTS (FUTURES & OPTIONS)

Total F&O Alerts: {stats['fno']['total_alerts']}
Valid: {stats['fno']['valid_alerts']} ({stats['fno']['valid_alerts']/max(1, stats['fno']['total_alerts'])*100:.1f}%)
Invalid: {stats['fno']['invalid_alerts']} ({stats['fno']['invalid_alerts']/max(1, stats['fno']['total_alerts'])*100:.1f}%)
Forward Validation Success Rate: {stats['fno']['forward_validation']['overall_success_rate']*100:.1f}%
Total Windows Evaluated: {stats['fno']['forward_validation']['total_windows']}
Success Windows: {stats['fno']['forward_validation']['total_success']}
Failure Windows: {stats['fno']['forward_validation']['total_failure']}
Average Max Move: {stats['fno']['forward_validation']['avg_max_move_pct']:.2f}%

═══════════════════════════════════════════════════════════════

FORWARD VALIDATION RESULTS

Total Windows Evaluated: {stats['forward_validation']['total_windows']}
Success: {stats['forward_validation']['total_success']} ({stats['forward_validation']['overall_success_rate']*100:.1f}%)
Failure: {stats['forward_validation']['total_failure']} ({stats['forward_validation']['total_failure']/max(1, stats['forward_validation']['total_windows'])*100:.1f}%)
Inconclusive: {stats['forward_validation']['total_inconclusive']} ({stats['forward_validation']['total_inconclusive']/max(1, stats['forward_validation']['total_windows'])*100:.1f}%)
Average Success Ratio: {stats['forward_validation']['avg_success_ratio_per_alert']*100:.1f}%
Average Max Move: {stats['forward_validation']['avg_max_move_pct']:.2f}%

═══════════════════════════════════════════════════════════════

PATTERN DISTRIBUTION

"""
        
        # Pattern distribution
        pattern_dist = stats['pattern_distribution']
        sorted_patterns = sorted(pattern_dist.items(), key=lambda x: x[1], reverse=True)
        for pattern, count in sorted_patterns:
            body += f"{pattern}: {count} alerts\n"
        
        body += f"\n═══════════════════════════════════════════════════════════════\n\n"
        body += f"TOP SYMBOLS\n\n"
        for symbol, count in list(stats['top_symbols'].items())[:10]:
            body += f"{symbol}: {count} alerts\n"
        
        # Alert details with indicators
        body += f"\n═══════════════════════════════════════════════════════════════\n\n"
        body += f"ALERT DETAILS WITH INDICATORS (All {len(self.alerts_95pct)} Alerts)\n\n"
        for i, alert in enumerate(self.alerts_95pct, 1):
            status_marker = "VALID" if alert['is_valid'] else "INVALID"
            indicators = alert.get('indicators', {})
            
            # Parse timestamp for display
            timestamp_str = alert.get('timestamp', '')
            try:
                if 'T' in timestamp_str:
                    ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    time_display = ts.strftime('%Y-%m-%d %H:%M:%S IST')
                else:
                    time_display = timestamp_str
            except:
                time_display = timestamp_str
            
            body += f"{i}. {status_marker} {alert['symbol']} - {alert['alert_type']}\n\n"
            body += f"Alert Time: {time_display}\n"
            body += f"Confidence: {alert['confidence']*100:.0f}%\n"
            body += f"Entry Price: Rs. {alert['entry_price']:.2f}\n"
            body += f"Signal: {alert['signal']}\n"
            body += f"Forward Validation Success: {alert['success_ratio']*100:.0f}% ({alert['success_count']}/{alert['total_windows']} windows)\n"
            body += f"Max Price Move: {alert['max_move_pct']:.2f}%\n\n"
            
            # Add indicators at that time
            body += f"Technical Indicators at Alert Time:\n\n"
            
            # Technical Indicators
            tech_indicators = []
            if indicators.get('rsi') is not None:
                rsi_val = indicators['rsi']
                tech_indicators.append(f"RSI: {rsi_val:.2f}")
            
            if indicators.get('macd') is not None:
                macd_val = indicators['macd']
                if isinstance(macd_val, dict):
                    tech_indicators.append(f"MACD: {macd_val.get('macd', 0):.2f} | Signal: {macd_val.get('signal', 0):.2f} | Hist: {macd_val.get('histogram', 0):.2f}")
                else:
                    tech_indicators.append(f"MACD: {macd_val:.2f}")
            
            ema_values = []
            for ema_period in [5, 10, 20, 50, 100, 200]:
                ema_key = f'ema_{ema_period}'
                if indicators.get(ema_key) is not None:
                    ema_values.append(f"EMA{ema_period}: {indicators[ema_key]:.2f}")
            if ema_values:
                tech_indicators.append(" | ".join(ema_values))
            
            if indicators.get('atr') is not None:
                tech_indicators.append(f"ATR: {indicators['atr']:.2f}")
            
            if indicators.get('vwap') is not None:
                tech_indicators.append(f"VWAP: ₹{indicators['vwap']:.2f}")
            
            if indicators.get('volume_ratio') is not None:
                tech_indicators.append(f"Volume Ratio: {indicators['volume_ratio']:.2f}x")
            
            # Bollinger Bands
            bb_vals = []
            if indicators.get('bb_upper') is not None:
                bb_vals.append(f"Upper: {indicators['bb_upper']:.2f}")
            if indicators.get('bb_middle') is not None:
                bb_vals.append(f"Middle: {indicators['bb_middle']:.2f}")
            if indicators.get('bb_lower') is not None:
                bb_vals.append(f"Lower: {indicators['bb_lower']:.2f}")
            if bb_vals:
                tech_indicators.append(f"Bollinger Bands: {' | '.join(bb_vals)}")
            
            # Options Greeks
            greeks_vals = []
            for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                if indicators.get(greek) is not None:
                    greeks_vals.append(f"{greek.capitalize()}: {indicators[greek]:.4f}")
            if greeks_vals:
                tech_indicators.append(f"Greeks: {' | '.join(greeks_vals)}")
            
            if tech_indicators:
                for ind_line in tech_indicators:
                    body += f"  - {ind_line}\n"
                body += "\n"
            else:
                body += "  Indicators not available at this time\n\n"
            
            body += f"Alert ID: {alert['alert_id']}\n\n"
            body += "───────────────────────────────────────────────────────────────\n\n"
        
        body += f"""
═══════════════════════════════════════════════════════════════

JOIN OUR TELEGRAM COMMUNITY

Looking for more trading insights and educational content?

Join our Telegram channel: @Options_Trading_For_Beginners
URL: https://t.me/Options_Trading_For_Beginners

Learn options trading from the ground up with beginner-friendly explanations, real-time market analysis, and trading strategies.

═══════════════════════════════════════════════════════════════

DISCLAIMER

This report is automatically generated by the AION Alert Validation System. All data is based on objective metrics with zero human bias. Results are provided for informational purposes only.

METHODOLOGY

- Alerts filtered by 95%+ initial confidence threshold
- Forward validation tracks actual price movements across multiple time windows (1min, 2min, 5min, 10min, 30min, 60min)
- Success is determined by price movement meeting expected threshold (0.1% minimum)
- All statistics are calculated objectively from validated data

Report generated at {datetime.now().strftime('%H:%M:%S IST')}
"""
        
        return title, body
    
    async def publish_to_telegram(self, message: str):
        """Publish report to Telegram (main channel and signal bot channels)"""
        try:
            # Send to main channel
            success_main = await self.telegram_bot._send_message_html(message)
            
            if success_main:
                logger.info("✅ Report sent to main Telegram channel")
            
            # Also send to signal bot channels
            signal_bot_config = self.telegram_bot.config.get('signal_bot', {})
            signal_channels = signal_bot_config.get('chat_ids', [])
            
            for channel in signal_channels:
                try:
                    success = await self.telegram_bot._send_message_html(message, channel)
                    if success:
                        logger.info(f"✅ Report sent to signal bot channel: {channel}")
                except Exception as e:
                    logger.error(f"❌ Failed to send to channel {channel}: {e}")
            
            return success_main
            
        except Exception as e:
            logger.error(f"Error publishing to Telegram: {e}")
            return False
    
    async def publish_to_reddit(self, title: str, body: str):
        """Publish report to Reddit community"""
        try:
            # Try to use Reddit bot if available
            try:
                import sys
                from pathlib import Path
                sys.path.insert(0, str(Path(__file__).parent))
                from community_bots.reddit_bot import AIONRedditBot
                reddit_bot = AIONRedditBot()
                if reddit_bot.reddit and reddit_bot.subreddit:
                    success = await reddit_bot.post_update(title=title, text=body)
                    if success:
                        logger.info("✅ Report posted to Reddit community")
                        return True
                    else:
                        logger.warning("⚠️ Reddit bot post_update returned False")
                else:
                    logger.warning("⚠️ Reddit bot not properly initialized, trying alternative method")
            except ImportError as e:
                logger.warning(f"⚠️ Reddit bot module not found: {e}, trying alternative method")
            except Exception as e:
                logger.warning(f"⚠️ Reddit bot error: {e}, trying alternative method")
            
            # Alternative: Publish via Redis channel (if community manager is running)
            try:
                import redis
                redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
                redis_client.publish('reddit:daily_report', json.dumps({
                    'title': title,
                    'body': body,
                    'timestamp': datetime.now().isoformat()
                }))
                logger.info("✅ Report published to Reddit Redis channel (community manager will handle posting)")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to publish to Reddit channel: {e}")
            
            return False
            
        except Exception as e:
            logger.error(f"Error publishing to Reddit: {e}")
            return False
    
    async def generate_and_publish(self):
        """Generate report and publish to all platforms"""
        logger.info("🚀 Starting daily 95% confidence report generation...")
        
        # Load data
        if not self.load_report_data():
            logger.error("❌ Failed to load report data")
            return False
        
        if not self.alerts_95pct:
            logger.warning("⚠️ No alerts with 95%+ confidence found")
            return False
        
        # Generate statistics
        stats = self.generate_statistics()
        logger.info(f"📊 Generated statistics for {stats['total_alerts']} alerts")
        
        # Format reports
        telegram_msg = self.format_telegram_report(stats)
        reddit_title, reddit_body = self.format_reddit_report(stats)
        
        # Save Reddit report to file for manual publishing
        # Save to everyday_validation_reports directory
        from pathlib import Path
        report_dir = Path("everyday_validation_reports")
        report_dir.mkdir(exist_ok=True)
        report_file = report_dir / f"reddit_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"TITLE:\n{reddit_title}\n\n")
            f.write("="*80 + "\n\n")
            f.write(f"BODY:\n{reddit_body}\n")
        logger.info(f"📄 Reddit report saved to: {report_file}")
        
        # Publish to Telegram
        logger.info("📡 Publishing to Telegram...")
        telegram_success = await self.publish_to_telegram(telegram_msg)
        
        # Publish to Reddit
        logger.info("📡 Publishing to Reddit...")
        reddit_success = await self.publish_to_reddit(reddit_title, reddit_body)
        
        # Summary
        if telegram_success and reddit_success:
            logger.info("✅ Report successfully published to all platforms!")
        elif telegram_success:
            logger.warning("⚠️ Report published to Telegram, but Reddit posting may have failed")
        elif reddit_success:
            logger.warning("⚠️ Report published to Reddit, but Telegram posting may have failed")
        else:
            logger.error("❌ Failed to publish report to any platform")
        
        return telegram_success or reddit_success


async def main():
    """Main execution"""
    import sys
    
    # Get report file path
    if len(sys.argv) > 1:
        report_path = sys.argv[1]
    else:
        # Default to latest report file
        report_path = "alert_validation_report_20251031_154513.json"
    
    if not Path(report_path).exists():
        logger.error(f"❌ Report file not found: {report_path}")
        return
    
    # Check if user wants all alerts (not just 95%+)
    include_all = '--all' in sys.argv or '--include-all' in sys.argv
    confidence_threshold = 0.95
    if '--threshold' in sys.argv:
        try:
            idx = sys.argv.index('--threshold')
            confidence_threshold = float(sys.argv[idx + 1])
        except (IndexError, ValueError):
            pass
    
    reporter = Daily95PercentReport(report_path)
    # Use Redis to get ALL alerts (not just validated ones)
    reporter.load_report_data(include_all_alerts=include_all, confidence_threshold=confidence_threshold, use_redis=True)
    await reporter.generate_and_publish()


if __name__ == "__main__":
    asyncio.run(main())

