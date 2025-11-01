#!/usr/bin/env python3
"""
Standalone Alert Validator for Real-time Backtesting
Uses Redis for alert processing and validates against multiple rolling windows
"""

import json
import time
import logging
import logging.handlers
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
from typing import Any, Dict, List, Optional, Tuple
import threading
from dataclasses import dataclass
import numpy as np
from pathlib import Path
import signal
import sys
import os
import pytz
import asyncio

from redis_files.redis_client import get_redis_client
from config.redis_config import get_database_for_data_type, get_redis_config
from utils.correct_volume_calculator import CorrectVolumeCalculator
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline

@dataclass
class ValidationResult:
    """Results of alert validation"""
    is_valid: bool
    confidence_score: float
    validation_metrics: Dict
    reasons: List[str]
    timestamp: datetime

class AlertValidator:
    def __init__(self, config_path: Optional[str] = None):
        # Setup logging first
        self.setup_logging()
        
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Initialize Redis clients using shared configuration
        redis_config = self.config.get('redis', {})
        datastore_config = self.config.get('datastores', {})
        client_options = {
            "redis_host": redis_config.get('host', 'localhost'),
            "redis_port": redis_config.get('port', 6379),
            "redis_db": redis_config.get('db', 0),
            "redis_password": redis_config.get('password'),
        }

        self.redis = get_redis_client(client_options)
        if not self.redis:
            raise RuntimeError("Unable to initialize RobustRedisClient for alert validation")

        # Primary connection (DB 0) for pub/sub and general reads
        self.redis_client = self.redis.redis_client or self.redis.get_client(0)
        if self.redis_client is None:
            raise RuntimeError("Primary Redis connection unavailable for validator")

        # Map logical datastores to actual Redis databases
        self.datastores = datastore_config
        alerts_data_type = datastore_config.get('alerts', 'pattern_alerts')
        rolling_data_type = datastore_config.get('rolling', 'daily_cumulative')

        try:
            alerts_db_num = get_database_for_data_type(alerts_data_type)
        except Exception:
            alerts_db_num = 1  # Use DB 1 (realtime) for alerts
        try:
            # Use realtime DB for incremental volume/bucket data
            rolling_db_num = get_database_for_data_type("incremental_volume")
        except Exception:
            rolling_db_num = 1  # Fallback to DB 1 (realtime)

        self.alert_store_client = self.redis.get_client(alerts_db_num) or self.redis_client
        self.rolling_store_client = self.redis.get_client(rolling_db_num) or self.redis_client
        self.volume_client = self.rolling_store_client
        self.cumulative_tracker = getattr(self.redis, "cumulative_tracker", None)
        
        # IST timezone for all time operations
        self.ist_tz = pytz.timezone("Asia/Kolkata")
        self.logger.info(f"🌏 Alert Validator initialized with IST timezone: {self.ist_tz}")
        
        # Alert database keys and patterns
        self.alert_keys = {
            'pattern_alerts': 'alert:*',
            'validation_results': 'validation:*',
            'alert_streams': 'alerts:notifications',
            'pending_validation': 'alerts:pending:validation'
        }
        
        # Initialize volume baseline manager
        self.volume_baseline_manager = TimeAwareVolumeBaseline(redis_client=self.redis_client)
        
        self.running = False
        
        # Get configuration values
        self.volume_config = self.config.get('volume_thresholds', {})
        self.validation_config = self.config.get('validation', {})
        self.channels = redis_config.get('channels', {})

        # Forward validation configuration (deferred outcome tracking)
        self.forward_config = self.config.get('forward_validation', {})
        self.forward_enabled = self.forward_config.get('enabled', True)
        self.forward_windows = sorted(
            set(self.forward_config.get('windows_minutes', [1, 2, 5, 10, 30, 60]))
        )
        self.forward_confidence_threshold = float(
            self.forward_config.get('confidence_threshold', 0.9)
        )
        self.forward_success_threshold_pct = float(
            self.forward_config.get('success_threshold_pct', 0.1)
        )
        self.forward_success_ratio_threshold = float(
            self.forward_config.get('success_ratio_threshold', 0.5)
        )
        self.forward_poll_interval = max(
            1, int(self.forward_config.get('poll_interval_seconds', 5))
        )
        self.forward_retry_delay = max(
            1, int(self.forward_config.get('retry_delay_seconds', 15))
        )
        self.forward_max_retries = max(0, int(self.forward_config.get('max_retries', 3)))
        self.forward_state_ttl = int(self.forward_config.get('state_ttl_seconds', 86400))
        self.forward_redis_prefix = self.forward_config.get('redis_prefix', 'forward_validation')
        self.forward_schedule_key = f"{self.forward_redis_prefix}:schedule"
        self.forward_alert_prefix = f"{self.forward_redis_prefix}:alert"
        self.forward_results_key = f"{self.forward_redis_prefix}:results"

        # Combined rolling windows used by legacy metrics + forward validation targets
        base_windows = self.config.get('rolling_windows', [1, 3, 5, 10, 15, 30, 60])
        self.windows = sorted(set(base_windows + self.forward_windows))
        
        # 🎯 CONSOLIDATED VALIDATOR CONFIGURATION
        configured_confidence_threshold = float(
            self.validation_config.get('confidence_threshold', 0.6)
        )
        if self.forward_enabled:
            self.confidence_threshold = max(
                self.forward_confidence_threshold, configured_confidence_threshold
            )
        else:
            self.confidence_threshold = max(0.85, configured_confidence_threshold)
        self.expected_move_accuracy_threshold = 0.6  # 60% accuracy for expected_move validation
        self.window_success_rate_threshold = 0.5  # 50% window success rate
        self.forward_worker_thread: Optional[threading.Thread] = None
        self.enforce_market_hours = bool(self.config.get('enforce_market_hours', True))
        self._market_closed_logged = False
        self._market_closed_sleep_seconds = int(self.config.get('market_closed_sleep_seconds', 5))
        
        # Results storage
        self.validation_results = {}
        
        # Cooldown tracking for symbols (symbol -> last_alert_time)
        self.symbol_cooldowns = {}
        self.cooldown_seconds = self.config.get('cooldown_seconds', 10)  # Configurable cooldown period
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def stop(self):
        """Stop the alert validator gracefully"""
        self.logger.info("Stopping alert validator...")
        self.running = False
        
        # Stop forward validation worker thread if running
        if self.forward_worker_thread and self.forward_worker_thread.is_alive():
            self.logger.info("Waiting for forward validation worker to stop...")
            # Give it a moment to finish current operation
            time.sleep(2)
        
        self.logger.info("Alert validator stopped")
        
    def setup_logging(self):
        """Setup production logging configuration"""
        # Create logs directory if it doesn't exist
        import os
        os.makedirs('logs', exist_ok=True)
        
        # Configure structured logging
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.handlers.RotatingFileHandler(
                    'logs/alert_validator.log',
                    maxBytes=10*1024*1024,  # 10MB
                    backupCount=5
                ),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('AlertValidator')
        
        # Add performance metrics
        self.performance_metrics = {
            'alerts_processed': 0,
            'validations_passed': 0,
            'validations_failed': 0,
            'fallback_validations': 0,
            'errors': 0,
            'start_time': time.time()
        }
    
    def _load_config(self, config_path: Optional[str] = None) -> Dict:
        """Load configuration from JSON file"""
        if config_path is None:
            config_path = Path(__file__).parent / "config" / "validator_config.json"
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                self.logger.info(f"Configuration loaded from {config_path}")
                return config
        except Exception as e:
            self.logger.warning(f"Could not load config from {config_path}: {e}")
            # Return default configuration
            return {
                "redis": {"host": "localhost", "port": 6379, "db": 0},
                "rolling_windows": [1, 3, 5, 10, 15, 30, 60],
                "volume_thresholds": {
                    "LOW": {"vix_max": 15, "volume_spike": 1.54, "volume_breakout": 1.44, "volume_price_divergence": 1.05, "upside_momentum": 0.91, "downside_momentum": 0.91, "breakout": 1.44, "reversal": 1.12, "hidden_accumulation": 1.12},
                    "NORMAL": {"vix_max": 20, "volume_spike": 2.2, "volume_breakout": 1.8, "volume_price_divergence": 1.5, "upside_momentum": 1.3, "downside_momentum": 1.3, "breakout": 1.8, "reversal": 1.6, "hidden_accumulation": 1.6},
                    "HIGH": {"vix_max": 25, "volume_spike": 2.86, "volume_breakout": 2.52, "volume_price_divergence": 1.95, "upside_momentum": 1.69, "downside_momentum": 1.69, "breakout": 2.52, "reversal": 2.08, "hidden_accumulation": 2.08},
                    "PANIC": {"vix_max": 100, "volume_spike": 3.96, "volume_breakout": 3.6, "volume_price_divergence": 2.7, "upside_momentum": 2.34, "downside_momentum": 2.34, "breakout": 3.6, "reversal": 2.88, "hidden_accumulation": 2.88}
                },
                "validation": {"confidence_threshold": 0.6, "min_windows_required": 3},
                "forward_validation": {
                    "enabled": True,
                    "confidence_threshold": 0.9,
                    "windows_minutes": [1, 2, 5, 10, 30, 60],
                    "success_threshold_pct": 0.1,
                    "success_ratio_threshold": 0.5,
                    "poll_interval_seconds": 5,
                    "retry_delay_seconds": 15,
                    "max_retries": 3,
                    "state_ttl_seconds": 86400,
                    "redis_prefix": "forward_validation"
                }
            }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.stop()
        sys.exit(0)
    
    def get_performance_metrics(self) -> Dict:
        """Get current performance metrics"""
        uptime = time.time() - self.performance_metrics['start_time']
        return {
            **self.performance_metrics,
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'alerts_per_minute': self.performance_metrics['alerts_processed'] / (uptime / 60) if uptime > 0 else 0,
            'success_rate': self.performance_metrics['validations_passed'] / max(1, self.performance_metrics['alerts_processed'])
        }
    
    def _standardize_volume_field(self, bucket_data: Dict) -> Tuple[int, int]:
        """Extract bucket_incremental_volume using the new bucket_incremental_volume architecture"""
        try:
            # Use the new bucket_incremental_volume resolver for consistent field resolution
            bucket_incremental_volume = int(CorrectVolumeCalculator.get_incremental_volume(bucket_data))
            cumulative = int(CorrectVolumeCalculator.get_cumulative_volume(bucket_data))
            
            # Use cumulative as count if available, otherwise default to 1
            count = max(1, cumulative) if cumulative > 0 else 1
            
            return bucket_incremental_volume, count
            
        except Exception as e:
            self.logger.error(f"Failed to extract bucket_incremental_volume from bucket: {e}")
            return 0, 1
    
    def get_current_vix_regime(self) -> str:
        """Get current VIX regime from Redis"""
        try:
            vix_value = self.redis_client.get('indian_vix:current')
            if vix_value:
                vix = float(vix_value)
                for regime, config in self.volume_config.items():
                    if vix <= config['vix_max']:
                        return regime
            return 'NORMAL'  # Default fallback
        except Exception as e:
            self.logger.error(f"Error getting VIX regime: {e}")
            return 'NORMAL'
    
    def get_volume_threshold(self, alert_type: str) -> float:
        """Get bucket_incremental_volume threshold based on current VIX regime and alert type using centralized config"""
        try:
            from config.thresholds import get_volume_threshold
            regime = self.get_current_vix_regime()
            return get_volume_threshold(alert_type, None, regime)
        except ImportError:
            # Fallback to hardcoded values if centralized config unavailable
            regime = self.get_current_vix_regime()
            return self.volume_config[regime].get(alert_type, 2.0)
        except Exception as e:
            self.logger.warning(f"Error getting centralized threshold for {alert_type}: {e}")
            # Fallback to hardcoded values
            regime = self.get_current_vix_regime()
            return self.volume_config[regime].get(alert_type, 2.0)
    
    def get_confidence_threshold(self, alert_type: str) -> float:
        """Get confidence threshold using centralized config"""
        try:
            from config.thresholds import get_confidence_threshold
            regime = self.get_current_vix_regime()
            return get_confidence_threshold(alert_type, regime)
        except ImportError:
            # Fallback to hardcoded values if centralized config unavailable
            return 0.7  # Default confidence threshold
        except Exception as e:
            self.logger.warning(f"Error getting centralized confidence threshold for {alert_type}: {e}")
            return 0.7  # Default confidence threshold
    
    def get_rolling_metrics(self, symbol: str, windows: List[int]) -> Dict:
        """Get rolling window metrics from Redis using actual system key patterns"""
        metrics = {}
        try:
            for window in windows:
                # Try to get pre-calculated rolling metrics first
                key = f"metrics:{symbol}:{window}min"
                store_client = self.rolling_store_client or self.redis_client
                window_data = store_client.get(key) if store_client else None
                
                if window_data:
                    metrics[window] = json.loads(window_data)
                    self.logger.debug(f"✅ Found pre-calculated metrics for {symbol} {window}min")
                else:
                    # Calculate from bucket_incremental_volume buckets using actual system patterns
                    metrics[window] = self._calculate_from_volume_buckets(symbol, window)
                    if metrics[window]:
                        self.logger.debug(f"✅ Calculated metrics for {symbol} {window}min from buckets")
                    else:
                        self.logger.debug(f"❌ No metrics available for {symbol} {window}min")
                    
        except Exception as e:
            self.logger.error(f"Error getting rolling metrics for {symbol}: {e}")
            
        return metrics
    
    def _get_bucket_volume_baseline(self, symbol: str, bucket_label: str = "5min") -> float:
        """Get bucket-specific bucket_incremental_volume baseline using VolumeBaselineManager"""
        try:
            # Get 20-day average bucket_incremental_volume as fallback
            avg_volume_20d = self._get_avg_volume_20d(symbol)
            fallback_volume = avg_volume_20d or 0.0
            
            # Get bucket-specific baseline
            baseline = self.volume_baseline_manager.get_baseline(symbol, bucket_label, fallback_volume)
            return baseline
            
        except Exception as e:
            self.logger.debug(f"Failed to get bucket baseline for {symbol}: {e}")
            return 0.0
    
    def _get_underlying_symbol(self, symbol: str) -> str:
        """Map option symbols to their underlying symbols for bucket data lookup."""
        # Extract underlying symbol from option symbols - check BANKNIFTY first
        if "BANKNIFTY" in symbol.upper():
            return "BANKNIFTY"
        elif "NIFTY" in symbol.upper():
            return "NIFTY"
        elif "FINNIFTY" in symbol.upper():
            return "FINNIFTY"
        elif "MIDCPNIFTY" in symbol.upper():
            return "MIDCPNIFTY"
        else:
            # For other symbols, try to extract the base symbol
            # Remove common option suffixes
            base_symbol = symbol
            for suffix in ["25NOV", "25DEC", "25JAN", "25FEB", "25MAR", "25APR", "25MAY", "25JUN", 
                          "25JUL", "25AUG", "25SEP", "25OCT", "CE", "PE", "FUT"]:
                if base_symbol.endswith(suffix):
                    base_symbol = base_symbol[:-len(suffix)]
                    break
            return base_symbol

    def _calculate_from_volume_buckets(self, symbol: str, window_minutes: int) -> Dict:
        """Calculate rolling metrics by directly accessing Redis bucket data."""
        try:
            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            cutoff_ts = (now - timedelta(minutes=window_minutes)).timestamp()
            
            # Get underlying symbol for bucket lookup
            underlying_symbol = self._get_underlying_symbol(symbol)
            
            # Get all bucket keys for this underlying symbol
            bucket_pattern = f"bucket_incremental_volume:bucket_incremental_volume:bucket*:{underlying_symbol}:buckets:*"
            self.logger.info(f"🔍 Looking for bucket pattern: {bucket_pattern}")
            self.logger.info(f"🔍 Using volume_client: {type(self.volume_client)}")
            self.logger.info(f"🔍 Volume client database: {self.volume_client.connection_pool.connection_kwargs.get('db', 'unknown')}")
            bucket_keys = self.volume_client.keys(bucket_pattern)
            
            self.logger.info(f"🔍 Found {len(bucket_keys)} bucket keys for {symbol} (underlying: {underlying_symbol})")
            if bucket_keys:
                self.logger.info(f"🔍 Sample bucket keys: {bucket_keys[:3]}")
            else:
                self.logger.warning(f"❌ No bucket keys found for pattern: {bucket_pattern}")
            
            volumes: List[float] = []
            counts: List[int] = []
            timestamps: List[float] = []

            for bucket_key in bucket_keys:
                try:
                    # Get bucket data from Redis
                    bucket_data = self.volume_client.hgetall(bucket_key)
                    if not bucket_data:
                        continue
                    
                    # Parse timestamps
                    first_ts_str = bucket_data.get('first_timestamp', '')
                    last_ts_str = bucket_data.get('last_timestamp', '')
                    
                    if first_ts_str:
                        # Parse ISO timestamp
                        first_ts = datetime.fromisoformat(first_ts_str.replace('+05:30', '')).timestamp()
                        # For testing, use a more lenient time window (2 hours instead of 30 minutes)
                        test_cutoff_ts = (now - timedelta(hours=2)).timestamp()
                        if first_ts < test_cutoff_ts:
                            continue
                    
                    # Get volume data
                    bucket_incremental_volume = bucket_data.get('bucket_incremental_volume')
                    if not bucket_incremental_volume:
                        continue
                    
                    volumes.append(float(bucket_incremental_volume))
                    counts.append(int(bucket_data.get('count', 1)))
                    
                    if first_ts_str:
                        timestamps.append(first_ts)
                        
                except Exception as e:
                    self.logger.debug(f"Error processing bucket {bucket_key}: {e}")
                    continue

            if not volumes:
                return {}

            volume_array = np.array(volumes, dtype=float)
            metrics = {
                "volume_mean": float(np.mean(volume_array)),
                "volume_std": float(np.std(volume_array)) if len(volume_array) > 1 else 0.0,
                "volume_sum": float(np.sum(volume_array)),
                "volume_change": float(volume_array[-1] - volume_array[0]) if len(volume_array) > 1 else 0.0,
                "trade_count": int(sum(counts)),
                "bucket_count": len(volumes),
                "window_minutes": window_minutes,
            }

            if timestamps:
                metrics["first_timestamp"] = float(min(timestamps))
                metrics["last_timestamp"] = float(max(timestamps))

            self.logger.debug(
                "📈 Calculated rolling metrics for %s (%s min): mean=%.0f sum=%.0f buckets=%d",
                symbol,
                window_minutes,
                metrics["volume_mean"],
                metrics["volume_sum"],
                metrics["bucket_count"],
            )
            return metrics

        except Exception as e:
            self.logger.error(f"Error calculating bucket_incremental_volume metrics for {symbol}: {e}")
            return {}
    
    def calculate_rolling_metrics(self, symbol: str, window_minutes: int) -> Dict:
        """Calculate rolling metrics from recent trade data"""
        try:
            # Get recent trades from Redis
            trades_key = f"trades:{symbol}"
            store_client = self.rolling_store_client or self.redis_client
            recent_trades = store_client.lrange(trades_key, 0, -1) if store_client else []
            
            if not recent_trades:
                return {}
                
            # Parse trades and calculate metrics
            trades = [json.loads(trade) for trade in recent_trades]
            df = pd.DataFrame(trades)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            
            # Filter for the rolling window
            cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
            window_trades = df[df.index >= cutoff_time]
            
            if len(window_trades) == 0:
                return {}
                
            metrics = {
                'volume_mean': float(window_trades['bucket_incremental_volume'].mean()),
                'volume_std': float(window_trades['bucket_incremental_volume'].std()),
                'price_change': float(window_trades['last_price'].iloc[-1] - window_trades['last_price'].iloc[0]),
                'price_volatility': float(window_trades['last_price'].std()),
                'trade_count': len(window_trades),
                'volume_sum': float(window_trades['bucket_incremental_volume'].sum())
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics for {symbol}: {e}")
            return {}
    
    def load_indicators_from_redis(self, symbol: str) -> Dict[str, Any]:
        """
        Load technical indicators from Redis for enhanced validation.
        
        Redis Storage Schema (from redis_storage.py):
        - DB 1 (realtime): indicators:{symbol}:{indicator_name}
        - Format: Simple indicators (RSI, EMA, ATR, VWAP) as string/number
                  Complex indicators (MACD, BB) as JSON with {'value': {...}, ...}
        - Fallback DBs: DB 4 and DB 5
        """
        indicators = {}
        try:
            # Get realtime client (DB 1)
            redis_db1 = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis_client
            if not redis_db1:
                return indicators
            
            # Normalize symbol
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # Indicator names (from HybridCalculations)
            indicator_names = ['rsi', 'atr', 'vwap', 'ema_20', 'ema_50', 'macd', 'bollinger_bands']
            
            for variant in symbol_variants:
                for indicator_name in indicator_names:
                    redis_key = f"indicators:{variant}:{indicator_name}"
                    try:
                        value = redis_db1.get(redis_key)
                        if value:
                            # Handle bytes
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            
                            # Parse JSON for complex indicators
                            try:
                                parsed = json.loads(value)
                                if isinstance(parsed, dict):
                                    indicators[indicator_name] = parsed.get('value', parsed)
                                else:
                                    indicators[indicator_name] = parsed
                            except (json.JSONDecodeError, TypeError):
                                # Simple numeric value
                                try:
                                    indicators[indicator_name] = float(value)
                                except (ValueError, TypeError):
                                    indicators[indicator_name] = value
                        if indicator_name in indicators:
                            break  # Found indicator for this variant
                    except Exception:
                        continue
                if indicators:
                    break  # Found indicators for this variant
        except Exception as e:
            logger.debug(f"Error loading indicators from Redis for {symbol}: {e}")
        
        return indicators
    
    def load_greeks_from_redis(self, symbol: str) -> Dict[str, Any]:
        """
        Load Options Greeks from Redis for enhanced validation.
        
        Redis Storage Schema (from redis_storage.py):
        - DB 1 (realtime): indicators:{symbol}:greeks (combined) or indicators:{symbol}:{greek} (individual)
        - Format: Combined as JSON with {'value': {'delta': ..., ...}, ...}
                  Individual as float string
        - Fallback DBs: DB 4 and DB 5
        """
        greeks = {}
        try:
            # Only for F&O instruments
            if not any(x in symbol.upper() for x in ['CE', 'PE', 'FUT']):
                return greeks
            
            # Get realtime client (DB 1)
            redis_db1 = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis_client
            if not redis_db1:
                return greeks
            
            # Normalize symbol
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            for variant in symbol_variants:
                # Try combined greeks first
                greeks_key = f"indicators:{variant}:greeks"
                try:
                    greeks_data = redis_db1.get(greeks_key)
                    if greeks_data:
                        if isinstance(greeks_data, bytes):
                            greeks_data = greeks_data.decode('utf-8')
                        parsed = json.loads(greeks_data) if isinstance(greeks_data, str) else greeks_data
                        if isinstance(parsed, dict):
                            greeks.update(parsed.get('value', parsed))
                        break
                except Exception:
                    pass
                
                # Try individual Greeks
                for greek_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                    greek_key = f"indicators:{variant}:{greek_name}"
                    try:
                        greek_value = redis_db1.get(greek_key)
                        if greek_value:
                            if isinstance(greek_value, bytes):
                                greek_value = greek_value.decode('utf-8')
                            greeks[greek_name] = float(greek_value)
                    except Exception:
                        continue
                
                if greeks:
                    break
        except Exception as e:
            logger.debug(f"Error loading Greeks from Redis for {symbol}: {e}")
        
        return greeks
    
    def validate_volume_alert(self, alert_data: Dict) -> ValidationResult:
        """Pure Redis-based validation - completely independent from main system"""
        symbol = alert_data.get('symbol', 'UNKNOWN')
        alert_type = alert_data.get('pattern', 'unknown')
        
        try:
            # Track performance
            self.performance_metrics['alerts_processed'] += 1
            
            # Get data directly from Redis
            rolling_metrics = self.get_rolling_metrics(symbol, self.windows)
            vix_regime = self.get_current_vix_regime()
            volume_threshold = self.get_volume_threshold(alert_type)
            
            # Get volume data from Redis
            volume_ratio = alert_data.get('volume_ratio', 1.0)
            last_price = alert_data.get('last_price', 0.0)
            price_change = alert_data.get('price_change', 0.0)
            
            # ✅ OPTIONAL: Load indicators/Greeks from Redis for enhanced validation
            # These can be used for additional validation criteria if needed
            indicators = self.load_indicators_from_redis(symbol)
            greeks = self.load_greeks_from_redis(symbol)
            
            # Simple Redis-based validation logic
            confidence_score = self._calculate_redis_based_confidence(
                alert_data, rolling_metrics, volume_threshold, vix_regime
            )
            
            # Determine if alert is valid
            is_valid = confidence_score >= self.validation_config.get('confidence_threshold', 0.6)
            
            if is_valid:
                self.performance_metrics['validations_passed'] += 1
            else:
                self.performance_metrics['validations_failed'] += 1
            
            # Create reasons (include indicators/Greeks if available)
            reasons = [
                f"🎯 REDIS-BASED VALIDATION for {symbol} - {alert_type}",
                f"Volume ratio: {volume_ratio:.2f}x (threshold: {volume_threshold:.2f})",
                f"VIX regime: {vix_regime}",
                f"Price change: {price_change:.2f}",
                f"Confidence: {confidence_score:.3f}",
            ]
            
            # Add indicators if available
            if indicators:
                indicator_summary = []
                if indicators.get('rsi'):
                    indicator_summary.append(f"RSI: {indicators['rsi']:.1f}")
                if indicators.get('atr'):
                    indicator_summary.append(f"ATR: {indicators['atr']:.2f}")
                if indicators.get('vwap'):
                    indicator_summary.append(f"VWAP: {indicators['vwap']:.2f}")
                if indicator_summary:
                    reasons.append(f"Indicators: {', '.join(indicator_summary)}")
            
            # Add Greeks if available
            if greeks:
                greek_summary = []
                if greeks.get('delta'):
                    greek_summary.append(f"Δ: {greeks['delta']:.3f}")
                if greeks.get('gamma'):
                    greek_summary.append(f"Γ: {greeks['gamma']:.4f}")
                if greek_summary:
                    reasons.append(f"Greeks: {', '.join(greek_summary)}")
            
            reasons.append(f"📊 DECISION: {'✅ VALID' if is_valid else '❌ REJECTED'}")
            
            # Store validation result
            validation_metrics = {
                'vix_regime': vix_regime, 
                'volume_threshold': volume_threshold,
                'redis_based_validation': True,
                'independent_validator': True,
                'statistical_model_ready': True
            }
            
            result = ValidationResult(
                is_valid=is_valid,
                confidence_score=confidence_score,
                validation_metrics=validation_metrics,
                reasons=reasons,
                timestamp=datetime.now()
            )
            
            # Store result in Redis for statistical model building
            self._store_validation_result(symbol, result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error validating volume alert for {symbol}: {e}")
            self.performance_metrics['errors'] += 1
            return self._fallback_validation(alert_data)
            
    def _calculate_redis_based_confidence(self, alert_data: Dict, rolling_metrics: Dict, 
                                        volume_threshold: float, vix_regime: str) -> float:
        """Calculate confidence using only Redis data - adapts to available data"""
        try:
            symbol = alert_data.get('symbol', 'UNKNOWN')
            volume_ratio = alert_data.get('volume_ratio', 1.0)
            price_change = alert_data.get('price_change', 0.0)
            confidence = alert_data.get('confidence', 0.0)
            
            # Count valid rolling windows
            valid_windows = sum(1 for metrics in rolling_metrics.values() if metrics and metrics.get('bucket_count', 0) > 0)
            total_windows = len(self.windows)
            
            # If we don't have adequate rolling data, weight volume and confidence more heavily
            if valid_windows < total_windows * 0.5:  # Less than 50% of windows have data
                # Use volume-based confidence when rolling data is insufficient
                volume_score = min(1.0, volume_ratio / volume_threshold) if volume_threshold > 0 else 0.5
                
                # Trust the original confidence calculation from pattern detector
                adjusted_confidence = confidence * volume_score
                
                self.logger.debug(f"Insufficient rolling data ({valid_windows}/{total_windows}), using volume-weighted confidence: {adjusted_confidence:.3f}")
                return max(0.05, min(0.95, adjusted_confidence))
            
            # Base confidence from volume ratio vs threshold
            volume_score = min(1.0, volume_ratio / volume_threshold) if volume_threshold > 0 else 0.5
            
            # Price movement score
            price_score = min(1.0, abs(price_change) / 0.01) if price_change != 0 else 0.3  # 1% = full score
            
            # Rolling metrics score
            rolling_score = 0.5  # Default
            if rolling_metrics:
                valid_windows = sum(1 for metrics in rolling_metrics.values() if metrics)
                rolling_score = min(1.0, valid_windows / len(self.windows))
            
            # VIX regime adjustment
            vix_multipliers = {
                'LOW': 0.8,    # Lower confidence in low VIX
                'NORMAL': 1.0, # Normal confidence
                'HIGH': 1.2,   # Higher confidence in high VIX
                'PANIC': 1.5   # Highest confidence in panic
            }
            vix_multiplier = vix_multipliers.get(vix_regime, 1.0)
            
            # Calculate final confidence
            base_confidence = (volume_score * 0.4 + price_score * 0.3 + rolling_score * 0.3)
            final_confidence = min(0.95, base_confidence * vix_multiplier * confidence)
            
            return max(0.05, final_confidence)  # Ensure minimum confidence
            
        except Exception as e:
            self.logger.error(f"Error calculating Redis-based confidence: {e}")
            return 0.5  # Default confidence
    
    def _store_validation_result(self, symbol: str, result: ValidationResult):
        """Store validation result in Redis for tracking"""
        try:
            alert_id = f"{symbol}:{int(result.timestamp.timestamp())}"
            result_data = {
                'alert_id': alert_id,
                'symbol': symbol,
                'is_valid': result.is_valid,
                'confidence_score': result.confidence_score,
                'timestamp': result.timestamp.isoformat(),
                'reasons': result.reasons[:3]  # Store first 3 reasons
            }

            # Persist via shared Redis helper (DB 6)
            self.redis.store_validation_result(alert_id, result_data)
            
            # Maintain rolling list of recent validations
            if self.alert_store_client:
                self.alert_store_client.lpush('validation_results:recent', json.dumps(result_data))
                self.alert_store_client.ltrim('validation_results:recent', 0, 99)
            
        except Exception as e:
            self.logger.error(f"Error storing validation result: {e}")
    
    def _fallback_validation(self, alert_data: Dict) -> ValidationResult:
        """Fallback validation when rolling metrics are not available"""
        symbol = alert_data.get('symbol', 'UNKNOWN')
        volume_ratio = alert_data.get('volume_ratio', 1.0)
        confidence = alert_data.get('confidence', 0.0)
        expected_move = alert_data.get('expected_move', 0.0)
        
        reasons = []
        confidence_score = 0.0
        validation_metrics = {}
        
        # Dynamic validation based on actual alert characteristics
        # Remove static confidence values and use dynamic calculation
        
        # Volume-based scoring (dynamic)
        if volume_ratio >= 2.0:
            volume_score = 1.0  # Excellent bucket_incremental_volume
        elif volume_ratio >= 1.5:
            volume_score = 0.8  # Good bucket_incremental_volume
        elif volume_ratio >= 1.2:
            volume_score = 0.6  # Moderate bucket_incremental_volume
        elif volume_ratio >= 1.0:
            volume_score = 0.4  # Baseline bucket_incremental_volume
        else:
            volume_score = 0.2  # Low bucket_incremental_volume
        
        # Move-based scoring (dynamic)
        if expected_move > 0:
            if expected_move >= 0.5:
                move_score = 1.0  # Strong move
            elif expected_move >= 0.3:
                move_score = 0.8  # Good move
            elif expected_move >= 0.15:
                move_score = 0.6  # Moderate move
            else:
                move_score = 0.4  # Weak move
        else:
            move_score = 0.3  # No expected move
        
        # Dynamic confidence calculation based on actual metrics
        # Weight bucket_incremental_volume more heavily as it's more reliable
        confidence_score = (volume_score * 0.6) + (move_score * 0.4)
        
        # Apply alert's own confidence as a multiplier (not static)
        confidence_score *= confidence
        
        # Store metrics
        validation_metrics['volume_ratio'] = volume_ratio
        validation_metrics['alert_confidence'] = confidence
        validation_metrics['expected_move'] = expected_move
        validation_metrics['fallback_validation'] = True
        validation_metrics['volume_score'] = volume_score
        validation_metrics['move_score'] = move_score
        
        # Use the same threshold as main validation from config
        threshold = self.validation_config.get('confidence_threshold', 0.6)
        is_valid = confidence_score >= threshold
        
        if is_valid:
            reasons.append(f"Fallback validation passed (bucket_incremental_volume: {volume_ratio:.1f}x, confidence: {confidence:.2f})")
        else:
            reasons.append(f"Fallback validation failed (bucket_incremental_volume: {volume_ratio:.1f}x, confidence: {confidence:.2f})")
        
        return ValidationResult(
            is_valid=is_valid,
            confidence_score=confidence_score,
            validation_metrics=validation_metrics,
            reasons=reasons,
            timestamp=datetime.now()
        )
    
    def validate_price_movement(self, alert_data: Dict, rolling_metrics: Dict) -> float:
        """Validate last_price movement consistency"""
        try:
            symbol = alert_data.get('symbol')
            last_price = alert_data.get('last_price')
            
            # Check last_price movement across windows
            consistent_movement = 0
            total_windows = 0
            
            for window, metrics in rolling_metrics.items():
                if not metrics:
                    continue
                    
                price_change = metrics.get('price_change', 0)
                price_volatility = metrics.get('price_volatility', 1)
                
                # Normalized last_price movement score
                movement_score = abs(price_change) / price_volatility if price_volatility > 0 else 0
                
                if movement_score > 0.5:  # Significant movement
                    consistent_movement += 1
                    
                total_windows += 1
            
            return consistent_movement / total_windows if total_windows > 0 else 0
            
        except Exception as e:
            self.logger.error(f"Error validating last_price movement: {e}")
            return 0
    
    def validate_expected_move_in_rolling_windows(self, symbol: str, expected_move: float, rolling_metrics: Dict) -> Dict:
        """Validate expected_move against rolling windows for accuracy"""
        try:
            if expected_move <= 0:
                return {
                    'is_valid': False,
                    'confidence': 0.0,
                    'accuracy': 0.0,
                    'reason': 'No expected move provided'
                }
            
            # Analyze expected_move accuracy across rolling windows
            window_accuracy = []
            total_windows = 0
            valid_windows = 0
            
            for window, metrics in rolling_metrics.items():
                if not metrics:
                    continue
                    
                total_windows += 1
                
                # Get actual price movement from rolling window
                price_change = abs(metrics.get('price_change', 0))
                price_volatility = metrics.get('price_volatility', 1)
                
                if price_volatility > 0:
                    # Calculate accuracy: how close actual movement is to expected
                    actual_move_ratio = price_change / price_volatility
                    expected_move_ratio = expected_move
                    
                    # Accuracy score (0-1): closer to 1 means more accurate
                    accuracy = 1.0 - abs(actual_move_ratio - expected_move_ratio) / max(actual_move_ratio, expected_move_ratio)
                    accuracy = max(0.0, min(1.0, accuracy))  # Clamp to 0-1
                    
                    window_accuracy.append(accuracy)
                    
                    if accuracy >= self.expected_move_accuracy_threshold:  # Use configured accuracy threshold
                        valid_windows += 1
            
            if not window_accuracy:
                return {
                    'is_valid': False,
                    'confidence': 0.0,
                    'accuracy': 0.0,
                    'reason': 'No valid rolling windows for expected_move validation'
                }
            
            # Calculate overall accuracy and confidence
            overall_accuracy = sum(window_accuracy) / len(window_accuracy)
            window_success_rate = valid_windows / total_windows if total_windows > 0 else 0
            
            # Combined confidence: accuracy + success rate
            confidence = (overall_accuracy * 0.7) + (window_success_rate * 0.3)
            
            # Validation criteria: use configured thresholds
            is_valid = overall_accuracy >= self.expected_move_accuracy_threshold and window_success_rate >= self.window_success_rate_threshold
            
            return {
                'is_valid': is_valid,
                'confidence': confidence,
                'accuracy': overall_accuracy,
                'window_success_rate': window_success_rate,
                'valid_windows': valid_windows,
                'total_windows': total_windows,
                'reason': f'Expected move validation: {overall_accuracy:.2f} accuracy, {window_success_rate:.2f} success rate'
            }
            
        except Exception as e:
            self.logger.error(f"Error validating expected_move for {symbol}: {e}")
            return {
                'is_valid': False,
                'confidence': 0.0,
                'accuracy': 0.0,
                'reason': f'Validation error: {str(e)}'
            }
    
    def process_alert(self, alert_data: Dict) -> Dict:
        """Process a single alert and return validation results"""
        validation_result = self.validate_volume_alert(alert_data)
        
        result = {
            'alert_id': alert_data.get('alert_id', 'unknown'),
            'symbol': alert_data.get('symbol'),
            'alert_type': alert_data.get('alert_type'),
            'timestamp': alert_data.get('timestamp'),
            'validation_result': {
                'is_valid': validation_result.is_valid,
                'confidence_score': validation_result.confidence_score,
                'reasons': validation_result.reasons,
                'metrics': validation_result.validation_metrics
            },
            'processed_at': datetime.now().isoformat()
        }
        
        # Store result in Redis
        self.store_validation_result(result)
        
        return result
    
    def process_alert_with_expected_move(self, alert_data: Dict) -> Dict:
        """Enhanced alert processing with expected_move validation in rolling windows"""
        symbol = alert_data.get('symbol', 'UNKNOWN')
        expected_move = alert_data.get('expected_move', 0.0)
        
        self.logger.info(f"🎯 PROCESSING ALERT WITH EXPECTED MOVE: {symbol} (expected_move: {expected_move:.3f})")
        
        # Get rolling metrics for expected_move validation
        rolling_metrics = self.get_rolling_metrics(symbol, self.windows)
        
        # Validate expected_move against rolling windows
        expected_move_validation = self.validate_expected_move_in_rolling_windows(
            symbol, expected_move, rolling_metrics
        )
        
        # Standard volume validation
        validation_result = self.validate_volume_alert(alert_data)
        
        # Combine results
        combined_confidence = (validation_result.confidence_score + expected_move_validation['confidence']) / 2
        combined_valid = validation_result.is_valid and expected_move_validation['is_valid']
        
        result = {
            'alert_id': alert_data.get('alert_id', 'unknown'),
            'symbol': alert_data.get('symbol'),
            'alert_type': alert_data.get('alert_type'),
            'timestamp': alert_data.get('timestamp'),
            'expected_move': expected_move,
            'validation_result': {
                'is_valid': combined_valid,
                'confidence_score': combined_confidence,
                'volume_validation': {
                    'is_valid': validation_result.is_valid,
                    'confidence_score': validation_result.confidence_score,
                    'reasons': validation_result.reasons
                },
                'expected_move_validation': expected_move_validation,
                'combined_metrics': {
                    'rolling_windows_analyzed': len(rolling_metrics),
                    'expected_move_accuracy': expected_move_validation['accuracy'],
                    'volume_confidence': validation_result.confidence_score,
                    'expected_move_confidence': expected_move_validation['confidence']
                }
            },
            'processed_at': datetime.now().isoformat()
        }
        
        return result
    
    def store_validation_result(self, result: Dict):
        """Store validation result in Redis"""
        try:
            alert_id = result.get('alert_id') or f"{result.get('symbol', 'unknown')}:{int(time.time())}"
            self.redis.store_validation_result(alert_id, result)

            if self.alert_store_client:
                recent_key = "validation_results:recent"
                self.alert_store_client.lpush(recent_key, json.dumps(result))
                self.alert_store_client.ltrim(recent_key, 0, 99)
            
        except Exception as e:
            self.logger.error(f"Error storing validation result: {e}")
    
    def start_alert_consumer(self):
        """Start consuming alerts from Redis pub/sub with deferred forward validation."""
        self.running = True
        if self.forward_enabled:
            self._ensure_forward_worker()

        pubsub = self.redis_client.pubsub()
        # Listen to the configured input channel
        input_channel = self.channels.get('alerts_input', 'alerts:notifications')
        pubsub.subscribe(input_channel)
        
        self.logger.info(f"Started consolidated alert validator - listening to '{input_channel}' channel")
        self.logger.info(f"🎯 FILTERING: Only processing alerts with {self.confidence_threshold*100:.0f}%+ confidence")
        if self.forward_enabled:
            self.logger.info(
                f"⏩ Forward validation enabled - tracking windows {self.forward_windows} minutes"
            )
        else:
            self.logger.info(
                f"🎯 EXPECTED MOVE VALIDATION: {self.expected_move_accuracy_threshold*100:.0f}% accuracy, "
                f"{self.window_success_rate_threshold*100:.0f}% window success rate"
            )
        
        for message in pubsub.listen():
            if not self.running:
                break
                
            if message['type'] == 'message':
                try:
                    alert_data = json.loads(message['data'])
                    symbol = alert_data.get('symbol', 'UNKNOWN')
                    # Support both 'pattern' and 'pattern_type' field names
                    pattern = alert_data.get('pattern') or alert_data.get('pattern_type', 'UNKNOWN')
                    # Support both 'confidence' and 'confidence_score' field names
                    confidence = alert_data.get('confidence') or alert_data.get('confidence_score', 0.0)

                    if self.enforce_market_hours and not self._is_within_market_hours():
                        if not self._market_closed_logged:
                            next_open = self._next_market_open()
                            self.logger.info(
                                "🛑 Market is closed (skipping alerts). Next session starts at %s IST",
                                next_open.strftime("%Y-%m-%d %H:%M"),
                            )
                            self._market_closed_logged = True
                        time.sleep(self._market_closed_sleep_seconds)
                        continue
                    else:
                        self._market_closed_logged = False
                    
                    # 🎯 85% CONFIDENCE FILTER
                    if confidence < self.confidence_threshold:
                        self.logger.debug(f"🚫 LOW CONFIDENCE: Skipping {symbol} {pattern} (confidence: {confidence:.2f} < {self.confidence_threshold})")
                        continue
                    
                    # Check cooldown before processing
                    if not self._check_symbol_cooldown(symbol):
                        self.logger.info(f"🚫 COOLDOWN: Skipping alert for {symbol} - still in cooldown period")
                        continue
                    
                    self.logger.info(f"✅ HIGH CONFIDENCE ALERT: {symbol} {pattern} (confidence: {confidence:.2f})")
                    
                    # Convert alert data to validator format
                    validator_alert = self._convert_alert_format(alert_data)

                    if self.forward_enabled:
                        state = self._schedule_forward_validation(validator_alert)
                        if state:
                            self.logger.info(
                                "🗓️ Scheduled forward validation for %s %s across %d windows",
                                symbol,
                                pattern,
                                len(self.forward_windows),
                            )
                        else:
                            self.logger.warning(
                                "⚠️ Failed to schedule forward validation for %s %s, "
                                "falling back to immediate processing",
                                symbol,
                                pattern,
                            )
                            result = self.process_alert_with_expected_move(validator_alert)
                            self._post_process_validation_result(result)
                    else:
                        # Legacy immediate validation flow
                        result = self.process_alert_with_expected_move(validator_alert)
                        self._post_process_validation_result(result)
                    
                    # Update cooldown for this symbol
                    self._update_symbol_cooldown(symbol)
                    
                except Exception as e:
                    self.logger.error(f"Error processing alert message: {e}")
    
    def _post_process_validation_result(self, result: Dict):
        """Persist and publish a completed validation result."""
        try:
            self.store_validation_result(result)

            output_channel = self.channels.get('validation_output', 'validation_results')
            self.redis_client.publish(output_channel, json.dumps(result))

            validation_confidence = result.get('validation_result', {}).get('confidence_score', 0.0)
            publish_threshold = max(0.9, self.confidence_threshold)
            if validation_confidence >= publish_threshold:
                self._publish_to_all_communities(result)

            symbol = result.get('symbol', 'UNKNOWN')
            pattern = result.get('alert_type', 'unknown')
            status = '✅ VALID' if result.get('validation_result', {}).get('is_valid') else '❌ REJECTED'
            self.logger.info(
                "🎯 VALIDATION RESULT: %s %s - %s (confidence: %.2f)",
                symbol,
                pattern,
                status,
                validation_confidence,
            )
        except Exception as exc:
            self.logger.error(f"Failed to persist/publish validation result: {exc}")

    def _is_within_market_hours(self, reference_time: Optional[datetime] = None) -> bool:
        """Check if current time (or supplied reference) falls within NSE trading hours."""
        if not self.enforce_market_hours:
            return True

        ist = pytz.timezone("Asia/Kolkata")
        if reference_time is None:
            current = datetime.now(ist)
        else:
            if reference_time.tzinfo is None:
                current = ist.localize(reference_time)
            else:
                current = reference_time.astimezone(ist)

        if current.weekday() >= 5:  # Saturday/Sunday
            return False

        start = dt_time(9, 15)
        end = dt_time(15, 30)
        return start <= current.time() <= end

    def _next_market_open(self) -> datetime:
        """Return the next market open datetime in IST."""
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        next_open_date = now.date()

        if now.time() >= dt_time(15, 30):
            next_open_date += timedelta(days=1)

        while datetime.combine(next_open_date, dt_time(9, 15)).weekday() >= 5:
            next_open_date += timedelta(days=1)

        next_open = datetime.combine(next_open_date, dt_time(9, 15))
        return ist.localize(next_open)
    
    def _check_symbol_cooldown(self, symbol: str) -> bool:
        """Check if symbol is in cooldown period"""
        current_time = time.time()
        
        if symbol in self.symbol_cooldowns:
            last_alert_time = self.symbol_cooldowns[symbol]
            time_since_last = current_time - last_alert_time
            
            if time_since_last < self.cooldown_seconds:
                remaining_cooldown = self.cooldown_seconds - time_since_last
                self.logger.debug(f"Symbol {symbol} in cooldown: {remaining_cooldown:.1f}s remaining")
                return False
        
        return True
    
    def _update_symbol_cooldown(self, symbol: str):
        """Update cooldown timestamp for symbol"""
        self.symbol_cooldowns[symbol] = time.time()
        self.logger.debug(f"Updated cooldown for {symbol}")
    
    def get_cooldown_stats(self) -> Dict:
        """Get cooldown statistics"""
        current_time = time.time()
        active_cooldowns = 0
        total_symbols = len(self.symbol_cooldowns)
        
        for symbol, last_alert_time in self.symbol_cooldowns.items():
            time_since_last = current_time - last_alert_time
            if time_since_last < self.cooldown_seconds:
                active_cooldowns += 1
        
        return {
            'total_symbols_tracked': total_symbols,
            'active_cooldowns': active_cooldowns,
            'cooldown_seconds': self.cooldown_seconds,
            'symbols_in_cooldown': [symbol for symbol, last_time in self.symbol_cooldowns.items() 
                                   if (current_time - last_time) < self.cooldown_seconds]
        }
    
    def _publish_to_all_communities(self, validation_result: Dict):
        """Publish high-confidence validation results to all community channels"""
        try:
            symbol = validation_result.get('symbol', 'UNKNOWN')
            confidence = validation_result['validation_result']['confidence_score']
            
            # Create enhanced validation message for communities
            community_message = self._format_validation_for_communities(validation_result)
            
            # Publish to Telegram channels
            telegram_channels = [
                'telegram:validation_results',
                'telegram:high_confidence_alerts',
                'telegram:community_updates'
            ]
            
            for channel in telegram_channels:
                try:
                    self.redis_client.publish(channel, json.dumps({
                        'type': 'validation_result',
                        'symbol': symbol,
                        'confidence': confidence,
                        'message': community_message,
                        'full_result': validation_result,
                        'timestamp': datetime.now().isoformat()
                    }))
                    self.logger.info(f"📡 Published validation to {channel}")
                except Exception as e:
                    self.logger.error(f"Failed to publish to {channel}: {e}")
            
            # Publish to Reddit channel
            try:
                self.redis_client.publish('reddit:validation_results', json.dumps({
                    'type': 'validation_result',
                    'symbol': symbol,
                    'confidence': confidence,
                    'message': community_message,
                    'full_result': validation_result,
                    'timestamp': datetime.now().isoformat()
                }))
                self.logger.info(f"📡 Published validation to Reddit channel")
            except Exception as e:
                self.logger.error(f"Failed to publish to Reddit: {e}")
            
            self.logger.info(f"🎯 HIGH-CONFIDENCE VALIDATION PUBLISHED TO ALL COMMUNITIES: {symbol} ({confidence:.1%})")
            
        except Exception as e:
            self.logger.error(f"Error publishing to all communities: {e}")
    
    def _format_validation_for_communities(self, validation_result: Dict) -> str:
        """Format validation result for community consumption"""
        try:
            symbol = validation_result.get('symbol', 'UNKNOWN')
            result_payload = validation_result.get('validation_result', {})
            confidence = result_payload.get('confidence_score', 0.0)
            is_valid = result_payload.get('is_valid', False)
            status_emoji = "✅" if is_valid else "❌"

            forward_validation = result_payload.get('forward_validation')
            if forward_validation:
                summary = forward_validation.get('summary', {})
                windows = forward_validation.get('windows', {})
                success_count = summary.get('success_count', 0)
                failure_count = summary.get('failure_count', 0)
                inconclusive_count = summary.get('inconclusive_count', 0)
                total_windows = summary.get('total_windows', len(windows))
                success_ratio = summary.get('success_ratio', 0.0)
                max_move = summary.get('max_directional_move_pct', 0.0)
                threshold = summary.get('threshold_pct', self.forward_success_threshold_pct)

                window_lines = []
                for window, window_data in windows.items():
                    result_data = window_data.get('result', {})
                    window_status = window_data.get('status', 'PENDING')
                    movement = result_data.get('directional_movement_pct', result_data.get('price_movement_pct', 0.0))
                    window_lines.append(
                        f"- {window}m: {window_status} ({movement:+.2f}%)"
                    )
                window_section = "\n".join(window_lines) if window_lines else "No window data"

                message = f"""
{status_emoji} **FORWARD VALIDATION RESULT: {symbol}**

**Confidence:** {confidence:.1%}
**Status:** {'VALID' if is_valid else 'REJECTED'}

**Window Outcomes:** {success_count} success / {failure_count} fail / {inconclusive_count} inconclusive
**Success Ratio:** {success_ratio:.1%} (threshold {self.forward_success_ratio_threshold:.1%})
**Max Directional Move:** {max_move:+.2f}%
**Per-Window Summary:**
{window_section}

⏰ **Completed:** {datetime.now().strftime('%H:%M:%S')} IST
                """.strip()
                return message

            # Get validation metrics
            volume_validation = result_payload.get('volume_validation', {})
            expected_move_validation = result_payload.get('expected_move_validation', {})
            
            # Format message
            message = f"""
{status_emoji} **VALIDATION RESULT: {symbol}**

**Confidence:** {confidence:.1%}
**Status:** {'VALID' if is_valid else 'REJECTED'}

**Volume Validation:**
- Confidence: {volume_validation.get('confidence_score', 0):.1%}
- Valid: {'Yes' if volume_validation.get('is_valid', False) else 'No'}

**Expected Move Validation:**
- Accuracy: {expected_move_validation.get('accuracy', 0):.1%}
- Confidence: {expected_move_validation.get('confidence', 0):.1%}
- Valid: {'Yes' if expected_move_validation.get('is_valid', False) else 'No'}

**Rolling Windows Analyzed:** {validation_result['validation_result'].get('combined_metrics', {}).get('rolling_windows_analyzed', 0)}

⏰ **Time:** {datetime.now().strftime('%H:%M:%S')} IST
            """.strip()
            
            return message
            
        except Exception as e:
            self.logger.error(f"Error formatting validation for communities: {e}")
            return f"Validation result for {symbol}: {confidence:.1%} confidence"

    def _ensure_forward_worker(self):
        """Start the forward validation worker thread if needed."""
        if not self.forward_enabled:
            return
        if self.forward_worker_thread and self.forward_worker_thread.is_alive():
            return

        self.forward_worker_thread = threading.Thread(
            target=self._forward_validation_loop,
            name="ForwardValidationWorker",
            daemon=True,
        )
        self.forward_worker_thread.start()
        self.logger.info("🚀 Started forward validation worker thread")

    def _forward_validation_loop(self):
        """Background loop that evaluates scheduled forward validation windows."""
        self.logger.info(
            "Forward validation worker running with poll interval %ss", self.forward_poll_interval
        )
        while self.running and self.forward_enabled:
            try:
                processed = self._process_due_forward_windows()
                if processed == 0:
                    time.sleep(self.forward_poll_interval)
            except Exception as exc:
                self.logger.error(f"Forward validation worker error: {exc}")
                time.sleep(self.forward_poll_interval)

    def _process_due_forward_windows(self, batch_size: int = 50) -> int:
        """Process any forward validation windows that are due."""
        if not self.forward_enabled:
            return 0

        now = time.time()
        due_entries = self.redis_client.zrangebyscore(
            self.forward_schedule_key, 0, now, start=0, num=batch_size
        )

        if not due_entries:
            return 0

        processed = 0

        for raw_entry in due_entries:
            entry = raw_entry.decode("utf-8") if isinstance(raw_entry, (bytes, bytearray)) else raw_entry
            self.redis_client.zrem(self.forward_schedule_key, entry)

            try:
                alert_id, window_str = entry.split("|", 1)
                window_minutes = int(window_str)
            except ValueError:
                self.logger.warning(f"Malformed forward schedule entry: {entry}")
                continue

            state = self._load_forward_alert_state(alert_id)
            if not state:
                self.logger.debug(
                    f"No forward state found for alert {alert_id}, skipping window {window_minutes}"
                )
                continue

            window_key = str(window_minutes)
            window_state = state.get("windows", {}).get(window_key)
            if not window_state or window_state.get("status") not in {"PENDING", "RETRYING"}:
                continue

            result = self._evaluate_forward_window(state, window_minutes)

            if result is None:
                retries = window_state.get("retries", 0) + 1
                window_state["retries"] = retries
                if retries <= self.forward_max_retries:
                    reschedule_at = time.time() + self.forward_retry_delay
                    window_state["status"] = "RETRYING"
                    window_state["due_ts"] = int(reschedule_at)
                    self.redis_client.zadd(self.forward_schedule_key, {entry: reschedule_at})
                    self.logger.debug(
                        "Re-queueing forward window %s for alert %s (retry %d/%d)",
                        window_minutes,
                        alert_id,
                        retries,
                        self.forward_max_retries,
                    )
                else:
                    window_state["status"] = "INCONCLUSIVE"
                    window_state["result"] = {
                        "status": "INCONCLUSIVE",
                        "reason": "PRICE_UNAVAILABLE",
                        "window_minutes": window_minutes,
                        "evaluated_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
                    }
                self._save_forward_alert_state(state)
                continue

            window_state["status"] = result.get("status", "INCONCLUSIVE")
            window_state["result"] = result
            window_state["evaluated_ts"] = int(time.time())
            self._save_forward_alert_state(state)
            processed += 1

            if self._is_forward_alert_complete(state):
                self._finalize_forward_validation(state)

        return processed

    def _schedule_forward_validation(self, alert_data: Dict) -> Optional[Dict[str, Any]]:
        """Persist forward validation state for an alert and schedule evaluation windows."""
        try:
            alert_id = alert_data.get("alert_id") or f"{alert_data.get('symbol', 'UNKNOWN')}_{int(time.time())}"
            symbol = alert_data.get("symbol", "UNKNOWN")
            pattern = alert_data.get("alert_type", alert_data.get("pattern", "unknown"))
            direction = self._resolve_alert_direction(alert_data)

            alert_time = self._parse_alert_timestamp(alert_data)
            alert_epoch = alert_time.timestamp()
            timestamp_ms = int(alert_epoch * 1000)

            entry_price = alert_data.get("last_price") or alert_data.get("price")
            if not entry_price:
                entry_price = self.redis.get_last_price(symbol) or 0.0
            entry_price = float(entry_price or 0.0)

            if entry_price <= 0:
                self.logger.warning(
                    f"Cannot schedule forward validation for {alert_id} ({symbol}) - missing entry price"
                )
                return None

            state = {
                "alert_id": alert_id,
                "symbol": symbol,
                "pattern": pattern,
                "signal": alert_data.get("signal", "UNKNOWN"),
                "direction": direction,
                "confidence": float(alert_data.get("confidence", 0.0) or 0.0),
                "expected_move": float(alert_data.get("expected_move", 0.0) or 0.0),
                "timestamp": alert_data.get("timestamp"),
                "timestamp_ms": timestamp_ms,
                "alert_time_iso": alert_time.isoformat(),
                "entry_price": entry_price,
                "windows": {},
                "created_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
            }

            schedule_entries = {}
            now_epoch = time.time()

            for window_minutes in self.forward_windows:
                due_time = alert_time + timedelta(minutes=window_minutes)
                due_epoch = max(due_time.timestamp(), now_epoch + 1)  # ensure future scheduling
                entry_key = f"{alert_id}|{window_minutes}"

                state["windows"][str(window_minutes)] = {
                    "window_minutes": window_minutes,
                    "due_ts": int(due_epoch),
                    "status": "PENDING",
                    "retries": 0,
                }
                schedule_entries[entry_key] = due_epoch

            self._save_forward_alert_state(state)
            if schedule_entries:
                self.redis_client.zadd(self.forward_schedule_key, schedule_entries)

            return state
        except Exception as exc:
            self.logger.error(f"Failed to schedule forward validation: {exc}")
            return None

    def _parse_alert_timestamp(self, alert_data: Dict) -> datetime:
        """Parse an alert timestamp into a timezone-aware datetime."""
        ist = pytz.timezone("Asia/Kolkata")
        timestamp_ms = alert_data.get("timestamp_ms")
        if timestamp_ms:
            try:
                return datetime.fromtimestamp(float(timestamp_ms) / 1000.0, tz=ist)
            except Exception:
                pass

        timestamp = alert_data.get("timestamp")
        if isinstance(timestamp, (int, float)):
            try:
                return datetime.fromtimestamp(float(timestamp), tz=ist)
            except Exception:
                pass
        if isinstance(timestamp, str):
            ts_str = timestamp.strip()
            try:
                if ts_str.endswith("Z"):
                    ts_str = ts_str.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(ts_str)
                if parsed.tzinfo is None:
                    parsed = ist.localize(parsed)
                return parsed.astimezone(ist)
            except Exception:
                try:
                    parsed = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                    parsed = ist.localize(parsed)
                    return parsed
                except Exception:
                    pass

        return datetime.now(ist)

    def _resolve_alert_direction(self, alert_data: Dict) -> int:
        """Infer trade direction from alert metadata."""
        for key in ("direction", "signal", "action", "side"):
            value = alert_data.get(key)
            direction = self._direction_from_value(value)
            if direction != 0:
                return direction

        symbol = alert_data.get("symbol", "")
        if symbol.endswith("CE"):
            return 1
        if symbol.endswith("PE"):
            return -1

        return 0

    @staticmethod
    def _direction_from_value(value: Optional[Any]) -> int:
        if value is None:
            return 0
        normalized = str(value).strip().lower()
        if normalized in {"buy", "long", "bullish", "call", "ce", "up"}:
            return 1
        if normalized in {"sell", "short", "bearish", "put", "pe", "down"}:
            return -1
        return 0

    def _load_forward_alert_state(self, alert_id: str) -> Optional[Dict[str, Any]]:
        """Load stored forward validation state from Redis."""
        try:
            key = f"{self.forward_alert_prefix}:{alert_id}"
            data = self.redis_client.get(key)
            if not data:
                return None
            if isinstance(data, (bytes, bytearray)):
                data = data.decode("utf-8")
            return json.loads(data)
        except Exception as exc:
            self.logger.error(f"Failed to load forward validation state for {alert_id}: {exc}")
            return None

    def _save_forward_alert_state(self, state: Dict[str, Any]) -> None:
        """Persist forward validation state back to Redis."""
        try:
            alert_id = state.get("alert_id")
            if not alert_id:
                return
            key = f"{self.forward_alert_prefix}:{alert_id}"
            payload = json.dumps(state, default=str)
            pipe = self.redis_client.pipeline()
            pipe.set(key, payload)
            if self.forward_state_ttl > 0:
                pipe.expire(key, self.forward_state_ttl)
            pipe.execute()
        except Exception as exc:
            self.logger.error(
                f"Failed to persist forward validation state for {state.get('alert_id')}: {exc}"
            )

    def _evaluate_forward_window(self, state: Dict[str, Any], window_minutes: int) -> Optional[Dict[str, Any]]:
        """Evaluate price movement for a specific forward validation window."""
        symbol = state.get("symbol", "UNKNOWN")
        entry_price = float(state.get("entry_price") or 0.0)
        direction = int(state.get("direction") or 0)
        if entry_price <= 0:
            return {
                "status": "INCONCLUSIVE",
                "reason": "INVALID_ENTRY_PRICE",
                "window_minutes": window_minutes,
            }

        current_price = self.redis.get_last_price(symbol)
        if current_price is None:
            return None

        current_price = float(current_price)
        pct_change = ((current_price - entry_price) / entry_price) * 100 if entry_price else 0.0
        directional_change = pct_change * direction if direction != 0 else pct_change

        status = "INCONCLUSIVE"
        if direction == 0:
            if abs(pct_change) >= self.forward_success_threshold_pct:
                status = "SUCCESS"
            else:
                status = "FAILURE"
        else:
            if directional_change >= self.forward_success_threshold_pct:
                status = "SUCCESS"
            elif directional_change <= -self.forward_success_threshold_pct:
                status = "FAILURE"
            else:
                status = "INCONCLUSIVE"

        evaluation_time = datetime.now(pytz.timezone("Asia/Kolkata"))

        return {
            "status": status,
            "window_minutes": window_minutes,
            "entry_price": entry_price,
            "observed_price": current_price,
            "price_movement_pct": pct_change,
            "directional_movement_pct": directional_change,
            "evaluated_at": evaluation_time.isoformat(),
            "success_threshold_pct": self.forward_success_threshold_pct,
        }

    def _is_forward_alert_complete(self, state: Dict[str, Any]) -> bool:
        """Determine whether all scheduled windows have been evaluated."""
        windows = state.get("windows", {})
        if not windows:
            return False
        return all(
            window.get("status") not in {"PENDING", "RETRYING"} for window in windows.values()
        )

    def _finalize_forward_validation(self, state: Dict[str, Any]) -> None:
        """Aggregate window outcomes, persist the final result, and publish it."""
        if state.get("finalized"):
            return

        windows = state.get("windows", {})
        success_count = sum(1 for w in windows.values() if w.get("status") == "SUCCESS")
        failure_count = sum(1 for w in windows.values() if w.get("status") == "FAILURE")
        inconclusive_count = sum(1 for w in windows.values() if w.get("status") == "INCONCLUSIVE")
        total_windows = len(windows)
        success_ratio = success_count / total_windows if total_windows else 0.0

        directional_changes = [
            w.get("result", {}).get("directional_movement_pct", 0.0)
            for w in windows.values()
            if w.get("result")
        ]
        abs_changes = [
            abs(w.get("result", {}).get("price_movement_pct", 0.0))
            for w in windows.values()
            if w.get("result")
        ]
        max_directional_move = max(directional_changes) if directional_changes else 0.0
        max_abs_move = max(abs_changes) if abs_changes else 0.0

        is_valid = success_ratio >= self.forward_success_ratio_threshold
        confidence_score = min(1.0, max(0.0, success_ratio))

        forward_summary = {
            "success_count": success_count,
            "failure_count": failure_count,
            "inconclusive_count": inconclusive_count,
            "total_windows": total_windows,
            "success_ratio": success_ratio,
            "max_directional_move_pct": max_directional_move,
            "max_abs_move_pct": max_abs_move,
            "threshold_pct": self.forward_success_threshold_pct,
        }

        result_payload = {
            "alert_id": state.get("alert_id"),
            "symbol": state.get("symbol"),
            "alert_type": state.get("pattern"),
            "timestamp": state.get("timestamp"),
            "entry_price": state.get("entry_price"),
            "signal": state.get("signal"),
            "confidence": state.get("confidence"),
            "expected_move": state.get("expected_move"),
            "validation_result": {
                "is_valid": is_valid,
                "confidence_score": confidence_score,
                "forward_validation": {
                    "summary": forward_summary,
                    "windows": state.get("windows", {}),
                },
            },
            "processed_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
        }

        try:
            if self.forward_results_key:
                summary_payload = {
                    "status": "SUCCESS" if is_valid else "FAILURE",
                    "success_ratio": success_ratio,
                    "max_directional_move_pct": max_directional_move,
                    "processed_at": result_payload["processed_at"],
                }
                self.redis_client.hset(
                    self.forward_results_key,
                    state.get("alert_id"),
                    json.dumps(summary_payload),
                )
        except Exception as exc:
            self.logger.warning(
                f"Failed to persist forward validation summary for {state.get('alert_id')}: {exc}"
            )

        self.performance_metrics["alerts_processed"] += 1
        if is_valid:
            self.performance_metrics["validations_passed"] += 1
        else:
            self.performance_metrics["validations_failed"] += 1

        state["finalized"] = True
        state["finalized_at"] = result_payload["processed_at"]
        self._save_forward_alert_state(state)

        # Also publish to validation stream (alerts:validation:results) with rolling windows
        try:
            from alert_validation.alert_streams import AlertStream
            alert_stream = AlertStream(self.redis)
            
            # Prepare outcome dict for stream publishing
            outcome = {
                'status': 'SUCCESS' if is_valid else 'FAILURE',
                'price_movement_pct': max_directional_move,
                'duration_minutes': max([w.get('duration_minutes', 0) for w in windows.values()] + [0]),
                'max_move_pct': max_abs_move,
                'details': f"Forward validation: {success_count}/{total_windows} windows successful",
            }
            
            # Prepare metadata with rolling windows
            metadata = {
                'alert_id': state.get('alert_id'),
                'symbol': state.get('symbol'),
                'pattern': state.get('pattern'),
                'confidence': state.get('confidence', 0.0),
                'signal': state.get('signal', 'NEUTRAL'),
                'timestamp_ms': int(time.time() * 1000),
                'rolling_windows': json.dumps(forward_summary),  # Include forward validation summary
            }
            
            # Publish to stream
            alert_stream.publish_validation_result_sync(
                state.get('alert_id'),
                outcome,
                metadata
            )
            self.logger.info(f"📊 Published forward validation result to stream: {state.get('symbol')}")
        except Exception as e:
            self.logger.warning(f"Could not publish to validation stream: {e}")
        
        self._post_process_validation_result(result_payload)

    def _convert_alert_format(self, alert_data: Dict) -> Dict:
        """Convert alert data from existing system to validator format"""
        try:
            # Extract key fields from the existing alert format
            symbol = alert_data.get('symbol', 'UNKNOWN')
            pattern = alert_data.get('pattern', 'unknown')
            volume_ratio = alert_data.get('volume_ratio', 1.0)
            last_price = alert_data.get('last_price', 0.0)
            bucket_incremental_volume = alert_data.get('bucket_incremental_volume', 0.0)
            confidence = alert_data.get('confidence', 0.0)
            
            # Create validator-compatible alert format
            validator_alert = {
                'alert_id': f"{symbol}_{pattern}_{int(time.time())}",
                'symbol': symbol,
                'alert_type': pattern,  # Use pattern as alert type
                'bucket_incremental_volume': bucket_incremental_volume,
                'last_price': last_price,
                'volume_ratio': volume_ratio,
                'confidence': confidence,
                'timestamp': alert_data.get('published_at', datetime.now().isoformat()),
                'pattern_type': pattern,
                'signal': alert_data.get('signal', 'UNKNOWN'),
                'expected_move': alert_data.get('expected_move', 0.0)
            }
            
            return validator_alert
            
        except Exception as e:
            self.logger.error(f"Error converting alert format: {e}")
            # Return minimal valid format
            return {
                'alert_id': f"unknown_{int(time.time())}",
                'symbol': 'UNKNOWN',
                'alert_type': 'unknown',
                'bucket_incremental_volume': 0,
                'last_price': 0,
                'timestamp': datetime.now().isoformat()
            }
    
    def test_volume_retrieval(self, symbol: str = "NIFTY50") -> Dict:
        """Test function to verify bucket_incremental_volume data retrieval from Redis"""
        self.logger.info(f"🧪 TESTING VOLUME RETRIEVAL for {symbol}")
        
        try:
            # Test Redis connection
            self.logger.info(f"   Redis connection: {self.redis_client.ping()}")
            
            # Test bucket_incremental_volume bucket patterns
            from datetime import datetime
            import pytz
            
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            current_session = now.strftime('%Y-%m-%d')
            
            # Test different key patterns
            test_patterns = [
                f"bucket_incremental_volume:{symbol}:buckets:{now.hour}:{now.minute // 5}",
                f"bucket:{symbol}:{current_session}:{now.hour}:{now.minute}",
                f"bucket_incremental_volume:{symbol}:daily:{current_session}"
            ]
            
            volume_client = self.rolling_store_client or self.redis_client
            results = {}
            for pattern in test_patterns:
                try:
                    key_type = volume_client.type(pattern) if volume_client else 'none'
                    if key_type == 'hash':
                        data = volume_client.hgetall(pattern)
                        results[pattern] = {'type': 'hash', 'data': data}
                        self.logger.info(f"   ✅ Hash key {pattern}: {data}")
                    elif key_type == 'string':
                        data = volume_client.get(pattern)
                        results[pattern] = {'type': 'string', 'data': data}
                        self.logger.info(f"   ✅ String key {pattern}: {data}")
                    else:
                        results[pattern] = {'type': key_type, 'data': None}
                        self.logger.info(f"   ❌ Key {pattern}: {key_type}")
                except Exception as e:
                    results[pattern] = {'type': 'error', 'data': str(e)}
                    self.logger.error(f"   ❌ Error testing {pattern}: {e}")
            
            # Test rolling metrics calculation
            self.logger.info(f"   Testing rolling metrics calculation...")
            test_windows = [1, 3, 5]
            rolling_metrics = self.get_rolling_metrics(symbol, test_windows)
            
            self.logger.info(f"   Rolling metrics result: {rolling_metrics}")
            
            return {
                'redis_connection': True,
                'test_patterns': results,
                'rolling_metrics': rolling_metrics,
                'symbol': symbol,
                'timestamp': now.isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Volume retrieval test failed: {e}")
            return {
                'redis_connection': False,
                'error': str(e),
                'symbol': symbol,
                'timestamp': datetime.now().isoformat()
            }
    
    def get_statistical_model_data(self) -> Dict:
        """Get statistical model data from Redis only - completely independent"""
        try:
            model_data = {
                'performance_metrics': self.get_performance_metrics(),
                'redis_validation_data': {},
                'timestamp': datetime.now().isoformat(),
                'redis_only_validator': True,
                'independent_from_main_system': True
            }
            
            # Get validation data from Redis
            try:
                if self.alert_store_client:
                    recent_validations = self.alert_store_client.lrange('validation_results:recent', 0, 99)
                    if recent_validations:
                        validation_data = [json.loads(v) for v in recent_validations]
                        model_data['redis_validation_data'] = {
                            'total_recent_validations': len(validation_data),
                            'valid_validations': sum(1 for v in validation_data if v.get('is_valid', False)),
                            'average_confidence': sum(v.get('confidence_score', 0) for v in validation_data) / len(validation_data) if validation_data else 0,
                            'pattern_distribution': {},
                            'vix_regime_distribution': {}
                        }
                        
                        # Calculate pattern distribution
                        pattern_counts = {}
                        vix_counts = {}
                        for v in validation_data:
                            pattern = v.get('pattern_type', 'unknown')
                            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
                            
                            vix_regime = v.get('validation_metrics', {}).get('vix_regime', 'UNKNOWN')
                            vix_counts[vix_regime] = vix_counts.get(vix_regime, 0) + 1
                        
                        model_data['redis_validation_data']['pattern_distribution'] = pattern_counts
                        model_data['redis_validation_data']['vix_regime_distribution'] = vix_counts
            except Exception as e:
                self.logger.warning(f"Error getting Redis validation data: {e}")
            
            return model_data
            
        except Exception as e:
            self.logger.error(f"Error getting statistical model data: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat(), 'redis_only_validator': True}
    
    def export_statistical_model_data(self, output_file: str = None) -> str:
        """Export statistical model data to JSON file"""
        try:
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"statistical_model_data_{timestamp}.json"
            
            model_data = self.get_statistical_model_data()
            
            with open(output_file, 'w') as f:
                json.dump(model_data, f, indent=2, default=str)
            
            self.logger.info(f"Statistical model data exported to {output_file}")
            return output_file
            
        except Exception as e:
            self.logger.error(f"Error exporting statistical model data: {e}")
            return None

def main():
    """Main function to run the alert validator"""
    validator = AlertValidator()
    
    try:
        # Start in a separate thread
        validator_thread = threading.Thread(target=validator.start_alert_consumer)
        validator_thread.daemon = True
        validator_thread.start()
        
        print("Alert Validator started. Press Ctrl+C to stop.")
        
        # Keep main thread alive and check market hours
        while validator.running:
            current_time = datetime.now().time()
            
            # Check if market is closed (after 3:30 PM IST)
            if current_time >= datetime.strptime("15:30", "%H:%M").time():
                validator.logger.info("Market closed - generating final report and stopping validator")
                validator.export_statistical_model_data()
                validator.stop()
                break
                
            time.sleep(60)  # Check every minute
            
    except KeyboardInterrupt:
        print("\nStopping Alert Validator...")
        validator.stop()
    except Exception as e:
        print(f"Error: {e}")
        validator.stop()

if __name__ == "__main__":
    main()
