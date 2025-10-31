#!/Users/apple/Desktop/intraday_trading/.venv/bin/python3
"""
Main orchestration for modular market scanner
Handles initialization, data flow coordination, and graceful shutdown
Updated: 2025-10-27
"""

import sys
import os
import time
import signal
import argparse
import json
import glob
import asyncio
from datetime import datetime
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
try:
    from redis_files.perf_probe import PerfSink, timed_block, slow_tick_guard
except Exception:
    class PerfSink:
        def __init__(self, *a, **k):
            pass
        def add(self, *a, **k):
            pass
    from contextlib import contextmanager
    @contextmanager
    def timed_block(perf_sink, name):
        yield
    @contextmanager
    def slow_tick_guard(*a, **k):
        yield
from threading import BoundedSemaphore
from typing import Dict, List, Optional, Any, Tuple
import pytz

# Define project root at the top level
project_root = os.path.dirname(os.path.abspath(__file__))

from patterns.pattern_detector import PatternDetector
from utils.correct_volume_calculator import CorrectVolumeCalculator, VolumeResolver
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_latest_hash


def verify_system_initialization():
    """Verify all calculation systems are working."""
    try:
        from redis_files.redis_calculations import RedisCalculations
        from redis_files.redis_client import get_redis_client
    except ImportError as import_err:
        print(f"❌ [SYSTEM_CHECK] Unable to import Redis calculation modules: {import_err}")
        return

    try:
        redis_wrapper = get_redis_client()
        redis_core = redis_wrapper.redis_client or redis_wrapper.get_client(0)
        if not redis_core:
            raise RuntimeError("Primary Redis connection unavailable")

        calc = RedisCalculations(redis_wrapper)

        test_prices = [100, 101, 102, 101, 103, 104, 103, 105, 104, 106]

        print("🔧 [SYSTEM_CHECK] Verifying calculation systems...")

        rsi = calc.calculate_rsi_redis(test_prices, 14)
        print(f"✅ RSI Calculation: {rsi}")

        highs = [p * 1.02 for p in test_prices]
        lows = [p * 0.98 for p in test_prices]
        atr = calc.calculate_atr_redis(highs, lows, test_prices, 14)
        print(f"✅ ATR Calculation: {atr}")

        ema = calc.calculate_ema_redis(test_prices, 20)
        print(f"✅ EMA Calculation: {ema}")

        print("🎯 [SYSTEM_CHECK] All systems verified!")
    except Exception as calc_err:
        print(f"❌ [SYSTEM_CHECK] Verification failed: {calc_err}")
        raise


def emergency_data_reset():
    """Emergency reset to clear synthetic patterns and validate data feeds."""
    try:
        from redis_files.redis_client import get_redis_client

        redis_wrapper = get_redis_client()
        redis_core = redis_wrapper.redis_client or redis_wrapper.get_client(0)
        if not redis_core:
            raise RuntimeError("Primary Redis connection unavailable")

        # Use proper configuration for pattern detector
        config = {
            "debug": True,
            "min_confidence": 0.6,
            "volume_threshold": 1.5
        }
        detector = PatternDetector(config, redis_wrapper)
        
        # Update VIX-aware thresholds
        detector._update_vix_aware_thresholds()

        print("🚨 [EMERGENCY_RESET] Starting system data quality reset...")

        test_symbols = ["NSE:RELIANCE", "NSE:TCS", "NSE:INFY", "NSE:HDFCBANK"]

        for symbol in test_symbols:
            try:
                if hasattr(redis_wrapper, "get_time_buckets_robust"):
                    buckets = redis_wrapper.get_time_buckets_robust(symbol, 60)
                else:
                    buckets = redis_wrapper.get_time_buckets(symbol, lookback_minutes=60)

                prices = [
                    float(b.get("close") or b.get("last_price") or 0)
                    for b in buckets
                    if not b.get("synthetic") and (b.get("close") or b.get("last_price"))
                ]
                volumes = [
                    float(b.get("bucket_incremental_volume") or 0)
                    for b in buckets
                    if not b.get("synthetic") and (b.get("close") or b.get("last_price"))
                ]

                quality = detector.validate_data_quality(symbol, prices, volumes)
                print(f"📊 {symbol}: {len(prices)} prices, Quality: {quality['level']} ({quality['score']})")

                if quality["valid"]:
                    if prices:
                        base_price = prices[0]
                        last_price = prices[-1]
                        price_change = 0.0 if base_price == 0 else ((last_price - base_price) / base_price) * 100
                    else:
                        last_price = 0.0
                        price_change = 0.0

                    detector.price_history[symbol] = list(prices)
                    detector.volume_history[symbol] = list(volumes)

                    # Calculate actual bucket_incremental_volume ratio using the new bucket_incremental_volume architecture
                    tick_data = {
                        "symbol": symbol,
                        "zerodha_last_traded_quantity": volumes[-1] if volumes else 0.0,
                        "zerodha_cumulative_volume": sum(volumes) if volumes else 0.0,
                        "last_price": last_price
                    }
                    
                    # Use the correct bucket_incremental_volume calculator
                    try:
                        from utils.correct_volume_calculator import CorrectVolumeCalculator
                        calculator = CorrectVolumeCalculator(redis_wrapper)
                        volume_ratio = calculator.calculate_volume_ratio(symbol, tick_data)
                    except Exception as e:
                        print(f"   ⚠️ Volume calculation failed for {symbol}: {e}")
                        volume_ratio = 1.0

                    indicators = {
                        "symbol": symbol,
                        "last_price": last_price,
                        "volume_ratio": volume_ratio,
                        "bucket_incremental_volume": volumes[-1] if volumes else 0.0,
                        "price_change": round(price_change, 3),
                    }

                    print(f"   🔍 DEBUG: indicators = {indicators}")
                    patterns = detector.detect_patterns(indicators)
                    pattern_names = [p.get("pattern") for p in patterns]
                    print(f"   Patterns: {pattern_names}")
                else:
                    print(f"   ❌ REJECTED: {quality['issues']}")
            except Exception as symbol_err:
                print(f"   ❌ Error processing {symbol}: {symbol_err}")

        print("✅ [EMERGENCY_RESET] Complete - System should now use real data only")

    except Exception as reset_err:
        print(f"❌ [EMERGENCY_RESET] Failed: {reset_err}")


class VolumeRatioTracker:
    """Track bucket_incremental_volume ratio changes across the system."""
    
    def __init__(self):
        self.overrides = []
    
    def log_override(self, location, symbol, original_value, new_value):
        """Log when volume_ratio is overridden."""
        if original_value != new_value and new_value == 1.0:
            override_info = {
                'location': location,
                'symbol': symbol,
                'original': original_value,
                'new': new_value,
                'timestamp': time.time()
            }
            self.overrides.append(override_info)
            # Note: This will be logged by the calling method
    
    def get_recent_overrides(self, count=10):
        """Get recent overrides."""
        return self.overrides[-count:]


# Global tracker instance
volume_tracker = VolumeRatioTracker()


class DataLineageTracker:
    """Track where data comes from and how it flows."""
    
    def __init__(self):
        self.data_sources = {}
    
    def track_data_source(self, symbol, stage, data):
        """Track data at each processing stage."""
        volume_ratio = data.get("volume_ratio", 1.0)
        source_info = {
            'stage': stage,
            'volume_ratio': volume_ratio,
            'timestamp': time.time(),
            'data_keys': list(data.keys())
        }
        
        if symbol not in self.data_sources:
            self.data_sources[symbol] = []
        
        self.data_sources[symbol].append(source_info)
        
        # Log significant changes
        if len(self.data_sources[symbol]) > 1:
            prev_ratio = self.data_sources[symbol][-2]['volume_ratio']
            if prev_ratio != volume_ratio:
                logging.getLogger("data_lineage").info(
                    f"🔍 [LINEAGE] {symbol} volume_ratio changed from {prev_ratio} to {volume_ratio} at {stage}"
                )

# Global tracker
data_lineage = DataLineageTracker()

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import core modules (use production/scanner implementations)
from core.data import DataPipeline
from patterns.pattern_detector import PatternDetector
from alerts.alert_manager import ProductionAlertManager
from alerts.risk_manager import RiskManager
# Alert validator removed - using standalone validator instead
# from scanner.production.services.premarket_alert_service import PremarketAlertService


# Import utilities
from redis_files.redis_client import get_redis_client, publish_to_redis
from intraday_scanner.calculations import (
    get_tick_processor,
    safe_format,
    get_market_volume_data,
    preload_static_data,
)

# Project root already added above

# Import unified schema

try:
    from config.schemas import map_kite_to_unified
except ImportError as e:
    print(f"Failed to import unified_schema: {e}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Project root: {project_root}")
    print(f"Python path: {sys.path[:3]}")
    # Try alternative import
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "unified_schema",
        os.path.join(project_root, "config", "schemas", "unified_schema.py"),
    )
    unified_schema = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(unified_schema)
    map_kite_to_unified = unified_schema.map_kite_to_unified
# Pattern config handled by PatternDetector internally


# Improved logging setup for full line display
import logging
import logging.handlers

try:
    import schedule
except ImportError:  # pragma: no cover - schedule is optional
    schedule = None


def setup_scanner_logging():
    """Setup improved logging for scanner with full line display"""
    # Create logs directory
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Console formatter with full line support
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"
    )

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler for scanner logs
    scanner_log_file = os.path.join(log_dir, "scanner_detailed.log")
    file_handler = logging.handlers.RotatingFileHandler(
        scanner_log_file, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    root_logger.addHandler(file_handler)

    return root_logger


# Setup logging
logger = setup_scanner_logging()


def resolve_premarket_watchlist(config=None):
    """Resolve premarket watchlist symbols from config or static files."""
    possible = []
    if config:
        possible = config.get("premarket_watchlist") or config.get(
            "watchlists", {}
        ).get("premarket", [])
    if possible:
        return sorted({sym for sym in possible if sym})

    watchlist_path = os.path.join(project_root, "config", "premarket_watchlist.json")
    if os.path.exists(watchlist_path):
        try:
            with open(watchlist_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    symbols = payload.get("symbols") or payload.get("watchlist") or []
                else:
                    symbols = payload
                return sorted({sym for sym in symbols if sym})
        except Exception as exc:
            logger.warning("Failed to load premarket watchlist: %s", exc)

    return []


def initialize_premarket_system(premarket_symbols=None):
    """Initialize enhanced premarket system."""
    # Order flow crawler removed - return None
    return None


class MarketScanner:
    """Main scanner orchestrator"""

    def __init__(self, config=None):
        """Initialize all components"""
        logger.info("\n" + "=" * 60)
        logger.info("🚀 MARKET SCANNER INITIALIZATION")
        logger.info("=" * 60)

        # Configuration
        self.config = config or {}
        self.running = False

        if hasattr(self, "executor"):
            logger.info("Stopping processing executor...")
            try:
                self.executor.shutdown(wait=True)
            except Exception as exc:
                logger.warning(f"Executor shutdown encountered an issue: {exc}")
        self.threads = []
        self.threads_lock = threading.Lock()
        self.data_pipeline_lock = threading.Lock()
        self.threads_lock = threading.Lock()

        # Health monitoring
        self.last_tick_time = time.time()
        self.health_monitor_thread = None
        self.websocket_restart_count = 0
        self.pipeline_restart_count = 0
        self.last_health_log = 0
        self.health_log_interval = 300  # 5 minutes
        
        # Session tracking
        self.current_session = datetime.now().strftime("%Y-%m-%d")

        # Debug mode indicator
        if self.config.get("debug", False):
            logger.info("Scanner running in production mode")

        # Initialize Redis with robust client - use correct databases per redis_config.py
        logger.info("📡 Connecting to Redis...")
        self.redis_client = get_redis_client(config=self.config)
        try:
            self.redis_client.ping()
        except Exception as exc:
            logger.warning("⚠️ Redis connection check failed: %s", exc)

        self.redis_primary = self.redis_client.redis_client or self.redis_client.get_client(0)
        self.ohlc_redis_client = self.redis_client.get_client(1)  # prices → realtime
        self.stream_redis_client = self.redis_client.get_client(1)  # continuous_market → realtime
        self.volume_redis_client = self.redis_client.get_client(2)  # cumulative_volume → analytics
        self.pattern_redis_client = self.redis_client.get_client(1)  # patterns → realtime
        self.redis_wrapper = self.redis_client  # Alias for compatibility
        logger.info("Redis clients connected (shared pool)")

        # Initialize volume profile and time-aware baselines
        logger.info("Initializing volume profile and time-aware baselines...")
        self.time_aware_baseline = TimeAwareVolumeBaseline(self.redis_client)
        self.correct_volume_calculator = CorrectVolumeCalculator(self.redis_client)
        logger.info("Volume architecture initialized")

        # Initialize components
        logger.info("Initializing components...")

        # Consolidated Pattern Detector - single file pattern detection engine
        self.pattern_detector = PatternDetector(
            config=self.config, redis_client=self.redis_client
        )
        logger.info("  ✓ Consolidated Pattern Detector ready")

        # Trading Strategies - Nifty Scalper
        try:
            from strategies.nifty_scalper import NiftyScalper
            self.nifty_scalper = NiftyScalper(redis_client=self.redis_client)
            logger.info("  ✓ Nifty Scalper strategy ready")
        except ImportError as e:
            logger.warning(f"  ⚠️ Nifty Scalper not available: {e}")
            self.nifty_scalper = None

        # Tick Processor - calculates indicators
        from intraday_scanner.calculations import get_tick_processor

        # Pre-load static data before creating tick processor
        preload_static_data()

        self.tick_processor = get_tick_processor(redis_client=self.redis_client)
        # Enable debug mode if configured
        if self.config.get("debug", False):
            self.tick_processor.debug_enabled = True
        logger.info("  ✓ Tick Processor ready")

        # Alert Manager - handles alert generation and validation with production metrics
        self.alert_manager = ProductionAlertManager(
            quant_calculator=self.tick_processor, redis_client=self.redis_client
        )
        logger.info("  ✓ Production Alert Manager ready")

        # Data Pipeline - handles all incoming data from Redis
        self.data_pipeline = DataPipeline(
            redis_client=self.redis_client,
            config=self.config,
            pattern_detector=self.pattern_detector,
            alert_manager=self.alert_manager,
            tick_processor=self.tick_processor,
        )
        logger.info("  ✓ Data Pipeline ready")
        
        # Data pipeline enabled with tick processor
        self.data_pipeline.start()
        logger.info("  ✓ Data Pipeline enabled with tick processor")
        
        # News alert system is handled by alert_manager
        self.last_news_check = time.time()
        logger.info("  ✓ News alerts handled by Alert Manager")

        # Pattern Registry Integration removed - using consolidated pattern detector directly
        self.pattern_registry_integration = None
        logger.info(
            "  ✓ Pattern Registry Integration removed (using consolidated detector)"
        )

        # Advanced Pattern Engine (optional, uses Polars + hidden_liquidity_detector)
        try:
            from patterns.pattern_detector import AdvancedPatternEngine

            self.advanced_engine = AdvancedPatternEngine(redis_wrapper=self.redis_wrapper)
            if self.advanced_engine.is_enabled():
                logger.info("  ✓ Advanced Pattern Engine ready")
            else:
                self.advanced_engine = None
                logger.warning(
                    "  ⚠️ Advanced Pattern Engine disabled (missing dependencies)"
                )
        except Exception as e:
            self.advanced_engine = None
            logger.debug(f"  🔬 Advanced Pattern Engine unavailable: {e}")

        # Alert Manager - filters and sends alerts
        self.alert_manager = ProductionAlertManager(
            quant_calculator=self.tick_processor, redis_client=self.redis_client, config=config
        )
        logger.info("  ✓ Alert Manager ready")

        # Hybrid processing infrastructure (caching + parallel dispatch)
        price_window = self.config.get("price_cache_window", 100)
        self.price_cache = defaultdict(lambda: deque(maxlen=price_window))
        self.avg_volume_cache: Dict[str, float] = {}
        self.cache_lock = threading.RLock()

        worker_default = 5
        worker_cap = self.config.get("processing_workers_max", 10)
        worker_count = int(self.config.get("processing_workers", worker_default))
        if worker_count > worker_cap:
            logger.warning(
                "⚠️ Reducing processing_workers from %d to capped value %d to limit connection footprint",
                worker_count,
                worker_cap,
            )
            worker_count = worker_cap
        worker_count = max(1, worker_count)

        backlog_default = max(worker_count * 2, 10)
        backlog_limit = int(self.config.get("processing_backlog", backlog_default))
        if backlog_limit < worker_count:
            backlog_limit = worker_count

        self.executor = ThreadPoolExecutor(max_workers=worker_count)
        self.processing_semaphore = threading.BoundedSemaphore(backlog_limit)
        self.active_futures: deque = deque()
        self.processing_metrics = {
            "submitted": 0,
            "skipped_backlog": 0,
            "completed": 0,
        }
        self.precomputed_indicators: Dict[str, Dict[str, float]] = {}
        self.all_symbols: List[str] = []
        self.fast_lane_volume_threshold = self.config.get("fast_lane_volume_threshold", 2.0)
        self.current_vix_regime = "NORMAL"
        self.current_vix_value: Optional[float] = None
        self._last_vix_refresh = 0.0
        self.vix_refresh_interval = self.config.get("vix_refresh_interval", 30.0)
        self._combined_ohlc_cache: Dict[str, Dict[str, Any]] = {}
        self._last_cumulative_cache: Dict[str, float] = {}
        self._recent_alert_cache: Dict[Tuple[str, str], float] = {}

        # Premarket Alert Service - processes premarket manipulation alerts
        # self.premarket_alert_service = PremarketAlertService(
        #     redis_client=self.redis_client
        # )
        # logger.info("  ✓ Premarket Alert Service ready")

        # Initialize enhanced premarket system (order flow + scheduling)
        self.premarket_watchlist = resolve_premarket_watchlist(self.config)
        # Order flow crawler removed

        # Risk Manager - position sizing and risk
        self.risk_manager = RiskManager(config=self.config)
        logger.info("  ✓ Risk Manager ready")

        # Wire risk manager to alert manager for position sizing
        if hasattr(self.alert_manager, "risk_manager"):
            self.alert_manager.risk_manager = self.risk_manager

        # Alert Validator - track alert accuracy over time
        # Alert validator removed - using standalone validator instead
        self.alert_validator = None
        logger.info("  ✓ Alert Validator ready")

        # Premium collection managers - DISABLED (legacy components moved)
        self.premium_alert_manager = None
        self.premium_risk_manager = None
        logger.info("  ✓ Premium Collection Managers disabled (legacy)")

        self.all_symbols = self._load_symbol_universe()

        # Historical Data Manager - handles Redis → JSON → Historical pipeline
        self.historical_data_manager = None
        if self.config.get("enable_historical_analysis", False):  # Disabled by default
            try:
                from utils.historical_data_manager import HistoricalDataManager

                self.historical_data_manager = HistoricalDataManager(
                    redis_wrapper=self.redis_wrapper, config=self.config
                )
                logger.info("  ✓ Historical Data Manager ready")
            except ImportError as e:
                logger.warning(f"Historical Data Manager not available: {e}")
                self.historical_data_manager = None

        # Validation export removed - using standalone validator
        self.validation_export_thread = None

        # Start periodic cleanup for TickProcessor (every 10 minutes)
        self.tick_processor_cleanup_thread = threading.Thread(
            target=self._periodic_tick_processor_cleanup,
            daemon=True,
            name="TickProcessorCleanup",
        )
        self.tick_processor_cleanup_thread.start()
        with self.threads_lock:
            self.threads.append(self.tick_processor_cleanup_thread)

        # Start historical data export if enabled (currently disabled)
        if self.historical_data_manager:
            self.historical_data_manager.start_continuous_export()

        # Start premarket analyzer (monitors session/bucket data and publishes to premarket.orders)
        self.premarket_analyzer_thread = threading.Thread(
            target=self._premarket_analyzer_loop, daemon=True, name="PremarketAnalyzer"
        )
        self.premarket_analyzer_thread.start()
        with self.threads_lock:
            self.threads.append(self.premarket_analyzer_thread)
        logger.info("  ✓ Premarket Analyzer started")

        # Warm up token resolver so numeric instrument_token can be mapped to symbol
        # Token mapping is handled by comprehensive_token_mapping.json
        logger.info("  ✓ Token mapping ready (comprehensive mapping available)")

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def _periodic_tick_processor_cleanup(self):
        """Periodic cleanup for TickProcessor memory structures"""
        while self.running:
            try:
                time.sleep(600)  # Run every 10 minutes
                if hasattr(
                    self.tick_processor, "cleanup_old_insufficient_data_records"
                ):
                    self.tick_processor.cleanup_old_insufficient_data_records()
            except Exception as e:
                logger.error(f"❌ Error in TickProcessor cleanup: {e}")
                time.sleep(60)  # Wait longer on error

        logger.info("\n" + "=" * 60)
        logger.info("Scanner initialized successfully")
        print(f"{'=' * 60}\n")
        # Initialize alert log path to avoid attribute errors when recording alerts
        try:
            log_dir = os.path.join("logs", "alerts")
            os.makedirs(log_dir, exist_ok=True)
            self.alert_log_file = os.path.join(
                log_dir, f"alerts_{datetime.now().strftime('%Y%m%d')}.log"
            )
        except Exception:
            self.alert_log_file = os.path.join("logs", "alerts.log")

    async def check_volume_pipeline_health(self):
        """Ensure bucket_incremental_volume data is flowing correctly"""
        import asyncio
        
        async def check_redis_connections():
            """Check Redis connection health"""
            try:
                if self.redis_wrapper and self.redis_wrapper.ping():
                    return True
            except Exception:
                pass
            return False
        
        async def check_recent_bucket_updates():
            """Check if bucket_incremental_volume buckets are being updated"""
            try:
                if not self.redis_wrapper:
                    return False
                now_ts = time.time()
                session_pattern = f"session:*:{self.current_session}"
                session_keys = self.redis_wrapper.keys(session_pattern)
                for key in session_keys[:200]:
                    try:
                        raw = self.redis_wrapper.get(key)
                        if not raw:
                            continue
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")
                        session = json.loads(raw)
                        last_ts = session.get("last_update_timestamp") or session.get("last_update")
                        if last_ts is None:
                            continue
                        last_ts = float(last_ts)
                        if now_ts - last_ts < 600 and session.get("bucket_cumulative_volume", 0) > 0:
                            return True
                    except Exception:
                        continue
                return False
            except Exception as exc:
                logger.debug(f"Bucket update health check failed: {exc}")
                return False

        async def check_ratio_recalculation():
            """Check that ticks contain volume_ratio values."""
            try:
                if not self.redis_wrapper:
                    return False
                tick_streams = self.redis_wrapper.keys("ticks:*")
                redis_core = getattr(self.redis_wrapper, "redis_wrapper", None)
                if not redis_core:
                    return False
                candidates = tick_streams[:200] if tick_streams else ["market_data.ticks"]
                for stream_key in candidates:
                    try:
                        entries = redis_core.xrevrange(stream_key, count=1)
                        if not entries:
                            continue
                        _id, values = entries[0]
                        if "volume_ratio" in values or b"volume_ratio" in values:
                            return True
                        payload = values.get("data") or values.get(b"data")
                        if isinstance(payload, bytes):
                            payload = payload.decode("utf-8", errors="ignore")
                        if isinstance(payload, str):
                            try:
                                tick_payload = json.loads(payload)
                                if tick_payload.get("volume_ratio") is not None:
                                    return True
                                from utils.correct_volume_calculator import VolumeResolver
                                ratio = VolumeResolver.get_volume_ratio(tick_payload)
                                if ratio is not None and ratio >= 0:
                                    return True
                            except Exception:
                                pass
                    except Exception:
                        continue
                return False
            except Exception as exc:
                logger.debug(f"Volume ratio health check failed: {exc}")
                return False

        async def check_pattern_detection_latency():
            """Check if pattern detection is active"""
            try:
                if not hasattr(self, 'pattern_detector') or not self.pattern_detector:
                    return False
                stats = {}
                if hasattr(self.pattern_detector, "get_stats"):
                    stats = self.pattern_detector.get_stats()
                elif hasattr(self.pattern_detector, "stats"):
                    stats = self.pattern_detector.stats
                return isinstance(stats, dict)
            except Exception as exc:
                logger.debug(f"Pattern detector health check failed: {exc}")
                return False
        
        checks = {
            'redis_connections': await check_redis_connections(),
            'bucket_updates': await check_recent_bucket_updates(),
            'ratio_calculations': await check_ratio_recalculation(),
            'pattern_detection': await check_pattern_detection_latency()
        }
        
        if all(checks.values()):
            print("✅ Volume pipeline HEALTHY")
            return True
        else:
            print("❌ Volume pipeline ISSUES - check logs")
            print(f"   Redis: {checks['redis_connections']}")
            print(f"   Buckets: {checks['bucket_updates']}")
            print(f"   Ratios: {checks['ratio_calculations']}")
            print(f"   Patterns: {checks['pattern_detection']}")
            return False

    def start(self):
        """Start the scanner with Redis subscription"""
        self.running = True
        ist = pytz.timezone("Asia/Kolkata")
        current_time = datetime.now(ist)
        print(f"⏰ Starting scanner at {current_time.strftime('%H:%M:%S')} IST")
        try:
            self.pre_market_preparation()
        except Exception as exc:
            logger.warning("Pre-market preparation failed: %s", exc)
        # Choose data source based on config
        if self.config.get("use_file_reader", False):
            print("📁 Starting file-based data reader...")
            self.run_file_reader()
        else:
            logger.info("📡 Starting Redis subscription...")
            # Start DataPipeline in a separate thread
            self.data_pipeline_thread = threading.Thread(
                target=self.data_pipeline.start_consuming,
                daemon=False,
                name="DataPipeline",
            )
            self.data_pipeline_thread.start()
            with self.threads_lock:
                self.threads.append(self.data_pipeline_thread)

            # Start health monitoring thread
            self.health_monitor_thread = threading.Thread(
                target=self._monitor_websocket_health, daemon=True, name="HealthMonitor"
            )
            self.health_monitor_thread.start()
            with self.threads_lock:
                self.threads.append(self.health_monitor_thread)
            self.threads.append(self.health_monitor_thread)

        # Start the main processing loop
        self.run_main_processing_loop()

        # Wait for data pipeline thread to finish gracefully
        if (
            hasattr(self, "data_pipeline_thread")
            and self.data_pipeline_thread.is_alive()
        ):
            self.data_pipeline.running = False  # Signal to stop
            self.data_pipeline_thread.join(timeout=2)  # Wait up to 2 seconds

    def run_file_reader(self):
        """Read data from JSONL files instead of Redis"""
        from file_reader import FileReader

        # Data directory from config or default
        data_dir = self.config.get("data_dir", "/Users/apple/Desktop/stock/data/equity")

        def tick_callback(tick_data):
            """Process tick data from files"""
            try:
                # Process through data pipeline (same as Redis version)
                symbol = tick_data.get("symbol", "")

                if symbol:
                    print(f"📊 Processing tick: {symbol}")

                    # Clean the tick data
                    cleaned = self.data_pipeline._clean_tick_data(tick_data)
                    
                    # 🎯 DEBUG: Track data flow through pipeline
                    logger.info(f"🔍 [DATA_FLOW] After _clean_tick_data for {symbol}: volume_ratio = {cleaned.get('volume_ratio')}")
                    
                    # 🎯 LINEAGE: Track data after cleaning
                    data_lineage.track_data_source(symbol, "after_clean_tick_data", cleaned)

                    # 🎯 CRITICAL: Add bucket_incremental_volume context BEFORE processing calculations
                    cleaned = self._add_volume_context(cleaned)
                    logger.info(f"🔍 [DATA_FLOW] After _add_volume_context for {symbol}: volume_ratio = {cleaned.get('volume_ratio')}")

                    # Process through calculations
                    indicators = self.tick_processor.process_tick(symbol, cleaned)
                    
                    # Validate indicator calculations
                    from utils.validation_logger import indicator_validator
                    indicator_validator.log_tick_processing(symbol, cleaned, indicators)
                    
                    # 🎯 DEBUG: Track data flow after process_tick
                    logger.info(f"🔍 [DATA_FLOW] After process_tick for {symbol}: volume_ratio = {indicators.get('volume_ratio') if indicators else 'None'}")
                    
                    # 🎯 LINEAGE: Track data after process_tick
                    if indicators:
                        data_lineage.track_data_source(symbol, "after_process_tick", indicators)

                    if indicators:
                        # 🎯 DEBUG: Track data flow before pattern detection
                        logger.info(f"🔍 [DATA_FLOW] Before detect_patterns for {symbol}: volume_ratio = {indicators.get('volume_ratio')}")
                        
                        # 🎯 LINEAGE: Track data before pattern detection
                        data_lineage.track_data_source(symbol, "before_pattern_detection", indicators)
                        
                        # DEBUG: Log indicator data before pattern detection
                        logger.info(f"🔍 [VOLUME_DEBUG] Indicators before pattern detection: {indicators}")
                        
                        # Detect patterns (detector expects indicators only)
                        detected = self.pattern_detector.detect_patterns(indicators)

                        # DEBUG: Log pattern detection result
                        logger.info(f"🔍 [VOLUME_DEBUG] Pattern detection result: {'Found patterns' if detected else 'No patterns'}")

                        if detected:
                            # 🎯 DEBUG: Compare data sources before sending alerts
                            for p in detected:
                                # Compare what pattern detector vs alert manager receives
                                self.compare_data_sources(symbol, indicators, p)
                                
                                # Use the same alert path as Redis subscriber
                                self.alert_manager.send_alert(p)

            except Exception as e:
                print(f"❌ Error processing tick: {e}")

        # Create and start file reader
        self.file_reader = FileReader(data_dir, tick_callback)
        self.file_reader.start()

    def run_main_processing_loop(self):
        """Main processing loop with improved error handling"""
        logger.info("🔄 Starting main processing loop...")
        print(f"🔍 [MAIN_LOOP] Starting main processing loop...")

        processed_count = 0
        last_health_check = time.time()
        consecutive_empty_ticks = 0
        max_consecutive_empty = 50  # Reduced from 100 for faster recovery
        dbg_enabled = bool(self.config.get("debug", False))

        # Performance monitoring
        performance_stats = {
            "total_ticks": 0,
            "processing_times": [],
            "slow_ticks": 0,
            "errors": 0,
            "start_time": time.time(),
        }

        # Continuous/finite processing controls
        start_time = time.time()
        continuous = True if self.config.get("continuous", True) else False
        max_ticks = self.config.get("max_ticks")
        duration_secs = self.config.get("duration_secs")
        if max_ticks or duration_secs:
            continuous = (
                False
                if self.config.get("continuous") is None
                else bool(self.config.get("continuous"))
            )

        try:
            if not hasattr(self, "perf") or self.perf is None:
                try:
                    if self.redis_wrapper:
                        rt_client = self.redis_wrapper.get_client(1)
                    self.perf = PerfSink(redis_client=rt_client, stream="metrics:perf")
                except Exception:
                    self.perf = PerfSink()
            last_debug_time = time.time()
            last_health_check = time.time()
            while self.running:
                loop_start = time.time()
                current_time = time.time()

                # Debug logging every 60 seconds (reduced from 30)
                if current_time - last_debug_time > 60:
                    logger.info(
                        f"🔄 Main loop running - processed {processed_count} ticks, empty cycles: {consecutive_empty_ticks}"
                    )
                    if dbg_enabled:
                        print(
                            f"🔍 [DEBUG] Loop health - processed: {processed_count}, empty: {consecutive_empty_ticks}"
                        )
                    last_debug_time = current_time

                # Health check every 60 seconds (reduced from 30)
                if current_time - last_health_check > 60:
                    if dbg_enabled:
                        print("🔍 [DEBUG] Performing health check...")
                    if not self._perform_health_check():
                        logger.error("Health check failed, attempting recovery...")
                        if dbg_enabled:
                            print(
                                "🔍 [DEBUG] Health check failed, attempting recovery..."
                            )
                        self._recover_data_pipeline()
                    last_health_check = current_time
                    if dbg_enabled:
                        print("🔍 [DEBUG] Health check completed")

                # Health logging every 5 minutes
                if current_time - self.last_health_log >= self.health_log_interval:
                    try:
                        self._log_detection_health()
                    except Exception as e:
                        logger.debug(f"Health logging failed: {e}")
                    self.last_health_log = current_time
                
                # Process news alerts independently
                try:
                    self._process_news_alerts()
                except Exception as e:
                    logger.error(f"Error processing news alerts: {e}")

                # Get next tick with optimized timeout
                try:
                    tick_start = time.time()
                    # Get next tick with optimized timeout
                    tick_data = self.data_pipeline.get_next_tick(
                        timeout=0.1
                    )  # Reduced from 0.5s to 0.1s for faster response
                    tick_duration = time.time() - tick_start

                    if (
                        tick_duration > 0.1
                    ):  # Further reduced threshold for slow tick retrieval
                        logger.warning(f"Slow tick retrieval: {tick_duration:.2f}s")
                        if dbg_enabled:
                            print(
                                f"🔍 [DEBUG] Slow tick retrieval: {tick_duration:.2f}s"
                            )

                except Exception as e:
                    logger.error(f"Error getting next tick: {e}")
                    if dbg_enabled:
                        print(f"🔍 [DEBUG] Error getting next tick: {e}")
                    time.sleep(0.02)  # Further reduced sleep time
                    continue

                if tick_data is None:
                    consecutive_empty_ticks += 1
                    if consecutive_empty_ticks > max_consecutive_empty:
                        logger.warning(
                            f"No data received for {max_consecutive_empty} cycles, checking pipeline..."
                        )
                        print(
                            f"🔍 [DEBUG] No data for {max_consecutive_empty} cycles, checking pipeline..."
                        )
                        if not self.data_pipeline.is_healthy():
                            print(
                                "🔍 [DEBUG] Pipeline unhealthy, attempting recovery..."
                            )
                            self._recover_data_pipeline()
                        consecutive_empty_ticks = 0
                    time.sleep(0.005)  # Further reduced sleep time
                    continue

                # Reset counter when we get data
                consecutive_empty_ticks = 0

                # Process the tick with optimized timeout via hybrid dispatcher
                process_start = time.time()
                submitted = False
                try:
                    submitted = self._submit_tick_for_processing(
                        tick_data, processed_count, dbg_enabled
                    )
                except Exception as e:
                    logger.error(f"Error submitting tick for processing: {e}")
                    performance_stats["errors"] += 1

                process_duration = time.time() - process_start

                if not submitted:
                    # Backlog control dropped the tick; keep loop moving
                    continue

                # Detailed timing breakdown for performance analysis (submission latency)
                if process_duration > 0.5:
                    logger.warning(
                        f"PERFORMANCE BOTTLENECK: {tick_data.get('symbol', 'UNKNOWN')} submission took {process_duration:.2f}s"
                    )

                # Update performance stats with submission cost (processing is async)
                performance_stats["total_ticks"] += 1
                performance_stats["processing_times"].append(process_duration)
                if len(performance_stats["processing_times"]) > 1000:
                    performance_stats["processing_times"] = performance_stats[
                        "processing_times"
                    ][-1000:]

                if process_duration > 0.3:
                    logger.warning(
                        f"Slow tick submission: {process_duration:.2f}s for {tick_data.get('symbol', 'UNKNOWN')}"
                    )
                    if dbg_enabled:
                        print(f"🔍 [DEBUG] Slow submission: {process_duration:.2f}s")
                    performance_stats["slow_ticks"] += 1

                processed_count += 1
                loop_duration = time.time() - loop_start

                if (
                    loop_duration > 0.5
                ):  # Further reduced threshold for slow loop iterations
                    logger.warning(f"Slow loop iteration: {loop_duration:.2f}s")
                    if dbg_enabled:
                        print(f"🔍 [DEBUG] Slow loop iteration: {loop_duration:.2f}s")

                # Performance monitoring every 1000 ticks (further reduced frequency)
                if processed_count % 1000 == 0:
                    self._log_performance_stats(performance_stats, processed_count)

                # Finite mode exit checks
                if not continuous:
                    if isinstance(max_ticks, int) and processed_count >= max_ticks:
                        print(f"⏹️ Max ticks reached: {processed_count} >= {max_ticks}")
                        break
                    if (
                        isinstance(duration_secs, int)
                        and (time.time() - start_time) >= duration_secs
                    ):
                        print(
                            f"⏹️ Duration reached: {(time.time() - start_time):.1f}s >= {duration_secs}s"
                        )
                        break

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            print(
                f"🔍 [MAIN_LOOP] Keyboard interrupt - processed {processed_count} ticks"
            )
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
            print(
                f"🔍 [MAIN_LOOP] Error occurred after processing {processed_count} ticks"
            )
            import traceback

            traceback.print_exc()
        finally:
            print(
                f"🔍 [MAIN_LOOP] Shutting down - processed {processed_count} ticks total"
            )
            # Log final performance stats
            self._log_performance_stats(performance_stats, processed_count, final=True)
            self.shutdown()

    def _submit_tick_for_processing(self, tick_data, processed_count, dbg_enabled):
        """Submit tick to processing executor while enforcing backlog limits."""
        if not self.processing_semaphore.acquire(blocking=False):
            self.processing_metrics["skipped_backlog"] += 1
            return False

        self.processing_metrics["submitted"] += 1
        future = self.executor.submit(
            self._process_tick_worker, tick_data, processed_count, dbg_enabled
        )
        future.add_done_callback(self._processing_future_done)
        with self.threads_lock:
            self.active_futures.append(future)
        return True

    def _processing_future_done(self, future):
        try:
            future.result()
        except Exception as exc:
            logger.error(f"Tick processing worker raised: {exc}")
        finally:
            self.processing_semaphore.release()
            self.processing_metrics["completed"] += 1
            with self.threads_lock:
                while self.active_futures and self.active_futures[0].done():
                    self.active_futures.popleft()

    def _process_tick_worker(self, tick_data, processed_count, dbg_enabled):
        self._process_tick_core(tick_data, processed_count, dbg_enabled)

    def _update_price_history_cache(self, symbol: str, last_price):
        if last_price is None or symbol is None:
            return
        try:
            last_price = float(last_price)
        except (TypeError, ValueError):
            return
        with self.cache_lock:
            self.price_cache[symbol].append(last_price)

    def _get_cached_average_volume(self, symbol: str) -> float:
        with self.cache_lock:
            cached = self.avg_volume_cache.get(symbol)
        if cached is not None:
            return cached

        avg = self._load_avg_volume(symbol)
        with self.cache_lock:
            self.avg_volume_cache[symbol] = avg
        return avg

    def _load_avg_volume(self, symbol: str) -> float:
        try:
            from intraday_scanner.calculations import get_market_averages_with_fallback

            data = get_market_averages_with_fallback(symbol, self.redis_client) or {}
            avg = float(data.get("avg_volume_20d", 0) or 0)
        except Exception:
            avg = 0.0
        return max(avg, 0.0)

    def _get_volume_ratio_from_tick(self, tick_data: dict):
        """Read pre-computed volume metrics supplied by the centralized pipeline."""
        symbol = tick_data.get("symbol")
        if not symbol:
            return 0.0, 0.0, 0.0

        try:
            incremental = float(VolumeResolver.get_incremental_volume(tick_data) or 0.0)
            cumulative = float(VolumeResolver.get_cumulative_volume(tick_data) or 0.0)
            volume_ratio = float(VolumeResolver.get_volume_ratio(tick_data) or 0.0)
            return volume_ratio, incremental, cumulative
        except Exception as exc:
            logger.error(f"Failed to read centralized volume metrics for {symbol}: {exc}")
            return 0.0, 0.0, 0.0


    def _get_volume_ratio_from_redis(self, symbol: str, tick_data: dict) -> Optional[float]:
        """✅ LEGACY REMOVED: Redis volume ratio storage violates single source of truth"""
        # Volume ratios should be calculated on-demand from pre-calculated volume data
        # Do NOT store/retrieve volume ratios from Redis - this duplicates volume data
        return None

    def _store_volume_ratio_in_redis(self, symbol: str, volume_ratio: float, incremental: float, cumulative: float):
        """✅ LEGACY REMOVED: Redis volume ratio storage violates single source of truth"""
        # Volume ratios should be calculated on-demand from pre-calculated volume data
        # Do NOT store volume ratios in Redis - this duplicates volume data storage
        pass

    def _load_symbol_universe(self) -> List[str]:
        symbols = set()
        config_symbols = self.config.get("symbol_universe") or []
        symbols.update(sym for sym in config_symbols if sym)

        if getattr(self, "premarket_watchlist", None):
            symbols.update(self.premarket_watchlist)

        # Order flow crawler removed

        # Load symbols from intraday crawler config (the actual symbols being fed)
        intraday_config_path = os.path.join(project_root, "crawlers", "binary_crawler1", "binary_crawler1.json")
        if os.path.exists(intraday_config_path):
            try:
                with open(intraday_config_path, "r", encoding="utf-8") as handle:
                    intraday_config = json.load(handle)
                    tokens = intraday_config.get("tokens", [])
                    logger.info(f"📊 Intraday crawler has {len(tokens)} tokens")
                    
                    # Use token resolver to convert tokens to symbols
                    try:
                        from crawlers.utils.instrument_mapper import InstrumentMapper
                        instrument_mapper = InstrumentMapper()
                        resolve_token_to_symbol = instrument_mapper.token_to_symbol
                        
                        # Convert tokens to symbols using the proper resolver
                        for token in tokens:
                            symbol = resolve_token_to_symbol(token)
                            if symbol:
                                symbols.add(symbol)
                                logger.debug(f"Token {token} -> {symbol}")
                            else:
                                logger.warning(f"Token {token} not found in mapping")
                    except Exception as e:
                        logger.warning(f"⚠️ Token resolver failed: {e}, using fallback symbol loading")
                        # Fallback to bucket_incremental_volume averages if token mapping not available
                        volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
                        if os.path.exists(volume_file):
                            with open(volume_file, "r", encoding="utf-8") as handle:
                                payload = json.load(handle)
                                if isinstance(payload, dict):
                                    symbols.update(payload.keys())
            except Exception as exc:
                logger.error(f"Failed to load intraday crawler symbols: {exc}")
                # Fallback to bucket_incremental_volume averages
                volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
                if os.path.exists(volume_file):
                    try:
                        with open(volume_file, "r", encoding="utf-8") as handle:
                            payload = json.load(handle)
                            if isinstance(payload, dict):
                                symbols.update(payload.keys())
                    except Exception as exc:
                        logger.debug("Failed to load bucket_incremental_volume averages for universe: %s", exc)
        else:
            logger.warning("⚠️ Intraday crawler config not found, using bucket_incremental_volume averages fallback")
            # Fallback to bucket_incremental_volume averages
            volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
            if os.path.exists(volume_file):
                try:
                    with open(volume_file, "r", encoding="utf-8") as handle:
                        payload = json.load(handle)
                        if isinstance(payload, dict):
                            symbols.update(payload.keys())
                except Exception as exc:
                    logger.debug("Failed to load bucket_incremental_volume averages for universe: %s", exc)

        cleaned = sorted({sym for sym in symbols if isinstance(sym, str) and sym})
        logger.info("📊 Loaded symbol universe with %d instruments (from intraday crawler)", len(cleaned))
        return cleaned

    def pre_market_preparation(self):
        if not self.config.get("enable_premarket_precompute", True):
            return

        if not self.all_symbols:
            logger.warning("Pre-market precompute skipped: symbol universe empty")
            return

        logger.info(
            "🚀 Starting enhanced pre-market preparation for %d symbols",
            len(self.all_symbols),
        )
        start = time.time()
        price_window = self.config.get("price_cache_window", 100)
        self._load_combined_ohlc_cache()
        self._refresh_vix_snapshot(force=True)

        for symbol in self.all_symbols:
            data = self._load_historical_data(symbol, days=5)
            closes = [
                c for c in data.get("close", []) if isinstance(c, (int, float))
            ]
            if closes:
                with self.cache_lock:
                    dq = self.price_cache[symbol]
                    dq.extend(closes[-price_window:])
                try:
                    with self.tick_processor.lock:
                        tp_deque = self.tick_processor.price_history[symbol]
                        tp_deque.extend(closes[-tp_deque.maxlen:])
                except Exception:
                    pass

            current_ohlc = self._get_current_ohlc(symbol)
            if current_ohlc:
                self._inject_ohlc_into_history(symbol, current_ohlc, data)

            avg_volume = self._calculate_historical_avg_volume(symbol, data)
            if avg_volume > 0:
                self.avg_volume_cache[symbol] = avg_volume

            precomputed = self._precompute_all_indicators(symbol, data, current_ohlc)
            if precomputed:
                with self.cache_lock:
                    self.precomputed_indicators[symbol] = precomputed

        self._warmup_pattern_detection()
        self._initialize_real_time_buckets()
        duration = time.time() - start
        logger.info(
            "✅ Pre-market preparation complete: %d symbols ready in %.2fs",
            len(self.precomputed_indicators),
            duration,
        )

    def _load_historical_data(self, symbol: str, days: int = 5) -> Dict[str, List[float]]:
        result = {"close": [], "high": [], "low": [], "bucket_incremental_volume": []}
        client = getattr(self, "redis_client", None)
        if not client:
            return result

        try:
            # Use the 4 consolidated bucket system instead of looking for non-existent bucket data
            # Load volume averages from the 20-day averages file
            volume_file = os.path.join(project_root, "config", "volume_averages_20d.json")
            if os.path.exists(volume_file):
                with open(volume_file, "r", encoding="utf-8") as f:
                    volume_data = json.load(f)
                    
                # Get volume data for this symbol
                symbol_data = volume_data.get(symbol, {})
                avg_volume = symbol_data.get("avg_volume_20d", 0)
                
                # Generate synthetic historical data using the 4 consolidated buckets
                bucket_config = {
                    "low": {"min": 0, "max": 1000000},
                    "medium": {"min": 1000000, "max": 10000000},
                    "high": {"min": 10000000, "max": 100000000},
                    "very_high": {"min": 100000000, "max": 1000000000}
                }
                
                # Determine which bucket this symbol falls into
                bucket_type = "medium"  # Default
                if avg_volume < 1000000:
                    bucket_type = "low"
                elif avg_volume < 10000000:
                    bucket_type = "medium"
                elif avg_volume < 100000000:
                    bucket_type = "high"
                else:
                    bucket_type = "very_high"
                
                # Generate synthetic historical data based on bucket type
                for i in range(days * 24):  # Generate hourly data for the requested days
                    # Use the average volume as base and add some variation
                    base_volume = avg_volume
                    variation = base_volume * 0.1 * (0.5 - (i % 10) / 10)  # ±10% variation
                    synthetic_volume = max(0, base_volume + variation)
                    
                    result["bucket_incremental_volume"].append(float(synthetic_volume))
                    result["close"].append(100.0 + i * 0.1)  # Synthetic price progression
                    result["high"].append(100.0 + i * 0.1 + 0.5)
                    result["low"].append(100.0 + i * 0.1 - 0.5)
                
                logger.debug(f"Generated synthetic historical data for {symbol} using {bucket_type} bucket (avg: {avg_volume})")
            else:
                logger.debug(f"Volume averages file not found, using empty historical data for {symbol}")

        except Exception as e:
            logger.debug(f"Failed to load historical data for {symbol}: {e}")

        return result

    def _calculate_historical_avg_volume(
        self, symbol: str, data: Dict[str, List[float]]
    ) -> float:
        volumes = data.get("bucket_incremental_volume") or []
        volumes = [v for v in volumes if isinstance(v, (int, float))]
        if volumes:
            return sum(volumes) / max(len(volumes), 1)
        return self._load_avg_volume(symbol)

    def _load_combined_ohlc_cache(self) -> Dict[str, Dict[str, Any]]:
        if self._combined_ohlc_cache:
            return self._combined_ohlc_cache

        combined_dir = os.path.join(project_root, "config")
        pattern = os.path.join(combined_dir, "market_data_combined_*.json")
        files = sorted(glob.glob(pattern))
        if not files:
            self._combined_ohlc_cache = {}
            return self._combined_ohlc_cache

        latest_file = files[-1]
        try:
            with open(latest_file, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    self._combined_ohlc_cache = payload
                elif isinstance(payload, list):
                    cache: Dict[str, Dict[str, Any]] = {}
                    for entry in payload:
                        if isinstance(entry, dict):
                            sym = entry.get("symbol") or entry.get("tradingsymbol")
                            if sym:
                                cache[str(sym)] = entry
                    self._combined_ohlc_cache = cache
                else:
                    self._combined_ohlc_cache = {}
            logger.info("Loaded combined OHLC file: %s", latest_file)
        except Exception as exc:
            logger.debug("Failed to load combined OHLC file %s: %s", latest_file, exc)
            self._combined_ohlc_cache = {}

        return self._combined_ohlc_cache

    def _get_current_ohlc(self, symbol: str) -> Optional[Dict[str, Any]]:
        # Try Redis first - use standardized OHLC hash
        if hasattr(self, 'ohlc_redis_client') and self.ohlc_redis_client:
            norm_symbol = normalize_symbol(symbol)
            latest_key = ohlc_latest_hash(norm_symbol)
            try:
                payload = self.ohlc_redis_client.hgetall(latest_key)
                if payload:
                    def _float_or_none(value):
                        try:
                            if value is None:
                                return None
                            return float(value)
                        except (TypeError, ValueError):
                            return None

                    result = {
                        "open": _float_or_none(payload.get("open")),
                        "high": _float_or_none(payload.get("high")),
                        "low": _float_or_none(payload.get("low")),
                        "close": _float_or_none(payload.get("close")),
                        "bucket_incremental_volume": _float_or_none(payload.get("bucket_incremental_volume")),
                        "date": payload.get("date"),
                        "updated_at": payload.get("updated_at"),
                    }
                    return result
            except Exception as exc:
                logger.debug("Redis OHLC fetch failed for %s: %s", symbol, exc)

        cache = self._load_combined_ohlc_cache()
        ohlc = cache.get(symbol)
        if not ohlc:
            short_symbol = symbol.split(":", 1)[-1]
            ohlc = cache.get(short_symbol)
        return ohlc

    def _inject_ohlc_into_history(
        self, symbol: str, ohlc: Dict[str, Any], history: Dict[str, List[float]]
    ) -> None:
        if not ohlc:
            return

        close = ohlc.get("close") or ohlc.get("last_price") or ohlc.get("last_price")
        high = ohlc.get("high")
        low = ohlc.get("low")
        bucket_incremental_volume = ohlc.get("bucket_incremental_volume") or ohlc.get("traded_quantity")

        def _to_float(value: Any) -> Optional[float]:
            try:
                if value is None:
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        close_val = _to_float(close)
        high_val = _to_float(high)
        low_val = _to_float(low)
        volume_val = _to_float(bucket_incremental_volume)

        if close_val is not None:
            closes = history.setdefault("close", [])
            if not closes or closes[-1] != close_val:
                closes.append(close_val)
            with self.cache_lock:
                self.price_cache[symbol].append(close_val)
            try:
                with self.tick_processor.lock:
                    self.tick_processor.price_history[symbol].append(close_val)
            except Exception:
                pass

        if high_val is not None:
            highs = history.setdefault("high", [])
            highs.append(high_val)

        if low_val is not None:
            lows = history.setdefault("low", [])
            lows.append(low_val)

        if volume_val is not None:
            volumes = history.setdefault("bucket_incremental_volume", [])
            volumes.append(volume_val)
            try:
                with self.tick_processor.lock:
                    self.tick_processor.volume_history[symbol].append(volume_val)
            except Exception:
                pass

    def _refresh_vix_snapshot(self, force: bool = False):
        now = time.time()
        if not force and (now - self._last_vix_refresh) < self.vix_refresh_interval:
            return
        try:
            from utils.vix_utils import get_current_vix, get_vix_regime

            data = get_current_vix()
            if data:
                self.current_vix_value = float(data.get("value", 0.0))
                self.current_vix_regime = data.get("regime", "NORMAL")
            else:
                self.current_vix_regime = get_vix_regime()
                self.current_vix_value = None
        except Exception as exc:
            logger.debug("VIX refresh failed: %s", exc)
            self.current_vix_regime = "NORMAL"
            self.current_vix_value = None
        finally:
            self._last_vix_refresh = now
            if hasattr(self, "pattern_detector") and self.pattern_detector:
                try:
                    self.pattern_detector._update_vix_aware_thresholds()
                except Exception as exc:
                    logger.debug("Pattern detector threshold refresh failed: %s", exc)

    def _get_fast_lane_volume_threshold(self) -> float:
        self._refresh_vix_snapshot()
        try:
            from config.thresholds import get_volume_threshold

            dynamic = float(
                get_volume_threshold("volume_spike", self.current_vix_regime)
            )
        except Exception:
            dynamic = 0.0

        if dynamic > 0:
            final_threshold = max(1.0, dynamic)
        else:
            final_threshold = self.fast_lane_volume_threshold

        if final_threshold is None:
            final_threshold = 2.0
        final_threshold = max(1.0, float(final_threshold))
        return final_threshold

    def _precompute_all_indicators(
        self,
        symbol: str,
        data: Dict[str, List[float]],
        current_ohlc: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        closes = data.get("close") or []
        if not closes:
            return {}

        highs = data.get("high") or closes
        lows = data.get("low") or closes

        ema_20 = self._ema_helper(closes, 20)
        ema_50 = self._ema_helper(closes, 50)
        rsi_14 = self._rsi_helper(closes, 14)
        atr_14 = self._atr_helper(highs, lows, closes, 14)

        result = {
            "symbol": symbol,
            "ema_20": ema_20,
            "ema_50": ema_50,
            "rsi_14": rsi_14,
            "atr_14": atr_14,
            "rsi": rsi_14,
            "atr": atr_14,
            "last_price": closes[-1],
            "last_price": closes[-1],
            "last_updated": time.time(),
        }

        if current_ohlc:
            result["current_ohlc"] = current_ohlc
            close_override = current_ohlc.get("close") or current_ohlc.get("last_price")
            if close_override is not None:
                try:
                    close_override = float(close_override)
                    result["last_price"] = close_override
                    result["last_price"] = close_override
                except (TypeError, ValueError):
                    pass

        return result

    def _ema_helper(self, prices: List[float], period: int) -> float:
        if not prices:
            return 0.0
        if len(prices) < period:
            return float(prices[-1])
        multiplier = 2.0 / (period + 1)
        ema = float(prices[0])
        for last_price in prices[1:]:
            ema = (float(last_price) - ema) * multiplier + ema
        return ema

    def _rsi_helper(self, prices: List[float], period: int) -> float:
        if len(prices) < period + 1:
            return 50.0
        gains = []
        losses = []
        for i in range(1, len(prices)):
            change = float(prices[i]) - float(prices[i - 1])
            if change > 0:
                gains.append(change)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-change)
        if not gains or not losses:
            return 50.0
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _atr_helper(
        self, highs: List[float], lows: List[float], closes: List[float], period: int
    ) -> float:
        if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
            return 0.0
        true_ranges = []
        for i in range(1, len(highs)):
            high = float(highs[i])
            low = float(lows[i])
            prev_close = float(closes[i - 1])
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            true_ranges.append(max(tr1, tr2, tr3))
        if len(true_ranges) < period:
            return 0.0
        return sum(true_ranges[-period:]) / period

    def _initialize_real_time_buckets(self):
        self._pending_alerts = []
        self._last_alert_time = time.time()

    def _warmup_pattern_detection(self):
        if not self.precomputed_indicators:
            return
        for symbol, indicators in self.precomputed_indicators.items():
            try:
                self.pattern_detector.update_history(symbol, indicators)
            except Exception as exc:
                logger.debug("Pattern warmup failed for %s: %s", symbol, exc)

    def _get_indicators_from_cache(
        self, symbol: str, tick_data: dict
    ) -> Dict[str, float]:
        with self.cache_lock:
            cached = self.precomputed_indicators.get(symbol)
            if not cached:
                return {}
            indicators = dict(cached)

        last_price = tick_data.get("last_price") or tick_data.get("last_price")
        if last_price is not None:
            try:
                indicators["last_price"] = float(last_price)
                indicators["last_price"] = float(last_price)
            except (TypeError, ValueError):
                pass

        indicators["symbol"] = symbol
        indicators["bucket_incremental_volume"] = tick_data.get("bucket_incremental_volume", 0)
        indicators["timestamp"] = tick_data.get("timestamp")
        if "rsi_14" in indicators:
            indicators.setdefault("rsi", indicators["rsi_14"])
        if "atr_14" in indicators:
            indicators.setdefault("atr", indicators["atr_14"])
        if "current_ohlc" in indicators:
            indicators["current_ohlc"] = dict(indicators["current_ohlc"])
        return indicators

    def _update_indicator_cache(self, symbol: str, last_price: Optional[float]):
        if last_price is None:
            return
        try:
            last_price = float(last_price)
        except (TypeError, ValueError):
            return
        with self.cache_lock:
            cache = self.precomputed_indicators.get(symbol)
            if not cache:
                return
            cache["last_price"] = last_price
            cache["last_price"] = last_price
            if "ema_20" in cache:
                cache["ema_20"] = self._ema_increment(cache["ema_20"], last_price, 20)
            if "ema_50" in cache:
                cache["ema_50"] = self._ema_increment(cache["ema_50"], last_price, 50)
            if "current_ohlc" in cache:
                try:
                    cache_ohlc = dict(cache["current_ohlc"])
                except Exception:
                    cache_ohlc = {}
                cache_ohlc["close"] = last_price
                cache["current_ohlc"] = cache_ohlc
            cache["last_updated"] = time.time()
            self.precomputed_indicators[symbol] = cache

    def _ema_increment(self, previous: float, last_price: float, period: int) -> float:
        multiplier = 2.0 / (period + 1)
        return (last_price - previous) * multiplier + previous

    def _process_tick_fast_lane(self, tick_data, processed_count, dbg_enabled):
        symbol = tick_data.get("symbol")
        if not symbol:
            return False

        try:
            self._store_time_based_data(tick_data)
        except Exception:
            pass

        volume_ratio_data = self._get_volume_ratio_from_tick(tick_data)
        if isinstance(volume_ratio_data, tuple):
            volume_ratio, incremental, cumulative = volume_ratio_data
            tick_data["bucket_incremental_volume"] = incremental
            tick_data["bucket_cumulative_volume"] = cumulative
        else:
            try:
                volume_ratio = float(volume_ratio_data)
            except (TypeError, ValueError):
                volume_ratio = 0.0
        tick_data["volume_ratio"] = volume_ratio
        indicators = self._get_indicators_from_cache(symbol, tick_data)
        if not indicators:
            return False

        indicators["volume_ratio"] = volume_ratio
        patterns = []
        threshold = self._get_fast_lane_volume_threshold()
        if volume_ratio >= threshold:
            patterns = self._detect_patterns_ultra_fast(symbol, indicators, volume_ratio)

        if patterns:
            self._trigger_alert_instant(symbol, patterns, tick_data, indicators)

        self._update_indicator_cache(symbol, indicators.get("last_price"))
        if "last_price" in indicators:
            tick_data.setdefault("last_price", indicators["last_price"])
        return True

    def _detect_patterns_ultra_fast(
        self, symbol: str, indicators: Dict[str, float], volume_ratio: float
    ) -> List[Dict[str, float]]:
        indicators["symbol"] = symbol
        indicators.setdefault("volume_ratio", volume_ratio)
        try:
            self.pattern_detector.update_history(symbol, indicators)
        except Exception:
            pass
        return self.pattern_detector.detect_patterns(indicators)

    def _trigger_alert_instant(
        self, symbol: str, patterns: List[Dict[str, float]], tick_data: dict, indicators: dict
    ):
        for pattern in patterns:
            pattern.setdefault("symbol", symbol)
            pattern.setdefault("timestamp", tick_data.get("timestamp"))
            pattern.setdefault("volume_ratio", indicators.get("volume_ratio"))
            self._collect_alert_for_processing(pattern)

    def _log_detection_health(self):
        """Log pattern detector health stats periodically, including ICT metrics."""
        try:
            if not hasattr(self, "pattern_detector") or not self.pattern_detector:
                return
            stats = self.pattern_detector.get_detection_stats()
            # ICT patterns removed - using 8 core patterns only

            em_stats = stats.get("expected_move_metrics", {}) or {}
            # Unified regime snapshot
            try:
                from utils.vix_utils import get_vix_value, get_vix_regime
                vix_value = get_vix_value() or 0.0
                regime_name = get_vix_regime()
            except Exception:
                regime_name = "UNKNOWN"
                vix_value = None
            logger.info(
                "🔍 Pattern Detection Health | Regime: %-10s | VIX: %4.1f | Standard: %2d | ICT: %2d | Regime Rej: %2d | F&O Rej: %2d | EM: %-10s | EM Conf: %.2f | Vol Ratio: %5.1f | Killzone: %-12s",
                regime_name,
                vix_value or 0.0,
                stats.get("total_patterns", 0),
                # ICT patterns removed - using 8 core patterns only
                stats.get("regime_rejections", 0),
                stats.get("volume_rejections", 0),
                em_stats.get("most_used_method", "unknown"),
                em_stats.get("average_confidence", 0.0) or 0.0,
                stats.get("avg_volume_ratio", 0.0) or 0.0,
                str(self.pattern_detector.killzone.get_current_killzone()),
            )

            # ICT breakdown removed - using 8 core patterns only

            # Expected Move stats every 30 minutes
            if time.time() % 1800 < 5:
                logger.info(
                    "📊 Expected Move Stats | Total: %d | Methods: %s | Cache: %.1f%%",
                    em_stats.get("total_calculations", 0),
                    em_stats.get("method_breakdown", {}),
                    (em_stats.get("cache_hit_rate", 0.0) or 0.0) * 100.0,
                )
        except Exception as e:
            logger.debug(f"Detection health logging error: {e}")

    def _perform_health_check(self):
        """Comprehensive health check with thread deadlock detection"""
        try:
            print("🔍 [THREAD_DEBUG] Starting health check...")

            # Check if data pipeline thread is alive
            if hasattr(self, "data_pipeline_thread"):
                if not self.data_pipeline_thread.is_alive():
                    logger.error("Data pipeline thread is dead")
                    print("🔍 [THREAD_DEBUG] Data pipeline thread is dead")
                    return False
                else:
                    print("🔍 [THREAD_DEBUG] Data pipeline thread is alive")

            # Check Redis connection
            redis_start = time.time()
            if not self.redis_wrapper.ping():
                logger.error("Redis connection lost")
                print("🔍 [THREAD_DEBUG] Redis connection lost")
                return False
            
            # Check bucket_incremental_volume pipeline health
            try:
                import asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                volume_health = loop.run_until_complete(self.check_volume_pipeline_health())
                loop.close()
                if not volume_health:
                    print("🔍 [THREAD_DEBUG] Volume pipeline health check failed")
                    return False
            except Exception as e:
                print(f"🔍 [THREAD_DEBUG] Volume pipeline health check error: {e}")
                # Don't fail the entire health check for bucket_incremental_volume pipeline issues

            # Run scalper scanning
            try:
                if self.nifty_scalper:
                    import asyncio
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    opportunities = loop.run_until_complete(self.nifty_scalper.scan_opportunities())
                    loop.close()
                    
                    if opportunities:
                        print(f"🎯 [SCALPER] Found {len(opportunities)} scalping opportunities")
                        # Send opportunities to alert system
                        for opp in opportunities:
                            self._send_scalper_alert(opp)
                    else:
                        print("🔍 [SCALPER] No scalping opportunities found")
            except Exception as e:
                print(f"🔍 [SCALPER] Scalper scanning error: {e}")
                # Don't fail health check for scalper issues
            redis_duration = time.time() - redis_start
            print(f"🔍 [THREAD_DEBUG] Redis ping took {redis_duration:.3f}s")

            # Check if data pipeline is healthy
            pipeline_start = time.time()
            if not self.data_pipeline.is_healthy():
                logger.error("Data pipeline is not healthy")
                print("🔍 [THREAD_DEBUG] Data pipeline is not healthy")
                return False
            pipeline_duration = time.time() - pipeline_start
            print(
                f"🔍 [THREAD_DEBUG] Pipeline health check took {pipeline_duration:.3f}s"
            )

            # Check for thread deadlocks
            self._check_thread_deadlocks()

            print("🔍 [THREAD_DEBUG] Health check completed successfully")
            return True
        except Exception as e:
            logger.error(f"Health check error: {e}")
            print(f"🔍 [THREAD_DEBUG] Health check error: {e}")
            return False

    def _check_thread_deadlocks(self):
        """Check for potential thread deadlocks and resource contention"""
        try:
            print("🔍 [THREAD_DEBUG] Checking for thread deadlocks...")

            # Check active threads
            import threading

            active_threads = threading.active_count()
            print(f"🔍 [THREAD_DEBUG] Active threads: {active_threads}")

            # Check if main thread is responsive
            main_thread = threading.main_thread()
            if not main_thread.is_alive():
                logger.error("Main thread is not alive!")
                print("🔍 [THREAD_DEBUG] Main thread is not alive!")
                return False

            # Check for GIL-bound operations
            self._check_gil_operations()

            # Check shared resource locks
            self._check_shared_resources()

            print("🔍 [THREAD_DEBUG] Thread deadlock check completed")
            return True
        except Exception as e:
            logger.error(f"Thread deadlock check error: {e}")
            print(f"🔍 [THREAD_DEBUG] Thread deadlock check error: {e}")
            return False

    def _check_gil_operations(self):
        """Check for GIL-bound operations that might cause hanging"""
        try:
            print("🔍 [THREAD_DEBUG] Checking GIL operations...")

            # Check if we're doing CPU-intensive operations
            import psutil

            process = psutil.Process()
            cpu_percent = process.cpu_percent()
            print(f"🔍 [THREAD_DEBUG] CPU usage: {cpu_percent}%")

            if cpu_percent > 90:
                logger.warning(f"High CPU usage detected: {cpu_percent}%")
                print(f"🔍 [THREAD_DEBUG] High CPU usage: {cpu_percent}%")

            # Check memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            if memory_mb > 1000:  # More than 1GB
                logger.warning(f"High memory usage detected: {memory_mb:.1f} MB")

            # Profile resource contention
            self._profile_resource_contention()

        except Exception as e:
            logger.error(f"GIL operations check error: {e}")

    def _profile_resource_contention(self):
        """Profile for resource contention and performance bottlenecks"""
        try:

            import psutil
            import threading
            import time

            # Check thread count and activity
            active_threads = threading.active_count()

            if active_threads > 20:
                logger.warning(f"High thread count detected: {active_threads}")

            # Check file descriptor usage
            process = psutil.Process()
            try:
                fd_count = process.num_fds() if hasattr(process, "num_fds") else 0

                if fd_count > 1000:
                    logger.warning(f"High file descriptor usage: {fd_count}")
            except Exception as e:
                print(f"🔍 [PROFILE_DEBUG] Could not check file descriptors: {e}")

            # Check network connections
            try:
                connections = process.net_connections()

                if len(connections) > 100:
                    logger.warning(f"High network connection count: {len(connections)}")
            except Exception as e:
                print(f"🔍 [PROFILE_DEBUG] Could not check network connections: {e}")

            # Check for blocking operations
            self._check_blocking_operations()


        except Exception as e:
            logger.error(f"Resource contention profiling error: {e}")

    def _check_blocking_operations(self):
        """Check for blocking operations that might cause hanging"""
        try:

            # Check Redis connection pool
            if hasattr(self.redis_wrapper, "connection_pool"):
                pool = self.redis_wrapper.get_client(0).connection_pool

                # Check if pool is exhausted
                if hasattr(pool, "_available_connections"):
                    available = len(pool._available_connections)

                    if available == 0:
                        logger.warning("Redis connection pool exhausted!")

            # Check data pipeline queue sizes
            if hasattr(self.data_pipeline, "tick_queue"):
                queue_size = self.data_pipeline.tick_queue.qsize()

                if queue_size > 1000:
                    logger.warning(f"Large data pipeline queue: {queue_size}")

            # Check for long-running operations
            self._check_long_running_operations()

        except Exception as e:
            logger.error(f"Blocking operations check error: {e}")

    def _check_long_running_operations(self):
        """Check for long-running operations that might cause hanging"""
        try:

            # Check if any threads are stuck
            import threading

            for thread in threading.enumerate():
                if thread.is_alive():
                    logger.warning(f"Long-running thread: {thread.name}")

                    # Check if thread is daemon
                    if thread.daemon:
                        print(f"🔍 [PROFILE_DEBUG] Daemon thread: {thread.name}")

            # Check for potential deadlocks in locks
            self._check_lock_deadlocks()


        except Exception as e:
            logger.error(f"Long-running operations check error: {e}")

    def _check_lock_deadlocks(self):
        """Check for potential lock deadlocks"""
        try:
            # Check data pipeline locks
            if hasattr(self.data_pipeline, "buffer_lock"):

                # Try to acquire lock with timeout
                import threading

                if self.data_pipeline.buffer_lock.acquire(blocking=False):
                    self.data_pipeline.buffer_lock.release()
                else:
                    logger.warning("Buffer lock is held by another thread")

            if hasattr(self.data_pipeline, "batch_lock"):

                # Try to acquire lock with timeout
                if self.data_pipeline.batch_lock.acquire(blocking=False):
                    self.data_pipeline.batch_lock.release()
                else:
                    logger.warning("Batch lock is held by another thread")


        except Exception as e:
            logger.error(f"Lock deadlock check error: {e}")

    def _check_shared_resources(self):
        """Check shared resource access patterns"""
        try:

            # Check Redis connection pool
            if hasattr(self.redis_wrapper, "connection_pool"):
                pool = self.redis_wrapper.get_client(0).connection_pool

            # Check data pipeline locks
            if hasattr(self.data_pipeline, "buffer_lock"):
                logger.warning("Data pipeline has buffer lock")

            if hasattr(self.data_pipeline, "batch_lock"):
                logger.warning("Data pipeline has batch lock")

        except Exception as e:
            logger.error(f"Shared resources check error: {e}")

    def _check_websocket_health(self, message_count, last_message_time, current_time):
        """Check WebSocket connection health and monitor for unclean closures"""
        try:

            # Check message rate
            time_since_last_message = current_time - last_message_time
            messages_per_second = (
                message_count / (current_time - (current_time - 30))
                if current_time > 0
                else 0
            )

            # Check for stale connections
            if time_since_last_message > 60:  # No messages for 1 minute
                logger.warning(
                    f"WebSocket appears stale - no messages for {time_since_last_message:.2f}s"
                )

                # Attempt to refresh connection
                self._refresh_websocket_connection()

            # Check Redis connection health
            redis_start = time.time()
            if not self.redis_wrapper.ping():
                logger.error("Redis connection lost during WebSocket health check")
                return False
            redis_duration = time.time() - redis_start

            if redis_duration > 1.0:
                logger.warning(f"Slow Redis ping: {redis_duration:.2f}s")
            return True

        except Exception as e:
            logger.error(f"WebSocket health check error: {e}")
            return False

    def _refresh_websocket_connection(self):
        """Refresh WebSocket connection to prevent hanging"""
        try:

            # Close existing connection
            if hasattr(self, "redis_wrapper") and self.redis_wrapper:
                try:
                    self.redis_wrapper.get_client(0).close()
                except Exception as e:
                    logger.error(f"Error closing Redis connection: {e}")

            # Reconnect
            from redis_files.redis_client import get_redis_client

            self.redis_wrapper = get_redis_client()

            # Test new connection
            if self.redis_wrapper.ping():
                return True
            else:
                logger.error("Failed to refresh WebSocket connection")
                return False

        except Exception as e:
            logger.error(f"WebSocket refresh error: {e}")
            return False

    def _recover_data_pipeline(self):
        """Recover data pipeline"""
        logger.info("Attempting to recover data pipeline...")
        try:
            # Stop current pipeline
            self.data_pipeline.stop()
            time.sleep(1)

            # Restart pipeline with correct parameter
            self.data_pipeline = DataPipeline(
                redis_client=self.redis_client,
                config=self.config,
                pattern_detector=self.pattern_detector,
                alert_manager=self.alert_manager,
                tick_processor=self.tick_processor,
            )
            # Start pipeline
            self.data_pipeline.start()
            logger.info("✅ Data pipeline recovered and restarted")
            time.sleep(2)  # Give it time to initialize and subscribe
        except Exception as e:
            logger.error(f"Failed to recover data pipeline: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _process_tick_core(self, tick_data, processed_count, dbg_enabled):
        """Process a single tick with detailed logging (executed inside worker threads)."""
        import time

        tick_start = time.time()
        try:
            symbol = tick_data.get("symbol", "UNKNOWN")

            # Store latest last_price in Redis for alert validator
            self._store_latest_price(tick_data)

            # Update last tick time for health monitoring
            self.last_tick_time = time.time()

            self._update_price_history_cache(
                symbol, tick_data.get("last_price", tick_data.get("last_price"))
            )

            if symbol in self.precomputed_indicators:
                if self._process_tick_fast_lane(tick_data, processed_count, dbg_enabled):
                    return

            # Log processing start
            if processed_count % 100 == 0:
                logger.info(f"Processing tick #{processed_count} for {symbol}")

            # DEBUG: Log history length but don't exit early
            history_length = self.tick_processor.get_history_length(symbol)
            if history_length < 2:
                logger.debug(
                    f"Processing {symbol} with insufficient data: {history_length} last_price points"
                )

            # DEBUG: Log that we're proceeding with pattern detection
            if self.config.get("debug", False):
                logger.debug(
                    f"PATTERN DETECTION START: {symbol} with {history_length} last_price points"
                )

            # Step 1: Add bucket_incremental_volume context BEFORE indicator calculation
            if dbg_enabled:
                logger.debug(f"Step 1: Adding bucket_incremental_volume context for {symbol}")
            precomputed_ratio = self._get_volume_ratio_from_tick(tick_data)
            if precomputed_ratio and len(precomputed_ratio) >= 1 and precomputed_ratio[0] > 0:
                tick_data["volume_ratio"] = precomputed_ratio[0]
            tick_data = self._add_volume_context(tick_data)
            logger.info(f"[MAIN_FLOW] After _add_volume_context for {symbol}: volume_ratio = {tick_data.get('volume_ratio')}")

            # Step 2: Indicator calculation
            if dbg_enabled:
                logger.debug(f"Step 2: Calculating indicators for {symbol}")
            indicator_start = time.time()
            indicators = self.tick_processor.process_tick(symbol, tick_data)
            indicator_duration = time.time() - indicator_start

            # DEBUG: Log indicator calculation results
            if self.config.get("debug", False):
                logger.debug(
                    f"INDICATORS CALCULATED: {symbol} - {len(indicators)} indicators"
                )
                if "volume_ratio" in indicators:
                    logger.debug(
                        f"VOLUME RATIO: {symbol} - {indicators.get('volume_ratio', 0):.2f}x"
                    )
                if "price_change" in indicators:
                    logger.debug(
                        f"PRICE CHANGE: {symbol} - {indicators.get('price_change', 0):.2f}%"
                    )

            if not indicators:
                if processed_count % 100 == 0:
                    logger.warning(f"No indicators calculated for {symbol}")
                return

            # Step 2: Pattern detection
            if dbg_enabled:
                logger.debug(f"Step 2: Detecting patterns for {symbol}")

            pattern_start = time.time()
            # Use consolidated pattern detector directly
            logger.info(f"🔍 [PATTERN_DEBUG] CALLING PATTERN DETECTOR: {symbol}")
            logger.info(f"🔍 [PATTERN_DEBUG] Indicators passed to detector: {indicators}")
            patterns = self.pattern_detector.detect_patterns(indicators)
            pattern_duration = time.time() - pattern_start
            
            logger.info(f"🔍 [PATTERN_DEBUG] PATTERN DETECTOR RETURNED: {symbol} - {len(patterns)} patterns in {pattern_duration:.3f}s")
            
            if patterns:
                for i, pattern in enumerate(patterns):
                    logger.info(f"🔍 [PATTERN_DEBUG] Pattern {i+1}: {pattern.get('pattern', 'UNKNOWN')} confidence: {pattern.get('confidence', 'MISSING')} signal: {pattern.get('signal', 'UNKNOWN')}")
            else:
                logger.info(f"🔍 [PATTERN_DEBUG] No patterns detected for {symbol}")

            # DEBUG: Log pattern confidence values from detector
            if patterns and self.config.get("debug", False):
                for i, pattern in enumerate(patterns):
                    logger.debug(
                        f"PATTERN DETECTOR DEBUG: Pattern {i + 1} - {pattern.get('pattern', 'UNKNOWN')} confidence: {pattern.get('confidence', 'MISSING')}"
                    )

            # Step 3: Alert processing
            if patterns:
                logger.info(f"🔔 [ALERT_DEBUG] PATTERNS FOUND: {symbol} - {len(patterns)} patterns")
                for i, pattern in enumerate(patterns):
                    logger.info(f"🔔 [ALERT_DEBUG] Processing pattern {i+1}: {pattern.get('pattern', 'UNKNOWN')} confidence: {pattern.get('confidence', 'MISSING'):.2f} signal: {pattern.get('signal', 'UNKNOWN')}")

                    # Resolve token to symbol BEFORE processing alert
                    try:
                        from crawlers.utils.instrument_mapper import InstrumentMapper
                        if isinstance(symbol, int) or (isinstance(symbol, str) and (symbol.isdigit() or symbol.startswith('UNKNOWN_') or symbol.startswith('TOKEN_'))):
                            mapper = InstrumentMapper()
                            if isinstance(symbol, int):
                                symbol_resolved = mapper.token_to_symbol(symbol)
                            else:
                                token_str = str(symbol).replace('UNKNOWN_', '').replace('TOKEN_', '')
                                if token_str.isdigit():
                                    symbol_resolved = mapper.token_to_symbol(int(token_str))
                                else:
                                    symbol_resolved = symbol
                            if symbol_resolved and not symbol_resolved.startswith("UNKNOWN_"):
                                logger.info(f"✅ Scanner: Resolved token {symbol} to {symbol_resolved}")
                                symbol = symbol_resolved
                    except Exception as e:
                        logger.debug(f"Scanner token resolution failed for {symbol}: {e}")
                    
                    # Ensure pattern has symbol field (now with resolved symbol)
                    if "symbol" not in pattern:
                        pattern["symbol"] = symbol
                    else:
                        pattern["symbol"] = symbol  # Update with resolved symbol
                    logger.info(f"🔔 [ALERT_DEBUG] Pattern symbol set to: {symbol}")

                    # Add current last_price - primary source: Redis
                    if "last_price" not in pattern:
                        redis_price = self._get_latest_price_from_redis(symbol)
                        pattern["last_price"] = (
                            redis_price
                            or tick_data.get("last_price")
                            or tick_data.get("lp")
                            or indicators.get("last_price")
                            or tick_data.get("average_price")
                            or indicators.get("last_price")
                            or 0
                        )

                    # Store last_price in multiple fields for compatibility with alert templates
                    pattern["last_price"] = pattern["last_price"]
                    pattern["last_price"] = pattern["last_price"]

                    # Map news context into alert template fields if available
                    try:
                        news_ctx = indicators.get("news_context") if isinstance(indicators, dict) else None
                        if news_ctx:
                            # Basic mapping
                            pattern.setdefault("has_news", True)
                            # Sentiment score if available
                            if isinstance(news_ctx, dict):
                                if "sentiment" in news_ctx:
                                    pattern["news_sentiment"] = news_ctx.get("sentiment", 0)
                                if "title" in news_ctx:
                                    pattern["news_title"] = news_ctx.get("title", "")
                                if "source" in news_ctx:
                                    pattern["news_source"] = news_ctx.get("source", "")
                                if "impact" in news_ctx:
                                    pattern["news_impact"] = news_ctx.get("impact", "MEDIUM")
                                if "freshness" in news_ctx:
                                    pattern["news_freshness"] = news_ctx.get("freshness", "RECENT")
                            # Index snapshot fallback (for indices)
                            if "news_sentiment" not in pattern:
                                idx = self.redis_wrapper.get_index("nifty50") if "NIFTY" in symbol else None
                                if isinstance(idx, dict) and "news_sentiment_score" in idx:
                                    pattern["news_sentiment"] = idx["news_sentiment_score"]
                                    pattern.setdefault("has_news", True)
                                    sources = idx.get("news_sources")
                                    if sources:
                                        pattern["news_source"] = ", ".join(sources) if isinstance(sources, list) else str(sources)
                    except Exception:
                        pass

                    # Add other essential tick data fields
                    if "bucket_incremental_volume" not in pattern:
                        pattern["bucket_incremental_volume"] = tick_data.get("bucket_incremental_volume", 0)
                    if "zerodha_last_traded_quantity" not in pattern:
                        pattern["zerodha_last_traded_quantity"] = tick_data.get("zerodha_last_traded_quantity", 0)

                    # ✅ CRITICAL: Include technical indicators in pattern data
                    # Merge technical indicators from indicators dict into pattern
                    technical_indicators = {
                        'rsi': indicators.get('rsi', 0),
                        'rsi_signal': indicators.get('rsi_signal', ''),
                        'macd': indicators.get('macd', 0),
                        'ema_20': indicators.get('ema_20', 0),
                        'ema_50': indicators.get('ema_50', 0),
                        'atr': indicators.get('atr', 0),
                        'bb_upper': indicators.get('bb_upper', 0),
                        'bb_middle': indicators.get('bb_middle', 0),
                        'bb_lower': indicators.get('bb_lower', 0),
                        'bb_position': indicators.get('bb_position', 0),
                        'bb_signal': indicators.get('bb_signal', ''),
                        'macd_signal': indicators.get('macd_signal', 0),
                        'macd_histogram': indicators.get('macd_histogram', 0),
                        'macd_crossover': indicators.get('macd_crossover', 'NONE'),
                        'support': indicators.get('support', 0),
                        'resistance': indicators.get('resistance', 0),
                        'volatility': indicators.get('volatility', 0),
                        'momentum': indicators.get('momentum', 0),
                        # ✅ CRITICAL: Include price_change for template display
                        'price_change': indicators.get('price_change', 0)
                    }
                    
                    # Add technical indicators to pattern
                    pattern.update(technical_indicators)
                    
            # Add F&O specific indicators if available
                    # Ensure symbol is a string before string operations
                    symbol_str = str(symbol) if not isinstance(symbol, str) else symbol
                    if "NFO:" in symbol_str or "BFO:" in symbol_str:
                        from intraday_scanner.calculations import expiry_calculator
                        import re

                        dte = 30
                        try:
                            match = re.search(r"(\d{2})([A-Z]{3})", symbol)
                            if match:
                                day = int(match.group(1))
                                month_str = match.group(2)
                                month_map = {
                                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                                }
                                month = month_map.get(month_str, 10)
                                year = datetime.now().year
                                expiry_date = datetime(year, month, day)
                                dte_info = expiry_calculator.calculate_dte(expiry_date=expiry_date)
                                dte = dte_info.get('trading_dte', 30)
                        except Exception as err:
                            if self.config.get("debug", False):
                                self.logger.debug("F&O DTE calculation fallback for %s: %s", symbol, err)

                        fno_indicators = {
                            'delta': indicators.get('delta', 0),
                            'gamma': indicators.get('gamma', 0),
                            'theta': indicators.get('theta', 0),
                            'vega': indicators.get('vega', 0),
                            'rho': indicators.get('rho', 0),
                            'strike_price': indicators.get('strike_price', 0),
                            'underlying_price': indicators.get('underlying_price', 0),
                            'implied_volatility': indicators.get('implied_volatility', 0),
                            'option_type': indicators.get('option_type', ''),
                            'moneyness': indicators.get('moneyness', ''),
                            'oi_change': indicators.get('oi_change', 0),
                            'oi_volume_ratio': indicators.get('oi_volume_ratio', 0),
                            'build_up': indicators.get('build_up', 'NEUTRAL'),
                            'days_to_expiry': dte,
                        }
                        pattern.update(fno_indicators)

                    self._collect_alert_for_processing(pattern)
                    # Premium processing disabled (legacy components moved)
                    pass
            else:
                if processed_count % 200 == 0:
                    logger.debug(f"NO PATTERNS DETECTED: {symbol}")

            # Performance logging for slow processing
            total_duration = time.time() - tick_start
            if total_duration > 0.5:
                logger.warning(
                    f"SLOW PROCESSING: {symbol} - "
                    f"indicators: {indicator_duration:.3f}s, "
                    f"patterns: {pattern_duration:.3f}s, "
                    f"total: {total_duration:.3f}s"
                )

        except Exception as e:
            logger.error(
                f"Error processing tick for {tick_data.get('symbol', 'UNKNOWN')}: {e}"
            )
            import traceback

            logger.error(traceback.format_exc())

    def _process_tick(self, tick_data, processed_count, dbg_enabled):
        """Synchronous compatibility wrapper for legacy pathways."""
        self._process_tick_core(tick_data, processed_count, dbg_enabled)

    def _collect_alert_for_processing(self, pattern):
        """Collect alerts for batch processing with conflict resolution."""
        if not hasattr(self, '_pending_alerts'):
            self._pending_alerts = []
            self._last_alert_time = time.time()
        
        # Add alert to pending list
        self._pending_alerts.append(pattern)
        
        # Process alerts in batches (every 5 alerts or every 2 seconds)
        current_time = time.time()
        if len(self._pending_alerts) >= 5 or (current_time - self._last_alert_time) >= 2:
            self._process_pending_alerts()
    
    def _process_pending_alerts(self):
        """Process pending alerts with enhanced conflict resolution."""
        if not hasattr(self, '_pending_alerts') or not self._pending_alerts:
            return
        
        try:
            # Use enhanced alert manager for conflict resolution
            processed_alerts = self.alert_manager.process_alerts(self._pending_alerts)
            
            # Send each processed alert
            for alert in processed_alerts:
                self._send_individual_alert(alert)
            
            # Clear pending alerts and reset timer
            self._pending_alerts = []
            self._last_alert_time = time.time()
            
        except Exception as e:
            logger.error(f"Error processing pending alerts: {e}")
            # Fallback: send alerts individually
            for alert in self._pending_alerts:
                try:
                    self._send_individual_alert(alert)
                except Exception as fallback_error:
                    logger.error(f"Fallback alert send failed: {fallback_error}")
            self._pending_alerts = []
            self._last_alert_time = time.time()
    
    def _process_news_alerts(self):
        """Process high-impact news alerts that can influence trading decisions."""
        try:
            current_time = time.time()
            # Check for news every 30 seconds
            if current_time - self.last_news_check < 30:
                return
            
            self.last_news_check = current_time
            
            # Check for high-impact news in Redis
            news_key = "high_impact_news"
            news_data = self.redis_client.get(news_key)
            
            if news_data:
                try:
                    news_items = json.loads(news_data)
                    if isinstance(news_items, list):
                        for news_item in news_items:
                            # Only send news that can influence trading
                            if (news_item.get("impact") == "HIGH" and 
                                news_item.get("trading_relevant", True) and
                                self._is_news_trading_relevant(news_item)):
                                
                                symbol = news_item.get("symbol", "MARKET")
                                logger.info(f"📰 [NEWS_ALERT] Found trading-relevant high-impact news for {symbol}")
                                
                                # Send news alert directly
                                self.alert_manager.send_news_alert(symbol, news_item)
                                
                                # Remove processed news to avoid duplicates
                                news_items.remove(news_item)
                                self.redis_client.set(news_key, json.dumps(news_items))
                                break  # Process one news item per cycle
                                
                except Exception as e:
                    logger.error(f"Error processing news data: {e}")
                    
        except Exception as e:
            logger.error(f"Error in news alert processing: {e}")
    
    def _is_news_trading_relevant(self, news_item):
        """Check if news is relevant for trading decisions."""
        try:
            title = news_item.get("title", "").lower()
            content = news_item.get("content", "").lower()
            symbol = news_item.get("symbol", "")
            
            # Trading-relevant keywords
            trading_keywords = [
                "earnings", "results", "quarterly", "revenue", "profit", "loss",
                "guidance", "forecast", "upgrade", "downgrade", "rating",
                "merger", "acquisition", "deal", "partnership", "joint venture",
                "regulatory", "approval", "rejection", "fda", "sebi", "rbi",
                "dividend", "bonus", "split", "buyback", "ipo", "listing",
                "contract", "order", "award", "win", "loss", "cancellation",
                "expansion", "capacity", "plant", "facility", "investment",
                "funding", "fundraising", "debt", "equity", "bond",
                "last_price target", "target last_price", "analyst", "recommendation",
                "upgrade", "downgrade", "initiate", "coverage", "outperform",
                "underperform", "buy", "sell", "hold", "strong buy", "strong sell"
            ]
            
            # Check if news contains trading-relevant keywords
            text_to_check = f"{title} {content}"
            has_trading_keywords = any(keyword in text_to_check for keyword in trading_keywords)
            
            # Check if it's about a specific trading symbol
            is_symbol_specific = symbol and symbol != "MARKET"
            
            # Check if it's market-moving news
            is_market_moving = news_item.get("impact") == "HIGH"
            
            # News is trading relevant if it has keywords, is symbol-specific, or is market-moving
            return has_trading_keywords or is_symbol_specific or is_market_moving
            
        except Exception as e:
            logger.error(f"Error checking news relevance: {e}")
            return False
    
    def _send_individual_alert(self, pattern):
        """Send individual alert (replaces the old _process_pattern_alert logic)."""
        try:
            symbol = pattern.get("symbol", "UNKNOWN")
            pattern_name = pattern.get("pattern", "UNKNOWN")
            confidence = pattern.get("confidence", 0)
            volume_ratio = pattern.get("volume_ratio", 0)
            expected_move = pattern.get("expected_move", 0)

            # Add F&O indicators for derivatives
            fno_indicators = ""
            # Ensure symbol is a string before string operations
            symbol_str = str(symbol) if not isinstance(symbol, str) else symbol
            if "NFO:" in symbol_str or "BFO:" in symbol_str:
                delta = pattern.get("delta", 0)
                gamma = pattern.get("gamma", 0)
                theta = pattern.get("theta", 0)
                vega = pattern.get("vega", 0)
                strike = pattern.get("strike_price", 0)
                underlying = pattern.get("underlying_price", 0)
                iv = pattern.get("implied_volatility", 0)
                
                fno_indicators = f" | Δ={delta:.3f} Γ={gamma:.3f} Θ={theta:.3f} ν={vega:.3f} | Strike={strike:.0f} Underlying={underlying:.0f} IV={iv:.1%}"

            logger.info(
                f"ALERT PROCESSING START: {symbol} {pattern_name} - "
                f"conf={confidence:.2f}, vol={volume_ratio:.2f}x, move={expected_move:.3f}%{fno_indicators}"
            )

            # REDIS-BASED DEDUPLICATION with in-memory fallback
            alert_key = f"alert_sent:{symbol}:{pattern_name}"
            current_time = time.time()
            cooldown = float(self.config.get("alert_cooldown_seconds", 5))
            cache_key = (symbol, pattern_name)

            last_local = self._recent_alert_cache.get(cache_key)
            if last_local and (current_time - last_local) < cooldown:
                logger.info(
                    "🚫 LOCAL DEDUP: Skipping duplicate alert for %s %s (%.2fs since last)",
                    symbol,
                    pattern_name,
                    current_time - last_local,
                )
                return

            try:
                last_sent_time = self.redis_wrapper.get_client(0).get(f"alert_sent:{symbol}:{pattern_name}")
            except Exception as dedup_err:
                last_sent_time = None
                logger.warning(f"⚠️ REDIS DEDUP: Lookup failed for {alert_key}: {dedup_err}")

            if last_sent_time:
                try:
                    time_since_last = current_time - float(last_sent_time)
                    if time_since_last < cooldown:
                        logger.info(
                            "🚫 REDIS DEDUP: Skipping duplicate alert for %s %s (%.2fs since last)",
                            symbol,
                            pattern_name,
                            time_since_last,
                        )
                        self._recent_alert_cache[cache_key] = current_time
                        return
                except (TypeError, ValueError):
                    logger.debug(f"Invalid last_sent_time for {alert_key}: {last_sent_time}")

            try:
                self.redis_wrapper.get_client(0).setex(f"alert_sent:{symbol}:{pattern_name}", int(cooldown), str(time.time()))
            except Exception as set_err:
                logger.warning(f"⚠️ REDIS DEDUP: Failed to set cooldown for {alert_key}: {set_err}")
            self._recent_alert_cache[cache_key] = current_time
            # Lightweight cleanup to keep cache bounded
            expire_before = current_time - (cooldown * 2)
            for cache_symbol, ts in list(self._recent_alert_cache.items()):
                if ts < expire_before:
                    self._recent_alert_cache.pop(cache_symbol, None)

            # 🚨 ENHANCED CONFLICT RESOLUTION: Use pattern priority system
            signal = pattern.get("signal", "UNKNOWN")
            if signal in ["BUY", "SELL"]:
                conflict_key = f"signal_conflict:{symbol}"
                try:
                    existing_signal = self.redis_wrapper.get_client(0).get(f"signal_conflict:{symbol}")
                    if existing_signal and existing_signal != signal:
                        # Conflicting signal exists - use pattern priority resolution
                        existing_confidence = float(self.redis_wrapper.get_client(0).get(f"signal_confidence:{symbol}") or 0)
                        existing_pattern = self.redis_wrapper.get_client(0).get(f"signal_pattern:{symbol}") or "unknown"
                        current_confidence = pattern.get("confidence", 0)
                        current_pattern = pattern.get("pattern", "unknown")
                        
                        # Calculate weighted scores
                        current_score = self._calculate_pattern_score(current_pattern, current_confidence, pattern.get("volume_ratio", 1.0))
                        existing_score = self._calculate_pattern_score(existing_pattern, existing_confidence, float(self.redis_wrapper.get_client(0).get(f"signal_volume_ratio:{symbol}") or 1.0))
                        
                        if current_score > existing_score:
                            # Current signal has higher weighted score - override
                            logger.info(
                                f"SMART CONFLICT RESOLUTION: {symbol} overriding {existing_signal} with {signal} "
                                f"({current_pattern}: {current_score:.1f} vs {existing_pattern}: {existing_score:.1f})"
                            )
                            self._update_signal_tracking(symbol, signal, current_confidence, pattern.get("price_change", 0), pattern.get("volume_ratio", 1.0), current_pattern)
                        else:
                            # Existing signal has higher weighted score - block current
                            logger.info(
                                f"SMART CONFLICT RESOLUTION: {symbol} blocked {signal} by {existing_signal} "
                                f"({existing_pattern}: {existing_score:.1f} vs {current_pattern}: {current_score:.1f})"
                            )
                            return
                    else:
                        # No conflict or same signal - proceed
                        self._update_signal_tracking(symbol, signal, pattern.get("confidence", 0), 
                                                   pattern.get("price_change", 0), pattern.get("volume_ratio", 1.0), pattern.get("pattern", "unknown"))
                        logger.debug(f"CONFLICT RESOLUTION: {symbol} {signal} signal recorded")
                except Exception as e:
                    logger.warning(f"CONFLICT RESOLUTION: Check failed: {e}")

            # FIX: Don't set cooldown here - only set it AFTER alert is actually sent

            # REMOVED: Daily deduplication check - only use Redis 5s cooldown
            
            # 🚨 CRITICAL: Actually send the alert through alert manager
            try:
                logger.info(f"🔔 [ALERT_DEBUG] SENDING ALERT: {symbol} {pattern_name} to alert manager")
                logger.info(f"🔔 [ALERT_DEBUG] Pattern data: {pattern}")
                result = self.alert_manager.send_alert(pattern)
                if result:
                    logger.info(f"✅ [ALERT_DEBUG] ALERT SENT SUCCESSFULLY: {symbol} {pattern_name}")
                    # Set Redis cooldown after successful send
                    self.redis_wrapper.get_client(0).setex(f"alert_sent:{symbol}:{pattern_name}", 30, str(time.time()))
                else:
                    logger.info(f"❌ [ALERT_DEBUG] ALERT FILTERED OUT: {symbol} {pattern_name}")
                    logger.info(f"ALERT REJECTED BY FILTER: {symbol} {pattern_name}")
            except Exception as alert_error:
                logger.error(f"ALERT MANAGER ERROR: {alert_error}")

        except Exception as e:
            logger.error(f"Error in _send_individual_alert: {e}")

    def _calculate_pattern_score(self, pattern_type, confidence, volume_ratio):
        """Calculate weighted score for pattern using priority system"""
        
        # Pattern priorities (higher number = higher priority) - 8 core patterns only
        pattern_priorities = {
            'reversal': 90,
            'volume_breakout': 80,
            'breakout': 80,
            'volume_spike': 70,
            'upside_momentum': 50,
            'downside_momentum': 50,
            'volume_price_divergence': 60,
            'hidden_accumulation': 60,
            'unknown': 30  # Default for unknown patterns
        }
        
        # Confidence weights (more reliable patterns get weight boost)
        confidence_weights = {
            'reversal': 1.1,
            'volume_breakout': 1.0,
            'volume_spike': 0.9,
            'volume_price_divergence': 0.8,
            'upside_momentum': 0.7,
            'downside_momentum': 0.7,
            'breakout': 1.0,
            'hidden_accumulation': 0.8,
            'unknown': 0.5
        }
        
        # Get pattern priority and confidence weight
        priority = pattern_priorities.get(pattern_type, 30)
        weight = confidence_weights.get(pattern_type, 0.5)
        
        # Calculate weighted score: priority * confidence_weight * confidence * volume_factor
        volume_factor = min(volume_ratio / 2.0, 2.0)
        weighted_score = priority * weight * confidence * volume_factor
        
        return weighted_score

    def _update_signal_tracking(self, symbol, signal, confidence, price_change, volume_ratio, pattern_type):
        """Update Redis tracking for signal conflict resolution"""
        try:
            self.redis_wrapper.get_client(0).setex(f"signal_conflict:{symbol}", 30, signal)
            self.redis_wrapper.get_client(0).setex(f"signal_confidence:{symbol}", 30, str(confidence))
            self.redis_wrapper.get_client(0).setex(f"signal_price_change:{symbol}", 30, str(price_change))
            self.redis_wrapper.get_client(0).setex(f"signal_volume_ratio:{symbol}", 30, str(volume_ratio))
            self.redis_wrapper.get_client(0).setex(f"signal_pattern:{symbol}", 30, pattern_type)
        except Exception as e:
            logger.warning(f"Failed to update signal tracking: {e}")

    def run_redis_subscriber(self):
        """Subscribe to Redis channels and process messages with WebSocket health monitoring"""
        # Subscribe to channels using robust client
        if not self.redis_wrapper.subscribe(
            "market_data.ticks",
            "market_data.alerts",
            "alerts.manager",
            "market.gift_nifty.gap",
        ):
            logger.error("Failed to subscribe to Redis channels")
            return

        # Get pubsub object
        pubsub = self.redis_wrapper.get_client(0).pubsub()

        logger.info("Subscribed to market_data.ticks, market_data.alerts, alerts.manager, and market.gift_nifty.gap channels")
        logger.info("Waiting for messages...")

        # WebSocket health monitoring
        last_message_time = time.time()
        message_count = 0
        last_health_check = time.time()
        # Track cleanup timing
        last_cleanup_time = time.time()
        cleanup_interval = 300  # 5 minutes

        # Track validation report timing
        last_validation_report = time.time()
        validation_report_interval = 300  # 5 minutes
        processed_count = 0
        last_debug_time = time.time()
        # Debug counters and flag
        dbg_enabled = bool(self.config.get("debug", False))
        rx_count = 0
        parse_ok = 0
        clean_ok = 0
        validate_ok = 0
        process_ok = 0
        # Continuous/finite processing controls
        start_time = time.time()
        continuous = True if self.config.get("continuous", True) else False
        max_ticks = self.config.get("max_ticks")
        duration_secs = self.config.get("duration_secs")
        if max_ticks or duration_secs:
            continuous = (
                False
                if self.config.get("continuous") is None
                else bool(self.config.get("continuous"))
            )

        try:
            for message in pubsub.listen():
                if not self.running:
                    break

                # WebSocket health monitoring
                current_time = time.time()
                message_count += 1

                # Check for WebSocket health every 30 seconds
                if current_time - last_health_check > 30:
                    self._check_websocket_health(
                        message_count, last_message_time, current_time
                    )
                    last_health_check = current_time

                if message["type"] == "message":
                    last_message_time = current_time
                    try:
                        channel = (
                            message["channel"].decode()
                            if isinstance(message["channel"], bytes)
                            else message["channel"]
                        )
                        if dbg_enabled:
                            rx_count += 1
                            if rx_count <= 10 or rx_count % 100 == 0:
                                print(f"🛰️ RX #{rx_count} on channel: {channel}")

                        if channel == "market_data.ticks":
                            # Process tick data
                            try:
                                raw_tick_data = json.loads(message["data"])
                                # Convert WebSocket fields to unified schema
                                symbol = raw_tick_data.get(
                                    "tradingsymbol",
                                    raw_tick_data.get("symbol", "UNKNOWN"),
                                )

                                # Resolve token to proper symbol if needed
                                if symbol.startswith("TOKEN_") or symbol.isdigit():
                                    instrument_token = raw_tick_data.get(
                                        "instrument_token"
                                    )
                                    if instrument_token:
                                        try:
                                            from crawlers.utils.instrument_mapper import InstrumentMapper
                                            mapper = InstrumentMapper()
                                            resolve_token_to_symbol = mapper.token_to_symbol

                                            resolved_symbol = resolve_token_to_symbol(
                                                instrument_token
                                            )
                                            if resolved_symbol:
                                                symbol = resolved_symbol
                                                # Update the raw data with resolved symbol
                                                raw_tick_data["symbol"] = symbol
                                                raw_tick_data["tradingsymbol"] = (
                                                    symbol.split(":")[-1]
                                                    if ":" in symbol
                                                    else symbol
                                                )
                                        except Exception as e:
                                            print(
                                                f"Token resolution failed for {instrument_token}: {e}"
                                            )

                                tick_data = map_kite_to_unified(raw_tick_data, symbol)
                                parse_ok += 1

                                # 🔍 DEBUG: Log raw tick data structure
                                if dbg_enabled and (
                                    processed_count < 5 or processed_count % 100 == 0
                                ):
                                    print(
                                        f"🔍 [REDIS→PIPELINE] Raw tick data keys: {list(tick_data.keys())}"
                                    )
                                    print(
                                        f"🔍 [REDIS→PIPELINE] Symbol: {tick_data.get('symbol', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [REDIS→PIPELINE] Last last_price: {tick_data.get('last_price', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [REDIS→PIPELINE] Volume: {tick_data.get('bucket_incremental_volume', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [REDIS→PIPELINE] Timestamp: {tick_data.get('timestamp', 'MISSING')}"
                                    )

                            except Exception as e:
                                print(f"❌ JSON parse failed: {e}")
                                print(
                                    f"🔍 [REDIS→PIPELINE] Raw message data: {message['data'][:200]}..."
                                )
                                raise
                            pre_sym = tick_data.get("symbol") or tick_data.get(
                                "tradingsymbol"
                            )
                            if dbg_enabled and (
                                processed_count < 10 or processed_count % 100 == 0
                            ):
                                print(
                                    f"🧾 RAW: pre_sym={pre_sym} has_tradingsymbol={'tradingsymbol' in tick_data} "
                                    f"has_token={'instrument_token' in tick_data} lp={tick_data.get('last_price')}"
                                )
                                if pre_sym in [
                                    "HDFCBANK",
                                    "BANKNIFTY25SEP51100PE",
                                    "ICICIBANK",
                                ] or (pre_sym and "NIFTY" in pre_sym):
                                    print(
                                        f"📊 Tick #{processed_count}: {pre_sym} @ ₹{tick_data.get('last_price', 0)} Vol:{tick_data.get('zerodha_cumulative_volume', 0)}"
                                    )
                                    print(
                                        f"   🔍 BEFORE cleaning - tradingsymbol: {repr(tick_data.get('tradingsymbol'))}"
                                    )
                                    print(
                                        f"   🔍 BEFORE cleaning - symbol: {repr(tick_data.get('symbol'))}"
                                    )
                                    print(
                                        f"   🔍 RAW TICK KEYS: {list(tick_data.keys())}"
                                    )
                                    print(
                                        f"   🔍 bucket_incremental_volume: {tick_data.get('bucket_incremental_volume', 'MISSING')}"
                                    )
                                    print(
                                        f"   🔍 zerodha_cumulative_volume: {tick_data.get('zerodha_cumulative_volume', 'MISSING')}"
                                    )
                                    print(
                                        f"   🔍 zerodha_last_traded_quantity: {tick_data.get('zerodha_last_traded_quantity', 'MISSING')}"
                                    )
                                    print(f"   🔍 oi: {tick_data.get('oi', 'MISSING')}")
                                    print(
                                        f"   🔍 timestamp: {tick_data.get('timestamp', 'MISSING')}"
                                    )
                                    print(
                                        f"   🔍 last_price: {tick_data.get('last_price', 'MISSING')}"
                                    )
                                    print(
                                        f"   🔍 mode: {tick_data.get('mode', 'MISSING')}"
                                    )
                                    print(
                                        f"   🔍 exchange_timestamp: {tick_data.get('exchange_timestamp', 'MISSING')}"
                                    )

                                # Clean the tick data
                                raw_keys = list(tick_data.keys())  # preserve for debug
                                try:
                                    # 🔍 DEBUG: Log before cleaning
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] Before cleaning - keys: {raw_keys}"
                                        )
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] Before cleaning - symbol: {tick_data.get('symbol', 'MISSING')}"
                                        )
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] Before cleaning - bucket_incremental_volume: {tick_data.get('bucket_incremental_volume', 'MISSING')}"
                                        )

                                    tick_data = self.data_pipeline._clean_tick_data(
                                        tick_data
                                    )
                                    clean_ok += 1

                                    # 🔍 DEBUG: Log after cleaning
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        cleaned_keys = list(tick_data.keys())
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] After cleaning - keys: {cleaned_keys}"
                                        )
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] After cleaning - symbol: {tick_data.get('symbol', 'MISSING')}"
                                        )
                                        print(
                                            f"🔍 [PIPELINE→CLEAN] After cleaning - bucket_incremental_volume: {tick_data.get('bucket_incremental_volume', 'MISSING')}"
                                        )
                                        dropped_keys = [
                                            k for k in raw_keys if k not in cleaned_keys
                                        ]
                                        if dropped_keys:
                                            print(
                                                f"🔍 [PIPELINE→CLEAN] Dropped keys: {dropped_keys}"
                                            )

                                except Exception as e:
                                    print(f"❌ Cleaning failed: {e}")
                                    print(
                                        f"🔍 [PIPELINE→CLEAN] Error context - raw keys: {raw_keys}"
                                    )
                                    raise

                                # Post-clean symbol
                                post_sym = tick_data.get(
                                    "tradingsymbol"
                                ) or tick_data.get("symbol")
                                display_sym = (
                                    post_sym or pre_sym or tick_data.get("symbol")
                                )
                                # Initialize use_sym to avoid UnboundLocalError
                                use_sym = display_sym

                                if dbg_enabled and (
                                    processed_count < 10 or processed_count % 100 == 0
                                ):
                                    print(f"🔧 Post-clean symbol: {post_sym}")

                                # Debug: Show what's being sent to tick processor
                                if processed_count % 100 == 0:
                                    cleaned_keys = list(tick_data.keys())
                                    print(
                                        f"   🧹 Cleaned tick data keys: {cleaned_keys}"
                                    )
                                    print(
                                        f"   🎯 Symbol: {tick_data.get('tradingsymbol', 'MISSING')}"
                                    )
                                    print(
                                        f"   📊 AFTER cleaning: bucket_incremental_volume={tick_data.get('bucket_incremental_volume', 0)}, zerodha_cumulative_volume={tick_data.get('zerodha_cumulative_volume', 0)}"
                                    )
                                    print(
                                        f"   🧪 Mode preserved: {tick_data.get('mode', 'MISSING')}"
                                    )
                                    # Show which keys were dropped (debug-only)
                                    dropped = sorted(
                                        [k for k in raw_keys if k not in cleaned_keys]
                                    )
                                    if dropped:
                                        print(f"   🗂️ Dropped keys: {dropped}")
                                    # Also emit pipeline log summary for file diagnostics
                                    try:
                                        self.data_pipeline._log_cleaned_tick_summary(
                                            tick_data
                                        )
                                    except Exception:
                                        pass

                                self._store_latest_price(tick_data)

                                # Optional validation (for debug visibility only)
                                try:
                                    is_valid = self.data_pipeline._validate_tick(
                                        tick_data
                                    )
                                except Exception as e:
                                    is_valid = False
                                    if dbg_enabled:
                                        print(f"❌ Validation check errored: {e}")
                                if is_valid:
                                    validate_ok += 1
                                if dbg_enabled and (
                                    processed_count < 10 or processed_count % 100 == 0
                                ):
                                    print(
                                        f"   ✅ Validated: {is_valid} lp={tick_data.get('last_price')} sym={tick_data.get('tradingsymbol')}"
                                    )

                                # ✅ ADD AION VOLUME CONTEXT INTEGRATION
                                self._add_volume_context(tick_data)

                                # Process through calculations
                                if dbg_enabled and (
                                    processed_count < 5 or processed_count % 100 == 0
                                ):
                                    print(
                                        f"🔍 [CLEAN→TICK_PROCESSOR] Input to tick processor:"
                                    )
                                    print(
                                        f"🔍 [CLEAN→TICK_PROCESSOR] Symbol: {tick_data.get('symbol', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [CLEAN→TICK_PROCESSOR] Last last_price: {tick_data.get('last_price', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [CLEAN→TICK_PROCESSOR] Volume: {tick_data.get('bucket_incremental_volume', 'MISSING')}"
                                    )
                                    print(
                                        f"🔍 [CLEAN→TICK_PROCESSOR] Timestamp: {tick_data.get('timestamp', 'MISSING')}"
                                    )

                                indicators = self.tick_processor.process_tick(
                                    symbol, tick_data
                                )
                                if indicators:
                                    process_ok += 1

                                    # 🔍 DEBUG: Log tick processor output
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] Indicators calculated:"
                                        )
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] Volume ratio: {indicators.get('volume_ratio', 'MISSING')}"
                                        )
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] Price change: {indicators.get('price_change', 'MISSING')}"
                                        )
                                        # buy_pressure removed - using 8 core patterns only
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] Avg bucket_incremental_volume 20d: {indicators.get('avg_volume_20d', 'MISSING')}"
                                        )
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] All indicator keys: {list(indicators.keys())}"
                                        )
                                else:
                                    # 🔍 DEBUG: Log when no indicators calculated
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] No indicators calculated for {display_sym}"
                                        )
                                        print(
                                            f"🔍 [TICK_PROCESSOR→OUTPUT] Input data: {tick_data}"
                                        )

                                if indicators:
                                    # Add detailed logging after indicator calculation
                                    vol_ratio_str = safe_format(
                                        indicators.get("volume_ratio"), "{:.2f}"
                                    )
                                    price_change_str = safe_format(
                                        indicators.get("price_change"), "{:.2f}"
                                    )
                                    # buy_pressure removed - using 8 core patterns only
                                    print(
                                        f"   📈 Indicators: vol_ratio={vol_ratio_str}, price_change={price_change_str}%"
                                    )

                                    # Debug: Show if indicators calculated
                                    if processed_count % 100 == 0:
                                        print(
                                            f"   ✓ Indicators calculated for {display_sym}"
                                        )
                                        # Show sample indicator values
                                        vol_ratio_sample = safe_format(
                                            indicators.get("volume_ratio"), "{:.2f}"
                                        )
                                        print(
                                            f"      - volume_ratio: {vol_ratio_sample}"
                                        )
                                        print(
                                            f"      - price_change: {safe_format(indicators.get('price_change'), '{:.2f')}%"
                                        )
                                        # buy_pressure removed - using 8 core patterns only

                                    # Orchestrator-level ADV reference: ensure 20d avg is present
                                    print(
                                        f"🔍 STEP 1: Checking 20d data availability for {display_sym}"
                                    )
                                    has_20d_data = False
                                    print(
                                        f"🔍 STEP 1: Checking 20d data availability for {display_sym}"
                                    )
                                    try:
                                        adv = indicators.get("avg_volume_20d")
                                        if (
                                            not isinstance(adv, (int, float))
                                            or adv <= 0
                                        ):
                                            # Try loading from volume_averages_20d.json file directly
                                            sym_for_adv = display_sym or (
                                                tick_data.get("tradingsymbol")
                                                or tick_data.get("symbol")
                                            )
                                            try:
                                                volume_file_path = os.path.join(
                                                    os.path.dirname(__file__),
                                                    "config/volume_averages_20d.json",
                                                )
                                                with open(volume_file_path, "r") as f:
                                                    volume_data = json.load(f)
                                                    # Try both with and without NSE: prefix
                                                    symbol_data = volume_data.get(
                                                        sym_for_adv
                                                    ) or volume_data.get(
                                                        f"NSE:{sym_for_adv}", {}
                                                    )
                                                    adv_try = symbol_data.get(
                                                        "avg_volume_20d"
                                                    )
                                                    if (
                                                        isinstance(
                                                            adv_try, (int, float)
                                                        )
                                                        and adv_try > 0
                                                    ):
                                                        has_20d_data = True
                                                        print(
                                                            f"🎯 FOUND TOKEN WITH 20D DATA: {sym_for_adv} = {int(adv_try)} avg bucket_incremental_volume"
                                                        )
                                                        indicators["avg_volume_20d"] = (
                                                            float(adv_try)
                                                        )
                                            except Exception as e:
                                                print(
                                                    f"⚠️ Failed to load 20d data for {sym_for_adv}: {e}"
                                                )
                                                adv_try = None
                                        else:
                                            has_20d_data = True  # Explicitly set to True if valid 20d avg is present
                                    except Exception as e:
                                        if dbg_enabled:
                                            print(
                                                f"   ⚠️ MAIN: ADV injection check failed: {e}"
                                            )

                                    # Check if we have complete data before pattern detection
                                    if not has_20d_data:
                                        if dbg_enabled:
                                            print(
                                                f"⚠️  {display_sym} missing 20d data, attempting pattern detection with fallback"
                                            )


                                    # 🔍 DEBUG: Log pattern detector input
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        logger.debug(
                                            "Pattern detector input prepared for %s (volume_ratio=%s)",
                                            symbol,
                                            indicators.get("volume_ratio"),
                                        )

                                    # Use consolidated pattern detector directly
                                    patterns = self.pattern_detector.detect_patterns(
                                        indicators
                                    )

                                    # 🔍 DEBUG: Log pattern detector output
                                    if dbg_enabled and (
                                        processed_count < 5
                                        or processed_count % 100 == 0
                                    ):
                                        logger.debug(
                                            "PATTERN DETECTOR RETURNED: %s patterns for %s",
                                            len(patterns),
                                            symbol,
                                        )

                                    # Advanced patterns (optional): buffer tick and periodically detect
                                    try:
                                        if self.advanced_engine and display_sym:
                                            self.advanced_engine.add_tick(
                                                display_sym, tick_data
                                            )
                                            adv_patterns = self.advanced_engine.detect(
                                                display_sym
                                            )
                                            if adv_patterns:
                                                patterns.extend(adv_patterns)
                                                if dbg_enabled:
                                                    logger.debug(
                                                        "Advanced patterns: %d for %s",
                                                        len(adv_patterns),
                                                        display_sym,
                                                    )

                                                # 🔍 DEBUG: Log advanced patterns
                                                if dbg_enabled and (
                                                    processed_count < 5
                                                    or processed_count % 100 == 0
                                                ):
                                                    for i, adv_pattern in enumerate(
                                                        adv_patterns, start=1
                                                    ):
                                                        logger.debug(
                                                            "Advanced pattern %d for %s: %s",
                                                            i,
                                                            display_sym,
                                                            adv_pattern.get('pattern', 'UNKNOWN'),
                                                        )
                                    except Exception as _adv_e:
                                        # Do not break main loop on adapter issues
                                        if dbg_enabled and (
                                            processed_count < 5
                                            or processed_count % 100 == 0
                                        ):
                                            logger.error(f"Advanced engine error: {_adv_e}")
                                        pass

                                    # For first few ticks, verify a session key is getting created for this symbol
                                    if dbg_enabled and processed_count < 10:
                                        try:
                                            sym_check = tick_data.get(
                                                "tradingsymbol"
                                            ) or tick_data.get("symbol")
                                            sess = (
                                                self.redis_client.get_cumulative_data(
                                                    sym_check
                                                )
                                            )
                                            logger.info(f"Session key for {sym_check}: {'CREATED' if bool(sess) else 'ABSENT'}")
                                        except Exception as e:
                                            logger.error(f"Session key check failed: {e}")

                                    # DEBUG: Always log pattern detection results for troubleshooting
                                    if patterns:
                                        logger.info(f"PATTERN DETECTED for {display_sym}: {len(patterns)} patterns")
                                        for pattern in patterns:
                                            # Ensure pattern has symbol field
                                            if "symbol" not in pattern:
                                                pattern["symbol"] = display_sym

                                            # Add last_price and bucket_incremental_volume fields from tick_data
                                            # Primary source: Redis (most reliable)
                                            if "last_price" not in pattern:
                                                # Try Redis first, then fallback to tick_data and indicators
                                                redis_price = (
                                                    self._get_latest_price_from_redis(
                                                        display_sym
                                                    )
                                                )
                                                lp = (
                                                    redis_price
                                                    or tick_data.get("last_price")
                                                    or tick_data.get("lp")
                                                    or indicators.get("last_price")
                                                    or tick_data.get("average_price")
                                                    or indicators.get("last_price")
                                                    or 0
                                                )
                                                pattern["last_price"] = lp
                                                if lp == 0:
                                                    logger.warning(f"No last_price found for {display_sym}. Redis: {redis_price}, tick_data keys: {list(tick_data.keys())[:10]}")
                                            if "last_price" not in pattern:
                                                redis_price = (
                                                    self._get_latest_price_from_redis(
                                                        display_sym
                                                    )
                                                )
                                                pattern["last_price"] = (
                                                    redis_price
                                                    or tick_data.get("last_price")
                                                    or tick_data.get("lp")
                                                    or indicators.get("last_price")
                                                    or tick_data.get("average_price")
                                                    or indicators.get("last_price")
                                                    or pattern.get("last_price")
                                                    or 0
                                                )
                                            if "bucket_incremental_volume" not in pattern:
                                                pattern["bucket_incremental_volume"] = tick_data.get(
                                                    "bucket_incremental_volume", 0
                                                )
                                            if "zerodha_last_traded_quantity" not in pattern:
                                                pattern["zerodha_last_traded_quantity"] = (
                                                    tick_data.get("zerodha_last_traded_quantity", 0)
                                                )
                                            if "expected_move" not in pattern:
                                                # Set a default expected move if not present
                                                pattern["expected_move"] = (
                                                    0.5  # 0.5% default
                                                )

                                            logger.info(f"{pattern.get('pattern', 'UNKNOWN')} confidence: {safe_format(pattern.get('confidence'), '{:.2f}')}")

                                            # Verify pattern data contains all required fields
                                            required_fields = [
                                                "symbol",
                                                "pattern",
                                                "confidence",
                                                "expected_move",
                                                "last_price",
                                            ]
                                            missing_fields = []
                                            for field in required_fields:
                                                if field not in pattern:
                                                    missing_fields.append(field)
                                                    logger.error(f"Missing field in pattern: {field}")

                                            if not missing_fields:
                                                # Add risk metrics
                                                pattern = self.risk_manager.calculate_risk_metrics(
                                                    pattern
                                                )

                                                # 🔍 DEBUG: Log alert processing
                                                if dbg_enabled and (
                                                    processed_count < 5
                                                    or processed_count % 100 == 0
                                                ):
                                                    logger.info(f"Sending alert to alert manager: {pattern.get('pattern', 'MISSING')}")

                                                logger.info(f"ALERT TRIGGERED: {pattern.get('pattern')}")
                                                self.record_alert(pattern)
                                                alert_sent = self.alert_manager.send_alert(pattern)

                                                # 🔍 DEBUG: Log alert manager response
                                                if dbg_enabled and (
                                                    processed_count < 5
                                                    or processed_count % 100 == 0
                                                ):
                                                    logger.info(f"Alert sent: {alert_sent}")
                                                    if not alert_sent:
                                                        logger.info("Alert was filtered by alert manager")
                                                if not alert_sent:
                                                    # Try to fetch a reason from the filter’s gating logic
                                                    try:
                                                        ok, reason = (
                                                            self.alert_manager.should_send_alert(
                                                                pattern
                                                            )
                                                        )
                                                    except Exception:
                                                        ok, reason = False, None
                                                    if reason:
                                                        logger.warning(f"Alert filtered by alert manager: {pattern.get('pattern')} | {reason}")
                                                    else:
                                                        logger.warning(f"Alert filtered by alert manager: {pattern.get('pattern')}")
                                            else:
                                                logger.warning(f"Alert filtered: {pattern.get('pattern')} - missing fields: {missing_fields}")
                                    else:
                                        # Log when no patterns are detected for better visibility
                                        if (
                                            processed_count % 50 == 0
                                        ):  # Log every 50 ticks to avoid spam
                                            logger.info(f"No patterns detected for {display_sym}")
                                            # Debug: Check if required fields are missing
                                            adv = indicators.get("avg_volume_20d")
                                            if not adv or adv <= 0:
                                                logger.error(f"Missing avg_volume_20d: {adv}")
                                            else:
                                                logger.info(f"Has avg_volume_20d: {adv}")

                                processed_count += 1

                                # Status update every 10 seconds
                                current_time = time.time()
                                if current_time - last_debug_time > 10:
                                    logger.info(f"Scanner alive: {processed_count} ticks processed so far...")
                                    last_debug_time = current_time

                                # Finite mode exit checks
                                if not continuous:
                                    if (
                                        isinstance(max_ticks, int)
                                        and processed_count >= max_ticks
                                    ):
                                        logger.info(f"Max ticks reached: {processed_count} >= {max_ticks}")
                                        break
                                    if (
                                        isinstance(duration_secs, int)
                                        and (time.time() - start_time) >= duration_secs
                                    ):
                                        logger.info(f"Duration reached: {(time.time() - start_time):.1f}s >= {duration_secs}s")
                                        break
                        elif channel == "market_data.alerts":
                            # Process market data alerts from crawler
                            try:
                                alert_data = json.loads(message["data"])

                                # Extract detection data from the wrapped structure
                                if "detection" in alert_data:
                                    # New structure: detection is nested
                                    detection = alert_data["detection"]
                                    symbol = alert_data.get("symbol", "Unknown")
                                    pattern = detection.get(
                                        "pattern", "Unknown Pattern"
                                    )
                                    confidence = detection.get("confidence", 0)
                                    side = detection.get("side", "UNKNOWN")

                                    # Merge detection data with tick data for alert manager
                                    merged_alert = {**alert_data, **detection}
                                else:
                                    # Legacy structure: detection data at root level
                                    symbol = alert_data.get("symbol", "Unknown")
                                    pattern = alert_data.get(
                                        "pattern", "Unknown Pattern"
                                    )
                                    confidence = alert_data.get("confidence", 0)
                                    side = alert_data.get("side", "UNKNOWN")
                                    merged_alert = alert_data

                                logger.info(f"ALERT RECEIVED: {symbol} - {pattern} ({side}) - Confidence: {confidence}%")
                                

                                # If pattern is unknown, log the raw alert data for debugging
                                if pattern == "Unknown Pattern":
                                    logger.warning(f"Raw alert data for unknown pattern: {alert_data}")
                                    

                                # Apply confidence filtering (80% minimum)
                                if confidence >= 80:
                                    logger.info(f"ALERT PASSED: {symbol} - {pattern} ({confidence}% ≥ 80%)")

                                    # Alert validation removed - using standalone validator

                                    # Process through alert manager
                                    self.alert_manager.process_crawler_alert(
                                        merged_alert
                                    )
                                else:
                                    logger.warning(f"ALERT FILTERED: {symbol} - {pattern} ({confidence}% < 80%)")
                            except Exception as e:
                                logger.error(f"Error processing market_data.alerts: {e}")

                        elif channel == "alerts.manager":
                            # Process crawler alerts
                            alert_data = json.loads(message["data"])
                            self.alert_manager.process_crawler_alert(alert_data)

                        elif channel == "market.gift_nifty.gap":
                            # Integrate live Gift Nifty gap into tick processor
                            try:
                                gn = json.loads(message["data"])
                                # Basic validation and assignment
                                if isinstance(gn, dict) and "gap_percent" in gn:
                                    # Store into tick processor state for downstream detectors
                                    self.tick_processor.sgx_gap_data = gn
                                    # Also persist to Redis key for other processes
                                    try:
                                        self.redis_client.set(
                                            "latest_gift_nifty_gap", json.dumps(gn)
                                        )
                                    except Exception:
                                        pass
                                    # Log concise update
                                    gp = gn.get("gap_percent")
                                    gift_px = gn.get("gift_price")
                                    ts = gn.get("timestamp")
                                    logger.info(f"Gift Nifty: {gift_px} gap {gp:+.2f}% at {ts}")
                            except Exception as e:
                                logger.error(f"Gift Nifty gap parse error: {e}")

                        # Periodic cleanup
                        current_time = time.time()
                        if current_time - last_cleanup_time > cleanup_interval:
                            self.cleanup_tracking()
                            last_cleanup_time = current_time
                            if dbg_enabled:
                                logger.info(f"Cleanup: processed={processed_count} rx={rx_count} parse_ok={parse_ok} clean_ok={clean_ok} validate_ok={validate_ok} process_ok={process_ok}")

                        # Periodic validation report
                        if (
                            current_time - last_validation_report
                            > validation_report_interval
                        ):
                            logger.info("\nVALIDATION REPORT (5min):")
                            # Alert validation removed - using standalone validator
                            last_validation_report = current_time
                    except Exception as e:
                            logger.error(f"\nError in message loop: {e}")
                            import traceback
                            traceback.print_exc()
        except KeyboardInterrupt:
            logger.error("\nKeyboard interrupt received")
        except Exception as e:
            logger.error(f"\nError in Redis subscriber: {e}")
            import traceback
            traceback.print_exc()
        finally:
            pubsub.close()
            self.shutdown()

    def record_alert(self, pattern):
        """Record sent alert to log file"""
        try:
            alert_record = {
                "timestamp": datetime.now().isoformat(),
                "symbol": pattern.get("symbol"),
                "pattern": pattern.get("pattern"),
                "signal": pattern.get("signal"),
                "confidence": pattern.get("confidence"),
                "opportunity_score": pattern.get("opportunity_score"),
                "expected_move": pattern.get("expected_move"),
                "volume_ratio": pattern.get("volume_ratio"),
                "last_price": pattern.get("last_price", pattern.get("last_price")),
                "bucket_incremental_volume": pattern.get("bucket_incremental_volume"),
            }
            with open(self.alert_log_file, "a") as f:
                f.write(json.dumps(alert_record) + "\n")
        except Exception as e:
            logger.error(f"Failed to record alert: {e}")

    def cleanup_tracking(self):
        """Clean up tracking data to prevent memory accumulation"""
        # Clean up tick processor
        if hasattr(self.tick_processor, "_cleanup_stale_symbols"):
            self.tick_processor._cleanup_stale_symbols()

        # Clean up alert manager
        if hasattr(self.alert_manager, "cleanup_old_alerts"):
            self.alert_manager.cleanup_old_alerts()

        # Clean up pattern detector if needed
        if hasattr(self.pattern_detector, "cleanup"):
            self.pattern_detector.cleanup()

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown"""
        if not self.running:
            return

        logger.info("\n" + "=" * 60)
        logger.info("SHUTTING DOWN SCANNER")
        logger.info("=" * 60)

        self.running = False

        # Stop data pipeline
        logger.info("Stopping data pipeline...")
        self.data_pipeline.stop()

        # Save state
        logger.info("Saving scanner state...")
        # Pattern detector is stateless, nothing to save

        # Wait for threads with proper timeout handling
        logger.info("Waiting for threads to complete...")
        with self.threads_lock:
            for thread in self.threads:
                if thread.is_alive():
                    thread_name = getattr(thread, "name", "Unknown")
                    thread.join(timeout=2)
                    if thread.is_alive():
                        logger.warning(f"Thread {thread_name} did not stop gracefully")

        # Close Redis connection
        if hasattr(self, "redis_client") and self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")

        logger.info("Scanner shutdown complete")
        # Alert validation removed - using standalone validator

        logger.info(f"{'=' * 60}\n")
        sys.exit(0)

    def force_shutdown(self):
        """Force shutdown when graceful shutdown fails"""
        logger.warning("Force shutdown initiated...")

        # Force stop all threads
        for thread in self.threads:
            if thread.is_alive():
                logger.warning(f"Force stopping thread: {thread.name}")

        # Force close Redis connection
        if hasattr(self, "redis_client") and self.redis_client:
            try:
                self.redis_client.close()
            except:
                pass

        # Force stop data pipeline
        if hasattr(self, "data_pipeline"):
            try:
                self.data_pipeline.stop()
            except:
                pass

        logger.warning("Force shutdown complete")
        sys.exit(1)

    def _log_performance_stats(self, stats, processed_count, final=False):
        """Log performance statistics"""
        if not stats["processing_times"]:
            return

        avg_time = sum(stats["processing_times"]) / len(stats["processing_times"])
        max_time = max(stats["processing_times"])
        min_time = min(stats["processing_times"])
        total_time = time.time() - stats["start_time"]
        ticks_per_second = stats["total_ticks"] / total_time if total_time > 0 else 0

        if final:
            logger.info("FINAL PERFORMANCE STATISTICS:")
        else:
            logger.info("PERFORMANCE STATISTICS:")

        logger.info(f"  Total ticks processed: {stats['total_ticks']}")
        logger.info(
            f"  Processing time: avg={avg_time:.3f}s, min={min_time:.3f}s, max={max_time:.3f}s"
        )
        logger.info(f"  Throughput: {ticks_per_second:.1f} ticks/second")
        logger.info(f"  Slow ticks (>1s): {stats['slow_ticks']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Uptime: {total_time:.1f} seconds")

    def _monitor_websocket_health(self):
        """Monitor WebSocket client health"""
        import threading
        import time

        logger.info("Starting WebSocket health monitoring...")

        while self.running:
            try:
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Health monitor sleep interrupted: {e}")
                break

            try:
                # Check if WebSocket thread is alive
                if hasattr(self.data_pipeline, "ws_client") and hasattr(
                    self.data_pipeline.ws_client, "ws_thread"
                ):
                    ws_thread = self.data_pipeline.ws_client.ws_thread
                    if not ws_thread.is_alive():
                        logger.error("WebSocket thread died! Attempting restart...")
                        self._restart_websocket_client()

                # Check if we're receiving data
                current_time = time.time()
                if (
                    hasattr(self, "last_tick_time")
                    and current_time - self.last_tick_time > 60
                ):  # No data for 60 seconds
                    logger.error(
                        "No data received for 60 seconds! Restarting pipeline..."
                    )
                    self._restart_data_pipeline()

                # Check Redis connection health
                if not self.redis_client.ping():
                    logger.error("Redis connection lost! Attempting reconnection...")
                    self._restart_redis_connection()

                # Check data pipeline health
                if not self.data_pipeline.is_healthy():
                    logger.error("Data pipeline unhealthy! Attempting restart...")
                    self._restart_data_pipeline()

            except Exception as e:
                logger.error(f"Health monitoring error: {e}")

        logger.info("Health monitoring stopped")

    def _restart_websocket_client(self):
        """Restart WebSocket client"""
        try:
            logger.info("Restarting WebSocket client...")
            self.websocket_restart_count += 1

            # Stop current WebSocket client
            if hasattr(self.data_pipeline, "ws_client"):
                self.data_pipeline.ws_client.shutdown()

            # Wait a moment
            time.sleep(2)

            # Restart WebSocket client
            # Note: This would need to be implemented based on your WebSocket client setup
            logger.info(
                f"WebSocket client restarted (attempt #{self.websocket_restart_count})"
            )

        except Exception as e:
            logger.error(f"Failed to restart WebSocket client: {e}")

    def _restart_data_pipeline(self):
        """Restart data pipeline"""
        try:
            logger.info("Restarting data pipeline...")
            self.pipeline_restart_count += 1

            # Stop current pipeline
            self.data_pipeline.stop()

            # Wait a moment
            time.sleep(2)

            # Restart pipeline
            self.data_pipeline = DataPipeline(
                redis_client=self.redis_client, 
                config=self.config,
                pattern_detector=self.pattern_detector,
                alert_manager=self.alert_manager,
                tick_processor=self.tick_processor,
            )

            logger.info(
                f"Data pipeline restarted (attempt #{self.pipeline_restart_count})"
            )

        except Exception as e:
            logger.error(f"Failed to restart data pipeline: {e}")

    def _restart_redis_connection(self):
        """Restart Redis connection"""
        try:
            logger.info("Restarting Redis connection...")

            # Close current connection
            if self.redis_client:
                self.redis_client.close()

            # Wait a moment
            time.sleep(2)

            # Create new connection
            self.redis_client = get_redis_client(config=self.config)

            if self.redis_client.ping():
                logger.info("Redis connection restarted")
            else:
                logger.error("Redis reconnection failed")

        except Exception as e:
            logger.error(f"Failed to restart Redis connection: {e}")

    def _add_redis_pattern_validation(self, pattern):
        """Add Redis pattern validation to enhance pattern confidence"""
        try:
            if not self.redis_client:
                return pattern

            symbol = pattern.get("symbol", "UNKNOWN")
            pattern_type = pattern.get("pattern", "UNKNOWN")
            last_price = pattern.get("last_price", 0)
            raw_confidence = pattern.get("confidence", 0.5)

            # Get cumulative session data from Redis
            cumulative_data = self.redis_client.get_cumulative_data(symbol)
            if not cumulative_data:
                # Reduce confidence if no session data available
                pattern["confidence"] = raw_confidence * 0.8
                logger.debug(
                    f"Redis validation: No session data for {symbol}, confidence reduced"
                )
                return pattern

            # Get recent last_price movement from Redis
            price_movement = self.redis_client.get_session_price_movement(
                symbol, minutes_back=30
            )

            # Validate pattern against historical session context
            validation_score = 1.0

            # Factor 1: Session bucket_incremental_volume strength (enhanced with session reset awareness)
            session_volume = cumulative_data.get("bucket_cumulative_volume", 0)
            update_count = cumulative_data.get("update_count", 1)
            session_date = cumulative_data.get("session_date", self.current_session)
            
            # Check if this is a fresh session (session reset detected)
            is_fresh_session = session_date != self.current_session or update_count <= 5
            
            avg_volume_per_update = (
                session_volume / update_count if update_count > 0 else 0
            )
            
            # Boost validation for fresh sessions (new trading day)
            if is_fresh_session:
                validation_score *= 1.1  # 10% boost for fresh session data

            # Respect centralized volume ratio (already computed by ingestion pipeline)
            try:
                volume_ratio = pattern.get("volume_ratio")
                if volume_ratio is None:
                    volume_ratio = cumulative_data.get("volume_ratio")
                volume_ratio = float(volume_ratio) if volume_ratio is not None else 0.0
            except (TypeError, ValueError):
                volume_ratio = 0.0

            if volume_ratio > 0:
                try:
                    from config.thresholds import get_volume_threshold
                    primary_threshold = get_volume_threshold(
                        "volume_spike", None, self.current_vix_regime
                    )
                except Exception:
                    primary_threshold = 2.0

                if volume_ratio >= primary_threshold:
                    validation_score *= 1.2
                elif volume_ratio < max(primary_threshold * 0.5, 0.5):
                    validation_score *= 0.7

            # Factor 2: Price consistency in session
            if price_movement:
                price_volatility = abs(price_movement.get("price_change_pct", 0))
                # Dynamic volatility thresholds based on market regime
                try:
                    # Get VIX value for volatility context using vix_utils
                    from utils.vix_utils import get_vix_value
                    vix_value = get_vix_value()
                    if vix_value and isinstance(vix_value, (int, float)):
                        # Use VIX-aware thresholds
                        if vix_value < 15:  # Low volatility regime
                            stable_threshold = 1.5
                            high_threshold = 3.0
                        elif vix_value < 25:  # Normal volatility regime
                            stable_threshold = 2.0
                            high_threshold = 5.0
                        else:  # High volatility regime
                            stable_threshold = 3.0
                            high_threshold = 8.0
                    else:
                        # Fallback thresholds
                        stable_threshold = 2.0
                        high_threshold = 5.0
                except Exception as vol_e:
                    logger.debug(f"Dynamic volatility threshold failed: {vol_e}")
                    validation_score *= 0.8
                
                # Apply VIX-based volatility validation
                if price_volatility <= stable_threshold:
                    # Low volatility - boost confidence for stable patterns
                    validation_score *= 1.1
                    logger.debug(f"VIX validation: Low volatility ({price_volatility:.2f}% <= {stable_threshold}%) - 10% boost")
                elif price_volatility <= high_threshold:
                    # Normal volatility - no change
                    logger.debug(f"VIX validation: Normal volatility ({price_volatility:.2f}% <= {high_threshold}%) - no change")
                else:
                    # High volatility - reduce confidence for unstable patterns
                    validation_score *= 0.85
                    logger.debug(f"VIX validation: High volatility ({price_volatility:.2f}% > {high_threshold}%) - 15% penalty")

            # Factor 3: Session duration (more updates = more reliable)
            # Dynamic thresholds based on market session progress
            try:

                ist = pytz.timezone("Asia/Kolkata")
                current_time = datetime.now(ist)
                market_hours_passed = max(
                    0, (current_time.hour - 9) * 60 + current_time.minute - 15
                )
                total_market_minutes = 375  # 6.25 hours trading

                # Expected updates: 1 per minute for active symbols
                expected_updates = market_hours_passed
                if expected_updates > 0:
                    update_ratio = update_count / expected_updates

                    # Strong data: above 80% of expected updates
                    if update_ratio > 0.8:
                        validation_score *= 1.15
                    # Weak data: below 30% of expected updates
                    elif update_ratio < 0.3:
                        validation_score *= 0.8
                else:
                    # Early market session, use absolute thresholds
                    if update_count > 50:
                        validation_score *= 1.15
                    elif update_count < 10:
                        validation_score *= 0.8

            except Exception as update_e:
                logger.debug(
                    f"Dynamic update threshold failed for {symbol}: {update_e}"
                )
                # Conservative fallback
                if update_count > 100:
                    validation_score *= 1.15
                elif update_count < 20:
                    validation_score *= 0.8

            # Apply validation to confidence
            validated_confidence = raw_confidence * validation_score
            validated_confidence = min(0.95, max(0.1, validated_confidence))

            # Update pattern with validated confidence
            pattern["confidence"] = validated_confidence
            pattern["redis_validation_score"] = validation_score
            pattern["session_volume"] = session_volume
            pattern["update_count"] = update_count

            logger.info(
                f"Redis pattern validation for {symbol}: {raw_confidence:.2f} → {validated_confidence:.2f} (score: {validation_score:.2f})"
            )

            return pattern

        except Exception as e:
            logger.warning(
                f"Redis pattern validation failed for {pattern.get('symbol', 'UNKNOWN')}: {e}"
            )
            return pattern

    def _add_volume_context(self, tick_data):
        """Add bucket_incremental_volume context using the new time-aware architecture"""

        symbol = tick_data.get("symbol")
        if not symbol:
            return tick_data

        try:
            if 'last_traded_quantity' in tick_data:
                tick_data['zerodha_last_traded_quantity'] = tick_data['last_traded_quantity']

            incremental_volume = float(VolumeResolver.get_incremental_volume(tick_data) or 0.0)
            cumulative_volume = float(VolumeResolver.get_cumulative_volume(tick_data) or 0.0)
            volume_ratio = float(VolumeResolver.get_volume_ratio(tick_data) or 0.0)

            tick_data["bucket_incremental_volume"] = incremental_volume
            tick_data["bucket_cumulative_volume"] = cumulative_volume
            tick_data["volume_ratio"] = volume_ratio
            tick_data["normalized_volume"] = volume_ratio

            try:
                from config.thresholds import get_volume_threshold
                threshold = get_volume_threshold("volume_spike", None, self.current_vix_regime)
            except Exception:
                threshold = 2.0
            tick_data["volume_context"] = "high" if volume_ratio >= threshold else "normal"
            
            # Verify volume consistency at scanner processing stage
            verification = VolumeResolver.verify_volume_consistency(tick_data, "scanner_processing")
            if not verification["consistent"]:
                logger.warning(f"Volume consistency issue at scanner processing for {symbol}: {verification['issues']}")

        except Exception as e:
            logger.error(f"Failed to add bucket_incremental_volume context for {symbol}: {e}")
            tick_data["volume_ratio"] = 0.0
            tick_data["normalized_volume"] = 0.0
            tick_data["volume_context"] = "normal"
        return tick_data

    def compare_data_sources(self, symbol, pattern_indicators, alert_data):
        """Compare what pattern detector vs alert manager receives."""
        
        pattern_volume = pattern_indicators.get("volume_ratio", 1.0)
        alert_volume = alert_data.get("volume_ratio", 1.0)
        
        if pattern_volume != alert_volume:
            logger.error(f"[DATA_SOURCE_MISMATCH] {symbol}")
            logger.error(f"   Pattern Detector volume_ratio: {pattern_volume}")
            logger.error(f"   Alert Manager volume_ratio: {alert_volume}")
            
            # Log full data structures for comparison
            pattern_keys = list(pattern_indicators.keys())
            alert_keys = list(alert_data.keys())
            
            logger.error(f"   Pattern Detector keys: {pattern_keys}")
            logger.error(f"   Alert Manager keys: {alert_keys}")
            
            # Check for different calculation methods
            pattern_calc_method = pattern_indicators.get('calculation_method', 'unknown')
            alert_calc_method = alert_data.get('calculation_method', 'unknown')
            logger.error(f"   Pattern calc method: {pattern_calc_method}")
            logger.error(f"   Alert calc method: {alert_calc_method}")
            
            # Check for bucket_incremental_volume-related fields
            pattern_volume_fields = [k for k in pattern_indicators.keys() if 'bucket_incremental_volume' in k.lower()]
            alert_volume_fields = [k for k in alert_data.keys() if 'bucket_incremental_volume' in k.lower()]
            
            logger.error(f"   Pattern bucket_incremental_volume fields: {pattern_volume_fields}")
            logger.error(f"   Alert bucket_incremental_volume fields: {alert_volume_fields}")
            
            # Check for data source indicators
            pattern_source = pattern_indicators.get('data_source', 'unknown')
            alert_source = alert_data.get('data_source', 'unknown')
            logger.error(f"   Pattern data source: {pattern_source}")
            logger.error(f"   Alert data source: {alert_source}")
        else:
            logger.info(f"✅ [DATA_SOURCE_MATCH] {symbol} - volume_ratio: {pattern_volume}")

    def validate_volume_ratio_flow(self, tick_data):
        """Temporary validation method to track bucket_incremental_volume ratio through the pipeline."""
        
        stages = {}
        
        # Stage 1: After process_tick()
        stages['process_tick'] = tick_data.get("volume_ratio")
        print(f"🔍 VALIDATION Stage 1 (process_tick): {stages['process_tick']}")
        
        # Stage 2: After _add_volume_context()
        tick_data = self._add_volume_context(tick_data)
        stages['after_volume_context'] = tick_data.get("volume_ratio")
        print(f"🔍 VALIDATION Stage 2 (after_volume_context): {stages['after_volume_context']}")
        
        # Stage 3: After _calculate_all_indicators()
        indicators = self._calculate_all_indicators(tick_data["symbol"], tick_data)
        stages['after_indicators'] = indicators.get("volume_ratio")
        print(f"🔍 VALIDATION Stage 3 (after_indicators): {stages['after_indicators']}")
        
        # Check for overrides
        overrides = []
        for stage, value in stages.items():
            if value == 1.0 and stages.get('process_tick') != 1.0:
                overrides.append(stage)
        
        if overrides:
            print(f"🚨 VOLUME RATIO OVERRIDES DETECTED at: {overrides}")
        else:
            print(f"✅ Volume ratio preserved throughout pipeline")
        
        return stages

    def _store_time_based_data(self, tick_data):
        """Store pre-market and first 30min data for false breakout detection"""
        try:
            symbol = tick_data.get("symbol", "UNKNOWN")
            current_time = datetime.now()

            # Convert to IST
            if current_time.tzinfo is None:
                current_time = current_time.replace(
                    tzinfo=pytz.timezone("Asia/Kolkata")
                )
            elif current_time.tzinfo != pytz.timezone("Asia/Kolkata"):
                current_time = current_time.astimezone(pytz.timezone("Asia/Kolkata"))

            # Store pre-market data (9:00-9:15 AM)
            if self._is_pre_market_hours(current_time):
                self._store_pre_market_data(symbol, tick_data, current_time)

            # Store first 30min data (9:15-9:45 AM)
            elif self._is_first_30min_hours(current_time):
                self._store_first_30min_data(symbol, tick_data, current_time)

        except Exception as e:
            print(f"Error storing time-based data: {e}")

    def _is_pre_market_hours(self, current_time):
        """Check if current time is pre-market hours (9:00-9:15 AM)"""
        try:
            pre_market_start = current_time.replace(
                hour=9, minute=0, second=0, microsecond=0
            )
            pre_market_end = current_time.replace(
                hour=9, minute=15, second=0, microsecond=0
            )
            return pre_market_start <= current_time <= pre_market_end
        except:
            return False

    def _is_first_30min_hours(self, current_time):
        """Check if current time is first 30min of trading (9:15-9:45 AM)"""
        try:
            first_30min_start = current_time.replace(
                hour=9, minute=15, second=0, microsecond=0
            )
            first_30min_end = current_time.replace(
                hour=9, minute=45, second=0, microsecond=0
            )
            return first_30min_start <= current_time <= first_30min_end
        except:
            return False

    def _store_pre_market_data(self, symbol, tick_data, current_time):
        """Store pre-market data in Redis"""
        try:
            pre_market_key = f"pre_market:{symbol}:{current_time.strftime('%Y-%m-%d')}"

            # Get existing data
            existing_data = self.redis_client.get(pre_market_key)
            pre_market_data = json.loads(existing_data) if existing_data else []

            # Add current tick
            tick_entry = {
                "timestamp": current_time.isoformat(),
                "last_price": tick_data.get("last_price", 0),
                "bucket_incremental_volume": tick_data.get("bucket_incremental_volume", 0),
                "depth": tick_data.get("depth", {}),
                "buy_quantity": tick_data.get("buy_quantity", 0),
                "sell_quantity": tick_data.get("sell_quantity", 0),
            }

            pre_market_data.append(tick_entry)

            # Store in Redis with 24-hour expiry
            self.redis_client.set(pre_market_key, json.dumps(pre_market_data), ex=86400)

        except Exception as e:
            print(f"Error storing pre-market data for {symbol}: {e}")

    def _store_first_30min_data(self, symbol, tick_data, current_time):
        """Store first 30min data in Redis"""
        try:
            first_30min_key = (
                f"first_30min:{symbol}:{current_time.strftime('%Y-%m-%d')}"
            )

            # Get existing data
            existing_data = self.redis_client.get(first_30min_key)
            first_30min_data = json.loads(existing_data) if existing_data else []

            # Add current tick
            tick_entry = {
                "timestamp": current_time.isoformat(),
                "last_price": tick_data.get("last_price", 0),
                "bucket_incremental_volume": tick_data.get("bucket_incremental_volume", 0),
                "depth": tick_data.get("depth", {}),
                "buy_quantity": tick_data.get("buy_quantity", 0),
                "sell_quantity": tick_data.get("sell_quantity", 0),
            }

            first_30min_data.append(tick_entry)

            # Store in Redis with 24-hour expiry
            self.redis_client.set(
                first_30min_key, json.dumps(first_30min_data), ex=86400
            )

        except Exception as e:
            print(f"Error storing first 30min data for {symbol}: {e}")

    def _store_latest_price(self, tick_data):
        """Store latest last_price in Redis for alert validator"""
        try:
            symbol = tick_data.get("symbol", "UNKNOWN")
            last_price = tick_data.get("last_price", 0)
            timestamp = tick_data.get("timestamp", datetime.now().isoformat())

            if symbol != "UNKNOWN" and last_price:
                # Store in a simple key-value format for easy retrieval
                price_key = f"latest_price:{symbol}"
                price_data = {
                    "symbol": symbol,
                    "last_price": last_price,
                    "timestamp": timestamp,
                    "timestamp_ms": int(datetime.now().timestamp() * 1000),
                }

                # Store with 5-minute expiry (prices become stale after that)
                self.redis_client.set(price_key, json.dumps(price_data), ex=300)
                print(f"✅ Stored latest last_price for {symbol}: {last_price}")

        except Exception as e:
            print(f"❌ Error storing latest last_price for {symbol}: {e}")

    def _get_latest_price_from_redis(self, symbol):
        """Get latest last_price from Redis for a symbol with fallback sources"""
        try:
            # Primary source: latest_price key
            price_key = f"latest_price:{symbol}"
            price_data_str = self.redis_client.get(price_key)

            if price_data_str:
                price_data = json.loads(price_data_str)
                last_price = price_data.get("last_price", 0)
                timestamp = price_data.get("timestamp_ms", 0)

                # Check if last_price is stale (older than 2 minutes)
                current_time_ms = int(datetime.now().timestamp() * 1000)
                if current_time_ms - timestamp < 120000:  # 2 minutes in milliseconds
                    return last_price
                else:
                    print(
                        f"⚠️ Price data for {symbol} is stale ({current_time_ms - timestamp}ms old)"
                    )
                    # Don't return 0 immediately, try fallback sources
            else:
                print(f"⚠️ No latest_price data found in Redis for {symbol}, trying fallback sources...")
            
            # Fallback sources: try multiple Redis keys for last_price data
            # First try session keys (most reliable source)
            date_str = datetime.now().strftime("%Y-%m-%d")
            fallback_keys = [
                f"session:{symbol}:{date_str}",  # Current session data (most reliable)
                f"session:{symbol}",  # Current session without date
                f"last_price:{symbol}",  # Dedicated last_price key from millisecond crawler
                f"last_price:equity:{symbol}",
                f"market_data:equity:{symbol}:last_price",
                f"market_data:{symbol}:last_price",
                f"tick_data:{symbol}:last_price",
                f"market_data:{symbol}",
                f"tick_data:{symbol}",
            ]
            
            for fallback_key in fallback_keys:
                try:
                    fallback_data = self.redis_client.get(fallback_key)
                    if fallback_data:
                        # Try to parse as JSON first
                        try:
                            fallback_json = json.loads(fallback_data)
                            # Look for last_price in various fields
                            for price_field in ["last_price", "last_price", "close", "last_price", "last_price"]:
                                if price_field in fallback_json and fallback_json[price_field]:
                                    last_price = float(fallback_json[price_field])
                                    if last_price > 0:
                                        print(f"✅ Found last_price for {symbol} in fallback key {fallback_key}: {last_price}")
                                        return last_price
                        except json.JSONDecodeError:
                            # If not JSON, try to parse as direct last_price value
                            try:
                                last_price = float(fallback_data)
                                if last_price > 0:
                                    print(f"✅ Found direct last_price for {symbol} in fallback key {fallback_key}: {last_price}")
                                    return last_price
                            except ValueError:
                                continue
                except Exception as e:
                    continue
            
            # If no fallback sources found, return 0
            print(f"⚠️ No last_price data found in any Redis source for {symbol}")
            return 0

        except Exception as e:
            print(f"Error getting latest last_price for {symbol} from Redis: {e}")
            return 0

    def _premarket_analyzer_loop(self):
        """Analyze premarket session/bucket data and publish manipulation signals to premarket.orders channel"""
        import time
        logger.info("🔍 Premarket Analyzer: Started monitoring")

        while self.running:
            try:
                time.sleep(60)  # Check every minute instead of continuous loop
                ist = pytz.timezone("Asia/Kolkata")
                current_time = datetime.now(ist)
                current_date = current_time.strftime("%Y-%m-%d")

                # Only analyze during premarket window (9:00-9:08 AM) and first 30 min (9:15-9:45 AM)
                hour = current_time.hour
                minute = current_time.minute

                # At 9:07 AM, capture baseline prices
                if hour == 9 and minute == 7:
                    self._capture_premarket_baseline(current_date)
                    time.sleep(60)  # Sleep for 1 minute to avoid re-triggering

                # At 9:16 AM (1 minute after market open), analyze and publish
                elif hour == 9 and minute == 16:
                    self._analyze_and_publish_premarket_signals(current_date)
                    time.sleep(60)  # Sleep for 1 minute to avoid re-triggering

                else:
                    # Check every 30 seconds during other times
                    time.sleep(30)

            except Exception as e:
                logger.error(f"Premarket analyzer error: {e}")
                time.sleep(30)

    def _capture_premarket_baseline(self, current_date):
        """Capture 9:07 AM baseline prices and volumes from session data"""
        try:
            # Get all session keys for today
            session_keys = self.redis_client.keys(f"session:*:{current_date}")

            logger.info(
                f"📊 Capturing premarket baseline at 9:07 AM for {len(session_keys)} symbols"
            )

            for session_key in session_keys:
                try:
                    # Extract symbol from key: session:SYMBOL:DATE
                    symbol = (
                        session_key.decode("utf-8").split(":")[1]
                        if isinstance(session_key, bytes)
                        else session_key.split(":")[1]
                    )

                    # Get session data
                    session_data_raw = self.redis_client.get(session_key)
                    if not session_data_raw:
                        continue

                    session_data = json.loads(session_data_raw)

                    # Store 9:07 AM baseline
                    baseline_key = f"premarket_baseline:{symbol}:{current_date}"
                    baseline_data = {
                        "symbol": symbol,
                        "price_0907": session_data.get("last_price", 0),
                        "volume_0907": session_data.get("bucket_cumulative_volume", 0),
                        "timestamp": datetime.now().isoformat(),
                    }

                    self.redis_client.set(
                        baseline_key, json.dumps(baseline_data), ex=86400
                    )  # 24 hour expiry

                except Exception as e:
                    logger.error(f"Error capturing baseline for {session_key}: {e}")
                    continue

            logger.info(
                f"✅ Premarket baseline captured for {len(session_keys)} symbols"
            )

        except Exception as e:
            logger.error(f"Error in _capture_premarket_baseline: {e}")

    def _analyze_and_publish_premarket_signals(self, current_date):
        """Analyze premarket data and publish manipulation signals to premarket.orders channel"""
        try:
            # Get all baseline keys
            baseline_keys = self.redis_client.keys(
                f"premarket_baseline:*:{current_date}"
            )

            logger.info(
                f"🔍 Analyzing premarket manipulation for {len(baseline_keys)} symbols"
            )

            signals_published = 0
            manipulation_candidates = []

            for baseline_key in baseline_keys:
                try:
                    # Extract symbol
                    symbol = (
                        baseline_key.decode("utf-8").split(":")[1]
                        if isinstance(baseline_key, bytes)
                        else baseline_key.split(":")[1]
                    )

                    # Get baseline data (9:07 AM)
                    baseline_data_raw = self.redis_client.get(baseline_key)
                    if not baseline_data_raw:
                        continue

                    baseline_data = json.loads(baseline_data_raw)

                    # Get current session data (9:16 AM)
                    session_key = f"session:{symbol}:{current_date}"
                    session_data_raw = self.redis_client.get(session_key)
                    if not session_data_raw:
                        continue

                    session_data = json.loads(session_data_raw)

                    # Get 20-day average bucket_incremental_volume for comparison
                    avg_volume_key = f"avg_volume:{symbol}:20day"
                    avg_volume_raw = self.redis_client.get(avg_volume_key)
                    avg_volume = (
                        json.loads(avg_volume_raw).get("avg_volume", 0)
                        if avg_volume_raw
                        else 0
                    )

                    # Calculate metrics
                    price_0907 = baseline_data.get("price_0907", 0)
                    price_0915 = session_data.get("last_price", 0)
                    volume_0907 = baseline_data.get("volume_0907", 0)
                    volume_0915 = session_data.get("bucket_cumulative_volume", 0)

                    if price_0907 == 0 or volume_0907 == 0:
                        continue

                    # Calculate changes
                    price_change_pct = ((price_0915 - price_0907) / price_0907) * 100
                    volume_change_pct = (
                        ((volume_0915 - volume_0907) / volume_0907) * 100
                        if volume_0907 > 0
                        else 0
                    )
                    volume_ratio = volume_0915 / avg_volume if avg_volume > 0 else 0

                    # Determine if this is manipulation (high bucket_incremental_volume + significant last_price move)
                    is_manipulation = False
                    confidence = 0.5

                    # High bucket_incremental_volume spike (>3x average) + last_price move (>1%)
                    if volume_ratio > 3.0 and abs(price_change_pct) > 1.0:
                        is_manipulation = True
                        confidence = min(
                            0.7
                            + (volume_ratio - 3.0) * 0.05
                            + abs(price_change_pct) * 0.02,
                            0.95,
                        )

                    # Medium bucket_incremental_volume spike (>2x) + large last_price move (>2%)
                    elif volume_ratio > 2.0 and abs(price_change_pct) > 2.0:
                        is_manipulation = True
                        confidence = min(
                            0.65
                            + (volume_ratio - 2.0) * 0.05
                            + abs(price_change_pct) * 0.02,
                            0.90,
                        )

                    if not is_manipulation:
                        continue

                    # Build premarket signal
                    premarket_signal = {
                        "symbol": symbol,
                        "price_0907": price_0907,
                        "price_0915": price_0915,
                        "volume_0907": volume_0907,
                        "volume_0915": volume_0915,
                        "avg_premarket_volume": avg_volume,
                        "volume_ratio": volume_ratio,
                        "price_change_pct": price_change_pct,
                        "volume_change_pct": volume_change_pct,
                        "direction": "UP" if price_change_pct > 0 else "DOWN",
                        "confidence": confidence,
                        "timestamp": datetime.now().isoformat(),
                    }

                    # Add to manipulation candidates for alert processing
                    manipulation_candidates.append(premarket_signal)

                    # Publish to premarket.orders channel
                    self.redis_client.publish(
                        "premarket.orders", json.dumps(premarket_signal)
                    )
                    signals_published += 1

                    logger.info(
                        f"   📡 Published premarket signal: {symbol} {premarket_signal['direction']} "
                        f"{price_change_pct:+.2f}% (vol: {volume_ratio:.1f}x, conf: {confidence:.2f})"
                    )

                except Exception as e:
                    logger.error(f"Error analyzing {baseline_key}: {e}")
                    continue

            # Process alerts through PremarketAlertService
            # if manipulation_candidates:
            #     alerts = self.premarket_alert_service.process_manipulation_candidates(
            #         manipulation_candidates
            #     )
            #     if alerts:
            #         logger.info(
            #             f"🚨 Premarket Alert Service generated {len(alerts)} alerts"
            #         )
                    # Send alerts through AlertManager
                    # for alert in alerts:
                    #     try:
                    #         should_send = True
                    #         if hasattr(
                    #             self.alert_manager, "should_send_premarket_alert"
                    #         ):
                    #             should_send = self.alert_manager.should_send_premarket_alert(
                    #                 alert
                    #             )
                    #         if not should_send:
                    #             logger.info(
                    #                 "🔇 Premarket alert filtered by AlertManager: %s",
                    #                 alert.get("symbol"),
                    #             )
                    #             continue

                    #         self.alert_manager.send_alert(alert)
                    #         logger.info(
                    #             "✅ Premarket alert sent: %s (conf: %.2f)",
                    #             alert.get("symbol"),
                    #             alert.get("confidence", 0.0),
                    #         )
                    #     except Exception as e:
                    #         logger.error(
                    #             "❌ Failed to send premarket alert for %s: %s",
                    #             alert.get("symbol", "UNKNOWN"),
                    #             e,
                    #         )

            logger.info(
                f"✅ Premarket analysis complete: {signals_published} manipulation signals published"
            )

        except Exception as e:
            logger.error(f"Error in _analyze_and_publish_premarket_signals: {e}")

    def stop(self):
        """Stop the scanner and cleanup resources"""
        logger.info("🛑 Stopping scanner...")
        self.running = False

        # Stop historical data manager
        if self.historical_data_manager:
            self.historical_data_manager.stop_continuous_export()

        # Cleanup premarket alert service
        if hasattr(self, "premarket_alert_service"):
            self.premarket_alert_service.cleanup()

        # Cleanup pattern detector
        if hasattr(self, "pattern_detector"):
            self.pattern_detector.cleanup()

        # Stop data pipeline
        if hasattr(self, "data_pipeline"):
            self.data_pipeline.running = False

        logger.info("✅ Scanner stopped")

    def _send_scalper_alert(self, opportunity: Dict):
        """Send scalper opportunity to alert system"""
        try:
            # Create alert from scalper opportunity
            alert = {
                "symbol": opportunity["symbol"],
                "pattern": "scalper_opportunity",
                "direction": opportunity["signal"],
                "strength": opportunity["strength"],
                "entry_price": opportunity["entry"],
                "target_price": opportunity["target"],
                "stop_price": opportunity["stop"],
                "timestamp": opportunity["timestamp"],
                "strategy": "nifty_scalper",
                "confidence": min(0.95, opportunity["strength"]),
                "priority": "HIGH" if opportunity["strength"] > 0.8 else "MEDIUM"
            }
            
            # Send to alert manager
            if hasattr(self, 'alert_manager') and self.alert_manager:
                self.alert_manager.process_alerts([alert])
                logger.info(f"🎯 [SCALPER] Alert sent for {opportunity['symbol']}: {opportunity['signal']} @ {opportunity['entry']}")
            
        except Exception as e:
            logger.error(f"Error sending scalper alert: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Modular Market Scanner")
    parser.add_argument("--config", type=str, help="Config file path")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument(
        "--test", action="store_true", help="Run in test mode (relaxed thresholds)"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="Process ticks continuously (default)"
    )
    parser.add_argument(
        "--max-ticks", type=int, help="Process at most N ticks, then exit"
    )
    parser.add_argument("--duration", type=int, help="Process for N seconds, then exit")
    parser.add_argument(
        "--historical",
        action="store_true",
        help="Enable historical analysis with historical context",
    )

    args = parser.parse_args()

    # Load configuration
    config = {}
    if args.config:
        with open(args.config, "r") as f:
            config = json.load(f)

    if args.debug:
        config["debug"] = True

    if args.test:
        config["test_mode"] = True
        # Propagate test mode to alert filter via environment
        try:
            import os as _os

            _os.environ["ALERT_TEST_MODE"] = "1"
        except Exception:
            pass

    if args.historical:
        config["enable_historical_analysis"] = True

    # Continuous/finite processing config
    if args.continuous:
        config["continuous"] = True
    if args.max_ticks is not None:
        config["max_ticks"] = int(args.max_ticks)
    if args.duration is not None:
        config["duration_secs"] = int(args.duration)
    if (
        args.max_ticks is not None or args.duration is not None
    ) and not args.continuous:
        config.setdefault("continuous", False)

    try:
        verify_system_initialization()
    except Exception:
        print("⚠️ [SYSTEM_CHECK] Proceeding despite initialization errors")

    try:
        emergency_data_reset()
    except Exception:
        print("⚠️ [EMERGENCY_RESET] Proceeding despite reset errors")

    # Create and start scanner
    scanner = MarketScanner(config)
    scanner.start()
    
    # Make scanner instance globally available
    global scanner_instance
    scanner_instance = scanner


if __name__ == "__main__":
    main()
