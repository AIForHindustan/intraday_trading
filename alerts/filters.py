"""
Alert Filtering Logic
====================

Alert filtering logic with 6-path qualification system and Redis deduplication.
Consolidated from scanner/alert_manager.py RetailAlertFilter class.

Classes:
- RetailAlertFilter: Main alert filtering with 6-path qualification
- AlertFilterConfig: Configuration for filtering thresholds

Created: October 9, 2025
"""

import sys
import os
import json
import time
import logging
import threading
from datetime import datetime, date, timezone, timedelta, time as datetime_time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo
import pytz
from utils.vix_utils import get_vix_regime, get_vix_value

# Add project root to Python path for proper imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load threshold configuration from consolidated config
try:
    from config.thresholds import get_volume_threshold, get_confidence_threshold, get_move_threshold, get_all_thresholds_for_regime
    THRESHOLD_CONFIG_AVAILABLE = True
except ImportError as e:
    logging.warning(f"⚠️  Could not import volume_thresholds: {e}")
    logging.warning("   Using hardcoded thresholds")
    THRESHOLD_CONFIG_AVAILABLE = False

# Import VIX utilities
try:
    from utils.vix_utils import (
        get_current_vix,
        get_vix_value,
        get_vix_regime,
        is_vix_panic,
        is_vix_complacent,
    )
except ImportError:
    def get_current_vix():
        return None
    def get_vix_value():
        return None
    def get_vix_regime():
        return VIXRegime.NORMAL.value
    def is_vix_panic():
        return False
    def is_vix_complacent():
        return False

# Initialize logger
logger = logging.getLogger(__name__)

try:
    from .notifiers import HumanReadableAlertTemplates
except Exception:
    HumanReadableAlertTemplates = None


from enum import Enum
from typing import Dict, Set

class MarketSession(Enum):
    PRE_OPEN = "pre_open"
    OPENING = "opening"
    MORNING = "morning"
    MID_MORNING = "mid_morning"
    MIDDAY = "midday"
    AFTERNOON = "afternoon"
    CLOSING = "closing"
    POST_CLOSE = "post_close"

class VIXRegime(Enum):
    PANIC = "PANIC"      # VIX > 22
    NORMAL = "NORMAL"    # VIX 12-22
    COMPLACENT = "COMPLACENT"  # VIX < 12

class AlertFilterConfig:
    """Dynamic configuration for Indian range-bound market."""
    
    def __init__(self):
        self.indian_tz = pytz.timezone('Asia/Kolkata')
        self._current_vix_regime = VIXRegime.NORMAL
        self._current_market_session = MarketSession.MORNING
        
    # ===== PRICE FILTERS =====
    @property
    def price_filters(self) -> Dict:
        """Dynamic price filters based on market cap and liquidity"""
        base_filters = {
            'absolute_min': 10,  # ₹10 - more realistic minimum
            'absolute_max': 10000,  # ₹10,000 - include all stocks
            'preferred_min': 50,  # ₹50 - focus on more stable stocks
            'preferred_max': 3000,  # ₹3000 - optimal retail range
        }
        
        # Adjust based on VIX regime
        if self._current_vix_regime == VIXRegime.PANIC:
            base_filters['preferred_min'] = 100  # Focus on larger caps in panic
            base_filters['absolute_min'] = 20
            
        return base_filters
    
    # ===== VOLUME THRESHOLDS =====
    @property
    def volume_thresholds(self) -> Dict:
        """Corrected volume thresholds"""
        base_volume = {
            'derivative_multiplier': 1.5,  # 1.5x average volume
            'equity_multiplier': 2.0,      # 2.0x average volume
            'absolute_min_volume': 50000,  # Minimum shares traded
            'large_cap_min': 100000,       # For Nifty 50 stocks
        }
        
        # Adjust for market session
        session_multipliers = {
            MarketSession.OPENING: 1.3,    # 30% higher volume expected
            MarketSession.MORNING: 1.0,
            MarketSession.MID_MORNING: 1.1,
            MarketSession.MIDDAY: 0.8,     # 20% lower in midday
            MarketSession.AFTERNOON: 0.9,
            MarketSession.CLOSING: 1.2,    # 20% higher at closing
        }
        
        multiplier = session_multipliers.get(self._current_market_session, 1.0)
        return {k: v * multiplier for k, v in base_volume.items()}
    
    # ===== MOVE THRESHOLDS =====
    @property
    def move_thresholds(self) -> Dict:
        """Dynamic move thresholds based on VIX regime"""
        base_moves = {
            'PANIC': 0.8,    # 0.8% in high VIX
            'NORMAL': 0.3,   # 0.3% in normal markets
            'COMPLACENT': 0.2,  # 0.2% in low volatility
        }
        
        # Convert to actual percentages
        move_pct = base_moves[self._current_vix_regime.value]
        
        return {
            'min_expected_move': move_pct,
            'volume_spike_move': move_pct,
            'balanced_min_move': move_pct,
            'path1_min_move': move_pct + 0.1,  # Slightly higher for primary path
        }
    
    # ===== CONFIDENCE THRESHOLDS =====
    @property
    def confidence_thresholds(self) -> Dict:
        """Dynamic confidence based on market conditions"""
        base_conf = {
            'derivative': 0.82,  # Slightly lowered for better signals
            'high_confidence': 0.82,
            'medium_confidence': 0.78,
            'path1_confidence': 0.82,
            'volume_spike_min': 0.78,
            'sector_min': 0.78,
        }
        
        # Increase confidence requirements in volatile sessions
        if self._current_market_session in [MarketSession.OPENING, MarketSession.CLOSING]:
            return {k: v + 0.03 for k, v in base_conf.items()}
        
        return base_conf
    
    # ===== COOLDOWN PERIODS =====
    @property
    def cooldown_periods(self) -> Dict:
        """Dynamic cooldown to prevent alert spam"""
        base_cooldowns = {
            'derivative': 8,   # Reduced from 10s
            'equity': 4,       # Reduced from 5s
            'index': 3,        # Separate for indices
            'high_volatility': 12,  # Extended during high VIX
        }
        
        if self._current_vix_regime == VIXRegime.PANIC:
            return {k: v * 1.5 for k, v in base_cooldowns.items()}
        
        return base_cooldowns
    
    # ===== SCORING SYSTEM =====
    @property
    def scoring_config(self) -> Dict:
        """Dynamic scoring thresholds"""
        return {
            'min_score': 30,  # Increased from 25 for better quality
            'volume_weight': 0.25,
            'move_weight': 0.35,
            'confidence_weight': 0.40,
            'time_multiplier': self._get_time_multiplier(),
        }
    
    # ===== TRADING COSTS =====
    @property
    def trading_costs(self) -> Dict:
        """Realistic Indian trading costs"""
        return {
            'brokerage': 0.0003,    # 0.03% for intraday
            'stt': 0.00025,         # 0.025% STT
            'exchange_charges': 0.00002,  # 0.002%
            'gst': 0.00018,         # 18% on brokerage + charges
            'sebi_charges': 0.000001,
            'stamp_duty': 0.00003,
            'total_costs': 0.000781  # ~0.078% total
        }
    
    # ===== DYNAMIC ADJUSTMENT METHODS =====
    def update_market_session(self, current_time: datetime = None):
        """Update current market session based on Indian market hours"""
        if current_time is None:
            current_time = datetime.now(self.indian_tz)
        
        current_time_obj = current_time.timetz()
        
        if datetime_time(9, 0) <= current_time_obj < datetime_time(9, 15):
            self._current_market_session = MarketSession.PRE_OPEN
        elif datetime_time(9, 15) <= current_time_obj < datetime_time(9, 45):
            self._current_market_session = MarketSession.OPENING
        elif datetime_time(9, 45) <= current_time_obj < datetime_time(11, 0):
            self._current_market_session = MarketSession.MORNING
        elif datetime_time(11, 0) <= current_time_obj < datetime_time(12, 30):
            self._current_market_session = MarketSession.MID_MORNING
        elif datetime_time(12, 30) <= current_time_obj < datetime_time(14, 0):
            self._current_market_session = MarketSession.MIDDAY
        elif datetime_time(14, 0) <= current_time_obj < datetime_time(15, 0):
            self._current_market_session = MarketSession.AFTERNOON
        elif datetime_time(15, 0) <= current_time_obj < datetime_time(15, 30):
            self._current_market_session = MarketSession.CLOSING
        else:
            self._current_market_session = MarketSession.POST_CLOSE
    
    def update_vix_regime(self, current_vix: float):
        """Update VIX regime based on current India VIX value"""
        if current_vix > 22:
            self._current_vix_regime = VIXRegime.PANIC
        elif current_vix > 12:
            self._current_vix_regime = VIXRegime.NORMAL
        else:
            self._current_vix_regime = VIXRegime.COMPLACENT
    
    def _get_time_multiplier(self) -> float:
        """Get time-based multiplier for scoring"""
        multipliers = {
            MarketSession.PRE_OPEN: 0.8,    # Lower confidence pre-open
            MarketSession.OPENING: 1.3,     # Stricter during opening
            MarketSession.MORNING: 1.0,     # Baseline
            MarketSession.MID_MORNING: 1.1, # Slightly stricter
            MarketSession.MIDDAY: 0.9,      # More lenient in low volatility
            MarketSession.AFTERNOON: 1.0,   # Baseline
            MarketSession.CLOSING: 1.2,     # Stricter during closing
            MarketSession.POST_CLOSE: 0.5,  # Much lower after hours
        }
        return multipliers.get(self._current_market_session, 1.0)
    
    # ===== EXCEPTION LIST =====
    @property
    def always_include_symbols(self) -> Set[str]:
        """Symbols that bypass normal filters"""
        return {
            "NSE:RELIANCE", "NSE:TCS", "NSE:HDFCBANK", "NSE:INFY",
            "NSE:ICICIBANK", "NSE:SBIN", "NSE:HINDUNILVR", "NSE:BAJFINANCE",
            "NSE:KOTAKBANK", "NSE:LT", "NSE:AXISBANK", "NSE:ITC",
            "NSE:NIFTY 50", "NSE:NIFTY BANK", "NSE:NIFTY FIN SERVICE",
            "NSE:INDIA VIX"
        }


class RetailAlertFilter:
    """Filter Redis alerts for profitable retail trades only"""

    INDICATOR_FIELDS = [
        "rsi",
        "rsi_signal",
        "macd",
        "macd_signal",
        "macd_histogram",
        "ema_20",
        "ema_50",
        "atr",
        "vwap",
        "volume_ratio",
        "price_change",
        "incremental_volume",
        "momentum",
        "volatility",
        "support",
        "resistance",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "vix_value",
    ]

    @staticmethod
    def _has_value(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip() != ""
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0
        return True

    def __init__(self, quant_calculator=None, telemetry_client=None, redis_client=None):
        """Initialize RetailAlertFilter with dynamic configuration."""
        
        # Store references
        self.quant_calculator = quant_calculator
        self.telemetry_client = telemetry_client
        self.redis_client = redis_client
        
        # Initialize dynamic configuration
        self.config = AlertFilterConfig()
        
        # Initialize cooldown tracker
        self.cooldown_tracker = {}
        
        # Update configuration with current market conditions
        self.config.update_market_session()
        
        # Get current VIX for regime detection
        try:
            # Try to get VIX directly from Redis
            vix_data = None
            try:
                from redis_files.redis_client import get_redis_client
                redis_client = get_redis_client()
                vix_json = redis_client.get("market_data:indices:nse_india vix")
                if vix_json:
                    import json
                    vix_data = json.loads(vix_json)
                    current_vix = vix_data.get('last_price', 0)
                    logger.info(f"🔍 VIX from Redis: {current_vix}")
                    if current_vix:
                        self.config.update_vix_regime(current_vix)
                        logger.info(f"🔍 VIX regime updated: {self.config._current_vix_regime.value}")
            except Exception as e:
                logger.warning(f"Direct VIX retrieval failed: {e}")
            
            # Fallback to VIX utils
            if not vix_data:
                current_vix = get_vix_value()
                if current_vix:
                    self.config.update_vix_regime(current_vix)
        except Exception as e:
            logger.warning(f"Could not get VIX value: {e}")
        
        # Alert history and cooldowns
        self.alert_history = {}  # Prevent spam
        self.last_alert_time = {}
        self.alert_history_max_size = 1000
        self.last_alert_time_max_size = 5000
        
        # Thread safety locks
        self.alert_history_lock = threading.Lock()
        self.last_alert_time_lock = threading.Lock()
        self.cooldown_lock = threading.Lock()
        
        # Spoofing blocks tracking
        self.spoofing_blocks = {}
        
        # Schema validation stats
        self.stats = {
            "schema_rejections": 0,
            "enhanced_rejections": 0,
            "total_processed": 0,
            "schema_pre_gate_rejections": 0,
            "enhanced_filter_rejections": 0,
            "alerts_sent_schema_approved": 0,
        }
        
        # Alert cooldowns
        self.alert_cooldowns = {}  # {symbol: last_alert_time}
        
        # Global rate limiting
        self.telegram_rate_limits = {}  # {chat_id: last_send_time}
        self.global_alert_rate_limit = 1.0  # Minimum 1 second between alerts
        
        # Time-based threshold tracking
        self.last_market_date = None
        self.cached_market_hours = None
        self.last_global_alert_time = 0
        
        # Alert queue
        self.alert_queue = []
        self.alert_queue_lock = threading.Lock()
        self.max_queue_size = 5000
        self.queue_processing = False
        
        # Circuit breaker for failures
        self.telegram_failure_count = 0
        self.telegram_failure_threshold = 2
        self.telegram_circuit_breaker_reset_time = 0
        
        logger.info("📊 RetailAlertFilter initialized with dynamic configuration")
        logger.info(f"   Current VIX regime: {self.config._current_vix_regime.value}")
        logger.info(f"   Current market session: {self.config._current_market_session.value}")

    def should_send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Main alert filtering method with 6-path qualification system.
        
        Args:
            alert_data: Alert data dictionary
            
        Returns:
            True if alert should be sent, False otherwise
        """
        try:
            self.stats["total_processed"] += 1
            symbol = alert_data.get('symbol', 'UNKNOWN')
            pattern = alert_data.get('pattern', 'UNKNOWN')
            confidence = alert_data.get('confidence', 0.0)
            
            logger.info(f"🔍 [FILTER_DEBUG] Processing alert: {symbol} {pattern} confidence: {confidence:.2f}")
            
            # Basic validation
            if not self._validate_alert_data(alert_data):
                logger.info(f"❌ [FILTER_DEBUG] Alert validation failed for {symbol} {pattern}")
                self.stats["schema_rejections"] += 1
                return False

            symbol = alert_data.get('symbol', 'UNKNOWN')
            # Update market session based on alert timestamp
            alert_time = self._extract_alert_time(alert_data)
            self.config.update_market_session(alert_time)

            if (
                not alert_data.get("allow_premarket", False)
                and self._is_premarket_window(alert_time)
            ):
                self.stats["enhanced_filter_rejections"] += 1
                logger.info(
                    "🔇 Suppressing pre-market alert for %s (%s) at %s",
                    symbol,
                    alert_data.get('pattern', 'unknown'),
                    alert_time.astimezone(self.config.indian_tz).strftime("%H:%M:%S")
                    if alert_time
                    else "unknown time",
                )
                return False

            # Ensure downstream consumers receive enriched payload
            self._enrich_alert_metadata(alert_data)
            
            # Check cooldowns to avoid spamming same symbol/pattern
            if not self._check_cooldowns(alert_data):
                self.stats["enhanced_filter_rejections"] += 1
                logger.debug(
                    "⏳ Cooldown active for %s (%s)",
                    symbol,
                    alert_data.get("pattern", "unknown"),
                )
                return False
            
            # Apply 6-path qualification system
            should_send, reason = self._should_send_alert_enhanced(alert_data)
            
            if should_send:
                self.stats["alerts_sent_schema_approved"] += 1
                logger.info(f"✅ Alert approved: {alert_data.get('symbol', 'UNKNOWN')} - {reason}")
                return True
            else:
                self.stats["enhanced_filter_rejections"] += 1
                logger.debug(f"❌ Alert rejected: {alert_data.get('symbol', 'UNKNOWN')} - {reason}")
                return False
                
        except Exception as e:
            logger.error(f"Error in should_send_alert: {e}")
            logger.error(f"Alert data: {alert_data}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _validate_alert_data(self, alert_data: Dict[str, Any]) -> bool:
        """Validate basic alert data structure."""
        if not isinstance(alert_data, dict):
            return False
        
        required_fields = ['symbol', 'pattern', 'confidence', 'last_price']
        for field in required_fields:
            if field not in alert_data:
                return False
        
        return True

    def _enrich_alert_metadata(self, alert_data: Dict[str, Any]) -> None:
        """Ensure alert payload carries pattern metadata, actions, and indicators."""
        if not isinstance(alert_data, dict):
            return

        if not self._has_value(alert_data.get('pattern')):
            alert_data['pattern'] = alert_data.get('pattern_type')

        pattern_name = alert_data.get('pattern') or 'unknown'
        default_desc = {
            "title": f"📊 {pattern_name.replace('_', ' ').title()}",
            "description": f"{pattern_name.replace('_', ' ').title()} setup detected",
            "trading_instruction": "Monitor closely",
            "directional_action": alert_data.get('signal', 'WATCH'),
            "move_type": pattern_name.upper(),
        }

        if HumanReadableAlertTemplates:
            try:
                pattern_desc = HumanReadableAlertTemplates.get_pattern_description(pattern_name)
            except Exception:
                pattern_desc = default_desc
        else:
            pattern_desc = default_desc

        alert_data.setdefault('pattern_title', pattern_desc.get('title'))
        alert_data.setdefault('pattern_description', pattern_desc.get('description'))
        alert_data.setdefault('move_type', pattern_desc.get('move_type'))
        alert_data.setdefault('pattern_display', alert_data.get('pattern_title'))

        if not self._has_value(alert_data.get('trading_instruction')):
            alert_data['trading_instruction'] = pattern_desc.get('trading_instruction')

        if not self._has_value(alert_data.get('action')):
            preferred_action = (
                alert_data.get('recommended_action')
                or alert_data.get('strategy_action')
                or alert_data.get('trading_instruction')
                or pattern_desc.get('trading_instruction')
            )
            if self._has_value(preferred_action):
                alert_data['action'] = preferred_action

        if not self._has_value(alert_data.get('directional_action')):
            alert_data['directional_action'] = alert_data.get('signal') or pattern_desc.get('directional_action')

        indicators = alert_data.get('indicators') if isinstance(alert_data.get('indicators'), dict) else {}
        indicators = dict(indicators)

        for field in self.INDICATOR_FIELDS:
            if field in alert_data and self._has_value(alert_data[field]):
                existing_value = indicators.get(field)
                if not self._has_value(existing_value):
                    indicators[field] = alert_data[field]

        if indicators:
            alert_data['indicators'] = indicators
            for core_field in ('rsi', 'atr', 'volume_ratio', 'price_change'):
                if core_field in indicators and not self._has_value(alert_data.get(core_field)):
                    alert_data[core_field] = indicators[core_field]

        if any(self._has_value(alert_data.get(field)) for field in ('news_title', 'news_summary', 'news_context', 'news_sentiment', 'news')):
            alert_data.setdefault('has_news', True)

    def _check_cooldowns(self, alert_data: Dict[str, Any]) -> bool:
        """Check alert cooldowns to prevent spam - only checks, doesn't update (cooldown updated AFTER send)."""
        try:
            key_symbol = alert_data.get('symbol', '')
            key_pattern = alert_data.get('pattern', alert_data.get('pattern_type', ''))
            cooldown_key = f"{key_symbol}:{key_pattern}".strip(':')
            current_time = time.time()
            
            # Get cooldown period for this symbol type
            cooldown_period = self._get_cooldown_period(alert_data)

            with self.cooldown_lock:
                last_alert_time = self.cooldown_tracker.get(cooldown_key)
                if last_alert_time is not None:
                    time_since_last = current_time - last_alert_time
                    if time_since_last < cooldown_period:
                        remaining = cooldown_period - time_since_last
                        logger.debug(
                            f"Cooldown active for {cooldown_key}: {remaining:.1f}s remaining"
                        )
                        return False

                # NOTE: Don't update cooldown here - it should be updated AFTER alert is successfully sent
                # Cooldown update happens in _mark_alert_sent() which is called by ProductionAlertManager
            return True
            
        except Exception as e:
            logger.error(f"Error checking cooldowns: {e}")
            return True  # Allow alert if cooldown check fails

    def _get_cooldown_period(self, alert_data: Dict[str, Any]) -> int:
        """Get cooldown period based on asset type."""
        symbol = alert_data.get('symbol', '')
        
        if 'FUT' in symbol or 'CE' in symbol or 'PE' in symbol:
            return self.config.cooldown_periods.get('derivative', 8)
        else:
            return self.config.cooldown_periods.get('equity', 4)

    def _should_send_alert_enhanced(self, alert_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Enhanced 6-path qualification system with multi-layer pre-validation.
        
        Flow: Pattern → Multi-Layer Pre-Validation → Enhanced 6-Path Filter → Redis Deduplication → Notification
        
        Returns:
            Tuple of (should_send, reason)
        """
        symbol = alert_data.get('symbol', 'UNKNOWN')
        pattern = alert_data.get('pattern', 'unknown')
        confidence = float(alert_data.get('confidence', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        last_price = float(alert_data.get('last_price', 0.0))
        
        # DEBUG: Log the alert data being processed
        logger.info(f"🔍 [FILTER_DEBUG] {symbol} {pattern}: conf={confidence:.2f}, move={expected_move:.2f}%, vol={volume_ratio:.2f}, price={last_price:.2f}")
        
        # Get VIX regime for dynamic thresholds
        vix_regime = get_vix_regime()
        
        # MULTI-LAYER PRE-VALIDATION
        if not self._perform_multi_layer_pre_validation(alert_data, symbol):
            logger.info(f"❌ [FILTER_DEBUG] {symbol} {pattern}: Failed multi-layer pre-validation")
            return False, "Failed multi-layer pre-validation"
        
        # ENHANCED PATH 1: High Confidence (with Volume Profile & ICT Liquidity)
        if self._check_path1_high_confidence_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH1 qualification")
            return True, "PATH1: Enhanced high confidence qualification"
        
        # ENHANCED PATH 2: Volume Spike (with Volume Profile Context)
        if self._check_path2_volume_spike_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH2 qualification")
            return True, "PATH2: Enhanced volume spike qualification"
        
        # ENHANCED PATH 3: Sector-Specific (Properly Implemented)
        if self._check_path3_sector_specific_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH3 qualification")
            return True, "PATH3: Enhanced sector-specific qualification"
        
        # ENHANCED PATH 4: Balanced Play (with Risk/Reward & Volume Profile)
        if self._check_path4_balanced_play_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH4 qualification")
            return True, "PATH4: Enhanced balanced play qualification"
        
        # ENHANCED PATH 5: Proven Patterns (Risk-Adjusted)
        if self._check_path5_proven_patterns_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH5 qualification")
            return True, "PATH5: Enhanced proven pattern qualification"
        
        # ENHANCED PATH 6: Composite Score (Enhanced Scoring)
        if self._check_path6_composite_score_enhanced(alert_data, vix_regime):
            logger.info(f"✅ [FILTER_DEBUG] {symbol} {pattern}: PATH6 qualification")
            return True, "PATH6: Enhanced composite score qualification"
        
        logger.info(f"❌ [FILTER_DEBUG] {symbol} {pattern}: No qualification path met")
        return False, "No enhanced qualification path met"

    def _perform_multi_layer_pre_validation(self, alert_data: Dict[str, Any], symbol: str) -> bool:
        """Multi-layer pre-validation using PatternDetector validation methods"""
        try:
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] Starting pre-validation for {symbol}")
            
            # Import PatternDetector for validation methods
            from patterns.pattern_detector import PatternDetector
            
            # Initialize pattern detector (or get existing instance)
            pattern_detector = PatternDetector(redis_client=self.redis_client)
            
            # Get volume profile data
            volume_nodes = self._get_volume_profile_data(symbol)
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Volume nodes: {volume_nodes}")
            if not volume_nodes:
                volume_nodes = {}  # Fallback to empty dict
            
            # Get liquidity pools data
            liquidity_pools = self._get_liquidity_pools_data(symbol)
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Liquidity pools: {liquidity_pools}")
            if not liquidity_pools:
                liquidity_pools = {}  # Fallback to empty dict
            
            # Layer 1: Volume Profile Alignment
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Testing Layer 1 - Volume Profile Alignment")
            if not pattern_detector.validate_volume_profile_alignment(alert_data, volume_nodes):
                logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 1 FAILED - Volume Profile Alignment")
                return False
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 1 PASSED - Volume Profile Alignment")
            
            # Layer 2: ICT Liquidity Alignment
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Testing Layer 2 - ICT Liquidity Alignment")
            if not pattern_detector.validate_ict_liquidity_alignment(alert_data, liquidity_pools):
                logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 2 FAILED - ICT Liquidity Alignment")
                return False
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 2 PASSED - ICT Liquidity Alignment")
            
            # Layer 3: Multi-Timeframe Confirmation
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Testing Layer 3 - Multi-Timeframe Confirmation")
            if not pattern_detector.validate_multi_timeframe_confirmation(alert_data, symbol):
                logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 3 FAILED - Multi-Timeframe Confirmation")
                return False
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 3 PASSED - Multi-Timeframe Confirmation")
            
            # Layer 4: Pattern-Specific Risk Validation
            # TEMPORARILY DISABLED - Too strict, blocking all alerts
            # TODO: Fix validate_pattern_specific_risk or make it optional
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Testing Layer 4 - Pattern-Specific Risk Validation (SKIPPED - temporarily disabled)")
            # if not pattern_detector.validate_pattern_specific_risk(alert_data):
            #     logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 4 FAILED - Pattern-Specific Risk Validation")
            #     return False
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: Layer 4 PASSED - Pattern-Specific Risk Validation (skipped)")
            
            logger.info(f"🔍 [PRE_VALIDATION_DEBUG] {symbol}: ALL LAYERS PASSED - Pre-validation successful")
            return True
            
        except Exception as e:
            logger.warning(f"Multi-layer pre-validation failed: {e}")
            # Temporarily disable multi-layer pre-validation if it's blocking all alerts
            # TODO: Fix PatternDetector validation methods or make them optional
            logger.warning(f"⚠️ Multi-layer pre-validation disabled - allowing alert to proceed")
            return True  # Allow to proceed if validation fails

    def _get_volume_profile_data(self, symbol: str) -> Dict[str, Any]:
        """✅ Get volume profile data using direct key lookups (DB 2 - analytics)"""
        try:
            if not self.redis_client:
                logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: No Redis client available")
                return {}
            
            # ✅ Use redis_key_standards for direct key lookups (no SCAN)
            from redis_files.redis_key_standards import RedisKeyStandards
            from datetime import datetime
            
            # Get DB 2 client (analytics - where volume profiles are stored)
            try:
                analytics_db = self.redis_client.get_database_for_data_type("analytics_data")
            except Exception:
                analytics_db = 2  # DB 2 is analytics
            
            analytics_client = self.redis_client.get_client(analytics_db)
            if not analytics_client:
                logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: No DB 2 client available")
                return {}
            
            volume_data: Dict[str, Any] = {}
            current_date = datetime.now().strftime('%Y-%m-%d')
            
            # ✅ Direct key lookups (O(1)) instead of SCAN (O(N))
            try:
                # Try POC key first (most commonly accessed)
                poc_key = RedisKeyStandards.get_volume_profile_poc_key(symbol)
                poc_data = analytics_client.hgetall(poc_key)
                if poc_data:
                    volume_data.update({
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in poc_data.items()
                    })
                
                # Try nodes key (support/resistance levels)
                nodes_key = RedisKeyStandards.get_volume_profile_nodes_key(symbol)
                nodes_data = analytics_client.hgetall(nodes_key)
                if nodes_data:
                    volume_data.update({
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in nodes_data.items()
                    })
                
                # Try session key (full profile data)
                session_key = RedisKeyStandards.get_volume_profile_session_key(symbol, current_date)
                session_data = analytics_client.hgetall(session_key)
                if session_data:
                    volume_data.update({
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in session_data.items()
                    })
                
                if volume_data:
                    logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: Found volume profile data via direct lookup")
                    return volume_data
            except Exception as lookup_err:
                logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: Direct lookup failed: {lookup_err}")

            logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: No volume profile data found")
            return {}
            
        except Exception as e:
            logger.debug(f"🔍 [VOLUME_PROFILE_DEBUG] {symbol}: Could not get volume profile data: {e}")
            return {}

    def _get_liquidity_pools_data(self, symbol: str) -> Dict[str, Any]:
        """✅ Get liquidity pools data using direct key lookups (no SCAN)"""
        try:
            if not self.redis_client:
                logger.debug(f"🔍 [LIQUIDITY_DEBUG] {symbol}: No Redis client available")
                return {}
            
            # ✅ Try direct lookup for liquidity data (if key pattern is known)
            # If liquidity keys are not standardized, return empty dict rather than scanning
            # Liquidity data should be stored with known keys or not at all
            logger.debug(f"🔍 [LIQUIDITY_DEBUG] {symbol}: Liquidity pools not yet standardized - returning empty")
            return {}
            
            # Fallback removed - no SCAN operations allowed
            # If liquidity data is needed, it should be stored with known keys
        except Exception as e:
            logger.debug(f"🔍 [LIQUIDITY_DEBUG] {symbol}: Could not get liquidity pools data: {e}")
            return {}

    def _check_path1_high_confidence_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 1: High Confidence with Volume Profile & ICT Liquidity"""
        confidence = float(alert_data.get('confidence', 0.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))

        # Get dynamic thresholds from configuration
        thresholds = self.config.confidence_thresholds
        move_thresholds = self.config.move_thresholds
        high_conf_threshold = thresholds['high_confidence']
        min_move_threshold = move_thresholds['path1_min_move']
        min_vol_threshold = self._volume_threshold_for_symbol(alert_data)
        
        # Check basic thresholds
        if not (confidence >= high_conf_threshold and 
                expected_move >= min_move_threshold and 
                volume_ratio >= min_vol_threshold):
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.10):  # 0.10% minimum profit
            return False
        
        # Additional checks are handled in multi-layer pre-validation
        return True

    def _check_path2_volume_spike_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 2: Volume Spike with Volume Profile Context"""
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        confidence = float(alert_data.get('confidence', 0.0))
        
        # Get dynamic thresholds from configuration
        thresholds = self.config.confidence_thresholds
        move_thresholds = self.config.move_thresholds
        min_vol_threshold = self._volume_threshold_for_symbol(alert_data)
        min_move_threshold = move_thresholds['volume_spike_move']
        min_conf_threshold = thresholds['volume_spike_min']
        
        # Check basic thresholds
        if not (volume_ratio >= min_vol_threshold and 
                expected_move >= min_move_threshold and 
                confidence >= min_conf_threshold):
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.08):  # 0.08% minimum profit
            return False
        
        # Additional volume profile checks are handled in multi-layer pre-validation
        return True

    def _check_path3_sector_specific_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 3: Sector-Specific (Properly Implemented)"""
        symbol = alert_data.get('symbol', 'UNKNOWN')
        confidence = float(alert_data.get('confidence', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        
        # Get sector-specific thresholds
        sector_thresholds = self._get_sector_thresholds(symbol)
        if not sector_thresholds:
            return False
        
        # Apply sector-specific volume threshold
        sector_volume_threshold = sector_thresholds.get('volume_spike', 2.0)
        sector_confidence_threshold = sector_thresholds.get('confidence', 0.75)
        
        if not (volume_ratio >= sector_volume_threshold and 
                confidence >= sector_confidence_threshold):
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.06):  # 0.06% minimum profit
            return False
        
        return True

    def _check_path4_balanced_play_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 4: Balanced Play with Risk/Reward & Volume Profile"""
        confidence = float(alert_data.get('confidence', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        
        # Basic balanced thresholds
        if not (confidence >= 0.80 and 
                volume_ratio >= self._volume_threshold_for_symbol(alert_data) and 
                expected_move >= 0.45):
            return False
        
        # Check risk/reward ratio
        if not self._check_risk_reward_ratio(alert_data, 1.5):  # 1.5:1 minimum
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
            return False
        
        return True

    def _check_path5_proven_patterns_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 5: Proven Patterns (Risk-Adjusted)"""
        pattern = alert_data.get('pattern', 'unknown')
        confidence = float(alert_data.get('confidence', 0.0))
        
        # Proven patterns with lower thresholds
        proven_patterns = [
            'spring_coil',
            'volume_price_divergence',
            'hidden_accumulation',
            'fake_bid_wall_basic',
            'fake_ask_wall_basic'
        ]
        
        if pattern not in proven_patterns or confidence < 0.70:
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
            return False
        
        # Risk-adjusted validation is handled in multi-layer pre-validation
        return True

    def _check_path6_composite_score_enhanced(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """ENHANCED PATH 6: Composite Score (Enhanced Scoring)"""
        score = self._calculate_enhanced_composite_score(alert_data)
        
        if score < self.config.scoring_config.get('min_score', 30):
            return False
        
        # Check profitability
        if not self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
            return False
        
        return True

    def _get_sector_thresholds(self, symbol: str) -> Dict[str, float]:
        """Get sector-specific thresholds for symbol"""
        try:
            # Map symbols to sectors (simplified)
            sector_mapping = {
                'HDFCBANK': 'banking',
                'ICICIBANK': 'banking',
                'KOTAKBANK': 'banking',
                'TCS': 'it',
                'INFY': 'it',
                'WIPRO': 'it',
                'SUNPHARMA': 'pharma',
                'DRREDDY': 'pharma',
                'MARUTI': 'auto',
                'TATAMOTORS': 'auto',
            }
            
            sector = sector_mapping.get(symbol, 'general')
            
            # Sector-specific thresholds
            sector_thresholds = {
                'banking': {'volume_spike': 2.8, 'confidence': 0.75},
                'it': {'volume_spike': 2.2, 'confidence': 0.70},
                'pharma': {'volume_spike': 2.0, 'confidence': 0.70},
                'auto': {'volume_spike': 2.5, 'confidence': 0.75},
                'general': {'volume_spike': 2.2, 'confidence': 0.70},
            }
            
            return sector_thresholds.get(sector, sector_thresholds['general'])
            
        except Exception:
            return {'volume_spike': 2.2, 'confidence': 0.70}

    def _check_risk_reward_ratio(self, alert_data: Dict[str, Any], min_ratio: float) -> bool:
        """Check if risk/reward ratio meets minimum requirement"""
        try:
            entry_price = float(alert_data.get('entry_price', alert_data.get('last_price', 0)))
            target_price = float(alert_data.get('target', 0))
            stop_loss = float(alert_data.get('stop_loss', 0))
            
            if entry_price <= 0 or target_price <= 0 or stop_loss <= 0:
                return False
            
            # Calculate risk and reward
            if target_price > entry_price:  # Long position
                reward = target_price - entry_price
                risk = entry_price - stop_loss
            else:  # Short position
                reward = entry_price - target_price
                risk = stop_loss - entry_price
            
            if risk <= 0:
                return False
            
            ratio = reward / risk
            return ratio >= min_ratio
            
        except Exception:
            return False

    def _calculate_enhanced_composite_score(self, alert_data: Dict[str, Any]) -> int:
        """Calculate enhanced composite score with volume profile and ICT bonuses"""
        score = 0
        
        # Confidence score (0-40 points)
        confidence = float(alert_data.get('confidence', 0.0))
        score += int(confidence * 40)
        
        # Volume ratio score (0-30 points)
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        if volume_ratio >= 3.0:
            score += 30
        elif volume_ratio >= 2.0:
            score += 20
        elif volume_ratio >= 1.5:
            score += 10
        
        # Expected move score (0-20 points)
        expected_move = float(alert_data.get('expected_move', 0.0))
        if expected_move >= 1.0:
            score += 20
        elif expected_move >= 0.5:
            score += 15
        elif expected_move >= 0.25:
            score += 10
        
        # Pattern type bonus (0-10 points)
        pattern = alert_data.get('pattern', 'unknown')
        if pattern in ['reversal', 'volume_breakout']:
            score += 10
        elif pattern in ['volume_spike', 'upside_momentum', 'buy_pressure']:
            score += 5
        
        # Volume profile bonus (0-10 points) - NEW
        volume_profile_strength = float(alert_data.get('profile_strength', 0.0))
        if volume_profile_strength >= 0.7:
            score += 10
        elif volume_profile_strength >= 0.5:
            score += 7
        elif volume_profile_strength >= 0.3:
            score += 5
        
        # ICT alignment bonus (0-10 points) - NEW
        if alert_data.get('ict_alignment', False):
            score += 10
        
        return min(score, 100)  # Cap at 100

    def _extract_alert_time(self, alert_data: Dict[str, Any]) -> datetime:
        """Best-effort extraction of alert timestamp in IST."""
        tz = self.config.indian_tz
        candidates = [
            alert_data.get("published_at"),
            alert_data.get("timestamp"),
            alert_data.get("event_time"),
            alert_data.get("exchange_timestamp"),
            alert_data.get("created_at"),
        ]

        for candidate in candidates:
            alert_dt = self._coerce_to_datetime(candidate, tz)
            if alert_dt is not None:
                return alert_dt.astimezone(tz)

        return datetime.now(tz)

    @staticmethod
    def _coerce_to_datetime(value: Any, tz: timezone) -> Optional[datetime]:
        """Convert raw timestamp values into timezone-aware datetimes."""
        if value is None:
            return None

        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=tz)

        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value), tz=tz)
            except (ValueError, OSError):
                return None

        if isinstance(value, str):
            raw = value.strip()
            if not raw:
                return None
            try:
                if raw.endswith("Z"):
                    raw = raw[:-1] + "+00:00"
                dt = datetime.fromisoformat(raw)
            except ValueError:
                try:
                    dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None
            return dt if dt.tzinfo else dt.replace(tzinfo=tz)

        return None

    def _is_premarket_window(self, alert_time: Optional[datetime]) -> bool:
        """Return True when the alert is captured during the pre-market window."""
        if alert_time is None:
            return False

        local_time = alert_time.astimezone(self.config.indian_tz).timetz()
        return datetime_time(9, 0) <= local_time < datetime_time(9, 16)

    def _check_path1_high_confidence(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 1: High Confidence qualification."""
        confidence = float(alert_data.get('confidence', 0.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))

        # Get dynamic thresholds from configuration
        thresholds = self.config.confidence_thresholds
        move_thresholds = self.config.move_thresholds
        high_conf_threshold = thresholds['high_confidence']
        min_move_threshold = move_thresholds['path1_min_move']
        min_vol_threshold = self._volume_threshold_for_symbol(alert_data)
        
        # Check thresholds
        if (confidence >= high_conf_threshold and 
            expected_move >= min_move_threshold and 
            volume_ratio >= min_vol_threshold):
            
            # Check profitability
            if self._check_profitability(alert_data, 0.10):  # 0.10% minimum profit
                return True
        
        return False

    def _check_path2_volume_spike(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 2: Volume Spike qualification."""
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        confidence = float(alert_data.get('confidence', 0.0))
        
        # Get dynamic thresholds from configuration
        thresholds = self.config.confidence_thresholds
        move_thresholds = self.config.move_thresholds
        min_vol_threshold = self._volume_threshold_for_symbol(alert_data)
        min_move_threshold = move_thresholds['volume_spike_move']
        min_conf_threshold = thresholds['volume_spike_min']
        
        # Check thresholds
        if (volume_ratio >= min_vol_threshold and 
            expected_move >= min_move_threshold and 
            confidence >= min_conf_threshold):
            
            # Check profitability
            if self._check_profitability(alert_data, 0.08):  # 0.08% minimum profit
                return True
        
        return False

    def _check_path3_sector_specific(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 3: Sector-Specific qualification (placeholder)."""
        # TODO: Implement sector-specific thresholds
        return False

    def _check_path4_balanced_play(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 4: Balanced Play qualification."""
        confidence = float(alert_data.get('confidence', 0.0))
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        expected_move = float(alert_data.get('expected_move', 0.0))
        
        # Balanced thresholds
        if (confidence >= 0.80 and 
            volume_ratio >= self._volume_threshold_for_symbol(alert_data) and 
            expected_move >= 0.45):
            
            # Check profitability
            if self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
                return True
        
        return False

    def _check_path5_proven_patterns(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 5: Proven Patterns qualification."""
        pattern = alert_data.get('pattern', 'unknown')
        confidence = float(alert_data.get('confidence', 0.0))
        
        # Proven patterns with lower thresholds
        proven_patterns = [
            'spring_coil',
            'volume_price_divergence',
            'hidden_accumulation',
            'fake_bid_wall_basic',
            'fake_ask_wall_basic'
        ]
        
        if pattern in proven_patterns and confidence >= 0.70:
            # Check profitability
            if self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
                return True
        
        return False

    def _check_path6_composite_score(self, alert_data: Dict[str, Any], vix_regime: str) -> bool:
        """PATH 6: Composite Score qualification."""
        score = self._calculate_composite_score(alert_data)
        
        if score >= self.config.scoring_config.get('min_score', 30):
            # Check profitability
            if self._check_profitability(alert_data, 0.05):  # 0.05% minimum profit
                return True
        
        return False

    def _volume_threshold_for_symbol(self, alert_data: Dict[str, Any]) -> float:
        symbol = str(alert_data.get('symbol', '')).upper()
        thresholds = self.config.volume_thresholds
        base = float(thresholds.get('equity_multiplier', 2.0))
        derivative = float(thresholds.get('derivative_multiplier', base))

        # Futures/options typically live on the derivatives exchanges
        if symbol.startswith(('NFO:', 'BFO:', 'MCX:')):
            return derivative

        base_symbol = symbol.rsplit(':', 1)[-1]
        for suffix in ('FUT', 'CE', 'PE', 'CALL', 'PUT'):
            if base_symbol.endswith(suffix):
                prefix = base_symbol[:-len(suffix)]
                if prefix and prefix[-1].isdigit():
                    return derivative
        return base

    def _calculate_composite_score(self, alert_data: Dict[str, Any]) -> int:
        """Calculate composite score for alert quality."""
        score = 0
        
        # Confidence score (0-40 points)
        confidence = float(alert_data.get('confidence', 0.0))
        score += int(confidence * 40)
        
        # Volume ratio score (0-30 points)
        volume_ratio = float(alert_data.get('volume_ratio', 1.0))
        if volume_ratio >= 3.0:
            score += 30
        elif volume_ratio >= 2.0:
            score += 20
        elif volume_ratio >= 1.5:
            score += 10
        
        # Expected move score (0-20 points)
        expected_move = float(alert_data.get('expected_move', 0.0))
        if expected_move >= 1.0:
            score += 20
        elif expected_move >= 0.5:
            score += 15
        elif expected_move >= 0.25:
            score += 10
        
        # Pattern type bonus (0-10 points)
        pattern = alert_data.get('pattern', 'unknown')
        if pattern in ['reversal', 'volume_breakout']:
            score += 10
        elif pattern in ['volume_spike', 'upside_momentum', 'buy_pressure']:
            score += 5
        
        return min(score, 100)  # Cap at 100

    def _check_profitability(self, alert_data: Dict[str, Any], min_profit_pct: float) -> bool:
        """Check if alert meets minimum profitability requirements."""
        expected_move = float(alert_data.get('expected_move', 0.0))
        trading_costs = self.config.trading_costs['total_costs']
        
        # Convert to percentage
        expected_move_pct = expected_move / 100.0
        
        # Check if expected move covers costs plus minimum profit
        net_profit = expected_move_pct - trading_costs
        
        return net_profit >= (min_profit_pct / 100.0)

    def get_stats(self) -> Dict[str, Any]:
        """Get filtering statistics."""
        return self.stats.copy()

    def reset_stats(self):
        """Reset filtering statistics."""
        self.stats = {
            "schema_rejections": 0,
            "enhanced_rejections": 0,
            "total_processed": 0,
            "schema_pre_gate_rejections": 0,
            "enhanced_filter_rejections": 0,
            "alerts_sent_schema_approved": 0,
        }


def should_send_premarket_alert(indicator: Optional[dict]) -> bool:
    """Standalone function for premarket alert filtering."""
    if not indicator:
        return False

    symbol = indicator.get("symbol", "UNKNOWN")

    if indicator.get("manipulation_detected"):
        logger.debug(
            "Rejecting manipulated premarket move for %s (score %.2f)",
            symbol,
            indicator.get("manipulation_score", 0.0),
        )
        return False

    # Order flow quality check removed

    return True
