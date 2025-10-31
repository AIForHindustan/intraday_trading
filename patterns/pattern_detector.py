#!/usr/bin/env python3
"""
SIMPLIFIED PATTERN DETECTOR - 8 Core Patterns Only
==================================================

Focused pattern detection using only the 8 most reliable patterns.
Replaces complex pattern detection with proven, high-signal patterns.

Core Patterns (8 total):
- Volume-based (3): volume_spike, volume_breakout, volume_price_divergence
- Momentum-based (2): upside_momentum, downside_momentum  
- Breakout/Reversal (2): breakout, reversal
- Microstructure (1): hidden_accumulation

Based on Zerodha WebSocket data:
- zerodha_cumulative_volume: Cumulative bucket_incremental_volume from session start
- last_traded_quantity: Per-trade quantity (incremental)
- total_buy_quantity: Total buy bucket_incremental_volume
- total_sell_quantity: Total sell bucket_incremental_volume
"""

import logging
import math
import sys
import os
import json
import random
from typing import Dict, Any, List, Optional, Set, Tuple
from collections import deque
from collections import defaultdict

# Import new bucket_incremental_volume architecture
from utils.correct_volume_calculator import CorrectVolumeCalculator, VolumeResolver
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from patterns.volume_thresholds import VolumeThresholdCalculator

# Import mathematical engine components
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline as MathematicalBaseline
# Removed duplicate import - using core.data.volume_thresholds as single source of truth
from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_stats_hash
from datetime import datetime, time as dt_time
from pathlib import Path
import time
from functools import wraps

# Import base detector
from .base_detector import BasePatternDetector
from .pattern_mathematics import PatternMathematics
from intraday_scanner.math_dispatcher import MathDispatcher

try:
    from .ict import (
        ICTPatternDetector,
        ICTLiquidityDetector,
        ICTFVGDetector,
        ICTOTECalculator,
        ICTPremiumDiscountDetector,
        ICTKillzoneDetector,
    )
    ICT_MODULES_AVAILABLE = True
except ImportError:
    ICTPatternDetector = None
    ICTLiquidityDetector = None
    ICTFVGDetector = None
    ICTOTECalculator = None
    ICTPremiumDiscountDetector = None
    ICTKillzoneDetector = None
    ICT_MODULES_AVAILABLE = False

# Import Risk Manager for mathematical integrity
try:
    from alerts.risk_manager import RiskManager
    RISK_MANAGER_AVAILABLE = True
except ImportError:
    RiskManager = None
    RISK_MANAGER_AVAILABLE = False

# Simplified imports for 8 core patterns only
# VIX utilities for dynamic thresholds
try:
    from utils.vix_utils import get_vix_value, get_vix_regime
    from config.thresholds import get_volume_threshold, get_confidence_threshold, get_sector_threshold
    THRESHOLD_CONFIG_AVAILABLE = True
except ImportError:
    def get_vix_value():
        return None
    def get_vix_regime():
        return "UNKNOWN"
    THRESHOLD_CONFIG_AVAILABLE = False


logger = logging.getLogger(__name__)

# Performance monitoring decorator
def time_pattern_method(method):
    """Decorator to time pattern detection methods and log slow ones"""
    @wraps(method)
    def timed_method(self, tick_data, *args, **kwargs):
        start_time = time.time()
        result = method(self, tick_data, *args, **kwargs)
        duration = time.time() - start_time
        
        # Log slow patterns (>100ms)
        if duration > 0.1:
            symbol = tick_data.get('symbol', 'UNKNOWN') if isinstance(tick_data, dict) else 'UNKNOWN'
            logger.warning(f"SLOW PATTERN: {method.__name__} took {duration:.3f}s for {symbol}")
        
        return result
    return timed_method

# Standardized field mapping for 8 core patterns
# Using canonical field names from optimized_field_mapping.yaml
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
    resolve_calculated_field as resolve_indicator_field,
)

# Initialize field mapping manager
FIELD_MAPPING_MANAGER = get_field_mapping_manager()

# Standardized helper functions for 8 core patterns
def _session_value(source: Dict[str, Any], field: str, default: float = 0.0) -> float:
    """Get session value using field mapping resolution."""
    resolved_field = resolve_session_field(field)
    return float(source.get(resolved_field, default))

def _indicator_value(source: Dict[str, Any], field: str, default: float = 0.0) -> float:
    """Get indicator value using field mapping resolution."""
    resolved_field = resolve_indicator_field(field)
    return float(source.get(resolved_field, default))


class PatternDetector(BasePatternDetector):
    """
    Simplified pattern detector using only 8 core patterns.
    Replaces complex pattern detection with focused, reliable patterns.
    Inherits from BasePatternDetector for common functionality.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, redis_client=None):
        """
        Initialize simplified pattern detector with 8 core patterns

        Args:
            config: Configuration dictionary
            redis_client: Redis client for market data
        """
        # Initialize base thresholds BEFORE parent class calls _update_vix_aware_thresholds()
        self.base_thresholds = {
            "volume_spike": 2.2,  # 2.2x average bucket_incremental_volume
            "momentum": 0.5,  # 0.5% last_price change
            "breakout": 0.3,  # 0.3% last_price change
            "reversal": 0.4,  # 0.4% last_price change
            "divergence_window": 5  # 5-tick window
        }
        
        # Initialize parent class first
        super().__init__(config=config, redis_client=redis_client)
        
        # Initialize field mapping manager for standardized field names
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field
        
        # Killzone configuration
        killzone_cfg = (self.config or {}).get("killzone", {}) if hasattr(self, "config") else {}
        self.killzone_fallback_enabled: bool = bool(killzone_cfg.get("enable_fallback", False))
        fallback_patterns = killzone_cfg.get("fallback_patterns") or []
        self.killzone_fallback_patterns: Set[str] = {p for p in fallback_patterns if isinstance(p, str)}
        windows_cfg = killzone_cfg.get("windows") or [
            ("09:15", "10:30"),
            ("13:30", "15:00"),
        ]
        self.killzone_windows: List[Tuple[dt_time, dt_time]] = self._parse_killzone_windows(windows_cfg)
        self.killzone_respect_patterns: Set[str] = set(self.killzone_fallback_patterns)
        if killzone_cfg.get("kow_respect_killzone", True):
            self.killzone_respect_patterns.add("kow_signal_straddle")
        
        # Core pattern definitions
        self.CORE_PATTERNS = {
            # Volume-based (3 patterns)
            "volume_spike": {
                "description": "Sudden bucket_incremental_volume increase > 2x average",
                "complexity": "low",
                "reliability": "high",
                "category": "bucket_incremental_volume"
            },
            "volume_breakout": {
                "description": "Price breakout with bucket_incremental_volume confirmation", 
                "complexity": "medium",
                "reliability": "high",
                "category": "bucket_incremental_volume"
            },
            "volume_price_divergence": {
                "description": "Price and bucket_incremental_volume moving opposite directions",
                "complexity": "medium", 
                "reliability": "medium",
                "category": "bucket_incremental_volume"
            },
            
            # Momentum-based (2 patterns)
            "upside_momentum": {
                "description": "Strong upward momentum with bucket_incremental_volume",
                "complexity": "low",
                "reliability": "high",
                "category": "momentum"
            },
            "downside_momentum": {
                "description": "Strong downward momentum with bucket_incremental_volume", 
                "complexity": "low",
                "reliability": "high",
                "category": "momentum"
            },
            
            # Breakout/Reversal (2 patterns)
            "breakout": {
                "description": "Price breaks resistance with bucket_incremental_volume",
                "complexity": "low",
                "reliability": "high",
                "category": "breakout"
            },
            "reversal": {
                "description": "Price reversal with bucket_incremental_volume confirmation",
                "complexity": "medium",
                "reliability": "medium",
                "category": "breakout"
            },
            
            # Microstructure (1 pattern)
            "hidden_accumulation": {
                "description": "Stealth institutional buying",
                "complexity": "high",
                "reliability": "medium",
                "category": "microstructure"
            }
        }
        
        # Base pattern detection thresholds - SINGLE SOURCE OF TRUTH from config/thresholds.py
        self.base_thresholds = {
            "volume_spike": 2.2,  # From config/thresholds.py BASE_THRESHOLDS
            "momentum": 0.5,  # 0.5% last_price change
            "breakout": 0.3,  # 0.3% breakout
            "reversal": 0.4,  # 0.4% reversal
            "divergence_window": 5  # 5-tick window for divergence
        }
        
        # VIX-aware dynamic thresholds
        self.vix_regime = "UNKNOWN"
        self.vix_value = None
        
        # Initialize thresholds with base values first
        self.VOLUME_SPIKE_THRESHOLD = self.base_thresholds["volume_spike"]
        self.MOMENTUM_THRESHOLD = self.base_thresholds["momentum"]
        self.BREAKOUT_THRESHOLD = self.base_thresholds["breakout"]
        self.REVERSAL_THRESHOLD = self.base_thresholds["reversal"]
        self.DIVERGENCE_WINDOW = self.base_thresholds["divergence_window"]
        
        # Then update with VIX-aware values
        self._update_vix_aware_thresholds()
        
        # Volume analysis
        self.volume_history = defaultdict(lambda: deque(maxlen=20))  # 20-tick rolling average
        self.price_history = defaultdict(lambda: deque(maxlen=20))
        
        # Pattern tracking
        self.pattern_counts = defaultdict(int)
        self.pattern_success_rates = defaultdict(float)
        
        # Statistics
        self.stats = {
            "total_detections": 0,
            "successful_detections": 0,
            "failed_detections": 0,
            "patterns_found": 0,
            "invocations": 0,
            "volume_spikes": 0,
            "momentum_signals": 0,
            "breakout_signals": 0,
            "reversal_signals": 0,
            "hidden_accumulation": 0,
            "pattern_counts": defaultdict(int)
        }
        
        # Initialize volume profile and time-aware baselines
        self.time_aware_baseline = TimeAwareVolumeBaseline(self.redis_client)
        self.correct_volume_calculator = CorrectVolumeCalculator(self.redis_client)
        
        
        # Mathematical volume engine
        try:
            # Initialize the mathematical foundation using single source of truth
            mathematical_baseline = MathematicalBaseline(self.redis_client)
            self.mathematical_calculator = VolumeThresholdCalculator(mathematical_baseline)
            self.logger.info("✅ Mathematical volume engine initialized")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Mathematical Calculator: {e}")
            self.mathematical_calculator = None
        
        # Volume State Manager for Volume Profile Integration
        try:
            from redis_files.volume_state_manager import get_volume_manager
            self.volume_state_manager = get_volume_manager()
            self.logger.info("✅ Volume State Manager initialized for Volume Profile patterns")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Volume State Manager: {e}")
            self.volume_state_manager = None
        
        # Volume Profile Detector for Advanced Pattern Detection
        try:
            from patterns.volume_profile_detector import VolumeProfileDetector
            self.volume_profile_detector = VolumeProfileDetector(self.redis_client)
            self.logger.info("✅ Volume Profile Detector initialized for advanced pattern detection")
        except Exception as e:
            self.logger.warning(f"Failed to initialize Volume Profile Detector: {e}")
            self.volume_profile_detector = None
        
        # Legacy threshold calculator (fallback)
        try:
            time_aware_baseline = TimeAwareVolumeBaseline(self.redis_client)
            self.threshold_calculator = VolumeThresholdCalculator(time_aware_baseline)
        except Exception as e:
            self.logger.warning(f"Failed to initialize VolumeThresholdCalculator: {e}")
            self.threshold_calculator = None
        
        # Unified pattern registry - ADD THESE LINES
        self.all_pattern_detectors = []
        
        # Add basic patterns (existing)
        if hasattr(self, '_detect_volume_patterns'):
            self.all_pattern_detectors.append(self._detect_volume_patterns)
        if hasattr(self, '_detect_momentum_patterns'):
            self.all_pattern_detectors.append(self._detect_momentum_patterns)
        if hasattr(self, '_detect_breakout_patterns'):
            self.all_pattern_detectors.append(self._detect_breakout_patterns)
        if hasattr(self, '_detect_microstructure_patterns'):
            self.all_pattern_detectors.append(self._detect_microstructure_patterns)
        
        # Add Volume Profile patterns (new)
        if hasattr(self, '_detect_volume_profile_patterns'):
            self.all_pattern_detectors.append(self._detect_volume_profile_patterns)
        
        # Add Volume Profile Detector patterns (advanced)
        if hasattr(self, '_detect_advanced_volume_profile_patterns'):
            self.all_pattern_detectors.append(self._detect_advanced_volume_profile_patterns)
        
        # ICT patterns are integrated directly into PatternDetector
        # Using centralized methods only - no external imports
        # All ICT logic uses self._get_current_killzone(), PatternMathematics, etc.
        
        # Add Straddle Strategy (5th straddle strategy) with configuration from thresholds
        try:
            from strategies.kow_signal_straddle import VWAPStraddleStrategy
            
            # Initialize straddle strategy with configuration from thresholds.py
            self.straddle_strategy = VWAPStraddleStrategy(redis_client)
            
            # Add straddle patterns to registry
            self.all_pattern_detectors.extend([
                self._detect_straddle_patterns,  # Wrapper method for straddle patterns
            ])
            
            self.logger.info("✅ Straddle strategy initialized and added to pattern registry")
        except ImportError as e:
            self.logger.warning(f"Straddle strategy not available: {e}")
            self.straddle_strategy = None
        
        self.logger.info("🎯 SimplifiedPatternDetector initialized with 8 core patterns + ICT patterns")
    
    def _update_vix_aware_thresholds(self):
        """Update thresholds based on VIX regime"""
        try:
            self.vix_value = get_vix_value()
            self.vix_regime = get_vix_regime()
            
            self.logger.debug(f"VIX Update: vix_value={self.vix_value}, vix_regime={self.vix_regime}")
            
            # Adjust thresholds based on VIX regime (using standardized 4-regime system)
            if self.vix_regime == "HIGH":
                # HIGH VIX = HIGHER thresholds (less sensitive, reduce noise)
                self.VOLUME_SPIKE_THRESHOLD = self.base_thresholds["volume_spike"] * 1.3  # 2.86x
                self.MOMENTUM_THRESHOLD = self.base_thresholds["momentum"] * 1.4          # 0.7%
                self.BREAKOUT_THRESHOLD = self.base_thresholds["breakout"] * 1.4          # 0.42%
                self.REVERSAL_THRESHOLD = self.base_thresholds["reversal"] * 1.4          # 0.56%
                print(f"🔍 HIGH regime: VOLUME_SPIKE_THRESHOLD={self.VOLUME_SPIKE_THRESHOLD}")
            elif self.vix_regime == "LOW":
                # LOW VIX = LOWER thresholds (more sensitive, catch weaker signals)  
                self.VOLUME_SPIKE_THRESHOLD = self.base_thresholds["volume_spike"] * 0.7  # 1.54x
                self.MOMENTUM_THRESHOLD = self.base_thresholds["momentum"] * 0.8          # 0.4%
                self.BREAKOUT_THRESHOLD = self.base_thresholds["breakout"] * 0.8          # 0.24%
                self.REVERSAL_THRESHOLD = self.base_thresholds["reversal"] * 0.8          # 0.32%
                self.logger.debug(f"LOW regime: VOLUME_SPIKE_THRESHOLD={self.VOLUME_SPIKE_THRESHOLD}")
            else:
                # Default thresholds for unknown regime
                self.VOLUME_SPIKE_THRESHOLD = self.base_thresholds["volume_spike"]
                self.MOMENTUM_THRESHOLD = self.base_thresholds["momentum"]
                self.BREAKOUT_THRESHOLD = self.base_thresholds["breakout"]
                self.REVERSAL_THRESHOLD = self.base_thresholds["reversal"]
                self.logger.debug(f"UNKNOWN regime: VOLUME_SPIKE_THRESHOLD={self.VOLUME_SPIKE_THRESHOLD}")
            
            self.DIVERGENCE_WINDOW = self.base_thresholds["divergence_window"]
            
        except Exception as e:
            self.logger.debug(f"VIX Update Error: {e}")
            self.logger.debug(f"Error updating VIX-aware thresholds: {e}")
            # Use base thresholds as fallback
            self.VOLUME_SPIKE_THRESHOLD = self.base_thresholds["volume_spike"]
            self.MOMENTUM_THRESHOLD = self.base_thresholds["momentum"]
            self.BREAKOUT_THRESHOLD = self.base_thresholds["breakout"]
            self.REVERSAL_THRESHOLD = self.base_thresholds["reversal"]
            self.DIVERGENCE_WINDOW = self.base_thresholds["divergence_window"]

    def _create_pattern_base(self, pattern_type: str, confidence: float, 
                            symbol: str, description: str, **kwargs) -> Dict:
        """Create pattern with PROPER mathematical bounds"""
        math_context = kwargs.pop("math_context", None)
        context_payload = {
            "pattern_type": pattern_type,
            "symbol": symbol,
            "description": description,
            "base_confidence": confidence,
            **kwargs,
        }
        if math_context:
            context_payload.update(math_context)

        dispatched_confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                dispatched_confidence = self.math_dispatcher.calculate_confidence(
                    pattern_type, context_payload
                )
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for {pattern_type}: {dispatch_error}")

        confidence_input = dispatched_confidence if dispatched_confidence is not None else confidence
        # ENFORCE probability bounds 0.0 - 1.0
        bounded_confidence = max(0.0, min(1.0, confidence_input))
        
        # Get risk metrics if Risk Manager available
        risk_data = {}
        if hasattr(self, 'risk_manager') and self.risk_manager:
            try:
                risk_data = self.risk_manager.calculate_risk_metrics({
                    'symbol': symbol,
                    'pattern_type': pattern_type,
                    'confidence': bounded_confidence,
                    **kwargs
                })
            except Exception as e:
                logger.warning(f"Risk manager error for {symbol}: {e}")
        
        news_context = kwargs.get('news_context', {})
        news_boost = self._calculate_news_boost(symbol, bounded_confidence, news_context)
        enhanced_confidence = self._apply_news_boost(bounded_confidence, news_boost)
        
        # Determine actionable trading action based on pattern and enhanced confidence
        action = self._determine_trading_action(pattern_type, enhanced_confidence, kwargs)
        
        # Calculate real stop loss
        stop_loss = self._calculate_stop_loss(
            pattern_type,
            kwargs.get('last_price', 0.0),
            kwargs.get('atr', 0.0),
            enhanced_confidence,
            action if isinstance(action, str) else kwargs.get('signal')
        )
        
        # Calculate real position size
        position_size = self._calculate_position_size(
            bounded_confidence, 
            kwargs.get('last_price', 0.0), 
            stop_loss,
            pattern_type
        )
        
        # Enhanced description with news context
        enhanced_description = description
        if news_context and news_context.get('impact') == 'HIGH':
            enhanced_description += f" | News: {news_context.get('title', 'High-impact news')}"
        
        pattern = {
            "symbol": symbol,
            "pattern": pattern_type,
            "confidence": enhanced_confidence,  # ✅ News-enhanced confidence
            "action": action,  # ✅ Actionable trading action
            "signal": risk_data.get('signal', kwargs.get('signal', 'NEUTRAL')),  # Use original signal if risk manager doesn't provide one
            "expected_move": risk_data.get('expected_move', 1.0),  # From Risk Manager
            "position_size": position_size,  # ✅ Calculated position size
            "stop_loss": stop_loss,  # ✅ Calculated stop loss
            "last_price": kwargs.get('last_price', 0.0),
            "volume_ratio": kwargs.get('volume_ratio', 1.0),  # Use actual calculated ratio
            "description": enhanced_description,  # ✅ News-enhanced description
            "pattern_type": kwargs.get('pattern_category', 'core'),
            "risk_metrics": risk_data,  # Include full risk assessment
            "news_context": news_context,  # ✅ Include news context
            "news_boost": news_boost  # ✅ Track boost for transparency
        }
        if math_context:
            pattern["math_context"] = math_context
        return pattern

    def _resolve_confidence(self, pattern_type: str, context: Dict[str, Any], fallback) -> Optional[float]:
        """
        Route confidence calculations through the centralized dispatcher.

        Args:
            pattern_type: Canonical pattern identifier.
            context: Payload describing the current pattern environment.
            fallback: Callable (or static value) returning the legacy confidence
                estimate when the dispatcher is unavailable.
        """
        dispatched = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                dispatched = self.math_dispatcher.calculate_confidence(pattern_type, context)
            except Exception as dispatch_error:
                self.logger.debug(f"MathDispatcher confidence error for {pattern_type}: {dispatch_error}")

        if dispatched is not None:
            return dispatched

        return fallback() if callable(fallback) else fallback

    def _determine_trading_action(self, pattern_type: str, confidence: float, kwargs: Dict) -> str:
        """Determine actionable trading action based on pattern and confidence."""
        price_change = kwargs.get('price_change', 0)
        volume_ratio = kwargs.get('volume_ratio', 1.0)
        signal_hint = (kwargs.get('signal') or '').upper()
        
        def _directional_action(default_buy: str, default_sell: str) -> str:
            if signal_hint == 'BUY':
                return default_buy
            if signal_hint == 'SELL':
                return default_sell
            return default_buy if price_change > 0 else default_sell

        if pattern_type == 'breakout':
            if confidence >= 0.8:
                return _directional_action('BUY_MARKET', 'SELL_SHORT')
            if confidence >= 0.4:
                return _directional_action('BUY_LIMIT', 'SELL_LIMIT')
            return _directional_action('WATCH_BUY', 'WATCH_SELL')

        if confidence >= 0.8:
            if pattern_type in ['volume_spike', 'volume_breakout', 'upside_momentum']:
                return 'BUY_MARKET' if price_change > 0 else 'SELL_SHORT'
            if pattern_type in ['downside_momentum', 'reversal']:
                return 'SELL_SHORT' if price_change < 0 else 'BUY_MARKET'
            if pattern_type in ['ict_liquidity_pools', 'ict_fair_value_gaps', 'ict_optimal_trade_entry',
                                'ict_premium_discount', 'ict_buy_pressure', 'ict_sell_pressure']:
                return _directional_action('BUY_LIMIT', 'SELL_LIMIT')
            if pattern_type in ['ict_killzone', 'ict_momentum']:
                return _directional_action('BUY_LIMIT', 'SELL_LIMIT')
            if pattern_type in ['iv_crush_play_straddle', 'premium_collection_strategy']:
                return 'SELL_STRADDLE'
            if pattern_type in ['range_bound_strangle']:
                return 'SELL_STRANGLE'
        
        if confidence >= 0.6:
            if pattern_type in ['volume_spike', 'volume_breakout', 'upside_momentum', 'downside_momentum']:
                return 'BUY_LIMIT' if price_change > 0 else 'SELL_LIMIT'
            if pattern_type in ['ict_liquidity_pools', 'ict_fair_value_gaps', 'ict_optimal_trade_entry',
                                'ict_premium_discount', 'ict_killzone', 'ict_momentum',
                                'ict_buy_pressure', 'ict_sell_pressure']:
                return _directional_action('BUY_LIMIT', 'SELL_LIMIT')
        
        if confidence >= 0.4:
            if pattern_type in ['volume_spike', 'volume_breakout', 'hidden_accumulation']:
                return 'BUY_LIMIT' if price_change > 0 else 'SELL_LIMIT'
            if pattern_type in ['ict_optimal_trade_entry', 'ict_premium_discount', 'ict_buy_pressure', 'ict_sell_pressure']:
                return _directional_action('WATCH_BUY', 'WATCH_SELL')
        
        return 'WATCH'

    def calculate_position_size(self, entry_price, stop_loss, risk_per_trade=0.01, account_size=1000000,
                                 pattern_type: Optional[str] = None, confidence: Optional[float] = None):
        """Calculate position size via dispatcher with legacy fallback."""
        context = {
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "risk_per_trade": risk_per_trade,
            "account_size": account_size,
            "pattern_type": pattern_type,
            "confidence": confidence,
        }
        position_size = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                position_size = self.math_dispatcher.calculate_position_size(pattern_type or "generic", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher position size error for {pattern_type}: {dispatch_error}")

        if position_size is None:
            position_size = self._legacy_position_size(entry_price, stop_loss, risk_per_trade, account_size)

        return max(1, min(1000, int(position_size)))
    
    def _legacy_position_size(self, entry_price, stop_loss, risk_per_trade=0.01, account_size=1000000):
        risk_per_share = entry_price - stop_loss
        if risk_per_share <= 0:
            return 1
        risk_amount = account_size * risk_per_trade
        return risk_amount / risk_per_share
    
    def _calculate_position_size(self, confidence: float, entry_price: float, stop_loss: float, pattern_type: str) -> int:
        """Calculate real position size using dispatcher with fallback."""
        return self.calculate_position_size(
            entry_price,
            stop_loss,
            0.01,
            1000000,
            pattern_type=pattern_type,
            confidence=confidence,
        )

    def _calculate_stop_loss(self, pattern_type: str, entry_price: float, atr: float, confidence: float, signal: Optional[str] = None) -> float:
        """Calculate stop loss using dispatcher with legacy fallback."""
        context = {
            "entry_price": entry_price,
            "atr": atr,
            "confidence": confidence,
            "signal": signal,
            "pattern_type": pattern_type,
        }
        stop_loss = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                stop_loss = self.math_dispatcher.calculate_stop_loss(pattern_type, context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher stop loss error for {pattern_type}: {dispatch_error}")

        if stop_loss is None:
            stop_loss = self._legacy_stop_loss(pattern_type, entry_price, atr, confidence, signal)

        return max(0.0, stop_loss)

    def _legacy_stop_loss(self, pattern_type: str, entry_price: float, atr: float, confidence: float, signal: Optional[str]) -> float:
        if entry_price <= 0:
            return 0.0

        if atr <= 0:
            atr = entry_price * 0.02

        pattern = pattern_type.lower()
        atr_multiplier_map = {
            'volume_spike': 1.5,
            'volume_breakout': 1.0,
            'upside_momentum': 1.0,
            'downside_momentum': 1.0,
            'breakout': 0.8,
            'reversal': 0.8,
            'ict_liquidity_pools': 1.2,
            'ict_fair_value_gaps': 1.2,
            'ict_optimal_trade_entry': 1.2,
            'ict_killzone': 1.0,
            'ict_momentum': 1.0,
            'ict_buy_pressure': 1.0,
            'ict_sell_pressure': 1.0,
            'ict_premium_discount': 1.3,
            'iv_crush_play_straddle': 2.0,
            'premium_collection_strategy': 2.0,
            'range_bound_strangle': 1.5,
            'market_maker_trap_detection': 1.0,
            'kow_signal_straddle': 1.5,
        }
        stop_distance = atr * atr_multiplier_map.get(pattern, 1.0)
        confidence_multiplier = 1.0 - (confidence - 0.5) * 0.2
        stop_distance *= confidence_multiplier

        direction_hint = (signal or '').upper()
        if 'downside' in pattern or 'sell' in pattern or direction_hint in {'SELL', 'SHORT'}:
            stop_loss = entry_price + stop_distance
        else:
            stop_loss = entry_price - stop_distance

        max_stop_distance = entry_price * 0.05
        if abs(entry_price - stop_loss) > max_stop_distance:
            if 'downside' in pattern or 'sell' in pattern or direction_hint in {'SELL', 'SHORT'}:
                stop_loss = entry_price + max_stop_distance
            else:
                stop_loss = entry_price - max_stop_distance

        return stop_loss

    def _calculate_news_boost(self, symbol: str, base_confidence: float, news_context: Dict) -> float:
        """Resolve news boost via dispatcher with legacy fallback."""
        if not news_context or base_confidence < 0.60:
            return 0.0

        boost = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                boost = self.math_dispatcher.calculate_news_boost(symbol, news_context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher news impact error for {symbol}: {dispatch_error}")

        if boost is None:
            boost = self._legacy_news_boost(news_context)

        return float(boost or 0.0)
    
    def _legacy_news_boost(self, news_context: Dict) -> float:
        base_boost = 0.0
        
        # Additive boosts (not multiplicative) - CONSERVATIVE VALUES
        if news_context.get('impact') == 'HIGH':
            base_boost += 0.06  # 6% (was 15%)
        elif news_context.get('impact') == 'MEDIUM':
            base_boost += 0.03  # 3% (was 8%)
        
        if news_context.get('volume_trigger'):
            base_boost += 0.03  # 3% (was 10%)
        
        # Sentiment-based adjustment - CONSERVATIVE VALUES
        sentiment = news_context.get('sentiment', 0)
        if isinstance(sentiment, str):
            if sentiment == 'positive':
                base_boost += 0.02  # 2% (was 5%)
            elif sentiment == 'negative':
                base_boost -= 0.02  # -2% (was -5%)
        elif isinstance(sentiment, (int, float)):
            if sentiment > 0.2:  # Positive sentiment
                base_boost += 0.02  # 2% (was 5%)
            elif sentiment < -0.2:  # Negative sentiment
                base_boost -= 0.02  # -2% (was -5%)
        
        # HARD CAP: Never exceed 10% total boost - CONSERVATIVE CAPS
        return max(-0.05, min(0.10, base_boost))
    
    def _apply_news_boost(self, pattern_confidence: float, news_boost: float) -> float:
        """Apply boost without distortion"""
        boosted = pattern_confidence + (news_boost or 0.0)
        return max(0.10, min(0.95, boosted))  # Keep within reasonable bounds
    
    def _parse_killzone_windows(self, windows_config: List[Tuple[Any, Any]]) -> List[Tuple[dt_time, dt_time]]:
        """Parse killzone window configuration into datetime.time tuples."""
        parsed: List[Tuple[dt_time, dt_time]] = []
        for window in windows_config:
            if not isinstance(window, (list, tuple)) or len(window) != 2:
                continue
            start_raw, end_raw = window
            try:
                start_time = datetime.strptime(str(start_raw), "%H:%M").time()
                end_time = datetime.strptime(str(end_raw), "%H:%M").time()
                parsed.append((start_time, end_time))
            except Exception:
                continue
        if not parsed:
            parsed = [
                (datetime.strptime("09:15", "%H:%M").time(), datetime.strptime("10:30", "%H:%M").time()),
                (datetime.strptime("13:30", "%H:%M").time(), datetime.strptime("15:00", "%H:%M").time()),
            ]
        return parsed

    def _killzone_allows(self, pattern_type: str, timestamp: Optional[Any] = None) -> bool:
        """Check if killzone fallback permits the given pattern."""
        if not self.killzone_fallback_enabled:
            return True
        if pattern_type not in self.killzone_fallback_patterns:
            return True
        return self._is_within_killzone(timestamp)

    def _should_enter_volume_spike(self, signal: Dict) -> bool:
        """Volume Spike Trading Rules"""
        return (signal.get('volume_ratio', 0) >= 2.0 and 
                signal.get('confidence', 0) >= 0.7 and
                signal.get('rsi', 50) < 70)

    def _calculate_volume_spike_exit(self, entry_price: float, atr: float) -> Dict:
        """Calculate volume spike exit conditions"""
        return {
            'stop_loss': entry_price - (atr * 1.5),
            'target': entry_price + (atr * 2.0),
            'timeframe': '30min'  # Exit after 30min regardless
        }

    def _is_within_killzone(self, timestamp: Optional[Any] = None) -> bool:
        """Check if a timestamp (or now) is within configured killzone windows."""
        if timestamp is None:
            current_time = datetime.now().time()
        elif isinstance(timestamp, datetime):
            current_time = timestamp.time()
        elif isinstance(timestamp, dt_time):
            current_time = timestamp
        else:
            try:
                current_time = datetime.strptime(str(timestamp), "%H:%M:%S").time()
            except Exception:
                try:
                    current_time = datetime.strptime(str(timestamp), "%H:%M").time()
                except Exception:
                    current_time = datetime.now().time()
        
        for start, end in self.killzone_windows:
            if start <= current_time <= end:
                return True
        return False

    def _should_enter_breakout(self, signal: Dict) -> bool:
        """Breakout Trading Rules"""
        return (signal.get('volume_ratio', 0) >= 1.5 and 
                signal.get('confidence', 0) >= 0.6 and
                abs(signal.get('price_change', 0)) >= 0.5)

    def _calculate_breakout_exit(self, entry_price: float, atr: float, direction: str) -> Dict:
        """Calculate breakout exit conditions"""
        if direction == 'BUY':
            return {
                'stop_loss': entry_price - (atr * 1.0),
                'target': entry_price + (atr * 2.5),
                'timeframe': '45min'
            }
        else:
            return {
                'stop_loss': entry_price + (atr * 1.0),
                'target': entry_price - (atr * 2.5),
                'timeframe': '45min'
            }

    def _calculate_stop_hunt_zones(self, pools: Dict[str, Optional[float]], indicators: Dict) -> List[float]:
        """CORRECT: Stop hunts occur at liquidity clusters, not fixed percentages"""
        levels = [pools.get("previous_day_high"), pools.get("previous_day_low")]
        hunt_zones = []
        
        # Get ATR from indicators (same pattern as other methods)
        last_price = indicators.get('last_price', 0.0)
        atr = indicators.get('atr', last_price * 0.02)  # Default 2% if ATR missing
        
        for level in levels:
            if level and level > 0:
                # Stop hunts typically occur 0.5-1.5 ATR beyond key levels
                hunt_zones.append(level + (atr * 0.7))   # Above resistance
                hunt_zones.append(level + (atr * 1.3))   # Further above for liquidity grabs
                hunt_zones.append(level - (atr * 0.7))   # Below support  
                hunt_zones.append(level - (atr * 1.3))   # Further below for liquidity grabs
        
        return hunt_zones

    def _calculate_position_size_safe(self, price: float, confidence: float, pattern_data: Dict = None) -> float:
        """SAFE: Conservative position sizing with multiple validations"""
        if price <= 0 or confidence < 0.4:  # 🚨 Minimum 40% confidence to trade
            return 0
        
        # Base size only 25% of maximum (conservative)
        base_size = self.max_position_size * 0.25
        
        # Confidence multiplier: 40% confidence = 0%, 100% confidence = 100% of base
        confidence_multiplier = max(0, (confidence - 0.4) / 0.6)  # Scale from 0.4 to 1.0
        
        # VIX adjustment: Reduce size in high volatility
        vix_multiplier = self._get_vix_position_multiplier()
        
        # Pattern quality adjustment
        pattern_multiplier = self._get_pattern_quality_multiplier(pattern_data)
        
        # Calculate final position size
        position_size = base_size * confidence_multiplier * vix_multiplier * pattern_multiplier
        
        # Hard limits
        position_size = min(position_size, self.max_position_size)
        position_size = max(position_size, self.min_position_size)  # Add minimum size
        
        return round(position_size / 1000) * 1000

    def _get_vix_position_multiplier(self) -> float:
        """Reduce position size in high VIX regimes"""
        vix_regime = self._get_current_vix_regime()
        multipliers = {
            "PANIC": 0.3,    # 70% size reduction
            "HIGH_VIX": 0.6,  # 40% size reduction  
            "NORMAL": 1.0,
            "LOW_VIX": 1.2   # 20% size increase in low volatility
        }
        return multipliers.get(vix_regime, 0.8)  # Default 20% reduction

    def _find_support_resistance(self) -> Tuple[List, List]:
        """IMPROVED: Statistical significance for S/R levels"""
        if len(self.price_volume) < 10:  # Need sufficient data
            return [], []
        
        sorted_prices = sorted(self.price_volume.keys())
        volumes_list = [self.price_volume[p] for p in sorted_prices]
        
        # Calculate volume statistics
        mean_volume = np.mean(volumes_list)
        std_volume = np.std(volumes_list)
        
        support, resistance = [], []
        
        for i in range(3, len(volumes_list) - 3):
            current_volume = volumes_list[i]
            
            # Volume must be statistically significant (above mean + 0.5 std)
            if current_volume < mean_volume + (0.5 * std_volume):
                continue
                
            # Check for local maximum with smoother window
            window = volumes_list[i-3:i+4]
            if current_volume == max(window):
                # Classify as support or resistance relative to POC
                if sorted_prices[i] < self.get_profile_data().get('poc_price', 0):
                    support.append(sorted_prices[i])
                else:
                    resistance.append(sorted_prices[i])
        
        return support[:3], resistance[:3]

    def validate_volume_profile_alignment(self, pattern: Dict, volume_nodes: Dict) -> bool:
        """CRITICAL: Validate pattern against volume profile structure"""
        # ✅ Use YAML field mapping for proper field resolution
        from utils.yaml_field_loader import resolve_calculated_field
        
        price = pattern.get(resolve_calculated_field('last_price')) or pattern.get('last_price')
        direction = pattern.get(resolve_calculated_field('signal')) or pattern.get('signal')
        pattern_type = pattern.get('pattern')
        
        # Debug logging to see what data we're getting
        self.logger.debug(f"Volume profile validation for {pattern.get('symbol', 'UNKNOWN')}: pattern_type={pattern_type}, direction={direction}, price={price}")
        self.logger.debug(f"Volume nodes data: {volume_nodes}")
        
        # If no volume profile data available, allow validation to pass
        if not volume_nodes or len(volume_nodes) == 0:
            self.logger.debug(f"Volume profile validation PASSED for {pattern.get('symbol', 'UNKNOWN')}: No volume profile data available, allowing validation")
            return True
        
        # Rule 1: Breakout patterns must be near Value Area boundaries
        if pattern_type in ['breakout', 'volume_breakout']:
            vah = volume_nodes.get('value_area_high')
            val = volume_nodes.get('value_area_low')
            if vah and val and vah > 0 and val > 0:
                if direction == 'BULLISH' and price < vah * 0.98:  # Within 2% of VAH
                    self.logger.debug(f"Breakout validation failed: price {price} < VAH {vah} * 0.98")
                    return False
                if direction == 'BEARISH' and price > val * 1.02:  # Within 2% of VAL
                    self.logger.debug(f"Breakout validation failed: price {price} > VAL {val} * 1.02")
                    return False
        
        # Rule 2: Reversal patterns must be near POC
        if pattern_type == 'reversal':
            poc = volume_nodes.get('poc_price')
            if poc and poc > 0:
                poc_distance = abs(price - poc) / poc
                if poc_distance > 0.015:  # More than 1.5% from POC
                    self.logger.debug(f"Reversal validation failed: distance {poc_distance} > 0.015 from POC {poc}")
                    return False
        
        # Rule 3: Volume spikes need volume profile confirmation
        if pattern_type == 'volume_spike':
            profile_strength = volume_nodes.get('profile_strength', 0.5)  # Default to moderate strength
            if profile_strength < 0.3:  # Weak volume profile
                self.logger.debug(f"Volume spike validation failed: profile_strength {profile_strength} < 0.3")
                return False
        
        self.logger.debug(f"Volume profile validation PASSED for {pattern.get('symbol', 'UNKNOWN')}")
        return True

    def validate_ict_liquidity_alignment(self, pattern: Dict, liquidity_pools: Dict) -> bool:
        """CRITICAL: Validate against ICT liquidity concepts"""
        # ✅ Use YAML field mapping for proper field resolution
        from utils.yaml_field_loader import resolve_calculated_field
        
        price = pattern.get(resolve_calculated_field('last_price')) or pattern.get('last_price')
        direction = pattern.get(resolve_calculated_field('signal')) or pattern.get('signal')
        
        # Debug logging to see what data we're getting
        self.logger.debug(f"ICT liquidity validation for {pattern.get('symbol', 'UNKNOWN')}: direction={direction}, price={price}")
        self.logger.debug(f"Liquidity pools data: {liquidity_pools}")
        
        # If no liquidity data available, allow validation to pass
        if not liquidity_pools or len(liquidity_pools) == 0:
            self.logger.debug(f"ICT liquidity validation PASSED for {pattern.get('symbol', 'UNKNOWN')}: No liquidity data available, allowing validation")
            return True
        
        pd_high = liquidity_pools.get('previous_day_high')
        pd_low = liquidity_pools.get('previous_day_low')
        stop_hunt_zones = liquidity_pools.get('stop_hunt_zones', [])
        
        # Rule 1: Bullish signals should be near previous day low (liquidity grab)
        if direction == 'BULLISH' and pd_low and pd_low > 0:
            distance_to_pd_low = abs(price - pd_low) / pd_low
            if distance_to_pd_low > 0.02:  # More than 2% from PD low
                self.logger.debug(f"ICT validation failed: distance to PD low {distance_to_pd_low} > 0.02")
                return False
        
        # Rule 2: Bearish signals should be near previous day high
        if direction == 'BEARISH' and pd_high and pd_high > 0:
            distance_to_pd_high = abs(price - pd_high) / pd_high  
            if distance_to_pd_high > 0.02:  # More than 2% from PD high
                self.logger.debug(f"ICT validation failed: distance to PD high {distance_to_pd_high} > 0.02")
                return False
        
        # Rule 3: Should be near stop hunt zone (liquidity present) - only if zones exist
        if stop_hunt_zones and len(stop_hunt_zones) > 0:
            near_stop_hunt = any(abs(price - zone) / zone <= 0.01 for zone in stop_hunt_zones)
            if not near_stop_hunt:
                self.logger.debug(f"ICT validation failed: not near stop hunt zones {stop_hunt_zones}")
                return False
        
        self.logger.debug(f"ICT liquidity validation PASSED for {pattern.get('symbol', 'UNKNOWN')}")
        return True

    def validate_multi_timeframe_confirmation(self, pattern: Dict, symbol: str) -> bool:
        """CRITICAL: Multi-timeframe pattern confirmation"""
        # ✅ Use YAML field mapping for proper field resolution
        from utils.yaml_field_loader import resolve_calculated_field
        
        direction = pattern.get(resolve_calculated_field('signal')) or pattern.get('signal')
        pattern_type = pattern.get('pattern')
        
        # Debug logging
        self.logger.debug(f"Multi-timeframe validation for {symbol}: pattern_type={pattern_type}, direction={direction}")
        
        timeframes = ['5min', '15min', '30min']
        confirmations = 0
        available_timeframes = 0
        
        for tf in timeframes:
            tf_data = self._get_timeframe_data(symbol, tf)
            if not tf_data:
                self.logger.debug(f"No data for timeframe {tf}, skipping")
                continue
            
            available_timeframes += 1
                
            # Check trend alignment
            tf_trend = self._get_timeframe_trend(tf_data)
            self.logger.debug(f"Timeframe {tf}: trend={tf_trend}")
            
            # For momentum/breakout patterns, require trend alignment
            if pattern_type in ['breakout', 'volume_breakout', 'upside_momentum', 'downside_momentum']:
                if tf_trend == direction:
                    confirmations += 1
                    self.logger.debug(f"Confirmation from {tf}: trend {tf_trend} matches direction {direction}")
            # For reversal patterns, allow counter-trend
            else:
                if tf_trend == 'SIDEWAYS' or tf_trend == direction:
                    confirmations += 1
                    self.logger.debug(f"Confirmation from {tf}: trend {tf_trend} acceptable for reversal")
        
        # If no timeframe data is available, allow validation to pass
        if available_timeframes == 0:
            self.logger.debug(f"Multi-timeframe validation PASSED for {symbol}: No timeframe data available, allowing validation")
            return True
        
        # Require at least 2 higher timeframe confirmations, or 1 if only 1 timeframe available
        required_confirmations = min(2, available_timeframes)
        result = confirmations >= required_confirmations
        self.logger.debug(f"Multi-timeframe validation result: {confirmations}/{required_confirmations} confirmations = {result}")
        return result

    def validate_pattern_specific_risk(self, pattern: Dict) -> bool:
        """CRITICAL: Pattern-specific risk validation"""
        pattern_type = pattern.get('pattern')
        confidence = pattern.get('confidence')
        
        risk_requirements = {
            'LOW_RISK': 0.65,    # volume_breakout, breakout
            'MEDIUM_RISK': 0.70,  # volume_spike, reversal  
            'HIGH_RISK': 0.80,    # hidden_accumulation, volume_price_divergence
        }
        
        risk_categories = {
            'LOW_RISK': ['volume_breakout', 'breakout'],
            'MEDIUM_RISK': ['volume_spike', 'reversal', 'ict_premium_discount', 'ict_momentum', 'upside_momentum', 'downside_momentum'],
            'HIGH_RISK': ['hidden_accumulation', 'volume_price_divergence']
        }
        
        # Determine risk category
        risk_category = 'HIGH_RISK'  # Default
        for category, patterns in risk_categories.items():
            if pattern_type in patterns:
                risk_category = category
                break
        
        # Apply risk-adjusted confidence requirement
        required_confidence = risk_requirements.get(risk_category, 0.75)
        
        # Debug logging
        self.logger.debug(f"Pattern-specific risk validation: pattern_type={pattern_type}, confidence={confidence}, risk_category={risk_category}, required_confidence={required_confidence}")
        
        if confidence < required_confidence:
            self.logger.debug(f"Risk validation failed: confidence {confidence} < required {required_confidence}")
            return False
        
        # Additional high-risk pattern validations
        if risk_category == 'HIGH_RISK':
            # Require stronger volume confirmation
            volume_ratio = pattern.get('volume_ratio', 1.0)
            if volume_ratio < 2.5:
                return False
            
            # Require larger expected move
            expected_move = pattern.get('expected_move', 0)
            if expected_move < 0.8:  # 0.8% minimum for high-risk
                return False
        
        return True

    def _get_timeframe_data(self, symbol: str, timeframe: str) -> Optional[Dict]:
        """Get timeframe data for multi-timeframe analysis"""
        try:
            if not self.redis_client:
                self.logger.debug(f"No Redis client for timeframe data: {symbol} {timeframe}")
                return None
            
            # Try to get OHLC data for the timeframe
            ohlc_key = f"ohlc:{symbol}:{timeframe}"
            ohlc_data = self.redis_client.hgetall(ohlc_key)
            
            if ohlc_data:
                # Convert bytes to strings if needed
                data = {}
                for k, v in ohlc_data.items():
                    key = k.decode() if isinstance(k, bytes) else k
                    value = v.decode() if isinstance(v, bytes) else v
                    try:
                        data[key] = float(value)
                    except (ValueError, TypeError):
                        data[key] = value
                
                self.logger.debug(f"Got timeframe data for {symbol} {timeframe}: {data}")
                return data
            
            # Fallback: pull recent ticks from the continuous market stream
            try:
                ticks_db = self.redis_client.get_database_for_data_type("ticks_stream")
            except Exception:
                ticks_db = 4  # default to continuous market DB if mapping unavailable

            tick_client = self.redis_client.get_client(ticks_db) if ticks_db is not None else None
            if tick_client:
                try:
                    stream_key = f"ticks:{symbol}"
                    stream_entries = tick_client.xrevrange(stream_key, count=32)
                    ohlc_from_stream = self._build_ohlc_from_stream(symbol, stream_entries)
                    if ohlc_from_stream:
                        self.logger.debug(f"Created timeframe data from stream ticks for {symbol} {timeframe}: {ohlc_from_stream}")
                        return ohlc_from_stream
                except Exception as stream_error:
                    self.logger.debug(f"Stream-based timeframe fallback failed for {symbol}: {stream_error}")

            self.logger.debug(f"No timeframe data found for {symbol} {timeframe}")
            return None
            
        except Exception as e:
            self.logger.debug(f"Error getting timeframe data for {symbol} {timeframe}: {e}")
            return None

    def _build_ohlc_from_stream(self, symbol: str, stream_entries: List[Any]) -> Optional[Dict[str, float]]:
        """Construct an OHLC snapshot from Redis stream tick data."""
        if not stream_entries:
            return None

        prices: List[float] = []
        cumulative_volume = 0.0
        latest_timestamp: Optional[float] = None

        for entry_id, payload in reversed(stream_entries):  # chronological order
            data = payload
            if not isinstance(data, dict):
                continue

            price_raw = (data.get("last_price") or data.get("price") or data.get("ltp"))
            if price_raw is None:
                continue
            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                continue

            prices.append(price)

            volume_raw = data.get("volume") or data.get("traded_quantity") or data.get("last_traded_quantity")
            try:
                cumulative_volume += float(volume_raw or 0.0)
            except (TypeError, ValueError):
                pass

            if latest_timestamp is None:
                ts_component = data.get("timestamp") or data.get("timestamp_ms") or data.get("exchange_timestamp")
                try:
                    latest_timestamp = float(ts_component) if ts_component is not None else None
                except (TypeError, ValueError):
                    latest_timestamp = None

        if not prices:
            return None

        return {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": cumulative_volume,
            "tick_count": len(prices),
            "source": "redis_stream",
            "timestamp": latest_timestamp,
            "symbol": symbol,
        }

    def _get_timeframe_trend(self, tf_data: Dict) -> str:
        """Determine trend direction from timeframe data"""
        try:
            if not tf_data:
                return 'SIDEWAYS'
            
            open_price = tf_data.get('open', 0)
            close_price = tf_data.get('close', 0)
            high_price = tf_data.get('high', 0)
            low_price = tf_data.get('low', 0)
            
            if not all([open_price, close_price, high_price, low_price]):
                return 'SIDEWAYS'
            
            # Calculate price change percentage
            price_change = (close_price - open_price) / open_price if open_price > 0 else 0
            
            # Determine trend based on price movement
            if price_change > 0.005:  # > 0.5% up
                trend = 'BULLISH'
            elif price_change < -0.005:  # > 0.5% down
                trend = 'BEARISH'
            else:
                trend = 'SIDEWAYS'
            
            self.logger.debug(f"Trend analysis: open={open_price}, close={close_price}, change={price_change:.4f}, trend={trend}")
            return trend
            
        except Exception as e:
            self.logger.debug(f"Error determining trend: {e}")
            return 'SIDEWAYS'

    @time_pattern_method
    def detect_volume_spike(self, indicators: Dict) -> Dict:
        """SAFE: Volume spike must have price movement confirmation"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change_pct = indicators.get('price_change_pct', 0)
        last_price = indicators.get('last_price', 0.0)
        
        # 🚨 CRITICAL: Volume spike alone means NOTHING - must have price confirmation
        dynamic_threshold = self._get_dynamic_threshold("volume_spike")
        min_price_move = 0.005  # 0.5% minimum price movement

        if not (
            volume_ratio >= dynamic_threshold
            and abs(price_change_pct) >= min_price_move
            and price_change_pct * volume_ratio > 0
        ):
            return None

        context = {
            "pattern_type": "volume_spike",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "dynamic_threshold": dynamic_threshold,
            "min_price_move": min_price_move,
            "price_change_pct": price_change_pct,
            "rsi": indicators.get('rsi', 50),
            "last_price": last_price,
            "atr": indicators.get('atr', last_price * 0.02),
            "timestamp": indicators.get('timestamp') or indicators.get('timestamp_ms'),
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("volume_spike", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for volume_spike {symbol}: {dispatch_error}")

        if confidence is None:
            volume_confidence = PatternMathematics.calculate_volume_confidence(
                volume_ratio, dynamic_threshold, 0.55
            )
            price_confidence = PatternMathematics.calculate_price_confidence(
                abs(price_change_pct), min_price_move, 0.45
            )
            confidence = max(0.4, min(0.9, volume_confidence * 0.55 + price_confidence * 0.45))
        else:
            confidence = max(0.4, min(0.95, confidence))

        if confidence < 0.45:
            return None

        if self.config.get('killzone_enabled', True) and not self._killzone_allows("volume_spike", context.get("timestamp")):
            logger.debug("Killzone configuration blocked volume_spike for %s", symbol)
            return None

        targets = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                targets = self.math_dispatcher.calculate_price_targets("volume_spike", {**context, "confidence": confidence})
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher targets error for volume_spike {symbol}: {dispatch_error}")

        if not targets:
            atr = context["atr"]
            if price_change_pct > 0:
                entry_price = last_price * 0.998
                stop_loss = last_price - (atr * 1.5)
                target_price = last_price + (atr * 2.0)
            else:
                entry_price = last_price * 1.002
                stop_loss = last_price + (atr * 1.5)
                target_price = last_price - (atr * 2.0)
            targets = {
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target_price": target_price,
            }

        expected_move_pct = (
            ((targets["target_price"] - targets["entry_price"]) / targets["entry_price"]) * 100
            if targets["entry_price"]
            else 0.0
        )

        news_context = indicators.get('news_context') if isinstance(indicators.get('news_context'), dict) else None

        return self._create_pattern_base(
            pattern_type="volume_spike",
            confidence=confidence,
            symbol=symbol,
            description=f"Volume spike: {volume_ratio:.1f}x + Price move: {price_change_pct:.2f}%",
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change_pct=price_change_pct,
            pattern_category="volume",
            action='BUY_LIMIT' if price_change_pct > 0 else 'SELL_LIMIT',
            entry_price=targets["entry_price"],
            stop_loss=targets["stop_loss"],
            target=targets["target_price"],
            expected_move=expected_move_pct,
            news_context=news_context or {},
            atr=context["atr"],
            timestamp=context.get("timestamp"),
            reason=f"Volume spike {volume_ratio:.1f}x + Price move {price_change_pct:.2f}%"
        )

    @time_pattern_method
    def detect_volume_breakout(self, indicators: Dict) -> Dict:
        """Volume breakout with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        last_price = indicators.get('last_price', 0.0)

        dynamic_threshold = self._get_dynamic_threshold("volume_breakout")

        if not (abs(price_change) > self.BREAKOUT_THRESHOLD and volume_ratio >= dynamic_threshold):
            return None

        context = {
            "pattern_type": "volume_breakout",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "dynamic_threshold": dynamic_threshold,
            "price_change": price_change,
            "last_price": last_price,
            "atr": indicators.get('atr', last_price * 0.02),
            "price_threshold": self.BREAKOUT_THRESHOLD,
            "timestamp": indicators.get('timestamp') or indicators.get('timestamp_ms'),
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("volume_breakout", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for volume_breakout {symbol}: {dispatch_error}")

        if confidence is None:
            price_confidence = PatternMathematics.calculate_price_confidence(price_change, self.BREAKOUT_THRESHOLD, 0.55)
            volume_confidence = PatternMathematics.calculate_volume_confidence(volume_ratio, dynamic_threshold, 0.45)
            confidence = max(0.45, min(0.9, price_confidence * 0.6 + volume_confidence * 0.4))
        else:
            confidence = max(0.45, min(0.95, confidence))

        if confidence < 0.45:
            return None

        if self.config.get('killzone_enabled', True) and not self._killzone_allows("volume_breakout", context.get("timestamp")):
            logger.debug("Killzone fallback blocked volume_breakout for %s", symbol)
            return None

        targets = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                targets = self.math_dispatcher.calculate_price_targets("volume_breakout", {**context, "confidence": confidence})
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher targets error for volume_breakout {symbol}: {dispatch_error}")

        if not targets:
            atr = context["atr"]
            if price_change > 0:
                entry_price = last_price * 1.002
                stop_loss = last_price - (atr * 1.0)
                target_price = last_price + (atr * 2.5)
            else:
                entry_price = last_price * 0.998
                stop_loss = last_price + (atr * 1.0)
                target_price = last_price - (atr * 2.5)
            targets = {
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target_price": target_price,
            }

        news_context = indicators.get('news_context') if isinstance(indicators.get('news_context'), dict) else None

        return self._create_pattern_base(
            pattern_type="volume_breakout",
            confidence=confidence,
            symbol=symbol,
            description=f"Volume breakout: {price_change:.2f}% with {volume_ratio:.1f}x volume",
            signal="BUY" if price_change > 0 else "SELL",
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change=price_change,
            pattern_category="volume",
            action='BUY_LIMIT' if price_change > 0 else 'SELL_LIMIT',
            entry_price=targets["entry_price"],
            stop_loss=targets["stop_loss"],
            target=targets["target_price"],
            news_context=news_context or {},
            atr=context["atr"],
            timestamp=context.get("timestamp"),
            reason=f"Breakout {price_change:.2f}% + Volume {volume_ratio:.1f}x"
        )

        return None

    def detect_volume_price_divergence(self, indicators: Dict) -> Dict:
        """Volume-price divergence with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        last_price = indicators.get('last_price', 0.0)
        
        dynamic_threshold = self._get_dynamic_threshold("volume_price_divergence")
        
        # VIX-aware divergence thresholds
        vix_multiplier = 1.4 if self.vix_regime == "HIGH" else 0.8 if self.vix_regime == "LOW" else 1.0
        price_threshold = 0.5 * vix_multiplier
        
        if not ((volume_ratio >= dynamic_threshold and price_change < -price_threshold) or
                (volume_ratio < 0.8 and price_change > price_threshold)):
            return None

        direction = "BUY" if volume_ratio > 1.5 else "SELL"
        context = {
            "pattern_type": "volume_price_divergence",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "dynamic_threshold": dynamic_threshold,
            "price_change": price_change,
            "price_threshold": price_threshold,
            "divergence_score": abs(price_change) / max(price_threshold, 0.1),
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("volume_price_divergence", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for volume_price_divergence {symbol}: {dispatch_error}")

        if confidence is None:
            confidence = PatternMathematics.calculate_volume_confidence(volume_ratio, dynamic_threshold, 0.6)

        if confidence < 0.4:
            return None

        return self._create_pattern_base(
            pattern_type="volume_price_divergence",
            confidence=confidence,
            symbol=symbol,
            description=f"Volume-price divergence: {volume_ratio:.1f}x vol, {price_change:.2f}% price",
            signal=direction,
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change=price_change,
            pattern_category="volume"
        )

    def detect_upside_momentum(self, indicators: Dict) -> Dict:
        """Upside momentum with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        rsi = indicators.get('rsi', 50)
        last_price = indicators.get('last_price', 0.0)
        
        dynamic_threshold = self._get_dynamic_threshold("upside_momentum")
        
        # VIX-aware RSI thresholds
        rsi_overbought = PatternMathematics.get_vix_aware_rsi_thresholds(self.vix_regime, "overbought")
        
        if not (price_change > self.MOMENTUM_THRESHOLD and volume_ratio >= dynamic_threshold and rsi < rsi_overbought):
            return None

        context = {
            "pattern_type": "upside_momentum",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "price_change": price_change,
            "momentum_strength": price_change,
            "momentum_threshold": self.MOMENTUM_THRESHOLD,
            "rsi": rsi,
            "last_price": last_price,
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("upside_momentum", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for upside_momentum {symbol}: {dispatch_error}")

        if confidence is None:
            confidence = PatternMathematics.calculate_price_confidence(price_change, self.MOMENTUM_THRESHOLD, 0.7)

        if confidence < 0.4:
            return None

        return self._create_pattern_base(
            pattern_type="upside_momentum",
            confidence=confidence,
            symbol=symbol,
            description=f"Upside momentum: RSI {rsi:.1f} with {volume_ratio:.1f}x volume",
            signal="BUY",
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change=price_change,
            rsi=rsi,
            pattern_category="momentum",
        )

    def detect_downside_momentum(self, indicators: Dict) -> Dict:
        """Downside momentum with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        rsi = indicators.get('rsi', 50)
        last_price = indicators.get('last_price', 0.0)
        
        dynamic_threshold = self._get_dynamic_threshold("downside_momentum")
        
        # VIX-aware RSI thresholds
        rsi_oversold = PatternMathematics.get_vix_aware_rsi_thresholds(self.vix_regime, "oversold")
        
        if not (price_change < -self.MOMENTUM_THRESHOLD and volume_ratio >= dynamic_threshold and rsi > rsi_oversold):
            return None

        context = {
            "pattern_type": "downside_momentum",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "price_change": price_change,
            "momentum_strength": abs(price_change),
            "momentum_threshold": self.MOMENTUM_THRESHOLD,
            "rsi": rsi,
            "last_price": last_price,
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("downside_momentum", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for downside_momentum {symbol}: {dispatch_error}")

        if confidence is None:
            confidence = PatternMathematics.calculate_price_confidence(abs(price_change), self.MOMENTUM_THRESHOLD, 0.7)

        if confidence < 0.4:
            return None

        return self._create_pattern_base(
            pattern_type="downside_momentum",
            confidence=confidence,
            symbol=symbol,
            description=f"Downside momentum: RSI {rsi:.1f} with {volume_ratio:.1f}x volume",
            signal="SELL",
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change=price_change,
            rsi=rsi,
            pattern_category="momentum",
        )

    @time_pattern_method
    def detect_breakout(self, indicators: Dict) -> Dict:
        """Breakout with MATHEMATICAL INTEGRITY and VOLUME PROFILE VALIDATION"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        rsi = indicators.get('rsi', 50)
        last_price = indicators.get('last_price', 0.0)
        
        dynamic_threshold = self._get_dynamic_threshold("breakout")
        
        # VIX-aware RSI thresholds
        rsi_overbought = PatternMathematics.get_vix_aware_rsi_thresholds(self.vix_regime, "overbought")
        
        if not (price_change > self.BREAKOUT_THRESHOLD and volume_ratio >= dynamic_threshold and rsi < rsi_overbought):
            return None

        volume_profile_context = {'volume_profile_validated': False}
        volume_nodes = self.volume_state_manager.get_volume_profile_data(symbol)
        if volume_nodes:
            temp_pattern = {
                'price': last_price,
                'direction': 'BULLISH',
                'pattern': 'breakout'
            }
            if not self.validate_volume_profile_alignment(temp_pattern, volume_nodes):
                self.logger.debug(f"Breakout pattern for {symbol} failed volume profile validation")
                return None
            volume_profile_context.update({
                'poc_price': volume_nodes.get('poc_price', 0),
                'value_area_high': volume_nodes.get('value_area_high', 0),
                'value_area_low': volume_nodes.get('value_area_low', 0),
                'profile_strength': volume_nodes.get('profile_strength', 0),
                'volume_profile_validated': True
            })

        context = {
            "pattern_type": "breakout",
            "symbol": symbol,
            "volume_ratio": volume_ratio,
            "dynamic_threshold": dynamic_threshold,
            "price_change": price_change,
            "price_threshold": self.BREAKOUT_THRESHOLD,
            "rsi": rsi,
            "last_price": last_price,
            "volume_profile_validated": volume_profile_context['volume_profile_validated'],
        }

        confidence = None
        if hasattr(self, "math_dispatcher") and self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence("breakout", context)
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher confidence error for breakout {symbol}: {dispatch_error}")

        if confidence is None:
            confidence = PatternMathematics.calculate_price_confidence(price_change, self.BREAKOUT_THRESHOLD, 0.8)
            if volume_profile_context.get('volume_profile_validated'):
                confidence = min(0.95, confidence + 0.05)

        if confidence < 0.45:
            return None

        return self._create_pattern_base(
            pattern_type="breakout",
            confidence=confidence,
            symbol=symbol,
            description=f"Breakout: RSI {rsi:.1f} with {volume_ratio:.1f}x volume" +
                        (" + VP validated" if volume_profile_context.get('volume_profile_validated') else ""),
            signal="BUY",
            last_price=last_price,
            volume_ratio=volume_ratio,
            price_change=price_change,
            rsi=rsi,
            pattern_category="breakout",
            **volume_profile_context
        )

    def detect_reversal(self, indicators: Dict) -> Dict:
        """Reversal with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        price_change = indicators.get('price_change', 0)
        last_price = indicators.get('last_price', 0.0)
        
        if len(self.price_history.get(symbol, [])) >= 5:
            recent_prices = list(self.price_history[symbol])[-5:]
            
            if len(recent_prices) >= 3:
                first_half = recent_prices[:3]
                second_half = recent_prices[-3:]
                
                first_trend = (first_half[-1] - first_half[0]) / first_half[0] if first_half[0] > 0 else 0
                second_trend = (second_half[-1] - second_half[0]) / second_half[0] if second_half[0] > 0 else 0
                
                # VIX-aware reversal thresholds
                vix_multiplier = 1.4 if self.vix_regime == "HIGH" else 0.8 if self.vix_regime == "LOW" else 1.0
                trend_threshold = 0.02 * vix_multiplier
                reversal_threshold = 0.01 * vix_multiplier
                
                dynamic_reversal_threshold = self._get_dynamic_threshold("reversal")
                
                if ((first_trend > trend_threshold and second_trend < -reversal_threshold) or
                    (first_trend < -trend_threshold and second_trend > reversal_threshold)) and \
                   volume_ratio >= dynamic_reversal_threshold:
                    
                    direction = "BUY" if second_trend > 0 else "SELL"
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "price_change_pct": price_change,
                        "first_trend": first_trend,
                        "second_trend": second_trend,
                        "trend_threshold": trend_threshold,
                        "reversal_threshold": reversal_threshold,
                        "dynamic_threshold": dynamic_reversal_threshold,
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "reversal",
                        math_context,
                        lambda: PatternMathematics.calculate_trend_confidence(abs(second_trend), trend_threshold, 0.6)
                    )
                    
                    return self._create_pattern_base(
                        pattern_type="reversal",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"Reversal: {first_trend:.2f} -> {second_trend:.2f} with {volume_ratio:.1f}x volume",
                        signal=direction,
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        price_change=price_change,
                        first_trend=first_trend,
                        second_trend=second_trend,
                        pattern_category="breakout",
                        math_context=math_context
                    )
        return None

    def detect_hidden_accumulation(self, indicators: Dict) -> Dict:
        """Hidden accumulation with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        rsi = indicators.get('rsi', 50)
        last_price = indicators.get('last_price', 0.0)
        
        dynamic_threshold = self._get_dynamic_threshold("hidden_accumulation")
        
        # VIX-aware RSI thresholds for neutral zone
        rsi_oversold, rsi_overbought = PatternMathematics.get_vix_aware_rsi_thresholds(self.vix_regime, "neutral")
        
        if (volume_ratio >= dynamic_threshold and
            rsi > rsi_oversold and rsi < rsi_overbought and  # Dynamic RSI neutral zone
            volume_ratio < 2.0):  # Not a spike, but steady accumulation
            
            math_context = {
                "symbol": symbol,
                "volume_ratio": volume_ratio,
                "dynamic_threshold": dynamic_threshold,
                "rsi": rsi,
                "rsi_band_low": rsi_oversold,
                "rsi_band_high": rsi_overbought,
                "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
            }
            confidence = self._resolve_confidence(
                "hidden_accumulation",
                math_context,
                lambda: PatternMathematics.calculate_volume_confidence(volume_ratio, dynamic_threshold, 0.6)
            )
            
            return self._create_pattern_base(
                pattern_type="hidden_accumulation",
                confidence=confidence,
                symbol=symbol,
                description=f"Hidden accumulation: RSI {rsi:.1f} with {volume_ratio:.1f}x volume",
                signal="BUY",
                last_price=last_price,
                volume_ratio=volume_ratio,
                rsi=rsi,
                pattern_category="microstructure",
                math_context=math_context
            )
        return None

    # ============================================================================
    # VOLUME PROFILE PATTERN DETECTION METHODS
    # ============================================================================
    
    def detect_volume_profile_breakout(self, indicators: Dict) -> Dict:
        """Volume Profile Breakout Detection with MATHEMATICAL INTEGRITY"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        current_price = indicators.get('last_price', 0.0)
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        
        # Get volume profile data
        if not self.volume_state_manager:
            return None
            
        volume_nodes = self.volume_state_manager.get_volume_profile_data(symbol)
        if not volume_nodes:
            return None
        
        poc_price = volume_nodes.get('poc_price', 0)
        value_area_high = volume_nodes.get('value_area_high', 0)
        value_area_low = volume_nodes.get('value_area_low', 0)
        support_levels = volume_nodes.get('support_levels', [])
        resistance_levels = volume_nodes.get('resistance_levels', [])
        
        # Volume Profile Breakout Logic
        breakout_signal = self._analyze_volume_profile_breakout(
            symbol, current_price, volume_ratio, poc_price, value_area_high, value_area_low,
            support_levels, resistance_levels
        )
        
        if breakout_signal:
            math_context = breakout_signal.get('math_context') if isinstance(breakout_signal, dict) else None
            return self._create_pattern_base(
                pattern_type="volume_profile_breakout",
                confidence=breakout_signal['confidence'],
                symbol=symbol,
                description=f"Volume Profile Breakout: {breakout_signal['direction']} with {volume_ratio:.1f}x volume",
                signal="BUY" if breakout_signal['direction'] == 'bullish' else "SELL",
                last_price=current_price,
                volume_ratio=volume_ratio,
                poc_price=poc_price,
                value_area_high=value_area_high,
                value_area_low=value_area_low,
                breakout_direction=breakout_signal['direction'],
                volume_profile_strength=volume_nodes.get('profile_strength', 0),
                pattern_category="volume_profile",
                math_context=math_context
            )
        return None
    
    def detect_volume_node_test(self, indicators: Dict) -> Dict:
        """Detect price testing key volume nodes"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        current_price = indicators.get('last_price', 0.0)
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        
        if not self.volume_state_manager:
            return None
            
        volume_nodes = self.volume_state_manager.get_volume_profile_data(symbol)
        if not volume_nodes:
            return None
        
        support_levels = volume_nodes.get('support_levels', [])
        resistance_levels = volume_nodes.get('resistance_levels', [])
        
        # Test support levels
        for support in support_levels:
            if abs(current_price - support) / support < 0.002:  # Within 0.2%
                if volume_ratio > 1.5:
                    # Support holding with volume
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "price_level": support,
                        "price_position": "support_hold",
                        "profile_strength": volume_nodes.get('profile_strength', 0),
                    }
                    confidence = self._resolve_confidence(
                        "volume_node_support_hold",
                        math_context,
                        lambda: PatternMathematics.calculate_bounded_confidence(
                            min(volume_ratio / 2.0, 0.8), 0.1
                        )
                    )
                    return self._create_pattern_base(
                        pattern_type="volume_node_support_hold",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"Volume Node Support Hold: {volume_ratio:.1f}x volume at {support:.2f}",
                        signal="BUY",
                        last_price=current_price,
                        volume_ratio=volume_ratio,
                        support_level=support,
                        direction="bullish",
                        pattern_category="volume_profile",
                        math_context=math_context
                    )
        
        # Test resistance levels  
        for resistance in resistance_levels:
            if abs(current_price - resistance) / resistance < 0.002:
                if volume_ratio > 1.5:
                    # Resistance holding with volume
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "price_level": resistance,
                        "price_position": "resistance_hold",
                        "profile_strength": volume_nodes.get('profile_strength', 0),
                    }
                    confidence = self._resolve_confidence(
                        "volume_node_resistance_hold",
                        math_context,
                        lambda: PatternMathematics.calculate_bounded_confidence(
                            min(volume_ratio / 2.0, 0.8), 0.1
                        )
                    )
                    return self._create_pattern_base(
                        pattern_type="volume_node_resistance_hold", 
                        confidence=confidence,
                        symbol=symbol,
                        description=f"Volume Node Resistance Hold: {volume_ratio:.1f}x volume at {resistance:.2f}",
                        signal="SELL",
                        last_price=current_price,
                        volume_ratio=volume_ratio,
                        resistance_level=resistance,
                        direction="bearish",
                        pattern_category="volume_profile",
                        math_context=math_context
                    )
        
        return None
    
    def _analyze_volume_profile_breakout(self, symbol: str, current_price: float, volume_ratio: float,
                                       poc: float, vah: float, val: float,
                                       support: List, resistance: List) -> Optional[Dict]:
        """Analyze breakout using volume profile levels"""
        
        # Breakout above Value Area High with volume confirmation
        if current_price > vah and volume_ratio > 1.8:
            math_context = {
                "symbol": symbol,
                "volume_ratio": volume_ratio,
                "price_position": "above_vah",
                "vah": vah,
                "val": val,
                "poc": poc,
                "profile_strength": volume_ratio / max(vah or 1.0, 1.0),
            }
            confidence = self._resolve_confidence(
                "volume_profile_breakout",
                math_context,
                lambda: PatternMathematics.calculate_bounded_confidence(
                    min(volume_ratio / 3.0, 0.9), 0.1
                )
            )
            return {'direction': 'bullish', 'confidence': confidence, 'math_context': math_context}
        
        # Breakdown below Value Area Low with volume confirmation  
        elif current_price < val and volume_ratio > 1.8:
            math_context = {
                "symbol": symbol,
                "volume_ratio": volume_ratio,
                "price_position": "below_val",
                "vah": vah,
                "val": val,
                "poc": poc,
                "profile_strength": volume_ratio / max(val or 1.0, 1.0),
            }
            confidence = self._resolve_confidence(
                "volume_profile_breakout",
                math_context,
                lambda: PatternMathematics.calculate_bounded_confidence(
                    min(volume_ratio / 3.0, 0.9), 0.1
                )
            )
            return {'direction': 'bearish', 'confidence': confidence, 'math_context': math_context}
        
        # POC Rejection - Price approaches POC and reverses with volume
        poc_distance = abs(current_price - poc) / poc
        if poc_distance < 0.005 and volume_ratio > 2.0:  # Within 0.5% of POC
            # Check for rejection (price moving away from POC)
            price_trend = self._get_price_trend()
            if (current_price > poc and price_trend < 0) or (current_price < poc and price_trend > 0):
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "price_position": "poc_rejection",
                    "poc": poc,
                    "price_trend": price_trend,
                    "profile_strength": volume_ratio / max(poc or 1.0, 1.0),
                }
                confidence = self._resolve_confidence(
                    "volume_profile_breakout",
                    math_context,
                    lambda: PatternMathematics.calculate_bounded_confidence(
                        min(volume_ratio / 2.5, 0.8), 0.15
                    )
                )
                direction = 'bearish' if current_price > poc else 'bullish'
                return {'direction': direction, 'confidence': confidence, 'math_context': math_context}
        
        return None
    
    def _get_price_trend(self) -> float:
        """Simple price trend calculation"""
        # Implement based on your existing price history
        # For now, return 0.0 as placeholder - can be enhanced with actual price history
        return 0.0  # Placeholder

    def _detect_volume_profile_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect Volume Profile patterns with MATHEMATICAL INTEGRITY"""
        patterns = []
        
        try:
            # Volume Profile Breakout Detection
            breakout_pattern = self.detect_volume_profile_breakout(indicators)
            if breakout_pattern:
                patterns.append(breakout_pattern)
            
            # Volume Node Test Detection
            node_test_pattern = self.detect_volume_node_test(indicators)
            if node_test_pattern:
                patterns.append(node_test_pattern)
                
        except Exception as e:
            self.logger.error(f"Volume profile pattern detection error: {e}")
        
        return patterns

    def _detect_advanced_volume_profile_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect advanced volume profile patterns using VolumeProfileDetector"""
        patterns = []
        
        try:
            if not self.volume_profile_detector or not self.volume_state_manager:
                return patterns
            
            symbol = indicators.get('symbol', 'UNKNOWN')
            
            # Get current volume profile data
            current_profile = self.volume_state_manager.get_volume_profile_data(symbol)
            if not current_profile:
                return patterns
            
            # Get historical profile data for trend analysis
            historical_data = self._get_historical_volume_profile_data(symbol)
            
            # Detect patterns using VolumeProfileDetector
            detected_patterns = self.volume_profile_detector.detect_patterns(
                symbol=symbol,
                current_profile=current_profile,
                historical_data=historical_data
            )
            
            # Convert VolumeProfileDetector patterns to PatternDetector format
            for pattern in detected_patterns:
                # Map field names to match PatternDetector expectations
                mapped_pattern = {
                    'pattern': pattern.get('pattern_type', 'unknown'),
                    'confidence': pattern.get('confidence', 0.0),
                    'symbol': symbol,
                    'last_price': indicators.get('last_price', 0.0),
                    'volume_ratio': indicators.get('volume_ratio', 1.0),
                    'price_change': indicators.get('price_change', 0.0),
                    'pattern_category': 'volume_profile_advanced',
                    'detection_time': pattern.get('detection_time', datetime.now().isoformat()),
                    'volume_profile_data': {
                        'price_level': pattern.get('price_level', 0.0),
                        'volume': pattern.get('volume', 0),
                        'gap_volume': pattern.get('gap_volume', 0),
                        'current_poc': pattern.get('current_poc', 0.0),
                        'previous_poc': pattern.get('previous_poc', 0.0),
                        'change_percent': pattern.get('change_percent', 0.0),
                        'strength_score': pattern.get('strength_score', 0.0),
                        'total_volume': pattern.get('total_volume', 0)
                    }
                }
                
                patterns.append(mapped_pattern)
            
        except Exception as e:
            self.logger.error(f"Error detecting advanced volume profile patterns: {e}")
        
        return patterns
    
    def _get_historical_volume_profile_data(self, symbol: str, lookback_hours: int = 24) -> List[Dict]:
        """Get historical volume profile data for trend analysis"""
        try:
            historical_data = []
            
            # Get historical data from Redis (DB 2 analytics for volume profiles)
            historical_key = f"volume_profile:historical:{symbol}"
            historical_data_raw = self.redis_client.get_client(2).lrange(historical_key, 0, lookback_hours - 1)
            
            for data_point in historical_data_raw:
                try:
                    profile_data = json.loads(data_point)
                    historical_data.append(profile_data)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            return historical_data
            
        except Exception as e:
            self.logger.error(f"Error getting historical volume profile data for {symbol}: {e}")
            return []
    
    def _initialize_risk_manager(self):
        if not RISK_MANAGER_AVAILABLE:
            logger.warning("❌ Risk Manager not available - using emergency fallback")
            return self._create_emergency_risk_manager()
            
        try:
            risk_mgr = RiskManager(redis_client=self.redis_client)
            
            # Validate Risk Manager is functional
            test_metrics = risk_mgr.calculate_risk_metrics({
                'symbol': 'TEST',
                'pattern_type': 'volume_spike',
                'confidence': 0.7
            })
            
            if test_metrics and 'expected_move' in test_metrics:
                logger.info("✅ Risk Manager successfully integrated and validated")
                return risk_mgr
            else:
                raise ValueError("Risk Manager returned invalid test metrics")
                
        except Exception as e:
            logger.error(f"❌ CRITICAL: Risk Manager integration failed: {e}")
            # Create emergency fallback
            return self._create_emergency_risk_manager()

    def _create_emergency_risk_manager(self):
        """Emergency fallback when Risk Manager fails"""
        logger.warning("🚨 USING EMERGENCY RISK MANAGER - FIX PRODUCTION RISK MANAGER")
        
        class EmergencyRiskManager:
            def calculate_risk_metrics(self, pattern_data):
                return {
                    'expected_move': 1.0,  # Conservative default
                    'position_size': 1,
                    'stop_loss': 0.0,
                    'risk_level': 'HIGH'  # Flag for manual review
                }
        
        return EmergencyRiskManager()

    def _get_incremental_volume(self, indicators: Dict[str, Any], default: float = 0.0) -> float:
        """SINGLE SOURCE OF TRUTH: Use canonical field names from optimized_field_mapping.yaml"""
        # Try canonical field first, then fallback
        return _session_value(indicators, "zerodha_last_traded_quantity", 
                            _session_value(indicators, "bucket_incremental_volume", default))

    def _get_cumulative_volume(self, indicators: Dict[str, Any], default: float = 0.0) -> float:
        """SINGLE SOURCE OF TRUTH: Use canonical field names from optimized_field_mapping.yaml"""
        # Try canonical field first, then fallback
        return _session_value(indicators, "zerodha_cumulative_volume",
                            _session_value(indicators, "bucket_cumulative_volume", default))

    def _get_zerodha_cumulative_volume(self, indicators: Dict[str, Any], default: float = 0.0) -> float:
        return _session_value(indicators, "zerodha_cumulative_volume", default)

    def _local_create_pattern(
        self,
        symbol: str,
        pattern_name: str,
        signal: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Fallback pattern builder used when schema helpers are unavailable."""
        timestamp = kwargs.pop("timestamp", time.time())
        timestamp_ms = kwargs.pop("timestamp_ms", None)
        confidence = float(kwargs.pop("confidence", 0.0) or 0.0)

        pattern: Dict[str, Any] = {
            "symbol": symbol,
            "pattern": pattern_name,
            "pattern_type": kwargs.pop("pattern_type", pattern_name),
            "signal": str(signal).upper(),
            "confidence": max(0.0, min(1.0, confidence)),
            "timestamp": float(timestamp),
            "source": "pattern_detector",
        }

        if timestamp_ms is not None:
            try:
                pattern["timestamp_ms"] = float(timestamp_ms)
            except (TypeError, ValueError):
                pass

        numeric_fields = (
            "last_price",
            "price_change",
            "bucket_incremental_volume",
            "volume_ratio",
            "bucket_incremental_volume",
            "bucket_cumulative_volume",
            "expected_move",
            "target_price",
            "stop_loss",
            "risk_reward",
            "vix_level",
        )

        for field in numeric_fields:
            if field in kwargs and kwargs[field] is not None:
                value = kwargs.pop(field)
                try:
                    pattern[field] = float(value)
                except (TypeError, ValueError):
                    pattern[field] = value

        description = kwargs.pop("description", None)
        if description:
            pattern["description"] = description

        details = kwargs.pop("details", None)
        if isinstance(details, dict) and details:
            pattern["details"] = details

        for key, value in list(kwargs.items()):
            if value is None:
                continue
            pattern[key] = value

        return pattern

    def _ensure_numeric_types(self, indicators):
        """Ensure all numeric indicators are proper float types"""
        safe_indicators = {}
        for key, value in indicators.items():
            if value is not None:
                try:
                    if isinstance(value, (int, float)):
                        safe_indicators[key] = float(value)
                    elif isinstance(value, str):
                        # Try to convert string to float
                        safe_indicators[key] = float(value)
                    else:
                        safe_indicators[key] = value
                except (TypeError, ValueError):
                    # Keep original value if conversion fails
                    safe_indicators[key] = value
            else:
                safe_indicators[key] = value
        return safe_indicators

        # Stats for monitoring
        self.stats = {
            "invocations": 0,
            "patterns_found": 0,
            "missing_indicator_calls": 0,
            "errors": 0,
        }
        # detection_stats already initialized above

        # VIX data will be read directly from Redis (published every 30 seconds)

        # quality_stats already initialized above

        # Additional counters and recent metrics
        self.recent_volume_ratios = deque(maxlen=200)
        self.regime_rejections = 0

        logger.info("✅ Consolidated Pattern Detector initialized")
        # Initialize ICT detector stack if modules are available
        if ICT_MODULES_AVAILABLE:
            try:
                self.ict_detector = ICTPatternDetector(redis_client)
                self.ict_liquidity_detector = ICTLiquidityDetector(redis_client)
                self.ict_fvg_detector = ICTFVGDetector()
                self.ict_ote_calculator = ICTOTECalculator()
                self.ict_premium_discount_detector = ICTPremiumDiscountDetector(redis_client)
                self.ict_killzone_detector = ICTKillzoneDetector()
                self.logger.info("✅ ICT detectors initialized")
            except Exception as ict_error:
                self.logger.warning(f"Failed to initialize ICT detectors: {ict_error}")
                self.ict_detector = None
                self.ict_liquidity_detector = None
                self.ict_fvg_detector = None
                self.ict_ote_calculator = None
                self.ict_premium_discount_detector = None
                self.ict_killzone_detector = None
        else:
            self.logger.info("ICT modules unavailable - ICT patterns disabled")
            self.ict_detector = None
            self.ict_liquidity_detector = None
            self.ict_fvg_detector = None
            self.ict_ote_calculator = None
            self.ict_premium_discount_detector = None
            self.ict_killzone_detector = None

        # Expected Move calculator (optional) - not available
        self.expected_move_calculator = None
        
        # FORCE Risk Manager integration
        self.risk_manager = self._initialize_risk_manager()
        self.math_dispatcher = MathDispatcher(PatternMathematics, self.risk_manager)
        
        # Unified VIX regime manager (optional) - not available
        self.vix_regime_manager = None
        
        # Schema helpers (optional) - use local implementation
        self._create_pattern = self._local_create_pattern
        self._validate_pattern = None
        # MM detectors removed - using 8 core patterns only
        logger.info("✅ Simplified pattern detector initialized")
        # Removed ICT counters - using 8 core patterns only
        # Pattern detection stats (8 core patterns only)
        self.patterns_detected = 0

    def calculate_correct_volume_ratio(self, symbol, indicators=None):
        """Calculate the correct bucket_incremental_volume ratio using the same method as alert manager."""
        try:
            # Get current volume from indicators or Redis
            if indicators:
                current_volume = indicators.get('bucket_incremental_volume', 0.0)
            else:
                # Try to get from Redis
                redis_key = f"volume_ratio:{symbol}:latest"
                stored_data = self.redis_client.get(redis_key) if self.redis_client else None
                if stored_data:
                    try:
                        data = json.loads(stored_data) if isinstance(stored_data, str) else stored_data
                        current_volume = data.get('bucket_incremental_volume', 0.0)
                    except:
                        current_volume = 0.0
                else:
                    current_volume = 0.0
            
            # Use centralized volume ratio from VolumeResolver (single source of truth)
            volume_ratio = VolumeResolver.get_volume_ratio({"bucket_incremental_volume": current_volume})
            
            return volume_ratio if volume_ratio is not None else 0.0
        except Exception as e:
            self.logger.error(f"Failed to calculate correct bucket_incremental_volume ratio for {symbol}: {e}")
            return 0.0

    def update_history(self, symbol: str, indicators: Dict[str, Any]) -> None:
        """Maintain rolling history for pattern detection with Redis persistence"""
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=50)
            self.high_history[symbol] = deque(maxlen=50)
            self.low_history[symbol] = deque(maxlen=50)
            self.volume_history[symbol] = deque(maxlen=50)
        
        # Keep last 50 periods
        max_history = 50
        last_price = indicators.get('last_price', 0)
        current_high = indicators.get('high', last_price)
        current_low = indicators.get('low', last_price)
        current_volume = indicators.get('bucket_incremental_volume', 0)
        
        # Update in-memory history
        self.price_history[symbol].append(last_price)
        self.high_history[symbol].append(current_high)
        self.low_history[symbol].append(current_low)
        self.volume_history[symbol].append(current_volume)
        
        # Trim to max_history
        for history in [self.price_history, self.high_history, self.low_history, self.volume_history]:
            if len(history[symbol]) > max_history:
                history[symbol] = history[symbol][-max_history:]
        
        # Store in Redis for persistence and cross-process access
        redis_conn = None
        if self.redis_client:
            redis_conn = getattr(self.redis_client, "redis", None)
            if redis_conn is None and hasattr(self.redis_client, "redis_client"):
                redis_conn = self.redis_client.redis_client

        if redis_conn:
            try:
                # Store last_price history in Redis
                price_key = f"pattern_history:last_price:{symbol}"
                redis_conn.lpush(price_key, last_price)
                redis_conn.ltrim(price_key, 0, max_history - 1)
                redis_conn.expire(price_key, 3600)  # 1 hour TTL
                
                # Store high history in Redis
                high_key = f"pattern_history:high:{symbol}"
                redis_conn.lpush(high_key, current_high)
                redis_conn.ltrim(high_key, 0, max_history - 1)
                redis_conn.expire(high_key, 3600)
                
                # Store low history in Redis
                low_key = f"pattern_history:low:{symbol}"
                redis_conn.lpush(low_key, current_low)
                redis_conn.ltrim(low_key, 0, max_history - 1)
                redis_conn.expire(low_key, 3600)
                
                # Store bucket_incremental_volume history in Redis
                volume_key = f"pattern_history:bucket_incremental_volume:bucket_incremental_volume:bucket:{symbol}"
                redis_conn.lpush(volume_key, current_volume)
                redis_conn.ltrim(volume_key, 0, max_history - 1)
                redis_conn.expire(volume_key, 3600)
                
            except Exception as e:
                logger.debug(f"Redis history storage failed for {symbol}: {e}")

    def validate_data_quality(self, symbol: str, prices: List[float], volumes: List[float]) -> Dict[str, Any]:
        """Validate data quality before running pattern detection."""
        quality_score = 100
        issues: List[str] = []

        price_count = len(prices)
        if price_count < 15:
            quality_score -= 40
            issues.append(f"Insufficient data points: {price_count}")

        valid_prices = [p for p in prices if isinstance(p, (int, float)) and p > 0]
        if len(valid_prices) != price_count:
            quality_score -= 20
            issues.append(f"Invalid prices: {price_count - len(valid_prices)}")

        if len(valid_prices) >= 3:
            price_changes = []
            for i in range(1, len(valid_prices)):
                prev = valid_prices[i - 1]
                curr = valid_prices[i]
                if prev != 0:
                    price_changes.append(abs((curr - prev) / prev))
            if price_changes:
                avg_change = sum(price_changes) / len(price_changes)
                if 0.0190 <= avg_change <= 0.0193:
                    quality_score = 0
                    issues.append("SYNTHETIC_DATA_DETECTED: Identical ~1.919% moves")

        if volumes:
            valid_volumes = [v for v in volumes if isinstance(v, (int, float)) and v >= 0]
            if len(valid_volumes) != len(volumes):
                quality_score -= 10
                issues.append("Invalid bucket_incremental_volume entries detected")
            if sum(valid_volumes) == 0:
                quality_score -= 20
                issues.append("Zero bucket_incremental_volume data")

        quality_score = max(0, quality_score)
        if quality_score >= 80:
            quality_level = "HIGH"
        elif quality_score >= 60:
            quality_level = "MEDIUM"
        elif quality_score >= 40:
            quality_level = "LOW"
        else:
            quality_level = "REJECT"

        return {
            "score": quality_score,
            "level": quality_level,
            "issues": issues,
            "valid": quality_score >= 60,
        }

    def load_history_from_redis(self, symbol: str) -> None:
        """Load historical data from Redis for pattern detection using standardized keys"""
        if not self.redis_client:
            return

        redis_conn = getattr(self.redis_client, "redis", None)
        if redis_conn is None and hasattr(self.redis_client, "redis_client"):
            redis_conn = self.redis_client.redis_client
        if not redis_conn:
            return

        try:
            # Use standardized Redis key patterns from optimized_field_mapping.yaml
            # Pattern: session:{symbol}:{date} for session data
            # Pattern: bucket_incremental_volume:{symbol}:{timestamp} for bucket_incremental_volume buckets
            
            # Load last_price history from session data
            session_key = f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = redis_conn.hgetall(session_key)
            if session_data:
                last_price = session_data.get('last_price')
                if last_price:
                    self.price_history[symbol] = deque([float(last_price)], maxlen=50)
            
            # Load high/low from session data
            if session_data:
                high = session_data.get('high')
                low = session_data.get('low')
                if high:
                    self.high_history[symbol] = deque([float(high)], maxlen=50)
                if low:
                    self.low_history[symbol] = deque([float(low)], maxlen=50)
            
            # Load bucket_incremental_volume from session data using canonical field names
            if session_data:
                zerodha_cumulative_volume = session_data.get('zerodha_cumulative_volume')
                if zerodha_cumulative_volume:
                    self.volume_history[symbol] = deque([float(zerodha_cumulative_volume)], maxlen=50)
                
        except Exception as e:
            logger.debug(f"Redis history loading failed for {symbol}: {e}")

    def _load_pattern_config(self) -> Dict[str, Any]:
        """Load pattern configuration from JSON file"""
        try:
            # Use absolute path to ensure we find the config file
            config_path = os.path.join(os.path.dirname(__file__), "data", "pattern_registry_config.json")
            with open(config_path, "r") as f:
                config = json.load(f)
                logger.info(f"✅ Loaded pattern config from {config_path}")
                return config
        except Exception as e:
            logger.warning(f"⚠️ Could not load pattern config: {e}")
            return {}

    def _load_thresholds(self) -> Dict[str, float]:
        """Load pattern-specific thresholds from config"""
        thresholds = {}
        try:
            pattern_configs = self.pattern_config.get("pattern_configs", {})
            for pattern_name, config in pattern_configs.items():
                # Skip boolean values and non-dict configs
                if isinstance(config, bool) or not isinstance(config, dict):
                    continue
                if "base_threshold" in config:
                    thresholds[pattern_name] = config["base_threshold"]

            # Set default thresholds if config loading fails
            if not thresholds:
                thresholds = {
                    "volume_spike": 2.2,  # Updated to match config/thresholds.py
                    "volume_breakout": 1.8,  # Updated to match config/thresholds.py
                    "volume_price_divergence": 1.5,  # Updated to match config/thresholds.py
                    "upside_momentum": 0.5,  # 0.5% for momentum
                    "downside_momentum": -0.5,  # -0.5% for momentum
                    "breakout": 0.3,  # 0.3% for breakout
                    "reversal": 0.4,  # 0.4% for reversal
                    "hidden_accumulation": 1.3,
                }

            logger.info(f"✅ Loaded {len(thresholds)} pattern thresholds from config")
        except Exception as e:
            logger.warning(f"⚠️ Could not load thresholds: {e}")
            # Fallback to reasonable defaults (updated to match config/thresholds.py)
            thresholds = {
                "volume_spike": 2.2,  # Updated to match config/thresholds.py
                "volume_breakout": 1.8,  # Updated to match config/thresholds.py
                "volume_price_divergence": 1.5,  # Updated to match config/thresholds.py
                "upside_momentum": 0.5,  # 0.5% for momentum
                "downside_momentum": -0.5,  # -0.5% for momentum
                "breakout": 0.3,  # 0.3% for breakout
                "reversal": 0.4,  # 0.4% for reversal
                "hidden_accumulation": 1.3,
            }

        return thresholds

    def _load_volume_baselines(self) -> Dict[str, Dict[str, float]]:
        """Load dynamic 20d/55d bucket_incremental_volume baselines from volume_averages_20d.json"""
        baselines = {}
        try:
            volume_file = (
                Path(__file__).parent.parent
                / "config"
                / "volume_averages_20d.json"
            )
            if volume_file.exists():
                with open(volume_file, "r") as f:
                    data = json.load(f)

                for symbol_key, symbol_data in data.items():
                    # Extract clean symbol (e.g., NSE:RELIANCE -> RELIANCE)
                    clean_symbol = symbol_key.split(":", 1)[-1]
                    baselines[clean_symbol] = {
                        "avg_volume_20d": symbol_data.get("avg_volume_20d", 0),
                        "avg_volume_55d": symbol_data.get("avg_volume_55d", 0),
                        "avg_price_20d": symbol_data.get("avg_price_20d", 0),
                        "avg_price_55d": symbol_data.get("avg_price_55d", 0),
                        "instrument_type": symbol_data.get("instrument_type", "EQUITY"),
                        "turtle_ready": symbol_data.get("turtle_ready", False),
                    }

                logger.info(
                    f"✅ Loaded dynamic bucket_incremental_volume baselines for {len(baselines)} instruments"
                )
            else:
                logger.warning(f"⚠️ Volume baselines file not found: {volume_file}")
        except Exception as e:
            logger.error(f"❌ Error loading bucket_incremental_volume baselines: {e}")

        return baselines

    def _load_correlation_groups(self) -> Dict[str, List[str]]:
        """Load correlation groups to avoid overexposure to correlated instruments"""
        return {
            "banks": [
                "HDFCBANK",
                "ICICIBANK",
                "KOTAKBANK",
                "AXISBANK",
                "SBIN",
                "BANKBARODA",
                "PNB",
                "INDUSINDBK",
            ],
            "it": [
                "TCS",
                "INFY",
                "HCLTECH",
                "TECHM",
                "WIPRO",
                "LTIM",
                "MPHASIS",
                "COFORGE",
            ],
            "energy": ["RELIANCE", "ONGC", "IOC", "BPCL", "HINDPETRO", "OIL"],
            "auto": [
                "TATAMOTORS",
                "M&M",
                "MARUTI",
                "BAJAJ-AUTO",
                "EICHERMOT",
                "HEROMOTOCO",
                "TVSMOTOR",
            ],
            "pharma": [
                "SUNPHARMA",
                "DRREDDY",
                "CIPLA",
                "LUPIN",
                "DIVISLAB",
                "BIOCON",
                "AUROPHARMA",
            ],
            "metals": [
                "TATASTEEL",
                "JSWSTEEL",
                "HINDALCO",
                "VEDL",
                "SAIL",
                "JINDALSTEL",
                "HINDZINC",
            ],
            "realty": ["DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "LODHA"],
            "fmcg": [
                "HINDUNILVR",
                "ITC",
                "BRITANNIA",
                "DABUR",
                "MARICO",
                "GODREJCP",
                "TATACONSUM",
            ],
        }

    def _get_instrument_quality_score(
        self, symbol: str, indicators: Dict[str, Any]
    ) -> float:
        """
        Calculate instrument quality score based on:
        1. Liquidity (bucket_incremental_volume vs baseline)
        2. Volatility (ATR relative to historical)
        3. Trend clarity (less noise)

        Returns: Quality score 0.0 to 1.0 (higher = better quality)
        """
        quality_score = 0.0

        # Clean symbol for lookup
        clean_symbol = symbol.split(":", 1)[-1].upper()
        baseline_data = self.volume_baselines.get(clean_symbol, {})

        # 1. Liquidity score (30% weight)
        current_volume = float(indicators.get("bucket_incremental_volume", 0))
        avg_volume_20d = baseline_data.get("avg_volume_20d", 0)

        if avg_volume_20d > 0:
            volume_ratio = current_volume / avg_volume_20d
            # Score well if bucket_incremental_volume > 0.8x average (not dead), cap at 3x to avoid over-weighting spikes
            liquidity_score = min(1.0, max(0.0, (volume_ratio - 0.5) / 2.5))
            quality_score += liquidity_score * 0.3

        # 2. Volatility score (30% weight) - healthy ATR
        atr = float(indicators.get("atr", 0) or indicators.get("atr_14", 0))
        last_price = float(indicators.get("last_price", 0))
        avg_price_20d = baseline_data.get("avg_price_20d", 0)

        if atr > 0 and last_price > 0:
            atr_pct = (atr / last_price) * 100
            # Score well for ATR between 0.5% and 3% (not dead, not too chaotic)
            if 0.5 <= atr_pct <= 3.0:
                volatility_score = 1.0 - abs(atr_pct - 1.5) / 1.5  # Peak at 1.5%
                quality_score += max(0.0, volatility_score) * 0.3

        # 3. Trend clarity score (40% weight) - based on RSI and last_price action
        rsi = float(indicators.get("rsi", 50) or 50)
        price_change = float(indicators.get("price_change", 0) or 0)

        # Clear trends: RSI away from 50, with last_price action confirming
        rsi_clarity = abs(rsi - 50) / 50  # 0 to 1, higher = clearer trend

        # Price momentum alignment
        if avg_price_20d > 0 and last_price > 0:
            price_vs_avg = (last_price - avg_price_20d) / avg_price_20d
            momentum_score = min(1.0, abs(price_vs_avg) * 10)  # Scale to 0-1
            trend_clarity = (rsi_clarity * 0.6) + (momentum_score * 0.4)
            quality_score += trend_clarity * 0.4

        self.quality_stats["instruments_evaluated"] += 1

        return min(1.0, quality_score)

    def _check_correlation_limit(
        self, symbol: str, active_positions: List[str]
    ) -> bool:
        """
        Check if adding this symbol would exceed correlation limits.
        Max 2 positions per correlated group.

        Args:
            symbol: Symbol to check
            active_positions: List of currently active position symbols

        Returns: True if within limits, False if would exceed
        """
        clean_symbol = symbol.split(":", 1)[-1].upper()

        # Find which correlation group this symbol belongs to
        symbol_group = None
        for group_name, symbols in self.correlation_groups.items():
            if clean_symbol in symbols:
                symbol_group = group_name
                break

        if not symbol_group:
            # Symbol not in any correlation group, allow
            return True

        # Count how many positions we already have in this group
        group_count = 0
        for pos in active_positions:
            clean_pos = pos.split(":", 1)[-1].upper()
            if clean_pos in self.correlation_groups[symbol_group]:
                group_count += 1

        # Allow max 2 positions per correlation group
        if group_count >= 2:
            self.quality_stats["correlation_filtered"] += 1
            logger.debug(
                f"⚠️ Correlation limit: {clean_symbol} rejected (already have {group_count} positions in {symbol_group} group)"
            )
            return False

        return True

    def _apply_dynamic_volume_threshold(
        self, symbol: str, base_threshold: float
    ) -> float:
        """
        Apply dynamic bucket_incremental_volume threshold based on instrument's historical volatility
        and sector characteristics.

        Args:
            symbol: Symbol to check
            base_threshold: Base threshold multiplier (e.g., 2.5 for bucket_incremental_volume spike)

        Returns: Adjusted threshold
        """
        clean_symbol = symbol.split(":", 1)[-1].upper()
        baseline_data = self.volume_baselines.get(clean_symbol, {})

        # Ensure base_threshold is a float
        if isinstance(base_threshold, (list, tuple)):
            base_threshold = base_threshold[-1] if base_threshold else 2.5
        elif isinstance(base_threshold, str):
            if base_threshold == '_removed':
                base_threshold = 2.5
            else:
                try:
                    base_threshold = float(base_threshold)
                except (ValueError, TypeError):
                    base_threshold = 2.5
        
        # Start with base threshold
        adjusted_threshold = float(base_threshold)

        # Adjust based on instrument type
        instrument_type = baseline_data.get("instrument_type", "EQUITY")

        if instrument_type == "FUTIDX":
            # Index futures are more liquid, need higher thresholds
            adjusted_threshold *= 1.2
        elif instrument_type == "FUTCOM":
            # Commodities can have erratic bucket_incremental_volume, be more lenient
            adjusted_threshold *= 0.9

        # Adjust based on turtle_ready flag (quality instruments)
        if baseline_data.get("turtle_ready", False):
            # High quality instruments with good liquidity need higher confirmation
            adjusted_threshold *= 1.15

        # Adjust based on 55d vs 20d bucket_incremental_volume trends
        avg_vol_20d = baseline_data.get("avg_volume_20d", 0)
        avg_vol_55d = baseline_data.get("avg_volume_55d", 0)

        if avg_vol_20d > 0 and avg_vol_55d > 0:
            volume_trend = avg_vol_20d / avg_vol_55d
            if volume_trend > 1.3:
                # Rising bucket_incremental_volume trend - be slightly more lenient
                adjusted_threshold *= 0.95
            elif volume_trend < 0.7:
                # Falling bucket_incremental_volume trend - need more confirmation
                adjusted_threshold *= 1.1

        return adjusted_threshold

    def _get_dynamic_threshold(self, pattern_name: str, sector: str = None) -> float:
        """Get dynamic threshold from config/thresholds.py"""
        if not THRESHOLD_CONFIG_AVAILABLE:
            # Fallback to base threshold
            return self.base_thresholds.get(pattern_name, 1.0)
        
        try:
            # Get current VIX regime
            vix_regime = get_vix_regime()
            if vix_regime == "UNKNOWN":
                vix_regime = "NORMAL"
            
            # Use sector-aware threshold if sector provided
            if sector:
                return get_sector_threshold(sector, pattern_name, vix_regime)
            else:
                return get_volume_threshold(pattern_name, None, vix_regime)
        except Exception as e:
            logger.warning(f"⚠️ Could not get dynamic threshold for {pattern_name}: {e}")
            return self.base_thresholds.get(pattern_name, 1.0)

                
        try:
            logger.debug(
                f"✅ VIX-aware thresholds applied: {vix_regime} (VIX: {float(vix_val):.1f})"
            )
        except Exception:
            pass

    def _schemaize(
        self, raw: Dict[str, Any], indicators: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert a manually built pattern dict to schema-aligned structure.

        Falls back to the original dict if schema helpers are unavailable or creation fails.
        """
        if not getattr(self, "_create_pattern", None):
            return raw
        try:
            symbol = raw.get("symbol") or indicators.get("symbol") or "UNKNOWN"
            name = raw.get("pattern") or raw.get("pattern_type") or "unknown"
            signal = str(raw.get("signal", "NEUTRAL")).upper()
            conf = float(raw.get("confidence", 0) or 0)
            last_price = float(
                raw.get("last_price", indicators.get("last_price", 0)) or 0
            )
            price_change = float(indicators.get("price_change", 0) or 0)
            bucket_incremental_volume = float(indicators.get("bucket_incremental_volume", 0) or 0)
            volume_ratio = _indicator_value(indicators, "volume_ratio", 0.0)
            # SINGLE SOURCE OF TRUTH: Use canonical field names from optimized_field_mapping.yaml
            bucket_incremental_volume = _session_value(indicators, "bucket_incremental_volume", 0.0)
            bucket_cumulative_volume = _session_value(indicators, "zerodha_cumulative_volume", 0.0)
            zerodha_last_traded_quantity = _session_value(indicators, "zerodha_last_traded_quantity", 0.0)

            # VIX level from cache/regime if available
            vix_level = None
            try:
                if getattr(self, "vix_regime_manager", None):
                    regime = self.vix_regime_manager.get_current_regime()
                    vix_level = float(regime.get("vix_value", 0.0) or 0.0)
            except Exception:
                vix_level = None

            # Preserve additional context in details
            details = {}
            for k, v in raw.items():
                if k not in {
                    "symbol",
                    "pattern",
                    "pattern_type",
                    "signal",
                    "confidence",
                    "last_price",
                    "price_change",
                    "bucket_incremental_volume",
                    "volume_ratio",
                    # "cumulative_delta",  # ❌ REDUNDANT
                    # "session_cumulative",  # ❌ REDUNDANT
                    "bucket_incremental_volume",  # ✅ OPTIMIZED
                    "bucket_cumulative_volume",   # ✅ OPTIMIZED
                    "bucket_incremental_volume",
                    "bucket_cumulative_volume",
                    "zerodha_cumulative_volume",
                    "expected_move",
                    "target_price",
                    "stop_loss",
                    "risk_reward",
                    "description",
                }:
                    details[k] = v

            # Prefer exchange timestamp from indicators for time-of-day correctness
            ts_ms = None
            try:
                if "exchange_timestamp_ms" in indicators:
                    ts_ms = float(indicators.get("exchange_timestamp_ms"))
                elif "exchange_timestamp" in indicators and indicators.get(
                    "exchange_timestamp"
                ):
                    # indicators may carry ISO string or epoch seconds
                    et = indicators.get("exchange_timestamp")
                    if isinstance(et, (int, float)):
                        ts_ms = float(et) * 1000.0
            except Exception:
                ts_ms = None

            newp = self._create_pattern(
                symbol,
                name,
                signal,
                timestamp=time.time(),
                confidence=conf,
                last_price=last_price,
                price_change=price_change,
                bucket_incremental_volume=bucket_incremental_volume,
                volume_ratio=volume_ratio,
                bucket_cumulative_volume=bucket_cumulative_volume,   # Raw cumulative bucket_incremental_volume from Zerodha
                vix_level=vix_level,
                expected_move=raw.get("expected_move"),
                target_price=raw.get("target_price"),
                stop_loss=raw.get("stop_loss"),
                risk_reward=raw.get("risk_reward"),
                description=raw.get("description"),
                details=details,
                **(
                    {"timestamp": ts_ms / 1000.0, "timestamp_ms": ts_ms}
                    if ts_ms
                    else {}
                ),
            )
            if getattr(self, "_validate_pattern", None):
                ok, issues = self._validate_pattern(newp)
                if not ok:
                    logger.debug(f"Schema validation issues for {name}: {issues}")
            return newp
        except Exception as e:
            logger.debug(f"Schema conversion failed: {e}")
            return raw

    def _should_allow_pattern_type(self, pattern_type: str) -> bool:
        try:
            if not getattr(self, "vix_regime_manager", None):
                return True
            return self.vix_regime_manager.is_pattern_type_allowed(pattern_type)
        except Exception:
            return True

    @time_pattern_method
    def detect_patterns(
        self, indicators: Dict[str, Any], active_positions: Optional[List[str]] = None, use_pipelining: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Comprehensive pattern detection with volume profile integration and Redis pipelining.
        
        Pattern Categories:
        - Volume-based (3): volume_spike, volume_breakout, volume_price_divergence
        - Momentum-based (2): upside_momentum, downside_momentum  
        - Breakout/Reversal (2): breakout, reversal
        - Microstructure (1): hidden_accumulation
        - ICT Patterns (6): liquidity_pools, fair_value_gaps, optimal_trade_entry, premium_discount, killzone, momentum
        - Straddle Strategies (5): iv_crush_play_straddle, range_bound_strangle, market_maker_trap_detection, premium_collection_strategy, kow_signal_straddle
        - Volume Profile (2): volume_profile_breakout, volume_profile_advanced

        Args:
            indicators: Market indicators for the symbol
            active_positions: List of currently active position symbols (optional)
            use_pipelining: Whether to use Redis pipelining for better performance

        Returns:
            List of detected patterns
        """
        self.stats["invocations"] += 1
        
        # Update VIX-aware thresholds periodically (every 100 invocations)
        if self.stats["invocations"] % 100 == 0:
            self._update_vix_aware_thresholds()
        
        symbol = indicators.get("symbol", "UNKNOWN")
        
        logger.info(f"Starting pattern detection for {symbol}")
        logger.debug(f"Indicators: {indicators}")
        
        try:
            if use_pipelining and self.redis_client and False:  # Disabled due to Redis 8.2 compatibility issues
                # Use Redis pipelining for better performance
                patterns = self._detect_patterns_with_pipelining(indicators, active_positions)
            else:
                # Fallback to original method
                patterns = self._detect_patterns_original(indicators, active_positions)
            
            # Update statistics
            self.stats["total_detections"] += len(patterns)
            for pattern in patterns:
                self.stats["pattern_counts"][pattern["pattern"]] += 1
            
            logger.info(f"Detected {len(patterns)} patterns for {symbol}")
            for i, pattern in enumerate(patterns):
                logger.debug(f"Pattern {i+1}: {pattern.get('pattern', 'UNKNOWN')} confidence: {pattern.get('confidence', 'MISSING')} signal: {pattern.get('signal', 'UNKNOWN')}")
            
        except Exception as e:
            self.logger.error(f"Error detecting patterns for {symbol}: {e}")
            logger.error(f"Error in pattern detection for {symbol}: {e}")
            patterns = []
        
        return patterns

    def _detect_patterns_with_pipelining(
        self, indicators: Dict[str, Any], active_positions: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Detect patterns using Redis pipelining for better performance"""
        symbol = indicators.get("symbol", "UNKNOWN")
        patterns = []
        
        try:
            # Create Redis pipeline for batch operations
            pipe = self.redis_client.pipeline()
            
            # Batch all Redis operations
            # 1. Session data
            from datetime import datetime
            session_date = datetime.now().date()
            pipe.hmget(f"session:{symbol}:{session_date}", "*")
            
            # 2. Volume averages
            pipe.get(f"volume_averages:{symbol}")
            
            # 3. Historical data keys
            pipe.exists(f"bucket_incremental_volume:history:5min:{symbol}")
            pipe.exists(f"bucket_incremental_volume:history:10min:{symbol}")
            pipe.exists(f"bucket_incremental_volume:history:2min:{symbol}")
            
            # 4. OHLC data
            pipe.hmget(f"ohlc:{symbol}:{session_date}", "*")
            
            # Execute all Redis operations in one round trip
            redis_results = pipe.execute()
            
            # Unpack results
            session_data = redis_results[0]
            volume_avg_data = redis_results[1]
            history_5min_exists = redis_results[2]
            history_10min_exists = redis_results[3]
            history_2min_exists = redis_results[4]
            ohlc_data = redis_results[5]
            
            # Use existing standard VIX methods
            vix_value = self._get_current_vix_value()
            vix_regime = self._get_current_vix_regime()
            
            # Create enhanced indicators with pre-fetched data
            enhanced_indicators = {
                **indicators,
                'vix_level': vix_value,
                'vix_regime': vix_regime,
                'session_data': session_data,
                'volume_avg_data': volume_avg_data,
                'ohlc_data': ohlc_data,
                'history_available': {
                    '5min': history_5min_exists,
                    '10min': history_10min_exists,
                    '2min': history_2min_exists
                }
            }
            
            # Update history for rolling calculations
            self._update_history(symbol, indicators)
            
            # Detect patterns with pre-fetched data
            # 1. Volume-based patterns (3 patterns)
            patterns.extend(self._detect_volume_patterns_with_data(enhanced_indicators))
            
            # 2. Momentum patterns (2 patterns)  
            patterns.extend(self._detect_momentum_patterns_with_data(enhanced_indicators))
            
            # 3. Breakout/Reversal patterns (2 patterns)
            patterns.extend(self._detect_breakout_patterns_with_data(enhanced_indicators))
            
            # 4. Microstructure patterns (1 pattern)
            patterns.extend(self._detect_microstructure_patterns_with_data(enhanced_indicators))
            
            # 5. ICT PATTERNS (8 patterns)
            patterns.extend(self._detect_ict_patterns_with_data(enhanced_indicators))
            
            # 6. STRADDLE PATTERNS (4th straddle strategy)
            patterns.extend(self._detect_straddle_patterns_with_data(enhanced_indicators))
            
            # 7. VOLUME PROFILE PATTERNS (Volume Profile Breakout Detection)
            patterns.extend(self._detect_volume_profile_patterns_with_data(enhanced_indicators))
            
        except Exception as e:
            self.logger.error(f"Error in pipelined pattern detection for {symbol}: {e}")
            # Fallback to original method
            patterns = self._detect_patterns_original(indicators, active_positions)
        
        return patterns

    def _detect_patterns_original(
        self, indicators: Dict[str, Any], active_positions: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Original pattern detection method (fallback)"""
        symbol = indicators.get("symbol", "UNKNOWN")
        patterns = []
        
        try:
            # Update history for rolling calculations
            self._update_history(symbol, indicators)
            
            # 1. Volume-based patterns (3 patterns)
            patterns.extend(self._detect_volume_patterns(indicators))
            
            # 2. Momentum patterns (2 patterns)  
            patterns.extend(self._detect_momentum_patterns(indicators))
            
            # 3. Breakout/Reversal patterns (2 patterns)
            patterns.extend(self._detect_breakout_patterns(indicators))
            
            # 4. Microstructure patterns (1 pattern)
            patterns.extend(self._detect_microstructure_patterns(indicators))
            
            # 5. ICT PATTERNS (8 patterns) - ADD THIS LINE
            patterns.extend(self._detect_ict_patterns(indicators))
            
            # 6. STRADDLE PATTERNS (4th straddle strategy)
            patterns.extend(self._detect_straddle_patterns(indicators))
            
            # 7. VOLUME PROFILE PATTERNS (Volume Profile Breakout Detection)
            patterns.extend(self._detect_volume_profile_patterns(indicators))
            
        except Exception as e:
            self.logger.error(f"Error in original pattern detection for {symbol}: {e}")
        
        return patterns

    def _detect_volume_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect volume patterns using pre-fetched data"""
        patterns = []
        symbol = enhanced_indicators.get("symbol", "UNKNOWN")
        last_price = enhanced_indicators.get("last_price", 0)
        volume_ratio = enhanced_indicators.get("volume_ratio", 1.0)
        
        if not last_price:
            return patterns
        
        # Use pre-fetched VIX data for dynamic thresholds
        vix_regime = enhanced_indicators.get("vix_regime", "NORMAL")
        
        # 1. Volume Spike
        volume_spike_pattern = self.detect_volume_spike_with_data(enhanced_indicators)
        if volume_spike_pattern:
            patterns.append(volume_spike_pattern)
            self.stats["volume_spikes"] += 1
        
        # 2. Volume Breakout
        volume_breakout_pattern = self.detect_volume_breakout_with_data(enhanced_indicators)
        if volume_breakout_pattern:
            patterns.append(volume_breakout_pattern)
        
        # 3. Volume-Price Divergence
        volume_divergence_pattern = self.detect_volume_price_divergence_with_data(enhanced_indicators)
        if volume_divergence_pattern:
            patterns.append(volume_divergence_pattern)
        
        return patterns

    def _detect_momentum_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect momentum patterns using pre-fetched data"""
        patterns = []
        
        # 1. Upside Momentum
        upside_pattern = self.detect_upside_momentum_with_data(enhanced_indicators)
        if upside_pattern:
            patterns.append(upside_pattern)
        
        # 2. Downside Momentum
        downside_pattern = self.detect_downside_momentum_with_data(enhanced_indicators)
        if downside_pattern:
            patterns.append(downside_pattern)
        
        return patterns

    def _detect_breakout_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect breakout patterns using pre-fetched data"""
        patterns = []
        
        # 1. Breakout
        breakout_pattern = self.detect_breakout_with_data(enhanced_indicators)
        if breakout_pattern:
            patterns.append(breakout_pattern)
        
        # 2. Reversal
        reversal_pattern = self.detect_reversal_with_data(enhanced_indicators)
        if reversal_pattern:
            patterns.append(reversal_pattern)
        
        return patterns

    def _detect_microstructure_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect microstructure patterns using pre-fetched data"""
        patterns = []
        
        # Hidden Accumulation
        accumulation_pattern = self.detect_hidden_accumulation_with_data(enhanced_indicators)
        if accumulation_pattern:
            patterns.append(accumulation_pattern)
        
        return patterns

    def _detect_ict_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect ICT patterns using pre-fetched data"""
        patterns = []
        symbol = enhanced_indicators.get("symbol", "UNKNOWN")
        
        try:
            # Use pre-fetched VIX data
            vix_level = enhanced_indicators.get("vix_level", 15.0)
            vix_regime = enhanced_indicators.get("vix_regime", "NORMAL")
            
            # Convert indicators to ICT detector expected format
            ict_indicators = {
                'symbol': symbol,
                'last_price': enhanced_indicators.get("last_price", 0),
                'resistance_level': enhanced_indicators.get("resistance_level"),
                'support_level': enhanced_indicators.get("support_level"), 
                'vix_level': vix_level,
                'vix_regime': vix_regime,
                'volume_ratio': PatternMathematics.protect_outliers(enhanced_indicators.get("volume_ratio", 1.0)),
                'dynamic_threshold': self._get_dynamic_threshold_from_data("ict_pattern", vix_regime),
                **enhanced_indicators
            }
            
            # Use centralized methods with pre-fetched data
            ict_pattern_methods = [
                ('detect_ict_liquidity_pools_with_data', 'liquidity_pools'),
                ('detect_ict_fair_value_gaps_with_data', 'fair_value_gaps'),
                ('detect_ict_optimal_trade_entry_with_data', 'optimal_trade_entry'),
                ('detect_ict_premium_discount_with_data', 'premium_discount'),
                ('detect_ict_killzone_with_data', 'killzone'),
                ('detect_ict_momentum_with_data', 'momentum')
            ]
            
            # Add ICT pressure patterns
            pressure_patterns = self.detect_ict_pressure_patterns_with_data(ict_indicators)
            patterns.extend(pressure_patterns)
            
            for method_name, pattern_type in ict_pattern_methods:
                if hasattr(self, method_name):
                    try:
                        method = getattr(self, method_name)
                        ict_result = method(ict_indicators)
                        if ict_result and ict_result.get("confidence", 0) > 0:
                            patterns.append(ict_result)
                    except Exception as e:
                        self.logger.debug(f"ICT pattern {pattern_type} failed: {e}")
        
        except Exception as e:
            self.logger.error(f"Error in ICT pattern detection with data for {symbol}: {e}")
        
        return patterns

    def _detect_straddle_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect straddle patterns using pre-fetched data"""
        patterns = []
        
        try:
            # Use pre-fetched OHLC data
            ohlc_data = enhanced_indicators.get("ohlc_data", {})
            
            # 1. IV Crush Play Straddle
            iv_crush_pattern = self.detect_iv_crush_play_straddle_with_data(enhanced_indicators)
            if iv_crush_pattern:
                patterns.append(iv_crush_pattern)
            
            # 2. Range Bound Strangle
            range_bound_pattern = self.detect_range_bound_strangle_with_data(enhanced_indicators)
            if range_bound_pattern:
                patterns.append(range_bound_pattern)
            
            # 3. Market Maker Trap Detection
            mm_trap_pattern = self.detect_market_maker_trap_detection_with_data(enhanced_indicators)
            if mm_trap_pattern:
                patterns.append(mm_trap_pattern)
            
            # 4. Premium Collection Strategy
            premium_collection_pattern = self.detect_premium_collection_strategy_with_data(enhanced_indicators)
            if premium_collection_pattern:
                patterns.append(premium_collection_pattern)
            
            # 5. Kow Signal Straddle
            kow_signal_pattern = self.detect_kow_signal_straddle_with_data(enhanced_indicators)
            if kow_signal_pattern:
                patterns.append(kow_signal_pattern)
        
        except Exception as e:
            self.logger.error(f"Error in straddle pattern detection with data: {e}")
        
        return patterns

    def _detect_volume_profile_patterns_with_data(self, enhanced_indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect volume profile patterns using pre-fetched data"""
        patterns = []
        
        try:
            # Use pre-fetched history data
            history_available = enhanced_indicators.get("history_available", {})
            
            # 1. Volume Profile Breakout
            vp_breakout_pattern = self.detect_volume_profile_breakout_with_data(enhanced_indicators)
            if vp_breakout_pattern:
                patterns.append(vp_breakout_pattern)
            
            # 2. Volume Node Test
            vn_test_pattern = self.detect_volume_node_test_with_data(enhanced_indicators)
            if vn_test_pattern:
                patterns.append(vn_test_pattern)
        
        except Exception as e:
            self.logger.error(f"Error in volume profile pattern detection with data: {e}")
        
        return patterns

    def _get_dynamic_threshold_from_data(self, pattern_name: str, vix_regime: str) -> float:
        """Get dynamic threshold using pre-fetched VIX regime data"""
        try:
            if not THRESHOLD_CONFIG_AVAILABLE:
                return self.base_thresholds.get(pattern_name, 1.0)
            
            return get_volume_threshold(pattern_name, None, vix_regime)
        except Exception:
            return self.base_thresholds.get(pattern_name, 1.0)

    # Pattern detection methods with pre-fetched data
    def detect_volume_spike_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Volume spike detection using pre-fetched data"""
        symbol = enhanced_indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(enhanced_indicators.get('volume_ratio', 1.0))
        price_change = enhanced_indicators.get('price_change', 0)
        price_change_pct = enhanced_indicators.get('price_change_pct', 0)
        last_price = enhanced_indicators.get('last_price', 0.0)
        
        # Use pre-fetched VIX regime for dynamic threshold
        vix_regime = enhanced_indicators.get('vix_regime', 'NORMAL')
        dynamic_threshold = self._get_dynamic_threshold_from_data("volume_spike", vix_regime)
        min_price_move = 0.005  # 0.5% minimum price movement
        
        if (volume_ratio >= dynamic_threshold and 
            abs(price_change_pct) >= min_price_move and
            price_change_pct * volume_ratio > 0):
            
            volume_confidence = PatternMathematics.calculate_volume_confidence(
                volume_ratio, dynamic_threshold, 0.4
            )
            price_confidence = PatternMathematics.calculate_price_confidence(
                abs(price_change_pct), min_price_move, 0.4
            )
            
            confidence = (volume_confidence + price_confidence) / 2
            
            if confidence >= 0.45:
                signal_data = {
                    "pattern": "volume_spike",
                    "confidence": confidence,
                    "signal": "BUY" if price_change_pct > 0 else "SELL",
                    "expected_move": abs(price_change_pct) * 2,
                    "position_size": int(volume_ratio * 10),
                    "stop_loss": last_price * (0.98 if price_change_pct > 0 else 1.02),
                    "last_price": last_price,
                    "volume_ratio": volume_ratio,
                    "description": f"Volume spike: {volume_ratio:.1f}x with {price_change_pct:.2%} price move",
                    "pattern_type": "volume",
                    "risk_metrics": self._calculate_risk_metrics(last_price, price_change_pct, volume_ratio),
                    "news_context": {},
                    "news_boost": 0.0,
                    "pattern_title": "📈 Volume Spike",
                    "pattern_description": "Significant volume increase with price confirmation",
                    "move_type": "VOLUME_SPIKE",
                    "action_explanation": "Volume spike with price movement - high probability continuation",
                    "pattern_display": "📈 Volume Spike",
                    "trading_instruction": f"Volume spike detected - {volume_ratio:.1f}x normal volume",
                    "directional_action": "BUY" if price_change_pct > 0 else "SELL",
                    "indicators": {
                        "volume_ratio": volume_ratio,
                        "last_price": last_price,
                        "indicators_source": "pattern_payload"
                    },
                    "market_data": {
                        "session_data": enhanced_indicators.get("session_data", {}),
                        "price_movement": {"change_pct": price_change_pct},
                        "pattern_data": {"volume_spike": True},
                        "timestamp": enhanced_indicators.get("timestamp", "")
                    }
                }
                
                return signal_data
        
        return {}

    def detect_ict_killzone_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT killzone detection using pre-fetched data"""
        symbol = enhanced_indicators.get('symbol', 'UNKNOWN')
        last_price = enhanced_indicators.get('last_price', 0)
        volume_ratio = enhanced_indicators.get('volume_ratio', 1.0)
        
        # Use pre-fetched VIX data
        vix_level = enhanced_indicators.get('vix_level', 15.0)
        vix_regime = enhanced_indicators.get('vix_regime', 'NORMAL')
        
        # ICT killzone logic (simplified for example)
        from datetime import datetime
        current_hour = datetime.now().hour
        
        # Killzone times: 9:15-9:45, 11:30-12:00, 14:30-15:00
        is_killzone = (
            (9 <= current_hour < 10) or 
            (11 <= current_hour < 12) or 
            (14 <= current_hour < 15)
        )
        
        if is_killzone and volume_ratio >= 2.0:
            confidence = min(0.95, 0.7 + (volume_ratio - 2.0) * 0.1)
            
            signal_data = {
                "pattern": "ict_killzone",
                "confidence": confidence,
                "signal": "NEUTRAL",
                "expected_move": 1.0,
                "position_size": int(volume_ratio * 50),
                "stop_loss": last_price * 0.995,
                "last_price": last_price,
                "volume_ratio": volume_ratio,
                "description": f"ICT Killzone: {current_hour}:00 active",
                "pattern_type": "ict",
                "risk_metrics": self._calculate_risk_metrics(last_price, 0.01, volume_ratio),
                "news_context": {},
                "news_boost": 0.0,
                "pattern_title": "⏰ ICT Killzone",
                "pattern_description": "High-probability time window for moves",
                "move_type": "ICT_KILLZONE",
                "action_explanation": "Killzone active - optimal time for entries",
                "pattern_display": "⏰ ICT Killzone",
                "trading_instruction": "⏰ KILLZONE: Enter during killzone - Target +1-2%, Stop -0.5%",
                "directional_action": "NEUTRAL",
                "indicators": {
                    "volume_ratio": volume_ratio,
                    "last_price": last_price,
                    "indicators_source": "pattern_payload"
                },
                "market_data": {
                    "session_data": enhanced_indicators.get("session_data", {}),
                    "price_movement": {},
                    "pattern_data": {"ict_killzone": True},
                    "timestamp": enhanced_indicators.get("timestamp", "")
                }
            }
            
            return signal_data
        
        return {}

    def detect_ict_pressure_patterns_with_data(self, enhanced_indicators: Dict) -> List[Dict]:
        """ICT pressure patterns using pre-fetched data"""
        patterns = []
        
        # Simplified pressure pattern detection
        symbol = enhanced_indicators.get('symbol', 'UNKNOWN')
        volume_ratio = enhanced_indicators.get('volume_ratio', 1.0)
        
        if volume_ratio >= 3.0:
            pattern = {
                "pattern": "ict_pressure",
                "confidence": 0.85,
                "signal": "BUY",
                "expected_move": 1.5,
                "position_size": int(volume_ratio * 30),
                "stop_loss": enhanced_indicators.get('last_price', 0) * 0.99,
                "last_price": enhanced_indicators.get('last_price', 0),
                "volume_ratio": volume_ratio,
                "description": f"ICT Pressure: {volume_ratio:.1f}x volume",
                "pattern_type": "ict",
                "risk_metrics": self._calculate_risk_metrics(
                    enhanced_indicators.get('last_price', 0), 0.015, volume_ratio
                ),
                "news_context": {},
                "news_boost": 0.0,
                "pattern_title": "💥 ICT Pressure",
                "pattern_description": "High volume pressure pattern",
                "move_type": "ICT_PRESSURE",
                "action_explanation": "Pressure building - expect breakout",
                "pattern_display": "💥 ICT Pressure",
                "trading_instruction": "💥 PRESSURE: High volume pressure detected",
                "directional_action": "BUY",
                "indicators": {
                    "volume_ratio": volume_ratio,
                    "last_price": enhanced_indicators.get('last_price', 0),
                    "indicators_source": "pattern_payload"
                },
                "market_data": {
                    "session_data": enhanced_indicators.get("session_data", {}),
                    "price_movement": {},
                    "pattern_data": {"ict_pressure": True},
                    "timestamp": enhanced_indicators.get("timestamp", "")
                }
            }
            patterns.append(pattern)
        
        return patterns

    # Placeholder methods for other pattern types (fallback to original methods)
    def detect_volume_breakout_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Volume breakout detection using pre-fetched data - fallback to original"""
        return self.detect_volume_breakout(enhanced_indicators)

    def detect_volume_price_divergence_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Volume-price divergence detection using pre-fetched data - fallback to original"""
        return self.detect_volume_price_divergence(enhanced_indicators)

    def detect_upside_momentum_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Upside momentum detection using pre-fetched data - fallback to original"""
        return self.detect_upside_momentum(enhanced_indicators)

    def detect_downside_momentum_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Downside momentum detection using pre-fetched data - fallback to original"""
        return self.detect_downside_momentum(enhanced_indicators)

    def detect_breakout_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Breakout detection using pre-fetched data - fallback to original"""
        return self.detect_breakout(enhanced_indicators)

    def detect_reversal_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Reversal detection using pre-fetched data - fallback to original"""
        return self.detect_reversal(enhanced_indicators)

    def detect_hidden_accumulation_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Hidden accumulation detection using pre-fetched data - fallback to original"""
        return self.detect_hidden_accumulation(enhanced_indicators)

    def detect_ict_liquidity_pools_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT liquidity pools detection using pre-fetched data - fallback to original"""
        return self.detect_ict_liquidity_pools(enhanced_indicators)

    def detect_ict_fair_value_gaps_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT fair value gaps detection using pre-fetched data - fallback to original"""
        return self.detect_ict_fair_value_gaps(enhanced_indicators)

    def detect_ict_optimal_trade_entry_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT optimal trade entry detection using pre-fetched data - fallback to original"""
        return self.detect_ict_optimal_trade_entry(enhanced_indicators)

    def detect_ict_premium_discount_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT premium discount detection using pre-fetched data - fallback to original"""
        return self.detect_ict_premium_discount(enhanced_indicators)

    def detect_ict_momentum_with_data(self, enhanced_indicators: Dict) -> Dict:
        """ICT momentum detection using pre-fetched data - fallback to original"""
        return self.detect_ict_momentum(enhanced_indicators)

    def detect_iv_crush_play_straddle_with_data(self, enhanced_indicators: Dict) -> Dict:
        """IV crush play straddle detection using pre-fetched data - fallback to original"""
        return self.detect_iv_crush_play_straddle(enhanced_indicators)

    def detect_range_bound_strangle_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Range bound strangle detection using pre-fetched data - fallback to original"""
        return self.detect_range_bound_strangle(enhanced_indicators)

    def detect_market_maker_trap_detection_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Market maker trap detection using pre-fetched data - fallback to original"""
        return self.detect_market_maker_trap_detection(enhanced_indicators)

    def detect_premium_collection_strategy_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Premium collection strategy detection using pre-fetched data - fallback to original"""
        return self.detect_premium_collection_strategy(enhanced_indicators)

    def detect_kow_signal_straddle_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Kow signal straddle detection using pre-fetched data - fallback to original"""
        return self.detect_kow_signal_straddle(enhanced_indicators)

    def detect_volume_profile_breakout_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Volume profile breakout detection using pre-fetched data - fallback to original"""
        return self.detect_volume_profile_breakout(enhanced_indicators)

    def detect_volume_node_test_with_data(self, enhanced_indicators: Dict) -> Dict:
        """Volume node test detection using pre-fetched data - fallback to original"""
        return self.detect_volume_node_test(enhanced_indicators)
    
    def _detect_ict_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Integrate ICT pattern detection with existing system"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        
        try:
            # Convert indicators to ICT detector expected format with MATHEMATICAL INTEGRITY
            ict_indicators = {
                'symbol': symbol,
                'last_price': indicators.get("last_price", 0),
                'resistance_level': indicators.get("resistance_level"),
                'support_level': indicators.get("support_level"), 
                'vix_level': self._get_current_vix_value(),
                'vix_regime': self._get_current_vix_regime(),
                # Apply mathematical integrity to volume_ratio
                'volume_ratio': PatternMathematics.protect_outliers(indicators.get("volume_ratio", 1.0)),
                # Add dynamic threshold for ICT patterns
                'dynamic_threshold': self._get_dynamic_threshold("ict_pattern"),
                # Add any other indicators your ICT detectors need
                **indicators  # Include all existing indicators
            }
            
            # Use centralized methods only - call individual ICT pattern detection methods
            ict_pattern_methods = [
                ('detect_ict_liquidity_pools', 'liquidity_pools'),
                ('detect_ict_fair_value_gaps', 'fair_value_gaps'),
                ('detect_ict_optimal_trade_entry', 'optimal_trade_entry'),
                ('detect_ict_premium_discount', 'premium_discount'),
                ('detect_ict_killzone', 'killzone'),
                ('detect_ict_momentum', 'momentum')
            ]
            
            # Add ICT pressure patterns (returns list)
            pressure_patterns = self.detect_ict_pressure_patterns(ict_indicators)
            patterns.extend(pressure_patterns)
            
            for method_name, pattern_type in ict_pattern_methods:
                if hasattr(self, method_name):
                    try:
                        method = getattr(self, method_name)
                        ict_result = method(ict_indicators)
                        
                        if ict_result:
                            # Ensure pattern_type is defined
                            if 'pattern_type' not in ict_result:
                                ict_result['pattern_type'] = method_name.replace('detect_', '')
                            
                            # Result is already in standard pattern format with mathematical integrity
                            patterns.append(ict_result)
                            
                    except Exception as e:
                        self.logger.error(f"ICT pattern {method_name} failed: {e}")
                    
        except Exception as e:
            self.logger.error(f"ICT pattern detection failed for {symbol}: {e}")
        
        return patterns
    
    def detect_ict_liquidity_pools(self, indicators: Dict) -> Dict:
        """ICT Liquidity Pools with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_liquidity_pools")
        
        # ICT-specific logic: Check for liquidity pool conditions
        if (hasattr(self, 'ict_liquidity_detector') and self.ict_liquidity_detector and
            volume_ratio >= dynamic_threshold):
            
            try:
                # Get liquidity pools from ICT detector
                liquidity_pools = self.ict_liquidity_detector.detect_liquidity_pools(symbol)
                
                if liquidity_pools and any(liquidity_pools.values()):
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "dynamic_threshold": dynamic_threshold,
                        "liquidity_levels": liquidity_pools,
                        "last_price": last_price,
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "ict_liquidity_pools",
                        math_context,
                        lambda: PatternMathematics.calculate_volume_confidence(
                            volume_ratio, dynamic_threshold, 0.7
                        ),
                    )

                    # Determine signal based on price vs pools
                    signal = "BUY" if last_price < liquidity_pools.get('previous_day_high', 0) else "SELL"
                    
                    return self._create_pattern_base(
                        pattern_type="ict_liquidity_pools",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"ICT Liquidity Pool: {liquidity_pools.get('previous_day_high', 0):.2f}",
                        signal=signal,
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        pattern_category="ict",
                        liquidity_pools=liquidity_pools,
                        math_context=math_context
                    )
            except Exception as e:
                logger.warning(f"ICT liquidity detector error for {symbol}: {e}")
        
        return None
    
    def detect_ict_fair_value_gaps(self, indicators: Dict) -> Dict:
        """ICT Fair Value Gaps with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_fair_value_gaps")
        
        # ICT-specific logic: Check for FVG conditions
        if (hasattr(self, 'ict_fvg_detector') and self.ict_fvg_detector and
            volume_ratio >= dynamic_threshold):
            
            try:
                # Get OHLC candles for FVG detection
                candles = self._get_ohlc_candles(symbol)
                
                if candles and len(candles) >= 3:
                    fvgs = self.ict_fvg_detector.detect_fvg(candles)
                    
                    if fvgs:
                        fvg_strength = max([fvg.get('strength', 0) for fvg in fvgs])
                        math_context = {
                            "symbol": symbol,
                            "volume_ratio": volume_ratio,
                            "dynamic_threshold": dynamic_threshold,
                            "fvg_strength": fvg_strength,
                            "last_price": last_price,
                            "fvg_count": len(fvgs),
                            "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                        }
                        confidence = self._resolve_confidence(
                            "ict_fair_value_gaps",
                            math_context,
                            lambda: PatternMathematics.calculate_volume_confidence(
                                volume_ratio, dynamic_threshold, 0.8
                            ),
                        )
                        
                        # Determine signal based on FVG type
                        latest_fvg = fvgs[-1]
                        signal = "BUY" if latest_fvg.get('type') == 'bullish' else "SELL"
                        
                        return self._create_pattern_base(
                            pattern_type="ict_fair_value_gaps",
                            confidence=confidence,
                            symbol=symbol,
                            description=f"ICT FVG {latest_fvg.get('type', 'unknown')}: {fvg_strength:.2%}",
                            signal=signal,
                            last_price=last_price,
                            volume_ratio=volume_ratio,
                            pattern_category="ict",
                            fvg_data=latest_fvg,
                            math_context=math_context
                        )
            except Exception as e:
                logger.warning(f"ICT FVG detector error for {symbol}: {e}")
        
        return None
    
    def detect_ict_optimal_trade_entry(self, indicators: Dict) -> Dict:
        """ICT Optimal Trade Entry with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_optimal_trade_entry")
        
        # ICT-specific logic: Check for OTE conditions
        if (hasattr(self, 'ict_ote_calculator') and self.ict_ote_calculator and
            volume_ratio >= dynamic_threshold):
            
            try:
                # Create impulse move data for OTE calculation
                impulse_move = {
                    'high': indicators.get('high', last_price),
                    'low': indicators.get('low', last_price),
                    'direction': 'up' if indicators.get('price_change', 0) > 0 else 'down'
                }
                
                ote_zones = self.ict_ote_calculator.calculate_ote_zones(impulse_move)
                
                if ote_zones and ote_zones.get('zone_low') and ote_zones.get('zone_high'):
                    # Check if current price is in OTE zone
                    zone_low = ote_zones['zone_low']
                    zone_high = ote_zones['zone_high']
                    
                    if zone_low <= last_price <= zone_high:
                        zone_strength = ote_zones.get('strength', 'weak')
                        base_confidence = 0.8 if zone_strength == 'strong' else 0.6
                        math_context = {
                            "symbol": symbol,
                            "volume_ratio": volume_ratio,
                            "dynamic_threshold": dynamic_threshold,
                            "zone_strength": zone_strength,
                            "ote_direction": ote_zones.get('direction'),
                            "zone_low": zone_low,
                            "zone_high": zone_high,
                            "last_price": last_price,
                            "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                        }
                        confidence = self._resolve_confidence(
                            "ict_optimal_trade_entry",
                            math_context,
                            lambda: PatternMathematics.calculate_volume_confidence(
                                volume_ratio, dynamic_threshold, base_confidence
                            ),
                        )
                        
                        # Determine signal based on direction
                        signal = "BUY" if ote_zones.get('direction') == 'up' else "SELL"
                        
                        return self._create_pattern_base(
                            pattern_type="ict_optimal_trade_entry",
                            confidence=confidence,
                            symbol=symbol,
                            description=f"ICT OTE {ote_zones.get('direction', 'unknown')}: {zone_low:.2f}-{zone_high:.2f}",
                            signal=signal,
                            last_price=last_price,
                            volume_ratio=volume_ratio,
                            pattern_category="ict",
                            ote_zones=ote_zones,
                            math_context=math_context
                        )
            except Exception as e:
                logger.warning(f"ICT OTE calculator error for {symbol}: {e}")
        
        return None
    
    def detect_ict_premium_discount(self, indicators: Dict) -> Dict:
        """ICT Premium/Discount Zones with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_premium_discount")
        
        # ICT-specific logic: Check for premium/discount conditions
        if (hasattr(self, 'ict_premium_discount_detector') and self.ict_premium_discount_detector and
            volume_ratio >= dynamic_threshold):
            
            try:
                # Get premium/discount zones
                zones = self.ict_premium_discount_detector.calculate_premium_discount_zones(indicators)
                
                if zones and zones.get('current_zone'):
                    current_zone = zones['current_zone']
                    distance_from_vwap = zones.get('distance_from_vwap', 0)
                    
                    zone_confidence = min(0.9, 0.5 + distance_from_vwap * 2)
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "dynamic_threshold": dynamic_threshold,
                        "current_zone": current_zone,
                        "distance_from_vwap": distance_from_vwap,
                        "vwap": zones.get('vwap'),
                        "premium_discount_levels": zones,
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "ict_premium_discount",
                        math_context,
                        lambda: PatternMathematics.calculate_volume_confidence(
                            volume_ratio, dynamic_threshold, zone_confidence
                        ),
                    )

                    # Determine signal based on zone
                    signal = "SELL" if current_zone == "premium" else "BUY"
                    
                    return self._create_pattern_base(
                        pattern_type="ict_premium_discount",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"ICT {current_zone.upper()} zone: {distance_from_vwap:.2%} from VWAP",
                        signal=signal,
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        pattern_category="ict",
                        zone_data=zones,
                        math_context=math_context
                    )
            except Exception as e:
                logger.warning(f"ICT premium/discount detector error for {symbol}: {e}")
        
        return None
    
    def _get_current_killzone(self) -> Optional[Dict]:
        """Get current ICT killzone for Indian market hours (IST)"""
        from datetime import datetime, time
        
        now_t = datetime.now().time()
        
        # ICT Killzones for Indian market (9:15-15:30 IST)
        killzones = {
            "opening_drive": {
                "start": time(9, 15),
                "end": time(10, 30),
                "bias": "volatile",
                "priority": "high",
            },
            "midday_drift": {
                "start": time(12, 0),
                "end": time(13, 30),
                "bias": "consolidation",
                "priority": "low",
            },
            "closing_power": {
                "start": time(14, 30),
                "end": time(15, 30),
                "bias": "directional",
                "priority": "high",
            },
        }
        
        for zone_name, info in killzones.items():
            if info["start"] <= now_t <= info["end"]:
                # Calculate time remaining
                end_dt = datetime.combine(datetime.now().date(), info["end"])
                time_remaining = max(0, int((end_dt - datetime.now()).total_seconds() // 60))
                
                return {
                    "zone": zone_name,
                    "bias": info["bias"],
                    "priority": info["priority"],
                    "time_remaining": time_remaining,
                }
        return None

    @time_pattern_method
    def detect_ict_killzone(self, indicators: Dict) -> Dict:
        """ICT Killzone Detection with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_killzone")
        
        # ICT-specific logic: Check for killzone conditions
        if volume_ratio >= dynamic_threshold:
            
            try:
                killzone = None
                if hasattr(self, 'ict_killzone_detector') and self.ict_killzone_detector:
                    killzone = self.ict_killzone_detector.get_current_killzone()
                if not killzone:
                    killzone = self._get_current_killzone()
                
                if killzone and killzone.get('zone'):
                    zone = killzone['zone']
                    priority = killzone.get('priority', 'low')
                    time_remaining = killzone.get('time_remaining', 0)
                    
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "dynamic_threshold": dynamic_threshold,
                        "killzone": zone,
                        "priority": priority,
                        "time_remaining": time_remaining,
                        "bias": killzone.get('bias'),
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "ict_killzone",
                        math_context,
                        lambda: PatternMathematics.calculate_bounded_confidence(
                            PatternMathematics.calculate_volume_confidence(
                                volume_ratio, dynamic_threshold
                            ),
                            (0.15 if priority == 'high' else 0.05) + min(0.15, time_remaining / 120.0),
                            max_bonus=0.2,
                        ),
                    )
                    
                    # Determine signal based on zone bias
                    bias = killzone.get('bias', 'neutral')
                    signal = "BUY" if bias == 'directional' else "NEUTRAL"
                    
                    return self._create_pattern_base(
                        pattern_type="ict_killzone",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"ICT {zone.replace('_', ' ').title()}: {time_remaining}min remaining",
                        signal=signal,
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        pattern_category="ict",
                        killzone_data=killzone,
                        math_context=math_context
                    )
            except Exception as e:
                logger.warning(f"ICT killzone detector error for {symbol}: {e}")
        
        return None
    
    def detect_ict_momentum(self, indicators: Dict) -> Dict:
        """ICT Momentum Patterns with mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        price_change = indicators.get('price_change', 0.0)
        rsi = indicators.get('rsi', 50)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_momentum")
        
        # ICT-specific logic: Check for momentum conditions
        if volume_ratio >= dynamic_threshold:
            # VIX-aware RSI thresholds
            rsi_oversold, rsi_overbought = PatternMathematics.get_vix_aware_rsi_thresholds(
                self.vix_regime, "neutral"
            )
            
            # Check for ICT momentum patterns
            if (abs(price_change) > 0.3 and  # Minimum price movement
                rsi_oversold < rsi < rsi_overbought and  # RSI in neutral zone
                volume_ratio >= dynamic_threshold):
                
                momentum_strength = abs(price_change) / 1.0  # Normalize to 1%
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "dynamic_threshold": dynamic_threshold,
                    "price_change_pct": price_change,
                    "momentum_strength": momentum_strength,
                    "rsi": rsi,
                    "rsi_band": (rsi_oversold, rsi_overbought),
                    "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                }
                confidence = self._resolve_confidence(
                    "ict_momentum",
                    math_context,
                    lambda: PatternMathematics.calculate_price_confidence(
                        abs(price_change), 0.3, 0.7
                    ),
                )
                
                # Determine signal based on direction
                signal = "BUY" if price_change > 0 else "SELL"
                
                return self._create_pattern_base(
                    pattern_type="ict_momentum",
                    confidence=confidence,
                    symbol=symbol,
                    description=f"ICT Momentum: {price_change:+.2%} with RSI {rsi:.1f}",
                    signal=signal,
                    last_price=last_price,
                    volume_ratio=volume_ratio,
                    price_change=price_change,
                    rsi=rsi,
                    pattern_category="ict",
                    math_context=math_context
                )
        
        return None
    
    def detect_ict_pressure_patterns(self, indicators: Dict) -> List[Dict]:
        """ICT Pressure Patterns (Buy/Sell Pressure) with mathematical integrity"""
        patterns = []
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("ict_pressure")
        
        # ICT-specific logic: Check for pressure conditions
        if volume_ratio >= dynamic_threshold:
            
            try:
                killzone = None
                if hasattr(self, 'ict_killzone_detector') and self.ict_killzone_detector:
                    killzone = self.ict_killzone_detector.get_current_killzone()
                if not killzone:
                    killzone = self._get_current_killzone()
                buy_pressure = indicators.get('buy_pressure', 0.5)
                
                # ICT Buy Pressure: High volume + high buy pressure
                if buy_pressure > 0.6:
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "dynamic_threshold": dynamic_threshold,
                        "buy_pressure": buy_pressure,
                        "pressure_direction": "buy",
                        "killzone": killzone,
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "ict_buy_pressure",
                        math_context,
                        lambda: PatternMathematics.calculate_volume_confidence(
                            volume_ratio, dynamic_threshold, 0.55
                        ),
                    )
                    
                    patterns.append(self._create_pattern_base(
                        pattern_type="ict_buy_pressure",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"ICT Buy Pressure: Volume {volume_ratio:.1f}x, Pressure {buy_pressure:.2f}",
                        signal="BUY",
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        pattern_category="ict",
                        pressure_data={
                            "direction": "buy",
                            "buy_pressure": buy_pressure,
                            "killzone": killzone
                        },
                        math_context=math_context
                    ))
                
                # ICT Sell Pressure: High volume + low buy pressure
                elif buy_pressure < 0.4:
                    math_context = {
                        "symbol": symbol,
                        "volume_ratio": volume_ratio,
                        "dynamic_threshold": dynamic_threshold,
                        "buy_pressure": buy_pressure,
                        "pressure_direction": "sell",
                        "killzone": killzone,
                        "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                    }
                    confidence = self._resolve_confidence(
                        "ict_sell_pressure",
                        math_context,
                        lambda: PatternMathematics.calculate_volume_confidence(
                            volume_ratio, dynamic_threshold, 0.55
                        ),
                    )
                    
                    patterns.append(self._create_pattern_base(
                        pattern_type="ict_sell_pressure",
                        confidence=confidence,
                        symbol=symbol,
                        description=f"ICT Sell Pressure: Volume {volume_ratio:.1f}x, Pressure {buy_pressure:.2f}",
                        signal="SELL",
                        last_price=last_price,
                        volume_ratio=volume_ratio,
                        pattern_category="ict",
                        pressure_data={
                            "direction": "sell",
                            "buy_pressure": buy_pressure,
                            "killzone": killzone
                        },
                        math_context=math_context
                    ))
                    
            except Exception as e:
                logger.warning(f"ICT pressure detector error for {symbol}: {e}")
        
        return patterns
    
    def detect_iv_crush_play_straddle(self, indicators: Dict) -> Dict:
        """IV Crush Play Straddle with options mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Options-specific data
        implied_volatility = indicators.get('implied_volatility', 0.0)
        iv_percentile = indicators.get('iv_percentile', 50.0)
        iv_rank = indicators.get('iv_rank', 50.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("iv_crush_play_straddle")
        
        # IV crush detection logic
        if (volume_ratio >= dynamic_threshold and 
            implied_volatility > 0.0 and 
            iv_percentile > 70.0):  # High IV percentile indicates crush potential
            
            # Calculate confidence based on IV percentile and volume
            iv_confidence = min(0.3, (iv_percentile - 70.0) * 0.01)  # Higher IV = higher confidence
            math_context = {
                "symbol": symbol,
                "volume_ratio": volume_ratio,
                "implied_volatility": implied_volatility,
                "iv_percentile": iv_percentile,
                "iv_rank": iv_rank,
                "dynamic_threshold": dynamic_threshold,
                "iv_confidence_bonus": iv_confidence,
                "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
            }
            confidence = self._resolve_confidence(
                "iv_crush_play_straddle",
                math_context,
                lambda: PatternMathematics.calculate_bounded_confidence(0.7, iv_confidence),
            )
            
            # Options-specific risk metrics
            risk_data = {}
            if hasattr(self, 'risk_manager') and self.risk_manager:
                try:
                    risk_data = self.risk_manager.calculate_risk_metrics({
                        'symbol': symbol,
                        'pattern_type': 'iv_crush_play_straddle',
                        'confidence': confidence,
                        'volume_ratio': volume_ratio,
                        'implied_volatility': implied_volatility,
                        'iv_percentile': iv_percentile,
                        'option_type': 'STRADDLE'
                    })
                except Exception as e:
                    logger.warning(f"Risk manager error for IV crush play {symbol}: {e}")
            
            return self._create_pattern_base(
                pattern_type="iv_crush_play_straddle",
                confidence=confidence,
                symbol=symbol,
                description=f"IV Crush Play: IV {implied_volatility:.1f}% (percentile: {iv_percentile:.1f}%)",
                signal="SELL_STRADDLE",
                last_price=last_price,
                volume_ratio=volume_ratio,
                pattern_category="straddle",
                implied_volatility=implied_volatility,
                iv_percentile=iv_percentile,
                iv_rank=iv_rank,
                option_type="STRADDLE",
                risk_metrics=risk_data,
                math_context=math_context
            )
        
        return None
    
    def detect_range_bound_strangle(self, indicators: Dict) -> Dict:
        """Range Bound Strangle with options mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Options-specific data
        implied_volatility = indicators.get('implied_volatility', 0.0)
        iv_percentile = indicators.get('iv_percentile', 50.0)
        support_level = indicators.get('support_level', 0.0)
        resistance_level = indicators.get('resistance_level', 0.0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("range_bound_strangle")
        
        # Range-bound strangle detection logic
        if (volume_ratio >= dynamic_threshold and 
            support_level > 0 and resistance_level > 0 and
            implied_volatility > 0.0):
            
            # Calculate range width
            range_width = (resistance_level - support_level) / last_price if last_price > 0 else 0
            
            # Range-bound conditions: narrow range + moderate IV
            if range_width < 0.05 and 30.0 < iv_percentile < 70.0:  # Narrow range, moderate IV
                # Calculate confidence based on range width and IV
                range_confidence = max(0.0, (0.05 - range_width) * 20)  # Narrower range = higher confidence
                iv_confidence = 0.2 if 40.0 < iv_percentile < 60.0 else 0.1  # Moderate IV preferred
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "range_width": range_width,
                    "implied_volatility": implied_volatility,
                    "iv_percentile": iv_percentile,
                    "support_level": support_level,
                    "resistance_level": resistance_level,
                    "dynamic_threshold": dynamic_threshold,
                    "range_confidence_bonus": range_confidence,
                    "iv_confidence_bonus": iv_confidence,
                    "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                }
                confidence = self._resolve_confidence(
                    "range_bound_strangle",
                    math_context,
                    lambda: PatternMathematics.calculate_bounded_confidence(0.6, range_confidence + iv_confidence),
                )
                
                # Options-specific risk metrics
                risk_data = {}
                if hasattr(self, 'risk_manager') and self.risk_manager:
                    try:
                        risk_data = self.risk_manager.calculate_risk_metrics({
                            'symbol': symbol,
                            'pattern_type': 'range_bound_strangle',
                            'confidence': confidence,
                            'volume_ratio': volume_ratio,
                            'implied_volatility': implied_volatility,
                            'iv_percentile': iv_percentile,
                            'option_type': 'STRANGLE',
                            'support_level': support_level,
                            'resistance_level': resistance_level
                        })
                    except Exception as e:
                        logger.warning(f"Risk manager error for range bound strangle {symbol}: {e}")
                
                return self._create_pattern_base(
                    pattern_type="range_bound_strangle",
                    confidence=confidence,
                    symbol=symbol,
                    description=f"Range Bound Strangle: Range {range_width:.2%}, IV {implied_volatility:.1f}%",
                    signal="SELL_STRANGLE",
                    last_price=last_price,
                    volume_ratio=volume_ratio,
                    pattern_category="straddle",
                    implied_volatility=implied_volatility,
                    iv_percentile=iv_percentile,
                    support_level=support_level,
                    resistance_level=resistance_level,
                    range_width=range_width,
                    option_type="STRANGLE",
                    risk_metrics=risk_data,
                    math_context=math_context
                )
        
        return None

    def _infer_atm_strike(self, symbol: str, reference_price: float) -> Optional[float]:
        """Best-effort ATM strike approximation when tick metadata is missing."""
        if not reference_price or reference_price <= 0:
            return None

        symbol_ref = (symbol or "").upper()
        step = 50  # default for NIFTY family
        if "BANKNIFTY" in symbol_ref:
            step = 100
        elif "FINNIFTY" in symbol_ref or "MIDCPNIFTY" in symbol_ref:
            step = 100

        try:
            return round(reference_price / step) * step
        except Exception:
            return None
    
    def detect_market_maker_trap_detection(self, indicators: Dict) -> Dict:
        """Market Maker Trap Detection with options mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Options-specific data
        implied_volatility = indicators.get('implied_volatility', 0.0)
        open_interest = indicators.get('open_interest', 0)
        unusual_volume = indicators.get('unusual_volume', False)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("market_maker_trap_detection")
        
        # Market maker trap detection logic
        if (volume_ratio >= dynamic_threshold and 
            implied_volatility > 0.0 and
            open_interest > 0):
            
            # MM trap conditions: high volume + unusual OI changes + IV manipulation
            oi_change = indicators.get('oi_change', 0)
            iv_change = indicators.get('iv_change', 0)
            
            if (unusual_volume and 
                abs(oi_change) > 0.2 and  # Significant OI change
                abs(iv_change) > 0.1):    # Significant IV change
                
                # Calculate confidence based on trap strength
                trap_strength = min(0.4, abs(oi_change) + abs(iv_change))
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "implied_volatility": implied_volatility,
                    "open_interest": open_interest,
                    "oi_change": oi_change,
                    "iv_change": iv_change,
                    "unusual_volume": unusual_volume,
                    "dynamic_threshold": dynamic_threshold,
                    "trap_strength": trap_strength,
                    "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                }
                confidence = self._resolve_confidence(
                    "market_maker_trap_detection",
                    math_context,
                    lambda: PatternMathematics.calculate_bounded_confidence(0.7, trap_strength),
                )
                
                # Options-specific risk metrics
                risk_data = {}
                if hasattr(self, 'risk_manager') and self.risk_manager:
                    try:
                        risk_data = self.risk_manager.calculate_risk_metrics({
                            'symbol': symbol,
                            'pattern_type': 'market_maker_trap_detection',
                            'confidence': confidence,
                            'volume_ratio': volume_ratio,
                            'implied_volatility': implied_volatility,
                            'open_interest': open_interest,
                            'oi_change': oi_change,
                            'iv_change': iv_change,
                            'option_type': 'TRAP_DETECTION'
                        })
                    except Exception as e:
                        logger.warning(f"Risk manager error for MM trap detection {symbol}: {e}")
                
                return self._create_pattern_base(
                    pattern_type="market_maker_trap_detection",
                    confidence=confidence,
                    symbol=symbol,
                    description=f"MM Trap: OI {oi_change:+.1%}, IV {iv_change:+.1%}, Vol {volume_ratio:.1f}x",
                    signal="TRAP_DETECTED",
                    last_price=last_price,
                    volume_ratio=volume_ratio,
                    pattern_category="straddle",
                    implied_volatility=implied_volatility,
                    open_interest=open_interest,
                    oi_change=oi_change,
                    iv_change=iv_change,
                    unusual_volume=unusual_volume,
                    option_type="TRAP_DETECTION",
                    risk_metrics=risk_data,
                    math_context=math_context
                )
        
        return None
    
    def detect_premium_collection_strategy(self, indicators: Dict) -> Dict:
        """Premium Collection Strategy with options mathematical integrity"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Options-specific data
        implied_volatility = indicators.get('implied_volatility', 0.0)
        iv_percentile = indicators.get('iv_percentile', 50.0)
        theta = indicators.get('theta', 0.0)  # Time decay
        days_to_expiry = indicators.get('days_to_expiry', 0)
        
        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("premium_collection_strategy")
        
        # Premium collection strategy detection logic
        if (volume_ratio >= dynamic_threshold and 
            implied_volatility > 0.0 and
            days_to_expiry > 0):
            
            # Premium collection conditions: high IV + time decay + reasonable DTE
            if (iv_percentile > 60.0 and  # High IV for premium collection
                theta < -0.1 and         # Significant time decay
                7 <= days_to_expiry <= 30):  # Optimal DTE range
                
                # Calculate confidence based on IV percentile and time decay
                iv_confidence = min(0.3, (iv_percentile - 60.0) * 0.01)
                theta_confidence = min(0.2, abs(theta) * 2)  # Higher theta = higher confidence
                dte_confidence = 0.1 if 14 <= days_to_expiry <= 21 else 0.05  # Optimal DTE bonus
                
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "implied_volatility": implied_volatility,
                    "iv_percentile": iv_percentile,
                    "theta": theta,
                    "days_to_expiry": days_to_expiry,
                    "dynamic_threshold": dynamic_threshold,
                    "iv_confidence_bonus": iv_confidence,
                    "theta_confidence_bonus": theta_confidence,
                    "dte_confidence_bonus": dte_confidence,
                    "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                }
                confidence = self._resolve_confidence(
                    "premium_collection_strategy",
                    math_context,
                    lambda: PatternMathematics.calculate_bounded_confidence(
                        0.6, iv_confidence + theta_confidence + dte_confidence
                    ),
                )
                
                # Options-specific risk metrics
                risk_data = {}
                if hasattr(self, 'risk_manager') and self.risk_manager:
                    try:
                        risk_data = self.risk_manager.calculate_risk_metrics({
                            'symbol': symbol,
                            'pattern_type': 'premium_collection_strategy',
                            'confidence': confidence,
                            'volume_ratio': volume_ratio,
                            'implied_volatility': implied_volatility,
                            'iv_percentile': iv_percentile,
                            'theta': theta,
                            'days_to_expiry': days_to_expiry,
                            'option_type': 'PREMIUM_COLLECTION'
                        })
                    except Exception as e:
                        logger.warning(f"Risk manager error for premium collection {symbol}: {e}")
                
                return self._create_pattern_base(
                    pattern_type="premium_collection_strategy",
                    confidence=confidence,
                    symbol=symbol,
                    description=f"Premium Collection: IV {implied_volatility:.1f}% ({iv_percentile:.1f}%), Theta {theta:.2f}, DTE {days_to_expiry}",
                    signal="SELL_PREMIUM",
                    last_price=last_price,
                    volume_ratio=volume_ratio,
                    pattern_category="straddle",
                    implied_volatility=implied_volatility,
                    iv_percentile=iv_percentile,
                    theta=theta,
                    days_to_expiry=days_to_expiry,
                    option_type="PREMIUM_COLLECTION",
                    risk_metrics=risk_data,
                    math_context=math_context
                )
        
        return None
    
    def detect_kow_signal_straddle(self, indicators: Dict) -> Dict:
        """Kow Signal Straddle with simplified detection for available data"""
        symbol = indicators.get('symbol', 'UNKNOWN')
        volume_ratio = PatternMathematics.protect_outliers(indicators.get('volume_ratio', 1.0))
        last_price = indicators.get('last_price', 0.0)
        
        # Only process NIFTY/BANKNIFTY symbols for straddle strategies
        if not any(underlying in symbol.upper() for underlying in ["NIFTY", "BANKNIFTY"]):
            return None

        if (
            "kow_signal_straddle" in self.killzone_respect_patterns
            and not self._is_within_killzone(indicators.get('timestamp'))
        ):
            logger.debug("Killzone gating prevented kow_signal_straddle for %s", symbol)
            return None

        # Get dynamic threshold
        dynamic_threshold = self._get_dynamic_threshold("kow_signal_straddle")
        
        # Simplified Kow Signal Straddle detection logic
        # Focus on volume spike + price action for NIFTY/BANKNIFTY
        if (volume_ratio >= dynamic_threshold and 
            last_price > 0):
            
            # Calculate ATM strike (simplified - round to nearest 50 for NIFTY, 100 for BANKNIFTY)
            if "NIFTY" in symbol.upper():
                atm_strike = round(last_price / 50) * 50
                strike_distance = abs(last_price - atm_strike) / last_price if last_price > 0 else 0
            elif "BANKNIFTY" in symbol.upper():
                atm_strike = round(last_price / 100) * 100
                strike_distance = abs(last_price - atm_strike) / last_price if last_price > 0 else 0
            else:
                return None
            
            # Kow Signal conditions: high volume + near ATM strike
            if strike_distance < 0.05:  # Near ATM strike (within 5%)
                
                # Calculate confidence based on volume and strike proximity
                volume_confidence = min(0.5, (volume_ratio - dynamic_threshold) * 0.1)  # Higher volume = higher confidence
                strike_confidence = max(0.0, (0.05 - strike_distance) * 20)  # Closer to ATM = higher confidence
                
                math_context = {
                    "symbol": symbol,
                    "volume_ratio": volume_ratio,
                    "last_price": last_price,
                    "strike_price": atm_strike,
                    "strike_distance": strike_distance,
                    "dynamic_threshold": dynamic_threshold,
                    "volume_confidence_bonus": volume_confidence,
                    "strike_confidence_bonus": strike_confidence,
                    "vix_regime": getattr(self, "vix_regime", "UNKNOWN"),
                }
                confidence = self._resolve_confidence(
                    "kow_signal_straddle",
                    math_context,
                    lambda: PatternMathematics.calculate_bounded_confidence(
                        0.75, volume_confidence + strike_confidence
                    ),
                )
                
                # Basic risk metrics for straddle strategy
                risk_data = self._calculate_risk_metrics(last_price, 0.02, volume_ratio)  # 2% expected move
                
                return self._create_pattern_base(
                    pattern_type="kow_signal_straddle",
                    confidence=confidence,
                    symbol=symbol,
                    description=f"Kow Signal Straddle: Strike {atm_strike:.0f}, Volume {volume_ratio:.1f}x",
                    signal="VWAP_STRADDLE",
                    last_price=last_price,
                    volume_ratio=volume_ratio,
                    pattern_category="straddle",
                    strike_price=atm_strike,
                    strike_distance=strike_distance,
                    option_type="VWAP_STRADDLE",
                    risk_metrics=risk_data,
                    math_context=math_context
                )
            else:
                if self.config.get("debug", False):
                    self.logger.debug(
                        "KOW skip %s: strike_distance=%.4f vol_ratio=%.2f (threshold %.2f)",
                        symbol,
                        strike_distance,
                        volume_ratio,
                        dynamic_threshold,
                    )

        return None
    
    def _detect_straddle_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect straddle patterns using individual straddle pattern detection methods with mathematical integrity"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        
        # Ensure symbol is a string (may be int like instrument_token)
        if not isinstance(symbol, str):
            symbol = str(symbol)
        
        try:
            # Only process NIFTY/BANKNIFTY for straddle strategies
            if not any(underlying in symbol.upper() for underlying in ["NIFTY", "BANKNIFTY"]):
                return patterns
            
            # Call individual straddle pattern detection methods with mathematical integrity
            straddle_pattern_methods = [
                ('detect_iv_crush_play_straddle', 'iv_crush_play_straddle'),
                ('detect_range_bound_strangle', 'range_bound_strangle'),
                ('detect_market_maker_trap_detection', 'market_maker_trap_detection'),
                ('detect_premium_collection_strategy', 'premium_collection_strategy'),
                ('detect_kow_signal_straddle', 'kow_signal_straddle')
            ]
            
            for method_name, pattern_type in straddle_pattern_methods:
                if hasattr(self, method_name):
                    try:
                        method = getattr(self, method_name)
                        straddle_result = method(indicators)
                        
                        if straddle_result:
                            # Result is already in standard pattern format with mathematical integrity
                            patterns.append(straddle_result)
                            
                    except Exception as e:
                        self.logger.error(f"Straddle pattern {method_name} failed: {e}")
            
            # Also use existing straddle strategy if available (for backward compatibility)
            if hasattr(self, 'straddle_strategy') and self.straddle_strategy:
                try:
                    # Handle async straddle strategy properly
                    import asyncio
                    if asyncio.iscoroutinefunction(self.straddle_strategy.on_tick):
                        # Run async method in sync context
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            signal = loop.run_until_complete(self.straddle_strategy.on_tick(indicators))
                        finally:
                            loop.close()
                    else:
                        signal = self.straddle_strategy.on_tick(indicators)
                    
                    if signal and isinstance(signal, dict):
                        # Convert straddle signal to standard pattern format
                        pattern = {
                            "symbol": symbol,
                            "pattern": signal.get("action", "straddle_signal"),
                            "confidence": signal.get("confidence", 0.85),
                            "signal": signal.get("action", "NEUTRAL"),
                            "expected_move": signal.get("expected_move", 1.0),
                            "last_price": signal.get("last_price", indicators.get("last_price", 0)),
                            "description": signal.get("reason", "Straddle signal detected"),
                            "pattern_type": "straddle",
                            "strike": signal.get("strike", 0),
                            "ce_symbol": signal.get("ce_symbol", ""),
                            "pe_symbol": signal.get("pe_symbol", ""),
                            "combined_premium": signal.get("entry_premium", 0.0),
                            "underlying": signal.get("underlying", "NIFTY"),
                            "expiry": signal.get("expiry", ""),
                            "strategy_type": "straddle",
                            "straddle_metadata": signal
                        }
                        patterns.append(pattern)
                        
                        self.logger.info(f"🎯 STRADDLE PATTERN: {symbol} - {signal.get('action', 'UNKNOWN')} "
                                       f"(confidence: {signal.get('confidence', 0):.1%})")
                        
                except Exception as e:
                    self.logger.error(f"Straddle strategy error for {symbol}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Straddle pattern detection failed for {symbol}: {e}")
        
        return patterns
    
    def _standardize_ict_patterns(self, ict_results: List[Dict], symbol: str, pattern_type: str = None) -> List[Dict]:
        """Convert ICT pattern results to standard pattern format with MATHEMATICAL INTEGRITY"""
        standardized = []
        
        # Handle single result (not a list)
        if not isinstance(ict_results, list):
            ict_results = [ict_results] if ict_results else []
        
        for ict_pattern in ict_results:
            # Apply mathematical integrity to confidence
            raw_confidence = ict_pattern.get("confidence", 0.5)
            bounded_confidence = PatternMathematics.calculate_bounded_confidence(raw_confidence, 0.0)
            
            # Apply outlier protection to volume_ratio
            volume_ratio = PatternMathematics.protect_outliers(ict_pattern.get("volume_ratio", 1.0))
            
            # Get risk metrics from Risk Manager
            risk_data = {}
            if hasattr(self, 'risk_manager') and self.risk_manager:
                try:
                    risk_data = self.risk_manager.calculate_risk_metrics({
                        'symbol': symbol,
                        'pattern_type': f"ict_{pattern_type}" if pattern_type else "ict_pattern",
                        'confidence': bounded_confidence,
                        'volume_ratio': volume_ratio,
                        **ict_pattern
                    })
                except Exception as e:
                    logger.warning(f"Risk manager error for ICT pattern {symbol}: {e}")
            
            # Convert to standard pattern format with mathematical integrity
            standardized_pattern = {
                "symbol": symbol,
                "pattern": ict_pattern.get("pattern_type", f"ict_{pattern_type}" if pattern_type else "ict_pattern"),
                "confidence": bounded_confidence,  # MATHEMATICAL INTEGRITY
                "signal": risk_data.get('signal', ict_pattern.get("signal", "NEUTRAL")),  # From Risk Manager
                "expected_move": risk_data.get('expected_move', ict_pattern.get("expected_move", 1.0)),  # From Risk Manager
                "position_size": risk_data.get('position_size', 1),  # From Risk Manager
                "stop_loss": risk_data.get('stop_loss', 0.0),  # From Risk Manager
                "last_price": ict_pattern.get("last_price", 0),
                "volume_ratio": volume_ratio,  # OUTLIER PROTECTION
                "description": ict_pattern.get("description", f"ICT {pattern_type} Pattern Detected" if pattern_type else "ICT Pattern Detected"),
                "pattern_type": "ict",  # Mark as ICT pattern for filtering
                "ict_metadata": ict_pattern,  # Keep original ICT data
                "risk_metrics": risk_data  # Include full risk assessment
            }
            standardized.append(standardized_pattern)
        
        return standardized
    
    def get_patterns_by_type(self, patterns: List[Dict], pattern_type: str = "all") -> List[Dict]:
        """Filter patterns by type (basic, ict, or all)"""
        if pattern_type == "all":
            return patterns
        elif pattern_type == "basic":
            return [p for p in patterns if p.get("pattern_type") != "ict"]
        elif pattern_type == "ict":
            return [p for p in patterns if p.get("pattern_type") == "ict"]
        return patterns
    
    def _standardize_indicators(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize field names to match optimized_field_mapping.yaml"""
        standardized = indicators.copy()
        
        # Map legacy field names to canonical names using YAML resolvers
        canonical_mappings = [
            ("high_price", self.resolve_session_field("high")),
            ("low_price", self.resolve_session_field("low")),
            ("volume", self.resolve_session_field("bucket_incremental_volume")),
            ("incremental_volume", self.resolve_session_field("bucket_incremental_volume")),
            ("cumulative_volume", self.resolve_session_field("bucket_cumulative_volume")),
        ]
        
        for legacy_field, canonical_field in canonical_mappings:
            if legacy_field in standardized and canonical_field not in standardized:
                standardized[canonical_field] = standardized[legacy_field]
        
        return standardized
    
    def _convert_ict_patterns(self, ict_patterns: List[Dict], symbol: str, pattern_type: str) -> List[Dict[str, Any]]:
        """Convert ICT patterns to standard format"""
        patterns = []
        for ict_pattern in ict_patterns:
            pattern = {
                "symbol": symbol,
                "pattern": f"ict_{pattern_type}",
                "confidence": ict_pattern.get("confidence", 0.7),
                "signal": ict_pattern.get("signal", "NEUTRAL"),
                "expected_move": ict_pattern.get("expected_move", 1.0),
                "last_price": ict_pattern.get("last_price", 0),
                "description": ict_pattern.get("description", f"ICT {pattern_type} pattern"),
                "pattern_type": "ict",
                "ict_metadata": ict_pattern
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_liquidity_patterns(self, liquidity_pools: Dict, symbol: str) -> List[Dict[str, Any]]:
        """Convert liquidity pool patterns to standard format"""
        patterns = []
        if liquidity_pools:
            pattern = {
                "symbol": symbol,
                "pattern": "ict_liquidity_pools",
                "confidence": 0.8,
                "signal": "NEUTRAL",
                "expected_move": 1.5,
                "last_price": 0,
                "description": f"ICT liquidity pools detected",
                "pattern_type": "ict",
                "ict_metadata": liquidity_pools
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_fvg_patterns(self, fvgs: List[Dict], symbol: str) -> List[Dict[str, Any]]:
        """Convert FVG patterns to standard format"""
        patterns = []
        for fvg in fvgs:
            pattern = {
                "symbol": symbol,
                "pattern": f"ict_fvg_{fvg.get('type', 'unknown')}",
                "confidence": min(0.9, 0.7 + fvg.get("strength", 0) * 0.2),
                "signal": "BUY" if fvg.get("type") == "bullish" else "SELL",
                "expected_move": fvg.get("strength", 0) * 2,
                "last_price": 0,
                "description": f"ICT FVG {fvg.get('type', 'unknown')} gap detected",
                "pattern_type": "ict",
                "ict_metadata": fvg
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_ote_patterns(self, ote_zones: Dict, symbol: str) -> List[Dict[str, Any]]:
        """Convert OTE patterns to standard format"""
        patterns = []
        if ote_zones:
            pattern = {
                "symbol": symbol,
                "pattern": "ict_ote",
                "confidence": 0.75,
                "signal": "NEUTRAL",
                "expected_move": 1.0,
                "last_price": 0,
                "description": "ICT OTE retracement zone detected",
                "pattern_type": "ict",
                "ict_metadata": ote_zones
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_premium_discount_patterns(self, premium_discount: Dict, symbol: str) -> List[Dict[str, Any]]:
        """Convert premium/discount patterns to standard format"""
        patterns = []
        if premium_discount:
            pattern = {
                "symbol": symbol,
                "pattern": "ict_premium_discount",
                "confidence": 0.8,
                "signal": "SELL" if premium_discount.get("zone") == "premium" else "BUY",
                "expected_move": 1.2,
                "last_price": 0,
                "description": f"ICT {premium_discount.get('zone', 'unknown')} zone detected",
                "pattern_type": "ict",
                "ict_metadata": premium_discount
            }
            patterns.append(pattern)
        return patterns
    
    def _calculate_risk_metrics(self, last_price: float, price_change_pct: float, volume_ratio: float) -> Dict[str, Any]:
        """Calculate basic risk metrics for patterns"""
        try:
            if last_price <= 0:
                return {"error": "Invalid last_price"}
            
            # Basic risk calculation
            stop_loss_pct = 0.02  # 2% stop loss
            target_pct = 0.01     # 1% target
            
            stop_loss = last_price * (1 - stop_loss_pct)
            target_price = last_price * (1 + target_pct)
            
            # Position size based on volume ratio (simplified)
            position_size = min(1000, max(100, int(volume_ratio * 200)))
            
            return {
                "stop_loss": stop_loss,
                "target_price": target_price,
                "position_size": position_size,
                "risk_reward_ratio": target_pct / stop_loss_pct,
                "max_loss": last_price - stop_loss,
                "max_gain": target_price - last_price
            }
        except Exception as e:
            return {"error": f"Risk calculation failed: {e}"}

    def _convert_killzone_patterns(self, killzone: Dict, symbol: str, indicators: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Convert killzone patterns to standard format"""
        patterns = []
        if killzone:
            # Get actual market data from indicators
            last_price = 0.0
            if indicators:
                last_price = float(indicators.get('last_price', 0) or 0)
            
            pattern = {
                "symbol": symbol,
                "pattern": "ict_killzone",
                "confidence": 0.95,  # Higher confidence for killzone
                "signal": "NEUTRAL",
                "expected_move": 1.0,
                "last_price": last_price,
                "description": f"ICT killzone {killzone.get('zone', 'unknown')} detected",
                "pattern_type": "ict",
                "ict_metadata": killzone
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_mm_patterns(self, mm_patterns: List[Dict], symbol: str) -> List[Dict[str, Any]]:
        """Convert MM exploitation patterns to standard format"""
        patterns = []
        for mm_pattern in mm_patterns:
            pattern = {
                "symbol": symbol,
                "pattern": "ict_mm_exploitation",
                "confidence": mm_pattern.get("confidence", 0.7),
                "signal": mm_pattern.get("signal", "NEUTRAL"),
                "expected_move": mm_pattern.get("expected_move", 1.0),
                "last_price": 0,
                "description": f"ICT MM exploitation: {mm_pattern.get('strategy', 'unknown')}",
                "pattern_type": "ict",
                "ict_metadata": mm_pattern
            }
            patterns.append(pattern)
        return patterns
    
    def _convert_range_patterns(self, range_patterns: List[Dict], symbol: str) -> List[Dict[str, Any]]:
        """Convert range bound patterns to standard format"""
        patterns = []
        for range_pattern in range_patterns:
            pattern = {
                "symbol": symbol,
                "pattern": "ict_range_bound",
                "confidence": range_pattern.get("confidence", 0.7),
                "signal": range_pattern.get("signal", "NEUTRAL"),
                "expected_move": range_pattern.get("expected_move", 1.0),
                "last_price": 0,
                "description": f"ICT range bound: {range_pattern.get('strategy', 'unknown')}",
                "pattern_type": "ict",
                "ict_metadata": range_pattern
            }
            patterns.append(pattern)
        return patterns
    
    def _get_ohlc_candles(self, symbol: str) -> List[Dict]:
        """Get OHLC candles for symbol from Redis"""
        try:
            if not self.redis_client:
                return []
            
            session_date = datetime.now().strftime("%Y-%m-%d")
            buckets = self.redis_client.get_ohlc_buckets(symbol, count=30, session_date=session_date)
            if not buckets:
                buckets = self.redis_client.get_time_buckets(symbol, session_date=session_date) or []
            
            candles: List[Dict[str, Any]] = []
            for bucket in buckets:
                try:
                    candles.append({
                        "open": float(bucket.get("open", bucket.get("close", 0.0))),
                        "high": float(bucket.get("high", bucket.get("close", 0.0))),
                        "low": float(bucket.get("low", bucket.get("close", 0.0))),
                        "close": float(bucket.get("close", 0.0)),
                        "timestamp": bucket.get("first_timestamp") or bucket.get("timestamp") or 0.0,
                        "volume": float(bucket.get("bucket_incremental_volume", 0.0)),
                    })
                except Exception as candle_error:
                    self.logger.debug(f"Skipping malformed OHLC bucket for {symbol}: {candle_error}")
                    continue
            return candles
        except Exception as e:
            self.logger.warning(f"Failed to get OHLC candles for {symbol}: {e}")
            return []
    
    def _update_history(self, symbol: str, indicators: Dict[str, Any]):
        """Update rolling history for calculations"""
        last_price = indicators.get("last_price", 0)
        # Use volume_ratio as proxy for bucket_incremental_volume since we don't have raw bucket_incremental_volume
        bucket_incremental_volume = indicators.get("volume_ratio", 1.0) * 1000  # Scale for history
        
        if last_price > 0:
            if symbol not in self.price_history:
                self.price_history[symbol] = deque(maxlen=20)
            self.price_history[symbol].append(last_price)
        
        if bucket_incremental_volume > 0:
            if symbol not in self.volume_history:
                self.volume_history[symbol] = deque(maxlen=20)
            self.volume_history[symbol].append(bucket_incremental_volume)
    
    def _detect_volume_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect bucket_incremental_volume-based patterns (3 patterns)"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        last_price = indicators.get("last_price", 0)
        volume_ratio = indicators.get("volume_ratio", 1.0)
        
        if not last_price:
            return patterns
        
        # Use volume_ratio directly from indicators (already calculated by HybridCalculations)
        # Note: Allow patterns even with low bucket_incremental_volume - other conditions will filter appropriately
        
        # 1. Volume Spike (using dedicated method with mathematical integrity)
        volume_spike_pattern = self.detect_volume_spike(indicators)
        if volume_spike_pattern:
            patterns.append(volume_spike_pattern)
            self.stats["volume_spikes"] += 1
        
        # 2. Volume Breakout (using dedicated method with mathematical integrity)
        volume_breakout_pattern = self.detect_volume_breakout(indicators)
        if volume_breakout_pattern:
            patterns.append(volume_breakout_pattern)
        
        # 3. Volume-Price Divergence (using dedicated method with mathematical integrity)
        volume_divergence_pattern = self.detect_volume_price_divergence(indicators)
        if volume_divergence_pattern:
            patterns.append(volume_divergence_pattern)
        
        return patterns
    
    def _detect_momentum_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect momentum patterns (2 patterns)"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        last_price = indicators.get("last_price", 0)
        volume_ratio = indicators.get("volume_ratio", 1.0)
        
        # Get last_price change directly from indicators
        price_change = indicators.get("price_change", 0)
        rsi = indicators.get("rsi", 50)
        
        if not last_price:
            return patterns
        
        # 1. Upside Momentum (using dedicated method with mathematical integrity)
        upside_momentum_pattern = self.detect_upside_momentum(indicators)
        if upside_momentum_pattern:
            patterns.append(upside_momentum_pattern)
            self.stats["momentum_signals"] += 1
        
        # 2. Downside Momentum (using dedicated method with mathematical integrity)
        downside_momentum_pattern = self.detect_downside_momentum(indicators)
        if downside_momentum_pattern:
            patterns.append(downside_momentum_pattern)
            self.stats["momentum_signals"] += 1
        
        return patterns
    
    def _detect_breakout_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect breakout/reversal patterns (2 patterns)"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        last_price = indicators.get("last_price", 0)
        volume_ratio = indicators.get("volume_ratio", 1.0)
        rsi = indicators.get("rsi", 50)
        
        # Get last_price change directly from indicators
        price_change = indicators.get("price_change", 0)
        rsi = indicators.get("rsi", 50)
        
        if not last_price:
            return patterns
        
        # 1. Breakout (using dedicated method with mathematical integrity)
        breakout_pattern = self.detect_breakout(indicators)
        if breakout_pattern:
            patterns.append(breakout_pattern)
            self.stats["breakout_signals"] += 1
        
        # 2. Reversal (using dedicated method with mathematical integrity)
        reversal_pattern = self.detect_reversal(indicators)
        if reversal_pattern:
            patterns.append(reversal_pattern)
            self.stats["reversal_signals"] += 1
        
        return patterns
    
    def _detect_microstructure_patterns(self, indicators: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect microstructure patterns (1 pattern)"""
        patterns = []
        symbol = indicators.get("symbol", "UNKNOWN")
        last_price = indicators.get("last_price", 0)
        volume_ratio = indicators.get("volume_ratio", 1.0)
        rsi = indicators.get("rsi", 50)
        
        if not last_price:
            return patterns
        
        # Hidden accumulation: Steady bucket_incremental_volume with last_price consolidation
        # Use RSI and volume_ratio to detect stealth buying
        # 1. Hidden Accumulation (using dedicated method with mathematical integrity)
        hidden_accumulation_pattern = self.detect_hidden_accumulation(indicators)
        if hidden_accumulation_pattern:
            patterns.append(hidden_accumulation_pattern)
            self.stats["hidden_accumulation"] += 1
        
        return patterns

    def _get_volume_ratio_from_redis(self, symbol: str, indicators: dict) -> Optional[float]:
        """Get pre-calculated bucket_incremental_volume ratio from Redis"""
        try:
            # Try to get from Redis using latest key
            redis_key = f"volume_ratio:{symbol}:latest"
            stored_data = self.redis_client.get(redis_key)
            
            if stored_data:
                try:
                    data = json.loads(stored_data) if isinstance(stored_data, str) else stored_data
                    # Check if the data is recent (within 60 seconds)
                    stored_time = data.get("timestamp", 0)
                    current_time = time.time()
                    if current_time - stored_time < 60:  # 60 seconds freshness
                        return float(data.get("volume_ratio", 0.0))
                except (json.JSONDecodeError, KeyError, ValueError):
                    pass
            
            return None
            
        except Exception as e:
            logger.debug(f"Redis bucket_incremental_volume ratio retrieval failed for {symbol}: {e}")
            return None
    
    def _calculate_std(self, values: List[float]) -> float:
        """Calculate standard deviation"""
        if len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance ** 0.5
        
        # 🎯 LINEAGE: Track data input to pattern detector (disabled for now)
        # try:
        #     from scanner_main import data_lineage
        #     data_lineage.track_data_source(symbol, "pattern_detector_input", indicators)
        # except ImportError:
        #     pass

        # Update historical caches and validate data quality before detection
        self.update_history(symbol, indicators)
        prices = list(self.price_history.get(symbol, []))
        volumes = list(self.volume_history.get(symbol, []))

        quality_check = self.validate_data_quality(symbol, prices, volumes)
        self.quality_stats["instruments_evaluated"] += 1
        self.quality_stats["quality_scores_calculated"] += 1
        indicators["data_quality"] = quality_check

        if not quality_check["valid"]:
            self.quality_stats["filtered_low_quality"] += 1
            self.logger.error(
                f"❌ [DATA_QUALITY_REJECT] {symbol} - Score: {quality_check['score']}, Issues: {quality_check['issues']}"
            )
            return []

        self.logger.info(
            f"✅ [DATA_QUALITY_PASS] {symbol} - Score: {quality_check['score']}, Level: {quality_check['level']}"
        )

        # Use the volume_ratio from indicators as-is (no temporary override)
        
        # Use the preserved volume_ratio from indicators (no fallback to 1.0)
        current_volume_ratio_raw = indicators.get("volume_ratio")
        volume_ratio_missing = current_volume_ratio_raw is None
        volume_ratio = _indicator_value(
            indicators,
            "volume_ratio",
            1.0 if volume_ratio_missing else current_volume_ratio_raw,
        )
        indicators["volume_ratio"] = volume_ratio
        # VIX data is now read directly from Redis (published every 30 seconds)
        # Update thresholds based on real-time VIX
        self._update_vix_aware_thresholds()

        patterns = []

        # Basic input validation for required indicators
        required_fields = ["volume_ratio", "last_price"]
        missing = [f for f in required_fields if f not in indicators]
        if missing:
            self.stats["missing_indicator_calls"] += 1
            if self.config.get("debug", False):
                logger.warning(
                    f"Missing indicators for {indicators.get('symbol', 'UNKNOWN')}: {missing}"
                )
            # Continue; some patterns may still be detectable

        # ✅ SINGLE SOURCE OF TRUTH: Use pre-calculated volume data from WebSocket parser
        # Do NOT retrieve bucket data from Redis - this violates the single source of truth principle
        # The WebSocket parser has already calculated incremental volume using VolumeStateManager
        symbol = indicators.get("symbol", "UNKNOWN")
        if self.redis_client:
            try:
                # Get Redis cumulative data for validation (this is OK - it's session data, not volume calculation)
                cumulative_data = self.redis_client.get_cumulative_data(symbol)
                
                # ✅ LEGACY REMOVED: Do NOT get time buckets for volume analysis
                # Volume data is already pre-calculated by WebSocket parser
                # time_buckets = self.redis_client.get_time_buckets(symbol)  # REMOVED
                
                if cumulative_data and cumulative_data.get("update_count", 0) > 10:
                    # Adjust confidence based on session data
                    session_confidence_boost = min(
                        1.0, cumulative_data.get("update_count", 0) / 100.0
                    )
                    # Store session context for later pattern validation
                    indicators["redis_session_context"] = {
                        "update_count": cumulative_data.get("update_count", 0),
                        "time_buckets_count": 0,  # Set to 0 since we're not retrieving buckets
                        "bucket_cumulative_volume": cumulative_data.get(
                            "bucket_cumulative_volume", 0
                        ),
                        "session_confidence_boost": session_confidence_boost,
                    }

            except Exception as e:
                logger.debug(f"Redis pattern enhancement failed for {symbol}: {e}")

        # Optimized logging for pattern detection
        symbol = indicators.get("symbol", "UNKNOWN")
        # Use raw cumulative bucket_incremental_volume for bucket_incremental_volume ratio calculation (no calculations)
        # Only recalculate if volume_ratio is truly missing (None), not when it's 0.0
        current_volume_ratio = current_volume_ratio_raw
        self.debug_volume_ratio_flow(symbol, "pattern_detector_input", current_volume_ratio)

        if volume_ratio_missing:
            try:
                # First try to get from Redis
                redis_ratio = self._get_volume_ratio_from_redis(symbol, indicators)
                if redis_ratio is not None:
                    indicators["volume_ratio"] = redis_ratio
                    indicators["volume_ratio_source"] = "redis"
                    self.debug_volume_ratio_flow(symbol, "pattern_detector_redis", current_volume_ratio, redis_ratio)
                else:
                    # Use centralized volume ratio from VolumeResolver (single source of truth)
                    enhanced = VolumeResolver.get_volume_ratio(indicators)
                    indicators["volume_ratio_enhanced"] = enhanced
                    indicators["volume_ratio"] = enhanced
                    indicators["volume_ratio_source"] = "calculated"
                    self.enhanced_ratio_used_count += 1
                    self.debug_volume_ratio_flow(symbol, "pattern_detector_recalculated", current_volume_ratio, enhanced)
                    
                    # Verify volume consistency at pattern detection stage
                    verification = VolumeResolver.verify_volume_consistency(indicators, "pattern_detection")
                    if not verification["consistent"]:
                        self.logger.warning(f"Volume consistency issue at pattern detection for {symbol}: {verification['issues']}")
            except Exception:
                pass
        else:
            if random.random() < 0.5:  # 50% reduction
                print(f"[VOLUME_DEBUG] {symbol} - preserving volume_ratio: {current_volume_ratio}")

        self.debug_volume_ratio_flow(symbol, "pattern_detector_final", indicators.get("volume_ratio"))
        raw_volume_ratio = float(indicators.get("volume_ratio", 0)) if indicators.get("volume_ratio") is not None else 0.0
        volume_ratio = raw_volume_ratio  # Use volume ratio as calculated by volume baseline system
        try:
            # Ensure volume_ratio is always a float for comparisons
            volume_ratio = float(volume_ratio) if volume_ratio is not None else 0.0
            if volume_ratio:
                self.recent_volume_ratios.append(float(volume_ratio))
        except Exception:
            pass
        price_change = float(indicators.get("price_change", 0)) if indicators.get("price_change") is not None else 0.0

        # Reduced logging frequency for performance
        if self.config.get("debug", False) and "volume_ratio" in indicators:
            logger.debug(
                f"✅ PATTERN DETECTOR: {symbol} - volume_ratio: {volume_ratio:.2f}x, price_change: {price_change:.2%}"
            )

        # Calculate instrument quality score
        quality_score = self._get_instrument_quality_score(symbol, indicators)

        # Apply quality filter - only process high-quality setups
        QUALITY_THRESHOLD = 0.0  # Disabled quality filter to allow all patterns through
        logger.info(f"🔍 QUALITY SCORE: {symbol} - {quality_score:.2f} (threshold: {QUALITY_THRESHOLD})")
        if quality_score < QUALITY_THRESHOLD:
            self.quality_stats["filtered_low_quality"] += 1
            logger.info(f"🔍 QUALITY FILTERED: {symbol} - quality score {quality_score:.2f} < {QUALITY_THRESHOLD}")
            return patterns

        self.quality_stats["high_quality_signals"] += 1

        # Check correlation limits if active positions provided
        if active_positions is not None:
            if not self._check_correlation_limit(symbol, active_positions):
                return []  # Skip due to correlation limits

        # Detect all pattern types with regime gating and quality scores
        volume_patterns = self._detect_volume_patterns(indicators)
        momentum_patterns = self._detect_momentum_patterns(indicators)
        pressure_patterns = self._detect_pressure_patterns(indicators)
        breakout_patterns = self._detect_breakout_patterns(indicators)
        reversal_patterns = self._detect_reversal_patterns(indicators)
        # MM patterns removed - using 8 core patterns only
        
        
        patterns.extend(self._get_regime_allowed_patterns(volume_patterns))
        patterns.extend(self._get_regime_allowed_patterns(momentum_patterns))
        patterns.extend(self._get_regime_allowed_patterns(pressure_patterns))
        patterns.extend(self._get_regime_allowed_patterns(breakout_patterns))
        patterns.extend(self._get_regime_allowed_patterns(reversal_patterns))
        # MM patterns removed - using 8 core patterns only

        # 🆕 CRITICAL: Apply direction-aware spoofing confidence adjustment
        spoofing_confidence = indicators.get("spoofing_confidence", 0.0)
        spoofing_direction = indicators.get("spoofing_direction", "none")
        
        if spoofing_confidence > 0:
            logger.debug(f"🎯 SPOOFING DETECTED: {symbol} - {spoofing_direction}@{spoofing_confidence:.2f}")
            for pattern in patterns:
                raw_confidence = pattern.get("confidence", 0)
                pattern_direction = self._get_pattern_direction(pattern)
                
                # Apply direction-aware spoofing adjustment
                adjusted_confidence = self._apply_directional_spoofing_adjustment(
                    raw_confidence, 
                    pattern_direction,
                    spoofing_confidence,
                    spoofing_direction
                )
                
                pattern.update({
                    "confidence": adjusted_confidence,
                    "raw_confidence": raw_confidence,
                    "spoofing_impact": raw_confidence - adjusted_confidence,  # Positive = reduction
                    "spoofing_alignment": self._get_spoofing_alignment(pattern_direction, spoofing_direction),
                    "spoofing_adjusted": True
                })

        # Boost confidence for high-quality instruments (apply to ALL patterns)
        if quality_score > 0.7:
            quality_boost = min(0.1, (quality_score - 0.7) * 0.3)
            for pattern in patterns:
                pattern["confidence"] = min(0.95, pattern["confidence"] + quality_boost)
                pattern["quality_score"] = quality_score

        # Add quality score to all patterns regardless of boost
        for pattern in patterns:
            if "quality_score" not in pattern:
                pattern["quality_score"] = quality_score

        # HIGH-VALUE PATTERNS (Critical institutional patterns)
        patterns.extend(
            self._get_regime_allowed_patterns(self._detect_high_value_patterns(indicators))
        )
        
        # F&O SPECIFIC PATTERNS (Options and Futures patterns)
        if symbol and ("NFO:" in symbol or "BFO:" in symbol):
            fno_patterns = self._detect_fno_patterns(indicators)
            patterns.extend(self._get_regime_allowed_patterns(fno_patterns))

        # ICT patterns removed - using 8 core patterns only

        # MM trap detection and exploitation removed - using 8 core patterns only

        # Attach expected move (percent) to all patterns for alerting
        try:
            if patterns:
                self._augment_with_expected_move(patterns, indicators)
        except Exception:
            pass

        # Premium collection disabled (legacy components moved)
        pass

        # Optimized logging for pattern detection results
        if patterns and self.config.get("debug", False):
            logger.debug(
                f"✅ PATTERN DETECTOR: Found {len(patterns)} patterns for {symbol}"
            )
            for i, pattern in enumerate(patterns):
                logger.debug(
                    f"   Pattern {i + 1}: {pattern.get('pattern')} - {pattern.get('signal')} (confidence: {pattern.get('confidence', 0):.2f})"
                )
        elif self.config.get("debug", False):
            logger.debug(f"❌ PATTERN DETECTOR: No patterns detected for {symbol}")

        # Apply pattern filtering and validation
        patterns = self._filter_patterns(patterns, indicators)
        self.stats["patterns_found"] += len(patterns)
        self.patterns_detected += len(patterns)


        return patterns

    def _get_regime_allowed_patterns(
        self, patterns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Filter a list of patterns based on current unified VIX regime allowances."""
        if not patterns:
            return []
        allowed: List[Dict[str, Any]] = []
        for p in patterns:
            # Prefer explicit pattern name; fallback to type
            p_name = str(p.get("pattern") or "").lower()
            p_type = str(p.get("pattern_type") or "").lower()
            key = p_name or p_type
            if self._should_allow_pattern_type(key):
                allowed.append(p)
            else:
                self.regime_rejections += 1
                if self.config.get("debug", False):
                    logger.debug(f"Regime rejected pattern: {key}")
        return allowed

    def _augment_with_expected_move(
        self, patterns: List[Dict[str, Any]], indicators: Dict[str, Any]
    ) -> None:
        """Compute and attach expected_move (percent) to each pattern.

        Use base expected move (iv/hv/atr/fallback) and adjust by:
        - bucket_incremental_volume intensity (volume_ratio)
        - VIX regime (cached vix)
        - killzone (opening/midday/closing)
        """
        if not getattr(self, "expected_move_calculator", None):
            return
        symbol = indicators.get("symbol")
        last_price = indicators.get("last_price") or 0
        if not symbol or not last_price:
            return

        base = self.expected_move_calculator.calculate_expected_move(symbol, "daily")
        base_pct = float(base.get("expected_move_percent") or 0.0)

        # Volume adjustment: up to 2x when float(volume_ratio) >= float(4x)
        vr = float(indicators.get("volume_ratio") or 0.0)
        vol_adj = min(vr / 2.0, 2.0)

        # VIX adjustment: normalize around 15 (get real-time from Redis)
        vix_val, _ = self._get_vix_from_redis()
        vix_adj = float(vix_val) / 15.0 if vix_val else 1.0

        # Killzone adjustment
        kz_adj = 1.0
        try:
            if self.ict_detector and getattr(self.ict_detector, "killzone", None):
                kz = self.ict_detector.killzone.get_current_killzone()
                if kz:
                    zone = kz.get("zone")
                    kz_adj = {
                        "opening_drive": 1.3,
                        "midday_drift": 0.8,
                        "closing_power": 1.2,
                    }.get(zone, 1.0)
        except Exception:
            pass

        final_pct = base_pct * vol_adj * vix_adj * kz_adj

        # Attach to each pattern and keep method/confidence for transparency
        for p in patterns:
            p["expected_move"] = final_pct  # final_pct is already in percentage format (1.0 = 1%)
            p["em_method"] = base.get("method")
            p["em_confidence"] = base.get("confidence")
            p["em_upper"] = base.get("upper_bound")
            p["em_lower"] = base.get("lower_bound")
            p["em_base_percent"] = base_pct * 100.0
            p["expected_move_context"] = {
                "expected_move": base.get("expected_move"),
                "expected_move_percent": base_pct,
                "upper_bound": base.get("upper_bound"),
                "lower_bound": base.get("lower_bound"),
                "confidence": base.get("confidence"),
            }

    def get_stats(self) -> Dict[str, Any]:
        """Return pattern detector stats."""
        return dict(self.stats)

    def get_detection_stats(self) -> Dict[str, Any]:
        base = dict(self.detection_stats)
        if base.get("avg_volume_ratio_count"):
            base["avg_volume_ratio"] = (
                base["avg_volume_ratio_sum"] / base["avg_volume_ratio_count"]
            )
        base.pop("avg_volume_ratio_sum", None)
        base.pop("avg_volume_ratio_count", None)

        # Compose minimal health view
        health = {
            "total_patterns": self.patterns_detected,
            "vix_aware_adjustments": base.get("vix_aware_adjustments", 0),
            "volume_rejections": base.get("volume_rejections", 0),
            "enhanced_ratio_used": self.enhanced_ratio_used_count,
            "avg_volume_ratio": base.get("avg_volume_ratio", 0.0),
            "session_progress": self._get_session_progress(),
            "vix_current": self._get_vix_from_redis()[0],
            "fo_validation_enabled": True,
            # ICT metrics removed - using 8 core patterns only
            # Expected Move metrics
            "expected_move_metrics": getattr(
                self.expected_move_calculator, "get_health_metrics", lambda: {}
            )(),
            # Regime info
            "regime_rejections": getattr(self, "regime_rejections", 0),
            "current_regime": self.vix_regime_manager.get_current_regime()
            if getattr(self, "vix_regime_manager", None)
            else {},
            # MM stats removed - using 8 core patterns only
        }
        # ICT detector stats removed - using 8 core patterns only

        health.update(base)
        return health

    def _get_current_killzone_status(self) -> str:
        # ICT killzone removed - using 8 core patterns only
        return "none"

    def _get_session_progress(self) -> float:
        """Return fraction [0,1] of session progress (9:15 to 15:30 IST-equivalent)."""
        try:
            now = datetime.now().time()
            start = dtime(9, 15)
            end = dtime(15, 30)
            if now <= start:
                return 0.0
            if now >= end:
                return 1.0
            total = (end.hour - start.hour) * 3600 + (end.minute - start.minute) * 60
            elapsed = (
                (now.hour - start.hour) * 3600
                + (now.minute - start.minute) * 60
                + now.second
            )
            return max(0.0, min(elapsed / total, 1.0))
        except Exception:
            return 0.5

    def _get_avg_volume_20d(self, symbol: str) -> Optional[float]:
        """Return 20-day average bucket_incremental_volume using standardized OHLC stats."""
        try:
            if not self.redis_client:
                return self._get_avg_volume_20d_from_file(symbol)

            norm_symbol = normalize_symbol(symbol)

            stats_key = ohlc_stats_hash(norm_symbol)
            try:
                avg_volume = self.redis_client.get_client(2).hget(stats_key, "avg_volume_20d")  # analytics DB
                if avg_volume:
                    return float(avg_volume)
            except Exception as exc:
                self.logger.debug(
                    "Failed to read OHLC stats for %s: %s", symbol, exc
                )

            if self.volume_baseline_manager:
                baseline = self.volume_baseline_manager.get_baseline(
                    norm_symbol, "20d", 0.0
                )
                if baseline and baseline > 0:
                    return float(baseline)

            return self._get_avg_volume_20d_from_file(norm_symbol)

        except Exception as e:
            self.logger.debug(f"Failed to get 20-day average bucket_incremental_volume for {symbol}: {e}")
            return None

    def debug_volume_ratio_flow(self, symbol, stage, current_volume_ratio, new_volume_ratio=None):
        """Debug method to track bucket_incremental_volume ratio changes through the pipeline"""
        if new_volume_ratio is not None:
            self.logger.info(f"🔍 {symbol} volume_ratio changed from {current_volume_ratio} to {new_volume_ratio} at {stage}")
        else:
            self.logger.info(f"🔍 {symbol} volume_ratio is {current_volume_ratio} at {stage}")

    def _get_bucket_volume_baseline(self, symbol: str, bucket_label: str = "5min") -> float:
        """Get bucket-specific volume baseline using time-aware baselines"""
        try:
            # Use time-aware baseline for volume calculations
            if self.time_aware_baseline:
                expected_volume = self.time_aware_baseline.get_expected_volume(symbol)
                if expected_volume > 0:
                    return expected_volume
                    
            # Get 20-day average volume as fallback
            avg_volume_20d = self._get_avg_volume_20d(symbol)
            return avg_volume_20d or 0.0
            
        except Exception as e:
            self.logger.debug(f"Failed to get volume baseline for {symbol}: {e}")
            return 0.0

    def _get_avg_volume_20d_from_file(self, symbol: str) -> Optional[float]:
        """SINGLE SOURCE OF TRUTH: Get 20-day average bucket_incremental_volume from config/volume_averages_20d.json"""
        try:
            # Try the main bucket_incremental_volume data file
            volume_file = "config/volume_averages_20d.json"
            if os.path.exists(volume_file):
                with open(volume_file, 'r') as f:
                    data = json.load(f)
                    if symbol in data:
                        return float(data[symbol].get("avg_volume_20d", 0))
            
            # Try the extracted data file
            extracted_file = "extracted_data/volume_data_20251005_160204.json"
            if os.path.exists(extracted_file):
                with open(extracted_file, 'r') as f:
                    data = json.load(f)
                    if symbol in data:
                        return float(data[symbol].get("avg_volume_20d", 0))
            
            return None
        except Exception as e:
            logger.debug(f"Failed to get bucket_incremental_volume data from file for {symbol}: {e}")
            return None



    def _calculate_enhanced_volume_ratio_core(
        self,
        bucket_incremental_volume: float,  # Incremental volume from centralized state manager
        bucket_cumulative_volume: float,    # Cumulative volume from centralized state manager
        avg_volume_20d: Optional[float],
        session_progress: Optional[float],
    ) -> float:
        """SINGLE SOURCE OF TRUTH: Use hybrid bucket_incremental_volume calculation with bucket + time-aware baselines"""
        try:
            symbol = getattr(self, 'current_symbol', 'UNKNOWN')
            
            # Try bucket-specific baseline first
            bucket_baseline = self._get_bucket_volume_baseline(symbol, "5min")
            if bucket_baseline > 0:
                return bucket_incremental_volume / bucket_baseline
            
            # Fallback to time-aware baseline
            from intraday_scanner.calculations import calculate_volume_ratio
            return calculate_volume_ratio(symbol, bucket_incremental_volume, avg_volume_20d or 1.0)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate enhanced bucket_incremental_volume ratio: {e}")
            return 0.0

    def _get_vix_from_redis(self):
        """Get real-time VIX data from Redis using the correct VIX utilities"""
        try:
            # Use the existing VIX utilities which handle Redis keys correctly
            vix_value = get_vix_value()
            vix_regime = get_vix_regime()
            
            return vix_value, vix_regime
        except Exception as e:
            logger.debug(f"Failed to get VIX from Redis: {e}")
            return None, None

    def _fo_min_volume_ok(self, symbol: str, indicators: Dict[str, Any]) -> bool:
        """Basic F&O bucket_incremental_volume validation using cumulative bucket_incremental_volume and historical averages from Redis.

        Rejects bucket_incremental_volume-based patterns early for low-activity F&O symbols.
        """
        try:
            if not symbol.startswith("NFO:"):
                return True

            # Use optimized fields instead of redundant ones
            bucket_incremental_volume = self._get_incremental_volume(indicators, 0.0)
            bucket_cumulative_volume = self._get_cumulative_volume(indicators, 0.0)

            min_required = 0
            avg20 = self._get_avg_volume_20d(symbol)
            if avg20 and avg20 > 0:
                min_required = max(min_required, float(avg20) * 0.10)

            # Heuristic minimum lots if we can't fetch averages
            if min_required <= 0:
                min_required = 100  # very minimal baseline to avoid noise

            return float(bucket_incremental_volume or 0) >= min_required
        except Exception:
            return True

    def _get_current_vix_regime(self) -> str:
        """Get current VIX regime using proper VIX utilities"""
        try:
            from utils.vix_utils import get_vix_regime
            return get_vix_regime()
        except Exception:
            return "NORMAL"  # Default fallback
    
    def _get_current_vix_value(self) -> float:
        """Get current VIX value using proper VIX utilities"""
        try:
            from utils.vix_utils import get_vix_value
            return get_vix_value() or 15.0  # Default to 15.0 if None
        except Exception:
            return 15.0  # Default fallback

    def _get_recent_volumes(self, symbol: str, resolution: str, limit: int = 20) -> List[float]:
        """Get recent volume history for statistical analysis"""
        try:
            # This would typically fetch from Redis or historical data
            # For now, return empty list - would be implemented based on your data storage
            return []
        except Exception:
            return []
    
    def calculate_expected_move(self, pattern_type: str, entry_price: float, indicators: Dict) -> Dict:
        """Calculate expected move based on pattern type and market conditions"""
        
        atr = indicators.get('atr', entry_price * 0.015)  # Default 1.5% if ATR missing
        vix_regime = self._get_current_vix_regime()
        volume_ratio = indicators.get('volume_ratio', 1.0)
        rsi = indicators.get('rsi', 50)
        
        # Base expected move by pattern type
        base_multipliers = {
            'volume_spike': 2.0,
            'volume_breakout': 2.2,
            'volume_price_divergence': 1.8,
            'upside_momentum': 1.5,
            'downside_momentum': 1.5,
            'breakout': 2.5,
            'reversal': 2.0,
            'hidden_accumulation': 1.2,
            'ict_liquidity_pools': 2.0,
            'ict_fair_value_gaps': 1.8,
            'ict_optimal_trade_entry': 1.5,
            'ict_premium_discount': 1.2,
            'ict_killzone': 2.2,
            'ict_momentum': 1.8,
            'iv_crush_play_straddle': 0.8,  # Smaller moves for IV crush
            'range_bound_strangle': 0.6,    # Even smaller for range-bound
            'market_maker_trap_detection': 2.5,  # Large moves for traps
            'premium_collection_strategy': 0.4,  # Small moves for premium collection
            'kow_signal_straddle': 1.0
        }
        
        # VIX regime adjustments
        vix_multipliers = {
            'PANIC': 1.8,
            'HIGH': 1.4,
            'NORMAL': 1.0,
            'LOW': 0.7
        }
        
        # Volume strength adjustments using centralized calculation
        # Use volume confidence to determine multiplier dynamically
        volume_confidence = PatternMathematics.calculate_volume_confidence(
            volume_ratio=volume_ratio,
            threshold=2.0,  # Base threshold
            base_confidence=0.5
        )
        volume_multiplier = 0.8 + (volume_confidence * 0.7)  # Range: 0.8 to 1.5
        
        # RSI adjustments (extremes get smaller expected moves)
        rsi_multiplier = 1.0
        if rsi > 80 or rsi < 20:
            rsi_multiplier = 0.6  # Overbought/oversold = smaller moves
        elif 40 <= rsi <= 60:
            rsi_multiplier = 1.2  # Mid-range = larger moves
        
        # Calculate final expected move
        base_move = atr * base_multipliers.get(pattern_type, 1.5)
        vix_adjusted = base_move * vix_multipliers.get(vix_regime, 1.0)
        volume_adjusted = vix_adjusted * volume_multiplier
        final_move_points = volume_adjusted * rsi_multiplier
        
        # Convert to percentage
        move_percent = (final_move_points / entry_price) * 100
        
        # Use centralized confidence calculation instead of hardcoded formula
        confidence = PatternMathematics.calculate_bounded_confidence(
            volume_ratio=volume_ratio,
            pattern_type=pattern_type,
            indicators=indicators
        )
        
        return {
            'points': final_move_points,
            'percent': move_percent,
            'direction': self._get_pattern_direction(pattern_type, indicators),
            'timeframe': self._get_pattern_timeframe(pattern_type),
            'confidence': confidence  # Use centralized confidence calculation
        }

    def _get_pattern_direction(self, pattern_type: str, indicators: Dict) -> str:
        """Determine expected move direction"""
        if any(x in pattern_type for x in ['upside', 'buy', 'long', 'bullish']):
            return 'UP'
        elif any(x in pattern_type for x in ['downside', 'sell', 'short', 'bearish']):
            return 'DOWN'
        else:
            # Default to signal direction or price momentum
            price_change = indicators.get('price_change', 0)
            return 'UP' if price_change > 0 else 'DOWN'

    def _get_pattern_timeframe(self, pattern_type: str) -> str:
        """Get typical timeframe for pattern completion"""
        timeframes = {
            'volume_spike': '15-30min',
            'volume_breakout': '30-60min', 
            'momentum': '45-90min',
            'breakout': '60-120min',
            'reversal': '90-180min',
            'hidden_accumulation': '120-240min',
            'ict_patterns': '30-90min',
            'straddle_strategies': '1-3 days'
        }
        
        for key, timeframe in timeframes.items():
            if key in pattern_type:
                return timeframe
    def _get_risk_adjusted_confidence_requirement(self, pattern_type: str) -> float:
        """
        Get risk-adjusted confidence requirement for a pattern
        
        Args:
            pattern_type: Pattern type
            
        Returns:
            Minimum confidence requirement based on pattern risk
        """
        try:
            from config.thresholds import get_risk_adjusted_confidence_requirement
            return get_risk_adjusted_confidence_requirement(pattern_type)
        except Exception as e:
            self.logger.error(f"Error getting risk-adjusted confidence requirement: {e}")
            return 0.70  # Default to medium risk requirement
    
    print("\\n✅ Consolidated Pattern Detector test completed!")
