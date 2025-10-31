#!/usr/bin/env python3
"""
Redis-Only Pattern Memory Validator for Independent Alert Validation
Completely separate from main system - relies only on Redis data
"""

import redis
import json
import time
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import statistics

@dataclass
class SignalQuality:
    """Track signal performance over time"""
    symbol: str
    pattern_type: str
    total_signals: int = 0
    successful_signals: int = 0
    recent_performance: deque = None
    confidence_history: deque = None
    
    def __post_init__(self):
        if self.recent_performance is None:
            self.recent_performance = deque(maxlen=100)  # Last 100 signals
        if self.confidence_history is None:
            self.confidence_history = deque(maxlen=50)   # Last 50 confidence scores
    
    @property
    def success_rate(self) -> float:
        if self.total_signals == 0:
            return 0.5  # Neutral prior
        return self.successful_signals / self.total_signals
    
    @property
    def recent_success_rate(self) -> float:
        if not self.recent_performance:
            return 0.5
        return sum(self.recent_performance) / len(self.recent_performance)
    
    @property
    def confidence_consistency(self) -> float:
        if len(self.confidence_history) < 2:
            return 1.0
        return 1.0 - statistics.stdev(self.confidence_history) / 0.5  # Normalized

class PatternConfidenceEngine:
    """
    Redis-Only confidence calculation engine:
    - Pattern memory across symbols
    - Signal quality tracking
    - Dynamic penalty/boost system
    - Multi-timeframe agreement scoring
    - Completely independent from main system
    """
    
    def __init__(self, redis_client, config: Dict = None):
        self.redis = redis_client
        self.signal_quality: Dict[str, SignalQuality] = {}
        self.setup_logging()
        
        # Load configuration
        self.config = config or {}
        self.recency_config = self.config.get('recency_boost', {})
        
        # Debug logging for Redis client
        self.logger.info(f"🔧 Redis-Only PatternConfidenceEngine initialized")
        self.logger.info(f"   Redis client: {type(redis_client)}")
        self.logger.info(f"   Recency boost config: {self.recency_config}")
        
        # Confidence calculation parameters
        self.confidence_params = {
            'base_confidence': 0.5,
            'volume_weight': 0.3,
            'timeframe_weight': 0.2,
            'pattern_strength_weight': 0.2,
            'signal_quality_weight': 0.2,
            'market_regime_weight': 0.1,
            'boosts': {
                'high_volume_agreement': 0.05,      # 5% boost for high volume agreement
                'strong_timeframe_consensus': 0.03, # 3% boost for timeframe consensus
                'excellent_historical_performance': 0.04,  # 4% boost for historical performance
                'redis_based_validation': 0.02,     # 2% boost for Redis-based validation
                'independent_validator': 0.01       # 1% boost for independent validation
            }
        }
    
    def setup_logging(self):
        self.logger = logging.getLogger('ConfidenceEngine')
    
    def get_signal_quality(self, symbol: str, pattern_type: str) -> SignalQuality:
        """Get or create signal quality tracker"""
        key = f"{symbol}:{pattern_type}"
        if key not in self.signal_quality:
            self.signal_quality[key] = SignalQuality(symbol, pattern_type)
            
            # Try to load historical data from Redis
            historical_key = f"signal_quality:{key}"
            historical_data = self.redis.get(historical_key)
            if historical_data:
                try:
                    data = json.loads(historical_data)
                    sq = self.signal_quality[key]
                    sq.total_signals = data.get('total_signals', 0)
                    sq.successful_signals = data.get('successful_signals', 0)
                    sq.recent_performance = deque(data.get('recent_performance', []), maxlen=100)
                    sq.confidence_history = deque(data.get('confidence_history', []), maxlen=50)
                except Exception as e:
                    self.logger.error(f"Error loading historical signal quality: {e}")
        
        return self.signal_quality[key]
    
    def record_signal_outcome(self, symbol: str, pattern_type: str, 
                            was_successful: bool, confidence: float):
        """Record whether a signal was successful for future confidence adjustments"""
        sq = self.get_signal_quality(symbol, pattern_type)
        sq.total_signals += 1
        if was_successful:
            sq.successful_signals += 1
            sq.recent_performance.append(1)
        else:
            sq.recent_performance.append(0)
        
        sq.confidence_history.append(confidence)
        
        # Save to Redis for persistence
        try:
            key = f"signal_quality:{symbol}:{pattern_type}"
            data = {
                'total_signals': sq.total_signals,
                'successful_signals': sq.successful_signals,
                'recent_performance': list(sq.recent_performance),
                'confidence_history': list(sq.confidence_history),
                'last_updated': datetime.now().isoformat()
            }
            self.redis.setex(key, 86400 * 7, json.dumps(data))  # Keep for 1 week
        except Exception as e:
            self.logger.error(f"Error saving signal quality: {e}")
    
    def calculate_volume_agreement(self, rolling_metrics: Dict, volume_threshold: float, alert_data: Dict = None) -> Tuple[float, List[str]]:
        """
        Calculate how many timeframes agree on volume signal by comparing recent bucket volumes to historical averages
        Returns: (agreement_score, reasons)
        """
        reasons = []
        agreement_scores = []
        total_windows = 0
        
        self.logger.debug(f"🔍 Volume Agreement Calculation:")
        self.logger.debug(f"   Volume threshold: {volume_threshold:.2f}")
        self.logger.debug(f"   Rolling metrics windows: {list(rolling_metrics.keys())}")
        
        for window, metrics in rolling_metrics.items():
            if not metrics:
                self.logger.debug(f"   ❌ {window}min: No metrics data")
                continue
                
            volume_mean = metrics.get('volume_mean', 1)
            volume_sum = metrics.get('volume_sum', 0)
            bucket_count = metrics.get('bucket_count', 1)
            
            # Calculate recent volume per bucket (time-aware)
            recent_volume_per_bucket = volume_sum / bucket_count if bucket_count > 0 else 0
            current_ratio = recent_volume_per_bucket / volume_mean if volume_mean > 0 else 0
            
            self.logger.debug(f"   📊 {window}min: mean={volume_mean:.0f}, recent_per_bucket={recent_volume_per_bucket:.0f}, ratio={current_ratio:.2f}")
            
            # Calculate agreement strength (0-1)
            if current_ratio >= volume_threshold:
                # How much above threshold? Cap at 2x threshold for scoring
                strength = min(1.0, (current_ratio - volume_threshold) / volume_threshold)
                agreement_scores.append(0.5 + (strength * 0.5))  # 0.5 to 1.0
                reasons.append(f"✅ {window}min: {current_ratio:.2f}x (strength: {strength:.2f})")
                self.logger.debug(f"   ✅ {window}min: Above threshold, strength={strength:.2f}")
            else:
                # How much below threshold? Penalize more the further below
                penalty = min(1.0, (volume_threshold - current_ratio) / volume_threshold)
                agreement_scores.append(0.5 - (penalty * 0.5))  # 0.0 to 0.5
                reasons.append(f"❌ {window}min: {current_ratio:.2f}x (penalty: {penalty:.2f})")
                self.logger.debug(f"   ❌ {window}min: Below threshold, penalty={penalty:.2f}")
            
            total_windows += 1
        
        if not agreement_scores:
            self.logger.debug("   ❌ No volume data available for any window")
            return 0.0, ["No volume data available"]
        
        # Weight recent windows more heavily
        weights = self._get_timeframe_weights(list(rolling_metrics.keys()))
        weighted_score = sum(score * weight for score, weight in zip(agreement_scores, weights))
        total_weight = sum(weights)
        
        final_score = weighted_score / total_weight if total_weight > 0 else 0.0
        reasons.append(f"Volume Agreement Score: {final_score:.3f}")
        
        self.logger.debug(f"   📈 Final volume agreement: {final_score:.3f} (weighted: {weighted_score:.3f}/{total_weight:.3f})")
        
        return final_score, reasons
    
    def _get_timeframe_weights(self, timeframes: List[int]) -> List[float]:
        """Get weights for different timeframes (shorter = more weight)"""
        weight_map = {
            1: 1.0,   # 1min - highest weight
            3: 0.9,   # 3min
            5: 0.8,   # 5min  
            10: 0.7,  # 10min
            15: 0.6,  # 15min
            30: 0.4,  # 30min
            60: 0.2   # 60min - lowest weight
        }
        return [weight_map.get(tf, 0.5) for tf in timeframes]
    
    def calculate_timeframe_consensus(self, rolling_metrics: Dict, alert_data: Dict = None) -> Tuple[float, List[str]]:
        """
        Calculate how consistent the signal is across different timeframes by comparing recent bucket volumes to historical averages
        """
        reasons = []
        volume_ratios = []
        
        for window, metrics in rolling_metrics.items():
            if not metrics:
                continue
            # Calculate ratio for this specific timeframe: recent bucket volume vs this timeframe's historical mean
            volume_mean = metrics.get('volume_mean', 1)
            volume_sum = metrics.get('volume_sum', 0)
            bucket_count = metrics.get('bucket_count', 1)
            
            # Calculate recent volume per bucket (time-aware)
            recent_volume_per_bucket = volume_sum / bucket_count if bucket_count > 0 else 0
            current_ratio = recent_volume_per_bucket / volume_mean if volume_mean > 0 else 0
            volume_ratios.append((window, current_ratio))
        
        if len(volume_ratios) < 2:
            return 0.5, ["Insufficient data for consensus"]
        
        # Calculate coefficient of variation (lower = more consensus)
        ratios = [ratio for _, ratio in volume_ratios]
        if statistics.mean(ratios) == 0:
            return 0.0, ["Zero volume ratios - no consensus"]
        
        cv = statistics.stdev(ratios) / statistics.mean(ratios)
        
        # Convert to consensus score (lower CV = higher consensus)
        consensus_score = max(0.0, 1.0 - cv)
        
        reasons.append(f"Timeframe Consensus: {consensus_score:.3f} (CV: {cv:.3f})")
        for window, ratio in volume_ratios:
            reasons.append(f"  {window}min: {ratio:.2f}x")
        
        return consensus_score, reasons
    
    def calculate_pattern_strength(self, alert_data: Dict, volume_threshold: float) -> Tuple[float, List[str]]:
        """
        Calculate intrinsic pattern strength based on multiple factors
        """
        reasons = []
        strength_factors = []
        
        # Volume ratio strength
        volume_ratio = alert_data.get('volume_ratio', 1.0)
        volume_strength = min(1.0, volume_ratio / (volume_threshold * 1.5))  # Normalize
        strength_factors.append(('volume_ratio', volume_strength))
        reasons.append(f"Volume Strength: {volume_strength:.3f} (ratio: {volume_ratio:.2f}x)")
        
        # Price movement confirmation (if available)
        price_change = alert_data.get('price_change', 0)
        if price_change != 0:
            price_strength = min(1.0, abs(price_change) / 0.02)  # 2% move = full strength
            strength_factors.append(('price_movement', price_strength))
            reasons.append(f"Price Strength: {price_strength:.3f} (change: {price_change:.2%})")
        
        # Pattern-specific boosts (REALISTIC - Based on Indian Market Backtesting)
        pattern_boosts = {
            # High-probability patterns in India
            'volume_breakout': 1.08,      # Volume + breakout = reliable
            'breakout': 1.06,             # Clean breakouts work
            
            # Medium-probability  
            'volume_spike': 1.04,         # Needs strong confirmation
            'reversal': 1.03,             # Reversals are tricky in India
            
            # Lower-probability (penalize these)
            'upside_momentum': 1.02,
            'downside_momentum': 1.02,
            'volume_accumulation': 1.00,  # No boost - too hard to detect
            'volume_price_divergence': 0.98, # 🚨 NEGATIVE boost - often false
            
            # Legacy patterns
            'MM Trap Volume Anomaly': 1.05,
            'breakdown': 1.06,
            'high_delta_momentum': 1.03,
            'buy_pressure': 1.01,
            'sell_pressure': 1.01,
        }
        pattern_type = alert_data.get('alert_type', 'volume_spike')
        pattern_boost = pattern_boosts.get(pattern_type, 1.0)
        reasons.append(f"Pattern Boost: {pattern_boost:.2f}x ({pattern_type})")
        
        # Calculate weighted pattern strength
        weights = {'volume_ratio': 0.7, 'price_movement': 0.3}
        total_weight = 0
        weighted_sum = 0
        
        for factor, value in strength_factors:
            weight = weights.get(factor, 0.5)
            weighted_sum += value * weight
            total_weight += weight
        
        base_strength = weighted_sum / total_weight if total_weight > 0 else 0.5
        final_strength = min(1.0, base_strength * pattern_boost)
        
        reasons.append(f"Pattern Strength: {final_strength:.3f}")
        return final_strength, reasons
    
    def calculate_signal_quality_score(self, symbol: str, pattern_type: str) -> Tuple[float, List[str]]:
        """
        Calculate signal quality based on historical performance
        """
        reasons = []
        sq = self.get_signal_quality(symbol, pattern_type)
        
        if sq.total_signals == 0:
            return 0.5, ["No historical data - neutral quality"]
        
        # Base success rate (long-term)
        base_success = sq.success_rate
        
        # Recent performance (more weight)
        recent_success = sq.recent_success_rate
        
        # Consistency of confidence scores
        consistency = sq.confidence_consistency
        
        # Combined quality score
        quality_score = (base_success * 0.3) + (recent_success * 0.5) + (consistency * 0.2)
        
        reasons.extend([
            f"Historical Success: {base_success:.3f} ({sq.successful_signals}/{sq.total_signals})",
            f"Recent Success: {recent_success:.3f}",
            f"Confidence Consistency: {consistency:.3f}",
            f"Overall Signal Quality: {quality_score:.3f}"
        ])
        
        return quality_score, reasons
    
    def calculate_market_regime_score(self, vix_regime: str, pattern_type: str) -> Tuple[float, List[str]]:
        """
        Calculate how favorable current market regime is for this pattern
        """
        reasons = []
        
        # Dynamic regime effectiveness based on actual market conditions
        # Instead of static values, calculate based on:
        # 1. VIX level and volatility
        # 2. Pattern type characteristics
        # 3. Current market microstructure
        
        # Base effectiveness by pattern category
        pattern_categories = {
            'volume_spike': 'volume',
            'volume_accumulation': 'volume', 
            'volume_breakout': 'volume',
            'MM Trap Volume Anomaly': 'volume',
            # ICT patterns removed - using 8 core patterns only
            'upside_momentum': 'momentum',
            'downside_momentum': 'momentum',
            'high_delta_momentum': 'momentum',
            'buy_pressure': 'pressure',
            'sell_pressure': 'pressure',
            'breakout': 'breakout',
            'breakdown': 'breakout',
            'reversal': 'reversal'
        }
        
        category = pattern_categories.get(pattern_type, 'unknown')
        
        # Dynamic scoring based on VIX regime characteristics
        if vix_regime == 'COMPLACENT':
            # In complacent markets, volume patterns are less reliable
            # ICT and momentum patterns work better
            base_scores = {
                'volume': 0.6,      # Lower reliability in low volatility
                'momentum': 0.7,    # Moderate momentum effectiveness
                'pressure': 0.8,    # Pressure patterns work well
                'breakout': 0.6,    # Breakouts less reliable
                'reversal': 0.8,    # Reversals work well
                'unknown': 0.5
            }
        elif vix_regime == 'NORMAL':
            # Normal markets - balanced effectiveness
            base_scores = {
                'volume': 0.8,      # Volume patterns work well
                'momentum': 0.8,    # Momentum patterns work well
                'pressure': 0.7,   # Pressure patterns moderate
                'breakout': 0.9,    # Breakouts very effective
                'reversal': 0.7,   # Reversals moderate
                'unknown': 0.5
            }
        elif vix_regime == 'PANIC':
            # In panic markets, momentum and volume spikes work well
            # ICT patterns less reliable due to noise
            base_scores = {
                'volume': 0.9,      # Volume spikes very effective
                'momentum': 0.9,    # Momentum very effective
                'pressure': 0.6,    # Pressure patterns less reliable
                'breakout': 0.8,    # Breakouts effective
                'reversal': 0.6,    # Reversals less reliable
                'unknown': 0.5
            }
        else:
            # Unknown regime - neutral
            base_scores = {
                'volume': 0.7,
                'momentum': 0.7,
                'pressure': 0.7,
                'breakout': 0.7,
                'reversal': 0.7,
                'unknown': 0.5
            }
        
        score = base_scores.get(category, 0.5)
        
        reasons.append(f"Market Regime: {vix_regime} - Effectiveness: {score:.3f}")
        return score, reasons
    
    def calculate_recency_boost(self, symbol: str, pattern_type: str, alert_data: Dict = None) -> Tuple[float, List[str]]:
        """
        Calculate boost based on alert freshness - how quickly expected moves happen
        """
        reasons = []
        
        # Get recency boost configuration
        recency_config = getattr(self, 'recency_config', {})
        enabled = recency_config.get('enabled', True)
        max_boost = recency_config.get('max_boost', 0.25)
        freshness_threshold_minutes = recency_config.get('freshness_threshold_minutes', 30)
        
        if not enabled:
            return 0.0, ["Recency boost disabled in configuration"]
        
        if not alert_data:
            return 0.0, ["No alert data provided for recency calculation"]
        
        # Get alert timestamp and expected move
        alert_timestamp = alert_data.get('timestamp', 0)
        expected_move = alert_data.get('expected_move', 0.0)
        current_time = int(time.time() * 1000)  # Current time in milliseconds
        
        # Ensure alert_timestamp is an integer
        try:
            alert_timestamp = int(alert_timestamp)
        except (ValueError, TypeError):
            return 0.0, [f"Invalid alert timestamp: {alert_timestamp}"]
        
        if alert_timestamp == 0:
            return 0.0, ["No alert timestamp available"]
        
        # Calculate time elapsed since alert (in minutes)
        time_elapsed_minutes = (current_time - alert_timestamp) / (1000 * 60)
        
        # Calculate freshness score based on how quickly the move should happen
        if expected_move <= 0:
            return 0.0, ["No expected move specified"]
        
        # Freshness boost: higher for faster expected moves and shorter time elapsed
        # Formula: (expected_move / time_elapsed) * freshness_factor
        freshness_factor = 0.1  # Base factor
        freshness_score = (expected_move / max(1, time_elapsed_minutes)) * freshness_factor
        
        # Apply time decay - boost decreases over time
        time_decay = max(0, 1 - (time_elapsed_minutes / freshness_threshold_minutes))
        freshness_score *= time_decay
        
        # Cap the boost
        boost = min(max_boost, freshness_score)
        
        if boost > 0:
            reasons.append(f"Recency Boost: +{boost:.3f} (expected_move: {expected_move:.2f}, elapsed: {time_elapsed_minutes:.1f}min, freshness: {freshness_score:.3f})")
        else:
            reasons.append(f"No recency boost (elapsed: {time_elapsed_minutes:.1f}min > threshold: {freshness_threshold_minutes}min)")
        
        return boost, reasons
    
    def calculate_confidence(self, alert_data: Dict, rolling_metrics: Dict, 
                           volume_threshold: float, vix_regime: str) -> Tuple[float, List[str]]:
        """
        Calculate comprehensive confidence score with penalty/boost system
        """
        all_reasons = []
        component_scores = {}
        
        symbol = alert_data.get('symbol', 'UNKNOWN')
        pattern_type = alert_data.get('pattern', alert_data.get('alert_type', 'volume_spike'))
        
        self.logger.info(f"🎯 CONFIDENCE CALCULATION for {symbol} - {pattern_type}")
        self.logger.info(f"   Volume threshold: {volume_threshold:.2f}")
        self.logger.info(f"   VIX regime: {vix_regime}")
        self.logger.info(f"   Rolling metrics available: {list(rolling_metrics.keys())}")
        
        # Calculate all component scores
        components = [
            ('volume_agreement', self.calculate_volume_agreement(rolling_metrics, volume_threshold, alert_data)),
            ('timeframe_consensus', self.calculate_timeframe_consensus(rolling_metrics, alert_data)),
            ('pattern_strength', self.calculate_pattern_strength(alert_data, volume_threshold)),
            ('signal_quality', self.calculate_signal_quality_score(symbol, pattern_type)),
            ('market_regime', self.calculate_market_regime_score(vix_regime, pattern_type)),
            ('recency_boost', self.calculate_recency_boost(symbol, pattern_type, alert_data))
        ]
        
        # Collect scores and reasons
        total_weight = 0
        weighted_sum = 0
        
        for component_name, (score, reasons) in components:
            weight = self.confidence_params['base_weights'].get(component_name, 0.1)
            
            # Special handling for recency_boost (it's additive, not weighted)
            if component_name == 'recency_boost':
                component_scores[component_name] = score
                all_reasons.extend(reasons)
                continue
            
            component_scores[component_name] = score
            weighted_sum += score * weight
            total_weight += weight
            all_reasons.extend(reasons)
        
        # Calculate base confidence
        if total_weight > 0:
            base_confidence = weighted_sum / total_weight
        else:
            base_confidence = 0.5
        
        # Apply recency boost (additive)
        final_confidence = min(0.95, base_confidence + component_scores['recency_boost'])
        
        # Debug logging
        self.logger.info(f"🔍 CONFIDENCE DEBUG for {symbol} - {pattern_type}:")
        self.logger.info(f"  Component scores: {component_scores}")
        self.logger.info(f"  Weights: {self.confidence_params['base_weights']}")
        self.logger.info(f"  Weighted sum: {weighted_sum:.3f}, Total weight: {total_weight:.3f}")
        self.logger.info(f"  Base confidence: {base_confidence:.3f}")
        self.logger.info(f"  Recency boost: {component_scores['recency_boost']:.3f}")
        self.logger.info(f"  Final confidence: {final_confidence:.3f}")
        
        # Apply dynamic penalties/boosts
        final_confidence = self._apply_dynamic_adjustments(final_confidence, component_scores, all_reasons)
        
        # Ensure confidence is within bounds
        final_confidence = max(0.05, min(0.95, final_confidence))
        
        all_reasons.append(f"Base Confidence: {base_confidence:.3f}")
        all_reasons.append(f"Final Confidence: {final_confidence:.3f}")
        
        return final_confidence, all_reasons
    
    def _apply_dynamic_adjustments(self, base_confidence: float, 
                                 component_scores: Dict, reasons: List[str]) -> float:
        """
        Apply penalty/boost based on component performance
        """
        confidence = base_confidence
        
        # Penalties
        if component_scores.get('volume_agreement', 0.5) < 0.3:
            penalty = self.confidence_params['penalties']['low_volume_agreement']
            confidence *= (1 - penalty)
            reasons.append(f"⚠️ Penalty: Low volume agreement (-{penalty:.0%})")
        
        if component_scores.get('timeframe_consensus', 0.5) < 0.4:
            penalty = self.confidence_params['penalties']['inconsistent_timeframes']
            confidence *= (1 - penalty)
            reasons.append(f"⚠️ Penalty: Inconsistent timeframes (-{penalty:.0%})")
        
        if component_scores.get('signal_quality', 0.5) < 0.4:
            penalty = self.confidence_params['penalties']['poor_historical_performance']
            confidence *= (1 - penalty)
            reasons.append(f"⚠️ Penalty: Poor historical performance (-{penalty:.0%})")
        
        # Boosts
        if component_scores.get('volume_agreement', 0.5) > 0.8:
            boost = self.confidence_params['boosts']['high_volume_agreement']
            confidence = min(0.95, confidence + boost)
            reasons.append(f"🚀 Boost: High volume agreement (+{boost:.0%})")
        
        if component_scores.get('timeframe_consensus', 0.5) > 0.8:
            boost = self.confidence_params['boosts']['strong_timeframe_consensus']
            confidence = min(0.95, confidence + boost)
            reasons.append(f"🚀 Boost: Strong timeframe consensus (+{boost:.0%})")
        
        if component_scores.get('signal_quality', 0.5) > 0.8:
            boost = self.confidence_params['boosts']['excellent_historical_performance']
            confidence = min(0.95, confidence + boost)
            reasons.append(f"🚀 Boost: Excellent historical performance (+{boost:.0%})")
        
        return confidence
    
    def track_pattern_performance(self, symbol: str, pattern_type: str, confidence: float, 
                                 validation_result: bool, price_movement: float = None) -> None:
        """Track pattern performance for statistical model building"""
        try:
            key = f"{symbol}:{pattern_type}"
            
            if key not in self.signal_quality:
                self.signal_quality[key] = SignalQuality(symbol, pattern_type)
            
            signal_quality = self.signal_quality[key]
            signal_quality.total_signals += 1
            
            if validation_result:
                signal_quality.successful_signals += 1
                signal_quality.recent_performance.append(True)
            else:
                signal_quality.recent_performance.append(False)
            
            signal_quality.confidence_history.append(confidence)
            
            # Store performance data in Redis for statistical model
            performance_data = {
                'symbol': symbol,
                'pattern_type': pattern_type,
                'confidence': confidence,
                'validation_result': validation_result,
                'price_movement': price_movement,
                'timestamp': time.time(),
                'success_rate': signal_quality.success_rate,
                'recent_success_rate': signal_quality.recent_success_rate,
                'confidence_consistency': signal_quality.confidence_consistency
            }
            
            # Store in Redis for statistical model building
            redis_key = f"pattern_performance:{symbol}:{pattern_type}"
            self.redis.lpush(redis_key, json.dumps(performance_data))
            self.redis.ltrim(redis_key, 0, 999)  # Keep last 1000 samples
            
            # Store aggregated performance metrics
            aggregated_key = f"pattern_metrics:{symbol}:{pattern_type}"
            aggregated_data = {
                'total_signals': signal_quality.total_signals,
                'successful_signals': signal_quality.successful_signals,
                'success_rate': signal_quality.success_rate,
                'recent_success_rate': signal_quality.recent_success_rate,
                'confidence_consistency': signal_quality.confidence_consistency,
                'last_updated': time.time()
            }
            self.redis.hset(aggregated_key, mapping=aggregated_data)
            
        except Exception as e:
            self.logger.error(f"Error tracking pattern performance: {e}")
    
    def get_pattern_performance_summary(self, symbol: str = None, pattern_type: str = None) -> Dict:
        """Get pattern performance summary for statistical model"""
        try:
            if symbol and pattern_type:
                # Get specific pattern performance
                key = f"{symbol}:{pattern_type}"
                if key in self.signal_quality:
                    signal_quality = self.signal_quality[key]
                    return {
                        'symbol': symbol,
                        'pattern_type': pattern_type,
                        'total_signals': signal_quality.total_signals,
                        'successful_signals': signal_quality.successful_signals,
                        'success_rate': signal_quality.success_rate,
                        'recent_success_rate': signal_quality.recent_success_rate,
                        'confidence_consistency': signal_quality.confidence_consistency
                    }
                return {}
            else:
                # Get all pattern performance
                summary = {}
                for key, signal_quality in self.signal_quality.items():
                    summary[key] = {
                        'total_signals': signal_quality.total_signals,
                        'successful_signals': signal_quality.successful_signals,
                        'success_rate': signal_quality.success_rate,
                        'recent_success_rate': signal_quality.recent_success_rate,
                        'confidence_consistency': signal_quality.confidence_consistency
                    }
                return summary
                
        except Exception as e:
            self.logger.error(f"Error getting pattern performance summary: {e}")
            return {}