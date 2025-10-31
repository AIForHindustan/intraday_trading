"""
MATHEMATICAL INTEGRITY ENGINE FOR ALL PATTERNS
Ensures consistent confidence bounds, risk integration, and outlier protection
"""
import logging
from typing import Any, Dict, Optional, Tuple, Union
from math import isnan

logger = logging.getLogger(__name__)

class PatternMathematics:
    """
    Centralized mathematical integrity for all 19 patterns
    """
    
    # VIX-aware RSI thresholds
    RSI_THRESHOLDS = {
        "HIGH_VIX": {"oversold": 25, "overbought": 75, "neutral_low": 40, "neutral_high": 60},
        "NORMAL": {"oversold": 30, "overbought": 70, "neutral_low": 45, "neutral_high": 65},
        "LOW_VIX": {"oversold": 35, "overbought": 65, "neutral_low": 50, "neutral_high": 60}
    }
    
    @staticmethod
    def calculate_bounded_confidence(base_confidence: float, strength_bonus: float, 
                                   max_bonus: float = 0.3) -> float:
        """Calculate confidence with proper 0.0-1.0 bounds"""
        confidence = base_confidence + min(strength_bonus, max_bonus)
        return max(0.0, min(1.0, confidence))
    
    @staticmethod
    def protect_outliers(value: float, max_value: float = 10.0) -> float:
        """Protect against extreme outlier values"""
        if value is None:
            return 1.0
        if isnan(value):
            return 1.0
        return min(value, max_value)
    
    @staticmethod
    def get_vix_aware_rsi_thresholds(
        vix_regime: str,
        threshold_type: str,
    ) -> Union[float, Tuple[float, float]]:
        """
        Return RSI thresholds adjusted for VIX regime.

        Args:
            vix_regime: Regime key such as HIGH_VIX, NORMAL, LOW_VIX.
            threshold_type: RSI band to resolve. Returns a float for single-value
                thresholds (e.g., "oversold", "overbought") and a (low, high)
                tuple when `"neutral"` is requested.

        Returns:
            Either the float threshold or a tuple of neutral bounds when
            `threshold_type` is `"neutral"`. Falls back to NORMAL regime defaults.
        """
        regime_thresholds = PatternMathematics.RSI_THRESHOLDS.get(
            vix_regime,
            PatternMathematics.RSI_THRESHOLDS["NORMAL"],
        )

        if threshold_type == "neutral":
            return (
                regime_thresholds.get("neutral_low", 45),
                regime_thresholds.get("neutral_high", 65),
            )

        return regime_thresholds.get(threshold_type, 50.0)
    
    @staticmethod
    def _get_vix_confidence_multiplier(vix_regime: str) -> float:
        """Get VIX regime confidence multiplier for centralized confidence calculations."""
        vix_multipliers = {
            "HIGH": 0.8,      # Reduce confidence in high VIX (more noise)
            "NORMAL": 1.0,    # Normal confidence
            "LOW": 1.2,       # Increase confidence in low VIX (cleaner signals)
            "UNKNOWN": 1.0    # Default to normal
        }
        return vix_multipliers.get(vix_regime, 1.0)
    
    @staticmethod
    def calculate_volume_confidence(volume_ratio: float, threshold: float, 
                                  base_confidence: float = 0.5) -> float:
        """SAFE: Volume confidence with diminishing returns and proper scaling"""
        if volume_ratio < threshold:
            return 0.0
        
        safe_ratio = PatternMathematics.protect_outliers(volume_ratio)
        
        # Diminishing returns: 1.5x volume = 70% confidence, 2x = 80%, 3x = 85%
        ratio_over_threshold = safe_ratio / threshold
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.35  # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.3   # 80% confidence  
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.2   # 70% confidence
        else:
            strength_bonus = 0.1   # 60% confidence
        
        return PatternMathematics.calculate_bounded_confidence(base_confidence, strength_bonus)
    
    @staticmethod
    def calculate_price_confidence(price_change: float, threshold: float,
                                 base_confidence: float = 0.6) -> float:
        """SAFE: Price confidence with diminishing returns and proper scaling"""
        if abs(price_change) < threshold:
            return 0.0
        
        # Diminishing returns: 1.5x threshold = 75% confidence, 2x = 80%, 3x = 85%
        ratio_over_threshold = abs(price_change) / threshold
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.25  # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.2   # 80% confidence
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.15  # 75% confidence
        else:
            strength_bonus = 0.05  # 65% confidence
        
        return PatternMathematics.calculate_bounded_confidence(base_confidence, strength_bonus)
    
    @staticmethod
    def calculate_trend_confidence(trend_strength: float, threshold: float, 
                                 base_confidence: float = 0.55) -> float:
        """SAFE: Trend confidence with diminishing returns and proper scaling"""
        if trend_strength < threshold:
            return 0.0
        
        # Diminishing returns: 1.5x threshold = 70% confidence, 2x = 80%, 3x = 85%
        ratio_over_threshold = trend_strength / threshold
        if ratio_over_threshold >= 3.0:
            strength_bonus = 0.3   # Max 85% confidence
        elif ratio_over_threshold >= 2.0:
            strength_bonus = 0.25  # 80% confidence
        elif ratio_over_threshold >= 1.5:
            strength_bonus = 0.15  # 70% confidence
        else:
            strength_bonus = 0.05  # 60% confidence
        
        return PatternMathematics.calculate_bounded_confidence(base_confidence, strength_bonus)
    
    @staticmethod
    def scale_low_volume_ratio(volume_ratio: float) -> float:
        """Return volume ratio as-is - scaling should be handled by volume baseline system"""
        # Don't scale here - let the volume baseline system handle proper volume calculations
        # This method exists only for backward compatibility
        return volume_ratio

    @staticmethod
    def calculate_confidence(pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """Aggregate confidence computation using available context metrics."""
        try:
            ptype = (pattern_type or "").lower()
            volume_ratio = float(context.get("volume_ratio", 0.0) or 0.0)
            price_change_pct = context.get("price_change_pct")
            if price_change_pct is None:
                price_change_pct = context.get("price_change")
            price_change_pct = float(price_change_pct or 0.0)
            dynamic_threshold = float(context.get("dynamic_threshold") or context.get("volume_threshold") or 1.0)
            min_price_move = float(context.get("min_price_move") or context.get("price_threshold") or 0.5)
            base_confidence = float(context.get("base_confidence") or 0.5)
            
            # Apply VIX regime confidence multiplier
            vix_regime = context.get("vix_regime", "UNKNOWN")
            vix_multiplier = PatternMathematics._get_vix_confidence_multiplier(vix_regime)

            if ptype in {"volume_spike", "volume_breakout"}:
                vol_conf = PatternMathematics.calculate_volume_confidence(
                    max(volume_ratio, 0.0), max(dynamic_threshold, 0.1), max(0.45, base_confidence)
                )
                price_conf = PatternMathematics.calculate_price_confidence(
                    abs(price_change_pct), max(min_price_move, 0.1), 0.45
                )
                confidence = max(0.0, min(0.95, vol_conf * 0.55 + price_conf * 0.45))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"breakout", "reversal"}:
                breakout_threshold = float(context.get("price_threshold") or min_price_move or 0.3)
                price_conf = PatternMathematics.calculate_price_confidence(
                    abs(price_change_pct), max(breakout_threshold, 0.1), 0.55
                )
                volume_conf = PatternMathematics.calculate_volume_confidence(
                    max(volume_ratio, 0.0), max(dynamic_threshold, 0.1), 0.45
                )
                confidence = max(0.0, min(0.95, price_conf * 0.6 + volume_conf * 0.4))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"volume_price_divergence"}:
                divergence_score = float(context.get("divergence_score") or 0.0)
                base = 0.5 + min(0.2, abs(divergence_score))
                confidence = max(0.0, min(0.85, base))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"upside_momentum", "downside_momentum"}:
                momentum_strength = abs(context.get("momentum_strength", price_change_pct))
                momentum_threshold = float(context.get("momentum_threshold") or min_price_move or 0.3)
                price_conf = PatternMathematics.calculate_price_confidence(
                    momentum_strength, max(momentum_threshold, 0.1), 0.6
                )
                confidence = max(0.0, min(0.9, price_conf))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"hidden_accumulation"}:
                absorption_score = float(context.get("absorption_score") or 0.0)
                confidence = max(0.0, min(0.9, base_confidence + min(absorption_score * 0.1, 0.2)))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            if ptype in {"ict_momentum", "ict_buy_pressure", "ict_sell_pressure"}:
                trend_strength = abs(context.get("trend_strength", price_change_pct))
                trend_threshold = float(context.get("trend_threshold") or 0.3)
                trend_conf = PatternMathematics.calculate_trend_confidence(
                    trend_strength, max(trend_threshold, 0.1), 0.5
                )
                confidence = max(0.0, min(0.9, trend_conf))
                return max(0.0, min(0.95, confidence * vix_multiplier))

            confidence = max(0.0, min(0.95, base_confidence))
            return max(0.0, min(0.95, confidence * vix_multiplier))
        except Exception:
            return None

    @staticmethod
    def calculate_position_size(pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """Generic position size helper using risk per trade heuristics."""
        try:
            entry_price = float(context.get("entry_price") or 0.0)
            stop_loss = float(context.get("stop_loss") or 0.0)
            account_size = float(context.get("account_size") or 1_000_000.0)
            risk_per_trade = float(context.get("risk_per_trade") or 0.01)

            if entry_price <= 0 or stop_loss <= 0 or entry_price == stop_loss:
                return None

            risk_amount = account_size * risk_per_trade
            risk_per_share = abs(entry_price - stop_loss)
            if risk_per_share <= 0:
                return None
            size = risk_amount / risk_per_share
            return max(1.0, size)
        except Exception:
            return None

    @staticmethod
    def calculate_stop_loss(pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """ATR-based stop loss helper."""
        try:
            entry_price = float(context.get("entry_price") or 0.0)
            atr = float(context.get("atr") or (entry_price * 0.02))
            confidence = float(context.get("confidence") or context.get("base_confidence") or 0.5)
            direction = (context.get("direction") or context.get("signal") or "BUY").upper()

            if entry_price <= 0:
                return None

            pattern = (pattern_type or "").lower()
            atr_multiplier_map = {
                "volume_spike": 1.5,
                "volume_breakout": 1.0,
                "breakout": 0.8,
                "reversal": 0.8,
                "ict_liquidity_pools": 1.2,
                "ict_fair_value_gaps": 1.2,
                "ict_optimal_trade_entry": 1.2,
                "ict_killzone": 1.0,
                "ict_momentum": 1.0,
                "ict_buy_pressure": 1.0,
                "ict_sell_pressure": 1.0,
                "ict_premium_discount": 1.3,
                "kow_signal_straddle": 1.5,
                "iv_crush_play_straddle": 2.0,
                "premium_collection_strategy": 2.0,
            }
            atr_mult = atr_multiplier_map.get(pattern, 1.0)
            confidence_adjustment = 1.0 - (confidence - 0.5) * 0.2
            stop_distance = atr * max(0.1, atr_mult * confidence_adjustment)

            if direction in {"SELL", "SHORT"} or "downside" in pattern:
                stop_loss = entry_price + stop_distance
            else:
                stop_loss = entry_price - stop_distance

            max_stop = entry_price * 0.05
            if abs(entry_price - stop_loss) > max_stop:
                if direction in {"SELL", "SHORT"} or "downside" in pattern:
                    stop_loss = entry_price + max_stop
                else:
                    stop_loss = entry_price - max_stop

            return max(0.0, stop_loss)
        except Exception:
            return None

    @staticmethod
    def calculate_price_targets(pattern_type: str, context: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Derive entry/target/stop structure for a pattern."""
        try:
            last_price = float(context.get("last_price") or context.get("entry_price") or 0.0)
            if last_price <= 0:
                return None

            direction = (context.get("direction") or context.get("signal") or "BUY").upper()
            atr = float(context.get("atr") or (last_price * 0.02))
            base_stop = PatternMathematics.calculate_stop_loss(pattern_type, context)
            if base_stop is None:
                return None

            if direction in {"SELL", "SHORT"}:
                entry_price = last_price * 0.998
                target_price = last_price - (atr * 2.0)
            else:
                entry_price = last_price * 1.002
                target_price = last_price + (atr * 2.0)

            stop_loss = base_stop
            return {
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "target_price": target_price,
            }
        except Exception:
            return None

    @staticmethod
    def calculate_news_impact(instrument: str, news_data: Dict[str, Any]) -> Optional[float]:
        """Safe, capped news boost calculation."""
        try:
            if not news_data:
                return 0.0

            impact = (news_data.get("impact") or news_data.get("market_impact") or "LOW").upper()
            sentiment = news_data.get("sentiment")
            if sentiment is None:
                sentiment = news_data.get("sentiment_score", 0.0)

            base_boost = 0.0
            if impact == "HIGH":
                base_boost += 0.06
            elif impact == "MEDIUM":
                base_boost += 0.03

            if news_data.get("volume_trigger"):
                base_boost += 0.03

            if isinstance(sentiment, str):
                sent = sentiment.lower()
                if sent == "positive":
                    base_boost += 0.02
                elif sent == "negative":
                    base_boost -= 0.02
            else:
                score = float(sentiment)
                if score > 0.2:
                    base_boost += 0.02
                elif score < -0.2:
                    base_boost -= 0.02

            return max(-0.05, min(0.10, base_boost))
        except Exception:
            return None
