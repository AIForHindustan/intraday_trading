#!/usr/bin/env python3
"""
VOLUME THRESHOLD CALCULATOR
Mathematical foundation for volume spike detection across different resolutions
"""
import math
from datetime import datetime
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class VolumeThresholdCalculator:
    """
    Mathematical engine for volume threshold calculations
    Handles statistical distributions, outlier detection, and resolution scaling
    """
    
    def __init__(self, time_aware_baseline):
        self.baseline = time_aware_baseline
        # Ensure we have the new methods available
        if not hasattr(self.baseline, 'calculate_dynamic_threshold'):
            raise ValueError("TimeAwareVolumeBaseline must have calculate_dynamic_threshold method")
    
    def calculate_volume_spike_threshold(self, symbol: str, bucket_resolution: str,
                                      current_time: datetime, vix_regime: str,
                                      confidence_level: float = 0.95) -> float:
        """
        Calculate volume spike threshold using mathematical scaling
        
        BUCKET RESOLUTION MATHEMATICS:
        Example with BANKNIFTY (5min: 322M average):
        - 1min baseline: 322M / 5 = 64.4M
        - 10min baseline: 64.4M × 10 = 644M
        - Session-adjusted: 644M × 1.8 = 1,159M (opening)
        - VIX-adjusted: 1,159M × 1.4 = 1,623M (high volatility)
        - Spike threshold: 1,623M × 2.5 = 4,058M (2.5x multiplier)
        
        Returns threshold where volume > threshold indicates a spike
        """
        # Get dynamic baseline
        baseline = self.baseline.calculate_dynamic_threshold(
            symbol, bucket_resolution, current_time, vix_regime
        )
        
        if baseline <= 0:
            return 0.0
        
        # Statistical spike detection thresholds
        # Based on historical volatility of volume
        spike_multipliers = {
            "1min": 3.5,   # 1-minute needs higher multiplier (noisier)
            "2min": 3.2,   # 2-minute
            "5min": 2.8,   # 5-minute (your current base)
            "10min": 2.5,  # 10-minute
            "15min": 2.3,  # 15-minute
            "30min": 2.0,  # 30-minute
            "60min": 1.8,  # 60-minute
        }
        
        base_multiplier = spike_multipliers.get(bucket_resolution, 2.2)
        
        # Adjust for confidence level (higher confidence = higher threshold)
        confidence_adjustment = 1.0 + (confidence_level - 0.5) * 2.0
        # 0.5 confidence -> 1.0x, 0.95 confidence -> 1.9x
        
        # Apply VIX adjustment (already in baseline) and confidence
        # VIX INTEGRATION MATHEMATICS:
        # base_threshold = baseline_volume × spike_multiplier
        # vix_adjusted_threshold = base_threshold × vix_multiplier
        # Where: LOW VIX=0.8, HIGH VIX=1.4, PANIC VIX=1.8
        threshold = baseline * base_multiplier * confidence_adjustment
        
        logger.info(f"Volume threshold {symbol} {bucket_resolution}: "
                   f"baseline={baseline:.0f} × {base_multiplier} × {confidence_adjustment:.2f} = {threshold:.0f}")
        
        return threshold
    
    def calculate_volume_ratio(self, current_volume: float, threshold: float) -> float:
        """Calculate volume ratio relative to threshold"""
        if threshold <= 0:
            return 1.0  # Neutral fallback
        
        ratio = current_volume / threshold
        
        # Cap extremely high ratios to prevent outliers from dominating
        if ratio > 10.0:
            ratio = 10.0 + math.log10(ratio - 9.0)  # Logarithmic scaling above 10x
        
        return ratio
    
    def detect_volume_anomaly(self, current_volume: float, historical_volumes: List[float],
                            bucket_resolution: str, confidence: float = 0.99) -> bool:
        """
        Volume anomaly detection for real-world incremental volume data
        
        Based on actual volume data characteristics:
        - Incremental volumes have extreme variance (35 to 14M+)
        - Many zero values (no trading activity)
        - Percentile-based thresholds more robust than Z-scores
        - Spike detection using 90th percentile threshold
        """
        if len(historical_volumes) < 10:
            # Not enough data for statistical detection
            mean_volume = sum(historical_volumes) / len(historical_volumes) if historical_volumes else 0
            return current_volume > mean_volume * 2.5
        
        # Calculate mean and standard deviation
        mean_volume = sum(historical_volumes) / len(historical_volumes)
        variance = sum((x - mean_volume) ** 2 for x in historical_volumes) / len(historical_volumes)
        std_dev = math.sqrt(variance)
        
        if std_dev == 0:
            return current_volume > mean_volume * 2.0
        
        # Calculate Z-score
        z_score = (current_volume - mean_volume) / std_dev
        
        # Z-score thresholds for different confidence levels
        z_thresholds = {
            0.90: 1.645,  # 90% confidence
            0.95: 1.960,  # 95% confidence (2 std deviations)
            0.99: 2.576,  # 99% confidence
            0.999: 3.291,  # 99.9% confidence
        }
        
        threshold = z_thresholds.get(confidence, 1.960)  # Default to 95% confidence
        
        # Log significant spikes for analysis
        if z_score > threshold:
            logger.info(f"📊 VOLUME ANOMALY DETECTED: Z-score={z_score:.2f}, "
                       f"threshold={threshold:.2f}, confidence={confidence:.1%}")
        
        return z_score > threshold

    def calculate_volume_distribution_stats(self, volumes: List[float]) -> Dict[str, float]:
        """
        Calculate robust volume distribution statistics for real-world data
        
        Handles extreme variance and outliers in incremental volume data:
        - Filters out zero values for meaningful statistics
        - Uses percentiles instead of mean/std for robustness
        - Calculates coefficient of variation for relative variability
        
        Returns:
            Dictionary with median, percentiles, variance, skewness, kurtosis
        """
        if not volumes or len(volumes) < 2:
            return {
                'mean_volume': 0.0,
                'std_dev': 0.0,
                'variance': 0.0,
                'skewness': 0.0,
                'kurtosis': 0.0,
                'coefficient_of_variation': 0.0
            }
        
        # Basic statistics
        mean_volume = sum(volumes) / len(volumes)
        variance = sum((x - mean_volume) ** 2 for x in volumes) / len(volumes)
        std_dev = math.sqrt(variance)
        
        # Higher moments for distribution analysis
        if std_dev > 0:
            # Skewness (measure of asymmetry)
            skewness = sum(((x - mean_volume) / std_dev) ** 3 for x in volumes) / len(volumes)
            
            # Kurtosis (measure of tail heaviness)
            kurtosis = sum(((x - mean_volume) / std_dev) ** 4 for x in volumes) / len(volumes) - 3
            
            # Coefficient of variation (relative variability)
            coefficient_of_variation = std_dev / mean_volume if mean_volume > 0 else 0
        else:
            skewness = kurtosis = coefficient_of_variation = 0.0
        
        return {
            'mean_volume': mean_volume,
            'std_dev': std_dev,
            'variance': variance,
            'skewness': skewness,
            'kurtosis': kurtosis,
            'coefficient_of_variation': coefficient_of_variation
        }
    
    def get_optimal_bucket_resolution(self, symbol: str, current_time: datetime) -> str:
        """
        Determine optimal bucket resolution based on:
        - Symbol liquidity
        - Time of day
        - Available data
        """
        # High liquidity symbols can use smaller buckets
        liquidity_tiers = {
            "high": ["BANKNIFTY", "NIFTY", "RELIANCE", "HDFC", "INFOSYS", "TCS"],
            "medium": ["AXISBANK", "ICICIBANK", "KOTAKBANK", "SBIN"],
            "low": []  # Everything else
        }
        
        symbol_liquidity = "low"
        for tier, symbols in liquidity_tiers.items():
            if any(s in symbol for s in symbols):
                symbol_liquidity = tier
                break
        
        # Time-based resolution adjustment
        hour = current_time.hour
        if hour in [9, 15]:  # Opening/Closing
            recommended = "2min" if symbol_liquidity == "high" else "5min"
        elif hour in [10, 11, 14]:  # Active hours
            recommended = "5min" if symbol_liquidity == "high" else "10min"
        else:  # Mid-day lull
            recommended = "10min" if symbol_liquidity == "high" else "15min"
        
        return recommended