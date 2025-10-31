#!/usr/bin/env python3
"""
Bayesian Feature Engineering Pipeline
====================================

Extract and engineer features for Bayesian model development.
Creates statistical priors and features from raw Redis data.

Usage:
    from research.bayesian_features import BayesianFeatureEngineer
    engineer = BayesianFeatureEngineer()
    features = engineer.create_bayesian_features(raw_data)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging

# Import field mapping utilities for canonical field names
try:
    from utils.yaml_field_loader import resolve_session_field, get_field_mapping_manager
    FIELD_MAPPING_AVAILABLE = True
except ImportError:
    FIELD_MAPPING_AVAILABLE = False
    def resolve_session_field(field_name: str) -> str:
        return field_name  # Fallback to original field name

logger = logging.getLogger(__name__)


class BayesianFeatureEngineer:
    """Feature engineering for Bayesian trading models"""
    
    def __init__(self):
        """Initialize feature engineer"""
        self.market_hours = {
            "pre_market": (8, 45),   # 8:45 AM
            "market_open": (9, 15),  # 9:15 AM
            "market_close": (15, 30), # 3:30 PM
            "post_market": (15, 45)  # 3:45 PM
        }
    
    def create_bayesian_features(self, raw_data: Dict) -> Dict:
        """Create comprehensive Bayesian features from raw data"""
        try:
            logger.info("🔧 Starting Bayesian feature engineering...")
            
            features = {
                "volume_features": self._extract_volume_priors(raw_data),
                "time_features": self._extract_time_priors(raw_data),
                "pattern_features": self._extract_pattern_priors(raw_data),
                "market_regime": self._detect_market_regime(raw_data),
                "price_features": self._extract_price_priors(raw_data),
                "correlation_features": self._extract_correlation_priors(raw_data)
            }
            
            # Add metadata
            features["metadata"] = {
                "feature_engineering_timestamp": datetime.now().isoformat(),
                "feature_count": sum(len(f) if isinstance(f, dict) else 1 for f in features.values() if f != features["metadata"]),
                "symbols_processed": len(raw_data.get("volume_buckets", {}))
            }
            
            logger.info(f"✅ Feature engineering completed: {features['metadata']['feature_count']} features created")
            return features
            
        except Exception as e:
            logger.error(f"❌ Error in feature engineering: {e}")
            return {}
    
    def _extract_volume_priors(self, data: Dict) -> Dict:
        """Extract volume-based Bayesian priors"""
        try:
            volume_buckets = data.get("volume_buckets", {})
            volume_features = {}
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < 3:
                    continue
                
                volumes = [b["volume"] for b in buckets if b["volume"] > 0]
                if len(volumes) < 2:
                    continue
                
                # Basic volume statistics
                volume_mean = np.mean(volumes)
                volume_std = np.std(volumes)
                volume_median = np.median(volumes)
                
                # Volume velocity (rate of change)
                volume_velocity = self._calculate_velocity(volumes)
                
                # Volume acceleration (second derivative)
                volume_acceleration = self._calculate_acceleration(volumes)
                
                # Time-of-day volume pattern
                time_pattern = self._extract_time_pattern(buckets)
                
                # Volume anomaly detection
                anomaly_score = self._detect_volume_anomalies(volumes)
                
                volume_features[symbol] = {
                    "volume_mean": float(volume_mean),
                    "volume_std": float(volume_std),
                    "volume_median": float(volume_median),
                    "volume_velocity": float(volume_velocity),
                    "volume_acceleration": float(volume_acceleration),
                    "time_of_day_pattern": time_pattern,
                    "anomaly_score": float(anomaly_score),
                    "volume_skewness": float(self._calculate_skewness(volumes)),
                    "volume_kurtosis": float(self._calculate_kurtosis(volumes))
                }
            
            return volume_features
            
        except Exception as e:
            logger.error(f"Error extracting volume priors: {e}")
            return {}
    
    def _extract_time_priors(self, data: Dict) -> Dict:
        """Extract time-based priors"""
        try:
            volume_buckets = data.get("volume_buckets", {})
            time_features = {}
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < 5:
                    continue
                
                # Extract time-based patterns
                hour_distribution = {}
                minute_distribution = {}
                
                for bucket in buckets:
                    hour = bucket.get("hour", 0)
                    minute_bucket = bucket.get("minute_bucket", 0)
                    volume = bucket.get("volume", 0)
                    
                    if hour not in hour_distribution:
                        hour_distribution[hour] = []
                    hour_distribution[hour].append(volume)
                    
                    if minute_bucket not in minute_distribution:
                        minute_distribution[minute_bucket] = []
                    minute_distribution[minute_bucket].append(volume)
                
                # Calculate hourly patterns
                hourly_patterns = {}
                for hour, volumes in hour_distribution.items():
                    if volumes:
                        hourly_patterns[hour] = {
                            "mean_volume": float(np.mean(volumes)),
                            "std_volume": float(np.std(volumes)),
                            "count": len(volumes)
                        }
                
                # Market session analysis
                session_analysis = self._analyze_market_sessions(buckets)
                
                time_features[symbol] = {
                    "hourly_patterns": hourly_patterns,
                    "minute_patterns": minute_distribution,
                    "session_analysis": session_analysis,
                    "market_hours_coverage": self._calculate_market_coverage(buckets)
                }
            
            return time_features
            
        except Exception as e:
            logger.error(f"Error extracting time priors: {e}")
            return {}
    
    def _extract_pattern_priors(self, data: Dict) -> Dict:
        """Extract pattern-based priors"""
        try:
            pattern_results = data.get("pattern_results", {})
            pattern_features = {}
            
            # Analyze pattern frequency
            pattern_types = {}
            symbol_patterns = {}
            
            for alert_id, alert_data in pattern_results.items():
                if not isinstance(alert_data, dict):
                    continue
                
                pattern_type = alert_data.get("pattern_type", "unknown")
                symbol = alert_data.get("symbol", "unknown")
                
                if pattern_type not in pattern_types:
                    pattern_types[pattern_type] = 0
                pattern_types[pattern_type] += 1
                
                if symbol not in symbol_patterns:
                    symbol_patterns[symbol] = []
                symbol_patterns[symbol].append({
                    "pattern_type": pattern_type,
                    "timestamp": alert_data.get("timestamp", ""),
                    "confidence": alert_data.get("confidence", 0.0)
                })
            
            # Calculate pattern statistics
            total_patterns = len(pattern_results)
            pattern_frequency = {pt: count/total_patterns for pt, count in pattern_types.items()} if total_patterns > 0 else {}
            
            # Symbol-specific pattern analysis
            symbol_analysis = {}
            for symbol, patterns in symbol_patterns.items():
                if len(patterns) > 0:
                    confidences = [p["confidence"] for p in patterns if p["confidence"] > 0]
                    symbol_analysis[symbol] = {
                        "pattern_count": len(patterns),
                        "avg_confidence": float(np.mean(confidences)) if confidences else 0.0,
                        "pattern_types": list(set(p["pattern_type"] for p in patterns))
                    }
            
            pattern_features = {
                "total_patterns": total_patterns,
                "pattern_frequency": pattern_frequency,
                "symbol_analysis": symbol_analysis,
                "pattern_types": list(pattern_types.keys()),
                "most_common_pattern": max(pattern_types.items(), key=lambda x: x[1])[0] if pattern_types else None
            }
            
            return pattern_features
            
        except Exception as e:
            logger.error(f"Error extracting pattern priors: {e}")
            return {}
    
    def _detect_market_regime(self, data: Dict) -> Dict:
        """Detect current market regime"""
        try:
            volume_buckets = data.get("volume_buckets", {})
            current_time = datetime.now()
            
            # Analyze recent volume patterns
            recent_volumes = []
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if buckets:
                    # Get last few buckets
                    recent_buckets = buckets[-5:] if len(buckets) >= 5 else buckets
                    recent_volumes.extend([b["volume"] for b in recent_buckets])
            
            if not recent_volumes:
                return {"regime": "unknown", "confidence": 0.0}
            
            # Calculate regime indicators
            volume_mean = np.mean(recent_volumes)
            volume_std = np.std(recent_volumes)
            volume_trend = self._calculate_trend(recent_volumes)
            
            # Determine regime
            if volume_std / volume_mean > 0.5:  # High volatility
                regime = "high_volatility"
                confidence = min(1.0, volume_std / volume_mean)
            elif volume_trend > 0.1:  # Increasing volume
                regime = "increasing_volume"
                confidence = min(1.0, volume_trend)
            elif volume_trend < -0.1:  # Decreasing volume
                regime = "decreasing_volume"
                confidence = min(1.0, abs(volume_trend))
            else:
                regime = "normal"
                confidence = 0.5
            
            return {
                "regime": regime,
                "confidence": float(confidence),
                "volume_mean": float(volume_mean),
                "volume_std": float(volume_std),
                "volume_trend": float(volume_trend),
                "detection_time": current_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error detecting market regime: {e}")
            return {"regime": "unknown", "confidence": 0.0}
    
    def _extract_price_priors(self, data: Dict) -> Dict:
        """Extract price-based priors from session data"""
        try:
            session_data = data.get("cumulative_sessions", {})
            price_features = {}
            
            for session_key, session_info in session_data.items():
                if not isinstance(session_info, dict):
                    continue
                
                # Extract price information (using canonical field names)
                first_price = session_info.get("first_price", 0)  # Legacy field
                last_price = session_info.get(resolve_session_field("last_price"), 0)
                high_price = session_info.get(resolve_session_field("high"), 0)
                low_price = session_info.get(resolve_session_field("low"), 0)
                
                if first_price > 0 and last_price > 0:
                    price_change = last_price - first_price
                    price_change_pct = (price_change / first_price) * 100
                    
                    # Calculate price range
                    if high_price > 0 and low_price > 0:
                        price_range = high_price - low_price
                        price_range_pct = (price_range / first_price) * 100
                    else:
                        price_range = 0
                        price_range_pct = 0
                    
                    # Extract symbol from session key
                    symbol = session_key.split(":")[1] if ":" in session_key else "unknown"
                    
                    price_features[symbol] = {
                        "first_price": float(first_price),  # Legacy field
                        "last_price": float(last_price),
                        "high_price": float(high_price),
                        "low_price": float(low_price),
                        "price_change": float(price_change),
                        "price_change_pct": float(price_change_pct),
                        "price_range": float(price_range),
                        "price_range_pct": float(price_range_pct)
                    }
            
            return price_features
            
        except Exception as e:
            logger.error(f"Error extracting price priors: {e}")
            return {}
    
    def _extract_correlation_priors(self, data: Dict) -> Dict:
        """Extract correlation-based priors"""
        try:
            volume_buckets = data.get("volume_buckets", {})
            session_data = data.get("cumulative_sessions", {})
            
            correlation_features = {}
            
            # Calculate volume-price correlations
            for symbol in volume_buckets.keys():
                if symbol in session_data:
                    # Get volume data
                    symbol_buckets = volume_buckets[symbol].get("buckets", [])
                    volumes = [b["volume"] for b in symbol_buckets if b["volume"] > 0]
                    
                    # Get price data
                    session_info = session_data.get(f"session:{symbol}:{datetime.now().strftime('%Y-%m-%d')}", {})
                    if not session_info:
                        continue
                    
                    first_price = session_info.get("first_price", 0)  # Legacy field
                    last_price = session_info.get(resolve_session_field("last_price"), 0)
                    
                    if len(volumes) > 1 and first_price > 0 and last_price > 0:
                        # Calculate volume-price correlation
                        price_change = last_price - first_price
                        volume_trend = self._calculate_trend(volumes)
                        
                        # Simple correlation measure
                        if len(volumes) > 2:
                            volume_std = np.std(volumes)
                            if volume_std > 0:
                                correlation = np.corrcoef(volumes, range(len(volumes)))[0, 1] if len(volumes) > 1 else 0
                            else:
                                correlation = 0
                        else:
                            correlation = 0
                        
                        correlation_features[symbol] = {
                            "volume_price_correlation": float(correlation),
                            "volume_trend": float(volume_trend),
                            "price_change": float(price_change),
                            "volume_std": float(np.std(volumes)) if len(volumes) > 1 else 0.0
                        }
            
            return correlation_features
            
        except Exception as e:
            logger.error(f"Error extracting correlation priors: {e}")
            return {}
    
    # Helper methods
    def _calculate_velocity(self, values: List[float]) -> float:
        """Calculate velocity (first derivative)"""
        if len(values) < 2:
            return 0.0
        return float(np.mean(np.diff(values)))
    
    def _calculate_acceleration(self, values: List[float]) -> float:
        """Calculate acceleration (second derivative)"""
        if len(values) < 3:
            return 0.0
        first_diff = np.diff(values)
        return float(np.mean(np.diff(first_diff)))
    
    def _extract_time_pattern(self, buckets: List[Dict]) -> Dict:
        """Extract time-of-day patterns"""
        hour_volumes = {}
        for bucket in buckets:
            hour = bucket.get("hour", 0)
            volume = bucket.get("volume", 0)
            if hour not in hour_volumes:
                hour_volumes[hour] = []
            hour_volumes[hour].append(volume)
        
        patterns = {}
        for hour, volumes in hour_volumes.items():
            if volumes:
                patterns[hour] = {
                    "mean_volume": float(np.mean(volumes)),
                    "count": len(volumes)
                }
        
        return patterns
    
    def _detect_volume_anomalies(self, volumes: List[float]) -> float:
        """Detect volume anomalies using statistical methods"""
        if len(volumes) < 3:
            return 0.0
        
        mean_vol = np.mean(volumes)
        std_vol = np.std(volumes)
        
        if std_vol == 0:
            return 0.0
        
        # Calculate z-scores
        z_scores = [abs(v - mean_vol) / std_vol for v in volumes]
        max_z_score = max(z_scores)
        
        # Return anomaly score (0-1)
        return float(min(1.0, max_z_score / 3.0))  # Normalize to 0-1 scale
    
    def _calculate_skewness(self, values: List[float]) -> float:
        """Calculate skewness"""
        if len(values) < 3:
            return 0.0
        return float(pd.Series(values).skew())
    
    def _calculate_kurtosis(self, values: List[float]) -> float:
        """Calculate kurtosis"""
        if len(values) < 4:
            return 0.0
        return float(pd.Series(values).kurtosis())
    
    def _analyze_market_sessions(self, buckets: List[Dict]) -> Dict:
        """Analyze market session patterns"""
        pre_market = []
        market_hours = []
        post_market = []
        
        for bucket in buckets:
            hour = bucket.get("hour", 0)
            minute = bucket.get("minute_bucket", 0) * 5
            volume = bucket.get("volume", 0)
            
            time_minutes = hour * 60 + minute
            
            if time_minutes < 555:  # Before 9:15 AM
                pre_market.append(volume)
            elif 555 <= time_minutes <= 930:  # 9:15 AM to 3:30 PM
                market_hours.append(volume)
            else:  # After 3:30 PM
                post_market.append(volume)
        
        return {
            "pre_market": {
                "mean_volume": float(np.mean(pre_market)) if pre_market else 0.0,
                "count": len(pre_market)
            },
            "market_hours": {
                "mean_volume": float(np.mean(market_hours)) if market_hours else 0.0,
                "count": len(market_hours)
            },
            "post_market": {
                "mean_volume": float(np.mean(post_market)) if post_market else 0.0,
                "count": len(post_market)
            }
        }
    
    def _calculate_market_coverage(self, buckets: List[Dict]) -> float:
        """Calculate market hours coverage"""
        if not buckets:
            return 0.0
        
        market_hour_buckets = 0
        for bucket in buckets:
            hour = bucket.get("hour", 0)
            minute = bucket.get("minute_bucket", 0) * 5
            time_minutes = hour * 60 + minute
            
            if 555 <= time_minutes <= 930:  # 9:15 AM to 3:30 PM
                market_hour_buckets += 1
        
        return float(market_hour_buckets / len(buckets))
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend using linear regression"""
        if len(values) < 2:
            return 0.0
        
        x = np.arange(len(values))
        y = np.array(values)
        
        # Simple linear regression
        slope = np.polyfit(x, y, 1)[0]
        return float(slope)


if __name__ == "__main__":
    # Test the feature engineer
    test_data = {
        "volume_buckets": {
            "RELIANCE": {
                "buckets": [
                    {"hour": 9, "minute_bucket": 3, "volume": 1000, "count": 10},
                    {"hour": 9, "minute_bucket": 4, "volume": 1200, "count": 12},
                    {"hour": 10, "minute_bucket": 0, "volume": 800, "count": 8}
                ]
            }
        },
        "cumulative_sessions": {
            "session:RELIANCE:2024-10-10": {
                "first_price": 100.0,
                "last_price": 102.0,
                "high_price": 103.0,
                "low_price": 99.0
            }
        },
        "pattern_results": {}
    }
    
    engineer = BayesianFeatureEngineer()
    features = engineer.create_bayesian_features(test_data)
    
    print("Feature engineering test completed:")
    print(f"Features created: {features.get('metadata', {}).get('feature_count', 0)}")
    print(f"Symbols processed: {features.get('metadata', {}).get('symbols_processed', 0)}")
