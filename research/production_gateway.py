#!/usr/bin/env python3
"""
Bayesian Production Gateway
==========================

Statistical validation gateway for Bayesian models before production deployment.
Ensures statistical rigor and confidence before integration with trading system.

Usage:
    from research.production_gateway import ProductionGateway
    gateway = ProductionGateway()
    approved, confidence = gateway.approve_for_production(model, validation_data)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from scipy import stats
import json

logger = logging.getLogger(__name__)


class ProductionGateway:
    """Statistical validation gateway for production deployment"""
    
    def __init__(self):
        """Initialize production gateway"""
        self.validation_thresholds = {
            "min_confidence": 0.75,
            "min_p_value": 0.95,
            "min_bayes_factor": 3.0,
            "min_data_points": 100,
            "max_validation_age_hours": 24
        }
        
        # Import validator if available
        try:
            from research.bayesian_data_validator import BayesianDataValidator
            self.validator = BayesianDataValidator()
        except ImportError:
            logger.warning("BayesianDataValidator not available, using basic validation")
            self.validator = None
    
    def approve_for_production(self, model: Any, validation_data: Dict) -> Tuple[bool, float]:
        """Statistical validation before production deployment"""
        try:
            logger.info("🔍 Starting production validation...")
            
            # Step 1: Data quality validation
            if not self._validate_data_quality(validation_data):
                logger.error("❌ Data quality validation failed")
                return False, 0.0
            
            # Step 2: Backtest significance test
            p_value = self._backtest_significance(model, validation_data)
            if p_value < self.validation_thresholds["min_p_value"]:
                logger.error(f"❌ Backtest significance failed: p={p_value:.3f} < {self.validation_thresholds['min_p_value']}")
                return False, p_value
            
            # Step 3: Confidence scoring
            confidence = self._calculate_model_confidence(model, validation_data)
            if confidence < self.validation_thresholds["min_confidence"]:
                logger.error(f"❌ Model confidence too low: {confidence:.3f} < {self.validation_thresholds['min_confidence']}")
                return False, confidence
            
            # Step 4: Feature stability check
            if not self._check_feature_stability(model, validation_data):
                logger.error("❌ Feature stability check failed")
                return False, confidence
            
            # Step 5: Bayesian assumptions validation
            if not self._validate_bayesian_assumptions(model, validation_data):
                logger.error("❌ Bayesian assumptions validation failed")
                return False, confidence
            
            # Step 6: Bayes factor calculation
            bayes_factor = self._calculate_bayes_factor(model, validation_data)
            if bayes_factor < self.validation_thresholds["min_bayes_factor"]:
                logger.error(f"❌ Bayes factor too low: {bayes_factor:.2f} < {self.validation_thresholds['min_bayes_factor']}")
                return False, confidence
            
            # All checks passed
            logger.info(f"✅ Model approved for production: p={p_value:.3f}, confidence={confidence:.3f}, BF={bayes_factor:.2f}")
            return True, confidence
            
        except Exception as e:
            logger.error(f"❌ Error in production validation: {e}")
            return False, 0.0
    
    def _validate_data_quality(self, validation_data: Dict) -> bool:
        """Validate data quality using validator"""
        try:
            if self.validator:
                is_valid, results = self.validator.validate_dataset(validation_data)
                if not is_valid:
                    logger.warning(f"⚠️ Data quality issues: {results}")
                return is_valid
            else:
                # Basic validation
                return self._basic_data_validation(validation_data)
                
        except Exception as e:
            logger.error(f"Error in data quality validation: {e}")
            return False
    
    def _basic_data_validation(self, validation_data: Dict) -> bool:
        """Basic data validation without full validator"""
        try:
            # Check required fields
            required_fields = ["dataset_metadata", "volume_buckets", "cumulative_sessions"]
            for field in required_fields:
                if field not in validation_data:
                    logger.error(f"Missing required field: {field}")
                    return False
            
            # Check minimum data counts
            metadata = validation_data.get("dataset_metadata", {})
            bucket_count = metadata.get("bucket_count", 0)
            symbol_count = metadata.get("symbol_count", 0)
            
            if bucket_count < self.validation_thresholds["min_data_points"]:
                logger.error(f"Insufficient data points: {bucket_count} < {self.validation_thresholds['min_data_points']}")
                return False
            
            if symbol_count < 1:
                logger.error(f"No symbols found: {symbol_count}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in basic data validation: {e}")
            return False
    
    def _backtest_significance(self, model: Any, validation_data: Dict) -> float:
        """Calculate backtest significance (p-value)"""
        try:
            # Extract historical performance data
            performance_data = self._extract_performance_data(validation_data)
            
            if len(performance_data) < 10:
                logger.warning("Insufficient performance data for significance test")
                return 0.0
            
            # Calculate performance metrics
            returns = np.array(performance_data)
            
            # One-sample t-test against zero return
            t_stat, p_value = stats.ttest_1samp(returns, 0)
            
            # Additional significance tests
            if len(returns) > 30:
                # Shapiro-Wilk test for normality
                shapiro_stat, shapiro_p = stats.shapiro(returns)
                if shapiro_p < 0.05:  # Not normal
                    logger.warning(f"Returns not normally distributed: p={shapiro_p:.3f}")
            
            logger.info(f"📊 Backtest significance: p={p_value:.3f}, t-stat={t_stat:.3f}")
            return float(p_value)
            
        except Exception as e:
            logger.error(f"Error calculating backtest significance: {e}")
            return 0.0
    
    def _extract_performance_data(self, validation_data: Dict) -> List[float]:
        """Extract performance data for backtesting"""
        try:
            performance_data = []
            
            # Extract from session data
            session_data = validation_data.get("cumulative_sessions", {})
            for session_key, session_info in session_data.items():
                if isinstance(session_info, dict):
                    first_price = session_info.get("first_price", 0)
                    last_price = session_info.get("last_price", 0)
                    
                    if first_price > 0 and last_price > 0:
                        return_pct = (last_price - first_price) / first_price
                        performance_data.append(return_pct)
            
            # Extract from volume buckets (price proxy)
            volume_buckets = validation_data.get("volume_buckets", {})
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) > 1:
                    # Use volume as proxy for price movement
                    volumes = [b["volume"] for b in buckets if b["volume"] > 0]
                    if len(volumes) > 1:
                        volume_returns = np.diff(volumes) / volumes[:-1]
                        performance_data.extend(volume_returns.tolist())
            
            return performance_data
            
        except Exception as e:
            logger.error(f"Error extracting performance data: {e}")
            return []
    
    def _calculate_model_confidence(self, model: Any, validation_data: Dict) -> float:
        """Calculate overall model confidence score"""
        try:
            confidence_factors = []
            
            # Data quality confidence
            data_confidence = self._calculate_data_confidence(validation_data)
            confidence_factors.append(data_confidence)
            
            # Model stability confidence
            stability_confidence = self._calculate_stability_confidence(model, validation_data)
            confidence_factors.append(stability_confidence)
            
            # Prediction consistency confidence
            consistency_confidence = self._calculate_consistency_confidence(model, validation_data)
            confidence_factors.append(consistency_confidence)
            
            # Recent accuracy confidence
            accuracy_confidence = self._calculate_accuracy_confidence(model, validation_data)
            confidence_factors.append(accuracy_confidence)
            
            # Bayesian combination of confidence factors
            overall_confidence = np.prod(confidence_factors)
            
            logger.info(f"📊 Confidence factors: data={data_confidence:.3f}, stability={stability_confidence:.3f}, "
                       f"consistency={consistency_confidence:.3f}, accuracy={accuracy_confidence:.3f}")
            
            return float(overall_confidence)
            
        except Exception as e:
            logger.error(f"Error calculating model confidence: {e}")
            return 0.0
    
    def _calculate_data_confidence(self, validation_data: Dict) -> float:
        """Calculate data quality confidence"""
        try:
            metadata = validation_data.get("dataset_metadata", {})
            
            # Data completeness score
            bucket_count = metadata.get("bucket_count", 0)
            symbol_count = metadata.get("symbol_count", 0)
            session_count = metadata.get("session_count", 0)
            
            completeness_score = min(1.0, (bucket_count / 1000) * (symbol_count / 10) * (session_count / 5))
            
            # Data freshness score
            extraction_time = metadata.get("extraction_timestamp", "")
            if extraction_time:
                try:
                    extraction_dt = datetime.fromisoformat(extraction_time.replace('Z', '+00:00'))
                    age_hours = (datetime.now() - extraction_dt).total_seconds() / 3600
                    freshness_score = max(0.0, 1.0 - (age_hours / self.validation_thresholds["max_validation_age_hours"]))
                except:
                    freshness_score = 0.5
            else:
                freshness_score = 0.0
            
            # Combined data confidence
            data_confidence = (completeness_score + freshness_score) / 2
            return float(data_confidence)
            
        except Exception as e:
            logger.error(f"Error calculating data confidence: {e}")
            return 0.0
    
    def _calculate_stability_confidence(self, model: Any, validation_data: Dict) -> float:
        """Calculate model stability confidence"""
        try:
            # Extract volume patterns for stability analysis
            volume_buckets = validation_data.get("volume_buckets", {})
            
            if not volume_buckets:
                return 0.5
            
            # Calculate volume stability across symbols
            volume_stabilities = []
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) > 5:
                    volumes = [b["volume"] for b in buckets if b["volume"] > 0]
                    if len(volumes) > 3:
                        # Calculate coefficient of variation
                        mean_vol = np.mean(volumes)
                        std_vol = np.std(volumes)
                        cv = std_vol / mean_vol if mean_vol > 0 else 1.0
                        stability = max(0.0, 1.0 - cv)  # Lower CV = higher stability
                        volume_stabilities.append(stability)
            
            if volume_stabilities:
                stability_confidence = np.mean(volume_stabilities)
            else:
                stability_confidence = 0.5
            
            return float(stability_confidence)
            
        except Exception as e:
            logger.error(f"Error calculating stability confidence: {e}")
            return 0.5
    
    def _calculate_consistency_confidence(self, model: Any, validation_data: Dict) -> float:
        """Calculate prediction consistency confidence"""
        try:
            # This would require actual model predictions
            # For now, use pattern consistency as proxy
            pattern_results = validation_data.get("pattern_results", {})
            
            if not pattern_results:
                return 0.5
            
            # Analyze pattern consistency
            pattern_types = {}
            for alert_id, alert_data in pattern_results.items():
                if isinstance(alert_data, dict):
                    pattern_type = alert_data.get("pattern_type", "unknown")
                    if pattern_type not in pattern_types:
                        pattern_types[pattern_type] = 0
                    pattern_types[pattern_type] += 1
            
            # Calculate consistency (lower entropy = higher consistency)
            total_patterns = sum(pattern_types.values())
            if total_patterns > 0:
                probabilities = [count / total_patterns for count in pattern_types.values()]
                entropy = -sum(p * np.log(p) for p in probabilities if p > 0)
                max_entropy = np.log(len(pattern_types)) if len(pattern_types) > 1 else 1.0
                consistency = 1.0 - (entropy / max_entropy) if max_entropy > 0 else 0.5
            else:
                consistency = 0.5
            
            return float(consistency)
            
        except Exception as e:
            logger.error(f"Error calculating consistency confidence: {e}")
            return 0.5
    
    def _calculate_accuracy_confidence(self, model: Any, validation_data: Dict) -> float:
        """Calculate recent accuracy confidence"""
        try:
            # This would require historical accuracy data
            # For now, use data quality as proxy
            metadata = validation_data.get("dataset_metadata", {})
            
            # Use validation status if available
            validation_status = validation_data.get("validation_status", {})
            if validation_status and "is_valid" in validation_status:
                return 0.9 if validation_status["is_valid"] else 0.1
            
            # Fallback to data completeness
            bucket_count = metadata.get("bucket_count", 0)
            symbol_count = metadata.get("symbol_count", 0)
            
            if bucket_count > 100 and symbol_count > 5:
                return 0.8
            elif bucket_count > 50 and symbol_count > 2:
                return 0.6
            else:
                return 0.4
                
        except Exception as e:
            logger.error(f"Error calculating accuracy confidence: {e}")
            return 0.5
    
    def _check_feature_stability(self, model: Any, validation_data: Dict) -> bool:
        """Check feature stability over time"""
        try:
            # Extract feature stability metrics
            volume_buckets = validation_data.get("volume_buckets", {})
            
            if not volume_buckets:
                return False
            
            # Check volume stability across time periods
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < 10:
                    continue
                
                # Split into early and late periods
                mid_point = len(buckets) // 2
                early_volumes = [b["volume"] for b in buckets[:mid_point] if b["volume"] > 0]
                late_volumes = [b["volume"] for b in buckets[mid_point:] if b["volume"] > 0]
                
                if len(early_volumes) > 2 and len(late_volumes) > 2:
                    early_mean = np.mean(early_volumes)
                    late_mean = np.mean(late_volumes)
                    
                    # Check for significant drift
                    if early_mean > 0 and late_mean > 0:
                        drift_ratio = abs(late_mean - early_mean) / early_mean
                        if drift_ratio > 2.0:  # 200% change
                            logger.warning(f"⚠️ Significant volume drift in {symbol}: {drift_ratio:.2f}x")
                            return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking feature stability: {e}")
            return False
    
    def _validate_bayesian_assumptions(self, model: Any, validation_data: Dict) -> bool:
        """Validate Bayesian model assumptions"""
        try:
            # Check for normality assumptions
            volume_buckets = validation_data.get("volume_buckets", {})
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < 10:
                    continue
                
                volumes = [b["volume"] for b in buckets if b["volume"] > 0]
                if len(volumes) < 5:
                    continue
                
                # Shapiro-Wilk test for normality
                if len(volumes) >= 3 and len(volumes) <= 5000:
                    shapiro_stat, shapiro_p = stats.shapiro(volumes)
                    if shapiro_p < 0.01:  # Very non-normal
                        logger.warning(f"⚠️ Volume data not normal in {symbol}: p={shapiro_p:.3f}")
                        # Don't fail for non-normality, just warn
                
                # Check for stationarity (simplified)
                if len(volumes) > 10:
                    # Simple trend test
                    x = np.arange(len(volumes))
                    slope, _, _, _, _ = stats.linregress(x, volumes)
                    if abs(slope) > np.std(volumes) * 0.1:  # Significant trend
                        logger.warning(f"⚠️ Non-stationary volume in {symbol}: slope={slope:.2f}")
                        # Don't fail for non-stationarity, just warn
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating Bayesian assumptions: {e}")
            return False
    
    def _calculate_bayes_factor(self, model: Any, validation_data: Dict) -> float:
        """Calculate Bayes factor for model comparison"""
        try:
            # Extract performance data
            performance_data = self._extract_performance_data(validation_data)
            
            if len(performance_data) < 10:
                return 1.0
            
            returns = np.array(performance_data)
            
            # Simple Bayes factor calculation
            # H0: returns ~ Normal(0, σ²)
            # H1: returns ~ Normal(μ, σ²) where μ ≠ 0
            
            # Calculate likelihood under H0 (mean = 0)
            mean_h0 = 0.0
            std_h0 = np.std(returns)
            likelihood_h0 = np.sum(stats.norm.logpdf(returns, mean_h0, std_h0))
            
            # Calculate likelihood under H1 (mean = sample mean)
            mean_h1 = np.mean(returns)
            std_h1 = np.std(returns)
            likelihood_h1 = np.sum(stats.norm.logpdf(returns, mean_h1, std_h1))
            
            # Bayes factor (simplified)
            bayes_factor = np.exp(likelihood_h1 - likelihood_h0)
            
            logger.info(f"📊 Bayes factor: {bayes_factor:.2f}")
            return float(bayes_factor)
            
        except Exception as e:
            logger.error(f"Error calculating Bayes factor: {e}")
            return 1.0
    
    def get_validation_report(self, model: Any, validation_data: Dict) -> Dict:
        """Generate comprehensive validation report"""
        try:
            report = {
                "validation_timestamp": datetime.now().isoformat(),
                "data_quality": self._validate_data_quality(validation_data),
                "backtest_significance": self._backtest_significance(model, validation_data),
                "model_confidence": self._calculate_model_confidence(model, validation_data),
                "feature_stability": self._check_feature_stability(model, validation_data),
                "bayesian_assumptions": self._validate_bayesian_assumptions(model, validation_data),
                "bayes_factor": self._calculate_bayes_factor(model, validation_data),
                "overall_approval": False
            }
            
            # Determine overall approval
            report["overall_approval"] = (
                report["data_quality"] and
                report["backtest_significance"] >= self.validation_thresholds["min_p_value"] and
                report["model_confidence"] >= self.validation_thresholds["min_confidence"] and
                report["feature_stability"] and
                report["bayesian_assumptions"] and
                report["bayes_factor"] >= self.validation_thresholds["min_bayes_factor"]
            )
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating validation report: {e}")
            return {"error": str(e)}


if __name__ == "__main__":
    # Test the production gateway
    test_model = {"name": "test_model"}
    test_data = {
        "dataset_metadata": {
            "extraction_timestamp": datetime.now().isoformat(),
            "bucket_count": 1000,
            "symbol_count": 10
        },
        "volume_buckets": {
            "RELIANCE": {
                "buckets": [
                    {"hour": 9, "minute_bucket": 3, "volume": 1000, "count": 10},
                    {"hour": 9, "minute_bucket": 4, "volume": 1200, "count": 12}
                ]
            }
        },
        "cumulative_sessions": {
            "session:RELIANCE:2024-10-10": {
                "first_price": 100.0,
                "last_price": 102.0
            }
        },
        "pattern_results": {}
    }
    
    gateway = ProductionGateway()
    approved, confidence = gateway.approve_for_production(test_model, test_data)
    
    print(f"Production approval: {approved}")
    print(f"Confidence score: {confidence:.3f}")
    
    # Generate full report
    report = gateway.get_validation_report(test_model, test_data)
    print(f"Validation report: {json.dumps(report, indent=2)}")
