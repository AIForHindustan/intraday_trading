"""
Correct Volume Calculator
========================

SINGLE source of truth for volume calculations across the entire system.
Replaces all inconsistent volume calculation methods with this unified approach.

Used by:
- intraday_scanner/scanner_main.py (scanner signal generation)
- patterns/pattern_detector.py (pattern scoring & confirmations)
- alert_validation/alert_validator.py (validation metrics)
- scripts/build_volume_baselines.py and scripts/seed_volume_baselines.py (baseline generation)
- redis_files/redis_client.py (volume-related helpers)
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, Any

logger = logging.getLogger(__name__)

class CorrectVolumeCalculator:
    """
    SINGLE source of truth for volume calculations
    """
    
    def __init__(self, redis_client=None):
        # Lazy imports to avoid circular dependencies
        from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
        from patterns.volume_thresholds import VolumeThresholdCalculator
        
        self.baseline_calc = TimeAwareVolumeBaseline(redis_client)
        self.threshold_calc = VolumeThresholdCalculator(self.baseline_calc)
    
    def _get_incremental_volume(self, tick_data: dict) -> float:
        """Extract incremental volume from tick data using all possible field names"""
        try:
            volume_fields = [
                "bucket_incremental_volume",
                "zerodha_cumulative_volume", 
                "vol",
                "zerodha_last_traded_quantity",
                "quantity",
            ]
            
            for field in volume_fields:
                if field in tick_data and tick_data[field] is not None:
                    volume = float(tick_data[field])
                    if volume > 0:
                        return volume
            
            return 0.0
            
        except Exception as e:
            logger.debug(f"Failed to extract volume from tick: {e}")
            return 0.0
    
    def calculate_volume_ratio(self, symbol: str, current_tick: dict) -> float:
        """Calculate MEANINGFUL volume ratio using time-aware baselines"""
        try:
            # Ensure symbol is a string (may be int like instrument_token)
            if not isinstance(symbol, str):
                symbol = str(symbol)
            # 1. Get CURRENT incremental volume (from this tick)
            current_volume = self._get_incremental_volume(current_tick)
            
            if current_volume <= 0:
                return 0.0
            
            # 2. Get EXPECTED volume for this time period using instance baseline_calc
            expected_volume = self.baseline_calc.calculate_dynamic_threshold(
                symbol, 
                "1min",  # Use 1-minute resolution for tick-level calculations
                datetime.now(),
                "NORMAL"  # Default VIX regime
            )
            
            # 3. Calculate MEANINGFUL ratio
            if expected_volume <= 0:
                logger.warning(f"No expected volume for {symbol}, using fallback")
                return 1.0  # Neutral ratio when no baseline
                
            volume_ratio = current_volume / expected_volume
            
            # Cap ratio at reasonable maximum
            return min(volume_ratio, 50.0)  # Max 50x ratio
            
        except Exception as e:
            logger.error(f"Failed to calculate volume ratio for {symbol}: {e}")
            return 0.0
    
    def calculate_bucket_volume_ratio(self, symbol: str, bucket_data: dict) -> float:
        """Calculate volume ratio for 5-minute buckets"""
        try:
            bucket_volume = bucket_data.get('bucket_incremental_volume', 0)
            
            if bucket_volume <= 0:
                return 0.0
            
            # Get expected volume for 5-minute period in current time using instance baseline_calc
            minute_baseline = self.baseline_calc.calculate_dynamic_threshold(
                symbol, 
                "1min",  # Get 1-minute baseline first
                datetime.now(),
                "NORMAL"  # Default VIX regime
            )
            five_minute_expected = minute_baseline * 5  # Scale to 5 minutes
            
            if five_minute_expected <= 0:
                logger.warning(f"No 5-minute baseline for {symbol}")
                return 1.0  # Neutral ratio
                
            ratio = bucket_volume / five_minute_expected
            return min(ratio, 50.0)  # Cap at 50x
            
        except Exception as e:
            logger.error(f"Failed to calculate bucket volume ratio for {symbol}: {e}")
            return 0.0
    
    def calculate_volume_metrics(self, symbol: str, tick_data: dict) -> Dict[str, float]:
        """Calculate comprehensive volume metrics"""
        try:
            # Get volume components
            incremental_volume = VolumeResolver.get_incremental_volume(tick_data)
            cumulative_volume = VolumeResolver.get_cumulative_volume(tick_data)
            
            # Calculate volume ratio using instance method
            volume_ratio = self.calculate_volume_ratio(symbol, tick_data)
            
            return {
                'incremental_volume': incremental_volume,
                'cumulative_volume': cumulative_volume,
                'volume_ratio': volume_ratio,
                'timestamp': tick_data.get('exchange_timestamp', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate volume metrics for {symbol}: {e}")
            return {
                'incremental_volume': 0.0,
                'cumulative_volume': 0.0,
                'volume_ratio': 0.0,
                'timestamp': 0
            }
    
    def calculate_volume_spike_threshold(self, symbol: str, bucket_resolution: str, 
                                       current_time: datetime, vix_regime: str,
                                       confidence_level: float = 0.95) -> float:
        """Calculate volume spike threshold using statistical methods"""
        return self.threshold_calc.calculate_volume_spike_threshold(
            symbol, bucket_resolution, current_time, vix_regime, confidence_level
        )

    def calculate_volume_ratio_advanced(self, current_volume: float, threshold: float) -> float:
        """Calculate volume ratio with advanced statistical methods"""
        return self.threshold_calc.calculate_volume_ratio(current_volume, threshold)

    def detect_volume_anomaly(self, current_volume: float, historical_volumes: list,
                            bucket_resolution: str, confidence: float = 0.99) -> bool:
        """Detect volume anomalies using statistical methods"""
        return self.threshold_calc.detect_volume_anomaly(
            current_volume, historical_volumes, bucket_resolution, confidence
        )

    def get_optimal_bucket_resolution(self, symbol: str, current_time: datetime) -> str:
        """Get optimal bucket resolution for symbol and time"""
        return self.threshold_calc.get_optimal_bucket_resolution(symbol, current_time)
    
    @staticmethod
    def validate_volume_data(tick_data: dict) -> Dict[str, bool]:
        """Validate that volume data is present and reasonable"""
        validation = {
            'has_incremental': False,
            'has_cumulative': False,
            'has_ratio': False,
            'is_valid': False
        }
        
        try:
            # Check incremental volume
            incremental = VolumeResolver.get_incremental_volume(tick_data)
            validation['has_incremental'] = incremental > 0
            
            # Check cumulative volume
            cumulative = VolumeResolver.get_cumulative_volume(tick_data)
            validation['has_cumulative'] = cumulative > 0
            
            # Check volume ratio
            ratio = VolumeResolver.get_volume_ratio(tick_data)
            validation['has_ratio'] = ratio > 0
            
            # Overall validation
            validation['is_valid'] = (validation['has_incremental'] and 
                                    validation['has_cumulative'])
            
        except Exception as e:
            logger.debug(f"Volume data validation failed: {e}")
        
        return validation

class VolumeResolver:
    """
    ONE way to resolve volume fields across entire system
    """
    
    @staticmethod
    def get_incremental_volume(tick_data: dict) -> float:
        """Get pre-calculated incremental volume from WebSocket parser (single source of truth)"""
        try:
            # ✅ SINGLE SOURCE OF TRUTH: Use pre-calculated volume data from WebSocket parser
            # The WebSocket parser has already calculated incremental volume using VolumeStateManager
            # Do NOT recalculate - this violates the single source of truth principle
            
            if 'bucket_incremental_volume' in tick_data and tick_data['bucket_incremental_volume'] is not None:
                return float(tick_data['bucket_incremental_volume'])
            
            # Fallback to other volume fields if bucket_incremental_volume not available
            if 'incremental_volume' in tick_data and tick_data['incremental_volume'] is not None:
                return float(tick_data['incremental_volume'])
            
            return 0.0
            
        except Exception as e:
            logger.debug(f"Failed to get pre-calculated incremental volume: {e}")
            return 0.0
    
    @staticmethod
    def get_cumulative_volume(tick_data: dict) -> float:
        """Get cumulative volume from ANY data source"""
        try:

            # PRIMARY: Direct from Zerodha
            if 'zerodha_cumulative_volume' in tick_data and tick_data['zerodha_cumulative_volume'] is not None:
                value = float(tick_data['zerodha_cumulative_volume'])
                if value > 0:
                    return value
            
            # SECONDARY: From bucket (calculated from price changes) 
            if 'bucket_cumulative_volume' in tick_data and tick_data['bucket_cumulative_volume'] is not None:
                value = float(tick_data['bucket_cumulative_volume'])
                if value > 0:
                    return value
            
            # FALLBACK: Any cumulative field
            for field in ['volume_traded', 'cumulative_volume', 'volume']:
                if field in tick_data and tick_data[field] is not None:
                    value = float(tick_data[field])
                    if value > 0:
                        return value
            
            return 0.0
            
        except (TypeError, ValueError) as e:
            logger.debug(f"Failed to get cumulative volume: {e}")
            return 0.0
    
    @staticmethod
    def get_volume_ratio(tick_data: dict) -> float:
        """Get pre-calculated volume ratio if available"""
        try:
            # Check for pre-calculated ratio
            for field in ['volume_ratio', 'normalized_volume']:
                if field in tick_data and tick_data[field] is not None:
                    return float(tick_data[field])
            
            return 0.0
            
        except (TypeError, ValueError) as e:
            logger.debug(f"Failed to get volume ratio: {e}")
            return 0.0
            
    @staticmethod
    def verify_volume_consistency(tick_data: dict, stage_name: str = "unknown") -> Dict[str, Any]:
        """
        Verify that volume metrics remain consistent through pipeline stages.
        This ensures the single source of truth principle is maintained.
        
        Returns:
            Dict with verification results and any inconsistencies found
        """
        try:
            verification = {
                "stage": stage_name,
                "timestamp": datetime.now().isoformat(),
                "consistent": True,
                "issues": [],
                "metrics": {}
            }
            
            # Get volume metrics using centralized resolver
            incremental = VolumeResolver.get_incremental_volume(tick_data)
            cumulative = VolumeResolver.get_cumulative_volume(tick_data)
            volume_ratio = VolumeResolver.get_volume_ratio(tick_data)
            
            verification["metrics"] = {
                "incremental_volume": incremental,
                "cumulative_volume": cumulative,
                "volume_ratio": volume_ratio
            }
            
            # Check for data consistency
            if incremental < 0:
                verification["issues"].append("Negative incremental volume detected")
                verification["consistent"] = False
                
            if cumulative < 0:
                verification["issues"].append("Negative cumulative volume detected")
                verification["consistent"] = False
                
            if volume_ratio < 0:
                verification["issues"].append("Negative volume ratio detected")
                verification["consistent"] = False
                
            # Check for reasonable volume ratio bounds (0-100x is reasonable)
            if volume_ratio > 100:
                verification["issues"].append(f"Extremely high volume ratio: {volume_ratio:.1f}x")
                verification["consistent"] = False
                
            # Log verification results
            if not verification["consistent"]:
                logger.warning(f"Volume consistency issues at {stage_name}: {verification['issues']}")
            else:
                logger.debug(f"Volume consistency verified at {stage_name}: ratio={volume_ratio:.2f}x")
                
            return verification
            
        except Exception as e:
            logger.error(f"Volume verification failed at {stage_name}: {e}")
            return {
                "stage": stage_name,
                "timestamp": datetime.now().isoformat(),
                "consistent": False,
                "issues": [f"Verification error: {str(e)}"],
                "metrics": {}
            }
