#!/usr/bin/env python3
"""
Bayesian Data Validator
======================

Statistical validation layer for Bayesian data extraction.
Ensures data quality and integrity before model training.

Usage:
    from research.bayesian_data_validator import BayesianDataValidator
    validator = BayesianDataValidator()
    is_valid, results = validator.validate_dataset(dataset)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import numpy as np

# Import field mapping utilities for canonical field names
try:
    from utils.yaml_field_loader import resolve_session_field, get_field_mapping_manager
    FIELD_MAPPING_AVAILABLE = True
except ImportError:
    FIELD_MAPPING_AVAILABLE = False
    def resolve_session_field(field_name: str) -> str:
        return field_name  # Fallback to original field name

logger = logging.getLogger(__name__)


class BayesianDataValidator:
    """Statistical validation for Bayesian datasets"""
    
    def __init__(self):
        """Initialize validator with validation rules"""
        self.validation_rules = {
            "min_buckets_per_symbol": 5,
            "max_time_gap_minutes": 10,
            "min_volume_threshold": 0,
            "max_volume_ratio": 100.0,  # Max volume can be 100x average
            "required_fields": ["volume", "count", "timestamp"],
            "market_hours": {
                "start": (9, 15),  # 9:15 AM
                "end": (15, 30)    # 3:30 PM
            }
        }
    
    def validate_dataset(self, dataset: Dict) -> Tuple[bool, Dict]:
        """Validate complete dataset and return results"""
        try:
            logger.info("🔍 Starting dataset validation...")
            
            validation_results = {
                "timestamp_continuity": self._check_timestamp_continuity(dataset),
                "volume_integrity": self._check_volume_integrity(dataset),
                "symbol_consistency": self._check_symbol_consistency(dataset),
                "missing_buckets": self._check_missing_buckets(dataset),
                "data_completeness": self._check_completeness(dataset),
                "market_hours_coverage": self._check_market_hours_coverage(dataset),
                "volume_anomalies": self._check_volume_anomalies(dataset),
                "session_data_quality": self._check_session_data_quality(dataset),
                "time_bucket_consistency": self._check_time_bucket_consistency(dataset)
            }
            
            # Overall validation result
            is_valid = all(validation_results.values())
            
            # Add summary statistics
            validation_results["summary"] = {
                "total_checks": len(validation_results) - 1,  # Exclude summary
                "passed_checks": sum(1 for v in validation_results.values() if v is True),
                "failed_checks": sum(1 for v in validation_results.values() if v is False),
                "overall_valid": is_valid,
                "validation_timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"✅ Validation completed: {validation_results['summary']['passed_checks']}/{validation_results['summary']['total_checks']} checks passed")
            
            return is_valid, validation_results
            
        except Exception as e:
            logger.error(f"❌ Error during validation: {e}")
            return False, {"error": str(e)}
    
    def _check_timestamp_continuity(self, dataset: Dict) -> bool:
        """Check for timestamp continuity in time bucket data"""
        try:
            session_data = dataset.get("session_data", {})
            
            for session_key, session_info in session_data.items():
                time_buckets = session_info.get("time_buckets", {})
                if len(time_buckets) < 2:
                    continue
                
                # Extract and sort time bucket keys
                bucket_times = []
                for bucket_key in time_buckets.keys():
                    try:
                        hour, minute = bucket_key.split(':')
                        bucket_times.append((int(hour), int(minute)))
                    except ValueError:
                        continue
                
                bucket_times.sort()
                
                # Check for chronological ordering
                for i in range(1, len(bucket_times)):
                    prev_hour, prev_minute = bucket_times[i-1]
                    curr_hour, curr_minute = bucket_times[i]
                    
                    # Calculate time difference in minutes
                    prev_minutes = prev_hour * 60 + prev_minute
                    curr_minutes = curr_hour * 60 + curr_minute
                    
                    if curr_minutes <= prev_minutes:
                        logger.warning(f"⚠️ Non-chronological timestamps in {session_key}: {bucket_times[i-1]} -> {bucket_times[i]}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking timestamp continuity: {e}")
            return False
    
    def _check_volume_integrity(self, dataset: Dict) -> bool:
        """Check volume data integrity"""
        try:
            volume_buckets = dataset.get("volume_buckets", {})
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                
                for bucket in buckets:
                    volume = bucket.get("volume", 0)
                    count = bucket.get("count", 0)
                    
                    # Check for negative values
                    if volume < 0 or count < 0:
                        logger.warning(f"⚠️ Negative values in {symbol}: volume={volume}, count={count}")
                        return False
                    
                    # Check for reasonable volume ratios
                    if count > 0:
                        avg_volume_per_trade = volume / count
                        if avg_volume_per_trade > 1000000:  # 1M per trade seems unreasonable
                            logger.warning(f"⚠️ Unusually high volume per trade in {symbol}: {avg_volume_per_trade}")
                            return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking volume integrity: {e}")
            return False
    
    def _check_symbol_consistency(self, dataset: Dict) -> bool:
        """Check symbol consistency across data sources"""
        try:
            volume_symbols = set(dataset.get("volume_buckets", {}).keys())
            session_symbols = set()
            pattern_symbols = set()
            
            # Extract symbols from session data
            for key in dataset.get("cumulative_sessions", {}).keys():
                if ":" in key:
                    symbol = key.split(":")[1]  # session:SYMBOL:date
                    session_symbols.add(symbol)
            
            # Extract symbols from pattern data
            for key, data in dataset.get("pattern_results", {}).items():
                if "symbol" in data:
                    pattern_symbols.add(data["symbol"])
            
            # Check for reasonable overlap
            total_symbols = len(volume_symbols | session_symbols | pattern_symbols)
            if total_symbols == 0:
                logger.warning("⚠️ No symbols found in any data source")
                return False
            
            # Log symbol distribution
            logger.info(f"📊 Symbol distribution: volume={len(volume_symbols)}, session={len(session_symbols)}, pattern={len(pattern_symbols)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking symbol consistency: {e}")
            return False
    
    def _check_missing_buckets(self, dataset: Dict) -> bool:
        """Check for missing 5-minute buckets during market hours"""
        try:
            volume_buckets = dataset.get("volume_buckets", {})
            market_start, market_end = self.validation_rules["market_hours"]["start"], self.validation_rules["market_hours"]["end"]
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < self.validation_rules["min_buckets_per_symbol"]:
                    logger.warning(f"⚠️ Insufficient buckets for {symbol}: {len(buckets)} < {self.validation_rules['min_buckets_per_symbol']}")
                    continue
                
                # Check for gaps in market hours
                timestamps = [(b["hour"], b["minute_bucket"]) for b in buckets]
                gaps = self._detect_time_gaps(timestamps, market_start, market_end)
                
                if gaps:
                    logger.warning(f"⚠️ Time gaps detected in {symbol}: {gaps}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking missing buckets: {e}")
            return False
    
    def _detect_time_gaps(self, timestamps: List[Tuple[int, int]], start: Tuple[int, int], end: Tuple[int, int]) -> List[str]:
        """Detect gaps in time series"""
        gaps = []
        
        if len(timestamps) < 2:
            return gaps
        
        # Convert to minutes since midnight
        def to_minutes(hour, minute_bucket):
            return hour * 60 + minute_bucket * 5
        
        start_minutes = to_minutes(*start)
        end_minutes = to_minutes(*end)
        
        # Filter timestamps within market hours
        market_timestamps = [
            (h, m) for h, m in timestamps
            if start_minutes <= to_minutes(h, m) <= end_minutes
        ]
        
        if len(market_timestamps) < 2:
            return gaps
        
        # Check for gaps
        for i in range(1, len(market_timestamps)):
            prev_minutes = to_minutes(*market_timestamps[i-1])
            curr_minutes = to_minutes(*market_timestamps[i])
            
            gap_minutes = curr_minutes - prev_minutes
            if gap_minutes > self.validation_rules["max_time_gap_minutes"]:
                gaps.append(f"{market_timestamps[i-1]} -> {market_timestamps[i]} ({gap_minutes}min)")
        
        return gaps
    
    def _check_completeness(self, dataset: Dict) -> bool:
        """Check data completeness"""
        try:
            metadata = dataset.get("dataset_metadata", {})
            
            # Check required metadata fields
            required_metadata = ["extraction_timestamp", "data_sources", "bucket_count", "symbol_count"]
            for field in required_metadata:
                if field not in metadata:
                    logger.warning(f"⚠️ Missing metadata field: {field}")
                    return False
            
            # Check data sources
            data_sources = metadata.get("data_sources", [])
            expected_sources = ["volume_buckets_db0", "cumulative_session_db5", "patterns_db6"]
            
            for source in expected_sources:
                if source not in data_sources:
                    logger.warning(f"⚠️ Missing data source: {source}")
                    return False
            
            # Check minimum data counts
            bucket_count = metadata.get("bucket_count", 0)
            symbol_count = metadata.get("symbol_count", 0)
            
            if bucket_count < 10:
                logger.warning(f"⚠️ Low bucket count: {bucket_count}")
                return False
            
            if symbol_count < 1:
                logger.warning(f"⚠️ No symbols found: {symbol_count}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking completeness: {e}")
            return False
    
    def _check_market_hours_coverage(self, dataset: Dict) -> bool:
        """Check coverage during market hours"""
        try:
            volume_buckets = dataset.get("volume_buckets", {})
            market_start, market_end = self.validation_rules["market_hours"]["start"], self.validation_rules["market_hours"]["end"]
            
            total_buckets = 0
            market_hour_buckets = 0
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                total_buckets += len(buckets)
                
                for bucket in buckets:
                    hour = bucket.get("hour", 0)
                    minute_bucket = bucket.get("minute_bucket", 0)
                    minute_actual = minute_bucket * 5
                    
                    # Check if within market hours
                    if (market_start[0] < hour < market_end[0] or
                        (hour == market_start[0] and minute_actual >= market_start[1]) or
                        (hour == market_end[0] and minute_actual <= market_end[1])):
                        market_hour_buckets += 1
            
            if total_buckets == 0:
                logger.warning("⚠️ No buckets found")
                return False
            
            coverage_ratio = market_hour_buckets / total_buckets
            if coverage_ratio < 0.5:  # At least 50% should be during market hours
                logger.warning(f"⚠️ Low market hours coverage: {coverage_ratio:.2%}")
                return False
            
            logger.info(f"📊 Market hours coverage: {coverage_ratio:.2%} ({market_hour_buckets}/{total_buckets})")
            return True
            
        except Exception as e:
            logger.error(f"Error checking market hours coverage: {e}")
            return False
    
    def _check_volume_anomalies(self, dataset: Dict) -> bool:
        """Check for volume anomalies"""
        try:
            volume_buckets = dataset.get("volume_buckets", {})
            
            for symbol, symbol_data in volume_buckets.items():
                buckets = symbol_data.get("buckets", [])
                if len(buckets) < 5:
                    continue
                
                volumes = [b.get("volume", 0) for b in buckets if b.get("volume", 0) > 0]
                if len(volumes) < 3:
                    continue
                
                # Calculate volume statistics
                mean_volume = np.mean(volumes)
                std_volume = np.std(volumes)
                
                if std_volume == 0:
                    continue
                
                # Check for extreme outliers
                for volume in volumes:
                    z_score = abs(volume - mean_volume) / std_volume
                    if z_score > 5:  # 5 standard deviations
                        logger.warning(f"⚠️ Extreme volume outlier in {symbol}: {volume} (z-score: {z_score:.2f})")
                        return False
                
                # Check for volume ratio anomalies
                max_volume = max(volumes)
                if mean_volume > 0 and max_volume / mean_volume > self.validation_rules["max_volume_ratio"]:
                    logger.warning(f"⚠️ High volume ratio in {symbol}: {max_volume/mean_volume:.1f}x mean")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking volume anomalies: {e}")
            return False
    
    def get_validation_summary(self, validation_results: Dict) -> str:
        """Get human-readable validation summary"""
        summary = validation_results.get("summary", {})
        
        if summary.get("overall_valid", False):
            return f"✅ Validation PASSED: {summary['passed_checks']}/{summary['total_checks']} checks passed"
        else:
            failed_checks = []
            for check, result in validation_results.items():
                if check != "summary" and result is False:
                    failed_checks.append(check)
            
            return f"❌ Validation FAILED: {summary['failed_checks']}/{summary['total_checks']} checks failed. Failed: {', '.join(failed_checks)}"
    
    def _check_session_data_quality(self, dataset: Dict) -> bool:
        """Check session data quality and structure"""
        try:
            session_data = dataset.get("session_data", {})
            
            for session_key, session_info in session_data.items():
                session_metadata = session_info.get("session_info", {})
                time_buckets = session_info.get("time_buckets", {})
                
                # Check required session fields (using canonical field names)
                required_fields = [
                    resolve_session_field("bucket_cumulative_volume"),  # Canonical field name
                    resolve_session_field("last_price"),                # Canonical field name
                    resolve_session_field("session_date")               # Canonical field name
                ]
                for field in required_fields:
                    if field not in session_metadata:
                        logger.warning(f"⚠️ Missing session field {field} in {session_key}")
                        return False
                
                # Check time bucket structure
                for bucket_key, bucket_data in time_buckets.items():
                    if not isinstance(bucket_data, dict):
                        logger.warning(f"⚠️ Invalid bucket data structure in {session_key}:{bucket_key}")
                        return False
                    
                    # Check required bucket fields
                    bucket_required = ["total_volume", "count", "open", "high", "low", "close"]
                    for field in bucket_required:
                        if field not in bucket_data:
                            logger.warning(f"⚠️ Missing bucket field {field} in {session_key}:{bucket_key}")
                            return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking session data quality: {e}")
            return False
    
    def _check_time_bucket_consistency(self, dataset: Dict) -> bool:
        """Check time bucket data consistency"""
        try:
            session_data = dataset.get("session_data", {})
            
            for session_key, session_info in session_data.items():
                time_buckets = session_info.get("time_buckets", {})
                
                for bucket_key, bucket_data in time_buckets.items():
                    # Check OHLC consistency (using canonical field names)
                    open_price = bucket_data.get("open", 0)  # Legacy field
                    high_price = bucket_data.get(resolve_session_field("high"), 0)
                    low_price = bucket_data.get(resolve_session_field("low"), 0)
                    close_price = bucket_data.get("close", 0)  # Legacy field
                    
                    if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
                        logger.warning(f"⚠️ OHLC inconsistency in {session_key}:{bucket_key}")
                        return False
                    
                    # Check volume and count consistency
                    total_volume = bucket_data.get("total_volume", 0)
                    count = bucket_data.get("count", 0)
                    
                    if total_volume < 0 or count < 0:
                        logger.warning(f"⚠️ Negative volume/count in {session_key}:{bucket_key}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking time bucket consistency: {e}")
            return False


if __name__ == "__main__":
    # Test the validator
    import json
    
    # Create a test dataset
    test_dataset = {
        "dataset_metadata": {
            "extraction_timestamp": datetime.now().isoformat(),
            "data_sources": ["session_data_db0", "price_data_db2", "continuous_market_db4"],
            "total_time_buckets": 100,
            "symbols_with_buckets": 5,
            "session_count": 5,
            "price_entries": 10,
            "market_entries": 15
        },
        "session_data": {
            "session:NSE:RELIANCE:2025-10-13": {
                "session_info": {
                    resolve_session_field("bucket_cumulative_volume"): 1000000,  # Canonical field name
                    resolve_session_field("last_price"): 2500.0,                # Canonical field name
                    resolve_session_field("high"): 2550.0,                      # Canonical field name
                    resolve_session_field("low"): 2480.0,                       # Canonical field name
                    resolve_session_field("session_date"): "2025-10-13"         # Canonical field name
                },
                "time_buckets": {
                    "9:15": {
                        "total_volume": 1000,
                        "count": 10,
                        "open": 2500.0,
                        "high": 2510.0,
                        "low": 2495.0,
                        "close": 2505.0
                    },
                    "9:20": {
                        "total_volume": 1200,
                        "count": 12,
                        "open": 2505.0,
                        "high": 2520.0,
                        "low": 2500.0,
                        "close": 2515.0
                    }
                }
            }
        },
        "price_data": {},
        "continuous_market_data": {},
        "cumulative_volume_data": {},
        "alert_system_data": {},
        "market_microstructure_data": {},
        "pattern_detection_data": {},
        "performance_metrics_data": {}
    }
    
    validator = BayesianDataValidator()
    is_valid, results = validator.validate_dataset(test_dataset)
    
    print(f"Validation result: {is_valid}")
    print(f"Summary: {validator.get_validation_summary(results)}")
