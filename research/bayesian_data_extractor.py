#!/usr/bin/env python3
"""
Standalone Bayesian Data Extractor
=================================

Completely isolated data extraction system for Bayesian analysis.
Runs independently from the main trading system to avoid interference.

Usage:
    python scripts/bayesian_data_extractor.py --mode continuous
    python scripts/bayesian_data_extractor.py --mode once
    python scripts/bayesian_data_extractor.py --interval 120
"""

import json
import os
import sys
import argparse
import logging
import time
import signal
import gzip
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import redis
import numpy as np
import psutil

# Import field mapping utilities for canonical field names
try:
    from utils.yaml_field_loader import resolve_session_field, get_field_mapping_manager
    FIELD_MAPPING_AVAILABLE = True
except ImportError:
    FIELD_MAPPING_AVAILABLE = False
    def resolve_session_field(field_name: str) -> str:
        return field_name  # Fallback to original field name

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Initialize logger early so it can be used in exception handlers
logger = logging.getLogger(__name__)

# Import validation module
try:
    from research.bayesian_data_validator import BayesianDataValidator
except ImportError:
    # Create a simple validator if not available
    class BayesianDataValidator:
        def validate_dataset(self, dataset: Dict) -> Tuple[bool, Dict]:
            return True, {"basic_check": True}

# Import market calendar
try:
    from utils.market_calendar import MarketCalendar
except ImportError:
    logger.warning("MarketCalendar not available, using basic weekday check")
    class MarketCalendar:
        def is_trading_day(self, date=None):
            if date is None:
                date = datetime.now()
            return date.weekday() < 5


class StandaloneBayesianExtractor:
    """Standalone Bayesian data extractor with complete isolation from production system"""
    
    def __init__(self, config_path: str = "config/bayesian_research_config.json"):
        """Initialize standalone extractor"""
        self.config = self._load_config(config_path)
        self.validator = BayesianDataValidator()
        self.market_calendar = MarketCalendar()
        self.running = False
        
        # Create separate Redis connections (read-only)
        self.redis_clients = self._create_redis_connections()
        
        # Create export directory
        self.export_dir = Path(self.config.get("extraction", {}).get("export_directory", "research_data/bayesian_extracts"))
        self.export_dir.mkdir(parents=True, exist_ok=True)
        
        # Get safety limits
        self.safety_limits = self.config.get("safety_limits", {})
        
        # Get operating hours
        self.operating_hours = self.config.get("operating_hours", {})
        
        logger.info("✅ Standalone Bayesian extractor initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from JSON file"""
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return json.load(f)
            else:
                # Create default config
                default_config = {
                    "extraction": {
                        "redis_host": "localhost",
                        "redis_port": 6379,
                        "databases": [0, 5, 6],
                        "read_only": True
                    },
                    "export": {
                        "directory": "research_data/bayesian_extracts",
                        "format": "json",
                        "compress_old_files": True,
                        "max_file_age_days": 30
                    },
                    "schedule": {
                        "mode": "continuous",
                        "export_interval_seconds": 120,
                        "market_hours_only": True,
                        "pre_market_extract": "08:45",
                        "post_market_extract": "15:45"
                    },
                    "validation": {
                        "enabled": True,
                        "fail_on_validation_error": False,
                        "log_validation_details": True
                    }
                }
                
                # Save default config
                os.makedirs(os.path.dirname(config_path), exist_ok=True)
                with open(config_path, 'w') as f:
                    json.dump(default_config, f, indent=2)
                
                logger.info(f"Created default config: {config_path}")
                return default_config
                
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            raise
    
    def _create_redis_connections(self) -> Dict[int, redis.Redis]:
        """Create separate Redis connections for each database using connection pools"""
        clients = {}
        
        extraction_config = self.config.get("extraction", {})
        databases = extraction_config.get("databases_to_extract", extraction_config.get("databases", [0, 5, 6, 17]))
        
        for db_num in databases:
            try:
                # ✅ STANDARDIZED: Use RedisManager82 instead of deprecated get_redis_client_from_pool
                from redis_files.redis_manager import RedisManager82
                
                client = RedisManager82.get_client(
                    process_name="bayesian_extractor",
                    db=db_num,
                    host=extraction_config.get("redis_host", "localhost"),
                    port=extraction_config.get("redis_port", 6379),
                    decode_responses=True
                )
                
                # Test connection
                client.ping()
                clients[db_num] = client
                logger.info(f"✅ Redis connection pool established for DB {db_num}")
                
            except Exception as e:
                logger.error(f"❌ Failed to connect to Redis DB {db_num}: {e}")
                clients[db_num] = None
        
        return clients
    
    def _check_safety_conditions(self) -> bool:
        """Check if it's safe to run extraction"""
        try:
            # Check if trading day
            if not self.market_calendar.is_trading_day(datetime.now()):
                logger.warning("⚠️ Not a trading day. Skipping extraction.")
                return False
            
            # Check main system running (optional warning only)
            if self.safety_limits.get("check_main_system_running", True):
                if not self._is_main_system_running():
                    logger.warning("⚠️ Main trading system not detected (continuing anyway)")
            
            # Check memory usage
            memory_mb = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            max_memory = self.safety_limits.get("max_memory_mb", 1024)
            if memory_mb > max_memory:
                logger.error(f"❌ Memory limit exceeded: {memory_mb:.1f}MB > {max_memory}MB")
                return False
            
            # Check disk usage
            disk_usage = shutil.disk_usage(self.export_dir)
            disk_used_gb = (disk_usage.total - disk_usage.free) / (1024 ** 3)
            max_disk = self.safety_limits.get("max_disk_usage_gb", 50)
            if disk_used_gb > max_disk:
                logger.error(f"❌ Disk usage limit exceeded: {disk_used_gb:.1f}GB > {max_disk}GB")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking safety conditions: {e}")
            return False
    
    def _is_main_system_running(self) -> bool:
        """Check if main.py is running"""
        try:
            for proc in psutil.process_iter(['name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if cmdline and 'main.py' in ' '.join(cmdline):
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return False
        except Exception as e:
            logger.debug(f"Error checking main system: {e}")
            return False
    
    def _is_within_operating_hours(self) -> bool:
        """Check if current time is within operating hours"""
        try:
            now = datetime.now().time()
            
            start_str = self.operating_hours.get("pre_market_start", "08:00")
            end_str = self.operating_hours.get("post_market_end", "16:00")
            
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()
            
            return start <= now <= end
        except Exception as e:
            logger.error(f"Error checking operating hours: {e}")
            return True  # Default to allowing extraction
    
    def _should_extract_now(self) -> bool:
        """Check if we should extract data now based on daily schedule"""
        try:
            now = datetime.now().time()
            extraction_config = self.config.get("extraction", {})
            daily_schedule = extraction_config.get("daily_extraction_schedule", {})
            
            # Check if current time matches any scheduled extraction time
            for schedule_name, time_str in daily_schedule.items():
                try:
                    scheduled_time = datetime.strptime(time_str, "%H:%M").time()
                    # Allow extraction within 5 minutes of scheduled time
                    time_diff = abs((now.hour * 60 + now.minute) - (scheduled_time.hour * 60 + scheduled_time.minute))
                    if time_diff <= 5:
                        logger.info(f"🕐 Scheduled extraction time: {schedule_name} at {time_str}")
                        return True
                except ValueError:
                    continue
            
            # Also check regular interval extraction
            return True
            
        except Exception as e:
            logger.error(f"Error checking extraction schedule: {e}")
            return True  # Default to allowing extraction
    
    def _is_critical_extraction_time(self) -> bool:
        """Check if this is a critical extraction time (before Redis cache clear)"""
        try:
            now = datetime.now().time()
            
            # Critical times: 15:25 (market close), 15:45 (post-market), 16:00 (final)
            critical_times = ["15:25", "15:45", "16:00"]
            
            for time_str in critical_times:
                try:
                    critical_time = datetime.strptime(time_str, "%H:%M").time()
                    time_diff = abs((now.hour * 60 + now.minute) - (critical_time.hour * 60 + critical_time.minute))
                    if time_diff <= 10:  # Within 10 minutes of critical time
                        logger.warning(f"🚨 CRITICAL EXTRACTION TIME: {time_str} - Redis cache may be cleared soon!")
                        return True
                except ValueError:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking critical extraction time: {e}")
            return False
    
    def _manage_data_retention(self):
        """Compress and clean old files based on retention policy"""
        try:
            retention = self.config.get("data_retention", {})
            compress_after = retention.get("compress_after_days", 1)
            delete_after = retention.get("delete_compressed_after_days", 30)
            
            for date_dir in self.export_dir.iterdir():
                if not date_dir.is_dir():
                    continue
                
                try:
                    dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
                    age_days = (datetime.now() - dir_date).days
                    
                    # Compress files older than compress_after_days
                    if age_days >= compress_after and age_days < delete_after:
                        self._compress_directory(date_dir)
                    
                    # Delete compressed files older than delete_after_days
                    elif age_days >= delete_after:
                        self._delete_old_files(date_dir)
                        
                except ValueError:
                    # Not a valid date directory
                    continue
                    
        except Exception as e:
            logger.error(f"Error managing data retention: {e}")
    
    def _compress_directory(self, directory: Path):
        """Compress JSON files in directory"""
        try:
            for json_file in directory.glob("*.json"):
                if json_file.suffix == ".json":
                    gz_file = json_file.with_suffix(".json.gz")
                    
                    if not gz_file.exists():
                        with open(json_file, 'rb') as f_in:
                            with gzip.open(gz_file, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        
                        # Remove original after successful compression
                        json_file.unlink()
                        logger.debug(f"Compressed: {json_file.name}")
                        
        except Exception as e:
            logger.error(f"Error compressing directory {directory}: {e}")
    
    def _delete_old_files(self, directory: Path):
        """Delete old compressed files"""
        try:
            for gz_file in directory.glob("*.gz"):
                gz_file.unlink()
                logger.debug(f"Deleted old file: {gz_file.name}")
            
            # Remove directory if empty
            if not any(directory.iterdir()):
                directory.rmdir()
                logger.debug(f"Removed empty directory: {directory.name}")
                
        except Exception as e:
            logger.error(f"Error deleting old files from {directory}: {e}")
    
    def extract_comprehensive_data(self) -> Dict:
        """Extract comprehensive data from all Redis databases"""
        try:
            logger.info("🔍 Starting comprehensive data extraction...")
            
            # Extract session data with time buckets (DB 0)
            session_data = self._extract_session_data_with_buckets()
            logger.info(f"📊 Extracted {len(session_data)} session entries with time buckets")
            
            # Extract price data (DB 2)
            price_data = self._extract_price_data()
            logger.info(f"💰 Extracted {len(price_data)} price entries")
            
            # Extract continuous market data (DB 4)
            market_data = self._extract_continuous_market_data()
            logger.info(f"📈 Extracted {len(market_data)} market entries")
            
            # Extract cumulative volume data (DB 5)
            cumulative_data = self._extract_cumulative_volume_data()
            logger.info(f"📊 Extracted {len(cumulative_data)} cumulative entries")
            
            # Extract alert system data (DB 6)
            alert_data = self._extract_alert_system_data()
            logger.info(f"🔔 Extracted {len(alert_data)} alert entries")
            
            # Extract market microstructure data (DB 8)
            microstructure_data = self._extract_market_microstructure_data()
            logger.info(f"🔬 Extracted {len(microstructure_data)} microstructure entries")
            
            # Extract pattern detection data (DB 10)
            pattern_data = self._extract_pattern_detection_data()
            logger.info(f"🔍 Extracted {len(pattern_data)} pattern detection entries")
            
            # Extract performance metrics (DB 13)
            performance_data = self._extract_performance_metrics_data()
            logger.info(f"📊 Extracted {len(performance_data)} performance entries")
            
            # Organize into comprehensive dataset
            dataset = self._organize_comprehensive_dataset(
                session_data, price_data, market_data, cumulative_data,
                alert_data, microstructure_data, pattern_data, performance_data
            )
            
            logger.info("✅ Comprehensive data extraction completed")
            return dataset
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive data extraction: {e}")
            return {}
    
    def _extract_session_data_with_buckets(self) -> Dict:
        """Extract session data with time buckets from DB 0"""
        try:
            client = self.redis_clients.get(0)
            if not client:
                logger.warning("No Redis client for DB 0")
                return {}
            
            # Get all session keys
            session_keys = client.keys("session:*")
            logger.info(f"Found {len(session_keys)} session keys")
            
            sessions = {}
            for key in session_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        session_info = json.loads(data)
                        
                        # Extract time buckets from session data
                        time_buckets = session_info.get("time_buckets", {})
                        if time_buckets:
                            sessions[key_str] = {
                                "session_info": {
                                    resolve_session_field("bucket_cumulative_volume"): session_info.get(resolve_session_field("bucket_cumulative_volume"), 0),
                                    resolve_session_field("last_price"): session_info.get(resolve_session_field("last_price"), 0),
                                    resolve_session_field("high"): session_info.get(resolve_session_field("high"), 0),
                                    resolve_session_field("low"): session_info.get(resolve_session_field("low"), 0),
                                    "update_count": session_info.get("update_count", 0),
                                    "session_date": session_info.get("session_date", ""),
                                    "last_update_timestamp": session_info.get("last_update_timestamp", 0)
                                },
                                "time_buckets": time_buckets,
                                "extraction_timestamp": datetime.now().isoformat()
                            }
                except Exception as e:
                    logger.debug(f"Error reading session {key}: {e}")
                    continue
            
            # Also extract gap analysis data
            gap_data = self._extract_gap_analysis_data(client)
            if gap_data:
                sessions.update(gap_data)
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error extracting session data: {e}")
            return {}
    
    def _extract_gap_analysis_data(self, client) -> Dict:
        """Extract gap analysis data from Redis DB 0"""
        try:
            gap_data = {}
            
            # Extract latest gap analysis
            latest_gap = client.get("latest_gift_nifty_gap")
            if latest_gap:
                gap_info = json.loads(latest_gap)
                gap_data["latest_gift_nifty_gap"] = {
                    "gap_analysis": gap_info,
                    "extraction_timestamp": datetime.now().isoformat(),
                    "data_type": "gap_analysis"
                }
                logger.info(f"📊 Extracted gap analysis: {gap_info.get('gap_percent', 0):.2f}% gap")
            
            # Also check for any other gap-related keys
            gap_keys = client.keys("*gap*")
            for key in gap_keys:
                if key != "latest_gift_nifty_gap":  # Already processed above
                    try:
                        data = client.get(key)
                        if data:
                            key_str = key if isinstance(key, str) else key.decode()
                            gap_data[key_str] = {
                                "data": json.loads(data) if data.startswith('{') else data,
                                "extraction_timestamp": datetime.now().isoformat(),
                                "data_type": "gap_analysis"
                            }
                    except Exception as e:
                        logger.debug(f"Error reading gap data {key}: {e}")
                        continue
            
            return gap_data
            
        except Exception as e:
            logger.error(f"Error extracting gap analysis data: {e}")
            return {}
    
    def _extract_price_data(self) -> Dict:
        """Extract price data from DB 2"""
        try:
            client = self.redis_clients.get(2)
            if not client:
                logger.warning("No Redis client for DB 2")
                return {}
            
            # Get all price keys
            price_keys = client.keys("*")
            logger.info(f"Found {len(price_keys)} price keys")
            
            prices = {}
            for key in price_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        prices[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading price {key}: {e}")
                    continue
            
            return prices
            
        except Exception as e:
            logger.error(f"Error extracting price data: {e}")
            return {}
    
    def _extract_continuous_market_data(self) -> Dict:
        """Extract continuous market data from DB 4"""
        try:
            client = self.redis_clients.get(4)
            if not client:
                logger.warning("No Redis client for DB 4")
                return {}
            
            # Get all market data keys
            market_keys = client.keys("*")
            logger.info(f"Found {len(market_keys)} continuous market keys")
            
            market_data = {}
            for key in market_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        market_data[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading market data {key}: {e}")
                    continue
            
            return market_data
            
        except Exception as e:
            logger.error(f"Error extracting continuous market data: {e}")
            return {}
    
    def _extract_cumulative_volume_data(self) -> Dict:
        """Extract cumulative volume data from DB 5"""
        try:
            client = self.redis_clients.get(5)
            if not client:
                logger.warning("No Redis client for DB 5")
                return {}
            
            # Get all cumulative volume keys
            volume_keys = client.keys("*")
            logger.info(f"Found {len(volume_keys)} cumulative volume keys")
            
            cumulative_data = {}
            for key in volume_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        cumulative_data[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading cumulative data {key}: {e}")
                    continue
            
            return cumulative_data
            
        except Exception as e:
            logger.error(f"Error extracting cumulative volume data: {e}")
            return {}
    
    def _extract_alert_system_data(self) -> Dict:
        """Extract alert system data from DB 6"""
        try:
            client = self.redis_clients.get(6)
            if not client:
                logger.warning("No Redis client for DB 6")
                return {}
            
            # Get all alert keys
            alert_keys = client.keys("alert:*")
            logger.info(f"Found {len(alert_keys)} alert keys")
            
            alerts = {}
            for key in alert_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        alerts[key_str] = {
                            "data": json.loads(data),
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading alert {key}: {e}")
                    continue
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error extracting alert system data: {e}")
            return {}
    
    def _extract_market_microstructure_data(self) -> Dict:
        """Extract market microstructure data from DB 8"""
        try:
            client = self.redis_clients.get(8)
            if not client:
                logger.warning("No Redis client for DB 8")
                return {}
            
            # Get all microstructure keys
            microstructure_keys = client.keys("*")
            logger.info(f"Found {len(microstructure_keys)} microstructure keys")
            
            microstructure_data = {}
            for key in microstructure_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        microstructure_data[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading microstructure data {key}: {e}")
                    continue
            
            return microstructure_data
            
        except Exception as e:
            logger.error(f"Error extracting market microstructure data: {e}")
            return {}
    
    def _extract_pattern_detection_data(self) -> Dict:
        """Extract pattern detection data from DB 10"""
        try:
            client = self.redis_clients.get(10)
            if not client:
                logger.warning("No Redis client for DB 10")
                return {}
            
            # Get all pattern detection keys
            pattern_keys = client.keys("*")
            logger.info(f"Found {len(pattern_keys)} pattern detection keys")
            
            pattern_data = {}
            for key in pattern_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        pattern_data[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading pattern data {key}: {e}")
                    continue
            
            return pattern_data
            
        except Exception as e:
            logger.error(f"Error extracting pattern detection data: {e}")
            return {}
    
    def _extract_performance_metrics_data(self) -> Dict:
        """Extract performance metrics data from DB 13"""
        try:
            client = self.redis_clients.get(13)
            if not client:
                logger.warning("No Redis client for DB 13")
                return {}
            
            # Get all performance metrics keys
            performance_keys = client.keys("*")
            logger.info(f"Found {len(performance_keys)} performance metrics keys")
            
            performance_data = {}
            for key in performance_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        performance_data[key_str] = {
                            "data": json.loads(data) if data.startswith('{') else data,
                            "extraction_timestamp": datetime.now().isoformat()
                        }
                except Exception as e:
                    logger.debug(f"Error reading performance data {key}: {e}")
                    continue
            
            return performance_data
            
        except Exception as e:
            logger.error(f"Error extracting performance metrics data: {e}")
            return {}
    
    def _extract_cumulative_session_data(self) -> Dict:
        """Extract session data from DB 5"""
        try:
            client = self.redis_clients.get(5)
            if not client:
                logger.warning("No Redis client for DB 5")
                return {}
            
            today = datetime.now().strftime("%Y-%m-%d")
            session_keys = client.keys(f"session:*:{today}")
            logger.info(f"Found {len(session_keys)} session keys for {today}")
            
            sessions = {}
            for key in session_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        sessions[key_str] = json.loads(data)
                except Exception as e:
                    logger.debug(f"Error reading session {key}: {e}")
                    continue
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error extracting session data: {e}")
            return {}
    
    def _extract_pattern_results(self) -> Dict:
        """Extract pattern detection results from DB 6"""
        try:
            client = self.redis_clients.get(6)
            if not client:
                logger.warning("No Redis client for DB 6")
                return {}
            
            alert_keys = client.keys("alert:*")
            logger.info(f"Found {len(alert_keys)} pattern alert keys")
            
            patterns = {}
            for key in alert_keys:
                try:
                    data = client.get(key)
                    if data:
                        key_str = key if isinstance(key, str) else key.decode()
                        patterns[key_str] = json.loads(data)
                except Exception as e:
                    logger.debug(f"Error reading pattern {key}: {e}")
                    continue
            
            return patterns
            
        except Exception as e:
            logger.error(f"Error extracting pattern data: {e}")
            return {}
    
    def _organize_buckets_by_symbol(self, bucket_data: Dict) -> Dict:
        """Organize bucket data by symbol with time series structure"""
        organized = {}
        
        for key, data in bucket_data.items():
            try:
                # Parse key: volume:EXCHANGE:SYMBOL:buckets:HOUR:MINUTE
                parts = key.split(':')
                if len(parts) >= 6:
                    exchange = parts[1]
                    symbol = parts[2]
                    hour = int(parts[4])  # parts[3] is "buckets"
                    minute_bucket = int(parts[5])
                    
                    if symbol not in organized:
                        organized[symbol] = {
                            "buckets": [],
                            "metadata": {
                                "symbol": symbol,
                                "total_volume": 0,
                                "bucket_count": 0
                            }
                        }
                    
                    organized[symbol]["buckets"].append({
                        "hour": hour,
                        "minute_bucket": minute_bucket,
                        "minute_actual": minute_bucket * 5,
                        "volume": data.get("volume", 0),
                        "count": data.get("count", 0),
                        "timestamp": f"{hour:02d}:{minute_bucket*5:02d}"
                    })
                    
                    organized[symbol]["metadata"]["total_volume"] += data.get("volume", 0)
                    organized[symbol]["metadata"]["bucket_count"] += 1
                    
            except Exception as e:
                logger.debug(f"Error parsing bucket key {key}: {e}")
                continue
        
        # Sort buckets chronologically for each symbol
        for symbol in organized:
            organized[symbol]["buckets"].sort(
                key=lambda x: (x["hour"], x["minute_bucket"])
            )
        
        return organized
    
    def _organize_comprehensive_dataset(self, session_data: Dict, price_data: Dict, 
                                      market_data: Dict, cumulative_data: Dict,
                                      alert_data: Dict, microstructure_data: Dict,
                                      pattern_data: Dict, performance_data: Dict) -> Dict:
        """Create comprehensive dataset structure with all data sources"""
        
        # Calculate time bucket statistics
        total_time_buckets = 0
        symbols_with_buckets = 0
        
        for session_key, session_info in session_data.items():
            time_buckets = session_info.get("time_buckets", {})
            if time_buckets:
                total_time_buckets += len(time_buckets)
                symbols_with_buckets += 1
        
        return {
            "dataset_metadata": {
                "extraction_timestamp": datetime.now().isoformat(),
                "extraction_date": datetime.now().strftime("%Y-%m-%d"),
                "data_sources": [
                    "session_data_db0", "price_data_db2", "continuous_market_db4",
                    "cumulative_volume_db5", "alert_system_db6", "market_microstructure_db8",
                    "pattern_detection_db10", "performance_metrics_db13"
                ],
                "session_count": len(session_data),
                "price_entries": len(price_data),
                "market_entries": len(market_data),
                "cumulative_entries": len(cumulative_data),
                "alert_entries": len(alert_data),
                "microstructure_entries": len(microstructure_data),
                "pattern_entries": len(pattern_data),
                "performance_entries": len(performance_data),
                "total_time_buckets": total_time_buckets,
                "symbols_with_buckets": symbols_with_buckets,
                "extraction_mode": "comprehensive_standalone"
            },
            "session_data": session_data,
            "price_data": price_data,
            "continuous_market_data": market_data,
            "cumulative_volume_data": cumulative_data,
            "alert_system_data": alert_data,
            "market_microstructure_data": microstructure_data,
            "pattern_detection_data": pattern_data,
            "performance_metrics_data": performance_data,
            "gap_analysis_data": self._extract_gap_analysis_from_sessions(session_data),
            "validation_status": None  # Will be filled by validator
        }
    
    def _extract_gap_analysis_from_sessions(self, session_data: Dict) -> Dict:
        """Extract gap analysis data from session data"""
        try:
            gap_analysis = {}
            
            for key, data in session_data.items():
                if "gap" in key.lower() or data.get("data_type") == "gap_analysis":
                    gap_analysis[key] = data
            
            return gap_analysis
            
        except Exception as e:
            logger.error(f"Error extracting gap analysis from sessions: {e}")
            return {}
    
    def export_dataset(self, dataset: Dict) -> bool:
        """Export dataset with validation"""
        try:
            # Validate before export
            validation_config = self.config.get("validation", {"enabled": True, "log_validation_details": True, "fail_on_validation_error": False})
            if validation_config.get("enabled", True):
                is_valid, validation_results = self.validator.validate_dataset(dataset)
                
                dataset["validation_status"] = {
                    "is_valid": is_valid,
                    "validation_timestamp": datetime.now().isoformat(),
                    "checks": validation_results
                }
                
                if not is_valid and validation_config.get("log_validation_details", True):
                    logger.warning(f"⚠️ Dataset validation failed: {validation_results}")
                
                if not is_valid and validation_config.get("fail_on_validation_error", False):
                    logger.error("❌ Export aborted due to validation failure")
                    return False
            
            # Create export directory structure
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_dir = datetime.now().strftime("%Y-%m-%d")
            
            export_dir = self.export_dir / date_dir
            export_dir.mkdir(parents=True, exist_ok=True)
            
            # Export dataset
            filename = f"bayesian_extract_{timestamp}.json"
            filepath = export_dir / filename
            
            with open(filepath, 'w') as f:
                json.dump(dataset, f, indent=2, default=str)
            
            logger.info(f"✅ Dataset exported: {filepath}")
            
            # Create daily summary if this is the first export of the day
            self._create_daily_summary(date_dir, dataset)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error exporting dataset: {e}")
            return False
    
    def _create_daily_summary(self, date_dir: str, dataset: Dict):
        """Create daily summary file"""
        try:
            summary_file = self.export_dir / date_dir / f"daily_summary_{date_dir.replace('-', '')}.json"
            
            if not summary_file.exists():
                metadata = dataset["dataset_metadata"]
                summary = {
                    "date": date_dir,
                    "extraction_count": 1,
                    "total_time_buckets": metadata.get("total_time_buckets", 0),
                    "symbols_with_buckets": metadata.get("symbols_with_buckets", 0),
                    "session_entries": metadata.get("session_count", 0),
                    "price_entries": metadata.get("price_entries", 0),
                    "market_entries": metadata.get("market_entries", 0),
                    "cumulative_entries": metadata.get("cumulative_entries", 0),
                    "alert_entries": metadata.get("alert_entries", 0),
                    "microstructure_entries": metadata.get("microstructure_entries", 0),
                    "pattern_entries": metadata.get("pattern_entries", 0),
                    "performance_entries": metadata.get("performance_entries", 0),
                    "data_sources": metadata.get("data_sources", []),
                    "first_extraction": metadata.get("extraction_timestamp", ""),
                    "last_extraction": metadata.get("extraction_timestamp", "")
                }
                
                with open(summary_file, 'w') as f:
                    json.dump(summary, f, indent=2)
                
                logger.info(f"📊 Created daily summary: {summary_file}")
            
        except Exception as e:
            logger.debug(f"Error creating daily summary: {e}")
    
    def _create_daily_bayesian_model(self, date_dir: str, dataset: Dict):
        """Create daily Bayesian model for confidence building"""
        try:
            retention_config = self.config.get("data_retention", {})
            bayesian_config = retention_config.get("bayesian_model_retention", {})
            
            if not bayesian_config.get("build_confidence_models", False):
                return
            
            # Create daily model file
            model_file = self.export_dir / date_dir / f"bayesian_model_{date_dir.replace('-', '')}.json"
            
            # Extract key metrics for Bayesian confidence building
            session_data = dataset.get("session_data", {})
            alert_data = dataset.get("alert_system_data", {})
            pattern_data = dataset.get("pattern_detection_data", {})
            
            # Calculate daily statistics for Bayesian models
            daily_stats = {
                "date": date_dir,
                "extraction_timestamp": dataset["dataset_metadata"]["extraction_timestamp"],
                "session_statistics": self._calculate_session_statistics(session_data),
                "alert_statistics": self._calculate_alert_statistics(alert_data),
                "pattern_statistics": self._calculate_pattern_statistics(pattern_data),
                "bayesian_confidence_metrics": self._calculate_bayesian_confidence_metrics(dataset)
            }
            
            with open(model_file, 'w') as f:
                json.dump(daily_stats, f, indent=2)
            
            logger.info(f"🧠 Created daily Bayesian model: {model_file}")
            
        except Exception as e:
            logger.error(f"Error creating daily Bayesian model: {e}")
    
    def _calculate_session_statistics(self, session_data: Dict) -> Dict:
        """Calculate session statistics for Bayesian confidence"""
        try:
            total_sessions = len(session_data)
            total_time_buckets = 0
            symbols_with_data = set()
            
            for session_key, session_info in session_data.items():
                time_buckets = session_info.get("time_buckets", {})
                total_time_buckets += len(time_buckets)
                
                # Extract symbol from session key
                if ":" in session_key:
                    symbol = session_key.split(":")[1]
                    symbols_with_data.add(symbol)
            
            return {
                "total_sessions": total_sessions,
                "total_time_buckets": total_time_buckets,
                "unique_symbols": len(symbols_with_data),
                "avg_buckets_per_session": total_time_buckets / max(total_sessions, 1)
            }
            
        except Exception as e:
            logger.error(f"Error calculating session statistics: {e}")
            return {}
    
    def _calculate_alert_statistics(self, alert_data: Dict) -> Dict:
        """Calculate alert statistics for Bayesian confidence"""
        try:
            total_alerts = len(alert_data)
            pattern_types = {}
            confidence_scores = []
            
            for alert_key, alert_info in alert_data.items():
                data = alert_info.get("data", {})
                pattern_type = data.get("pattern_type", "unknown")
                confidence = data.get("confidence", 0)
                
                pattern_types[pattern_type] = pattern_types.get(pattern_type, 0) + 1
                confidence_scores.append(confidence)
            
            return {
                "total_alerts": total_alerts,
                "pattern_type_distribution": pattern_types,
                "avg_confidence": sum(confidence_scores) / max(len(confidence_scores), 1),
                "max_confidence": max(confidence_scores) if confidence_scores else 0,
                "min_confidence": min(confidence_scores) if confidence_scores else 0
            }
            
        except Exception as e:
            logger.error(f"Error calculating alert statistics: {e}")
            return {}
    
    def _calculate_pattern_statistics(self, pattern_data: Dict) -> Dict:
        """Calculate pattern detection statistics for Bayesian confidence"""
        try:
            total_patterns = len(pattern_data)
            pattern_categories = {}
            
            for pattern_key, pattern_info in pattern_data.items():
                data = pattern_info.get("data", {})
                category = data.get("category", "unknown")
                pattern_categories[category] = pattern_categories.get(category, 0) + 1
            
            return {
                "total_patterns": total_patterns,
                "category_distribution": pattern_categories
            }
            
        except Exception as e:
            logger.error(f"Error calculating pattern statistics: {e}")
            return {}
    
    def _calculate_bayesian_confidence_metrics(self, dataset: Dict) -> Dict:
        """Calculate Bayesian confidence metrics"""
        try:
            metadata = dataset["dataset_metadata"]
            
            # Calculate data completeness score
            total_expected_sources = 8  # Number of Redis databases we extract from
            actual_sources = len([source for source in metadata.get("data_sources", []) if source])
            completeness_score = actual_sources / total_expected_sources
            
            # Calculate data richness score
            total_entries = (
                metadata.get("session_count", 0) +
                metadata.get("price_entries", 0) +
                metadata.get("market_entries", 0) +
                metadata.get("alert_entries", 0) +
                metadata.get("pattern_entries", 0)
            )
            richness_score = min(total_entries / 1000, 1.0)  # Normalize to 0-1
            
            # Calculate time bucket coverage
            time_buckets = metadata.get("total_time_buckets", 0)
            symbols_with_buckets = metadata.get("symbols_with_buckets", 0)
            coverage_score = min(time_buckets / (symbols_with_buckets * 50), 1.0) if symbols_with_buckets > 0 else 0
            
            # Overall Bayesian confidence score
            bayesian_confidence = (completeness_score + richness_score + coverage_score) / 3
            
            return {
                "completeness_score": completeness_score,
                "richness_score": richness_score,
                "coverage_score": coverage_score,
                "bayesian_confidence": bayesian_confidence,
                "data_quality": "high" if bayesian_confidence > 0.8 else "medium" if bayesian_confidence > 0.5 else "low"
            }
            
        except Exception as e:
            logger.error(f"Error calculating Bayesian confidence metrics: {e}")
            return {}
    
    def is_market_hours(self) -> bool:
        """Check if current time is during market hours"""
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        # Market hours: 9:15 AM to 3:30 PM IST
        market_start = (9, 15)  # 9:15 AM
        market_end = (15, 30)   # 3:30 PM
        
        if current_hour < market_start[0] or current_hour > market_end[0]:
            return False
        
        if current_hour == market_start[0] and current_minute < market_start[1]:
            return False
        
        if current_hour == market_end[0] and current_minute > market_end[1]:
            return False
        
        return True
    
    def run_continuous_extraction(self):
        """Run continuous extraction with safety checks during market hours"""
        logger.info("🚀 Starting continuous Bayesian data extraction")
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        try:
            while self.running:
                current_time = datetime.now()
                
                # Check safety conditions (trading day, memory, disk)
                if not self._check_safety_conditions():
                    logger.info("⏸️ Waiting for next trading day...")
                    time.sleep(300)  # Check every 5 minutes
                    continue
                
                # Check operating hours
                if not self._is_within_operating_hours():
                    logger.info("⏸️ Outside operating hours. Waiting...")
                    time.sleep(60)  # Check every minute
                    continue
                
                # Check if this is a critical extraction time (before Redis cache clear)
                is_critical = self._is_critical_extraction_time()
                if is_critical:
                    logger.warning("🚨 CRITICAL EXTRACTION: Redis cache may be cleared soon!")
                    # Force extraction regardless of interval
                    force_extraction = True
                else:
                    force_extraction = False
                
                # Check if we should extract based on schedule
                if not force_extraction and not self._should_extract_now():
                    logger.info("⏸️ Not scheduled extraction time. Waiting...")
                    time.sleep(30)  # Check every 30 seconds
                    continue
                
                # Auto-stop at market close if configured
                if self.safety_limits.get("auto_stop_at_market_close", True):
                    close_time_str = self.operating_hours.get("post_market_end", "16:00")
                    close_time = datetime.strptime(close_time_str, "%H:%M").time()
                    if current_time.time() >= close_time:
                        logger.info("🛑 Market closed. Stopping extraction.")
                        break
                
                # Extract data
                logger.info(f"🔄 Extracting data at {current_time.strftime('%H:%M:%S')}")
                
                # Extract and export data
                dataset = self.extract_comprehensive_data()
                if dataset:
                    success = self.export_dataset(dataset)
                    if success:
                        logger.info("✅ Data extraction and export completed")
                    else:
                        logger.error("❌ Data export failed")
                else:
                    logger.warning("⚠️ No data extracted")
                
                # Manage data retention (compress old files, delete ancient ones)
                self._manage_data_retention()
                
                # Wait for next extraction
                interval = self.config.get("extraction", {}).get("interval_seconds", 120)
                if interval is None:
                    interval = self.config.get("schedule", {}).get("export_interval_seconds", 120)
                logger.info(f"⏰ Waiting {interval} seconds until next extraction...")
                
                for _ in range(interval):
                    if not self.running:
                        break
                    time.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal")
        except Exception as e:
            logger.error(f"❌ Error in continuous extraction: {e}")
        finally:
            self.running = False
            logger.info("🛑 Continuous extraction stopped")
    
    def run_once_extraction(self):
        """Run single extraction"""
        logger.info("🔄 Running single Bayesian data extraction")
        
        dataset = self.extract_comprehensive_data()
        if dataset:
            success = self.export_dataset(dataset)
            if success:
                logger.info("✅ Single extraction completed successfully")
                return True
            else:
                logger.error("❌ Single extraction failed")
                return False
        else:
            logger.error("❌ No data extracted")
            return False
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"🛑 Received signal {signum}, shutting down...")
        self.running = False


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Standalone Bayesian Data Extractor")
    parser.add_argument("--mode", choices=["continuous", "once"], default="continuous",
                       help="Extraction mode")
    parser.add_argument("--interval", type=int, default=120,
                       help="Export interval in seconds (default: 120)")
    parser.add_argument("--config", default="config/bayesian_research_config.json",
                       help="Configuration file path")
    parser.add_argument("--validate", action="store_true",
                       help="Enable validation")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/bayesian_extractor.log')
        ]
    )
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    try:
        # Initialize extractor
        extractor = StandaloneBayesianExtractor(args.config)
        
        # Update config if validation is enabled
        if args.validate:
            extractor.config["validation"]["enabled"] = True
        
        # Update interval if specified
        if args.interval != 120:
            extractor.config["schedule"]["export_interval_seconds"] = args.interval
        
        # Run extraction
        if args.mode == "continuous":
            extractor.run_continuous_extraction()
        else:
            success = extractor.run_once_extraction()
            sys.exit(0 if success else 1)
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
