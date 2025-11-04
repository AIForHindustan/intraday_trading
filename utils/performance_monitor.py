#!/usr/bin/env python3
"""
Performance Monitor for Redis Streams and Historical Archive
Monitors both Redis and archival performance in real-time
"""

import logging
import time
import threading
from typing import Optional, Dict, Any
from datetime import datetime
from redis_files.redis_manager import RedisManager82

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Monitor Redis streams and historical archive performance"""
    
    def __init__(
        self,
        redis_db: int = 1,
        check_interval: int = 60,
        streams_to_monitor: Optional[list] = None,
        historical_archive_instance: Optional[Any] = None
    ):
        """
        Initialize performance monitor
        
        Args:
            redis_db: Redis database number for streams
            check_interval: Monitoring interval in seconds (default: 60)
            streams_to_monitor: List of stream names to monitor (default: common streams)
            historical_archive_instance: HistoricalArchive instance to monitor
        """
        self.redis_db = redis_db
        self.check_interval = check_interval
        self.streams_to_monitor = streams_to_monitor or [
            'ticks:intraday:processed',
            'ticks:raw:binary',
            'alerts:stream'
        ]
        self.historical_archive = historical_archive_instance
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Statistics
        self.stats = {
            'checks_performed': 0,
            'last_check_time': None,
            'errors': 0
        }
    
    def start(self):
        """Start the monitoring thread"""
        if self.running:
            logger.warning("Performance monitor already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="PerformanceMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"✅ Performance monitor started (interval: {self.check_interval}s)")
    
    def stop(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5.0)
        logger.info("📊 Performance monitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self._check_performance()
                self.stats['checks_performed'] += 1
                self.stats['last_check_time'] = datetime.now()
                
                # Sleep for check interval
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"❌ Error in performance monitor: {e}")
                self.stats['errors'] += 1
                time.sleep(self.check_interval)
    
    def _check_performance(self):
        """Check Redis and archive performance"""
        try:
            # Get Redis client
            # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
            redis_client = RedisManager82.get_client(process_name="performance_monitor", db=self.redis_db)
            if not redis_client:
                logger.warning("⚠️  Could not get Redis client for monitoring")
                return
            
            # Get Redis info
            redis_info = redis_client.info()
            memory_used = redis_info.get('used_memory_human', 'N/A')
            memory_peak = redis_info.get('used_memory_peak_human', 'N/A')
            connected_clients = redis_info.get('connected_clients', 0)
            
            # Monitor streams
            stream_info = {}
            for stream_name in self.streams_to_monitor:
                try:
                    stream_len = redis_client.xlen(stream_name)
                    
                    # Get consumer group info
                    try:
                        groups = redis_client.xinfo_groups(stream_name)
                        group_info = []
                        for group in groups:
                            group_name = group.get('name', 'unknown')
                            if isinstance(group_name, bytes):
                                group_name = group_name.decode('utf-8')
                            pending = group.get('pending', 0)
                            consumers = group.get('consumers', 0)
                            group_info.append(f"{group_name}(pending:{pending},consumers:{consumers})")
                    except Exception:
                        group_info = []
                    
                    stream_info[stream_name] = {
                        'length': stream_len,
                        'groups': group_info
                    }
                except Exception as e:
                    if "no such key" not in str(e).lower():
                        logger.debug(f"Could not get info for stream {stream_name}: {e}")
                    stream_info[stream_name] = {'length': 0, 'groups': []}
            
            # Get archive buffer size
            archive_buffer_size = 0
            archive_stats = {}
            if self.historical_archive:
                try:
                    # Get buffer size (thread-safe)
                    if hasattr(self.historical_archive, 'parquet_buffer'):
                        with getattr(self.historical_archive, 'parquet_batch_lock', threading.Lock()):
                            archive_buffer_size = len(self.historical_archive.parquet_buffer)
                    
                    # Get archive stats
                    if hasattr(self.historical_archive, 'get_stats'):
                        archive_stats = self.historical_archive.get_stats()
                    elif hasattr(self.historical_archive, 'stats'):
                        archive_stats = self.historical_archive.stats
                except Exception as e:
                    logger.debug(f"Could not get archive stats: {e}")
            
            # Log performance metrics
            timestamp = datetime.now().strftime("%H:%M:%S")
            logger.info(
                f"📊 [{timestamp}] Performance Monitor:"
            )
            
            # Stream info
            for stream_name, info in stream_info.items():
                groups_str = ", ".join(info['groups']) if info['groups'] else "no groups"
                logger.info(
                    f"   Stream {stream_name}: {info['length']} msgs | Groups: {groups_str}"
                )
            
            # Redis memory
            logger.info(
                f"   Redis Memory: {memory_used} (peak: {memory_peak}) | "
                f"Connected Clients: {connected_clients}"
            )
            
            # Archive info
            if self.historical_archive:
                logger.info(
                    f"   Archive Buffer: {archive_buffer_size} ticks | "
                    f"Archived: {archive_stats.get('ticks_archived', 0)} | "
                    f"Batches: {archive_stats.get('batches_written', 0)} | "
                    f"Errors: {archive_stats.get('errors', 0)}"
                )
            
            # Check for warnings
            self._check_warnings(stream_info, redis_info, archive_buffer_size, archive_stats)
            
        except Exception as e:
            logger.error(f"❌ Error checking performance: {e}")
            self.stats['errors'] += 1
    
    def _check_warnings(self, stream_info: Dict, redis_info: Dict, archive_buffer_size: int, archive_stats: Dict):
        """Check for performance warnings"""
        warnings = []
        
        # Check stream lengths (should be trimmed)
        for stream_name, info in stream_info.items():
            stream_len = info.get('length', 0)
            if stream_name == 'ticks:intraday:processed' and stream_len > 10000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len} (target: 5000)")
            elif stream_name == 'ticks:raw:binary' and stream_len > 20000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len} (target: 10000)")
            elif stream_name == 'alerts:stream' and stream_len > 2000:
                warnings.append(f"⚠️  {stream_name} too long: {stream_len} (target: 1000)")
        
        # Check pending messages in consumer groups
        for stream_name, info in stream_info.items():
            for group_str in info.get('groups', []):
                if 'pending:' in group_str:
                    try:
                        pending = int(group_str.split('pending:')[1].split(',')[0])
                        if pending > 1000:
                            warnings.append(f"⚠️  {stream_name} has {pending} pending messages")
                    except:
                        pass
        
        # Check Redis memory
        memory_used_bytes = redis_info.get('used_memory', 0)
        maxmemory = redis_info.get('maxmemory', 0)
        if maxmemory > 0:
            memory_percent = (memory_used_bytes / maxmemory) * 100
            if memory_percent > 90:
                warnings.append(f"⚠️  Redis memory usage: {memory_percent:.1f}%")
        
        # Check archive buffer
        if archive_buffer_size > 5000:
            warnings.append(f"⚠️  Archive buffer large: {archive_buffer_size} ticks")
        
        # Check archive errors
        archive_errors = archive_stats.get('errors', 0)
        if archive_errors > 10:
            warnings.append(f"⚠️  Archive has {archive_errors} errors")
        
        # Log warnings
        if warnings:
            for warning in warnings:
                logger.warning(warning)
    
    def get_stats(self) -> Dict:
        """Get monitoring statistics"""
        return {
            **self.stats,
            'interval_seconds': self.check_interval,
            'streams_monitored': len(self.streams_to_monitor),
            'archive_monitored': self.historical_archive is not None
        }


def monitor_performance(
    redis_db: int = 1,
    check_interval: int = 60,
    historical_archive_instance: Optional[Any] = None
) -> PerformanceMonitor:
    """
    Start performance monitoring
    
    Args:
        redis_db: Redis database number for streams
        check_interval: Monitoring interval in seconds
        historical_archive_instance: HistoricalArchive instance to monitor
    
    Returns:
        PerformanceMonitor instance
    """
    monitor = PerformanceMonitor(
        redis_db=redis_db,
        check_interval=check_interval,
        historical_archive_instance=historical_archive_instance
    )
    monitor.start()
    return monitor


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("📊 Starting Performance Monitor...")
    monitor = monitor_performance(check_interval=60)
    
    try:
        # Run until interrupted
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🛑 Stopping performance monitor...")
        monitor.stop()
        print("✅ Performance monitor stopped")


