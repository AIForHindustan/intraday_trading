#!/usr/bin/env python3
"""
Proactive Stream Monitor
Continuously monitors and maintains Redis stream health.

✅ SOLUTION 5: Replaces reactive optimizer with proactive monitoring
- Monitors stream lengths continuously
- Proactively trims streams when they exceed targets
- Monitors consumer group health
- Reports connection pool status
"""

import logging
import time
import threading
import sys
from pathlib import Path
from typing import Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from redis_files.redis_manager import RedisManager82

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StreamMonitor:
    """
    Proactive stream monitor that continuously maintains stream health.
    
    ✅ SOLUTION 5: Replaces reactive optimizer with continuous proactive monitoring.
    Monitors streams every 5 minutes and trims when they exceed 10% over target.
    """
    
    def __init__(self, db: int = 1, check_interval: int = 300):
        """
        Initialize stream monitor.
        
        Args:
            db: Redis database number (default: 1 for realtime streams)
            check_interval: How often to check streams in seconds (default: 300 = 5 minutes)
        """
        self.db = db
        self.check_interval = check_interval
        self.running = False
        self.monitor_thread = None
        
        # ✅ SOLUTION 4: Use PROCESS_POOL_CONFIG for monitor (3 connections)
        self.redis_client = RedisManager82.get_client(
            process_name="monitor",
            db=db,
            max_connections=None  # Use PROCESS_POOL_CONFIG (3 connections)
        )
        
        # Stream configurations with target maxlen
        self.stream_configs = {
            'ticks:raw:binary': 10000,
            'ticks:intraday:processed': 5000,
            'alerts:stream': 1000,
        }
        
        logger.info(f"✅ StreamMonitor initialized (check interval: {check_interval}s)")
    
    def start(self):
        """Start monitoring in background thread"""
        if self.running:
            logger.warning("⚠️ Stream monitor already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="StreamMonitor"
        )
        self.monitor_thread.start()
        logger.info("🚀 StreamMonitor started (proactive monitoring)")
    
    def stop(self):
        """Stop monitoring"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("🛑 StreamMonitor stopped")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                # Test connection
                self.redis_client.ping()
                consecutive_errors = 0  # Reset on success
                
                # Monitor streams
                self._monitor_streams()
                
                # Check consumer group health
                self._check_consumer_groups()
                
                # Sleep until next check
                time.sleep(self.check_interval)
                
            except Exception as e:
                consecutive_errors += 1
                error_str = str(e).lower()
                
                if "connection" in error_str or "too many connections" in error_str:
                    logger.warning(f"⚠️ Redis connection error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                    # Reconnect
                    try:
                        self.redis_client.close()
                    except:
                        pass
                    self.redis_client = RedisManager82.get_client(
                        process_name="monitor",
                        db=self.db,
                        max_connections=None
                    )
                else:
                    logger.error(f"❌ Stream monitoring error ({consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("❌ Max consecutive errors reached. Stopping monitor.")
                    self.running = False
                    break
                
                # Wait before retry
                time.sleep(min(60, self.check_interval))
    
    def _monitor_streams(self):
        """
        Monitor stream lengths and proactively trim when needed.
        """
        for stream, target_maxlen in self.stream_configs.items():
            try:
                # Check if stream exists
                if not self.redis_client.exists(stream):
                    continue  # Stream doesn't exist yet
                
                current_len = self.redis_client.xlen(stream)
                
                # Proactively trim if stream is 10% over target
                threshold = target_maxlen * 1.1
                
                if current_len > threshold:
                    # Trim stream to target
                    trimmed = self.redis_client.xtrim(
                        stream, 
                        maxlen=target_maxlen, 
                        approximate=True
                    )
                    
                    # Check new length
                    new_len = self.redis_client.xlen(stream)
                    
                    logger.warning(
                        f"🔧 Proactively trimmed {stream}: "
                        f"{current_len:,} → {new_len:,} messages "
                        f"(removed ~{trimmed:,}, target: {target_maxlen:,})"
                    )
                elif current_len > target_maxlen:
                    # Stream is over target but not at threshold yet
                    logger.debug(
                        f"ℹ️  {stream}: {current_len:,} messages "
                        f"(target: {target_maxlen:,}, will trim at {int(threshold):,})"
                    )
                else:
                    # Stream is healthy
                    logger.debug(
                        f"✅ {stream}: {current_len:,} messages (target: {target_maxlen:,})"
                    )
                    
            except Exception as e:
                error_str = str(e).lower()
                if "no such key" in error_str:
                    # Stream doesn't exist yet, skip
                    continue
                else:
                    logger.warning(f"⚠️  Error monitoring {stream}: {e}")
    
    def _check_consumer_groups(self):
        """
        Check consumer group health and report issues.
        """
        for stream in self.stream_configs.keys():
            try:
                if not self.redis_client.exists(stream):
                    continue
                
                groups = self.redis_client.xinfo_groups(stream)
                
                for group in groups:
                    try:
                        # Decode group name
                        name_bytes = group.get('name', b'')
                        if isinstance(name_bytes, bytes):
                            group_name = name_bytes.decode('utf-8')
                        else:
                            group_name = str(name_bytes)
                        
                        # Get pending count
                        pending = group.get('pending', 0)
                        if isinstance(pending, bytes):
                            pending = int(pending.decode('utf-8'))
                        
                        # Get consumer count
                        consumers = group.get('consumers', 0)
                        if isinstance(consumers, bytes):
                            consumers = int(consumers.decode('utf-8'))
                        
                        # Warn if large backlog
                        if pending > 1000:
                            logger.warning(
                                f"⚠️  Consumer group '{group_name}' in {stream}: "
                                f"{pending:,} pending messages ({consumers} consumers)"
                            )
                        elif pending > 100:
                            logger.info(
                                f"ℹ️  Consumer group '{group_name}' in {stream}: "
                                f"{pending:,} pending messages ({consumers} consumers)"
                            )
                        else:
                            logger.debug(
                                f"✅ Consumer group '{group_name}' in {stream}: "
                                f"{pending} pending ({consumers} consumers)"
                            )
                            
                    except Exception as e:
                        logger.debug(f"Error parsing group info: {e}")
                        continue
                        
            except Exception as e:
                error_str = str(e).lower()
                if "no such key" in error_str:
                    continue
                else:
                    logger.warning(f"⚠️  Error checking consumer groups for {stream}: {e}")
    
    def get_stream_status(self) -> Dict[str, Dict]:
        """
        Get current status of all monitored streams.
        
        Returns:
            Dictionary mapping stream names to status information
        """
        status = {}
        
        for stream, target_maxlen in self.stream_configs.items():
            try:
                if not self.redis_client.exists(stream):
                    status[stream] = {
                        'exists': False,
                        'length': 0,
                        'target': target_maxlen,
                        'status': 'not_exists'
                    }
                    continue
                
                current_len = self.redis_client.xlen(stream)
                health = 'healthy' if current_len <= target_maxlen else 'over_target'
                
                # Get consumer group info
                groups_info = []
                try:
                    groups = self.redis_client.xinfo_groups(stream)
                    for group in groups:
                        name_bytes = group.get('name', b'')
                        name = name_bytes.decode('utf-8') if isinstance(name_bytes, bytes) else str(name_bytes)
                        pending = group.get('pending', 0)
                        if isinstance(pending, bytes):
                            pending = int(pending.decode('utf-8'))
                        consumers = group.get('consumers', 0)
                        if isinstance(consumers, bytes):
                            consumers = int(consumers.decode('utf-8'))
                        
                        groups_info.append({
                            'name': name,
                            'pending': pending,
                            'consumers': consumers
                        })
                except:
                    pass
                
                status[stream] = {
                    'exists': True,
                    'length': current_len,
                    'target': target_maxlen,
                    'health': health,
                    'over_target_pct': ((current_len - target_maxlen) / target_maxlen * 100) if current_len > target_maxlen else 0,
                    'consumer_groups': groups_info
                }
                
            except Exception as e:
                status[stream] = {
                    'exists': False,
                    'error': str(e),
                    'status': 'error'
                }
        
        return status


def create_stream_monitor(db: int = 1, check_interval: int = 300) -> StreamMonitor:
    """
    Factory function to create and return a StreamMonitor instance.
    
    Args:
        db: Redis database number (default: 1)
        check_interval: Check interval in seconds (default: 300 = 5 minutes)
        
    Returns:
        StreamMonitor instance
    """
    return StreamMonitor(db=db, check_interval=check_interval)


if __name__ == "__main__":
    # Test script
    import signal
    
    monitor = StreamMonitor(db=1, check_interval=60)  # 1 minute for testing
    
    def signal_handler(sig, frame):
        logger.info("🛑 Received interrupt signal, stopping monitor...")
        monitor.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("🚀 Starting StreamMonitor (test mode - 60s interval)")
    logger.info("Press Ctrl+C to stop")
    
    monitor.start()
    
    # Keep running
    try:
        while monitor.running:
            time.sleep(1)
    except KeyboardInterrupt:
        monitor.stop()

