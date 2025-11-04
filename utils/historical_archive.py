#!/usr/bin/env python3
"""
Tier 2: Historical Storage Archive
Archives ticks to PostgreSQL/TimescaleDB for historical analysis (keep everything)
"""

import json
import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import threading
from queue import Queue, Empty
import time

logger = logging.getLogger(__name__)

# Try to import PostgreSQL/TimescaleDB dependencies
try:
    import psycopg2
    from psycopg2.extras import execute_values
    import psycopg2.pool
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    logger.warning("PostgreSQL (psycopg2) not available - historical archival will use Parquet fallback only")

# Configuration
STREAM_CONFIG = {
    'ticks:raw:binary': {'maxlen': 10000},      # Last 10k ticks
    'ticks:intraday:processed': {'maxlen': 5000}, # Last 5k processed
    'alerts:stream': {'maxlen': 1000}           # Last 1k alerts
}


class HistoricalArchive:
    """
    Tier 2: Historical Storage Archive
    Archives all tick data to PostgreSQL/TimescaleDB for historical analysis.
    Keeps everything while Redis streams are trimmed for performance.
    """
    
    def __init__(
        self,
        postgres_config: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        batch_interval: float = 5.0,
        enable_postgres: bool = True,
        fallback_to_parquet: bool = True
    ):
        """
        Initialize historical archive
        
        Args:
            postgres_config: PostgreSQL connection config
            batch_size: Number of ticks to batch before writing
            batch_interval: Seconds to wait before flushing batch
            enable_postgres: Enable PostgreSQL archival
            fallback_to_parquet: Fallback to Parquet if PostgreSQL unavailable
        """
        self.postgres_config = postgres_config or {}
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        self.enable_postgres = enable_postgres and POSTGRES_AVAILABLE
        self.fallback_to_parquet = fallback_to_parquet
        
        # Batch queue for async processing
        self.archive_queue = Queue(maxsize=10000)  # Buffer up to 10k ticks
        self.running = False
        self.archive_thread: Optional[threading.Thread] = None
        
        # Statistics
        self.stats = {
            'ticks_archived': 0,
            'batches_written': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # PostgreSQL connection pool (if available)
        self.pg_pool = None
        if self.enable_postgres:
            self._init_postgres()
        
        # Parquet fallback (always enabled for reliability)
        if self.fallback_to_parquet:
            self.parquet_buffer: List[Dict] = []
            self.parquet_buffer_size = batch_size  # Use same batch size for Parquet
            self.parquet_batch_lock = threading.Lock()  # Thread-safe batching
            self.parquet_dir = Path("historical_archive/parquet")
            self.parquet_dir.mkdir(parents=True, exist_ok=True)
    
    def _init_postgres(self):
        """Initialize PostgreSQL connection pool"""
        if not self.enable_postgres:
            return
        
        try:
            # Get connection params from config or environment
            db_config = {
                'host': self.postgres_config.get('host', 'localhost'),
                'port': self.postgres_config.get('port', 5432),
                'database': self.postgres_config.get('database', 'tick_data'),
                'user': self.postgres_config.get('user', 'postgres'),
                'password': self.postgres_config.get('password', ''),
            }
            
            # Create connection pool
            self.pg_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                **db_config
            )
            
            # Ensure TimescaleDB hypertable exists
            self._ensure_timescaledb_schema()
            
            logger.info("✅ PostgreSQL/TimescaleDB archive initialized")
            
        except Exception as e:
            logger.warning(f"⚠️ PostgreSQL archive unavailable: {e}. Using Parquet fallback only.")
            self.enable_postgres = False
            self.pg_pool = None
    
    def _ensure_timescaledb_schema(self):
        """Ensure TimescaleDB hypertable schema exists"""
        if not self.pg_pool:
            return
        
        try:
            conn = self.pg_pool.getconn()
            try:
                cursor = conn.cursor()
                
                # Create table if not exists
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS tick_data_historical (
                    id BIGSERIAL,
                    instrument_token BIGINT NOT NULL,
                    symbol VARCHAR(100),
                    exchange VARCHAR(20),
                    timestamp TIMESTAMPTZ NOT NULL,
                    exchange_timestamp TIMESTAMPTZ,
                    last_price DOUBLE PRECISION,
                    volume BIGINT,
                    bucket_incremental_volume BIGINT,
                    zerodha_cumulative_volume BIGINT,
                    best_bid_price DOUBLE PRECISION,
                    best_ask_price DOUBLE PRECISION,
                    open_interest BIGINT,
                    tick_data JSONB,
                    archived_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (id, timestamp)
                );
                """
                cursor.execute(create_table_sql)
                
                # Create hypertable if TimescaleDB extension is available
                try:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
                    cursor.execute("""
                        SELECT create_hypertable(
                            'tick_data_historical',
                            'timestamp',
                            if_not_exists => TRUE
                        );
                    """)
                    logger.info("✅ TimescaleDB hypertable created/verified")
                except Exception:
                    # TimescaleDB not available, use regular table
                    logger.info("ℹ️  TimescaleDB extension not available, using regular table")
                
                # Create indexes
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tick_symbol_time 
                    ON tick_data_historical(symbol, timestamp DESC);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_tick_token_time 
                    ON tick_data_historical(instrument_token, timestamp DESC);
                """)
                
                conn.commit()
                logger.info("✅ Historical archive schema ready")
                
            finally:
                self.pg_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"❌ Error setting up PostgreSQL schema: {e}")
            self.enable_postgres = False
    
    def start(self):
        """Start the archive worker thread"""
        if self.running:
            return
        
        self.running = True
        self.archive_thread = threading.Thread(
            target=self._archive_worker,
            name="HistoricalArchiveWorker",
            daemon=True
        )
        self.archive_thread.start()
        logger.info("✅ Historical archive worker started")
    
    def stop(self):
        """Stop the archive worker and flush remaining data"""
        self.running = False
        
        if self.archive_thread:
            self.archive_thread.join(timeout=10.0)
        
        # Flush remaining queue items
        self._flush_queue()
        
        # Flush Parquet buffer (thread-safe)
        if self.fallback_to_parquet:
            with self.parquet_batch_lock:
                if self.parquet_buffer:
                    buffer_to_flush = self.parquet_buffer.copy()
                    self.parquet_buffer.clear()
                else:
                    buffer_to_flush = []
            
            if buffer_to_flush:
                self._flush_parquet_buffer_impl(buffer_to_flush)
        
        logger.info(f"📊 Historical archive stopped. Stats: {self.stats}")
    
    def archive_tick(self, tick_data: Dict[str, Any]):
        """
        Archive a tick to historical storage (non-blocking)
        
        Args:
            tick_data: Tick data dictionary to archive
        """
        if not self.running:
            self.start()
        
        try:
            # Add archival metadata
            archived_tick = {
                **tick_data,
                'archived_at': datetime.now().isoformat(),
                'archive_source': 'redis_stream'
            }
            
            # ✅ OPTIMIZATION 1: Batch Parquet writes with thread-safe buffer
            if self.fallback_to_parquet:
                with self.parquet_batch_lock:
                    self.parquet_buffer.append(archived_tick)
                    # Flush if buffer reaches batch size
                    if len(self.parquet_buffer) >= self.parquet_buffer_size:
                        # Flush in a separate thread to avoid blocking
                        threading.Thread(
                            target=self._flush_parquet_buffer_safe,
                            daemon=True
                        ).start()
            
            # Also add to queue for worker thread (PostgreSQL path)
            try:
                self.archive_queue.put_nowait(archived_tick)
            except Exception:
                # Queue full - log but don't block
                # Parquet buffer is already handled above, so this is just for PostgreSQL
                logger.debug("Archive queue full, dropping tick (Parquet buffer handled separately)")
                self.stats['errors'] += 1
                
        except Exception as e:
            logger.error(f"Error queuing tick for archive: {e}")
            self.stats['errors'] += 1
    
    def _archive_worker(self):
        """Background worker that processes archive queue"""
        batch = []
        last_flush = time.time()
        
        while self.running:
            try:
                # Collect ticks from queue
                try:
                    tick = self.archive_queue.get(timeout=1.0)
                    batch.append(tick)
                except Empty:
                    # Timeout - check if we should flush batch
                    if batch and (time.time() - last_flush) >= self.batch_interval:
                        self._write_batch(batch)
                        batch = []
                        last_flush = time.time()
                    continue
                
                # Flush batch if it reaches size
                if len(batch) >= self.batch_size:
                    self._write_batch(batch)
                    batch = []
                    last_flush = time.time()
                
            except Exception as e:
                logger.error(f"Error in archive worker: {e}")
                self.stats['errors'] += 1
                time.sleep(1)
        
        # Flush remaining batch on shutdown
        if batch:
            self._write_batch(batch)
    
    def _write_batch(self, batch: List[Dict]):
        """Write a batch of ticks to historical storage"""
        if not batch:
            return
        
        try:
            # Try PostgreSQL first
            if self.enable_postgres:
                success = self._write_to_postgres(batch)
                if success:
                    self.stats['ticks_archived'] += len(batch)
                    self.stats['batches_written'] += 1
                    return
            
            # Fallback to Parquet
            if self.fallback_to_parquet:
                self._write_to_parquet_batch(batch)
                self.stats['ticks_archived'] += len(batch)
                self.stats['batches_written'] += 1
                
        except Exception as e:
            logger.error(f"Error writing archive batch: {e}")
            self.stats['errors'] += 1
    
    def _write_to_postgres(self, batch: List[Dict]) -> bool:
        """Write batch to PostgreSQL/TimescaleDB"""
        if not self.pg_pool:
            return False
        
        try:
            conn = self.pg_pool.getconn()
            try:
                cursor = conn.cursor()
                
                # Prepare data for batch insert
                values = []
                for tick in batch:
                    # Extract key fields
                    instrument_token = tick.get('instrument_token', 0)
                    symbol = tick.get('symbol', '')
                    timestamp = tick.get('timestamp') or tick.get('exchange_timestamp')
                    if isinstance(timestamp, str):
                        try:
                            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        except:
                            timestamp = datetime.now()
                    elif timestamp is None:
                        timestamp = datetime.now()
                    
                    values.append((
                        instrument_token,
                        symbol,
                        tick.get('exchange', ''),
                        timestamp,
                        tick.get('exchange_timestamp'),
                        tick.get('last_price'),
                        tick.get('volume') or tick.get('zerodha_cumulative_volume', 0),
                        tick.get('bucket_incremental_volume', 0),
                        tick.get('zerodha_cumulative_volume', 0),
                        tick.get('best_bid_price'),
                        tick.get('best_ask_price'),
                        tick.get('open_interest', 0),
                        json.dumps(tick)  # Full tick data as JSONB
                    ))
                
                # Batch insert
                insert_sql = """
                INSERT INTO tick_data_historical (
                    instrument_token, symbol, exchange, timestamp, exchange_timestamp,
                    last_price, volume, bucket_incremental_volume, zerodha_cumulative_volume,
                    best_bid_price, best_ask_price, open_interest, tick_data
                ) VALUES %s
                """
                execute_values(cursor, insert_sql, values)
                conn.commit()
                
                logger.debug(f"✅ Archived {len(batch)} ticks to PostgreSQL")
                return True
                
            finally:
                self.pg_pool.putconn(conn)
                
        except Exception as e:
            logger.error(f"❌ Error writing to PostgreSQL: {e}")
            return False
    
    def _write_to_parquet_batch(self, batch: List[Dict]):
        """Write batch to Parquet file (fallback) - called from worker thread"""
        try:
            # Add to buffer with lock
            with self.parquet_batch_lock:
                self.parquet_buffer.extend(batch)
                buffer_size = len(self.parquet_buffer)
            
            # Flush if buffer is large enough
            if buffer_size >= self.parquet_buffer_size:
                self._flush_parquet_buffer_safe()
                
        except Exception as e:
            logger.error(f"Error writing to Parquet: {e}")
    
    def _flush_parquet_buffer_safe(self):
        """Thread-safe Parquet buffer flush"""
        with self.parquet_batch_lock:
            if not self.parquet_buffer:
                return
            
            # Copy buffer and clear it
            buffer_to_write = self.parquet_buffer.copy()
            self.parquet_buffer.clear()
        
        # Write outside lock to avoid blocking
        self._flush_parquet_buffer_impl(buffer_to_write)
    
    def _flush_parquet_buffer_impl(self, buffer_to_write: List[Dict]):
        """Internal implementation of Parquet buffer flush"""
        if not buffer_to_write:
            return
        
        try:
            import pandas as pd
            from datetime import datetime
            
            # Normalize data types before creating DataFrame
            normalized_buffer = self._normalize_types(buffer_to_write)
            
            # Create DataFrame and write to Parquet
            df = pd.DataFrame(normalized_buffer)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = self.parquet_dir / f"historical_archive_{timestamp}.parquet"
            
            # Use pyarrow engine for better performance
            df.to_parquet(filepath, index=False, compression='snappy', engine='pyarrow')
            logger.info(f"✅ Archived {len(buffer_to_write)} ticks to Parquet: {filepath}")
            
            # Update stats
            self.stats['ticks_archived'] += len(buffer_to_write)
            self.stats['batches_written'] += 1
            
        except Exception as e:
            logger.error(f"Error flushing Parquet buffer: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Don't clear buffer on error - will retry on next flush
            # Re-add to buffer (with lock)
            with self.parquet_batch_lock:
                self.parquet_buffer.extend(buffer_to_write)
            self.stats['errors'] += 1
    
    def _normalize_types(self, buffer: List[Dict]) -> List[Dict]:
        """Normalize data types for Parquet writing"""
        normalized_buffer = []
        for tick in buffer:
            normalized_tick = {}
            for key, value in tick.items():
                # Convert instrument_token and other numeric IDs to proper types
                if key in ('instrument_token', 'volume', 'bucket_incremental_volume', 
                          'zerodha_cumulative_volume', 'open_interest', 'count'):
                    # Convert to int64 (nullable if None)
                    if value is None:
                        normalized_tick[key] = None
                    else:
                        try:
                            normalized_tick[key] = int(value)
                        except (ValueError, TypeError):
                            normalized_tick[key] = None
                elif key in ('last_price', 'best_bid_price', 'best_ask_price', 
                             'high', 'low', 'open', 'close', 'price_change', 'expected_move'):
                    # Convert to float64 (nullable if None)
                    if value is None:
                        normalized_tick[key] = None
                    else:
                        try:
                            normalized_tick[key] = float(value)
                        except (ValueError, TypeError):
                            normalized_tick[key] = None
                else:
                    # Keep other fields as-is (strings, timestamps, etc.)
                    normalized_tick[key] = value
            normalized_buffer.append(normalized_tick)
        return normalized_buffer
    
    def _flush_queue(self):
        """Flush remaining items from queue"""
        batch = []
        while True:
            try:
                tick = self.archive_queue.get_nowait()
                batch.append(tick)
            except Empty:
                break
        
        if batch:
            self._write_batch(batch)
    
    def get_stats(self) -> Dict:
        """Get archive statistics"""
        uptime = time.time() - self.stats['start_time']
        return {
            **self.stats,
            'uptime_seconds': uptime,
            'uptime_hours': uptime / 3600,
            'ticks_per_second': self.stats['ticks_archived'] / uptime if uptime > 0 else 0,
            'queue_size': self.archive_queue.qsize(),
            'postgres_enabled': self.enable_postgres,
            'parquet_fallback_enabled': self.fallback_to_parquet
        }


# Global archive instance (singleton pattern)
_archive_instance: Optional[HistoricalArchive] = None


def get_historical_archive(
    postgres_config: Optional[Dict[str, Any]] = None,
    **kwargs
) -> HistoricalArchive:
    """Get or create global historical archive instance"""
    global _archive_instance
    
    if _archive_instance is None:
        _archive_instance = HistoricalArchive(postgres_config=postgres_config, **kwargs)
        _archive_instance.start()
    
    return _archive_instance


async def archive_to_historical_db(tick_data: Dict[str, Any]):
    """
    Async wrapper for archiving tick data to historical storage
    
    Args:
        tick_data: Tick data dictionary to archive
    """
    archive = get_historical_archive()
    archive.archive_tick(tick_data)

