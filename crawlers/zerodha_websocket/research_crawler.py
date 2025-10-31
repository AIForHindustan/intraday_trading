import json
import logging
import struct
import time
from typing import Dict, List, Any, Optional
from pathlib import Path
import zlib
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from crawlers.base_crawler import BaseCrawler, CrawlerConfig
from crawlers.websocket_message_parser import ZerodhaWebSocketMessageParser
from crawlers.metadata_resolver import metadata_resolver

logger = logging.getLogger(__name__)


class ResearchCrawler(BaseCrawler):
    """
    Research Crawler (Crawler 3)

    Purpose:
    --------
    Specialized crawler for cross-venue analysis and research that:
    - Connects to Zerodha WebSocket in full mode
    - Writes Parquet files to disk for research and analysis
    - Pure file writing for cross-venue analysis and research
    - Optimized for data completeness and research flexibility
    - Supports SSO-XVENUE cross-venue analysis

    Dependent Scripts:
    -----------------
    - crawlers/metadata_resolver.py: Provides instrument metadata resolution
    - crawlers/websocket_message_parser.py: WebSocket message parsing
    - crawlers/base_crawler.py: Base crawler functionality

    Important Aspects:
    -----------------
    - Uses metadata_resolver for centralized instrument metadata
    - Writes Parquet files to research_data directory
    - Pure file writing only (no external data publishing)
    - Optimized for data completeness and research flexibility
    - Supports SSO-XVENUE cross-venue analysis
    - Handles research data files for analysis
           - Supports Snappy compression for storage efficiency
           - Conservative reconnection settings for stability
           - Optimized buffer size (1,500 records) for 1,466 instruments (corrected EUR/USD)
    """

    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments: List[int],
        instrument_info: Dict[int, Dict],
        data_directory: str = "crawlers/raw_data/research_data",
        websocket_url: str = "wss://ws.kite.trade",
        name: str = "research_crawler",
    ):
        """
        Initialize Research Crawler

        Args:
            api_key: Zerodha API key
            access_token: Zerodha access token
            instruments: List of instrument tokens to subscribe to
            instrument_info: Mapping of token -> instrument details
            data_directory: Directory to store Parquet research data
            websocket_url: WebSocket endpoint URL
            name: Crawler name
        """
        self.api_key = api_key
        self.access_token = access_token
        self.instruments = instruments
        self.instrument_info = instrument_info

        # Initialize research data directory
        self.data_directory = Path(data_directory)
        self.data_directory.mkdir(parents=True, exist_ok=True)

        # Initialize parser for validation and metadata extraction (file-only crawler, no Redis)
        self.parser = ZerodhaWebSocketMessageParser(instrument_info, redis_client=None)

        # Parquet writing state
        self.buffer = []
        self.buffer_size = 1500  # Optimized for 1,466 instruments (corrected EUR/USD)
        self.parquet_writer = None
        self._total_research_messages = 0
        self._records_written = 0

        # Research-specific metadata tracking
        self._venue_analysis_data = {}
        self._cross_venue_stats = {}

        # Configure crawler for research (file writing only)
        config = CrawlerConfig(
            name=name,
            websocket_url=websocket_url,
            tokens=instruments,
            max_reconnect_attempts=3,  # Conservative reconnection for research
            reconnect_delay=10.0,  # Slower reconnection
            heartbeat_interval=120.0,  # Less frequent heartbeats
            connection_timeout=20.0,
            max_threads=3,  # Fewer threads for research
            buffer_size=1000,
            enable_compression=True,
        )

        super().__init__(config)

        # Research-specific state
        self._subscription_sent = False
        self._last_heartbeat_time = time.time()
        self._start_time = time.time()
        self._session_id = f"research_{int(time.time())}"

        logger.info(
            f"Initialized Research Crawler for {len(instruments)} instruments "
            f"(Parquet file writing to {self.data_directory})"
        )

    def _subscribe_to_tokens(self):
        """Subscribe to instruments in full mode for research"""
        try:
            if not self.websocket:
                logger.error("WebSocket not connected")
                return

            # First: Subscribe to instruments (quote mode)
            subscription_message = {
                "a": "subscribe",
                "v": self.instruments
            }
            self.websocket.send(json.dumps(subscription_message))

            # Second: Set full mode for all instruments
            mode_message = {
                "a": "mode",
                "v": ["full", self.instruments]
            }
            self.websocket.send(json.dumps(mode_message))

            self._subscription_sent = True
            logger.info(f"Subscribed to {len(self.instruments)} instruments in full mode for research")

        except Exception as e:
            logger.error(f"Error subscribing to tokens: {e}")

    def _process_message(self, message):
        """Process incoming WebSocket messages for research"""
        try:
            # Handle binary messages (tick data) - write to Parquet
            if isinstance(message, bytes):
                self._process_binary_message(message)
            # Handle text messages (heartbeats, errors, etc.) - log only
            elif isinstance(message, str):
                self._process_text_message(message)
            else:
                logger.warning(f"Unknown message type: {type(message)}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _process_binary_message(self, binary_data: bytes):
        """Process binary WebSocket message and write to Parquet"""
        try:
            self._total_research_messages += 1
            
            # Parse the binary message using the parser
            ticks = self.parser.parse_websocket_message(binary_data)
            
            for tick in ticks:
                if tick:  # Only process valid ticks
                    self._write_to_parquet(tick)
                    
        except Exception as e:
            logger.error(f"Error processing binary message: {e}")

    def _write_to_parquet(self, tick_data):
        """Write tick data to Parquet with metadata"""
        try:
            # Add metadata to each tick
            metadata = metadata_resolver.get_metadata(tick_data.get('instrument_token', 0))
            tick_data_with_meta = {
                **tick_data,
                'symbol': metadata['symbol'],
                'exchange': metadata['exchange'],
                'instrument_type': metadata['instrument_type'],
                'segment': metadata['segment'],
                'tradingsymbol': metadata['tradingsymbol'],
                'processing_timestamp': datetime.now().isoformat(),
                'session_id': self._session_id
                # Note: research_metadata removed - empty dicts cause Parquet write errors
            }
            
            self.buffer.append(tick_data_with_meta)
            
            # Write when buffer reaches size
            if len(self.buffer) >= self.buffer_size:
                self._flush_buffer()
                
        except Exception as e:
            logger.error(f"Error writing to Parquet buffer: {e}")

    def _flush_buffer(self):
        """Flush buffer to Parquet file"""
        if not self.buffer:
            return
            
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"research_{timestamp}.parquet"
            filepath = self.data_directory / filename
            
            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to Parquet
            df.to_parquet(filepath, index=False, compression='snappy')
            
            logger.info(f"✅ Written {len(self.buffer)} records to {filepath}")
            self._records_written += len(self.buffer)
            self.buffer.clear()
            
        except Exception as e:
            logger.error(f"❌ Error writing Parquet: {e}")

    def _process_text_message(self, text_message: str):
        """Process text WebSocket messages (heartbeats, errors, etc.)"""
        try:
            message_data = json.loads(text_message)
            message_type = message_data.get("type")

            if message_type == "order":
                logger.debug(f"Order update: {message_data}")
            elif message_type == "error":
                logger.error(f"WebSocket error: {message_data}")
            elif "ping" in text_message.lower():
                self._handle_ping(message_data)
            else:
                logger.debug(f"Unknown text message: {message_data}")

        except json.JSONDecodeError:
            logger.debug(f"Non-JSON text message: {text_message}")
        except Exception as e:
            logger.error(f"Error processing text message: {e}")

    def _handle_ping(self, ping_data: Dict[str, Any]):
        """Handle ping messages"""
        try:
            # Send pong response
            pong_message = {"a": "pong"}
            if self.websocket:
                self.websocket.send(json.dumps(pong_message))
            logger.debug("Sent pong response")
        except Exception as e:
            logger.error(f"Error handling ping: {e}")

    def stop(self, timeout: float = 10.0):
        """Override stop to flush remaining buffer"""
        try:
            if self.buffer:
                self._flush_buffer()
            logger.info(f"Research Crawler stopped. Total records written: {self._records_written}")
        except Exception as e:
            logger.error(f"Error during stop: {e}")
        finally:
            super().stop(timeout)

    def get_stats(self) -> Dict[str, Any]:
        """Get crawler statistics"""
        elapsed = time.time() - self._start_time if hasattr(self, "_start_time") else 0
        messages_per_second = (
            self._total_research_messages / elapsed if elapsed > 0 else 0
        )

        return {
            "name": self.config.name,
            "status": "running" if self.is_running else "stopped",
            "total_messages": self._total_research_messages,
            "records_written": self._records_written,
            "buffer_size": len(self.buffer),
            "uptime_seconds": elapsed,
            "messages_per_second": messages_per_second,
            "data_directory": str(self.data_directory),
            "instruments_count": len(self.instruments),
            "session_id": self._session_id,
            "venue_analysis_data": self._venue_analysis_data,
            "cross_venue_stats": self._cross_venue_stats,
        }


def create_research_crawler(
    api_key: str,
    access_token: str,
    instruments: List[int],
    instrument_info: Dict[int, Dict],
    **kwargs,
) -> ResearchCrawler:
    """
    Factory function to create Research Crawler

    Args:
        api_key: Zerodha API key
        access_token: Zerodha access token
        instruments: List of instrument tokens
        instrument_info: Instrument token mapping
        **kwargs: Additional arguments for ResearchCrawler

    Returns:
        Configured ResearchCrawler instance
    """
    return ResearchCrawler(
        api_key=api_key,
        access_token=access_token,
        instruments=instruments,
        instrument_info=instrument_info,
        **kwargs,
    )