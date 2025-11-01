"""
Alert News Hook - Robust Consumer Worker
=========================================

A drop-in replacement for news event consumption that:
- Uses centralized Redis client (get_redis_client)
- Reads news:events via consumer group
- ACKs on success
- Reclaims stale pendings (auto-reclaim on start)
- Dedupes with ZSET (24h TTL)
- Sends via Telegram

Usage:
    # Run as long-lived worker
    python -m alerts.alert_news_hook
    
    # Or import for testing
    from alerts.alert_news_hook import NewsEventConsumer
    consumer = NewsEventConsumer()
    consumer.run_once()  # Single iteration
    consumer.run()  # Long-lived loop
"""

import sys
import os
import json
import time
import logging
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from redis_files.redis_client import get_redis_client, create_consumer_group_if_needed, xreadgroup_blocking
from alerts.notifiers import TelegramNotifier

logger = logging.getLogger(__name__)

# Configuration
NEWS_STREAM = os.getenv("NEWS_STREAM", "news:events")
CONSUMER_GROUP = os.getenv("NEWS_CONSUMER_GROUP", "alert_news_hook")
CONSUMER_NAME = os.getenv("NEWS_CONSUMER_NAME", f"worker_{os.getpid()}")
DEDUPE_ZSET = os.getenv("NEWS_DEDUPE_ZSET", "news:dedupe")
DEDUPE_TTL_HOURS = int(os.getenv("NEWS_DEDUPE_TTL_HOURS", "24"))
PENDING_RECLAIM_MS = int(os.getenv("NEWS_PENDING_RECLAIM_MS", "300000"))  # 5 minutes
BATCH_SIZE = int(os.getenv("NEWS_BATCH_SIZE", "10"))
BLOCK_MS = int(os.getenv("NEWS_BLOCK_MS", "5000"))  # 5 seconds
NEWS_DB = int(os.getenv("NEWS_DB", "1"))  # DB 1 (realtime) where news streams are


class NewsEventConsumer:
    """Robust news event consumer with deduplication and pending reclaim."""
    
    def __init__(self, redis_client=None, telegram_notifier=None, stream_name=None, group_name=None, consumer_name=None):
        """Initialize consumer with Redis client and Telegram notifier."""
        self.stream_name = stream_name if stream_name is not None else NEWS_STREAM
        self.group_name = group_name if group_name is not None else CONSUMER_GROUP
        self.consumer_name = consumer_name if consumer_name is not None else CONSUMER_NAME
        
        self.redis_client = redis_client or get_redis_client(db=NEWS_DB, decode_responses=False)
        self.telegram_notifier = telegram_notifier or TelegramNotifier()
        
        # Ensure consumer group exists
        try:
            create_consumer_group_if_needed(self.stream_name, self.group_name)
            logger.info(f"✅ Consumer group '{self.group_name}' ready for stream '{self.stream_name}'")
        except Exception as e:
            logger.error(f"❌ Failed to create consumer group: {e}")
            raise
        
        self.running = False
        self.processed_count = 0
        self.skipped_duplicates = 0
        self.failed_count = 0
        
        logger.info(f"🚀 NewsEventConsumer initialized: stream={self.stream_name}, group={self.group_name}, consumer={self.consumer_name}")
    
    def _generate_dedup_key(self, news_item: Dict[str, Any]) -> str:
        """Generate deduplication key from news item."""
        # Use headline + timestamp + source for deduplication
        headline = str(news_item.get('headline', news_item.get('title', '')))
        timestamp = str(news_item.get('timestamp', news_item.get('published_at', '')))
        source = str(news_item.get('source', news_item.get('news_source', '')))
        
        # Create hash for consistent key
        key_str = f"{headline}:{timestamp}:{source}"
        return hashlib.md5(key_str.encode('utf-8')).hexdigest()
    
    def _is_duplicate(self, news_item: Dict[str, Any]) -> bool:
        """Check if news item is duplicate using ZSET."""
        dedup_key = self._generate_dedup_key(news_item)
        
        try:
            # Check if key exists in ZSET (score is timestamp)
            exists = self.redis_client.zscore(DEDUPE_ZSET, dedup_key)
            if exists:
                return True
            
            # Add to ZSET with current timestamp
            now = time.time()
            self.redis_client.zadd(DEDUPE_ZSET, {dedup_key: now})
            
            # Expire old entries (cleanup entries older than TTL)
            cutoff_time = now - (DEDUPE_TTL_HOURS * 3600)
            self.redis_client.zremrangebyscore(DEDUPE_ZSET, 0, cutoff_time)
            
            # Set ZSET TTL (keep for 48 hours to allow for cleanup window)
            self.redis_client.expire(DEDUPE_ZSET, DEDUPE_TTL_HOURS * 3600 * 2)
            
            return False
        except Exception as e:
            logger.warning(f"⚠️ Deduplication check failed: {e}, allowing message")
            return False  # On error, allow message (fail open)
    
    def _reclaim_pending(self) -> int:
        """Reclaim pending messages that have been idle too long."""
        try:
            # Get pending messages for this consumer
            pending = self.redis_client.xpending_range(
                self.stream_name,
                self.group_name,
                min="-",
                max="+",
                count=100,
                consumername=self.consumer_name
            )
            
            if not pending:
                return 0
            
            reclaimed = 0
            now_ms = int(time.time() * 1000)
            
            for msg in pending:
                # Check if message is stale (idle > threshold)
                idle_ms = msg.get('time_since_delivered', 0)
                if idle_ms > PENDING_RECLAIM_MS:
                    try:
                        # Claim message (change owner to this consumer)
                        claimed = self.redis_client.xclaim(
                            self.stream_name,
                            self.group_name,
                            self.consumer_name,
                            PENDING_RECLAIM_MS,
                            [msg['message_id']]
                        )
                        if claimed:
                            reclaimed += len(claimed)
                            logger.info(f"🔄 Reclaimed {len(claimed)} pending message(s)")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to reclaim message {msg['message_id']}: {e}")
            
            return reclaimed
        except Exception as e:
            logger.warning(f"⚠️ Pending reclaim failed: {e}")
            return 0
    
    def _send_news_alert(self, news_item: Dict[str, Any]) -> bool:
        """Send news item as Telegram alert."""
        try:
            # Format news as alert
            symbol = news_item.get('symbol', news_item.get('ticker', 'MARKET'))
            headline = news_item.get('headline', news_item.get('title', 'No headline'))
            source = news_item.get('source', news_item.get('news_source', 'Unknown'))
            url = news_item.get('url', news_item.get('link', ''))
            sentiment = news_item.get('sentiment_score', 0.0)
            
            # Create alert-like structure
            alert_data = {
                'symbol': symbol,
                'pattern': 'NEWS_ALERT',
                'pattern_type': 'news_alert',
                'confidence': 0.8,  # Default confidence for news alerts
                'action': 'MONITOR',
                'last_price': 0.0,
                'timestamp': news_item.get('timestamp', int(time.time() * 1000)),
                'news': [news_item],
                'top_headline': headline,
                'news_sentiment': 'positive' if sentiment > 0.3 else ('negative' if sentiment < -0.3 else 'neutral'),
                'description': f"📰 {headline}",
                'news_source': source,
                'news_url': url,
            }
            
            # Send via Telegram
            success = self.telegram_notifier.send_alert(alert_data)
            
            if success:
                logger.info(f"✅ Sent news alert: {symbol} - {headline[:50]}...")
            else:
                logger.warning(f"⚠️ Failed to send news alert: {symbol}")
            
            return success
        except Exception as e:
            logger.error(f"❌ Error sending news alert: {e}")
            return False
    
    def _process_message(self, stream_name: str, message_id: str, fields: Dict[bytes, bytes]) -> bool:
        """Process a single message from stream."""
        try:
            # Parse message data
            # Handle both 'data' and b'data' field keys
            data_field = fields.get(b'data') or fields.get('data')
            if not data_field:
                logger.warning(f"⚠️ Message {message_id} has no 'data' field")
                return False
            
            # Decode and parse JSON
            if isinstance(data_field, bytes):
                try:
                    import orjson
                    news_item = orjson.loads(data_field)
                except ImportError:
                    news_item = json.loads(data_field.decode('utf-8'))
            else:
                news_item = json.loads(data_field) if isinstance(data_field, str) else data_field
            
            # Check for duplicates
            if self._is_duplicate(news_item):
                self.skipped_duplicates += 1
                logger.debug(f"🛑 Duplicate news skipped: {news_item.get('headline', 'Unknown')[:50]}")
                return True  # ACK duplicate (don't retry)
            
            # Send news alert
            success = self._send_news_alert(news_item)
            
            if success:
                self.processed_count += 1
                return True  # ACK on success
            else:
                self.failed_count += 1
                return False  # Don't ACK on failure (will retry)
                
        except Exception as e:
            logger.error(f"❌ Error processing message {message_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.failed_count += 1
            return False  # Don't ACK on error
    
    def run_once(self) -> int:
        """Run single iteration (for testing or bounded jobs)."""
        try:
            # Reclaim stale pending messages first
            reclaimed = self._reclaim_pending()
            
            # Read messages from stream
            # xreadgroup_blocking signature: (group, consumer, streams, count, block_ms)
            try:
                messages = xreadgroup_blocking(
                    self.group_name,          # group (first param)
                    self.consumer_name,       # consumer (second param)
                    {self.stream_name: ">"},  # streams dict: {stream_name: ">"} (third param)
                    count=BATCH_SIZE,
                    block_ms=BLOCK_MS
                )
            except Exception as e:
                # If stream doesn't exist yet, return empty
                if "no such key" in str(e).lower() or "no such stream" in str(e).lower():
                    logger.debug(f"Stream '{self.stream_name}' doesn't exist yet, waiting for messages...")
                    return 0
                raise
            
            if not messages:
                return 0
            
            # Process messages and collect IDs to ACK
            ack_ids = []
            
            for stream_name, stream_messages in messages.items():
                for message_id, fields in stream_messages:
                    success = self._process_message(stream_name, message_id, fields)
                    if success:
                        ack_ids.append(message_id)
            
            # ACK successful messages
            if ack_ids:
                try:
                    self.redis_client.xack(self.stream_name, self.group_name, *ack_ids)
                    logger.debug(f"✅ ACKed {len(ack_ids)} message(s)")
                except Exception as e:
                    logger.error(f"❌ Failed to ACK messages: {e}")
            
            return len(ack_ids)
            
        except Exception as e:
            logger.error(f"❌ Error in run_once: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
    
    def run(self, max_iterations: Optional[int] = None):
        """Run long-lived consumer loop."""
        self.running = True
        iteration = 0
        
        logger.info(f"🔄 Starting news event consumer loop (max_iterations={max_iterations or 'unlimited'})")
        
        try:
            while self.running:
                if max_iterations and iteration >= max_iterations:
                    logger.info(f"✅ Reached max_iterations ({max_iterations}), stopping")
                    break
                
                processed = self.run_once()
                iteration += 1
                
                # Log stats periodically
                if iteration % 10 == 0:
                    logger.info(
                        f"📊 Stats: processed={self.processed_count}, "
                        f"skipped={self.skipped_duplicates}, "
                        f"failed={self.failed_count}, "
                        f"iteration={iteration}"
                    )
                
                # Small delay to prevent tight loop
                if not processed:
                    time.sleep(0.5)
                    
        except KeyboardInterrupt:
            logger.info("🛑 Received keyboard interrupt, stopping consumer")
        except Exception as e:
            logger.error(f"❌ Fatal error in consumer loop: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.running = False
            logger.info(f"✅ Consumer stopped. Final stats: processed={self.processed_count}, skipped={self.skipped_duplicates}, failed={self.failed_count}")


# Legacy functions for backward compatibility
def enrich_alert_if_needed(alert_data: Dict[str, Any], redis_client=None, **kwargs) -> Dict[str, Any]:
    """Legacy function - kept for backward compatibility."""
    from alerts.news_enrichment_integration import enrich_alert_with_news, should_enrich_with_news
    
    if not alert_data or not redis_client:
        return alert_data
    
    try:
        if not should_enrich_with_news(alert_data):
            return alert_data
        
        lookback_minutes = kwargs.get('lookback_minutes', 60)
        top_k = kwargs.get('top_k', 3)
        
        enriched = enrich_alert_with_news(alert_data, redis_client, lookback_minutes=lookback_minutes, top_k=top_k)
        
        if enriched.get('news'):
            logger.info(f"📰 Enriched {enriched.get('symbol', 'UNKNOWN')} alert with {len(enriched['news'])} news items")
        
        return enriched
    except Exception as e:
        logger.error(f"Error in alert news enrichment: {e}")
        return alert_data


def enrich_alert_before_notification(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Legacy function - kept for backward compatibility."""
    return enrich_alert_if_needed(alert_data, redis_client, lookback_minutes=30, top_k=2)


def enrich_alert_for_analysis(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Legacy function - kept for backward compatibility."""
    return enrich_alert_if_needed(alert_data, redis_client, lookback_minutes=120, top_k=5)


def create_news_driven_alert(symbol: str, news_items: list, redis_client=None, base_alert: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Legacy function - kept for backward compatibility."""
    if not news_items or not symbol:
        return base_alert or {}
    
    try:
        if not base_alert:
            base_alert = {
                'symbol': symbol,
                'pattern': 'news_alert',
                'pattern_type': 'news_alert',
                'confidence': 0.75,
                'last_price': 0.0,
                'timestamp': int(time.time() * 1000)
            }
        
        base_alert.update({
            'news': news_items,
            'top_headline': news_items[0].get('headline', ''),
            'news_sentiment': 'neutral',
            'news_impact': 'medium',
            'alert_type': 'news_driven',
            'priority': 'high'
        })
        
        return base_alert
    except Exception as e:
        logger.error(f"Error creating news-driven alert: {e}")
        return base_alert or {}


def _calculate_news_sentiment(news_items: list) -> str:
    """Calculate overall sentiment from news items."""
    if not news_items:
        return 'neutral'
    
    sentiment_scores = [float(item.get('sentiment_score', 0)) for item in news_items if item.get('sentiment_score') is not None]
    
    if not sentiment_scores:
        return 'neutral'
    
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
    
    if avg_sentiment > 0.3:
        return 'positive'
    elif avg_sentiment < -0.3:
        return 'negative'
    else:
        return 'neutral'


def _assess_news_impact(news_items: list) -> str:
    """Assess news impact level."""
    if not news_items:
        return 'low'
    
    high_impact_keywords = [
        'earnings', 'results', 'profit', 'revenue', 'guidance',
        'merger', 'acquisition', 'deal', 'partnership',
        'regulatory', 'approval', 'rejection', 'investigation',
        'upgrade', 'downgrade', 'rating', 'outlook'
    ]
    
    impact_score = sum(
        1 for item in news_items
        for keyword in high_impact_keywords
        if keyword in item.get('headline', '').lower()
    )
    
    if impact_score >= 3:
        return 'high'
    elif impact_score >= 1:
        return 'medium'
    else:
        return 'low'


# Convenience functions
def auto_enrich_alerts(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Auto-enrich alerts - main integration point."""
    return enrich_alert_if_needed(alert_data, redis_client)


def quick_news_enrich(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Quick news enrichment for real-time alerts."""
    return enrich_alert_before_notification(alert_data, redis_client)


def deep_news_enrich(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Deep news enrichment for analysis."""
    return enrich_alert_for_analysis(alert_data, redis_client)


# Main entry point
if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description="News event consumer worker")
    parser.add_argument("--once", action="store_true", help="Run single iteration (for testing)")
    parser.add_argument("--max-iterations", type=int, help="Maximum iterations before stopping")
    parser.add_argument("--stream", default=NEWS_STREAM, help="Redis stream name")
    parser.add_argument("--group", default=CONSUMER_GROUP, help="Consumer group name")
    parser.add_argument("--consumer", default=CONSUMER_NAME, help="Consumer name")
    
    args = parser.parse_args()
    
    # Create consumer with custom config if provided
    consumer = NewsEventConsumer(
        stream_name=args.stream,
        group_name=args.group,
        consumer_name=args.consumer
    )
    
    if args.once:
        logger.info("🔍 Running single iteration (test mode)")
        processed = consumer.run_once()
        logger.info(f"✅ Processed {processed} message(s)")
    else:
        consumer.run(max_iterations=args.max_iterations)
