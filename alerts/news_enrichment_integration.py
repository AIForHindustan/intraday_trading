"""
News Enrichment Integration for Alert System
===========================================

Integrates news enrichment into the existing alert system without modifying core files.
This module provides hooks and utilities to enrich alerts with news context.

Usage:
    from alerts.news_enrichment_integration import enrich_alert_with_news, NewsEnrichmentManager

    # Enrich an alert before sending
    enriched_alert = enrich_alert_with_news(alert_data, redis_client)
"""

import sys
import os
import json
import time
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from news_flat_ingestor import NewsEnricher
except ImportError:
    NewsEnricher = None

import json

logger = logging.getLogger(__name__)


class NewsEnrichmentManager:
    """Manages news enrichment for alerts"""
    
    def __init__(self, redis_client=None, news_db: int = 0):
        """Initialize news enrichment manager"""
        self.redis_client = redis_client
        self.news_db = news_db
        self.enricher = None
        
        if redis_client:
            self.enricher = NewsEnricher(redis_client, news_db)
            logger.info("✅ News enrichment manager initialized")
        else:
            logger.warning("⚠️ No Redis client provided - news enrichment disabled")
    
    def enrich_alert(self, alert_data: Dict[str, Any], lookback_minutes: int = 60, top_k: int = 3) -> Dict[str, Any]:
        """Enrich alert with recent news"""
        if not self.enricher or not alert_data:
            return alert_data
        
        try:
            symbol = alert_data.get('symbol', '')
            if not symbol:
                return alert_data
            
            # Check if already enriched
            if alert_data.get('news') or alert_data.get('news_context'):
                logger.debug(f"Alert for {symbol} already has news context")
                return alert_data
            
            # Enrich with news
            enriched = self.enricher.enrich_alert(
                alert_data, 
                symbol=symbol, 
                lookback_minutes=lookback_minutes, 
                top_k=top_k
            )
            
            if enriched.get('news'):
                logger.info(f"✅ Enriched {symbol} alert with {len(enriched['news'])} news items")
            else:
                logger.debug(f"No news found for {symbol} in last {lookback_minutes} minutes")
            
            return enriched
            
        except Exception as e:
            logger.error(f"Error enriching alert with news: {e}")
            return alert_data
    
    def get_news_summary(self, symbol: str, hours: int = 24) -> Dict[str, Any]:
        """Get news summary for a symbol"""
        if not self.enricher:
            return {"symbol": symbol, "count": 0, "items": []}
        
        try:
            now_ms = int(time.time() * 1000)
            since_ms = now_ms - (hours * 60 * 60 * 1000)
            
            news_items = self.enricher.fetch_recent_news(symbol, since_ms, limit=10)
            
            return {
                "symbol": symbol,
                "count": len(news_items),
                "items": news_items[:5],  # First 5 items
                "timeframe_hours": hours
            }
            
        except Exception as e:
            logger.error(f"Error getting news summary for {symbol}: {e}")
            return {"symbol": symbol, "count": 0, "items": []}


def _fetch_news_from_symbol_keys(redis_client, symbol: str, lookback_seconds: int = 3600, limit: int = 3) -> List[Dict[str, Any]]:
    """
    Fetch news from Redis news:symbol:* sorted sets (gift_nifty_gap.py format)
    Converts nested structure to template format
    """
    if not redis_client or not symbol:
        return []
    
    try:
        # Get Redis client for DB 0 (where news:symbol:* keys are stored)
        r = redis_client.get_client(0) if hasattr(redis_client, 'get_client') else redis_client
        
        # Try multiple symbol variations
        symbol_variants = [
            symbol,
            symbol.split(':')[-1] if ':' in symbol else symbol,
            f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol
        ]
        
        import time
        current_time = time.time()
        cutoff_time = current_time - lookback_seconds
        
        news_items = []
        
        for variant in symbol_variants:
            news_key = f"news:symbol:{variant}"
            
            # Fetch from sorted set (sorted by timestamp score)
            items = r.zrevrangebyscore(
                news_key,
                "+inf",
                cutoff_time,
                withscores=False,
                start=0,
                num=limit * 2  # Get more to filter
            )
            
            if items:
                # Parse and convert to template format
                for item_str in items[:limit]:
                    try:
                        item = json.loads(item_str)
                        
                        # Handle nested 'data' structure (gift_nifty_gap.py format)
                        if 'data' in item:
                            data = item['data']
                        else:
                            data = item
                        
                        # Convert sentiment (string to float)
                        sentiment_score = data.get('sentiment', 0.0)
                        if isinstance(sentiment_score, str):
                            sentiment_map = {'positive': 0.8, 'negative': -0.8, 'neutral': 0.0}
                            sentiment_score = sentiment_map.get(sentiment_score.lower(), 0.0)
                        elif sentiment_score is None:
                            sentiment_score = 0.0
                        else:
                            try:
                                sentiment_score = float(sentiment_score)
                            except (ValueError, TypeError):
                                sentiment_score = 0.0
                        
                        # Map to template format
                        formatted_item = {
                            'headline': data.get('title', ''),
                            'news_source': data.get('source', data.get('publisher', '')),
                            'url': data.get('link', ''),
                            'sentiment_score': sentiment_score,
                            'timestamp': item.get('timestamp', data.get('collected_at', ''))
                        }
                        
                        # Only include if we have a headline
                        if formatted_item['headline']:
                            news_items.append(formatted_item)
                            if len(news_items) >= limit:
                                break
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.debug(f"Error parsing news item: {e}")
                        continue
                
                if news_items:
                    break  # Found news for this variant
        
        return news_items[:limit]
        
    except Exception as e:
        logger.error(f"Error fetching news from symbol keys: {e}")
        return []


def enrich_alert_with_news(alert_data: Dict[str, Any], redis_client=None, lookback_minutes: int = 60, top_k: int = 3) -> Dict[str, Any]:
    """
    Standalone function to enrich an alert with news.
    Supports both Redis streams (news:{symbol}) and sorted sets (news:symbol:{symbol})
    
    Args:
        alert_data: Alert data dictionary
        redis_client: Redis client instance
        lookback_minutes: How far back to look for news (default: 60)
        top_k: Maximum number of news items to attach (default: 3)
    
    Returns:
        Enriched alert data with news context
    """
    if not alert_data or not redis_client:
        return alert_data
    
    try:
        symbol = alert_data.get('symbol', '')
        
        if not symbol:
            return alert_data
        
        # First try fetching from news:symbol:* sorted sets (gift_nifty_gap.py format)
        lookback_seconds = lookback_minutes * 60
        news_items = _fetch_news_from_symbol_keys(redis_client, symbol, lookback_seconds, top_k)
        
        # Fallback to Redis streams if no news found in sorted sets
        if not news_items and NewsEnricher:
            try:
                enricher = NewsEnricher(redis_client, news_db=0)
                enriched = enricher.enrich_alert(
                    alert_data, 
                    symbol=symbol, 
                    lookback_minutes=lookback_minutes, 
                    top_k=top_k
                )
                if enriched.get('news'):
                    return enriched
            except Exception as e:
                logger.debug(f"Redis streams news fetch failed: {e}")
        
        # If we found news from sorted sets, attach it
        if news_items:
            alert_data.setdefault("news", news_items)
            alert_data.setdefault("top_headline", news_items[0].get("headline"))
            logger.debug(f"✅ Enriched {symbol} alert with {len(news_items)} news items from sorted sets")
        
        return alert_data
        
    except Exception as e:
        logger.error(f"Error in enrich_alert_with_news: {e}")
        return alert_data


def should_enrich_with_news(alert_data: Dict[str, Any]) -> bool:
    """
    Determine if an alert should be enriched with news.
    
    Args:
        alert_data: Alert data dictionary
    
    Returns:
        True if alert should be enriched with news
    """
    if not alert_data:
        return False
    
    # Don't enrich if already has news
    if alert_data.get('news') or alert_data.get('news_context'):
        return False
    
    # Enrich for high-confidence alerts or specific patterns
    confidence = float(alert_data.get('confidence', 0.0))
    pattern = alert_data.get('pattern', '').lower()
    
    # High confidence alerts
    if confidence >= 0.85:
        return True
    
    # Specific patterns that benefit from news context
    news_relevant_patterns = [
        'volume_breakout',
        'reversal',
        'breakout',
        'earnings_announcement',
        'news_alert',
        'sector_rotation',
        'institutional_flow'
    ]
    
    if any(pattern_name in pattern for pattern_name in news_relevant_patterns):
        return True
    
    # Enrich for major symbols
    symbol = alert_data.get('symbol', '').upper()
    major_symbols = [
        'NIFTY', 'BANKNIFTY', 'RELIANCE', 'TCS', 'HDFCBANK', 
        'ICICIBANK', 'INFY', 'SBIN', 'KOTAKBANK', 'LT'
    ]
    
    if any(major_symbol in symbol for major_symbol in major_symbols):
        return True
    
    return False


def create_news_alert(alert_data: Dict[str, Any], news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create a news-specific alert from enriched data.
    
    Args:
        alert_data: Original alert data
        news_items: List of news items
    
    Returns:
        News alert data
    """
    if not news_items:
        return alert_data
    
    # Create news alert
    news_alert = alert_data.copy()
    news_alert.update({
        'pattern': 'news_alert',
        'pattern_type': 'news_alert',
        'news': news_items,
        'top_headline': news_items[0].get('headline', ''),
        'news_sentiment': _calculate_news_sentiment(news_items),
        'news_impact': _assess_news_impact(news_items),
        'alert_type': 'news_driven',
        'priority': 'high' if _assess_news_impact(news_items) == 'high' else 'medium'
    })
    
    return news_alert


def _calculate_news_sentiment(news_items: List[Dict[str, Any]]) -> str:
    """Calculate overall sentiment from news items"""
    if not news_items:
        return 'neutral'
    
    sentiment_scores = []
    for item in news_items:
        score = item.get('sentiment_score')
        if score is not None:
            sentiment_scores.append(float(score))
    
    if not sentiment_scores:
        return 'neutral'
    
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
    
    if avg_sentiment > 0.3:
        return 'positive'
    elif avg_sentiment < -0.3:
        return 'negative'
    else:
        return 'neutral'


def _assess_news_impact(news_items: List[Dict[str, Any]]) -> str:
    """Assess news impact level"""
    if not news_items:
        return 'low'
    
    # Count high-impact keywords
    high_impact_keywords = [
        'earnings', 'results', 'profit', 'revenue', 'guidance',
        'merger', 'acquisition', 'deal', 'partnership',
        'regulatory', 'approval', 'rejection', 'investigation',
        'upgrade', 'downgrade', 'rating', 'outlook'
    ]
    
    impact_score = 0
    for item in news_items:
        headline = item.get('headline', '').lower()
        for keyword in high_impact_keywords:
            if keyword in headline:
                impact_score += 1
    
    if impact_score >= 3:
        return 'high'
    elif impact_score >= 1:
        return 'medium'
    else:
        return 'low'


# Global news enrichment manager instance
_news_manager = None

def get_news_enrichment_manager(redis_client=None) -> NewsEnrichmentManager:
    """Get or create global news enrichment manager"""
    global _news_manager
    if _news_manager is None and redis_client:
        _news_manager = NewsEnrichmentManager(redis_client)
    return _news_manager


def initialize_news_enrichment(redis_client):
    """Initialize news enrichment for the alert system"""
    global _news_manager
    _news_manager = NewsEnrichmentManager(redis_client)
    logger.info("✅ News enrichment initialized for alert system")
    return _news_manager
