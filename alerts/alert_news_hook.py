"""
Alert News Hook - Drop-in Integration
====================================

This module provides drop-in integration for news enrichment in the existing alert system.
Import this module to automatically enrich alerts with news context.

Usage:
    # In your alert processing pipeline
    from alerts.alert_news_hook import enrich_alert_if_needed
    
    # Before sending alert
    enriched_alert = enrich_alert_if_needed(alert_data, redis_client)
"""

import sys
import os
import logging
from typing import Dict, Any, Optional

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from alerts.news_enrichment_integration import (
    enrich_alert_with_news, 
    should_enrich_with_news,
    get_news_enrichment_manager
)

logger = logging.getLogger(__name__)


def enrich_alert_if_needed(alert_data: Dict[str, Any], redis_client=None, **kwargs) -> Dict[str, Any]:
    """
    Drop-in function to enrich alerts with news if needed.
    
    This function can be called anywhere in the alert pipeline to automatically
    enrich alerts with relevant news context.
    
    Args:
        alert_data: Alert data dictionary
        redis_client: Redis client instance
        **kwargs: Additional arguments passed to enrichment
    
    Returns:
        Enriched alert data (unchanged if no enrichment needed/applicable)
    """
    if not alert_data or not redis_client:
        return alert_data
    
    try:
        # Check if alert should be enriched
        if not should_enrich_with_news(alert_data):
            return alert_data
        
        # Get enrichment parameters
        lookback_minutes = kwargs.get('lookback_minutes', 60)
        top_k = kwargs.get('top_k', 3)
        
        # Enrich the alert
        enriched = enrich_alert_with_news(
            alert_data, 
            redis_client, 
            lookback_minutes=lookback_minutes, 
            top_k=top_k
        )
        
        # Log enrichment result
        if enriched.get('news'):
            news_count = len(enriched['news'])
            symbol = enriched.get('symbol', 'UNKNOWN')
            logger.info(f"📰 Enriched {symbol} alert with {news_count} news items")
        else:
            symbol = alert_data.get('symbol', 'UNKNOWN')
            logger.debug(f"No news found for {symbol}")
        
        return enriched
        
    except Exception as e:
        logger.error(f"Error in alert news enrichment: {e}")
        return alert_data


def enrich_alert_before_notification(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """
    Specific hook for enriching alerts before notification.
    
    This function is designed to be called right before sending notifications
    to ensure alerts have the latest news context.
    
    Args:
        alert_data: Alert data dictionary
        redis_client: Redis client instance
    
    Returns:
        Enriched alert data
    """
    if not alert_data or not redis_client:
        return alert_data
    
    try:
        # Use shorter lookback for real-time notifications
        enriched = enrich_alert_with_news(
            alert_data, 
            redis_client, 
            lookback_minutes=30,  # Last 30 minutes
            top_k=2  # Top 2 news items
        )
        
        return enriched
        
    except Exception as e:
        logger.error(f"Error enriching alert before notification: {e}")
        return alert_data


def enrich_alert_for_analysis(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """
    Hook for enriching alerts for analysis purposes.
    
    This function uses a longer lookback period for comprehensive news context
    during analysis and backtesting.
    
    Args:
        alert_data: Alert data dictionary
        redis_client: Redis client instance
    
    Returns:
        Enriched alert data
    """
    if not alert_data or not redis_client:
        return alert_data
    
    try:
        # Use longer lookback for analysis
        enriched = enrich_alert_with_news(
            alert_data, 
            redis_client, 
            lookback_minutes=120,  # Last 2 hours
            top_k=5  # Top 5 news items
        )
        
        return enriched
        
    except Exception as e:
        logger.error(f"Error enriching alert for analysis: {e}")
        return alert_data


def create_news_driven_alert(symbol: str, news_items: list, redis_client=None, base_alert: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create a news-driven alert from scratch.
    
    Args:
        symbol: Trading symbol
        news_items: List of news items
        redis_client: Redis client instance
        base_alert: Base alert data to build upon
    
    Returns:
        News-driven alert data
    """
    if not news_items or not symbol:
        return base_alert or {}
    
    try:
        # Create base alert if not provided
        if not base_alert:
            base_alert = {
                'symbol': symbol,
                'pattern': 'news_alert',
                'pattern_type': 'news_alert',
                'confidence': 0.75,  # Default confidence for news alerts
                'last_price': 0.0,
                'timestamp': int(time.time() * 1000)
            }
        
        # Enrich with news
        base_alert.update({
            'news': news_items,
            'top_headline': news_items[0].get('headline', ''),
            'news_sentiment': _calculate_news_sentiment(news_items),
            'news_impact': _assess_news_impact(news_items),
            'alert_type': 'news_driven',
            'priority': 'high' if _assess_news_impact(news_items) == 'high' else 'medium'
        })
        
        return base_alert
        
    except Exception as e:
        logger.error(f"Error creating news-driven alert: {e}")
        return base_alert or {}


def _calculate_news_sentiment(news_items: list) -> str:
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


def _assess_news_impact(news_items: list) -> str:
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


# Convenience functions for easy integration
def auto_enrich_alerts(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Auto-enrich alerts - main integration point"""
    return enrich_alert_if_needed(alert_data, redis_client)


def quick_news_enrich(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Quick news enrichment for real-time alerts"""
    return enrich_alert_before_notification(alert_data, redis_client)


def deep_news_enrich(alert_data: Dict[str, Any], redis_client=None) -> Dict[str, Any]:
    """Deep news enrichment for analysis"""
    return enrich_alert_for_analysis(alert_data, redis_client)
