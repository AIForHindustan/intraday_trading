from redis_files.redis_client import RedisManager82
"""
Market Activity Analyzer
========================

Analyzes real-time market activity from Redis to dynamically categorize assets.

⚠️ NOTE: This file is NOT wired up yet - for future integration
"""

import redis
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class ActivityAnalyzer:
    """
    Analyzes market activity from Redis to determine asset tiers dynamically.
    """
    
    def __init__(self, redis_client: redis.Redis = None):
        """
        Initialize Activity Analyzer.
        
        Args:
            redis_client: Redis client (DB 1) - if None, creates new connection
        """
        if redis_client is None:
            self.redis = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
        else:
            self.redis = redis_client
        
        # Activity thresholds
        self.VOLUME_RATIO_TIER_2 = 1.0  # Above 1.0x = Tier 2
        self.VOLUME_RATIO_TIER_3 = 0.5  # 0.5-1.0x = Tier 3
        self.VOLUME_RATIO_TIER_4 = 0.5  # Below 0.5x = Tier 4
    
    def analyze_equity_activity(self) -> Dict[str, Dict]:
        """
        Analyze equity stock activity from Redis volume ratios.
        
        Returns:
            Dict mapping symbol to activity data:
            {
                'symbol': {
                    'volume_ratio': float,
                    'tier': int,
                    'category': str
                }
            }
        """
        activity_data = {}
        
        try:
            # Get all NSE equity volume ratios
            vol_keys = self.redis.keys("ind:volume:NSE:*:volume_ratio")
            
            for key in vol_keys:
                try:
                    symbol = key.split(':')[3]  # ind:volume:NSE:SYMBOL:volume_ratio
                    vol_ratio = self.redis.get(key)
                    
                    if vol_ratio:
                        vol_val = float(vol_ratio)
                        
                        # Determine tier based on volume ratio
                        if vol_val >= self.VOLUME_RATIO_TIER_2:
                            tier = 2
                            category = "High Volatility"
                        elif vol_val >= self.VOLUME_RATIO_TIER_3:
                            tier = 3
                            category = "Standard Activity"
                        else:
                            tier = 4
                            category = "Low Activity"
                        
                        activity_data[symbol] = {
                            'volume_ratio': vol_val,
                            'tier': tier,
                            'category': category
                        }
                except Exception as e:
                    logger.debug(f"Error processing {key}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error analyzing equity activity: {e}")
        
        return activity_data
    
    def get_tier_2_candidates(self) -> List[Tuple[str, float]]:
        """
        Get Tier 2 equity candidates (high volume ratio).
        
        Returns:
            List of (symbol, volume_ratio) tuples, sorted by volume ratio descending
        """
        activity = self.analyze_equity_activity()
        tier_2 = [
            (symbol, data['volume_ratio'])
            for symbol, data in activity.items()
            if data['tier'] == 2
        ]
        tier_2.sort(key=lambda x: x[1], reverse=True)
        return tier_2
    
    def get_tier_4_candidates(self) -> List[Tuple[str, float]]:
        """
        Get Tier 4 candidates (low activity - consider removing).
        
        Returns:
            List of (symbol, volume_ratio) tuples, sorted by volume ratio ascending
        """
        activity = self.analyze_equity_activity()
        tier_4 = [
            (symbol, data['volume_ratio'])
            for symbol, data in activity.items()
            if data['tier'] == 4
        ]
        tier_4.sort(key=lambda x: x[1])
        return tier_4
    
    def get_activity_summary(self) -> Dict:
        """
        Get summary of market activity by tier.
        
        Returns:
            Dict with tier counts and examples
        """
        activity = self.analyze_equity_activity()
        
        tier_counts = defaultdict(int)
        tier_examples = defaultdict(list)
        
        for symbol, data in activity.items():
            tier = data['tier']
            tier_counts[tier] += 1
            
            # Keep top 5 examples per tier
            if len(tier_examples[tier]) < 5:
                tier_examples[tier].append({
                    'symbol': symbol,
                    'volume_ratio': data['volume_ratio']
                })
        
        return {
            'tier_counts': dict(tier_counts),
            'tier_examples': {
                tier: sorted(examples, key=lambda x: x['volume_ratio'], reverse=True)
                for tier, examples in tier_examples.items()
            },
            'total_analyzed': len(activity)
        }


def analyze_market_activity() -> Dict:
    """
    Convenience function to analyze current market activity.
    
    Returns:
        Activity summary dictionary
    """
    analyzer = ActivityAnalyzer()
    return analyzer.get_activity_summary()


if __name__ == "__main__":
    # Test the analyzer
    analyzer = ActivityAnalyzer()
    summary = analyzer.get_activity_summary()
    
    print("\n📊 Market Activity Summary:")
    print("=" * 60)
    for tier, count in sorted(summary['tier_counts'].items()):
        print(f"Tier {tier}: {count} assets")
        if tier in summary['tier_examples']:
            print(f"  Examples: {', '.join([e['symbol'] for e in summary['tier_examples'][tier][:5]])}")
    print(f"\nTotal analyzed: {summary['total_analyzed']} assets")

