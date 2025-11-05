# dashboard/pattern_integration.py
import redis
import json
from typing import List, Dict
import pandas as pd

class PatternVisualization:
    def __init__(self):
        self.redis = redis.Redis()
    
    async def get_active_patterns(self, symbol: str) -> List[Dict]:
        """Get patterns for visualization on chart"""
        patterns = []
        
        # Get recent patterns from Redis
        pattern_data = self.redis.lrange(f"patterns:{symbol}", 0, 50)
        
        for pattern_json in pattern_data:
            pattern = json.loads(pattern_json)
            patterns.append({
                "type": pattern['pattern_type'],
                "timestamp": pattern['timestamp'],
                "price_level": pattern['price'],
                "confidence": pattern['confidence'],
                "direction": pattern.get('direction', 'neutral'),
                "metadata": pattern
            })
        
        return patterns
    
    def calculate_support_resistance(self, ohlc_data: pd.DataFrame) -> Dict:
        """Calculate support/resistance levels for chart"""
        highs = ohlc_data['high']
        lows = ohlc_data['low']
        
        # Simple S/R calculation
        resistance_levels = self.find_significant_highs(highs)
        support_levels = self.find_significant_lows(lows)
        
        return {
            "support": support_levels,
            "resistance": resistance_levels
        }