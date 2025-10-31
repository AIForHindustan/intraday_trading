"""Validation logging for technical indicators"""
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class IndicatorValidationLogger:
    def __init__(self):
        self.validation_counts = {
            'total_ticks': 0,
            'successful_calculations': 0,
            'failed_calculations': 0,
            'default_values': 0
        }
        
    def log_tick_processing(self, symbol: str, tick_data: Dict[str, Any], indicators: Optional[Dict[str, Any]]) -> None:
        """Log detailed validation info for each tick processed"""
        self.validation_counts['total_ticks'] += 1
        
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        
        # Basic tick validation
        logger.info(f"[{timestamp}] Processing tick for {symbol}")
        logger.info(f"Input tick data: price={tick_data.get('price')}, volume={tick_data.get('volume')}")
        
        if indicators is None:
            self.validation_counts['failed_calculations'] += 1
            logger.warning(f"❌ No indicators generated for {symbol}")
            return
            
        # Check for default values
        default_checks = {
            'rsi': (lambda x: abs(x - 50.0) < 0.01, "RSI defaulting to 50"),
            'ema20': (lambda x: x == 0.0, "EMA20 defaulting to 0"),
            'ema50': (lambda x: x == 0.0, "EMA50 defaulting to 0"),
            'macd': (lambda x: x == 0.0, "MACD defaulting to 0"),
            'volume_ratio': (lambda x: x == 1.0, "Volume ratio defaulting to 1")
        }
        
        defaults_found = []
        for ind_name, (check_func, message) in default_checks.items():
            if ind_name in indicators and check_func(indicators[ind_name]):
                defaults_found.append(message)
        
        if defaults_found:
            self.validation_counts['default_values'] += 1
            logger.warning(f"⚠️ Default values detected for {symbol}: {', '.join(defaults_found)}")
        else:
            self.validation_counts['successful_calculations'] += 1
            logger.info(f"✅ Valid indicators calculated for {symbol}: {json.dumps(indicators, indent=2)}")
        
        # Log calculation statistics
        if self.validation_counts['total_ticks'] % 100 == 0:
            success_rate = (self.validation_counts['successful_calculations'] / 
                          self.validation_counts['total_ticks'] * 100)
            default_rate = (self.validation_counts['default_values'] / 
                          self.validation_counts['total_ticks'] * 100)
            
            logger.info(f"""
📊 Indicator Calculation Statistics:
Total ticks processed: {self.validation_counts['total_ticks']}
Success rate: {success_rate:.1f}%
Default values rate: {default_rate:.1f}%
Failed calculations: {self.validation_counts['failed_calculations']}
""")

# Global instance
indicator_validator = IndicatorValidationLogger()