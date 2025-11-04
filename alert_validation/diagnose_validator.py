#!/usr/bin/env python3
"""
Diagnostic script for AlertValidator
Tests price resolution and bucket metrics
"""

import sys
import os
import logging
from datetime import datetime, timedelta
import pytz

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_price_resolution():
    """Test price resolution for various symbols."""
    try:
        from alert_validation.alert_validator import AlertValidator
        # Patches have been integrated into alert_validator.py - no longer needed
        
        logger.info("🧪 Testing AlertValidator price resolution...")
        
        # Create validator instance
        validator = AlertValidator()
        
        # Test symbols
        test_symbols = [
            "BANKNIFTY25NOV58900CE",
            "NIFTY25NOV26200PE", 
            "BANKNIFTY",
            "NIFTY"
        ]
        
        for symbol in test_symbols:
            logger.info(f"Testing price resolution for: {symbol}")
            price = validator._get_current_price(symbol)
            if price:
                logger.info(f"✅ {symbol}: {price}")
            else:
                logger.warning(f"❌ {symbol}: No price found")
                
    except Exception as e:
        logger.error(f"Error in price resolution test: {e}")

def test_bucket_metrics():
    """Test bucket metrics calculation."""
    try:
        from alert_validation.alert_validator import AlertValidator
        # Patches have been integrated into alert_validator.py - no longer needed
        
        logger.info("🧪 Testing AlertValidator bucket metrics...")
        
        # Create validator instance
        validator = AlertValidator()
        
        # Force bucket metrics
        if not hasattr(validator, 'config'):
            validator.config = {}
        if 'validation' not in validator.config:
            validator.config['validation'] = {}
        validator.config['validation']['force_bucket_metrics'] = True
        
        # Test symbols
        test_symbols = ["BANKNIFTY", "NIFTY"]
        window_minutes = 30
        
        for symbol in test_symbols:
            logger.info(f"Testing bucket metrics for: {symbol} ({window_minutes}min)")
            metrics = validator.get_rolling_metrics(symbol, window_minutes)
            if metrics:
                logger.info(f"✅ {symbol}: {len(metrics)} metrics - {metrics}")
            else:
                logger.warning(f"❌ {symbol}: No metrics found")
                
    except Exception as e:
        logger.error(f"Error in bucket metrics test: {e}")

def test_validation_process():
    """Test the complete validation process."""
    try:
        from alert_validation.alert_validator import AlertValidator
        # Patches have been integrated into alert_validator.py - no longer needed
        
        logger.info("🧪 Testing complete validation process...")
        
        # Create validator instance
        validator = AlertValidator()
        
        # Force bucket metrics
        if not hasattr(validator, 'config'):
            validator.config = {}
        if 'validation' not in validator.config:
            validator.config['validation'] = {}
        validator.config['validation']['force_bucket_metrics'] = True
        
        # Create test alert data
        test_alert = {
            'symbol': 'BANKNIFTY25NOV58900CE',
            'pattern': 'kow_signal_straddle',
            'confidence': 0.85,
            'timestamp': int(datetime.now().timestamp() * 1000)
        }
        
        # Test forward window evaluation
        logger.info(f"Testing forward window evaluation for: {test_alert['symbol']}")
        result = validator._evaluate_forward_window(test_alert, 30)
        
        if result:
            logger.info(f"✅ Validation result: {result}")
        else:
            logger.warning(f"❌ No validation result")
            
    except Exception as e:
        logger.error(f"Error in validation process test: {e}")

def main():
    """Run all diagnostic tests."""
    logger.info("🚀 Starting AlertValidator diagnostics...")
    
    test_price_resolution()
    test_bucket_metrics()
    test_validation_process()
    
    logger.info("✅ Diagnostics complete!")

if __name__ == "__main__":
    main()
