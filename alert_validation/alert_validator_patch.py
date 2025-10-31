# file: alert_validator_patch.py
# (drop-in patch; import AFTER importing your alert_validator module)
# Usage in your runner:  import alert_validator; import alert_validator_patch
# — Adds price resolution, avoids retry looping, and forces bucket metrics (config flag).

import logging
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)

def patch_alert_validator():
    """Apply patches to AlertValidator to fix retry loops and bucket metrics issues."""
    
    # Import the AlertValidator class
    from alert_validation.alert_validator import AlertValidator
    
    # 1. Add symbol resolution for prices (option -> underlying)
    def _get_underlying_symbol_for_price(self, symbol: str) -> str:
        """Map option symbols to their underlying symbols for price lookup."""
        if "NIFTY" in symbol.upper():
            return "NIFTY"
        elif "BANKNIFTY" in symbol.upper():
            return "BANKNIFTY"
        elif "FINNIFTY" in symbol.upper():
            return "FINNIFTY"
        elif "MIDCPNIFTY" in symbol.upper():
            return "MIDCPNIFTY"
        else:
            # For other symbols, try to extract the base symbol
            base_symbol = symbol
            for suffix in ["25NOV", "25DEC", "25JAN", "25FEB", "25MAR", "25APR", "25MAY", "25JUN", 
                          "25JUL", "25AUG", "25SEP", "25OCT", "CE", "PE", "FUT"]:
                if base_symbol.endswith(suffix):
                    base_symbol = base_symbol[:-len(suffix)]
                    break
            return base_symbol
    
    # 2. Add _get_current_price() with fallbacks
    def _get_current_price(self, symbol: str) -> float:
        """Get current price with multiple fallbacks."""
        try:
            # Try underlying symbol first
            underlying_symbol = self._get_underlying_symbol_for_price(symbol)
            
            # Fallback 1: Try last price from Redis
            if hasattr(self.redis, 'get_last_price'):
                price = self.redis.get_last_price(underlying_symbol)
                if price and price > 0:
                    logger.info(f"✅ Got price for {symbol} (underlying: {underlying_symbol}): {price}")
                    return float(price)
            
            # Fallback 2: Try last candle OHLC
            try:
                ohlc_key = f"ohlc:{underlying_symbol}:1min"
                ohlc_data = self.redis_client.hgetall(ohlc_key)
                if ohlc_data and 'close' in ohlc_data:
                    price = float(ohlc_data['close'])
                    if price > 0:
                        logger.info(f"✅ Got price from OHLC for {symbol} (underlying: {underlying_symbol}): {price}")
                        return price
            except Exception as e:
                logger.debug(f"OHLC fallback failed for {symbol}: {e}")
            
            # Fallback 3: Try last trade from tick data
            try:
                tick_key = f"ticks:{underlying_symbol}"
                # Get latest tick from stream
                latest_ticks = self.redis_client.xrevrange(tick_key, count=1)
                if latest_ticks:
                    tick_data = latest_ticks[0][1]
                    if 'last_price' in tick_data:
                        price = float(tick_data['last_price'])
                        if price > 0:
                            logger.info(f"✅ Got price from tick data for {symbol} (underlying: {underlying_symbol}): {price}")
                            return price
            except Exception as e:
                logger.debug(f"Tick data fallback failed for {symbol}: {e}")
            
            logger.warning(f"❌ No price found for {symbol} (underlying: {underlying_symbol})")
            return None
            
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return None
    
    # 3. Patch _evaluate_forward_window() to use _get_current_price and avoid infinite retries
    original_evaluate_forward_window = AlertValidator._evaluate_forward_window
    
    def _evaluate_forward_window_patched(self, alert_data, window_minutes):
        """Patched version that handles missing prices gracefully."""
        try:
            symbol = alert_data.get('symbol', 'UNKNOWN')
            logger.info(f"🔍 Evaluating forward window for {symbol} ({window_minutes}min)")
            
            # Get current price with fallbacks
            current_price = self._get_current_price(symbol)
            
            if current_price is None:
                logger.warning(f"⚠️ No price available for {symbol}, marking validation as INCONCLUSIVE")
                return {
                    'is_valid': False,
                    'confidence_score': 0.0,
                    'validation_metrics': {},
                    'reasons': [f'No price data available for {symbol}'],
                    'timestamp': datetime.now(pytz.timezone("Asia/Kolkata")),
                    'status': 'INCONCLUSIVE'
                }
            
            # Call original method with valid price
            return original_evaluate_forward_window(self, alert_data, window_minutes)
            
        except Exception as e:
            logger.error(f"Error in forward window evaluation for {symbol}: {e}")
            return {
                'is_valid': False,
                'confidence_score': 0.0,
                'validation_metrics': {},
                'reasons': [f'Evaluation error: {str(e)}'],
                'timestamp': datetime.now(pytz.timezone("Asia/Kolkata")),
                'status': 'ERROR'
            }
    
    # 4. Patch get_rolling_metrics() to force bucket path
    original_get_rolling_metrics = AlertValidator.get_rolling_metrics
    
    def get_rolling_metrics_patched(self, symbol, window_minutes):
        """Patched version that forces bucket metrics path."""
        try:
            # Check if we should force bucket metrics
            force_bucket = getattr(self, 'config', {}).get('validation', {}).get('force_bucket_metrics', False)
            
            if force_bucket:
                logger.info(f"🪣 FORCING bucket metrics for {symbol} ({window_minutes}min)")
                return self._calculate_from_volume_buckets(symbol, window_minutes)
            
            # Try original method first
            result = original_get_rolling_metrics(self, symbol, window_minutes)
            
            # If result is empty or invalid, fall back to bucket metrics
            if not result or result.get('bucket_count', 0) == 0:
                logger.info(f"🪣 FALLBACK to bucket metrics for {symbol} ({window_minutes}min)")
                return self._calculate_from_volume_buckets(symbol, window_minutes)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in get_rolling_metrics for {symbol}: {e}")
            # Fall back to bucket metrics
            logger.info(f"🪣 EXCEPTION fallback to bucket metrics for {symbol} ({window_minutes}min)")
            return self._calculate_from_volume_buckets(symbol, window_minutes)
    
    # 5. Wrap _calculate_from_volume_buckets() with INFO entry logs
    original_calculate_from_volume_buckets = AlertValidator._calculate_from_volume_buckets
    
    def _calculate_from_volume_buckets_patched(self, symbol, window_minutes):
        """Patched version with INFO logging."""
        logger.info(f"🪣 ENTERING _calculate_from_volume_buckets for {symbol} ({window_minutes}min)")
        try:
            result = original_calculate_from_volume_buckets(self, symbol, window_minutes)
            logger.info(f"🪣 EXITING _calculate_from_volume_buckets for {symbol}: {len(result) if result else 0} metrics")
            return result
        except Exception as e:
            logger.error(f"🪣 ERROR in _calculate_from_volume_buckets for {symbol}: {e}")
            return {}
    
    # Apply patches
    AlertValidator._get_underlying_symbol_for_price = _get_underlying_symbol_for_price
    AlertValidator._get_current_price = _get_current_price
    AlertValidator._evaluate_forward_window = _evaluate_forward_window_patched
    AlertValidator.get_rolling_metrics = get_rolling_metrics_patched
    AlertValidator._calculate_from_volume_buckets = _calculate_from_volume_buckets_patched
    
    logger.info("✅ AlertValidator patches applied successfully")

# Auto-apply patches when imported
patch_alert_validator()
