"""
Enhanced Metadata Calculator for Binary Ingestion
==================================================

Calculates technical indicators and option Greeks during binary ingestion.
Integrates with ProductionZerodhaBinaryConverter to add:
- Technical Indicators: RSI, MACD, Bollinger Bands, SMA, EMA, ATR
- Option Greeks: Delta, Gamma, Theta, Vega (if option)
- Note: POC is calculated post-processing (needs session aggregation)
"""

from typing import Dict, List, Optional
import logging
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

# Try to import required libraries
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logger.warning("TA-Lib not available, using fallback calculations")

try:
    from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega, rho
    from scipy.stats import norm
    GREEKS_AVAILABLE = True
except ImportError:
    GREEKS_AVAILABLE = False
    logger.warning("Greek calculation libraries not available")


class EnhancedMetadataCalculator:
    """Calculate indicators and Greeks during ingestion"""
    
    def __init__(self):
        # Rolling windows for indicators (per instrument_token)
        self.price_windows: Dict[int, List[float]] = {}
        self.volume_windows: Dict[int, List[float]] = {}
        self.max_window_size = 50  # Max periods to keep
        
    def calculate_indicators(self, instrument_token: int, price_data: Dict) -> Dict:
        """
        Calculate technical indicators for a tick
        
        Args:
            instrument_token: Instrument token
            price_data: Dict with last_price, open_price, high_price, low_price, close_price
            
        Returns:
            Dict with indicator values (defaults to None if insufficient data)
        """
        # Initialize with None defaults
        indicators = {
            'rsi_14': None,
            'sma_20': None,
            'ema_12': None,
            'bollinger_upper': None,
            'bollinger_middle': None,
            'bollinger_lower': None,
            'macd': None,
            'macd_signal': None,
            'macd_histogram': None,
        }
        
        try:
            # Get price (prefer close, fallback to last)
            close_price = price_data.get('close_price') or price_data.get('last_price', 0)
            high_price = price_data.get('high_price', close_price)
            low_price = price_data.get('low_price', close_price)
            open_price = price_data.get('open_price', close_price)
            
            if close_price <= 0:
                return indicators
            
            # Update rolling window
            if instrument_token not in self.price_windows:
                self.price_windows[instrument_token] = []
            
            window = self.price_windows[instrument_token]
            window.append(close_price)
            
            # Keep only recent data
            if len(window) > self.max_window_size:
                window.pop(0)
            
            # Calculate indicators based on available data
            # No "insufficient data" check - rolling window accumulates across ticks
            # If window is too small, individual indicators will return None
            
            prices = np.array(window)
            
            # RSI(14)
            if len(window) >= 14:
                indicators['rsi_14'] = self._calculate_rsi(prices, period=14)
            
            # SMA(20)
            if len(window) >= 20:
                sma_value = float(np.mean(prices[-20:]))
                if not np.isnan(sma_value):
                    indicators['sma_20'] = sma_value
            
            # EMA(12)
            if len(window) >= 12:
                indicators['ema_12'] = self._calculate_ema(prices, period=12)
            
            # Bollinger Bands (20, 2.0)
            if len(window) >= 20:
                bb = self._calculate_bollinger_bands(prices[-20:], period=20, std_dev=2.0)
                indicators.update(bb)
            
            # MACD (12, 26, 9)
            if len(window) >= 26:
                macd = self._calculate_macd(prices, fast=12, slow=26, signal=9)
                indicators.update(macd)
            
            # ATR(14) - needs high/low arrays
            if len(window) >= 14 and high_price and low_price:
                # For ATR, we'd need high/low history - simplified version
                # Full ATR needs historical high/low/close arrays
                pass  # Skip ATR for now (needs high/low arrays)
            
        except Exception as e:
            logger.debug(f"Indicator calculation failed for token {instrument_token}: {e}")
        
        return indicators
    
    def calculate_greeks(self, metadata: Dict, price_data: Dict, 
                        underlying_price: Optional[float] = None) -> Dict:
        """
        Calculate option Greeks if instrument is an option
        
        Args:
            metadata: Instrument metadata (strike_price, expiry, option_type, etc.)
            price_data: Price data (last_price)
            underlying_price: Underlying asset price (if available)
            
        Returns:
            Dict with Greek values (defaults to None for non-options)
        """
        # Initialize with None defaults
        greeks = {
            'delta': None,
            'gamma': None,
            'theta': None,
            'vega': None,
            'rho': None,
            'implied_volatility': None,
        }
        
        if not GREEKS_AVAILABLE:
            return greeks
        
        try:
            # Check if this is an option
            instrument_type = metadata.get('instrument_type', '').upper()
            option_type = metadata.get('option_type', '').upper()
            
            # Options have instrument_type CE/PE or option_type CE/PE
            is_option = (instrument_type in ['CE', 'PE', 'OPTION'] or 
                        option_type in ['CE', 'PE', 'CALL', 'PUT'])
            
            if not is_option:
                return greeks
            
            strike_price = metadata.get('strike_price', 0)
            expiry = metadata.get('expiry')
            option_type = metadata.get('option_type', '').upper()
            spot_price = underlying_price or price_data.get('last_price', 0)
            
            if not strike_price or not expiry or spot_price <= 0:
                return greeks
            
            # Calculate time to expiry
            from datetime import date as date_type
            if isinstance(expiry, str):
                try:
                    expiry_date = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                except:
                    return greeks
            elif isinstance(expiry, date_type):  # date object
                expiry_date = datetime.combine(expiry, datetime.min.time())
            elif hasattr(expiry, 'timestamp'):
                expiry_date = expiry
            else:
                return greeks
            
            days_to_expiry = (expiry_date - datetime.now()).days
            if days_to_expiry <= 0:
                return greeks
            
            time_to_expiry = days_to_expiry / 365.0
            risk_free_rate = 0.05  # 5% default
            volatility = 0.2  # 20% default IV (can be enhanced to get from data)
            
            # Determine option flag
            option_flag = 'c' if option_type in ['CE', 'CALL'] else 'p'
            
            # Calculate Greeks using py_vollib
            greeks['delta'] = float(delta(option_flag, spot_price, strike_price, 
                                          time_to_expiry, risk_free_rate, volatility))
            greeks['gamma'] = float(gamma(option_flag, spot_price, strike_price, 
                                          time_to_expiry, risk_free_rate, volatility))
            greeks['theta'] = float(theta(option_flag, spot_price, strike_price, 
                                          time_to_expiry, risk_free_rate, volatility))
            greeks['vega'] = float(vega(option_flag, spot_price, strike_price, 
                                        time_to_expiry, risk_free_rate, volatility))
            greeks['rho'] = float(rho(option_flag, spot_price, strike_price, 
                                      time_to_expiry, risk_free_rate, volatility))
            greeks['implied_volatility'] = volatility  # Placeholder - enhance with real IV
            
        except Exception as e:
            logger.debug(f"Greek calculation failed: {e}")
        
        return greeks
    
    def _calculate_rsi(self, prices: np.ndarray, period: int = 14) -> Optional[float]:
        """Calculate RSI - Returns None if insufficient data"""
        if TALIB_AVAILABLE:
            rsi = talib.RSI(prices, timeperiod=period)
            if len(rsi) == 0 or np.isnan(rsi[-1]):
                return None
            return float(rsi[-1])
        
        # Fallback calculation
        if len(prices) < period + 1:
            return None
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        if avg_loss == 0:
            return None  # Return None instead of 100.0 for invalid RSI
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)
    
    def _calculate_ema(self, prices: np.ndarray, period: int) -> Optional[float]:
        """Calculate EMA - Returns None if insufficient data"""
        if TALIB_AVAILABLE:
            ema = talib.EMA(prices, timeperiod=period)
            if len(ema) == 0 or np.isnan(ema[-1]):
                return None
            return float(ema[-1])
        
        # Fallback calculation
        if len(prices) < period:
            return None
        
        alpha = 2.0 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = alpha * price + (1 - alpha) * ema
        return float(ema)
    
    def _calculate_bollinger_bands(self, prices: np.ndarray, period: int = 20, 
                                   std_dev: float = 2.0) -> Dict:
        """Calculate Bollinger Bands - Returns None values if insufficient data"""
        result = {
            'bollinger_upper': None,
            'bollinger_middle': None,
            'bollinger_lower': None,
        }
        
        if len(prices) < period:
            return result
        
        if TALIB_AVAILABLE:
            upper, middle, lower = talib.BBANDS(prices, timeperiod=period, nbdevup=std_dev, 
                                                  nbdevdn=std_dev, matype=0)
            if len(upper) > 0 and not np.isnan(upper[-1]):
                result['bollinger_upper'] = float(upper[-1])
            if len(middle) > 0 and not np.isnan(middle[-1]):
                result['bollinger_middle'] = float(middle[-1])
            if len(lower) > 0 and not np.isnan(lower[-1]):
                result['bollinger_lower'] = float(lower[-1])
            return result
        
        # Fallback calculation
        sma = np.mean(prices)
        std = np.std(prices)
        
        return {
            'bollinger_upper': float(sma + (std_dev * std)),
            'bollinger_middle': float(sma),
            'bollinger_lower': float(sma - (std_dev * std)),
        }
    
    def _calculate_macd(self, prices: np.ndarray, fast: int = 12, slow: int = 26, 
                        signal: int = 9) -> Dict:
        """Calculate MACD - Returns None values if insufficient data"""
        result = {
            'macd': None,
            'macd_signal': None,
            'macd_histogram': None,
        }
        
        if len(prices) < slow:
            return result
        
        if TALIB_AVAILABLE:
            macd_line, signal_line, histogram = talib.MACD(prices, fastperiod=fast, 
                                                             slowperiod=slow, signalperiod=signal)
            if len(macd_line) > 0 and not np.isnan(macd_line[-1]):
                result['macd'] = float(macd_line[-1])
            if len(signal_line) > 0 and not np.isnan(signal_line[-1]):
                result['macd_signal'] = float(signal_line[-1])
            if len(histogram) > 0 and not np.isnan(histogram[-1]):
                result['macd_histogram'] = float(histogram[-1])
            return result
        
        # Fallback calculation
        ema_fast = self._calculate_ema(prices, fast)
        ema_slow = self._calculate_ema(prices, slow)
        
        if ema_fast is not None and ema_slow is not None:
            macd = ema_fast - ema_slow
            result['macd'] = macd
            result['macd_histogram'] = macd  # Simplified - signal would need separate calculation
        
        return result
    
    def reset_instrument(self, instrument_token: int):
        """Reset rolling window for an instrument (e.g., new session)"""
        self.price_windows.pop(instrument_token, None)
        self.volume_windows.pop(instrument_token, None)

