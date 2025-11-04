"""
Calculations Module - High-Performance Hybrid Calculations
Uses Polars for maximum performance with exact Zerodha field names
Replaces all legacy calculation methods with optimized implementations
Updated: 2025-10-27
"""

import time
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Union, Optional, Tuple, Any
import math
import statistics
from collections import defaultdict, deque
from pathlib import Path
import re
from enum import Enum
from functools import wraps

from utils.correct_volume_calculator import CorrectVolumeCalculator

def _extract_volume_ratio(source: Dict[str, Any]) -> float:
    """Read pre-computed volume ratio from incoming data."""
    if not isinstance(source, dict):
        return 0.0
    for field in ("volume_ratio", "volume_ratio_enhanced", "normalized_volume"):
        value = source.get(field)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return 0.0

# Essential imports for HybridCalculations
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

# Fast EMA Classes using Numba for ultra-fast calculations
class FastEMA:
    """Ultra-fast EMA calculation using Numba optimization"""
    
    def __init__(self, period: int):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.ema = None
        self.is_initialized = False
        
    def update_fast(self, new_value: float) -> float:
        """Fast EMA update - optimized for single tick processing"""
        if not self.is_initialized:
            self.ema = new_value
            self.is_initialized = True
        else:
            self.ema = (new_value * self.alpha) + (self.ema * (1 - self.alpha))
        return self.ema
    
    def update_batch(self, values: np.array) -> np.array:
        """Process batch of values - much faster than loop"""
        if len(values) == 0:
            return np.array([])
            
        if not self.is_initialized:
            self.ema = values[0]
            self.is_initialized = True
            result = [self.ema]
        else:
            result = []
        
        # Vectorized EMA calculation
        alpha = self.alpha
        for value in values[len(result):]:
            self.ema = (value * alpha) + (self.ema * (1 - alpha))
            result.append(self.ema)
            
        return np.array(result)

class CircularEMA:
    """Pre-allocated circular buffer EMA for maximum performance"""
    
    def __init__(self, period: int, max_points: int = 10000):
        self.period = period
        self.alpha = 2.0 / (period + 1.0)
        self.buffer = np.zeros(max_points)
        self.emas = np.zeros(max_points)
        self.index = 0
        self.ema = 0.0
        self.is_initialized = False
        
    def add_value(self, value: float) -> float:
        """Add single value - O(1) operation"""
        if not self.is_initialized:
            self.ema = value
            self.is_initialized = True
        else:
            self.ema = (value * self.alpha) + (self.ema * (1 - self.alpha))
        
        self.buffer[self.index] = value
        self.emas[self.index] = self.ema
        self.index = (self.index + 1) % len(self.buffer)
        
        return self.ema
    
    def get_current_ema(self) -> float:
        return self.ema

# TA-Lib imports for enhanced technical analysis (fallback only)
try:
    import talib
    TALIB_AVAILABLE = True
    logger.info("✅ TA-Lib loaded (used as fallback only - pure Python libraries preferred)")
except ImportError as e:
    TALIB_AVAILABLE = False
    logger.warning(f"⚠️ TA-Lib not available: {e}")

# pandas_ta - Pure Python alternative to TA-Lib (preferred)
try:
    import pandas_ta as pta
    PANDAS_TA_AVAILABLE = True
    logger.info("✅ pandas_ta loaded successfully (pure Python technical analysis)")
except ImportError as e:
    PANDAS_TA_AVAILABLE = False
    logger.debug(f"pandas_ta not available: {e}")

# Numba imports for ultra-fast calculations
try:
    import numba
    NUMBA_AVAILABLE = True
    logger.info("✅ Numba loaded successfully for ultra-fast calculations")
except ImportError as e:
    NUMBA_AVAILABLE = False
    logger.warning(f"⚠️ Numba not available: {e}")

# Greek calculations imports
try:
    import pandas_market_calendars as mcal
    from scipy.stats import norm
    # Use py_vollib for standardized Greek calculations
    from py_vollib.black_scholes.greeks.analytical import (
        delta, gamma, theta, vega, rho
    )
    PY_VOLLIB_AVAILABLE = True
    GREEK_CALCULATIONS_AVAILABLE = True
    logger.info("✅ Greek calculation libraries loaded successfully (py_vollib + scipy)")
except ImportError as e:
    PY_VOLLIB_AVAILABLE = False
    GREEK_CALCULATIONS_AVAILABLE = False
    logger.warning(f"⚠️ Greek calculation libraries not available: {e}")

class HybridCalculations:
    """
    High-performance calculations using Polars for data processing
    Uses exact Zerodha field names from optimized_field_mapping.yaml
    Replaces legacy calculation methods with optimized implementations
    """
    
    def __init__(self, max_cache_size: int = 1000, max_batch_size: int = 174):
        """Initialize with field mapping support"""
        # Import field mapping utilities
        from utils.yaml_field_loader import (
            get_field_mapping_manager,
            resolve_session_field,
            resolve_calculated_field as resolve_indicator_field,
        )
        
        self.field_mapping_manager = get_field_mapping_manager()
        self.resolve_session_field = resolve_session_field
        self.resolve_indicator_field = resolve_indicator_field
        
        # Initialize with standard parameters
        self.max_cache_size = max_cache_size
        self.max_batch_size = max_batch_size
        self.rolling_windows = {}  # symbol -> DataFrame
        self.cache = {}  # Legacy cache (kept for backward compatibility)
        
        # ✅ FIXED: Initialize missing attributes that were causing errors
        self._symbol_windows = {}  # symbol -> rolling window data
        
        # ✅ FAST EMA INSTANCES: Initialize all EMA windows for ultra-fast calculations
        self.fast_emas = {
            'ema_5': FastEMA(5),
            'ema_10': FastEMA(10),
            'ema_20': FastEMA(20),
            'ema_50': FastEMA(50),
            'ema_100': FastEMA(100),
            'ema_200': FastEMA(200)
        }
        
        # ✅ CIRCULAR EMA BUFFERS: For maximum performance with large datasets
        self.circular_emas = {
            'ema_5': CircularEMA(5),
            'ema_10': CircularEMA(10),
            'ema_20': CircularEMA(20),
            'ema_50': CircularEMA(50),
            'ema_100': CircularEMA(100),
            'ema_200': CircularEMA(200)
        }
        
        self._polars_cache = {}  # Polars DataFrame cache (for DataFrame reuse)
        self._window_size = 100  # Default rolling window size
        
        # ✅ SIMPLIFIED CACHING: Single unified cache with TTL (replaces redundant layers)
        self._cache = {}  # {symbol_indicators: {data, timestamp, data_hash}}
        self._cache_ttl = 300  # 5 minutes TTL
        self._last_calculation_time = {}  # Track when each symbol was last calculated
        self._fresh_calculations = set()  # Track which symbols had fresh calculations (not cached)
        
        self._volume_calculator = None  # Lazy-initialized CorrectVolumeCalculator

    def _get_volume_calculator(self) -> CorrectVolumeCalculator:
        """Return shared volume calculator wired to centralized baselines."""
        if self._volume_calculator is None:
            self._volume_calculator = CorrectVolumeCalculator(getattr(self, 'redis_client', None))
        return self._volume_calculator

    def _to_polars_dataframe(self, data: Union[Dict, List[Dict]], symbol: str = None) -> pl.DataFrame:
        """Convert data to Polars DataFrame with optimized schema using Zerodha field names"""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for HybridCalculations")
            
        # Check cache first
        cache_key = f"{symbol}_{hash(str(data))}" if symbol else None
        if cache_key and cache_key in self._polars_cache:
            return self._polars_cache[cache_key]
        
        # Handle both Dict and List[Dict] inputs
        if isinstance(data, list):
            # Convert list of dicts to Polars DataFrame directly
            df = pl.DataFrame(data)
        else:
            # Convert dict with lists to Polars DataFrame using Zerodha field names
            df = pl.DataFrame({
                'exchange_timestamp_ms': pl.Series(data.get('exchange_timestamp_ms', []), dtype=pl.Int64),
                'timestamp_ns_ms': pl.Series(data.get('timestamp_ns_ms', []), dtype=pl.Int64),
                'timestamp_ms': pl.Series(data.get('timestamp_ms', []), dtype=pl.Int64),
                'last_price': pl.Series(data.get('last_price', []), dtype=pl.Float64),
                'zerodha_cumulative_volume': pl.Series(data.get('zerodha_cumulative_volume', []), dtype=pl.Float64),
                'zerodha_last_traded_quantity': pl.Series(data.get('zerodha_last_traded_quantity', []), dtype=pl.Float64),
                'bucket_incremental_volume': pl.Series(data.get('bucket_incremental_volume', []), dtype=pl.Float64),
                'bucket_cumulative_volume': pl.Series(data.get('bucket_cumulative_volume', []), dtype=pl.Float64),
                'high': pl.Series(data.get('high', []), dtype=pl.Float64),
                'low': pl.Series(data.get('low', []), dtype=pl.Float64),
                'average_price': pl.Series(data.get('average_price', []), dtype=pl.Float64)
            })
        
        # Sort by primary timestamp if available
        if 'exchange_timestamp_ms' in df.columns:
            df = df.sort('exchange_timestamp_ms')
        elif 'timestamp_ms' in df.columns:
            df = df.sort('timestamp_ms')
        
        # Cache if symbol provided
        if cache_key:
            self._polars_cache[cache_key] = df
            
        return df
    
    def calculate_atr(self, highs: List[float], lows: List[float], 
                     closes: List[float], period: int = 14, symbol: str = None) -> float:
        """ATR using pure Python libraries (pandas/pandas_ta preferred) with TA-Lib fallback"""
        
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_atr = get_indicator_from_redis(symbol or "SYMBOL", "atr", self.redis_client)
            if redis_atr is not None:
                return redis_atr
        
        # ✅ PREFERRED: Use pandas_ta (pure Python) first
        if PANDAS_TA_AVAILABLE and len(highs) >= period:
            try:
                import pandas as pd
                df = pd.DataFrame({
                    'high': highs,
                    'low': lows,
                    'close': closes
                })
                atr = pta.atr(df['high'], df['low'], df['close'], length=period)
                result = float(atr.iloc[-1]) if len(atr) > 0 and not pd.isna(atr.iloc[-1]) else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"pandas_ta ATR failed, using fallback: {e}")
        
        # ✅ FALLBACK 1: Use pandas (pure Python)
        if PANDAS_AVAILABLE and len(highs) >= period + 1:
            try:
                import pandas as pd
                df = pd.DataFrame({
                    'high': highs,
                    'low': lows,
                    'close': closes
                })
                
                # True Range calculation
                df['tr1'] = df['high'] - df['low']
                df['tr2'] = (df['high'] - df['close'].shift(1)).abs()
                df['tr3'] = (df['low'] - df['close'].shift(1)).abs()
                df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
                
                # ATR as rolling mean
                atr = df['tr'].rolling(window=period).mean()
                result = float(atr.iloc[-1]) if len(atr) > 0 and not pd.isna(atr.iloc[-1]) else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"pandas ATR failed, using fallback: {e}")
        
        # ✅ FALLBACK 2: Use Polars (pure Python)
        if POLARS_AVAILABLE and len(highs) >= period + 1:
            try:
                df = pl.DataFrame({
                    'high': highs,
                    'low': lows, 
                    'close': closes
                })
                
                # True Range calculation in Polars
                df = df.with_columns([
                    (pl.col('high') - pl.col('low')).alias('tr1'),
                    (pl.col('high') - pl.col('close').shift(1)).abs().alias('tr2'),
                    (pl.col('low') - pl.col('close').shift(1)).abs().alias('tr3')
                ]).with_columns([
                    pl.max_horizontal('tr1', 'tr2', 'tr3').alias('tr')
                ])
                
                # ATR as rolling mean
                atr = df['tr'].tail(period).mean()
                result = float(atr) if atr is not None else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"Polars ATR failed, using fallback: {e}")
        
        # ✅ FALLBACK 3: Use TA-Lib (if available)
        if TALIB_AVAILABLE and len(highs) >= period:
            try:
                np_highs = np.array(highs, dtype=np.float64)
                np_lows = np.array(lows, dtype=np.float64)
                np_closes = np.array(closes, dtype=np.float64)
                
                atr = talib.ATR(np_highs, np_lows, np_closes, timeperiod=period)
                result = float(atr[-1]) if len(atr) > 0 and not np.isnan(atr[-1]) else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"TA-Lib ATR failed, using simple fallback: {e}")
        
        # Simple fallback
        return self._fallback_atr(highs, lows, closes, period)
    
    def calculate_ema(self, prices: List[float], period: int = 20) -> float:
        """EMA using pure Python libraries (pandas/pandas_ta preferred) with TA-Lib fallback"""
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_ema = get_indicator_from_redis("SYMBOL", f"ema_{period}", self.redis_client)
            if redis_ema is not None:
                return redis_ema
        
        # ✅ PREFERRED: Use pandas_ta (pure Python) first
        if PANDAS_TA_AVAILABLE and len(prices) >= period:
            try:
                import pandas as pd
                series = pd.Series(prices)
                ema = pta.ema(series, length=period)
                result = float(ema.iloc[-1]) if len(ema) > 0 and not pd.isna(ema.iloc[-1]) else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"pandas_ta EMA failed, using fallback: {e}")
        
        # ✅ FALLBACK 1: Use pandas (pure Python)
        if PANDAS_AVAILABLE and len(prices) >= period:
            try:
                import pandas as pd
                series = pd.Series(prices)
                ema = series.ewm(span=period, adjust=False).mean()
                result = float(ema.iloc[-1]) if len(ema) > 0 and not pd.isna(ema.iloc[-1]) else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"pandas EMA failed, using fallback: {e}")
        
        # ✅ FALLBACK 2: Use Polars (pure Python)
        if POLARS_AVAILABLE and len(prices) >= period:
            try:
                series = pl.Series('last_price', prices)
                ema = series.ewm_mean(span=period).tail(1)[0]
                result = float(ema) if ema is not None else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"Polars EMA failed, using fallback: {e}")
        
        # ✅ FALLBACK 3: Use TA-Lib (if available)
        if TALIB_AVAILABLE and len(prices) >= period:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                ema = talib.EMA(np_prices, timeperiod=period)
                result = float(ema[-1]) if len(ema) > 0 and not np.isnan(ema[-1]) else float(prices[-1]) if prices else 0.0
                if result > 0:
                    return result
            except Exception as e:
                logger.debug(f"TA-Lib EMA failed, using simple fallback: {e}")
        
        # Simple fallback
        return self._fallback_ema(prices, period)
    
    def calculate_ema_5(self, prices: List[float]) -> float:
        """EMA 5 - Short-term trend"""
        return self.calculate_ema(prices, 5)
    
    def calculate_ema_10(self, prices: List[float]) -> float:
        """EMA 10 - Short-term trend"""
        return self.calculate_ema(prices, 10)
    
    def calculate_ema_20(self, prices: List[float]) -> float:
        """EMA 20 - Medium-term trend"""
        return self.calculate_ema(prices, 20)
    
    def calculate_ema_50(self, prices: List[float]) -> float:
        """EMA 50 - Long-term trend"""
        return self.calculate_ema(prices, 50)
    
    def calculate_ema_100(self, prices: List[float]) -> float:
        """EMA 100 - Long-term trend"""
        return self.calculate_ema(prices, 100)
    
    def calculate_ema_200(self, prices: List[float]) -> float:
        """EMA 200 - Very long-term trend"""
        return self.calculate_ema(prices, 200)
    
    def calculate_ema_crossover(self, prices: List[float]) -> Dict:
        """Calculate multiple EMAs for crossover analysis"""
        return {
            'ema_5': self.calculate_ema_5(prices),
            'ema_10': self.calculate_ema_10(prices),
            'ema_20': self.calculate_ema_20(prices),
            'ema_50': self.calculate_ema_50(prices),
            'ema_100': self.calculate_ema_100(prices),
            'ema_200': self.calculate_ema_200(prices)
        }
    
    # ✅ FAST EMA METHODS: Ultra-fast EMA calculations using Numba optimization
    def calculate_ema_fast(self, prices: List[float], period: int = 20) -> float:
        """Ultra-fast EMA calculation using FastEMA class"""
        if not prices:
            return 0.0
            
        # Use FastEMA for single tick processing
        ema_key = f'ema_{period}'
        if ema_key in self.fast_emas:
            fast_ema = self.fast_emas[ema_key]
            if len(prices) == 1:
                return fast_ema.update_fast(prices[0])
            else:
                # Process batch
                np_prices = np.array(prices)
                result = fast_ema.update_batch(np_prices)
                return float(result[-1]) if len(result) > 0 else 0.0
        
        # Fallback to original method
        return self.calculate_ema(prices, period)
    
    def calculate_ema_circular(self, prices: List[float], period: int = 20) -> float:
        """Ultra-fast EMA using circular buffer for maximum performance"""
        if not prices:
            return 0.0
            
        ema_key = f'ema_{period}'
        if ema_key in self.circular_emas:
            circular_ema = self.circular_emas[ema_key]
            for price in prices:
                circular_ema.add_value(price)
            return circular_ema.get_current_ema()
        
        # Fallback to original method
        return self.calculate_ema(prices, period)
    
    def update_ema_for_tick(self, symbol: str, price: float, period: int = 20) -> float:
        """Update EMA for a single tick - O(1) operation"""
        ema_key = f'ema_{period}'
        if ema_key in self.fast_emas:
            return self.fast_emas[ema_key].update_fast(price)
        
        # Fallback: get current window and recalculate
        if symbol in self._symbol_windows:
            window = self._symbol_windows[symbol]
            prices = window['last_price'].to_list()
            prices.append(price)
            return self.calculate_ema(prices, period)
        
        return price  # First tick
    
    def get_all_emas_fast(self, symbol: str, price: float) -> Dict:
        """Get all EMA windows for a single tick - ultra-fast"""
        return {
            'ema_5': self.update_ema_for_tick(symbol, price, 5),
            'ema_10': self.update_ema_for_tick(symbol, price, 10),
            'ema_20': self.update_ema_for_tick(symbol, price, 20),
            'ema_50': self.update_ema_for_tick(symbol, price, 50),
            'ema_100': self.update_ema_for_tick(symbol, price, 100),
            'ema_200': self.update_ema_for_tick(symbol, price, 200)
        }
    
    def _should_recalculate(self, symbol: str, current_time: float, new_data: List[Dict]) -> bool:
        """Determine if we need fresh calculations (not from cache)"""
        cache_key = f"{symbol}_indicators"
        
        # No cache? Recalculate
        if cache_key not in self._cache:
            return True
        
        cached = self._cache[cache_key]
        
        # Time expired? Recalculate
        if current_time - cached['timestamp'] >= self._cache_ttl:
            return True
        
        # Data changed significantly? Recalculate
        new_hash = hash(str(new_data))
        if new_hash != cached['data_hash']:
            return True
        
        # Otherwise use cache
        return False
    
    def calculate_rsi(self, prices: List[float], period: int = 14, symbol: str = None) -> float:
        """RSI using TA-Lib (preferred) with Polars fallback and Redis caching"""
        
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_rsi = get_indicator_from_redis(symbol or "SYMBOL", "rsi", self.redis_client)
            if redis_rsi is not None:
                return redis_rsi
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= period + 1:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                rsi = talib.RSI(np_prices, timeperiod=period)
                result = float(rsi[-1]) if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.0
                return result
            except Exception as e:
                logger.warning(f"TA-Lib RSI failed, using fallback: {e}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= period + 1:
            try:
                df = pl.DataFrame({'last_price': prices})
                rsi = df.with_columns([
                    pl.col('last_price').diff().alias('delta')
                ]).with_columns([
                    pl.when(pl.col('delta') > 0)
                      .then(pl.col('delta'))
                      .otherwise(0)
                      .alias('gain'),
                    pl.when(pl.col('delta') < 0) 
                      .then(-pl.col('delta'))
                      .otherwise(0)
                      .alias('loss')
                ]).with_columns([
                    pl.col('gain').rolling_mean(period).alias('avg_gain'),
                    pl.col('loss').rolling_mean(period).alias('avg_loss')
                ]).with_columns([
                    (100 - (100 / (1 + (pl.col('avg_gain') / pl.col('avg_loss'))))).alias('rsi')
                ])
                
                return float(rsi['rsi'].tail(1)[0]) if rsi['rsi'].tail(1)[0] is not None else 50.0
            except Exception as e:
                logger.warning(f"Polars RSI failed, using simple fallback: {e}")
        
        # Simple fallback
        return self._fallback_rsi(prices, period)
    
    def calculate_vwap(self, prices: List[float], volumes: List[float]) -> float:
        """Calculate VWAP from price and volume lists"""
        if not prices or not volumes or len(prices) != len(volumes):
            return 0.0
        
        # Note: TA-Lib doesn't have VWAP function, using optimized fallback
        # TA-Lib VWAP is not available in version 0.6.8
        
        # Fallback to simple calculation
        total_volume = 0.0
        weighted_sum = 0.0
        
        for price, volume in zip(prices, volumes):
            if price > 0 and volume > 0:
                total_volume += volume
                weighted_sum += price * volume
        
        return weighted_sum / total_volume if total_volume > 0 else 0.0
    
    def calculate_vwap_batch(self, tick_data: List[Dict]) -> float:
        """VWAP using TA-Lib (preferred) with Polars fallback and Redis caching"""
        if not tick_data:
            return 0.0
        
        # Try TA-Lib first (fastest and most accurate) – note TA-Lib does not expose VWAP, so we
        # approximate using cumulative price * volume / cumulative volume when enough data exists.
        if TALIB_AVAILABLE and len(tick_data) >= 20:
            try:
                prices: List[float] = []
                volumes: List[float] = []

                for tick in tick_data:
                    price = tick.get("last_price", 0.0)
                    volume = (
                        tick.get("zerodha_cumulative_volume", 0)
                        or tick.get("bucket_incremental_volume", 0)
                        or tick.get("bucket_cumulative_volume", 0)
                        or 0
                    )
                    if price > 0 and volume > 0:
                        prices.append(float(price))
                        volumes.append(float(volume))

                if prices and volumes:
                    weighted_sum = sum(p * v for p, v in zip(prices, volumes))
                    total_volume = sum(volumes)
                    if total_volume > 0:
                        return weighted_sum / total_volume
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("TA-Lib VWAP fallback failed: %s", exc)

        # Fallback to Polars
        if POLARS_AVAILABLE:
            try:
                df = pl.DataFrame(tick_data)

                # VWAP calculation using available volume fields
                volume_col = None
                for col_name in [
                    "zerodha_cumulative_volume",
                    "bucket_incremental_volume",
                    "bucket_cumulative_volume",
                ]:
                    if col_name in df.columns and df[col_name].sum() > 0:
                        volume_col = col_name
                        break

                if volume_col:
                    vwap = df.select(
                        (pl.col("last_price") * pl.col(volume_col)).sum()
                        / pl.col(volume_col).sum()
                    ).row(0)[0]
                else:
                    # Fallback: use simple average if no volume data
                    vwap = df["last_price"].mean()

                return float(vwap) if vwap is not None else 0.0
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Polars VWAP failed, using simple fallback: %s", exc)

        # Simple fallback
        return self._fallback_vwap(tick_data)
    
    def calculate_volume_profile(self, ticks: List[Dict], price_bins: int = 20) -> Dict:
        """Volume profile using Polars (high performance) with Zerodha field names"""
        if not POLARS_AVAILABLE:
            return self._fallback_volume_profile(ticks, price_bins)
            
        if not ticks:
            return {}
        
        try:
            df = pl.DataFrame(ticks)
            
            # Filter out NaN values and ensure we have valid data
            df = df.filter(pl.col('last_price').is_not_nan() & pl.col('last_price').is_not_null())
            
            if df.is_empty():
                return {}
            
            # Create last_price bins and aggregate bucket_incremental_volume using Zerodha field names
            # NOTE: This is for display visualization only - POC/VA calculation is done by VolumeProfileManager
            # Use bucket_incremental_volume (not cumulative) for correct bin aggregation
            min_price, max_price = df['last_price'].min(), df['last_price'].max()
            
            if min_price == max_price:
                # For single price level, use bucket_incremental_volume
                return {'bins': [min_price], 'volumes': [df['bucket_incremental_volume'].sum()]}
            
            bin_size = (max_price - min_price) / price_bins
            
            volume_profile = df.with_columns([
                ((pl.col('last_price') - min_price) / bin_size).floor().cast(pl.Int64).alias('bin')
            ]).group_by('bin').agg([
                pl.col('bucket_incremental_volume').sum().alias('total_volume'),  # ✅ FIXED: Use incremental volume
                pl.col('last_price').mean().alias('avg_price')
            ]).sort('bin')
            
            return {
                'bins': volume_profile['avg_price'].to_list(),
                'volumes': volume_profile['total_volume'].to_list()
            }
        except Exception:
            return self._fallback_volume_profile(ticks, price_bins)
    
    def calculate_macd(self, prices: List[float], fast_period: int = 12, 
                      slow_period: int = 26, signal_period: int = 9) -> Dict:
        """MACD using TA-Lib (preferred) with Polars fallback and Redis caching"""
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_macd = get_indicator_from_redis("SYMBOL", "macd", self.redis_client)
            if redis_macd is not None:
                if isinstance(redis_macd, dict):
                    return redis_macd
                else:
                    return {'macd': redis_macd, 'signal': 0.0, 'histogram': 0.0}
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= slow_period:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                macd, macd_signal, macd_hist = talib.MACD(np_prices, 
                                                         fastperiod=fast_period,
                                                         slowperiod=slow_period, 
                                                         signalperiod=signal_period)
                
                return {
                    'macd': float(macd[-1]) if len(macd) > 0 and not np.isnan(macd[-1]) else 0.0,
                    'signal': float(macd_signal[-1]) if len(macd_signal) > 0 and not np.isnan(macd_signal[-1]) else 0.0,
                    'histogram': float(macd_hist[-1]) if len(macd_hist) > 0 and not np.isnan(macd_hist[-1]) else 0.0
                }
            except Exception as e:
                logger.warning(f"TA-Lib MACD failed, using fallback: {e}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= slow_period:
            try:
                df = pl.DataFrame({'last_price': prices})
                
                # Calculate EMAs
                ema_fast = df['last_price'].ewm_mean(span=fast_period)
                ema_slow = df['last_price'].ewm_mean(span=slow_period)
                
                # MACD line
                macd_line = ema_fast - ema_slow
                
                # Signal line (EMA of MACD)
                signal_line = macd_line.ewm_mean(span=signal_period)
                
                # Histogram
                histogram = macd_line - signal_line
                
                return {
                    'macd': float(macd_line.tail(1)[0]) if macd_line.tail(1)[0] is not None else 0.0,
                    'signal': float(signal_line.tail(1)[0]) if signal_line.tail(1)[0] is not None else 0.0,
                    'histogram': float(histogram.tail(1)[0]) if histogram.tail(1)[0] is not None else 0.0
                }
            except Exception as e:
                logger.warning(f"Polars MACD failed, using simple fallback: {e}")
        
        # Simple fallback
        return self._fallback_macd(prices, fast_period, slow_period, signal_period)
    
    def calculate_bollinger_bands(self, prices: List[float], period: int = 20, 
                                 std_dev: float = 2.0) -> Dict:
        """Bollinger Bands using TA-Lib (preferred) with Polars fallback and Redis caching"""
        # First try Redis fallback
        if hasattr(self, 'redis_client') and self.redis_client:
            redis_bb = get_indicator_from_redis("SYMBOL", "bollinger_bands", self.redis_client)
            if redis_bb is not None:
                if isinstance(redis_bb, dict):
                    return redis_bb
                else:
                    return {'upper': redis_bb, 'middle': 0.0, 'lower': 0.0}
        
        # Try TA-Lib first (fastest and most accurate)
        if TALIB_AVAILABLE and len(prices) >= period:
            try:
                np_prices = np.array(prices, dtype=np.float64)
                upper, middle, lower = talib.BBANDS(np_prices, timeperiod=period, 
                                                  nbdevup=std_dev, nbdevdn=std_dev, 
                                                  matype=0)
                
                return {
                    'upper': float(upper[-1]) if len(upper) > 0 and not np.isnan(upper[-1]) else 0.0,
                    'middle': float(middle[-1]) if len(middle) > 0 and not np.isnan(middle[-1]) else 0.0,
                    'lower': float(lower[-1]) if len(lower) > 0 and not np.isnan(lower[-1]) else 0.0
                }
            except Exception as e:
                logger.warning(f"TA-Lib Bollinger Bands failed, using fallback: {e}")
        
        # Fallback to Polars
        if POLARS_AVAILABLE and len(prices) >= period:
            try:
                df = pl.DataFrame({'last_price': prices})
                
                # Calculate moving average and standard deviation
                middle_band = df['last_price'].rolling_mean(period)
                std = df['last_price'].rolling_std(period)
                
                # Bollinger Bands
                upper_band = middle_band + (std * std_dev)
                lower_band = middle_band - (std * std_dev)
                
                return {
                    'upper': float(upper_band.tail(1)[0]) if upper_band.tail(1)[0] is not None else 0.0,
                    'middle': float(middle_band.tail(1)[0]) if middle_band.tail(1)[0] is not None else 0.0,
                    'lower': float(lower_band.tail(1)[0]) if lower_band.tail(1)[0] is not None else 0.0
                }
            except Exception as e:
                logger.warning(f"Polars Bollinger Bands failed, using simple fallback: {e}")
        
        # Simple fallback
        return self._fallback_bollinger_bands(prices, period, std_dev)
    
    def calculate_volume_ratio(self, symbol: str, current_volume: float, 
                              avg_volume: float) -> float:
        """Volume ratio calculation using Zerodha field names"""
        if avg_volume <= 0:
            return 1.0
        
        return float(current_volume / avg_volume)

    def calculate_volume_statistics(self, volumes: List[float]) -> Dict[str, float]:
        """
        Calculate robust volume statistics for real-world incremental volume data
        
        Handles the actual characteristics of volume data:
        - Extreme variance: 35 to 14,668,800 (400,000x difference)
        - Many zero values (no trading activity)
        - Uses percentiles instead of mean/std for robustness
        - Spike detection based on 90th percentile threshold
        
        Args:
            volumes: List of historical incremental volume values
            
        Returns:
            Dictionary with median, percentiles, spike detection, and confidence
        """
        if not volumes or len(volumes) < 2:
            return {
                'median_volume': 0.0,
                'percentile_75': 0.0,
                'percentile_90': 0.0,
                'is_spike': False,
                'spike_ratio': 1.0,
                'confidence_level': 0.0
            }
        
        # Filter out zeros and extreme outliers for robust statistics
        non_zero_volumes = [v for v in volumes if v > 0]
        if not non_zero_volumes:
            return {
                'median_volume': 0.0,
                'percentile_75': 0.0,
                'percentile_90': 0.0,
                'is_spike': False,
                'spike_ratio': 1.0,
                'confidence_level': 0.0
            }
        
        # Sort for percentile calculations
        sorted_volumes = sorted(non_zero_volumes)
        n = len(sorted_volumes)
        
        # Calculate robust statistics (percentiles instead of mean/std)
        median_volume = sorted_volumes[n // 2]
        percentile_75 = sorted_volumes[int(n * 0.75)]
        percentile_90 = sorted_volumes[int(n * 0.90)]
        
        # Current volume (last in list)
        current_volume = volumes[-1]
        
        # Spike detection using percentile-based thresholds
        # Spike if current volume > 90th percentile
        is_spike = current_volume > percentile_90
        
        # Calculate spike ratio relative to median
        spike_ratio = current_volume / median_volume if median_volume > 0 else 1.0
        
        # Confidence level based on how far above median
        if current_volume > percentile_90:
            confidence_level = 0.9
        elif current_volume > percentile_75:
            confidence_level = 0.7
        elif current_volume > median_volume:
            confidence_level = 0.5
        else:
            confidence_level = 0.3
        
        return {
            'median_volume': median_volume,
            'percentile_75': percentile_75,
            'percentile_90': percentile_90,
            'is_spike': is_spike,
            'spike_ratio': spike_ratio,
            'confidence_level': confidence_level
        }

    def calculate_volume_spike_detection(self, symbol: str, current_volume: float, 
                                       historical_volumes: List[float], 
                                       confidence_threshold: float = 0.95) -> Dict[str, any]:
        """
        Volume spike detection for real-world incremental volume data
        
        Uses percentile-based thresholds instead of Z-scores due to:
        - Extreme variance in volume values (35 to 14M+)
        - Many zero values affecting statistical calculations
        - Percentiles more robust than mean/std for this data type
        
        Args:
            symbol: Symbol name
            current_volume: Current incremental volume value
            historical_volumes: List of historical incremental volumes
            confidence_threshold: Confidence level (0.95 = 90th percentile, 0.90 = 75th percentile)
            
        Returns:
            Dictionary with spike analysis using percentile-based methods
        """
        if not historical_volumes or len(historical_volumes) < 5:
            # Not enough data for analysis
            avg_volume = sum(historical_volumes) / len(historical_volumes) if historical_volumes else 0
            return {
                'is_spike': current_volume > avg_volume * 3.0 if avg_volume > 0 else False,
                'confidence': 0.5,
                'method': 'simple_threshold',
                'spike_ratio': current_volume / avg_volume if avg_volume > 0 else 1.0,
                'statistical_significance': 'low'
            }
        
        # Calculate robust statistics
        stats = self.calculate_volume_statistics(historical_volumes + [current_volume])
        
        # Determine spike based on percentile thresholds
        if confidence_threshold >= 0.95:
            # High confidence: must be above 90th percentile
            is_spike = current_volume > stats['percentile_90']
        elif confidence_threshold >= 0.90:
            # Medium confidence: must be above 75th percentile
            is_spike = current_volume > stats['percentile_75']
        else:
            # Low confidence: must be above median
            is_spike = current_volume > stats['median_volume']
        
        # Determine statistical significance based on spike ratio
        if stats['spike_ratio'] > 10.0:
            significance = 'very_high'
        elif stats['spike_ratio'] > 5.0:
            significance = 'high'
        elif stats['spike_ratio'] > 2.0:
            significance = 'medium'
        else:
            significance = 'low'
        
        return {
            'is_spike': is_spike,
            'confidence': confidence_threshold,
            'method': 'percentile_based',
            'spike_ratio': stats['spike_ratio'],
            'median_volume': stats['median_volume'],
            'percentile_90': stats['percentile_90'],
            'statistical_significance': significance,
            'volume_ratio': stats['spike_ratio']
        }
    
    def calculate_price_change(self, last_price: float, previous_price: float) -> float:
        """Price change percentage using Zerodha field names"""
        if previous_price <= 0:
            return 0.0
        
        return float((last_price - previous_price) / previous_price * 100)
    
    def calculate_net_change(self, last_price: float, close_price: float) -> float:
        """Net change calculation using Zerodha field names"""
        return float(last_price - close_price)
    
    def calculate_session_metrics(self, session_data: Dict) -> Dict:
        """Calculate session metrics using Zerodha field names"""
        return {
            'session_date': session_data.get('session_date', ''),
            'update_count': session_data.get('update_count', 0),
            'last_price': session_data.get('last_price', 0.0),
            'high': session_data.get('high', 0.0),
            'low': session_data.get('low', 0.0),
            'zerodha_cumulative_volume': session_data.get('zerodha_cumulative_volume', 0),
            'bucket_cumulative_volume': session_data.get('bucket_cumulative_volume', 0),
            'bucket_incremental_volume': session_data.get('bucket_incremental_volume', 0),
            'last_update_timestamp': session_data.get('last_update_timestamp', 0)
        }
    
    def calculate_derivatives_metrics(self, derivatives_data: Dict) -> Dict:
        """Calculate derivatives metrics using Zerodha field names"""
        return {
            'oi': derivatives_data.get('oi', 0),
            'oi_day_high': derivatives_data.get('oi_day_high', 0),
            'oi_day_low': derivatives_data.get('oi_day_low', 0),
            'strike_price': derivatives_data.get('strike_price', 0.0),
            'expiry': derivatives_data.get('expiry', ''),
            'option_type': derivatives_data.get('option_type', ''),
            'underlying': derivatives_data.get('underlying', ''),
            'net_change': derivatives_data.get('net_change', 0.0)
        }
    
    def calculate_market_depth_metrics(self, depth_data: Dict) -> Dict:
        """Calculate market depth metrics using Zerodha field names"""
        return {
            'buy_quantity': depth_data.get('buy_quantity', 0),
            'sell_quantity': depth_data.get('sell_quantity', 0),
            'lower_circuit_limit': depth_data.get('lower_circuit_limit', 0.0),
            'upper_circuit_limit': depth_data.get('upper_circuit_limit', 0.0),
            'depth': depth_data.get('depth', {})
        }
    
    def calculate_ohlc_metrics(self, ohlc_data: Dict) -> Dict:
        """Calculate OHLC metrics using Zerodha field names"""
        return {
            'last_price': ohlc_data.get('last_price', 0.0),
            'average_price': ohlc_data.get('average_price', 0.0),
            'ohlc': ohlc_data.get('ohlc', {}),
            'net_change': ohlc_data.get('net_change', 0.0),
            'mode': ohlc_data.get('mode', 'unknown'),
            'tradable': ohlc_data.get('tradable', True)
        }
    
    
    def batch_calculate_indicators(self, tick_data: List[Dict], symbol: str = None) -> Dict:
        """Batch calculate all indicators using Polars for maximum performance with Redis fallback"""
        if not POLARS_AVAILABLE:
            return self._fallback_batch_calculate(tick_data, symbol)
        
        if not tick_data:
            return {}
        
        # Convert to Polars DataFrame
        df = self._to_polars_dataframe(tick_data, symbol)
        
        if df.is_empty():
            return {}
        
        # Extract last_price and bucket_incremental_volume data
        prices = df['last_price'].to_list()
        volumes = df['bucket_incremental_volume'].to_list()
        highs = df['high'].to_list()
        lows = df['low'].to_list()
        
        # Get Redis client if available
        redis_client = getattr(self, 'redis_client', None)
        
        # Get real last_price from Redis if available
        last_price = prices[-1] if prices else 0.0
        if redis_client and hasattr(redis_client, 'get_last_price') and symbol:
            redis_price = redis_client.get_last_price(symbol)
            if redis_price and redis_price > 0:
                last_price = redis_price
        
        # ✅ STANDARDIZED EMA CALCULATIONS: Use price series for proper EMA calculation
        # ❌ FIXED: Previous code used shared global EMA instances with single price - caused contamination
        # ✅ NOW: Calculate EMAs from actual price series using proper Polars/Numpy methods
        if not prices or len(prices) == 0:
            # No price data, return default EMAs
            ema_5 = ema_10 = ema_20 = ema_50 = ema_100 = ema_200 = last_price
        else:
            # Calculate EMAs from actual price series (batch calculation)
            # Use Polars for vectorized EMA calculation
            try:
                price_series = pl.Series('price', prices)
                ema_5 = float(price_series.ewm_mean(span=5).tail(1)[0]) if len(prices) >= 5 else float(prices[-1])
                ema_10 = float(price_series.ewm_mean(span=10).tail(1)[0]) if len(prices) >= 10 else float(prices[-1])
                ema_20 = float(price_series.ewm_mean(span=20).tail(1)[0]) if len(prices) >= 20 else float(prices[-1])
                ema_50 = float(price_series.ewm_mean(span=50).tail(1)[0]) if len(prices) >= 50 else float(prices[-1])
                ema_100 = float(price_series.ewm_mean(span=100).tail(1)[0]) if len(prices) >= 100 else float(prices[-1])
                ema_200 = float(price_series.ewm_mean(span=200).tail(1)[0]) if len(prices) >= 200 else float(prices[-1])
            except Exception as e:
                logger.debug(f"Polars EMA calculation failed for {symbol}, using fallback: {e}")
                # Fallback: Use calculate_ema for each period
                ema_5 = self.calculate_ema(prices, 5)
                ema_10 = self.calculate_ema(prices, 10)
                ema_20 = self.calculate_ema(prices, 20)
                ema_50 = self.calculate_ema(prices, 50)
                ema_100 = self.calculate_ema(prices, 100)
                ema_200 = self.calculate_ema(prices, 200)
        
        fast_emas = {
            'ema_5': ema_5,
            'ema_10': ema_10,
            'ema_20': ema_20,
            'ema_50': ema_50,
            'ema_100': ema_100,
            'ema_200': ema_200
        }
        
        # Calculate all indicators in batch using TA-Lib (preferred) with Redis fallback
        last_tick = tick_data[-1] if tick_data else {}
        volume_ratio = _extract_volume_ratio(last_tick)
        if (not volume_ratio) and symbol:
            try:
                volume_ratio = float(self._get_volume_calculator().calculate_volume_ratio(symbol, last_tick))
            except Exception as vr_calc_err:
                logger.debug(f"Volume calculator fallback in batch_calculate_indicators for {symbol}: {vr_calc_err}")
                volume_ratio = 0.0

        indicators = {
            'symbol': symbol,  # ✅ FIXED: Include symbol in batch calculations
            'last_price': last_price,  # ✅ FIXED: Use Redis-fetched last_price
            'timestamp': last_tick.get('exchange_timestamp_ms', last_tick.get('timestamp', 0)),
            # ✅ FAST EMAs: All windows calculated ultra-fast
            'ema_5': fast_emas['ema_5'],
            'ema_10': fast_emas['ema_10'],
            'ema_20': fast_emas['ema_20'],
            'ema_50': fast_emas['ema_50'],
            'ema_100': fast_emas['ema_100'],
            'ema_200': fast_emas['ema_200'],
            'ema_crossover': {
                'ema_5': fast_emas['ema_5'],
                'ema_10': fast_emas['ema_10'],
                'ema_20': fast_emas['ema_20'],
                'ema_50': fast_emas['ema_50'],
                'ema_100': fast_emas['ema_100'],
                'ema_200': fast_emas['ema_200']
            },
            
            # Other technical indicators with TA-Lib + Redis fallback
            'rsi': self.calculate_rsi(prices, 14),
            'atr': self.calculate_atr(highs, lows, prices, 14),
            'vwap': calculate_vwap_with_redis_fallback(tick_data, symbol, redis_client),
            'macd': self.calculate_macd(prices, 12, 26, 9),
            'bollinger_bands': self.calculate_bollinger_bands(prices),
            'volume_profile': self.calculate_volume_profile(tick_data),
            'volume_ratio': volume_ratio,
            'price_change': self.calculate_price_change(prices[-1] if prices else 0, 
                                                      prices[-2] if len(prices) > 1 else 0)
        }
        
        # 🎯 ADD GREEK CALCULATIONS FOR F&O OPTIONS (same as process_tick)
        if symbol and self._is_option_symbol(symbol) and GREEK_CALCULATIONS_AVAILABLE:
            try:
                # Use last tick for Greek calculations
                greeks = greek_calculator.calculate_greeks_for_tick_data(last_tick)
                if greeks and any(greeks.get(greek, 0) != 0 for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                    indicators.update({
                        'delta': greeks.get('delta', 0.0),
                        'gamma': greeks.get('gamma', 0.0),
                        'theta': greeks.get('theta', 0.0),
                        'vega': greeks.get('vega', 0.0),
                        'rho': greeks.get('rho', 0.0),
                        'dte_years': greeks.get('dte_years', 0.0),
                        'trading_dte': greeks.get('trading_dte', 0),
                        'expiry_series': greeks.get('expiry_series', 'UNKNOWN')
                    })
            except Exception as e:
                logger.debug(f"Greek calculation failed in batch for {symbol}: {e}")
        
        return indicators
    
    def _fallback_atr(self, highs: List[float], lows: List[float], closes: List[float], period: int) -> float:
        """Fallback ATR calculation when Polars is not available"""
        if len(highs) < period + 1:
            return 0.0
        
        tr_values = []
        for i in range(1, len(highs)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            tr_values.append(max(tr1, tr2, tr3))
        
        return sum(tr_values[-period:]) / period if tr_values else 0.0
    
    def _fallback_ema(self, prices: List[float], period: int) -> float:
        """Fallback EMA calculation when Polars is not available"""
        if len(prices) < period:
            return float(prices[-1]) if prices else 0.0
        
        alpha = 2.0 / (period + 1)
        ema = prices[0]
        for last_price in prices[1:]:
            ema = alpha * last_price + (1 - alpha) * ema
        
        return float(ema)
    
    def _fallback_rsi(self, prices: List[float], period: int) -> float:
        """Fallback RSI calculation when Polars is not available"""
        if len(prices) < period + 1:
            return 50.0
        
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(-change)
        
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return float(rsi)
    
    def _fallback_vwap(self, tick_data: List[Dict]) -> float:
        """Fallback VWAP calculation when Polars is not available"""
        if not tick_data:
            return 0.0
        
        total_volume = 0
        total_price_volume = 0
        
        for tick in tick_data:
            last_price = tick.get('last_price', 0)
            # Try different volume fields in order of preference
            volume = (tick.get('zerodha_cumulative_volume', 0) or 
                     tick.get('bucket_incremental_volume', 0) or 
                     tick.get('bucket_cumulative_volume', 0) or 0)
            
            if volume > 0 and last_price > 0:
                total_volume += volume
                total_price_volume += last_price * volume
        
        if total_volume > 0:
            return total_price_volume / total_volume
        else:
            # Fallback: use simple average if no volume data
            prices = [tick.get('last_price', 0) for tick in tick_data if tick.get('last_price', 0) > 0]
            return sum(prices) / len(prices) if prices else 0.0
    
    def _fallback_volume_profile(self, ticks: List[Dict], price_bins: int) -> Dict:
        """Fallback bucket_incremental_volume profile calculation when Polars is not available"""
        if not ticks:
            return {}
        
        prices = [tick.get('last_price', 0) for tick in ticks]
        volumes = [tick.get('bucket_incremental_volume', 0) for tick in ticks]
        
        min_price, max_price = min(prices), max(prices)
        bin_size = (max_price - min_price) / price_bins if max_price != min_price else 1.0
        
        bins = {}
        for last_price, bucket_incremental_volume in zip(prices, volumes):
            if bin_size == 1.0:  # All prices are the same
                bin_idx = 0
            else:
                bin_idx = int((last_price - min_price) / bin_size)
            if bin_idx not in bins:
                bins[bin_idx] = {'total_volume': 0, 'avg_price': 0, 'count': 0}
            bins[bin_idx]['total_volume'] += bucket_incremental_volume
            bins[bin_idx]['avg_price'] += last_price
            bins[bin_idx]['count'] += 1
        
        # Calculate averages
        for bin_idx in bins:
            bins[bin_idx]['avg_price'] /= bins[bin_idx]['count']
        
        return {
            'bins': [bins[i]['avg_price'] for i in sorted(bins.keys())],
            'volumes': [bins[i]['total_volume'] for i in sorted(bins.keys())]
        }
    
    def _fallback_macd(self, prices: List[float], fast_period: int, slow_period: int, signal_period: int) -> Dict:
        """Fallback MACD calculation when Polars is not available"""
        if len(prices) < slow_period:
            return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
        
        ema_fast = self._fallback_ema(prices, fast_period)
        ema_slow = self._fallback_ema(prices, slow_period)
        macd_line = ema_fast - ema_slow
        
        # Simplified signal line calculation
        signal_line = macd_line * 0.9  # Approximation
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': macd_line - signal_line
        }
    
    def _fallback_bollinger_bands(self, prices: List[float], period: int, std_dev: float) -> Dict:
        """Fallback Bollinger Bands calculation when Polars is not available"""
        if len(prices) < period:
            return {'upper': 0.0, 'middle': 0.0, 'lower': 0.0}
        
        recent_prices = prices[-period:]
        middle_band = sum(recent_prices) / len(recent_prices)
        
        # Calculate standard deviation
        variance = sum((p - middle_band) ** 2 for p in recent_prices) / len(recent_prices)
        std = variance ** 0.5
        
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band
        }
    
    
    def _fallback_batch_calculate(self, tick_data: List[Dict], symbol: str) -> Dict:
        """Fallback batch calculation when Polars is not available"""
        if not tick_data:
            return {}
        
        prices = [tick.get('last_price', 0) for tick in tick_data]
        volumes = [tick.get('bucket_incremental_volume', 0) for tick in tick_data]
        highs = [tick.get('high', 0) for tick in tick_data]
        lows = [tick.get('low', 0) for tick in tick_data]
        
        # Get real last_price from Redis if available
        last_price = prices[-1] if prices else 0
        redis_client = getattr(self, 'redis_client', None)
        if redis_client and hasattr(redis_client, 'get_last_price') and symbol:
            redis_price = redis_client.get_last_price(symbol)
            if redis_price and redis_price > 0:
                last_price = redis_price
        
        return {
            'symbol': symbol,  # ✅ FIXED: Include symbol in fallback calculations
            'last_price': last_price,  # ✅ FIXED: Use Redis-fetched last_price
            'timestamp': tick_data[-1].get('exchange_timestamp_ms', tick_data[-1].get('timestamp', 0)) if tick_data else 0,
            # EMA calculations - comprehensive coverage
            'ema_5': self._fallback_ema(prices, 5),
            'ema_10': self._fallback_ema(prices, 10),
            'ema_20': self._fallback_ema(prices, 20),
            'ema_50': self._fallback_ema(prices, 50),
            'ema_100': self._fallback_ema(prices, 100),
            'ema_200': self._fallback_ema(prices, 200),
            'ema_crossover': {
                'ema_5': self._fallback_ema(prices, 5),
                'ema_10': self._fallback_ema(prices, 10),
                'ema_20': self._fallback_ema(prices, 20),
                'ema_50': self._fallback_ema(prices, 50),
                'ema_100': self._fallback_ema(prices, 100),
                'ema_200': self._fallback_ema(prices, 200)
            },
            
            # Other technical indicators
            'rsi': self._fallback_rsi(prices, 14),
            'atr': self._fallback_atr(highs, lows, prices, 14),
            'vwap': self._fallback_vwap(tick_data),
            'macd': self._fallback_macd(prices, 12, 26, 9),
            'bollinger_bands': self._fallback_bollinger_bands(prices, 20, 2.0),
            'volume_profile': self._fallback_volume_profile(tick_data, 20),
            'volume_ratio': self.calculate_volume_ratio(symbol, volumes[-1] if volumes else 0, 
                                                       sum(volumes) / len(volumes) if volumes else 0),
            'price_change': self.calculate_price_change(prices[-1] if prices else 0, 
                                                      prices[-2] if len(prices) > 1 else 0)
        }
    
    def clear_cache(self):
        """Clear calculation cache"""
        self._polars_cache.clear()
        self._cache.clear()  # ✅ SIMPLIFIED: Clear unified cache
        self._symbol_windows.clear()
        self._fresh_calculations.clear()
    
    def get_rolling_window_stats(self) -> Dict:
        """Get statistics about rolling windows"""
        return {
            'total_symbols': len(self._symbol_windows),
            'window_size': self._window_size,
            'symbols_with_data': len([s for s, w in self._symbol_windows.items() if w.height > 0]),
            'average_window_length': sum(w.height for w in self._symbol_windows.values()) / max(len(self._symbol_windows), 1),
            'memory_usage_mb': sum(w.estimated_size() for w in self._symbol_windows.values()) / (1024 * 1024) if POLARS_AVAILABLE else 0
        }
    
    def cleanup_old_windows(self, max_age_minutes: int = 30):
        """Clean up old rolling windows to prevent memory bloat"""
        import time
        current_time = time.time()
        cutoff_time = current_time - (max_age_minutes * 60)
        
        symbols_to_remove = []
        for symbol, window in self._symbol_windows.items():
            if window.height > 0:
                # Get the latest timestamp from the window
                latest_timestamp = window['timestamp'].max()
                # Convert from milliseconds to seconds if needed
                if latest_timestamp > 1e10:  # Likely milliseconds
                    latest_timestamp = latest_timestamp / 1000
                
                if latest_timestamp < cutoff_time:
                    symbols_to_remove.append(symbol)
        
        for symbol in symbols_to_remove:
            del self._symbol_windows[symbol]
        
        if symbols_to_remove:
            logger.info(f"Cleaned up {len(symbols_to_remove)} old rolling windows")
        
        return len(symbols_to_remove)
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'polars_cache_size': len(self._polars_cache),
            'indicator_cache_size': len(self._cache),
            'fresh_calculations_count': len(self._fresh_calculations),
            'polars_available': POLARS_AVAILABLE,
            'pandas_available': PANDAS_AVAILABLE,
            'numpy_available': NUMPY_AVAILABLE
        }
    
    def _enforce_cache_bounds(self):
        """Enforce cache size bounds using LRU eviction"""
        # ✅ SIMPLIFIED: Only enforce bounds on unified cache and Polars cache
        if len(self._polars_cache) > self.max_cache_size:
            # Remove oldest entries (simple LRU)
            excess = len(self._polars_cache) - self.max_cache_size
            keys_to_remove = list(self._polars_cache.keys())[:excess]
            for key in keys_to_remove:
                del self._polars_cache[key]
        
        # Enforce bounds on unified indicator cache
        if len(self._cache) > self.max_cache_size:
            # Remove oldest entries (simple LRU based on timestamp)
            excess = len(self._cache) - self.max_cache_size
            # Sort by timestamp and remove oldest
            sorted_keys = sorted(self._cache.keys(), 
                               key=lambda k: self._cache[k].get('timestamp', 0))
            keys_to_remove = sorted_keys[:excess]
            for key in keys_to_remove:
                del self._cache[key]
    
    def batch_process_symbols(self, symbol_data: Dict[str, List[Dict]], max_ticks_per_symbol: int = 50) -> Dict[str, Dict]:
        """
        ✅ SIMPLIFIED CACHING: Process symbols with intelligent caching.
        Only recalculates when cache expired or data changed significantly.
        
        Args:
            symbol_data: Dict mapping symbol -> list of tick data
            max_ticks_per_symbol: Maximum ticks per symbol to prevent memory bloat
            
        Returns:
            Dict mapping symbol -> calculated indicators
        """
        if not POLARS_AVAILABLE:
            return self._fallback_batch_process_symbols(symbol_data, max_ticks_per_symbol)
        
        # Enforce batch size bounds
        if len(symbol_data) > self.max_batch_size:
            logger.warning(f"Batch size {len(symbol_data)} exceeds max {self.max_batch_size}, truncating")
            symbol_data = dict(list(symbol_data.items())[:self.max_batch_size])
        
        # ✅ SIMPLIFIED CACHING: Track fresh calculations
        self._fresh_calculations.clear()
        current_time = time.time()
        results = {}
        
        # Prepare bounded data for all symbols
        for symbol, ticks in symbol_data.items():
            # Limit ticks per symbol to prevent memory bloat
            limited_ticks = ticks[-max_ticks_per_symbol:] if len(ticks) > max_ticks_per_symbol else ticks
            
            cache_key = f"{symbol}_indicators"
            
            # ✅ SIMPLIFIED CACHING: Check if we should recalculate
            should_recalculate = self._should_recalculate(symbol, current_time, limited_ticks)
            
            if should_recalculate:
                # Calculate fresh indicators
                if len(limited_ticks) < 20:
                    results[symbol] = {}
                    continue
                
                try:
                    # Convert to Polars DataFrame for processing
                    batch_data = []
                    for tick in limited_ticks:
                        batch_data.append({
                            'symbol': symbol,
                            'last_price': tick.get('last_price', 0.0),
                            'zerodha_cumulative_volume': tick.get('zerodha_cumulative_volume', 0.0),
                            'high': tick.get('high', 0.0),
                            'low': tick.get('low', 0.0),
                            'timestamp': tick.get('exchange_timestamp_ms', 0)
                        })
                    
                    if batch_data:
                        df = pl.DataFrame(batch_data)
                        prices = df['last_price'].to_list()
                        volumes = df['zerodha_cumulative_volume'].to_list()
                        highs = df['high'].to_list()
                        lows = df['low'].to_list()
                        
                        tick_data = [
                            {
                                'last_price': p,
                                'zerodha_cumulative_volume': v,
                                'high': h,
                                'low': l,
                                'exchange_timestamp_ms': t
                            } for p, v, h, l, t in zip(prices, volumes, highs, lows, df['timestamp'].to_list())
                        ]
                        
                        indicators = self.batch_calculate_indicators(tick_data, symbol)
                        results[symbol] = indicators
                        
                        # ✅ SIMPLIFIED CACHING: Update cache with fresh calculation
                        self._cache[cache_key] = {
                            'data': indicators,
                            'timestamp': current_time,
                            'data_hash': hash(str(limited_ticks))
                        }
                        self._last_calculation_time[symbol] = current_time
                        self._fresh_calculations.add(symbol)  # Mark as fresh calculation
                        
                except Exception as e:
                    logger.error(f"Error calculating indicators for {symbol}: {e}")
                    results[symbol] = {}
            else:
                # ✅ SIMPLIFIED CACHING: Use cached result
                results[symbol] = self._cache[cache_key]['data']
                logger.debug(f"✅ Using cached indicators for {symbol} (age: {current_time - self._cache[cache_key]['timestamp']:.1f}s)")
        
        return results
    
    def _fallback_batch_process_symbols(self, symbol_data: Dict[str, List[Dict]], max_ticks_per_symbol: int) -> Dict[str, Dict]:
        """Fallback batch processing when Polars is not available"""
        results = {}
        
        for symbol, ticks in symbol_data.items():
            # Limit ticks per symbol
            limited_ticks = ticks[-max_ticks_per_symbol:] if len(ticks) > max_ticks_per_symbol else ticks
            
            if len(limited_ticks) >= 20:
                try:
                    indicators = self.batch_calculate_indicators(limited_ticks, symbol)
                    results[symbol] = indicators
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    results[symbol] = {}
        
        return results
    
    def get_memory_usage(self) -> Dict[str, Any]:
        """Get memory usage statistics"""
        import sys
        
        polars_memory = sum(sys.getsizeof(v) for v in self._polars_cache.values())
        # ✅ SIMPLIFIED: Calculate memory for unified cache
        indicator_cache_memory = sum(
            sys.getsizeof(v) + sys.getsizeof(k) 
            for k, v in self._cache.items()
        )
        
        return {
            'polars_cache_memory_mb': polars_memory / (1024 * 1024),
            'indicator_cache_memory_mb': indicator_cache_memory / (1024 * 1024),
            'total_memory_mb': (polars_memory + indicator_cache_memory) / (1024 * 1024),
            'max_cache_size': self.max_cache_size,
            'max_batch_size': self.max_batch_size,
            'cache_utilization': len(self._cache) / self.max_cache_size if self.max_cache_size > 0 else 0
        }
    
    def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
        """
        Process individual tick with rolling window (compatible with existing scanner)
        This method provides backward compatibility while using optimized calculations
        """
        try:
            # Update rolling window for real-time processing
            self._update_rolling_window(symbol, tick_data)
            
            # Get current window for this symbol
            if symbol not in self._symbol_windows or self._symbol_windows[symbol].height < 20:
                # Fallback to single tick processing if insufficient data
                tick_list = [tick_data]
                return self.batch_calculate_indicators(tick_list, symbol)
            
            # Use rolling window for real-time indicators
            window = self._symbol_windows[symbol]
            return self._calculate_realtime_indicators(symbol, window, tick_data)
            
        except Exception as e:
            logger.error(f"Error processing tick for {symbol}: {e}")
            return {}
    
    def _update_rolling_window(self, symbol: str, tick_data: Dict):
        """Update rolling window for symbol with new tick data"""
        try:
            if not POLARS_AVAILABLE:
                return
            
            # ✅ FIXED: Handle timestamp conversion properly - handle ISO strings and milliseconds
            timestamp_ms = tick_data.get('exchange_timestamp_ms') or tick_data.get('timestamp_ms')
            if timestamp_ms is None:
                # Try to convert ISO string or other timestamp formats
                timestamp_val = tick_data.get('exchange_timestamp') or tick_data.get('timestamp')
                if timestamp_val:
                    try:
                        from config.utils.timestamp_normalizer import TimestampNormalizer
                        timestamp_ms = TimestampNormalizer.to_epoch_ms(timestamp_val)
                    except Exception:
                        timestamp_ms = 0
                else:
                    timestamp_ms = 0
            else:
                # Ensure it's an integer
                try:
                    timestamp_ms = int(timestamp_ms)
                except (TypeError, ValueError):
                    timestamp_ms = 0
                
            # Create new tick DataFrame using exact Zerodha field names with consistent data types
            new_tick = pl.DataFrame([{
                'timestamp': timestamp_ms,
                'last_price': float(tick_data.get('last_price', 0.0)),
                'zerodha_cumulative_volume': int(tick_data.get('zerodha_cumulative_volume', 0)),
                'zerodha_last_traded_quantity': int(tick_data.get('zerodha_last_traded_quantity', 0)),
                'high': float(tick_data.get('high', tick_data.get('last_price', 0.0))),
                'low': float(tick_data.get('low', tick_data.get('last_price', 0.0))),
                'bucket_incremental_volume': int(tick_data.get('bucket_incremental_volume', 0)),
                'bucket_cumulative_volume': int(tick_data.get('bucket_cumulative_volume', 0))
            }])
            
            # Initialize window if needed
            if symbol not in self._symbol_windows:
                # ✅ SINGLE SOURCE OF TRUTH: Initialize with historical bucket data from Redis
                historical_data = self._get_historical_bucket_data(symbol)
                if historical_data is not None and historical_data.height > 0:
                    self._symbol_windows[symbol] = historical_data
                    logger.info(f"🔍 [ROLLING_WINDOW_INIT] {symbol} - Initialized with {historical_data.height} historical buckets")
                else:
                    self._symbol_windows[symbol] = new_tick
                    logger.info(f"🔍 [ROLLING_WINDOW_INIT] {symbol} - Initialized with new tick (no historical data)")
                return
            
            # Concatenate with existing window
            window = pl.concat([self._symbol_windows[symbol], new_tick])
            
            # Keep only last N ticks (rolling window)
            if window.height > self._window_size:
                window = window.tail(self._window_size)
            
            self._symbol_windows[symbol] = window
            
        except Exception as e:
            logger.error(f"Error updating rolling window for {symbol}: {e}")
    
    def _get_historical_bucket_data(self, symbol: str) -> Optional[pl.DataFrame]:
        """Get historical bucket data from Redis for rolling window initialization
        
        ✅ FIXED: Per REDIS_STORAGE_SIGNATURE.md, buckets are stored in session data (DB 0)
        Format: session:{symbol}:{date} -> time_buckets (nested JSON with prices/volumes)
        """
        try:
            redis_client = getattr(self, 'redis_client', None)
            if not redis_client:
                logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - No Redis client available")
                return None
            
            # ✅ FIXED: Get buckets from session data (DB 0) per REDIS_STORAGE_SIGNATURE.md
            # Try RobustRedisClient method first (if available)
            if hasattr(redis_client, 'get_rolling_window_buckets'):
                logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - Using RobustRedisClient.get_rolling_window_buckets")
                try:
                    historical_buckets = redis_client.get_rolling_window_buckets(symbol, lookback_minutes=60, max_days_back=None)
                    if historical_buckets:
                        logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} buckets from RobustRedisClient")
                    else:
                        logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - No buckets from RobustRedisClient, trying session data")
                except Exception as e:
                    logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} - RobustRedisClient method failed: {e}, trying session data")
                    historical_buckets = None
            else:
                historical_buckets = None
            
            # ✅ FIXED: Fallback to session data (DB 0) per REDIS_STORAGE_SIGNATURE.md
            if not historical_buckets:
                logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - Reading buckets from session data (DB 0)")
                historical_buckets = self._get_buckets_from_session_data(redis_client, symbol)
            
            if not historical_buckets:
                logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - No historical buckets returned")
                return None
            
            logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - Got {len(historical_buckets)} buckets, processing...")
            # Convert bucket data to Polars DataFrame
            bucket_data = []
            for bucket in historical_buckets:
                try:
                    # Parse timestamp properly (ISO format: "2025-10-27T10:35:03+05:30")
                    timestamp_str = bucket.get('timestamp', '')
                    if timestamp_str:
                        try:
                            from datetime import datetime
                            # Parse ISO timestamp and convert to milliseconds
                            dt = datetime.fromisoformat(timestamp_str.replace('+05:30', ''))
                            timestamp_ms = int(dt.timestamp() * 1000)
                        except Exception as e:
                            logger.debug(f"Error parsing timestamp {timestamp_str}: {e}")
                            timestamp_ms = 0
                    else:
                        timestamp_ms = 0
                    
                    # ✅ FIXED: Map bucket data from session data structure per REDIS_STORAGE_SIGNATURE.md
                    # Session buckets have: close, bucket_incremental_volume, count, last_timestamp, hour, minute_bucket
                    last_price = bucket.get('close') or bucket.get('last_price', 0.0)
                    bucket_incremental = bucket.get('bucket_incremental_volume', 0)
                    bucket_cumulative = bucket.get('bucket_cumulative_volume') or bucket.get('zerodha_cumulative_volume', 0)
                    
                    # Use last_timestamp if available, otherwise use timestamp field
                    if not timestamp_str:
                        timestamp_str = bucket.get('last_timestamp', '')
                        if timestamp_str:
                            try:
                                dt = datetime.fromisoformat(timestamp_str.replace('+05:30', ''))
                                timestamp_ms = int(dt.timestamp() * 1000)
                            except Exception:
                                timestamp_ms = 0
                    
                    bucket_data.append({
                        'timestamp': timestamp_ms,
                        'last_price': float(last_price),
                        'zerodha_cumulative_volume': int(bucket_cumulative),
                        'zerodha_last_traded_quantity': int(bucket.get('count', 0)),  # Use count as quantity
                        'high': float(last_price),  # Use close/last_price as high/low fallback
                        'low': float(last_price),   # Use close/last_price as low fallback
                        'bucket_incremental_volume': int(bucket_incremental),
                        'bucket_cumulative_volume': int(bucket_cumulative)
                    })
                except Exception as e:
                    logger.debug(f"Error processing bucket data: {e}")
                    continue
            
            if bucket_data:
                logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - Created DataFrame with {len(bucket_data)} rows")
                return pl.DataFrame(bucket_data)
            
            logger.info(f"🔍 [HISTORICAL_DEBUG] {symbol} - No valid bucket data after processing")
            return None
            
        except Exception as e:
            logger.error(f"Error getting historical bucket data for {symbol}: {e}")
            return None
    
    def _get_buckets_from_session_data(self, redis_client, symbol: str, lookback_minutes: int = 60) -> List[Dict[str, Any]]:
        """Get buckets from session data (DB 0) per REDIS_STORAGE_SIGNATURE.md
        
        Format: session:{symbol}:{date} -> time_buckets (nested JSON with prices/volumes)
        """
        try:
            from datetime import datetime, timedelta
            import pytz
            import json
            
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            cutoff_ts = (now - timedelta(minutes=lookback_minutes)).timestamp()
            
            # Try current date and recent dates
            dates_to_try = [
                now.strftime("%Y-%m-%d"),  # Today
                (now - timedelta(days=1)).strftime("%Y-%m-%d"),  # Yesterday
                (now - timedelta(days=2)).strftime("%Y-%m-%d"),  # Day before
            ]
            
            # Also try symbol variants (original, underlying)
            symbols_to_try = [symbol]
            # Try to extract underlying symbol
            underlying_symbol = symbol
            if '25NOV' in symbol or '25DEC' in symbol or '25JAN' in symbol:
                underlying_symbol = symbol.replace('25NOV', '').replace('25DEC', '').replace('25JAN', '')
                underlying_symbol = underlying_symbol.replace('CE', '').replace('PE', '').replace('FUT', '')
                if underlying_symbol and underlying_symbol != symbol:
                    symbols_to_try.append(underlying_symbol)
            
            # Get DB 0 client for session data
            session_client = None
            if hasattr(redis_client, 'get_client'):
                session_client = redis_client.get_client(0)
            elif hasattr(redis_client, 'redis_client'):
                session_client = redis_client.redis_client
                # Ensure we're on DB 0
                if hasattr(session_client, 'select'):
                    session_client.select(0)
            else:
                session_client = redis_client
            
            buckets_data = []
            
            # Try session data first (where buckets are actually stored)
            for try_symbol in symbols_to_try:
                for date_str in dates_to_try:
                    session_key = f"session:{try_symbol}:{date_str}"
                    try:
                        session_data_str = session_client.get(session_key)
                        if session_data_str:
                            if isinstance(session_data_str, bytes):
                                session_data_str = session_data_str.decode('utf-8')
                            session_data = json.loads(session_data_str) if isinstance(session_data_str, str) else session_data_str
                            time_buckets = session_data.get('time_buckets', {})
                            
                            if time_buckets:
                                logger.info(f"✅ [HISTORICAL_DEBUG] {symbol} -> Found {len(time_buckets)} buckets in {session_key}")
                                # Convert time_buckets to list of bucket data
                                for bucket_key, bucket_data in time_buckets.items():
                                    if isinstance(bucket_data, dict):
                                        # Extract timestamp from bucket
                                        last_ts_str = bucket_data.get('last_timestamp', '')
                                        if last_ts_str:
                                            try:
                                                bucket_ts = datetime.fromisoformat(last_ts_str.replace('+05:30', '')).timestamp()
                                                if bucket_ts >= cutoff_ts:
                                                    buckets_data.append(bucket_data)
                                            except Exception:
                                                pass
                                        else:
                                            # Include bucket even without timestamp (within date range)
                                            buckets_data.append(bucket_data)
                                if buckets_data:
                                    break  # Found buckets, no need to check other dates
                    except Exception as e:
                        logger.debug(f"Session lookup failed for {session_key}: {e}")
                        continue
                
                if buckets_data:
                    break  # Found buckets, no need to check other symbols
            
            if buckets_data:
                logger.info(f"✅ [HISTORICAL_DEBUG] {symbol} -> Retrieved {len(buckets_data)} buckets from session data")
                return buckets_data
            
            logger.debug(f"🔍 [HISTORICAL_DEBUG] {symbol} -> No buckets found in session data")
            return []
            
        except Exception as e:
            logger.error(f"Error getting buckets from session data for {symbol}: {e}")
            return []
    
    def _calculate_realtime_indicators(self, symbol: str, window: pl.DataFrame, tick_data: Dict) -> Dict:
        """Calculate real-time indicators using rolling window with Redis fallback"""
        try:
            if window.height < 20:
                return {}
            
            # Extract data from window using canonical field names
            prices = window['last_price'].to_list()
            volumes = window['zerodha_cumulative_volume'].to_list()
            highs = window['high'].to_list()
            lows = window['low'].to_list()
            
            # Get Redis client if available
            redis_client = getattr(self, 'redis_client', None)
            
            # ✅ FAST EMA CALCULATIONS: Use ultra-fast EMA methods for all windows
            # Get real price from Redis if available, fallback to tick_data
            current_price = tick_data.get('last_price', 0.0)
            if redis_client and hasattr(redis_client, 'get_last_price'):
                redis_price = redis_client.get_last_price(symbol)
                logger.debug(f"🔍 REDIS PRICE: {symbol} - tick_price={tick_data.get('last_price', 0)}, redis_price={redis_price}")
                if redis_price and redis_price > 0:
                    current_price = redis_price
            fast_emas = self.get_all_emas_fast(symbol, current_price)
            
            volume_ratio = _extract_volume_ratio(tick_data)
            if (not volume_ratio) and symbol:
                try:
                    volume_ratio = float(self._get_volume_calculator().calculate_volume_ratio(symbol, tick_data))
                except Exception as vr_calc_err:
                    logger.debug(f"Volume calculator fallback in _calculate_realtime_indicators for {symbol}: {vr_calc_err}")
                    volume_ratio = 0.0

            # Calculate indicators using TA-Lib (preferred) with Redis fallback
            indicators = {
                'symbol': symbol,
                'timestamp': tick_data.get('exchange_timestamp_ms', tick_data.get('timestamp', 0)),
                'last_price': current_price,
                'price_change': self.calculate_price_change(
                    current_price,
                    prices[-2] if len(prices) >= 2 else prices[-1]
                ),
                # ✅ FAST EMAs: All windows calculated ultra-fast
                'ema_5': fast_emas['ema_5'],
                'ema_10': fast_emas['ema_10'],
                'ema_20': fast_emas['ema_20'],
                'ema_50': fast_emas['ema_50'],
                'ema_100': fast_emas['ema_100'],
                'ema_200': fast_emas['ema_200'],
                'ema_crossover': {
                    'ema_5': fast_emas['ema_5'],
                    'ema_10': fast_emas['ema_10'],
                    'ema_20': fast_emas['ema_20'],
                    'ema_50': fast_emas['ema_50'],
                    'ema_100': fast_emas['ema_100'],
                    'ema_200': fast_emas['ema_200']
                },
                # ✅ Other indicators
                'rsi': self.calculate_rsi(prices, 14),
                'atr': self.calculate_atr(highs, lows, prices, 14),
                'vwap': calculate_vwap_with_redis_fallback([
                    {'last_price': p, 'zerodha_cumulative_volume': v} 
                    for p, v in zip(prices, volumes)
                ], symbol, redis_client),
                'volume_ratio': volume_ratio
            }
            
            # Add MACD and Bollinger Bands if enough data
            if len(prices) >= 50:
                macd = self.calculate_macd(prices, 12, 26, 9)
                indicators.update(macd)
                
                bb = self.calculate_bollinger_bands(prices, 20, 2.0)
                indicators.update(bb)
            
            # 🎯 ADD GREEK CALCULATIONS FOR F&O OPTIONS
            if self._is_option_symbol(symbol) and GREEK_CALCULATIONS_AVAILABLE:
                try:
                    greeks = greek_calculator.calculate_greeks_for_tick_data(tick_data)
                    if greeks and any(greeks.get(greek, 0) != 0 for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                        indicators.update({
                            'delta': greeks.get('delta', 0.0),
                            'gamma': greeks.get('gamma', 0.0),
                            'theta': greeks.get('theta', 0.0),
                            'vega': greeks.get('vega', 0.0),
                            'rho': greeks.get('rho', 0.0),
                            'dte_years': greeks.get('dte_years', 0.0),
                            'trading_dte': greeks.get('trading_dte', 0),
                            'expiry_series': greeks.get('expiry_series', 'UNKNOWN')
                        })
                except Exception as e:
                    logger.debug(f"Greek calculation failed for {symbol}: {e}")
            
            return indicators
            
        except Exception as e:
            logger.error(f"Error calculating real-time indicators for {symbol}: {e}")
            return {}
    
    def _is_option_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O option with robust parsing"""
        if not symbol:
            return False
        
        symbol_upper = symbol.upper()
        
        # Must start with NFO: (F&O segment)
        if not symbol_upper.startswith('NFO:'):
            return False
        
        # Extract the suffix after NFO:
        suffix = symbol_upper[4:]  # Remove 'NFO:'
        
        # Must end with CE or PE
        if not (suffix.endswith('CE') or suffix.endswith('PE')):
            return False
        
        # Must have digits immediately before CE/PE (strike price)
        import re
        before_ce_pe = suffix[:-2]  # Remove CE/PE
        
        # Must end with digits (strike price)
        if not re.search(r'\d+$', before_ce_pe):
            return False
        
        # Must have proper expiry format (digits + month abbreviation)
        # Pattern: SYMBOL + DDMMM + STRIKE + CE/PE
        # e.g., NIFTY25NOV19500CE
        expiry_pattern = r'\d{2}[A-Z]{3}\d+$'
        if not re.search(expiry_pattern, before_ce_pe):
            return False
        
        return True
    
    def _is_future_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O future with robust parsing"""
        if not symbol:
            return False
        
        symbol_upper = symbol.upper()
        
        # Must start with NFO: (F&O segment)
        if not symbol_upper.startswith('NFO:'):
            return False
        
        # Extract the suffix after NFO:
        suffix = symbol_upper[4:]  # Remove 'NFO:'
        
        # Must end with FUT
        if not suffix.endswith('FUT'):
            return False
        
        # Must have proper expiry format (digits + month abbreviation)
        # Pattern: SYMBOL + DDMMM + FUT
        # e.g., NIFTY25NOVFUT
        before_fut = suffix[:-3]  # Remove FUT
        expiry_pattern = r'\d{2}[A-Z]{3}$'
        if not re.search(expiry_pattern, before_fut):
            return False
        
        return True
    
    def _is_fno_symbol(self, symbol: str) -> bool:
        """Check if symbol is any F&O instrument (option or future)"""
        return self._is_option_symbol(symbol) or self._is_future_symbol(symbol)
    
    def get_tick_processor(self, redis_client=None, config=None):
        """
        Factory function to create TickProcessor-compatible instance
        Provides backward compatibility with existing scanner
        """
        class TickProcessorWrapper:
            def __init__(self, hybrid_calc, redis_client=None, config=None):
                self.hybrid_calc = hybrid_calc
                self.redis_client = redis_client
                self.config = config or {}
                self.debug_enabled = False
                
                # Compatibility attributes
                self.price_history = {}
                self.volume_history = {}
                self.stats = {"ticks_processed": 0, "indicators_computed": 0, "errors": 0}
            
            def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
                """Process individual tick using HybridCalculations"""
                try:
                    indicators = self.hybrid_calc.process_tick(symbol, tick_data)
                    self.stats["ticks_processed"] += 1
                    if indicators:
                        self.stats["indicators_computed"] += 1
                    return indicators
                except Exception as e:
                    self.stats["errors"] += 1
                    logger.error(f"Error in TickProcessorWrapper: {e}")
                    return {}
            
            def get_history_length(self, symbol: str) -> int:
                """Get history length for symbol (compatibility method)"""
                return 50  # Default history length
            
            def get_stats(self) -> Dict:
                """Get processing statistics"""
                return self.stats.copy()
        
        return TickProcessorWrapper(self, redis_client, config)


def get_tick_processor(redis_client=None, config=None):
    """
    Factory function to create TickProcessor-compatible instance
    Provides backward compatibility with existing scanner
    """
    # Create HybridCalculations instance with bounds
    hybrid_calc = HybridCalculations(max_cache_size=500, max_batch_size=174)
    
    # ✅ SINGLE SOURCE OF TRUTH: Pass Redis client to HybridCalculations for rolling window data
    hybrid_calc.redis_client = redis_client
    
    class TickProcessorWrapper:
        def __init__(self, hybrid_calc, redis_client=None, config=None):
            self.hybrid_calc = hybrid_calc
            self.redis_client = redis_client
            self.config = config or {}
            self.debug_enabled = False
            
            # ✅ FIXED: Pass Redis client to HybridCalculations
            self.hybrid_calc.redis_client = redis_client
            
            # Compatibility attributes
            self.price_history = {}
            self.volume_history = {}
            self.stats = {"ticks_processed": 0, "indicators_computed": 0, "errors": 0}
        
        def process_tick(self, symbol: str, tick_data: Dict) -> Dict:
            """Process individual tick using HybridCalculations"""
            try:
                indicators = self.hybrid_calc.process_tick(symbol, tick_data)
                self.stats["ticks_processed"] += 1
                if indicators:
                    self.stats["indicators_computed"] += 1
                return indicators
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"Error in TickProcessorWrapper: {e}")
                return {}
        
        def get_rolling_window_stats(self) -> Dict:
            """Get rolling window statistics"""
            return self.hybrid_calc.get_rolling_window_stats()
        
        def cleanup_old_windows(self, max_age_minutes: int = 30) -> int:
            """Clean up old rolling windows"""
            return self.hybrid_calc.cleanup_old_windows(max_age_minutes)
        
        def get_history_length(self, symbol: str) -> int:
            """Get history length for symbol (compatibility method)"""
            return 50  # Default history length
        
        def get_stats(self) -> Dict:
            """Get processing statistics"""
            return self.stats.copy()
    
    return TickProcessorWrapper(hybrid_calc, redis_client, config)


def safe_format(value, format_str: str = "{:.2f}") -> str:
    """Safely format a value with fallback for None/NaN values"""
    try:
        if value is None or (isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf'))):
            return "N/A"
        return format_str.format(value)
    except (ValueError, TypeError):
        return "N/A"

def get_market_volume_data(symbol: str, redis_client=None) -> dict:
    """Get market bucket_incremental_volume data for a symbol"""
    try:
        if redis_client:
            # Try to get from Redis
            data = redis_client.hgetall(f"market_data:{symbol}")
            if data:
                return {k.decode() if isinstance(k, bytes) else k: 
                       v.decode() if isinstance(v, bytes) else v 
                       for k, v in data.items()}
    except Exception:
        pass
    return {}

def preload_static_data():
    """Preload static data for the system"""
    # This is a placeholder - in a real system, this would load
    # static market data, configuration, etc.
    pass

def get_market_averages_with_fallback(symbol: str, redis_client=None) -> dict:
    """Get market averages with fallback"""
    try:
        if redis_client:
            from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_stats_hash

            stats_key = ohlc_stats_hash(normalize_symbol(symbol))
            data = redis_client.hgetall(stats_key)
            if data:
                result = {}
                for key, value in data.items():
                    if isinstance(key, bytes):
                        key = key.decode()
                    if isinstance(value, bytes):
                        value = value.decode()
                    result[key] = value
                return result
    except Exception:
        pass
    return {}

def normalize_volume(symbol: str, bucket_incremental_volume: float) -> float:
    """Deprecated legacy helper. Delegates to CorrectVolumeCalculator for consistency."""
    if not symbol or bucket_incremental_volume <= 0:
        return 1.0

    try:
        from utils.correct_volume_calculator import CorrectVolumeCalculator
        calculator = CorrectVolumeCalculator()
        tick_payload = {
            "bucket_incremental_volume": bucket_incremental_volume,
            "exchange_timestamp": datetime.now(),
        }
        ratio = calculator.calculate_volume_ratio(symbol, tick_payload)
        return float(ratio) if ratio is not None else 1.0
    except Exception as exc:
        logger.debug(f"normalize_volume fallback for {symbol}: {exc}")
        return 1.0

def get_all_thresholds_for_regime(vix_regime: str) -> dict:
    """Get all thresholds for a VIX regime using centralized config"""
    try:
        from config.thresholds import get_all_thresholds_for_regime as centralized_get_all_thresholds
        return centralized_get_all_thresholds(vix_regime)
    except ImportError:
        # Fallback to hardcoded values if centralized config unavailable
        thresholds = {
            "LOW": {
                "volume_spike": 3.08,  # Updated to match config/thresholds.py (2.2 * 1.4)
                "momentum": 0.7,
                "breakout": 0.4,
                "reversal": 0.5
            },
            "NORMAL": {
                "volume_spike": 2.2,  # Updated to match config/thresholds.py
                "momentum": 0.5,
                "breakout": 0.3,
                "reversal": 0.4
            },
            "HIGH": {
                "volume_spike": 2.86,  # Updated to match config/thresholds.py (2.2 * 1.3)
                "momentum": 0.35,
                "breakout": 0.18,
                "reversal": 0.28
            },
            "PANIC": {
                "volume_spike": 3.96,  # Updated to match config/thresholds.py (2.2 * 1.8)
                "momentum": 0.25,
                "breakout": 0.12,
                "reversal": 0.20
            }
        }
        return thresholds.get(vix_regime, thresholds["NORMAL"])

def _get_or_calculate_volume_ratio(symbol: str, current_volume: float, avg_volume: float, redis_client=None) -> float:
    """Get bucket_incremental_volume ratio from Redis or calculate if not available"""
    try:
        # First try to get from Redis
        redis_ratio = _get_volume_ratio_from_redis(symbol, redis_client)
        if redis_ratio is not None:
            return redis_ratio
        
        # Fallback to calculation
        return calculate_volume_ratio_with_redis_fallback(symbol, current_volume, avg_volume, redis_client)
        
    except Exception as e:
        logger.debug(f"Volume ratio retrieval failed for {symbol}: {e}")
        return calculate_volume_ratio_with_redis_fallback(symbol, current_volume, avg_volume, redis_client)

def _get_volume_ratio_from_redis(symbol: str, redis_client=None) -> Optional[float]:
    """Get pre-calculated bucket_incremental_volume ratio from Redis"""
    try:
        if not redis_client:
            return None
        
        # Try to get from Redis using latest key
        redis_key = f"volume_ratio:{symbol}:latest"
        stored_data = redis_client.get(redis_key)
        
        if stored_data:
            try:
                data = json.loads(stored_data) if isinstance(stored_data, str) else stored_data
                # Check if the data is recent (within 60 seconds)
                stored_time = data.get("timestamp", 0)
                current_time = time.time()
                if current_time - stored_time < 60:  # 60 seconds freshness
                    return float(data.get("volume_ratio", 0.0))
            except (json.JSONDecodeError, KeyError, ValueError):
                pass
        
        return None
        
    except Exception as e:
        logger.debug(f"Redis bucket_incremental_volume ratio retrieval failed for {symbol}: {e}")
        return None

# Log backend availability
if POLARS_AVAILABLE:
    logger.info("✅ Polars loaded successfully for optimized technical analysis")
elif NUMPY_AVAILABLE:
    logger.info("✅ NumPy loaded successfully for technical analysis")
else:
    logger.warning("⚠️ Using fallback methods for technical analysis")


# ============ Redis Fallback Functions ============

def get_indicator_from_redis(symbol: str, indicator_name: str, redis_client=None) -> Optional[float]:
    """Get indicator value from Redis as fallback"""
    try:
        if not redis_client:
            return None
        
        # Try to get from Redis using retrieve_by_data_type
        redis_key = f"indicators:{symbol}:{indicator_name}"
        indicator_data = redis_client.retrieve_by_data_type(redis_key, "analysis_cache")
        
        if indicator_data:
            try:
                # Parse JSON if it's a complex indicator
                if isinstance(indicator_data, str) and indicator_data.startswith('{'):
                    indicator_json = json.loads(indicator_data)
                    return float(indicator_json.get('value', 0.0))
                else:
                    return float(indicator_data)
            except (json.JSONDecodeError, ValueError, TypeError):
                pass
        
        return None
        
    except Exception as e:
        logger.debug(f"Redis indicator retrieval failed for {symbol}:{indicator_name}: {e}")
        return None

def get_volume_ratio_from_redis(symbol: str, redis_client=None) -> Optional[float]:
    """Get bucket_incremental_volume ratio from Redis as fallback"""
    try:
        if not redis_client:
            return None
        
        # Try to get from Redis using retrieve_by_data_type
        redis_key = f"volume_ratio:{symbol}:latest"
        volume_data = redis_client.retrieve_by_data_type(redis_key, "metrics_cache")
        
        if volume_data:
            try:
                volume_json = json.loads(volume_data) if isinstance(volume_data, str) else volume_data
                # Check if the data is recent (within 60 seconds)
                stored_time = volume_json.get("timestamp", 0)
                current_time = time.time()
                if current_time - stored_time < 60:  # 60 seconds freshness
                    return float(volume_json.get("volume_ratio", 0.0))
            except (json.JSONDecodeError, KeyError, ValueError):
                pass
        
        return None
        
    except Exception as e:
        logger.debug(f"Redis bucket_incremental_volume ratio retrieval failed for {symbol}: {e}")
        return None

def calculate_rsi_with_redis_fallback(prices: List[float], period: int = 14, symbol: str = None, redis_client=None) -> float:
    """Calculate RSI with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_rsi = get_indicator_from_redis(symbol, "rsi", redis_client)
            if redis_rsi is not None:
                return redis_rsi
        
        # Fallback to calculation
        if not POLARS_AVAILABLE:
            return calculate_rsi_with_redis_fallback(prices, period, symbol, redis_client)
            
        if len(prices) < period + 1:
            return 50.0
        
        df = pl.DataFrame({'last_price': prices})
        
        # RSI calculation in Polars
        rsi = df.with_columns([
            pl.col('last_price').diff().alias('delta')
        ]).with_columns([
            pl.when(pl.col('delta') > 0)
              .then(pl.col('delta'))
              .otherwise(0)
              .alias('gain'),
            pl.when(pl.col('delta') < 0) 
              .then(-pl.col('delta'))
              .otherwise(0)
              .alias('loss')
        ]).with_columns([
            pl.col('gain').rolling_mean(period).alias('avg_gain'),
            pl.col('loss').rolling_mean(period).alias('avg_loss')
        ]).with_columns([
            (100 - (100 / (1 + (pl.col('avg_gain') / pl.col('avg_loss'))))).alias('rsi')
        ])
        
        return float(rsi['rsi'].tail(1)[0]) if rsi['rsi'].tail(1)[0] is not None else 50.0
        
    except Exception as e:
        logger.debug(f"RSI calculation failed for {symbol}: {e}")
        return 50.0

def calculate_atr_with_redis_fallback(highs: List[float], lows: List[float], closes: List[float], 
                                    period: int = 14, symbol: str = None, redis_client=None) -> float:
    """Calculate ATR with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_atr = get_indicator_from_redis(symbol, "atr", redis_client)
            if redis_atr is not None:
                return redis_atr
        
        # Fallback to calculation
        if not POLARS_AVAILABLE:
            return calculate_atr_with_redis_fallback(highs, lows, closes, period, symbol, redis_client)
            
        if len(highs) < period + 1:
            return 0.0
        
        # Use Polars for rolling window calculations
        df = pl.DataFrame({
            'high': highs,
            'low': lows, 
            'close': closes
        })
        
        # True Range calculation in Polars
        df = df.with_columns([
            (pl.col('high') - pl.col('low')).alias('tr1'),
            (pl.col('high') - pl.col('close').shift(1)).abs().alias('tr2'),
            (pl.col('low') - pl.col('close').shift(1)).abs().alias('tr3')
        ]).with_columns([
            pl.max_horizontal('tr1', 'tr2', 'tr3').alias('tr')
        ])
        
        # ATR as rolling mean
        atr = df['tr'].tail(period).mean()
        return float(atr) if atr is not None else 0.0
        
    except Exception as e:
        logger.debug(f"ATR calculation failed for {symbol}: {e}")
        return 0.0

def calculate_ema_with_redis_fallback(prices: List[float], period: int = 20, symbol: str = None, redis_client=None) -> float:
    """Calculate EMA with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_ema = get_indicator_from_redis(symbol, f"ema_{period}", redis_client)
            if redis_ema is not None:
                return redis_ema
        
        # Fallback to calculation
        if not POLARS_AVAILABLE:
            return calculate_ema_with_redis_fallback(prices, period, symbol, redis_client)
            
        if len(prices) < period:
            return float(prices[-1]) if prices else 0.0
        
        series = pl.Series('last_price', prices)
        # Polars exponential moving average
        ema = series.ewm_mean(span=period).tail(1)[0]
        return float(ema)
        
    except Exception as e:
        logger.debug(f"EMA calculation failed for {symbol}: {e}")
        return float(prices[-1]) if prices else 0.0

def calculate_vwap_with_redis_fallback(tick_data: List[Dict], symbol: str = None, redis_client=None) -> float:
    """Calculate VWAP with Redis fallback using TA-Lib (preferred)"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_vwap = get_indicator_from_redis(symbol, "vwap", redis_client)
            if redis_vwap is not None:
                return redis_vwap
        
        # Try TA-Lib first (fastest and most accurate) using price/volume weighting
        if TALIB_AVAILABLE and len(tick_data) >= 20:
            try:
                prices: List[float] = []
                volumes: List[float] = []

                for tick in tick_data:
                    price = tick.get("last_price", 0.0)
                    volume = (
                        tick.get("zerodha_cumulative_volume", 0)
                        or tick.get("bucket_incremental_volume", 0)
                        or tick.get("bucket_cumulative_volume", 0)
                        or 0
                    )

                    if price > 0 and volume > 0:
                        prices.append(float(price))
                        volumes.append(float(volume))

                if prices and volumes:
                    weighted_sum = sum(p * v for p, v in zip(prices, volumes))
                    total_volume = sum(volumes)
                    if total_volume > 0:
                        return weighted_sum / total_volume
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("TA-Lib VWAP fallback failed for %s: %s", symbol, exc)

        # Fallback to Polars
        if POLARS_AVAILABLE:
            try:
                df = pl.DataFrame(tick_data)
                
                # VWAP calculation using available volume fields
                volume_col = None
                for col_name in ['zerodha_cumulative_volume', 'bucket_incremental_volume', 'bucket_cumulative_volume']:
                    if col_name in df.columns and df[col_name].sum() > 0:
                        volume_col = col_name
                        break
                
                if volume_col:
                    vwap = df.select([
                        (pl.col('last_price') * pl.col(volume_col)).sum() / 
                        pl.col(volume_col).sum()
                    ]).row(0)[0]
                else:
                    # Fallback: use simple average if no volume data
                    vwap = df['last_price'].mean()
                
                return float(vwap) if vwap is not None else 0.0
            except Exception as e:
                logger.warning(f"Polars VWAP failed for {symbol}, using simple fallback: {e}")
        
        # Simple fallback
        return calculate_vwap_with_redis_fallback(tick_data, symbol, redis_client)
        
    except Exception as e:
        logger.debug(f"VWAP calculation failed for {symbol}: {e}")
        return 0.0

def calculate_macd_with_redis_fallback(prices: List[float], fast_period: int = 12, 
                                     slow_period: int = 26, signal_period: int = 9,
                                     symbol: str = None, redis_client=None) -> Dict:
    """Calculate MACD with Redis fallback"""
    try:
        # First try Redis fallback
        if symbol and redis_client:
            redis_macd = get_indicator_from_redis(symbol, "macd", redis_client)
            if redis_macd is not None:
                if isinstance(redis_macd, dict):
                    return redis_macd
                else:
                    return {'macd': redis_macd, 'signal': 0.0, 'histogram': 0.0}
        
        # Fallback to calculation
        if not POLARS_AVAILABLE:
            return calculate_macd_with_redis_fallback(prices, fast_period, slow_period, signal_period, symbol, redis_client)
            
        if len(prices) < slow_period:
            return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}
        
        df = pl.DataFrame({'last_price': prices})
        
        # Calculate EMAs
        ema_fast = df['last_price'].ewm_mean(span=fast_period)
        ema_slow = df['last_price'].ewm_mean(span=slow_period)
        
        # MACD line
        macd_line = ema_fast - ema_slow
        
        # Signal line (EMA of MACD)
        signal_line = macd_line.ewm_mean(span=signal_period)
        
        # Histogram
        histogram = macd_line - signal_line
        
        return {
            'macd': float(macd_line.tail(1)[0]) if macd_line.tail(1)[0] is not None else 0.0,
            'signal': float(signal_line.tail(1)[0]) if signal_line.tail(1)[0] is not None else 0.0,
            'histogram': float(histogram.tail(1)[0]) if histogram.tail(1)[0] is not None else 0.0
        }
        
    except Exception as e:
        logger.debug(f"MACD calculation failed for {symbol}: {e}")
        return {'macd': 0.0, 'signal': 0.0, 'histogram': 0.0}

def calculate_volume_ratio_with_redis_fallback(symbol: str, current_volume: float, 
                                             avg_volume: float, redis_client=None) -> float:
    """
    Legacy wrapper retained for backward compatibility.
    Delegates to CorrectVolumeCalculator so calculations honor the centralized baseline.
    """
    if not symbol:
        return 1.0

    try:
        from utils.correct_volume_calculator import CorrectVolumeCalculator
        calculator = CorrectVolumeCalculator(redis_client)
        tick_payload = {
            "bucket_incremental_volume": current_volume,
            "exchange_timestamp": datetime.now(),
        }
        ratio = calculator.calculate_volume_ratio(symbol, tick_payload)
        return float(ratio) if ratio is not None else 1.0
    except Exception as exc:
        logger.debug(f"Centralized volume ratio fallback failed for {symbol}: {exc}")
        return 1.0


def calculate_straddle_metrics(ce_data: Dict, pe_data: Dict, underlying_price: float) -> Dict[str, float]:
    """
    Calculate straddle-specific metrics for VWAP Straddle Strategy
    
    Args:
        ce_data: Call option data (price, volume, etc.)
        pe_data: Put option data (price, volume, etc.)
        underlying_price: Current underlying price
        
    Returns:
        Dictionary with straddle metrics
    """
    try:
        ce_price = ce_data.get('last_price', 0.0)
        pe_price = pe_data.get('last_price', 0.0)
        ce_volume = ce_data.get('volume', 0.0)
        pe_volume = pe_data.get('volume', 0.0)
        
        # Combined premium
        combined_premium = ce_price + pe_price
        
        # Combined volume
        combined_volume = ce_volume + pe_volume
        
        # Volume-weighted average premium
        if combined_volume > 0:
            vwap_premium = ((ce_price * ce_volume) + (pe_price * pe_volume)) / combined_volume
        else:
            vwap_premium = combined_premium
        
        # Premium ratio (current vs VWAP)
        premium_ratio = combined_premium / vwap_premium if vwap_premium > 0 else 1.0
        
        # Volume ratio (combined vs average)
        avg_volume = (ce_data.get('avg_volume', 0) + pe_data.get('avg_volume', 0)) / 2
        volume_ratio = combined_volume / avg_volume if avg_volume > 0 else 1.0
        
        # Strike analysis
        ce_strike = ce_data.get('strike', 0)
        pe_strike = pe_data.get('strike', 0)
        atm_strike = (ce_strike + pe_strike) / 2 if ce_strike > 0 and pe_strike > 0 else underlying_price
        
        # Moneyness
        moneyness = underlying_price / atm_strike if atm_strike > 0 else 1.0
        
        return {
            'combined_premium': combined_premium,
            'combined_volume': combined_volume,
            'vwap_premium': vwap_premium,
            'premium_ratio': premium_ratio,
            'volume_ratio': volume_ratio,
            'ce_price': ce_price,
            'pe_price': pe_price,
            'ce_volume': ce_volume,
            'pe_volume': pe_volume,
            'atm_strike': atm_strike,
            'moneyness': moneyness,
            'underlying_price': underlying_price
        }
        
    except Exception as e:
        logger.error(f"Straddle metrics calculation failed: {e}")
        return {
            'combined_premium': 0.0,
            'combined_volume': 0.0,
            'vwap_premium': 0.0,
            'premium_ratio': 1.0,
            'volume_ratio': 1.0,
            'ce_price': 0.0,
            'pe_price': 0.0,
            'ce_volume': 0.0,
            'pe_volume': 0.0,
            'atm_strike': underlying_price,
            'moneyness': 1.0,
            'underlying_price': underlying_price
        }


def calculate_straddle_vwap(underlying_prices: List[Dict], period: int = 20) -> float:
    """
    Calculate VWAP for straddle strategy using underlying price data
    
    Args:
        underlying_prices: List of price data with timestamp, price, volume
        period: VWAP calculation period
        
    Returns:
        VWAP value
    """
    try:
        if not underlying_prices or len(underlying_prices) < 2:
            return 0.0
        
        # Take last N periods
        recent_prices = underlying_prices[-period:] if len(underlying_prices) >= period else underlying_prices
        
        total_volume = sum(price_data.get('volume', 0) for price_data in recent_prices)
        if total_volume == 0:
            return recent_prices[-1].get('price', 0.0)
        
        # Calculate volume-weighted average price
        vwap = sum(price_data.get('price', 0) * price_data.get('volume', 0) 
                  for price_data in recent_prices) / total_volume
        
        return vwap
        
    except Exception as e:
        logger.error(f"Straddle VWAP calculation failed: {e}")
        return 0.0


def calculate_straddle_profit_loss(entry_premium: float, current_premium: float, 
                                 position_size: int) -> Dict[str, float]:
    """
    Calculate profit/loss for straddle position
    
    Args:
        entry_premium: Entry combined premium
        current_premium: Current combined premium
        position_size: Position size (number of lots)
        
    Returns:
        Dictionary with P&L metrics
    """
    try:
        # Premium change
        premium_change = current_premium - entry_premium
        
        # P&L calculation (premium decreases = profit for seller)
        pnl = -premium_change * position_size * 50  # 50 is lot size for NIFTY options
        
        # P&L percentage
        pnl_percentage = (premium_change / entry_premium) * 100 if entry_premium > 0 else 0
        
        # Profit target (50% of premium)
        profit_target = entry_premium * 0.5
        profit_target_pnl = profit_target * position_size * 50
        
        # Stop loss (100% of premium)
        stop_loss = entry_premium * 1.0
        stop_loss_pnl = -stop_loss * position_size * 50
        
        return {
            'premium_change': premium_change,
            'pnl': pnl,
            'pnl_percentage': pnl_percentage,
            'profit_target': profit_target,
            'profit_target_pnl': profit_target_pnl,
            'stop_loss': stop_loss,
            'stop_loss_pnl': stop_loss_pnl,
            'position_size': position_size,
            'entry_premium': entry_premium,
            'current_premium': current_premium
        }
        
    except Exception as e:
        logger.error(f"Straddle P&L calculation failed: {e}")
        return {
            'premium_change': 0.0,
            'pnl': 0.0,
            'pnl_percentage': 0.0,
            'profit_target': 0.0,
            'profit_target_pnl': 0.0,
            'stop_loss': 0.0,
            'stop_loss_pnl': 0.0,
            'position_size': position_size,
            'entry_premium': entry_premium,
            'current_premium': current_premium
        }


# ============ Enhanced Greek Calculations ============

class ExpiryCalculator:
    """Exchange-aware expiry calculations"""
    
    def __init__(self, exchange='NSE'):
        self.exchange = exchange
        if GREEK_CALCULATIONS_AVAILABLE:
            try:
                self.calendar = mcal.get_calendar('NSE')  # For NSE India
            except Exception:
                self.calendar = None
        else:
            self.calendar = None
    
    @staticmethod
    def parse_expiry_from_symbol(symbol: str) -> datetime:
        """Parse expiry date from option symbol"""
        try:
            # Example: "NIFTY25DEC23CE18000" -> extract "25DEC23"
            # Different symbol formats
            if 'NIFTY' in symbol or 'BANKNIFTY' in symbol:
                # Extract date part (assumes standard NSE format)
                import re
                match = re.search(r'(NIFTY|BANKNIFTY)(\d{1,2}[A-Z]{3}\d{2,4})', symbol)
                if match:
                    date_str = match.group(2)
                    return datetime.strptime(date_str, '%d%b%y')
        except Exception:
            pass
        return None
    
    def get_next_expiry(self, current_date: datetime = None) -> datetime:
        """Get next Thursday expiry date considering trading holidays"""
        if current_date is None:
            current_date = datetime.now()
        
        # Start from current date and find next Thursday
        days_ahead = 3 - current_date.weekday()  # 3 = Thursday
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        potential_expiry = current_date + timedelta(days=days_ahead)
        
        # Check if it's a trading day, if not find next trading day
        while not self.is_trading_day(potential_expiry):
            potential_expiry += timedelta(days=1)
        
        return potential_expiry
    
    def is_trading_day(self, date: datetime) -> bool:
        """Check if date is a trading day"""
        if not self.calendar:
            return True  # Fallback to assuming all days are trading days
        
        try:
            schedule = self.calendar.schedule(
                start_date=date.date(), 
                end_date=date.date()
            )
            return len(schedule) > 0
        except Exception:
            return True  # Fallback to assuming it's a trading day
    
    def calculate_dte(self, expiry_date: datetime = None, symbol: str = None, 
                     current_date: datetime = None) -> float:
        """Calculate days to expiry with multiple input methods"""
        if current_date is None:
            current_date = datetime.now()
        
        # Get expiry date from symbol if provided
        if expiry_date is None and symbol:
            expiry_date = self.parse_expiry_from_symbol(symbol)
        
        # If still no expiry, use next weekly expiry
        if expiry_date is None:
            expiry_date = self.get_next_expiry(current_date)
        
        # Calculate trading days only
        trading_days = self.calculate_trading_dte(expiry_date, current_date)
        calendar_days = max(0.0, (expiry_date - current_date).total_seconds() / (24 * 60 * 60))
        
        return {
            'calendar_dte': calendar_days,
            'trading_dte': trading_days,
            'expiry_date': expiry_date,
            'is_weekly': self.is_weekly_expiry(expiry_date)
        }
    
    def calculate_trading_dte(self, expiry_date: datetime, current_date: datetime = None) -> int:
        """Calculate trading days to expiry (more accurate for options)"""
        if current_date is None:
            current_date = datetime.now()
        
        if not self.calendar:
            # Fallback to calendar days
            return max(0, (expiry_date - current_date).days)
        
        try:
            # Get trading days between current date and expiry
            schedule = self.calendar.schedule(
                start_date=current_date.date(),
                end_date=expiry_date.date()
            )
            return len(schedule) - 1  # Exclude current day
        except Exception:
            # Fallback to calendar days
            return max(0, (expiry_date - current_date).days)
    
    def is_weekly_expiry(self, expiry_date: datetime) -> bool:
        """Check if expiry is weekly (Thursday) considering market holidays"""
        # Weekly expiries are typically Thursdays
        if expiry_date.weekday() != 3:  # Not Thursday
            return False
        
        # Additional logic to distinguish weekly vs monthly
        # Monthly expiries are typically last Thursday of month
        next_week = expiry_date + timedelta(days=7)
        return next_week.month == expiry_date.month  # If next week is same month, it's weekly
    
    def get_expiry_series(self, expiry_date: datetime) -> str:
        """Get expiry series (WEEKLY, MONTHLY)"""
        if self.is_weekly_expiry(expiry_date):
            return "WEEKLY"
        return "MONTHLY"
    
    def get_all_upcoming_expiries(self, days_lookahead: int = 30) -> list:
        """Get all upcoming expiry dates"""
        if not self.calendar:
            return []
        
        current_date = datetime.now()
        end_date = current_date + timedelta(days=days_lookahead)
        
        try:
            schedule = self.calendar.schedule(
                start_date=current_date.date(),
                end_date=end_date.date()
            )
            
            # Filter Thursdays (typical expiry days)
            expiries = []
            for date in schedule.index:
                if date.weekday() == 3:  # Thursday
                    expiries.append(date)
            
            return expiries
        except Exception:
            return []


class EnhancedGreekCalculator:
    """Greek calculations with proper DTE awareness"""
    
    def __init__(self, exchange='NSE'):
        self.expiry_calc = ExpiryCalculator(exchange)
    
    def calculate_greeks_with_dte(self, spot_price: float, strike_price: float, 
                                 expiry_date: datetime = None, symbol: str = None,
                                 risk_free_rate: float = 0.05, volatility: float = 0.2,
                                 option_type: str = 'call') -> dict:
        """Calculate Greeks with proper DTE handling"""
        
        if not GREEK_CALCULATIONS_AVAILABLE:
            return self._zero_greeks()
        
        # Calculate proper DTE
        dte_info = self.expiry_calc.calculate_dte(expiry_date, symbol)
        calendar_dte = dte_info['calendar_dte']
        trading_dte = dte_info['trading_dte']
        
        # Use trading DTE for more accurate calculations
        effective_dte = trading_dte / 365.0
        
        greeks = self.black_scholes_greeks(
            spot_price, strike_price, effective_dte, 
            risk_free_rate, volatility, option_type
        )
        
        return {
            **greeks,
            'calendar_dte': calendar_dte,
            'trading_dte': trading_dte,
            'expiry_date': dte_info['expiry_date'],
            'expiry_series': dte_info.get('expiry_series', 'UNKNOWN')
        }
    
    def black_scholes_greeks(self, spot: float, strike: float, dte_years: float,
                            risk_free: float, volatility: float, option_type: str) -> dict:
        """Black-Scholes Greek calculations using py_vollib (standardized library)"""
        if dte_years <= 0 or volatility <= 0:
            return self._zero_greeks()
        
        try:
            # Use py_vollib for standardized, tested Greek calculations
            if PY_VOLLIB_AVAILABLE:
                # py_vollib uses 'c' for call, 'p' for put
                flag = 'c' if option_type.lower() in ['call', 'ce'] else 'p'
                
                return {
                    'delta': delta(flag, spot, strike, dte_years, risk_free, volatility),
                    'gamma': gamma(flag, spot, strike, dte_years, risk_free, volatility),
                    'theta': theta(flag, spot, strike, dte_years, risk_free, volatility),
                    'vega': vega(flag, spot, strike, dte_years, risk_free, volatility),
                    'rho': rho(flag, spot, strike, dte_years, risk_free, volatility),
                    'dte_years': dte_years
                }
            else:
                # Fallback to manual calculation if py_vollib not available
                from math import log, sqrt, exp
                
                d1 = (log(spot / strike) + (risk_free + 0.5 * volatility**2) * dte_years) / (volatility * sqrt(dte_years))
                d2 = d1 - volatility * sqrt(dte_years)
                
                if option_type.lower() in ['call', 'ce']:
                    delta_val = norm.cdf(d1)
                    theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years)) 
                            - risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(d2)) / 365
                else:  # put
                    delta_val = norm.cdf(d1) - 1
                    theta_val = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(dte_years)) 
                            + risk_free * strike * exp(-risk_free * dte_years) * norm.cdf(-d2)) / 365
                
                gamma_val = norm.pdf(d1) / (spot * volatility * sqrt(dte_years))
                vega_val = spot * norm.pdf(d1) * sqrt(dte_years) / 100
                rho_val = (strike * dte_years * exp(-risk_free * dte_years) * 
                      (norm.cdf(d2) if option_type.lower() in ['call', 'ce'] else -norm.cdf(-d2))) / 100
                
                return {
                    'delta': delta_val,
                    'gamma': gamma_val,
                    'theta': theta_val,
                    'vega': vega_val,
                    'rho': rho_val,
                    'dte_years': dte_years
                }
            
        except Exception as e:
            logger.error(f"Greek calculation error: {e}")
            return self._zero_greeks()
    
    def _zero_greeks(self) -> dict:
        """Return zero greeks for error cases"""
        return {
            'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 
            'vega': 0.0, 'rho': 0.0, 'dte_years': 0.0
        }
    
    def calculate_greeks_for_tick_data(self, tick_data: dict, risk_free_rate: float = 0.05, 
                                     volatility: float = 0.2) -> dict:
        """Calculate Greeks from tick data using field mapping"""
        try:
            # Extract data using field mapping
            from utils.yaml_field_loader import resolve_session_field
            
            spot_price = tick_data.get('underlying_price', 0.0)
            strike_price = tick_data.get('strike_price', 0.0)
            option_type = tick_data.get('option_type', 'call')
            symbol = tick_data.get('symbol', '')
            expiry_date = tick_data.get('expiry_date')
            
            if spot_price <= 0 or strike_price <= 0:
                return self._zero_greeks()
            
            # Convert expiry_date if it's a string
            if isinstance(expiry_date, str):
                try:
                    expiry_date = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
                except Exception:
                    expiry_date = None
            
            return self.calculate_greeks_with_dte(
                spot_price=spot_price,
                strike_price=strike_price,
                expiry_date=expiry_date,
                symbol=symbol,
                risk_free_rate=risk_free_rate,
                volatility=volatility,
                option_type=option_type
            )
            
        except Exception as e:
            logger.error(f"Greek calculation from tick data failed: {e}")
            return self._zero_greeks()


# Global instances for easy access
expiry_calculator = ExpiryCalculator('NSE')
greek_calculator = EnhancedGreekCalculator('NSE')
