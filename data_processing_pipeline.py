"""
Data Processing Pipeline: Raw Data → Feature Engineering Layer
Processes raw tick data and computes technical indicators, option Greeks, and news sentiment
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import logging

import duckdb
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from db_config_helper import get_optimized_connection

# Try to import TA-Lib for technical indicators
try:
    import talib
    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False
    logging.warning("TA-Lib not available, using fallback calculations")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessingPipeline:
    """Pipeline to process raw data into feature engineering layer"""
    
    def __init__(self, db_path: str = "new_tick_data.db"):
        """
        Initialize pipeline with optimized DuckDB connection
        
        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path
        self.conn = get_optimized_connection(db_path)
        logger.info("DataProcessingPipeline initialized")
    
    def process_raw_to_features(
        self, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        instrument_tokens: Optional[List[int]] = None,
        time_buckets: List[str] = ['1min', '5min', '15min']
    ) -> Dict[str, int]:
        """
        Process raw tick data to feature engineering layer
        
        Args:
            start_date: Start date (YYYY-MM-DD), None for all data
            end_date: End date (YYYY-MM-DD), None for all data
            instrument_tokens: List of instrument tokens to process, None for all
            time_buckets: List of time buckets to aggregate ('1min', '5min', '15min')
            
        Returns:
            Dictionary with counts of records processed
        """
        logger.info("Starting raw_to_features processing...")
        
        results = {
            'minute_features': 0,
            'option_greeks': 0,
            'errors': 0
        }
        
        try:
            # 1. Read raw tick data
            raw_data = self._read_raw_tick_data(start_date, end_date, instrument_tokens)
            
            if raw_data.empty:
                logger.warning("No raw data found to process")
                return results
            
            logger.info(f"Processing {len(raw_data):,} raw tick records")
            
            # 2. Process each time bucket
            for time_bucket in time_buckets:
                logger.info(f"Processing {time_bucket} aggregation...")
                
                # Aggregate to minute bars
                minute_data = self._aggregate_to_minutes(raw_data, time_bucket)
                
                # 3. Calculate technical indicators
                features = self._calculate_technical_indicators(minute_data)
                
                # 4. Calculate order book features
                features = self._calculate_orderbook_features(features, raw_data, time_bucket)
                
                # 5. Calculate volatility features
                features = self._calculate_volatility_features(features)
                
                # 6. Write to minute_features table
                count = self._write_minute_features(features, time_bucket)
                results['minute_features'] += count
                logger.info(f"  ✓ Wrote {count:,} {time_bucket} features")
            
            # 7. Calculate option Greeks (if applicable)
            if instrument_tokens:
                greeks_count = self._calculate_option_greeks(instrument_tokens, start_date, end_date)
                results['option_greeks'] = greeks_count
                logger.info(f"  ✓ Calculated Greeks for {greeks_count:,} records")
            
            logger.info("✅ Raw to features processing complete")
            
        except Exception as e:
            logger.error(f"Error in process_raw_to_features: {e}", exc_info=True)
            results['errors'] += 1
        
        return results
    
    def incremental_update(
        self,
        last_processed_timestamp_ns: Optional[int] = None,
        time_buckets: List[str] = ['1min', '5min', '15min']
    ) -> Dict[str, int]:
        """
        Process only new data since last update (incremental update)
        
        Args:
            last_processed_timestamp_ns: Last processed timestamp in nanoseconds
                                        If None, uses max timestamp from minute_features
            time_buckets: List of time buckets to process
        
        Returns:
            Dictionary with counts of records processed
        """
        logger.info("Starting incremental update...")
        
        # Get last processed timestamp if not provided
        if last_processed_timestamp_ns is None:
            try:
                result = self.conn.execute("""
                    SELECT MAX(timestamp_ns) 
                    FROM minute_features
                """).fetchone()
                last_processed_timestamp_ns = result[0] if result[0] else 0
            except:
                last_processed_timestamp_ns = 0
        
        logger.info(f"Processing data after timestamp_ns: {last_processed_timestamp_ns:,}")
        
        # Get date range for new data
        if last_processed_timestamp_ns > 0:
            last_dt = datetime.fromtimestamp(last_processed_timestamp_ns / 1e9)
            start_date = last_dt.strftime('%Y-%m-%d')
        else:
            start_date = None
        
        # Process only new data
        results = self.process_raw_to_features(
            start_date=start_date,
            instrument_tokens=None,
            time_buckets=time_buckets
        )
        
        logger.info("✅ Incremental update complete")
        return results
    
    def _read_raw_tick_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        instrument_tokens: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """Read raw tick data from database"""
        query = "SELECT * FROM raw_tick_data WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        if instrument_tokens:
            placeholders = ','.join(['?' for _ in instrument_tokens])
            query += f" AND instrument_token IN ({placeholders})"
            params.extend(instrument_tokens)
        
        query += " ORDER BY instrument_token, exchange_timestamp_ns"
        
        df = self.conn.execute(query, params).df()
        return df
    
    def _aggregate_to_minutes(
        self, 
        raw_data: pd.DataFrame, 
        time_bucket: str
    ) -> pd.DataFrame:
        """Aggregate raw tick data to minute bars"""
        
        # Parse time bucket (e.g., '5min' -> 5 minutes)
        minutes = int(time_bucket.replace('min', ''))
        bucket_seconds = minutes * 60
        bucket_ns = bucket_seconds * 1_000_000_000
        
        # Round timestamp_ns to bucket
        raw_data['bucket_timestamp_ns'] = (
            (raw_data['exchange_timestamp_ns'] // bucket_ns) * bucket_ns
        )
        
        # Group by instrument and bucket
        agg_data = raw_data.groupby(['instrument_token', 'bucket_timestamp_ns']).agg({
            'last_price': ['first', 'max', 'min', 'last'],  # OHLC
            'volume': 'sum',
            'bid_price1': 'mean',
            'ask_price1': 'mean',
            'exchange': 'first',
            'date': 'first',
        }).reset_index()
        
        # Flatten column names
        agg_data.columns = [
            'instrument_token',
            'timestamp_ns',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'volume',
            'avg_bid_price1',
            'avg_ask_price1',
            'exchange',
            'date'
        ]
        
        # Calculate VWAP (simplified - using average bid/ask)
        if len(raw_data) > 0:
            vwap_data = raw_data.groupby(['instrument_token', 'bucket_timestamp_ns']).apply(
                lambda x: (x['last_price'] * x['volume']).sum() / x['volume'].sum() 
                if x['volume'].sum() > 0 else x['last_price'].iloc[-1]
            ).reset_index(name='vwap')
            agg_data = agg_data.merge(vwap_data, on=['instrument_token', 'timestamp_ns'], how='left')
        else:
            agg_data['vwap'] = agg_data['close_price']
        
        return agg_data
    
    def _calculate_technical_indicators(self, minute_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators using TA-Lib or fallback methods"""
        
        features = minute_data.copy()
        
        # Sort by timestamp for rolling calculations
        features = features.sort_values(['instrument_token', 'timestamp_ns'])
        
        # Group by instrument
        for instrument_token in features['instrument_token'].unique():
            mask = features['instrument_token'] == instrument_token
            instrument_data = features[mask].copy()
            prices = instrument_data['close_price'].values
            
            if len(prices) < 20:  # Need minimum data
                continue
            
            idx = features[mask].index
            
            # RSI(14)
            if TALIB_AVAILABLE:
                rsi = talib.RSI(prices, timeperiod=14)
            else:
                rsi = self._calculate_rsi_fallback(prices, 14)
            features.loc[idx, 'rsi_14'] = rsi
            
            # SMA(20)
            if TALIB_AVAILABLE:
                sma = talib.SMA(prices, timeperiod=20)
            else:
                sma = pd.Series(prices).rolling(window=20).mean().values
            features.loc[idx, 'sma_20'] = sma
            
            # EMA(12)
            if TALIB_AVAILABLE:
                ema = talib.EMA(prices, timeperiod=12)
            else:
                ema = pd.Series(prices).ewm(span=12, adjust=False).mean().values
            features.loc[idx, 'ema_12'] = ema
            
            # Bollinger Bands
            if TALIB_AVAILABLE:
                upper, middle, lower = talib.BBANDS(prices, timeperiod=20, nbdevup=2, nbdevdn=2)
                features.loc[idx, 'bollinger_upper'] = upper
                features.loc[idx, 'bollinger_lower'] = lower
            else:
                rolling_mean = pd.Series(prices).rolling(window=20).mean()
                rolling_std = pd.Series(prices).rolling(window=20).std()
                features.loc[idx, 'bollinger_upper'] = (rolling_mean + 2 * rolling_std).values
                features.loc[idx, 'bollinger_lower'] = (rolling_mean - 2 * rolling_std).values
            
            # ATR(14)
            if len(instrument_data) > 14:
                highs = instrument_data['high_price'].values
                lows = instrument_data['low_price'].values
                closes = instrument_data['close_price'].values
                
                if TALIB_AVAILABLE:
                    atr = talib.ATR(highs, lows, closes, timeperiod=14)
                else:
                    atr = self._calculate_atr_fallback(highs, lows, closes, 14)
                features.loc[idx, 'atr_14'] = atr
            
            # MACD
            if TALIB_AVAILABLE:
                macd, signal, hist = talib.MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9)
                features.loc[idx, 'macd'] = macd
            else:
                macd = self._calculate_macd_fallback(prices)
                features.loc[idx, 'macd'] = macd
        
        # Fill NaN values
        features = features.fillna(0)
        
        return features
    
    def _calculate_rsi_fallback(self, prices: np.ndarray, period: int = 14) -> np.ndarray:
        """Fallback RSI calculation"""
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = pd.Series(gains).rolling(window=period).mean().values
        avg_loss = pd.Series(losses).rolling(window=period).mean().values
        
        rs = np.where(avg_loss != 0, avg_gain / avg_loss, 0)
        rsi = 100 - (100 / (1 + rs))
        
        # Pad with NaN for first period values
        rsi = np.concatenate([[np.nan] * period, rsi[period:]]) if len(rsi) > period else rsi
        return rsi
    
    def _calculate_atr_fallback(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int) -> np.ndarray:
        """Fallback ATR calculation"""
        tr1 = highs - lows
        tr2 = np.abs(highs - np.roll(closes, 1))
        tr3 = np.abs(lows - np.roll(closes, 1))
        
        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = pd.Series(tr).rolling(window=period).mean().values
        return atr
    
    def _calculate_macd_fallback(self, prices: np.ndarray) -> np.ndarray:
        """Fallback MACD calculation"""
        ema_fast = pd.Series(prices).ewm(span=12, adjust=False).mean()
        ema_slow = pd.Series(prices).ewm(span=26, adjust=False).mean()
        macd = (ema_fast - ema_slow).values
        return macd
    
    def _calculate_orderbook_features(
        self, 
        features: pd.DataFrame, 
        raw_data: pd.DataFrame, 
        time_bucket: str
    ) -> pd.DataFrame:
        """Calculate order book features from raw data"""
        
        minutes = int(time_bucket.replace('min', ''))
        bucket_seconds = minutes * 60
        bucket_ns = bucket_seconds * 1_000_000_000
        
        raw_data['bucket_timestamp_ns'] = (
            (raw_data['exchange_timestamp_ns'] // bucket_ns) * bucket_ns
        )
        
        # Calculate order book metrics per bucket
        orderbook_metrics = raw_data.groupby(['instrument_token', 'bucket_timestamp_ns']).agg({
            'bid_price1': 'mean',
            'ask_price1': 'mean',
            'bid_quantity1': 'sum',
            'bid_quantity2': 'sum',
            'bid_quantity3': 'sum',
            'bid_quantity4': 'sum',
            'bid_quantity5': 'sum',
            'ask_quantity1': 'sum',
            'ask_quantity2': 'sum',
            'ask_quantity3': 'sum',
            'ask_quantity4': 'sum',
            'ask_quantity5': 'sum',
        }).reset_index()
        
        orderbook_metrics['bid_ask_spread'] = (
            orderbook_metrics['ask_price1'] - orderbook_metrics['bid_price1']
        )
        orderbook_metrics['total_bid_depth'] = (
            orderbook_metrics['bid_quantity1'] + orderbook_metrics['bid_quantity2'] +
            orderbook_metrics['bid_quantity3'] + orderbook_metrics['bid_quantity4'] +
            orderbook_metrics['bid_quantity5']
        )
        orderbook_metrics['total_ask_depth'] = (
            orderbook_metrics['ask_quantity1'] + orderbook_metrics['ask_quantity2'] +
            orderbook_metrics['ask_quantity3'] + orderbook_metrics['ask_quantity4'] +
            orderbook_metrics['ask_quantity5']
        )
        orderbook_metrics['order_imbalance'] = (
            (orderbook_metrics['total_bid_depth'] - orderbook_metrics['total_ask_depth']) /
            (orderbook_metrics['total_bid_depth'] + orderbook_metrics['total_ask_depth'] + 1e-10)
        )
        
        # Merge with features
        features = features.merge(
            orderbook_metrics[['instrument_token', 'bucket_timestamp_ns', 
                             'bid_ask_spread', 'total_bid_depth', 'total_ask_depth', 'order_imbalance']],
            left_on=['instrument_token', 'timestamp_ns'],
            right_on=['instrument_token', 'bucket_timestamp_ns'],
            how='left'
        )
        
        features = features.drop(columns=['bucket_timestamp_ns'], errors='ignore')
        features = features.fillna(0)
        
        return features
    
    def _calculate_volatility_features(self, features: pd.DataFrame) -> pd.DataFrame:
        """Calculate volatility features"""
        
        features = features.sort_values(['instrument_token', 'timestamp_ns'])
        
        for instrument_token in features['instrument_token'].unique():
            mask = features['instrument_token'] == instrument_token
            prices = features[mask]['close_price'].values
            
            if len(prices) < 2:
                continue
            
            idx = features[mask].index
            
            # Realized volatility (rolling std of returns)
            returns = pd.Series(prices).pct_change().dropna()
            if len(returns) >= 20:
                realized_vol = returns.rolling(window=20).std().values * np.sqrt(252 * 390)  # Annualized
                features.loc[idx[1:], 'realized_volatility'] = realized_vol
            
            # Price momentum (rate of change)
            if len(prices) >= 5:
                momentum = ((prices[5:] - prices[:-5]) / prices[:-5]) * 100
                features.loc[idx[5:], 'price_momentum'] = momentum
        
        features = features.fillna(0)
        return features
    
    def _write_minute_features(self, features: pd.DataFrame, time_bucket: str) -> int:
        """Write computed features to minute_features table"""
        
        # Ensure required columns exist
        required_cols = [
            'instrument_token', 'timestamp_ns', 'date', 'time_bucket',
            'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'vwap',
            'rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 'bollinger_lower',
            'atr_14', 'macd',
            'bid_ask_spread', 'total_bid_depth', 'total_ask_depth', 'order_imbalance',
            'realized_volatility', 'price_momentum'
        ]
        
        for col in required_cols:
            if col not in features.columns:
                if col == 'time_bucket':
                    features['time_bucket'] = time_bucket
                else:
                    features[col] = 0.0
        
        # Select only the columns we need
        features_to_write = features[required_cols].copy()
        
        # Use INSERT OR REPLACE to handle duplicates
        # DuckDB doesn't support INSERT OR REPLACE directly, so we use DELETE + INSERT
        self.conn.execute("""
            DELETE FROM minute_features 
            WHERE time_bucket = ?
        """, [time_bucket])
        
        # Insert new data
        self.conn.executemany("""
            INSERT INTO minute_features 
            (instrument_token, timestamp_ns, date, time_bucket,
             open_price, high_price, low_price, close_price, volume, vwap,
             rsi_14, sma_20, ema_12, bollinger_upper, bollinger_lower,
             atr_14, macd,
             bid_ask_spread, total_bid_depth, total_ask_depth, order_imbalance,
             realized_volatility, price_momentum)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, features_to_write.values.tolist())
        
        return len(features_to_write)
    
    def _calculate_option_greeks(
        self, 
        instrument_tokens: List[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> int:
        """
        Calculate option Greeks (delta, gamma, theta, vega, IV)
        Note: This requires option pricing library (py_vollib) and underlying prices
        """
        # Placeholder - implement with py_vollib if available
        logger.warning("Option Greeks calculation not yet implemented")
        return 0
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    # Example usage
    pipeline = DataProcessingPipeline()
    
    # Process all data
    results = pipeline.process_raw_to_features(time_buckets=['5min'])
    print(f"Processed: {results}")
    
    # Incremental update
    results = pipeline.incremental_update(time_buckets=['5min'])
    print(f"Incremental update: {results}")
    
    pipeline.close()

