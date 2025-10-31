#!/usr/bin/env python3
"""
Volume Baseline Builder
======================

Build time-aware volume baselines for accurate volume ratio calculations.
This script analyzes historical data to create time-of-day volume patterns.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from utils.volume_baselines import VolumeBaselineManager
from redis_files.redis_ohlc_keys import normalize_symbol
from crawlers.utils.token_resolver import resolve_token_to_symbol
from redis_files.redis_client import get_redis_client
from config.zerodha_config import ZerodhaConfig
import requests
from datetime import datetime, timedelta
# Get 174 instruments from intraday crawler
def get_intraday_instruments():
    """Get 174 instruments from intraday crawler token list"""
    try:
        # Load from binary_crawler1.json which contains the 174 tokens
        import json
        
        crawler_config_file = "crawlers/binary_crawler1/binary_crawler1.json"
        
        with open(crawler_config_file, 'r') as f:
            crawler_data = json.load(f)
        
        # Get tokens from the crawler config - return tokens directly for API calls
        tokens = crawler_data.get('tokens', [])
        
        logger.info(f"Loaded {len(tokens)} tokens from intraday crawler")
        return tokens  # Return tokens directly, not symbols
        
    except Exception as e:
        logger.error(f"Failed to get intraday instruments: {e}")
        return []

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VolumeBaselineBuilder:
    """Build historical volume patterns for each symbol"""
    
    def __init__(self):
        self.redis = get_redis_client()
        # Use DB 5 for volume baselines (same as volume averages)
        self.redis.select(5)
        self.baseline_calc = TimeAwareVolumeBaseline(self.redis)
        self.volume_baseline_manager = VolumeBaselineManager(self.redis)
        
        # Define custom time periods for Indian market
        self.trading_periods = {
            'premarket': {'start': '09:00', 'end': '09:08', 'multiplier': 0.1},
            'open': {'start': '09:15', 'end': '09:45', 'multiplier': 0.3},
            'morning': {'start': '09:45', 'end': '11:30', 'multiplier': 0.4},
            'midday': {'start': '11:30', 'end': '12:30', 'multiplier': 0.2},
            'afternoon': {'start': '12:30', 'end': '14:30', 'multiplier': 0.3},
            'close': {'start': '14:30', 'end': '15:30', 'multiplier': 0.2}
        }
    
    def build_time_aware_baselines(self, symbols: List[str] = None) -> Dict[str, Dict[str, float]]:
        """Build historical volume patterns for each symbol and time period"""
        if symbols is None:
            symbols = get_intraday_instruments()
        
        logger.info(f"Building time-aware baselines for {len(symbols)} symbols")
        
        baselines = {}
        
        for i, token in enumerate(symbols, 1):
            logger.info(f"Processing token {token} ({i}/{len(symbols)})")
            
            try:
                symbol_baselines = self._build_symbol_baselines(token)
                baselines[str(token)] = symbol_baselines
                
                logger.info(f"✅ Built baselines for token {token}: {len(symbol_baselines)} periods")
                
            except Exception as e:
                logger.error(f"❌ Failed to build baselines for token {token}: {e}")
                baselines[str(token)] = {}
        
        # Summary
        total_periods = sum(len(symbol_baselines) for symbol_baselines in baselines.values())
        logger.info(f"🎯 Built {total_periods} baseline periods across {len(symbols)} symbols")
        
        return baselines
    
    def _build_symbol_baselines(self, token: int) -> Dict[str, float]:
        """Build baselines for a specific symbol"""
        symbol_baselines = {}
        
        # Get time periods from our custom trading periods
        time_periods = list(self.trading_periods.keys())
        
        for period in time_periods:
            try:
                # Calculate baseline for this period
                period_volume = self._calculate_period_volume(token, period)
                
                if period_volume > 0:
                    # Store in Redis
                    baseline_key = f"volume_baseline:{token}:{period}"
                    self.redis.set(baseline_key, str(period_volume))
                    
                    symbol_baselines[period] = period_volume
                    logger.debug(f"  {period}: {period_volume:.2f}")
                
            except Exception as e:
                logger.warning(f"Failed to build baseline for token {token} {period}: {e}")
        
        return symbol_baselines
    
    def _calculate_period_volume(self, token: int, period: str) -> float:
        """Calculate average volume for a specific time period using existing dynamic data"""
        try:
            # Get base average volume from existing Redis data (20-day and 55-day averages)
            base_avg = self._get_dynamic_average_volume(token)
            
            if base_avg <= 0:
                logger.warning(f"No dynamic volume data found for token {token}, skipping baseline")
                return 0.0
            
            # Apply time period multiplier from our custom periods
            multiplier = self.trading_periods.get(period, {}).get('multiplier', 1.0)
            period_volume = base_avg * multiplier
            
            logger.debug(f"  token {token} {period}: base={base_avg:.0f} * {multiplier} = {period_volume:.0f}")
            return period_volume
            
        except Exception as e:
            logger.warning(f"Failed to calculate period volume for token {token} {period}: {e}")
            return 0.0  # Don't create baselines without real data
    
    def _get_dynamic_average_volume(self, token: int) -> float:
        """Return average daily volume using the standardized baseline manager."""
        norm_symbol = normalize_symbol(str(token))
        try:
            avg_volume = self.volume_baseline_manager.get_baseline(
                norm_symbol, "20d", 0.0
            )
            if avg_volume and avg_volume > 0:
                return float(avg_volume)
        except Exception as exc:
            logger.debug(f"VolumeBaselineManager lookup failed for token {token}: {exc}")
        
        # Try to fetch from Zerodha API if no Redis data
        try:
            return self._fetch_historical_volume_from_zerodha(token)
        except Exception as exc:
            logger.error(f"Failed to get dynamic average volume for token {token}: {exc}")
            return 0.0
    
    def _fetch_historical_volume_from_zerodha(self, token: int) -> float:
        """Fetch historical volume data from Zerodha API"""
        try:
            import json
            # Load credentials from token file
            with open(ZerodhaConfig.TOKEN_FILE, 'r') as f:
                token_data = json.load(f)
            
            access_token = token_data.get('access_token')
            api_key = token_data.get('api_key')
            
            if not access_token or not api_key:
                logger.warning("Zerodha credentials not found")
                return 0.0
            
            # Get historical data for last 20 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=20)
            
            # Use proper Zerodha API endpoint for historical data
            url = f"https://kite.zerodha.com/oms/instruments/historical/{token}/day"
            headers = {
                'Authorization': f'token {api_key}:{access_token}',
                'X-Kite-Version': '3'
            }
            
            params = {
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d'),
                'interval': 'day'
            }
            
            logger.info(f"Fetching historical data for token {token} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and 'candles' in data['data']:
                candles = data['data']['candles']
                if candles:
                    # Calculate average volume from historical data
                    # Zerodha candles format: [timestamp, open, high, low, close, volume, oi]
                    total_volume = sum(float(candle[5]) for candle in candles)  # Volume is at index 5
                    avg_volume = total_volume / len(candles)
                    logger.info(f"✅ Fetched historical volume for token {token}: {avg_volume:.0f} (from {len(candles)} days)")
                    return avg_volume
                else:
                    logger.warning(f"No candle data for token {token}")
            else:
                logger.warning(f"No data field in response for token {token}: {data}")
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Failed to fetch historical data for token {token}: {e}")
            return 0.0
    
    def verify_baselines(self, symbols: List[str] = None) -> Dict[str, bool]:
        """Verify that baselines were built correctly"""
        if symbols is None:
            symbols = get_intraday_instruments()
        
        verification = {}
        
        for token in symbols:
            try:
                # Check if all time periods have baselines (use token directly)
                time_periods = list(self.trading_periods.keys())
                missing_periods = []
                
                for period in time_periods:
                    baseline_key = f"volume_baseline:{token}:{period}"
                    baseline = self.redis.get(baseline_key)
                    
                    if not baseline or float(baseline) <= 0:
                        missing_periods.append(period)
                
                verification[str(token)] = len(missing_periods) == 0
                
                if missing_periods:
                    logger.warning(f"❌ Token {token} missing baselines for: {missing_periods}")
                else:
                    logger.info(f"✅ Token {token} has all baselines")
                
            except Exception as e:
                logger.error(f"❌ Failed to verify token {token}: {e}")
                verification[str(token)] = False
        
        return verification
    
    def get_baseline_summary(self, symbols: List[str] = None) -> Dict[str, Dict[str, float]]:
        """Get a summary of all baselines"""
        if symbols is None:
            symbols = get_intraday_instruments()
        
        summary = {}
        
        for token in symbols:
            symbol_summary = {}
            
            for period in self.trading_periods.keys():
                try:
                    baseline_key = f"volume_baseline:{token}:{period}"
                    baseline = self.redis.get(baseline_key)
                    
                    if baseline:
                        symbol_summary[period] = float(baseline)
                    else:
                        symbol_summary[period] = 0.0
                        
                except Exception as e:
                    logger.debug(f"Failed to get baseline for token {token} {period}: {e}")
                    symbol_summary[period] = 0.0
            
            summary[str(token)] = symbol_summary
        
        return summary

def main():
    """Main function to build volume baselines"""
    logger.info("🚀 Starting Volume Baseline Builder")
    
    try:
        builder = VolumeBaselineBuilder()
        
        # Build baselines
        logger.info("📊 Building time-aware baselines...")
        baselines = builder.build_time_aware_baselines()
        
        # Verify baselines
        logger.info("🔍 Verifying baselines...")
        verification = builder.verify_baselines()
        
        # Summary
        successful = sum(1 for success in verification.values() if success)
        total = len(verification)
        
        logger.info(f"🎯 Baseline building complete: {successful}/{total} symbols successful")
        
        if successful == total:
            logger.info("✅ All baselines built successfully!")
        else:
            logger.warning(f"⚠️ {total - successful} symbols failed baseline building")
        
        # Show sample baselines
        logger.info("📋 Sample baselines:")
        sample_symbols = list(baselines.keys())[:3]
        for symbol in sample_symbols:
            logger.info(f"  {symbol}: {baselines[symbol]}")
        
    except Exception as e:
        logger.error(f"❌ Baseline building failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
