#!/usr/bin/env python3
"""
Volume Profile Manager
=====================

Real-time Volume Profile Management with Redis Integration for Enhanced Pattern Detection

PURPOSE:
- Calculates Point of Control (POC), Value Area (VA), and High/Low Volume Nodes
- Integrates with VolumeStateManager for consistent volume data flow
- Uses MarketProfile library for mathematical precision
- Publishes volume profile indicators to Redis for pattern detection

DATA FLOW:
Raw Ticks → VolumeStateManager → VolumeProfileManager → MarketProfile Library → 
POC/VA Calculation → Redis Storage → Enhanced Pattern Detection → Trading Signals

INTEGRATION POINTS:
- Uses canonical field names from config/optimized_field_mapping.yaml
- Integrates with core/data/volume_state_manager.py for volume data
- Publishes to Redis using core/data/redis_storage.py patterns
- Accessed by patterns/pattern_detector.py for enhanced pattern detection

Author: AION Integration Team
Date: October 26, 2025
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
from market_profile import MarketProfile

class VolumeProfileManager:
    """
    Real-time Volume Profile Management with Redis Integration
    
    SINGLE SOURCE OF TRUTH for volume profile calculations across the entire system.
    Integrates with VolumeStateManager and uses canonical field names from field mapping.
    
    Redis Database: DB 2 (analytics) - Stores all volume profile data (consolidated from DB 5)
    Key Patterns:
    - volume_profile:session:SYMBOL:YYYY-MM-DD (Hash)
    - volume_profile:poc:SYMBOL (Hash) 
    - volume_profile:nodes:SYMBOL (Hash)
    - volume_profile:distribution:SYMBOL:YYYY-MM-DD (Hash)
    - volume_profile:patterns:SYMBOL:daily (Sorted Set)
    - volume_profile:historical:SYMBOL (List)
    """
    
    def __init__(self, redis_client, token_resolver=None):
        """
        Initialize Volume Profile Manager
        
        Args:
            redis_client: Redis client for data persistence (uses DB 2 analytics for volume profiles)
            token_resolver: Token resolver for symbol resolution (optional)
        """
        self.redis_client = redis_client
        self.token_resolver = token_resolver
        self.profiles = {}
        self.tick_sizes = self._load_tick_sizes()
        self.logger = logging.getLogger(__name__)
        
        # Ensure we're using the correct database for volume profile data (DB 2 analytics)
        self.volume_profile_db = 2  # analytics database (consolidated from DB 5)
        
    def _load_tick_sizes(self) -> Dict:
        """Load instrument-specific tick sizes"""
        return {
            'NIFTY': 0.05, 'BANKNIFTY': 0.05, 'FINNIFTY': 0.05,
            'RELIANCE': 0.05, 'TCS': 0.05, 'INFY': 0.05,
            'HDFCBANK': 0.05, 'ICICIBANK': 0.05, 'SBIN': 0.05
        }
    
    def update_price_volume(self, symbol: str, price: float, volume: int, timestamp: datetime):
        """
        Update volume profile with new tick data using canonical field names
        
        Args:
            symbol: Trading symbol (uses canonical field names)
            price: Last traded price (last_price field)
            volume: Incremental volume (bucket_incremental_volume field)
            timestamp: Exchange timestamp (exchange_timestamp field)
        """
        try:
            # Get or create profile for symbol
            if symbol not in self.profiles:
                tick_size = self.tick_sizes.get(symbol, 0.05)
                self.profiles[symbol] = SymbolVolumeProfile(symbol, tick_size, self.redis_client)
            
            # Update profile with canonical field names
            self.profiles[symbol].update(price, volume, timestamp)
            
            # Store in Redis every 100 ticks or 1 minute
            if self.profiles[symbol].should_persist():
                self._persist_to_redis(symbol)
                
        except Exception as e:
            self.logger.error(f"Volume profile update error for {symbol}: {e}")

    def _persist_to_redis(self, symbol: str):
        """
        Persist volume profile data to Redis using standardized key patterns
        
        Redis Key Patterns (following system standards):
        - indicators:poc_price:SYMBOL → Stream for POC price indicator
        - indicators:poc_volume:SYMBOL → Stream for POC volume indicator  
        - indicators:value_area_high:SYMBOL → Stream for VA high indicator
        - indicators:value_area_low:SYMBOL → Stream for VA low indicator
        - indicators:profile_strength:SYMBOL → Stream for profile strength indicator
        - indicators:support_levels:SYMBOL → Stream for support levels indicator
        - indicators:resistance_levels:SYMBOL → Stream for resistance levels indicator
        
        Additional Storage Keys:
        - volume_profile:session:SYMBOL:YYYY-MM-DD → Hash with full profile data
        - volume_profile:poc:SYMBOL → Hash with POC/VA for quick access
        - volume_profile:distribution:SYMBOL:YYYY-MM-DD → Hash with price-volume distribution
        - volume_profile:patterns:SYMBOL:daily → Sorted Set with historical patterns
        - volume_profile:historical:SYMBOL → List with historical profile data (24h lookback)
        """
        profile = self.profiles[symbol]
        profile_data = profile.get_profile_data()
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # Store full profile data using standardized session key pattern
        session_key = f"volume_profile:session:{symbol}:{current_date}"
        if hasattr(self.redis_client, 'get_client'):
            redis_client = self.redis_client.get_client(self.volume_profile_db)
        else:
            redis_client = self.redis_client
            redis_client.select(self.volume_profile_db)
        
        for key, value in profile_data.items():
            redis_client.hset(session_key, key, str(value))
        
        # Store POC and Value Area separately for quick access (following system patterns)
        poc_key = f"volume_profile:poc:{symbol}"
        poc_data = {
            'poc_price': profile_data.get('poc_price', 0),
            'poc_volume': profile_data.get('poc_volume', 0),
            'value_area_high': profile_data.get('value_area_high', 0),
            'value_area_low': profile_data.get('value_area_low', 0),
            'profile_strength': profile_data.get('profile_strength', 0),
            'exchange_timestamp': datetime.now().isoformat()  # Canonical timestamp field
        }
        for key, value in poc_data.items():
            redis_client.hset(poc_key, key, str(value))
        
        # Store support/resistance levels separately for pattern detection
        nodes_key = f"volume_profile:nodes:{symbol}"
        nodes_data = {
            'support_levels': json.dumps(profile_data.get('support_levels', [])),
            'resistance_levels': json.dumps(profile_data.get('resistance_levels', [])),
            'exchange_timestamp': datetime.now().isoformat()
        }
        for key, value in nodes_data.items():
            redis_client.hset(nodes_key, key, str(value))
        
        # NEW: Store price-volume distribution buckets (as specified by user)
        distribution_key = f"volume_profile:distribution:{symbol}:{current_date}"
        price_volume_data = profile.price_volume
        for price, volume in price_volume_data.items():
            redis_client.hset(distribution_key, f"{price:.2f}", str(volume))
        
        # NEW: Store historical profile patterns (as specified by user)
        patterns_key = f"volume_profile:patterns:{symbol}:daily"
        pattern_data = {
            'poc_price': profile_data.get('poc_price', 0),
            'profile_strength': profile_data.get('profile_strength', 0),
            'value_area_range': profile_data.get('value_area_high', 0) - profile_data.get('value_area_low', 0),
            'session_date': current_date,
            'total_volume': profile_data.get('total_volume', 0),
            'price_levels': len(price_volume_data)
        }
        timestamp = datetime.now().timestamp()
        # Use underlying Redis client for zadd operation
        redis_client.zadd(patterns_key, {json.dumps(pattern_data): timestamp})
        
        # NEW: Store historical profile data for pattern detection (as specified by user)
        historical_key = f"volume_profile:historical:{symbol}"
        historical_data = {
            'session_date': current_date,
            'poc_price': profile_data.get('poc_price', 0),
            'poc_volume': profile_data.get('poc_volume', 0),
            'value_area_high': profile_data.get('value_area_high', 0),
            'value_area_low': profile_data.get('value_area_low', 0),
            'profile_strength': profile_data.get('profile_strength', 0),
            'total_volume': profile_data.get('total_volume', 0),
            'price_levels': len(price_volume_data),
            'exchange_timestamp': datetime.now().isoformat()
        }
        redis_client.lpush(historical_key, json.dumps(historical_data))
        
        # Keep only last 24 hours of historical data
        redis_client.ltrim(historical_key, 0, 23)
        
        # Set TTL for all keys (24 hours)
        for key in [session_key, distribution_key, patterns_key, historical_key]:
            if hasattr(self.redis_client, 'get_client'):
                redis_client = self.redis_client.get_client(self.volume_profile_db)
            else:
                redis_client = self.redis_client
                redis_client.select(self.volume_profile_db)
            redis_client.expire(key, 86400)

    def get_volume_nodes(self, symbol: str) -> Optional[Dict]:
        """
        Get volume profile nodes for pattern detection using standardized Redis keys
        
        Returns canonical field names for integration with patterns/pattern_detector.py
        """
        try:
            if symbol in self.profiles:
                return self.profiles[symbol].get_trading_nodes()
            
            # Fallback to Redis using standardized key patterns
            poc_key = f"volume_profile:poc:{symbol}"
            nodes_key = f"volume_profile:nodes:{symbol}"
            
            # Get POC and Value Area data from correct database
            if hasattr(self.redis_client, 'get_client'):
                redis_client = self.redis_client.get_client(self.volume_profile_db)
            else:
                redis_client = self.redis_client
                redis_client.select(self.volume_profile_db)
            
            poc_data = redis_client.hgetall(poc_key)
            nodes_data = redis_client.hgetall(nodes_key)
            
            # Convert bytes to proper types (Redis returns bytes)
            result = {}
            if poc_data:
                for key, value in poc_data.items():
                    key_str = key.decode() if isinstance(key, bytes) else key
                    if key_str == 'exchange_timestamp':
                        result[key_str] = value.decode() if isinstance(value, bytes) else value
                    else:
                        try:
                            result[key_str] = float(value.decode()) if isinstance(value, bytes) else float(value)
                        except (ValueError, TypeError):
                            result[key_str] = value.decode() if isinstance(value, bytes) else value
            
            # Add support/resistance levels
            if nodes_data:
                for key, value in nodes_data.items():
                    key_str = key.decode() if isinstance(key, bytes) else key
                    if key_str in ['support_levels', 'resistance_levels']:
                        try:
                            result[key_str] = json.loads(value.decode() if isinstance(value, bytes) else value)
                        except (json.JSONDecodeError, TypeError):
                            result[key_str] = []
                    elif key_str == 'exchange_timestamp':
                        result[key_str] = value.decode() if isinstance(value, bytes) else value
            
            return result if result else None
            
        except Exception as e:
            self.logger.error(f"Error getting volume nodes for {symbol}: {e}")
            return None
    
    def get_price_volume_distribution(self, symbol: str, date: str = None) -> Optional[Dict]:
        """
        Get price-volume distribution buckets for a symbol and date
        
        Args:
            symbol: Trading symbol
            date: Date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            Dict with price levels as keys and volumes as values
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            distribution_key = f"volume_profile:distribution:{symbol}:{date}"
            if hasattr(self.redis_client, 'get_client'):
                redis_client = self.redis_client.get_client(self.volume_profile_db)
            else:
                redis_client = self.redis_client
                redis_client.select(self.volume_profile_db)
            
            distribution_data = redis_client.hgetall(distribution_key)
            
            if not distribution_data:
                return None
            
            # Convert string values to integers
            result = {}
            for price_str, volume_str in distribution_data.items():
                try:
                    price = float(price_str)
                    volume = int(volume_str)
                    result[price] = volume
                except (ValueError, TypeError):
                    continue
            
            return result if result else None
            
        except Exception as e:
            self.logger.error(f"Error getting price-volume distribution for {symbol}: {e}")
            return None
    
    def get_historical_profiles(self, symbol: str, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Get historical volume profiles for pattern detection (as specified by user)
        
        Args:
            symbol: Trading symbol
            start_date: Start date for historical data
            end_date: End date for historical data
            
        Returns:
            List of historical profile data sorted by date
        """
        try:
            patterns_key = f"volume_profile:patterns:{symbol}:daily"
            
            # Get profiles within date range
            start_ts = start_date.timestamp()
            end_ts = end_date.timestamp()
            
            # Use underlying Redis client for zrangebyscore operation
            if hasattr(self.redis_client, 'get_client'):
                redis_client = self.redis_client.get_client(self.volume_profile_db)
            else:
                redis_client = self.redis_client
                redis_client.select(self.volume_profile_db)
            
            profile_data = redis_client.zrangebyscore(patterns_key, start_ts, end_ts)
            
            profiles = []
            for data in profile_data:
                try:
                    profile = json.loads(data)
                    profiles.append(profile)
                except json.JSONDecodeError:
                    continue
            
            return sorted(profiles, key=lambda x: x.get('session_date', ''))
            
        except Exception as e:
            self.logger.error(f"Error getting historical profiles for {symbol}: {e}")
            return []
    
    def get_session_profile_summary(self, symbol: str, date: str = None) -> Optional[Dict]:
        """
        Get session profile summary for a symbol and date
        
        Args:
            symbol: Trading symbol
            date: Date in YYYY-MM-DD format (defaults to today)
            
        Returns:
            Dict with session profile summary data
        """
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            session_key = f"volume_profile:session:{symbol}:{date}"
            if hasattr(self.redis_client, 'get_client'):
                redis_client = self.redis_client.get_client(self.volume_profile_db)
            else:
                redis_client = self.redis_client
                redis_client.select(self.volume_profile_db)
            
            session_data = redis_client.hgetall(session_key)
            
            if not session_data:
                return None
            
            # Convert string values to appropriate types
            result = {}
            for key, value in session_data.items():
                key_str = key.decode() if isinstance(key, bytes) else key
                value_str = value.decode() if isinstance(value, bytes) else value
                
                if key_str in ['poc_price', 'poc_volume', 'value_area_high', 'value_area_low', 
                              'profile_strength', 'total_volume', 'price_levels']:
                    try:
                        result[key_str] = float(value_str)
                    except (ValueError, TypeError):
                        result[key_str] = value_str
                elif key_str in ['support_levels', 'resistance_levels']:
                    try:
                        result[key_str] = json.loads(value_str)
                    except json.JSONDecodeError:
                        result[key_str] = []
                else:
                    result[key_str] = value_str
            
            return result if result else None
            
        except Exception as e:
            self.logger.error(f"Error getting session profile summary for {symbol}: {e}")
            return None
    
    def calculate_poc_from_ohlc(self, symbol: str, ohlc_data: Dict) -> Dict:
        """
        Calculate POC from OHLC data for historical volume profile
        
        Args:
            symbol: Trading symbol
            ohlc_data: OHLC data dict with open, high, low, close, volume
            
        Returns:
            Dict with POC, Value Area, and profile metrics
        """
        try:
            open_price = float(ohlc_data.get('open', 0))
            high_price = float(ohlc_data.get('high', 0))
            low_price = float(ohlc_data.get('low', 0))
            close_price = float(ohlc_data.get('close', 0))
            volume = int(ohlc_data.get('volume', 0))
            
            if not all([open_price, high_price, low_price, close_price, volume]):
                return {}
            
            # Create price-volume distribution from OHLC
            # Use typical price (HLC/3) as POC approximation
            typical_price = (high_price + low_price + close_price) / 3
            
            # Calculate price range and create distribution
            price_range = high_price - low_price
            if price_range == 0:
                # Single price level
                poc_price = close_price
                poc_volume = volume
                value_area_high = high_price
                value_area_low = low_price
            else:
                # Use VWAP approximation for POC
                poc_price = typical_price
                poc_volume = volume
                
                # Calculate Value Area (70% around POC)
                va_range = price_range * 0.7
                value_area_high = poc_price + (va_range / 2)
                value_area_low = poc_price - (va_range / 2)
            
            # Calculate profile strength (volume concentration)
            profile_strength = min(volume / 1000000, 1.0)  # Normalize to 0-1
            
            return {
                'poc_price': round(poc_price, 2),
                'poc_volume': poc_volume,
                'value_area_high': round(value_area_high, 2),
                'value_area_low': round(value_area_low, 2),
                'profile_strength': round(profile_strength, 4),
                'total_volume': volume,
                'price_levels': 1,  # Single OHLC bar
                'calculation_method': 'ohlc_historical',
                'exchange_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating POC from OHLC for {symbol}: {e}")
            return {}
    
    def populate_poc_from_ohlc_data(self, redis_client=None):
        """
        Populate POC data for all instruments from OHLC data in Redis
        
        Args:
            redis_client: Redis client (uses self.redis_client if None)
        """
        try:
            if redis_client is None:
                redis_client = self.redis_client
            
            # Get underlying Redis client for DB 1 (OHLC data in realtime)
            ohlc_client = redis_client.get_client(1) if hasattr(redis_client, 'get_client') else redis_client
            
            # Get all OHLC latest keys
            ohlc_keys = ohlc_client.keys('ohlc_latest:*')
            
            print(f"🔄 POPULATING POC FROM OHLC DATA")
            print(f"📊 Processing {len(ohlc_keys)} instruments...")
            
            populated_count = 0
            failed_count = 0
            
            for ohlc_key in ohlc_keys:
                try:
                    # Extract symbol from key (ohlc_latest:SYMBOL)
                    symbol = ohlc_key.replace('ohlc_latest:', '')
                    
                    # Get OHLC data
                    ohlc_data = ohlc_client.hgetall(ohlc_key)
                    if not ohlc_data:
                        continue
                    
                    # Calculate POC from OHLC data
                    poc_data = self.calculate_poc_from_ohlc(symbol, ohlc_data)
                    if not poc_data:
                        failed_count += 1
                        continue
                    
                    # Store POC data in Redis (DB 2 analytics)
                    poc_key = f"volume_profile:poc:{symbol}"
                    for key, value in poc_data.items():
                        redis_client.hset(poc_key, key, str(value))
                    
                    # Store session profile data (DB 2 analytics)
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    session_key = f"volume_profile:session:{symbol}:{current_date}"
                    for key, value in poc_data.items():
                        redis_client.hset(session_key, key, str(value))
                    
                    populated_count += 1
                    
                    if populated_count % 20 == 0:
                        print(f"   ✅ Processed {populated_count} instruments...")
                
                except Exception as e:
                    self.logger.error(f"Error processing {ohlc_key}: {e}")
                    failed_count += 1
            
            print(f"\\n📈 POC POPULATION SUMMARY:")
            print(f"   ✅ Successfully populated: {populated_count}")
            print(f"   ❌ Failed: {failed_count}")
            print(f"   📊 Total processed: {len(ohlc_keys)}")
            
            return populated_count, failed_count
            
        except Exception as e:
            self.logger.error(f"Error populating POC from OHLC data: {e}")
            return 0, 0

    def populate_poc_from_time_buckets(self, redis_client=None):
        """
        Populate POC data for all instruments from time-bucket data in Redis
        
        This method works with the actual data structure stored in Redis:
        - session:SYMBOL:YYYY-MM-DD (contains time buckets with OHLC data)
        
        Args:
            redis_client: Redis client (uses self.redis_client if None)
        """
        try:
            if redis_client is None:
                redis_client = self.redis_client
            
            # Get underlying Redis client for DB 0 (session data)
            session_client = redis_client.get_client(0) if hasattr(redis_client, 'get_client') else redis_client
            
            # Get all session keys
            session_keys = session_client.keys('session:*')
            
            print(f"🔄 POPULATING POC FROM TIME BUCKETS")
            print(f"📊 Processing {len(session_keys)} instruments...")
            
            populated_count = 0
            failed_count = 0
            
            for session_key in session_keys:
                try:
                    # Extract symbol from key (session:SYMBOL:YYYY-MM-DD)
                    # Handle both bytes and string keys
                    key_str = session_key.decode() if isinstance(session_key, bytes) else session_key
                    key_parts = key_str.split(':')
                    if len(key_parts) != 3:
                        continue
                    symbol = key_parts[1]
                    
                    # Get session data
                    session_data = session_client.get(session_key)
                    if not session_data:
                        continue
                    
                    # Parse JSON data
                    try:
                        session_json = json.loads(session_data)
                    except json.JSONDecodeError:
                        continue
                    
                    # Extract time buckets
                    time_buckets = session_json.get('time_buckets', {})
                    if not time_buckets:
                        continue
                    
                    # Calculate POC from time buckets
                    poc_data = self.calculate_poc_from_time_buckets(symbol, time_buckets)
                    if not poc_data:
                        failed_count += 1
                        continue
                    
                    # Store POC data in Redis (DB 2 analytics)
                    poc_key = f"volume_profile:poc:{symbol}"
                    for key, value in poc_data.items():
                        redis_client.hset(poc_key, key, str(value))
                    
                    # Store session profile data (DB 2 analytics)
                    current_date = datetime.now().strftime('%Y-%m-%d')
                    session_profile_key = f"volume_profile:session:{symbol}:{current_date}"
                    for key, value in poc_data.items():
                        redis_client.hset(session_profile_key, key, str(value))
                    
                    populated_count += 1
                    
                    if populated_count % 20 == 0:
                        print(f"   ✅ Processed {populated_count} instruments...")
                
                except Exception as e:
                    self.logger.error(f"Error processing {session_key}: {e}")
                    failed_count += 1
            
            print(f"\\n📈 POC POPULATION SUMMARY:")
            print(f"   ✅ Successfully populated: {populated_count}")
            print(f"   ❌ Failed: {failed_count}")
            print(f"   📊 Total processed: {len(session_keys)}")
            
            return populated_count, failed_count
            
        except Exception as e:
            self.logger.error(f"Error populating POC from time buckets: {e}")
            return 0, 0

    def calculate_poc_from_time_buckets(self, symbol: str, time_buckets: Dict) -> Dict:
        """
        Calculate POC from time-bucket data for volume profile
        
        Args:
            symbol: Trading symbol
            time_buckets: Dict with time bucket data containing OHLC and volume
            
        Returns:
            Dict with POC, Value Area, and profile metrics
        """
        try:
            if not time_buckets:
                return {}
            
            # Aggregate volume by price levels from time buckets
            price_volume_distribution = {}
            total_volume = 0
            
            for bucket_time, bucket_data in time_buckets.items():
                try:
                    # Extract OHLC and volume from bucket
                    open_price = float(bucket_data.get('open', 0))
                    high_price = float(bucket_data.get('high', 0))
                    low_price = float(bucket_data.get('low', 0))
                    close_price = float(bucket_data.get('close', 0))
                    volume = int(bucket_data.get('bucket_incremental_volume', 0))
                    
                    if not all([open_price, high_price, low_price, close_price, volume]):
                        continue
                    
                    # Distribute volume across price levels
                    # Use VWAP approximation: (H+L+C)/3
                    vwap_price = (high_price + low_price + close_price) / 3
                    
                    # Add volume to price level (rounded to tick size)
                    tick_size = self.tick_sizes.get(symbol, 0.05)
                    rounded_price = round(vwap_price / tick_size) * tick_size
                    
                    price_volume_distribution[rounded_price] = price_volume_distribution.get(rounded_price, 0) + volume
                    total_volume += volume
                    
                except (ValueError, TypeError) as e:
                    continue
            
            if not price_volume_distribution or total_volume == 0:
                return {}
            
            # Find POC (price with highest volume)
            poc_price = max(price_volume_distribution.items(), key=lambda x: x[1])[0]
            poc_volume = price_volume_distribution[poc_price]
            
            # Calculate Value Area (70% of volume around POC)
            sorted_prices = sorted(price_volume_distribution.keys())
            poc_index = sorted_prices.index(poc_price)
            
            cumulative_volume = poc_volume
            value_area_high = poc_price
            value_area_low = poc_price
            
            high_index = poc_index + 1
            low_index = poc_index - 1
            
            while cumulative_volume < total_volume * 0.7:
                high_volume = price_volume_distribution.get(sorted_prices[high_index], 0) if high_index < len(sorted_prices) else 0
                low_volume = price_volume_distribution.get(sorted_prices[low_index], 0) if low_index >= 0 else 0
                
                if high_volume >= low_volume and high_index < len(sorted_prices):
                    cumulative_volume += high_volume
                    value_area_high = sorted_prices[high_index]
                    high_index += 1
                elif low_index >= 0:
                    cumulative_volume += low_volume
                    value_area_low = sorted_prices[low_index]
                    low_index -= 1
                else:
                    break
            
            # Calculate profile strength
            profile_strength = min(poc_volume / total_volume, 1.0)
            
            return {
                'poc_price': round(poc_price, 2),
                'poc_volume': poc_volume,
                'value_area_high': round(value_area_high, 2),
                'value_area_low': round(value_area_low, 2),
                'profile_strength': round(profile_strength, 4),
                'total_volume': total_volume,
                'price_levels': len(price_volume_distribution),
                'calculation_method': 'time_buckets',
                'exchange_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating POC from time buckets for {symbol}: {e}")
            return {}

class SymbolVolumeProfile:
    """
    Volume Profile for a single symbol using canonical field names
    
    Integrates with VolumeStateManager and uses MarketProfile library for calculations.
    All field names follow config/optimized_field_mapping.yaml conventions.
    """
    
    def __init__(self, symbol: str, tick_size: float, redis_client):
        """
        Initialize symbol volume profile
        
        Args:
            symbol: Trading symbol
            tick_size: Instrument tick size
            redis_client: Redis client for persistence
        """
        self.symbol = symbol
        self.tick_size = tick_size
        self.redis_client = redis_client
        self.price_volume = {}
        self.ticks_count = 0
        self.last_persist = datetime.now()
        self.session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        self.logger = logging.getLogger(__name__)
        
    def update(self, price: float, volume: int, timestamp: datetime):
        """
        Update with new price-volume data using canonical field names
        
        Args:
            price: Last traded price (last_price field)
            volume: Incremental volume (bucket_incremental_volume field)
            timestamp: Exchange timestamp (exchange_timestamp field)
        """
        # Round price to tick size
        rounded_price = self._round_to_tick(price)
        
        # Accumulate volume at price level using canonical field names
        self.price_volume[rounded_price] = self.price_volume.get(rounded_price, 0) + volume
        self.ticks_count += 1
        
    def _round_to_tick(self, price: float) -> float:
        """Round price to instrument tick size"""
        return round(price / self.tick_size) * self.tick_size
    
    def should_persist(self) -> bool:
        """Check if should persist to Redis"""
        return (self.ticks_count % 100 == 0 or 
                (datetime.now() - self.last_persist).seconds >= 60)
    
    def get_profile_data(self) -> Dict:
        """
        Calculate volume profile using custom implementation with canonical field names
        
        Returns data compatible with config/optimized_field_mapping.yaml
        """
        if not self.price_volume:
            return {}
            
        try:
            # Custom volume profile calculation (MarketProfile library has limited methods)
            return self._calculate_custom_profile()
            
        except Exception as e:
            self.logger.error(f"Volume profile calculation error for {self.symbol}: {e}")
            return self._calculate_basic_profile()
    
    def _calculate_custom_profile(self) -> Dict:
        """
        Custom volume profile calculation using mathematical approach
        
        Calculates POC, Value Area (70% volume), and profile metrics
        """
        if not self.price_volume:
            return {}
            
        # Find POC (price with highest volume)
        poc_price = max(self.price_volume.items(), key=lambda x: x[1])[0]
        poc_volume = self.price_volume[poc_price]
        total_volume = sum(self.price_volume.values())
        
        # Calculate Value Area (70% of volume around POC) - CORRECT METHOD
        # Get sorted price levels (must be contiguous for Value Area)
        sorted_prices = sorted(self.price_volume.keys())
        poc_index = sorted_prices.index(poc_price)
        
        # Expand outward from POC until we capture 70% of volume
        cumulative_volume = self.price_volume[poc_price]
        value_area_high = poc_price
        value_area_low = poc_price
        
        high_index = poc_index + 1
        low_index = poc_index - 1
        
        while cumulative_volume < total_volume * 0.7:
            # Check which direction to expand (higher volume side first)
            high_volume = self.price_volume.get(sorted_prices[high_index], 0) if high_index < len(sorted_prices) else 0
            low_volume = self.price_volume.get(sorted_prices[low_index], 0) if low_index >= 0 else 0
            
            if high_volume >= low_volume and high_index < len(sorted_prices):
                cumulative_volume += high_volume
                value_area_high = sorted_prices[high_index]
                high_index += 1
            elif low_index >= 0:
                cumulative_volume += low_volume
                value_area_low = sorted_prices[low_index]
                low_index -= 1
            else:
                break
        
        profile_range = value_area_high - value_area_low
        
        # Return with canonical field names
        return {
            'poc_price': poc_price,
            'poc_volume': poc_volume,
            'value_area_high': value_area_high,
            'value_area_low': value_area_low,
            'total_volume': total_volume,
            'price_levels': len(self.price_volume),
            'profile_range': profile_range,
            'session_start': self.session_start.isoformat(),
            'exchange_timestamp': datetime.now().isoformat(),  # Canonical timestamp field
            'calculation_method': 'custom_mathematical'
        }
    
    def _calculate_basic_profile(self) -> Dict:
        """
        Fallback profile calculation using canonical field names
        
        Used when MarketProfile library fails
        """
        if not self.price_volume:
            return {}
            
        # Find POC (price with highest volume)
        poc_price = max(self.price_volume.items(), key=lambda x: x[1])[0]
        total_volume = sum(self.price_volume.values())
        
        # Simple value area (prices with 70% of volume)
        sorted_prices = sorted(self.price_volume.items(), key=lambda x: x[1], reverse=True)
        cumulative_volume = 0
        value_area_prices = []
        
        for price, volume in sorted_prices:
            cumulative_volume += volume
            value_area_prices.append(price)
            if cumulative_volume >= total_volume * 0.7:
                break
                
        # Return with canonical field names
        return {
            'poc_price': poc_price,
            'poc_volume': self.price_volume[poc_price],
            'value_area_high': max(value_area_prices) if value_area_prices else poc_price,
            'value_area_low': min(value_area_prices) if value_area_prices else poc_price,
            'total_volume': total_volume,
            'price_levels': len(self.price_volume),
            'profile_range': max(value_area_prices) - min(value_area_prices) if value_area_prices else 0,
            'calculation_method': 'basic_fallback',
            'exchange_timestamp': datetime.now().isoformat()  # Canonical timestamp field
        }
    
    def get_trading_nodes(self) -> Dict:
        """
        Get trading-relevant volume nodes using canonical field names
        
        Returns data compatible with patterns/pattern_detector.py
        """
        profile_data = self.get_profile_data()
        if not profile_data:
            return {}
            
        # Identify key support/resistance levels
        support, resistance = self._find_support_resistance()
        
        # Return with canonical field names for pattern detection
        return {
            'poc_price': profile_data['poc_price'],
            'value_area_high': profile_data['value_area_high'],
            'value_area_low': profile_data['value_area_low'],
            'support_levels': support,
            'resistance_levels': resistance,
            'profile_strength': self._calculate_profile_strength(profile_data),
            'exchange_timestamp': datetime.now().isoformat()  # Canonical timestamp field
        }
    
    def _find_support_resistance(self) -> Tuple[List, List]:
        """Identify support and resistance levels from volume profile"""
        if len(self.price_volume) < 5:
            return [], []
            
        sorted_prices = sorted(self.price_volume.keys())
        volumes = [self.price_volume[p] for p in sorted_prices]
        
        # Find local volume maxima (potential S/R levels)
        support = []
        resistance = []
        
        for i in range(2, len(volumes) - 2):
            if (volumes[i] > volumes[i-1] and volumes[i] > volumes[i-2] and
                volumes[i] > volumes[i+1] and volumes[i] > volumes[i+2]):
                
                # High volume node - classify as support or resistance
                if sorted_prices[i] < self.get_profile_data().get('poc_price', 0):
                    support.append(sorted_prices[i])
                else:
                    resistance.append(sorted_prices[i])
        
        return support[:3], resistance[:3]  # Return top 3 each
    
    def _calculate_profile_strength(self, profile_data: Dict) -> float:
        """Calculate profile strength (0.0-1.0)"""
        if profile_data['total_volume'] == 0:
            return 0.0
            
        # Strength based on volume concentration
        poc_concentration = profile_data['poc_volume'] / profile_data['total_volume']
        price_levels_ratio = profile_data['price_levels'] / 100  # Normalize
        
        return min(poc_concentration * (1.0 - price_levels_ratio), 1.0)