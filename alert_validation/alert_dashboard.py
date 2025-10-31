#!/usr/bin/env python3
"""
Interactive Sent Alerts Dashboard with Real-time Data Visualization

A comprehensive Dash-based web dashboard for visualizing and analyzing sent trading alerts
from the intraday trading system. Displays alerts enriched with news, technical indicators,
and options Greeks, with interactive price charts and filtering capabilities.

Features:
    - Sent Alerts Visualization: Displays alerts that were successfully sent to Telegram/Redis
    - News Enrichment: Shows related news articles for each alert when available
    - Technical Indicators: Displays RSI, MACD, EMA, ATR, VWAP, Bollinger Bands for equity/futures
    - Options Greeks: Shows Delta, Gamma, Theta, Vega, Rho for options instruments
    - Price Charts: Interactive Plotly charts with price action and volume analysis
    - Instrument Filtering: Filters alerts to only intraday crawler instruments
    - Expiry Dates: Extracts and displays expiry dates for F&O instruments
    - Pattern Analysis: Shows pattern distribution and confidence scores
    - Summary Statistics: Alert counts, news enrichment rates, symbol rankings

Data Sources:
    - Redis DB 0: Alert data (alert:*, alerts:stream)
    - Redis DB 1: OHLC data, technical indicators, Greeks (ohlc_latest:*, indicators:*, greeks:*)
    - Token Lookup: Core data/token_lookup_enriched.json for instrument resolution

Usage:
    python alert_validation/alert_dashboard.py
    
    Access dashboard at: http://localhost:8050
    Network access: http://<local-ip>:8050
"""

import json
import pandas as pd
import numpy as np
import redis
from datetime import datetime, timedelta, date
from pathlib import Path
import re
import logging
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import sys
import os

logger = logging.getLogger(__name__)

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.redis_config import get_redis_config
from redis_files.redis_client import get_redis_client

class AlertValidationDashboard:
    def __init__(self):
        # Use shared Redis configuration
        redis_config = get_redis_config()
        self.redis_client = redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            password=redis_config.get('password'),
            db=0,
            decode_responses=True
        )
        # DB 1 for OHLC data
        self.redis_db1 = redis.Redis(
            host=redis_config.get('host', 'localhost'),
            port=redis_config.get('port', 6379),
            password=redis_config.get('password'),
            db=1,
            decode_responses=True
        )
        self.alerts_data = []
        self.price_data = []
        self.correlation_data = []
        self.intraday_instruments = set()  # Set of symbols from intraday crawler
        self.instrument_cache = {}
        self.expiry_map = {}  # Maps symbol -> expiry date
        self.all_patterns = []  # All available patterns/strategies
        
    def load_intraday_crawler_instruments(self):
        """Load instruments from binary_crawler1.json (intraday crawler) and resolve symbols"""
        try:
            crawler_config_file = Path(project_root) / 'crawlers' / 'binary_crawler1' / 'binary_crawler1.json'
            if not crawler_config_file.exists():
                print(f"❌ Intraday crawler config not found: {crawler_config_file}")
                return
            
            with open(crawler_config_file, 'r') as f:
                config_data = json.load(f)
            
            tokens = config_data.get('tokens', [])
            tokens = [int(token) if isinstance(token, str) else token for token in tokens]
            
            print(f"⚡ Loading {len(tokens)} intraday crawler instruments...")
            
            # Load token lookup for symbol resolution
            token_lookup_file = Path(project_root) / "core" / "data" / "token_lookup_enriched.json"
            token_lookup = {}
            if token_lookup_file.exists():
                with open(token_lookup_file, 'r') as fd:
                    token_lookup = json.load(fd)
            
            # Resolve tokens to symbols
            for token in tokens:
                token_str = str(token)
                if token_str in token_lookup:
                    data = token_lookup[token_str]
                    key_field = data.get('key', '')
                    if key_field:
                        # Extract symbol (remove exchange prefix if present)
                        symbol = key_field.split(':')[-1] if ':' in key_field else key_field
                        self.intraday_instruments.add(symbol)
                        self.intraday_instruments.add(key_field)  # Also add full key
            
            print(f"✅ Loaded {len(self.intraday_instruments)} intraday crawler instrument symbols")
        except Exception as e:
            print(f"⚠️ Error loading intraday crawler instruments: {e}")
    
    def parse_expiry_from_symbol(self, symbol: str) -> date:
        """Parse expiry date from symbol (e.g., NIFTY25NOV25850CE -> 2025-11-25)"""
        try:
            # Pattern: 2 digits + 3 letter month (e.g., "25NOV")
            match = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', symbol.upper())
            if match:
                day = int(match.group(1))
                month_str = match.group(2)
                month_map = {
                    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                }
                month = month_map.get(month_str)
                if month:
                    # Use current year (2025)
                    expiry_year = datetime.now().year
                    return date(expiry_year, month, day)
        except Exception:
            pass
        return None
    
    def load_instrument_cache(self):
        """Load instrument cache from Redis for symbol resolution"""
        try:
            inst_data = self.redis_client.get('instruments:master')
            if inst_data:
                self.instrument_cache = json.loads(inst_data)
                print(f"✅ Loaded instrument cache: {len(self.instrument_cache)} instruments")
                
                # Build expiry map for all instruments
                for token, inst_data in self.instrument_cache.items():
                    key_field = inst_data.get('key', '')
                    if key_field:
                        expiry_date = self.parse_expiry_from_symbol(key_field)
                        if expiry_date:
                            self.expiry_map[key_field] = expiry_date
                            # Also map without exchange prefix
                            symbol = key_field.split(':')[-1] if ':' in key_field else key_field
                            self.expiry_map[symbol] = expiry_date
                
                print(f"✅ Parsed {len(self.expiry_map)} instruments with expiry dates")
            else:
                print("⚠️ No instrument cache found in Redis")
                self.instrument_cache = {}
        except Exception as e:
            print(f"⚠️ Error loading instrument cache: {e}")
            self.instrument_cache = {}
    
    def load_all_patterns(self):
        """Load all available patterns/strategies from pattern registry"""
        try:
            # Load from pattern registry config
            pattern_registry_path = Path(__file__).parent.parent / "patterns" / "data" / "pattern_registry_config.json"
            if pattern_registry_path.exists():
                with open(pattern_registry_path, 'r') as f:
                    registry = json.load(f)
                    patterns = []
                    # Extract all patterns from categories
                    for category, category_data in registry.get('categories', {}).items():
                        if category_data.get('enabled', True):
                            patterns.extend(category_data.get('patterns', []))
                    # Also add NEWS_ALERT if not already present
                    if 'NEWS_ALERT' not in patterns:
                        patterns.append('NEWS_ALERT')
                    self.all_patterns = sorted(set(patterns))
                    print(f"✅ Loaded {len(self.all_patterns)} patterns from registry")
                    return
            
            # Fallback: Hardcoded list of all patterns
            self.all_patterns = sorted([
                # Core Patterns
                'volume_spike', 'volume_breakout', 'volume_price_divergence',
                'upside_momentum', 'downside_momentum', 'breakout', 'reversal',
                'hidden_accumulation',
                # ICT Patterns
                'ict_liquidity_pools', 'ict_fair_value_gaps', 'ict_optimal_trade_entry',
                'ict_premium_discount', 'ict_killzone', 'ict_momentum',
                # Straddle Strategies
                'kow_signal_straddle', 'iv_crush_play_straddle', 'range_bound_strangle',
                'market_maker_trap_detection', 'premium_collection_strategy',
                # Other
                'NEWS_ALERT', 'scalper_opportunity', 'spring_coil',
                'fake_bid_wall_basic', 'fake_ask_wall_basic'
            ])
            print(f"✅ Loaded {len(self.all_patterns)} patterns from fallback list")
        except Exception as e:
            logger.error(f"Error loading patterns: {e}")
            # Minimal fallback
            self.all_patterns = sorted(['volume_spike', 'volume_breakout', 'kow_signal_straddle', 'reversal', 'breakout', 'NEWS_ALERT'])
    
    def get_instrument_name(self, symbol: str) -> str:
        """Get human-readable instrument name from cache"""
        if not self.instrument_cache:
            return symbol
        
        # Try to find instrument by symbol in cache
        # Cache keys are tokens, values have 'name' and 'key' fields
        for token, inst_data in self.instrument_cache.items():
            cache_key = inst_data.get('key', '')
            cache_name = inst_data.get('name', '')
            
            # Match by key or name
            if symbol in cache_key or cache_key.endswith(symbol) or symbol.endswith(cache_name):
                return cache_name or symbol
        
        return symbol
    
    def is_intraday_instrument(self, symbol: str) -> bool:
        """Check if symbol is from intraday crawler"""
        if not self.intraday_instruments:
            return True  # If not loaded, allow all
        
        # Check various symbol formats
        symbol_variants = [
            symbol,
            symbol.split(':')[-1] if ':' in symbol else symbol,
            f"NFO:{symbol}" if not symbol.startswith('NFO:') else symbol,
            f"NSE:{symbol}" if not symbol.startswith('NSE:') else symbol
        ]
        
        for variant in symbol_variants:
            if variant in self.intraday_instruments:
                return True
        
        return False
    
    def is_option_instrument(self, symbol: str) -> bool:
        """Check if symbol is an options instrument (ends with CE/PE, with digits before it)"""
        symbol_upper = symbol.upper()
        # Remove exchange prefix if present
        clean_symbol = symbol_upper.split(':')[-1] if ':' in symbol_upper else symbol_upper
        
        # Options format: SYMBOL + expiry + strike + CE/PE (e.g., NIFTY25NOV25850CE)
        # Must end with CE or PE, and have digits immediately before CE/PE
        if clean_symbol.endswith('CE') or clean_symbol.endswith('PE'):
            # Check if there are digits before CE/PE (strike price)
            before_ce_pe = clean_symbol[:-2]  # Remove CE/PE
            if re.search(r'\d+$', before_ce_pe):  # Ends with digits
                return True
        return False
    
    def is_futures_instrument(self, symbol: str) -> bool:
        """Check if symbol is a futures instrument (ends with FUT)"""
        symbol_upper = symbol.upper()
        clean_symbol = symbol_upper.split(':')[-1] if ':' in symbol_upper else symbol_upper
        return clean_symbol.endswith('FUT')
    
    def load_indicators_for_symbol(self, symbol: str) -> dict:
        """Load technical indicators from Redis DB 1 for equity/futures"""
        indicators = {}
        try:
            # Normalize symbol for Redis key lookup
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
                f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol
            ]
            
            indicator_names = ['rsi', 'macd', 'bollinger_bands', 'ema_20', 'ema_50', 'atr', 'vwap']
            
            for variant in symbol_variants:
                found_any = False
                for indicator_name in indicator_names:
                    redis_key = f"indicators:{variant}:{indicator_name}"
                    value = self.redis_db1.get(redis_key)
                    
                    if value:
                        try:
                            # Try parsing as JSON (for complex indicators like MACD, BB)
                            parsed = json.loads(value)
                            if isinstance(parsed, dict) and 'value' in parsed:
                                indicators[indicator_name] = parsed['value']
                            else:
                                indicators[indicator_name] = parsed
                        except:
                            # Simple numeric value
                            try:
                                indicators[indicator_name] = float(value)
                            except:
                                indicators[indicator_name] = value
                        found_any = True
                
                if found_any:
                    break  # Found indicators for this variant
            
        except Exception as e:
            print(f"⚠️ Error loading indicators for {symbol}: {e}")
        
        return indicators
    
    def load_greeks_for_symbol(self, symbol: str) -> dict:
        """Load Greeks from Redis DB 1 for options"""
        greeks = {}
        try:
            # Try to load combined greeks first
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol
            ]
            
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho', 'greeks']
            
            for variant in symbol_variants:
                # Try combined greeks key first
                greeks_key = f"indicators:{variant}:greeks"
                greeks_data = self.redis_db1.get(greeks_key)
                
                if greeks_data:
                    try:
                        parsed = json.loads(greeks_data)
                        if isinstance(parsed, dict):
                            if 'value' in parsed:
                                greeks.update(parsed['value'])
                            else:
                                greeks.update(parsed)
                        break  # Found combined greeks
                    except:
                        pass
                
                # Fallback: try individual greek keys
                found_any = False
                for greek_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                    redis_key = f"indicators:{variant}:{greek_name}"
                    value = self.redis_db1.get(redis_key)
                    
                    if value:
                        try:
                            greeks[greek_name] = float(value)
                            found_any = True
                        except:
                            pass
                
                if found_any:
                    break
            
        except Exception as e:
            print(f"⚠️ Error loading Greeks for {symbol}: {e}")
        
        return greeks
    
    def get_expiry_date(self, symbol: str) -> str:
        """Get expiry date string for symbol"""
        expiry = self.expiry_map.get(symbol)
        if expiry is None:
            # Try without exchange prefix
            symbol_no_prefix = symbol.split(':')[-1] if ':' in symbol else symbol
            expiry = self.expiry_map.get(symbol_no_prefix)
        
        if expiry is not None:
            # Check if expired
            today = date.today()
            status = "✅ Active" if expiry >= today else "❌ Expired"
            return f"{expiry.strftime('%Y-%m-%d')} ({status})"
        
        return "N/A (Equity)"
    
    def load_data(self):
        """Load SENT alerts (not validation) and price data from Redis"""
        print("Loading SENT alerts from Redis...")
        
        # Load intraday crawler instruments first (must be before loading alerts)
        self.load_intraday_crawler_instruments()
        
        # Load all patterns/strategies
        self.load_all_patterns()
        
        # Load instrument cache for symbol resolution and expiry info
        self.load_instrument_cache()
        
        # Load SENT alerts (not validation alerts)
        # Check multiple possible alert storage locations
        alert_keys = []
        
        # Method 1: Check alert:* keys in DB 1 (where store_alert() stores them)
        try:
            db1_keys = self.redis_db1.keys("alert:*")
            alert_keys.extend([(key, 1) for key in db1_keys])  # (key, db_number)
            print(f"📊 Found {len(db1_keys)} alert:* keys in DB 1")
        except Exception as e:
            print(f"⚠️ Error checking DB 1 alerts: {e}")
        
        # Method 2: Check alerts:stream in DB 0 (stream of sent alerts)
        try:
            # Get recent messages from stream (last 1000)
            stream_messages = self.redis_client.xrevrange("alerts:stream", count=1000)
            alert_keys.extend([(f"stream:{msg_id}", 0) for msg_id, _ in stream_messages])
            print(f"📊 Found {len(stream_messages)} messages in alerts:stream")
        except Exception as e:
            print(f"⚠️ Error checking alerts:stream: {e}")
        
        # If no sent alerts found, fall back to validation alerts for testing
        if not alert_keys:
            print("⚠️ No sent alerts found, falling back to validation alerts")
            validation_keys = self.redis_client.keys("forward_validation:alert:*")
            alert_keys = [(key, 0) for key in validation_keys]
        
        # Process alert keys
        keys_to_process = [key for key, db in alert_keys]
        filtered_count = 0
        for key_with_db in alert_keys:
            key, db_num = key_with_db
            try:
                # Get data from appropriate DB
                if db_num == 1:
                    redis_client = self.redis_db1
                else:
                    redis_client = self.redis_client
                
                # Handle stream messages vs regular keys
                if key.startswith("stream:"):
                    # Extract message from stream
                    msg_id = key.replace("stream:", "")
                    messages = self.redis_client.xrange("alerts:stream", min=msg_id, max=msg_id, count=1)
                    if messages:
                        _, msg_data = messages[0]
                        # Stream messages may have 'data' field containing JSON string
                        if 'data' in msg_data:
                            try:
                                data = json.loads(msg_data['data'])
                            except (json.JSONDecodeError, TypeError):
                                # If data is already a dict, use it directly
                                data = msg_data['data'] if isinstance(msg_data['data'], dict) else msg_data
                        else:
                            # Use message data directly if no 'data' field
                            data = msg_data
                    else:
                        continue
                else:
                    # Regular key lookup
                    raw_data = redis_client.get(key)
                    if not raw_data:
                        continue
                    data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                # Parse timestamp safely
                timestamp_str = data.get('timestamp', '')
                try:
                    if 'Z' in timestamp_str:
                        alert_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    else:
                        alert_time = datetime.fromisoformat(timestamp_str)
                except:
                    alert_time = datetime.now()
                
                symbol = data.get('symbol', 'UNKNOWN')
                
                # Only include alerts for intraday crawler instruments
                if not self.is_intraday_instrument(symbol):
                    filtered_count += 1
                    continue
                
                instrument_name = self.get_instrument_name(symbol)
                expiry_info = self.get_expiry_date(symbol)
                
                # Load indicators or Greeks based on instrument type
                is_option = self.is_option_instrument(symbol)
                is_futures = self.is_futures_instrument(symbol)
                
                indicators = {}
                greeks = {}
                
                if is_option:
                    # Load Greeks for options
                    greeks = self.load_greeks_for_symbol(symbol)
                elif is_futures or not is_option:
                    # Load technical indicators for equity/futures
                    indicators = self.load_indicators_for_symbol(symbol)
                
                # Load news for this symbol if available
                news_items = []
                try:
                    # Use the public enrichment function
                    from alerts.news_enrichment_integration import enrich_alert_with_news
                    temp_alert = {'symbol': symbol}
                    enriched = enrich_alert_with_news(temp_alert, self.redis_client, lookback_minutes=60, top_k=5)
                    news_items = enriched.get('news', [])
                except Exception as e:
                    logger.debug(f"News fetch skipped for {symbol}: {e}")
                
                # Convert reasons list to string for DataTable compatibility
                reasons_list = data.get('reasons', [])
                reasons_str = ', '.join(str(r) for r in reasons_list) if isinstance(reasons_list, list) else str(reasons_list) if reasons_list else ''
                
                alert_info = {
                    'symbol': symbol,
                    'instrument_name': instrument_name,  # Human-readable name
                    'expiry': expiry_info,  # Expiry date with status
                    'pattern': data.get('pattern', data.get('pattern_type', 'unknown')),
                    'signal': data.get('signal', data.get('action', 'UNKNOWN')),
                    'confidence': float(data.get('confidence', 0.0)),
                    'entry_price': float(data.get('entry_price', data.get('last_price', 0.0))),
                    'expected_move': float(data.get('expected_move', 0.0)),
                    'timestamp': timestamp_str,
                    'alert_time': alert_time,
                    'time_of_day': alert_time.strftime('%H:%M:%S'),
                    # Optional validation fields (if present)
                    'validation_score': data.get('validation_score', None),
                    'is_valid': str(data.get('is_valid', '')) if data.get('is_valid') is not None else '',
                    'reasons': reasons_str,  # Convert list to string
                    'instrument_type': 'Options' if is_option else ('Futures' if is_futures else 'Equity'),
                    'has_news': len(news_items) > 0,  # Flag for news-driven alerts
                    'news_count': len(news_items),  # Count of news items
                    # Technical indicators (for equity/futures)
                    'rsi': indicators.get('rsi', None),
                    'macd': indicators.get('macd', {}).get('macd', None) if isinstance(indicators.get('macd'), dict) else None,
                    'bb_upper': indicators.get('bollinger_bands', {}).get('upper', None) if isinstance(indicators.get('bollinger_bands'), dict) else None,
                    'bb_middle': indicators.get('bollinger_bands', {}).get('middle', None) if isinstance(indicators.get('bollinger_bands'), dict) else None,
                    'bb_lower': indicators.get('bollinger_bands', {}).get('lower', None) if isinstance(indicators.get('bollinger_bands'), dict) else None,
                    'ema_20': indicators.get('ema_20', None),
                    'ema_50': indicators.get('ema_50', None),
                    'atr': indicators.get('atr', {}).get('atr', None) if isinstance(indicators.get('atr'), dict) else indicators.get('atr', None),
                    'vwap': indicators.get('vwap', {}).get('vwap', None) if isinstance(indicators.get('vwap'), dict) else indicators.get('vwap', None),
                    # Greeks (for options)
                    'delta': greeks.get('delta', None),
                    'gamma': greeks.get('gamma', None),
                    'theta': greeks.get('theta', None),
                    'vega': greeks.get('vega', None),
                    'rho': greeks.get('rho', None),
                    # News data (store as JSON string for display)
                    'news_data': json.dumps(news_items) if news_items else None,
                }
                self.alerts_data.append(alert_info)
            except Exception as e:
                print(f"Error loading validation alert {key}: {e}")
        
        if filtered_count > 0:
            print(f"⚡ Filtered out {filtered_count} alerts (not in intraday crawler instruments)")
        
        # Load price data from multiple sources
        symbols = list(set([alert['symbol'] for alert in self.alerts_data]))
        print(f"Loading price data for {len(symbols)} symbols from Redis...")
        
        # Try multiple data sources:
        # 1. OHLC latest data from DB 1 (most recent)
        # 2. Price buckets from DB 0 (historical)
        # 3. Use today's date dynamically
        
        today = datetime.now().date()
        dates_to_try = [
            today.strftime('%Y-%m-%d'),
            (today - timedelta(days=1)).strftime('%Y-%m-%d'),
            (today - timedelta(days=2)).strftime('%Y-%m-%d'),
        ]
        
        for symbol in symbols:
            # Method 1: Try OHLC latest data from DB 1
            try:
                # Normalize symbol for Redis key lookup
                normalized_symbol = symbol.replace(':', '').replace(' ', '').upper()
                ohlc_key = f"ohlc_latest:{normalized_symbol}"
                
                # Try different normalized formats
                ohlc_variants = [
                    ohlc_key,
                    f"ohlc_latest:{symbol}",
                    f"ohlc_latest:{symbol.replace('NFO:', '').replace('NSE:', '')}"
                ]
                
                ohlc_data = None
                for variant in ohlc_variants:
                    ohlc_data = self.redis_db1.hgetall(variant)
                    if ohlc_data:
                        break
                
                if ohlc_data:
                    try:
                        price_info = {
                            'symbol': symbol,
                            'open': float(ohlc_data.get('open', 0)),
                            'high': float(ohlc_data.get('high', 0)),
                            'low': float(ohlc_data.get('low', 0)),
                            'close': float(ohlc_data.get('close', 0)),
                            'volume': int(ohlc_data.get('volume', 0)),
                            'last_price': float(ohlc_data.get('last_price', ohlc_data.get('close', 0))),
                            'timestamp': datetime.now(),  # Use current time for latest data
                            'time_str': datetime.now().strftime('%H:%M:%S'),
                            'source': 'ohlc_latest'
                        }
                        self.price_data.append(price_info)
                        continue  # Skip bucket lookup if we have OHLC data
                    except Exception as e:
                        pass  # Fall through to bucket lookup
            except Exception as e:
                pass
            
            # Method 2: Try price buckets for recent dates
            for date_str in dates_to_try:
                try:
                    bucket_keys = self.redis_client.keys(f"bucket_incremental_volume:bucket:{symbol}:{date_str}:*")
                    if bucket_keys:
                        # Take the latest bucket for this symbol
                        for key in sorted(bucket_keys)[-10:]:  # Last 10 buckets
                            try:
                                data = json.loads(self.redis_client.get(key))
                                price_info = {
                                    'symbol': symbol,
                                    'hour': data.get('hour', 0),
                                    'minute_bucket': data.get('minute_bucket', 0),
                                    'open': data.get('open', 0),
                                    'high': data.get('high', 0),
                                    'low': data.get('low', 0),
                                    'close': data.get('close', 0),
                                    'volume': data.get('bucket_incremental_volume', 0),
                                    'timestamp': datetime.strptime(date_str, '%Y-%m-%d').replace(
                                        hour=data.get('hour', 0),
                                        minute=data.get('minute_bucket', 0)
                                    ),
                                    'time_str': f"{data.get('hour')}:{data.get('minute_bucket'):02d}",
                                    'source': 'bucket'
                                }
                                self.price_data.append(price_info)
                            except Exception as e:
                                pass
                        break  # Found data for this date
                except Exception:
                    continue
        
        print(f"Loaded {len(self.alerts_data)} alerts and {len(self.price_data)} price records")
        
    def create_price_chart(self, symbol, alert_time, entry_price):
        """Create a time series chart for a specific alert"""
        # Get price data for the symbol
        symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
        
        if not symbol_prices:
            return go.Figure()
        
        # Sort by timestamp
        symbol_prices.sort(key=lambda x: x['timestamp'])
        
        # Create time series data
        times = [p['timestamp'] for p in symbol_prices]
        closes = [p['close'] for p in symbol_prices]
        highs = [p['high'] for p in symbol_prices]
        lows = [p['low'] for p in symbol_prices]
        volumes = [p['volume'] for p in symbol_prices]
        
        # Find alert position in time series
        alert_timestamp = alert_time
        alert_index = None
        for i, t in enumerate(times):
            if t >= alert_timestamp:
                alert_index = i
                break
        
        # Create the chart
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.1,
            subplot_titles=(f'{symbol} - Price Movement', 'Volume'),
            row_heights=[0.7, 0.3]
        )
        
        # Price chart
        fig.add_trace(
            go.Scatter(
                x=times, y=closes,
                mode='lines+markers',
                name='Close Price',
                line=dict(color='blue', width=2),
                marker=dict(size=4)
            ),
            row=1, col=1
        )
        
        # High-Low range
        fig.add_trace(
            go.Scatter(
                x=times, y=highs,
                mode='lines',
                name='High',
                line=dict(color='green', width=1, dash='dash'),
                opacity=0.7
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=times, y=lows,
                mode='lines',
                name='Low',
                line=dict(color='red', width=1, dash='dash'),
                opacity=0.7,
                fill='tonexty',
                fillcolor='rgba(0,100,80,0.1)'
            ),
            row=1, col=1
        )
        
        # Alert line
        if alert_index is not None:
            fig.add_vline(
                x=alert_timestamp,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Alert: {entry_price}",
                annotation_position="top",
                row=1, col=1
            )
        
        # Entry price line
        fig.add_hline(
            y=entry_price,
            line_dash="dot",
            line_color="orange",
            annotation_text=f"Entry: {entry_price}",
            annotation_position="right",
            row=1, col=1
        )
        
        # Volume chart
        fig.add_trace(
            go.Bar(
                x=times, y=volumes,
                name='Volume',
                marker_color='lightblue',
                opacity=0.7
            ),
            row=2, col=1
        )
        
        # Calculate price movement
        if alert_index is not None and alert_index < len(closes):
            final_price = closes[-1]
            price_change = final_price - entry_price
            price_change_pct = (price_change / entry_price) * 100
            
            fig.add_annotation(
                x=times[-1], y=final_price,
                text=f"Final: {final_price:.2f}<br>Change: {price_change:+.2f} ({price_change_pct:+.2f}%)",
                showarrow=True,
                arrowhead=2,
                arrowcolor="green" if price_change > 0 else "red",
                row=1, col=1
            )
        
        # Update layout
        fig.update_layout(
            title=f'{symbol} - Alert at {alert_time.strftime("%H:%M:%S")}',
            xaxis_title='Time',
            yaxis_title='Price',
            height=600,
            showlegend=True,
            hovermode='x unified'
        )
        
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
        
        return fig
    
    def create_dashboard(self):
        """Create the Dash dashboard"""
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        
        # Convert to DataFrames
        df_alerts = pd.DataFrame(self.alerts_data)
        df_prices = pd.DataFrame(self.price_data)
        
        # Create summary statistics (robust for empty frames)
        total_alerts = len(df_alerts)
        unique_symbols = int(df_alerts['symbol'].nunique()) if 'symbol' in df_alerts.columns else 0
        try:
            avg_confidence_raw = df_alerts['confidence'].mean() if 'confidence' in df_alerts.columns else 0.0
            avg_confidence = float(0.0 if pd.isna(avg_confidence_raw) else avg_confidence_raw)
        except Exception:
            avg_confidence = 0.0
        
        # News-enriched alerts stats (sent alerts, not validation)
        if not df_alerts.empty and 'has_news' in df_alerts.columns:
            alerts_with_news = len(df_alerts[df_alerts['has_news'] == True])
        else:
            alerts_with_news = 0
        news_rate = (alerts_with_news / total_alerts * 100) if total_alerts > 0 else 0.0
        
        # Pattern distribution
        pattern_counts = df_alerts['pattern'].value_counts() if 'pattern' in df_alerts.columns else pd.Series(dtype=int)
        
        # Top symbols by alert count
        symbol_counts = (df_alerts['symbol'].value_counts().head(10)
                         if 'symbol' in df_alerts.columns else pd.Series(dtype=int))
        
        app.layout = dbc.Container(children=[
            dbc.Row([
                dbc.Col([
                    html.H1("📊 Sent Alerts Dashboard", className="text-center mb-4"),
                    html.P("Real-time sent alerts with news, indicators, and Greeks", className="text-center text-muted")
                ])
            ]),
            
            # Summary Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(f"{total_alerts}", className="card-title"),
                            html.P("Total Alerts", className="card-text")
                        ])
                    ], color="primary", outline=True)
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(f"{unique_symbols}", className="card-title"),
                            html.P("Unique Symbols", className="card-text")
                        ])
                    ], color="success", outline=True)
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(f"{avg_confidence:.1%}", className="card-title"),
                            html.P("Avg Confidence", className="card-text")
                        ])
                    ], color="info", outline=True)
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4(f"{news_rate:.1f}%", className="card-title"),
                            html.P("News-Enriched Alerts", className="card-text")
                        ])
                    ], color="warning", outline=True)
                ], width=3),
            ], className="mb-4"),
            
            # Controls
            dbc.Row([
                dbc.Col([
                    html.Label("Select Symbol:"),
                    dcc.Dropdown(
                        id='symbol-dropdown',
                        options=[{'label': symbol, 'value': symbol} for symbol in sorted(self.intraday_instruments)],
                        value=sorted(self.intraday_instruments)[0] if self.intraday_instruments else None,
                        clearable=True,
                        placeholder="All Symbols"
                    )
                ], width=4),
                dbc.Col([
                    html.Label("Select Pattern:"),
                    dcc.Dropdown(
                        id='pattern-dropdown',
                        options=[{'label': pattern, 'value': pattern} for pattern in sorted(self.all_patterns)],
                        value=None,
                        clearable=True,
                        placeholder="All Patterns"
                    )
                ], width=4),
                dbc.Col([
                    html.Label("Select Alert:"),
                    dcc.Dropdown(
                        id='alert-dropdown',
                        clearable=False
                    )
                ], width=4),
            ], className="mb-4"),
            
            # Price Chart
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id='price-chart')
                ], width=12)
            ], className="mb-4"),
            
            # News Section (always visible, content populated by callback)
            dbc.Row([
                dbc.Col([
                    html.Div(
                        id='news-display',
                        children=html.Div([
                            dbc.Alert("Select an alert to view related news (if available)", color="info", className="text-center")
                        ], style={"padding": "20px"})
                    )
                ], width=12)
            ], className="mb-4"),
            
            # Alert Details Table
            dbc.Row([
                dbc.Col([
                    html.H4("Alert Details"),
                    dash_table.DataTable(
                        id='alert-table',
                        columns=[
                            {"name": "Symbol", "id": "symbol"},
                            {"name": "Instrument Name", "id": "instrument_name"},
                            {"name": "Type", "id": "instrument_type"},
                            {"name": "Expiry", "id": "expiry"},
                            {"name": "Pattern", "id": "pattern"},
                            {"name": "Signal", "id": "signal"},
                            {"name": "Confidence", "id": "confidence", "type": "numeric", "format": {"specifier": ".1%"}},
                            {"name": "Entry Price", "id": "entry_price", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "Alert Time", "id": "time_of_day"},
                            {"name": "Expected Move", "id": "expected_move", "type": "numeric", "format": {"specifier": ".1%"}},
                            {"name": "Valid", "id": "is_valid", "type": "text", "hideable": True},
                            {"name": "Validation Score", "id": "validation_score", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "Reasons", "id": "reasons", "type": "text", "hideable": True},
                            # Technical Indicators (for equity/futures)
                            {"name": "RSI", "id": "rsi", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "MACD", "id": "macd", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "EMA 20", "id": "ema_20", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "EMA 50", "id": "ema_50", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "ATR", "id": "atr", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            {"name": "VWAP", "id": "vwap", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
                            # Greeks (for options)
                            {"name": "Delta", "id": "delta", "type": "numeric", "format": {"specifier": ".4f"}, "hideable": True},
                            {"name": "Gamma", "id": "gamma", "type": "numeric", "format": {"specifier": ".4f"}, "hideable": True},
                            {"name": "Theta", "id": "theta", "type": "numeric", "format": {"specifier": ".4f"}, "hideable": True},
                            {"name": "Vega", "id": "vega", "type": "numeric", "format": {"specifier": ".4f"}, "hideable": True},
                            {"name": "Rho", "id": "rho", "type": "numeric", "format": {"specifier": ".4f"}, "hideable": True},
                        ],
                        data=df_alerts.to_dict('records'),
                        sort_action="native",
                        filter_action="native",
                        page_action="native",
                        page_current=0,
                        page_size=20,
                        style_cell={'textAlign': 'left'},
                        style_header={'backgroundColor': 'rgb(230, 230, 230)', 'fontWeight': 'bold'}
                    )
                ], width=12)
            ], className="mb-4"),
            
            # Pattern Distribution + Top Symbols
            dbc.Row([
                dbc.Col(
                    dcc.Graph(
                        figure=go.Figure(
                            data=[go.Bar(x=list(pattern_counts.index), y=list(pattern_counts.values))],
                            layout=go.Layout(
                                title="Pattern Distribution",
                                xaxis_title="Pattern Type",
                                yaxis_title="Count"
                            )
                        )
                    ), width=6
                ),
                dbc.Col(
                    dcc.Graph(
                        figure=go.Figure(
                            data=[go.Bar(x=list(symbol_counts.index), y=list(symbol_counts.values))],
                            layout=go.Layout(
                                title="Top 10 Symbols by Alert Count",
                                xaxis_title="Symbol",
                                yaxis_title="Alert Count"
                            )
                        )
                    ), width=6
                )
            ], className="mb-4")
        ], fluid=True)
        
        
        
        # Callbacks
        @app.callback(
            Output('alert-dropdown', 'options'),
            Output('alert-dropdown', 'value'),
            Input('symbol-dropdown', 'value'),
            Input('pattern-dropdown', 'value')
        )
        def update_alert_dropdown(selected_symbol, selected_pattern):
            # Filter alerts based on symbol and pattern
            filtered_alerts = df_alerts.copy()
            
            if selected_symbol:
                filtered_alerts = filtered_alerts[filtered_alerts['symbol'] == selected_symbol]
            
            if selected_pattern:
                filtered_alerts = filtered_alerts[filtered_alerts['pattern'] == selected_pattern]
            
            options = []
            for idx, alert in filtered_alerts.iterrows():
                label = f"{alert['time_of_day']} - {alert['pattern']} - {alert['entry_price']:.2f}"
                options.append({'label': label, 'value': idx})
            
            return options, options[0]['value'] if options else None
        
        @app.callback(
            Output('price-chart', 'figure'),
            Input('alert-dropdown', 'value')
        )
        def update_price_chart(selected_alert_idx):
            try:
                if selected_alert_idx is not None and not df_alerts.empty:
                    # Handle both integer index and label-based indexing
                    if isinstance(selected_alert_idx, (int, float)) and selected_alert_idx in df_alerts.index:
                        alert = df_alerts.loc[selected_alert_idx]
                    elif isinstance(selected_alert_idx, (int, float)) and 0 <= int(selected_alert_idx) < len(df_alerts):
                        alert = df_alerts.iloc[int(selected_alert_idx)]
                    else:
                        # Try to find by index value
                        mask = df_alerts.index == selected_alert_idx
                        if mask.any():
                            alert = df_alerts[mask].iloc[0]
                        else:
                            return go.Figure()
                    
                    # Extract values safely
                    symbol = str(alert.get('symbol', '')) if 'symbol' in alert else ''
                    alert_time = alert.get('alert_time')
                    entry_price = float(alert.get('entry_price', alert.get('last_price', 0.0))) if 'entry_price' in alert or 'last_price' in alert else 0.0
                    
                    # Convert alert_time if it's a string
                    if isinstance(alert_time, str):
                        try:
                            if 'Z' in alert_time:
                                alert_time = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                            else:
                                alert_time = datetime.fromisoformat(alert_time)
                        except Exception:
                            # Fallback: use timestamp from alert
                            timestamp_str = alert.get('timestamp', '')
                            try:
                                if 'Z' in timestamp_str:
                                    alert_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                else:
                                    alert_time = datetime.fromisoformat(timestamp_str)
                            except Exception:
                                alert_time = datetime.now()
                    
                    if symbol and alert_time:
                        return self.create_price_chart(symbol, alert_time, entry_price)
                    else:
                        return go.Figure()
                return go.Figure()
            except Exception as e:
                logger.error(f"Error updating price chart: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return go.Figure()
        
        @app.callback(
            Output('news-display', 'children'),
            Input('alert-dropdown', 'value')
        )
        def update_news_display(selected_alert_idx):
            try:
                if selected_alert_idx is not None and not df_alerts.empty:
                    # Handle both integer index and label-based indexing (same as price chart)
                    if isinstance(selected_alert_idx, (int, float)) and selected_alert_idx in df_alerts.index:
                        alert = df_alerts.loc[selected_alert_idx]
                    elif isinstance(selected_alert_idx, (int, float)) and 0 <= int(selected_alert_idx) < len(df_alerts):
                        alert = df_alerts.iloc[int(selected_alert_idx)]
                    else:
                        # Try to find by index value
                        mask = df_alerts.index == selected_alert_idx
                        if mask.any():
                            alert = df_alerts[mask].iloc[0]
                        else:
                            return html.Div()
                    
                    # Check if alert has news
                    has_news = alert.get('has_news', False)
                    news_count = alert.get('news_count', 0)
                    
                    # Try multiple ways to get news data
                    news_items = None
                    
                    # Method 1: Check news_data field (stored as JSON string)
                    news_data_str = alert.get('news_data')
                    if news_data_str:
                        try:
                            if isinstance(news_data_str, str):
                                news_items = json.loads(news_data_str)
                            else:
                                news_items = news_data_str
                        except (json.JSONDecodeError, TypeError):
                            pass
                    
                    # Method 2: Try to fetch news on the fly if not stored
                    if (not news_items or len(news_items) == 0) and has_news:
                        try:
                            from alerts.news_enrichment_integration import enrich_alert_with_news
                            symbol = alert.get('symbol', '')
                            if symbol:
                                temp_alert = {'symbol': symbol}
                                enriched = enrich_alert_with_news(temp_alert, self.redis_client, lookback_minutes=60, top_k=5)
                                news_items = enriched.get('news', [])
                        except Exception as e:
                            logger.debug(f"Could not fetch news on-the-fly: {e}")
                    
                    # Display news if available
                    if news_items and len(news_items) > 0:
                        news_cards = []
                        for i, news in enumerate(news_items[:5], 1):  # Show top 5
                            headline = news.get('headline', news.get('title', 'No headline'))
                            source = news.get('news_source', news.get('source', 'Unknown source'))
                            sentiment = news.get('sentiment_score', 0.0)
                            url = news.get('url', news.get('link', ''))
                            timestamp = news.get('timestamp', '')
                            
                            # Format sentiment
                            try:
                                if isinstance(sentiment, str):
                                    sentiment_map = {'positive': 0.8, 'negative': -0.8, 'neutral': 0.0}
                                    sentiment_score = sentiment_map.get(sentiment.lower(), 0.0)
                                else:
                                    sentiment_score = float(sentiment)
                                
                                if sentiment_score > 0.3:
                                    sentiment_badge = dbc.Badge("🟢 Positive", color="success", className="me-2")
                                    border_color = "#28a745"
                                elif sentiment_score < -0.3:
                                    sentiment_badge = dbc.Badge("🔴 Negative", color="danger", className="me-2")
                                    border_color = "#dc3545"
                                else:
                                    sentiment_badge = dbc.Badge("🟡 Neutral", color="warning", className="me-2")
                                    border_color = "#ffc107"
                            except:
                                sentiment_badge = dbc.Badge("🟡 Neutral", color="warning", className="me-2")
                                sentiment_score = 0.0
                                border_color = "#ffc107"
                            
                            # Format timestamp if available
                            time_display = ""
                            if timestamp:
                                try:
                                    if isinstance(timestamp, str):
                                        if 'Z' in timestamp:
                                            ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                        else:
                                            ts = datetime.fromisoformat(timestamp)
                                    else:
                                        ts = datetime.fromtimestamp(float(timestamp))
                                    time_display = f" • {ts.strftime('%H:%M:%S')}"
                                except:
                                    pass
                            
                            news_card = dbc.Card([
                                dbc.CardBody([
                                    html.H5(f"{i}. {headline[:100]}{'...' if len(headline) > 100 else ''}", className="card-title", style={"font-size": "1.1rem"}),
                                    html.P([
                                        html.Strong("Source: "), source, html.Span(time_display, className="text-muted"),
                                        html.Br(),
                                        sentiment_badge,
                                        html.Small(f" Score: {sentiment_score:.2f}", className="text-muted ms-2")
                                    ], className="mb-2"),
                                    html.A("📰 Read more →", href=url, target="_blank", className="btn btn-sm btn-outline-primary") if url else html.P("No URL available", className="text-muted")
                                ])
                            ], className="mb-3", style={"border-left": f"4px solid {border_color}", "box-shadow": "0 2px 4px rgba(0,0,0,0.1)"})
                            
                            news_cards.append(news_card)
                        
                        return html.Div([
                            dbc.Card([
                                dbc.CardHeader([
                                    html.H4("📰 Related News", className="mb-0"),
                                    html.Small(f"{len(news_items)} news item(s) found", className="text-muted")
                                ], style={"background-color": "#e3f2fd", "border-bottom": "2px solid #2196F3"}),
                                dbc.CardBody(news_cards)
                            ], className="border-primary")
                        ], style={"margin-top": "20px", "margin-bottom": "20px"})
                    
                    # If has_news flag is True but no news items found
                    elif has_news or news_count > 0:
                        return html.Div([
                            dbc.Card([
                                dbc.CardHeader(html.H4("📰 Related News", className="mb-0")),
                                dbc.CardBody([
                                    html.P("News was detected for this alert but details are not available.", className="text-muted mb-0")
                                ])
                            ])
                        ], style={"margin-top": "20px", "margin-bottom": "20px"})
                    
                    # No news - show placeholder
                    return html.Div([
                        dbc.Alert([
                            html.H5("📰 Related News", className="mb-2"),
                            html.P("No news available for this alert.", className="mb-0 text-muted")
                        ], color="secondary")
                    ], style={"margin-top": "20px", "margin-bottom": "20px"})
                
                # No alert selected - show placeholder
                return html.Div([
                    dbc.Alert("Select an alert to view related news (if available)", color="info", className="text-center")
                ], style={"padding": "20px"})
                
            except Exception as e:
                logger.error(f"Error updating news display: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return html.Div([
                    dbc.Alert([
                        html.Strong("Error loading news: "), str(e)
                    ], color="danger")
                ], style={"margin-top": "20px", "margin-bottom": "20px"})
        
        return app
    
    def run_dashboard(self, port=8050, host='0.0.0.0'):
        """Run the dashboard"""
        self.load_data()
        app = self.create_dashboard()
        print(f"Starting dashboard on http://{host}:{port}")
        print(f"Access from network: http://192.168.1.38:{port}")
        app.run(debug=True, host=host, port=port)

if __name__ == "__main__":
    dashboard = AlertValidationDashboard()
    dashboard.run_dashboard(port=53056)
