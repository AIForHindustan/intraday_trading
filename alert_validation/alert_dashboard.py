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
    
    Access dashboard at:
    - Local: http://localhost:53056
    - Local Network: http://<local-ip>:53056
    - External (Public): https://remember-prefers-thinkpad-distributors.trycloudflare.com
    
    CRITICAL: Port 53056 and host 0.0.0.0 are FIXED for consumer access.
    External access is provided via Cloudflare Tunnel (primary) with Mullvad SSH fallback.
    To maintain external access, ensure tunnel manager is running:
    python scripts/tunnel_manager.py daemon
    DO NOT change port/host values without explicit approval.
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
        # Use shared Redis configuration with get_redis_client (includes decode_responses support)
        redis_config = get_redis_config()
        # Use get_redis_client with decode_responses=True for JSON/indicator reads
        self.redis_client = get_redis_client(db=0, decode_responses=True)
        self.redis_db1 = get_redis_client(db=1, decode_responses=True)
        # DB 2 is analytics database where volume profile is stored
        try:
            self.redis_db2 = get_redis_client(db=2, decode_responses=True)
        except Exception:
            self.redis_db2 = self.redis_db1  # Fallback to DB 1
        # DB 4 and DB 5 may also contain indicators/Greeks (user confirmed)
        try:
            self.redis_db4 = get_redis_client(db=4, decode_responses=True)
            self.redis_db5 = get_redis_client(db=5, decode_responses=True)
        except Exception:
            # If DB 4/5 don't exist, create dummy clients (methods will handle gracefully)
            self.redis_db4 = self.redis_db1  # Fallback to DB 1
            self.redis_db5 = self.redis_db1  # Fallback to DB 1
        self.alerts_data = []
        self.price_data = []
        self.correlation_data = []
        self.intraday_instruments = set()  # Set of symbols from intraday crawler (standardized, no duplicates)
        self.intraday_instruments_full = {}  # Maps base symbol -> full key (e.g., "NIFTY25NOV25850CE" -> "NFO:NIFTY25NOV25850CE")
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
            
            # Resolve tokens to symbols - STANDARDIZED: Only base symbols (no exchange prefix)
            # Create mapping for lookup: base_symbol -> full_key
            for token in tokens:
                token_str = str(token)
                if token_str in token_lookup:
                    data = token_lookup[token_str]
                    key_field = data.get('key', '')
                    if key_field:
                        # Extract base symbol (remove exchange prefix if present)
                        base_symbol = key_field.split(':')[-1] if ':' in key_field else key_field
                        
                        # Only add base symbol to instruments set (deduplicated)
                        self.intraday_instruments.add(base_symbol)
                        
                        # Store mapping: base_symbol -> full_key (for lookup when needed)
                        if base_symbol not in self.intraday_instruments_full:
                            self.intraday_instruments_full[base_symbol] = key_field
            
            print(f"✅ Loaded {len(self.intraday_instruments)} unique intraday crawler instrument symbols (standardized, no duplicates)")
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
        """Get human-readable instrument name from cache or token lookup"""
        if not symbol or symbol == "UNKNOWN":
            return symbol
        
        # First try: Check if symbol itself is a token number
        if symbol.isdigit():
            try:
                from crawlers.utils.instrument_mapper import InstrumentMapper
                mapper = InstrumentMapper()
                resolved = mapper.token_to_symbol(int(symbol))
                if resolved and not resolved.startswith("UNKNOWN_"):
                    return resolved
            except Exception as e:
                logger.debug(f"Token resolution failed for {symbol}: {e}")
        
        # Second try: Check instrument cache (from Redis instruments:master)
        if self.instrument_cache:
            # Normalize symbol for matching (handle exchange prefixes)
            symbol_base = symbol.split(':')[-1] if ':' in symbol else symbol
            symbol_variants = [
                symbol,  # Full symbol with prefix (e.g., "NSE:RELIANCE")
                symbol_base,  # Base symbol (e.g., "RELIANCE")
                f"NSE:{symbol_base}",  # With NSE prefix
                f"NFO:{symbol_base}",  # With NFO prefix
            ]
            
            # Try exact match first
            for token, inst_data in self.instrument_cache.items():
                cache_key = inst_data.get('key', '')
                cache_name = inst_data.get('name', '')
                
                # Exact match
                for variant in symbol_variants:
                    if cache_key == variant or cache_key == symbol:
                        return cache_name or cache_key or symbol
                
                # Partial match (symbol is in key or vice versa)
                cache_key_base = cache_key.split(':')[-1] if ':' in cache_key else cache_key
                for variant in symbol_variants:
                    if variant == cache_key_base or variant == cache_name:
                        return cache_name or cache_key or symbol
            
            # Try fuzzy matching as last resort
            for token, inst_data in self.instrument_cache.items():
                cache_key = inst_data.get('key', '')
                cache_name = inst_data.get('name', '')
                cache_key_base = cache_key.split(':')[-1] if ':' in cache_key else cache_key
                
                if symbol_base.upper() == cache_key_base.upper() or symbol_base.upper() == cache_name.upper():
                    return cache_name or cache_key or symbol
        
        # Third try: Use InstrumentMapper for token lookup
        try:
            from crawlers.utils.instrument_mapper import InstrumentMapper
            mapper = InstrumentMapper()
            # Try to find by symbol in token metadata
            for token, metadata in mapper.export_token_metadata().items():
                tradingsymbol = metadata.get('tradingsymbol') or metadata.get('symbol') or metadata.get('name')
                if tradingsymbol and (symbol == tradingsymbol or symbol.endswith(tradingsymbol) or tradingsymbol.endswith(symbol)):
                    return metadata.get('name') or tradingsymbol or symbol
        except Exception as e:
            logger.debug(f"InstrumentMapper lookup failed: {e}")
        
        # Fallback: Return symbol as-is
        return symbol
    
    def is_intraday_instrument(self, symbol: str) -> bool:
        """Check if symbol is from intraday crawler - ONLY binary crawler instruments"""
        # If intraday_instruments is empty or not loaded, show ALL alerts (no filtering)
        if not self.intraday_instruments or len(self.intraday_instruments) == 0:
            return True  # Allow all if no crawler config loaded
        
        # STANDARDIZE: Normalize symbol to base symbol (remove exchange prefix)
        # intraday_instruments now only contains base symbols (no prefixes)
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        
        # Check against standardized base symbols only
        return base_symbol in self.intraday_instruments
    
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
        """
        Load technical indicators from Redis using correct key patterns.
        
        Redis Storage Schema (from redis_storage.py):
        - DB 1 (realtime): indicators:{symbol}:{indicator_name} via analysis_cache data type
        - Format: Simple indicators (RSI, EMA, ATR, VWAP) stored as string/number
                  Complex indicators (MACD, BB) stored as JSON with {'value': {...}, 'timestamp': ..., 'symbol': ...}
        - Additional DBs: DB 4 and DB 5 may also contain indicators (fallback)
        """
        indicators = {}
        try:
            # Normalize symbol for Redis key lookup
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol and 'NIFTY' in symbol.upper() or 'BANKNIFTY' in symbol.upper() else symbol,
                f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # ✅ COMPREHENSIVE INDICATOR LIST: All indicators calculated by HybridCalculations
            indicator_names = [
                # Basic indicators
                'rsi', 'atr', 'vwap', 'volume_ratio', 'price_change',
                # EMAs (all windows)
                'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
                # Complex indicators (stored as JSON)
                'macd', 'bollinger_bands', 'volume_profile',
            ]
            
            # Check multiple databases: DB 1 (primary), DB 4, DB 5 (fallbacks)
            redis_clients = [
                ('DB1', self.redis_db1),  # Primary location (realtime)
                ('DB4', self.redis_db4),  # Fallback
                ('DB5', self.redis_db5),  # Fallback
            ]
            
            for variant in symbol_variants:
                found_any = False
                for db_name, redis_client in redis_clients:
                    for indicator_name in indicator_names:
                        # ✅ PRIMARY KEY FORMAT: indicators:{symbol}:{indicator_name}
                        # This is the format used by redis_storage.publish_indicators_to_redis()
                        redis_key = f"indicators:{variant}:{indicator_name}"
                        
                        try:
                            value = redis_client.get(redis_key)
                            
                            if value:
                                try:
                                    # Handle bytes response
                                    if isinstance(value, bytes):
                                        value = value.decode('utf-8')
                                    
                                    # Try parsing as JSON first (for complex indicators)
                                    try:
                                        parsed = json.loads(value)
                                        if isinstance(parsed, dict):
                                            # Check for nested structure with 'value' field (from redis_storage)
                                            if 'value' in parsed:
                                                indicators[indicator_name] = parsed['value']
                                            else:
                                                indicators[indicator_name] = parsed
                                        else:
                                            indicators[indicator_name] = parsed
                                    except (json.JSONDecodeError, TypeError):
                                        # Simple numeric/string value (RSI, EMA, ATR, VWAP, etc.)
                                        try:
                                            indicators[indicator_name] = float(value)
                                        except (ValueError, TypeError):
                                            indicators[indicator_name] = value
                                    found_any = True
                                    logger.debug(f"✅ Loaded {indicator_name} for {variant} from {db_name}")
                                except Exception as e:
                                    logger.debug(f"Error parsing indicator {indicator_name} for {variant} from {db_name}: {e}")
                        except Exception as e:
                            logger.debug(f"Error getting indicator {indicator_name} for {variant} from {db_name}: {e}")
                    
                    if found_any:
                        break  # Found indicators for this variant
                
                if found_any:
                    break  # Found indicators for this variant
            
            # Debug: log results
            if not indicators:
                logger.debug(f"⚠️ No indicators found for {symbol} in Redis")
            else:
                logger.debug(f"✅ Found {len(indicators)} indicators for {symbol}: {list(indicators.keys())[:5]}")
            
        except Exception as e:
            logger.error(f"⚠️ Error loading indicators for {symbol}: {e}")
        
        return indicators
    
    def _extract_indicator_value(self, data_source: dict, key: str):
        """
        Extract indicator value from data source, handling both nested and top-level formats.
        
        Handles:
        - Top-level: data_source['rsi'] = 65.5
        - Nested indicators: data_source['indicators']['rsi'] = 65.5
        - Nested with 'value': data_source['indicators']['rsi'] = {'value': 65.5}
        """
        if not data_source or not isinstance(data_source, dict):
            return None
        
        # First try direct key access (top-level format, e.g., data['rsi'])
        value = data_source.get(key)
        if value is not None:
            if isinstance(value, dict):
                # If value is a dict, try to get 'value' key
                return value.get('value') or value.get(key)
            try:
                return float(value) if value != '' else None
            except (ValueError, TypeError):
                return value
        
        # Then try nested indicators dict (e.g., data['indicators']['rsi'])
        indicators = data_source.get('indicators', {})
        if isinstance(indicators, dict):
            value = indicators.get(key)
            if value is not None:
                if isinstance(value, dict):
                    # Try common nested keys
                    return value.get('value') or value.get(key) or value.get(str(key).upper())
                try:
                    return float(value) if value != '' else None
                except (ValueError, TypeError):
                    return value
        
        return None
    
    def _extract_macd_value(self, data_source: dict):
        """
        Extract MACD value from data source, handling multiple formats.
        Checks both indicators dict and top-level fields.
        """
        if not data_source or not isinstance(data_source, dict):
            return None
        
        # Try direct key access first (top-level: data['macd'])
        macd_data = data_source.get('macd')
        
        # If not found, try nested indicators dict
        if macd_data is None:
            indicators = data_source.get('indicators', {})
            if isinstance(indicators, dict):
                macd_data = indicators.get('macd')
        
        if macd_data is None:
            return None
        
        if isinstance(macd_data, dict):
            # MACD can have structure: {'macd': 1.23, 'signal': 1.20, 'histogram': 0.03}
            return macd_data.get('macd') or macd_data.get('value')
        try:
            return float(macd_data) if macd_data != '' else None
        except (ValueError, TypeError):
            return macd_data
    
    def _extract_bb_value(self, data_source: dict, bb_key: str):
        """
        Extract Bollinger Bands value (upper/middle/lower) from data source.
        Checks both indicators dict and top-level fields.
        """
        if not data_source or not isinstance(data_source, dict):
            return None
        
        # Try top-level field first (e.g., data['bb_upper'])
        bb_key_direct = f'bb_{bb_key}'
        value = data_source.get(bb_key_direct)
        if value is not None:
            try:
                return float(value) if value != '' else None
            except (ValueError, TypeError):
                return value
        
        # Try nested bollinger_bands dict
        bb_data = data_source.get('bollinger_bands')
        if bb_data is None:
            # Try indicators dict
            indicators = data_source.get('indicators', {})
            if isinstance(indicators, dict):
                bb_data = indicators.get('bollinger_bands')
        
        if bb_data is None:
            return None
        
        if isinstance(bb_data, dict):
            return bb_data.get(bb_key) or bb_data.get(bb_key.upper())
        return None
    
    def load_greeks_for_symbol(self, symbol: str) -> dict:
        """
        Load Options Greeks from Redis using correct key patterns.
        
        Redis Storage Schema (from redis_storage.py):
        - DB 1 (realtime): indicators:{symbol}:greeks (combined dict) and indicators:{symbol}:{greek} (individual)
        - Format: Combined stored as JSON with {'value': {'delta': ..., 'gamma': ..., ...}, ...}
                  Individual Greeks stored as string/number
        - Calculated by: EnhancedGreekCalculator.black_scholes_greeks() using scipy.stats.norm (pure Python)
        """
        greeks = {}
        try:
            # Normalize symbol for Redis key lookup
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # Greek names (from EnhancedGreekCalculator)
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
            
            # Check multiple databases: DB 1 (primary), DB 4, DB 5 (fallbacks)
            redis_clients = [
                ('DB1', self.redis_db1),  # Primary location (realtime)
                ('DB4', self.redis_db4),  # Fallback
                ('DB5', self.redis_db5),  # Fallback
            ]
            
            for variant in symbol_variants:
                # ✅ METHOD 1: Try combined greeks key first (preferred - more efficient)
                # Key format: indicators:{symbol}:greeks
                # Stored by redis_storage.publish_indicators_to_redis() as JSON
                greeks_key = f"indicators:{variant}:greeks"
                
                for db_name, redis_client in redis_clients:
                    try:
                        greeks_data = redis_client.get(greeks_key)
                        
                        if greeks_data:
                            try:
                                # Handle bytes
                                if isinstance(greeks_data, bytes):
                                    greeks_data = greeks_data.decode('utf-8')
                                
                                # Parse JSON
                                parsed = json.loads(greeks_data) if isinstance(greeks_data, str) else greeks_data
                                if isinstance(parsed, dict):
                                    # Check for nested structure with 'value' field (from redis_storage)
                                    if 'value' in parsed:
                                        greeks.update(parsed['value'])
                                    else:
                                        greeks.update(parsed)
                                logger.debug(f"✅ Loaded combined Greeks for {variant} from {db_name}")
                                break  # Found combined greeks
                            except Exception as e:
                                logger.debug(f"Error parsing combined Greeks for {variant} from {db_name}: {e}")
                    except Exception as e:
                        logger.debug(f"Error getting combined Greeks for {variant} from {db_name}: {e}")
                
                if greeks:
                    break  # Found combined greeks
                
                # ✅ METHOD 2: Try individual greek keys (fallback if combined not found)
                # Key format: indicators:{symbol}:{greek_name}
                # Stored by redis_storage.publish_indicators_to_redis() as individual values
                found_any = False
                for db_name, redis_client in redis_clients:
                    for greek_name in greek_names:
                        greek_key = f"indicators:{variant}:{greek_name}"
                        
                        try:
                            greek_value = redis_client.get(greek_key)
                            
                            if greek_value:
                                try:
                                    # Handle bytes
                                    if isinstance(greek_value, bytes):
                                        greek_value = greek_value.decode('utf-8')
                                    
                                    # Parse as float (Greeks are numeric)
                                    try:
                                        greeks[greek_name] = float(greek_value)
                                        found_any = True
                                        logger.debug(f"✅ Loaded {greek_name} for {variant} from {db_name}")
                                    except (ValueError, TypeError):
                                        # Try JSON parse in case it's wrapped
                                        try:
                                            parsed = json.loads(greek_value)
                                            if isinstance(parsed, dict) and 'value' in parsed:
                                                greeks[greek_name] = float(parsed['value'])
                                            else:
                                                greeks[greek_name] = float(parsed)
                                            found_any = True
                                        except:
                                            pass
                                except Exception as e:
                                    logger.debug(f"Error parsing {greek_name} for {variant} from {db_name}: {e}")
                        except Exception as e:
                            logger.debug(f"Error getting {greek_name} for {variant} from {db_name}: {e}")
                    
                    if found_any:
                        break  # Found individual Greeks
                
                if found_any or greeks:
                    break  # Found Greeks for this variant
            
            # Debug: log results
            if not greeks:
                logger.debug(f"⚠️ No Greeks found for {symbol} in Redis")
            else:
                logger.debug(f"✅ Found {len(greeks)} Greeks for {symbol}: {list(greeks.keys())}")
            
        except Exception as e:
            logger.error(f"⚠️ Error loading Greeks for {symbol}: {e}")
        
        return greeks
    
    def load_volume_profile_for_symbol(self, symbol: str) -> dict:
        """Load Volume Profile data (POC, Value Area, Distribution) from Redis DB 2 (analytics)"""
        volume_profile = {}
        try:
            # Normalize symbol (remove exchange prefix)
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            symbol_variants = [symbol, base_symbol, f"NFO:{base_symbol}", f"NSE:{base_symbol}"]
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            for variant in symbol_variants:
                # Method 1: Try volume_profile:poc:SYMBOL (Hash with POC/VA for quick access)
                poc_key = f"volume_profile:poc:{variant}"
                poc_data = None
                
                # Try DB 2 (analytics) first - where volume profile is stored
                try:
                    if self.redis_db2:
                        poc_data = self.redis_db2.hgetall(poc_key)
                except:
                    pass
                
                # Fallback: Try DB 1
                if not poc_data or len(poc_data) == 0:
                    try:
                        poc_data = self.redis_db1.hgetall(poc_key)
                    except:
                        pass
                
                if poc_data:
                    # Handle bytes keys/values
                    if poc_data and isinstance(poc_data, dict):
                        poc_price_val = poc_data.get('poc_price') or poc_data.get(b'poc_price')
                        poc_vol_val = poc_data.get('poc_volume') or poc_data.get(b'poc_volume')
                        vah_val = poc_data.get('value_area_high') or poc_data.get(b'value_area_high')
                        val_val = poc_data.get('value_area_low') or poc_data.get(b'value_area_low')
                        
                        if poc_price_val:
                            if isinstance(poc_price_val, bytes):
                                poc_price_val = poc_price_val.decode('utf-8')
                            volume_profile['poc_price'] = float(poc_price_val)
                        if poc_vol_val:
                            if isinstance(poc_vol_val, bytes):
                                poc_vol_val = poc_vol_val.decode('utf-8')
                            volume_profile['poc_volume'] = float(poc_vol_val)
                        if vah_val:
                            if isinstance(vah_val, bytes):
                                vah_val = vah_val.decode('utf-8')
                            volume_profile['value_area_high'] = float(vah_val)
                        if val_val:
                            if isinstance(val_val, bytes):
                                val_val = val_val.decode('utf-8')
                            volume_profile['value_area_low'] = float(val_val)
                        logger.debug(f"✅ Loaded POC data from {poc_key}: POC={volume_profile.get('poc_price')}")
                    break
                
                # Method 2: Try indicators:SYMBOL:poc_price (from redis_storage)
                poc_indicator_key = f"indicators:{variant}:poc_price"
                for db_client in [self.redis_db2, self.redis_db1]:
                    try:
                        poc_value = db_client.get(poc_indicator_key)
                        if poc_value:
                            if isinstance(poc_value, bytes):
                                poc_value = poc_value.decode('utf-8')
                            volume_profile['poc_price'] = float(poc_value)
                            logger.debug(f"✅ Loaded POC from indicator key: {poc_indicator_key}")
                            break
                    except:
                        continue
                
                # Method 3: Try analysis_cache:indicators:SYMBOL:poc_price
                if 'poc_price' not in volume_profile:
                    poc_cache_key = f"analysis_cache:indicators:{variant}:poc_price"
                    for db_client in [self.redis_db2, self.redis_db1]:
                        try:
                            poc_value = db_client.get(poc_cache_key)
                            if poc_value:
                                if isinstance(poc_value, bytes):
                                    poc_value = poc_value.decode('utf-8')
                                volume_profile['poc_price'] = float(poc_value)
                                logger.debug(f"✅ Loaded POC from cache key: {poc_cache_key}")
                                break
                        except:
                            continue
                
                # Method 4: Try volume_profile:distribution:SYMBOL:YYYY-MM-DD (price-volume buckets)
                distribution_key = f"volume_profile:distribution:{variant}:{today_str}"
                distribution_data = None
                try:
                    if self.redis_db2:
                        distribution_data = self.redis_db2.hgetall(distribution_key)
                except:
                    pass
                
                if not distribution_data or len(distribution_data) == 0:
                    try:
                        distribution_data = self.redis_db1.hgetall(distribution_key)
                    except:
                        pass
                
                if distribution_data:
                    # Convert price-volume distribution to dict
                    price_volume_dist = {}
                    for price_key, volume_val in distribution_data.items():
                        try:
                            price_str = price_key.decode('utf-8') if isinstance(price_key, bytes) else str(price_key)
                            volume_str = volume_val.decode('utf-8') if isinstance(volume_val, bytes) else str(volume_val)
                            price = float(price_str)
                            volume = int(float(volume_str))
                            price_volume_dist[price] = volume
                        except:
                            continue
                    
                    if price_volume_dist:
                        volume_profile['distribution'] = price_volume_dist
                        # Calculate POC from distribution if not already found
                        if 'poc_price' not in volume_profile and price_volume_dist:
                            poc_price = max(price_volume_dist.items(), key=lambda x: x[1])[0]
                            volume_profile['poc_price'] = poc_price
                            volume_profile['poc_volume'] = price_volume_dist[poc_price]
                        logger.debug(f"✅ Loaded volume profile distribution: {len(price_volume_dist)} price levels")
                    break
            
            if volume_profile:
                logger.info(f"📊 Loaded Volume Profile for {symbol}: POC={volume_profile.get('poc_price')}, VA=[{volume_profile.get('value_area_low')}, {volume_profile.get('value_area_high')}]")
            else:
                logger.debug(f"⚠️ No volume profile data found for {symbol}")
                
        except Exception as e:
            logger.error(f"Error loading volume profile for {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        return volume_profile
    
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
        
        # Method 1: Check alerts:stream in DB 1 (primary source - where RedisNotifier publishes)
        try:
            # Get recent messages from stream (last 1000) and process them directly
            stream_messages = self.redis_db1.xrevrange("alerts:stream", count=1000)
            print(f"📊 Found {len(stream_messages)} messages in alerts:stream (DB 1)")
            # Process stream messages directly instead of storing as keys
            for msg_id, msg_data in stream_messages:
                try:
                    # Handle both 'data' and b'data' field keys (decode_responses=True may still return bytes)
                    data_field = None
                    if 'data' in msg_data:
                        data_field = msg_data['data']
                    elif b'data' in msg_data:
                        data_field = msg_data[b'data']
                    
                    if data_field:
                        # Handle bytes data (written with orjson)
                        if isinstance(data_field, bytes):
                            try:
                                import orjson
                                data = orjson.loads(data_field)
                                alert_keys.append((('stream_direct', data), 1))
                                continue
                            except Exception:
                                # Try UTF-8 decode then json
                                try:
                                    data_field = data_field.decode('utf-8')
                                    data = json.loads(data_field)
                                    alert_keys.append((('stream_direct', data), 1))
                                    continue
                                except Exception:
                                    pass
                        
                        # Handle string data
                        if isinstance(data_field, str):
                            try:
                                data = json.loads(data_field)
                                alert_keys.append((('stream_direct', data), 1))
                            except (json.JSONDecodeError, TypeError):
                                # Try orjson
                                try:
                                    import orjson
                                    data = orjson.loads(data_field.encode('utf-8'))
                                    alert_keys.append((('stream_direct', data), 1))
                                except Exception:
                                    pass
                    else:
                        # No 'data' field, use message data directly
                        alert_keys.append((('stream_direct', msg_data), 1))
                except Exception as e:
                    print(f"⚠️ Error processing stream message {msg_id}: {e}")
                    continue
        except Exception as e:
            print(f"⚠️ Error checking alerts:stream in DB 1: {e}")
        
        # Method 2: Check alert:* keys in DB 1 (backup source)
        try:
            db1_keys = self.redis_db1.keys("alert:*")
            alert_keys.extend([(key, 1) for key in db1_keys])  # (key, db_number)
            print(f"📊 Found {len(db1_keys)} alert:* keys in DB 1")
        except Exception as e:
            print(f"⚠️ Error checking DB 1 alerts: {e}")
        
        # Method 3: Check alerts:stream in DB 0 (legacy fallback)
        try:
            stream_messages = self.redis_client.xrevrange("alerts:stream", count=100)
            alert_keys.extend([(f"stream:{msg_id}", 0) for msg_id, _ in stream_messages])
            print(f"📊 Found {len(stream_messages)} messages in alerts:stream (DB 0 - legacy)")
        except Exception as e:
            print(f"⚠️ Error checking alerts:stream in DB 0: {e}")
        
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
                
                # Handle direct stream data (already parsed from stream)
                if isinstance(key, tuple) and key[0] == 'stream_direct':
                    data = key[1]  # Data already parsed, use directly
                # Handle stream messages by ID (legacy path)
                elif key.startswith("stream:"):
                    # Extract message from stream - use xrevrange to get the actual message
                    # The msg_id from xrevrange is already the actual message ID, we need to fetch it
                    try:
                        msg_id_str = key.replace("stream:", "")
                        # Parse msg_id - it's a string like "1761840899712-0"
                        # Use xrange to get the message, or better - use xrevrange and match
                        messages = None
                        if db_num == 1:
                            # Get all recent messages and find matching one
                            all_msgs = self.redis_db1.xrevrange("alerts:stream", count=1000)
                            for mid, mdata in all_msgs:
                                if str(mid) == msg_id_str or mid == msg_id_str:
                                    messages = [(mid, mdata)]
                                    break
                        else:
                            all_msgs = self.redis_client.xrevrange("alerts:stream", count=100)
                            for mid, mdata in all_msgs:
                                if str(mid) == msg_id_str or mid == msg_id_str:
                                    messages = [(mid, mdata)]
                                    break
                        
                        if messages:
                            _, msg_data = messages[0]
                            # Handle decode_responses - data might be bytes or string
                            # Stream messages have 'data' field containing JSON string or binary
                            if 'data' in msg_data or b'data' in msg_data:
                                data_field = msg_data.get('data') or msg_data.get(b'data')
                                
                                # If binary, decode it
                                if isinstance(data_field, bytes):
                                    try:
                                        data_field = data_field.decode('utf-8')
                                    except Exception:
                                        pass
                                
                                # Parse JSON
                                if isinstance(data_field, str):
                                    try:
                                        data = json.loads(data_field)
                                    except (json.JSONDecodeError, TypeError):
                                        # Try orjson if available
                                        try:
                                            import orjson
                                            data = orjson.loads(data_field)
                                        except Exception:
                                            # Fallback: treat as dict if already parsed
                                            data = msg_data.get('data') or msg_data
                                else:
                                    data = data_field if data_field else msg_data
                            else:
                                # Use message data directly if no 'data' field
                                data = msg_data
                        else:
                            continue
                    except Exception as e:
                        print(f"⚠️ Error parsing stream message {key}: {e}")
                        continue
                else:
                    # Regular key lookup
                    raw_data = redis_client.get(key)
                    if not raw_data:
                        continue
                    data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                # Parse timestamp safely - CRITICAL: Use actual alert timestamp, not current time
                timestamp_str = data.get('timestamp', '')
                alert_time = None
                try:
                    if timestamp_str:
                        if 'Z' in timestamp_str:
                            alert_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        elif 'T' in timestamp_str:
                            alert_time = datetime.fromisoformat(timestamp_str.split('.')[0])
                        else:
                            alert_time = datetime.fromisoformat(timestamp_str)
                    else:
                        # Try alternative timestamp fields
                        for ts_field in ['alert_timestamp', 'created_at', 'time', 'event_time']:
                            if ts_field in data and data[ts_field]:
                                ts_val = data[ts_field]
                                if isinstance(ts_val, (int, float)):
                                    alert_time = datetime.fromtimestamp(float(ts_val) / 1000.0 if ts_val > 1e10 else ts_val)
                                    timestamp_str = alert_time.isoformat()
                                    break
                                elif isinstance(ts_val, str):
                                    alert_time = datetime.fromisoformat(ts_val.split('.')[0].split('+')[0])
                                    timestamp_str = alert_time.isoformat()
                                    break
                    
                    if not alert_time:
                        # Last resort: use current time but log warning
                        alert_time = datetime.now()
                        logger.warning(f"⚠️ No valid timestamp found for alert {symbol}, using current time")
                        timestamp_str = alert_time.isoformat()
                except:
                    alert_time = datetime.now()
                
                symbol = data.get('symbol', 'UNKNOWN')
                
                # FIXED: Resolve token numbers to symbols if symbol is a token
                if symbol.isdigit() or str(symbol).startswith('UNKNOWN_'):
                    try:
                        from crawlers.utils.instrument_mapper import InstrumentMapper
                        mapper = InstrumentMapper()
                        # Try instrument_token field first
                        token = data.get('instrument_token') or data.get('token') or symbol
                        resolved = mapper.token_to_symbol(int(token))
                        if resolved and not resolved.startswith("UNKNOWN_"):
                            symbol = resolved
                            logger.debug(f"✅ Resolved token {token} to {resolved}")
                    except Exception as e:
                        logger.debug(f"Token resolution failed: {e}")
                
                # STANDARDIZE: Normalize symbol to base symbol (remove exchange prefix)
                # This ensures we only check against binary crawler instruments (which are stored as base symbols)
                base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                
                # Only include alerts for intraday crawler instruments (check against base symbol)
                if not self.is_intraday_instrument(base_symbol):
                    filtered_count += 1
                    continue
                
                # Use standardized base symbol for all further processing
                symbol = base_symbol
                
                instrument_name = self.get_instrument_name(symbol)
                expiry_info = self.get_expiry_date(symbol)
                
                # Load indicators or Greeks based on instrument type
                is_option = self.is_option_instrument(symbol)
                is_futures = self.is_futures_instrument(symbol)
                
                indicators = {}
                greeks = {}
                
                # CRITICAL: Load indicators from alert data FIRST (primary source - scanner includes them in payload)
                # The scanner has 1000+ ticks across buckets, so indicators are calculated and included in alert
                # PRIORITY 1: Extract indicators from alert payload (most reliable - from process_tick())
                # Check data['indicators'] dict first
                if data.get('indicators'):
                    indicators_raw = data.get('indicators', {})
                    if isinstance(indicators_raw, str):
                        try:
                            alert_indicators = json.loads(indicators_raw)
                            if alert_indicators and isinstance(alert_indicators, dict):
                                indicators.update(alert_indicators)
                                print(f"📊 Loaded indicators from alert data['indicators'] for {symbol}: {list(alert_indicators.keys())}")
                        except:
                            pass
                    elif isinstance(indicators_raw, dict) and indicators_raw:
                        indicators.update(indicators_raw)
                        print(f"📊 Loaded indicators from alert data['indicators'] for {symbol}: {list(indicators_raw.keys())}")
                
                # PRIORITY 2: Extract top-level indicator fields (scanner stores them at pattern level)
                indicator_fields = ['rsi', 'macd', 'ema_20', 'ema_50', 'ema_5', 'ema_10', 'ema_100', 'ema_200', 
                                  'atr', 'vwap', 'volume_ratio', 'bb_upper', 'bb_middle', 'bb_lower', 'bollinger_bands']
                found_top_level = False
                for field in indicator_fields:
                    if field in data:
                        value = data[field]
                        if value is not None:
                            if not indicators:
                                indicators = {}
                            # Handle bollinger_bands dict specially
                            if field == 'bollinger_bands' and isinstance(value, dict):
                                indicators['bb_upper'] = value.get('upper') or value.get('bb_upper')
                                indicators['bb_middle'] = value.get('middle') or value.get('bb_middle')
                                indicators['bb_lower'] = value.get('lower') or value.get('bb_lower')
                            else:
                                indicators[field] = value
                            found_top_level = True
                
                if found_top_level:
                    print(f"📊 Extracted {len([f for f in indicator_fields if f in data and data[f] is not None])} top-level indicators from alert data for {symbol}")
                
                # PRIORITY 3: For options, extract Greeks from alert data
                if is_option:
                    # Check alert data for Greeks first
                    if data.get('greeks'):
                        greeks_raw = data.get('greeks', {})
                        if isinstance(greeks_raw, dict):
                            greeks.update(greeks_raw)
                            print(f"📊 Loaded Greeks from alert data for {symbol}: {list(greeks.keys())}")
                    
                    # Also check individual Greek fields at top level
                    for greek_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                        if greek_name in data and data[greek_name] is not None:
                            if not greeks:
                                greeks = {}
                            greeks[greek_name] = data[greek_name]
                    
                    # FALLBACK: Load from Redis if not in alert data
                    if not greeks:
                        greeks = self.load_greeks_for_symbol(symbol)
                        print(f"📊 Loaded Greeks from Redis (fallback) for {symbol}: {list(greeks.keys()) if greeks else 'none'}")
                    elif len(greeks) < 3:  # If partial, try to fill from Redis
                        redis_greeks = self.load_greeks_for_symbol(symbol)
                        for k, v in redis_greeks.items():
                            if k not in greeks:
                                greeks[k] = v
                
                # PRIORITY 4: For equity/futures, fallback to Redis if alert data missing indicators
                elif is_futures or not is_option:
                    # Only load from Redis if we don't have enough indicators from alert
                    if not indicators or len([k for k in indicators.keys() if k in ['rsi', 'macd', 'ema_20', 'atr', 'vwap']]) < 3:
                        redis_indicators = self.load_indicators_for_symbol(symbol)
                        # Merge Redis indicators as fallback (don't overwrite alert data)
                        for k, v in redis_indicators.items():
                            if k not in indicators:
                                indicators[k] = v
                        print(f"📊 Loaded indicators from Redis (fallback) for {symbol}: {list(redis_indicators.keys()) if redis_indicators else 'none'}")
                
                # Load news for this symbol if available - CRITICAL: Extract from alert data
                news_items = []
                try:
                    # First check if alert already has news data (could be dict or list)
                    if data.get('news'):
                        news_raw = data.get('news')
                        if isinstance(news_raw, list):
                            news_items = news_raw
                        elif isinstance(news_raw, dict):
                            news_items = [news_raw]
                        elif isinstance(news_raw, str):
                            try:
                                news_items = json.loads(news_raw)
                                if not isinstance(news_items, list):
                                    news_items = [news_items]
                            except:
                                pass
                    
                    # Then check news_context (scanner stores it here)
                    if not news_items and data.get('news_context'):
                        news_context = data.get('news_context')
                        if isinstance(news_context, list):
                            news_items = news_context
                        elif isinstance(news_context, dict):
                            # Convert single news_context dict to list format
                            news_items = [news_context]
                        elif isinstance(news_context, str):
                            try:
                                news_items = json.loads(news_context)
                                if not isinstance(news_items, list):
                                    news_items = [news_items]
                            except:
                                pass
                    
                    # ALWAYS try enrichment function (even if news_context exists, it might be empty)
                    # CRITICAL: News enrichment may find news even if alert doesn't have it
                    if not news_items or len(news_items) == 0:
                        try:
                            from alerts.news_enrichment_integration import enrich_alert_with_news
                            # Clean symbol for news lookup (remove exchange prefixes, get base symbol)
                            clean_symbol = symbol.replace('NFO:', '').replace('NSE:', '').replace('BFO:', '').split(':')[-1]
                            # Remove expiry info for futures/options (e.g., "NIFTY25DECFUT" -> "NIFTY", "TECHM25NOVFUT" -> "TECHM")
                            if clean_symbol.endswith('FUT') or re.search(r'\d{2}[A-Z]{3}(FUT|CE|PE)', clean_symbol):
                                # Extract base symbol (e.g., "NIFTY" from "NIFTY25DECFUT", "TECHM" from "TECHM25NOVFUT")
                                base_match = re.match(r'^([A-Z]+)', clean_symbol)
                                if base_match:
                                    clean_symbol = base_match.group(1)
                                # Also try removing date+expiry pattern (e.g., "TECHM25NOVFUT" -> "TECHM")
                                if re.search(r'\d{2}[A-Z]{3}', clean_symbol):
                                    clean_symbol = re.sub(r'\d{2}[A-Z]{3}(FUT|CE|PE)?$', '', clean_symbol)
                            
                            # Try multiple symbol variations for news lookup
                            symbol_variants = [clean_symbol, symbol]
                            enriched_news = []
                            
                            for variant in symbol_variants:
                                temp_alert = {'symbol': variant}
                                enriched = enrich_alert_with_news(temp_alert, self.redis_client, lookback_minutes=120, top_k=10)
                                variant_news = enriched.get('news', [])
                                if variant_news:
                                    enriched_news.extend(variant_news)
                            
                            # Also check Redis directly for news keys with symbol patterns
                            if not enriched_news:
                                try:
                                    # Check news:* keys for symbol-specific news
                                    news_patterns = [
                                        f'news:{clean_symbol}:*',
                                        f'news:{symbol}:*',
                                        f'news:*{clean_symbol}*',
                                        f'news:item:*{clean_symbol}*'
                                    ]
                                    for pattern in news_patterns:
                                        news_keys = self.redis_db1.keys(pattern)
                                        for key in news_keys[:5]:  # Limit to 5
                                            try:
                                                news_data = self.redis_db1.get(key)
                                                if news_data:
                                                    news_item = json.loads(news_data) if isinstance(news_data, str) else news_data
                                                    if news_item and news_item not in enriched_news:
                                                        enriched_news.append(news_item)
                                            except:
                                                pass
                                except Exception as redis_err:
                                    logger.debug(f"Direct Redis news lookup failed: {redis_err}")
                            
                            if enriched_news:
                                news_items = enriched_news
                                print(f"📰 Enriched {len(enriched_news)} news items for {symbol} (cleaned: {clean_symbol})")
                        except Exception as enrich_err:
                            logger.debug(f"News enrichment failed for {symbol}: {enrich_err}")
                            print(f"⚠️ News enrichment error for {symbol}: {enrich_err}")
                except Exception as e:
                    logger.debug(f"News fetch skipped for {symbol}: {e}")
                    news_items = []
                
                # Debug: Log news items found
                if news_items:
                    print(f"📰 Found {len(news_items)} news items for {symbol}")
                else:
                    print(f"⚠️ No news found for {symbol} (checked alert data and enrichment)")
                
                # Convert reasons list to string for DataTable compatibility
                reasons_list = data.get('reasons', [])
                reasons_str = ', '.join(str(r) for r in reasons_list) if isinstance(reasons_list, list) else str(reasons_list) if reasons_list else ''
                
                # Fix expected_move: Convert to decimal format for display (DataTable format .1% multiplies by 100)
                # Store as decimal (0.01 = 1%) so format will display correctly as 1.0%
                expected_move_raw = float(data.get('expected_move', 0.0))
                if expected_move_raw >= 1.0 and expected_move_raw <= 100:
                    # Already in percentage format (1.0 = 1%, 2.0 = 2%), convert to decimal for display
                    expected_move = expected_move_raw / 100.0
                elif expected_move_raw > 100:
                    # Already in percentage format but > 100 (e.g., 150 = 150%), convert to decimal
                    expected_move = expected_move_raw / 100.0
                else:
                    # Already in decimal format (0.01 = 1%), use as-is
                    expected_move = expected_move_raw
                
                # Extract action (BUY/SELL/MONITOR) - CRITICAL for dashboard display
                action = data.get('action', data.get('signal', 'MONITOR'))
                if action is None or action == '':
                    action = 'MONITOR'
                
                # Extract trading parameters - CRITICAL: Handle direction correctly (BUY vs SELL)
                # For SELL: Stop Loss should be HIGHER, Target should be LOWER
                # For BUY: Stop Loss should be LOWER, Target should be HIGHER
                
                entry_price = float(data.get('entry_price', data.get('last_price', 0.0)))
                
                # Extract Stop Loss
                stop_loss = None
                if 'stop_loss' in data and data['stop_loss'] is not None:
                    try:
                        stop_loss_val = float(data['stop_loss'])
                        stop_loss = stop_loss_val if stop_loss_val > 0 else None
                    except (ValueError, TypeError):
                        stop_loss = None
                
                # Extract Target - Check multiple field names
                target = None
                target_fields = ['target', 'target_price', 'take_profit', 'profit_target']
                
                # Check direct fields first
                for field_name in target_fields:
                    if field_name in data and data[field_name] is not None:
                        try:
                            target_val = float(data[field_name])
                            if target_val > 0:
                                target = target_val
                                break
                        except (ValueError, TypeError):
                            continue
                
                # If not found, check nested risk_metrics
                if target is None and 'risk_metrics' in data:
                    risk_metrics = data['risk_metrics']
                    if isinstance(risk_metrics, dict):
                        for field_name in target_fields:
                            if field_name in risk_metrics and risk_metrics[field_name] is not None:
                                try:
                                    target_val = float(risk_metrics[field_name])
                                    if target_val > 0:
                                        target = target_val
                                        break
                                except (ValueError, TypeError):
                                    continue
                
                # Also check if risk_metrics is a string (JSON)
                if target is None and 'risk_metrics' in data and isinstance(data['risk_metrics'], str):
                    try:
                        risk_metrics = json.loads(data['risk_metrics'])
                        if isinstance(risk_metrics, dict):
                            for field_name in target_fields:
                                if field_name in risk_metrics and risk_metrics[field_name] is not None:
                                    try:
                                        target_val = float(risk_metrics[field_name])
                                        if target_val > 0:
                                            target = target_val
                                            break
                                    except (ValueError, TypeError):
                                        continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                
                # CRITICAL: Fix swap issue for SELL action
                # For SELL: Stop Loss should be HIGHER than entry, Target should be LOWER
                # If action is SELL and stop_loss < entry < target, they're swapped!
                if action.upper() in ['SELL', 'SHORT'] and entry_price > 0:
                    if stop_loss and target:
                        # Check if they're swapped (stop_loss lower, target higher - wrong for SELL)
                        if stop_loss < entry_price < target:
                            # They're swapped! Swap them back
                            logger.warning(f"🔄 Swapping stop_loss and target for SELL action: {symbol} (entry={entry_price}, old_sl={stop_loss}, old_target={target})")
                            stop_loss, target = target, stop_loss
                            logger.info(f"✅ Corrected: stop_loss={stop_loss} (should be > entry), target={target} (should be < entry)")
                        elif stop_loss < entry_price:
                            # Only stop_loss is wrong (too low), target might be correct or missing
                            # If target exists and is higher, swap them
                            if target > entry_price:
                                stop_loss, target = target, stop_loss
                                logger.info(f"✅ Swapped stop_loss and target for SELL: {symbol}")
                        elif target > entry_price:
                            # Only target is wrong (too high), stop_loss might be correct
                            # If stop_loss exists and is lower, swap them
                            if stop_loss < entry_price:
                                stop_loss, target = target, stop_loss
                                logger.info(f"✅ Swapped stop_loss and target for SELL: {symbol}")
                elif action.upper() in ['BUY', 'LONG'] and entry_price > 0:
                    # For BUY: Stop Loss should be LOWER, Target should be HIGHER
                    if stop_loss and target:
                        # Check if they're swapped (stop_loss higher, target lower - wrong for BUY)
                        if stop_loss > entry_price > target:
                            # They're swapped! Swap them back
                            logger.warning(f"🔄 Swapping stop_loss and target for BUY action: {symbol} (entry={entry_price}, old_sl={stop_loss}, old_target={target})")
                            stop_loss, target = target, stop_loss
                            logger.info(f"✅ Corrected: stop_loss={stop_loss} (should be < entry), target={target} (should be > entry)")
                        elif stop_loss > entry_price:
                            # Only stop_loss is wrong (too high)
                            if target < entry_price:
                                stop_loss, target = target, stop_loss
                                logger.info(f"✅ Swapped stop_loss and target for BUY: {symbol}")
                        elif target < entry_price:
                            # Only target is wrong (too low)
                            if stop_loss > entry_price:
                                stop_loss, target = target, stop_loss
                                logger.info(f"✅ Swapped stop_loss and target for BUY: {symbol}")
                
                # Quantity
                quantity = None
                if 'quantity' in data and data['quantity'] is not None:
                    try:
                        quantity_val = float(data['quantity'])
                        quantity = quantity_val if quantity_val > 0 else None  # Only set if > 0
                    except (ValueError, TypeError):
                        quantity = None
                
                alert_info = {
                    'symbol': symbol,
                    'instrument_name': instrument_name,  # Human-readable name
                    'expiry': expiry_info,  # Expiry date with status
                    'pattern': data.get('pattern', data.get('pattern_type', 'unknown')),
                    'action': str(action).upper(),  # CRITICAL: Action field (BUY/SELL/MONITOR)
                    'signal': data.get('signal', data.get('action', 'UNKNOWN')),  # Keep signal for backward compatibility
                    'confidence': float(data.get('confidence', 0.0)),
                    'entry_price': float(data.get('entry_price', data.get('last_price', 0.0))),
                    'stop_loss': stop_loss,  # CRITICAL: Stop loss for trading
                    'target': target,  # CRITICAL: Target price
                    'quantity': quantity,  # CRITICAL: Quantity/size
                    'expected_move': expected_move,
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
                    # Technical indicators (for equity/futures) - CRITICAL: Extract from multiple sources
                    # Priority: 1) alert data top-level, 2) alert data['indicators'], 3) Redis-loaded indicators
                    'rsi': (self._extract_indicator_value(data, 'rsi') or 
                           self._extract_indicator_value(indicators, 'rsi') or
                           None),
                    'macd': (self._extract_macd_value(data) or 
                            self._extract_macd_value(indicators) or
                            None),
                    'ema_20': (self._extract_indicator_value(data, 'ema_20') or 
                              self._extract_indicator_value(indicators, 'ema_20') or
                              None),
                    'ema_50': (self._extract_indicator_value(data, 'ema_50') or 
                              self._extract_indicator_value(indicators, 'ema_50') or
                              None),
                    'atr': (self._extract_indicator_value(data, 'atr') or 
                           self._extract_indicator_value(indicators, 'atr') or
                           None),
                    'vwap': (self._extract_indicator_value(data, 'vwap') or 
                            self._extract_indicator_value(indicators, 'vwap') or
                            None),
                    # Store BB as separate fields for display
                    'bb_upper': (self._extract_bb_value(data, 'upper') or 
                                self._extract_bb_value(indicators, 'upper') or
                                None),
                    'bb_middle': (self._extract_bb_value(data, 'middle') or 
                                 self._extract_bb_value(indicators, 'middle') or
                                 None),
                    'bb_lower': (self._extract_bb_value(data, 'lower') or 
                                self._extract_bb_value(indicators, 'lower') or
                                None),
                    # Also include volume_ratio if present in indicators or alert data
                    'volume_ratio': (self._extract_indicator_value(indicators, 'volume_ratio') or 
                                   self._extract_indicator_value(data, 'volume_ratio') or
                                   (float(data.get('volume_ratio', 0.0)) if data.get('volume_ratio') else None)),
                    # Greeks (for options) - CRITICAL: Extract from loaded greeks
                    'delta': greeks.get('delta') if greeks else None,
                    'gamma': greeks.get('gamma') if greeks else None,
                    'theta': greeks.get('theta') if greeks else None,
                    'vega': greeks.get('vega') if greeks else None,
                    'rho': greeks.get('rho') if greeks else None,
                    # News data (store as JSON string for display)
                    'news_data': json.dumps(news_items) if news_items else None,
                }
                
                # Debug: Log indicator values for troubleshooting
                if indicators or any(alert_info.get(k) for k in ['rsi', 'macd', 'ema_20', 'ema_50', 'atr', 'vwap']):
                    print(f"📊 Indicator values for {symbol}: RSI={alert_info.get('rsi')}, MACD={alert_info.get('macd')}, EMA20={alert_info.get('ema_20')}, EMA50={alert_info.get('ema_50')}, ATR={alert_info.get('atr')}, VWAP={alert_info.get('vwap')}, VolumeRatio={alert_info.get('volume_ratio')}")
                else:
                    print(f"⚠️ No indicators found for {symbol} - checked Redis and alert data")
                if greeks or any(alert_info.get(k) for k in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                    print(f"📊 Greeks values for {symbol}: Delta={alert_info.get('delta')}, Gamma={alert_info.get('gamma')}, Theta={alert_info.get('theta')}, Vega={alert_info.get('vega')}, Rho={alert_info.get('rho')}")
                elif is_option:
                    print(f"⚠️ No Greeks found for option {symbol}")
                self.alerts_data.append(alert_info)
            except Exception as e:
                print(f"Error loading validation alert {key}: {e}")
        
        if filtered_count > 0:
            print(f"⚡ Filtered out {filtered_count} alerts (not in intraday crawler instruments)")
        
        # Load price data from multiple sources - CRITICAL: Load full day's time series data
        symbols = list(set([alert['symbol'] for alert in self.alerts_data]))
        print(f"Loading price data for {len(symbols)} symbols from Redis time series...")
        
        today = datetime.now().date()
        dates_to_try = [
            today.strftime('%Y-%m-%d'),
            (today - timedelta(days=1)).strftime('%Y-%m-%d'),
            (today - timedelta(days=2)).strftime('%Y-%m-%d'),
        ]
        
        for symbol in symbols:
            symbol_loaded = False
            
            # Method 1: Load from ohlc_updates:{symbol} stream (full day's data)
            try:
                normalized_symbol = symbol.replace(':', '').replace(' ', '').upper()
                base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                
                stream_variants = [
                    f"ohlc_updates:{symbol}",
                    f"ohlc_updates:{base_symbol}",
                    f"ohlc_updates:{normalized_symbol}"
                ]
                
                for stream_key in stream_variants:
                    try:
                        # Read all messages from stream (last 1000 entries for full day)
                        stream_messages = self.redis_db1.xrevrange(stream_key, count=1000)
                        
                        if stream_messages:
                            print(f"📊 Loading {len(stream_messages)} OHLC records from {stream_key} for {symbol}")
                            
                            for msg_id, fields in stream_messages:
                                try:
                                    # Parse OHLC data from stream
                                    timestamp_ms = int(fields.get('timestamp', fields.get(b'timestamp', 0)))
                                    if timestamp_ms == 0:
                                        continue
                                    
                                    # Convert timestamp_ms to datetime
                                    alert_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
                                    
                                    # Check if this is today's data (or within last 3 days)
                                    alert_date_str = alert_dt.strftime('%Y-%m-%d')
                                    if alert_date_str not in dates_to_try:
                                        continue
                                    
                                    price_info = {
                                        'symbol': symbol,
                                        'open': float(fields.get('open', fields.get(b'open', 0))),
                                        'high': float(fields.get('high', fields.get(b'high', 0))),
                                        'low': float(fields.get('low', fields.get(b'low', 0))),
                                        'close': float(fields.get('close', fields.get(b'close', 0))),
                                        'volume': int(float(fields.get('volume', fields.get(b'volume', 0)))),
                                        'timestamp': alert_dt,
                                        'time_str': alert_dt.strftime('%H:%M:%S'),
                                        'source': 'ohlc_stream'
                                    }
                                    
                                    # Only add if we have valid OHLC data
                                    if price_info['close'] > 0:
                                        self.price_data.append(price_info)
                                        symbol_loaded = True
                                except Exception as e:
                                    logger.debug(f"Error parsing stream message for {symbol}: {e}")
                                    continue
                            
                            if symbol_loaded:
                                break  # Found data in this stream
                    except Exception as e:
                        logger.debug(f"Error reading stream {stream_key}: {e}")
                        continue
                
                if symbol_loaded:
                    continue  # Skip to next symbol if we loaded from stream
                    
            except Exception as e:
                logger.debug(f"Error loading from OHLC stream for {symbol}: {e}")
            
            # Method 2: Load from ohlc_daily:{symbol} sorted set (full day's OHLC)
            # CRITICAL: Check DB 5 first (where ohlc_daily sorted sets are stored)
            try:
                base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                
                # For NIFTY/BANKNIFTY, try with expiry date from instrument cache
                symbol_with_expiry = None
                if base_symbol.startswith('NIFTY') or base_symbol.startswith('BANKNIFTY'):
                    # Try to get expiry date from instrument cache
                    for cached_symbol, cached_data in self.instrument_cache.items():
                        if cached_symbol.startswith(base_symbol) and 'FUT' in cached_symbol:
                            symbol_with_expiry = cached_symbol
                            break
                    # Also try common expiry patterns
                    if not symbol_with_expiry and '25DEC' in base_symbol.upper():
                        symbol_with_expiry = base_symbol
                
                zset_variants = [
                    f"ohlc_daily:{symbol_with_expiry}" if symbol_with_expiry else None,
                    f"ohlc_daily:{symbol}",
                    f"ohlc_daily:{base_symbol}",
                    f"ohlc_daily:{symbol.replace(':', '').upper()}",
                    f"ohlc_daily:NFONIFTY25DECFUT" if 'NIFTY' in base_symbol and '25DEC' in symbol else None,
                    f"ohlc_daily:NFOBANKNIFTY25DECFUT" if 'BANKNIFTY' in base_symbol and '25DEC' in symbol else None,
                ]
                # Remove None values
                zset_variants = [v for v in zset_variants if v]
                
                for zset_key in zset_variants:
                    try:
                        # CRITICAL: Check DB 5 (where ohlc_daily sorted sets are stored)
                        zset_entries = self.redis_db5.zrange(zset_key, 0, -1, withscores=True)
                        
                        if not zset_entries:
                            # Fallback to DB 1
                            zset_entries = self.redis_db1.zrange(zset_key, 0, -1, withscores=True)
                        
                        if zset_entries:
                            print(f"📊 Loading {len(zset_entries)} OHLC records from {zset_key} for {symbol}")
                            
                            for entry_data, timestamp_score in zset_entries:
                                try:
                                    # Parse JSON payload
                                    if isinstance(entry_data, bytes):
                                        entry_str = entry_data.decode('utf-8')
                                    else:
                                        entry_str = entry_data
                                    
                                    ohlc_json = json.loads(entry_str)
                                    
                                    # CRITICAL FIX: Timestamp score is in SECONDS, not milliseconds
                                    # Check if it's milliseconds (> 1e10) or seconds (< 1e10)
                                    timestamp_float = float(timestamp_score)
                                    if timestamp_float > 1e10:
                                        # It's in milliseconds, convert to seconds
                                        alert_dt = datetime.fromtimestamp(timestamp_float / 1000.0)
                                    else:
                                        # It's already in seconds
                                        alert_dt = datetime.fromtimestamp(timestamp_float)
                                    
                                    # Check if this is today's data
                                    alert_date_str = alert_dt.strftime('%Y-%m-%d')
                                    if alert_date_str not in dates_to_try:
                                        continue
                                    
                                    price_info = {
                                        'symbol': symbol,
                                        'open': float(ohlc_json.get('o', 0)),
                                        'high': float(ohlc_json.get('h', 0)),
                                        'low': float(ohlc_json.get('l', 0)),
                                        'close': float(ohlc_json.get('c', 0)),
                                        'volume': int(float(ohlc_json.get('v', 0))),
                                        'timestamp': alert_dt,
                                        'time_str': alert_dt.strftime('%H:%M:%S'),
                                        'source': 'ohlc_daily'
                                    }
                                    
                                    if price_info['close'] > 0:
                                        self.price_data.append(price_info)
                                        symbol_loaded = True
                                except Exception as e:
                                    logger.debug(f"Error parsing zset entry for {symbol}: {e}")
                                    continue
                            
                            if symbol_loaded:
                                break
                    except Exception as e:
                        logger.debug(f"Error reading zset {zset_key}: {e}")
                        continue
                
                if symbol_loaded:
                    continue
                    
            except Exception as e:
                logger.debug(f"Error loading from OHLC daily zset for {symbol}: {e}")
            
            # Method 3: Fallback to price buckets (if time series not available)
            for date_str in dates_to_try:
                try:
                    bucket_keys = self.redis_client.keys(f"bucket_incremental_volume:bucket:{symbol}:{date_str}:*")
                    if bucket_keys:
                        print(f"📊 Loading {len(bucket_keys)} bucket records for {symbol} on {date_str}")
                        # Load ALL buckets for the day (full trading session)
                        for key in sorted(bucket_keys):
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
                                    'time_str': f"{data.get('hour'):02d}:{data.get('minute_bucket'):02d}",
                                    'source': 'bucket'
                                }
                                if price_info['close'] > 0:
                                    self.price_data.append(price_info)
                                    symbol_loaded = True
                            except Exception as e:
                                pass
                        if symbol_loaded:
                            break  # Found data for this date
                except Exception:
                    continue
            
            # Method 4: Fallback to ohlc_latest:{symbol} hash keys (DB 1 or DB 0)
            # CRITICAL: OHLC data is stored as Redis Hash, not JSON string
            # Timestamp format: IST datetime string (e.g., "2025-10-31T07:48:19.535175"), not epoch
            if not symbol_loaded:
                try:
                    base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                    
                    # Try multiple key formats
                    hash_key_variants = [
                        f"ohlc_latest:{symbol}",
                        f"ohlc_latest:{base_symbol}",
                        f"ohlc_latest:{symbol.replace(':', '').upper()}",
                    ]
                    
                    # Check both DB 1 and DB 0
                    redis_clients = [
                        ('DB1', self.redis_db1),
                        ('DB0', self.redis_client),
                    ]
                    
                    for db_name, redis_client in redis_clients:
                        for hash_key in hash_key_variants:
                            try:
                                # Get hash data
                                ohlc_hash = redis_client.hgetall(hash_key)
                                
                                if ohlc_hash and len(ohlc_hash) > 0:
                                    # Parse hash fields
                                    # Handle bytes keys/values from Redis
                                    def get_field(name):
                                        # Try both bytes and string keys
                                        if isinstance(name, str):
                                            name_bytes = name.encode('utf-8')
                                        else:
                                            name_bytes = name
                                        value = ohlc_hash.get(name) or ohlc_hash.get(name_bytes)
                                        if value:
                                            if isinstance(value, bytes):
                                                return value.decode('utf-8')
                                            return value
                                        return None
                                    
                                    close = get_field('close') or get_field('last_price')
                                    if not close:
                                        continue
                                    
                                    close_val = float(close)
                                    if close_val <= 0:
                                        continue
                                    
                                    # Parse timestamp from updated_at field (IST datetime string)
                                    updated_at_str = get_field('updated_at')
                                    date_str = get_field('date')
                                    
                                    if updated_at_str:
                                        try:
                                            # Parse IST datetime string: "2025-10-31T07:48:19.535175"
                                            # fromisoformat can handle this format directly
                                            if 'T' in updated_at_str:
                                                # ISO format with T separator
                                                alert_dt = datetime.fromisoformat(updated_at_str)
                                            else:
                                                # Space-separated format
                                                alert_dt = datetime.fromisoformat(updated_at_str)
                                            # Note: If microseconds cause issues, strip them
                                            if hasattr(alert_dt, 'microsecond') and alert_dt.microsecond:
                                                # Already parsed correctly
                                                pass
                                        except Exception as parse_err:
                                            # Fallback: try parsing date string and use current time
                                            if date_str:
                                                try:
                                                    alert_dt = datetime.strptime(date_str, '%Y-%m-%d')
                                                    # Use current time for today's date
                                                    if alert_dt.date() == datetime.now().date():
                                                        alert_dt = datetime.now()
                                                except Exception:
                                                    alert_dt = datetime.now()
                                            else:
                                                alert_dt = datetime.now()
                                    elif date_str:
                                        try:
                                            alert_dt = datetime.strptime(date_str, '%Y-%m-%d')
                                            # Use current time for today's date
                                            if alert_dt.date() == datetime.now().date():
                                                alert_dt = datetime.now()
                                        except Exception:
                                            alert_dt = datetime.now()
                                    else:
                                        alert_dt = datetime.now()
                                    
                                    # Check if date matches
                                    alert_date_str = alert_dt.strftime('%Y-%m-%d')
                                    if alert_date_str not in dates_to_try:
                                        continue
                                    
                                    price_info = {
                                        'symbol': symbol,
                                        'open': float(get_field('open') or 0),
                                        'high': float(get_field('high') or close_val),
                                        'low': float(get_field('low') or close_val),
                                        'close': close_val,
                                        'volume': int(float(get_field('volume') or 0)),
                                        'timestamp': alert_dt,
                                        'time_str': alert_dt.strftime('%H:%M:%S'),
                                        'source': f'ohlc_latest_hash_{db_name.lower()}'
                                    }
                                    
                                    if price_info['close'] > 0:
                                        self.price_data.append(price_info)
                                        symbol_loaded = True
                                        print(f"📊 Loaded OHLC from {db_name} hash key {hash_key} for {symbol}")
                                        break  # Found in this variant
                            except Exception as e:
                                logger.debug(f"Error reading hash {hash_key} from {db_name}: {e}")
                                continue
                        
                        if symbol_loaded:
                            break  # Found in this DB
                except Exception as e:
                    logger.error(f"Error loading from ohlc_latest hash for {symbol}: {e}")
            
            if not symbol_loaded:
                logger.debug(f"⚠️ No price data found for {symbol} from any source")
        
        print(f"Loaded {len(self.alerts_data)} alerts and {len(self.price_data)} price records")
        
    def create_price_chart(self, symbol, alert_time, entry_price, show_indicators=True):
        """Create a time series chart for a specific alert with optional technical indicators
        
        Available indicators:
        - VWAP: Volume-Weighted Average Price
        - EMAs: 5, 10, 20, 50, 100, 200 period Exponential Moving Averages
        - Bollinger Bands: Upper, Middle (SMA), Lower bands
        - RSI: Relative Strength Index (subplot)
        - MACD: Moving Average Convergence Divergence (subplot)
        - ATR: Average True Range (bands around price)
        """
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
        
        # Load indicators if enabled
        indicators = {}
        volume_profile_data = {}
        if show_indicators:
            # Try to load indicators from alert data or Redis
            indicators = self.load_indicators_for_symbol(symbol)
            
            # Load Volume Profile data (POC, Value Area, Distribution)
            volume_profile_data = self.load_volume_profile_for_symbol(symbol)
        
        # Calculate VWAP from price data if not available in indicators
        if show_indicators and 'vwap' not in indicators and len(closes) > 0 and len(volumes) > 0:
            # Calculate cumulative VWAP
            cumulative_pv = []
            cumulative_volume = []
            vwap_values = []
            for i in range(len(closes)):
                pv = closes[i] * volumes[i]  # Price * Volume
                cumulative_pv.append(sum([closes[j] * volumes[j] for j in range(i+1)]))
                cumulative_volume.append(sum(volumes[:i+1]))
                if cumulative_volume[i] > 0:
                    vwap_values.append(cumulative_pv[i] / cumulative_volume[i])
                else:
                    vwap_values.append(closes[i])
            indicators['vwap'] = vwap_values[-1] if vwap_values else None
        
        # Find alert position in time series
        alert_timestamp = alert_time
        alert_index = None
        for i, t in enumerate(times):
            if t >= alert_timestamp:
                alert_index = i
                break
        
        # Determine number of rows for subplots (add rows for RSI/MACD/Volume Profile if available)
        num_rows = 2  # Price + Volume (base)
        show_rsi = show_indicators and ('rsi' in indicators or any('rsi' in str(k).lower() for k in indicators.keys()))
        show_macd = show_indicators and ('macd' in indicators or any('macd' in str(k).lower() for k in indicators.keys()))
        show_volume_profile = show_indicators and volume_profile_data and volume_profile_data.get('distribution')
        if show_rsi:
            num_rows += 1
        if show_macd:
            num_rows += 1
        if show_volume_profile:
            num_rows += 1  # Add volume profile histogram
        
        # Create the chart with dynamic rows
        subplot_titles = [f'{symbol} - Price Movement', 'Volume']
        row_heights = [0.5, 0.2]  # Price, Volume
        if show_rsi:
            subplot_titles.append('RSI')
            row_heights.append(0.15)
        if show_macd:
            subplot_titles.append('MACD')
            row_heights.append(0.15)
        if show_volume_profile:
            subplot_titles.append('Volume Profile')
            row_heights.append(0.15)
        
        # Normalize row heights
        total = sum(row_heights)
        row_heights = [h/total for h in row_heights]
        
        fig = make_subplots(
            rows=num_rows, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            subplot_titles=subplot_titles,
            row_heights=row_heights
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
        
        # TECHNICAL INDICATORS (if available)
        if show_indicators and indicators:
            # VWAP line
            vwap_value = indicators.get('vwap') or indicators.get('VWAP')
            if vwap_value and isinstance(vwap_value, (int, float)):
                # Use constant VWAP line (most recent value)
                fig.add_hline(
                    y=vwap_value,
                    line_dash="dashdot",
                    line_color="purple",
                    annotation_text=f"VWAP: {vwap_value:.2f}",
                    annotation_position="right",
                    row=1, col=1,
                    opacity=0.8
                )
            elif vwap_value and isinstance(vwap_value, list) and len(vwap_value) == len(times):
                # Time-series VWAP (calculated from data)
                fig.add_trace(
                    go.Scatter(
                        x=times, y=vwap_value,
                        mode='lines',
                        name='VWAP',
                        line=dict(color='purple', width=2, dash='dashdot'),
                        opacity=0.8
                    ),
                    row=1, col=1
                )
            
            # EMAs - Add multiple EMAs if available
            for ema_period in [5, 10, 20, 50, 100, 200]:
                ema_key = f'ema_{ema_period}'
                ema_value = indicators.get(ema_key) or indicators.get(f'EMA{ema_period}')
                if ema_value and isinstance(ema_value, (int, float)):
                    colors = {5: 'cyan', 10: 'lightblue', 20: 'blue', 50: 'darkblue', 100: 'navy', 200: 'black'}
                    fig.add_hline(
                        y=ema_value,
                        line_dash="dot",
                        line_color=colors.get(ema_period, 'gray'),
                        annotation_text=f"EMA{ema_period}: {ema_value:.2f}",
                        annotation_position="left",
                        row=1, col=1,
                        opacity=0.6
                    )
            
            # Bollinger Bands
            bb_upper = indicators.get('bb_upper') or indicators.get('bollinger_bands', {}).get('upper') if isinstance(indicators.get('bollinger_bands'), dict) else None
            bb_middle = indicators.get('bb_middle') or indicators.get('bollinger_bands', {}).get('middle') if isinstance(indicators.get('bollinger_bands'), dict) else None
            bb_lower = indicators.get('bb_lower') or indicators.get('bollinger_bands', {}).get('lower') if isinstance(indicators.get('bollinger_bands'), dict) else None
            
            if bb_upper and isinstance(bb_upper, (int, float)):
                fig.add_hline(y=bb_upper, line_dash="dot", line_color="gray", opacity=0.5, row=1, col=1)
            if bb_middle and isinstance(bb_middle, (int, float)):
                fig.add_hline(y=bb_middle, line_dash="dot", line_color="gray", opacity=0.7, row=1, col=1)
            if bb_lower and isinstance(bb_lower, (int, float)):
                fig.add_hline(y=bb_lower, line_dash="dot", line_color="gray", opacity=0.5, row=1, col=1)
            
            # If all three BB values exist, add shaded area
            if all(x and isinstance(x, (int, float)) for x in [bb_upper, bb_middle, bb_lower]):
                # Add upper band
                fig.add_trace(
                    go.Scatter(
                        x=times, y=[bb_upper] * len(times),
                        mode='lines',
                        name='BB Upper',
                        line=dict(color='gray', width=1, dash='dot'),
                        opacity=0.5,
                        showlegend=False
                    ),
                    row=1, col=1
                )
                # Add lower band with fill
                fig.add_trace(
                    go.Scatter(
                        x=times, y=[bb_lower] * len(times),
                        mode='lines',
                        name='BB Lower',
                        line=dict(color='gray', width=1, dash='dot'),
                        fill='tonexty',
                        fillcolor='rgba(128,128,128,0.1)',
                        opacity=0.5,
                        showlegend=True
                    ),
                    row=1, col=1
                )
        
        # VOLUME PROFILE - POC and Value Area lines
        if show_indicators and volume_profile_data:
            poc_price = volume_profile_data.get('poc_price')
            value_area_high = volume_profile_data.get('value_area_high')
            value_area_low = volume_profile_data.get('value_area_low')
            
            if poc_price and isinstance(poc_price, (int, float)) and poc_price > 0:
                # POC line - horizontal line at POC price
                fig.add_hline(
                    y=poc_price,
                    line_dash="solid",
                    line_color="red",
                    line_width=2,
                    annotation_text=f"POC: {poc_price:.2f}",
                    annotation_position="right",
                    row=1, col=1,
                    opacity=0.9
                )
            
            # Value Area (70% volume range)
            if value_area_high and value_area_low and isinstance(value_area_high, (int, float)) and isinstance(value_area_low, (int, float)):
                # Value Area High
                fig.add_hline(
                    y=value_area_high,
                    line_dash="dot",
                    line_color="blue",
                    annotation_text=f"VA High: {value_area_high:.2f}",
                    annotation_position="right",
                    row=1, col=1,
                    opacity=0.6
                )
                # Value Area Low
                fig.add_hline(
                    y=value_area_low,
                    line_dash="dot",
                    line_color="blue",
                    annotation_text=f"VA Low: {value_area_low:.2f}",
                    annotation_position="right",
                    row=1, col=1,
                    opacity=0.6
                )
                # Shaded Value Area
                fig.add_trace(
                    go.Scatter(
                        x=[times[0], times[-1], times[-1], times[0], times[0]],
                        y=[value_area_low, value_area_low, value_area_high, value_area_high, value_area_low],
                        mode='lines',
                        name='Value Area (70%)',
                        fill='toself',
                        fillcolor='rgba(0,100,255,0.1)',
                        line=dict(color='blue', width=0),
                        opacity=0.3,
                        showlegend=True,
                        hoverinfo='skip'
                    ),
                    row=1, col=1
                )
        
        # Volume chart
        volume_row = 2
        fig.add_trace(
            go.Bar(
                x=times, y=volumes,
                name='Volume',
                marker_color='lightblue',
                opacity=0.7
            ),
            row=volume_row, col=1
        )
        
        # RSI subplot (if available)
        if show_rsi:
            rsi_value = indicators.get('rsi') or indicators.get('RSI')
            if rsi_value and isinstance(rsi_value, (int, float)):
                rsi_row = volume_row + 1
                fig.add_trace(
                    go.Scatter(
                        x=times, y=[rsi_value] * len(times),
                        mode='lines',
                        name='RSI',
                        line=dict(color='orange', width=2),
                        fill='tozeroy',
                        fillcolor='rgba(255,165,0,0.2)'
                    ),
                    row=rsi_row, col=1
                )
                # Add RSI reference lines (70 overbought, 30 oversold, 50 neutral)
                fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.5, row=rsi_row, col=1)
                fig.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.3, row=rsi_row, col=1)
                fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.5, row=rsi_row, col=1)
                fig.update_yaxes(range=[0, 100], title_text="RSI", row=rsi_row, col=1)
        
        # MACD subplot (if available)
        if show_macd:
            macd_data = indicators.get('macd')
            if macd_data:
                if isinstance(macd_data, dict):
                    macd_line = macd_data.get('macd') or macd_data.get('value')
                    signal_line = macd_data.get('signal')
                    histogram = macd_data.get('histogram')
                elif isinstance(macd_data, (int, float)):
                    macd_line = macd_data
                    signal_line = None
                    histogram = None
                else:
                    macd_line = None
                
                if macd_line and isinstance(macd_line, (int, float)):
                    macd_row = volume_row + (2 if show_rsi else 1)
                    fig.add_trace(
                        go.Scatter(
                            x=times, y=[macd_line] * len(times),
                            mode='lines',
                            name='MACD',
                            line=dict(color='blue', width=2)
                        ),
                        row=macd_row, col=1
                    )
                    if signal_line and isinstance(signal_line, (int, float)):
                        fig.add_trace(
                            go.Scatter(
                                x=times, y=[signal_line] * len(times),
                                mode='lines',
                                name='Signal',
                                line=dict(color='red', width=1, dash='dash')
                            ),
                            row=macd_row, col=1
                        )
                    if histogram and isinstance(histogram, (int, float)):
                        fig.add_trace(
                            go.Bar(
                                x=times, y=[histogram] * len(times),
                                name='Histogram',
                                marker_color='green' if histogram > 0 else 'red',
                                opacity=0.6
                            ),
                            row=macd_row, col=1
                        )
                    fig.update_yaxes(title_text="MACD", row=macd_row, col=1)
        
        # Volume Profile Histogram (if distribution data available)
        if show_volume_profile:
            vp_row = volume_row + (2 if show_rsi else 1) + (1 if show_macd else 0)
            distribution = volume_profile_data.get('distribution', {})
            
            if distribution:
                # Sort prices and volumes for histogram
                sorted_prices = sorted(distribution.keys())
                sorted_volumes = [distribution[price] for price in sorted_prices]
                
                # Create horizontal bar chart (volume on x-axis, price on y-axis)
                fig.add_trace(
                    go.Bar(
                        x=sorted_volumes,
                        y=[f"{p:.2f}" for p in sorted_prices],
                        orientation='h',
                        name='Volume Profile',
                        marker=dict(
                            color=sorted_volumes,
                            colorscale='Viridis',
                            showscale=True,
                            colorbar=dict(title="Volume", x=1.15)
                        ),
                        hovertemplate='Price: %{y}<br>Volume: %{x:,}<extra></extra>'
                    ),
                    row=vp_row, col=1
                )
                
                # Highlight POC in volume profile
                poc_price = volume_profile_data.get('poc_price')
                if poc_price and poc_price in distribution:
                    poc_index = sorted_prices.index(poc_price)
                    fig.add_trace(
                        go.Bar(
                            x=[sorted_volumes[poc_index]],
                            y=[f"{poc_price:.2f}"],
                            orientation='h',
                            name='POC',
                            marker=dict(color='red', line=dict(color='darkred', width=2)),
                            showlegend=True
                        ),
                        row=vp_row, col=1
                    )
                
                fig.update_xaxes(title_text="Volume", row=vp_row, col=1)
                fig.update_yaxes(title_text="Price", row=vp_row, col=1)
            else:
                # If no distribution, just show POC value
                poc_price = volume_profile_data.get('poc_price')
                if poc_price:
                    fig.add_annotation(
                        xref=f"x{vp_row}", yref=f"y{vp_row}",
                        x=0.5, y=0.5,
                        text=f"POC: {poc_price:.2f}<br>Distribution data not available",
                        showarrow=False,
                        font=dict(size=12),
                        bgcolor="rgba(255,255,255,0.8)"
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
        
        fig.update_xaxes(title_text="Time", row=volume_row, col=1)
        fig.update_yaxes(title_text="Volume", row=volume_row, col=1)
        
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
                    html.H1("WELCOME TO AION LABS INTRADAY SIGNALS", className="text-center mb-4", style={"font-weight": "bold"}),
                    html.P("Real-time sent alerts with news, indicators, and Greeks", className="text-center text-muted")
                ])
            ]),
            
            # Market Indices Section (VIX, NIFTY 50, BANKNIFTY) - Auto-refreshes every 30 seconds
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H4("📊 Market Indices", className="mb-0"),
                            html.Small("Auto-refreshing every 30 seconds", className="text-muted")
                        ], style={"background-color": "#e3f2fd", "border-bottom": "2px solid #2196F3"}),
                        dbc.CardBody([
                            html.Div(id='market-indices-display', children=[
                                dbc.Alert("Loading market indices...", color="info")
                            ]),
                            dcc.Interval(
                                id='indices-interval',
                                interval=30*1000,  # Update every 30 seconds
                                n_intervals=0
                            )
                        ])
                    ], className="border-primary")
                ], width=12)
            ], className="mb-4"),
            
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
            
            # Global News Feed Section (always visible, auto-refreshing)
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H4("📰 Latest Market News", className="mb-0"),
                            html.Small("Auto-refreshing every 30 seconds", className="text-muted")
                        ], style={"background-color": "#e8f5e9", "border-bottom": "2px solid #4caf50"}),
                        dbc.CardBody([
                            html.Div(
                                id='global-news-feed',
                                style={
                                    'height': '300px',
                                    'overflow-y': 'auto',
                                    'border': '1px solid #ddd',
                                    'padding': '15px',
                                    'background-color': '#fafafa',
                                    'border-radius': '5px'
                                },
                                children=[
                                    dbc.Alert("Loading latest news...", color="info")
                                ]
                            ),
                            dcc.Interval(
                                id='news-interval',
                                interval=30*1000,  # Update every 30 seconds
                                n_intervals=0
                            )
                        ])
                    ], className="border-success")
                ], width=12)
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
                            {"name": "Action", "id": "action", "type": "text"},  # CRITICAL: Action field (BUY/SELL/MONITOR)
                            {"name": "Signal", "id": "signal", "hideable": True},  # Keep for backward compatibility
                            {"name": "Confidence", "id": "confidence", "type": "numeric", "format": {"specifier": ".1%"}},
                            {"name": "Entry Price", "id": "entry_price", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "Stop Loss", "id": "stop_loss", "type": "numeric", "format": {"specifier": ".2f"}},  # CRITICAL: Stop loss - always visible
                            {"name": "Target", "id": "target", "type": "numeric", "format": {"specifier": ".2f"}},  # CRITICAL: Target price - always visible
                            {"name": "Quantity", "id": "quantity", "type": "numeric", "format": {"specifier": ".0f"}, "hideable": True},  # CRITICAL: Quantity
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
                            {"name": "Volume Ratio", "id": "volume_ratio", "type": "numeric", "format": {"specifier": ".2f"}, "hideable": True},
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
                        style_cell={
                            'textAlign': 'center',
                            'border': '2px solid #ccc'
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)', 
                            'fontWeight': 'bold',
                            'textAlign': 'center',
                            'border': '2px solid #999'
                        },
                        style_data={
                            'border': '2px solid #ccc',
                            'textAlign': 'center'
                        },
                        style_data_conditional=[
                            {
                                'if': {'column_id': 'symbol'},
                                'textAlign': 'left'
                            }
                        ]
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
            
            # Don't auto-select first alert - return None so no chart shows by default
            return options, None
        
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
            Output('market-indices-display', 'children'),
            Input('indices-interval', 'n_intervals')
        )
        def update_market_indices(n):
            """Fetch and display VIX, NIFTY 50, BANKNIFTY from Redis (refreshes every 30s)"""
            try:
                # Verify Redis connection
                if not self.redis_db1:
                    logger.error("Redis DB1 client not initialized")
                    return dbc.Alert("Redis connection not available", color="danger")
                
                indices_data = {}
                
                # Fetch VIX
                vix_keys = [
                    "index:NSE:INDIA VIX",
                    "market_data:indices:nse_india_vix",
                    "index_data:indiavix",
                    "index_data:india_vix"
                ]
                vix_value = None
                vix_change = None
                vix_change_pct = None
                for key in vix_keys:
                    try:
                        vix_data = self.redis_db1.get(key)
                        if vix_data:
                            # Handle bytes, string, and dict formats
                            if isinstance(vix_data, bytes):
                                vix_json = json.loads(vix_data.decode('utf-8'))
                            elif isinstance(vix_data, str):
                                vix_json = json.loads(vix_data)
                            else:
                                vix_json = vix_data
                            
                            vix_value = vix_json.get('last_price') or vix_json.get('value') or vix_json.get('close')
                            # Handle both 'net_change'/'change' and 'percent_change'/'change_pct'
                            vix_change = vix_json.get('net_change') or vix_json.get('change', 0)
                            vix_change_pct = vix_json.get('percent_change') or vix_json.get('change_pct', 0)
                            if vix_value:
                                logger.debug(f"✅ Fetched VIX from {key}: {vix_value}")
                                break
                    except Exception as e:
                        logger.error(f"Error fetching VIX from {key}: {e}", exc_info=True)
                        continue
                
                # Fetch NIFTY 50
                nifty_keys = [
                    "index:NSE:NIFTY 50",
                    "market_data:indices:nse_nifty_50",
                    "index_data:nifty50",
                    "index_data:nifty_50"
                ]
                nifty_value = None
                nifty_change = None
                nifty_change_pct = None
                for key in nifty_keys:
                    try:
                        nifty_data = self.redis_db1.get(key)
                        if nifty_data:
                            # Handle bytes, string, and dict formats
                            if isinstance(nifty_data, bytes):
                                nifty_json = json.loads(nifty_data.decode('utf-8'))
                            elif isinstance(nifty_data, str):
                                nifty_json = json.loads(nifty_data)
                            else:
                                nifty_json = nifty_data
                            
                            nifty_value = nifty_json.get('last_price') or nifty_json.get('value') or nifty_json.get('close')
                            # Handle both 'net_change'/'change' and 'percent_change'/'change_pct'
                            nifty_change = nifty_json.get('net_change') or nifty_json.get('change', 0)
                            nifty_change_pct = nifty_json.get('percent_change') or nifty_json.get('change_pct', 0)
                            if nifty_value:
                                logger.debug(f"✅ Fetched NIFTY 50 from {key}: {nifty_value}")
                                break
                    except Exception as e:
                        logger.error(f"Error fetching NIFTY 50 from {key}: {e}", exc_info=True)
                        continue
                
                # Fetch BANKNIFTY
                banknifty_keys = [
                    "index:NSE:NIFTY BANK",
                    "market_data:indices:nse_nifty_bank",
                    "index_data:niftybank",
                    "index_data:nifty_bank",
                    "index_data:banknifty"
                ]
                banknifty_value = None
                banknifty_change = None
                banknifty_change_pct = None
                for key in banknifty_keys:
                    try:
                        banknifty_data = self.redis_db1.get(key)
                        if banknifty_data:
                            # Handle bytes, string, and dict formats
                            if isinstance(banknifty_data, bytes):
                                banknifty_json = json.loads(banknifty_data.decode('utf-8'))
                            elif isinstance(banknifty_data, str):
                                banknifty_json = json.loads(banknifty_data)
                            else:
                                banknifty_json = banknifty_data
                            
                            banknifty_value = banknifty_json.get('last_price') or banknifty_json.get('value') or banknifty_json.get('close')
                            # Handle both 'net_change'/'change' and 'percent_change'/'change_pct'
                            banknifty_change = banknifty_json.get('net_change') or banknifty_json.get('change', 0)
                            banknifty_change_pct = banknifty_json.get('percent_change') or banknifty_json.get('change_pct', 0)
                            if banknifty_value:
                                logger.debug(f"✅ Fetched BANKNIFTY from {key}: {banknifty_value}")
                                break
                    except Exception as e:
                        logger.error(f"Error fetching BANKNIFTY from {key}: {e}", exc_info=True)
                        continue
                
                # Create display cards
                cards = []
                
                # VIX Card
                if vix_value is not None:
                    vix_color = "danger" if float(vix_value) > 15 else ("warning" if float(vix_value) > 12 else "success")
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("India VIX", className="card-title"),
                                    html.H3(f"{float(vix_value):.2f}", className="mb-2"),
                                    html.P([
                                        html.Span(f"{vix_change:+.2f}" if vix_change is not None else "N/A", 
                                                className="text-danger" if vix_change and vix_change < 0 else "text-success"),
                                        html.Span(f" ({vix_change_pct:+.2f}%)" if vix_change_pct is not None else "", 
                                                className="text-danger" if vix_change_pct and vix_change_pct < 0 else "text-success")
                                    ], className="mb-0")
                                ])
                            ], color=vix_color, outline=True)
                        ], width=4)
                    )
                else:
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("India VIX", className="card-title"),
                                    html.P("Loading...", className="text-muted")
                                ])
                            ], outline=True)
                        ], width=4)
                    )
                
                # NIFTY 50 Card
                if nifty_value is not None:
                    nifty_color = "success" if nifty_change and nifty_change > 0 else ("danger" if nifty_change and nifty_change < 0 else "secondary")
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("NIFTY 50", className="card-title"),
                                    html.H3(f"{float(nifty_value):.2f}", className="mb-2"),
                                    html.P([
                                        html.Span(f"{nifty_change:+.2f}" if nifty_change is not None else "N/A", 
                                                className="text-success" if nifty_change and nifty_change > 0 else "text-danger"),
                                        html.Span(f" ({nifty_change_pct:+.2f}%)" if nifty_change_pct is not None else "", 
                                                className="text-success" if nifty_change_pct and nifty_change_pct > 0 else "text-danger")
                                    ], className="mb-0")
                                ])
                            ], color=nifty_color, outline=True)
                        ], width=4)
                    )
                else:
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("NIFTY 50", className="card-title"),
                                    html.P("Loading...", className="text-muted")
                                ])
                            ], outline=True)
                        ], width=4)
                    )
                
                # BANKNIFTY Card
                if banknifty_value is not None:
                    banknifty_color = "success" if banknifty_change and banknifty_change > 0 else ("danger" if banknifty_change and banknifty_change < 0 else "secondary")
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("BANK NIFTY", className="card-title"),
                                    html.H3(f"{float(banknifty_value):.2f}", className="mb-2"),
                                    html.P([
                                        html.Span(f"{banknifty_change:+.2f}" if banknifty_change is not None else "N/A", 
                                                className="text-success" if banknifty_change and banknifty_change > 0 else "text-danger"),
                                        html.Span(f" ({banknifty_change_pct:+.2f}%)" if banknifty_change_pct is not None else "", 
                                                className="text-success" if banknifty_change_pct and banknifty_change_pct > 0 else "text-danger")
                                    ], className="mb-0")
                                ])
                            ], color=banknifty_color, outline=True)
                        ], width=4)
                    )
                else:
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("BANK NIFTY", className="card-title"),
                                    html.P("Loading...", className="text-muted")
                                ])
                            ], outline=True)
                        ], width=4)
                    )
                
                return dbc.Row(cards)
                
            except Exception as e:
                logger.error(f"Error updating market indices: {e}", exc_info=True)
                import traceback
                logger.error(traceback.format_exc())
                return dbc.Alert([
                    html.Strong("Error loading market indices: "), str(e)
                ], color="danger")
        
        @app.callback(
            Output('global-news-feed', 'children'),
            Input('news-interval', 'n_intervals')
        )
        def update_global_news_feed(n):
            """Fetch and display latest news from Redis with 180-minute TTL filtering"""
            try:
                news_items = []
                now = datetime.now()
                
                # Method 1: Fetch from news:MARKET_NEWS:* keys (primary format used by store_news_sentiment)
                try:
                    market_news_keys = self.redis_db1.keys('news:MARKET_NEWS:*')
                    # Handle bytes keys and convert to strings
                    market_news_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in market_news_keys]
                    for key in sorted(market_news_keys, reverse=True)[:50]:  # Get latest 50, then filter by TTL
                        try:
                            # Check TTL - exclude if less than 180 minutes (10800 seconds) remaining
                            ttl = self.redis_db1.ttl(key)
                            if ttl == -1:  # Key has no expiry (persistent) - include
                                pass
                            elif ttl == -2:  # Key doesn't exist (shouldn't happen if we got it from keys())
                                continue
                            elif ttl < 10800:  # Less than 180 minutes remaining
                                continue  # Skip this news item
                            
                            news_data = self.redis_db1.get(key)
                            if news_data:
                                # Handle bytes, string, and dict formats
                                if isinstance(news_data, bytes):
                                    news_item = json.loads(news_data.decode('utf-8'))
                                elif isinstance(news_data, str):
                                    news_item = json.loads(news_data)
                                else:
                                    news_item = news_data
                                
                                # Handle nested JSON structure (if news is wrapped in 'data' field)
                                if 'data' in news_item and isinstance(news_item['data'], dict):
                                    # Extract from nested 'data' field
                                    news_item = news_item['data']
                                
                                # Normalize field names (handle title/headline, publisher/source, etc.)
                                if 'title' in news_item and 'headline' not in news_item:
                                    news_item['headline'] = news_item['title']
                                if 'publisher' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['publisher']
                                if 'source' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['source']
                                if 'collected_at' in news_item and 'timestamp' not in news_item:
                                    news_item['timestamp'] = news_item['collected_at']
                                
                                # Add key and TTL info for deduplication (handle bytes keys)
                                news_key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                                news_item['_redis_key'] = news_key_str
                                news_item['_ttl'] = ttl
                                news_items.append(news_item)
                        except Exception as e:
                            logger.debug(f"Error parsing news item {key}: {e}")
                except Exception as e:
                    logger.debug(f"Error fetching news:MARKET_NEWS keys: {e}")
                
                # Method 2: Fetch from news:item:* keys (today's date) - Check TTL (180 mins = 10800 seconds)
                today_str = datetime.now().strftime('%Y%m%d')
                try:
                    news_keys = self.redis_db1.keys(f'news:item:{today_str}:*')
                    # Handle bytes keys and convert to strings
                    news_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in news_keys]
                    for key in sorted(news_keys, reverse=True)[:50]:  # Check more keys to filter by TTL
                        try:
                            # Check TTL - exclude if less than 180 minutes (10800 seconds) remaining
                            ttl = self.redis_db1.ttl(key)
                            if ttl == -1:  # Key has no expiry (persistent)
                                # Include persistent keys (they're valid)
                                pass
                            elif ttl == -2:  # Key doesn't exist (shouldn't happen if we got it from keys())
                                continue
                            elif ttl < 10800:  # Less than 180 minutes remaining
                                continue  # Skip this news item
                            
                            # Skip if already added (dedupe by key)
                            if any(n.get('_redis_key') == key for n in news_items):
                                continue
                            
                            news_data = self.redis_db1.get(key)
                            if news_data:
                                # Handle bytes, string, and dict formats
                                if isinstance(news_data, bytes):
                                    news_item = json.loads(news_data.decode('utf-8'))
                                elif isinstance(news_data, str):
                                    news_item = json.loads(news_data)
                                else:
                                    news_item = news_data
                                
                                # Handle nested JSON structure (if news is wrapped in 'data' field)
                                if 'data' in news_item and isinstance(news_item['data'], dict):
                                    # Extract from nested 'data' field
                                    news_item = news_item['data']
                                
                                # Normalize field names
                                if 'title' in news_item and 'headline' not in news_item:
                                    news_item['headline'] = news_item['title']
                                if 'publisher' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['publisher']
                                if 'source' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['source']
                                if 'collected_at' in news_item and 'timestamp' not in news_item:
                                    news_item['timestamp'] = news_item['collected_at']
                                
                                # Add key and TTL info for deduplication (handle bytes keys)
                                news_key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                                news_item['_redis_key'] = news_key_str
                                news_item['_ttl'] = ttl
                                news_items.append(news_item)
                        except Exception as e:
                            logger.debug(f"Error parsing news item {key}: {e}")
                except Exception as e:
                    logger.debug(f"Error fetching news:item keys: {e}")
                
                # Method 3: Fetch from market_data.news:latest (summary with all news)
                try:
                    news_summary = self.redis_db1.get('market_data.news:latest')
                    if news_summary:
                        summary_data = json.loads(news_summary) if isinstance(news_summary, str) else news_summary
                        # Summary doesn't contain individual items, but we can use it for stats
                except Exception:
                    pass
                
                # Method 4: Try fetching from news:latest:* keys - Check TTL
                try:
                    latest_keys = self.redis_db1.keys('news:latest:*')
                    # Handle bytes keys
                    latest_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in latest_keys]
                    for key in sorted(latest_keys, reverse=True)[:20]:  # Check more to filter by TTL
                        try:
                            # Check TTL - exclude if less than 180 minutes (10800 seconds) remaining
                            ttl = self.redis_db1.ttl(key)
                            if ttl == -1:  # Key has no expiry (persistent) - include
                                pass
                            elif ttl == -2:  # Key doesn't exist
                                continue
                            elif ttl < 10800:  # Less than 180 minutes remaining
                                continue  # Skip this news item
                            
                            news_data = self.redis_db1.get(key)
                            if news_data:
                                # Handle bytes, string, and dict formats
                                if isinstance(news_data, bytes):
                                    news_item = json.loads(news_data.decode('utf-8'))
                                elif isinstance(news_data, str):
                                    news_item = json.loads(news_data)
                                else:
                                    news_item = news_data
                                
                                # Handle nested JSON structure (if news is wrapped in 'data' field)
                                if 'data' in news_item and isinstance(news_item['data'], dict):
                                    news_item = news_item['data']
                                # Normalize field names
                                if 'title' in news_item and 'headline' not in news_item:
                                    news_item['headline'] = news_item['title']
                                if 'publisher' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['publisher']
                                if 'source' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['source']
                                if 'collected_at' in news_item and 'timestamp' not in news_item:
                                    news_item['timestamp'] = news_item['collected_at']
                                
                                # Dedupe based on headline
                                headline = news_item.get('headline', news_item.get('title', ''))
                                if headline and not any(n.get('headline', n.get('title', '')) == headline or 
                                                      n.get('_redis_key') == key for n in news_items):
                                    news_item['_redis_key'] = key
                                    news_item['_ttl'] = ttl
                                    news_items.append(news_item)
                        except Exception as e:
                            logger.debug(f"Error parsing news:latest item {key}: {e}")
                except Exception:
                    pass
                
                # Method 5: Try fetching from news:{symbol}:* keys (aggregate all) - Check TTL
                try:
                    symbol_news_keys = self.redis_db1.keys('news:*:*')
                    # Handle bytes keys and convert to strings
                    symbol_news_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in symbol_news_keys]
                    for key in sorted(symbol_news_keys, reverse=True)[:30]:  # Check more to filter by TTL
                        try:
                            # Skip if already added (dedupe by key)
                            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                            if any(n.get('_redis_key') == key_str for n in news_items):
                                continue
                            
                            # Check TTL - exclude if less than 180 minutes (10800 seconds) remaining
                            ttl = self.redis_db1.ttl(key)
                            if ttl == -1:  # Key has no expiry (persistent) - include
                                pass
                            elif ttl == -2:  # Key doesn't exist
                                continue
                            elif ttl < 10800:  # Less than 180 minutes remaining
                                continue  # Skip this news item
                            
                            news_data = self.redis_db1.get(key)
                            if news_data:
                                # Handle bytes, string, and dict formats
                                if isinstance(news_data, bytes):
                                    news_item = json.loads(news_data.decode('utf-8'))
                                elif isinstance(news_data, str):
                                    news_item = json.loads(news_data)
                                else:
                                    news_item = news_data
                                
                                # Handle nested JSON structure (if news is wrapped in 'data' field)
                                if 'data' in news_item and isinstance(news_item['data'], dict):
                                    news_item = news_item['data']
                                
                                # Normalize field names
                                if 'title' in news_item and 'headline' not in news_item:
                                    news_item['headline'] = news_item['title']
                                if 'publisher' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['publisher']
                                if 'source' in news_item and 'news_source' not in news_item:
                                    news_item['news_source'] = news_item['source']
                                if 'collected_at' in news_item and 'timestamp' not in news_item:
                                    news_item['timestamp'] = news_item['collected_at']
                                
                                headline = news_item.get('headline', news_item.get('title', ''))
                                # Only add if not duplicate
                                if headline and not any(n.get('headline', n.get('title', '')) == headline or 
                                                      n.get('_redis_key') == key for n in news_items):
                                    news_item['_redis_key'] = key
                                    news_item['_ttl'] = ttl
                                    news_items.append(news_item)
                        except Exception:
                            pass
                except Exception:
                    pass
                
                # Sort by timestamp (most recent first)
                news_items.sort(key=lambda x: (
                    x.get('timestamp', x.get('published_time', x.get('date', '')))), reverse=True)
                
                # Limit to latest 20 items for display (after TTL filtering)
                news_items = news_items[:20]
                
                if news_items:
                    news_display = []
                    for i, news in enumerate(news_items, 1):
                        headline = news.get('headline', news.get('title', 'No headline'))
                        source = news.get('news_source', news.get('source', news.get('publisher', 'Unknown source')))
                        sentiment = news.get('sentiment_score', news.get('sentiment', 0.0))
                        url = news.get('url', news.get('link', ''))
                        timestamp = news.get('timestamp', news.get('published_time', news.get('date', '')))
                        
                        # Format sentiment
                        try:
                            if isinstance(sentiment, str):
                                sentiment_map = {'positive': 0.8, 'negative': -0.8, 'neutral': 0.0}
                                sentiment_score = sentiment_map.get(sentiment.lower(), 0.0)
                            else:
                                sentiment_score = float(sentiment)
                            
                            if sentiment_score > 0.3:
                                sentiment_color = "🟢"
                                text_color = "#28a745"
                            elif sentiment_score < -0.3:
                                sentiment_color = "🔴"
                                text_color = "#dc3545"
                            else:
                                sentiment_color = "🟡"
                                text_color = "#ffc107"
                        except:
                            sentiment_color = "🟡"
                            sentiment_score = 0.0
                            text_color = "#ffc107"
                        
                        # Format timestamp
                        time_display = ""
                        if timestamp:
                            try:
                                if isinstance(timestamp, str):
                                    if 'Z' in timestamp:
                                        ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                    elif 'T' in timestamp:
                                        ts = datetime.fromisoformat(timestamp.split('.')[0])
                                    else:
                                        ts = datetime.fromisoformat(timestamp)
                                else:
                                    ts = datetime.fromtimestamp(float(timestamp))
                                time_display = ts.strftime('%H:%M:%S')
                            except:
                                time_display = str(timestamp)[:8] if timestamp else ""
                        
                        # Show TTL info (minutes remaining)
                        ttl_minutes = "N/A"
                        if '_ttl' in news:
                            ttl_seconds = news.get('_ttl', -1)
                            if ttl_seconds == -1:
                                ttl_minutes = "Persistent"
                            elif ttl_seconds >= 0:
                                ttl_minutes = f"{ttl_seconds // 60} min"
                        
                        # Create news entry
                        news_entry = html.Div([
                            html.Div([
                                html.Strong(f"{sentiment_color} ", style={"color": text_color}),
                                html.Strong(f"{headline[:120]}{'...' if len(headline) > 120 else ''}", 
                                           style={"font-size": "0.95rem", "color": "#333"}),
                            ], className="mb-1"),
                            html.Div([
                                html.Small([
                                    html.Strong("Source: "), source,
                                    html.Span(f" • {time_display}", className="text-muted") if time_display else "",
                                    html.Span(f" • Sentiment: {sentiment_score:.2f}", className="text-muted"),
                                    html.Span(f" • TTL: {ttl_minutes}", className="text-muted")
                                ], style={"font-size": "0.85rem"})
                            ], className="mb-2"),
                            html.Hr(style={"margin": "8px 0", "border-color": "#ddd"})
                        ], style={"padding": "5px 0"})
                        
                        news_display.append(news_entry)
                    
                    return html.Div([
                        html.P([
                            html.Strong(f"📰 {len(news_items)} latest news items"), 
                            html.Small(" (refreshed every 30s)", className="text-muted ms-2")
                        ], className="mb-3"),
                        html.Div(news_display)
                    ])
                else:
                    return html.Div([
                        dbc.Alert([
                            html.P("No news found in Redis.", className="mb-0"),
                            html.Small("News will appear here when published to Redis keys: news:item:* or news:latest:*", 
                                     className="text-muted")
                        ], color="info")
                    ])
                    
            except Exception as e:
                logger.error(f"Error updating global news feed: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return html.Div([
                    dbc.Alert([
                        html.Strong("Error loading news: "), str(e)
                    ], color="danger")
                ])
        
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
                    
                    # Method 2: Try to fetch news on the fly if not stored (always try, not just if has_news)
                    if not news_items or len(news_items) == 0:
                        try:
                            from alerts.news_enrichment_integration import enrich_alert_with_news
                            symbol = alert.get('symbol', '')
                            if symbol:
                                # Clean symbol for news lookup (remove exchange prefixes)
                                clean_symbol = symbol.replace('NFO:', '').replace('NSE:', '').replace('BFO:', '').split(':')[-1]
                                temp_alert = {'symbol': clean_symbol}
                                enriched = enrich_alert_with_news(temp_alert, self.redis_client, lookback_minutes=120, top_k=10)
                                enriched_news = enriched.get('news', [])
                                if enriched_news:
                                    news_items = enriched_news
                                    print(f"📰 Fetched {len(enriched_news)} news items on-the-fly for {symbol}")
                        except Exception as e:
                            logger.debug(f"Could not fetch news on-the-fly for {symbol}: {e}")
                            print(f"⚠️ News enrichment failed for {symbol}: {e}")
                    
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
    
    def run_dashboard(self, port=None, host='0.0.0.0'):
        """
        Run the dashboard
        
        CRITICAL: This dashboard is exposed externally via Cloudflare Tunnel (primary) with Mullvad SSH fallback
        External consumers access this dashboard at: https://remember-prefers-thinkpad-distributors.trycloudflare.com
        Do NOT change the default port (53056) or host (0.0.0.0) without approval.
        """
        # CRITICAL: Hardcoded port for consumer access - DO NOT CHANGE
        # External consumers access this dashboard at: https://remember-prefers-thinkpad-distributors.trycloudflare.com (via Cloudflare Tunnel)
        DASHBOARD_PORT = 53056
        DASHBOARD_HOST = '0.0.0.0'  # Bind to all interfaces for network access
        
        # Override with hardcoded values (ignore parameter to prevent accidental changes)
        port = DASHBOARD_PORT
        host = DASHBOARD_HOST
        
        self.load_data()
        app = self.create_dashboard()
        print(f"Starting dashboard on http://{host}:{port}")
        print(f"✅ Public access: https://remember-prefers-thinkpad-distributors.trycloudflare.com (via Cloudflare Tunnel)")
        print(f"✅ Local network: http://<local-ip>:{port}")
        print(f"✅ Local access: http://localhost:{port}")
        print(f"⚠️  WARNING: Port {port} and host {host} are fixed for consumer access - DO NOT CHANGE")
        print(f"📋 Note: External access requires Cloudflare Tunnel running: python scripts/tunnel_manager.py daemon")
        # Use threaded=True and processes=1 for better network accessibility
        app.run(debug=True, host=host, port=port, threaded=True, processes=1)

if __name__ == "__main__":
    dashboard = AlertValidationDashboard()
    # CRITICAL: Port 53056 is hardcoded for consumer access
    # External access via Cloudflare Tunnel: https://remember-prefers-thinkpad-distributors.trycloudflare.com
    # DO NOT CHANGE port without approval
    dashboard.run_dashboard(port=53056)
