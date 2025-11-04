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
    python aion_alert_dashboard/alert_dashboard.py
    
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
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from redis_files.redis_manager import RedisManager82
from redis_files.redis_client import create_consumer_group_if_needed
# ✅ STANDARDIZED: xreadgroup_blocking removed - use RobustStreamConsumer instead
from redis_files.redis_key_standards import RedisKeyStandards
from aion_alert_dashboard.services.patterns import normalize_pattern_name, pattern_display_label
import threading
import time

class AlertValidationDashboard:
    def __init__(self):
        # ✅ CONSISTENCY: Use RedisManager82 for process-specific connection pools
        # Maintain single robust client for helper methods (get_time_buckets, retrieve_by_data_type, etc.)
        # Map of db -> redis.Redis client (direct connections from shared pool)
        self.redis_clients: Dict[int, Optional[redis.Redis]] = {}
        for db in (0, 1, 2, 4, 5):
            try:
                self.redis_clients[db] = RedisManager82.get_client(
                    process_name="dashboard",
                    db=db,
                    max_connections=None,  # Use PROCESS_POOL_CONFIG value
                    decode_responses=True
                )
            except Exception:
                self.redis_clients[db] = None
        
        # Create wrapper for compatibility with existing code that expects get_client method
        class RedisWrapper:
            def __init__(self, redis_clients):
                self.redis_clients = redis_clients
            def get_client(self, db):
                return self.redis_clients.get(db)
        
        self.redis_manager = RedisWrapper(self.redis_clients)
        
        # Legacy attributes preserved for downstream code paths
        self.redis_client = self.redis_clients.get(0)
        self.redis_db1 = self.redis_clients.get(1) or self.redis_client
        # DB 2 is analytics database where volume profile is stored
        self.redis_db2 = self.redis_clients.get(2) or self.redis_db1
        # DB 4 and DB 5 may also contain indicators/Greeks (user confirmed)
        self.redis_db4 = self.redis_clients.get(4) or self.redis_db1
        self.redis_db5 = self.redis_clients.get(5) or self.redis_db1
        self.alerts_data = []
        self.alerts_data_lock = threading.Lock()  # Thread-safe lock for alerts_data
        self.price_data = []
        self.correlation_data = []
        # Real-time subscription state
        self.alerts_stream_consumer_group = "dashboard_consumers"
        self.alerts_stream_consumer_name = f"dashboard_{os.getpid()}"
        self.new_alerts_count = 0  # Counter for triggering UI updates
        self.new_alerts_count_lock = threading.Lock()
        self.stream_reading_thread = None
        self.stream_reading_running = False
        # Forward validation stream reader
        self.validation_stream_consumer_group = "dashboard_validation_consumers"
        self.validation_stream_consumer_name = f"dashboard_validation_{os.getpid()}"
        self.validation_stream_reading_thread = None
        self.validation_stream_reading_running = False
        self.new_validations_count = 0
        self.new_validations_count_lock = threading.Lock()
        self.validation_results_lock = threading.Lock()  # Lock for validation_results updates
        self.intraday_instruments = set()  # Set of symbols from intraday crawler (standardized, no duplicates)
        self.intraday_instruments_full = {}  # Maps base symbol -> full key (e.g., "NIFTY25NOV25850CE" -> "NFO:NIFTY25NOV25850CE")
        self.instrument_cache = {}
        self.expiry_map = {}  # Maps symbol -> expiry date
        self.all_patterns = []  # All available patterns/strategies
        self.pattern_labels: Dict[str, str] = {}
        self.validation_results = []  # Validation results from alert_validator
        self.statistical_model_data = {}  # Statistical model data for analysis
        
        # ✅ SMART CACHING: In-memory indicator cache matching HybridCalculations and alert_manager
        self._indicator_cache = {}  # {symbol: {indicator_name: {timestamp, value}}}
        self._greeks_cache = {}  # {symbol: {greek_name: {timestamp, value}}}
        self._cache_ttl = 300  # 5 minutes TTL (matches HybridCalculations)
        self._cache_lock = threading.Lock()
        self._cache_max_size = 1000  # Max cache entries
        self._cache_hits = 0
        self._cache_misses = 0
        
        # ✅ FIX: Load validation results at startup
        try:
            self.load_validation_results()
        except Exception as e:
            print(f"⚠️ Could not load validation results at startup: {e}")
    
    # Class-level constants for Redis-aware lookups
    INDICATOR_NAMES: Tuple[str, ...] = (
        "rsi",
        "atr",
        "vwap",
        "volume_ratio",
        "ema_5",
        "ema_10",
        "ema_20",
        "ema_50",
        "ema_100",
        "ema_200",
        "macd",
        "bollinger_bands",
    )
    GREEK_NAMES: Tuple[str, ...] = ("delta", "gamma", "theta", "vega", "rho")
    NEWS_LOOKBACK_MINUTES: int = 180
    MAX_NEWS_ITEMS_PER_DAY: int = 200  # Reasonable cap based on crawler output

    # ------------------------------------------------------------------
    # Helper methods for Redis key construction and decoding
    # ------------------------------------------------------------------

    def _generate_symbol_variants(self, symbol: str) -> List[str]:
        """Return ordered, deduplicated list of symbol variants for direct Redis lookups."""
        if not symbol:
            return []
        
        variants: List[str] = []
        seen: set[str] = set()
        
        def _add(candidate: Optional[str]) -> None:
            if not candidate:
                return
            if candidate not in seen:
                seen.add(candidate)
                variants.append(candidate)
        
        base_symbol = symbol.split(":")[-1]
        normalized = base_symbol.replace(" ", "").replace("-", "")
        no_colon = symbol.replace(":", "")
        
        for candidate in (
            symbol,
            base_symbol,
            normalized,
            base_symbol.upper(),
            base_symbol.lower(),
            normalized.upper(),
            normalized.lower(),
            no_colon.upper(),
            no_colon.lower(),
        ):
            _add(candidate)
        
        for prefix in ("NFO", "NSE", "BSE"):
            _add(f"{prefix}:{base_symbol}")
            _add(f"{prefix}:{normalized}")
            _add(f"{prefix}:{base_symbol.upper()}")
            _add(f"{prefix}:{normalized.upper()}")
        
        return variants

    @staticmethod
    def _decode_redis_value(value: Any) -> Optional[str]:
        """Convert Redis return types to string for downstream JSON parsing."""
        if value is None:
            return None
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return None
        if isinstance(value, (str, int, float)):
            return str(value)
        return json.dumps(value)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """Parse ISO or epoch timestamp into datetime."""
        if value is None:
            return None
        try:
            if isinstance(value, datetime):
                return value
            if isinstance(value, (int, float)):
                # Treat large integers as epoch seconds
                return datetime.fromtimestamp(float(value))
            if isinstance(value, str):
                val = value.strip()
                if not val:
                    return None
                if val.isdigit():
                    return datetime.fromtimestamp(float(val))
                # Handle 'Z' suffix
                if val.endswith("Z"):
                    val = val.replace("Z", "+00:00")
                try:
                    return datetime.fromisoformat(val)
                except ValueError:
                    # Fallback common formats
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            return datetime.strptime(val, fmt)
                        except ValueError:
                            continue
        except Exception:
            return None
        return None

    def _snapshot_indicator_keys(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Collect indicator keys for debug views without using pattern matching.
        
        Returns:
            List of {db, key, indicator_name, value} dicts (value truncated for readability)
        """
        results: List[Dict[str, Any]] = []
        seen_keys: set[str] = set()
        redis_clients = [
            ("DB1", self.redis_db1),
            ("DB4", self.redis_db4),
            ("DB5", self.redis_db5),
        ]
        
        for variant in self._generate_symbol_variants(symbol):
            for indicator_name in self.INDICATOR_NAMES:
                keys_to_try = (
                    RedisKeyStandards.get_indicator_key(variant, indicator_name, use_cache=True),
                    RedisKeyStandards.get_indicator_key(variant, indicator_name, use_cache=False),
                )
                for db_name, client in redis_clients:
                    for key in keys_to_try:
                        if key in seen_keys:
                            continue
                        value_str = self._decode_redis_value(self._safe_redis_get(client, key))
                        if value_str is not None:
                            seen_keys.add(key)
                            results.append(
                                {
                                    "db": db_name,
                                    "key": key,
                                    "indicator": indicator_name,
                                    "value": value_str[:120],
                                }
                            )
                            if len(results) >= limit:
                                return results
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _safe_redis_get(client: Optional[redis.Redis], key: Optional[str]) -> Any:
        """Wrap Redis GET with safety checks."""
        if not client or not key:
            return None
        try:
            return client.get(key)
        except Exception as exc:
            logger.debug(f"Redis GET failed for key={key}: {exc}")
            return None

    @staticmethod
    def _safe_redis_ttl(client: Optional[redis.Redis], key: Optional[str]) -> Optional[int]:
        """Wrap Redis TTL with safety checks."""
        if not client or not key:
            return None
        try:
            return client.ttl(key)
        except Exception as exc:
            logger.debug(f"Redis TTL failed for key={key}: {exc}")
            return None

    def _load_bucket_series_from_manager(
        self,
        symbol: str,
        session_date: Optional[str] = None,
        lookback_minutes: int = 375,
    ) -> List[Dict[str, Any]]:
        """
        Fetch bucket series using RobustRedisClient helper (no pattern matching).
        Tries symbol variants and returns first non-empty result.
        """
        buckets: List[Dict[str, Any]] = []
        if not getattr(self, "redis_manager", None):
            return buckets
        
        variant_list = self._generate_symbol_variants(symbol)
        session_dates: List[str] = []
        if session_date:
            session_dates.append(session_date)
        else:
            today = datetime.now().date()
            session_dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
        
        for variant in variant_list:
            for session in session_dates:
                try:
                    data = self.redis_manager.get_time_buckets(
                        variant,
                        session_date=session,
                        lookback_minutes=lookback_minutes,
                        use_history_lists=True,
                    )
                    if data:
                        buckets.extend(
                            {
                                **bucket,
                                "_session_date": session,
                                "_symbol_variant": variant,
                            }
                            for bucket in data
                        )
                        # Continue searching other sessions for completeness
                except Exception as exc:
                    logger.debug(f"Time bucket fetch failed for {variant} on {session}: {exc}")
            if buckets:
                break  # Keep first successful variant to avoid mixing data across variants
        return buckets

    def _collect_recent_news_items(self, max_items: int = 50) -> List[Dict[str, Any]]:
        """Collect recent market news items using deterministic keys."""
        if not self.redis_db1:
            return []
        
        now = datetime.now()
        collected: List[Dict[str, Any]] = []
        
        # Include direct latest key if published
        latest_market_key = "news:MARKET_NEWS:latest"
        latest_payload = self._decode_redis_value(self._safe_redis_get(self.redis_db1, latest_market_key))
        if latest_payload:
            try:
                parsed_latest = json.loads(latest_payload)
                if isinstance(parsed_latest, dict):
                    parsed_latest["_redis_key"] = latest_market_key
                    collected.append(parsed_latest)
            except json.JSONDecodeError:
                collected.append({"headline": latest_payload, "_redis_key": latest_market_key})
        
        date_windows = [now - timedelta(days=offset) for offset in range(0, 2)]
        for date_obj in date_windows:
            date_str = date_obj.strftime("%Y%m%d")
            consecutive_misses = 0
            for item_idx in range(self.MAX_NEWS_ITEMS_PER_DAY):
                key = f"news:item:{date_str}:{item_idx}"
                value = self._decode_redis_value(self._safe_redis_get(self.redis_db1, key))
                if not value:
                    consecutive_misses += 1
                    if consecutive_misses >= 5:
                        break
                    continue
                consecutive_misses = 0
                
                try:
                    parsed = json.loads(value)
                except json.JSONDecodeError:
                    parsed = {"headline": value}
                
                payload = parsed.get("data") if isinstance(parsed, dict) and isinstance(parsed.get("data"), dict) else parsed
                if not isinstance(payload, dict):
                    continue
                
                ts_field = payload.get("timestamp") or payload.get("collected_at") or payload.get("published_at")
                parsed_ts = self._parse_datetime(ts_field)
                if parsed_ts and (now - parsed_ts) > timedelta(minutes=self.NEWS_LOOKBACK_MINUTES):
                    continue
                
                payload["_redis_key"] = key
                ttl_value = self._safe_redis_ttl(self.redis_db1, key)
                if ttl_value is not None:
                    payload["_ttl"] = ttl_value
                
                collected.append(payload)
                if len(collected) >= max_items:
                    return collected
        return collected

    @staticmethod
    def _dedupe_news_items(news_items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate news entries by Redis key or (headline, timestamp) fallback."""
        deduped: List[Dict[str, Any]] = []
        seen: set = set()
        for item in news_items:
            if not isinstance(item, dict):
                continue
            key = item.get("_redis_key") or (item.get("headline"), item.get("timestamp"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped
        
    
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
            self.pattern_labels = {}
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
                    normalized_patterns = []
                    for pattern_name in patterns:
                        if not pattern_name:
                            continue
                        key = normalize_pattern_name(pattern_name)
                        normalized_patterns.append(key)
                        if key not in self.pattern_labels:
                            self.pattern_labels[key] = pattern_display_label(key, pattern_name)
                    self.all_patterns = sorted(set(normalized_patterns))
                    print(f"✅ Loaded {len(self.all_patterns)} patterns from registry")
                    return
            
            # Fallback: Hardcoded list of all patterns
            fallback_patterns = [
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
            ]
            normalized_patterns = []
            for pattern_name in fallback_patterns:
                key = normalize_pattern_name(pattern_name)
                normalized_patterns.append(key)
                if key not in self.pattern_labels:
                    self.pattern_labels[key] = pattern_display_label(key, pattern_name)
            self.all_patterns = sorted(set(normalized_patterns))
            print(f"✅ Loaded {len(self.all_patterns)} patterns from fallback list")
        except Exception as e:
            logger.error(f"Error loading patterns: {e}")
            # Minimal fallback
            minimal = ['volume_spike', 'volume_breakout', 'kow_signal_straddle', 'reversal', 'breakout', 'NEWS_ALERT']
            self.all_patterns = sorted(set(normalize_pattern_name(p) for p in minimal))
            for pattern_name in minimal:
                key = normalize_pattern_name(pattern_name)
                if key not in self.pattern_labels:
                    self.pattern_labels[key] = pattern_display_label(key, pattern_name)
    
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
    
    def debug_indicators_loading(self, symbol: str):
        """Debug method to trace indicator loading for a symbol."""
        import time
        debug_info = {
            'symbol': symbol,
            'timestamp': time.time(),
            'steps': []
        }
        
        # Step 1: Check Redis streams
        try:
            stream_length = self.redis_db1.xlen("alerts:stream")
            debug_info['steps'].append(f"Redis stream length: {stream_length}")
            
            # Get last few messages for this symbol
            stream_messages = self.redis_db1.xrevrange("alerts:stream", count=50)
            symbol_messages = []
            for msg_id, msg_data in stream_messages:
                try:
                    # Handle bytes keys
                    data_field = msg_data.get('data') or msg_data.get(b'data')
                    if data_field:
                        if isinstance(data_field, bytes):
                            try:
                                import orjson
                                msg_data_parsed = orjson.loads(data_field)
                            except:
                                msg_data_parsed = json.loads(data_field.decode('utf-8'))
                        else:
                            msg_data_parsed = json.loads(data_field) if isinstance(data_field, str) else data_field
                        
                        msg_symbol = msg_data_parsed.get('symbol', '')
                        if msg_symbol and symbol in str(msg_symbol):
                            symbol_messages.append({
                                'msg_id': str(msg_id),
                                'symbol': msg_symbol,
                                'has_indicators': 'indicators' in str(msg_data_parsed),
                                'data_keys': list(msg_data_parsed.keys())[:10]  # First 10 keys
                            })
                except:
                    pass
            
            debug_info['steps'].append(f"Recent messages for {symbol}: {len(symbol_messages)}")
            debug_info['recent_messages'] = symbol_messages
        except Exception as e:
            debug_info['steps'].append(f"Redis stream error: {e}")
        
        # Step 2: Check Redis indicator keys
        try:
            indicator_snapshot = self._snapshot_indicator_keys(symbol, limit=20)
            debug_info['steps'].append(f"Redis indicator keys found: {len(indicator_snapshot)}")
            debug_info['indicator_keys'] = [entry['key'] for entry in indicator_snapshot]
            
            if indicator_snapshot:
                sample_entry = indicator_snapshot[0]
                debug_info['steps'].append(
                    f"Sample key ({sample_entry['key']} @ {sample_entry['db']}): {sample_entry['value']}"
                )
        except Exception as e:
            debug_info['steps'].append(f"Redis keys error: {e}")
        
        # Step 3: Check database connections
        try:
            debug_info['redis_db1_connected'] = self.redis_db1.ping()
            debug_info['redis_db4_connected'] = self.redis_db4.ping() if hasattr(self, 'redis_db4') else False
            debug_info['redis_db5_connected'] = self.redis_db5.ping() if hasattr(self, 'redis_db5') else False
        except Exception as e:
            debug_info['steps'].append(f"Redis connection check error: {e}")
        
        # Log debug info
        logger.error(f"🔍 INDICATOR DEBUG for {symbol}:")
        for step in debug_info['steps']:
            logger.error(f"   {step}")
        
        return debug_info
    
    def load_historical_indicators_for_symbol(self, symbol, lookback_hours=24):
        """Load historical indicator time series data from Redis"""
        historical_indicators = {}
        
        try:
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            # CRITICAL: Check exchange-prefixed variants (NFO, NSE, BSE) - data is stored with prefixes
            symbol_variants = [
                symbol,
                base_symbol,
                f"NFO:{base_symbol}",
                f"NSE:{base_symbol}",
                f"BSE:{base_symbol}",
                f"NFONFO{base_symbol}",  # Some keys may have double prefix
                f"NFONSE{base_symbol}",
                f"NFO{base_symbol}",  # Direct prefix without colon
                f"NSE{base_symbol}",
                f"BSE{base_symbol}",
            ]
            
            # Define the time range for historical data
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=lookback_hours)
            
            for variant in symbol_variants:
                # METHOD 1: Check Redis Time Series (if using RedisTimeSeries)
                try:
                    ts_keys = [
                        f"ts:indicators:{variant}:rsi",
                        f"ts:indicators:{variant}:macd", 
                        f"ts:indicators:{variant}:ema_20",
                        f"ts:indicators:{variant}:ema_50",
                        f"ts:indicators:{variant}:vwap",
                        f"ts:indicators:{variant}:bb_upper",
                        f"ts:indicators:{variant}:bb_lower"
                    ]
                    
                    for db_client in [self.redis_db1, self.redis_db4, self.redis_db5]:
                        for ts_key in ts_keys:
                            if db_client.exists(ts_key):
                                print(f"✅ Found time series: {ts_key}")
                                # Implement time series range query here
                                # This depends on your Redis time series setup
                except Exception as e:
                    print(f"Time series check failed: {e}")
                
                # METHOD 2: Check OHLC streams for price-based indicator calculation
                try:
                    ohlc_stream_key = f"ohlc_updates:{variant}"
                    stream_data = self.redis_db1.xrange(ohlc_stream_key, min='-', max='+', count=1000)
                    
                    if stream_data:
                        print(f"✅ Found {len(stream_data)} OHLC records for {variant}")
                        
                        # Extract timestamps and prices for indicator calculation
                        timestamps = []
                        closes = []
                        volumes = []
                        highs = []
                        lows = []
                        
                        for msg_id, fields in stream_data:
                            try:
                                # Parse timestamp
                                if 'timestamp' in fields:
                                    ts_val = fields['timestamp']
                                elif b'timestamp' in fields:
                                    ts_val = fields[b'timestamp']
                                else:
                                    ts_val = msg_id.split('-')[0] if isinstance(msg_id, str) and '-' in msg_id else None
                                
                                if ts_val:
                                    if isinstance(ts_val, bytes):
                                        ts_val = ts_val.decode('utf-8')
                                    timestamp_ms = int(ts_val)
                                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0)
                                    
                                    # Parse OHLC values
                                    close_val = float(fields.get('close', fields.get(b'close', 0)))
                                    high_val = float(fields.get('high', fields.get(b'high', 0)))
                                    low_val = float(fields.get('low', fields.get(b'low', 0)))
                                    volume_val = float(fields.get('volume', fields.get(b'volume', 0)))
                                    
                                    if close_val > 0:
                                        timestamps.append(timestamp)
                                        closes.append(close_val)
                                        highs.append(high_val)
                                        lows.append(low_val)
                                        volumes.append(volume_val)
                            except Exception as e:
                                continue
                        
                        # Calculate indicators from OHLC data if we have enough points
                        if len(closes) >= 20:
                            print(f"📊 Calculating indicators from {len(closes)} price points for {symbol}")
                            
                            # Convert to numpy for calculations
                            close_prices = np.array(closes)
                            high_prices = np.array(highs)
                            low_prices = np.array(lows)
                            volume_array = np.array(volumes)
                            
                            # Calculate RSI
                            rsi_values = self.calculate_rsi(close_prices, period=14)
                            if rsi_values is not None and len(rsi_values) > 0:
                                # Store both time-series and latest value for backward compatibility
                                historical_indicators['rsi'] = rsi_values[-1]  # Latest value
                                historical_indicators['rsi_ts'] = {
                                    'values': rsi_values.tolist(),
                                    'timestamps': timestamps[-len(rsi_values):]
                                }
                            
                            # Calculate EMA
                            ema_20_values = self.calculate_ema(close_prices, period=20)
                            if ema_20_values is not None and len(ema_20_values) > 0:
                                historical_indicators['ema_20'] = ema_20_values[-1]  # Latest value
                                historical_indicators['ema_20_ts'] = {
                                    'values': ema_20_values.tolist(),
                                    'timestamps': timestamps
                                }
                            
                            ema_50_values = self.calculate_ema(close_prices, period=50)
                            if ema_50_values is not None and len(ema_50_values) > 0:
                                historical_indicators['ema_50'] = ema_50_values[-1]  # Latest value
                                historical_indicators['ema_50_ts'] = {
                                    'values': ema_50_values.tolist(),
                                    'timestamps': timestamps
                                }
                            
                            # Calculate VWAP
                            vwap_values = self.calculate_vwap(close_prices, volume_array, high_prices, low_prices)
                            if vwap_values is not None and len(vwap_values) > 0:
                                historical_indicators['vwap'] = vwap_values[-1]  # Latest value
                                historical_indicators['vwap_ts'] = {
                                    'values': vwap_values.tolist(),
                                    'timestamps': timestamps
                                }
                            
                            # Calculate MACD
                            macd_data = self.calculate_macd(close_prices)
                            if macd_data:
                                historical_indicators['macd'] = {
                                    'macd': macd_data['macd'][-1] if len(macd_data['macd']) > 0 else 0,
                                    'signal': macd_data['signal'][-1] if len(macd_data['signal']) > 0 else 0,
                                    'histogram': macd_data['histogram'][-1] if len(macd_data['histogram']) > 0 else 0
                                }
                                historical_indicators['macd_ts'] = {
                                    'macd': macd_data['macd'].tolist(),
                                    'signal': macd_data['signal'].tolist(),
                                    'histogram': macd_data['histogram'].tolist(),
                                    'timestamps': timestamps[-len(macd_data['macd']):]
                                }
                            
                            # Calculate Bollinger Bands
                            bb_data = self.calculate_bollinger_bands(close_prices, period=20)
                            if bb_data:
                                historical_indicators['bb_upper'] = bb_data['upper'][-1] if len(bb_data['upper']) > 0 else None
                                historical_indicators['bb_middle'] = bb_data['middle'][-1] if len(bb_data['middle']) > 0 else None
                                historical_indicators['bb_lower'] = bb_data['lower'][-1] if len(bb_data['lower']) > 0 else None
                                historical_indicators['bollinger_bands'] = {
                                    'upper': bb_data['upper'].tolist(),
                                    'middle': bb_data['middle'].tolist(),
                                    'lower': bb_data['lower'].tolist(),
                                    'timestamps': timestamps[-len(bb_data['upper']):]
                                }
                            
                            break  # Found data for this variant
                            
                except Exception as e:
                    print(f"OHLC stream processing failed for {variant}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error loading historical indicators for {symbol}: {e}")
            import traceback
            print(traceback.format_exc())
        
        return historical_indicators
    
    def load_indicators_for_symbol(self, symbol: str) -> dict:
        """
        Load technical indicators from Redis - Wrapper that calls historical method
        Returns latest values for backward compatibility
        
        Priority:
        1. Try Redis direct lookup (fastest - if scanner stored them)
        2. Try historical indicators (includes OHLC calculation fallback)
        3. Try Redis fallback with more symbol variants
        """
        # First try direct Redis lookup (fastest path if indicators exist)
        indicators = self._load_indicators_from_redis_fallback(symbol)
        
        # If not found, try historical method (includes OHLC calculation)
        if not indicators:
            historical = self.load_historical_indicators_for_symbol(symbol)
            
            # Extract latest values for backward compatibility (existing code expects single values)
            for key, value in historical.items():
                # Skip time-series keys (_ts suffix)
                if not key.endswith('_ts'):
                    indicators[key] = value
        
        return indicators
    
    
    def _get_cached_indicator(self, symbol: str, indicator_name: str) -> Optional[Any]:
        """Get cached indicator if available and not expired."""
        with self._cache_lock:
            if symbol in self._indicator_cache:
                symbol_cache = self._indicator_cache[symbol]
                if indicator_name in symbol_cache:
                    cached = symbol_cache[indicator_name]
                    if time.time() - cached['timestamp'] < self._cache_ttl:
                        self._cache_hits += 1
                        return cached['value']
            self._cache_misses += 1
            return None
    
    def _cache_indicator(self, symbol: str, indicator_name: str, value: Any):
        """Cache indicator value with timestamp."""
        with self._cache_lock:
            # Enforce cache size bounds
            if len(self._indicator_cache) >= self._cache_max_size:
                # Remove oldest entry
                oldest_symbol = min(self._indicator_cache.keys(), 
                                  key=lambda k: min(
                                      [x.get('timestamp', 0) if isinstance(x, dict) else 0 
                                       for x in (self._indicator_cache.get(k).values() 
                                                if isinstance(self._indicator_cache.get(k), dict) else [])] or [0]
                                  ))
                del self._indicator_cache[oldest_symbol]
            
            if symbol not in self._indicator_cache:
                self._indicator_cache[symbol] = {}
            
            self._indicator_cache[symbol][indicator_name] = {
                'timestamp': time.time(),
                'value': value
            }
    
    def _load_indicators_from_redis_fallback(self, symbol: str) -> dict:
        """
        ✅ SMART CACHING: Redis lookup logic with in-memory cache.
        Checks cache first, then Redis (DB 5 → DB 1 → DB 4), then caches result.
        
        Scanner stores via: store_by_data_type("analysis_cache", f"indicators:{symbol}:{indicator_name}", value)
        Storage location: DB 5 (primary per user), DB 1 (realtime fallback), DB 4 (fallback)
        Key format: indicators:{symbol}:{indicator_name} (stored AS-IS, no prefix added)
        """
        indicators = {}
        
        # ✅ SMART CACHING: Check cache first
        indicator_names = [
            'rsi', 'atr', 'vwap', 'volume_ratio', 
            'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
            'macd', 'bollinger_bands'
        ]
        
        cached_indicators = {}
        for indicator_name in indicator_names:
            cached_value = self._get_cached_indicator(symbol, indicator_name)
            if cached_value is not None:
                cached_indicators[indicator_name] = cached_value
        
        # If all indicators found in cache, return immediately
        if len(cached_indicators) == len(indicator_names):
            return cached_indicators
        
        # Merge cached indicators
        indicators.update(cached_indicators)
        
        # CRITICAL: Match scanner's symbol format exactly
        # Scanner uses symbol exactly as received from tick_data (can be with/without exchange prefix)
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        clean_base = base_symbol.replace(' ', '').replace('-', '').upper()
        
        # Try ALL possible symbol variants (matching scanner storage logic)
        symbol_variants = [
            symbol,  # Original
            symbol.upper(),
            symbol.lower(),
            base_symbol,  # Without exchange prefix
            base_symbol.upper(),
            base_symbol.lower(),
            clean_base,
            f"NFO:{base_symbol}",  # With NFO prefix
            f"NFO:{base_symbol.upper()}",
            f"NSE:{base_symbol}",  # With NSE prefix
            f"NSE:{base_symbol.upper()}",
            symbol.replace(':', '').upper(),  # No colon
            symbol.replace(':', '').lower(),
        ]
        # Remove duplicates while preserving order
        seen = set()
        symbol_variants = [x for x in symbol_variants if not (x in seen or seen.add(x))]
        
        indicator_names = [
            'rsi', 'atr', 'vwap', 'volume_ratio', 
            'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
            'macd', 'bollinger_bands'
        ]
        
        # CRITICAL: Use retrieve_by_data_type() to match scanner's retrieval method exactly
        # Scanner stores via store_by_data_type("analysis_cache", ...), so we retrieve the same way
        # Check if we have access to RobustRedisClient with retrieve_by_data_type
        has_retrieve_method = False
        redis_wrapper = None
        try:
            # Try to get the RobustRedisClient wrapper (has retrieve_by_data_type method)
            if hasattr(self, 'redis_client') and hasattr(self.redis_client, 'retrieve_by_data_type'):
                redis_wrapper = self.redis_client
                has_retrieve_method = True
            elif hasattr(self, 'redis_db1') and hasattr(self.redis_db1, 'retrieve_by_data_type'):
                redis_wrapper = self.redis_db1
                has_retrieve_method = True
        except:
            pass
        
        # ✅ PRIORITY: Check DB 5 first (user confirmed indicators are in DB 5), then DB 1, then DB 4
        redis_dbs = [
            ('DB5', self.redis_db5),
            ('DB1', self.redis_db1),
        ]
        
        # Method 1: Use retrieve_by_data_type if available (matches scanner exactly)
        if has_retrieve_method and redis_wrapper:
            # Fetch missing indicators from Redis (check DB 5 → DB 1 → DB 4)
            missing_indicator_names = [name for name in indicator_names if name not in cached_indicators]
            
            for variant in symbol_variants:
                found_any = False
                for indicator_name in missing_indicator_names:
                    redis_key = f"indicators:{variant}:{indicator_name}"
                    
                    # Try each Redis DB in priority order (DB 5 → DB 1 → DB 4)
                    value = None
                    for db_name, db_client in redis_dbs:
                        if not db_client:
                            continue
                        try:
                            # Try retrieve_by_data_type first (matches scanner storage)
                            # ✅ FIX: Check indicators_cache (DB 5) first, then analysis_cache (DB 1) for backward compatibility
                            if hasattr(db_client, 'retrieve_by_data_type'):
                                try:
                                    # Priority 1: indicators_cache (DB 5) - where scanner now stores
                                    value = db_client.retrieve_by_data_type(redis_key, "indicators_cache")
                                    if value:
                                        break
                                except Exception:
                                    pass
                                
                                # Priority 2: analysis_cache (DB 1) - legacy location for backward compatibility
                                if not value:
                                    try:
                                        value = db_client.retrieve_by_data_type(redis_key, "analysis_cache")
                                        if value:
                                            break
                                    except Exception:
                                        pass
                            
                            # Fallback to direct GET
                            if not value:
                                try:
                                    value = db_client.get(redis_key)
                                    if value:
                                        break
                                except Exception:
                                    continue
                        except Exception as e:
                            logger.debug(f"Error checking {db_name} for {redis_key}: {e}")
                            continue
                    
                    if value:
                        try:
                            # Handle bytes
                            if isinstance(value, bytes):
                                value = value.decode('utf-8')
                            
                            # Try parsing as JSON first (for complex indicators with 'value' wrapper)
                            try:
                                parsed = json.loads(value)
                                if isinstance(parsed, dict):
                                    # Check for nested structure: {'value': {...}, 'timestamp': ..., 'symbol': ...}
                                    if 'value' in parsed:
                                        # Extract actual indicator value
                                        indicator_value = parsed['value']
                                    else:
                                        indicator_value = parsed
                                else:
                                    indicator_value = parsed
                                
                                indicators[indicator_name] = indicator_value
                                # ✅ SMART CACHING: Cache the fetched indicator
                                self._cache_indicator(symbol, indicator_name, indicator_value)
                                found_any = True
                            except (json.JSONDecodeError, TypeError):
                                # Simple numeric/string value (RSI, EMA, ATR, VWAP, etc.)
                                try:
                                    indicator_value = float(value)
                                except (ValueError, TypeError):
                                    indicator_value = value
                                
                                indicators[indicator_name] = indicator_value
                                # ✅ SMART CACHING: Cache the fetched indicator
                                self._cache_indicator(symbol, indicator_name, indicator_value)
                                found_any = True
                        except Exception as e:
                            logger.debug(f"Error parsing indicator {indicator_name} for {variant}: {e}")
                
                if found_any:
                    # Found indicators for this variant - return immediately
                    logger.debug(f"✅ Loaded {len(indicators)} indicators from Redis for {variant} (original: {symbol})")
                    # Merge cached indicators back in
                    indicators.update(cached_indicators)
                    return indicators
        
        return indicators
    
    def _get_cached_greek(self, symbol: str, greek_name: str) -> Optional[Any]:
        """Get cached Greek if available and not expired."""
        with self._cache_lock:
            if symbol in self._greeks_cache:
                symbol_cache = self._greeks_cache[symbol]
                if greek_name in symbol_cache:
                    cached = symbol_cache[greek_name]
                    if time.time() - cached['timestamp'] < self._cache_ttl:
                        self._cache_hits += 1
                        return cached['value']
            self._cache_misses += 1
            return None
    
    def _cache_greek(self, symbol: str, greek_name: str, value: Any):
        """Cache Greek value with timestamp."""
        with self._cache_lock:
            # Enforce cache size bounds
            if len(self._greeks_cache) >= self._cache_max_size:
                # Remove oldest entry
                oldest_symbol = min(self._greeks_cache.keys(), 
                                  key=lambda k: min(
                                      [x.get('timestamp', 0) if isinstance(x, dict) else 0 
                                       for x in (self._greeks_cache.get(k).values() 
                                                if isinstance(self._greeks_cache.get(k), dict) else [])] or [0]
                                  ))
                del self._greeks_cache[oldest_symbol]
            
            if symbol not in self._greeks_cache:
                self._greeks_cache[symbol] = {}
            
            self._greeks_cache[symbol][greek_name] = {
                'timestamp': time.time(),
                'value': value
            }
    
    def _fetch_greeks_from_redis(self, symbol: str) -> dict:
        """
        ✅ SMART CACHING: Fetch Options Greeks from Redis with in-memory cache.
        Checks cache first, then Redis (DB 5 → DB 1 → DB 4), then caches result.
        
        Redis Storage Schema:
        - DB 5 (primary per user): indicators:{symbol}:greeks (combined dict) and indicators:{symbol}:{greek} (individual)
        - DB 1 (realtime): indicators:{symbol}:greeks (combined dict) and indicators:{symbol}:{greek} (individual)
        - DB 4 (fallback): Additional indicator storage
        - Format: Combined stored as JSON with {'value': {'delta': ..., 'gamma': ..., ...}, ...}
                  Individual Greeks stored as string/number
        """
        greeks = {}
        
        # ✅ SMART CACHING: Check cache first
        greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
        cached_greeks = {}
        for greek_name in greek_names:
            cached_value = self._get_cached_greek(symbol, greek_name)
            if cached_value is not None:
                cached_greeks[greek_name] = cached_value
        
        # ✅ PRIORITY: Check DB 5 first (user confirmed indicators/Greeks are in DB 5), then DB 1, then DB 4
        redis_dbs = [
            ('DB5', self.redis_db5),
            ('DB1', self.redis_db1),
            ('DB4', self.redis_db4),
        ]
        
        try:
            # Merge cached Greeks
            greeks.update(cached_greeks)
            
            # Normalize symbol (same variants as alert_manager)
            symbol_variants = [
                symbol,
                symbol.split(':')[-1] if ':' in symbol else symbol,
                f"NFO:{symbol.split(':')[-1]}" if ':' not in symbol and ('NIFTY' in symbol.upper() or 'BANKNIFTY' in symbol.upper()) else symbol,
                f"NSE:{symbol.split(':')[-1]}" if ':' not in symbol else symbol,
            ]
            
            # Fetch missing Greeks from Redis (check DB 5 → DB 1 → DB 4)
            missing_greek_names = [name for name in greek_names if name not in cached_greeks]
            
            for variant in symbol_variants:
                # Try combined greeks key first (check DB 5 → DB 1 → DB 4)
                greeks_key = f"indicators:{variant}:greeks"
                greeks_data = None
                
                for db_name, db_client in redis_dbs:
                    if not db_client:
                        continue
                    try:
                             # Try retrieve_by_data_type first (matches scanner storage)
                             # ✅ FIX: Check indicators_cache (DB 5) first, then analysis_cache (DB 1) for backward compatibility
                             if hasattr(db_client, 'retrieve_by_data_type'):
                                 try:
                                     # Priority 1: indicators_cache (DB 5) - where scanner now stores
                                     greeks_data = db_client.retrieve_by_data_type(greeks_key, "indicators_cache")
                                     if greeks_data:
                                         break
                                 except Exception:
                                     pass
                                 
                                 # Priority 2: analysis_cache (DB 1) - legacy location for backward compatibility
                                 if not greeks_data:
                                     try:
                                         greeks_data = db_client.retrieve_by_data_type(greeks_key, "analysis_cache")
                                         if greeks_data:
                                             break
                                     except Exception:
                                         pass
                                 
                                 # Fallback to direct GET
                                 if not greeks_data:
                                     try:
                                         greeks_data = db_client.get(greeks_key)
                                         if greeks_data:
                                             break
                                     except Exception:
                                         continue
                    except Exception as e:
                        logger.debug(f"Error checking {db_name} for {greeks_key}: {e}")
                        continue
                
                if greeks_data:
                    try:
                        if isinstance(greeks_data, bytes):
                            greeks_data = greeks_data.decode('utf-8')
                        parsed = json.loads(greeks_data) if isinstance(greeks_data, str) else greeks_data
                        if isinstance(parsed, dict):
                            if 'value' in parsed and isinstance(parsed['value'], dict):
                                # ✅ SMART CACHING: Cache each Greek from combined dict
                                for greek_name, greek_value in parsed['value'].items():
                                    if greek_name in greek_names:
                                        greeks[greek_name] = greek_value
                                        self._cache_greek(symbol, greek_name, greek_value)
                            else:
                                # ✅ SMART CACHING: Cache each Greek
                                for greek_name, greek_value in parsed.items():
                                    if greek_name in greek_names:
                                        greeks[greek_name] = greek_value
                                        self._cache_greek(symbol, greek_name, greek_value)
                            break
                    except Exception as e:
                        logger.debug(f"Error getting combined Greeks for {variant}: {e}")
                
                # Try individual Greeks (check DB 5 → DB 1 → DB 4)
                found_any = False
                for greek_name in missing_greek_names:
                    greek_key = f"indicators:{variant}:{greek_name}"
                    greek_value = None
                    
                    # Try each Redis DB in priority order
                    for db_name, db_client in redis_dbs:
                        if not db_client:
                            continue
                        try:
                                    # Try retrieve_by_data_type first
                                    # ✅ FIX: Check indicators_cache (DB 5) first, then analysis_cache (DB 1) for backward compatibility
                                    if hasattr(db_client, 'retrieve_by_data_type'):
                                        try:
                                            # Priority 1: indicators_cache (DB 5) - where scanner now stores
                                            greek_value = db_client.retrieve_by_data_type(greek_key, "indicators_cache")
                                            if greek_value:
                                                break
                                        except Exception:
                                            pass
                                        
                                        # Priority 2: analysis_cache (DB 1) - legacy location for backward compatibility
                                        if not greek_value:
                                            try:
                                                greek_value = db_client.retrieve_by_data_type(greek_key, "analysis_cache")
                                                if greek_value:
                                                    break
                                            except Exception:
                                                pass
                                        
                                        # Fallback to direct GET
                                        if not greek_value:
                                            try:
                                                greek_value = db_client.get(greek_key)
                                                if greek_value:
                                                    break
                                            except Exception:
                                                continue
                                    
                                    # Fallback to direct GET if retrieve_by_data_type not available
                                    if not greek_value:
                                        try:
                                            greek_value = db_client.get(greek_key)
                                            if greek_value:
                                                break
                                        except Exception:
                                            continue
                        except Exception as e:
                            logger.debug(f"Error checking {db_name} for {greek_key}: {e}")
                            continue
                    
                    if greek_value:
                        try:
                            if isinstance(greek_value, bytes):
                                greek_value = greek_value.decode('utf-8')
                            try:
                                greek_value_float = float(greek_value)
                            except (ValueError, TypeError):
                                try:
                                    parsed = json.loads(greek_value) if isinstance(greek_value, str) else greek_value
                                    if isinstance(parsed, dict) and 'value' in parsed:
                                        greek_value_float = parsed['value']
                                    else:
                                        greek_value_float = parsed
                                except (json.JSONDecodeError, TypeError):
                                    greek_value_float = greek_value
                            
                            greeks[greek_name] = greek_value_float
                            # ✅ SMART CACHING: Cache the fetched Greek
                            self._cache_greek(symbol, greek_name, greek_value_float)
                            found_any = True
                        except Exception as e:
                            logger.debug(f"Error getting {greek_name} for {variant}: {e}")
                
                if found_any or greeks:
                    break
            
            # Merge cached Greeks back in
            greeks.update(cached_greeks)
            
            if greeks:
                logger.debug(f"✅ Loaded Greeks from Redis for {symbol}: {list(greeks.keys())}")
            else:
                logger.debug(f"⚠️ No Greeks found in Redis for {symbol}")
        except Exception as e:
            logger.debug(f"Error fetching Greeks from Redis for {symbol}: {e}")
        
        return greeks
    
    def calculate_rsi(self, prices, period=14):
        """Calculate RSI from price data"""
        if len(prices) < period + 1:
            return None
            
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gains = np.zeros_like(prices)
        avg_losses = np.zeros_like(prices)
        
        # Initial values
        avg_gains[period] = np.mean(gains[:period])
        avg_losses[period] = np.mean(losses[:period])
        
        # Smooth the averages
        for i in range(period + 1, len(prices)):
            avg_gains[i] = (avg_gains[i-1] * (period - 1) + gains[i-1]) / period
            avg_losses[i] = (avg_losses[i-1] * (period - 1) + losses[i-1]) / period
        
        # Calculate RS and RSI
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi[period:]
    
    def calculate_ema(self, prices, period):
        """Calculate Exponential Moving Average"""
        if len(prices) < period:
            return None
        return pd.Series(prices).ewm(span=period, adjust=False).mean().values
    
    def calculate_vwap(self, closes, volumes, highs, lows):
        """Calculate Volume Weighted Average Price"""
        typical_prices = (highs + lows + closes) / 3
        cumulative_typical_volume = np.cumsum(typical_prices * volumes)
        cumulative_volume = np.cumsum(volumes)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            vwap = np.where(cumulative_volume != 0, cumulative_typical_volume / cumulative_volume, typical_prices)
        
        return vwap
    
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """Calculate MACD indicator"""
        if len(prices) < slow:
            return None
            
        ema_fast = pd.Series(prices).ewm(span=fast, adjust=False).mean()
        ema_slow = pd.Series(prices).ewm(span=slow, adjust=False).mean()
        
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line.values,
            'signal': signal_line.values,
            'histogram': histogram.values
        }
    
    def calculate_bollinger_bands(self, prices, period=20, std_dev=2):
        """Calculate Bollinger Bands"""
        if len(prices) < period:
            return None
            
        rolling_mean = pd.Series(prices).rolling(window=period).mean()
        rolling_std = pd.Series(prices).rolling(window=period).std()
        
        upper_band = rolling_mean + (rolling_std * std_dev)
        lower_band = rolling_mean - (rolling_std * std_dev)
        
        return {
            'upper': upper_band.values,
            'middle': rolling_mean.values,
            'lower': lower_band.values
        }
    
    def _safe_float(self, value):
        """Safely convert value to float, handling None, empty strings, and invalid values"""
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if not value or value in ['None', 'null', '']:
                return None
        try:
            num_value = float(value)
            # Return None for NaN or Inf values
            if np.isnan(num_value) or np.isinf(num_value):
                return None
            return num_value
        except (ValueError, TypeError):
            return None
    
    def debug_redis_indicators(self, symbol):
        """Debug what indicator keys actually exist in Redis for a symbol"""
        print(f"\n🔍 DEBUGGING REDIS FOR SYMBOL: {symbol}")
        
        redis_clients = [
            ("DB1", self.redis_db1),
            ("DB4", self.redis_db4),
            ("DB5", self.redis_db5),
        ]
        symbol_variants = self._generate_symbol_variants(symbol)
        
        for db_name, redis_client in redis_clients:
            print(f"\n📁 Checking {db_name}:")
            if not redis_client:
                print("  ⚠️ Redis client unavailable")
                continue
            
            hits = 0
            for variant in symbol_variants:
                for indicator_name in self.INDICATOR_NAMES:
                    for use_cache in (True, False):
                        key = RedisKeyStandards.get_indicator_key(variant, indicator_name, use_cache=use_cache)
                        try:
                            value = self._safe_redis_get(redis_client, key)
                            if value is None:
                                continue
                            hits += 1
                            decoded = self._decode_redis_value(value)
                            cache_label = "analysis_cache" if use_cache else "direct"
                            preview = decoded[:100] if decoded else "[binary/unprintable]"
                            print(f"  ✅ {variant} [{cache_label}] {indicator_name}: {preview}...")
                        except Exception as exc:
                            print(f"  ❌ {variant} key={key}: {exc}")
            
            if hits == 0:
                print("  ⚠️ No indicators found in this database for any variant")
    
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
        - DB 1 (realtime via analysis_cache): analysis_cache:indicators:{symbol}:greeks and analysis_cache:indicators:{symbol}:{greek}
        - Format: Combined stored as JSON with {'value': {'delta': ..., 'gamma': ..., ...}, ...}
                  Individual Greeks stored as string/number
        - Calculated by: EnhancedGreekCalculator.black_scholes_greeks() using scipy.stats.norm (pure Python)
        """
        greeks = {}
        try:
            # Normalize symbol - AGGRESSIVE matching to handle all symbol formats (same as load_indicators_for_symbol)
            original_symbol = symbol
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            clean_base = base_symbol.replace(' ', '').replace('-', '').upper()
            
            symbol_variants = [
                # Original variations
                symbol,
                symbol.upper(),
                symbol.lower(),
                base_symbol,
                base_symbol.upper(),
                base_symbol.lower(),
                clean_base,
                # Exchange prefix variants (with colon)
                f"NSE:{base_symbol}",
                f"NSE:{base_symbol.upper()}",
                f"NFO:{base_symbol}",
                f"NFO:{base_symbol.upper()}",
                f"BSE:{base_symbol}",
                f"BSE:{base_symbol.upper()}",
                f"FO:{base_symbol}",
                f"FO:{base_symbol.upper()}",
                # Exchange prefix variants (without colon - direct prefix)
                f"NSE{base_symbol}",
                f"NSE{base_symbol.upper()}",
                f"NFO{base_symbol}",
                f"NFO{base_symbol.upper()}",
                f"BSE{base_symbol}",
                f"BSE{base_symbol.upper()}",
                # Additional formats
                symbol.replace(':', '').upper(),
                symbol.replace(':', '').lower(),
                base_symbol.replace(':', '').upper(),
            ]
            
            # Remove duplicates while preserving order
            seen = set()
            symbol_variants = [x for x in symbol_variants if not (x in seen or seen.add(x))]
            
            # Greek names (from EnhancedGreekCalculator)
            greek_names = ['delta', 'gamma', 'theta', 'vega', 'rho']
            
            # Check multiple databases: DB 1 (primary), DB 2 (analytics), DB 4, DB 5 (fallbacks)
            redis_clients = [
                ('DB1', self.redis_db1),  # Primary location (realtime, analysis_cache)
                ('DB2', self.redis_db2),  # Analytics database (may have analysis_cache)
                ('DB4', self.redis_db4),  # Fallback
                ('DB5', self.redis_db5)   # Fallback
            ]
            
            # CRITICAL: Try using retrieve_by_data_type first (matches scanner exactly)
            # Check if we have RobustRedisClient wrapper with retrieve_by_data_type
            has_retrieve_method = False
            redis_wrapper = None
            try:
                if hasattr(self, 'redis_client') and hasattr(self.redis_client, 'retrieve_by_data_type'):
                    redis_wrapper = self.redis_client
                    has_retrieve_method = True
                elif hasattr(self, 'redis_db1') and hasattr(self.redis_db1, 'retrieve_by_data_type'):
                    redis_wrapper = self.redis_db1
                    has_retrieve_method = True
            except:
                pass
            
            for variant in symbol_variants:
                # ✅ METHOD 1: Try combined greeks key first (preferred - more efficient)
                # Key format: indicators:{symbol}:greeks
                # Stored by redis_storage.publish_indicators_to_redis() as JSON
                greeks_key = f"indicators:{variant}:greeks"
                
                # Try retrieve_by_data_type first (matches scanner)
                # ✅ FIX: Check indicators_cache (DB 5) first, then analysis_cache (DB 1) for backward compatibility
                if has_retrieve_method and redis_wrapper:
                    greeks_data = None
                    try:
                        # Priority 1: indicators_cache (DB 5) - where scanner now stores
                        greeks_data = redis_wrapper.retrieve_by_data_type(greeks_key, "indicators_cache")
                    except Exception:
                        pass
                    
                    # Priority 2: analysis_cache (DB 1) - legacy location for backward compatibility
                    if not greeks_data:
                        try:
                            greeks_data = redis_wrapper.retrieve_by_data_type(greeks_key, "analysis_cache")
                        except Exception:
                            pass
                    
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
                            logger.debug(f"✅ Loaded combined Greeks using retrieve_by_data_type for {variant}")
                            break  # Found combined greeks
                        except Exception as e:
                            logger.debug(f"Error parsing combined Greeks for {variant}: {e}")
                
                # Fallback to direct GET
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
                                        greek_float = self._safe_float(greek_value)
                                        if greek_float is not None:
                                            greeks[greek_name] = greek_float
                                            found_any = True
                                            logger.debug(f"✅ Loaded {greek_name} for {variant} from {db_name}: {greek_float}")
                                    except (ValueError, TypeError):
                                        # Try JSON parse in case it's wrapped
                                        try:
                                            parsed = json.loads(greek_value)
                                            if isinstance(parsed, dict) and 'value' in parsed:
                                                greek_float = self._safe_float(parsed['value'])
                                                if greek_float is not None:
                                                    greeks[greek_name] = greek_float
                                                    found_any = True
                                            else:
                                                greek_float = self._safe_float(parsed)
                                                if greek_float is not None:
                                                    greeks[greek_name] = greek_float
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
                
                # ✅ METHOD 3: Try analysis_cache format (analysis_cache:indicators:{symbol}:greeks)
                # This is how redis_storage.py stores indicators via store_by_data_type
                for db_name, redis_client in redis_clients:
                    try:
                        # Try combined greeks in analysis_cache format
                        cache_greeks_key = f"analysis_cache:indicators:{variant}:greeks"
                        greeks_data = redis_client.get(cache_greeks_key)
                        
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
                                        if isinstance(parsed['value'], dict):
                                            greeks.update(parsed['value'])
                                        else:
                                            greeks.update(parsed['value'])
                                    else:
                                        greeks.update(parsed)
                                logger.debug(f"✅ Loaded combined Greeks from analysis_cache for {variant} from {db_name}")
                                break  # Found combined greeks
                            except Exception as e:
                                logger.debug(f"Error parsing combined Greeks from analysis_cache for {variant} from {db_name}: {e}")
                    except Exception as e:
                        logger.debug(f"Error getting combined Greeks from analysis_cache for {variant} from {db_name}: {e}")
                
                if greeks:
                    break  # Found combined greeks
                
                # ✅ METHOD 4: Try individual greek keys in analysis_cache format
                found_any_cache = False
                for db_name, redis_client in redis_clients:
                    for greek_name in greek_names:
                        cache_greek_key = f"analysis_cache:indicators:{variant}:{greek_name}"
                        
                        try:
                            greek_value = redis_client.get(cache_greek_key)
                            
                            if greek_value:
                                try:
                                    # Handle bytes
                                    if isinstance(greek_value, bytes):
                                        greek_value = greek_value.decode('utf-8')
                                    
                                    # Parse as float (Greeks are numeric)
                                    greek_float = self._safe_float(greek_value)
                                    if greek_float is not None:
                                        greeks[greek_name] = greek_float
                                        found_any_cache = True
                                        logger.debug(f"✅ Loaded {greek_name} from analysis_cache for {variant} from {db_name}: {greek_float}")
                                except Exception as e:
                                    logger.debug(f"Error parsing {greek_name} from analysis_cache for {variant} from {db_name}: {e}")
                        except Exception as e:
                            logger.debug(f"Error getting {greek_name} from analysis_cache for {variant} from {db_name}: {e}")
                    
                    if found_any_cache:
                        break  # Found individual Greeks
                
                if found_any_cache or greeks:
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
    
    def process_alerts_data(self):
        """Convert alerts_data list to DataFrame with proper formatting for display"""
        if not self.alerts_data:
            return pd.DataFrame()
        
        df_alerts = pd.DataFrame(self.alerts_data)
        
        # Ensure pattern keys exist for normalized filtering
        if 'pattern_key' in df_alerts.columns:
            df_alerts['pattern_key'] = df_alerts['pattern_key'].apply(normalize_pattern_name)
        elif 'pattern' in df_alerts.columns:
            df_alerts['pattern_key'] = df_alerts['pattern'].apply(normalize_pattern_name)
        else:
            df_alerts['pattern_key'] = 'unknown'

        # CRITICAL FIX: Replace NaN with None for Dash DataTable display
        indicator_cols = ['rsi', 'macd', 'ema_20', 'ema_50', 'atr', 'vwap', 'volume_ratio']
        greek_cols = ['delta', 'gamma', 'theta', 'vega', 'rho']
        all_numeric_cols = indicator_cols + greek_cols
        
        for col in all_numeric_cols:
            if col in df_alerts.columns:
                # Replace NaN with None so Dash DataTable can display empty cells properly
                df_alerts[col] = df_alerts[col].where(pd.notna(df_alerts[col]), None)
        
        return df_alerts
    
    def start_alerts_stream_reader(self):
        """Start background thread to read new alerts from alerts:stream using XREADGROUP"""
        if self.stream_reading_running:
            print("⚠️ Alerts stream reader already running")
            return
        
        # Ensure consumer group exists
        try:
            # ✅ CONSISTENCY: Use RedisManager82 for stream operations
            stream_client = RedisManager82.get_client(
                process_name="dashboard",
                db=1,
                max_connections=None,
                decode_responses=False  # Binary mode for streams
            )
            create_consumer_group_if_needed('alerts:stream', self.alerts_stream_consumer_group)
            print(f"✅ Consumer group '{self.alerts_stream_consumer_group}' ready for alerts:stream")
        except Exception as e:
            print(f"⚠️ Failed to create consumer group: {e}")
            return
        
        self.stream_reading_running = True
        self.stream_reading_thread = threading.Thread(
            target=self._read_alerts_stream_loop,
            daemon=True,
            name="AlertsStreamReader"
        )
        self.stream_reading_thread.start()
        print(f"🚀 Started real-time alerts stream reader (consumer: {self.alerts_stream_consumer_name})")
    
    def _read_alerts_stream_loop(self):
        """Background loop to read new alerts from alerts:stream using RobustStreamConsumer (modern implementation)"""
        # ✅ FIXED: Use RobustStreamConsumer instead of manual XREADGROUP
        from intraday_scanner.data_pipeline import RobustStreamConsumer
        
        print(f"📡 Started reading alerts:stream (group: {self.alerts_stream_consumer_group}, consumer: {self.alerts_stream_consumer_name})")
        
        # Create RobustStreamConsumer instance
        consumer = RobustStreamConsumer(
            stream_key='alerts:stream',
            group_name=self.alerts_stream_consumer_group,
            consumer_name=self.alerts_stream_consumer_name,
            db=1  # DB 1 for realtime streams
        )
        
        # Define callback for processing messages
        def process_alert_message(message_data):
            """Callback to process alert messages from stream"""
            try:
                # message_data is dict: {b'data': binary_json, ...}
                # Extract binary data
                data_field = message_data.get(b'data') or message_data.get('data')
                if not data_field:
                    return True  # ACK even if no data
                
                # Parse JSON
                if isinstance(data_field, bytes):
                    try:
                        import orjson
                        alert_data = orjson.loads(data_field)
                    except:
                        alert_data = json.loads(data_field.decode('utf-8'))
                else:
                    alert_data = json.loads(data_field) if isinstance(data_field, str) else data_field
                
                # Filter: Only process alerts from today
                today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                today_start_ms = int(today_start.timestamp() * 1000)
                alert_timestamp_ms = alert_data.get('timestamp') or alert_data.get('timestamp_ms')
                
                if alert_timestamp_ms:
                    try:
                        alert_ts = int(alert_timestamp_ms)
                        if alert_ts < today_start_ms:
                            return True  # Old alert - ACK but skip
                    except (ValueError, TypeError):
                        pass
                
                # Process alert
                processed_alert = self._process_single_alert_data(alert_data)
                
                if processed_alert:
                    # Thread-safe append to alerts_data
                    with self.alerts_data_lock:
                        # Check for duplicates
                        existing = False
                        for existing_alert in self.alerts_data:
                            if (existing_alert.get('symbol') == processed_alert.get('symbol') and
                                existing_alert.get('pattern') == processed_alert.get('pattern') and
                                existing_alert.get('timestamp') == processed_alert.get('timestamp')):
                                existing = True
                                break
                        
                        if not existing:
                            self.alerts_data.append(processed_alert)
                            # Keep only last 1000 alerts in memory
                            if len(self.alerts_data) > 1000:
                                self.alerts_data = self.alerts_data[-1000:]
                
                return True  # ACK successful processing
                
            except Exception as e:
                print(f"⚠️ Error processing alert message: {e}")
                return True  # ACK even on error to prevent infinite reprocessing
        
        # Start processing messages using RobustStreamConsumer
        # This handles pending messages, new messages, reconnection, and ACK automatically
        # Note: process_messages() blocks, but this is fine since _read_alerts_stream_loop runs in a thread
        try:
            consumer.process_messages(process_alert_message)
        except Exception as e:
            print(f"❌ RobustStreamConsumer error: {e}")
        finally:
            print(f"🛑 Dashboard stream reader stopped")
    
    def start_validation_stream_reader(self):
        """Start background thread to read forward validation results from alerts:validation:results stream"""
        if self.validation_stream_reading_running:
            print("⚠️ Validation stream reader already running")
            return
        
        # Ensure consumer group exists
        try:
            # ✅ CONSISTENCY: Use RedisManager82 for stream operations
            stream_client = RedisManager82.get_client(
                process_name="dashboard",
                db=0,
                max_connections=None,
                decode_responses=False  # Binary mode for streams
            )
            create_consumer_group_if_needed('alerts:validation:results', self.validation_stream_consumer_group)
            print(f"✅ Consumer group '{self.validation_stream_consumer_group}' ready for alerts:validation:results")
        except Exception as e:
            print(f"⚠️ Failed to create validation consumer group: {e}")
            return
        
        self.validation_stream_reading_running = True
        self.validation_stream_reading_thread = threading.Thread(
            target=self._read_validation_stream_loop,
            daemon=True,
            name="ValidationStreamReader"
        )
        self.validation_stream_reading_thread.start()
        print(f"🚀 Started real-time validation stream reader (consumer: {self.validation_stream_consumer_name})")
    
    def _read_validation_stream_loop(self):
        """Background loop to read forward validation results from alerts:validation:results stream using RobustStreamConsumer"""
        # ✅ STANDARDIZED: Use RobustStreamConsumer instead of manual XREADGROUP
        from intraday_scanner.data_pipeline import RobustStreamConsumer
        
        print(f"📡 Started reading alerts:validation:results (group: {self.validation_stream_consumer_group}, consumer: {self.validation_stream_consumer_name})")
        
        # Create RobustStreamConsumer instance
        consumer = RobustStreamConsumer(
            stream_key='alerts:validation:results',
            group_name=self.validation_stream_consumer_group,
            consumer_name=self.validation_stream_consumer_name,
            db=0  # DB 0 for validation results
        )
        
        # Define callback for processing validation messages
        def process_validation_message(message_data):
            """Callback to process validation messages from stream"""
            try:
                # message_data is dict with validation fields
                # _process_validation_message expects message_id and fields, returns ack_ids and count
                # But RobustStreamConsumer handles ACK, so we just process
                dummy_message_id = "0-0"
                ack_ids = []
                new_validations_added = 0
                
                ack_ids, new_validations_added = self._process_validation_message(
                    dummy_message_id, message_data, ack_ids, new_validations_added
                )
                
                if new_validations_added > 0:
                    with self.new_validations_count_lock:
                        self.new_validations_count += new_validations_added
                
                return True  # ACK successful processing
                
            except Exception as e:
                print(f"⚠️ Error processing validation message: {e}")
                return True  # ACK even on error to prevent infinite reprocessing
        
        # Start processing messages using RobustStreamConsumer
        try:
            consumer.process_messages(process_validation_message)
        except Exception as e:
            print(f"❌ RobustStreamConsumer error: {e}")
        finally:
            print("🛑 Validation stream reader stopped")
    
    def _process_validation_message(self, message_id, fields, ack_ids, new_validations_added):
        """Process a single validation result message and update validation_results
        
        ✅ CRITICAL: Normalize stream format (flat) to match validation_results:recent format (nested)
        Stream format: status, price_movement_pct, rolling_windows (JSON string)
        Expected format: validation_result.is_valid, validation_result.confidence_score, forward_validation
        """
        try:
            # Extract data from stream message
            validation_data = {}
            
            # Handle both binary and string field keys
            for key, value in fields.items():
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = str(key)
                
                if isinstance(value, bytes):
                    try:
                        # Try to decode as JSON first (for rolling_windows field)
                        value = json.loads(value.decode('utf-8'))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # If not JSON, decode as string
                        value = value.decode('utf-8')
                
                validation_data[key_str] = value
            
            # Extract alert_id
            alert_id = validation_data.get('alert_id') or validation_data.get(b'alert_id')
            if isinstance(alert_id, bytes):
                alert_id = alert_id.decode('utf-8')
            
            # ✅ CRITICAL: Normalize stream format to match validation_results:recent format
            # Stream has: status, price_movement_pct, rolling_windows (JSON string)
            # Expected format: validation_result.is_valid, validation_result.confidence_score
            
            # Extract status and convert to is_valid boolean
            status = validation_data.get('status', 'INCONCLUSIVE')
            if isinstance(status, bytes):
                status = status.decode('utf-8')
            
            is_valid = (status == 'SUCCESS')
            
            # Calculate confidence_score from status and price movement
            price_movement = float(validation_data.get('price_movement_pct', 0.0) or 0.0)
            confidence_score = 0.0
            if is_valid and price_movement > 0:
                # Positive movement in correct direction = higher confidence
                confidence_score = min(1.0, abs(price_movement) / 2.0)  # Scale movement to 0-1
            
            # Parse rolling_windows if available (JSON string)
            rolling_windows_str = validation_data.get('rolling_windows', '{}')
            if isinstance(rolling_windows_str, bytes):
                rolling_windows_str = rolling_windows_str.decode('utf-8')
            rolling_windows = {}
            try:
                if isinstance(rolling_windows_str, str):
                    rolling_windows = json.loads(rolling_windows_str)
                elif isinstance(rolling_windows_str, dict):
                    rolling_windows = rolling_windows_str
            except (json.JSONDecodeError, TypeError):
                rolling_windows = {}
            
            # Extract forward_validation summary from rolling_windows if available
            forward_validation = None
            if rolling_windows:
                forward_validation = {
                    'summary': {
                        'success_count': rolling_windows.get('success_count', 0),
                        'failure_count': rolling_windows.get('failure_count', 0),
                        'inconclusive_count': rolling_windows.get('inconclusive_count', 0),
                        'total_windows': rolling_windows.get('total_windows', 0),
                        'success_ratio': rolling_windows.get('success_ratio', 0.0),
                        'max_directional_move_pct': rolling_windows.get('max_directional_move_pct', 0.0),
                        'max_abs_move_pct': rolling_windows.get('max_abs_move_pct', 0.0),
                        'threshold_pct': rolling_windows.get('threshold_pct', 0.1)
                    },
                    'windows': {}  # Individual window data not in rolling_windows summary
                }
            
            # Build normalized structure matching validation_results:recent format
            normalized_validation = {
                'alert_id': alert_id,
                'symbol': validation_data.get('symbol', alert_id.split('_')[0] if alert_id else 'UNKNOWN'),
                'alert_type': validation_data.get('pattern', 'unknown'),
                'validation_result': {
                    'is_valid': is_valid,
                    'confidence_score': confidence_score,
                    'forward_validation': forward_validation
                },
                'timestamp': datetime.now().isoformat(),
                'processed_at': validation_data.get('validation_timestamp_ms', int(time.time() * 1000))
            }
            
            # Update validation_results (thread-safe)
            with self.validation_results_lock:
                found = False
                for i, existing in enumerate(self.validation_results):
                    if isinstance(existing, dict) and existing.get('alert_id') == alert_id:
                        # Update existing validation result
                        self.validation_results[i] = normalized_validation
                        found = True
                        break
                
                if not found:
                    # Add new validation result
                    self.validation_results.append(normalized_validation)
                    new_validations_added += 1
                
                # Keep only last 200 validation results
                if len(self.validation_results) > 200:
                    self.validation_results = self.validation_results[-200:]
            
            ack_ids.append(message_id)
            if alert_id:
                print(f"✅ Processed validation result: {alert_id} (status: {status}, is_valid: {is_valid}, confidence: {confidence_score:.2f})")
            
        except Exception as e:
            print(f"⚠️ Error processing validation result from stream: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            # Still ACK to avoid reprocessing
            ack_ids.append(message_id)
        
        return ack_ids, new_validations_added
    
    def _process_stream_message(self, message_id, fields, ack_ids, new_alerts_added):
        """Process a single stream message and return updated ack_ids and new_alerts_added count"""
        try:
            # Extract binary data from stream message
            data_field = fields.get(b'data') or fields.get('data')
            if not data_field:
                ack_ids.append(message_id)
                return ack_ids, new_alerts_added
            
            # Parse JSON (handle both bytes and str)
            if isinstance(data_field, bytes):
                try:
                    import orjson
                    alert_data = orjson.loads(data_field)
                except:
                    alert_data = json.loads(data_field.decode('utf-8'))
            else:
                alert_data = json.loads(data_field) if isinstance(data_field, str) else data_field
            
            # Filter: Only process alerts from today (ignore old alerts that might be in stream)
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ms = int(today_start.timestamp() * 1000)
            alert_timestamp_ms = alert_data.get('timestamp') or alert_data.get('timestamp_ms')
            
            if alert_timestamp_ms:
                try:
                    alert_ts = int(alert_timestamp_ms)
                    if alert_ts < today_start_ms:
                        # Old alert - skip but ACK to avoid reprocessing
                        ack_ids.append(message_id)
                        return ack_ids, new_alerts_added
                except (ValueError, TypeError):
                    pass  # If timestamp parsing fails, continue processing (might be today's)
            
            # Process alert using same logic as load_data()
            processed_alert = self._process_single_alert_data(alert_data, stream_msg_id=str(message_id))
            
            if processed_alert:
                # Thread-safe append to alerts_data
                with self.alerts_data_lock:
                    # Check for duplicates by symbol+pattern+timestamp
                    existing = False
                    for existing_alert in self.alerts_data:
                        if (existing_alert.get('symbol') == processed_alert.get('symbol') and
                            existing_alert.get('pattern') == processed_alert.get('pattern') and
                            existing_alert.get('timestamp') == processed_alert.get('timestamp')):
                            existing = True
                            break
                    
                    if not existing:
                        self.alerts_data.append(processed_alert)
                        new_alerts_added += 1
                        
                        # Increment counter for UI refresh trigger
                        with self.new_alerts_count_lock:
                            self.new_alerts_count += 1
                
                ack_ids.append(message_id)
                print(f"✅ Processed new alert: {processed_alert.get('symbol')} - {processed_alert.get('pattern')}")
            
        except Exception as e:
            print(f"⚠️ Error processing alert from stream: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            # Still ACK to avoid reprocessing
            ack_ids.append(message_id)
        
        return ack_ids, new_alerts_added
    
    def _match_validation_to_alert(self, symbol: str, alert_time, pattern: str = None) -> Dict:
        """
        Match validation results to an alert by symbol and timestamp.
        
        Args:
            symbol: Alert symbol
            alert_time: Alert timestamp (datetime)
            pattern: Optional pattern name for better matching
            
        Returns:
            Dict with validation fields: is_valid, validation_score, reasons
        """
        if not self.validation_results:
            return {'is_valid': None, 'validation_score': None, 'reasons': None}
        
        # Try to match by symbol and approximate timestamp (within 5 minutes)
        if alert_time:
            alert_timestamp_ms = int(alert_time.timestamp() * 1000)
            time_window_ms = 5 * 60 * 1000  # 5 minutes
            
            for validation in self.validation_results:
                val_symbol = validation.get('symbol', '')
                val_pattern_raw = validation.get('pattern', validation.get('alert_type', ''))
                val_pattern = normalize_pattern_name(val_pattern_raw)
                
                # Match by symbol (normalized)
                symbol_match = (
                    symbol == val_symbol or
                    symbol.replace('NFO:', '').replace('NSE:', '') == val_symbol.replace('NFO:', '').replace('NSE:', '')
                )
                
                # Match by pattern if provided
                pattern_match = True
                if pattern and val_pattern:
                    pattern_match = normalize_pattern_name(pattern) == val_pattern
                
                if symbol_match and pattern_match:
                    # Check timestamp proximity
                    val_timestamp = validation.get('timestamp_ms') or validation.get('processed_at') or validation.get('validation_timestamp_ms')
                    if val_timestamp:
                        if isinstance(val_timestamp, str):
                            try:
                                val_timestamp = int(float(val_timestamp))
                            except:
                                continue
                        if abs(val_timestamp - alert_timestamp_ms) <= time_window_ms:
                            # Match found!
                            is_valid_val = validation.get('status', '').upper() == 'SUCCESS'
                            validation_score_val = validation.get('confidence_score', validation.get('validation_score'))
                            reasons_val = validation.get('reasons', validation.get('details', ''))
                            
                            # Format reasons as string if it's a list
                            if isinstance(reasons_val, list):
                                reasons_val = ', '.join(str(r) for r in reasons_val)
                            elif reasons_val is None:
                                reasons_val = ''
                            
                            return {
                                'is_valid': 'True' if is_valid_val else ('False' if validation.get('status', '').upper() == 'FAILURE' else ''),
                                'validation_score': validation_score_val,
                                'reasons': str(reasons_val) if reasons_val else ''
                            }
        
        return {'is_valid': None, 'validation_score': None, 'reasons': None}
    
    def _process_single_alert_data(self, data, stream_msg_id=None):
        """
        Process a single alert data dict into alert_info format.
        Reuses logic from load_data() but simplified for single alert processing.
        """
        try:
            # Add stream message ID for timestamp extraction if needed
            if stream_msg_id and isinstance(data, dict):
                data['_stream_msg_id'] = stream_msg_id
            
            # Parse timestamp (same priority logic as load_data)
            alert_time = None
            timestamp_str = ''
            
            # Simplified timestamp parsing (same as load_data priority 1-6)
            alert_time_val = data.get('alert_time')
            if alert_time_val:
                if isinstance(alert_time_val, str):
                    try:
                        if 'Z' in alert_time_val:
                            alert_time = datetime.fromisoformat(alert_time_val.replace('Z', '+00:00'))
                        elif 'T' in alert_time_val:
                            alert_time = datetime.fromisoformat(alert_time_val.split('.')[0])
                        else:
                            alert_time = datetime.fromisoformat(alert_time_val)
                        timestamp_str = alert_time.isoformat()
                    except:
                        pass
                elif isinstance(alert_time_val, (int, float)):
                    alert_time = datetime.fromtimestamp(float(alert_time_val) / 1000.0 if alert_time_val > 1e10 else alert_time_val)
                    timestamp_str = alert_time.isoformat()
            
            if not alert_time:
                timestamp_val = data.get('timestamp')
                if timestamp_val is not None and isinstance(timestamp_val, (int, float)):
                    alert_time = datetime.fromtimestamp(float(timestamp_val) / 1000.0 if timestamp_val > 1e10 else timestamp_val)
                    timestamp_str = alert_time.isoformat()
                elif timestamp_val and isinstance(timestamp_val, str):
                    try:
                        if 'Z' in timestamp_val:
                            alert_time = datetime.fromisoformat(timestamp_val.replace('Z', '+00:00'))
                        elif 'T' in timestamp_val:
                            alert_time = datetime.fromisoformat(timestamp_val.split('.')[0])
                        timestamp_str = alert_time.isoformat() if alert_time else ''
                    except:
                        pass
            
            if not alert_time:
                # Fallback to current time
                alert_time = datetime.now()
                timestamp_str = alert_time.isoformat()
            
            # Extract symbol
            symbol = data.get('symbol', 'UNKNOWN')
            if isinstance(symbol, bytes):
                symbol = symbol.decode('utf-8')
            symbol = str(symbol)
            
            # Resolve token if needed
            if symbol.isdigit() or str(symbol).startswith('UNKNOWN_'):
                try:
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    mapper = InstrumentMapper()
                    token = data.get('instrument_token') or data.get('token') or symbol
                    resolved = mapper.token_to_symbol(int(token))
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        symbol = resolved
                except:
                    pass
            
            # Normalize to base symbol
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            
            # Check if instrument is in intraday crawler (reuse existing logic)
            if not self.is_intraday_instrument(base_symbol):
                return None  # Skip this alert
            
            symbol_full = symbol
            display_symbol = base_symbol
            symbol = symbol_full

            instrument_name = self.get_instrument_name(display_symbol)
            expiry_info = self.get_expiry_date(symbol_full)
            is_option = self.is_option_instrument(symbol_full)
            is_futures = self.is_futures_instrument(symbol_full)
            
            # Extract indicators and Greeks
            indicators = {}
            greeks = {}
            
            redis_indicators = self.load_indicators_for_symbol(symbol_full)
            if not redis_indicators:
                variants = [symbol_full, display_symbol, display_symbol.upper(), display_symbol.lower(), display_symbol.replace(' ', '')]
                for variant in [v for v in variants if v]:
                    if not redis_indicators:
                        candidate = self.load_indicators_for_symbol(variant)
                        if candidate:
                            redis_indicators = candidate
                            logger.debug(f"✅ Found indicators using symbol variant: {variant} (original: {symbol_full})")
                            break
            
            if redis_indicators:
                indicators = redis_indicators
                logger.debug(f"✅ Loaded {len(indicators)} indicators from Redis for {symbol_full}: {list(indicators.keys())[:5]}")
                print(f"✅ Loaded {len(indicators)} indicators from Redis for {symbol_full}: {list(indicators.keys())[:5]}")
            else:
                # FALLBACK: Extract from alert data (usually empty, but try anyway)
                if data.get('indicators'):
                    indicators_raw = data.get('indicators', {})
                    if isinstance(indicators_raw, str):
                        try:
                            indicators = json.loads(indicators_raw)
                        except:
                            pass
                    elif isinstance(indicators_raw, dict):
                        indicators.update(indicators_raw)
                
                if not indicators:
                    logger.debug(f"⚠️ No indicators found for {symbol} in Redis or alert data")
                    print(f"⚠️ No indicators found for {symbol} - checked Redis and alert data")
            
            # PRIORITY 1: Load Greeks from Redis (ALWAYS - alert payloads don't have Greeks)
            if is_option:
                # Try Redis first (same as load_data() logic)
                greeks = self._fetch_greeks_from_redis(symbol_full)
                
                # FALLBACK: Extract from alert data if Redis didn't have them
                if not greeks and data.get('greeks'):
                    greeks_raw = data.get('greeks', {})
                    if isinstance(greeks_raw, dict):
                        greeks.update(greeks_raw)
            
            # Extract action and trading params
            action = data.get('action', data.get('signal', 'MONITOR'))
            if action is None or action == '':
                action = 'MONITOR'
            
            entry_price = float(data.get('entry_price', data.get('last_price', 0.0)))
            stop_loss = None
            if 'stop_loss' in data and data['stop_loss'] is not None:
                try:
                    stop_loss = float(data['stop_loss']) if float(data['stop_loss']) > 0 else None
                except:
                    pass
            
            # CRITICAL: Extract target from multiple sources (risk_metrics nested dict)
            target = None
            # First check direct fields
            for field in ['target', 'target_price', 'take_profit', 'profit_target']:
                if field in data and data[field] is not None:
                    try:
                        target = float(data[field]) if float(data[field]) > 0 else None
                        if target:
                            break
                    except:
                        continue
            
            # If not found, check risk_metrics nested dict
            if target is None and 'risk_metrics' in data:
                risk_metrics = data['risk_metrics']
                # Handle string JSON
                if isinstance(risk_metrics, str):
                    try:
                        risk_metrics = json.loads(risk_metrics)
                    except:
                        risk_metrics = {}
                # Extract from nested dict
                if isinstance(risk_metrics, dict):
                    target = risk_metrics.get('target_price') or risk_metrics.get('target')
                    if target:
                        try:
                            target = float(target) if float(target) > 0 else None
                        except:
                            target = None
            
            # CRITICAL: Extract quantity from multiple sources (risk_metrics nested dict)
            quantity = None
            # First check direct fields
            if 'quantity' in data and data['quantity'] is not None:
                try:
                    quantity = float(data['quantity']) if float(data['quantity']) > 0 else None
                except:
                    pass
            
            # If not found, calculate from risk_metrics.position_size
            if quantity is None and 'risk_metrics' in data and entry_price > 0:
                risk_metrics = data['risk_metrics']
                # Handle string JSON
                if isinstance(risk_metrics, str):
                    try:
                        risk_metrics = json.loads(risk_metrics)
                    except:
                        risk_metrics = {}
                # Extract position_size and calculate quantity
                if isinstance(risk_metrics, dict):
                    position_size = risk_metrics.get('position_size')
                    if position_size:
                        try:
                            quantity = float(position_size) / entry_price if entry_price > 0 else None
                            quantity = quantity if quantity and quantity > 0 else None
                        except:
                            quantity = None
            
            pattern_raw = data.get('pattern', data.get('pattern_type', 'unknown'))
            pattern_key = normalize_pattern_name(pattern_raw)
            if pattern_key not in self.pattern_labels:
                self.pattern_labels[pattern_key] = pattern_display_label(pattern_key, pattern_raw)
            pattern_label = self.pattern_labels.get(pattern_key, pattern_raw or pattern_key)

            # Build alert_info (same structure as load_data)
            alert_info = {
                'symbol': display_symbol,
                'symbol_full': symbol_full,
                'instrument_name': instrument_name,
                'expiry': expiry_info,
                'pattern': pattern_label,
                'pattern_key': pattern_key,
                'action': str(action).upper(),
                'signal': data.get('signal', data.get('action', 'UNKNOWN')),
                'confidence': float(data.get('confidence', 0.0)),
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'target': target,
                'quantity': quantity,
                'expected_move': float(data.get('expected_move', 0.0)) / 100.0 if data.get('expected_move', 0.0) >= 1.0 else float(data.get('expected_move', 0.0)),
                'timestamp': timestamp_str,
                'alert_time': alert_time,
                'time_of_day': alert_time.strftime('%H:%M:%S'),
                # ✅ FIX: Match validation results to alert (by symbol + timestamp)
                **self._match_validation_to_alert(display_symbol, alert_time, pattern_key),
                'instrument_type': 'Options' if is_option else ('Futures' if is_futures else 'Equity'),
                'has_news': len(data.get('news', [])) > 0 if data.get('news') else False,
                'news_count': len(data.get('news', [])) if isinstance(data.get('news', []), list) else (1 if data.get('news') else 0),
                # CRITICAL: Indicators are loaded from Redis in load_indicators_for_symbol() above
                # Use the loaded indicators dict directly (already tried Redis with all symbol variants)
                'rsi': self._safe_float(indicators.get('rsi') or data.get('rsi')),
                'macd': self._safe_float(
                    self._extract_macd_value(indicators) or 
                    self._extract_macd_value(data) or
                    (indicators.get('macd') if isinstance(indicators.get('macd'), (int, float)) else None)
                ),
                'ema_20': self._safe_float(indicators.get('ema_20') or data.get('ema_20')),
                'ema_50': self._safe_float(indicators.get('ema_50') or data.get('ema_50')),
                'atr': self._safe_float(indicators.get('atr') or data.get('atr')),
                'vwap': self._safe_float(indicators.get('vwap') or data.get('vwap')),
                'volume_ratio': self._safe_float(indicators.get('volume_ratio') or data.get('volume_ratio')),
                # CRITICAL: Greeks are loaded from Redis in load_greeks_for_symbol() and _fetch_greeks_from_redis() above
                # Use the loaded greeks dict directly (already tried Redis with all symbol variants)
                'delta': self._safe_float(greeks.get('delta') if greeks else (data.get('delta') if data else None)),
                'gamma': self._safe_float(greeks.get('gamma') if greeks else (data.get('gamma') if data else None)),
                'theta': self._safe_float(greeks.get('theta') if greeks else (data.get('theta') if data else None)),
                'vega': self._safe_float(greeks.get('vega') if greeks else (data.get('vega') if data else None)),
                'rho': self._safe_float(greeks.get('rho') if greeks else (data.get('rho') if data else None)),
                'news_data': json.dumps(data.get('news', [])) if data.get('news') else None,
            }
            
            return alert_info
            
        except Exception as e:
            print(f"⚠️ Error processing single alert: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def load_validation_results(self):
        """Load validation results from Redis (from alert_validator)"""
        try:
            # Load from validation_results:recent list (DB 1)
            recent_results = self.redis_db1.lrange('validation_results:recent', 0, 99)
            validation_data = []
            
            for result_str in recent_results:
                try:
                    result = json.loads(result_str) if isinstance(result_str, str) else result_str
                    validation_data.append(result)
                except (json.JSONDecodeError, TypeError):
                    continue
            
            self.validation_results = validation_data
            print(f"✅ Loaded {len(validation_data)} validation results from Redis")
            
            # Also try to get forward validation results
            try:
                forward_results_key = 'forward_validation:results'
                forward_results = self.redis_client.hgetall(forward_results_key)
                if forward_results:
                    print(f"✅ Found {len(forward_results)} forward validation results")
                    for alert_id, result_str in forward_results.items():
                        try:
                            # Handle bytes from Redis
                            if isinstance(alert_id, bytes):
                                alert_id = alert_id.decode('utf-8')
                            if isinstance(result_str, bytes):
                                result_str = result_str.decode('utf-8')
                            
                            # Parse JSON string
                            if isinstance(result_str, str):
                                result = json.loads(result_str)
                            else:
                                result = result_str
                            
                            # Add alert_id to result for tracking
                            if isinstance(result, dict):
                                result['alert_id'] = alert_id
                                validation_data.append(result)
                        except (json.JSONDecodeError, TypeError, AttributeError) as e:
                            print(f"⚠️ Error parsing forward validation result {alert_id}: {e}")
                            continue
            except Exception as e:
                print(f"⚠️ Could not load forward validation results: {e}")
            
            return validation_data
        except Exception as e:
            print(f"⚠️ Error loading validation results: {e}")
            return []
    
    def get_statistical_model_data(self):
        """Get statistical model data from validation results"""
        try:
            if not self.validation_results:
                self.load_validation_results()
            
            if not self.validation_results:
                return {
                    'total_validations': 0,
                    'valid_rate': 0.0,
                    'avg_confidence': 0.0,
                    'pattern_performance': {},
                    'forward_validation_summary': {}
                }
            
            # Normalize validation results - ensure all are dicts (not bytes)
            normalized_results = []
            for r in self.validation_results:
                try:
                    # Handle bytes objects
                    if isinstance(r, bytes):
                        r = json.loads(r.decode('utf-8'))
                    elif isinstance(r, str):
                        r = json.loads(r)
                    
                    # Ensure it's a dict
                    if isinstance(r, dict):
                        normalized_results.append(r)
                except (json.JSONDecodeError, TypeError, AttributeError):
                    # Skip invalid entries
                    continue
            
            if not normalized_results:
                return {
                    'total_validations': 0,
                    'valid_rate': 0.0,
                    'avg_confidence': 0.0,
                    'pattern_performance': {},
                    'forward_validation_summary': {}
                }
            
            # Calculate statistics
            total = len(normalized_results)
            valid_count = sum(1 for r in normalized_results 
                            if isinstance(r, dict) and r.get('validation_result', {}).get('is_valid', False))
            valid_rate = valid_count / total if total > 0 else 0.0
            
            # Calculate average confidence
            confidences = []
            for r in normalized_results:
                if isinstance(r, dict):
                    conf = r.get('validation_result', {}).get('confidence_score')
                    if conf is not None:
                        try:
                            confidences.append(float(conf))
                        except (ValueError, TypeError):
                            continue
            avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
            
            # Pattern performance analysis
            pattern_performance = {}
            for result in normalized_results:
                if not isinstance(result, dict):
                    continue
                    
                pattern = result.get('alert_type') or result.get('pattern', 'unknown')
                validation = result.get('validation_result', {})
                if not isinstance(validation, dict):
                    validation = {}
                    
                is_valid = validation.get('is_valid', False)
                confidence = validation.get('confidence_score', 0.0)
                try:
                    confidence = float(confidence) if confidence is not None else 0.0
                except (ValueError, TypeError):
                    confidence = 0.0
                
                if pattern not in pattern_performance:
                    pattern_performance[pattern] = {
                        'total': 0,
                        'valid': 0,
                        'invalid': 0,
                        'total_confidence': 0.0,
                        'avg_confidence': 0.0
                    }
                
                pattern_performance[pattern]['total'] += 1
                if is_valid:
                    pattern_performance[pattern]['valid'] += 1
                else:
                    pattern_performance[pattern]['invalid'] += 1
                pattern_performance[pattern]['total_confidence'] += confidence
            
            # Calculate average confidence per pattern
            for pattern, stats in pattern_performance.items():
                if stats['total'] > 0:
                    stats['avg_confidence'] = stats['total_confidence'] / stats['total']
                    stats['success_rate'] = stats['valid'] / stats['total']
                    del stats['total_confidence']  # Clean up intermediate value
            
            # Forward validation summary
            forward_validations = [
                r for r in normalized_results
                if isinstance(r, dict) and r.get('validation_result', {}).get('forward_validation')
            ]
            forward_summary = {
                'total_forward_validations': len(forward_validations),
                'avg_success_ratio': 0.0,
                'window_performance': {}
            }
            
            if forward_validations:
                success_ratios = []
                for fv in forward_validations:
                    fv_data = fv.get('validation_result', {}).get('forward_validation', {})
                    summary = fv_data.get('summary', {})
                    success_ratio = summary.get('success_ratio', 0.0)
                    if success_ratio is not None:
                        success_ratios.append(success_ratio)
                    
                    # Window-level performance
                    windows = fv_data.get('windows', {})
                    for window_key, window_data in windows.items():
                        if window_key not in forward_summary['window_performance']:
                            forward_summary['window_performance'][window_key] = {
                                'total': 0,
                                'success': 0,
                                'failure': 0,
                                'inconclusive': 0
                            }
                        
                        status = window_data.get('status', 'UNKNOWN')
                        forward_summary['window_performance'][window_key]['total'] += 1
                        if status == 'SUCCESS':
                            forward_summary['window_performance'][window_key]['success'] += 1
                        elif status == 'FAILURE':
                            forward_summary['window_performance'][window_key]['failure'] += 1
                        else:
                            forward_summary['window_performance'][window_key]['inconclusive'] += 1
                
                if success_ratios:
                    forward_summary['avg_success_ratio'] = sum(success_ratios) / len(success_ratios)
            
            self.statistical_model_data = {
                'total_validations': total,
                'valid_rate': valid_rate,
                'avg_confidence': avg_confidence,
                'pattern_performance': pattern_performance,
                'forward_validation_summary': forward_summary,
                'timestamp': datetime.now().isoformat()
            }
            
            return self.statistical_model_data
        except Exception as e:
            print(f"⚠️ Error calculating statistical model data: {e}")
            return {}
    
    def load_data(self):
        """Load data with better defaults for dashboard"""
        print("Loading data for enhanced dashboard...")
        
        # Load intraday crawler instruments first (must be before loading alerts)
        self.load_intraday_crawler_instruments()
        
        # Load all patterns/strategies
        self.load_all_patterns()
        
        # Load instrument cache for symbol resolution and expiry info
        self.load_instrument_cache()
        
        # Load validation results for statistical model
        self.load_validation_results()
        
        # Load SENT alerts (not validation alerts)
        # Check multiple possible alert storage locations
        alert_keys = []
        
        # Method 1: Check alerts:stream in DB 1 (primary source - where RedisNotifier publishes)
        try:
            # Load only today's alerts - filter by date to avoid showing old alerts
            stream_length = self.redis_db1.xlen("alerts:stream")
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ms = int(today_start.timestamp() * 1000)
            
            # Get more messages than needed to ensure we capture all of today's alerts
            max_messages = min(stream_length, 2000)  # Check up to 2000 messages
            stream_messages = self.redis_db1.xrevrange("alerts:stream", count=max_messages)
            
            # Filter to today's alerts with stream-id fallback (handles missing timestamps)
            today_alerts = []
            for msg_id, msg_data in stream_messages:
                try:
                    data_field = None
                    if 'data' in msg_data:
                        data_field = msg_data['data']
                    elif b'data' in msg_data:
                        data_field = msg_data[b'data']

                    parsed_payload = None
                    if data_field is not None:
                        if isinstance(data_field, bytes):
                            try:
                                import orjson
                                parsed_payload = orjson.loads(data_field)
                            except Exception:
                                parsed_payload = json.loads(data_field.decode('utf-8'))
                        elif isinstance(data_field, str):
                            parsed_payload = json.loads(data_field)
                        elif isinstance(data_field, dict):
                            parsed_payload = data_field

                    candidate_ts_ms = None
                    if parsed_payload:
                        raw_ts = parsed_payload.get('timestamp') or parsed_payload.get('timestamp_ms')
                        if raw_ts is not None:
                            try:
                                candidate_ts_ms = int(float(raw_ts))
                            except (ValueError, TypeError):
                                candidate_ts_ms = None

                    if candidate_ts_ms is None:
                        msg_id_str = msg_id.decode('utf-8') if isinstance(msg_id, bytes) else str(msg_id)
                        if msg_id_str and '-' in msg_id_str:
                            try:
                                candidate_ts_ms = int(msg_id_str.split('-')[0])
                            except ValueError:
                                candidate_ts_ms = None

                    if candidate_ts_ms is None or candidate_ts_ms >= today_start_ms:
                        today_alerts.append((msg_id, msg_data))
                except Exception:
                    # If parsing fails, include it (might still be a valid alert)
                    today_alerts.append((msg_id, msg_data))

            stream_messages = today_alerts
            print(f"📊 Loading {len(stream_messages)} alerts from today from alerts:stream (DB 1) out of {stream_length} total messages")
            print(f"   Filtered from {max_messages} recent messages (today starts at {today_start.strftime('%Y-%m-%d %H:%M:%S')})")
            print(f"   Note: New alerts will appear automatically via real-time stream reader")
                    # Process stream messages directly instead of storing as keys
            for msg_id, msg_data in stream_messages:
                try:
                    # Store message ID for timestamp fallback (Redis stream IDs contain timestamp)
                    stream_msg_id = msg_id
                    if isinstance(stream_msg_id, bytes):
                        stream_msg_id = stream_msg_id.decode('utf-8')
                    
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
                                # Store message ID in data for timestamp fallback
                                data['_stream_msg_id'] = stream_msg_id
                                alert_keys.append((('stream_direct', data), 1))
                                continue
                            except Exception:
                                # Try UTF-8 decode then json
                                try:
                                    data_field = data_field.decode('utf-8')
                                    data = json.loads(data_field)
                                    # Store message ID in data for timestamp fallback
                                    data['_stream_msg_id'] = stream_msg_id
                                    alert_keys.append((('stream_direct', data), 1))
                                    continue
                                except Exception:
                                    pass
                        
                        # Handle string data
                        if isinstance(data_field, str):
                            try:
                                data = json.loads(data_field)
                                # Store message ID in data for timestamp fallback
                                data['_stream_msg_id'] = stream_msg_id
                                alert_keys.append((('stream_direct', data), 1))
                            except (json.JSONDecodeError, TypeError):
                                # Try orjson
                                try:
                                    import orjson
                                    data = orjson.loads(data_field.encode('utf-8'))
                                    # Store message ID in data for timestamp fallback
                                    data['_stream_msg_id'] = stream_msg_id
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
        
        # Method 2: Use alerts:stream (already processed in Method 1)
        # ❌ REMOVED: Pattern matching .keys("alert:*") - FORBIDDEN per Redis key standards
        # ✅ Use stream iteration instead (already done in Method 1)
        # If we need specific alert IDs, use direct lookup: redis.get(f"alert:{alert_id}")
        pass
        
        # Method 3: Check alerts:stream in DB 0 (legacy fallback)
        try:
            stream_length_db0 = self.redis_client.xlen("alerts:stream")
            max_messages_db0 = min(stream_length_db0, 10000)
            stream_messages = self.redis_client.xrevrange("alerts:stream", count=max_messages_db0)
            alert_keys.extend([(f"stream:{msg_id}", 0) for msg_id, _ in stream_messages])
            print(f"📊 Found {len(stream_messages)} messages in alerts:stream (DB 0 - legacy) out of {stream_length_db0} total")
        except Exception as e:
            print(f"⚠️ Error checking alerts:stream in DB 0: {e}")
        
        # If no sent alerts found, fall back to validation alerts for testing
        # ❌ REMOVED: Pattern matching .keys("forward_validation:alert:*") - FORBIDDEN
        # ✅ Use validation stream instead (XREADGROUP or direct lookup if alert_id known)
        if not alert_keys:
            print("⚠️ No sent alerts found, falling back to validation stream")
            try:
                # Use stream instead of pattern matching
                validation_stream = "alerts:validation:results"
                if self.redis_client.exists(validation_stream):
                    stream_length = self.redis_client.xlen(validation_stream)
                    max_msgs = min(stream_length, 1000)
                    validation_messages = self.redis_client.xrevrange(validation_stream, count=max_msgs)
                    for msg_id, msg_data in validation_messages:
                        # Extract alert_id from message if available
                        alert_id = msg_data.get('alert_id') or msg_data.get(b'alert_id')
                        if alert_id:
                            if isinstance(alert_id, bytes):
                                alert_id = alert_id.decode('utf-8')
                            # Direct lookup using known alert_id
                            validation_key = f"forward_validation:alert:{alert_id}"
                            alert_keys.append((validation_key, 0))
            except Exception as e:
                print(f"⚠️ Error reading validation stream: {e}")
        
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
                elif isinstance(key, (str, bytes)) and (key.startswith("stream:") if isinstance(key, str) else key.startswith(b"stream:")):
                    # Extract message from stream - use xrevrange to get the actual message
                    # The msg_id from xrevrange is already the actual message ID, we need to fetch it
                    try:
                        # Handle bytes key
                        if isinstance(key, bytes):
                            msg_id_str = key.replace(b"stream:", b"").decode('utf-8')
                        else:
                            msg_id_str = key.replace("stream:", "")
                        # Parse msg_id - it's a string like "1761840899712-0"
                        # Use xrange to get the message, or better - use xrevrange and match
                        messages = None
                        if db_num == 1:
                            # Get all recent messages and find matching one (use same limit as main load)
                            stream_length_legacy = self.redis_db1.xlen("alerts:stream")
                            max_messages_legacy = min(stream_length_legacy, 10000)
                            all_msgs = self.redis_db1.xrevrange("alerts:stream", count=max_messages_legacy)
                            for mid, mdata in all_msgs:
                                if str(mid) == msg_id_str or mid == msg_id_str:
                                    messages = [(mid, mdata)]
                                    break
                        else:
                            stream_length_legacy = self.redis_client.xlen("alerts:stream")
                            max_messages_legacy = min(stream_length_legacy, 10000)
                            all_msgs = self.redis_client.xrevrange("alerts:stream", count=max_messages_legacy)
                            for mid, mdata in all_msgs:
                                if str(mid) == msg_id_str or mid == msg_id_str:
                                    messages = [(mid, mdata)]
                                    break
                        
                        if messages:
                            msg_id_legacy, msg_data = messages[0]
                            # Store message ID for timestamp extraction
                            stream_msg_id_legacy = msg_id_legacy
                            if isinstance(stream_msg_id_legacy, bytes):
                                stream_msg_id_legacy = stream_msg_id_legacy.decode('utf-8')
                            
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
                                        # Store message ID for timestamp fallback
                                        if isinstance(data, dict):
                                            data['_stream_msg_id'] = stream_msg_id_legacy
                                    except (json.JSONDecodeError, TypeError):
                                        # Try orjson if available
                                        try:
                                            import orjson
                                            data = orjson.loads(data_field)
                                            # Store message ID for timestamp fallback
                                            if isinstance(data, dict):
                                                data['_stream_msg_id'] = stream_msg_id_legacy
                                        except Exception:
                                            # Fallback: treat as dict if already parsed
                                            data = msg_data.get('data') or msg_data
                                            if isinstance(data, dict):
                                                data['_stream_msg_id'] = stream_msg_id_legacy
                                else:
                                    data = data_field if data_field else msg_data
                                    if isinstance(data, dict):
                                        data['_stream_msg_id'] = stream_msg_id_legacy
                            else:
                                # Use message data directly if no 'data' field
                                data = msg_data
                                if isinstance(data, dict):
                                    data['_stream_msg_id'] = stream_msg_id_legacy
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
                # Priority: 1) alert_time (if in data), 2) timestamp (numeric epoch), 3) timestamp_ms (numeric), 
                #           4) published_at (ISO string), 5) stream message ID timestamp, 6) other fields
                alert_time = None
                timestamp_str = ''
                
                try:
                    # PRIORITY 1: Check if alert_time field exists directly (most reliable)
                    alert_time_val = data.get('alert_time')
                    if alert_time_val:
                        if isinstance(alert_time_val, str):
                            try:
                                # Handle ISO format strings
                                if 'Z' in alert_time_val:
                                    alert_time = datetime.fromisoformat(alert_time_val.replace('Z', '+00:00'))
                                elif 'T' in alert_time_val:
                                    alert_time = datetime.fromisoformat(alert_time_val.split('.')[0])
                                else:
                                    alert_time = datetime.fromisoformat(alert_time_val)
                                timestamp_str = alert_time.isoformat()
                            except Exception as e:
                                logger.debug(f"Failed to parse alert_time string '{alert_time_val}': {e}")
                        elif isinstance(alert_time_val, (int, float)):
                            # Numeric timestamp
                            alert_time = datetime.fromtimestamp(float(alert_time_val) / 1000.0 if alert_time_val > 1e10 else alert_time_val)
                            timestamp_str = alert_time.isoformat()
                    
                    # PRIORITY 2: Check timestamp field (numeric epoch format)
                    if not alert_time:
                        timestamp_val = data.get('timestamp')
                        if timestamp_val is not None:
                            if isinstance(timestamp_val, (int, float)):
                                # Numeric timestamp - handle both seconds and milliseconds
                                alert_time = datetime.fromtimestamp(float(timestamp_val) / 1000.0 if timestamp_val > 1e10 else timestamp_val)
                                timestamp_str = alert_time.isoformat()
                            elif isinstance(timestamp_val, str) and timestamp_val:
                                # String timestamp - try ISO format
                                try:
                                    if 'Z' in timestamp_val:
                                        alert_time = datetime.fromisoformat(timestamp_val.replace('Z', '+00:00'))
                                    elif 'T' in timestamp_val:
                                        alert_time = datetime.fromisoformat(timestamp_val.split('.')[0])
                                    else:
                                        alert_time = datetime.fromisoformat(timestamp_val)
                                    timestamp_str = alert_time.isoformat()
                                except Exception as e:
                                    logger.debug(f"Failed to parse timestamp string: {e}")
                    
                    # PRIORITY 3: Check timestamp_ms (numeric)
                    if not alert_time:
                        timestamp_ms_val = data.get('timestamp_ms')
                        if timestamp_ms_val is not None and isinstance(timestamp_ms_val, (int, float)):
                            alert_time = datetime.fromtimestamp(float(timestamp_ms_val) / 1000.0)
                            timestamp_str = alert_time.isoformat()
                    
                    # PRIORITY 4: Check published_at (ISO string - when alert was published/generated)
                    if not alert_time:
                        published_at = data.get('published_at')
                        if published_at:
                            try:
                                if isinstance(published_at, str):
                                    # Handle various ISO formats
                                    pub_str = published_at.strip()
                                    if 'Z' in pub_str:
                                        alert_time = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
                                    elif 'T' in pub_str:
                                        # Try full precision first, then without microseconds
                                        try:
                                            alert_time = datetime.fromisoformat(pub_str)
                                        except:
                                            alert_time = datetime.fromisoformat(pub_str.split('.')[0])
                                    else:
                                        alert_time = datetime.fromisoformat(pub_str)
                                    timestamp_str = alert_time.isoformat()
                                    logger.debug(f"✅ Parsed published_at: {pub_str} -> {alert_time}")
                            except Exception as e:
                                logger.debug(f"Failed to parse published_at '{published_at}': {e}")
                    
                    # PRIORITY 5: Extract from stream message ID (Redis stream message IDs contain timestamp)
                    # Redis stream message IDs are in format: timestamp-sequence (e.g., "1761898689359-0")
                    if not alert_time:
                        try:
                            # Try to get message ID from stream_msg_id (if we're processing stream directly)
                            msg_id_for_timestamp = None
                            
                            # Check if message ID is stored in data (from stream processing)
                            if isinstance(data, dict) and '_stream_msg_id' in data:
                                msg_id_for_timestamp = data.get('_stream_msg_id')
                                # Remove it from data after extracting
                                data.pop('_stream_msg_id', None)
                            # Check if key is a message ID format
                            elif isinstance(key, (str, bytes)):
                                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                                if '-' in key_str and key_str.replace('-', '').replace('.', '').isdigit():
                                    msg_id_for_timestamp = key_str
                            # Check if message ID is embedded in data
                            elif isinstance(key, tuple) and len(key) >= 1:
                                key_str = str(key[0]) if key[0] else ''
                                if '-' in key_str and key_str.replace('-', '').replace('.', '').isdigit():
                                    msg_id_for_timestamp = key_str
                            
                            # Extract timestamp from message ID
                            if msg_id_for_timestamp and '-' in msg_id_for_timestamp:
                                parts = msg_id_for_timestamp.split('-')
                                if len(parts) >= 1:
                                    try:
                                        stream_timestamp_ms = int(parts[0])
                                        alert_time = datetime.fromtimestamp(stream_timestamp_ms / 1000.0)
                                        timestamp_str = alert_time.isoformat()
                                        logger.debug(f"✅ Extracted timestamp from stream message ID {msg_id_for_timestamp}: {stream_timestamp_ms} -> {alert_time}")
                                    except (ValueError, OSError) as e:
                                        logger.debug(f"Failed to parse timestamp from message ID {msg_id_for_timestamp}: {e}")
                        except Exception as e:
                            logger.debug(f"Failed to extract timestamp from message ID: {e}")
                    
                    # PRIORITY 6: Try other alternative timestamp fields
                    if not alert_time:
                        for ts_field in ['alert_timestamp', 'created_at', 'time', 'event_time', 'detected_at']:
                            if ts_field in data and data[ts_field]:
                                ts_val = data[ts_field]
                                try:
                                    if isinstance(ts_val, (int, float)):
                                        alert_time = datetime.fromtimestamp(float(ts_val) / 1000.0 if ts_val > 1e10 else ts_val)
                                        timestamp_str = alert_time.isoformat()
                                        break
                                    elif isinstance(ts_val, str) and ts_val.strip():
                                        # Try parsing ISO format
                                        try:
                                            ts_clean = ts_val.strip().split('.')[0].split('+')[0]
                                            alert_time = datetime.fromisoformat(ts_clean)
                                            timestamp_str = alert_time.isoformat()
                                            break
                                        except:
                                            continue
                                except Exception as e:
                                    logger.debug(f"Failed to parse {ts_field}: {e}")
                                    continue
                    
                    # LAST RESORT: Log warning but still try to use a reasonable default
                    # Only use current time if absolutely no timestamp can be found
                    if not alert_time:
                        logger.warning(f"⚠️ No valid timestamp found for alert {data.get('symbol', 'UNKNOWN')}, using current time as last resort")
                        alert_time = datetime.now()
                        timestamp_str = alert_time.isoformat()
                        
                except Exception as e:
                    # On parsing error, try to recover before falling back to current time
                    logger.error(f"⚠️ Error parsing timestamp for alert {data.get('symbol', 'UNKNOWN')}: {e}")
                    # Try one more time with published_at if it exists
                    try:
                        published_at = data.get('published_at')
                        if published_at and isinstance(published_at, str):
                            alert_time = datetime.fromisoformat(published_at.split('.')[0])
                            timestamp_str = alert_time.isoformat()
                            logger.info(f"✅ Recovered timestamp from published_at after error")
                        else:
                            alert_time = datetime.now()
                            timestamp_str = alert_time.isoformat()
                    except:
                        alert_time = datetime.now()
                        timestamp_str = alert_time.isoformat()
                
                # NOTE: No date filtering for alerts - dashboard shows last 1000 messages from stream
                # as per DASHBOARD_INTEGRATION.md documentation. Only OHLC/time-series data is filtered by date.
                
                symbol = data.get('symbol', 'UNKNOWN')
                
                # CRITICAL FIX: Ensure symbol is a string (not bytes)
                if isinstance(symbol, bytes):
                    symbol = symbol.decode('utf-8')
                symbol = str(symbol)
                original_symbol = symbol
                
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
                
                symbol_full = symbol
                display_symbol = base_symbol
                symbol = symbol_full  # use full symbol for downstream indicator lookups
                
                # ENHANCED: Debug indicators for first few symbols
                if len(self.alerts_data) < 5:  # Debug first 5 symbols
                    try:
                        self.debug_indicators_loading(symbol_full)
                    except Exception as e:
                        logger.debug(f"Debug indicators loading failed: {e}")
                
                instrument_name = self.get_instrument_name(display_symbol)
                expiry_info = self.get_expiry_date(symbol_full)
                
                # Load indicators or Greeks based on instrument type
                is_option = self.is_option_instrument(symbol_full)
                is_futures = self.is_futures_instrument(symbol_full)
                
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
                                logger.debug(f"✅ Loaded indicators from alert payload for {symbol}: {list(alert_indicators.keys())}")
                        except:
                            pass
                    elif isinstance(indicators_raw, dict) and indicators_raw:
                        indicators.update(indicators_raw)
                        logger.debug(f"✅ Loaded indicators from alert payload for {symbol}: {list(indicators_raw.keys())}")
                
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
                    logger.debug(f"✅ Extracted {len([f for f in indicator_fields if f in data and data[f] is not None])} top-level indicators from alert data for {symbol}")
                
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
                    # Use _fetch_greeks_from_redis (same method as alert_manager) for consistency
                    if not greeks:
                        greeks = self._fetch_greeks_from_redis(symbol)
                        if not greeks:
                            # Try load_greeks_for_symbol as secondary fallback
                            greeks = self.load_greeks_for_symbol(symbol)
                        print(f"📊 Loaded Greeks from Redis (fallback) for {symbol}: {list(greeks.keys()) if greeks else 'none'}")
                    elif len(greeks) < 3:  # If partial, try to fill from Redis
                        redis_greeks = self._fetch_greeks_from_redis(symbol)
                        if not redis_greeks:
                            redis_greeks = self.load_greeks_for_symbol(symbol)
                        for k, v in redis_greeks.items():
                            if k not in greeks:
                                greeks[k] = v
                    
                    # FINAL FALLBACK: Calculate Greeks on-demand if still not found and we have required data
                    if not greeks or len(greeks) < 3:
                        print(f"🔧 Attempting on-demand Greek calculation for {symbol}...")
                        try:
                            # Check if we have the required data to calculate Greeks
                            underlying_price = data.get('underlying_price') or data.get('spot_price') or data.get('underlying')
                            strike_price = data.get('strike_price') or data.get('strike')
                            expiry_date = data.get('expiry_date') or (expiry_info.get('expiry_date') if expiry_info and isinstance(expiry_info, dict) else None)
                            
                            print(f"   Initial values - underlying_price: {underlying_price}, strike_price: {strike_price}, expiry_date: {expiry_date}")
                            
                            # Extract option type from symbol (CE = call, PE = put)
                            option_type = 'call'
                            if 'CE' in symbol.upper():
                                option_type = 'call'
                            elif 'PE' in symbol.upper():
                                option_type = 'put'
                            
                            # Try to get strike from symbol if not in data (e.g., NIFTY25NOV26000CE -> strike = 26000)
                            if not strike_price:
                                # Match pattern like 26000CE or 26000PE (strike before CE/PE)
                                match = re.search(r'(\d+)(CE|PE)', symbol.upper())
                                if match:
                                    strike_price = float(match.group(1))
                            
                            # Get underlying price from Redis if not in alert data
                            if not underlying_price:
                                # Extract underlying symbol (NIFTY, BANKNIFTY, etc.) from option symbol
                                # Pattern: NIFTY25NOV26000CE -> underlying = NIFTY
                                underlying_match = re.match(r'^([A-Z]+)', symbol.upper())
                                if underlying_match:
                                    underlying_symbol = underlying_match.group(1)
                                    
                                    # Try multiple methods to get underlying price
                                    # CRITICAL: ohlc_latest keys are stored as HASHES, not strings
                                    price_sources = [
                                        # Method 1: Direct price key (string)
                                        f"price:{underlying_symbol}",
                                        # Method 2: Hash keys for futures (NFO prefix is most common)
                                        f"ohlc_latest:NFO{underlying_symbol}25DECFUT",
                                        f"ohlc_latest:NFO{underlying_symbol}25NOVFUT",
                                        f"ohlc_latest:{underlying_symbol}25DECFUT",
                                        f"ohlc_latest:{underlying_symbol}25NOVFUT",
                                    ]
                                    
                                    # Also add DB 2 sources (ohlc_daily sorted sets)
                                    db2_price_sources = [
                                        f"ohlc_daily:NFO{underlying_symbol}25DECFUT",
                                        f"ohlc_daily:NFO{underlying_symbol}25NOVFUT",
                                        f"ohlc_daily:{underlying_symbol}25DECFUT",
                                        f"ohlc_daily:{underlying_symbol}25NOVFUT",
                                        f"ohlc_daily:{underlying_symbol}",
                                    ]
                                    price_sources.extend(db2_price_sources)
                                    
                                    # Try both DB 1 and DB 5 for each source
                                    for price_source in price_sources:
                                        try:
                                            # For ohlc_latest keys, they are ALWAYS hashes, skip GET
                                            if price_source.startswith('ohlc_latest:'):
                                                # Try DB 1 first, then DB 2 (daily snapshots stored in DB2 as well)
                                                for db_client, db_name in [(self.redis_db1, 'DB1'), (self.redis_db2, 'DB2')]:
                                                    try:
                                                        price_hash = db_client.hgetall(price_source)
                                                        if price_hash and len(price_hash) > 0:
                                                            # Handle bytes keys/values
                                                            def get_hash_field(name):
                                                                if isinstance(name, str):
                                                                    name_bytes = name.encode('utf-8')
                                                                else:
                                                                    name_bytes = name
                                                                val = price_hash.get(name) or price_hash.get(name_bytes)
                                                                if val:
                                                                    if isinstance(val, bytes):
                                                                        return val.decode('utf-8')
                                                                    return val
                                                                return None
                                                            
                                                            close_val = get_hash_field('close') or get_hash_field('last_price') or get_hash_field('lastPrice')
                                                            if close_val:
                                                                try:
                                                                    # CRITICAL FIX: Handle bytes values properly
                                                                    if isinstance(close_val, bytes):
                                                                        close_val = close_val.decode('utf-8')
                                                                    underlying_price = float(close_val)
                                                                    logger.debug(f"✅ Got underlying price from {price_source} (hash) in {db_name}: {underlying_price}")
                                                                    print(f"✅ Got underlying price for on-demand Greek calculation: {underlying_price}")
                                                                    break
                                                                except Exception as e:
                                                                    logger.debug(f"Failed to convert close_val to float: {close_val}, error: {e}")
                                                                    pass
                                                    except Exception:
                                                        continue
                                                    if underlying_price:
                                                        break
                                                if underlying_price:
                                                    break
                                            elif price_source.startswith('ohlc_daily:'):
                                                # Try DB 2 sorted set (ohlc_daily keys are sorted sets with latest price)
                                                try:
                                                    # Get latest entry from sorted set (highest score = most recent)
                                                    latest_entry = self.redis_db2.zrange(price_source, -1, -1, withscores=True)
                                                    if latest_entry:
                                                        # Entry format: [b'{"close": 26000.0, ...}', timestamp]
                                                        entry_data = latest_entry[0][0]
                                                        if isinstance(entry_data, bytes):
                                                            entry_data = entry_data.decode('utf-8')
                                                        try:
                                                            price_data = json.loads(entry_data)
                                                            if isinstance(price_data, dict):
                                                                underlying_price = price_data.get('close') or price_data.get('last_price') or price_data.get('price')
                                                            else:
                                                                underlying_price = float(price_data) if price_data else None
                                                            if underlying_price:
                                                                underlying_price = float(underlying_price)
                                                                logger.debug(f"✅ Got underlying price from {price_source} (sorted set) in DB2: {underlying_price}")
                                                                print(f"✅ Got underlying price for on-demand Greek calculation from DB2: {underlying_price}")
                                                                break
                                                        except json.JSONDecodeError:
                                                            # If not JSON, might be just the price value
                                                            try:
                                                                underlying_price = float(entry_data)
                                                                logger.debug(f"✅ Got underlying price from {price_source} (direct value) in DB2: {underlying_price}")
                                                                break
                                                            except:
                                                                pass
                                                except Exception as e:
                                                    logger.debug(f"Failed to get price from {price_source} in DB2: {e}")
                                                    pass
                                            else:
                                                # For non-ohlc keys, try GET first in both DB 1 and DB 2
                                                for db_client, db_name in [(self.redis_db1, 'DB1'), (self.redis_db2, 'DB2')]:
                                                    try:
                                                        price_val = db_client.get(price_source)
                                                        if price_val:
                                                            if isinstance(price_val, bytes):
                                                                price_val = price_val.decode('utf-8')
                                                            try:
                                                                underlying_price = float(price_val)
                                                                logger.debug(f"✅ Got underlying price from {price_source} in {db_name}: {underlying_price}")
                                                                print(f"✅ Got underlying price for on-demand Greek calculation: {underlying_price}")
                                                                break
                                                            except:
                                                                # Try JSON parse
                                                                try:
                                                                    price_data = json.loads(price_val)
                                                                    if isinstance(price_data, dict):
                                                                        underlying_price = price_data.get('last_price') or price_data.get('price') or price_data.get('close')
                                                                    else:
                                                                        underlying_price = float(price_data) if price_data else None
                                                                    if underlying_price:
                                                                        logger.debug(f"✅ Got underlying price from {price_source} (JSON) in {db_name}: {underlying_price}")
                                                                        print(f"✅ Got underlying price for on-demand Greek calculation: {underlying_price}")
                                                                        break
                                                                except:
                                                                    pass
                                                    except Exception as get_error:
                                                        # If GET fails (e.g., WRONGTYPE for hash), ignore and continue
                                                        logger.debug(f"GET failed for {price_source} in {db_name} (may be hash): {get_error}")
                                                        pass
                                                    if underlying_price:
                                                        break
                                        except Exception as e:
                                            logger.debug(f"Failed to get price from {price_source}: {e}")
                                            continue
                                    
                                    if not underlying_price:
                                        logger.debug(f"⚠️ Could not find underlying price for {underlying_symbol} from any source")
                            
                            # If we have both strike and underlying, calculate Greeks
                            print(f"   Final check - underlying_price: {underlying_price}, strike_price: {strike_price}")
                            if underlying_price and strike_price and underlying_price > 0 and strike_price > 0:
                                try:
                                    print(f"   ✅ All required data available, calculating Greeks...")
                                    from intraday_scanner.calculations import greek_calculator
                                    
                                    # Build tick_data-like dict for Greek calculation
                                    tick_data_for_greeks = {
                                        'symbol': symbol,
                                        'underlying_price': float(underlying_price),
                                        'strike_price': float(strike_price),
                                        'option_type': option_type,
                                        'expiry_date': expiry_date
                                    }
                                    
                                    print(f"   Tick data for Greeks: {tick_data_for_greeks}")
                                    calculated_greeks = greek_calculator.calculate_greeks_for_tick_data(tick_data_for_greeks)
                                    print(f"   Calculated Greeks result: {calculated_greeks}")
                                    
                                    if calculated_greeks and any(calculated_greeks.get(greek, 0) != 0 for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                                        if not greeks:
                                            greeks = {}
                                        # Merge calculated Greeks (only add missing ones)
                                        for greek_name in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                                            if greek_name not in greeks and calculated_greeks.get(greek_name) is not None:
                                                greek_value = self._safe_float(calculated_greeks.get(greek_name))
                                                if greek_value is not None:
                                                    greeks[greek_name] = greek_value
                                        print(f"✅ Calculated Greeks on-demand for {symbol}: {list(greeks.keys()) if greeks else 'none'} - Values: {greeks}")
                                    else:
                                        print(f"⚠️ Greek calculation returned empty/zero values for {symbol}")
                                except Exception as calc_error:
                                    print(f"❌ On-demand Greek calculation failed for {symbol}: {calc_error}")
                                    import traceback
                                    logger.debug(f"On-demand Greek calculation failed for {symbol}: {calc_error}\n{traceback.format_exc()}")
                            else:
                                print(f"⚠️ Missing required data for Greek calculation - underlying: {underlying_price}, strike: {strike_price}")
                        except Exception as e:
                            print(f"❌ Error in on-demand Greek calculation fallback for {symbol}: {e}")
                            import traceback
                            logger.debug(f"Error in on-demand Greek calculation fallback for {symbol}: {e}\n{traceback.format_exc()}")
                
                # PRIORITY 3: Load from Redis (ALWAYS try this for equity/futures/options)
                # CRITICAL: Try Redis even if we have some indicators, as Redis may have more complete data
                # Use the EXACT same symbol format as scanner uses when storing
                # Scanner stores with symbol exactly as received from tick_data (usually with exchange prefix)
                redis_indicators = self.load_indicators_for_symbol(symbol)
                
                # Also try with symbol variants in case scanner stored with different format
                if not redis_indicators:
                    # Try additional symbol variants that scanner might use
                    symbol_variants_to_try = []
                    base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                    # Scanner might store with full symbol, base symbol, or normalized
                    if ':' in symbol:
                        symbol_variants_to_try.append(base_symbol)  # Try without exchange prefix
                        symbol_variants_to_try.append(f"NFO:{base_symbol}")  # Try with NFO prefix
                        symbol_variants_to_try.append(f"NSE:{base_symbol}")  # Try with NSE prefix
                    else:
                        # No prefix - try adding common prefixes
                        symbol_variants_to_try.append(f"NFO:{symbol}")  # Try with NFO prefix
                        symbol_variants_to_try.append(f"NSE:{symbol}")  # Try with NSE prefix
                    
                    for variant in symbol_variants_to_try:
                        if variant != symbol:  # Already tried original symbol
                            variant_indicators = self.load_indicators_for_symbol(variant)
                            if variant_indicators:
                                redis_indicators = variant_indicators
                                logger.debug(f"✅ Found indicators using symbol variant: {variant} (original: {symbol})")
                                break
                
                if redis_indicators:
                    if not indicators:
                        # No alert indicators - use Redis
                        indicators = redis_indicators
                        logger.debug(f"✅ Loaded {len(indicators)} indicators from Redis for {symbol}")
                        print(f"✅ Loaded {len(indicators)} indicators from Redis for {symbol}: {list(indicators.keys())[:5]}")
                    else:
                        # Supplement alert indicators with Redis (Redis takes precedence for missing values)
                        before_count = len(indicators)
                        indicators.update(redis_indicators)
                        added_count = len(indicators) - before_count
                        if added_count > 0:
                            logger.debug(f"✅ Supplemented with {added_count} indicators from Redis for {symbol}")
                            print(f"✅ Supplemented with {added_count} indicators from Redis for {symbol}")
                elif not indicators:
                    logger.debug(f"⚠️ No indicators found for {symbol} in alert data or Redis")
                    print(f"⚠️ No indicators found for {symbol} - checked Redis and alert data")
                
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
                            
                            # CRITICAL: For options and futures, extract underlying symbol only
                            # Options: NIFTY25NOV26000CE/PE -> NIFTY
                            # Futures: NIFTY25DECFUT -> NIFTY
                            # Equity: RELIANCE -> RELIANCE (unchanged)
                            
                            # Pattern for options: SYMBOL + DATE + STRIKE + CE/PE (e.g., NIFTY25NOV26000CE)
                            option_match = re.match(r'^([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE)$', clean_symbol)
                            if option_match:
                                # Extract underlying symbol from option
                                clean_symbol = option_match.group(1)
                            # Pattern for futures: SYMBOL + DATE + FUT (e.g., NIFTY25DECFUT)
                            elif clean_symbol.endswith('FUT') or re.search(r'\d{2}[A-Z]{3}FUT$', clean_symbol):
                                # Extract base symbol by removing date+FUT pattern
                                base_match = re.match(r'^([A-Z]+)', clean_symbol)
                                if base_match:
                                    clean_symbol = base_match.group(1)
                                # Also try removing date+expiry pattern more aggressively
                                if re.search(r'\d{2}[A-Z]{3}', clean_symbol):
                                    clean_symbol = re.sub(r'\d{2}[A-Z]{3}(FUT|CE|PE)?.*$', '', clean_symbol)
                            
                            # Try multiple symbol variations for news lookup
                            symbol_variants = [clean_symbol, symbol]
                            enriched_news = []
                            
                            for variant in symbol_variants:
                                temp_alert = {'symbol': variant}
                                enriched = enrich_alert_with_news(temp_alert, self.redis_client, lookback_minutes=120, top_k=10)
                                variant_news = enriched.get('news', [])
                                if variant_news:
                                    enriched_news.extend(variant_news)
                            
                            # Also check Redis directly using deterministic symbol keys
                            if not enriched_news and self.redis_db1:
                                direct_items = []
                                for variant in self._generate_symbol_variants(symbol):
                                    try:
                                        symbol_news_key = f"news:symbol:{variant}"
                                        zset_items = self.redis_db1.zrevrange(symbol_news_key, 0, 4)
                                        for raw_item in zset_items or []:
                                            decoded_item = self._decode_redis_value(raw_item)
                                            if not decoded_item:
                                                continue
                                            try:
                                                parsed_item = json.loads(decoded_item)
                                            except json.JSONDecodeError:
                                                parsed_item = {"headline": decoded_item}
                                            parsed_payload = parsed_item.get("data") if isinstance(parsed_item, dict) else parsed_item
                                            if isinstance(parsed_payload, dict) and parsed_payload not in direct_items:
                                                direct_items.append(parsed_payload)
                                    except Exception as redis_err:
                                        logger.debug(f"Symbol news lookup failed for {variant}: {redis_err}")
                                    
                                    latest_key = f"news:latest:{variant}"
                                    latest_payload = self._decode_redis_value(self._safe_redis_get(self.redis_db1, latest_key))
                                    if latest_payload:
                                        try:
                                            latest_item = json.loads(latest_payload)
                                        except json.JSONDecodeError:
                                            latest_item = {"headline": latest_payload}
                                        if isinstance(latest_item, dict) and latest_item not in direct_items:
                                            direct_items.append(latest_item)
                                
                                if direct_items:
                                    enriched_news.extend(direct_items)
                            
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
                
                # If not found, check nested risk_metrics (CRITICAL: RiskManager stores target_price here)
                if target is None and 'risk_metrics' in data:
                    risk_metrics = data['risk_metrics']
                    # Handle string JSON
                    if isinstance(risk_metrics, str):
                        try:
                            risk_metrics = json.loads(risk_metrics)
                        except:
                            risk_metrics = {}
                    # Extract from nested dict
                    if isinstance(risk_metrics, dict):
                        # RiskManager stores as 'target_price' (not 'target')
                        target = risk_metrics.get('target_price') or risk_metrics.get('target')
                        if target:
                            try:
                                target = float(target) if float(target) > 0 else None
                            except (ValueError, TypeError):
                                target = None
                
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
                
                # CRITICAL: Extract quantity from multiple sources (risk_metrics nested dict)
                quantity = None
                # First check direct fields
                if 'quantity' in data and data['quantity'] is not None:
                    try:
                        quantity_val = float(data['quantity'])
                        quantity = quantity_val if quantity_val > 0 else None  # Only set if > 0
                    except (ValueError, TypeError):
                        quantity = None
                
                # If not found, calculate from risk_metrics.position_size
                if quantity is None and 'risk_metrics' in data and entry_price > 0:
                    risk_metrics = data['risk_metrics']
                    # Handle string JSON
                    if isinstance(risk_metrics, str):
                        try:
                            risk_metrics = json.loads(risk_metrics)
                        except:
                            risk_metrics = {}
                    # Extract position_size and calculate quantity
                    if isinstance(risk_metrics, dict):
                        position_size = risk_metrics.get('position_size')
                        if position_size:
                            try:
                                quantity = float(position_size) / entry_price if entry_price > 0 else None
                                quantity = quantity if quantity and quantity > 0 else None
                            except:
                                quantity = None
                
                pattern_raw = data.get('pattern', data.get('pattern_type', 'unknown'))
                pattern_key = normalize_pattern_name(pattern_raw)
                # Store human-friendly label; prefer original casing if provided
                if pattern_key not in self.pattern_labels:
                    self.pattern_labels[pattern_key] = pattern_display_label(pattern_key, pattern_raw)
                if pattern_key not in self.all_patterns:
                    self.all_patterns.append(pattern_key)
                pattern_label = self.pattern_labels.get(pattern_key, pattern_raw or pattern_key)
                
                # Ensure alert_time safe for formatting
                display_alert_time = alert_time.strftime('%H:%M:%S') if alert_time else ''
                
                alert_info = {
                    'symbol': display_symbol,
                    'symbol_full': symbol_full,
                    'instrument_name': instrument_name,  # Human-readable name
                    'expiry': expiry_info,  # Expiry date with status
                    'pattern': pattern_label,
                    'pattern_key': pattern_key,
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
                    'time_of_day': display_alert_time,
                    # Optional validation fields (if present)
                    'validation_score': data.get('validation_score', None),
                    'is_valid': str(data.get('is_valid', '')) if data.get('is_valid') is not None else '',
                    'reasons': reasons_str,  # Convert list to string
                    'instrument_type': 'Options' if is_option else ('Futures' if is_futures else 'Equity'),
                    'has_news': len(news_items) > 0,  # Flag for news-driven alerts
                    'news_count': len(news_items),  # Count of news items
                    # Technical indicators (for equity/futures) - CRITICAL: Extract from multiple sources
                    # Priority: 1) loaded indicators dict, 2) alert data top-level, 3) alert data['indicators']
                    'rsi': self._safe_float(
                        self._extract_indicator_value(indicators, 'rsi') or
                        self._extract_indicator_value(data, 'rsi')
                    ),
                    'macd': self._safe_float(
                        self._extract_macd_value(indicators) or
                        self._extract_macd_value(data)
                    ),  # Note: MACD may be a dict with 'macd', 'signal', 'histogram' - we extract the main 'macd' value
                    'ema_20': self._safe_float(
                        self._extract_indicator_value(indicators, 'ema_20') or
                        self._extract_indicator_value(data, 'ema_20')
                    ),
                    'ema_50': self._safe_float(
                        self._extract_indicator_value(indicators, 'ema_50') or
                        self._extract_indicator_value(data, 'ema_50')
                    ),
                    'atr': self._safe_float(
                        self._extract_indicator_value(indicators, 'atr') or
                        self._extract_indicator_value(data, 'atr')
                    ),
                    'vwap': self._safe_float(
                        self._extract_indicator_value(indicators, 'vwap') or
                        self._extract_indicator_value(data, 'vwap')
                    ),
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
                    'volume_ratio': self._safe_float(
                        self._extract_indicator_value(indicators, 'volume_ratio') or
                        self._extract_indicator_value(data, 'volume_ratio') or
                        data.get('volume_ratio')
                    ),
                    # Greeks (for options) - CRITICAL: Extract from loaded greeks dict
                    # Handle None, empty strings, and invalid values
                    'delta': self._safe_float(greeks.get('delta') if greeks else None),
                    'gamma': self._safe_float(greeks.get('gamma') if greeks else None),
                    'theta': self._safe_float(greeks.get('theta') if greeks else None),
                    'vega': self._safe_float(greeks.get('vega') if greeks else None),
                    'rho': self._safe_float(greeks.get('rho') if greeks else None),
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
        
        # After loading some alerts, debug a few symbols
        if len(self.alerts_data) > 0:
            test_symbols = [alert['symbol'] for alert in self.alerts_data[:3]]  # Test first 3 symbols
            print(f"🔍 Debugging Redis indicators for first 3 symbols: {test_symbols}")
            for symbol in test_symbols:
                self.debug_redis_indicators(symbol)
        
        # Load price data from multiple sources - CRITICAL: Load full day's time series data
        symbols = list(set([alert['symbol'] for alert in self.alerts_data]))
        print(f"Loading price data for {len(symbols)} symbols from Redis time series...")
        
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
                                    
                                    # NOTE: Don't filter by date for chart display - we want ALL available data
                                    # Date filtering is only for rolling window calculations, not for chart display
                                    
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
                        zset_entries = self.redis_db2.zrange(zset_key, 0, -1, withscores=True)
                        
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
                                    
                                    # NOTE: Don't filter by date for chart display - we want ALL available data
                                    # Date filtering is only for rolling window calculations, not for chart display
                                    
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
            if not symbol_loaded:
                bucket_series = self._load_bucket_series_from_manager(symbol)
                if bucket_series:
                    print(f"📊 Loaded {len(bucket_series)} bucket entries for {symbol} via manager helper")
                    for bucket in bucket_series:
                        try:
                            hour = int(bucket.get('hour', bucket.get('bucket_hour', 0)) or 0)
                            minute_bucket = int(bucket.get('minute_bucket', bucket.get('bucket_index', 0)) or 0)
                            timestamp_value = bucket.get('timestamp') or bucket.get('bucket_timestamp')
                            bucket_dt = self._parse_datetime(timestamp_value)
                            if not bucket_dt:
                                session_date = bucket.get('_session_date') or datetime.now().strftime('%Y-%m-%d')
                                bucket_dt = datetime.strptime(session_date, '%Y-%m-%d').replace(
                                    hour=hour,
                                    minute=minute_bucket,
                                    second=0,
                                    microsecond=0,
                                )

                            price_info = {
                                'symbol': symbol,
                                'open': float(bucket.get('open', bucket.get('first_price', 0) or 0.0)),
                                'high': float(bucket.get('high', bucket.get('last_price', 0) or 0.0)),
                                'low': float(bucket.get('low', bucket.get('last_price', 0) or 0.0)),
                                'close': float(bucket.get('close', bucket.get('last_price', 0) or 0.0)),
                                'volume': int(float(bucket.get('bucket_incremental_volume', bucket.get('volume', 0) or 0.0))),
                                'timestamp': bucket_dt,
                                'time_str': bucket_dt.strftime('%H:%M'),
                                'source': 'bucket',
                            }
                            if price_info['close'] > 0:
                                self.price_data.append(price_info)
                                symbol_loaded = True
                        except Exception as bucket_err:
                            logger.debug(f"Bucket conversion failed for {symbol}: {bucket_err}")
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
                                    
                                    # NOTE: Don't filter by date for chart display - we want ALL available data
                                    # Date filtering is only for rolling window calculations, not for chart display
                                    # ohlc_latest is a snapshot, so we include it regardless of date
                                    
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
        
        print(f"✅ Loaded {len(self.alerts_data)} alerts and {len(self.price_data)} price records")
        
        # ENHANCED: Preload price data for ALL intraday instruments, not just those with alerts
        # This ensures charts show immediately when symbols are selected
        print("📊 Preloading price data for all intraday instruments...")
        symbols_already_loaded = set(p['symbol'] for p in self.price_data)
        symbols_to_load = [sym for sym in list(self.intraday_instruments)[:50] if sym not in symbols_already_loaded]
        
        if symbols_to_load:
            print(f"   Loading data for {len(symbols_to_load)} symbols (limited to first 50 for performance)...")
            for symbol in symbols_to_load:
                try:
                    # Load recent price data for this symbol
                    recent_prices = self.load_recent_ohlc_data(symbol)
                    if recent_prices:
                        self.price_data.extend(recent_prices)
                        print(f"   ✅ Loaded {len(recent_prices)} price records for {symbol}")
                except Exception as e:
                    logger.debug(f"Error preloading price data for {symbol}: {e}")
                    continue
        
        # Debug: Show what we have
        print(f"\n📊 Data Loading Summary:")
        print(f"   • Alerts: {len(self.alerts_data)}")
        print(f"   • Price records: {len(self.price_data)}")
        
        if self.alerts_data:
            import pandas as pd
            df_alerts_temp = pd.DataFrame(self.alerts_data)
            if 'symbol' in df_alerts_temp.columns and not df_alerts_temp.empty:
                top_symbols = df_alerts_temp['symbol'].value_counts().head(5).to_dict()
                print(f"   • Top symbols with alerts: {top_symbols}")
        
        if self.price_data:
            symbols_with_data = len(set(p['symbol'] for p in self.price_data))
            print(f"   • Price data available for {symbols_with_data} symbols")
        
    def _calculate_time_series_indicators(self, closes, highs, lows, volumes):
        """Calculate time-series indicators from price data using pandas
        
        Returns dict with time-series arrays for each indicator
        """
        import numpy as np
        
        if len(closes) < 20:  # Need minimum data points
            return {}
        
        df = pd.DataFrame({
            'close': closes,
            'high': highs,
            'low': lows,
            'volume': volumes
        })
        
        indicators_ts = {}
        
        try:
            # Calculate VWAP (Volume-Weighted Average Price) - time series
            cumulative_pv = (df['close'] * df['volume']).cumsum()
            cumulative_volume = df['volume'].cumsum()
            indicators_ts['vwap'] = (cumulative_pv / cumulative_volume).fillna(df['close']).tolist()
            
            # Calculate EMAs (Exponential Moving Averages) - time series
            for period in [5, 10, 20, 50, 100, 200]:
                if len(closes) >= period:
                    ema = df['close'].ewm(span=period, adjust=False).mean()
                    indicators_ts[f'ema_{period}'] = ema.tolist()
            
            # Calculate RSI (Relative Strength Index) - time series
            if len(closes) >= 14:
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                indicators_ts['rsi'] = rsi.fillna(50).tolist()  # Fill NaN with neutral 50
            
            # Calculate MACD (Moving Average Convergence Divergence) - time series
            if len(closes) >= 26:
                ema12 = df['close'].ewm(span=12, adjust=False).mean()
                ema26 = df['close'].ewm(span=26, adjust=False).mean()
                macd_line = ema12 - ema26
                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                histogram = macd_line - signal_line
                
                indicators_ts['macd'] = {
                    'macd': macd_line.tolist(),
                    'signal': signal_line.tolist(),
                    'histogram': histogram.tolist()
                }
            
            # Calculate Bollinger Bands - time series
            if len(closes) >= 20:
                sma20 = df['close'].rolling(window=20).mean()
                std20 = df['close'].rolling(window=20).std()
                indicators_ts['bb_upper'] = (sma20 + (std20 * 2)).fillna(df['close']).tolist()
                indicators_ts['bb_middle'] = sma20.fillna(df['close']).tolist()
                indicators_ts['bb_lower'] = (sma20 - (std20 * 2)).fillna(df['close']).tolist()
            
            # Calculate ATR (Average True Range) - time series
            if len(closes) >= 14:
                high_low = df['high'] - df['low']
                high_close = np.abs(df['high'] - df['close'].shift())
                low_close = np.abs(df['low'] - df['close'].shift())
                true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = true_range.rolling(window=14).mean()
                indicators_ts['atr'] = atr.fillna(0).tolist()
        
        except Exception as e:
            logger.debug(f"Error calculating time-series indicators: {e}")
        
        return indicators_ts
    
    def create_enhanced_price_chart(self, symbol, alert_time, entry_price, show_indicators=True):
        """Create an enhanced time series chart with historical indicators and candlestick visualization"""
        
        # Get price data for the symbol
        symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
        
        if not symbol_prices:
            # Try to debug why no price data
            self.debug_redis_indicators(symbol)
            return go.Figure()
        
        # Sort by timestamp
        symbol_prices.sort(key=lambda x: x['timestamp'])
        
        # Create time series data
        times = [p['timestamp'] for p in symbol_prices]
        opens = [p.get('open', p.get('close', 0)) for p in symbol_prices]  # Fallback to close if open not available
        closes = [p['close'] for p in symbol_prices]
        highs = [p['high'] for p in symbol_prices]
        lows = [p['low'] for p in symbol_prices]
        volumes = [p['volume'] for p in symbol_prices]
        
        # Load indicators if enabled
        indicators = {}
        volume_profile_data = {}
        if show_indicators:
            # Load historical indicators (includes both latest values and time-series)
            historical_indicators = self.load_historical_indicators_for_symbol(symbol)
            
            # Extract time-series data for plotting (keys ending with _ts)
            indicators_ts = {}
            for key, value in historical_indicators.items():
                if key.endswith('_ts') and isinstance(value, dict):
                    # Extract base indicator name (remove _ts suffix)
                    base_key = key.replace('_ts', '')
                    # Store time-series array (values only, timestamps handled separately)
                    indicators_ts[base_key] = value.get('values', [])
                elif not key.endswith('_ts'):
                    # Latest values for backward compatibility (table display)
                    indicators[key] = value
            
            # CRITICAL: If no historical data from OHLC streams, calculate from current price data
            if not indicators_ts and len(closes) >= 20:
                indicators_ts_from_price = self._calculate_time_series_indicators(closes, highs, lows, volumes)
                indicators_ts.update(indicators_ts_from_price)
            
            # Merge time-series indicators into main indicators dict (for plotting)
            # Time-series arrays are used by the chart plotting code
            for key, value in indicators_ts.items():
                if value and isinstance(value, list):  # Only update if we have time-series array
                    indicators[key] = value
            
            # Load Volume Profile data (POC, Value Area, Distribution)
            volume_profile_data = self.load_volume_profile_for_symbol(symbol)
        
        # Find alert position in time series
        alert_timestamp = alert_time
        alert_index = None
        for i, t in enumerate(times):
            if t >= alert_timestamp:
                alert_index = i
                break
        
        # Determine subplot configuration
        num_rows = 2  # Price + Volume (base)
        show_rsi = show_indicators and ('rsi' in historical_indicators or 'rsi_ts' in historical_indicators or 'rsi' in indicators)
        show_macd = show_indicators and ('macd' in historical_indicators or 'macd_ts' in historical_indicators or 'macd' in indicators)
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
        
        # PRICE CHART - Use Candlestick instead of line
        fig.add_trace(
            go.Candlestick(
                x=times,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
                name='OHLC',
                increasing_line_color='green',
                decreasing_line_color='red'
            ),
            row=1, col=1
        )
        
        # Alert line
        fig.add_vline(
            x=alert_time,
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
        
        # Add historical indicators to price chart
        if show_indicators and historical_indicators:
            # VWAP
            vwap_data = historical_indicators.get('vwap_ts') or {}
            if vwap_data and isinstance(vwap_data, dict) and 'values' in vwap_data:
                vwap_values = vwap_data['values']
                vwap_times = vwap_data.get('timestamps', times[-len(vwap_values):] if len(vwap_values) <= len(times) else times)
                if len(vwap_values) == len(vwap_times):
                    fig.add_trace(
                        go.Scatter(
                            x=vwap_times, y=vwap_values,
                            mode='lines',
                            name='VWAP',
                            line=dict(color='purple', width=2, dash='dot'),
                            opacity=0.9,
                            showlegend=True,
                            legendgroup='indicators'
                        ),
                        row=1, col=1
                    )
            
            # EMAs
            for ema_period, color in [(20, 'blue'), (50, 'orange'), (5, 'cyan'), (10, 'lightblue')]:
                ema_key = f'ema_{ema_period}_ts'
                ema_data = historical_indicators.get(ema_key, {})
                if ema_data and isinstance(ema_data, dict) and 'values' in ema_data:
                    ema_values = ema_data['values']
                    ema_times = ema_data.get('timestamps', times[-len(ema_values):] if len(ema_values) <= len(times) else times)
                    if len(ema_values) == len(ema_times):
                        fig.add_trace(
                            go.Scatter(
                                x=ema_times, y=ema_values,
                                mode='lines',
                                name=f'EMA {ema_period}',
                                line=dict(color=color, width=1.5),
                                opacity=0.7
                            ),
                            row=1, col=1
                        )
            
            # Bollinger Bands
            bb_data = historical_indicators.get('bollinger_bands', {})
            if bb_data and isinstance(bb_data, dict):
                bb_upper = bb_data.get('upper', [])
                bb_lower = bb_data.get('lower', [])
                bb_middle = bb_data.get('middle', [])
                bb_times = bb_data.get('timestamps', times[-len(bb_upper):] if len(bb_upper) <= len(times) else times)
                
                if len(bb_upper) == len(bb_times) and len(bb_lower) == len(bb_times):
                    # Upper band
                    fig.add_trace(
                        go.Scatter(
                            x=bb_times, y=bb_upper,
                            mode='lines',
                            name='BB Upper',
                            line=dict(color='gray', width=1, dash='dot'),
                            opacity=0.6,
                            showlegend=False
                        ),
                        row=1, col=1
                    )
                    # Lower band with fill
                    fig.add_trace(
                        go.Scatter(
                            x=bb_times, y=bb_lower,
                            mode='lines',
                            name='BB Lower',
                            line=dict(color='gray', width=1, dash='dot'),
                            fill='tonexty',
                            fillcolor='rgba(128,128,128,0.1)',
                            opacity=0.6,
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
        
        # RSI SUBPLOT
        if show_rsi:
            rsi_data = historical_indicators.get('rsi_ts', {})
            rsi_row = volume_row + 1
            
            if rsi_data and isinstance(rsi_data, dict) and 'values' in rsi_data:
                rsi_values = rsi_data['values']
                rsi_times = rsi_data.get('timestamps', times[-len(rsi_values):] if len(rsi_values) <= len(times) else times)
                if len(rsi_values) == len(rsi_times):
                    fig.add_trace(
                        go.Scatter(
                            x=rsi_times, y=rsi_values,
                            mode='lines',
                            name='RSI',
                            line=dict(color='orange', width=2),
                            fill='tozeroy',
                            fillcolor='rgba(255,165,0,0.2)'
                        ),
                        row=rsi_row, col=1
                    )
            
            # RSI reference lines
            fig.add_hline(y=70, line_dash="dash", line_color="red", opacity=0.5, row=rsi_row, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", opacity=0.3, row=rsi_row, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", opacity=0.5, row=rsi_row, col=1)
            fig.update_yaxes(range=[0, 100], title_text="RSI", row=rsi_row, col=1)
        
        # MACD SUBPLOT
        if show_macd:
            macd_data = historical_indicators.get('macd_ts', {})
            macd_row = volume_row + (2 if show_rsi else 1)
            
            if macd_data and isinstance(macd_data, dict):
                macd_times = macd_data.get('timestamps', times)
                macd_line = macd_data.get('macd', [])
                signal_line = macd_data.get('signal', [])
                histogram = macd_data.get('histogram', [])
                
                # MACD line
                if len(macd_line) == len(macd_times):
                    fig.add_trace(
                        go.Scatter(
                            x=macd_times, y=macd_line,
                            mode='lines',
                            name='MACD',
                            line=dict(color='blue', width=2)
                        ),
                        row=macd_row, col=1
                    )
                
                # Signal line
                if len(signal_line) == len(macd_times):
                    fig.add_trace(
                        go.Scatter(
                            x=macd_times, y=signal_line,
                            mode='lines',
                            name='Signal',
                            line=dict(color='red', width=1, dash='dash')
                        ),
                        row=macd_row, col=1
                    )
                
                # Histogram
                if len(histogram) == len(macd_times):
                    colors = ['green' if h >= 0 else 'red' for h in histogram]
                    fig.add_trace(
                        go.Bar(
                            x=macd_times, y=histogram,
                            name='Histogram',
                            marker_color=colors,
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
        
        # Calculate and show price movement
        if len(closes) > 0:
            final_price = closes[-1]
            price_change = final_price - entry_price
            price_change_pct = (price_change / entry_price) * 100 if entry_price > 0 else 0
            
            fig.add_annotation(
                x=times[-1], y=final_price,
                text=f"Final: {final_price:.2f}<br>Change: {price_change:+.2f} ({price_change_pct:+.2f}%)",
                showarrow=True,
                arrowhead=2,
                arrowcolor="green" if price_change > 0 else "red",
                row=1, col=1
            )
        
        # Enhanced layout with range selector
        fig.update_layout(
            title=f'{symbol} - Alert at {alert_time.strftime("%H:%M:%S")}',
            xaxis_title='Time',
            yaxis_title='Price',
            height=700,
            showlegend=True,
            hovermode='x unified',
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1, label="1h", step="hour", stepmode="backward"),
                        dict(count=4, label="4h", step="hour", stepmode="backward"),
                        dict(count=1, label="1d", step="day", stepmode="backward"),
                        dict(step="all")
                    ])
                ),
                rangeslider=dict(visible=False),
                type="date"
            )
        )
        
        fig.update_xaxes(title_text="Time", row=volume_row, col=1)
        fig.update_yaxes(title_text="Volume", row=volume_row, col=1)
        
        return fig
    
    def create_price_chart(self, symbol, alert_time, entry_price, show_indicators=True):
        """Legacy method - calls create_enhanced_price_chart for backward compatibility"""
        return self.create_enhanced_price_chart(symbol, alert_time, entry_price, show_indicators)
    
    def create_default_symbol_chart(self, symbol, lookback_days=20, show_indicators=True):
        """Create a comprehensive chart for a symbol even without alerts"""
        try:
            print(f"🔍 [DEBUG] create_default_symbol_chart: symbol={symbol}")
            # Get price data for the symbol
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            print(f"🔍 [DEBUG] Initial price data: {len(symbol_prices)} records")
            
            if not symbol_prices:
                print(f"🔍 [DEBUG] No price data in memory, loading from Redis...")
                # Try to load recent price data from Redis
                symbol_prices = self.load_recent_ohlc_data(symbol, lookback_days)
                print(f"🔍 [DEBUG] Loaded from Redis: {len(symbol_prices)} records")
                if not symbol_prices:
                    print(f"⚠️ [DEBUG] No price data found, returning empty chart")
                    return self.create_empty_chart(symbol)
            
            # Sort by timestamp
            symbol_prices.sort(key=lambda x: x['timestamp'])
            
            # Create time series data
            times = [p['timestamp'] for p in symbol_prices]
            closes = [p['close'] for p in symbol_prices]
            opens = [p.get('open', p.get('close', 0)) for p in symbol_prices]
            highs = [p.get('high', p.get('close', 0)) for p in symbol_prices]
            lows = [p.get('low', p.get('close', 0)) for p in symbol_prices]
            volumes = [p.get('volume', 0) for p in symbol_prices]
            
            # Create candlestick chart
            fig = go.Figure()
            
            # Add candlestick
            fig.add_trace(go.Candlestick(
                x=times,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
                name='Price'
            ))
            
            # Add technical indicators only if enabled
            if show_indicators:
                print(f"🔍 [DEBUG] create_default_symbol_chart: show_indicators=True, calculating indicators")
                indicators = self.calculate_technical_indicators(opens, highs, lows, closes, volumes)
                print(f"🔍 [DEBUG] create_default_symbol_chart: calculated {list(indicators.keys())}")
                
                # Add VWAP
                vwap_values = self.calculate_vwap(closes, volumes, highs, lows)
                if vwap_values is not None and len(vwap_values) > 0 and any(not np.isnan(v) for v in vwap_values):
                    print(f"🔍 [DEBUG] create_default_symbol_chart: Adding VWAP")
                    fig.add_trace(go.Scatter(
                        x=times, y=vwap_values,
                        mode='lines',
                        name='VWAP',
                        line=dict(color='purple', width=2, dash='dot'),
                        opacity=0.9,
                        showlegend=True,
                        legendgroup='indicators'
                    ))
                
                # Add moving averages
                if 'sma_20' in indicators and len([v for v in indicators['sma_20'] if not np.isnan(v)]) > 0:
                    print(f"🔍 [DEBUG] create_default_symbol_chart: Adding SMA 20")
                    fig.add_trace(go.Scatter(
                        x=times, y=indicators['sma_20'],
                        mode='lines',
                        name='SMA 20',
                        line=dict(color='orange', width=2),
                        showlegend=True,
                        legendgroup='indicators'
                    ))
                
                if 'sma_50' in indicators and len([v for v in indicators['sma_50'] if not np.isnan(v)]) > 0:
                    print(f"🔍 [DEBUG] create_default_symbol_chart: Adding SMA 50")
                    fig.add_trace(go.Scatter(
                        x=times, y=indicators['sma_50'],
                        mode='lines',
                        name='SMA 50', 
                        line=dict(color='blue', width=2),
                        showlegend=True,
                        legendgroup='indicators'
                    ))
                
                if 'ema_12' in indicators and len([v for v in indicators['ema_12'] if not np.isnan(v)]) > 0:
                    print(f"🔍 [DEBUG] create_default_symbol_chart: Adding EMA 12")
                    fig.add_trace(go.Scatter(
                        x=times, y=indicators['ema_12'],
                        mode='lines',
                        name='EMA 12',
                        line=dict(color='green', width=1.5),
                        showlegend=True,
                        legendgroup='indicators'
                    ))
            else:
                print(f"🔍 [DEBUG] create_default_symbol_chart: show_indicators=False, skipping indicators")
            
            # Add volume in subplot
            if len([v for v in volumes if v > 0]) > 0:
                fig.add_trace(go.Bar(
                    x=times, y=volumes,
                    name='Volume',
                    marker_color='lightblue',
                    opacity=0.6,
                    yaxis='y2'
                ))
            
            # Update layout
            instrument_name = self.get_instrument_name(symbol)
            fig.update_layout(
                title=f'{symbol} - {instrument_name}',
                xaxis_title='Time',
                yaxis_title='Price',
                yaxis2=dict(
                    title='Volume',
                    overlaying='y',
                    side='right',
                    showgrid=False
                ),
                height=500,
                showlegend=True,
                xaxis_rangeslider_visible=False
            )
            
            return fig
                
        except Exception as e:
            print(f"Error creating default chart for {symbol}: {e}")
            logger.error(f"Error creating default chart for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def create_candlestick_chart(self, symbol, alert_time=None, entry_price=None, show_indicators=True):
        """Create candlestick chart with optional alert markers"""
        try:
            print(f"🔍 [DEBUG] create_candlestick_chart: symbol={symbol}, show_indicators={show_indicators}, type={type(show_indicators)}, has_price_data={len(self.price_data)} records")
            # Get price data
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            print(f"🔍 [DEBUG] Found {len(symbol_prices)} price records for {symbol}")
            
            if not symbol_prices:
                print(f"🔍 [DEBUG] No price data, calling create_default_symbol_chart")
                return self.create_default_symbol_chart(symbol, show_indicators=show_indicators)
            
            symbol_prices.sort(key=lambda x: x['timestamp'])
            
            times = [p['timestamp'] for p in symbol_prices]
            opens = [p.get('open', p.get('close', 0)) for p in symbol_prices]
            highs = [p.get('high', p.get('close', 0)) for p in symbol_prices]
            lows = [p.get('low', p.get('close', 0)) for p in symbol_prices]
            closes = [p['close'] for p in symbol_prices]
            volumes = [p.get('volume', 0) for p in symbol_prices]
            
            # Create figure with secondary y-axis
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=[f'{symbol} - Price', 'Volume'],
                row_heights=[0.7, 0.3]
            )
            
            # Candlestick
            fig.add_trace(go.Candlestick(
                x=times, open=opens, high=highs, low=lows, close=closes,
                name='OHLC'
            ), row=1, col=1)
            
            # Volume
            if len([v for v in volumes if v > 0]) > 0:
                fig.add_trace(go.Bar(
                    x=times, y=volumes, name='Volume',
                    marker_color='lightblue'
                ), row=2, col=1)
            
            # Add alert markers if provided
            if alert_time and entry_price:
                fig.add_vline(
                    x=alert_time,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Alert",
                    row=1, col=1
                )
                
                fig.add_hline(
                    y=entry_price,
                    line_dash="dot", 
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}",
                    row=1, col=1
                )
            
            # Add technical indicators if enabled
            print(f"🔍 [DEBUG] BEFORE if show_indicators: show_indicators={show_indicators}, type={type(show_indicators)}, bool={bool(show_indicators)}")
            
            # Force boolean conversion to be absolutely sure
            show_indicators_bool = bool(show_indicators) if show_indicators is not None else True
            
            if show_indicators_bool:
                print(f"🔍 [DEBUG] ✅ ENTERING indicator code block for {symbol}")
                try:
                    print(f"🔍 [DEBUG] Calculating indicators for {symbol} (show_indicators={show_indicators_bool})")
                    indicators = self.calculate_technical_indicators(opens, highs, lows, closes, volumes)
                    print(f"🔍 [DEBUG] Calculated indicators: {list(indicators.keys())}")
                    
                    # Add VWAP (always show if available)
                    vwap_values = self.calculate_vwap(closes, volumes, highs, lows)
                    print(f"🔍 [DEBUG] VWAP calculated: type={type(vwap_values)}, length={len(vwap_values) if vwap_values is not None else 'None'}, has_valid={any(not np.isnan(v) for v in vwap_values) if vwap_values is not None and len(vwap_values) > 0 else False}")
                    
                    if vwap_values is not None and len(vwap_values) > 0 and any(not np.isnan(v) for v in vwap_values):
                        print(f"🔍 [DEBUG] ✅ Adding VWAP trace to chart")
                        fig.add_trace(go.Scatter(
                            x=times, y=vwap_values,
                            mode='lines',
                            name='VWAP',
                            line=dict(color='purple', width=2, dash='dot'),
                            opacity=0.9,
                            showlegend=True,
                            legendgroup='indicators'
                        ), row=1, col=1)
                        print(f"🔍 [DEBUG] ✅ VWAP trace added successfully")
                    else:
                        print(f"🔍 [DEBUG] ❌ VWAP calculation failed or returned NaN values: vwap_values={vwap_values[:5] if vwap_values is not None and len(vwap_values) > 5 else vwap_values}")
                    
                    # Add moving averages
                    if 'sma_20' in indicators:
                        sma_valid = len([v for v in indicators['sma_20'] if not np.isnan(v)]) > 0
                        print(f"🔍 [DEBUG] SMA 20 check: in_indicators={True}, valid_count={len([v for v in indicators['sma_20'] if not np.isnan(v)])}, will_add={sma_valid}")
                        if sma_valid:
                            print(f"🔍 [DEBUG] ✅ Adding SMA 20 trace")
                            fig.add_trace(go.Scatter(
                                x=times, y=indicators['sma_20'],
                                mode='lines',
                                name='SMA 20',
                                line=dict(color='orange', width=1.5),
                                showlegend=True,
                                legendgroup='indicators'
                            ), row=1, col=1)
                            print(f"🔍 [DEBUG] ✅ SMA 20 trace added successfully")
                    else:
                        print(f"🔍 [DEBUG] ❌ SMA 20 not in indicators dict")
                    
                    if 'ema_12' in indicators:
                        ema_valid = len([v for v in indicators['ema_12'] if not np.isnan(v)]) > 0
                        print(f"🔍 [DEBUG] EMA 12 check: in_indicators={True}, valid_count={len([v for v in indicators['ema_12'] if not np.isnan(v)])}, will_add={ema_valid}")
                        if ema_valid:
                            print(f"🔍 [DEBUG] ✅ Adding EMA 12 trace")
                            fig.add_trace(go.Scatter(
                                x=times, y=indicators['ema_12'],
                                mode='lines',
                                name='EMA 12',
                                line=dict(color='green', width=1.5),
                                showlegend=True,
                                legendgroup='indicators'
                            ), row=1, col=1)
                            print(f"🔍 [DEBUG] ✅ EMA 12 trace added successfully")
                    else:
                        print(f"🔍 [DEBUG] ❌ EMA 12 not in indicators dict")
                        
                    print(f"🔍 [DEBUG] Indicator code block completed for {symbol}")
                except Exception as e:
                    print(f"🔍 [DEBUG] ❌ EXCEPTION in indicator code block: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"🔍 [DEBUG] ❌ SKIPPING indicators (show_indicators={show_indicators_bool})")
            
            fig.update_layout(
                height=600,
                showlegend=True,
                xaxis_rangeslider_visible=False
            )
            
            return fig
                
        except Exception as e:
            print(f"Error creating candlestick chart: {e}")
            logger.error(f"Error creating candlestick chart: {e}")
            return self.create_empty_chart(symbol)
    
    def create_line_chart(self, symbol, alert_time=None, entry_price=None, show_indicators=True):
        """Create line chart for users who prefer simpler view"""
        try:
            print(f"🔍 [DEBUG] create_line_chart: symbol={symbol}")
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            print(f"🔍 [DEBUG] Found {len(symbol_prices)} price records for {symbol}")
            
            if not symbol_prices:
                print(f"🔍 [DEBUG] No price data, calling create_default_symbol_chart")
                return self.create_default_symbol_chart(symbol, show_indicators=show_indicators)
            
            symbol_prices.sort(key=lambda x: x['timestamp'])
            
            times = [p['timestamp'] for p in symbol_prices]
            closes = [p['close'] for p in symbol_prices]
            volumes = [p.get('volume', 0) for p in symbol_prices]
            
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=[f'{symbol} - Close Price', 'Volume'],
                row_heights=[0.7, 0.3]
            )
            
            # Price line
            fig.add_trace(go.Scatter(
                x=times, y=closes,
                mode='lines',
                name='Close Price',
                line=dict(color='blue', width=2)
            ), row=1, col=1)
            
            # Volume
            if len([v for v in volumes if v > 0]) > 0:
                fig.add_trace(go.Bar(
                    x=times, y=volumes,
                    name='Volume',
                    marker_color='lightblue'
                ), row=2, col=1)
            
            # Add VWAP (always show if indicators enabled)
            if show_indicators:
                highs = [p.get('high', p.get('close', 0)) for p in symbol_prices]
                lows = [p.get('low', p.get('close', 0)) for p in symbol_prices]
                vwap_values = self.calculate_vwap(closes, volumes, highs, lows)
                if vwap_values is not None and len(vwap_values) > 0 and any(not np.isnan(v) for v in vwap_values):
                    fig.add_trace(go.Scatter(
                        x=times, y=vwap_values,
                        mode='lines',
                        name='VWAP',
                        line=dict(color='purple', width=2, dash='dot'),
                        opacity=0.9,
                        showlegend=True,
                        legendgroup='indicators'
                    ), row=1, col=1)
            
            # Alert markers
            if alert_time and entry_price:
                fig.add_vline(
                    x=alert_time,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Alert",
                    row=1, col=1
                )
                
                fig.add_hline(
                    y=entry_price,
                    line_dash="dot",
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}",
                    row=1, col=1
                )
            
            fig.update_layout(height=600, showlegend=True)
            return fig
                
        except Exception as e:
            print(f"Error creating line chart: {e}")
            logger.error(f"Error creating line chart: {e}")
            return self.create_empty_chart(symbol)
    
    def create_empty_chart(self, symbol):
        """Create placeholder chart when no data is available"""
        fig = go.Figure()
        fig.add_annotation(
            text=f"No price data available for {symbol}<br>Try selecting a different symbol",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        fig.update_layout(
            title=f"{symbol} - No Data",
            xaxis_title="Time",
            yaxis_title="Price",
            height=400
        )
        return fig
    
    def load_recent_ohlc_data(self, symbol, lookback_days=20):
        """Load recent OHLC data for a symbol from Redis"""
        try:
            prices = []
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            
            print(f"🔍 [DEBUG] load_recent_ohlc_data: symbol={symbol}, base_symbol={base_symbol}")
            
            # Try multiple Redis key patterns (including exchange prefixes)
            # CRITICAL: Check for exchange-prefixed variants like NFONTPC, NSENTPC, etc.
            key_variants = [
                # Daily sorted sets (DB 5)
                f"ohlc_daily:NFO{base_symbol}",
                f"ohlc_daily:NSE{base_symbol}",
                f"ohlc_daily:BSE{base_symbol}",
                f"ohlc_daily:{base_symbol}",
                f"ohlc_daily:{symbol}",
                # Latest hash (DB 1) - these contain current OHLC snapshot
                f"ohlc_latest:NFO{base_symbol}",
                f"ohlc_latest:NSE{base_symbol}",
                f"ohlc_latest:BSE{base_symbol}",
                f"ohlc_latest:{base_symbol}",
                f"ohlc_latest:{symbol}",
                # Stream updates
                f"ohlc_updates:{base_symbol}",
            ]
            
            # First try sorted sets (ohlc_daily) in DB 5
            for key in key_variants:
                if not key.startswith('ohlc_latest:'):
                    try:
                        zset_data = self.redis_db2.zrange(key, 0, -1, withscores=True)
                        if zset_data:
                            print(f"🔍 [DEBUG] Found sorted set {key} with {len(zset_data)} entries")
                            for entry_data, timestamp in zset_data:
                                try:
                                    if isinstance(entry_data, bytes):
                                        entry_data = entry_data.decode('utf-8')
                                    ohlc = json.loads(entry_data)
                                    
                                    price_info = {
                                        'symbol': symbol,
                                        'open': float(ohlc.get('o', 0)),
                                        'high': float(ohlc.get('h', 0)),
                                        'low': float(ohlc.get('l', 0)),
                                        'close': float(ohlc.get('c', 0)),
                                        'volume': int(float(ohlc.get('v', 0))),
                                        'timestamp': datetime.fromtimestamp(timestamp / 1000.0 if timestamp > 1e10 else timestamp),
                                        'source': 'ohlc_daily'
                                    }
                                    
                                    if price_info['close'] > 0:
                                        prices.append(price_info)
                                except Exception as e:
                                    print(f"🔍 [DEBUG] Error parsing zset entry: {e}")
                                    continue
                            if prices:
                                print(f"🔍 [DEBUG] Loaded {len(prices)} prices from sorted set {key}")
                                return prices
                    except Exception as e:
                        continue
            
            # Then try hash format (ohlc_latest) in DB 1 - these are snapshots, not historical
            for key in key_variants:
                if key.startswith('ohlc_latest:'):
                    try:
                        # CRITICAL: ohlc_latest keys are HASHES, not strings
                        hash_data = self.redis_db1.hgetall(key)
                        if hash_data and len(hash_data) > 0:
                            print(f"🔍 [DEBUG] Found hash {key} with {len(hash_data)} fields")
                            
                            # Helper to get hash field (handle bytes keys/values)
                            def get_field(name):
                                if isinstance(name, str):
                                    name_bytes = name.encode('utf-8')
                                else:
                                    name_bytes = name
                                value = hash_data.get(name) or hash_data.get(name_bytes)
                                if value:
                                    if isinstance(value, bytes):
                                        return value.decode('utf-8')
                                    return str(value)
                                return None
                            
                            close_val = float(get_field('close') or get_field('last_price') or 0)
                            if close_val > 0:
                                # Parse timestamp
                                updated_at_str = get_field('updated_at')
                                date_str = get_field('date')
                                
                                try:
                                    if updated_at_str:
                                        if 'T' in updated_at_str:
                                            timestamp_dt = datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
                                        else:
                                            timestamp_dt = datetime.strptime(updated_at_str, '%Y-%m-%d %H:%M:%S')
                                    elif date_str:
                                        timestamp_dt = datetime.strptime(date_str, '%Y-%m-%d')
                                    else:
                                        timestamp_dt = datetime.now()
                                except:
                                    timestamp_dt = datetime.now()
                                
                                price_info = {
                                    'symbol': symbol,
                                    'open': float(get_field('open') or close_val),
                                    'high': float(get_field('high') or close_val),
                                    'low': float(get_field('low') or close_val),
                                    'close': close_val,
                                    'volume': int(float(get_field('volume') or 0)),
                                    'timestamp': timestamp_dt,
                                    'source': 'ohlc_latest_hash'
                                }
                                
                                prices.append(price_info)
                                print(f"🔍 [DEBUG] Loaded 1 price snapshot from hash {key}: {close_val}")
                                # Hash contains only current snapshot, so return after first successful read
                                if prices:
                                    return prices
                    except Exception as e:
                        print(f"🔍 [DEBUG] Error reading hash {key}: {e}")
                        continue
            
            print(f"🔍 [DEBUG] No price data found for {symbol} from any key variant")
            return prices
        except Exception as e:
            print(f"🔍 [DEBUG] Error in load_recent_ohlc_data for {symbol}: {e}")
            logger.error(f"Error loading recent OHLC data for {symbol}: {e}")
            return []
    
    def load_realtime_data(self, symbol):
        """Load real-time data from streams (ohlc_updates, ticks)"""
        try:
            prices = []
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            normalized_symbol = symbol.replace(':', '').replace(' ', '').upper()
            
            stream_variants = [
                f"ohlc_updates:{symbol}",
                f"ohlc_updates:{base_symbol}",
                f"ohlc_updates:{normalized_symbol}"
            ]
            
            for stream_key in stream_variants:
                try:
                    stream_messages = self.redis_db1.xrevrange(stream_key, count=1000)
                    if stream_messages:
                        for msg_id, fields in stream_messages:
                            try:
                                timestamp_ms = int(fields.get('timestamp', fields.get(b'timestamp', 0)))
                                if timestamp_ms == 0:
                                    continue
                                
                                alert_dt = datetime.fromtimestamp(timestamp_ms / 1000.0)
                                price_info = {
                                    'symbol': symbol,
                                    'open': float(fields.get('open', fields.get(b'open', 0))),
                                    'high': float(fields.get('high', fields.get(b'high', 0))),
                                    'low': float(fields.get('low', fields.get(b'low', 0))),
                                    'close': float(fields.get('close', fields.get(b'close', 0))),
                                    'volume': int(float(fields.get('volume', fields.get(b'volume', 0)))),
                                    'timestamp': alert_dt,
                                    'source': 'realtime_stream'
                                }
                                if price_info['close'] > 0:
                                    prices.append(price_info)
                            except Exception:
                                continue
                        if prices:
                            return sorted(prices, key=lambda x: x['timestamp'])
                except Exception:
                    continue
            
            return prices
        except Exception as e:
            logger.error(f"Error loading real-time data for {symbol}: {e}")
            return []
    
    def load_daily_ohlc_data(self, symbol):
        """Load data from daily OHLC sorted sets only"""
        try:
            prices = []
            base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
            
            zset_variants = [
                f"ohlc_daily:{symbol}",
                f"ohlc_daily:{base_symbol}",
                f"ohlc_daily:{symbol.replace(':', '').upper()}",
            ]
            
            for zset_key in zset_variants:
                try:
                    zset_entries = self.redis_db2.zrange(zset_key, 0, -1, withscores=True)
                    if not zset_entries:
                        zset_entries = self.redis_db1.zrange(zset_key, 0, -1, withscores=True)
                    
                    if zset_entries:
                        for entry_data, timestamp_score in zset_entries:
                            try:
                                if isinstance(entry_data, bytes):
                                    entry_str = entry_data.decode('utf-8')
                                else:
                                    entry_str = entry_data
                                
                                ohlc_json = json.loads(entry_str)
                                
                                timestamp_float = float(timestamp_score)
                                alert_dt = datetime.fromtimestamp(timestamp_float / 1000.0 if timestamp_float > 1e10 else timestamp_float)
                                
                                price_info = {
                                    'symbol': symbol,
                                    'open': float(ohlc_json.get('o', 0)),
                                    'high': float(ohlc_json.get('h', 0)),
                                    'low': float(ohlc_json.get('l', 0)),
                                    'close': float(ohlc_json.get('c', 0)),
                                    'volume': int(float(ohlc_json.get('v', 0))),
                                    'timestamp': alert_dt,
                                    'source': 'daily_ohlc'
                                }
                                
                                if price_info['close'] > 0:
                                    prices.append(price_info)
                            except Exception:
                                continue
                        if prices:
                            return sorted(prices, key=lambda x: x['timestamp'])
                except Exception:
                    continue
            
            return prices
        except Exception as e:
            logger.error(f"Error loading daily OHLC data for {symbol}: {e}")
            return []
    
    def load_bucket_data(self, symbol):
        """Load bucket-based price data using helper without pattern matching."""
        try:
            bucket_series = self._load_bucket_series_from_manager(symbol)
            prices: List[Dict[str, Any]] = []
            
            for bucket in bucket_series:
                try:
                    hour = int(bucket.get('hour', bucket.get('bucket_hour', 0)) or 0)
                    minute_bucket = int(bucket.get('minute_bucket', bucket.get('bucket_index', 0)) or 0)
                    timestamp_value = bucket.get('timestamp') or bucket.get('bucket_timestamp')
                    bucket_dt = self._parse_datetime(timestamp_value)
                    if not bucket_dt:
                        session_date = bucket.get('_session_date') or datetime.now().strftime('%Y-%m-%d')
                        bucket_dt = datetime.strptime(session_date, '%Y-%m-%d').replace(
                            hour=hour,
                            minute=minute_bucket,
                            second=0,
                            microsecond=0,
                        )
                    
                    price_info = {
                        'symbol': symbol,
                        'open': float(bucket.get('open', bucket.get('first_price', 0) or 0.0)),
                        'high': float(bucket.get('high', bucket.get('last_price', 0) or 0.0)),
                        'low': float(bucket.get('low', bucket.get('last_price', 0) or 0.0)),
                        'close': float(bucket.get('close', bucket.get('last_price', 0) or 0.0)),
                        'volume': int(float(bucket.get('bucket_incremental_volume', bucket.get('volume', 0) or 0.0))),
                        'timestamp': bucket_dt,
                        'source': 'bucket',
                    }
                    
                    if price_info['close'] > 0:
                        prices.append(price_info)
                except Exception as bucket_err:
                    logger.debug(f"Bucket conversion failed for {symbol}: {bucket_err}")
                    continue
            
            prices.sort(key=lambda x: x['timestamp'])
            return prices
        except Exception as e:
            logger.error(f"Error loading bucket data for {symbol}: {e}")
            return []
    
    def create_price_volume_scatter(self, symbol, alert_time=None, entry_price=None):
        """Create a 2D scatter plot showing price vs volume relationship"""
        try:
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            if not symbol_prices:
                symbol_prices = self.load_recent_ohlc_data(symbol)
            
            if not symbol_prices:
                return self.create_empty_chart(symbol)
            
            prices = [p.get('close', 0) for p in symbol_prices if p.get('close', 0) > 0]
            volumes = [p.get('volume', 0) for p in symbol_prices if p.get('close', 0) > 0]
            times = [p['timestamp'] for p in symbol_prices if p.get('close', 0) > 0]
            
            if len(prices) == 0 or len(volumes) == 0:
                return self.create_empty_chart(symbol)
            
            fig = go.Figure(data=go.Scatter(
                x=volumes,
                y=prices,
                mode='markers',
                marker=dict(
                    size=8,
                    color=volumes,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Volume"),
                    opacity=0.6
                ),
                text=[t.strftime('%Y-%m-%d %H:%M') for t in times],
                hovertemplate='Volume: %{x:,}<br>Price: ₹%{y:.2f}<br>Time: %{text}<extra></extra>',
                name='Price-Volume'
            ))
            
            if alert_time and entry_price:
                # Find volume at entry price
                entry_volume = next((v for p, v in zip(prices, volumes) if abs(p - entry_price) < 0.01), None)
                if entry_volume:
                    fig.add_trace(go.Scatter(
                        x=[entry_volume],
                        y=[entry_price],
                        mode='markers',
                        marker=dict(size=15, color='orange', symbol='star'),
                        name='Entry',
                        hovertemplate=f'Entry: ₹{entry_price:.2f}<br>Volume: {entry_volume:,}<extra></extra>'
                    ))
            
            fig.update_layout(
                title=f"{symbol} - Price vs Volume Scatter",
                xaxis_title="Volume",
                yaxis_title="Price (₹)",
                height=600,
                showlegend=True,
                hovermode='closest'
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating scatter plot for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def create_ohlc_bars_chart(self, symbol, alert_time=None, entry_price=None, show_indicators=True):
        """Create OHLC bars chart with multi-timeframe view"""
        try:
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            if not symbol_prices:
                symbol_prices = self.load_recent_ohlc_data(symbol)
            
            if not symbol_prices:
                return self.create_empty_chart(symbol)
            
            symbol_prices.sort(key=lambda x: x['timestamp'])
            
            times = [p['timestamp'] for p in symbol_prices]
            opens = [p.get('open', p.get('close', 0)) for p in symbol_prices]
            highs = [p.get('high', p.get('close', 0)) for p in symbol_prices]
            lows = [p.get('low', p.get('close', 0)) for p in symbol_prices]
            closes = [p['close'] for p in symbol_prices]
            volumes = [p.get('volume', 0) for p in symbol_prices]
            
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.1,
                subplot_titles=[f'{symbol} - OHLC Bars', 'Volume'],
                row_heights=[0.7, 0.3]
            )
            
            # OHLC bars (similar to candlestick but with bars)
            fig.add_trace(go.Ohlc(
                x=times,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
                name='OHLC'
            ), row=1, col=1)
            
            # Add VWAP
            if show_indicators:
                vwap_values = self.calculate_vwap(closes, volumes, highs, lows)
                if vwap_values is not None and len(vwap_values) > 0:
                    fig.add_trace(go.Scatter(
                        x=times,
                        y=vwap_values,
                        mode='lines',
                        name='VWAP',
                        line=dict(color='purple', width=2, dash='dot'),
                        opacity=0.9,
                        showlegend=True,
                        legendgroup='indicators'
                    ), row=1, col=1)
            
            # Volume bars
            if len([v for v in volumes if v > 0]) > 0:
                fig.add_trace(go.Bar(
                    x=times,
                    y=volumes,
                    name='Volume',
                    marker_color='lightblue'
                ), row=2, col=1)
            
            # Alert markers
            if alert_time and entry_price:
                fig.add_vline(
                    x=alert_time,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Alert",
                    row=1, col=1
                )
                fig.add_hline(
                    y=entry_price,
                    line_dash="dot",
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}",
                    row=1, col=1
                )
            
            fig.update_layout(
                height=700,
                showlegend=True,
                xaxis_rangeslider_visible=False
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating OHLC bars chart for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def create_price_volume_2d_histogram(self, symbol, alert_time=None, entry_price=None):
        """Create a 2D histogram showing price-volume distribution"""
        try:
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            if not symbol_prices:
                symbol_prices = self.load_recent_ohlc_data(symbol)
            
            if not symbol_prices:
                return self.create_empty_chart(symbol)
            
            prices = [p.get('close', 0) for p in symbol_prices if p.get('close', 0) > 0]
            volumes = [p.get('volume', 0) for p in symbol_prices if p.get('close', 0) > 0]
            
            if len(prices) == 0 or len(volumes) == 0:
                return self.create_empty_chart(symbol)
            
            # Create 2D histogram
            fig = go.Figure(data=go.Histogram2d(
                x=prices,
                y=volumes,
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title="Count")
            ))
            
            fig.update_layout(
                title=f"{symbol} - Price-Volume 2D Histogram",
                xaxis_title="Price (₹)",
                yaxis_title="Volume",
                height=600
            )
            
            if alert_time and entry_price:
                fig.add_vline(
                    x=entry_price,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}"
                )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating 2D histogram for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def create_volume_profile_heatmap(self, symbol, alert_time=None, entry_price=None):
        """Create a volume profile heatmap showing price levels with volume intensity"""
        try:
            # Load volume profile data
            volume_profile = self.load_volume_profile_for_symbol(symbol)
            
            # Also load price data for time-based visualization
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            if not symbol_prices:
                symbol_prices = self.load_recent_ohlc_data(symbol)
            
            if not symbol_prices and not volume_profile.get('distribution'):
                return self.create_empty_chart(symbol)
            
            fig = go.Figure()
            
            # If we have distribution data from volume profile
            if volume_profile.get('distribution'):
                dist_data = volume_profile['distribution']
                price_levels = sorted(dist_data.keys())
                volumes = [dist_data[price] for price in price_levels]
                
                # Create heatmap-like visualization
                fig.add_trace(go.Bar(
                    x=price_levels,
                    y=volumes,
                    marker=dict(
                        color=volumes,
                        colorscale='Hot',
                        showscale=True,
                        colorbar=dict(title="Volume")
                    ),
                    name='Volume Profile',
                    orientation='v'
                ))
                
                # Add POC line if available
                if volume_profile.get('poc_price'):
                    poc_price = volume_profile['poc_price']
                    poc_volume = volume_profile.get('poc_volume', max(volumes) if volumes else 0)
                    fig.add_trace(go.Scatter(
                        x=[poc_price, poc_price],
                        y=[0, poc_volume],
                        mode='lines',
                        name='POC',
                        line=dict(color='red', width=2, dash='dash')
                    ))
                
                # Add Value Area lines
                if volume_profile.get('value_area_high'):
                    vah = volume_profile['value_area_high']
                    fig.add_trace(go.Scatter(
                        x=[vah, vah],
                        y=[0, max(volumes) if volumes else 0],
                        mode='lines',
                        name='VA High',
                        line=dict(color='green', width=1, dash='dot')
                    ))
                
                if volume_profile.get('value_area_low'):
                    val = volume_profile['value_area_low']
                    fig.add_trace(go.Scatter(
                        x=[val, val],
                        y=[0, max(volumes) if volumes else 0],
                        mode='lines',
                        name='VA Low',
                        line=dict(color='green', width=1, dash='dot')
                    ))
            else:
                # Fallback: create from price data
                prices = [p.get('close', 0) for p in symbol_prices if p.get('close', 0) > 0]
                volumes_data = [p.get('volume', 0) for p in symbol_prices if p.get('close', 0) > 0]
                
                if prices and volumes_data:
                    # Create bins for price levels
                    price_min, price_max = min(prices), max(prices)
                    num_bins = 50
                    bin_size = (price_max - price_min) / num_bins
                    
                    # Aggregate volumes by price bins
                    price_volumes = {}
                    for price, volume in zip(prices, volumes_data):
                        bin_price = round((price - price_min) / bin_size) * bin_size + price_min
                        price_volumes[bin_price] = price_volumes.get(bin_price, 0) + volume
                    
                    price_levels = sorted(price_volumes.keys())
                    volumes = [price_volumes[p] for p in price_levels]
                    
                    fig.add_trace(go.Bar(
                        x=price_levels,
                        y=volumes,
                        marker=dict(
                            color=volumes,
                            colorscale='Hot',
                            showscale=True,
                            colorbar=dict(title="Volume")
                        ),
                        name='Volume Profile'
                    ))
            
            if alert_time and entry_price:
                fig.add_vline(
                    x=entry_price,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}"
                )
            
            fig.update_layout(
                title=f"{symbol} - Volume Profile Heatmap",
                xaxis_title="Price (₹)",
                yaxis_title="Volume",
                height=600,
                showlegend=True
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating volume profile heatmap for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def create_volume_distribution_histogram(self, symbol, alert_time=None, entry_price=None):
        """Create a histogram showing volume distribution across price levels"""
        try:
            symbol_prices = [p for p in self.price_data if p['symbol'] == symbol]
            if not symbol_prices:
                symbol_prices = self.load_recent_ohlc_data(symbol)
            
            if not symbol_prices:
                return self.create_empty_chart(symbol)
            
            prices = [p.get('close', 0) for p in symbol_prices if p.get('close', 0) > 0]
            volumes = [p.get('volume', 0) for p in symbol_prices if p.get('close', 0) > 0]
            
            if len(prices) == 0 or len(volumes) == 0:
                return self.create_empty_chart(symbol)
            
            # Create bins for histogram
            price_min, price_max = min(prices), max(prices)
            num_bins = 30
            
            fig = go.Figure(data=go.Histogram(
                x=prices,
                y=volumes,
                histfunc='sum',
                nbinsx=num_bins,
                marker_color='steelblue',
                name='Volume Distribution'
            ))
            
            fig.update_layout(
                title=f"{symbol} - Volume Distribution Histogram",
                xaxis_title="Price (₹)",
                yaxis_title="Total Volume",
                height=600,
                showlegend=True
            )
            
            if alert_time and entry_price:
                fig.add_vline(
                    x=entry_price,
                    line_dash="dash",
                    line_color="orange",
                    annotation_text=f"Entry: ₹{entry_price:.2f}"
                )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating volume distribution histogram for {symbol}: {e}")
            return self.create_empty_chart(symbol)
    
    def calculate_technical_indicators(self, opens, highs, lows, closes, volumes):
        """Calculate basic technical indicators from price data"""
        indicators = {}
        
        try:
            closes_series = pd.Series(closes)
            
            # Simple Moving Averages - calculate even with fewer points (will have NaN at start)
            if len(closes) >= 5:  # Minimum for any meaningful calculation
                indicators['sma_20'] = closes_series.rolling(window=min(20, len(closes))).mean().values
                if len(closes) >= 50:
                    indicators['sma_50'] = closes_series.rolling(window=50).mean().values
                
                # Exponential Moving Averages - work with any length
                indicators['ema_12'] = closes_series.ewm(span=min(12, len(closes)), adjust=False).mean().values
                if len(closes) >= 26:
                    indicators['ema_26'] = closes_series.ewm(span=26, adjust=False).mean().values
                
                # RSI - minimum 14 points
                if len(closes) >= 14:
                    delta = closes_series.diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs = gain / loss
                    indicators['rsi'] = 100 - (100 / (1 + rs)).fillna(50).values  # Fill NaN with neutral 50
                
                # VWAP - calculate if we have volume data
                if len(volumes) == len(closes) and len([v for v in volumes if v > 0]) > 0:
                    typical_price = np.array([(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)])
                    cumulative_tp = np.cumsum(typical_price * volumes)
                    cumulative_volume = np.cumsum(volumes)
                    with np.errstate(divide='ignore', invalid='ignore'):
                        vwap = np.where(cumulative_volume != 0, cumulative_tp / cumulative_volume, typical_price)
                        indicators['vwap'] = vwap
                        
        except Exception as e:
            print(f"🔍 [DEBUG] Error calculating indicators: {e}")
            logger.error(f"Error calculating indicators: {e}")
        
        print(f"🔍 [DEBUG] calculate_technical_indicators: input lengths - closes={len(closes)}, volumes={len(volumes)}, returns={list(indicators.keys())}")
        return indicators
    
    def generate_actionable_insights(self, symbol, alert_data=None, indicators=None, greeks=None, price_data=None):
        """
        Generate actionable trading insights from indicators, Greeks, and price data.
        Returns a list of insight dictionaries with priority, type, message, and recommendation.
        """
        insights = []
        
        try:
            # Get latest price if available
            current_price = None
            if price_data and len(price_data) > 0:
                latest_price = sorted(price_data, key=lambda x: x.get('timestamp', datetime.min))[-1]
                current_price = latest_price.get('close', latest_price.get('last_price'))
            
            # Load indicators if not provided
            if not indicators:
                indicators = self.load_indicators_for_symbol(symbol)
                print(f"🔍 [DEBUG] Loaded {len(indicators)} indicators for insights: {list(indicators.keys())[:5]}")
            
            # Load Greeks if not provided and symbol is an option
            if not greeks:
                # Check if it's an option from alert_data or symbol pattern
                is_option = False
                if alert_data:
                    instrument_type = alert_data.get('instrument_type', '')
                    is_option = 'Option' in instrument_type
                else:
                    # Check symbol pattern
                    is_option = 'CE' in symbol or 'PE' in symbol or 'option' in symbol.lower()
                
                if is_option:
                    greeks = self.load_greeks_for_symbol(symbol)
                    print(f"🔍 [DEBUG] Loaded {len(greeks)} Greeks for insights: {list(greeks.keys())}")
            
            # Get alert context if available
            entry_price = alert_data.get('entry_price') if alert_data else None
            stop_loss = alert_data.get('stop_loss') if alert_data else None
            target = alert_data.get('target') if alert_data else None
            action = alert_data.get('action') if alert_data else None
            confidence = alert_data.get('confidence', 0) if alert_data else 0
            
            # === TECHNICAL INDICATOR INSIGHTS ===
            
            # RSI Analysis
            rsi = indicators.get('rsi')
            if rsi is not None:
                if rsi >= 70:
                    insights.append({
                        'priority': 'high',
                        'type': 'warning',
                        'category': 'Momentum',
                        'title': 'Overbought Condition',
                        'message': f'RSI is {rsi:.1f} - strongly overbought territory',
                        'recommendation': 'Consider taking profits or tightening stop-loss. High reversal risk.',
                        'icon': '📈'
                    })
                elif rsi <= 30:
                    insights.append({
                        'priority': 'high',
                        'type': 'opportunity',
                        'category': 'Momentum',
                        'title': 'Oversold Condition',
                        'message': f'RSI is {rsi:.1f} - strongly oversold territory',
                        'recommendation': 'Potential bounce opportunity. Consider entry with tight stop-loss.',
                        'icon': '📉'
                    })
                elif 50 <= rsi < 70:
                    insights.append({
                        'priority': 'medium',
                        'type': 'info',
                        'category': 'Momentum',
                        'title': 'Bullish Momentum',
                        'message': f'RSI at {rsi:.1f} indicates bullish momentum',
                        'recommendation': 'Price action aligned with uptrend. Consider holding long positions.',
                        'icon': '✅'
                    })
                elif 30 < rsi < 50:
                    insights.append({
                        'priority': 'medium',
                        'type': 'info',
                        'category': 'Momentum',
                        'title': 'Bearish Momentum',
                        'message': f'RSI at {rsi:.1f} indicates bearish momentum',
                        'recommendation': 'Price action aligned with downtrend. Consider short positions or avoid longs.',
                        'icon': '⚠️'
                    })
            
            # MACD Analysis
            macd_data = indicators.get('macd')
            if isinstance(macd_data, dict):
                macd_line = macd_data.get('macd', 0)
                signal = macd_data.get('signal', 0)
                histogram = macd_data.get('histogram', 0)
                
                if histogram > 0 and macd_line > signal:
                    insights.append({
                        'priority': 'medium',
                        'type': 'opportunity',
                        'category': 'Trend',
                        'title': 'MACD Bullish Crossover',
                        'message': 'MACD line above signal with positive histogram',
                        'recommendation': 'Bullish momentum confirmed. Consider long positions.',
                        'icon': '🟢'
                    })
                elif histogram < 0 and macd_line < signal:
                    insights.append({
                        'priority': 'medium',
                        'type': 'warning',
                        'category': 'Trend',
                        'title': 'MACD Bearish Crossover',
                        'message': 'MACD line below signal with negative histogram',
                        'recommendation': 'Bearish momentum confirmed. Consider short positions or exit longs.',
                        'icon': '🔴'
                    })
            elif isinstance(macd_data, (int, float)) and macd_data > 0:
                insights.append({
                    'priority': 'low',
                    'type': 'info',
                    'category': 'Trend',
                    'title': 'Positive MACD',
                    'message': 'MACD indicates bullish momentum',
                    'recommendation': 'Monitor for trend continuation.',
                    'icon': '📊'
                })
            
            # Price vs Moving Averages
            ema_20 = indicators.get('ema_20')
            ema_50 = indicators.get('ema_50')
            
            if current_price and ema_20 and ema_50:
                if current_price > ema_20 > ema_50:
                    insights.append({
                        'priority': 'high',
                        'type': 'opportunity',
                        'category': 'Trend',
                        'title': 'Strong Uptrend',
                        'message': f'Price ({current_price:.2f}) > EMA 20 ({ema_20:.2f}) > EMA 50 ({ema_50:.2f})',
                        'recommendation': 'Bullish alignment suggests trend strength. Consider long positions.',
                        'icon': '🚀'
                    })
                elif current_price < ema_20 < ema_50:
                    insights.append({
                        'priority': 'high',
                        'type': 'warning',
                        'category': 'Trend',
                        'title': 'Strong Downtrend',
                        'message': f'Price ({current_price:.2f}) < EMA 20 ({ema_20:.2f}) < EMA 50 ({ema_50:.2f})',
                        'recommendation': 'Bearish alignment suggests trend weakness. Avoid longs or consider shorts.',
                        'icon': '📉'
                    })
            
            # VWAP Analysis
            vwap = indicators.get('vwap')
            if current_price and vwap:
                price_vs_vwap = ((current_price - vwap) / vwap) * 100
                if price_vs_vwap > 2:
                    insights.append({
                        'priority': 'medium',
                        'type': 'warning',
                        'category': 'Volume',
                        'title': 'Price Above VWAP',
                        'message': f'Price is {price_vs_vwap:.2f}% above VWAP ({vwap:.2f})',
                        'recommendation': 'Price may be overextended. Consider profit-taking near resistance.',
                        'icon': '⚠️'
                    })
                elif price_vs_vwap < -2:
                    insights.append({
                        'priority': 'medium',
                        'type': 'opportunity',
                        'category': 'Volume',
                        'title': 'Price Below VWAP',
                        'message': f'Price is {abs(price_vs_vwap):.2f}% below VWAP ({vwap:.2f})',
                        'recommendation': 'Price below VWAP suggests potential bounce. Watch for support.',
                        'icon': '💡'
                    })
            
            # === OPTIONS GREEKS INSIGHTS ===
            if greeks:
                delta = greeks.get('delta')
                gamma = greeks.get('gamma')
                theta = greeks.get('theta')
                vega = greeks.get('vega')
                
                if delta is not None:
                    if abs(delta) > 0.7:
                        insights.append({
                            'priority': 'high',
                            'type': 'info',
                            'category': 'Options',
                            'title': 'High Delta Option',
                            'message': f'Delta: {delta:.3f} - behaves like {abs(delta)*100:.0f} shares',
                            'recommendation': 'High directional exposure. Price moves directly with underlying.',
                            'icon': '🎯'
                        })
                    elif abs(delta) < 0.3:
                        insights.append({
                            'priority': 'medium',
                            'type': 'warning',
                            'category': 'Options',
                            'title': 'Low Delta Option',
                            'message': f'Delta: {delta:.3f} - limited price sensitivity',
                            'recommendation': 'Low directional exposure. May need larger underlying move for profit.',
                            'icon': '⚡'
                        })
                
                if theta is not None and theta < -0.05:
                    insights.append({
                        'priority': 'high',
                        'type': 'warning',
                        'category': 'Options',
                        'title': 'High Time Decay',
                        'message': f'Theta: {theta:.4f} - losing {abs(theta)*100:.2f}% value daily',
                        'recommendation': 'Time decay accelerating. Consider shorter holding period or exit.',
                        'icon': '⏰'
                    })
                
                if vega is not None and vega > 0.1:
                    insights.append({
                        'priority': 'medium',
                        'type': 'info',
                        'category': 'Options',
                        'title': 'High Volatility Sensitivity',
                        'message': f'Vega: {vega:.4f} - sensitive to IV changes',
                        'recommendation': 'IV changes will significantly impact option price. Monitor volatility.',
                        'icon': '📊'
                    })
            
            # === ALERT-SPECIFIC INSIGHTS ===
            if alert_data and entry_price and current_price:
                price_change_pct = ((current_price - entry_price) / entry_price) * 100
                
                if action == 'BUY':
                    if price_change_pct > 2:
                        insights.append({
                            'priority': 'high',
                            'type': 'success',
                            'category': 'Trade',
                            'title': 'Profit Target Approach',
                            'message': f'Price up {price_change_pct:.2f}% from entry ({entry_price:.2f})',
                            'recommendation': f'Consider taking partial profits. Target: {target:.2f}' if target else 'Monitor for profit-taking opportunity.',
                            'icon': '💰'
                        })
                    elif price_change_pct < -1:
                        insights.append({
                            'priority': 'high',
                            'type': 'warning',
                            'category': 'Trade',
                            'title': 'Stop Loss Risk',
                            'message': f'Price down {abs(price_change_pct):.2f}% from entry',
                            'recommendation': f'Monitor stop-loss: {stop_loss:.2f}' if stop_loss else 'Consider tightening stop-loss.',
                            'icon': '🛑'
                        })
                
                if stop_loss and current_price:
                    stop_loss_pct = ((current_price - stop_loss) / stop_loss) * 100
                    if stop_loss_pct < 2:
                        insights.append({
                            'priority': 'critical',
                            'type': 'danger',
                            'category': 'Risk',
                            'title': 'Near Stop Loss',
                            'message': f'Price ({current_price:.2f}) within 2% of stop-loss ({stop_loss:.2f})',
                            'recommendation': 'Stop-loss imminent. Consider exiting or tightening position.',
                            'icon': '🚨'
                        })
                
                if target and current_price:
                    target_pct = ((target - current_price) / current_price) * 100
                    if target_pct < 2:
                        insights.append({
                            'priority': 'high',
                            'type': 'success',
                            'category': 'Trade',
                            'title': 'Near Target',
                            'message': f'Price ({current_price:.2f}) within 2% of target ({target:.2f})',
                            'recommendation': 'Consider taking profits or trailing stop-loss.',
                            'icon': '🎯'
                        })
            
            # Add a general insight if we have limited data
            if not insights and not indicators and not greeks:
                insights.append({
                    'priority': 'medium',
                    'type': 'info',
                    'category': 'Data',
                    'title': 'Limited Data Available',
                    'message': f'No technical indicators or Greeks found for {symbol}',
                    'recommendation': 'Try selecting a different symbol or alert that has indicator data loaded.',
                    'icon': 'ℹ️'
                })
            
            # Confidence-based insight
            if confidence and confidence > 0:
                if confidence > 0.8:
                    insights.append({
                        'priority': 'medium',
                        'type': 'info',
                        'category': 'Signal',
                        'title': 'High Confidence Alert',
                        'message': f'Confidence score: {confidence:.0%}',
                        'recommendation': 'Strong signal quality. Monitor closely for entry/exit.',
                        'icon': '⭐'
                    })
                elif confidence < 0.5:
                    insights.append({
                        'priority': 'low',
                        'type': 'warning',
                        'category': 'Signal',
                        'title': 'Low Confidence Alert',
                        'message': f'Confidence score: {confidence:.0%}',
                        'recommendation': 'Lower signal quality. Exercise caution and use tight stop-loss.',
                        'icon': '⚠️'
                    })
                else:
                    insights.append({
                        'priority': 'medium',
                        'type': 'info',
                        'category': 'Signal',
                        'title': 'Moderate Confidence Alert',
                        'message': f'Confidence score: {confidence:.0%}',
                        'recommendation': 'Moderate signal quality. Monitor price action and use proper risk management.',
                        'icon': '📊'
                    })
            
            # Sort insights by priority
            priority_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
            insights.sort(key=lambda x: priority_order.get(x.get('priority', 'low'), 3))
            
        except Exception as e:
            logger.error(f"Error generating insights for {symbol}: {e}")
            insights.append({
                'priority': 'low',
                'type': 'info',
                'category': 'System',
                'title': 'Insights Unavailable',
                'message': f'Error generating insights: {str(e)}',
                'recommendation': 'Please try refreshing or selecting another symbol.',
                'icon': 'ℹ️'
            })
        
        return insights
    
    def _format_insights_html(self, insights):
        """Format insights list as HTML for display"""
        if not insights:
            return html.Div([
                dbc.Alert("No insights available for this symbol. Try selecting a different symbol or alert.", color="info", className="text-center")
            ], style={"padding": "20px"})
        
        insight_cards = []
        priority_colors = {
            'critical': 'danger',
            'high': 'warning',
            'medium': 'info',
            'low': 'secondary'
        }
        
        type_icons = {
            'warning': '⚠️',
            'danger': '🚨',
            'success': '✅',
            'opportunity': '💡',
            'info': 'ℹ️'
        }
        
        for insight in insights:
            priority = insight.get('priority', 'low')
            insight_type = insight.get('type', 'info')
            category = insight.get('category', 'General')
            title = insight.get('title', 'Insight')
            message = insight.get('message', '')
            recommendation = insight.get('recommendation', '')
            icon = insight.get('icon', type_icons.get(insight_type, 'ℹ️'))
            
            color = priority_colors.get(priority, 'secondary')
            
            card = dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.H5([
                            html.Span(icon, style={"margin-right": "10px"}),
                            title
                        ], className="mb-2"),
                        html.P(message, className="mb-2 text-muted"),
                        html.P([
                            html.Strong("Recommendation: "),
                            recommendation
                        ], className="mb-0")
                    ])
                ])
            ], className=f"mb-3 border-{color}", style={"border-left": f"4px solid"})
            
            insight_cards.append(card)
        
        return html.Div(insight_cards, style={"max-height": "600px", "overflow-y": "auto"})
    
    def _create_pattern_performance_chart(self, pattern_data):
        """Create pattern performance visualization"""
        try:
            if not pattern_data:
                return go.Figure(layout=go.Layout(title="No pattern data available", height=350))
            
            patterns = list(pattern_data.keys())
            success_rates = [pattern_data[p].get('success_rate', 0.0) for p in patterns]
            avg_confidences = [pattern_data[p].get('avg_confidence', 0.0) for p in patterns]
            totals = [pattern_data[p].get('total', 0) for p in patterns]
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Success rate bars
            fig.add_trace(go.Bar(
                x=patterns,
                y=success_rates,
                name='Success Rate',
                marker_color='lightgreen',
                text=[f"{sr:.1%}" for sr in success_rates],
                textposition='outside'
            ), secondary_y=False)
            
            # Average confidence line
            fig.add_trace(go.Scatter(
                x=patterns,
                y=avg_confidences,
                name='Avg Confidence',
                mode='lines+markers',
                line=dict(color='blue', width=2),
                marker=dict(size=8)
            ), secondary_y=True)
            
            fig.update_xaxes(title_text="Pattern Type")
            fig.update_yaxes(title_text="Success Rate", range=[0, 1], secondary_y=False)
            fig.update_yaxes(title_text="Confidence", range=[0, 1], secondary_y=True)
            
            fig.update_layout(
                title="Pattern Performance Analysis",
                height=400,
                hovermode='x unified',
                legend=dict(x=0.7, y=1)
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating pattern performance chart: {e}")
            return go.Figure(layout=go.Layout(title="Error loading chart", height=350))
    
    def _create_forward_validation_chart(self, forward_summary):
        """Create forward validation window performance chart"""
        try:
            window_perf = forward_summary.get('window_performance', {})
            if not window_perf:
                return go.Figure(layout=go.Layout(title="No forward validation data", height=350))
            
            windows = sorted([int(k) for k in window_perf.keys()])
            success_rates = []
            for w in windows:
                stats = window_perf[str(w)]
                total = stats.get('total', 1)
                success = stats.get('success', 0)
                success_rates.append(success / total if total > 0 else 0.0)
            
            fig = go.Figure(data=[go.Bar(
                x=[f"{w}m" for w in windows],
                y=success_rates,
                marker=dict(
                    color=success_rates,
                    colorscale='Greens',
                    showscale=True,
                    colorbar=dict(title="Success Rate")
                ),
                text=[f"{sr:.1%}" for sr in success_rates],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Success Rate: %{y:.1%}<extra></extra>'
            )])
            
            fig.update_layout(
                title=f"Forward Validation Performance (Avg: {forward_summary.get('avg_success_ratio', 0):.1%})",
                xaxis_title="Time Window",
                yaxis_title="Success Rate",
                height=350,
                hovermode='closest'
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating forward validation chart: {e}")
            return go.Figure(layout=go.Layout(title="Error loading chart", height=350))
    
    def _create_confidence_distribution_chart(self, df_alerts):
        """Create confidence distribution chart"""
        try:
            if df_alerts.empty or 'confidence' not in df_alerts.columns:
                return go.Figure(layout=go.Layout(
                    title="No confidence data available",
                    height=350
                ))
            
            # Categorize confidence levels
            df_alerts = df_alerts.copy()
            df_alerts['confidence_category'] = pd.cut(
                df_alerts['confidence'].fillna(0),
                bins=[0, 0.5, 0.7, 0.8, 0.9, 1.0],
                labels=['Low (0-50%)', 'Moderate (50-70%)', 'Good (70-80%)', 'High (80-90%)', 'Very High (90-100%)'],
                include_lowest=True
            )
            
            confidence_counts = df_alerts['confidence_category'].value_counts().sort_index()
            
            # Color map based on confidence level
            color_map = {
                'Low (0-50%)': 'red',
                'Moderate (50-70%)': 'orange',
                'Good (70-80%)': 'yellow',
                'High (80-90%)': 'lightgreen',
                'Very High (90-100%)': 'green'
            }
            
            colors = [color_map.get(cat, 'gray') for cat in confidence_counts.index]
            
            fig = go.Figure(data=[go.Bar(
                x=list(confidence_counts.index),
                y=list(confidence_counts.values),
                marker=dict(color=colors),
                text=[f"{v} alerts" for v in confidence_counts.values],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                customdata=[(v/len(df_alerts)*100) if len(df_alerts) > 0 else 0 for v in confidence_counts.values]
            )])
            
            fig.update_layout(
                title={
                    'text': f"Confidence Distribution (Avg: {df_alerts['confidence'].mean():.1%})",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title="Confidence Level",
                yaxis_title="Alert Count",
                height=350,
                hovermode='closest',
                margin=dict(l=50, r=30, t=60, b=80)
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating confidence distribution chart: {e}")
            return go.Figure(layout=go.Layout(title="Error loading chart", height=350))
    
    def _create_instrument_type_chart(self, df_alerts):
        """Create instrument type breakdown chart"""
        try:
            if df_alerts.empty or 'instrument_type' not in df_alerts.columns:
                return go.Figure(layout=go.Layout(
                    title="No instrument type data available",
                    height=350
                ))
            
            type_counts = df_alerts['instrument_type'].value_counts()
            
            # Color map for instrument types
            color_map = {
                'Equity': '#3498db',
                'Futures': '#e74c3c',
                'Options': '#2ecc71'
            }
            
            colors = [color_map.get(inst_type, '#95a5a6') for inst_type in type_counts.index]
            
            fig = go.Figure(data=[go.Bar(
                x=list(type_counts.index),
                y=list(type_counts.values),
                marker=dict(color=colors),
                text=[f"{v} alerts<br>{v/len(df_alerts)*100:.1f}%" for v in type_counts.values],
                textposition='outside',
                hovertemplate='<b>%{x}</b><br>Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                customdata=[(v/len(df_alerts)*100) if len(df_alerts) > 0 else 0 for v in type_counts.values]
            )])
            
            fig.update_layout(
                title={
                    'text': f"Instrument Type Breakdown ({len(type_counts)} types)",
                    'x': 0.5,
                    'xanchor': 'center'
                },
                xaxis_title="Instrument Type",
                yaxis_title="Alert Count",
                height=350,
                hovermode='closest',
                margin=dict(l=50, r=30, t=60, b=80)
            )
            
            return fig
        except Exception as e:
            logger.error(f"Error creating instrument type chart: {e}")
            return go.Figure(layout=go.Layout(title="Error loading chart", height=350))
    
    def create_dashboard(self):
        """Create the Dash dashboard"""
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        
        # Add health check endpoint for ngrok manager
        @app.server.route('/health')
        def health_check():
            """Health check endpoint for ngrok tunnel monitoring"""
            import json
            return json.dumps({
                'status': 'healthy',
                'port': 53056,
                'timestamp': datetime.now().isoformat()
            }), 200, {'Content-Type': 'application/json'}
        
        @app.server.route('/debug/indicators/<symbol>')
        def debug_indicators(symbol):
            """Debug endpoint to check indicator loading for a symbol."""
            debug_info = self.debug_indicators_loading(symbol)
            return json.dumps(debug_info, indent=2), 200, {'Content-Type': 'application/json'}
        
        @app.server.route('/debug/redis')
        def debug_redis():
            """Debug Redis connections."""
            import time
            status = {
                'db1_connected': self.redis_db1.ping(),
                'db4_connected': getattr(self, 'redis_db4', None) and self.redis_db4.ping(),
                'db5_connected': getattr(self, 'redis_db5', None) and self.redis_db5.ping(),
                'stream_length': self.redis_db1.xlen("alerts:stream"),
                'timestamp': time.time()
            }
            return json.dumps(status, indent=2), 200, {'Content-Type': 'application/json'}
        
        # Convert to DataFrames using helper method
        df_alerts = self.process_alerts_data()
        
        # Debug: Check DataFrame columns and sample data
        print(f"📊 DataFrame created with {len(df_alerts)} alerts")
        print(f"📋 DataFrame columns: {list(df_alerts.columns)}")
        print("🔍 [DEBUG] Starting Dash app and callback registration...")
        print(f"🔍 [DEBUG] DataFrame shape: {df_alerts.shape}, columns: {list(df_alerts.columns)[:10]}")
        if len(df_alerts) > 0:
            # Check for Greeks columns
            greek_cols = ['delta', 'gamma', 'theta', 'vega', 'rho']
            greeks_found = {col: df_alerts[col].notna().sum() for col in greek_cols if col in df_alerts.columns}
            print(f"📊 Greeks columns in DataFrame: {greeks_found}")
            # Show sample of Greeks for first option if any
            options = df_alerts[df_alerts['instrument_type'] == 'Option'] if 'instrument_type' in df_alerts.columns else df_alerts.head(0)
            if len(options) > 0:
                first_option = options.iloc[0]
                print(f"📊 Sample option Greeks - Symbol: {first_option.get('symbol')}, Delta: {first_option.get('delta')}, Gamma: {first_option.get('gamma')}, Theta: {first_option.get('theta')}, Vega: {first_option.get('vega')}, Rho: {first_option.get('rho')}")
            # Check indicators
            indicator_cols = ['rsi', 'macd', 'ema_20', 'ema_50', 'vwap', 'atr']
            indicators_found = {col: df_alerts[col].notna().sum() for col in indicator_cols if col in df_alerts.columns}
            print(f"📊 Indicators columns in DataFrame: {indicators_found}")
        df_prices = pd.DataFrame(self.price_data)
        
        # DEBUG: Verify indicator columns exist and have data
        if not df_alerts.empty:
            print(f"\n📊 DataFrame Indicator Summary:")
            for col in indicator_cols:
                if col in df_alerts.columns:
                    non_null_count = df_alerts[col].notna().sum()
                    total_count = len(df_alerts)
                    if non_null_count > 0:
                        sample_val = df_alerts[col].dropna().iloc[0] if non_null_count > 0 else None
                        print(f"  ✅ {col}: {non_null_count}/{total_count} non-null values (sample: {sample_val})")
                    else:
                        print(f"  ❌ {col}: 0/{total_count} non-null values")
                else:
                    print(f"  ⚠️ {col}: Column missing from DataFrame!")
            
            options_df = df_alerts[df_alerts['instrument_type'] == 'Options']
            if not options_df.empty:
                print(f"\n📊 Greeks Summary for {len(options_df)} options:")
                for col in greek_cols:
                    if col in options_df.columns:
                        non_null_count = options_df[col].notna().sum()
                        if non_null_count > 0:
                            print(f"  ✅ {col}: {non_null_count}/{len(options_df)} non-null values")
                        else:
                            print(f"  ❌ {col}: 0/{len(options_df)} non-null values")
        
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
        
        # ENHANCED: Create default views
        default_symbol = None
        default_pattern = None
        
        # Try to find the most active symbol today
        if not df_alerts.empty:
            symbol_counts_for_default = df_alerts['symbol'].value_counts()
            if len(symbol_counts_for_default) > 0:
                default_symbol = symbol_counts_for_default.index[0]
        
        # If no alerts, use the first intraday instrument
        if not default_symbol and self.intraday_instruments:
            default_symbol = sorted(self.intraday_instruments)[0]

        pattern_option_keys = []
        if 'pattern_key' in df_alerts.columns:
            pattern_option_keys = sorted(df_alerts['pattern_key'].dropna().unique())
        pattern_option_keys = sorted(set(pattern_option_keys) | set(self.all_patterns))
        pattern_dropdown_options = [
            {
                'label': self.pattern_labels.get(key, pattern_display_label(key)),
                'value': key
            }
            for key in pattern_option_keys
        ]
        if default_pattern and default_pattern not in {opt['value'] for opt in pattern_dropdown_options}:
            default_pattern = None

        print("🔍 [DEBUG] Creating app.layout...")
        try:
            print("🔍 [DEBUG] Variables: total_alerts={}, unique_symbols={}, avg_confidence={}".format(total_alerts, unique_symbols, avg_confidence))
            app.layout = dbc.Container(children=[
            dbc.Row([
                dbc.Col([
                    html.H1("AION LABS INTRADAY SIGNALS", className="text-center mb-4", style={"font-weight": "bold"}),
                    html.P("Real-time trading alerts with technical analysis", className="text-center text-muted"),
                    html.Div([
                        dbc.Alert([
                            html.H5("💡 How to use this dashboard:", className="mb-2"),
                            html.Ul([
                                html.Li("Select a symbol to view its price chart and technical indicators"),
                                html.Li("Select a pattern to filter alerts by strategy type"), 
                                html.Li("Click on any alert to see detailed analysis and news"),
                                html.Li("Charts show 20-day moving averages and key technical levels")
                            ])
                        ], color="info")
                    ], className="mb-3")
                ])
            ]),
            
            # Market Indices Section (VIX, NIFTY 50, BANKNIFTY, GIFT NIFTY) - Auto-refreshes every 30 seconds
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
            
            # ENHANCED: Controls with better defaults
            dbc.Row([
                dbc.Col([
                    html.Label("📈 Select Symbol:"),
                    dcc.Dropdown(
                        id='symbol-dropdown',
                        # CRITICAL FIX: Don't call get_instrument_name() during layout creation (blocking!)
                        # Build options lazily or use symbol only - instrument names loaded via callback
                        options=[{'label': sym, 'value': sym} 
                                for sym in sorted(self.intraday_instruments)],
                        value=default_symbol,
                        clearable=False,
                        placeholder="Choose a symbol to analyze..."
                    )
                ], width=4),
                dbc.Col([
                    html.Label("🎯 Select Pattern:"),
                    dcc.Dropdown(
                        id='pattern-dropdown',
                        options=pattern_dropdown_options,
                        value=default_pattern,
                        clearable=True,
                        placeholder="All Patterns"
                    )
                ], width=4),
                dbc.Col([
                    html.Label("🔔 Select Alert:"),
                    dcc.Dropdown(
                        id='alert-dropdown',
                        clearable=True,
                        placeholder="No alert selected - showing symbol chart"
                    )
                ], width=4),
            ], className="mb-4"),
            
            # ENHANCED: Price Chart with better default behavior
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H4("📊 Price Analysis", className="mb-0"),
                            html.Small(id='chart-title', children="Select a symbol to view price chart")
                        ], style={"background-color": "#fff3cd", "border-bottom": "2px solid #ffc107"}),
                        dbc.CardBody([
                            dcc.Graph(id='price-chart'),
                            dbc.Row([
                                dbc.Col([
                                    html.Label("📊 Visualization Type:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                                    dcc.Dropdown(
                                        id='chart-type-selector',
                                        options=[
                                            {'label': '📊 Time Series (Candlestick)', 'value': 'time_series'},
                                            {'label': '📈 Line Chart', 'value': 'line_chart'},
                                            {'label': '📉 2D Histogram (Price-Volume)', 'value': 'histogram_2d'},
                                            {'label': '🔥 Volume Profile Heatmap', 'value': 'volume_heatmap'},
                                            {'label': '📊 Volume Distribution Histogram', 'value': 'volume_distribution'},
                                            {'label': '🌡️ Price-Volume Scatter', 'value': 'scatter_2d'},
                                            {'label': '📅 OHLC Bars (Multi-timeframe)', 'value': 'ohlc_bars'},
                                        ],
                                        value='time_series',
                                        clearable=False,
                                        style={'fontSize': '13px'},
                                        className="mt-1"
                                    )
                                ], width=6),
                                dbc.Col([
                                    html.Label("📡 Data Source:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                                    dcc.Dropdown(
                                        id='data-source-selector',
                                        options=[
                                            {'label': '🔄 Real-time (Streams)', 'value': 'realtime'},
                                            {'label': '📚 Historical (All Data)', 'value': 'historical'},
                                            {'label': '📊 Daily OHLC (Sorted Sets)', 'value': 'daily_ohlc'},
                                            {'label': '🪣 Bucket Data (Time Buckets)', 'value': 'buckets'},
                                        ],
                                        value='historical',
                                        clearable=False,
                                        style={'fontSize': '13px'},
                                        className="mt-1"
                                    )
                                ], width=6)
                            ], className="mb-2"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Switch(
                                        id='show-indicators',
                                        label="Show Technical Indicators",
                                        value=True,  # Default to ON
                                        className="mt-2"
                                    ),
                                    html.Small("(Toggle ON to see VWAP, SMA, EMA)", className="text-muted d-block mt-1")
                                ], width=6),
                                dbc.Col([
                                    dbc.Switch(
                                        id='show-candlesticks',
                                        label="Candlestick View",
                                        value=True,
                                        className="mt-2"
                                    )
                                ], width=6)
                            ])
                        ])
                    ], className="border-warning")
                ], width=12)
            ], className="mb-4"),
            
            # Actionable Insights Section
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H4("💡 Actionable Insights", className="mb-0"),
                            html.Small("Real-time trading recommendations based on indicators, Greeks, and price action")
                        ], style={"background-color": "#d1ecf1", "border-bottom": "2px solid #0c5460"}),
                        dbc.CardBody([
                            html.Div(
                                id='insights-display',
                                children=html.Div([
                                    dbc.Alert("Select a symbol or alert to view actionable insights", color="info", className="text-center")
                                ], style={"padding": "20px"})
                            )
                        ])
                    ], className="border-info mb-4")
                ], width=12)
            ]),
            
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
                    html.H4("Recent Alerts"),
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
                            # Technical Indicators (for equity/futures) - CRITICAL: Visible by default so users can track patterns/instruments
                            {"name": "RSI", "id": "rsi", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "MACD", "id": "macd", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "EMA 20", "id": "ema_20", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "EMA 50", "id": "ema_50", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "ATR", "id": "atr", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "VWAP", "id": "vwap", "type": "numeric", "format": {"specifier": ".2f"}},
                            {"name": "Volume Ratio", "id": "volume_ratio", "type": "numeric", "format": {"specifier": ".2f"}},
                            # Greeks (for options) - CRITICAL: Visible by default so users can track patterns/instruments
                            {"name": "Delta", "id": "delta", "type": "numeric", "format": {"specifier": ".4f"}},
                            {"name": "Gamma", "id": "gamma", "type": "numeric", "format": {"specifier": ".4f"}},
                            {"name": "Theta", "id": "theta", "type": "numeric", "format": {"specifier": ".4f"}},
                            {"name": "Vega", "id": "vega", "type": "numeric", "format": {"specifier": ".4f"}},
                            {"name": "Rho", "id": "rho", "type": "numeric", "format": {"specifier": ".4f"}},
                        ],
                        data=df_alerts.to_dict('records') if not df_alerts.empty else [],
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
                    ),
                    # Interval component to poll for new alerts (every 3 seconds for real-time updates)
                    dcc.Interval(
                        id='alerts-refresh-interval',
                        interval=3*1000,  # Check every 3 seconds for faster updates
                        n_intervals=0
                    )
                ], width=12)
            ], className="mb-4"),
            
            # ENHANCED: Pattern Distribution + Top Symbols with better styling and insights
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("📊 Pattern Distribution", className="mb-0"),
                            html.Small("Click a pattern to filter alerts", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='pattern-distribution-chart',
                                figure=go.Figure(
                                    data=[go.Bar(
                                        x=list(pattern_counts.index),
                                        y=list(pattern_counts.values),
                                        marker=dict(
                                            color=list(pattern_counts.values),
                                            colorscale='Blues',
                                            showscale=True,
                                            colorbar=dict(title="Count")
                                        ),
                                        text=[f"{v} alerts<br>{v/total_alerts*100:.1f}%" if total_alerts > 0 else f"{v} alerts" 
                                              for v in pattern_counts.values],
                                        textposition='outside',
                                        hovertemplate='<b>%{x}</b><br>Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                                        customdata=[(v/total_alerts*100) if total_alerts > 0 else 0 for v in pattern_counts.values]
                                    )],
                                    layout=go.Layout(
                                        title={
                                            'text': f"Pattern Distribution ({len(pattern_counts)} patterns)",
                                            'x': 0.5,
                                            'xanchor': 'center'
                                        },
                                        xaxis=dict(
                                            title="Pattern Type",
                                            tickangle=-45,
                                            type='category'
                                        ),
                                        yaxis_title="Alert Count",
                                        height=350,
                                        hovermode='closest',
                                        showlegend=False,
                                        margin=dict(l=50, r=30, t=60, b=100)
                                    )
                                ),
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("🏆 Top 10 Symbols by Alert Count", className="mb-0"),
                            html.Small("Most active symbols today", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='symbol-count-chart',
                                figure=go.Figure(
                                    data=[go.Bar(
                                        x=list(symbol_counts.index),
                                        y=list(symbol_counts.values),
                                        marker=dict(
                                            color='lightblue',
                                            line=dict(color='darkblue', width=1.5)
                                        ),
                                        text=[f"{v} alert{'s' if v > 1 else ''}" for v in symbol_counts.values],
                                        textposition='outside',
                                        hovertemplate='<b>%{x}</b><br>Alert Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                                        customdata=[(v/total_alerts*100) if total_alerts > 0 else 0 for v in symbol_counts.values]
                                    )],
                                    layout=go.Layout(
                                        title={
                                            'text': f"Top 10 Symbols ({symbol_counts.sum()} alerts)",
                                            'x': 0.5,
                                            'xanchor': 'center'
                                        },
                                        xaxis=dict(
                                            title="Symbol",
                                            tickangle=-45,
                                            type='category'
                                        ),
                                        yaxis_title="Alert Count",
                                        height=350,
                                        hovermode='closest',
                                        showlegend=False,
                                        margin=dict(l=50, r=30, t=60, b=100)
                                    )
                                ),
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=6)
            ], className="mb-4"),
            
            # NEW: Additional Analytics Charts
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("📈 Confidence Distribution", className="mb-0"),
                            html.Small("Alert quality breakdown", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='confidence-distribution-chart',
                                figure=self._create_confidence_distribution_chart(df_alerts),
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("📦 Instrument Type Breakdown", className="mb-0"),
                            html.Small("Equity vs Options vs Futures", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='instrument-type-chart',
                                figure=self._create_instrument_type_chart(df_alerts),
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=6)
            ], className="mb-4"),
            
            # NEW: Statistical Model Section
            html.Hr(),
            dbc.Row([
                dbc.Col([
                    html.H2("📊 Statistical Model Analysis", className="text-center mb-4"),
                    html.P("Real-time validation performance and pattern accuracy metrics", 
                           className="text-center text-muted mb-4")
                ], width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("📈 Validation Performance Overview", className="mb-0"),
                            html.Small("Real-time validation statistics", className="text-muted")
                        ]),
                        dbc.CardBody([
                            html.Div(id='validation-stats-display'),
                            # Auto-refresh interval component (hidden, triggers every 5 seconds)
                            dcc.Interval(
                                id='interval-component',
                                interval=5*1000,  # 5 seconds in milliseconds
                                n_intervals=0
                            )
                        ])
                    ], className="mb-4")
                ], width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("🎯 Pattern Performance Analysis", className="mb-0"),
                            html.Small("Success rates and confidence by pattern type", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='pattern-performance-chart',
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=12)
            ]),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.H5("⏩ Forward Validation Performance", className="mb-0"),
                            html.Small("Time-window success rates", className="text-muted")
                        ]),
                        dbc.CardBody([
                            dcc.Graph(
                                id='forward-validation-chart',
                                config={'displayModeBar': False}
                            )
                        ])
                    ], className="mb-4")
                ], width=12)
            ])
        ], fluid=True)
            print("🔍 [DEBUG] Layout created successfully!")
        except Exception as layout_error:
            print(f"❌ [DEBUG] ERROR creating layout: {layout_error}")
            import traceback
            traceback.print_exc()
            raise
        
        # ENHANCED CALLBACKS
        
        
        @app.callback(
            Output('chart-title', 'children'),
            Input('symbol-dropdown', 'value'),
            Input('alert-dropdown', 'value')
        )
        def update_chart_title(selected_symbol, selected_alert):
            try:
                print(f"🔍 [DEBUG] update_chart_title: symbol={selected_symbol}, alert={selected_alert}")
                if selected_alert and selected_alert != 'no_alerts':
                    title = f"Alert Analysis - {selected_symbol}"
                elif selected_symbol:
                    title = f"Price Chart - {selected_symbol}"
                else:
                    title = "Select a symbol to view price chart"
                print(f"🔍 [DEBUG] Chart title: {title}")
                return title
            except Exception as e:
                print(f"❌ [DEBUG] Error in update_chart_title: {e}")
                return "Select a symbol to view price chart"
        
        @app.callback(
            Output('alert-table', 'data'),
            Input('alerts-refresh-interval', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_alerts_table(n_intervals):
            """Update alerts table periodically and when new alerts are detected"""
            try:
                # Get current alerts_data (thread-safe)
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                    # Get alert count for debugging
                    alert_count = len(current_alerts)
                
                # Check for new alerts
                with self.new_alerts_count_lock:
                    new_count = self.new_alerts_count
                    # Reset counter after checking (so we can detect future changes)
                    if new_count > 0:
                        self.new_alerts_count = 0
                        print(f"🔄 Refreshing alerts table: {alert_count} total alerts (detected {new_count} new)")
                
                # Convert to DataFrame and format
                if not current_alerts:
                    if n_intervals > 0:  # Only log after initial load
                        print(f"📊 No alerts to display (refresh #{n_intervals})")
                    return []
                
                df_alerts = pd.DataFrame(current_alerts)
                
                # ✅ FIX: Ensure validation columns exist (they might not be in all alerts)
                validation_cols = ['is_valid', 'validation_score', 'reasons']
                for col in validation_cols:
                    if col not in df_alerts.columns:
                        df_alerts[col] = None
                
                # Replace NaN with None for Dash DataTable compatibility
                indicator_cols = ['rsi', 'macd', 'ema_20', 'ema_50', 'atr', 'vwap', 'volume_ratio']
                greek_cols = ['delta', 'gamma', 'theta', 'vega', 'rho']
                all_numeric_cols = indicator_cols + greek_cols + validation_cols
                
                for col in all_numeric_cols:
                    if col in df_alerts.columns:
                        df_alerts[col] = df_alerts[col].where(pd.notna(df_alerts[col]), None)
                
                # Sort by timestamp (most recent first)
                if 'timestamp' in df_alerts.columns:
                    df_alerts = df_alerts.sort_values('timestamp', ascending=False, na_position='last')
                
                return df_alerts.replace({pd.NA: None, pd.NaT: None}).to_dict('records')
            except Exception as e:
                logger.error(f"Error updating alerts table: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                # Return current data on error
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                if current_alerts:
                    df_alerts = pd.DataFrame(current_alerts)
                    return df_alerts.replace({pd.NA: None, pd.NaT: None}).to_dict('records')
                return []
        
        # ✅ WIRED: Chart update callbacks - update all charts when alerts refresh
        @app.callback(
            Output('pattern-distribution-chart', 'figure'),
            Input('alerts-refresh-interval', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_pattern_distribution_chart(n_intervals):
            """Update pattern distribution chart when alerts refresh"""
            try:
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                
                if not current_alerts:
                    return go.Figure(layout=go.Layout(title="No alerts available"))
                
                df_alerts = pd.DataFrame(current_alerts)
                if 'pattern' not in df_alerts.columns or df_alerts.empty:
                    return go.Figure(layout=go.Layout(title="No pattern data available"))
                
                pattern_counts = df_alerts['pattern'].value_counts().head(10)
                total_alerts = len(df_alerts)
                
                return go.Figure(
                    data=[go.Bar(
                        x=list(pattern_counts.index),
                        y=list(pattern_counts.values),
                        marker=dict(
                            color=list(pattern_counts.values),
                            colorscale='Blues',
                            showscale=True,
                            colorbar=dict(title="Count")
                        ),
                        text=[f"{v} alerts<br>{v/total_alerts*100:.1f}%" if total_alerts > 0 else f"{v} alerts" 
                              for v in pattern_counts.values],
                        textposition='outside',
                        hovertemplate='<b>%{x}</b><br>Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                        customdata=[(v/total_alerts*100) if total_alerts > 0 else 0 for v in pattern_counts.values]
                    )],
                    layout=go.Layout(
                        title={
                            'text': f"Pattern Distribution ({len(pattern_counts)} patterns)",
                            'x': 0.5,
                            'xanchor': 'center'
                        },
                        xaxis=dict(
                            title="Pattern Type",
                            tickangle=-45,
                            type='category'
                        ),
                        yaxis_title="Alert Count",
                        height=350,
                        hovermode='closest',
                        showlegend=False,
                        margin=dict(l=50, r=30, t=60, b=100)
                    )
                )
            except Exception as e:
                logger.error(f"Error updating pattern distribution chart: {e}")
                return go.Figure(layout=go.Layout(title=f"Error: {str(e)}"))
        
        @app.callback(
            Output('symbol-count-chart', 'figure'),
            Input('alerts-refresh-interval', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_symbol_count_chart(n_intervals):
            """Update top 10 symbols chart when alerts refresh"""
            try:
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                
                if not current_alerts:
                    return go.Figure(layout=go.Layout(title="No alerts available"))
                
                df_alerts = pd.DataFrame(current_alerts)
                if 'symbol' not in df_alerts.columns or df_alerts.empty:
                    return go.Figure(layout=go.Layout(title="No symbol data available"))
                
                symbol_counts = df_alerts['symbol'].value_counts().head(10)
                total_alerts = len(df_alerts)
                
                return go.Figure(
                    data=[go.Bar(
                        x=list(symbol_counts.index),
                        y=list(symbol_counts.values),
                        marker=dict(
                            color='lightblue',
                            line=dict(color='darkblue', width=1.5)
                        ),
                        text=[f"{v} alert{'s' if v > 1 else ''}" for v in symbol_counts.values],
                        textposition='outside',
                        hovertemplate='<b>%{x}</b><br>Alert Count: %{y}<br>Percentage: %{customdata:.1f}%<extra></extra>',
                        customdata=[(v/total_alerts*100) if total_alerts > 0 else 0 for v in symbol_counts.values]
                    )],
                    layout=go.Layout(
                        title={
                            'text': f"Top 10 Symbols ({symbol_counts.sum()} alerts)",
                            'x': 0.5,
                            'xanchor': 'center'
                        },
                        xaxis=dict(
                            title="Symbol",
                            tickangle=-45,
                            type='category'
                        ),
                        yaxis_title="Alert Count",
                        height=350,
                        hovermode='closest',
                        showlegend=False,
                        margin=dict(l=50, r=30, t=60, b=100)
                    )
                )
            except Exception as e:
                logger.error(f"Error updating symbol count chart: {e}")
                return go.Figure(layout=go.Layout(title=f"Error: {str(e)}"))
        
        @app.callback(
            Output('confidence-distribution-chart', 'figure'),
            Input('alerts-refresh-interval', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_confidence_distribution_chart(n_intervals):
            """Update confidence distribution chart when alerts refresh"""
            try:
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                
                if not current_alerts:
                    return self._create_confidence_distribution_chart(pd.DataFrame())
                
                df_alerts = pd.DataFrame(current_alerts)
                return self._create_confidence_distribution_chart(df_alerts)
            except Exception as e:
                logger.error(f"Error updating confidence distribution chart: {e}")
                return go.Figure(layout=go.Layout(title=f"Error: {str(e)}"))
        
        @app.callback(
            Output('instrument-type-chart', 'figure'),
            Input('alerts-refresh-interval', 'n_intervals'),
            prevent_initial_call=False
        )
        def update_instrument_type_chart(n_intervals):
            """Update instrument type breakdown chart when alerts refresh"""
            try:
                with self.alerts_data_lock:
                    current_alerts = self.alerts_data.copy()
                
                if not current_alerts:
                    return self._create_instrument_type_chart(pd.DataFrame())
                
                df_alerts = pd.DataFrame(current_alerts)
                return self._create_instrument_type_chart(df_alerts)
            except Exception as e:
                logger.error(f"Error updating instrument type chart: {e}")
                return go.Figure(layout=go.Layout(title=f"Error: {str(e)}"))
        
        @app.callback(
            Output('alert-dropdown', 'options'),
            Output('alert-dropdown', 'value'),
            Input('symbol-dropdown', 'value'),
            Input('pattern-dropdown', 'value')
        )
        def update_alert_dropdown(selected_symbol, selected_pattern):
            try:
                print(f"🔍 [DEBUG] update_alert_dropdown: symbol={selected_symbol}, pattern={selected_pattern}")
                if selected_symbol is None:
                    print(f"🔍 [DEBUG] No symbol selected, returning empty options")
                    return [], None
                    
                # Filter alerts for the selected symbol and pattern
                filtered_alerts = df_alerts[df_alerts['symbol'] == selected_symbol]
                print(f"🔍 [DEBUG] Found {len(filtered_alerts)} alerts for symbol {selected_symbol}")
                
                if selected_pattern:
                    if 'pattern_key' in filtered_alerts.columns:
                        filtered_alerts = filtered_alerts[filtered_alerts['pattern_key'] == selected_pattern]
                    else:
                        filtered_alerts = filtered_alerts[filtered_alerts['pattern'] == selected_pattern]
                    print(f"🔍 [DEBUG] After pattern filter: {len(filtered_alerts)} alerts")
                
                if filtered_alerts.empty:
                    print(f"🔍 [DEBUG] No alerts found, returning no_alerts option")
                    return [{'label': 'No alerts found for this symbol/pattern', 'value': 'no_alerts'}], None
                
                options = []
                for idx, alert in filtered_alerts.iterrows():
                    action_icon = "🟢" if alert.get('action') == 'BUY' else "🔴" if alert.get('action') == 'SELL' else "🟡"
                    label = f"{action_icon} {alert['time_of_day']} - {alert['pattern']} - ₹{alert['entry_price']:.2f}"
                    options.append({'label': label, 'value': idx})
                
                # Auto-select the most recent alert
                default_value = options[0]['value'] if options else None
                print(f"🔍 [DEBUG] Returning {len(options)} options, default={default_value}")
                
                return options, default_value
            except Exception as e:
                print(f"❌ [DEBUG] Error in update_alert_dropdown: {e}")
                import traceback
                traceback.print_exc()
                return [], None
        
        @app.callback(
            Output('price-chart', 'figure'),
            Output('insights-display', 'children'),
            Input('symbol-dropdown', 'value'),
            Input('alert-dropdown', 'value'),
            Input('show-indicators', 'value'),
            Input('show-candlesticks', 'value'),
            Input('chart-type-selector', 'value'),
            Input('data-source-selector', 'value')
        )
        def update_price_chart(selected_symbol, selected_alert, show_indicators, show_candlesticks, chart_type, data_source):
            try:
                print(f"🔍 [DEBUG] update_price_chart called: symbol={selected_symbol}, alert={selected_alert}, indicators={show_indicators}, candlesticks={show_candlesticks}")
                
                # Handle toggle values - ensure boolean conversion
                # Dash Switch returns True/False, but be explicit
                show_indicators = bool(show_indicators) if show_indicators is not None else True
                print(f"🔍 [DEBUG] show_indicators final value: {show_indicators} (type: {type(show_indicators)})")
                    
                show_candlesticks = bool(show_candlesticks) if show_candlesticks is not None else True
                print(f"🔍 [DEBUG] show_candlesticks final value: {show_candlesticks}")
                
                # If no symbol selected, return empty figure
                if selected_symbol is None:
                    print(f"🔍 [DEBUG] No symbol selected, returning empty figure")
                    empty_insights = html.Div([
                        dbc.Alert("Please select a symbol to view insights", color="info", className="text-center")
                    ])
                    return go.Figure(), empty_insights
                
                print(f"🔍 [DEBUG] Processing chart for symbol: {selected_symbol}")
                
                # Get alert details if an alert is selected
                alert_time = None
                entry_price = None
                
                if selected_alert and selected_alert != 'no_alerts':
                    print(f"🔍 [DEBUG] Alert selected: {selected_alert}")
                    try:
                        if selected_alert in df_alerts.index:
                            alert = df_alerts.loc[selected_alert]
                            alert_time = alert.get('alert_time')
                            entry_price = float(alert.get('entry_price', 0))
                            print(f"🔍 [DEBUG] Alert data: time={alert_time}, price={entry_price}")
                            
                            # Convert alert_time if it's a string
                            if isinstance(alert_time, str):
                                try:
                                    if 'Z' in alert_time:
                                        alert_time = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                                    else:
                                        alert_time = datetime.fromisoformat(alert_time)
                                    print(f"🔍 [DEBUG] Parsed alert_time: {alert_time}")
                                except Exception as e:
                                    print(f"🔍 [DEBUG] Failed to parse alert_time: {e}")
                                    alert_time = None
                        else:
                            print(f"⚠️ [DEBUG] Alert index {selected_alert} not found in df_alerts")
                    except Exception as e:
                        print(f"🔍 [DEBUG] Error getting alert data: {e}")
                        pass
                
                # Get alert data for insights if alert is selected
                alert_data = None
                if selected_alert and selected_alert != 'no_alerts':
                    try:
                        if selected_alert in df_alerts.index:
                            alert_data = df_alerts.loc[selected_alert].to_dict()
                    except:
                        pass
                
                # Load data based on data source selector FIRST
                if data_source == 'realtime':
                    # Load from real-time streams
                    symbol_price_data = self.load_realtime_data(selected_symbol)
                elif data_source == 'daily_ohlc':
                    # Load from daily OHLC sorted sets only
                    symbol_price_data = self.load_daily_ohlc_data(selected_symbol)
                elif data_source == 'buckets':
                    # Load from bucket data
                    symbol_price_data = self.load_bucket_data(selected_symbol)
                else:
                    # Historical (default) - load from all sources
                    symbol_price_data = [p for p in self.price_data if p['symbol'] == selected_symbol]
                    if not symbol_price_data:
                        symbol_price_data = self.load_recent_ohlc_data(selected_symbol)
                
                # Generate actionable insights using the loaded data
                insights = self.generate_actionable_insights(
                    selected_symbol,
                    alert_data=alert_data,
                    price_data=symbol_price_data
                )
                insights_html = self._format_insights_html(insights)
                
                # Temporarily override price_data for chart creation
                original_price_data = self.price_data.copy()
                self.price_data = symbol_price_data if symbol_price_data else self.price_data
                
                try:
                    # Create the appropriate chart based on chart type selector
                    print(f"🔍 [DEBUG] Creating chart: type={chart_type}, source={data_source}, candlesticks={show_candlesticks}, indicators={show_indicators}")
                    
                    if chart_type == 'time_series':
                        # Time series defaults to candlestick view
                        if show_candlesticks:
                            print(f"🔍 [DEBUG] Creating time series candlestick chart...")
                            chart = self.create_candlestick_chart(selected_symbol, alert_time, entry_price, show_indicators)
                        else:
                            print(f"🔍 [DEBUG] Creating time series line chart...")
                            chart = self.create_line_chart(selected_symbol, alert_time, entry_price, show_indicators)
                    elif chart_type == 'histogram_2d':
                        chart = self.create_price_volume_2d_histogram(selected_symbol, alert_time, entry_price)
                    elif chart_type == 'volume_heatmap':
                        chart = self.create_volume_profile_heatmap(selected_symbol, alert_time, entry_price)
                    elif chart_type == 'volume_distribution':
                        chart = self.create_volume_distribution_histogram(selected_symbol, alert_time, entry_price)
                    elif chart_type == 'scatter_2d':
                        chart = self.create_price_volume_scatter(selected_symbol, alert_time, entry_price)
                    elif chart_type == 'ohlc_bars':
                        chart = self.create_ohlc_bars_chart(selected_symbol, alert_time, entry_price, show_indicators)
                    elif chart_type == 'line_chart':
                        print(f"🔍 [DEBUG] Calling create_line_chart...")
                        chart = self.create_line_chart(selected_symbol, alert_time, entry_price, show_indicators)
                    elif show_candlesticks:
                        print(f"🔍 [DEBUG] Calling create_candlestick_chart...")
                        chart = self.create_candlestick_chart(selected_symbol, alert_time, entry_price, show_indicators)
                    else:
                        print(f"🔍 [DEBUG] Calling create_line_chart...")
                        chart = self.create_line_chart(selected_symbol, alert_time, entry_price, show_indicators)
                finally:
                    # Restore original price_data
                    self.price_data = original_price_data
                
                print(f"🔍 [DEBUG] Chart created: type={type(chart)}, data_points={len(chart.data) if hasattr(chart, 'data') else 'N/A'}")
                
                # Ensure we return a valid Figure object
                if chart is None:
                    print(f"⚠️ [DEBUG] Chart is None, returning empty figure")
                    return go.Figure(), insights_html
                
                print(f"✅ [DEBUG] Returning chart successfully")
                return chart, insights_html
                
            except Exception as e:
                print(f"❌ [DEBUG] Exception in update_price_chart: {e}")
                logger.error(f"Error in update_price_chart callback: {e}")
                import traceback
                traceback.print_exc()
                logger.error(traceback.format_exc())
                error_insights = html.Div([
                    dbc.Alert([
                        html.Strong("Error: "), str(e)
                    ], color="danger")
                ])
                return go.Figure(), error_insights
        
        @app.callback(
            Output('market-indices-display', 'children'),
            Input('indices-interval', 'n_intervals')
        )
        def update_market_indices(n):
            """Fetch and display VIX, NIFTY 50, BANKNIFTY, GIFT NIFTY from Redis (refreshes every 30s)"""
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
                
                # Fetch GIFT NIFTY
                giftnifty_keys = [
                    "index:NSEIX:GIFT NIFTY",
                    "market_data:indices:nseix_gift_nifty",
                    "index_data:giftnifty",
                    "index_data:gift_nifty",
                    "index_data:sgx_nifty"
                ]
                giftnifty_value = None
                giftnifty_change = None
                giftnifty_change_pct = None
                for key in giftnifty_keys:
                    try:
                        giftnifty_data = self.redis_db1.get(key)
                        if giftnifty_data:
                            # Handle bytes, string, and dict formats
                            if isinstance(giftnifty_data, bytes):
                                giftnifty_json = json.loads(giftnifty_data.decode('utf-8'))
                            elif isinstance(giftnifty_data, str):
                                giftnifty_json = json.loads(giftnifty_data)
                            else:
                                giftnifty_json = giftnifty_data
                            
                            giftnifty_value = giftnifty_json.get('last_price') or giftnifty_json.get('value') or giftnifty_json.get('close')
                            # Handle both 'net_change'/'change' and 'percent_change'/'change_pct'
                            giftnifty_change = giftnifty_json.get('net_change') or giftnifty_json.get('change', 0)
                            giftnifty_change_pct = giftnifty_json.get('percent_change') or giftnifty_json.get('change_pct', 0)
                            if giftnifty_value:
                                logger.debug(f"✅ Fetched GIFT NIFTY from {key}: {giftnifty_value}")
                                break
                    except Exception as e:
                        logger.error(f"Error fetching GIFT NIFTY from {key}: {e}", exc_info=True)
                        continue
                
                # Calculate gap between GIFT NIFTY and NIFTY 50
                gap_points = None
                gap_percent = None
                gap_signal = None
                if giftnifty_value is not None and nifty_value is not None:
                    gap_points = float(giftnifty_value) - float(nifty_value)
                    if float(nifty_value) > 0:
                        gap_percent = (gap_points / float(nifty_value)) * 100
                        # Generate gap signal
                        if gap_percent > 0.75:
                            gap_signal = "STRONG_GAP_UP"
                        elif gap_percent > 0.3:
                            gap_signal = "GAP_UP"
                        elif gap_percent < -0.75:
                            gap_signal = "STRONG_GAP_DOWN"
                        elif gap_percent < -0.3:
                            gap_signal = "GAP_DOWN"
                        else:
                            gap_signal = "FLAT_OPEN"
                
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
                        ], width=3)
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
                        ], width=3)
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
                        ], width=3)
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
                        ], width=3)
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
                        ], width=3)
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
                        ], width=3)
                    )
                
                # GIFT NIFTY Card with Gap Analysis
                if giftnifty_value is not None:
                    giftnifty_color = "success" if giftnifty_change and giftnifty_change > 0 else ("danger" if giftnifty_change and giftnifty_change < 0 else "secondary")
                    # Enhanced card with gap information
                    card_body_children = [
                        html.H5("GIFT NIFTY", className="card-title"),
                        html.H3(f"{float(giftnifty_value):.2f}", className="mb-2"),
                        html.P([
                            html.Span(f"{giftnifty_change:+.2f}" if giftnifty_change is not None else "N/A", 
                                    className="text-success" if giftnifty_change and giftnifty_change > 0 else "text-danger"),
                            html.Span(f" ({giftnifty_change_pct:+.2f}%)" if giftnifty_change_pct is not None else "", 
                                    className="text-success" if giftnifty_change_pct and giftnifty_change_pct > 0 else "text-danger")
                        ], className="mb-1")
                    ]
                    
                    # Add gap information if available
                    if gap_points is not None and gap_percent is not None:
                        gap_color_class = "text-success" if gap_points > 0 else ("text-danger" if gap_points < 0 else "text-muted")
                        gap_signal_color = "danger" if "STRONG_GAP" in str(gap_signal) else ("warning" if "GAP" in str(gap_signal) else "secondary")
                        card_body_children.append(
                            html.Hr(className="my-2")
                        )
                        card_body_children.append(
                            html.P([
                                html.Strong("Gap: ", className="text-muted"),
                                html.Span(f"{gap_points:+.2f} pts ({gap_percent:+.2f}%)", className=gap_color_class)
                            ], className="mb-1", style={"font-size": "0.9em"})
                        )
                        if gap_signal:
                            card_body_children.append(
                                html.P([
                                    html.Small(gap_signal, className=f"badge bg-{gap_signal_color}")
                                ], className="mb-0")
                            )
                    
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody(card_body_children)
                            ], color=giftnifty_color, outline=True)
                        ], width=3)
                    )
                else:
                    cards.append(
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H5("GIFT NIFTY", className="card-title"),
                                    html.P("Loading...", className="text-muted")
                                ])
                            ], outline=True)
                        ], width=3)
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
                now = datetime.now()
                raw_items = self._collect_recent_news_items(max_items=80)
                symbol_specific_items: List[Dict[str, Any]] = []
                
                if self.redis_db1 and self.intraday_instruments:
                    sampled_symbols = list(self.intraday_instruments)[:25]
                    for base_symbol in sampled_symbols:
                        variants = self._generate_symbol_variants(base_symbol)[:4]
                        for variant in variants:
                            latest_key = f"news:latest:{variant}"
                            latest_payload = self._decode_redis_value(
                                self._safe_redis_get(self.redis_db1, latest_key)
                            )
                            if latest_payload:
                                try:
                                    latest_item = json.loads(latest_payload)
                                except json.JSONDecodeError:
                                    latest_item = {"headline": latest_payload}
                                if isinstance(latest_item, dict):
                                    latest_item["_redis_key"] = latest_key
                                    latest_item["_symbol_variant"] = variant
                                    symbol_specific_items.append(latest_item)
                            
                            try:
                                zset_key = f"news:symbol:{variant}"
                                zset_entries = self.redis_db1.zrevrange(zset_key, 0, 0)
                                for entry in zset_entries or []:
                                    decoded_entry = self._decode_redis_value(entry)
                                    if not decoded_entry:
                                        continue
                                    try:
                                        parsed_entry = json.loads(decoded_entry)
                                    except json.JSONDecodeError:
                                        parsed_entry = {"headline": decoded_entry}
                                    payload = (
                                        parsed_entry.get("data")
                                        if isinstance(parsed_entry, dict) and isinstance(parsed_entry.get("data"), dict)
                                        else parsed_entry
                                    )
                                    if isinstance(payload, dict):
                                        payload["_redis_key"] = f"{zset_key}:{payload.get('headline', '')}"
                                        payload["_symbol_variant"] = variant
                                        symbol_specific_items.append(payload)
                            except Exception as zset_err:
                                logger.debug(f"Symbol zset news lookup failed for {variant}: {zset_err}")
                
                news_items = self._dedupe_news_items([*raw_items, *symbol_specific_items])
                
                normalized_items: List[Dict[str, Any]] = []
                for item in news_items:
                    if not isinstance(item, dict):
                        continue
                    normalized = dict(item)
                    
                    if 'title' in normalized and 'headline' not in normalized:
                        normalized['headline'] = normalized['title']
                    if 'publisher' in normalized and 'news_source' not in normalized:
                        normalized['news_source'] = normalized['publisher']
                    if 'source' in normalized and 'news_source' not in normalized:
                        normalized['news_source'] = normalized['source']
                    if 'collected_at' in normalized and 'timestamp' not in normalized:
                        normalized['timestamp'] = normalized['collected_at']
                    
                    ts_value = normalized.get('timestamp') or normalized.get('published_time') or normalized.get('date')
                    parsed_ts = self._parse_datetime(ts_value) or now
                    normalized['_timestamp_dt'] = parsed_ts
                    normalized_items.append(normalized)
                
                normalized_items.sort(key=lambda x: x.get('_timestamp_dt', now), reverse=True)
                news_items = normalized_items[:20]
                
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
        
        # Statistical Model Callbacks
        # Auto-refresh validation results every 5 seconds
        @app.callback(
            Output('validation-stats-display', 'children', allow_duplicate=True),
            Output('pattern-performance-chart', 'figure', allow_duplicate=True),
            Output('forward-validation-chart', 'figure', allow_duplicate=True),
            Input('interval-component', 'n_intervals'),  # Trigger on interval refresh
            prevent_initial_call=True
        )
        def update_statistical_model_auto_refresh(_):
            """Auto-refresh statistical model visualizations"""
            try:
                # Reload validation results from Redis
                self.load_validation_results()
                # Get statistical model data
                stats_data = self.get_statistical_model_data()
                
                # Build validation stats display
                if stats_data.get('total_validations', 0) > 0:
                    stats_html = dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['total_validations']}", className="text-center text-primary mb-1"),
                                    html.P("Total Validations", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['valid_rate']:.1%}", className="text-center text-success mb-1"),
                                    html.P("Validation Success Rate", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['avg_confidence']:.1%}", className="text-center text-info mb-1"),
                                    html.P("Average Confidence", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['forward_validation_summary'].get('total_forward_validations', 0)}", className="text-center text-warning mb-1"),
                                    html.P("Forward Validations", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3)
                    ], className="mb-4")
                else:
                    stats_html = dbc.Alert([
                        html.H5("📊 No Validation Data Available", className="mb-2"),
                        html.P("Validation results will appear here once the alert_validator processes alerts.", 
                               className="mb-0 text-muted")
                    ], color="info")
                
                # Pattern performance chart
                pattern_perf_fig = self._create_pattern_performance_chart(stats_data.get('pattern_performance', {}))
                
                # Forward validation chart
                forward_val_fig = self._create_forward_validation_chart(stats_data.get('forward_validation_summary', {}))
                
                return stats_html, pattern_perf_fig, forward_val_fig
            except Exception as e:
                logger.error(f"Error updating statistical model (auto-refresh): {e}")
                import traceback
                logger.error(traceback.format_exc())
                error_msg = dbc.Alert([
                    html.Strong("Error loading statistical model: "), str(e)
                ], color="danger")
                empty_fig = go.Figure(layout=go.Layout(title="Error loading chart", height=350))
                return error_msg, empty_fig, empty_fig
        
        @app.callback(
            Output('validation-stats-display', 'children'),
            Output('pattern-performance-chart', 'figure'),
            Output('forward-validation-chart', 'figure'),
            Input('symbol-dropdown', 'value')  # Trigger on any dashboard interaction
        )
        def update_statistical_model(_):
            """Update statistical model visualizations"""
            try:
                # Get statistical model data (use cached validation_results)
                stats_data = self.get_statistical_model_data()
                
                # Build validation stats display
                if stats_data.get('total_validations', 0) > 0:
                    stats_html = dbc.Row([
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['total_validations']}", className="text-center text-primary mb-1"),
                                    html.P("Total Validations", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['valid_rate']:.1%}", className="text-center text-success mb-1"),
                                    html.P("Validation Success Rate", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['avg_confidence']:.1%}", className="text-center text-info mb-1"),
                                    html.P("Average Confidence", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3),
                        dbc.Col([
                            dbc.Card([
                                dbc.CardBody([
                                    html.H3(f"{stats_data['forward_validation_summary'].get('total_forward_validations', 0)}", className="text-center text-warning mb-1"),
                                    html.P("Forward Validations", className="text-center text-muted mb-0")
                                ])
                            ])
                        ], width=3)
                    ], className="mb-4")
                else:
                    stats_html = dbc.Alert([
                        html.H5("📊 No Validation Data Available", className="mb-2"),
                        html.P("Validation results will appear here once the alert_validator processes alerts.", 
                               className="mb-0 text-muted")
                    ], color="info")
                
                # Pattern performance chart
                pattern_perf_fig = self._create_pattern_performance_chart(stats_data.get('pattern_performance', {}))
                
                # Forward validation chart
                forward_val_fig = self._create_forward_validation_chart(stats_data.get('forward_validation_summary', {}))
                
                return stats_html, pattern_perf_fig, forward_val_fig
            except Exception as e:
                logger.error(f"Error updating statistical model: {e}")
                import traceback
                logger.error(traceback.format_exc())
                error_msg = dbc.Alert([
                    html.Strong("Error loading statistical model: "), str(e)
                ], color="danger")
                empty_fig = go.Figure(layout=go.Layout(title="Error loading chart", height=350))
                return error_msg, empty_fig, empty_fig
        
        print("🔍 [DEBUG] All callbacks registered, returning app object...")
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
        # Load validation results at startup
        print("📊 Loading validation results from alert_validator...")
        self.load_validation_results()
        app = self.create_dashboard()
        
        # Start real-time alerts stream reader BEFORE starting the app
        print("🚀 Starting real-time alerts subscription...")
        self.start_alerts_stream_reader()
        
        # Start forward validation stream reader
        print("🚀 Starting real-time forward validation subscription...")
        self.start_validation_stream_reader()
        
        print(f"Starting dashboard on http://{host}:{port}")
        print(f"✅ Public access: https://remember-prefers-thinkpad-distributors.trycloudflare.com (via Cloudflare Tunnel)")
        print(f"✅ Local network: http://<local-ip>:{port}")
        print(f"✅ Local access: http://localhost:{port}")
        print(f"⚠️  WARNING: Port {port} and host {host} are fixed for consumer access - DO NOT CHANGE")
        print(f"📋 Note: External access via ngrok: https://jere-unporous-magan.ngrok-free.dev (managed by launchd service)")
        print(f"🔄 Real-time alerts subscription: ACTIVE")
        # Use threaded=True and processes=1 for better network accessibility
        # Keep connections alive for ngrok compatibility
        app.run(debug=False, host=host, port=port, threaded=True, processes=1, use_reloader=False)

if __name__ == "__main__":
    dashboard = AlertValidationDashboard()
    # CRITICAL: Port 53056 is hardcoded for consumer access
    # External access via Cloudflare Tunnel: https://remember-prefers-thinkpad-distributors.trycloudflare.com
    # DO NOT CHANGE port without approval
    dashboard.run_dashboard(port=53056)
