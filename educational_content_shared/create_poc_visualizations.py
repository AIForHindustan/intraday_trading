#!/usr/bin/env python3
"""
POC Visualization Generator for Educational Content
===================================================

Wires real market data to volume_profile_plotters for clean, professional charts.
Uses data from Redis and DuckDB to generate educational content visualizations.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import json
from typing import Dict, List, Optional
import duckdb

# Redis imports
from redis_files.redis_config import get_redis_config
from redis_files.redis_client import get_redis_client

# Token resolution
from crawlers.utils.token_resolver import resolve_token_to_symbol
from crawlers.utils.instrument_mapper import InstrumentMapper

# Volume profile plotters
from educational_content_shared.volume_profile_plotters import (
    plot_daily_price_with_poc,
    plot_volume_profile,
    plot_multiday_poc,
)


class POCVisualizationGenerator:
    """Generate POC visualizations using real market data"""
    
    def __init__(self):
        """Initialize with data sources"""
        self.redis_db0 = get_redis_client(db=0, decode_responses=False)
        self.redis_db1 = get_redis_client(db=1, decode_responses=False)
        self.redis_db2 = get_redis_client(db=2, decode_responses=False)
        self.redis_db5 = get_redis_client(db=5, decode_responses=False)
        
        # Initialize token resolvers
        self.instrument_mapper = InstrumentMapper()
        self.token_to_symbol_cache = {}  # Cache token -> symbol mappings
        
        # Initialize DuckDB connection
        duckdb_path = project_root / "nse_tick_data.duckdb"
        if duckdb_path.exists():
            self.conn = duckdb.connect(str(duckdb_path))
        else:
            prod_db = project_root / "tick_data_production.db"
            if prod_db.exists():
                self.conn = duckdb.connect(str(prod_db))
            else:
                self.conn = None
                print("⚠️ No DuckDB database found. Will use Redis only.")
        
        # Output directory
        self.output_dir = project_root / "educational_content_shared" / "poc_visualizations"
        self.output_dir.mkdir(exist_ok=True)
    
    def resolve_token_to_symbol(self, token_id_str: str) -> Optional[str]:
        """Resolve UNKNOWN_tokenID or tokenID to actual symbol"""
        try:
            # Extract token number
            if token_id_str.startswith('UNKNOWN_'):
                token_num = int(token_id_str.replace('UNKNOWN_', ''))
            elif token_id_str.isdigit():
                token_num = int(token_id_str)
            else:
                return None
            
            # Check cache first
            if token_num in self.token_to_symbol_cache:
                return self.token_to_symbol_cache[token_num]
            
            # Try InstrumentMapper first
            symbol = self.instrument_mapper.token_to_symbol(token_num)
            if symbol and not symbol.startswith('UNKNOWN_'):
                self.token_to_symbol_cache[token_num] = symbol
                return symbol
            
            # Try token_resolver
            resolved = resolve_token_to_symbol(token_num)
            if resolved:
                # Extract symbol from EXCHANGE:SYMBOL format
                if ':' in resolved:
                    symbol = resolved.split(':')[1]
                else:
                    symbol = resolved
                if symbol and not symbol.startswith('UNKNOWN_'):
                    self.token_to_symbol_cache[token_num] = symbol
                    return symbol
            
            return None
        except Exception:
            return None
    
    def find_symbols_with_oct31_data(self) -> Dict[str, List[str]]:
        """Find all symbols (including resolved from tokens) that have Oct 31 data"""
        symbols_data = {}
        
        # Check all bucket history keys
        all_bucket_keys = self.redis_db0.keys('bucket_incremental_volume:history:*')
        
        print(f"🔍 Scanning {len(all_bucket_keys)} bucket history keys for Oct 31 data...")
        
        for key in all_bucket_keys:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else key
                symbol_from_key = key_str.split(':')[-1]
                
                # Resolve if it's a token ID
                if symbol_from_key.startswith('UNKNOWN_') or symbol_from_key.isdigit():
                    resolved_symbol = self.resolve_token_to_symbol(symbol_from_key)
                    if resolved_symbol:
                        symbol_from_key = resolved_symbol
                    else:
                        continue  # Skip if can't resolve
                
                # Check for Oct 31 data
                history = self.redis_db0.lrange(key_str, 0, 2000)  # Check up to 2000 entries
                oct31_entries = []
                
                for entry_bytes in history:
                    try:
                        if isinstance(entry_bytes, bytes):
                            entry = json.loads(entry_bytes.decode('utf-8'))
                        else:
                            entry = json.loads(entry_bytes)
                        
                        ts_str = entry.get('timestamp', '')
                        if '2025-10-31' in ts_str:
                            price = entry.get('last_price', 0)
                            volume = entry.get('bucket_incremental_volume', 0)
                            if price > 0 and volume > 0:
                                oct31_entries.append({
                                    'timestamp': ts_str,
                                    'price': float(price),
                                    'volume': float(volume)
                                })
                    except Exception:
                        continue
                
                if oct31_entries:
                    symbols_data[symbol_from_key] = oct31_entries
                    
            except Exception:
                continue
        
        print(f"✅ Found {len(symbols_data)} symbols with Oct 31 data")
        return symbols_data
    
    def find_available_date(self, symbol: str) -> Optional[date]:
        """Find the most recent date with data for a symbol"""
        # Prefer Oct 31, 2025 if available
        preferred_date = date(2025, 10, 31)
        
        # Check if preferred date has data
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        base_for_buckets = base_symbol.replace('NFO', '').replace('NSE', '').replace('BSE', '')
        
        # Check bucket history for Oct 31
        for interval in ['1min', '5min', '10min']:
            bucket_key = f"bucket_incremental_volume:history:{interval}:{interval}:{base_for_buckets}"
            try:
                history = self.redis_db0.lrange(bucket_key, 0, -1)
                if history:
                    for entry_bytes in history:
                        try:
                            entry = json.loads(entry_bytes.decode('utf-8') if isinstance(entry_bytes, bytes) else entry_bytes)
                            timestamp = entry.get('timestamp', '')
                            if '2025-10-31' in timestamp:
                                return preferred_date
                        except Exception:
                            continue
            except Exception:
                continue
        
        # Try DB 5 OHLC daily sorted sets
        for prefix in ['NFO', 'NSE', '']:
            key = f"ohlc_daily:{prefix}{base_symbol}" if prefix else f"ohlc_daily:{base_symbol}"
            try:
                entries = self.redis_db5.zrange(key, 0, -1, withscores=True)
                if entries:
                    # Check for Oct 31 first
                    for entry_data, score in entries:
                        dt = datetime.fromtimestamp(score / 1000.0 if score > 1e10 else score)
                        if dt.date() == preferred_date:
                            return preferred_date
                    # If not found, use latest
                    entry_data, score = entries[-1]
                    dt = datetime.fromtimestamp(score / 1000.0 if score > 1e10 else score)
                    return dt.date()
            except Exception:
                continue
        
        # Try DB 0 bucket history
        for interval in ['1min', '5min', '10min']:
            for name_variant in [symbol, base_symbol, symbol.replace(' ', '_'), symbol.upper()]:
                bucket_key = f"bucket_incremental_volume:history:{interval}:{interval}:{name_variant}"
                try:
                    history = self.redis_db0.lrange(bucket_key, -1, -1)
                    if history:
                        entry = json.loads(history[0]) if isinstance(history[0], bytes) else json.loads(history[0])
                        timestamp = entry.get('timestamp', entry.get('exchange_timestamp', 0))
                        if isinstance(timestamp, str):
                            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        else:
                            dt = datetime.fromtimestamp(timestamp / 1000.0 if timestamp > 1e10 else timestamp)
                        return dt.date()
                except Exception:
                    continue
        
        # Try DB 1 streams
        stream_key = f"ohlc_updates:{base_symbol}"
        try:
            stream_messages = self.redis_db1.xrevrange(stream_key, count=100)
            if stream_messages:
                for msg_id, fields in stream_messages:
                    try:
                        timestamp_ms = int(fields.get('timestamp', fields.get(b'timestamp', 0)))
                        if timestamp_ms > 0:
                            ts = datetime.fromtimestamp(timestamp_ms / 1000.0)
                            return ts.date()
                    except Exception:
                        continue
        except Exception:
            pass
        
        # Default to Oct 31, 2025 if no data found
        return date(2025, 10, 31)
    
    def load_intraday_data(self, symbol: str, trade_date: date) -> Optional[pd.DataFrame]:
        """Load intraday price data for a specific date"""
        # Try DB 0 bucket history lists first (most reliable)
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        
        # Try different symbol name formats - remove prefixes for bucket keys
        # Bucket keys use base symbol names (BANKNIFTY, not NFOBANKNIFTY)
        base_for_buckets = base_symbol.replace('NFO', '').replace('NSE', '').replace('BSE', '')
        symbol_variants = [
            base_for_buckets,  # Most likely - just the base name
            base_symbol,
            symbol,
            symbol.upper(),
            symbol.replace(' ', '_'),
            symbol.replace('_', ' '),
            f"NFO{base_symbol}",
            f"NSE{base_symbol}"
        ]
        
        # Also check token-based keys - find tokens that resolve to this symbol
        # Scan for UNKNOWN_* keys that resolve to our symbol
        token_keys_to_check = []
        all_bucket_keys = self.redis_db0.keys('bucket_incremental_volume:history:*')
        for key in all_bucket_keys:
            key_str = key.decode('utf-8') if isinstance(key, bytes) else key
            token_symbol = key_str.split(':')[-1]
            if token_symbol.startswith('UNKNOWN_') or token_symbol.isdigit():
                resolved = self.resolve_token_to_symbol(token_symbol)
                if resolved and (resolved == symbol or resolved == base_symbol or resolved == base_for_buckets):
                    token_keys_to_check.append(key_str)
        
        # Add token keys to search list
        for token_key in token_keys_to_check[:5]:  # Limit to avoid too many checks
            symbol_variants.append(token_key.split(':')[-1])  # Use the token ID for lookup
        
        for name_variant in symbol_variants:
            # Try 1min buckets first (highest resolution)
            for interval in ['1min', '5min', '10min']:
                bucket_key = f"bucket_incremental_volume:history:{interval}:{interval}:{name_variant}"
                try:
                    history_list = self.redis_db0.lrange(bucket_key, 0, -1)
                    if history_list and len(history_list) > 0:
                        data = []
                        for entry_bytes in history_list:
                            try:
                                if isinstance(entry_bytes, bytes):
                                    entry_str = entry_bytes.decode('utf-8')
                                else:
                                    entry_str = entry_bytes
                                
                                entry = json.loads(entry_str)
                                
                                # Extract timestamp
                                timestamp = entry.get('timestamp', entry.get('exchange_timestamp', 0))
                                if isinstance(timestamp, str):
                                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                else:
                                    ts = datetime.fromtimestamp(timestamp / 1000.0 if timestamp > 1e10 else timestamp)
                                
                                if ts.date() != trade_date:
                                    continue
                                
                                # Extract price
                                price = entry.get('last_price', entry.get('close', entry.get('price', 0)))
                                if price <= 0:
                                    continue
                                
                                # Extract volume - bucket history uses bucket_incremental_volume
                                volume = entry.get('bucket_incremental_volume', entry.get('volume_traded_for_the_day', entry.get('volume', entry.get('incremental_volume', 0))))
                                
                                data.append({
                                    'ts': ts,
                                    'last_price': float(price),
                                    'volume': int(float(volume))
                                })
                            except Exception:
                                continue
                        
                        if data:
                            df = pd.DataFrame(data)
                            df = df.sort_values('ts')
                            # Ensure IST timezone
                            if df['ts'].dt.tz is None:
                                df['ts'] = df['ts'].dt.tz_localize('Asia/Kolkata')
                            elif df['ts'].dt.tz != pd.Timestamp.now(tz='Asia/Kolkata').tz:
                                df['ts'] = df['ts'].dt.tz_convert('Asia/Kolkata')
                            return df
                except Exception:
                    continue
        
        # Try Redis streams as fallback
        stream_key = f"ohlc_updates:{base_symbol}"
        
        try:
            stream_messages = self.redis_db1.xrevrange(stream_key, count=10000)
            if stream_messages:
                data = []
                for msg_id, fields in stream_messages:
                    try:
                        timestamp_ms = int(fields.get('timestamp', fields.get(b'timestamp', 0)))
                        if timestamp_ms == 0:
                            continue
                        
                        ts = datetime.fromtimestamp(timestamp_ms / 1000.0)
                        if ts.date() != trade_date:
                            continue
                        
                        price = float(fields.get('close', fields.get(b'close', 0)))
                        if price > 0:
                            data.append({
                                'ts': ts,
                                'last_price': price,
                                'open': float(fields.get('open', fields.get(b'open', price))),
                                'high': float(fields.get('high', fields.get(b'high', price))),
                                'low': float(fields.get('low', fields.get(b'low', price))),
                                'volume': int(float(fields.get('volume', fields.get(b'volume', 0))))
                            })
                    except Exception:
                        continue
                
                if data:
                    df = pd.DataFrame(data)
                    df = df.sort_values('ts')
                    # Ensure IST timezone if needed
                    if df['ts'].dt.tz is None:
                        # Assume IST (UTC+5:30)
                        df['ts'] = df['ts'].dt.tz_localize('Asia/Kolkata')
                    return df
        except Exception as e:
            print(f"Redis stream error: {e}")
        
        # Try DuckDB as fallback
        if self.conn:
            try:
                for table_name in ['tick_data_full', 'tick_data']:
                    try:
                        # Try different column name combinations
                        query = f"""
                        SELECT 
                            timestamp as ts,
                            last_price,
                            volume
                        FROM {table_name}
                        WHERE symbol = '{symbol}'
                            AND DATE(timestamp) = '{trade_date}'
                        ORDER BY timestamp
                        """
                        df = self.conn.execute(query).df()
                        if not df.empty:
                            df['ts'] = pd.to_datetime(df['ts'])
                            if df['ts'].dt.tz is None:
                                df['ts'] = df['ts'].dt.tz_localize('Asia/Kolkata')
                            df['last_price'] = df['last_price']
                            return df
                    except Exception:
                        # Try with exchange_timestamp
                        try:
                            query = f"""
                            SELECT 
                                exchange_timestamp as ts,
                                last_price,
                                volume
                            FROM {table_name}
                            WHERE symbol = '{symbol}'
                                AND DATE(exchange_timestamp) = '{trade_date}'
                            ORDER BY exchange_timestamp
                            """
                            df = self.conn.execute(query).df()
                            if not df.empty:
                                df['ts'] = pd.to_datetime(df['ts'])
                                if df['ts'].dt.tz is None:
                                    df['ts'] = df['ts'].dt.tz_localize('Asia/Kolkata')
                                df['last_price'] = df['last_price']
                                return df
                        except Exception:
                            continue
            except Exception as e:
                print(f"DuckDB error: {e}")
        
        return None
    
    def load_daily_data(self, symbol: str, days: int = 5) -> Optional[pd.DataFrame]:
        """Load daily OHLC data with POC/VA"""
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        
        # Try Redis sorted sets (DB 5 first, then DB 1)
        for db in [self.redis_db5, self.redis_db1]:
            # Try multiple prefixes and symbol formats
            symbol_variants = [
                f"NFO{base_symbol}",
                f"NSE{base_symbol}",
                base_symbol,
                symbol.upper(),
                symbol.replace(' ', '_'),
                symbol.replace('_', ' ')
            ]
            
            for name_variant in symbol_variants:
                key = f"ohlc_daily:{name_variant}"
                try:
                    zset_data = db.zrange(key, 0, -1, withscores=True)
                    if zset_data:
                        data = []
                        for entry_data, timestamp_score in zset_data:
                            try:
                                if isinstance(entry_data, bytes):
                                    entry_data = entry_data.decode('utf-8')
                                ohlc = json.loads(entry_data)
                                
                                ts = datetime.fromtimestamp(timestamp_score / 1000.0 if timestamp_score > 1e10 else timestamp_score)
                                
                                # Try to get POC from volume profile data or calculate from bucket
                                poc_data = self.get_poc_for_date(name_variant, ts.date())
                                if not poc_data:
                                    # Calculate POC from daily OHLC
                                    poc_data = self.calculate_poc_from_daily_ohlc(name_variant, ts.date())
                                
                                data.append({
                                    'session_date': ts.date(),
                                    'open': float(ohlc.get('o', 0)),
                                    'high': float(ohlc.get('h', 0)),
                                    'low': float(ohlc.get('l', 0)),
                                    'close': float(ohlc.get('c', 0)),
                                    'volume': int(float(ohlc.get('v', 0))),
                                    'poc_price': poc_data.get('poc_price') if poc_data else None,
                                    'vah': poc_data.get('value_area_high', poc_data.get('vah')) if poc_data else None,
                                    'val': poc_data.get('value_area_low', poc_data.get('val')) if poc_data else None,
                                })
                            except Exception:
                                continue
                        
                        if data:
                            df = pd.DataFrame(data)
                            df = df.sort_values('session_date').tail(days)
                            return df
                except Exception:
                    continue
        
        return None
    
    def calculate_poc_from_daily_ohlc(self, symbol: str, trade_date: date) -> Optional[Dict]:
        """Calculate POC from bucket data for a date"""
        # Load bucket data for the date
        symbol_variants = [symbol, symbol.replace('_', ' '), symbol.upper()]
        
        for name_variant in symbol_variants:
            for interval in ['1min', '5min']:
                bucket_key = f"bucket_incremental_volume:history:{interval}:{interval}:{name_variant}"
                try:
                    history = self.redis_db0.lrange(bucket_key, 0, -1)
                    if history:
                        buckets = []
                        for entry_bytes in history:
                            try:
                                if isinstance(entry_bytes, bytes):
                                    entry = json.loads(entry_bytes.decode('utf-8'))
                                else:
                                    entry = json.loads(entry_bytes)
                                
                                timestamp = entry.get('timestamp', entry.get('exchange_timestamp', 0))
                                if isinstance(timestamp, str):
                                    ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                else:
                                    ts = datetime.fromtimestamp(timestamp / 1000.0 if timestamp > 1e10 else timestamp)
                                
                                if ts.date() != trade_date:
                                    continue
                                
                                price = entry.get('last_price', entry.get('close', 0))
                                volume = entry.get('volume', entry.get('bucket_incremental_volume', 0))
                                
                                if price > 0:
                                    buckets.append({'price': float(price), 'volume': float(volume)})
                            except Exception:
                                continue
                        
                        if buckets:
                            # Calculate POC from buckets
                            return self._calculate_poc_from_buckets(buckets)
                except Exception:
                    continue
        
        return None
    
    def _calculate_poc_from_buckets(self, buckets: List[Dict]) -> Dict:
        """Calculate POC from bucket data"""
        if not buckets:
            return {}
        
        # Create price-volume distribution
        price_volume = {}
        for bucket in buckets:
            price = round(bucket['price'], 2)
            price_volume[price] = price_volume.get(price, 0) + bucket['volume']
        
        if not price_volume:
            return {}
        
        # Find POC
        poc_price = max(price_volume.items(), key=lambda x: x[1])[0]
        total_volume = sum(price_volume.values())
        
        # Calculate Value Area (70%)
        sorted_prices = sorted(price_volume.keys())
        poc_index = sorted_prices.index(poc_price)
        
        cumulative = price_volume[poc_price]
        vah = val = poc_price
        
        high_idx = poc_index + 1
        low_idx = poc_index - 1
        
        while cumulative < total_volume * 0.7 and (high_idx < len(sorted_prices) or low_idx >= 0):
            high_vol = price_volume.get(sorted_prices[high_idx], 0) if high_idx < len(sorted_prices) else 0
            low_vol = price_volume.get(sorted_prices[low_idx], 0) if low_idx >= 0 else 0
            
            if high_vol >= low_vol and high_idx < len(sorted_prices):
                cumulative += high_vol
                vah = sorted_prices[high_idx]
                high_idx += 1
            elif low_idx >= 0:
                cumulative += low_vol
                val = sorted_prices[low_idx]
                low_idx -= 1
            else:
                break
        
        return {
            'poc_price': float(poc_price),
            'value_area_high': float(vah),
            'value_area_low': float(val),
            'vah': float(vah),
            'val': float(val)
        }
    
    def get_poc_for_date(self, symbol: str, trade_date: date) -> Optional[Dict]:
        """Get POC data from Redis volume profile"""
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        date_str = trade_date.strftime('%Y-%m-%d')
        
        # Try DB 2 first (analytics)
        for variant in [symbol, base_symbol, f"NFO{base_symbol}", f"NSE{base_symbol}"]:
            poc_key = f"volume_profile:poc:{variant}"
            session_key = f"volume_profile:session:{variant}:{date_str}"
            
            for key in [poc_key, session_key]:
                try:
                    data = self.redis_db2.hgetall(key)
                    if data and len(data) > 0:
                        result = {}
                        for k, v in data.items():
                            k_str = k.decode('utf-8') if isinstance(k, bytes) else k
                            v_str = v.decode('utf-8') if isinstance(v, bytes) else v
                            try:
                                result[k_str] = float(v_str)
                            except:
                                result[k_str] = v_str
                        
                        if 'poc_price' in result:
                            return {
                                'poc_price': result.get('poc_price'),
                                'value_area_high': result.get('value_area_high', result.get('vah')),
                                'value_area_low': result.get('value_area_low', result.get('val')),
                            }
                except Exception:
                    continue
        
        return None
    
    def calculate_volume_profile(self, df: pd.DataFrame) -> Dict:
        """Calculate volume profile from intraday data"""
        if df.empty or 'last_price' not in df.columns:
            return {}
        
        # Create price bins (tick-aligned)
        price_min = df['last_price'].min()
        price_max = df['last_price'].max()
        price_range = price_max - price_min
        
        # Choose appropriate bin size
        if price_range > 1000:
            bin_size = 5.0
        elif price_range > 100:
            bin_size = 1.0
        elif price_range > 10:
            bin_size = 0.5
        else:
            bin_size = 0.1
        
        # Create price levels
        price_levels = np.arange(
            np.floor(price_min / bin_size) * bin_size,
            np.ceil(price_max / bin_size) * bin_size + bin_size,
            bin_size
        )
        
        # Calculate volume per price level
        volumes = np.zeros(len(price_levels))
        for _, row in df.iterrows():
            price = row['last_price']
            volume = row.get('volume', 0)
            
            # Find closest price level
            idx = np.argmin(np.abs(price_levels - price))
            volumes[idx] += volume
        
        # Find POC (price with highest volume)
        poc_idx = np.argmax(volumes)
        poc_price = price_levels[poc_idx]
        poc_volume = volumes[poc_idx]
        total_volume = volumes.sum()
        
        if total_volume == 0:
            return {}
        
        # Calculate Value Area (70% of volume)
        sorted_indices = np.argsort(volumes)[::-1]  # Descending volume
        cumulative_volume = 0
        va_indices = set()
        
        for idx in sorted_indices:
            va_indices.add(idx)
            cumulative_volume += volumes[idx]
            if cumulative_volume >= total_volume * 0.7:
                break
        
        if va_indices:
            va_prices = price_levels[list(va_indices)]
            vah = va_prices.max()
            val = va_prices.min()
        else:
            vah = val = poc_price
        
        return {
            'price_levels': price_levels.tolist(),
            'volumes': volumes.tolist(),
            'poc_price': float(poc_price),
            'value_area_high': float(vah),
            'value_area_low': float(val),
            'poc_volume': float(poc_volume),
            'total_volume': float(total_volume)
        }
    
    def generate_visualizations(self, symbol: str, trade_date: Optional[date] = None, days: int = 5):
        """Generate all three visualization types for a symbol"""
        if trade_date is None:
            # Use October 31, 2025 as specified
            trade_date = date(2025, 10, 31)
            print(f"Using date: {trade_date} for {symbol}")
        
        print(f"\n{'='*60}")
        print(f"Generating visualizations for {symbol}")
        print(f"{'='*60}")
        
        # 1. Daily Price with POC
        print(f"📊 Creating daily price with POC for {symbol} on {trade_date}")
        df_intraday = self.load_intraday_data(symbol, trade_date)
        
        if df_intraday is not None and not df_intraday.empty:
            # Calculate volume profile
            profile = self.calculate_volume_profile(df_intraday)
            
            if profile:
                # Plot 1: Volume Profile (Primary - better understanding per user preference)
                outpath = self.output_dir / f"{symbol}_{trade_date}_profile.png"
                plot_volume_profile(
                    price_levels=profile['price_levels'],
                    volumes=profile['volumes'],
                    poc=profile['poc_price'],
                    vah=profile.get('value_area_high'),
                    val=profile.get('value_area_low'),
                    title=f"{symbol} — Volume Profile ({trade_date})",
                    outpath=str(outpath)
                )
                print(f"✅ Saved: {outpath}")
                
                # Optional: Also generate price with POC (commented out - user prefers profile only)
                # outpath_price = self.output_dir / f"{symbol}_{trade_date}_price_poc.png"
                # plot_daily_price_with_poc(
                #     times=df_intraday['ts'],
                #     prices=df_intraday['last_price'],
                #     poc=profile['poc_price'],
                #     vah=profile.get('value_area_high'),
                #     val=profile.get('value_area_low'),
                #     title=f"{symbol} — {trade_date} Price with POC",
                #     outpath=str(outpath_price)
                # )
                # print(f"✅ Saved: {outpath_price}")
        else:
            print(f"⚠️ No intraday data for {symbol} on {trade_date}")
        
        # 3. Multi-day POC Evolution
        print(f"📊 Creating multi-day POC evolution for {symbol}")
        df_daily = self.load_daily_data(symbol, days=days)
        
        if df_daily is not None and not df_daily.empty:
            # Filter out rows without POC
            df_daily = df_daily[df_daily['poc_price'].notna()]
            
            if not df_daily.empty:
                outpath = self.output_dir / f"{symbol}_multiday_poc.png"
                plot_multiday_poc(
                    dates=df_daily['session_date'],
                    poc_prices=df_daily['poc_price'],
                    vahs=df_daily['vah'] if 'vah' in df_daily.columns else None,
                    vals=df_daily['val'] if 'val' in df_daily.columns else None,
                    title=f"{symbol} — POC Evolution",
                    outpath=str(outpath)
                )
                print(f"✅ Saved: {outpath}")
            else:
                print(f"⚠️ No POC data found for multi-day chart")
        else:
            print(f"⚠️ No daily data for {symbol}")


def main():
    """Main function to generate visualizations"""
    generator = POCVisualizationGenerator()
    
    # Find symbols with Oct 31 data (including those stored under token IDs)
    print("🔍 Finding symbols with Oct 31 data...")
    oct31_symbols = generator.find_symbols_with_oct31_data()
    
    # Separate futures and equities
    futures_symbols = []
    equity_symbols = []
    
    for symbol in oct31_symbols.keys():
        # Check if it's futures (contains FUT, expiry dates, CE/PE)
        is_futures = any(x in symbol.upper() for x in ['FUT', '25NOV', '25DEC', '25JAN', 'CE', 'PE', '25FEB', '25MAR'])
        if is_futures:
            futures_symbols.append(symbol)
        else:
            equity_symbols.append(symbol)
    
    print(f"\n📊 Found {len(futures_symbols)} futures and {len(equity_symbols)} equities with Oct 31 data")
    
    # Select representative symbols
    symbols_to_process = []
    
    # Add some futures
    if futures_symbols:
        symbols_to_process.extend(futures_symbols[:3])
        print(f"Futures: {futures_symbols[:5]}")
    
    # Add some equities
    if equity_symbols:
        symbols_to_process.extend(equity_symbols[:3])
        print(f"Equities: {equity_symbols[:5]}")
    
    # Also try common symbols in case they have data
    common_symbols = ['BANKNIFTY', 'NIFTY', 'RELIANCE', 'HDFCBANK', 'TCS', 'INFY']
    for sym in common_symbols:
        if sym not in symbols_to_process:
            symbols_to_process.append(sym)
    
    print(f"\n📈 Processing {len(symbols_to_process)} symbols...")
    
    for symbol in symbols_to_process:
        try:
            generator.generate_visualizations(symbol, trade_date=date(2025, 10, 31), days=5)
        except Exception as e:
            print(f"❌ Error generating visualizations for {symbol}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*60}")
    print("📊 Visualization Generation Complete!")
    print(f"All charts saved to: {generator.output_dir}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
