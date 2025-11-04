#!/usr/bin/env python3
"""
Process Parquet Files to Production Schema
==========================================
- Handles nested OHLC and depth structures
- Enriches with token metadata
- Calculates indicators and Greeks
- Maps to production tick_data_corrected schema
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import json
from datetime import datetime, date
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import (
    ensure_production_schema,
    COLUMN_ORDER
)
from binary_to_parquet.enhanced_metadata_calculator import EnhancedMetadataCalculator

# Load raw token lookup for option symbol parsing
_raw_token_lookup = None

def _load_raw_token_lookup(cache_path: str):
    """Load raw token lookup JSON to access 'key' field for option parsing"""
    global _raw_token_lookup
    if _raw_token_lookup is not None:
        return _raw_token_lookup
    
    try:
        import json
        with open(cache_path, 'r') as f:
            _raw_token_lookup = json.load(f)
        logger.info(f"Loaded raw token lookup with {len(_raw_token_lookup):,} entries")
    except Exception as e:
        logger.warning(f"Could not load raw token lookup: {e}")
        _raw_token_lookup = {}
    
    return _raw_token_lookup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_ohlc(ohlc_data: Any) -> Dict[str, Optional[float]]:
    """Extract OHLC from nested dict structure"""
    result = {
        'open_price': None,
        'high_price': None,
        'low_price': None,
        'close_price': None,
    }
    
    if isinstance(ohlc_data, dict):
        result['open_price'] = ohlc_data.get('open')
        result['high_price'] = ohlc_data.get('high')
        result['low_price'] = ohlc_data.get('low')
        result['close_price'] = ohlc_data.get('close')
    elif isinstance(ohlc_data, str):
        try:
            parsed = json.loads(ohlc_data)
            if isinstance(parsed, dict):
                result['open_price'] = parsed.get('open')
                result['high_price'] = parsed.get('high')
                result['low_price'] = parsed.get('low')
                result['close_price'] = parsed.get('close')
        except:
            pass
    
    return result


def classify_session(ts: Optional[datetime], exchange: Optional[str]) -> str:
    """
    Classify market session based on timestamp
    Pre-market: 9:00-9:15 IST = 3:30-3:45 UTC
    Regular: 9:15-15:40 IST = 3:45-10:10 UTC
    Post-market: 15:40-16:00 IST = 10:10-10:30 UTC
    """
    if ts is None:
        return "unknown"
    hour = ts.hour
    minute = ts.minute
    
    if exchange in {None, "NSE", "BSE", "NFO", "BFO", "CDS", "MCX"}:
        if hour == 3 and 30 <= minute < 45:
            return "pre_market"
        if (hour == 3 and minute >= 45) or (4 <= hour < 10) or (hour == 10 and minute < 10):
            return "regular"
        if hour == 10 and 10 <= minute < 30:
            return "post_market"
        return "off_market"
    return "unknown"

def is_index_instrument(symbol: str, segment: str, instrument_type: str) -> bool:
    """Check if instrument is an index (indices don't have depth)"""
    if segment and 'INDEX' in str(segment).upper():
        return True
    if symbol and any(idx in str(symbol).upper() for idx in ['NIFTY 50', 'NIFTY50', 'BANKNIFTY', 'FINNIFTY', 'VIX', 'GIFT NIFTY']):
        return True
    # Index futures/options might have NIFTY in name but still be tradeable
    if instrument_type in ['FUT', 'CE', 'PE'] and 'NIFTY' in str(symbol).upper():
        return False  # These are tradeable, not pure indices
    return False

def extract_depth(depth_data: Any) -> Dict[str, Optional[Any]]:
    """Extract market depth from nested dict/array structure"""
    result = {}
    
    # Initialize all levels
    for level in range(1, 6):
        result[f'bid_{level}_price'] = None
        result[f'bid_{level}_quantity'] = 0
        result[f'bid_{level}_orders'] = 0
        result[f'ask_{level}_price'] = None
        result[f'ask_{level}_quantity'] = 0
        result[f'ask_{level}_orders'] = 0
    
    if not isinstance(depth_data, dict):
        return result
    
    # Handle 'buy' array (bids)
    buy_entries = depth_data.get('buy', [])
    if isinstance(buy_entries, (list, np.ndarray)):
        for idx, entry in enumerate(buy_entries[:5]):  # Max 5 levels
            if isinstance(entry, dict):
                level = idx + 1
                result[f'bid_{level}_price'] = entry.get('price')
                result[f'bid_{level}_quantity'] = entry.get('quantity', 0)
                result[f'bid_{level}_orders'] = entry.get('orders', 0)
    
    # Handle 'sell' array (asks)
    sell_entries = depth_data.get('sell', [])
    if isinstance(sell_entries, (list, np.ndarray)):
        for idx, entry in enumerate(sell_entries[:5]):  # Max 5 levels
            if isinstance(entry, dict):
                level = idx + 1
                result[f'ask_{level}_price'] = entry.get('price')
                result[f'ask_{level}_quantity'] = entry.get('quantity', 0)
                result[f'ask_{level}_orders'] = entry.get('orders', 0)
    
    return result


def parse_option_symbol(full_symbol: str) -> Dict[str, Optional[Any]]:
    """Parse option symbol to extract expiry, strike price, and option type"""
    result = {
        'expiry': None,
        'strike_price': None,
        'option_type': None,
    }
    
    if not full_symbol:
        return result
    
    import re
    from datetime import date
    
    # Remove exchange prefix if present (e.g., "NFO:ADANIENT25NOV2500PE")
    if ':' in full_symbol:
        full_symbol = full_symbol.split(':', 1)[1]
    
    full_symbol_upper = full_symbol.upper()
    
    # Parse expiry: DDMMM (e.g., 25NOV)
    expiry_match = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', full_symbol_upper)
    if expiry_match:
        day = int(expiry_match.group(1))
        month_str = expiry_match.group(2)
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
        }
        month = month_map.get(month_str)
        year = datetime.now().year
        if month:
            try:
                result['expiry'] = date(year, month, day)
            except ValueError:
                # Invalid date, try next year
                try:
                    result['expiry'] = date(year + 1, month, day)
                except:
                    pass
    
    # Parse strike and option type: STRIKE + CE/PE
    strike_match = re.search(r'(\d+(?:\.\d+)?)(CE|PE)$', full_symbol_upper)
    if strike_match:
        result['strike_price'] = float(strike_match.group(1))
        result['option_type'] = strike_match.group(2)
    
    return result


def convert_timestamp_to_ns(ts: Any) -> Optional[int]:
    """Convert timestamp to nanoseconds"""
    if pd.isna(ts):
        return None
    
    if isinstance(ts, (int, np.int64)):
        # Assume seconds, convert to nanoseconds
        return int(ts * 1e9)
    elif isinstance(ts, pd.Timestamp):
        return int(ts.timestamp() * 1e9)
    elif isinstance(ts, datetime):
        return int(ts.timestamp() * 1e9)
    elif isinstance(ts, str):
        try:
            dt = pd.to_datetime(ts)
            return int(dt.timestamp() * 1e9)
        except:
            return None
    
    return None


def process_parquet_file(
    file_path: Path,
    conn: duckdb.DuckDBPyConnection,
    token_cache: TokenCacheManager,
    metadata_calculator: EnhancedMetadataCalculator,
    batch_size: int = 5000
) -> Dict[str, Any]:
    """Process a single parquet file and insert into production schema"""
    
    logger.info(f"Processing: {file_path.name}")
    
    try:
        # Read parquet file with robust error handling
        df = None
        read_error = None
        
        try:
            # Method 1: Standard pandas read
            df = pd.read_parquet(file_path)
        except Exception as e1:
            read_error = str(e1)
            logger.warning(f"  Standard read failed: {read_error[:100]}")
            
            # Method 2: Try pyarrow with row group reading (for incomplete files)
            try:
                import pyarrow.parquet as pq
                parquet_file = pq.ParquetFile(file_path)
                
                # Try reading all row groups separately and concatenating
                row_group_dfs = []
                for rg_idx in range(parquet_file.num_row_groups):
                    try:
                        rg_table = parquet_file.read_row_group(rg_idx)
                        rg_df = rg_table.to_pandas()
                        row_group_dfs.append(rg_df)
                    except Exception as rg_err:
                        logger.warning(f"  Row group {rg_idx} failed: {str(rg_err)[:80]}, skipping...")
                        continue
                
                if row_group_dfs:
                    df = pd.concat(row_group_dfs, ignore_index=True)
                    logger.info(f"  Recovered {len(df)} rows by reading row groups individually")
                else:
                    raise Exception("All row groups failed to read")
                    
            except Exception as e2:
                # Method 3: Try reading columns individually (for column-level corruption)
                try:
                    parquet_file = pq.ParquetFile(file_path)
                    schema = parquet_file.schema
                    column_names = [schema[i].name for i in range(len(schema))]
                    
                    # Read columns one by one and handle failures
                    recovered_data = {}
                    for col_name in column_names:
                        try:
                            col_table = parquet_file.read(columns=[col_name])
                            recovered_data[col_name] = col_table.column(0).to_pandas()
                        except Exception as col_err:
                            logger.warning(f"  Column '{col_name}' failed, skipping: {str(col_err)[:50]}")
                            # Create null column with expected length
                            if recovered_data:
                                expected_len = len(next(iter(recovered_data.values())))
                                recovered_data[col_name] = pd.Series([None] * expected_len)
                    
                    if recovered_data:
                        # Find minimum length (most reliable)
                        min_len = min(len(v) for v in recovered_data.values() if v is not None)
                        # Truncate all columns to minimum length
                        recovered_data_truncated = {
                            k: v.iloc[:min_len] if v is not None else pd.Series([None] * min_len)
                            for k, v in recovered_data.items()
                        }
                        df = pd.DataFrame(recovered_data_truncated)
                        logger.info(f"  Recovered {len(df)} rows by reading columns individually")
                    else:
                        raise Exception("All columns failed to read")
                        
                except Exception as e3:
                    raise Exception(f"All recovery methods failed. Last error: {str(e3)[:100]}")
        
        if df is None or len(df) == 0:
            error_msg = f"Could not read any data: {read_error[:150] if read_error else 'Unknown error'}"
            logger.error(f"  {error_msg}")
            return {"success": False, "error": error_msg, "row_count": 0, "total_rows": 0}
        
        total_rows = len(df)
        logger.info(f"  Loaded {total_rows:,} rows from parquet")
        
        # Process in batches
        processed_rows = []
        errors = 0
        skipped = 0  # Track intentionally skipped rows
        duplicates_dropped = 0  # Track rows dropped due to duplicate primary keys
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx].copy()
            
            batch_processed = []
            
            for idx, row in batch_df.iterrows():
                try:
                    # Extract core fields
                    instrument_token = int(row.get('instrument_token', 0))
                    if instrument_token <= 0:
                        skipped += 1
                        continue
                    
                    # Get timestamp with priority: exchange_timestamp > timestamp_ms > timestamp > exchange_timestamp_epoch > skip
                    # Priority 1: exchange_timestamp (most accurate, exchange-provided)
                    exchange_timestamp = row.get('exchange_timestamp')
                    
                    # Priority 2: timestamp_ms (milliseconds since epoch)
                    if pd.isna(exchange_timestamp):
                        timestamp_ms = row.get('timestamp_ms')
                        if pd.notna(timestamp_ms):
                            try:
                                # Convert milliseconds to datetime
                                exchange_timestamp = pd.to_datetime(timestamp_ms, unit='ms')
                            except Exception as e:
                                logger.debug(f"Could not parse timestamp_ms: {e}")
                                exchange_timestamp = None
                    
                    # Priority 3: timestamp field (string format, always available)
                    if pd.isna(exchange_timestamp):
                        timestamp_str = row.get('timestamp')
                        if pd.notna(timestamp_str):
                            try:
                                # Parse string timestamp (e.g., "2025-10-28T08:56:41.296137")
                                exchange_timestamp = pd.to_datetime(timestamp_str)
                            except Exception as e:
                                logger.debug(f"Could not parse timestamp field: {e}")
                                exchange_timestamp = None
                    
                    # Priority 4: exchange_timestamp_epoch (if valid)
                    if pd.isna(exchange_timestamp):
                        exchange_timestamp_epoch = row.get('exchange_timestamp_epoch')
                        if pd.notna(exchange_timestamp_epoch) and exchange_timestamp_epoch > 0:
                            try:
                                # Try as seconds first
                                exchange_timestamp = pd.to_datetime(exchange_timestamp_epoch, unit='s')
                            except:
                                try:
                                    # Try as milliseconds
                                    exchange_timestamp = pd.to_datetime(exchange_timestamp_epoch, unit='ms')
                                except:
                                    exchange_timestamp = None
                    
                    # Skip only if no valid timestamp found at all
                    if pd.isna(exchange_timestamp):
                        # No valid timestamp available - skip this row
                        skipped += 1
                        continue
                    
                    # Get metadata
                    metadata = token_cache.get_instrument_info(instrument_token)
                    
                    # Get instrument type for option detection
                    instrument_type_str = str(metadata.get('instrument_type', row.get('instrument_type', ''))).upper()
                    
                    # Extract OHLC
                    ohlc = extract_ohlc(row.get('ohlc'))
                    
                    # Extract depth
                    depth = extract_depth(row.get('depth'))
                    
                    # Check if this is an index (indices don't have depth - that's expected)
                    is_index = is_index_instrument(
                        metadata.get('symbol', row.get('symbol', '')),
                        metadata.get('segment', row.get('segment', '')),
                        instrument_type_str
                    )
                    
                    # If depth extraction failed and NOT an index, try flattened fields
                    if not is_index and depth['bid_1_price'] is None and 'best_bid_price' in row:
                        depth['bid_1_price'] = row.get('best_bid_price')
                        depth['bid_1_quantity'] = int(row.get('best_bid_quantity', 0)) if pd.notna(row.get('best_bid_quantity')) else None
                        depth['bid_1_orders'] = int(row.get('best_bid_orders', 0)) if pd.notna(row.get('best_bid_orders')) else None
                        depth['ask_1_price'] = row.get('best_ask_price')
                        depth['ask_1_quantity'] = int(row.get('best_ask_quantity', 0)) if pd.notna(row.get('best_ask_quantity')) else None
                        depth['ask_1_orders'] = int(row.get('best_ask_orders', 0)) if pd.notna(row.get('best_ask_orders')) else None
                    
                    # For indices, ensure depth fields are None (not 0) - indices don't have depth
                    if is_index:
                        for key in depth.keys():
                            if key.startswith('bid_') or key.startswith('ask_'):
                                depth[key] = None
                    
                    # Prepare price data for indicators
                    price_data = {
                        'last_price': row.get('last_price'),
                        'open_price': ohlc.get('open_price'),
                        'high_price': ohlc.get('high_price'),
                        'low_price': ohlc.get('low_price'),
                        'close_price': ohlc.get('close_price'),
                    }
                    
                    # Calculate indicators
                    indicators = metadata_calculator.calculate_indicators(instrument_token, price_data)
                    
                    # Convert timestamps (exchange_timestamp is already set from priority logic above)
                    # Classify session based on timestamp
                    session_type = classify_session(exchange_timestamp, metadata.get('exchange', row.get('exchange')))
                    
                    # Convert to nanoseconds - use exchange_timestamp (already prioritized)
                    exchange_timestamp_ns = convert_timestamp_to_ns(exchange_timestamp)
                    
                    # Fallback to epoch if conversion failed
                    if exchange_timestamp_ns is None:
                        exchange_timestamp_epoch = row.get('exchange_timestamp_epoch')
                        if pd.notna(exchange_timestamp_epoch) and exchange_timestamp_epoch > 0:
                            exchange_timestamp_ns = convert_timestamp_to_ns(exchange_timestamp_epoch)
                    
                    # If still None, skip this row (no valid timestamp for exchange_timestamp_ns)
                    if exchange_timestamp_ns is None:
                        skipped += 1
                        continue
                    
                    last_trade_time = row.get('last_trade_time')
                    if pd.isna(last_trade_time):
                        last_trade_time = None
                    
                    last_traded_timestamp_ns = convert_timestamp_to_ns(last_trade_time)
                    
                    # Handle expiry, strike, and option_type for options
                    # Initialize with explicit None defaults
                    expiry = metadata.get('expiry') or None
                    strike_price = metadata.get('strike_price') or None
                    option_type = metadata.get('option_type') or None
                    
                    # If instrument is CE/PE but missing metadata, try parsing from 'key' field
                    if (instrument_type_str in ['CE', 'PE'] and 
                        (not expiry or not strike_price or not option_type)):
                        
                        # Load raw token lookup to get 'key' field
                        raw_lookup = _load_raw_token_lookup(token_cache.cache_path)
                        token_str = str(instrument_token)
                        
                        # Get raw metadata with 'key' field
                        raw_metadata = raw_lookup.get(token_str, {})
                        if not raw_metadata and isinstance(raw_lookup, dict):
                            # Try with different key formats
                            for k, v in raw_lookup.items():
                                if isinstance(v, dict) and v.get('token') == instrument_token:
                                    raw_metadata = v
                                    break
                        
                        key = raw_metadata.get('key', '')
                        if key:
                            parsed = parse_option_symbol(key)
                            if parsed.get('expiry'):
                                expiry = parsed['expiry']
                            if parsed.get('strike_price'):
                                strike_price = parsed['strike_price']
                            if parsed.get('option_type'):
                                option_type = parsed['option_type']
                    
                    # Convert expiry to date if needed
                    if expiry:
                        if isinstance(expiry, str):
                            try:
                                expiry = pd.to_datetime(expiry).date()
                            except:
                                expiry = None
                        elif isinstance(expiry, pd.Timestamp):
                            expiry = expiry.date()
                        elif not isinstance(expiry, date):
                            expiry = None
                    else:
                        expiry = None
                    
                    # Ensure strike_price is float or None
                    if strike_price:
                        try:
                            strike_price = float(strike_price)
                        except:
                            strike_price = None
                    else:
                        strike_price = None
                    
                    # Ensure option_type is CE/PE or None
                    if option_type:
                        option_type = str(option_type).upper()
                        # Normalize CALL/PUT to CE/PE
                        if option_type == 'CALL':
                            option_type = 'CE'
                        elif option_type == 'PUT':
                            option_type = 'PE'
                        # Validate it's CE or PE
                        if option_type not in ['CE', 'PE']:
                            option_type = None
                    else:
                        option_type = None
                    
                    # Calculate Greeks (if option) - AFTER metadata parsing
                    # Pass enriched metadata with expiry, strike, option_type
                    greek_metadata = {
                        'instrument_type': instrument_type_str,
                        'option_type': option_type,
                        'strike_price': strike_price,
                        'expiry': expiry,
                        'symbol': metadata.get('symbol', ''),
                    }
                    greeks = metadata_calculator.calculate_greeks(greek_metadata, price_data)
                    
                    # Build production row
                    production_row = {
                        # Core identifiers
                        'instrument_token': instrument_token,
                        'symbol': metadata.get('symbol', row.get('symbol', '')),
                        'exchange': metadata.get('exchange', row.get('exchange', '')),
                        'segment': metadata.get('segment', row.get('segment', '')),
                        'instrument_type': metadata.get('instrument_type', row.get('instrument_type', '')),
                        'expiry': expiry,
                        'strike_price': strike_price,
                        'option_type': option_type,
                        'lot_size': metadata.get('lot_size'),
                        'tick_size': metadata.get('tick_size'),
                        'is_expired': metadata.get('is_expired', False),
                        
                        # Timestamps
                        'timestamp': datetime.now(),
                        'exchange_timestamp': exchange_timestamp,
                        'exchange_timestamp_ns': exchange_timestamp_ns,
                        'last_traded_timestamp': last_trade_time,
                        'last_traded_timestamp_ns': last_traded_timestamp_ns,
                        
                        # Prices
                        'last_price': row.get('last_price'),
                        'open_price': ohlc.get('open_price'),
                        'high_price': ohlc.get('high_price'),
                        'low_price': ohlc.get('low_price'),
                        'close_price': ohlc.get('close_price'),
                        'average_traded_price': row.get('average_traded_price'),
                        
                        # Volume & OI (handle NaN properly)
                        'volume': int(row.get('volume') or row.get('volume_traded', 0)) if pd.notna(row.get('volume') or row.get('volume_traded')) else 0,
                        'last_traded_quantity': int(row.get('last_traded_quantity', 0)) if pd.notna(row.get('last_traded_quantity')) else 0,
                        'total_buy_quantity': int(row.get('total_buy_quantity', 0)) if pd.notna(row.get('total_buy_quantity')) else 0,
                        'total_sell_quantity': int(row.get('total_sell_quantity', 0)) if pd.notna(row.get('total_sell_quantity')) else 0,
                        'open_interest': int(row.get('oi', 0)) if pd.notna(row.get('oi')) else 0,
                        'oi_day_high': int(row.get('oi_day_high', 0)) if pd.notna(row.get('oi_day_high')) else None,
                        'oi_day_low': int(row.get('oi_day_low', 0)) if pd.notna(row.get('oi_day_low')) else None,
                        
                        # Market depth (from extracted depth)
                        **depth,
                        
                        # Technical indicators
                        **indicators,
                        
                        # Option Greeks
                        **greeks,
                        
                        # Metadata
                        'packet_type': 'full',
                        'data_quality': 'complete',
                        'session_type': session_type,
                        'source_file': str(file_path),
                        'processing_batch': 'parquet_ingestion_oct27_plus',
                        'session_id': None,
                        'processed_at': datetime.now(),
                        'parser_version': 'v2.0',
                    }
                    
                    batch_processed.append(production_row)
                    
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        logger.warning(f"  Error processing row {idx}: {e}")
                    continue
            
            if batch_processed:
                processed_rows.extend(batch_processed)
            
            if (start_idx // batch_size + 1) % 10 == 0:
                logger.info(f"  Progress: {len(processed_rows):,}/{total_rows:,} rows processed")
        
        # Insert into database
        if processed_rows:
            logger.info(f"  Inserting {len(processed_rows):,} rows into database...")
            
            # Convert to DataFrame
            production_df = pd.DataFrame(processed_rows)
            
            # Handle duplicate primary keys BEFORE insertion
            # If multiple rows have same (instrument_token, exchange_timestamp_ns, source_file),
            # INSERT OR REPLACE will keep only the last one, losing others
            if len(production_df) > 0:
                # Count duplicates before deduplication
                duplicate_mask = production_df.duplicated(
                    subset=['instrument_token', 'exchange_timestamp_ns', 'source_file'],
                    keep=False
                )
                duplicate_count = duplicate_mask.sum()
                
                if duplicate_count > 0:
                    logger.warning(f"  ⚠️  Found {duplicate_count} rows with duplicate primary keys")
                    logger.warning(f"  Keeping last occurrence for each duplicate (INSERT OR REPLACE behavior)")
                    # Keep last occurrence (matches INSERT OR REPLACE behavior)
                    production_df = production_df.drop_duplicates(
                        subset=['instrument_token', 'exchange_timestamp_ns', 'source_file'],
                        keep='last'
                    )
                    duplicates_dropped = len(processed_rows) - len(production_df)
                    logger.info(f"  After deduplication: {len(production_df):,} rows (dropped {duplicates_dropped:,} duplicates)")
                    # Update row_count to reflect actual rows that will be inserted
                    processed_rows = production_df.to_dict('records')
            
            # Replace NaN with None for proper NULL handling in DuckDB
            # This ensures non-applicable columns are NULL, not NaN
            # NOTE: We preserve valid 0.0 values (e.g., RSI=0 for extremely oversold, MACD=0 at crossover)
            #       The calculator already returns None for non-applicable cases
            for col in production_df.columns:
                if production_df[col].dtype in ['float64', 'float32']:
                    # Convert NaN to None (NULL in DuckDB) - using replace method
                    production_df[col] = production_df[col].replace([np.nan, float('nan')], None)
                    # Convert pd.NA to None as well
                    production_df[col] = production_df[col].where(production_df[col].notna(), None)
                elif production_df[col].dtype == 'object':
                    # For object columns, replace NaN strings with None
                    production_df[col] = production_df[col].replace(['nan', 'NaN', np.nan], None)
                    production_df[col] = production_df[col].where(production_df[col].notna(), None)
            
            logger.info(f"  DataFrame created with {len(production_df.columns)} columns")
            logger.info(f"  Missing from COLUMN_ORDER: {set(COLUMN_ORDER) - set(production_df.columns)}")
            
            # Ensure all required columns exist
            for col in COLUMN_ORDER:
                if col not in production_df.columns:
                    # Initialize with appropriate default based on type
                    if col in ['timestamp', 'exchange_timestamp', 'last_traded_timestamp']:
                        production_df[col] = None  # Timestamp null - use None for DuckDB
                    elif col in ['expiry']:
                        production_df[col] = None  # Date null - use None for DuckDB
                    elif col in ['is_expired']:
                        production_df[col] = False  # Boolean default
                    elif 'price' in col or 'strike' in col or col in ['tick_size', 'average_traded_price', 
                                                                       'rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 
                                                                       'bollinger_middle', 'bollinger_lower', 'macd', 
                                                                       'macd_signal', 'macd_histogram', 'delta', 'gamma', 
                                                                       'theta', 'vega', 'rho', 'implied_volatility']:
                        production_df[col] = None  # Float defaults to None
                    elif col in ['instrument_token', 'exchange_timestamp_ns', 'last_traded_timestamp_ns',
                                 'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
                                 'open_interest', 'oi_day_high', 'oi_day_low', 'lot_size',
                                 *[f'bid_{i}_quantity' for i in range(1, 6)],
                                 *[f'ask_{i}_quantity' for i in range(1, 6)]]:
                        production_df[col] = 0  # Integer defaults to 0
                    elif col in [f'bid_{i}_orders' for i in range(1, 6)] + [f'ask_{i}_orders' for i in range(1, 6)]:
                        production_df[col] = 0  # Integer defaults to 0
                    else:
                        production_df[col] = None  # String/other defaults to None
            
            # Add processed_at and parser_version if missing
            if 'processed_at' not in production_df.columns:
                production_df['processed_at'] = datetime.now()
            if 'parser_version' not in production_df.columns:
                production_df['parser_version'] = 'v2.0'
            
            # Reorder columns to match COLUMN_ORDER + processed_at, parser_version
            all_expected_cols = COLUMN_ORDER + ['processed_at', 'parser_version']
            production_df = production_df[all_expected_cols]
            
            logger.info(f"  Final DataFrame: {len(production_df.columns)} columns, {len(production_df)} rows")
            logger.info(f"  Columns: {list(production_df.columns[:10])}... (showing first 10)")
            
            # Register as temporary view
            view_name = f"temp_parquet_data_{file_path.stem}"
            conn.register(view_name, production_df)
            
            # Debug: Check what DuckDB sees
            try:
                duckdb_cols = conn.execute(f"DESCRIBE SELECT * FROM {view_name} LIMIT 1").fetchdf()['column_name'].tolist()
                logger.info(f"  DuckDB sees {len(duckdb_cols)} columns: {duckdb_cols[:10]}...")
                missing_in_duckdb = set(production_df.columns) - set(duckdb_cols)
                if missing_in_duckdb:
                    logger.warning(f"  ⚠️  Columns in DataFrame but not in DuckDB view: {missing_in_duckdb}")
            except Exception as e:
                logger.warning(f"  Could not describe view: {e}")
            
            # Insert via DuckDB with explicit column list
            # Only use columns that DuckDB can see
            try:
                visible_cols = conn.execute(f"DESCRIBE SELECT * FROM {view_name} LIMIT 1").fetchdf()['column_name'].tolist()
                column_list = ", ".join(visible_cols)
            except:
                column_list = ", ".join(production_df.columns)
            
            conn.execute("BEGIN TRANSACTION")
            try:
                conn.execute(
                    f"INSERT OR REPLACE INTO tick_data_corrected ({column_list}) "
                    f"SELECT {column_list} FROM {view_name}"
                )
                conn.execute("COMMIT")
                logger.info(f"  ✅ Successfully inserted {len(processed_rows):,} rows")
            except Exception as e:
                conn.execute("ROLLBACK")
                raise e
            finally:
                try:
                    conn.unregister(view_name)
                except:
                    pass
        
        # Calculate final row count (after deduplication)
        final_row_count = len(processed_rows) if processed_rows else 0
        
        return {
            "success": True,
            "row_count": final_row_count,
            "total_rows": total_rows,
            "errors": errors,
            "skipped": skipped,
            "duplicates_dropped": duplicates_dropped
        }
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "row_count": 0
        }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Process parquet files to production schema")
    parser.add_argument("--file", type=str, help="Single parquet file to process (for testing)")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json", 
                       help="Token lookup path")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for processing")
    
    args = parser.parse_args()
    
    if not args.file:
        parser.print_help()
        logger.error("Please provide --file to process")
        return
    
    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return
    
    # Load token cache
    logger.info("Loading token cache...")
    token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=True)
    logger.info(f"✅ Loaded {len(token_cache.token_map):,} tokens")
    
    # Initialize metadata calculator
    metadata_calculator = EnhancedMetadataCalculator()
    
    # Connect to database
    logger.info(f"Connecting to database: {args.db}")
    conn = duckdb.connect(args.db)
    ensure_production_schema(conn)
    
    # Process file
    result = process_parquet_file(
        file_path,
        conn,
        token_cache,
        metadata_calculator,
        batch_size=args.batch_size
    )
    
    conn.close()
    
    if result.get("success"):
        logger.info(f"\n✅ SUCCESS")
        logger.info(f"   Rows processed: {result.get('row_count', 0):,}")
        logger.info(f"   Total rows in file: {result.get('total_rows', 0):,}")
        logger.info(f"   Errors: {result.get('errors', 0)}")
    else:
        logger.error(f"\n❌ FAILED: {result.get('error')}")


if __name__ == "__main__":
    main()

