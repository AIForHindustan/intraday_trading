"""Parquet file ingester for tick data with metadata enrichment."""

import json
import logging
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date

import pandas as pd
import pyarrow as pa
import duckdb
from token_cache import TokenCacheManager
from singleton_db import DatabaseConnectionManager
from binary_to_parquet.production_binary_converter import ensure_production_schema

logger = logging.getLogger(__name__)

# Semaphore to limit concurrent DuckDB connections (DuckDB has connection limits)
_db_connection_semaphore = threading.Semaphore(4)  # Max 4 concurrent DB connections


def enrich_parquet_with_metadata(df: pd.DataFrame, token_cache: TokenCacheManager) -> pd.DataFrame:
    """Enrich parquet DataFrame with metadata from token_lookup.json."""
    enriched_df = df.copy()
    
    # Add metadata columns if they don't exist
    metadata_columns = [
        'token_symbol', 'token_exchange', 'token_segment', 'token_instrument_type',
        'token_expiry', 'token_strike_price', 'token_option_type',
        'token_lot_size', 'token_tick_size', 'token_is_expired',
        'token_sector', 'token_asset_class', 'token_sub_category'
    ]
    
    for col in metadata_columns:
        if col not in enriched_df.columns:
            enriched_df[col] = None
    
    # Enrich each row with token metadata
    def enrich_row(row):
        token = row.get('instrument_token')
        if not token or pd.isna(token):
            return row
        
        try:
            token = int(token)
            metadata = token_cache.get_instrument_info(token)
            
            if metadata:
                # Update metadata fields
                row['token_symbol'] = metadata.get('symbol') or row.get('symbol')
                row['token_exchange'] = metadata.get('exchange') or row.get('exchange')
                row['token_segment'] = metadata.get('segment') or row.get('segment')
                row['token_instrument_type'] = metadata.get('instrument_type') or row.get('instrument_type')
                
                # Additional metadata
                if metadata.get('expiry'):
                    row['token_expiry'] = metadata['expiry']
                if metadata.get('strike_price'):
                    row['token_strike_price'] = metadata['strike_price']
                if metadata.get('option_type'):
                    row['token_option_type'] = metadata['option_type']
                if metadata.get('lot_size'):
                    row['token_lot_size'] = metadata['lot_size']
                if metadata.get('tick_size'):
                    row['token_tick_size'] = metadata['tick_size']
                if metadata.get('is_expired') is not None:
                    row['token_is_expired'] = bool(metadata['is_expired'])
                
                # Extended metadata (if available in token_lookup)
                if 'sector' in metadata:
                    row['token_sector'] = metadata['sector']
                if 'asset_class' in metadata:
                    row['token_asset_class'] = metadata['asset_class']
                if 'sub_category' in metadata:
                    row['token_sub_category'] = metadata['sub_category']
        except (ValueError, TypeError) as e:
            logger.debug(f"Error enriching token {token}: {e}")
        
        return row
    
    # Apply enrichment
    enriched_df = enriched_df.apply(enrich_row, axis=1)
    
    return enriched_df


def map_to_duckdb_schema(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Map parquet DataFrame columns to DuckDB schema format."""
    mapped_df = pd.DataFrame()
    
    # Core instrument identifiers - check for various possible column names
    if 'instrument_token' in df.columns:
        mapped_df['instrument_token'] = df['instrument_token'].astype('int64')
    elif 'token' in df.columns:
        mapped_df['instrument_token'] = pd.to_numeric(df['token'], errors='coerce').fillna(0).astype('int64')
    elif 'instrument_id' in df.columns:
        mapped_df['instrument_token'] = pd.to_numeric(df['instrument_id'], errors='coerce').fillna(0).astype('int64')
    else:
        # No instrument_token found - return empty DataFrame
        logger.warning(f"No instrument_token column found in {source_file}. Columns: {list(df.columns)}")
        return pd.DataFrame()
    
    # Symbol and exchange (prefer token_* enriched columns)
    def get_column(df, *col_names, default=None):
        for col in col_names:
            if col in df.columns:
                return df[col]
        return pd.Series([default] * len(df), index=df.index)
    
    mapped_df['symbol'] = get_column(df, 'token_symbol', 'symbol', 'tradingsymbol').fillna('')
    mapped_df['exchange'] = get_column(df, 'token_exchange', 'exchange').fillna('')
    mapped_df['segment'] = get_column(df, 'token_segment', 'segment').fillna('')
    mapped_df['instrument_type'] = get_column(df, 'token_instrument_type', 'instrument_type').fillna('')
    
    # Normalize expiry dates to date objects or YYYY-MM-DD strings
    expiry_col = get_column(df, 'token_expiry', 'expiry')
    def normalize_expiry_date(expiry_val):
        if pd.isna(expiry_val) or expiry_val is None:
            return None
        if isinstance(expiry_val, date):
            return expiry_val
        if isinstance(expiry_val, datetime):
            return expiry_val.date()
        if isinstance(expiry_val, str):
            try:
                # Try multiple date formats
                # Format 1: YYYY-MM-DD
                try:
                    return datetime.strptime(expiry_val[:10], "%Y-%m-%d").date()
                except ValueError:
                    # Format 2: DD-MMM-YYYY (e.g., "29-Dec-2025")
                    try:
                        return datetime.strptime(expiry_val, "%d-%b-%Y").date()
                    except ValueError:
                        # Format 3: DD/MM/YYYY
                        try:
                            return datetime.strptime(expiry_val, "%d/%m/%Y").date()
                        except ValueError:
                            # Format 4: YYYYMMDD
                            try:
                                if len(expiry_val) >= 8 and expiry_val[:8].isdigit():
                                    return datetime.strptime(expiry_val[:8], "%Y%m%d").date()
                                else:
                                    return None
                            except ValueError:
                                return None
            except Exception:
                return None
        return None
    mapped_df['expiry'] = expiry_col.apply(normalize_expiry_date)
    mapped_df['strike_price'] = pd.to_numeric(get_column(df, 'token_strike_price', 'strike_price', default=None), errors='coerce')
    mapped_df['option_type'] = get_column(df, 'token_option_type', 'option_type')
    mapped_df['lot_size'] = pd.to_numeric(get_column(df, 'token_lot_size', 'lot_size', default=None), errors='coerce')
    mapped_df['tick_size'] = pd.to_numeric(get_column(df, 'token_tick_size', 'tick_size', default=None), errors='coerce')
    is_expired_col = get_column(df, 'token_is_expired', 'is_expired', default=False)
    mapped_df['is_expired'] = is_expired_col.astype(bool) if is_expired_col.dtype == 'object' else is_expired_col.fillna(False)
    
    # Timestamps - normalize to datetime
    if 'exchange_timestamp' in df.columns:
        mapped_df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp'], errors='coerce')
        # Calculate nanoseconds (convert datetime64 to int64 nanoseconds)
        mapped_df['exchange_timestamp_ns'] = (
            (mapped_df['exchange_timestamp'] - pd.Timestamp('1970-01-01')).dt.total_seconds() * 1_000_000_000
        ).fillna(0).astype('int64')
    elif 'exchange_timestamp_epoch' in df.columns:
        # Convert epoch to datetime
        mapped_df['exchange_timestamp'] = pd.to_datetime(df['exchange_timestamp_epoch'], unit='s', errors='coerce')
        mapped_df['exchange_timestamp_ns'] = (df['exchange_timestamp_epoch'] * 1_000_000_000).astype('int64').fillna(0)
    else:
        mapped_df['exchange_timestamp'] = pd.NaT
        mapped_df['exchange_timestamp_ns'] = 0
    
    if 'last_trade_time' in df.columns:
        mapped_df['last_traded_timestamp'] = pd.to_datetime(df['last_trade_time'], errors='coerce')
        mapped_df['last_traded_timestamp_ns'] = (
            (mapped_df['last_traded_timestamp'] - pd.Timestamp('1970-01-01')).dt.total_seconds() * 1_000_000_000
        ).fillna(0).astype('int64')
    else:
        mapped_df['last_traded_timestamp'] = pd.NaT
        mapped_df['last_traded_timestamp_ns'] = 0
    
    mapped_df['timestamp'] = pd.Timestamp.utcnow()
    
    # Price data
    if 'last_price' in df.columns:
        mapped_df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce').fillna(0.0)
    elif 'last_traded_price' in df.columns:
        mapped_df['last_price'] = pd.to_numeric(df['last_traded_price'], errors='coerce').fillna(0.0)
    else:
        mapped_df['last_price'] = 0.0
    
    # OHLC handling
    if 'ohlc' in df.columns and df['ohlc'].notna().any():
        # OHLC might be a dict or nested structure
        ohlc_values = df['ohlc'].apply(lambda x: x if isinstance(x, dict) else {})
        mapped_df['open_price'] = pd.Series(pd.to_numeric([x.get('o', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['high_price'] = pd.Series(pd.to_numeric([x.get('h', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['low_price'] = pd.Series(pd.to_numeric([x.get('l', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
        mapped_df['close_price'] = pd.Series(pd.to_numeric([x.get('c', 0) for x in ohlc_values], errors='coerce'), index=df.index).fillna(0.0)
    else:
        mapped_df['open_price'] = 0.0
        mapped_df['high_price'] = 0.0
        mapped_df['low_price'] = 0.0
        mapped_df['close_price'] = 0.0
    
    avg_price_col = 'average_traded_price' if 'average_traded_price' in df.columns else 'average_price'
    if avg_price_col in df.columns:
        mapped_df['average_traded_price'] = pd.to_numeric(df[avg_price_col], errors='coerce').fillna(0.0)
    else:
        mapped_df['average_traded_price'] = 0.0
    
    # Volume and quantity
    if 'volume' in df.columns:
        mapped_df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype('int64')
    elif 'volume_traded' in df.columns:
        mapped_df['volume'] = pd.to_numeric(df['volume_traded'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['volume'] = 0
    
    if 'last_traded_quantity' in df.columns:
        mapped_df['last_traded_quantity'] = pd.to_numeric(df['last_traded_quantity'], errors='coerce').fillna(0).astype('int64')
    elif 'zerodha_last_traded_quantity' in df.columns:
        mapped_df['last_traded_quantity'] = pd.to_numeric(df['zerodha_last_traded_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['last_traded_quantity'] = 0
    
    if 'total_buy_quantity' in df.columns:
        mapped_df['total_buy_quantity'] = pd.to_numeric(df['total_buy_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['total_buy_quantity'] = 0
    
    if 'total_sell_quantity' in df.columns:
        mapped_df['total_sell_quantity'] = pd.to_numeric(df['total_sell_quantity'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['total_sell_quantity'] = 0
    
    # Open interest
    if 'oi' in df.columns:
        mapped_df['open_interest'] = pd.to_numeric(df['oi'], errors='coerce').fillna(0).astype('int64')
    elif 'open_interest' in df.columns:
        mapped_df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['open_interest'] = 0
    
    if 'oi_day_high' in df.columns:
        mapped_df['oi_day_high'] = pd.to_numeric(df['oi_day_high'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['oi_day_high'] = 0
    
    if 'oi_day_low' in df.columns:
        mapped_df['oi_day_low'] = pd.to_numeric(df['oi_day_low'], errors='coerce').fillna(0).astype('int64')
    else:
        mapped_df['oi_day_low'] = 0
    
    # Market depth - extract from depth column if available
    for level in range(1, 6):
        mapped_df[f'bid_{level}_price'] = None
        mapped_df[f'bid_{level}_quantity'] = 0
        mapped_df[f'bid_{level}_orders'] = 0
        mapped_df[f'ask_{level}_price'] = None
        mapped_df[f'ask_{level}_quantity'] = 0
        mapped_df[f'ask_{level}_orders'] = 0
    
    # Extract best bid/ask
    if 'best_bid_price' in df.columns:
        mapped_df['bid_1_price'] = pd.to_numeric(df['best_bid_price'], errors='coerce')
        if 'best_bid_quantity' in df.columns:
            mapped_df['bid_1_quantity'] = pd.to_numeric(df['best_bid_quantity'], errors='coerce').fillna(0).astype('int64')
        else:
            mapped_df['bid_1_quantity'] = 0
        if 'best_bid_orders' in df.columns:
            mapped_df['bid_1_orders'] = pd.to_numeric(df['best_bid_orders'], errors='coerce').fillna(0).astype('int32')
        else:
            mapped_df['bid_1_orders'] = 0
    
    if 'best_ask_price' in df.columns:
        mapped_df['ask_1_price'] = pd.to_numeric(df['best_ask_price'], errors='coerce')
        if 'best_ask_quantity' in df.columns:
            mapped_df['ask_1_quantity'] = pd.to_numeric(df['best_ask_quantity'], errors='coerce').fillna(0).astype('int64')
        else:
            mapped_df['ask_1_quantity'] = 0
        if 'best_ask_orders' in df.columns:
            mapped_df['ask_1_orders'] = pd.to_numeric(df['best_ask_orders'], errors='coerce').fillna(0).astype('int32')
        else:
            mapped_df['ask_1_orders'] = 0
    
    # Metadata fields
    mapped_df['packet_type'] = df['mode'] if 'mode' in df.columns else 'full'
    mapped_df['data_quality'] = 'complete'
    mapped_df['session_type'] = 'regular'  # Could be enhanced with session detection
    mapped_df['source_file'] = source_file
    mapped_df['processing_batch'] = 'parquet_ingestion_enriched'
    
    return mapped_df


def ingest_parquet_file(file_path: Path, db_path: str, token_cache: Optional[TokenCacheManager] = None, enrich: bool = True) -> Dict[str, Any]:
    """Ingest a parquet file into DuckDB with optional metadata enrichment."""
    try:
        # Load parquet file with error handling for corrupt files
        try:
            df = pd.read_parquet(file_path)
        except Exception as read_error:
            # Handle corrupt parquet files specifically
            if "invalid number of bytes" in str(read_error).lower() or "corrupt" in str(read_error).lower():
                logger.warning(f"⚠️  Corrupt parquet file detected: {file_path.name} - {read_error}")
                return {
                    "success": False,
                    "error": f"Corrupt file: {read_error}",
                    "row_count": 0,
                    "original_rows": 0,
                    "corrupt": True
                }
            # Re-raise other errors
            raise
        
        original_rows = len(df)
        
        # Enrich with metadata if requested
        if enrich and token_cache:
            df = enrich_parquet_with_metadata(df, token_cache)
        
        # Map to DuckDB schema
        mapped_df = map_to_duckdb_schema(df, str(file_path))
        
        # Ensure required columns exist
        required_cols = [
            'instrument_token', 'symbol', 'exchange_timestamp', 'exchange_timestamp_ns',
            'last_price', 'volume'
        ]
        
        missing_cols = [col for col in required_cols if col not in mapped_df.columns]
        if missing_cols:
            logger.warning(f"Missing required columns in {file_path}: {missing_cols}")
            return {
                "success": False,
                "error": f"Missing required columns: {missing_cols}",
                "row_count": 0,
                "original_rows": original_rows
            }
        
        # Filter rows with valid instrument_token (skip if no instrument_token column)
        if 'instrument_token' in mapped_df.columns:
            mapped_df = mapped_df[mapped_df['instrument_token'] > 0].copy()
        else:
            logger.warning(f"No instrument_token column in mapped_df for {file_path}")
            return {
                "success": False,
                "error": "No instrument_token column after mapping",
                "row_count": 0,
                "original_rows": original_rows
            }
        
        # ✅ Filter out rows with UNKNOWN symbols - these lack proper metadata
        if 'symbol' in mapped_df.columns:
            before_filter = len(mapped_df)
            # Filter out: NULL, empty, or UNKNOWN_* symbols
            mapped_df = mapped_df[
                mapped_df['symbol'].notna() & 
                (mapped_df['symbol'] != '') & 
                (~mapped_df['symbol'].astype(str).str.startswith('UNKNOWN', na=False))
            ].copy()
            filtered_out = before_filter - len(mapped_df)
            if filtered_out > 0:
                logger.warning(f"Filtered out {filtered_out:,} rows with UNKNOWN/missing symbols from {file_path.name}")
        
        valid_rows = len(mapped_df)
        
        # Insert into DuckDB
        # Create a new connection per thread (DuckDB connections are not thread-safe)
        conn = duckdb.connect(db_path)
        try:
            ensure_production_schema(conn)
            
            # Convert to Arrow table for efficient insertion
            arrow_table = pa.Table.from_pandas(mapped_df)
            
            # Register as view and insert (sanitize view name - no dots allowed)
            view_name = f"temp_parquet_{int(datetime.now().timestamp() * 1000000)}"
            conn.register(view_name, arrow_table)
            
            try:
                # Get column list
                columns = ', '.join(mapped_df.columns)
                conn.execute(f"""
                    INSERT OR REPLACE INTO tick_data_corrected ({columns})
                    SELECT {columns} FROM {view_name}
                """)
                
                logger.info(f"Ingested {valid_rows}/{original_rows} rows from {file_path.name}")
            finally:
                try:
                    conn.unregister(view_name)
                except Exception:
                    pass  # Ignore errors when unregistering
        finally:
            conn.close()
        
        return {
            "row_count": valid_rows,
            "original_rows": original_rows,
            "columns": list(mapped_df.columns),
            "success": True
        }
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def ingest_jsonl_file(file_path: Path, db_path: str) -> Dict[str, Any]:
    """Ingest a JSONL file into DuckDB."""
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        records = []
        for line in lines:
            records.append(json.loads(line.strip()))
        
        df = pd.DataFrame(records)
        
        # Convert to Arrow table
        table = pa.Table.from_pandas(df)
        
        return {
            "row_count": len(df),
            "columns": list(df.columns),
            "arrow_table": table,
            "success": True
        }
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}")
        return {
            "success": False,
            "error": str(e)
        }

