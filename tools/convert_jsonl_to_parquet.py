#!/usr/bin/env python3
"""
Convert JSONL.zst files to standardized Parquet format

Reads JSONL.zst files and converts them to parquet files matching
the tick_data_corrected schema for ingestion into DuckDB.
"""

import sys
from pathlib import Path
import json
import zstandard as zstd
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Dict, Optional, List
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_timestamp_to_ns(timestamp_str: Optional[str], timestamp_ms: Optional[float]) -> Optional[int]:
    """Convert timestamp to nanoseconds since epoch"""
    if timestamp_ms:
        return int(timestamp_ms * 1_000_000)  # Convert ms to ns
    
    if timestamp_str:
        try:
            # Parse ISO format: "2025-09-30T08:53:06.273710"
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1_000_000_000)
        except Exception:
            pass
    
    return None


def convert_jsonl_to_parquet(
    jsonl_file: Path,
    output_dir: Path,
    token_cache: Optional[TokenCacheManager] = None
) -> Dict[str, any]:
    """Convert a single JSONL.zst file to parquet"""
    
    logger.info(f"Converting {jsonl_file.name}...")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    dctx = zstd.ZstdDecompressor()
    records = []
    
    try:
        with open(jsonl_file, 'rb') as f:
            compressed_data = f.read()
            
            # Try streaming decompression
            try:
                stream = io.BytesIO(compressed_data)
                reader = dctx.stream_reader(stream)
                decompressed = reader.read()
            except Exception:
                # Try partial decompression for corrupted files
                max_size = 100 * 1024 * 1024  # 100MB
                chunk_size = min(len(compressed_data), max_size)
                decompressed = dctx.decompress(compressed_data[:chunk_size])
                logger.debug(f"Partially decompressed {jsonl_file.name}")
        
        lines = decompressed.decode('utf-8').split('\n')
        
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                continue
            
            try:
                record = json.loads(line)
                
                # Map JSONL fields to standardized schema - EXACT field names match tick_data_corrected schema
                # Order matches the schema definition exactly
                mapped = {
                    # Instrument Identifiers (REQUIRED)
                    'instrument_token': record.get('instrument_token'),
                    'symbol': record.get('tradingsymbol') or record.get('symbol', ''),
                    'exchange': record.get('exchange', ''),
                    'segment': record.get('segment', ''),
                    'instrument_type': record.get('segment', ''),  # FUT, OPT, EQ, etc.
                    'expiry': record.get('expiry'),
                    'strike_price': record.get('strike_price') if record.get('strike_price') else None,
                    'option_type': record.get('option_type') if record.get('option_type') else None,
                    'lot_size': record.get('lot_size') if record.get('lot_size') else None,
                    'tick_size': record.get('tick_size') if record.get('tick_size') else None,
                    'is_expired': False,  # Default to False
                    
                    # Timestamps (CRITICAL: exchange_timestamp_ns is part of PRIMARY KEY)
                    'timestamp': record.get('timestamp'),
                    'exchange_timestamp': record.get('exchange_timestamp'),
                    'exchange_timestamp_ns': parse_timestamp_to_ns(
                        record.get('timestamp'),
                        record.get('timestamp_ms')
                    ),
                    'last_traded_timestamp': record.get('last_trade_time'),
                    'last_traded_timestamp_ns': None,  # Can parse if needed
                    
                    # Price Data
                    'last_price': record.get('last_price') if record.get('last_price') else None,
                    'open_price': None,
                    'high_price': None,
                    'low_price': None,
                    'close_price': None,
                    'average_traded_price': record.get('average_price') if record.get('average_price') else None,
                    
                    # Volume & Open Interest
                    'volume': record.get('volume') if record.get('volume') else None,
                    'last_traded_quantity': record.get('last_quantity') if record.get('last_quantity') else None,
                    'total_buy_quantity': record.get('buy_quantity') if record.get('buy_quantity') else None,
                    'total_sell_quantity': record.get('sell_quantity') if record.get('sell_quantity') else None,
                    'open_interest': record.get('oi') if record.get('oi') else None,
                    'oi_day_high': record.get('oi_day_high') if record.get('oi_day_high') else None,
                    'oi_day_low': record.get('oi_day_low') if record.get('oi_day_low') else None,
                    
                    # Market Depth: Bid Side (5 levels) - EXACT field names
                    'bid_1_price': None,
                    'bid_1_quantity': None,
                    'bid_1_orders': None,
                    'bid_2_price': None,
                    'bid_2_quantity': None,
                    'bid_2_orders': None,
                    'bid_3_price': None,
                    'bid_3_quantity': None,
                    'bid_3_orders': None,
                    'bid_4_price': None,
                    'bid_4_quantity': None,
                    'bid_4_orders': None,
                    'bid_5_price': None,
                    'bid_5_quantity': None,
                    'bid_5_orders': None,
                    
                    # Market Depth: Ask Side (5 levels) - EXACT field names
                    'ask_1_price': None,
                    'ask_1_quantity': None,
                    'ask_1_orders': None,
                    'ask_2_price': None,
                    'ask_2_quantity': None,
                    'ask_2_orders': None,
                    'ask_3_price': None,
                    'ask_3_quantity': None,
                    'ask_3_orders': None,
                    'ask_4_price': None,
                    'ask_4_quantity': None,
                    'ask_4_orders': None,
                    'ask_5_price': None,
                    'ask_5_quantity': None,
                    'ask_5_orders': None,
                    
                    # Technical Indicators (NULL if not applicable) - EXACT field names
                    'rsi_14': None,
                    'sma_20': None,
                    'ema_12': None,
                    'bollinger_upper': None,
                    'bollinger_middle': None,
                    'bollinger_lower': None,
                    'macd': None,
                    'macd_signal': None,
                    'macd_histogram': None,
                    
                    # Option Greeks (NULL for non-options) - EXACT field names
                    'delta': None,
                    'gamma': None,
                    'theta': None,
                    'vega': None,
                    'rho': None,
                    'implied_volatility': None,
                    
                    # Metadata - EXACT field names
                    'packet_type': record.get('mode', 'full'),
                    'data_quality': 'complete',
                    'session_type': 'regular',  # 'pre_market', 'regular', 'post_market', 'off_market'
                    'source_file': jsonl_file.name,  # PRIMARY KEY component
                    'processing_batch': 'jsonl_conversion',
                    'session_id': None,
                    'processed_at': datetime.now(),
                    'parser_version': 'jsonl_v1.0',
                }
                
                # Extract OHLC from nested structure
                ohlc = record.get('ohlc', {})
                if ohlc:
                    mapped['open_price'] = ohlc.get('open')
                    mapped['high_price'] = ohlc.get('high')
                    mapped['low_price'] = ohlc.get('low')
                    mapped['close_price'] = ohlc.get('close')
                
                # Extract market depth
                depth = record.get('depth', {})
                if depth:
                    buy = depth.get('buy', [])
                    sell = depth.get('sell', [])
                    
                    # Map buy side (bids)
                    for i in range(min(5, len(buy))):
                        level = buy[i]
                        if level.get('price', 0) > 0:  # Only if valid
                            mapped[f'bid_{i+1}_price'] = level.get('price')
                            mapped[f'bid_{i+1}_quantity'] = level.get('quantity')
                            mapped[f'bid_{i+1}_orders'] = level.get('orders')
                    
                    # Map sell side (asks)
                    for i in range(min(5, len(sell))):
                        level = sell[i]
                        if level.get('price', 0) > 0:  # Only if valid
                            mapped[f'ask_{i+1}_price'] = level.get('price')
                            mapped[f'ask_{i+1}_quantity'] = level.get('quantity')
                            mapped[f'ask_{i+1}_orders'] = level.get('orders')
                
                # Enrich with token cache if available
                if token_cache and mapped['instrument_token']:
                    cache_info = token_cache.get_instrument_info(mapped['instrument_token'])
                    if cache_info:
                        # Only fill in missing fields
                        if not mapped['symbol'] or mapped['symbol'].startswith('UNKNOWN'):
                            mapped['symbol'] = cache_info.get('symbol', mapped['symbol'])
                        if not mapped['exchange']:
                            mapped['exchange'] = cache_info.get('exchange', '')
                        if not mapped['segment']:
                            mapped['segment'] = cache_info.get('segment', '')
                        if not mapped['instrument_type']:
                            mapped['instrument_type'] = cache_info.get('instrument_type', '')
                
                # Filter out records with invalid instrument_token
                if mapped['instrument_token'] and mapped['instrument_token'] > 0:
                    records.append(mapped)
                
            except json.JSONDecodeError as e:
                logger.debug(f"JSON decode error at line {line_num}: {e}")
                continue
            except Exception as e:
                logger.debug(f"Error processing line {line_num}: {e}")
                continue
        
        if not records:
            logger.warning(f"No valid records found in {jsonl_file.name}")
            return {"success": False, "error": "No valid records", "row_count": 0}
        
        # Define column order to match tick_data_corrected schema exactly
        column_order = [
            # Instrument Identifiers
            'instrument_token', 'symbol', 'exchange', 'segment', 'instrument_type',
            'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size', 'is_expired',
            # Timestamps
            'timestamp', 'exchange_timestamp', 'exchange_timestamp_ns',
            'last_traded_timestamp', 'last_traded_timestamp_ns',
            # Price Data
            'last_price', 'open_price', 'high_price', 'low_price', 'close_price', 'average_traded_price',
            # Volume & Open Interest
            'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
            'open_interest', 'oi_day_high', 'oi_day_low',
            # Market Depth: Bid Side (5 levels)
            'bid_1_price', 'bid_1_quantity', 'bid_1_orders',
            'bid_2_price', 'bid_2_quantity', 'bid_2_orders',
            'bid_3_price', 'bid_3_quantity', 'bid_3_orders',
            'bid_4_price', 'bid_4_quantity', 'bid_4_orders',
            'bid_5_price', 'bid_5_quantity', 'bid_5_orders',
            # Market Depth: Ask Side (5 levels)
            'ask_1_price', 'ask_1_quantity', 'ask_1_orders',
            'ask_2_price', 'ask_2_quantity', 'ask_2_orders',
            'ask_3_price', 'ask_3_quantity', 'ask_3_orders',
            'ask_4_price', 'ask_4_quantity', 'ask_4_orders',
            'ask_5_price', 'ask_5_quantity', 'ask_5_orders',
            # Technical Indicators
            'rsi_14', 'sma_20', 'ema_12',
            'bollinger_upper', 'bollinger_middle', 'bollinger_lower',
            'macd', 'macd_signal', 'macd_histogram',
            # Option Greeks
            'delta', 'gamma', 'theta', 'vega', 'rho', 'implied_volatility',
            # Metadata
            'packet_type', 'data_quality', 'session_type',
            'source_file', 'processing_batch', 'session_id',
            'processed_at', 'parser_version',
        ]
        
        # Create DataFrame with all columns in correct order
        df = pd.DataFrame(records)
        
        # Ensure all schema columns exist (add missing ones as None)
        for col in column_order:
            if col not in df.columns:
                df[col] = None
        
        # Reorder columns to match schema
        df = df[column_order]
        
        # Convert timestamps
        for ts_col in ['timestamp', 'exchange_timestamp', 'last_traded_timestamp']:
            if ts_col in df.columns:
                df[ts_col] = pd.to_datetime(df[ts_col], errors='coerce')
        
        # Ensure exchange_timestamp_ns is required (fill with calculated if missing)
        if 'exchange_timestamp_ns' in df.columns:
            mask = df['exchange_timestamp_ns'].isna() | (df['exchange_timestamp_ns'] == 0)
            if mask.any():
                # Calculate from exchange_timestamp if available
                if 'exchange_timestamp' in df.columns:
                    ts_values = pd.to_datetime(df.loc[mask, 'exchange_timestamp'], errors='coerce')
                    df.loc[mask, 'exchange_timestamp_ns'] = (
                        ts_values.astype('int64') * 1_000_000_000
                    ).fillna(0).astype('int64')
        
        # Filter out rows with invalid instrument_token or exchange_timestamp_ns (required fields)
        initial_count = len(df)
        df = df[(df['instrument_token'].notna()) & (df['instrument_token'] > 0)].copy()
        df = df[(df['exchange_timestamp_ns'].notna()) & (df['exchange_timestamp_ns'] > 0)].copy()
        
        if len(df) < initial_count:
            logger.info(f"Filtered {initial_count - len(df)} rows with invalid instrument_token or exchange_timestamp_ns")
        
        if len(df) == 0:
            logger.warning(f"No valid records after filtering in {jsonl_file.name}")
            return {"success": False, "error": "No valid records after filtering", "row_count": 0}
        
        # Output file
        output_file = output_dir / f"{jsonl_file.stem}.parquet"
        
        # Write parquet using pandas (handles schema automatically)
        df.to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        logger.info(f"✅ Created {output_file.name} ({len(df):,} rows, {output_file.stat().st_size / 1024 / 1024:.2f} MB)")
        
        return {
            "success": True,
            "row_count": len(df),
            "parquet_file": output_file,
            "records_processed": len(records)
        }
        
    except Exception as e:
        logger.error(f"Error converting {jsonl_file.name}: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e), "row_count": 0}


def batch_convert_jsonl_to_parquet(
    jsonl_dir: Path,
    output_dir: Path,
    token_cache_path: Optional[str] = None,
    date_filter: Optional[str] = None
) -> Dict:
    """Batch convert all JSONL.zst files in a directory"""
    
    jsonl_files = list(jsonl_dir.glob("*.jsonl.zst"))
    
    if date_filter:
        jsonl_files = [f for f in jsonl_files if date_filter in f.name]
    
    logger.info(f"Found {len(jsonl_files)} JSONL.zst files to convert")
    
    # Load token cache if provided
    token_cache = None
    if token_cache_path:
        token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
    
    results = []
    for i, jsonl_file in enumerate(jsonl_files, 1):
        logger.info(f"[{i}/{len(jsonl_files)}] Processing {jsonl_file.name}...")
        
        result = convert_jsonl_to_parquet(jsonl_file, output_dir, token_cache)
        results.append({
            'file': jsonl_file.name,
            **result
        })
    
    # Summary
    successful = [r for r in results if r.get('success')]
    total_rows = sum(r.get('row_count', 0) for r in successful)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"BATCH CONVERSION SUMMARY")
    logger.info(f"{'='*70}")
    logger.info(f"Total files: {len(jsonl_files)}")
    logger.info(f"Successful: {len(successful)}")
    logger.info(f"Failed: {len(results) - len(successful)}")
    logger.info(f"Total rows created: {total_rows:,}")
    
    return {
        "total_files": len(jsonl_files),
        "successful": len(successful),
        "failed": len(results) - len(successful),
        "total_rows": total_rows,
        "results": results
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert JSONL.zst files to standardized Parquet")
    parser.add_argument("--jsonl-dir", default="temp_parquet/jsonl_source", help="Directory with JSONL.zst files")
    parser.add_argument("--output-dir", default="temp_parquet/jsonl_converted_parquet", help="Output directory for parquet files")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json", help="Token cache path")
    parser.add_argument("--date", help="Date filter (e.g., 20250930)")
    parser.add_argument("--file", help="Single file to convert (instead of batch)")
    
    args = parser.parse_args()
    
    jsonl_dir = Path(args.jsonl_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.file:
        # Single file conversion
        jsonl_file = Path(args.file)
        token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False) if args.token_cache else None
        result = convert_jsonl_to_parquet(jsonl_file, output_dir, token_cache)
        print(f"\n✅ Result: {result}")
    else:
        # Batch conversion
        batch_convert_jsonl_to_parquet(
            jsonl_dir,
            output_dir,
            args.token_cache,
            args.date
        )

