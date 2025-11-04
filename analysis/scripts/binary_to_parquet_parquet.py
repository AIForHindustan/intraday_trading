#!/usr/bin/env python3
"""
Convert Binary .dat/.bin Files to Parquet (Standard Format)
Then ingest via existing parquet pipeline for speed
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import logging
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from binary_to_parquet.production_binary_converter import (
    ProductionZerodhaBinaryConverter,
    COLUMN_ORDER
)
from token_cache import TokenCacheManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_binary_to_parquet(
    input_file: Path,
    output_dir: Path,
    token_cache: TokenCacheManager,
    metadata_calculator=None,
    jsonl_enricher=None
) -> Dict[str, Any]:
    """
    Convert binary .dat/.bin file to parquet in standard format
    
    Returns:
        Dict with 'success', 'parquet_file', 'row_count', 'packets_processed'
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Output parquet file name (match input name pattern)
    output_file = output_dir / f"{input_file.stem}.parquet"
    
    logger.info(f"Converting {input_file.name} to {output_file.name}")
    
    # Initialize converter (without DB - we'll just parse and convert)
    converter = ProductionZerodhaBinaryConverter(
        db_path=None,  # No DB needed - just parsing
        token_cache=token_cache,
        drop_unknown_tokens=False,
        ensure_schema=False,  # No schema needed
    )
    
    # Parse binary file
    try:
        raw_data = input_file.read_bytes()
        packets = converter._detect_packet_format(raw_data)
        logger.info(f"  Parsed {len(packets):,} packets")
    except Exception as e:
        logger.error(f"Failed to parse {input_file.name}: {e}")
        return {"success": False, "error": str(e), "row_count": 0}
    
    # Enrich packets with JSONL metadata if available
    if jsonl_enricher:
        logger.info(f"  Enriching {len(packets):,} packets with JSONL metadata...")
        enriched_packets = []
        for packet in packets:
            enriched = jsonl_enricher.enrich_packet(packet.copy())
            enriched_packets.append(enriched)
        packets = enriched_packets
        got_metadata = sum(1 for p in packets if p.get('symbol') and not p.get('symbol', '').startswith('UNKNOWN'))
        logger.info(f"  {got_metadata}/{len(packets)} packets now have metadata")
    
    # Process packets and collect records
    all_records = []
    errors = 0
    skipped = 0
    
    # Process in batches for efficiency
    batch_size = 5000
    for batch_start in range(0, len(packets), batch_size):
        batch = packets[batch_start:batch_start + batch_size]
        
        # Enrich packets (no DB connection needed for enrichment - uses token_cache only)
        enriched_records, batch_errors, batch_skipped = converter._enrich_packets(
            batch, input_file, None  # conn=None - we don't need DB for enrichment (uses token_cache)
        )
        
        errors += batch_errors
        skipped += batch_skipped
        all_records.extend(enriched_records)
        
        if (batch_start + batch_size) % 50000 == 0:
            logger.info(f"  Processed {batch_start + batch_size:,}/{len(packets):,} packets ({len(all_records):,} records)...")
    
    if not all_records:
        logger.warning(f"  No valid records extracted from {input_file.name}")
        return {"success": False, "error": "No valid records", "row_count": 0}
    
    # Convert to DataFrame
    logger.info(f"  Creating DataFrame from {len(all_records):,} records...")
    df = pd.DataFrame(all_records)
    
    # Normalize expiry dates to YYYY-MM-DD format (date objects)
    if 'expiry' in df.columns:
        def normalize_expiry(expiry_val):
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
        df['expiry'] = df['expiry'].apply(normalize_expiry)
    
    # Ensure all COLUMN_ORDER columns exist
    for col in COLUMN_ORDER:
        if col not in df.columns:
            # Set appropriate defaults
            if col in ['timestamp', 'exchange_timestamp', 'last_traded_timestamp']:
                df[col] = None
            elif col == 'expiry':
                df[col] = None
            elif col == 'is_expired':
                df[col] = False
            elif 'price' in col or 'strike' in col or col in ['tick_size', 'average_traded_price',
                                                               'rsi_14', 'sma_20', 'ema_12', 'bollinger_upper', 
                                                               'bollinger_middle', 'bollinger_lower', 'macd', 
                                                               'macd_signal', 'macd_histogram', 'delta', 'gamma', 
                                                               'theta', 'vega', 'rho', 'implied_volatility']:
                df[col] = None
            elif col in ['instrument_token', 'exchange_timestamp_ns', 'last_traded_timestamp_ns',
                         'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
                         'open_interest', 'oi_day_high', 'oi_day_low', 'lot_size',
                         *[f'bid_{i}_quantity' for i in range(1, 6)],
                         *[f'ask_{i}_quantity' for i in range(1, 6)]]:
                df[col] = 0
            elif col in [f'bid_{i}_orders' for i in range(1, 6)] + [f'ask_{i}_orders' for i in range(1, 6)]:
                df[col] = 0
            else:
                df[col] = None
    
    # Add processed_at and parser_version if not present
    if 'processed_at' not in df.columns:
        df['processed_at'] = datetime.now()
    if 'parser_version' not in df.columns:
        df['parser_version'] = 'v2.0'
    
    # Reorder columns to match expected order
    all_expected_cols = COLUMN_ORDER + ['processed_at', 'parser_version']
    existing_cols = [col for col in all_expected_cols if col in df.columns]
    df = df[existing_cols]
    
    # Replace NaN with None for proper NULL handling
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].replace([np.nan, float('nan')], None)
            df[col] = df[col].where(df[col].notna(), None)
        elif df[col].dtype == 'object':
            df[col] = df[col].replace(['nan', 'NaN', np.nan], None)
            df[col] = df[col].where(df[col].notna(), None)
    
    # Write to parquet
    logger.info(f"  Writing to {output_file.name}...")
    try:
        df.to_parquet(
            output_file,
            index=False,
            compression='snappy',
            engine='pyarrow'
        )
        
        file_size_mb = output_file.stat().st_size / 1024 / 1024
        logger.info(f"  ✅ Created {output_file.name} ({file_size_mb:.1f} MB, {len(df):,} rows)")
        
        return {
            "success": True,
            "parquet_file": output_file,
            "row_count": len(df),
            "packets_processed": len(packets),
            "errors": errors,
            "skipped": skipped
        }
    except Exception as e:
        logger.error(f"Failed to write parquet: {e}")
        return {"success": False, "error": str(e), "row_count": 0}


def batch_convert_binary_to_parquet(
    date: str,
    crawler: str,
    output_dir: Path = Path("parquet_output/binary_converted"),
    token_cache_path: str = "core/data/token_lookup_enriched.json"
) -> Dict[str, Any]:
    """Convert all binary files for a date to parquet"""
    date_pattern = date.replace('-', '')  # 20251008
    
    # Find files
    if crawler == "intraday_data":
        data_dir = Path("crawlers/raw_data/intraday_data")
        pattern = f"intraday_data_{date_pattern}*.dat"
    elif crawler == "data_mining":
        data_dir = Path("crawlers/raw_data/data_mining")
        pattern = f"*{date_pattern}*.dat"
    elif crawler == "research_data":
        data_dir = Path("research_data")
        pattern = f"*{date_pattern}*.dat"
    elif crawler == "binary_crawler":
        data_dir = Path("crawlers/raw_data/binary_crawler/binary_crawler/raw_binary")
        pattern = f"ticks_{date_pattern}*.bin"
    elif crawler == "zerodha_websocket":
        data_dir = Path("crawlers/raw_data/zerodha_websocket/raw_binary")
        # zerodha_websocket has date subdirectories: raw_binary/YYYYMMDD/ticks_*.bin
        # Search in date subdirectory OR recursively
        date_subdir = data_dir / date_pattern
        if date_subdir.exists():
            pattern = f"ticks_{date_pattern}*.bin"
            binary_files = sorted(date_subdir.glob(pattern))
        else:
            # Fallback: recursive search
            pattern = f"ticks_{date_pattern}*.bin"
            binary_files = sorted(data_dir.rglob(pattern))
    else:
        data_dir = Path(f"crawlers/raw_data/{crawler}")
        pattern = f"*{date_pattern}*.dat"
    
    if not data_dir.exists():
        logger.warning(f"Directory not found: {data_dir}")
        return {"success": False, "error": f"Directory not found: {data_dir}"}
    
    # Use rglob for recursive search to find files in subdirectories (if binary_files not already set)
    if crawler != "zerodha_websocket":
        binary_files = sorted(data_dir.rglob(pattern))
    
    if not binary_files:
        file_ext = ".bin" if crawler in ["binary_crawler", "zerodha_websocket"] else ".dat"
        logger.warning(f"No {file_ext} files found for {date} ({crawler}) in {data_dir}")
        return {"success": False, "error": "No files found", "files_processed": 0}
    
    file_ext = ".bin" if crawler in ["binary_crawler", "zerodha_websocket"] else ".dat"
    logger.info(f"Found {len(binary_files)} {file_ext} files for {date}")
    
    # Load token cache
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
    logger.info(f"Loaded {len(token_cache.token_map):,} instruments")
    
    # Create output directory with date/crawler structure
    output_date_dir = output_dir / crawler / date_pattern
    output_date_dir.mkdir(parents=True, exist_ok=True)
    
    # Convert each file
    parquet_files = []
    total_rows = 0
    files_processed = 0
    files_failed = 0
    
    for i, binary_file in enumerate(binary_files, 1):
        logger.info(f"\n[{i}/{len(binary_files)}] {binary_file.name}")
        
        result = convert_binary_to_parquet(
            binary_file,
            output_date_dir,
            token_cache,
            None
        )
        
        if result.get("success"):
            parquet_files.append(result["parquet_file"])
            total_rows += result["row_count"]
            files_processed += 1
            logger.info(f"  ✅ {result['row_count']:,} rows -> {result['parquet_file'].name}")
        else:
            files_failed += 1
            logger.error(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
    
    logger.info(f"\n{'='*80}")
    logger.info(f"CONVERSION COMPLETE")
    logger.info(f"  Files processed: {files_processed}/{len(binary_files)}")
    logger.info(f"  Files failed: {files_failed}")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Parquet files: {len(parquet_files)}")
    logger.info(f"{'='*80}")
    
    return {
        "success": files_processed > 0,
        "parquet_files": parquet_files,
        "total_rows": total_rows,
        "files_processed": files_processed,
        "files_failed": files_failed,
        "output_dir": str(output_date_dir)
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert binary .dat files to parquet")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--crawler", type=str, default="intraday_data",
                       choices=["intraday_data", "data_mining", "research_data", "binary_crawler", "zerodha_websocket"],
                       help="Crawler name")
    parser.add_argument("--output-dir", type=str, default="parquet_output/binary_converted",
                       help="Output directory for parquet files")
    
    args = parser.parse_args()
    
    # Validate date
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD format")
        sys.exit(1)
    
    batch_convert_binary_to_parquet(args.date, args.crawler, Path(args.output_dir))

