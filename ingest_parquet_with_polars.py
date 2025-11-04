#!/usr/bin/env python3
"""
Ingest Parquet Files Using Polars + DuckDB Native SQL
- Uses Polars for fast parquet reading
- Enriches with token metadata
- Uses DuckDB native SQL for efficient insertion
"""

import sys
from pathlib import Path
import json
import duckdb
import polars as pl
from typing import List, Optional
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ensure_production_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_token_metadata_table(db_path: str, token_cache: TokenCacheManager) -> str:
    """Load token metadata into DuckDB as a temporary table and return table name"""
    conn = duckdb.connect(db_path)
    
    # Build metadata DataFrame
    metadata_rows = []
    for token_str, info in token_cache.token_map.items():
        try:
            token = int(token_str)
            
            # Extract symbol from 'key' field or 'name'
            key_field = info.get("key", "")
            if ":" in key_field:
                symbol = key_field.split(":", 1)[1]
                exchange = key_field.split(":", 1)[0]
            else:
                symbol = info.get("name", "")
                exchange = info.get("exchange", "")
            
            metadata_rows.append({
                "token": token,
                "symbol": symbol or info.get("name", ""),
                "exchange": exchange or info.get("exchange", ""),
                "instrument_type": info.get("instrument_type", ""),
                "segment": info.get("segment") or info.get("source", ""),
            })
        except (ValueError, TypeError):
            continue
    
    if not metadata_rows:
        logger.warning("No token metadata to load")
        return None
    
    # Create Polars DataFrame and register with DuckDB
    metadata_df = pl.DataFrame(metadata_rows)
    view_name = "token_metadata_lookup"
    
    try:
        conn.unregister(view_name)
    except:
        pass
    
    conn.register(view_name, metadata_df)
    logger.info(f"Loaded {len(metadata_rows):,} token metadata entries into {view_name}")
    
    return view_name


def ingest_parquet_file_duckdb_native(
    file_path: Path,
    db_path: str,
    token_metadata_view: Optional[str] = None
) -> dict:
    """Ingest parquet file using DuckDB native read_parquet() with metadata enrichment"""
    try:
        conn = duckdb.connect(db_path)
        ensure_production_schema(conn)
        
        # Read parquet directly with DuckDB
        # First check what columns exist
        sample_query = f"""
            SELECT * FROM read_parquet('{file_path}')
            LIMIT 1
        """
        
        try:
            sample = conn.execute(sample_query).fetchone()
            if not sample:
                return {"success": False, "error": "Empty parquet file", "row_count": 0}
        except Exception as e:
            logger.error(f"Error reading parquet {file_path}: {e}")
            return {"success": False, "error": str(e), "row_count": 0}
        
        # Get column info
        cols = conn.execute(sample_query.replace("SELECT *", "SELECT *")).columns
        
        # Map parquet columns to DuckDB schema
        # Parquet has: token, timestamp, last_price, volume, etc.
        # Need: instrument_token, symbol, exchange, exchange_timestamp, etc.
        
        # Build INSERT query with metadata enrichment
        if token_metadata_view and 'token' in [c.lower() for c in cols]:
            # Join with metadata to enrich
            insert_query = f"""
                INSERT OR REPLACE INTO tick_data_corrected
                SELECT
                    CAST(p.token AS BIGINT) as instrument_token,
                    COALESCE(m.symbol, '') as symbol,
                    COALESCE(m.exchange, '') as exchange,
                    COALESCE(m.segment, '') as segment,
                    COALESCE(m.instrument_type, '') as instrument_type,
                    CAST(p.timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(p.timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(p.timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(p.timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(p.last_price AS DOUBLE) as last_price,
                    CAST(p.open AS DOUBLE) as open_price,
                    CAST(p.high AS DOUBLE) as high_price,
                    CAST(p.low AS DOUBLE) as low_price,
                    CAST(p.close AS DOUBLE) as close_price,
                    CAST(p.volume AS BIGINT) as volume,
                    CAST(COALESCE(p.total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(p.total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(p.oi, 0) AS BIGINT) as open_interest,
                    CAST(COALESCE(p.bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(p.bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    CAST(COALESCE(p.ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(p.ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as bid_1_orders,
                    0 as ask_1_orders,
                    'full' as packet_type,
                    'complete' as data_quality,
                    'regular' as session_type,
                    '{file_path}' as source_file,
                    'parquet_ingestion_duckdb_native' as processing_batch
                FROM read_parquet('{file_path}') p
                LEFT JOIN {token_metadata_view} m ON CAST(p.token AS BIGINT) = m.token
                WHERE CAST(p.token AS BIGINT) > 0
            """
        else:
            # No metadata enrichment
            insert_query = f"""
                INSERT OR REPLACE INTO tick_data_corrected
                SELECT
                    CAST(token AS BIGINT) as instrument_token,
                    '' as symbol,
                    '' as exchange,
                    '' as segment,
                    '' as instrument_type,
                    CAST(timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(last_price AS DOUBLE) as last_price,
                    CAST(open AS DOUBLE) as open_price,
                    CAST(high AS DOUBLE) as high_price,
                    CAST(low AS DOUBLE) as low_price,
                    CAST(close AS DOUBLE) as close_price,
                    CAST(volume AS BIGINT) as volume,
                    CAST(COALESCE(total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(oi, 0) AS BIGINT) as open_interest,
                    CAST(COALESCE(bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    CAST(COALESCE(ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as bid_1_orders,
                    0 as ask_1_orders,
                    'full' as packet_type,
                    'complete' as data_quality,
                    'regular' as session_type,
                    '{file_path}' as source_file,
                    'parquet_ingestion_duckdb_native' as processing_batch
                FROM read_parquet('{file_path}')
                WHERE CAST(token AS BIGINT) > 0
            """
        
        # Get row count before insert
        count_query = f"SELECT COUNT(*) FROM read_parquet('{file_path}') WHERE CAST(token AS BIGINT) > 0"
        row_count = conn.execute(count_query).fetchone()[0]
        
        # Execute insert
        conn.execute(insert_query)
        
        conn.close()
        
        return {
            "success": True,
            "row_count": row_count,
            "original_rows": row_count
        }
        
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "row_count": 0
        }


def get_pending_parquet_files(db_path: str, search_dirs: List[str] = None) -> List[Path]:
    """Get list of parquet files that haven't been ingested"""
    if search_dirs is None:
        search_dirs = ["real_parquet_data", "parquet_output", "binary_to_parquet"]
    
    conn = duckdb.connect(db_path)
    
    # Get ingested parquet files
    ingested_parquet = conn.execute("""
        SELECT DISTINCT source_file
        FROM tick_data_corrected
        WHERE source_file LIKE '%.parquet'
    """).fetchall()
    
    ingested_paths = {Path(row[0]) for row in ingested_parquet if row[0]}
    conn.close()
    
    # Find parquet files on disk
    excluded_patterns = ['.venv', '__pycache__', 'site-packages', 'tests', 'test_', 'backtesting']
    
    all_parquet = set()
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            for f in dir_path.rglob('*.parquet'):
                if any(excluded in str(f) for excluded in excluded_patterns):
                    continue
                if f.exists():
                    all_parquet.add(f)
    
    # Find missing ones
    missing_parquet = all_parquet - ingested_paths
    return sorted(list(missing_parquet))


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest parquet files using Polars + DuckDB native SQL")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--token-cache", default="zerodha_token_list/all_extracted_tokens_merged.json", 
                       help="Token lookup path")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search")
    parser.add_argument("--auto", action="store_true", help="Auto-find and ingest pending files")
    parser.add_argument("--limit", type=int, help="Limit number of files")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    
    args = parser.parse_args()
    
    if not args.auto:
        parser.print_help()
        return
    
    # Load token cache
    token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False)
    logger.info(f"Loaded token cache: {len(token_cache.token_map):,} tokens")
    
    # Get pending files
    search_dirs = args.search_dirs or ["real_parquet_data", "parquet_output", "binary_to_parquet"]
    file_paths = get_pending_parquet_files(args.db, search_dirs)
    
    if args.limit:
        file_paths = file_paths[:args.limit]
    
    logger.info(f"Found {len(file_paths):,} pending parquet files")
    
    if not file_paths:
        logger.info("No pending files")
        return
    
    if args.dry_run:
        logger.info("DRY RUN - Would ingest these files")
        for f in file_paths[:10]:
            logger.info(f"  {f}")
        return
    
    # Load token metadata into DuckDB
    token_metadata_view = load_token_metadata_table(args.db, token_cache)
    
    # Ingest files sequentially (DuckDB handles this efficiently)
    total_rows = 0
    files_completed = 0
    errors = 0
    
    for file_path in file_paths:
        result = ingest_parquet_file_duckdb_native(file_path, args.db, token_metadata_view)
        if result.get("success"):
            total_rows += result.get("row_count", 0)
            files_completed += 1
            if files_completed % 100 == 0:
                logger.info(f"Progress: {files_completed}/{len(file_paths)} files, {total_rows:,} rows")
        else:
            errors += 1
            if errors <= 10:
                logger.error(f"Failed {file_path.name}: {result.get('error')}")
    
    logger.info(f"✅ Complete: {files_completed} files, {total_rows:,} rows, {errors} errors")


if __name__ == "__main__":
    main()

