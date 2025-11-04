#!/usr/bin/env python3
"""
Ingest Parquet Files Using DuckDB Native SQL Only
- Uses DuckDB's read_parquet() function
- Joins with token metadata for enrichment
- Single connection, sequential processing (no connection conflicts)
"""

import sys
from pathlib import Path
import json
import duckdb
from typing import List, Optional
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ensure_production_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_token_metadata_into_duckdb(conn: duckdb.DuckDBPyConnection, token_cache: TokenCacheManager) -> str:
    """Load token metadata into DuckDB as a temporary table and return table name"""
    # Build metadata as a list of tuples for DuckDB insertion
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
            
            metadata_rows.append((
                token,
                symbol or info.get("name", ""),
                exchange or info.get("exchange", ""),
                info.get("instrument_type", ""),
                info.get("segment") or info.get("source", ""),
            ))
        except (ValueError, TypeError):
            continue
    
    if not metadata_rows:
        logger.warning("No token metadata to load")
        return None
    
    # Create temporary table
    view_name = "token_metadata_lookup"
    
    try:
        conn.execute(f"DROP TABLE IF EXISTS {view_name}")
    except:
        pass
    
    # Create and populate table
    conn.execute(f"""
        CREATE TEMP TABLE {view_name} (
            token BIGINT,
            symbol VARCHAR,
            exchange VARCHAR,
            instrument_type VARCHAR,
            segment VARCHAR
        )
    """)
    
    conn.executemany(
        f"INSERT INTO {view_name} VALUES (?, ?, ?, ?, ?)",
        metadata_rows
    )
    
    logger.info(f"Loaded {len(metadata_rows):,} token metadata entries into {view_name}")
    return view_name


def ingest_parquet_file_duckdb_native(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    token_metadata_view: Optional[str] = None
) -> dict:
    """Ingest parquet file using DuckDB native read_parquet() with SQL joins"""
    try:
        # First check if file can be read and what schema it has
        try:
            # Try to read first row to check schema
            sample = conn.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 1").fetchone()
            if not sample:
                return {"success": False, "error": "Empty parquet file", "row_count": 0}
            
            # Get column names
            columns = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1").fetchdf()['column_name'].tolist()
            
            # Check if this is already in the correct schema (has instrument_token)
            if 'instrument_token' in columns:
                # Already in correct format - but need to explicitly map columns
                count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}') WHERE instrument_token > 0").fetchone()[0]
                if count == 0:
                    return {"success": False, "error": "No valid rows", "row_count": 0}
                
                # Build explicit column list matching database schema order
                # Parquet has 65 columns, DB has 67 (missing processed_at, parser_version)
                db_columns = [
                    'instrument_token', 'symbol', 'exchange', 'segment', 'instrument_type',
                    'expiry', 'strike_price', 'option_type', 'lot_size', 'tick_size', 'is_expired',
                    'timestamp', 'exchange_timestamp', 'exchange_timestamp_ns',
                    'last_traded_timestamp', 'last_traded_timestamp_ns', 'last_price',
                    'open_price', 'high_price', 'low_price', 'close_price', 'average_traded_price',
                    'volume', 'last_traded_quantity', 'total_buy_quantity', 'total_sell_quantity',
                    'open_interest', 'oi_day_high', 'oi_day_low',
                    'bid_1_price', 'bid_1_quantity', 'bid_1_orders',
                    'bid_2_price', 'bid_2_quantity', 'bid_2_orders',
                    'bid_3_price', 'bid_3_quantity', 'bid_3_orders',
                    'bid_4_price', 'bid_4_quantity', 'bid_4_orders',
                    'bid_5_price', 'bid_5_quantity', 'bid_5_orders',
                    'ask_1_price', 'ask_1_quantity', 'ask_1_orders',
                    'ask_2_price', 'ask_2_quantity', 'ask_2_orders',
                    'ask_3_price', 'ask_3_quantity', 'ask_3_orders',
                    'ask_4_price', 'ask_4_quantity', 'ask_4_orders',
                    'ask_5_price', 'ask_5_quantity', 'ask_5_orders',
                    'packet_type', 'data_quality', 'session_type', 
                    'source_file', 'processing_batch', 'session_id',
                    'processed_at', 'parser_version'  # DB has these, parquet doesn't
                ]
                
                # Select columns that exist in parquet, use defaults for missing ones
                select_cols = []
                for col in db_columns:
                    if col in columns:
                        select_cols.append(f"p.{col}")
                    elif col == 'source_file':
                        # Override with actual file path
                        select_cols.append(f"'{file_path}'")
                    elif col == 'processing_batch':
                        # Override with batch name
                        select_cols.append("'parquet_ingestion_duckdb_native'")
                    elif col == 'processed_at':
                        # Use current timestamp
                        select_cols.append("CURRENT_TIMESTAMP")
                    elif col == 'parser_version':
                        # Use default version
                        select_cols.append("'v2.0'")
                    else:
                        # Use appropriate default based on column type
                        if 'timestamp' in col or col == 'expiry':
                            select_cols.append("NULL")
                        elif col in ['symbol', 'exchange', 'segment', 'instrument_type', 'option_type', 'packet_type', 'data_quality', 'session_type', 'session_id']:
                            select_cols.append("''")
                        elif 'price' in col or 'strike' in col or col in ['tick_size', 'average_traded_price']:
                            select_cols.append("0.0")
                        elif col in ['volume', 'quantity', 'interest', 'high', 'low', 'orders', 'lot_size'] or 'bid_' in col or 'ask_' in col:
                            select_cols.append("0")
                        elif col == 'is_expired':
                            select_cols.append("FALSE")
                        else:
                            select_cols.append("NULL")
                
                # Direct insert - already in correct format
                insert_query = f"""
                    INSERT OR REPLACE INTO tick_data_corrected
                    SELECT {', '.join(select_cols)}
                    FROM read_parquet('{file_path}') p
                    WHERE instrument_token > 0
                """
                
                conn.execute(insert_query)
                return {
                    "success": True,
                    "row_count": count,
                    "original_rows": count
                }
        except Exception as e:
            logger.error(f"Error reading parquet {file_path}: {e}")
            return {"success": False, "error": str(e), "row_count": 0}
        
        # If we get here, need to transform (has 'token' instead of 'instrument_token')
        # Build INSERT query with metadata enrichment using SQL
        if token_metadata_view:
            insert_query = f"""
                INSERT OR REPLACE INTO tick_data_corrected
                SELECT
                    CAST(p.token AS BIGINT) as instrument_token,
                    COALESCE(m.symbol, '') as symbol,
                    COALESCE(m.exchange, '') as exchange,
                    COALESCE(m.segment, '') as segment,
                    COALESCE(m.instrument_type, '') as instrument_type,
                    NULL as expiry,
                    NULL as strike_price,
                    NULL as option_type,
                    NULL as lot_size,
                    NULL as tick_size,
                    CAST(p.timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(p.timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(p.timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(p.timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(p.last_price AS DOUBLE) as last_price,
                    CAST(COALESCE(p.open, p.last_price) AS DOUBLE) as open_price,
                    CAST(COALESCE(p.high, p.last_price) AS DOUBLE) as high_price,
                    CAST(COALESCE(p.low, p.last_price) AS DOUBLE) as low_price,
                    CAST(COALESCE(p.close, p.last_price) AS DOUBLE) as close_price,
                    CAST(p.last_price AS DOUBLE) as average_traded_price,
                    CAST(COALESCE(p.volume, 0) AS BIGINT) as volume,
                    CAST(COALESCE(p.total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(p.total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(p.oi, 0) AS BIGINT) as open_interest,
                    NULL as oi_day_high,
                    NULL as oi_day_low,
                    CAST(COALESCE(p.bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(p.bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    0 as bid_1_orders,
                    NULL as bid_2_price,
                    0 as bid_2_quantity,
                    0 as bid_2_orders,
                    NULL as bid_3_price,
                    0 as bid_3_quantity,
                    0 as bid_3_orders,
                    NULL as bid_4_price,
                    0 as bid_4_quantity,
                    0 as bid_4_orders,
                    NULL as bid_5_price,
                    0 as bid_5_quantity,
                    0 as bid_5_orders,
                    CAST(COALESCE(p.ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(p.ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as ask_1_orders,
                    NULL as ask_2_price,
                    0 as ask_2_quantity,
                    0 as ask_2_orders,
                    NULL as ask_3_price,
                    0 as ask_3_quantity,
                    0 as ask_3_orders,
                    NULL as ask_4_price,
                    0 as ask_4_quantity,
                    0 as ask_4_orders,
                    NULL as ask_5_price,
                    0 as ask_5_quantity,
                    0 as ask_5_orders,
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
                    NULL as expiry,
                    NULL as strike_price,
                    NULL as option_type,
                    NULL as lot_size,
                    NULL as tick_size,
                    CAST(timestamp AS TIMESTAMP) as exchange_timestamp,
                    CAST(timestamp AS BIGINT) as exchange_timestamp_ns,
                    CAST(timestamp AS TIMESTAMP) as last_traded_timestamp,
                    CAST(timestamp AS BIGINT) as last_traded_timestamp_ns,
                    CURRENT_TIMESTAMP as timestamp,
                    CAST(last_price AS DOUBLE) as last_price,
                    CAST(COALESCE(open, last_price) AS DOUBLE) as open_price,
                    CAST(COALESCE(high, last_price) AS DOUBLE) as high_price,
                    CAST(COALESCE(low, last_price) AS DOUBLE) as low_price,
                    CAST(COALESCE(close, last_price) AS DOUBLE) as close_price,
                    CAST(last_price AS DOUBLE) as average_traded_price,
                    CAST(COALESCE(volume, 0) AS BIGINT) as volume,
                    CAST(COALESCE(total_buy_quantity, 0) AS BIGINT) as total_buy_quantity,
                    CAST(COALESCE(total_sell_quantity, 0) AS BIGINT) as total_sell_quantity,
                    CAST(COALESCE(oi, 0) AS BIGINT) as open_interest,
                    NULL as oi_day_high,
                    NULL as oi_day_low,
                    CAST(COALESCE(bid_price_1, 0) AS DOUBLE) as bid_1_price,
                    CAST(COALESCE(bid_quantity_1, 0) AS BIGINT) as bid_1_quantity,
                    0 as bid_1_orders,
                    NULL as bid_2_price,
                    0 as bid_2_quantity,
                    0 as bid_2_orders,
                    NULL as bid_3_price,
                    0 as bid_3_quantity,
                    0 as bid_3_orders,
                    NULL as bid_4_price,
                    0 as bid_4_quantity,
                    0 as bid_4_orders,
                    NULL as bid_5_price,
                    0 as bid_5_quantity,
                    0 as bid_5_orders,
                    CAST(COALESCE(ask_price_1, 0) AS DOUBLE) as ask_1_price,
                    CAST(COALESCE(ask_quantity_1, 0) AS BIGINT) as ask_1_quantity,
                    0 as ask_1_orders,
                    NULL as ask_2_price,
                    0 as ask_2_quantity,
                    0 as ask_2_orders,
                    NULL as ask_3_price,
                    0 as ask_3_quantity,
                    0 as ask_3_orders,
                    NULL as ask_4_price,
                    0 as ask_4_quantity,
                    0 as ask_4_orders,
                    NULL as ask_5_price,
                    0 as ask_5_quantity,
                    0 as ask_5_orders,
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
    excluded_patterns = ['.venv', '__pycache__', 'site-packages', 'tests', 'test_', 'backtesting', 'binary_to_parquet']
    
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
    
    parser = argparse.ArgumentParser(description="Ingest parquet files using DuckDB native SQL")
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
    
    # Single DuckDB connection for all files
    conn = duckdb.connect(args.db)
    ensure_production_schema(conn)
    
    # Load token metadata into DuckDB
    token_metadata_view = load_token_metadata_into_duckdb(conn, token_cache)
    
    # Ingest files sequentially using the same connection
    total_rows = 0
    files_completed = 0
    errors = 0
    
    for file_path in file_paths:
        try:
            result = ingest_parquet_file_duckdb_native(conn, file_path, token_metadata_view)
            if result.get("success"):
                total_rows += result.get("row_count", 0)
                files_completed += 1
                # Log progress more frequently
                if files_completed <= 50 or files_completed % 50 == 0:
                    logger.info(f"Progress: {files_completed}/{len(file_paths)} files, {total_rows:,} rows")
            else:
                errors += 1
                if errors <= 10:
                    logger.error(f"Failed {file_path.name}: {result.get('error')}")
        except Exception as e:
            errors += 1
            logger.error(f"Exception processing {file_path.name}: {e}", exc_info=True)
            if errors > 100:
                logger.error("Too many errors, stopping")
                break
    
    conn.close()
    
    logger.info(f"✅ Complete: {files_completed} files, {total_rows:,} rows, {errors} errors")


if __name__ == "__main__":
    main()

