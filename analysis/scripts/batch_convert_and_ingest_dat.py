#!/usr/bin/env python3
"""
Convert Binary .dat files to Parquet, then ingest via fast parquet pipeline
Two-step process for maximum speed:
1. Convert .dat -> .parquet (fast parsing, no DB overhead)
2. Ingest parquet -> DuckDB (DuckDB native parquet reader, very fast)
"""

import sys
from pathlib import Path
import duckdb
import logging
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from binary_to_parquet_parquet import batch_convert_binary_to_parquet
from process_parquet_to_production import process_parquet_file
from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ensure_production_schema
from binary_to_parquet.enhanced_metadata_calculator import EnhancedMetadataCalculator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_and_ingest(
    date: str,
    crawler: str,
    db_path: str = "tick_data_production.db",
    parquet_output_dir: Path = Path("parquet_output/binary_converted"),
    token_cache_path: str = "core/data/token_lookup_enriched.json",
    skip_parquet_if_exists: bool = True
) -> Dict[str, Any]:
    """
    Step 1: Convert binary .dat files to parquet
    Step 2: Ingest parquet files into DuckDB
    """
    logger.info("="*80)
    logger.info(f"BINARY TO PARQUET TO DUCKDB PIPELINE")
    logger.info(f"Date: {date} | Crawler: {crawler}")
    logger.info("="*80)
    
    # Step 1: Convert .dat to .parquet
    logger.info("\n📦 STEP 1: Converting .dat files to parquet...")
    conversion_result = batch_convert_binary_to_parquet(
        date, crawler, parquet_output_dir, token_cache_path
    )
    
    if not conversion_result.get("success"):
        logger.error("❌ Conversion failed. Aborting ingestion.")
        return conversion_result
    
    parquet_files = conversion_result.get("parquet_files", [])
    if not parquet_files:
        logger.warning("⚠️  No parquet files created. Nothing to ingest.")
        return conversion_result
    
    logger.info(f"\n✅ Conversion complete: {len(parquet_files)} parquet files created")
    
    # Step 2: Ingest parquet files into DuckDB
    logger.info(f"\n💾 STEP 2: Ingesting {len(parquet_files)} parquet files into DuckDB...")
    
    # Connect to database
    conn = duckdb.connect(db_path)
    ensure_production_schema(conn)
    
    # Load token cache and metadata calculator
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
    metadata_calculator = EnhancedMetadataCalculator()
    
    # Process each parquet file
    total_rows = 0
    total_errors = 0
    total_skipped = 0
    total_duplicates = 0
    files_ingested = 0
    files_failed = 0
    
    for i, parquet_file in enumerate(parquet_files, 1):
        logger.info(f"\n[{i}/{len(parquet_files)}] Ingesting: {parquet_file.name}")
        
        try:
            result = process_parquet_file(
                parquet_file,
                conn,
                token_cache,
                metadata_calculator,
                batch_size=5000
            )
            
            if result.get("success"):
                rows = result.get("row_count", 0)
                total_rows += rows
                total_errors += result.get("errors", 0)
                total_skipped += result.get("skipped", 0)
                total_duplicates += result.get("duplicates_dropped", 0)
                files_ingested += 1
                
                msg_parts = []
                if result.get("errors", 0) > 0:
                    msg_parts.append(f"{result.get('errors')} errors")
                if result.get("skipped", 0) > 0:
                    msg_parts.append(f"{result.get('skipped')} skipped")
                if result.get("duplicates_dropped", 0) > 0:
                    msg_parts.append(f"{result.get('duplicates_dropped')} duplicates")
                
                logger.info(f"  ✅ {rows:,} rows" + (f" ({', '.join(msg_parts)})" if msg_parts else ""))
            else:
                files_failed += 1
                logger.error(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
        except Exception as e:
            files_failed += 1
            total_errors += 1
            logger.error(f"  ❌ Exception: {e}", exc_info=True)
    
    conn.close()
    
    # Final summary
    logger.info("\n" + "="*80)
    logger.info("PIPELINE COMPLETE")
    logger.info("="*80)
    logger.info(f"  Conversion:")
    logger.info(f"    Parquet files created: {len(parquet_files)}")
    logger.info(f"    Conversion errors: {conversion_result.get('files_failed', 0)}")
    logger.info(f"  Ingestion:")
    logger.info(f"    Files ingested: {files_ingested}/{len(parquet_files)}")
    logger.info(f"    Files failed: {files_failed}")
    logger.info(f"    Total rows inserted: {total_rows:,}")
    logger.info(f"    Total errors: {total_errors}")
    logger.info(f"    Total skipped: {total_skipped:,}")
    logger.info(f"    Total duplicates dropped: {total_duplicates:,}")
    logger.info("="*80)
    
    return {
        "success": files_ingested > 0,
        "conversion": conversion_result,
        "ingestion": {
            "files_ingested": files_ingested,
            "files_failed": files_failed,
            "total_rows": total_rows,
            "total_errors": total_errors,
            "total_skipped": total_skipped,
            "total_duplicates": total_duplicates
        }
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert binary .dat files to parquet, then ingest into DuckDB"
    )
    parser.add_argument("--date", type=str, required=True, 
                       help="Date in YYYY-MM-DD format (e.g., 2025-10-08)")
    parser.add_argument("--crawler", type=str, default="intraday_data",
                       choices=["intraday_data", "data_mining", "research_data", "binary_crawler", "zerodha_websocket"],
                       help="Crawler name")
    parser.add_argument("--db", type=str, default="tick_data_production.db",
                       help="Path to DuckDB database")
    parser.add_argument("--parquet-output-dir", type=str, 
                       default="parquet_output/binary_converted",
                       help="Output directory for intermediate parquet files")
    
    args = parser.parse_args()
    
    # Validate date
    try:
        from datetime import datetime
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD format")
        sys.exit(1)
    
    result = convert_and_ingest(
        args.date,
        args.crawler,
        args.db,
        Path(args.parquet_output_dir)
    )
    
    if not result.get("success"):
        sys.exit(1)

