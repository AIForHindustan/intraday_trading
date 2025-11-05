#!/usr/bin/env python3
"""
Retry failed parquet file ingestion
Recovers corrupted files and re-ingests them
"""

import sys
from pathlib import Path
import logging
from typing import List
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingest_parquet_duckdb_native import (
    enrich_parquet_file_worker,
    bulk_ingest_enriched_parquets,
    get_pending_parquet_files
)
import duckdb
from binary_to_parquet.production_binary_converter import ensure_production_schema
from concurrent.futures import ProcessPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_failed_files_from_log(log_file: str) -> List[str]:
    """Extract failed file paths from ingestion log"""
    failed_files = []
    try:
        with open(log_file, 'r') as f:
            for line in f:
                if "Failed to enrich" in line:
                    # Extract file path from log line
                    # Format: "WARNING:__main__:  Failed to enrich <path>: <error>"
                    parts = line.split("Failed to enrich", 1)
                    if len(parts) > 1:
                        file_part = parts[1].split(":", 1)[0].strip()
                        if file_part:
                            failed_files.append(file_part)
    except FileNotFoundError:
        logger.warning(f"Log file not found: {log_file}")
    
    return sorted(set(failed_files))


def recover_and_retry_failed_files(
    failed_files: List[str],
    db_path: str,
    token_cache_path: str,
    workers: int = 3
) -> dict:
    """Recover and retry failed parquet files"""
    
    logger.info(f"🔄 Retrying {len(failed_files):,} failed files with recovery...")
    
    # Step 1: Re-enrich with recovery
    enriched_files = []
    still_failed = []
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(enrich_parquet_file_worker, str(f), token_cache_path): f 
            for f in failed_files
        }
        
        for future in as_completed(futures):
            result = future.result()
            if result.get("success"):
                enriched_files.append(result["enriched_file"])
                logger.info(f"  ✅ Recovered: {Path(result['file']).name} -> {result['rows']} rows")
            else:
                still_failed.append({
                    "file": result.get('file', 'unknown'),
                    "error": result.get('error', 'unknown')
                })
                logger.warning(f"  ❌ Still failed: {Path(result.get('file', 'unknown')).name}: {result.get('error', 'unknown')[:80]}")
    
    logger.info(f"✅ Recovered {len(enriched_files):,} files")
    logger.info(f"❌ Still failed: {len(still_failed):,} files (severely corrupted)")
    
    if not enriched_files:
        return {
            "success": False,
            "error": "No files recovered",
            "recovered": 0,
            "still_failed": still_failed
        }
    
    # Step 2: Bulk ingest recovered files
    logger.info(f"📥 Ingesting {len(enriched_files):,} recovered files...")
    
    conn = duckdb.connect(db_path)
    ensure_production_schema(conn)
    
    # Get DB columns
    db_columns = [row[0] for row in conn.execute("DESCRIBE tick_data_corrected").fetchall()]
    
    ingest_result = bulk_ingest_enriched_parquets(conn, enriched_files, db_columns)
    
    conn.close()
    
    # Cleanup enriched files
    for enriched_file in enriched_files:
        try:
            Path(enriched_file).unlink()
        except:
            pass
    
    return {
        "success": ingest_result.get("success", False),
        "recovered_files": len(enriched_files),
        "total_files": len(failed_files),
        "ingested_rows": ingest_result.get("row_count", 0),
        "still_failed": still_failed
    }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Retry failed parquet file ingestion with recovery")
    parser.add_argument("--db", default="analysis/tick_data_production.db", help="Database path")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json",
                       help="Token lookup path")
    parser.add_argument("--log-file", default="/tmp/ingest_intraday_enriched.log",
                       help="Log file to extract failed files from")
    parser.add_argument("--failed-files", type=str, help="File containing list of failed files (one per line)")
    parser.add_argument("--workers", type=int, default=3, help="Number of parallel workers")
    
    args = parser.parse_args()
    
    # Get failed files
    if args.failed_files:
        failed_files = [Path(f.strip()) for f in open(args.failed_files).readlines() if f.strip()]
    else:
        failed_files_str = extract_failed_files_from_log(args.log_file)
        failed_files = [Path(f) for f in failed_files_str if Path(f).exists()]
    
    if not failed_files:
        logger.info("No failed files found")
        return
    
    logger.info(f"Found {len(failed_files):,} failed files to retry")
    
    # Retry with recovery
    result = recover_and_retry_failed_files(
        failed_files,
        args.db,
        args.token_cache,
        args.workers
    )
    
    print("\n" + "=" * 80)
    print("RETRY RESULTS")
    print("=" * 80)
    print(f"Total failed files: {result['total_files']:,}")
    print(f"Recovered files: {result['recovered_files']:,}")
    print(f"Still failed (severely corrupted): {len(result.get('still_failed', [])):,}")
    print(f"Ingested rows: {result['ingested_rows']:,}")
    print(f"Success: {result['success']}")
    
    if result.get('still_failed'):
        print(f"\n⚠️  Severely corrupted files (cannot be recovered):")
        for failed in result['still_failed'][:10]:  # Show first 10
            print(f"   - {Path(failed['file']).name}: {failed['error'][:60]}")
        if len(result['still_failed']) > 10:
            print(f"   ... and {len(result['still_failed']) - 10} more")
    
    print("=" * 80)
    
    # Save still_failed to a file for reference
    if result.get('still_failed'):
        failed_file = Path(args.db).parent / "severely_corrupted_files.json"
        with open(failed_file, 'w') as f:
            json.dump(result['still_failed'], f, indent=2)
        print(f"\n📄 List of severely corrupted files saved to: {failed_file}")


if __name__ == "__main__":
    main()

