#!/usr/bin/env python3
"""
Batch Process .dat Files to Production Database
Processes binary .dat files day by day using ProductionZerodhaBinaryConverter
"""

import sys
from pathlib import Path
import duckdb
import argparse
import logging
from datetime import datetime
from collections import Counter

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
from token_cache import TokenCacheManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_dat_files(date: str, crawler: str) -> list[Path]:
    """Find .dat files for a specific date and crawler"""
    date_pattern = date.replace('-', '')  # 20251008 format
    
    if crawler == "intraday_data":
        data_dir = Path("crawlers/raw_data/intraday_data")
        pattern = f"intraday_data_{date_pattern}*.dat"
    elif crawler == "data_mining":
        data_dir = Path("crawlers/raw_data/data_mining")
        pattern = f"*{date_pattern}*.dat"
    elif crawler == "research_data":
        data_dir = Path("research_data")
        pattern = f"*{date_pattern}*.dat"
    else:
        data_dir = Path(f"crawlers/raw_data/{crawler}")
        pattern = f"*{date_pattern}*.dat"
    
    if not data_dir.exists():
        logger.warning(f"Directory not found: {data_dir}")
        return []
    
    files = sorted(data_dir.glob(pattern))
    logger.info(f"Found {len(files)} .dat files in {data_dir}")
    return files


def batch_process_dat_files(date: str, crawler: str, db_path: str = "tick_data_production.db", dry_run: bool = False):
    """Process all .dat files for a specific date and crawler"""
    print("=" * 80)
    print(f"BATCH PROCESSING .DAT FILES")
    print(f"Date: {date} | Crawler: {crawler}")
    print("=" * 80)
    
    # Find files
    dat_files = find_dat_files(date, crawler)
    
    if not dat_files:
        print(f"\n❌ No .dat files found for {date} ({crawler})")
        return
    
    print(f"\n📋 Found {len(dat_files)} files to process")
    
    if dry_run:
        print("\n🔍 DRY RUN - Files that would be processed:")
        for f in dat_files[:10]:
            print(f"   {f.name}")
        if len(dat_files) > 10:
            print(f"   ... and {len(dat_files) - 10} more")
        return
    
    # Initialize converter
    print(f"\n🔧 Initializing converter...")
    token_cache = TokenCacheManager(
        cache_path='core/data/token_lookup_enriched.json',
        verbose=False
    )
    print(f"   ✅ Token cache loaded: {len(token_cache.token_map):,} instruments")
    
    converter = ProductionZerodhaBinaryConverter(
        db_path=db_path,
        token_cache=token_cache,
        drop_unknown_tokens=False,
    )
    
    # Connect to database for tracking
    conn = duckdb.connect(db_path)
    
    # Process files
    total_packets = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0
    files_processed = 0
    files_failed = 0
    
    print(f"\n⏳ Processing {len(dat_files)} files...")
    
    for i, file_path in enumerate(dat_files, 1):
        print(f"\n[{i}/{len(dat_files)}] Processing: {file_path.name}")
        
        # Check if already processed
        already_count = conn.execute(f"""
            SELECT COUNT(*) FROM tick_data_corrected 
            WHERE source_file = '{file_path}'
        """).fetchone()[0]
        
        if already_count > 0:
            print(f"   ⏭️  Already processed ({already_count:,} rows), skipping...")
            continue
        
        try:
            import time
            start_time = time.time()
            result = converter.process_file_with_metadata_capture(file_path)
            elapsed = time.time() - start_time
            
            total_packets += result.total_packets
            total_inserted += result.successful_inserts
            
            # Get actual count from database
            after_count = conn.execute(f"""
                SELECT COUNT(*) FROM tick_data_corrected 
                WHERE source_file = '{file_path}'
            """).fetchone()[0]
            
            print(f"   ✅ {result.total_packets:,} packets, {after_count:,} rows inserted ({elapsed:.1f}s)")
            
            if result.total_packets > 0:
                success_rate = (result.successful_inserts / result.total_packets) * 100
                pps = result.total_packets / elapsed if elapsed > 0 else 0
                print(f"      Success rate: {success_rate:.1f}% | Speed: {pps:,.0f} packets/s")
            
            files_processed += 1
            
        except Exception as e:
            files_failed += 1
            total_errors += 1
            logger.error(f"Failed to process {file_path.name}: {e}", exc_info=True)
            print(f"   ❌ FAILED: {e}")
    
    conn.close()
    
    # Final summary
    print("\n" + "=" * 80)
    print("BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"   Files processed: {files_processed}/{len(dat_files)}")
    print(f"   Files failed: {files_failed}")
    print(f"   Total packets parsed: {total_packets:,}")
    print(f"   Total rows inserted: {total_inserted:,}")
    print(f"   Success rate: {files_processed/len(dat_files)*100:.1f}%")
    
    # Database statistics
    conn = duckdb.connect(db_path)
    db_stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT source_file) as unique_files,
            COUNT(DISTINCT instrument_token) as unique_instruments
        FROM tick_data_corrected
        WHERE source_file LIKE '%{date.replace('-', '')}%'
          AND source_file LIKE '%{crawler}%'
    """).fetchone()
    
    print(f"\n📊 Database Statistics (for {date}):")
    print(f"   Total rows: {db_stats[0]:,}")
    print(f"   Unique files: {db_stats[1]:,}")
    print(f"   Unique instruments: {db_stats[2]:,}")
    
    conn.close()
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Batch process .dat files for a specific date and crawler",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=str, default="2025-10-08",
                       help="Date in YYYY-MM-DD format (default: 2025-10-08)")
    parser.add_argument("--crawler", type=str, default="intraday_data",
                       choices=["intraday_data", "data_mining", "research_data"],
                       help="Crawler name (default: intraday_data)")
    parser.add_argument("--db", type=str, default="tick_data_production.db",
                       help="Database path (default: tick_data_production.db)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be processed without actually processing")
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD format (e.g., 2025-10-08)")
        return
    
    batch_process_dat_files(args.date, args.crawler, args.db, args.dry_run)


if __name__ == "__main__":
    main()

