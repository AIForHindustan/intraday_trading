#!/usr/bin/env python3
"""
Batch process parquet files from data_mining crawler for October 27th
"""

import sys
from pathlib import Path
import duckdb
from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ensure_production_schema
from process_parquet_to_production import process_parquet_file, _load_raw_token_lookup
from binary_to_parquet.enhanced_metadata_calculator import EnhancedMetadataCalculator

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch process parquet files")
    parser.add_argument("--date", type=str, default="20251028", help="Date in YYYYMMDD format (default: 20251028)")
    parser.add_argument("--crawler", type=str, default="data_mining", help="Crawler name (default: data_mining)")
    
    args = parser.parse_args()
    
    # Find parquet files for specified date
    if args.crawler == "data_mining":
        data_dir = Path("crawlers/raw_data/data_mining")
    elif args.crawler == "research_data":
        data_dir = Path("research_data")
    elif args.crawler == "intraday_data":
        data_dir = Path("crawlers/raw_data/intraday_data")
    else:
        data_dir = Path(f"crawlers/raw_data/{args.crawler}")
    
    parquet_files = sorted(data_dir.glob(f"*{args.date}*.parquet"))
    
    print("=" * 80)
    print(f"BATCH PROCESSING: {args.crawler} PARQUET FILES - {args.date}")
    print("=" * 80)
    print(f"\n📁 Found {len(parquet_files)} parquet files to process")
    
    if len(parquet_files) == 0:
        print("❌ No files found")
        return
    
    # Load token cache
    print("\n📋 Loading token cache...")
    token_cache = TokenCacheManager(
        cache_path="core/data/token_lookup_enriched.json",
        verbose=True
    )
    
    # Load raw token lookup for option parsing
    _load_raw_token_lookup(str(token_cache.cache_path))
    
    # Initialize metadata calculator
    metadata_calculator = EnhancedMetadataCalculator()
    
    # Connect to database
    print("\n🔌 Connecting to database...")
    db_path = "tick_data_production.db"
    conn = duckdb.connect(db_path)
    ensure_production_schema(conn)
    
    # Process files
    total_rows = 0
    total_errors = 0
    total_skipped = 0
    total_duplicates = 0
    files_processed = 0
    batch_size = 5000
    
    print(f"\n🚀 Processing {len(parquet_files)} files...")
    print("=" * 80)
    
    for idx, file_path in enumerate(parquet_files, 1):
        print(f"\n[{idx:3d}/{len(parquet_files)}] {file_path.name}", end=" ... ", flush=True)
        
        try:
            result = process_parquet_file(
                file_path,
                conn,
                token_cache,
                metadata_calculator,
                batch_size=batch_size
            )
            
            if result.get("success"):
                rows = result.get("row_count", 0)
                total_rows += rows
                files_processed += 1
                errors = result.get("errors", 0)
                skipped = result.get("skipped", 0)
                duplicates = result.get("duplicates_dropped", 0)
                total_errors += errors
                total_skipped += skipped
                total_duplicates += duplicates
                msg_parts = []
                if errors > 0:
                    msg_parts.append(f"{errors} errors")
                if skipped > 0:
                    msg_parts.append(f"{skipped} skipped")
                if duplicates > 0:
                    msg_parts.append(f"{duplicates} duplicates")
                print(f"✅ {rows:,} rows" + (f" ({', '.join(msg_parts)})" if msg_parts else ""))
                
                # Progress update every 20 files
                if idx % 20 == 0:
                    print(f"\n   Progress: {idx}/{len(parquet_files)} files, {total_rows:,} total rows")
            else:
                total_errors += 1
                error_msg = result.get("error", "Unknown error")
                print(f"❌ Failed: {error_msg[:100]}")
                
        except Exception as e:
            total_errors += 1
            print(f"❌ Exception: {str(e)[:100]}")
            continue
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ BATCH PROCESSING COMPLETE")
    print("=" * 80)
    print(f"   Files processed: {files_processed}/{len(parquet_files)}")
    print(f"   Total rows inserted: {total_rows:,}")
    print(f"   Total errors: {total_errors}")
    print(f"   Total skipped: {total_skipped:,}")
    print(f"   Total duplicates dropped: {total_duplicates:,}")
    print(f"   Success rate: {files_processed/len(parquet_files)*100:.1f}%")
    print("=" * 80)

if __name__ == "__main__":
    main()

