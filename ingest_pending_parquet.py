#!/usr/bin/env python3
"""
Ingest Pending Parquet Files
Uses parquet_ingester.py to ingest parquet files that haven't been ingested yet
"""

import sys
from pathlib import Path
import duckdb
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parquet_ingester import ingest_parquet_file
from token_cache import TokenCacheManager

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Suppress verbose logs

def get_pending_parquet_files(db_path: str, search_dirs: list = None) -> list:
    """Get list of parquet files that haven't been ingested"""
    if search_dirs is None:
        search_dirs = [
            "real_parquet_data",
            "parquet_output",
            "binary_to_parquet",
        ]
    
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
                # Exclude test files and backup data
                if any(excluded in str(f) for excluded in excluded_patterns):
                    continue
                if f.exists():
                    all_parquet.add(f)
    
    # Find missing ones
    missing_parquet = all_parquet - ingested_paths
    
    return sorted(list(missing_parquet))

def ingest_parquet_files_batch(db_path: str, file_paths: list, token_cache_path: str = None, max_workers: int = 4):
    """Ingest parquet files in parallel"""
    
    if not file_paths:
        print("No files to ingest")
        return {"files_completed": 0, "rows_inserted": 0, "errors": 0}
    
    print(f"\n{'='*80}")
    print(f"INGESTING {len(file_paths)} PENDING PARQUET FILES")
    print(f"{'='*80}")
    
    # Load token cache
    token_cache = None
    if token_cache_path:
        token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
        print(f"Using token cache: {token_cache_path} ({len(token_cache.token_map):,} tokens)")
    else:
        token_cache = TokenCacheManager(verbose=False)
        print(f"Using default token cache ({len(token_cache.token_map):,} tokens)")
    
    # Group by directory for better logging
    by_dir = defaultdict(list)
    total_size = 0
    for f in file_paths:
        dir_name = str(f.parent)
        size_mb = f.stat().st_size / (1024**2)
        total_size += size_mb
        by_dir[dir_name].append((f, size_mb))
    
    print(f"\nFiles to ingest:")
    print(f"  Total files: {len(file_paths):,}")
    print(f"  Total size: {total_size / (1024**2):.2f} MB")
    print(f"  Directories: {len(by_dir)}")
    
    for dir_name, files in sorted(by_dir.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
        dir_size = sum(s for _, s in files)
        print(f"    {dir_name}: {len(files)} files ({dir_size:.2f} MB)")
    
    # Process files
    files_completed = 0
    total_rows = 0
    errors = 0
    
    print(f"\nProcessing with {max_workers} workers...")
    
    # Use ThreadPoolExecutor for parallel processing (better for I/O-bound tasks)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {}
        for file_path in file_paths:
            future = executor.submit(ingest_parquet_file, file_path, db_path, token_cache, enrich=True)
            future_to_file[future] = file_path
        
        # Process results as they complete
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                if result.get("success"):
                    rows = result.get("row_count", 0)
                    total_rows += rows
                    files_completed += 1
                    if files_completed % 100 == 0:
                        print(f"  Progress: {files_completed}/{len(file_paths)} files, {total_rows:,} rows")
                else:
                    errors += 1
                    if errors <= 10:  # Show first 10 errors
                        print(f"  ❌ {file_path.name}: {result.get('error', 'Unknown error')}")
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ❌ {file_path.name}: {e}")
    
    print(f"\n{'='*80}")
    print(f"INGESTION SUMMARY")
    print(f"{'='*80}")
    print(f"   Files completed: {files_completed:,}")
    print(f"   Rows inserted: {total_rows:,}")
    print(f"   Errors: {errors:,}")
    
    return {
        "files_completed": files_completed,
        "rows_inserted": total_rows,
        "errors": errors
    }

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Ingest pending parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find and ingest all pending parquet files
  python ingest_pending_parquet.py --db tick_data_production.db --auto

  # Ingest from specific directories
  python ingest_pending_parquet.py --db tick_data_production.db \\
      --search-dirs real_parquet_data parquet_output
        """
    )
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for parquet files")
    parser.add_argument("--auto", action="store_true", help="Automatically find and ingest pending parquet files")
    parser.add_argument("--token-cache", default="zerodha_token_list/all_extracted_tokens_merged.json", 
                       help="Path to enriched token lookup")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    
    args = parser.parse_args()
    
    if args.auto:
        print("📋 Finding pending parquet files...")
        
        search_dirs = args.search_dirs or [
            "real_parquet_data",
            "parquet_output",
            "binary_to_parquet",
        ]
        
        file_paths = get_pending_parquet_files(args.db, search_dirs)
        print(f"   Found {len(file_paths):,} pending parquet files")
        
        if args.limit:
            file_paths = file_paths[:args.limit]
            print(f"   Limited to first {len(file_paths):,} files")
        
        if not file_paths:
            print("✅ No pending parquet files found")
            return
        
        # Show summary
        total_size = sum(f.stat().st_size for f in file_paths if f.exists())
        print(f"\n   Total size: {total_size / (1024**2):.2f} MB")
        print(f"   Sample files:")
        for f in file_paths[:10]:
            size_mb = f.stat().st_size / (1024**2) if f.exists() else 0
            print(f"      - {f} ({size_mb:.2f} MB)")
        if len(file_paths) > 10:
            print(f"      ... and {len(file_paths) - 10} more files")
        
        if args.dry_run:
            print("\n🔍 DRY RUN - Would ingest these files")
            return
        
        # Confirm (skip in non-interactive mode)
        print(f"\n⚠️  About to ingest {len(file_paths):,} parquet files")
        print(f"   Using enriched metadata ({args.token_cache})")
        try:
            response = input("Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("Cancelled.")
                return
        except EOFError:
            # Non-interactive mode - proceed automatically
            print("Non-interactive mode - proceeding automatically")
        
        # Ingest
        summary = ingest_parquet_files_batch(
            args.db, 
            file_paths, 
            token_cache_path=args.token_cache if Path(args.token_cache).exists() else None,
            max_workers=args.workers
        )
        
        print(f"\n{'='*80}")
        print(f"✅ INGESTION COMPLETE")
        print(f"{'='*80}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

