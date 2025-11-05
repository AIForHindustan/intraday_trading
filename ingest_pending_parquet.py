#!/usr/bin/env python3
"""
Ingest Pending Parquet Files
Uses parquet_ingester.py to ingest parquet files that haven't been ingested yet
"""

import sys
from pathlib import Path
import duckdb
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import time
import logging
import multiprocessing
import resource

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from parquet_ingester import ingest_parquet_file
from token_cache import TokenCacheManager

# Set up logging
logging.basicConfig(level=logging.WARNING)  # Suppress verbose logs

def get_pending_parquet_files(db_path: str, search_dirs: list = None, date_filter: str = None) -> list:
    """Get list of parquet files that haven't been ingested
    
    Args:
        db_path: Path to DuckDB database
        search_dirs: Directories to search for parquet files
        date_filter: Optional date filter (e.g., '20251103' for Nov 3, 2025)
    """
    if search_dirs is None:
        search_dirs = [
            "real_parquet_data",
            "parquet_output",
            "binary_to_parquet",
            "crawlers/raw_data",  # Include crawlers directory
            "crawlers",  # Include all crawlers subdirectories (rglob will search nested)
            "research",  # Include research directory
            "research_data",  # Include research_data directory
        ]
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Get ingested parquet files
        if date_filter:
            ingested_parquet = conn.execute("""
                SELECT DISTINCT source_file
                FROM tick_data_corrected
                WHERE source_file LIKE '%.parquet'
                  AND source_file LIKE ?
            """, [f'%{date_filter}%']).fetchall()
        else:
            ingested_parquet = conn.execute("""
                SELECT DISTINCT source_file
                FROM tick_data_corrected
                WHERE source_file LIKE '%.parquet'
            """).fetchall()
        
        ingested_paths = {Path(row[0]) for row in ingested_parquet if row[0]}
        conn.close()
    except Exception as e:
        logging.warning(f"Could not check database for ingested files: {e}")
        ingested_paths = set()
    
    # Find parquet files on disk
    excluded_patterns = ['.venv', '__pycache__', 'site-packages', 'tests', 'test_', 'backtesting']
    
    all_parquet = set()
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            pattern = f'*{date_filter}*.parquet' if date_filter else '*.parquet'
            for f in dir_path.rglob(pattern):
                # Exclude test files and backup data
                if any(excluded in str(f) for excluded in excluded_patterns):
                    continue
                if f.exists():
                    all_parquet.add(f)
    
    # Find missing ones
    missing_parquet = all_parquet - ingested_paths
    
    return sorted(list(missing_parquet))

def ingest_parquet_files_batch(db_path: str, file_paths: list, token_cache_path: str = None, max_workers: int = 4, max_memory_gb: float = None, use_multiprocessing: bool = False):
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
    
    if use_multiprocessing:
        print(f"\n⚠️  WARNING: DuckDB doesn't support concurrent writes from multiple processes")
        print(f"   Switching to threading mode (workers can share database connection)")
        print(f"\nProcessing with {max_workers} workers (threading with shared connection)...")
        use_multiprocessing = False  # Force threading for database access
    
    if use_multiprocessing:
        print(f"\nProcessing with {max_workers} workers (multiprocessing)...")
        if max_memory_gb:
            print(f"Max memory per worker: {max_memory_gb}GB")
        
        # Use ProcessPoolExecutor for CPU-bound tasks
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Set memory limit for each worker
            if max_memory_gb:
                max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)
                try:
                    resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
                except ValueError:
                    logging.warning("Could not set memory limit, using system default")
            
            # Submit all tasks
            future_to_file = {}
            for file_path in file_paths:
                future = executor.submit(
                    ingest_worker_mp, 
                    str(file_path), 
                    db_path, 
                    token_cache_path if token_cache_path else "core/data/token_lookup_enriched.json",
                    True
                )
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
                        if errors <= 10:
                            print(f"  ❌ {file_path.name}: {result.get('error', 'Unknown error')}")
                except Exception as e:
                    errors += 1
                    if errors <= 10:
                        print(f"  ❌ {file_path.name}: {e}")
    
    if not use_multiprocessing:
        print(f"\nProcessing with {max_workers} workers (threading)...")
        
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


def ingest_worker_mp(file_path_str: str, db_path: str, token_cache_path: str, enrich: bool) -> dict:
    """Worker function for multiprocessing ingestion."""
    import sys
    from pathlib import Path
    
    # Add project root to path (needed in worker process)
    # Use current working directory which should be the project root
    project_root = Path.cwd()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    from parquet_ingester import ingest_parquet_file
    from token_cache import TokenCacheManager
    
    try:
        # Load token cache in worker
        token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
        
        # Ingest file
        result = ingest_parquet_file(
            file_path=Path(file_path_str),
            db_path=db_path,
            token_cache=token_cache,
            enrich=enrich
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e), "row_count": 0}

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
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers")
    parser.add_argument("--max-memory", type=float, default=None, help="Maximum memory per worker in GB")
    parser.add_argument("--mp", action="store_true", help="Use multiprocessing instead of threading")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--date-filter", type=str, help="Filter files by date (e.g., '20251103' for Nov 3, 2025)")
    
    args = parser.parse_args()
    
    if args.auto:
        print("📋 Finding pending parquet files...")
        
        search_dirs = args.search_dirs or [
            "real_parquet_data",
            "parquet_output",
            "binary_to_parquet",
            "crawlers/raw_data",  # Include crawlers directory
        ]
        
        file_paths = get_pending_parquet_files(args.db, search_dirs, date_filter=args.date_filter)
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
            max_workers=args.workers,
            max_memory_gb=args.max_memory,
            use_multiprocessing=args.mp
        )
        
        print(f"\n{'='*80}")
        print(f"✅ INGESTION COMPLETE")
        print(f"{'='*80}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

