#!/usr/bin/env python3
"""
Re-ingest Dropped Files with Updated Token Metadata
Forces re-processing of files that were previously dropped due to missing metadata
"""

import sys
from pathlib import Path
import duckdb
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from binary_to_parquet.parallel_ingestion_driver import ParallelBinaryIngestionDriver
from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
from token_cache import TokenCacheManager
from singleton_db import DatabaseConnectionManager

def get_dropped_files(db_path: str, search_dirs: list = None) -> list:
    """Get list of files that exist but have no data in database"""
    if search_dirs is None:
        search_dirs = [
            "crawlers/raw_data",
            "parquet_output",
            "real_parquet_data",
        ]
    
    conn = duckdb.connect(db_path)
    
    # Get all ingested source files
    ingested_files = set(conn.execute("""
        SELECT DISTINCT source_file 
        FROM tick_data_corrected 
        WHERE source_file IS NOT NULL
    """).fetchall())
    ingested_files = {Path(row[0]) for row in ingested_files if row[0]}
    
    # Find all binary/dat files
    all_files = set()
    excluded_patterns = ['.venv', 'venv', '__pycache__', 'site-packages', 'tests', 'test_', 'research', 'bayesian_model']
    
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            for ext in ['.bin', '.dat']:
                for file_path in dir_path.rglob(f"*{ext}"):
                    if any(excluded in str(file_path) for excluded in excluded_patterns):
                        continue
                    if file_path.exists():
                        all_files.add(file_path)
    
    # Normalize for comparison
    def normalize(p):
        try:
            return str(p.resolve())
        except:
            return str(p)
    
    ingested_normalized = {normalize(f) for f in ingested_files}
    all_normalized = {normalize(f) for f in all_files}
    
    dropped = all_normalized - ingested_normalized
    
    return sorted([Path(f) for f in dropped])

def clear_file_log_entries(db_path: str, file_paths: list):
    """Clear processed_files_log entries to force re-processing"""
    conn = duckdb.connect(db_path)
    
    cleared = 0
    for file_path in file_paths:
        result = conn.execute("""
            DELETE FROM processed_files_log 
            WHERE file_path = ?
        """, [str(file_path)]).fetchone()
        if result:
            cleared += 1
    
    conn.commit()
    conn.close()
    
    print(f"   Cleared {cleared} entries from processed_files_log")
    return cleared

def verify_file_has_data(db_path: str, file_path: Path) -> tuple:
    """Check if file has data in database"""
    conn = duckdb.connect(db_path)
    
    row_count = conn.execute("""
        SELECT COUNT(*) 
        FROM tick_data_corrected 
        WHERE source_file = ?
    """, [str(file_path)]).fetchone()[0]
    
    conn.close()
    return row_count > 0, row_count

def reingest_files(db_path: str, file_paths: list, force: bool = False):
    """Re-ingest specific files"""
    
    print(f"\n{'='*80}")
    print(f"RE-INGESTING {len(file_paths)} FILES")
    print(f"{'='*80}")
    
    # Clear processed_files_log entries for these files to force re-processing
    if force:
        print("\n1. Clearing processed_files_log entries...")
        clear_file_log_entries(db_path, file_paths)
    
    # Group files by directory for better organization
    by_dir = defaultdict(list)
    for file_path in file_paths:
        by_dir[str(file_path.parent)].append(file_path)
    
    print(f"\n2. Files grouped into {len(by_dir)} directories:")
    for dir_name, files in sorted(by_dir.items()):
        total_size = sum(f.stat().st_size for f in files if f.exists())
        print(f"   {dir_name}: {len(files)} files ({total_size / (1024**2):.2f} MB)")
    
    # Create ingestion driver
    print(f"\n3. Initializing ingestion driver...")
    
    # Use the best token cache (all_extracted_tokens_merged.json has 315k tokens, no HDFC)
    token_cache_path = None
    best_cache = Path("zerodha_token_list/all_extracted_tokens_merged.json")
    if best_cache.exists():
        token_cache_path = str(best_cache)
        print(f"   Using token cache: {token_cache_path}")
    else:
        # Fallback to default
        default_cache = Path("core/data/token_lookup_enriched.json")
        if default_cache.exists():
            token_cache_path = str(default_cache)
            print(f"   Using default token cache: {token_cache_path}")
    
    driver = ParallelBinaryIngestionDriver(
        db_path=db_path,
        workers=4,  # Use 4 workers for parallel processing
        batch_size=500,
        token_cache_path=token_cache_path,  # Use the best token cache
        verbose=True,
    )
    
    # Get directory paths for ingestion
    directories_to_ingest = sorted(set(str(f.parent) for f in file_paths))
    
    print(f"\n4. Starting ingestion of {len(file_paths)} files from {len(directories_to_ingest)} directories...")
    print(f"   Directories: {', '.join(directories_to_ingest[:5])}{'...' if len(directories_to_ingest) > 5 else ''}")
    
    # Run ingestion
    try:
        summary = driver.run(directories_to_ingest)
        
        print(f"\n{'='*80}")
        print(f"INGESTION SUMMARY")
        print(f"{'='*80}")
        print(f"   Files completed: {summary.get('files_completed', 0):,}")
        print(f"   Rows inserted: {summary.get('rows_inserted', 0):,}")
        print(f"   Errors: {summary.get('errors', 0):,}")
        
        return summary
        
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def verify_results(db_path: str, file_paths: list):
    """Verify which files now have data after re-ingestion"""
    print(f"\n{'='*80}")
    print(f"VERIFYING RESULTS")
    print(f"{'='*80}")
    
    ingested_count = 0
    still_missing = []
    
    for file_path in file_paths:
        has_data, row_count = verify_file_has_data(db_path, file_path)
        if has_data:
            ingested_count += 1
            print(f"   ✅ {file_path.name}: {row_count:,} rows")
        else:
            still_missing.append(file_path)
            print(f"   ❌ {file_path.name}: Still no data")
    
    print(f"\n   Summary:")
    print(f"   - Successfully ingested: {ingested_count}/{len(file_paths)} files")
    print(f"   - Still missing: {len(still_missing)} files")
    
    if still_missing:
        print(f"\n   Files still without data (may have invalid format or all packets missing metadata):")
        for f in still_missing[:10]:
            size_mb = f.stat().st_size / (1024**2) if f.exists() else 0
            print(f"      - {f} ({size_mb:.2f} MB)")
        if len(still_missing) > 10:
            print(f"      ... and {len(still_missing) - 10} more")
    
    return ingested_count, len(still_missing)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Re-ingest dropped files with updated token metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find and re-ingest all dropped files
  python reingest_dropped_files.py --db tick_data_production.db --auto

  # Re-ingest specific files
  python reingest_dropped_files.py --db tick_data_production.db \\
      --files path/to/file1.bin path/to/file2.bin

  # Re-ingest files from specific directories
  python reingest_dropped_files.py --db tick_data_production.db \\
      --search-dirs crawlers/raw_data/binary_crawler
        """
    )
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for dropped files")
    parser.add_argument("--files", nargs="+", help="Specific files to re-ingest")
    parser.add_argument("--auto", action="store_true", help="Automatically find and re-ingest all dropped files")
    parser.add_argument("--force", action="store_true", help="Force re-processing even if file was logged")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")
    parser.add_argument("--verify-only", action="store_true", help="Only verify current state, don't re-ingest")
    
    args = parser.parse_args()
    
    if args.verify_only:
        # Just verify current state
        if args.files:
            file_paths = [Path(f) for f in args.files]
        else:
            file_paths = get_dropped_files(args.db, args.search_dirs)
        
        verify_results(args.db, file_paths)
        return
    
    # Get files to re-ingest
    if args.files:
        file_paths = [Path(f) for f in args.files]
        print(f"📋 Re-ingesting {len(file_paths)} specified files...")
    elif args.auto:
        search_dirs = args.search_dirs or [
            "crawlers/raw_data",
            "parquet_output",
            "real_parquet_data",
        ]
        file_paths = get_dropped_files(args.db, search_dirs)
        print(f"📋 Found {len(file_paths)} dropped files to re-ingest...")
    else:
        parser.print_help()
        print("\n❌ Please specify --auto, --files, or --search-dirs")
        return
    
    if not file_paths:
        print("✅ No files to re-ingest!")
        return
    
    # Show summary
    total_size = sum(f.stat().st_size for f in file_paths if f.exists())
    print(f"\n   Total size: {total_size / (1024**2):.2f} MB")
    print(f"   Files:")
    for f in file_paths[:10]:
        size_mb = f.stat().st_size / (1024**2) if f.exists() else 0
        print(f"      - {f} ({size_mb:.2f} MB)")
    if len(file_paths) > 10:
        print(f"      ... and {len(file_paths) - 10} more files")
    
    if args.dry_run:
        print("\n🔍 DRY RUN - Would re-ingest these files")
        print("   Remove --dry-run to actually re-ingest")
        return
    
    # Confirm
    print(f"\n⚠️  About to re-ingest {len(file_paths)} files")
    print(f"   This will process files with updated token metadata")
    response = input("Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    # Re-ingest
    summary = reingest_files(args.db, file_paths, force=args.force)
    
    if summary:
        # Verify results
        verify_results(args.db, file_paths)
        
        print(f"\n{'='*80}")
        print(f"✅ RE-INGESTION COMPLETE")
        print(f"{'='*80}")
        print(f"   Check results above to see which files were successfully ingested")
        print(f"   Files that still have no data may need manual investigation")

if __name__ == "__main__":
    main()

