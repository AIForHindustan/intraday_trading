#!/usr/bin/env python3
"""
Re-process Already-Ingested Files with Enriched Metadata
Updates files that were ingested before enriched metadata was available
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
from singleton_db import DatabaseConnectionManager

def get_already_ingested_files(db_path: str, exclude_patterns: list = None) -> list:
    """Get list of files that were already ingested"""
    if exclude_patterns is None:
        exclude_patterns = [
            '%binary_crawler%',
            '%zerodha_websocket/raw_binary%',
            '%sso_xvenue%',
        ]
    
    conn = duckdb.connect(db_path)
    
    # Build WHERE clause
    exclude_conditions = " AND ".join([f"source_file NOT LIKE '{pattern}'" for pattern in exclude_patterns])
    
    source_files = conn.execute(f"""
        SELECT DISTINCT source_file
        FROM tick_data_corrected
        WHERE source_file IS NOT NULL
          AND (source_file LIKE '%.bin' 
               OR source_file LIKE '%.dat'
               OR source_file LIKE '%.parquet')
          AND {exclude_conditions}
        ORDER BY source_file
    """).fetchall()
    
    ingested_files = [Path(row[0]) for row in source_files if row[0] and Path(row[0]).exists()]
    
    conn.close()
    
    return ingested_files

def clear_file_log_entries(db_path: str, file_paths: list):
    """Clear processed_files_log entries to force re-processing"""
    conn = duckdb.connect(db_path)
    
    cleared = 0
    for file_path in file_paths:
        result = conn.execute("""
            DELETE FROM processed_files_log 
            WHERE file_path = ?
        """, [str(file_path)])
        cleared += 1
    
    conn.commit()
    conn.close()
    
    print(f"   Cleared {cleared} entries from processed_files_log")
    return cleared

def reprocess_files(db_path: str, file_paths: list, force: bool = False):
    """Re-process specific files with enriched metadata"""
    
    print(f"\n{'='*80}")
    print(f"RE-PROCESSING {len(file_paths)} ALREADY-INGESTED FILES")
    print(f"{'='*80}")
    
    if not file_paths:
        print("   No files to re-process")
        return None
    
    # Clear processed_files_log entries for these files
    if force:
        print("\n1. Clearing processed_files_log entries...")
        clear_file_log_entries(db_path, file_paths)
    
    # Group files by directory
    by_dir = defaultdict(list)
    for file_path in file_paths:
        by_dir[str(file_path.parent)].append(file_path)
    
    print(f"\n2. Files grouped into {len(by_dir)} directories:")
    total_size = 0
    for dir_name, files in sorted(by_dir.items()):
        dir_size = sum(f.stat().st_size for f in files if f.exists())
        total_size += dir_size
        print(f"   {dir_name}: {len(files)} files ({dir_size / (1024**2):.2f} MB)")
    
    print(f"\n   Total size to re-process: {total_size / (1024**2):.2f} MB")
    
    # Use the enriched token cache
    token_cache_path = "zerodha_token_list/all_extracted_tokens_merged.json"
    print(f"\n3. Using enriched token cache: {token_cache_path}")
    print(f"   (315,465 tokens - 84k more than before)")
    
    # Create ingestion driver
    print(f"\n4. Initializing ingestion driver...")
    driver = ParallelBinaryIngestionDriver(
        db_path=db_path,
        workers=4,
        batch_size=500,
        token_cache_path=token_cache_path,  # Use enriched metadata
        verbose=True,
    )
    
    # Get directory paths
    directories_to_ingest = sorted(set(str(f.parent) for f in file_paths))
    
    print(f"\n5. Starting re-processing...")
    print(f"   This will use SmartUpsertConverter to handle duplicates")
    print(f"   New rows will be added, existing rows may be updated")
    
    try:
        summary = driver.run(directories_to_ingest)
        
        print(f"\n{'='*80}")
        print(f"RE-PROCESSING SUMMARY")
        print(f"{'='*80}")
        print(f"   Files completed: {summary.get('files_completed', 0):,}")
        print(f"   Rows inserted: {summary.get('rows_inserted', 0):,}")
        print(f"   Errors: {summary.get('errors', 0):,}")
        
        return summary
        
    except Exception as e:
        print(f"\n❌ Re-processing failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def verify_improvement(db_path: str, file_paths: list):
    """Verify that re-processing added more rows"""
    conn = duckdb.connect(db_path)
    
    print(f"\n{'='*80}")
    print(f"VERIFYING IMPROVEMENT")
    print(f"{'='*80}")
    
    for file_path in file_paths[:10]:  # Check first 10 files
        row_count = conn.execute("""
            SELECT COUNT(*) 
            FROM tick_data_corrected 
            WHERE source_file = ?
        """, [str(file_path)]).fetchone()[0]
        
        if row_count > 0:
            print(f"   ✅ {file_path.name}: {row_count:,} rows")
        else:
            print(f"   ❌ {file_path.name}: Still no rows")
    
    # Total improvement
    total_rows = conn.execute("SELECT COUNT(*) FROM tick_data_corrected").fetchone()[0]
    print(f"\n   Total rows in database: {total_rows:,}")
    
    conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Re-process already-ingested files with enriched metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find and re-process all already-ingested files
  python reprocess_ingested_files.py --db tick_data_production.db --auto

  # Re-process files from specific directory
  python reprocess_ingested_files.py --db tick_data_production.db \\
      --search-dirs crawlers/raw_data/trading_crawler
        """
    )
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--auto", action="store_true", help="Automatically find and re-process all already-ingested files")
    parser.add_argument("--force", action="store_true", help="Force re-processing by clearing processed_files_log")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without actually doing it")
    parser.add_argument("--limit", type=int, help="Limit number of files to process (for testing)")
    
    args = parser.parse_args()
    
    if args.auto:
        print("📋 Finding already-ingested files...")
        file_paths = get_already_ingested_files(args.db)
        print(f"   Found {len(file_paths)} files")
        
        if args.limit:
            file_paths = file_paths[:args.limit]
            print(f"   Limited to first {len(file_paths)} files")
        
        if not file_paths:
            print("✅ No files to re-process")
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
            print("\n🔍 DRY RUN - Would re-process these files")
            return
        
        # Confirm
        print(f"\n⚠️  About to re-process {len(file_paths)} files")
        print(f"   This will use enriched metadata (315k tokens)")
        print(f"   SmartUpsertConverter will handle duplicates")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Cancelled.")
            return
        
        # Re-process
        summary = reprocess_files(args.db, file_paths, force=args.force)
        
        if summary:
            verify_improvement(args.db, file_paths)
            
            print(f"\n{'='*80}")
            print(f"✅ RE-PROCESSING COMPLETE")
            print(f"{'='*80}")
            print(f"   Check summary above for results")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

