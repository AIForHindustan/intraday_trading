#!/usr/bin/env python3
"""
Analyze File Ingestion Status
Identifies which binary/dat files were ingested vs dropped due to missing metadata
"""

import duckdb
from pathlib import Path
from collections import defaultdict
import json
from datetime import datetime

def analyze_file_ingestion(db_path: str = "tick_data_production.db", search_paths: list = None):
    """Analyze which files were ingested and which were dropped"""
    
    if search_paths is None:
        search_paths = [
            "crawlers/raw_data",
            "parquet_output",
            "real_parquet_data",
        ]
    
    print("="*80)
    print("FILE INGESTION ANALYSIS")
    print("="*80)
    
    conn = duckdb.connect(db_path)
    
    # 1. Check processed_files_log
    print("\n1. Checking processed_files_log table...")
    log_count = conn.execute("SELECT COUNT(*) FROM processed_files_log").fetchone()[0]
    
    if log_count == 0:
        print("   ⚠️  processed_files_log is empty - files may not have been logged during ingestion")
        print("   This means files were processed before logging was implemented, or logging was disabled")
        print("   Will use source_file field in tick_data_corrected as alternative tracking method")
    else:
        print(f"   Found {log_count:,} files in processed_files_log")
        
        status_summary = conn.execute("""
            SELECT 
                status,
                COUNT(*) as file_count,
                SUM(records_processed) as total_records,
                SUM(file_size) as total_size_bytes
            FROM processed_files_log
            GROUP BY status
            ORDER BY file_count DESC
        """).fetchdf()
        
        print("\n   Status Summary:")
        print(status_summary.to_string())
        
        # Show failed files
        failed_files = conn.execute("""
            SELECT file_path, error_message, file_size
            FROM processed_files_log
            WHERE status IN ('failed', 'partial')
            ORDER BY file_size DESC
            LIMIT 20
        """).fetchdf()
        
        if not failed_files.empty:
            print(f"\n   Failed/Partial Files ({len(failed_files)}):")
            print(failed_files.to_string())
    
    # 2. Check source_file field in tick_data_corrected
    print("\n2. Analyzing source_file field in tick_data_corrected...")
    
    source_file_stats = conn.execute("""
        SELECT 
            source_file,
            COUNT(*) as row_count,
            COUNT(DISTINCT instrument_token) as unique_instruments,
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp
        FROM tick_data_corrected
        WHERE source_file IS NOT NULL
        GROUP BY source_file
        ORDER BY row_count DESC
        LIMIT 20
    """).fetchdf()
    
    print(f"\n   Top 20 source files by row count:")
    print(source_file_stats.to_string())
    
    # Count unique source files
    unique_sources = conn.execute("""
        SELECT COUNT(DISTINCT source_file) 
        FROM tick_data_corrected 
        WHERE source_file IS NOT NULL
    """).fetchone()[0]
    
    total_rows = conn.execute("SELECT COUNT(*) FROM tick_data_corrected").fetchone()[0]
    rows_with_source = conn.execute("""
        SELECT COUNT(*) 
        FROM tick_data_corrected 
        WHERE source_file IS NOT NULL
    """).fetchone()[0]
    
    print(f"\n   Source File Statistics:")
    print(f"   - Unique source files with data: {unique_sources:,}")
    print(f"   - Total rows: {total_rows:,}")
    print(f"   - Rows with source_file: {rows_with_source:,} ({(rows_with_source/total_rows*100):.1f}%)")
    
    # 3. Identify files that might have been dropped
    print("\n3. Identifying potentially dropped files...")
    
    # Get all .bin and .dat files in specified search paths (exclude venv and test data)
    all_binary_files = set()
    excluded_patterns = [
        '.venv', 'venv', '__pycache__', 'node_modules',
        'site-packages', 'tests', 'test_', 'research'
    ]
    
    search_paths_list = [Path(p) for p in search_paths] if search_paths else [
        Path("crawlers/raw_data"),
        Path("parquet_output"),
        Path("real_parquet_data"),
    ]
    
    for directory in search_paths_list:
        if directory.exists():
            for ext in ['.bin', '.dat']:
                for file_path in directory.rglob(f"*{ext}"):
                    # Skip files in excluded directories
                    if any(excluded in str(file_path) for excluded in excluded_patterns):
                        continue
                    all_binary_files.add(file_path)
    
    print(f"   Found {len(all_binary_files)} binary/dat files in trading data directories")
    
    # Check which ones are in database
    if unique_sources > 0:
        # Get source files from database
        db_source_files = set(conn.execute("""
            SELECT DISTINCT source_file 
            FROM tick_data_corrected 
            WHERE source_file IS NOT NULL
        """).fetchall())
        db_source_files = {row[0] for row in db_source_files}
        
        # Normalize paths for comparison
        def normalize_path(p):
            """Normalize path for comparison"""
            if isinstance(p, str):
                p = Path(p)
            return str(p.resolve())
        
        db_files_normalized = {normalize_path(f) for f in db_source_files}
        binary_files_normalized = {normalize_path(f) for f in all_binary_files}
        
        ingested = db_files_normalized & binary_files_normalized
        not_ingested = binary_files_normalized - db_files_normalized
        
        print(f"\n   File Comparison:")
        print(f"   - Binary files found: {len(binary_files_normalized):,}")
        print(f"   - Ingested (found in DB): {len(ingested):,}")
        print(f"   - Not ingested (missing from DB): {len(not_ingested):,}")
        
        if not_ingested:
            print(f"\n   ⚠️  Files not found in database (potentially dropped):")
            not_ingested_list = sorted(list(not_ingested))[:50]  # Show first 50
            for f in not_ingested_list:
                file_path = Path(f)
                if file_path.exists():
                    size_mb = file_path.stat().st_size / (1024**2)
                    print(f"      {f} ({size_mb:.2f} MB)")
                else:
                    print(f"      {f} (file not found)")
            
            if len(not_ingested) > 50:
                print(f"      ... and {len(not_ingested) - 50} more files")
    
    # 4. Check for missing metadata issues
    print("\n4. Checking for missing metadata indicators...")
    
    # Check for UNKNOWN symbols (indicating missing metadata)
    unknown_symbols = conn.execute("""
        SELECT 
            COUNT(*) as total_unknown,
            COUNT(DISTINCT instrument_token) as unique_tokens,
            COUNT(DISTINCT source_file) as affected_files
        FROM tick_data_corrected
        WHERE symbol LIKE 'UNKNOWN_%'
    """).fetchone()
    
    print(f"   Unknown Symbol Statistics:")
    print(f"   - Rows with UNKNOWN symbols: {unknown_symbols[0]:,}")
    print(f"   - Unique tokens without metadata: {unknown_symbols[1]:,}")
    print(f"   - Files with unknown tokens: {unknown_symbols[2]:,}")
    
    # Check files with high unknown token ratio
    files_with_unknowns = conn.execute("""
        SELECT 
            source_file,
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE symbol LIKE 'UNKNOWN_%') as unknown_rows,
            ROUND(100.0 * COUNT(*) FILTER (WHERE symbol LIKE 'UNKNOWN_%') / COUNT(*), 2) as unknown_pct
        FROM tick_data_corrected
        WHERE source_file IS NOT NULL
        GROUP BY source_file
        HAVING COUNT(*) FILTER (WHERE symbol LIKE 'UNKNOWN_%') > 0
        ORDER BY unknown_pct DESC, total_rows DESC
        LIMIT 20
    """).fetchdf()
    
    if not files_with_unknowns.empty:
        print(f"\n   Files with UNKNOWN symbols (top 20):")
        print(files_with_unknowns.to_string())
    
    # 5. Check source_file patterns
    print("\n5. Analyzing source file patterns...")
    
    source_patterns = conn.execute("""
        SELECT 
            CASE
                WHEN source_file LIKE '%.bin' THEN '.bin'
                WHEN source_file LIKE '%.dat' THEN '.dat'
                WHEN source_file LIKE '%.parquet' THEN '.parquet'
                WHEN source_file LIKE '%.json' THEN '.json'
                ELSE 'other'
            END as file_type,
            COUNT(DISTINCT source_file) as file_count,
            COUNT(*) as row_count
        FROM tick_data_corrected
        WHERE source_file IS NOT NULL
        GROUP BY file_type
        ORDER BY row_count DESC
    """).fetchdf()
    
    print(source_patterns.to_string())
    
    conn.close()
    
    # Generate report
    report = {
        "analysis_date": datetime.now().isoformat(),
        "database": db_path,
        "processed_files_log": {
            "total_files": int(log_count),
            "status_breakdown": status_summary.to_dict('records') if log_count > 0 else []
        },
        "source_files_in_db": {
            "unique_files": int(unique_sources),
            "total_rows": int(total_rows),
            "rows_with_source": int(rows_with_source)
        },
        "missing_metadata": {
            "unknown_symbol_rows": int(unknown_symbols[0]),
            "unique_unknown_tokens": int(unknown_symbols[1]),
            "affected_files": int(unknown_symbols[2])
        }
    }
    
    report_file = Path(db_path).parent / "file_ingestion_report.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n{'='*80}")
    print(f"Report saved to: {report_file}")
    print(f"{'='*80}")

def find_dropped_files(db_path: str = "tick_data_production.db", search_dirs: list = None):
    """Find binary/dat files that exist but weren't ingested"""
    
    if search_dirs is None:
        search_dirs = [
            Path("."),
            Path("parquet_output"),
            Path("real_parquet_data"),
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
    for directory in search_dirs:
        if Path(directory).exists():
            for ext in ['.bin', '.dat']:
                all_files.update(Path(directory).rglob(f"*{ext}"))
    
    # Normalize for comparison
    def normalize(p):
        return str(p.resolve())
    
    ingested_normalized = {normalize(f) for f in ingested_files}
    all_normalized = {normalize(f) for f in all_files}
    
    dropped = all_normalized - ingested_normalized
    
    return sorted([Path(f) for f in dropped])

def main():
    """Main analysis function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze file ingestion status")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--find-dropped", action="store_true", help="List dropped files")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for binary files")
    
    args = parser.parse_args()
    
    if args.find_dropped:
        print("Finding dropped files...")
        dropped = find_dropped_files(args.db, args.search_dirs)
        print(f"\nFound {len(dropped)} files that exist but weren't ingested:")
        for f in dropped[:100]:  # Show first 100
            if f.exists():
                size_mb = f.stat().st_size / (1024**2)
                print(f"  {f} ({size_mb:.2f} MB)")
        if len(dropped) > 100:
            print(f"  ... and {len(dropped) - 100} more files")
    else:
        analyze_file_ingestion(args.db)

if __name__ == "__main__":
    main()

