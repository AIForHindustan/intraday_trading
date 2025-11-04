#!/usr/bin/env python3
"""
Fix Timestamp-as-Token Issue
Identifies and fixes rows where instrument_token contains timestamp values
"""

import duckdb
from datetime import datetime
from pathlib import Path
import sys

def identify_timestamp_tokens(db_path: str):
    """Identify rows where instrument_token is actually a timestamp"""
    conn = duckdb.connect(db_path)
    
    print("="*80)
    print("IDENTIFYING TIMESTAMP-AS-TOKEN ROWS")
    print("="*80)
    
    # Epoch range for 2020-2026 (reasonable trading dates)
    MIN_EPOCH = 1577836800  # Jan 1, 2020
    MAX_EPOCH = 1767225600  # Jan 1, 2026
    
    # Find rows where instrument_token is in timestamp range and symbol is UNKNOWN
    timestamp_tokens = conn.execute(f"""
        SELECT 
            instrument_token,
            COUNT(*) as row_count,
            COUNT(DISTINCT source_file) as file_count,
            MIN(timestamp) as min_processing_ts,
            MAX(timestamp) as max_processing_ts
        FROM tick_data_corrected
        WHERE instrument_token >= {MIN_EPOCH}
          AND instrument_token <= {MAX_EPOCH}
          AND symbol LIKE 'UNKNOWN_%'
        GROUP BY instrument_token
        ORDER BY row_count DESC
        LIMIT 20
    """).fetchdf()
    
    if not timestamp_tokens.empty:
        print(f"\nFound {len(timestamp_tokens)} unique timestamp-as-token values")
        print(timestamp_tokens.to_string())
        
        # Verify they're timestamps
        print("\nVerifying as timestamps:")
        for _, row in timestamp_tokens.head(5).iterrows():
            token = row['instrument_token']
            try:
                dt = datetime.fromtimestamp(token)
                print(f"  Token {token:,} = {dt}")
            except:
                print(f"  Token {token:,} = Not a valid timestamp")
        
        # Get total count
        total_rows = conn.execute(f"""
            SELECT COUNT(*) 
            FROM tick_data_corrected
            WHERE instrument_token >= {MIN_EPOCH}
              AND instrument_token <= {MAX_EPOCH}
              AND symbol LIKE 'UNKNOWN_%'
        """).fetchone()[0]
        
        print(f"\n⚠️  Total rows with timestamp-as-token: {total_rows:,}")
        return total_rows
    else:
        print("\n✅ No timestamp-as-token rows found")
        return 0

def fix_timestamp_tokens(db_path: str, dry_run: bool = True):
    """Fix or delete rows where instrument_token is actually a timestamp"""
    conn = duckdb.connect(db_path)
    
    MIN_EPOCH = 1577836800  # Jan 1, 2020
    MAX_EPOCH = 1767225600  # Jan 1, 2026
    
    if dry_run:
        print("\n" + "="*80)
        print("DRY RUN - Would delete timestamp-as-token rows")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("FIXING: Deleting timestamp-as-token rows")
        print("="*80)
    
    count = conn.execute(f"""
        SELECT COUNT(*) 
        FROM tick_data_corrected
        WHERE instrument_token >= {MIN_EPOCH}
          AND instrument_token <= {MAX_EPOCH}
          AND symbol LIKE 'UNKNOWN_%'
    """).fetchone()[0]
    
    print(f"Rows to delete: {count:,}")
    
    if not dry_run and count > 0:
        conn.execute(f"""
            DELETE FROM tick_data_corrected
            WHERE instrument_token >= {MIN_EPOCH}
              AND instrument_token <= {MAX_EPOCH}
              AND symbol LIKE 'UNKNOWN_%'
        """)
        conn.commit()
        print(f"✅ Deleted {count:,} rows with timestamp-as-token values")
    elif not dry_run:
        print("✅ No rows to delete")
    
    conn.close()
    return count

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix timestamp-as-token issues")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--fix", action="store_true", help="Actually delete bad rows")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    
    args = parser.parse_args()
    
    # Identify the issue
    total_rows = identify_timestamp_tokens(args.db)
    
    if total_rows > 0:
        # Fix it
        if args.fix:
            fix_timestamp_tokens(args.db, dry_run=False)
        else:
            fix_timestamp_tokens(args.db, dry_run=True)
            print("\n⚠️  Use --fix to actually delete these rows")
    else:
        print("\n✅ No issues found!")

if __name__ == "__main__":
    main()

