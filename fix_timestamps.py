#!/usr/bin/env python3
"""
Fix Timestamp Issues in DuckDB
Corrects epoch timestamp parsing errors in tick_data_production.db
"""

import duckdb
from datetime import datetime, timezone
import sys
from pathlib import Path

def analyze_timestamp_issue(conn):
    """Analyze timestamp issues in the database"""
    print("="*80)
    print("TIMESTAMP ISSUE ANALYSIS")
    print("="*80)
    
    # Check distribution of timestamp values
    print("\n1. Checking timestamp distribution...")
    stats = conn.execute("""
        SELECT 
            MIN(timestamp) as min_timestamp,
            MAX(timestamp) as max_timestamp,
            MIN(exchange_timestamp) as min_exchange_timestamp,
            MAX(exchange_timestamp) as max_exchange_timestamp,
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE timestamp < '2025-01-01') as rows_before_2025,
            COUNT(*) FILTER (WHERE exchange_timestamp < '2025-01-01') as exchange_before_2025,
            COUNT(*) FILTER (WHERE exchange_timestamp_ns IS NOT NULL) as has_ns
        FROM tick_data_corrected
    """).fetchone()
    
    print(f"   Total rows: {stats[4]:,}")
    print(f"   Timestamp range: {stats[0]} to {stats[1]}")
    print(f"   Exchange timestamp range: {stats[2]} to {stats[3]}")
    print(f"   Rows with timestamp < 2025: {stats[5]:,}")
    print(f"   Rows with exchange_timestamp < 2025: {stats[6]:,}")
    print(f"   Rows with exchange_timestamp_ns: {stats[7]:,}")
    
    # Sample exchange_timestamp_ns values
    print("\n2. Sample exchange_timestamp_ns values...")
    ns_samples = conn.execute("""
        SELECT 
            exchange_timestamp_ns,
            exchange_timestamp,
            timestamp,
            CASE 
                WHEN exchange_timestamp_ns >= 1000000000000000000 THEN 'nanoseconds'
                WHEN exchange_timestamp_ns >= 1000000000000 THEN 'milliseconds'
                WHEN exchange_timestamp_ns >= 1000000000 THEN 'seconds'
                ELSE 'too_small'
            END as format_guess
        FROM tick_data_corrected
        WHERE exchange_timestamp_ns IS NOT NULL
        ORDER BY exchange_timestamp_ns DESC
        LIMIT 10
    """).fetchdf()
    print(ns_samples.to_string())
    
    # Check what valid timestamps look like
    print("\n3. Valid timestamps (after 2025-01-01)...")
    valid_samples = conn.execute("""
        SELECT 
            timestamp,
            exchange_timestamp,
            exchange_timestamp_ns,
            EXTRACT(EPOCH FROM timestamp) as timestamp_epoch_seconds,
            CASE 
                WHEN exchange_timestamp_ns IS NOT NULL THEN exchange_timestamp_ns / 1e9
                ELSE NULL
            END as ns_to_seconds
        FROM tick_data_corrected
        WHERE timestamp >= '2025-10-01'
          AND timestamp < '2025-11-01'
          AND exchange_timestamp_ns IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 5
    """).fetchdf()
    print(valid_samples.to_string())
    
    return stats

def fix_exchange_timestamps(conn, dry_run=True):
    """Fix exchange_timestamp by converting from exchange_timestamp_ns"""
    print("\n" + "="*80)
    print(f"FIXING EXCHANGE TIMESTAMPS ({'DRY RUN' if dry_run else 'ACTUAL FIX'})")
    print("="*80)
    
    # Strategy: Use exchange_timestamp_ns to reconstruct proper timestamps
    # First, let's understand the format better
    print("\n1. Analyzing exchange_timestamp_ns format...")
    
    # Check if exchange_timestamp_ns values are actually in nanoseconds
    # If they're small (< 1e9), they might be in a different format
    analysis = conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE exchange_timestamp_ns >= 1000000000000000) as nanoseconds_count,
            COUNT(*) FILTER (WHERE exchange_timestamp_ns >= 1000000000000 AND exchange_timestamp_ns < 1000000000000000) as milliseconds_count,
            COUNT(*) FILTER (WHERE exchange_timestamp_ns >= 1000000000 AND exchange_timestamp_ns < 1000000000000) as seconds_count,
            COUNT(*) FILTER (WHERE exchange_timestamp_ns < 1000000000) as too_small,
            MIN(exchange_timestamp_ns) as min_ns,
            MAX(exchange_timestamp_ns) as max_ns,
            AVG(exchange_timestamp_ns) as avg_ns
        FROM tick_data_corrected
        WHERE exchange_timestamp_ns IS NOT NULL
    """).fetchone()
    
    print(f"   Total with ns: {analysis[0]:,}")
    print(f"   >= 1e15 (nanoseconds): {analysis[1]:,}")
    print(f"   1e12-1e15 (milliseconds): {analysis[2]:,}")
    print(f"   1e9-1e12 (seconds): {analysis[3]:,}")
    print(f"   < 1e9 (too small): {analysis[4]:,}")
    print(f"   Range: {analysis[5]} to {analysis[6]}")
    print(f"   Average: {analysis[7]:.0f}")
    
    # Try to use timestamp field as reference since it seems correct
    print("\n2. Using timestamp field to fix exchange_timestamp...")
    
    # Check correlation between timestamp and exchange_timestamp_ns
    correlation = conn.execute("""
        SELECT 
            COUNT(*) as rows_with_both,
            COUNT(*) FILTER (WHERE timestamp >= '2025-01-01' AND timestamp < '2025-11-01') as valid_timestamp,
            AVG(EXTRACT(EPOCH FROM timestamp) - (exchange_timestamp_ns / 1e9)) as avg_diff_seconds
        FROM tick_data_corrected
        WHERE timestamp >= '2025-01-01'
          AND exchange_timestamp_ns IS NOT NULL
          AND exchange_timestamp_ns > 0
    """).fetchone()
    
    print(f"   Rows with both: {correlation[0]:,}")
    print(f"   Valid timestamps: {correlation[1]:,}")
    print(f"   Avg difference: {correlation[2]:.2f} seconds")
    
    if not dry_run:
        print("\n3. Fixing exchange_timestamp from exchange_timestamp_ns...")
        
        # Strategy: Use exchange_timestamp_ns when it's in nanoseconds (>= 1e15 or between reasonable bounds)
        # Use timestamp as fallback when exchange_timestamp_ns is clearly wrong
        
        # Fix rows with valid exchange_timestamp_ns (nanoseconds in reasonable range)
        # Valid range: between 1e15 (2025) and 4e18 (reasonable future), excluding overflow values
        # Overflow values like 4294967295000000000 are clearly wrong
        
        # First, get count of rows that would be updated
        count_query = """
            SELECT COUNT(*)
            FROM tick_data_corrected
            WHERE exchange_timestamp < '2025-01-01'
              AND exchange_timestamp_ns IS NOT NULL
              AND exchange_timestamp_ns >= 1700000000000000000
              AND exchange_timestamp_ns < 2000000000000000000
        """
        rows_to_fix = conn.execute(count_query).fetchone()[0]
        print(f"   Rows with valid exchange_timestamp_ns to fix: {rows_to_fix:,}")
        
        # Fix rows with valid exchange_timestamp_ns (reasonable nanoseconds)
        conn.execute("""
            UPDATE tick_data_corrected
            SET exchange_timestamp = TO_TIMESTAMP(exchange_timestamp_ns / 1000000000.0)
            WHERE exchange_timestamp < '2025-01-01'
              AND exchange_timestamp_ns IS NOT NULL
              AND exchange_timestamp_ns >= 1700000000000000000
              AND exchange_timestamp_ns < 2000000000000000000
        """)
        
        print(f"   ✅ Updated exchange_timestamp from valid exchange_timestamp_ns")
        
        # For rows with invalid exchange_timestamp_ns or missing, use timestamp as fallback
        count_query2 = """
            SELECT COUNT(*)
            FROM tick_data_corrected
            WHERE exchange_timestamp < '2025-01-01'
              AND timestamp >= '2025-01-01'
              AND (exchange_timestamp_ns IS NULL 
                   OR exchange_timestamp_ns = 0 
                   OR exchange_timestamp_ns < 1000000000
                   OR exchange_timestamp_ns >= 2000000000000000000)
        """
        rows_to_fix2 = conn.execute(count_query2).fetchone()[0]
        print(f"   Rows to fix using timestamp as fallback: {rows_to_fix2:,}")
        
        conn.execute("""
            UPDATE tick_data_corrected
            SET exchange_timestamp = timestamp
            WHERE exchange_timestamp < '2025-01-01'
              AND timestamp >= '2025-01-01'
              AND (exchange_timestamp_ns IS NULL 
                   OR exchange_timestamp_ns = 0 
                   OR exchange_timestamp_ns < 1000000000
                   OR exchange_timestamp_ns >= 2000000000000000000)
        """)
        
        print(f"   ✅ Updated exchange_timestamp using timestamp as reference")
        
        # Commit changes
        conn.commit()
    else:
        print("\n3. [DRY RUN] Would fix exchange_timestamp...")
        affected = conn.execute("""
            SELECT COUNT(*)
            FROM tick_data_corrected
            WHERE exchange_timestamp < '2025-01-01'
              AND (exchange_timestamp_ns IS NOT NULL OR timestamp >= '2025-01-01')
        """).fetchone()[0]
        print(f"   Would update: {affected:,} rows")

def fix_last_traded_timestamps(conn, dry_run=True):
    """Fix last_traded_timestamp similarly"""
    print("\n" + "="*80)
    print(f"FIXING LAST_TRADED_TIMESTAMPS ({'DRY RUN' if dry_run else 'ACTUAL FIX'})")
    print("="*80)
    
    if not dry_run:
        # Count rows to fix
        count_query = """
            SELECT COUNT(*)
            FROM tick_data_corrected
            WHERE last_traded_timestamp < '2025-01-01'
              AND last_traded_timestamp_ns IS NOT NULL
              AND ((last_traded_timestamp_ns >= 1700000000000000000 AND last_traded_timestamp_ns < 2000000000000000000)
                   OR (last_traded_timestamp_ns < 1000000000 AND timestamp >= '2025-01-01'))
        """
        rows_to_fix = conn.execute(count_query).fetchone()[0]
        print(f"   Rows with valid last_traded_timestamp_ns to fix: {rows_to_fix:,}")
        
        # Fix valid nanoseconds
        conn.execute("""
            UPDATE tick_data_corrected
            SET last_traded_timestamp = TO_TIMESTAMP(last_traded_timestamp_ns / 1000000000.0)
            WHERE last_traded_timestamp < '2025-01-01'
              AND last_traded_timestamp_ns IS NOT NULL
              AND last_traded_timestamp_ns >= 1700000000000000000
              AND last_traded_timestamp_ns < 2000000000000000000
        """)
        
        # Fix using timestamp as fallback
        conn.execute("""
            UPDATE tick_data_corrected
            SET last_traded_timestamp = timestamp
            WHERE last_traded_timestamp < '2025-01-01'
              AND timestamp >= '2025-01-01'
              AND (last_traded_timestamp_ns IS NULL 
                   OR last_traded_timestamp_ns = 0 
                   OR last_traded_timestamp_ns < 1000000000
                   OR last_traded_timestamp_ns >= 2000000000000000000)
        """)
        
        print(f"   ✅ Updated last_traded_timestamp")
        conn.commit()
    else:
        affected = conn.execute("""
            SELECT COUNT(*)
            FROM tick_data_corrected
            WHERE last_traded_timestamp < '2025-01-01'
              AND (last_traded_timestamp_ns IS NOT NULL OR timestamp >= '2025-01-01')
        """).fetchone()[0]
        print(f"   [DRY RUN] Would update: {affected:,} rows")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix timestamp issues in DuckDB")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only, don't fix")
    parser.add_argument("--fix", action="store_true", help="Actually fix the timestamps")
    
    args = parser.parse_args()
    
    if not Path(args.db).exists():
        print(f"Error: Database {args.db} not found")
        sys.exit(1)
    
    conn = duckdb.connect(args.db)
    
    # Analyze first
    analyze_timestamp_issue(conn)
    
    if args.fix:
        if args.dry_run:
            print("\n⚠️  --fix and --dry-run both specified. Doing dry run only.")
            fix_exchange_timestamps(conn, dry_run=True)
            fix_last_traded_timestamps(conn, dry_run=True)
        else:
            print("\n⚠️  About to fix timestamps. This will modify the database!")
            try:
                response = input("Continue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled.")
                    return
            except EOFError:
                # Non-interactive mode, proceed automatically
                print("Non-interactive mode: proceeding with fix...")
            
            fix_exchange_timestamps(conn, dry_run=False)
            fix_last_traded_timestamps(conn, dry_run=False)
            print("\n✅ Timestamp fixes completed!")
    else:
        print("\n" + "="*80)
        print("DRY RUN - No changes made")
        print("Use --fix to actually update timestamps")
        print("="*80)
        fix_exchange_timestamps(conn, dry_run=True)
        fix_last_traded_timestamps(conn, dry_run=True)
    
    conn.close()

if __name__ == "__main__":
    main()

