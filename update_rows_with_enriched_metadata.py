#!/usr/bin/env python3
"""
Update Existing Rows with Enriched Metadata
Uses SQL UPDATE to enrich existing rows instead of re-parsing files
Much faster than re-processing thousands of files!
"""

import json
from pathlib import Path
import duckdb
from collections import defaultdict

def load_enriched_metadata(token_lookup_path: str = "zerodha_token_list/all_extracted_tokens_merged.json"):
    """Load enriched token metadata"""
    print(f"Loading enriched metadata from: {token_lookup_path}")
    
    path = Path(token_lookup_path)
    if not path.exists():
        print(f"❌ Token lookup file not found: {token_lookup_path}")
        return None
    
    with open(path) as f:
        data = json.load(f)
    
    # Build token -> metadata mapping
    token_metadata = {}
    for token_str, info in data.items():
        try:
            token = int(token_str)
            
            # Extract symbol from 'key' field or 'name'
            key_field = info.get("key", "")
            if ":" in key_field:
                tradingsymbol = key_field.split(":", 1)[1]
                exchange = key_field.split(":", 1)[0]
            else:
                tradingsymbol = info.get("name", "")
                exchange = info.get("exchange", "")
            
            token_metadata[token] = {
                "symbol": tradingsymbol or info.get("name", ""),
                "exchange": exchange or info.get("exchange", ""),
                "instrument_type": info.get("instrument_type", ""),
                "segment": info.get("segment") or info.get("source", ""),
                "name": info.get("name", ""),
            }
        except (ValueError, TypeError):
            continue
    
    print(f"✅ Loaded metadata for {len(token_metadata):,} tokens")
    return token_metadata

def update_rows_with_metadata(db_path: str, token_metadata: dict, dry_run: bool = True):
    """Update existing rows with enriched metadata"""
    
    conn = duckdb.connect(db_path)
    
    print(f"\n{'='*80}")
    print(f"UPDATING ROWS WITH ENRICHED METADATA ({'DRY RUN' if dry_run else 'ACTUAL UPDATE'})")
    print(f"{'='*80}")
    
    # Get tokens that exist in database but might benefit from enriched metadata
    # Find tokens that either:
    # 1. Have NULL/empty symbol/exchange
    # 2. Need better metadata
    
    print("\n1. Analyzing existing rows...")
    
    # Count rows by token metadata status
    stats = conn.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT instrument_token) as unique_tokens,
            COUNT(*) FILTER (WHERE symbol IS NULL OR symbol = '') as null_symbol,
            COUNT(*) FILTER (WHERE exchange IS NULL OR exchange = '') as null_exchange
        FROM tick_data_corrected
    """).fetchone()
    
    print(f"   Total rows: {stats[0]:,}")
    print(f"   Unique tokens: {stats[1]:,}")
    print(f"   Rows with NULL symbol: {stats[2]:,}")
    print(f"   Rows with NULL exchange: {stats[3]:,}")
    
    # Find tokens in database that are in enriched metadata
    tokens_to_update = conn.execute("""
        SELECT DISTINCT instrument_token
        FROM tick_data_corrected
        WHERE instrument_token IS NOT NULL
    """).fetchall()
    
    tokens_in_db = {row[0] for row in tokens_to_update}
    tokens_in_metadata = set(token_metadata.keys())
    
    tokens_with_metadata = tokens_in_db & tokens_in_metadata
    tokens_missing_metadata = tokens_in_db - tokens_in_metadata
    
    print(f"\n2. Token matching:")
    print(f"   Tokens in database: {len(tokens_in_db):,}")
    print(f"   Tokens in enriched metadata: {len(tokens_in_metadata):,}")
    print(f"   Tokens with enriched metadata available: {len(tokens_with_metadata):,}")
    print(f"   Tokens missing from enriched metadata: {len(tokens_missing_metadata):,}")
    
    if not tokens_with_metadata:
        print("\n⚠️  No tokens match enriched metadata")
        conn.close()
        return 0
    
    # Create a temporary table with enriched metadata
    print(f"\n3. Creating temporary metadata table...")
    
    # Build INSERT values for enriched tokens
    metadata_rows = []
    for token in tokens_with_metadata:
        meta = token_metadata[token]
        metadata_rows.append((
            token,
            meta["symbol"],
            meta["exchange"],
            meta["instrument_type"],
            meta["segment"],
            meta["name"],
        ))
    
    # Create temp table
    conn.execute("""
        CREATE TEMPORARY TABLE enriched_metadata (
            instrument_token BIGINT,
            symbol VARCHAR,
            exchange VARCHAR,
            instrument_type VARCHAR,
            segment VARCHAR,
            name VARCHAR
        )
    """)
    
    # Insert enriched metadata
    conn.executemany("""
        INSERT INTO enriched_metadata 
        (instrument_token, symbol, exchange, instrument_type, segment, name)
        VALUES (?, ?, ?, ?, ?, ?)
    """, metadata_rows)
    
    print(f"   Inserted {len(metadata_rows):,} token metadata entries")
    
    # Check what would be updated
    print(f"\n4. Checking rows that would be updated...")
    
    # Count rows that would benefit from update
    rows_to_update = conn.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (
                WHERE t.symbol IS NULL OR t.symbol = '' 
                OR t.exchange IS NULL OR t.exchange = ''
                OR t.instrument_type IS NULL
            ) as missing_data,
            COUNT(*) FILTER (
                WHERE t.symbol IS NOT NULL 
                AND t.symbol != '' 
                AND e.symbol IS NOT NULL
                AND e.symbol != ''
                AND t.symbol != e.symbol
            ) as different_symbol
        FROM tick_data_corrected t
        INNER JOIN enriched_metadata e ON t.instrument_token = e.instrument_token
    """).fetchone()
    
    print(f"   Rows with enriched metadata available: {rows_to_update[0]:,}")
    print(f"   Rows missing data (symbol/exchange NULL): {rows_to_update[1]:,}")
    print(f"   Rows with different symbols: {rows_to_update[2]:,}")
    
    if not dry_run and rows_to_update[0] > 0:
        print(f"\n5. Updating rows...")
        
        # DuckDB UPDATE syntax - use subquery approach
        # First, update exchange (most missing: 9.7M rows)
        print("   Updating exchange column...")
        exchange_updated = conn.execute("""
            UPDATE tick_data_corrected t
            SET exchange = e.exchange
            FROM enriched_metadata e
            WHERE t.instrument_token = e.instrument_token
              AND (t.exchange IS NULL OR t.exchange = '')
              AND e.exchange IS NOT NULL AND e.exchange != ''
        """).fetchone()
        
        # Update symbol if missing
        print("   Updating symbol column...")
        symbol_updated = conn.execute("""
            UPDATE tick_data_corrected t
            SET symbol = e.symbol
            FROM enriched_metadata e
            WHERE t.instrument_token = e.instrument_token
              AND (t.symbol IS NULL OR t.symbol = '')
              AND e.symbol IS NOT NULL AND e.symbol != ''
        """).fetchone()
        
        # Update instrument_type if missing
        print("   Updating instrument_type column...")
        type_updated = conn.execute("""
            UPDATE tick_data_corrected t
            SET instrument_type = e.instrument_type
            FROM enriched_metadata e
            WHERE t.instrument_token = e.instrument_token
              AND t.instrument_type IS NULL
              AND e.instrument_type IS NOT NULL AND e.instrument_type != ''
        """).fetchone()
        
        # Update segment if missing
        print("   Updating segment column...")
        segment_updated = conn.execute("""
            UPDATE tick_data_corrected t
            SET segment = e.segment
            FROM enriched_metadata e
            WHERE t.instrument_token = e.instrument_token
              AND (t.segment IS NULL OR t.segment = '')
              AND e.segment IS NOT NULL AND e.segment != ''
        """).fetchone()
        
        conn.commit()
        
        print(f"   ✅ Update complete")
        
        # Verify counts
        verify = conn.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE exchange IS NOT NULL AND exchange != '') as has_exchange,
                COUNT(*) FILTER (WHERE symbol IS NOT NULL AND symbol != '') as has_symbol,
                COUNT(*) FILTER (WHERE instrument_type IS NOT NULL) as has_type
            FROM tick_data_corrected
        """).fetchone()
        
        print(f"   After update: {verify[0]:,} rows with exchange, {verify[1]:,} with symbol, {verify[2]:,} with type")
        
        return rows_to_update[0]
    elif dry_run:
        print(f"\n5. [DRY RUN] Would update rows with enriched metadata")
        return rows_to_update[0]
    else:
        print(f"\n5. No rows to update")
        return 0
    
    conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Update existing rows with enriched metadata using SQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script updates existing database rows with enriched metadata instead of
re-parsing files. Much faster for large datasets!

Examples:
  # Dry run to see what would be updated
  python update_rows_with_enriched_metadata.py --db tick_data_production.db --dry-run

  # Actually update rows
  python update_rows_with_enriched_metadata.py --db tick_data_production.db --update
        """
    )
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--token-lookup", default="zerodha_token_list/all_extracted_tokens_merged.json", 
                       help="Path to enriched token lookup")
    parser.add_argument("--update", action="store_true", help="Actually update rows")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    
    args = parser.parse_args()
    
    # Load enriched metadata
    token_metadata = load_enriched_metadata(args.token_lookup)
    if not token_metadata:
        return
    
    # Update rows
    dry_run = not args.update
    if args.dry_run:
        dry_run = True
    
    rows_updated = update_rows_with_metadata(args.db, token_metadata, dry_run=dry_run)
    
    print(f"\n{'='*80}")
    if dry_run:
        print(f"DRY RUN COMPLETE")
        print(f"   Would update: {rows_updated:,} rows")
        print(f"   Use --update to actually update")
    else:
        print(f"✅ UPDATE COMPLETE")
        print(f"   Updated: {rows_updated:,} rows")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()

