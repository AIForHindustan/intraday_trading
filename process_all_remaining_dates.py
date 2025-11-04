#!/usr/bin/env python3
"""
Process all remaining dates and crawlers that haven't been processed yet.
Checks database to skip already processed files.
"""

import sys
from pathlib import Path
import subprocess
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load available dates
def load_available_dates():
    """Load available dates from file or scan directories."""
    dates_file = Path("available_dates_by_crawler.txt")
    if dates_file.exists():
        with open(dates_file, 'r') as f:
            return json.load(f)
    
    # Fallback: return known dates
    return {
        "zerodha_websocket": ["20250930", "20251001", "20251002", "20251003", "20251004"],
        "intraday_data": ["20251008", "20251009", "20251010", "20251012", "20251013", "20251014", "20251015", "20251016", "20251017", "20251018", "20251019", "20251020", "20251021", "20251022", "20251023", "20251024", "20251025", "20251026"],
        "data_mining": ["20251008", "20251009", "20251010", "20251012", "20251013", "20251014", "20251015", "20251016", "20251017", "20251018", "20251019", "20251020", "20251021", "20251022", "20251023", "20251024"],
        "research_data": [],
        "binary_crawler": ["20251006", "20251007"],
    }

def check_file_processed(db_path: str, date: str, crawler: str) -> bool:
    """Check if files for this date/crawler are already in database."""
    import duckdb
    try:
        conn = duckdb.connect(db_path)
        date_pattern = date.replace('-', '') if '-' in date else date
        
        # Build query based on crawler
        if crawler == "zerodha_websocket":
            pattern = f"%zerodha_websocket%{date_pattern}%"
        elif crawler == "intraday_data":
            pattern = f"%intraday_data%{date_pattern}%"
        elif crawler == "data_mining":
            pattern = f"%data_mining%{date_pattern}%"
        elif crawler == "research_data":
            pattern = f"%research_data%{date_pattern}%"
        elif crawler == "binary_crawler":
            pattern = f"%binary_crawler%{date_pattern}%"
        else:
            pattern = f"%{crawler}%{date_pattern}%"
        
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM tick_data_corrected 
            WHERE source_file LIKE ?
        """, [pattern]).fetchone()[0]
        
        conn.close()
        return result > 0
    except Exception as e:
        logger.warning(f"Error checking database: {e}")
        return False

def process_date_and_crawler(date: str, crawler: str, db_path: str):
    """Process a single date and crawler."""
    logger.info(f"Processing: {crawler} for {date}")
    
    try:
        cmd = [
            sys.executable,
            "batch_convert_and_ingest_dat.py",
            "--date", date,
            "--crawler", crawler,
            "--db", db_path,
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            logger.info(f"✅ Success: {crawler} for {date}")
            return True
        else:
            # Check if it's just "no files found" (expected)
            if "No .dat files" in result.stderr or "No .bin files" in result.stderr:
                logger.info(f"⏭️  Skipped: {crawler} for {date} (no files)")
                return True  # Not an error
            else:
                logger.error(f"❌ Failed: {crawler} for {date}")
                logger.error(f"Error: {result.stderr[:500]}")
                return False
    except Exception as e:
        logger.error(f"❌ Exception: {crawler} for {date}: {e}")
        return False

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Process all remaining dates for all crawlers")
    parser.add_argument("--db", type=str, default="tick_data_production.db",
                       help="Path to DuckDB database")
    parser.add_argument("--skip-check", action="store_true",
                       help="Skip database check (process even if already processed)")
    parser.add_argument("--start-date", type=str, default=None,
                       help="Start from this date (YYYY-MM-DD or YYYYMMDD)")
    parser.add_argument("--end-date", type=str, default=None,
                       help="End at this date (YYYY-MM-DD or YYYYMMDD)")
    
    args = parser.parse_args()
    
    # Load available dates
    available_dates = load_available_dates()
    
    # Convert dates to YYYY-MM-DD format
    def normalize_date(date_str: str) -> str:
        if '-' in date_str:
            return date_str
        # Convert YYYYMMDD to YYYY-MM-DD
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    
    def denormalize_date(date_str: str) -> str:
        # Convert YYYY-MM-DD to YYYYMMDD
        return date_str.replace('-', '')
    
    logger.info("="*80)
    logger.info("PROCESSING ALL REMAINING DATES")
    logger.info("="*80)
    
    total_tasks = 0
    successful = 0
    skipped = 0
    failed = 0
    
    # Process each crawler
    for crawler, dates in available_dates.items():
        if not dates:
            logger.info(f"\n⏭️  Skipping {crawler}: No dates available")
            continue
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing crawler: {crawler}")
        logger.info(f"  Dates available: {len(dates)}")
        logger.info(f"{'='*80}")
        
        for date_str in sorted(dates):
            date_normalized = normalize_date(date_str)
            
            # Check date filters
            if args.start_date:
                start_norm = normalize_date(args.start_date).replace('-', '')
                if date_str < start_norm:
                    continue
            if args.end_date:
                end_norm = normalize_date(args.end_date).replace('-', '')
                if date_str > end_norm:
                    continue
            
            # Check if already processed
            if not args.skip_check:
                date_for_check = date_normalized
                if check_file_processed(args.db, date_str, crawler):
                    logger.info(f"⏭️  Already processed: {crawler} for {date_normalized}")
                    skipped += 1
                    continue
            
            total_tasks += 1
            success = process_date_and_crawler(date_normalized, crawler, args.db)
            
            if success:
                successful += 1
            else:
                failed += 1
    
    logger.info("\n" + "="*80)
    logger.info("PROCESSING COMPLETE")
    logger.info("="*80)
    logger.info(f"Total tasks: {total_tasks}")
    logger.info(f"✅ Successful: {successful}")
    logger.info(f"⏭️  Skipped (already processed): {skipped}")
    logger.info(f"❌ Failed: {failed}")
    logger.info(f"Completed at: {datetime.now()}")
    logger.info("="*80)

if __name__ == "__main__":
    main()

