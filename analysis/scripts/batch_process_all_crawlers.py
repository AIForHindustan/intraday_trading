#!/usr/bin/env python3
"""
Batch process all crawlers for a given date with caffeinate -i to keep Mac awake.
Runs overnight processing for all available crawlers.
"""

import sys
from pathlib import Path
import subprocess
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# All crawlers to process
ALL_CRAWLERS = [
    "zerodha_websocket",
    "intraday_data",
    "data_mining",
    "research_data",
    "binary_crawler",
]

def run_batch_for_date_and_crawler(date: str, crawler: str, db_path: str = "tick_data_production.db"):
    """Run batch processing for a single date and crawler."""
    logger.info(f"\n{'='*80}")
    logger.info(f"Processing: {crawler} for {date}")
    logger.info(f"{'='*80}\n")
    
    try:
        # Run the batch conversion and ingestion script
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
            logger.info(f"✅ Successfully processed {crawler} for {date}")
            return True
        else:
            logger.error(f"❌ Failed to process {crawler} for {date}")
            logger.error(f"Error output: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Exception processing {crawler} for {date}: {e}")
        return False


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch process all crawlers with caffeinate")
    parser.add_argument("--date", type=str, required=True, 
                       help="Date in YYYY-MM-DD format (e.g., 2025-10-01)")
    parser.add_argument("--db", type=str, default="tick_data_production.db",
                       help="Path to DuckDB database")
    parser.add_argument("--crawlers", type=str, nargs="+", default=ALL_CRAWLERS,
                       help="List of crawlers to process (default: all)")
    parser.add_argument("--skip-crawlers", type=str, nargs="+", default=[],
                       help="List of crawlers to skip")
    
    args = parser.parse_args()
    
    # Filter out skipped crawlers
    crawlers_to_process = [c for c in args.crawlers if c not in args.skip_crawlers]
    
    logger.info("="*80)
    logger.info("BATCH PROCESSING ALL CRAWLERS")
    logger.info(f"Date: {args.date}")
    logger.info(f"Crawlers: {', '.join(crawlers_to_process)}")
    logger.info("="*80)
    logger.info(f"\n⚠️  Running with caffeinate -i to keep Mac awake")
    logger.info(f"   Starting at: {datetime.now()}\n")
    
    # Run with caffeinate -i (keeps Mac awake indefinitely)
    # Use shell=True to run caffeinate wrapper
    results = {}
    
    for crawler in crawlers_to_process:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing crawler: {crawler}")
        logger.info(f"{'='*80}\n")
        
        success = run_batch_for_date_and_crawler(args.date, crawler, args.db)
        results[crawler] = success
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("BATCH PROCESSING SUMMARY")
    logger.info("="*80)
    
    successful = [c for c, s in results.items() if s]
    failed = [c for c, s in results.items() if not s]
    
    logger.info(f"\n✅ Successful: {len(successful)}/{len(crawlers_to_process)}")
    for crawler in successful:
        logger.info(f"   ✅ {crawler}")
    
    if failed:
        logger.info(f"\n❌ Failed: {len(failed)}/{len(crawlers_to_process)}")
        for crawler in failed:
            logger.info(f"   ❌ {crawler}")
    
    logger.info(f"\nCompleted at: {datetime.now()}")
    logger.info("="*80)


if __name__ == "__main__":
    main()

