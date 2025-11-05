#!/usr/bin/env python3
"""
Fix UNKNOWN symbols in parquet files by re-processing their source binary files.

This script:
1. Reads list of parquet files with UNKNOWN symbols from CSV
2. Finds the original binary source files (.dat/.bin)
3. Re-processes them with improved parsing to create corrected parquet files
4. Deletes UNKNOWN rows from database for those source files
5. Ingests the corrected parquet files
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import re
import duckdb
import pandas as pd
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import resource
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analysis.scripts.binary_to_parquet_parquet import convert_binary_to_parquet
from tools.enrich_binary_with_jsonl_metadata import JSONLMetadataEnricher
from token_cache import TokenCacheManager
from parquet_ingester import ingest_parquet_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ParquetUnknownFixer:
    """Fix UNKNOWN symbols in parquet files by re-processing source binary files."""
    
    def __init__(self, db_path: str, output_dir: Path = None):
        self.db_path = db_path
        self.output_dir = output_dir or Path('temp_parquet/fixed_parquet')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load token cache
        self.token_cache = TokenCacheManager(
            cache_path="core/data/token_lookup_enriched.json",
            verbose=False
        )
        logger.info(f"Loaded {len(self.token_cache.token_map):,} instruments")
        
        # Initialize JSONL enricher (will load JSONL files on demand)
        # Try to find JSONL directory
        jsonl_dirs = [
            Path('temp_parquet/windowsdata_source'),
            Path('temp_parquet/macdata_source'),
            Path('temp_parquet'),
        ]
        self.jsonl_dir = None
        for jsonl_dir in jsonl_dirs:
            if jsonl_dir.exists() and any(jsonl_dir.glob("*.jsonl.zst")):
                self.jsonl_dir = jsonl_dir
                break
        self.jsonl_enricher = None
        
        # Search paths for binary files
        self.search_paths = [
            Path('temp_parquet/windowsdata_source'),
            Path('temp_parquet/macdata_source'),
            Path('temp'),
            Path('temp_parquet'),
            Path('crawlers/raw_data/intraday_data'),
            Path('crawlers/raw_data/data_mining'),
            Path('crawlers/raw_data/binary_crawler/binary_crawler/raw_binary'),
        ]
    
    def find_original_binary_file(self, parquet_name: str) -> Optional[Path]:
        """Find the original binary file that created this parquet file."""
        stem = Path(parquet_name).stem
        
        # Extract date and time from parquet name
        # Format: intraday_data_20251009_123305
        match = re.search(r'(\d{8})_(\d{6})', stem)
        if match:
            date = match.group(1)
            time = match.group(2)
        else:
            date = None
            time = None
        
        # Search for binary files
        for search_path in self.search_paths:
            if not search_path.exists():
                continue
            
            # Try different patterns
            patterns = [
                f"*{date}*{time}*.bin" if date and time else None,
                f"*{date}*{time}*.dat" if date and time else None,
                f"*{stem}*.bin",
                f"*{stem}*.dat",
                f"{stem}.bin",
                f"{stem}.dat",
            ]
            
            for pattern in patterns:
                if pattern is None:
                    continue
                matches = list(search_path.rglob(pattern))
                if matches:
                    # Prefer exact match
                    exact_matches = [m for m in matches if m.stem == stem]
                    if exact_matches:
                        return exact_matches[0]
                    return matches[0]
        
        return None
    
    def delete_unknown_rows_from_db(self, source_file: str) -> int:
        """Delete rows with UNKNOWN symbols for a given source file."""
        conn = duckdb.connect(self.db_path)
        try:
            # Count rows to delete
            count_query = """
                SELECT COUNT(*) 
                FROM tick_data_corrected
                WHERE source_file = ?
                  AND exchange_timestamp >= '2025-01-01' AND exchange_timestamp < '2026-01-01'
                  AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
            """
            count = conn.execute(count_query, [source_file]).fetchone()[0]
            
            if count == 0:
                logger.info(f"  No UNKNOWN rows to delete for {Path(source_file).name}")
                return 0
            
            # Delete UNKNOWN rows
            delete_query = """
                DELETE FROM tick_data_corrected
                WHERE source_file = ?
                  AND exchange_timestamp >= '2025-01-01' AND exchange_timestamp < '2026-01-01'
                  AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
            """
            conn.execute(delete_query, [source_file])
            conn.commit()
            
            logger.info(f"  ✅ Deleted {count:,} UNKNOWN rows for {Path(source_file).name}")
            return count
        except Exception as e:
            logger.error(f"  ❌ Error deleting UNKNOWN rows: {e}")
            try:
                conn.rollback()
            except:
                pass
            return 0
        finally:
            conn.close()
    
    def fix_parquet_file(self, parquet_file_name: str) -> Dict:
        """Fix a single parquet file by re-processing its source binary file."""
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing: {parquet_file_name}")
        logger.info(f"{'='*80}")
        
        # Find original binary file
        binary_file = self.find_original_binary_file(parquet_file_name)
        if not binary_file:
            logger.warning(f"  ❌ Could not find source binary file for {parquet_file_name}")
            return {"success": False, "error": "Source binary file not found"}
        
        if not binary_file.exists():
            logger.warning(f"  ❌ Binary file does not exist: {binary_file}")
            return {"success": False, "error": "Binary file not found"}
        
        logger.info(f"  Found source: {binary_file}")
        
        # Load JSONL enricher if not loaded (lazy load)
        if self.jsonl_enricher is None and self.jsonl_dir:
            logger.info(f"  Loading JSONL metadata enricher from {self.jsonl_dir}...")
            self.jsonl_enricher = JSONLMetadataEnricher(self.jsonl_dir)
            self.jsonl_enricher.load_jsonl_files()
            logger.info(f"  Loaded JSONL metadata from {len(self.jsonl_enricher.metadata_by_token)} tokens")
        elif self.jsonl_enricher is None:
            logger.info("  No JSONL directory found - skipping JSONL metadata enrichment")
        
        # Convert binary to parquet
        logger.info(f"  Re-processing binary file to create corrected parquet...")
        result = convert_binary_to_parquet(
            input_file=binary_file,
            output_dir=self.output_dir,
            token_cache=self.token_cache,
            metadata_calculator=None,
            jsonl_enricher=self.jsonl_enricher
        )
        
        if not result.get("success"):
            logger.error(f"  ❌ Failed to convert: {result.get('error', 'Unknown error')}")
            return result
        
        corrected_parquet = result["parquet_file"]
        logger.info(f"  ✅ Created corrected parquet: {corrected_parquet.name} ({result['row_count']:,} rows)")
        
        # Delete UNKNOWN rows from database
        logger.info(f"  Deleting UNKNOWN rows from database...")
        deleted_count = self.delete_unknown_rows_from_db(parquet_file_name)
        
        # Ingest corrected parquet file
        logger.info(f"  Ingesting corrected parquet file...")
        try:
            # Note: ingest_parquet_file uses the parquet file path as source_file
            # We'll need to update the source_file in the database after ingestion
            ingest_result = ingest_parquet_file(
                file_path=corrected_parquet,
                db_path=self.db_path,
                token_cache=self.token_cache,
                enrich=True
            )
            
            if ingest_result.get("success"):
                rows_inserted = ingest_result.get('row_count', 0)
                logger.info(f"  ✅ Ingested {rows_inserted:,} rows")
                
                # Update source_file and processing_batch to match original parquet file name
                if rows_inserted > 0:
                    conn = duckdb.connect(self.db_path)
                    try:
                        update_query = """
                            UPDATE tick_data_corrected
                            SET source_file = ?,
                                processing_batch = 'fix_unknown_reprocess'
                            WHERE source_file = ?
                              AND exchange_timestamp >= '2025-01-01' AND exchange_timestamp < '2026-01-01'
                              AND processing_batch = 'parquet_ingestion_enriched'
                        """
                        updated = conn.execute(update_query, [parquet_file_name, str(corrected_parquet)]).fetchone()
                        conn.commit()
                        logger.info(f"  ✅ Updated source_file and processing_batch for {rows_inserted:,} rows")
                    except Exception as e:
                        logger.warning(f"  ⚠️  Could not update source_file: {e}")
                    finally:
                        conn.close()
                
                return {
                    "success": True,
                    "parquet_file": corrected_parquet,
                    "rows_inserted": rows_inserted,
                    "rows_deleted": deleted_count,
                    "packets_processed": result.get("packets_processed", 0)
                }
            else:
                logger.error(f"  ❌ Ingestion failed: {ingest_result.get('error', 'Unknown error')}")
                return {"success": False, "error": f"Ingestion failed: {ingest_result.get('error')}"}
        except Exception as e:
            logger.error(f"  ❌ Error ingesting: {e}")
            return {"success": False, "error": str(e)}
    
    def fix_parquet_files_from_csv(self, csv_file: Path, limit: int = None, workers: int = 1, max_memory_gb: float = None) -> Dict:
        """Fix parquet files listed in CSV file."""
        logger.info(f"Reading parquet files from: {csv_file}")
        df = pd.read_csv(csv_file)
        
        if limit:
            df = df.head(limit)
            logger.info(f"Processing first {limit} files")
        
        logger.info(f"Total files to process: {len(df):,}")
        logger.info(f"Workers: {workers}")
        if max_memory_gb:
            logger.info(f"Max memory per worker: {max_memory_gb}GB")
        
        results = {
            "success": [],
            "failed": [],
            "total_rows_inserted": 0,
            "total_rows_deleted": 0,
            "total_files_processed": 0
        }
        
        if workers == 1:
            # Sequential processing
            for idx, row in df.iterrows():
                logger.info(f"\n[{idx+1}/{len(df)}] Processing file {idx+1}")
                result = self.fix_parquet_file(row['source_file'])
                
                if result.get("success"):
                    results["success"].append(row['source_file'])
                    results["total_rows_inserted"] += result.get("rows_inserted", 0)
                    results["total_rows_deleted"] += result.get("rows_deleted", 0)
                    results["total_files_processed"] += 1
                else:
                    results["failed"].append({
                        "file": row['source_file'],
                        "error": result.get("error", "Unknown error")
                    })
        else:
            # Parallel processing
            file_list = df['source_file'].tolist()
            tasks = [(i+1, len(file_list), file_name) for i, file_name in enumerate(file_list)]
            
            with ProcessPoolExecutor(max_workers=workers) as executor:
                # Set memory limit for each worker
                if max_memory_gb:
                    # Convert GB to bytes
                    max_memory_bytes = int(max_memory_gb * 1024 * 1024 * 1024)
                    # Set memory limit (this will be inherited by child processes)
                    try:
                        # Set soft limit (will raise exception if exceeded)
                        resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))
                    except ValueError:
                        # On some systems, we can't set this limit
                        logger.warning(f"Could not set memory limit, using system default")
                
                # Submit all tasks
                future_to_file = {
                    executor.submit(process_file_worker, self.db_path, str(self.output_dir), file_name, idx, total): file_name
                    for idx, total, file_name in tasks
                }
                
                # Process completed tasks
                completed = 0
                for future in as_completed(future_to_file):
                    file_name = future_to_file[future]
                    completed += 1
                    try:
                        result = future.result()
                        if result.get("success"):
                            results["success"].append(file_name)
                            results["total_rows_inserted"] += result.get("rows_inserted", 0)
                            results["total_rows_deleted"] += result.get("rows_deleted", 0)
                            results["total_files_processed"] += 1
                            logger.info(f"[{completed}/{len(tasks)}] ✅ {Path(file_name).name}: {result.get('rows_inserted', 0):,} rows")
                        else:
                            results["failed"].append({
                                "file": file_name,
                                "error": result.get("error", "Unknown error")
                            })
                            logger.error(f"[{completed}/{len(tasks)}] ❌ {Path(file_name).name}: {result.get('error')}")
                    except Exception as e:
                        logger.error(f"[{completed}/{len(tasks)}] ❌ {Path(file_name).name}: {str(e)}")
                        results["failed"].append({
                            "file": file_name,
                            "error": str(e)
                        })
        
        logger.info(f"\n{'='*80}")
        logger.info(f"SUMMARY")
        logger.info(f"{'='*80}")
        logger.info(f"Files processed successfully: {len(results['success']):,}")
        logger.info(f"Files failed: {len(results['failed']):,}")
        logger.info(f"Total rows inserted: {results['total_rows_inserted']:,}")
        logger.info(f"Total rows deleted: {results['total_rows_deleted']:,}")
        
        if results["failed"]:
            logger.info(f"\nFailed files:")
            for fail in results["failed"][:10]:  # Show first 10
                logger.info(f"  - {Path(fail['file']).name}: {fail['error']}")
            if len(results["failed"]) > 10:
                logger.info(f"  ... and {len(results['failed']) - 10} more")
        
        return results


def process_file_worker(db_path: str, output_dir: str, parquet_file_name: str, idx: int, total: int) -> Dict:
    """Worker function to process a single file in parallel."""
    # Set up logging for worker
    worker_logger = logging.getLogger(f"worker_{idx}")
    worker_logger.info(f"[{idx}/{total}] Processing: {parquet_file_name}")
    
    try:
        # Create a new fixer instance for this worker
        fixer = ParquetUnknownFixer(
            db_path=db_path,
            output_dir=Path(output_dir)
        )
        
        # Process the file
        result = fixer.fix_parquet_file(parquet_file_name)
        return result
    except Exception as e:
        worker_logger.error(f"[{idx}/{total}] Error processing {parquet_file_name}: {e}")
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Fix UNKNOWN symbols in parquet files")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("analysis/parquet_files_with_unknown.csv"),
        help="CSV file with list of parquet files to fix"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="analysis/tick_data_production.db",
        help="Path to DuckDB database"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("temp_parquet/fixed_parquet"),
        help="Output directory for corrected parquet files"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to process (for testing)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1)"
    )
    parser.add_argument(
        "--max-memory",
        type=float,
        default=None,
        help="Maximum memory per worker in GB (default: no limit)"
    )
    
    args = parser.parse_args()
    
    # Check if CSV exists
    if not args.csv.exists():
        logger.error(f"CSV file not found: {args.csv}")
        logger.error("Run the identification script first to create the CSV")
        return 1
    
    # Initialize fixer
    fixer = ParquetUnknownFixer(
        db_path=args.db,
        output_dir=args.output_dir
    )
    
    # Fix files
    results = fixer.fix_parquet_files_from_csv(
        args.csv, 
        limit=args.limit,
        workers=args.workers,
        max_memory_gb=args.max_memory
    )
    
    return 0 if len(results["failed"]) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

