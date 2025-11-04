#!/usr/bin/env python3
"""
Batch Convert External Volume Files (.bin/.dat) to Parquet
Converts all files from windowsdata_source and macdata_source to standard parquet format
"""

import sys
from pathlib import Path
import logging
from typing import List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from analysis.scripts.binary_to_parquet_parquet import convert_binary_to_parquet
from token_cache import TokenCacheManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_all_external_files(
    input_dirs: List[Path],
    output_dir: Path = Path("temp_parquet/converted_parquet"),
    token_cache_path: str = None  # Will auto-detect best available
) -> dict:
    """Convert all .bin and .dat files from external volume copies to parquet"""
    
    # Collect all binary files
    all_files = []
    for input_dir in input_dirs:
        if not input_dir.exists():
            logger.warning(f"Input directory not found: {input_dir}")
            continue
        
        bin_files = list(input_dir.glob("*.bin"))
        dat_files = list(input_dir.glob("*.dat"))
        all_files.extend(bin_files)
        all_files.extend(dat_files)
        logger.info(f"Found {len(bin_files)} .bin and {len(dat_files)} .dat files in {input_dir}")
    
    if not all_files:
        logger.error("No binary files found to convert")
        return {"success": False, "error": "No files found"}
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Converting {len(all_files)} files to parquet...")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"{'='*60}\n")
    
    # Load token cache
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=True)
    # Auto-detect token cache if not provided
    if token_cache_path is None:
        # Prefer enriched version (more complete)
        enriched_path = Path("core/data/token_lookup_enriched.json")
        default_path = Path("core/data/token_lookup.json")
        
        if enriched_path.exists():
            token_cache_path = str(enriched_path)
            logger.info(f"Using token cache: {enriched_path.name} (auto-detected)")
        elif default_path.exists():
            token_cache_path = str(default_path)
            logger.info(f"Using token cache: {default_path.name} (auto-detected)")
        else:
            logger.warning(f"⚠️  No token cache found. Will continue without metadata enrichment.")
            token_cache_path = None
    
    # Load token cache
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=True)
    logger.info(f"Loaded {len(token_cache.token_map):,} instruments from token cache\n")
    
    # Convert each file
    output_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_files = []
    total_rows = 0
    files_processed = 0
    files_failed = 0
    total_errors = 0
    total_skipped = 0
    
    for i, binary_file in enumerate(sorted(all_files), 1):
        logger.info(f"\n[{i}/{len(all_files)}] Processing: {binary_file.name}")
        logger.info(f"  Source: {binary_file.parent.name}")
        
        try:
            result = convert_binary_to_parquet(
                binary_file,
                output_dir,
                token_cache,
                None
            )
            
            if result.get("success"):
                parquet_files.append(result["parquet_file"])
                total_rows += result["row_count"]
                total_errors += result.get("errors", 0)
                total_skipped += result.get("skipped", 0)
                files_processed += 1
                logger.info(f"  ✅ Success: {result['row_count']:,} rows")
            else:
                files_failed += 1
                logger.error(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            files_failed += 1
            logger.error(f"  ❌ Exception: {e}", exc_info=True)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("CONVERSION SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total files: {len(all_files)}")
    logger.info(f"  ✅ Successful: {files_processed}")
    logger.info(f"  ❌ Failed: {files_failed}")
    logger.info(f"Total rows converted: {total_rows:,}")
    logger.info(f"Total errors: {total_errors:,}")
    logger.info(f"Total skipped: {total_skipped:,}")
    logger.info(f"Parquet files created: {len(parquet_files)}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"{'='*60}\n")
    
    return {
        "success": files_processed > 0,
        "files_processed": files_processed,
        "files_failed": files_failed,
        "total_rows": total_rows,
        "parquet_files": parquet_files,
        "output_dir": output_dir
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Convert external volume files to parquet")
    parser.add_argument(
        "--input-dirs",
        nargs="+",
        default=[
            "temp_parquet/windowsdata_source",
            "temp_parquet/macdata_source"
        ],
        help="Input directories containing .bin/.dat files"
    )
    parser.add_argument(
        "--output-dir",
        default="temp_parquet/converted_parquet",
        help="Output directory for parquet files"
    )
    parser.add_argument(
        "--token-cache",
        default=None,
        help="Path to token cache file (default: auto-detect token_lookup_enriched.json or token_lookup.json)"
    )
    
    args = parser.parse_args()
    
    input_dirs = [Path(d) for d in args.input_dirs]
    output_dir = Path(args.output_dir)
    
    result = convert_all_external_files(
        input_dirs=input_dirs,
        output_dir=output_dir,
        token_cache_path=args.token_cache
    )
    
    if result["success"]:
        logger.info(f"\n✅ Conversion complete! Ready for ingestion.")
        logger.info(f"Run ingestion script to load parquet files into DuckDB")
    else:
        logger.error(f"\n❌ Conversion had errors. Check logs above.")
        sys.exit(1)

