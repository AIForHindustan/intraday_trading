# data_recovery.py
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import json
import logging
from typing import Dict, List, Optional
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Optional import for check_and_repair_file (used if available)
try:
    from tools.check_parquet_offset_before_discard import check_and_repair_file
except ImportError:
    check_and_repair_file = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def recover_corrupt_data(data_directory: str, output_dir: Optional[str] = None) -> Dict:
    """Recover data from corrupt files and emergency saves"""
    base_path = Path(data_directory)
    output_path = Path(output_dir) if output_dir else base_path / "recovered"
    output_path.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "emergency_saves_recovered": 0,
        "corrupt_files_found": 0,
        "corrupt_files_recovered": 0,
        "total_records_recovered": 0
    }
    
    # Recover from emergency JSON saves
    json_files = list(base_path.glob("emergency_save_*.json"))
    logger.info(f"Found {len(json_files)} emergency save files")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            if data:
                df = pd.DataFrame(data)
                parquet_file = output_path / json_file.with_suffix('.parquet').name
                df.to_parquet(parquet_file, engine='pyarrow', compression='snappy')
                stats["emergency_saves_recovered"] += 1
                stats["total_records_recovered"] += len(data)
                logger.info(f"✅ Recovered {len(data)} records from {json_file.name}")
                json_file.unlink()  # Remove JSON file after recovery
        except Exception as e:
            logger.error(f"❌ Failed to recover {json_file}: {e}")
    
    # Recover from corrupt Parquet files
    corrupt_files = list(base_path.glob("*.corrupt"))
    logger.info(f"Found {len(corrupt_files)} corrupt parquet files")
    stats["corrupt_files_found"] = len(corrupt_files)
    
    for corrupt_file in corrupt_files:
        try:
            recovered_df = recover_corrupt_parquet(corrupt_file)
            if recovered_df is not None and len(recovered_df) > 0:
                recovered_file = output_path / corrupt_file.with_suffix('.parquet').name
                recovered_df.to_parquet(recovered_file, engine='pyarrow', compression='snappy')
                stats["corrupt_files_recovered"] += 1
                stats["total_records_recovered"] += len(recovered_df)
                logger.info(f"✅ Recovered {len(recovered_df)} records from {corrupt_file.name}")
        except Exception as e:
            logger.error(f"❌ Failed to recover {corrupt_file.name}: {e}")
    
    # Also check for files with Thrift errors
    # Get pattern if date specified via glob
    pattern = "*.parquet"
    
    parquet_files = list(base_path.glob(pattern))
    logger.info(f"Checking {len(parquet_files)} parquet files for corruption...")
    
    for parquet_file in parquet_files:
        # Skip already recovered files
        if "recovered_" in parquet_file.name or parquet_file.parent.name == "recovered":
            continue
            
        try:
            # Quick validation
            pq.ParquetFile(parquet_file)
        except Exception as e:
            error_str = str(e)
            if "TProtocolException" in error_str or "deserialize thrift" in error_str.lower() or "Invalid data" in error_str:
                logger.warning(f"⚠️  Found corrupted file: {parquet_file.name}")
                try:
                    # Use the check script's recovery method (if available)
                    recovered_df = None
                    if check_and_repair_file:
                        result = check_and_repair_file(parquet_file, repair=False)
                        
                        if result.get("status") == "recovered_partial" and "recovered_df" in result:
                            recovered_df = result["recovered_df"]
                    
                    # If check_and_repair_file didn't work, try our own recovery method
                    if recovered_df is None or len(recovered_df) == 0:
                        recovered_df = recover_corrupt_parquet(parquet_file)
                        if recovered_df is not None and len(recovered_df) > 0:
                            recovered_file = output_path / f"recovered_{parquet_file.name}"
                            recovered_df.to_parquet(recovered_file, engine='pyarrow', compression='snappy')
                            stats["corrupt_files_recovered"] += 1
                            stats["total_records_recovered"] += len(recovered_df)
                            logger.info(f"✅ Recovered {len(recovered_df)} records from {parquet_file.name}")
                except Exception as recover_error:
                    logger.error(f"❌ Recovery failed for {parquet_file.name}: {recover_error}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Recovery Summary:")
    logger.info(f"  Emergency saves recovered: {stats['emergency_saves_recovered']}")
    logger.info(f"  Corrupt files found: {stats['corrupt_files_found']}")
    logger.info(f"  Corrupt files recovered: {stats['corrupt_files_recovered']}")
    logger.info(f"  Total records recovered: {stats['total_records_recovered']:,}")
    
    return stats


def recover_corrupt_parquet(file_path: Path) -> Optional[pd.DataFrame]:
    """Attempt to recover data from a corrupt parquet file"""
    try:
        # Try reading with row groups
        pf = pq.ParquetFile(file_path)
        row_group_dfs = []
        
        for rg_idx in range(pf.num_row_groups):
            try:
                rg_table = pf.read_row_group(rg_idx)
                rg_df = rg_table.to_pandas()
                row_group_dfs.append(rg_df)
            except Exception:
                continue
        
        if row_group_dfs:
            return pd.concat(row_group_dfs, ignore_index=True)
    except Exception:
        pass
    
    # Try different engines
    for engine in ['pyarrow', 'fastparquet']:
        try:
            df = pd.read_parquet(file_path, engine=engine)
            return df
        except Exception:
            continue
    
    return None


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Recover data from corrupt files")
    parser.add_argument("--directory", default="crawlers/raw_data/intraday_data", help="Data directory")
    parser.add_argument("--output", help="Output directory for recovered files")
    parser.add_argument("--date", help="Filter by date (YYYYMMDD)")
    
    args = parser.parse_args()
    
    # Store args for use in function
    import argparse as ap
    recover_corrupt_data(args.directory, args.output)