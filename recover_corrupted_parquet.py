#!/usr/bin/env python3
"""
Parquet File Recovery Utility
Recovers readable data from corrupted parquet files
"""

from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def recover_parquet_file(corrupted_file: Path, output_file: Path = None) -> dict:
    """
    Attempt to recover data from a corrupted parquet file.
    
    Args:
        corrupted_file: Path to corrupted parquet file
        output_file: Path for recovered file (default: adds _RECOVERED suffix)
    
    Returns:
        dict with recovery statistics
    """
    if not corrupted_file.exists():
        raise FileNotFoundError(f"File not found: {corrupted_file}")
    
    if output_file is None:
        output_file = corrupted_file.parent / f"{corrupted_file.stem}_RECOVERED.parquet"
    
    logger.info(f"Recovering file: {corrupted_file.name}")
    
    try:
        # Try to read metadata
        parquet_file = pq.ParquetFile(corrupted_file)
        schema = parquet_file.schema
        expected_rows = parquet_file.metadata.num_rows
        
        logger.info(f"Schema: {len(schema)} columns, Expected rows: {expected_rows:,}")
        
        # Test each column
        readable_columns = []
        corrupted_columns = []
        
        for col in schema:
            try:
                table = parquet_file.read(columns=[col.name])
                if len(table) == expected_rows:
                    readable_columns.append(col.name)
                else:
                    logger.warning(f"Column '{col.name}' readable but row count mismatch: {len(table)} vs {expected_rows}")
                    readable_columns.append(col.name)  # Still include it
            except Exception as e:
                corrupted_columns.append(col.name)
                logger.debug(f"Column '{col.name}' corrupted: {str(e)[:80]}")
        
        logger.info(f"Readable: {len(readable_columns)}/{len(schema)} ({len(readable_columns)/len(schema)*100:.1f}%)")
        logger.info(f"Corrupted: {len(corrupted_columns)}/{len(schema)} ({len(corrupted_columns)/len(schema)*100:.1f}%)")
        
        if len(readable_columns) == 0:
            return {
                "success": False,
                "error": "No readable columns found",
                "readable_columns": 0,
                "corrupted_columns": len(corrupted_columns)
            }
        
        # Read all readable columns
        logger.info(f"Reading {len(readable_columns)} readable columns...")
        recovered_table = parquet_file.read(columns=readable_columns)
        
        logger.info(f"Successfully recovered {len(recovered_table):,} rows")
        
        # Write recovered file
        logger.info(f"Writing recovered file: {output_file.name}")
        pq.write_table(recovered_table, output_file, compression='snappy')
        
        # Verify recovered file
        recovered_meta = pq.ParquetFile(output_file)
        recovered_table_check = recovered_meta.read()
        
        return {
            "success": True,
            "original_rows": expected_rows,
            "recovered_rows": len(recovered_table_check),
            "readable_columns": len(readable_columns),
            "corrupted_columns": len(corrupted_columns),
            "readable_column_names": readable_columns,
            "corrupted_column_names": corrupted_columns,
            "output_file": str(output_file),
            "output_size": output_file.stat().st_size
        }
        
    except Exception as e:
        logger.error(f"Recovery failed: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "readable_columns": 0,
            "corrupted_columns": 0
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Recover data from corrupted parquet files")
    parser.add_argument("file", type=str, help="Path to corrupted parquet file")
    parser.add_argument("-o", "--output", type=str, default=None, help="Output file path")
    
    args = parser.parse_args()
    
    corrupted_file = Path(args.file)
    output_file = Path(args.output) if args.output else None
    
    result = recover_parquet_file(corrupted_file, output_file)
    
    print("\n" + "=" * 80)
    print("RECOVERY RESULTS")
    print("=" * 80)
    
    if result["success"]:
        print(f"✅ Recovery successful!")
        print(f"   Original rows: {result['original_rows']:,}")
        print(f"   Recovered rows: {result['recovered_rows']:,}")
        print(f"   Readable columns: {result['readable_columns']}/{result['readable_columns'] + result['corrupted_columns']}")
        print(f"   Output file: {result['output_file']}")
        print(f"   Output size: {result['output_size']:,} bytes")
        
        if result['corrupted_column_names']:
            print(f"\n   Missing columns ({len(result['corrupted_column_names'])}):")
            for col in result['corrupted_column_names']:
                print(f"      - {col}")
    else:
        print(f"❌ Recovery failed: {result.get('error', 'Unknown error')}")
    
    print("=" * 80)
    
    sys.exit(0 if result["success"] else 1)

