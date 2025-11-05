#!/usr/bin/env python3
"""
Fix corrupt parquet files by investigating offset issues and recovering data
Recovers files from .corrupt_temp directory and re-ingests them
"""

import sys
from pathlib import Path
import logging
import pyarrow.parquet as pq
import pandas as pd
from typing import Dict, List, Optional
import struct

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from token_cache import TokenCacheManager
from ingest_parquet_duckdb_native import enrich_parquet_file_worker, bulk_ingest_enriched_parquets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def try_repair_metadata_offset(file_path: Path) -> Optional[Path]:
    """Try to repair metadata offset issues by finding correct footer position"""
    try:
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
        
        file_size = len(file_bytes)
        
        # Check last 8 bytes for footer length
        if file_size < 8:
            return None
        
        last_8 = file_bytes[-8:]
        footer_len = struct.unpack('<I', last_8[:4])[0]
        magic = last_8[4:]
        
        # If footer length seems wrong, try to find correct footer
        if footer_len > file_size or footer_len < 100:
            # Search backwards for PAR1 magic bytes
            par1_positions = []
            search_start = max(0, file_size - 50000)  # Search last 50KB
            pos = file_size - 4
            
            while pos >= search_start:
                if file_bytes[pos:pos+4] == b'PAR1':
                    par1_positions.append(pos)
                pos -= 1
            
            if par1_positions:
                # Try the last valid PAR1 position
                for par1_pos in reversed(par1_positions):
                    if par1_pos >= 8:
                        try_footer_len = struct.unpack('<I', file_bytes[par1_pos-8:par1_pos-4])[0]
                        if 1000 < try_footer_len < file_size:
                            # This might be a valid footer
                            logger.info(f"  Found potential footer at offset {par1_pos}, length {try_footer_len}")
                            # Create repaired file with corrected footer
                            repaired_data = file_bytes[:par1_pos+4]
                            # Update footer length in last 8 bytes
                            repaired_data[-8:-4] = struct.pack('<I', try_footer_len)
                            repaired_data[-4:] = b'PAR1'
                            
                            repaired_path = file_path.parent / f"{file_path.stem}_REPAIRED.parquet"
                            repaired_path.write_bytes(repaired_data)
                            logger.info(f"  Created repaired file: {repaired_path.name}")
                            return repaired_path
        
        return None
    except Exception as e:
        logger.debug(f"  Metadata repair failed: {e}")
        return None


def recover_parquet_column_by_column(file_path: Path) -> Optional[pd.DataFrame]:
    """Recover parquet file by reading columns individually"""
    # First try to repair metadata if needed
    repaired_file = try_repair_metadata_offset(file_path)
    file_to_read = repaired_file if repaired_file else file_path
    
    try:
        parquet_file = pq.ParquetFile(file_to_read)
        schema = parquet_file.schema
        metadata = parquet_file.metadata
        
        logger.info(f"  Schema: {len(schema)} columns, Expected rows: {metadata.num_rows}")
        
        recovered_data = {}
        failed_columns = []
        
        # Try reading row groups first (more efficient if it works)
        try:
            row_group_dfs = []
            for rg_idx in range(metadata.num_row_groups):
                try:
                    rg_table = parquet_file.read_row_group(rg_idx)
                    rg_df = rg_table.to_pandas()
                    row_group_dfs.append(rg_df)
                    logger.info(f"  ✅ Row group {rg_idx}: {len(rg_df)} rows")
                except Exception as e:
                    logger.debug(f"  Row group {rg_idx} failed: {str(e)[:60]}")
                    continue
            
            if row_group_dfs:
                df = pd.concat(row_group_dfs, ignore_index=True)
                logger.info(f"  ✅ Recovered {len(df)} rows via row group method")
                return df
        except:
            pass
        
        # Fallback: column-by-column recovery
        for col in schema:
            col_name = col.name
            try:
                # Try reading just this column from all row groups
                col_table = parquet_file.read(columns=[col_name])
                col_series = col_table.column(0).to_pandas()
                recovered_data[col_name] = col_series
            except Exception as e:
                failed_columns.append(col_name)
                logger.debug(f"  Column '{col_name}' failed: {str(e)[:60]}")
        
        if not recovered_data:
            return None
        
        # Find minimum length across all recovered columns
        lengths = [len(v) for v in recovered_data.values()]
        if not lengths:
            return None
            
        min_len = min(lengths)
        logger.info(f"  Recovered {len(recovered_data)}/{len(schema)} columns, min length: {min_len} rows")
        
        # Truncate all columns to minimum length
        recovered_data_truncated = {
            k: v.iloc[:min_len] for k, v in recovered_data.items()
        }
        
        # Add missing columns as None
        for col_name in schema.names:
            if col_name not in recovered_data_truncated:
                recovered_data_truncated[col_name] = pd.Series([None] * min_len, dtype=object)
        
        df = pd.DataFrame(recovered_data_truncated)
        
        # Ensure all schema columns are present
        for col in schema:
            if col.name not in df.columns:
                df[col.name] = None
        
        return df
        
    except Exception as e:
        # If metadata is completely corrupted, try DuckDB native read
        logger.info(f"  Trying DuckDB native read as last resort...")
        try:
            import duckdb
            conn = duckdb.connect(":memory:")
            try:
                df = conn.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 10000").fetchdf()
                if len(df) > 0:
                    logger.info(f"  ✅ Recovered {len(df)} rows via DuckDB native read")
                    conn.close()
                    return df
            except:
                pass
            conn.close()
        except:
            pass
        
        logger.error(f"  Recovery failed: {e}")
        return None


def fix_and_recover_corrupt_file(corrupt_file: Path, output_dir: Path, token_cache: TokenCacheManager) -> Dict:
    """Fix a single corrupt file and recover data"""
    original_name = corrupt_file.name.replace("CORRUPT_", "")
    
    logger.info(f"\n🔧 Fixing: {original_name}")
    logger.info(f"  Size: {corrupt_file.stat().st_size:,} bytes")
    
    # Try column-by-column recovery
    df = recover_parquet_column_by_column(corrupt_file)
    
    if df is None or len(df) == 0:
        return {
            "success": False,
            "error": "Could not recover any data",
            "file": original_name
        }
    
    # Check if we have critical columns
    critical_cols = ['instrument_token', 'exchange_timestamp_ns', 'symbol']
    missing_critical = [col for col in critical_cols if col not in df.columns or df[col].isna().all()]
    
    if missing_critical:
        logger.warning(f"  ⚠️  Missing critical columns: {missing_critical}")
    
    # Save recovered file
    recovered_file = output_dir / f"RECOVERED_{original_name}"
    try:
        df.to_parquet(recovered_file, engine='pyarrow', compression='snappy')
        logger.info(f"  ✅ Recovered {len(df)} rows, saved to {recovered_file.name}")
        
        # Now enrich and ingest
        logger.info(f"  📦 Enriching recovered file...")
        result = enrich_parquet_file_worker(
            str(recovered_file),
            str(Path("core/data/token_lookup_enriched.json"))
        )
        
        if result.get("success"):
            enriched_file = Path(result.get("enriched_file", ""))
            if enriched_file.exists():
                logger.info(f"  ✅ Enriched successfully: {len(enriched_file.read_bytes())} bytes")
                return {
                    "success": True,
                    "file": original_name,
                    "rows_recovered": len(df),
                    "columns_recovered": len(recovered_data) if 'recovered_data' in locals() else len(df.columns),
                    "enriched_file": str(enriched_file)
                }
            else:
                return {
                    "success": False,
                    "error": "Enrichment succeeded but file not found",
                    "file": original_name
                }
        else:
            return {
                "success": False,
                "error": result.get("error", "Enrichment failed"),
                "file": original_name,
                "rows_recovered": len(df)
            }
            
    except Exception as e:
        logger.error(f"  ❌ Failed to save recovered file: {e}")
        return {
            "success": False,
            "error": f"Save failed: {str(e)}",
            "file": original_name,
            "rows_recovered": len(df)
        }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix corrupt parquet files")
    parser.add_argument("--corrupt-dir", default="crawlers/raw_data/intraday_data/.corrupt_temp",
                       help="Directory containing corrupt files")
    parser.add_argument("--output-dir", default="crawlers/raw_data/intraday_data/.recovered_temp",
                       help="Output directory for recovered files")
    parser.add_argument("--db", default="analysis/tick_data_production.db",
                       help="Database path")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json",
                       help="Token cache path")
    parser.add_argument("--workers", type=int, default=1,
                       help="Number of workers (use 1 for sequential processing)")
    parser.add_argument("--ingest", action="store_true",
                       help="Ingest recovered files to database")
    
    args = parser.parse_args()
    
    corrupt_dir = Path(args.corrupt_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if not corrupt_dir.exists():
        logger.error(f"Corrupt directory not found: {corrupt_dir}")
        return
    
    # Find corrupt files
    corrupt_files = sorted(corrupt_dir.glob("CORRUPT_*.parquet"))
    logger.info(f"Found {len(corrupt_files)} corrupt files to recover")
    
    if not corrupt_files:
        logger.info("No corrupt files found")
        return
    
    # Load token cache
    token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False)
    
    # Recover files
    recovered_files = []
    failed_files = []
    
    for i, corrupt_file in enumerate(corrupt_files):
        logger.info(f"\n[{i+1}/{len(corrupt_files)}] Processing: {corrupt_file.name}")
        
        result = fix_and_recover_corrupt_file(corrupt_file, output_dir, token_cache)
        
        if result.get("success"):
            recovered_files.append(result)
            if result.get("enriched_file"):
                recovered_files[-1]["enriched_file_path"] = result["enriched_file"]
        else:
            failed_files.append(result)
        
        if (i + 1) % 10 == 0:
            logger.info(f"\nProgress: {i+1}/{len(corrupt_files)} files processed")
            logger.info(f"  Recovered: {len(recovered_files)}, Failed: {len(failed_files)}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Recovery Summary:")
    logger.info(f"  Total files: {len(corrupt_files)}")
    logger.info(f"  Recovered: {len(recovered_files)}")
    logger.info(f"  Failed: {len(failed_files)}")
    
    if recovered_files:
        total_rows = sum(r.get("rows_recovered", 0) for r in recovered_files)
        logger.info(f"  Total rows recovered: {total_rows:,}")
    
    # Ingest recovered files if requested
    if args.ingest and recovered_files:
        logger.info(f"\n📥 Ingesting {len(recovered_files)} recovered files...")
        
        enriched_files = [r["enriched_file_path"] for r in recovered_files if r.get("enriched_file_path")]
        
        if enriched_files:
            import duckdb
            from binary_to_parquet.production_binary_converter import ensure_production_schema
            
            conn = duckdb.connect(args.db)
            ensure_production_schema(conn)
            
            # Get actual columns from database schema
            schema_cols = conn.execute("DESCRIBE tick_data_corrected").fetchdf()
            db_columns = schema_cols['column_name'].tolist()
            
            logger.info(f"Database has {len(db_columns)} columns")
            logger.info(f"Ingesting {len(enriched_files)} enriched files...")
            
            ingest_result = bulk_ingest_enriched_parquets(conn, enriched_files, db_columns)
            
            conn.close()
            
            logger.info(f"\n✅ Ingestion complete:")
            logger.info(f"  Files ingested: {ingest_result.get('files_ingested', 0)}")
            logger.info(f"  Rows ingested: {ingest_result.get('rows_ingested', 0):,}")
            logger.info(f"  Errors: {ingest_result.get('errors', 0)}")
    
    # Save summary
    summary_file = output_dir / "recovery_summary.json"
    import json
    summary = {
        "recovered": recovered_files,
        "failed": failed_files,
        "total_files": len(corrupt_files),
        "recovered_count": len(recovered_files),
        "failed_count": len(failed_files)
    }
    summary_file.write_text(json.dumps(summary, indent=2))
    logger.info(f"\n💾 Summary saved to: {summary_file}")


if __name__ == "__main__":
    main()

