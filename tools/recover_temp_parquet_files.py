#!/usr/bin/env python3
"""
Comprehensive Parquet File Recovery Tool
=========================================
Recovers data from corrupted parquet files in temp_parquet directory using multiple strategies:
1. PyArrow recovery (row-group level, column level)
2. FastParquet recovery (fallback)
3. Re-parse from original binary if available
4. Byte-level corruption handling
5. Snappy decompression error recovery
"""

import sys
from pathlib import Path
import logging
from typing import Dict, List, Optional, Any, Tuple
import traceback
from datetime import datetime
import json
import struct
import subprocess

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import fastparquet
    FASTPARQUET_AVAILABLE = True
except ImportError:
    FASTPARQUET_AVAILABLE = False

try:
    import snappy
    SNAPPY_AVAILABLE = True
except ImportError:
    SNAPPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
from token_cache import TokenCacheManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetRecoveryTool:
    """Comprehensive parquet file recovery with multiple fallback strategies."""
    
    def __init__(
        self,
        token_cache: Optional[TokenCacheManager] = None,
        output_dir: Optional[Path] = None,
        reparse_from_binary: bool = True
    ):
        self.token_cache = token_cache or TokenCacheManager(
            cache_path='core/data/token_lookup_enriched.json',
            verbose=False
        )
        self.output_dir = output_dir or Path("temp_parquet/recovered")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.reparse_from_binary = reparse_from_binary
        self.binary_converter = None
        if reparse_from_binary:
            self.binary_converter = ProductionZerodhaBinaryConverter(
                db_path=None,
                token_cache=self.token_cache,
                ensure_schema=False
            )
        
    def diagnose_file(self, file_path: Path) -> Dict[str, Any]:
        """Diagnose file to determine corruption type and recovery strategy."""
        diagnosis = {
            "file_path": str(file_path),
            "file_size": file_path.stat().st_size if file_path.exists() else 0,
            "exists": file_path.exists(),
            "can_read_metadata": False,
            "parquet_format": None,  # "pyarrow", "fastparquet", "unknown"
            "row_groups": 0,
            "columns": 0,
            "expected_rows": 0,
            "corruption_type": None,
            "recovery_strategy": None,
            "error": None,
            "has_par1_header": False,
            "has_par1_footer": False,
            "thrift_corruption": False
        }
        
        if not file_path.exists():
            diagnosis["error"] = "File does not exist"
            return diagnosis
        
        if file_path.stat().st_size == 0:
            diagnosis["error"] = "File is empty"
            return diagnosis
        
        # Check file structure
        try:
            with open(file_path, 'rb') as f:
                # Check PAR1 header (first 4 bytes should be "PAR1")
                header = f.read(4)
                diagnosis["has_par1_header"] = header == b'PAR1'
                
                # Check PAR1 footer (last 4 bytes should be "PAR1")
                f.seek(-4, 2)  # Seek to 4 bytes from end
                footer = f.read(4)
                diagnosis["has_par1_footer"] = footer == b'PAR1'
                
                # Try to read footer length (8 bytes from end: 4-byte length + 4-byte PAR1)
                f.seek(-8, 2)
                footer_bytes = f.read(8)
                if len(footer_bytes) == 8:
                    footer_length = int.from_bytes(footer_bytes[:4], byteorder='little')
                    logger.debug(f"  Footer length: {footer_length} bytes")
        except Exception as e:
            logger.debug(f"  Error checking file structure: {e}")
        
        # Try to read metadata with PyArrow
        if PYARROW_AVAILABLE:
            try:
                parquet_file = pq.ParquetFile(file_path)
                diagnosis["can_read_metadata"] = True
                diagnosis["parquet_format"] = "pyarrow"
                diagnosis["row_groups"] = parquet_file.num_row_groups
                diagnosis["columns"] = len(parquet_file.schema)
                diagnosis["expected_rows"] = parquet_file.metadata.num_rows
                return diagnosis
            except Exception as e:
                error_str = str(e).lower()
                # Check for Thrift protocol errors (common in corrupted parquet files)
                if "thrift" in error_str or "tprotocolexception" in error_str or "invalid data" in error_str:
                    diagnosis["corruption_type"] = "thrift_metadata_corruption"
                    diagnosis["thrift_corruption"] = True
                    logger.info(f"  ⚠️  Thrift metadata corruption detected - footer metadata is corrupted")
                elif "invalid" in error_str or "corrupt" in error_str:
                    diagnosis["corruption_type"] = "metadata_corruption"
                elif "snappy" in error_str or "decompress" in error_str:
                    diagnosis["corruption_type"] = "snappy_compression"
                elif "invalid number of bytes" in error_str:
                    diagnosis["corruption_type"] = "byte_corruption"
                else:
                    diagnosis["corruption_type"] = "unknown_corruption"
                diagnosis["error"] = str(e)
        
        # Try FastParquet as fallback
        if FASTPARQUET_AVAILABLE and not diagnosis["can_read_metadata"]:
            try:
                pf = fastparquet.ParquetFile(str(file_path))
                diagnosis["can_read_metadata"] = True
                diagnosis["parquet_format"] = "fastparquet"
                diagnosis["row_groups"] = len(pf.row_groups)
                diagnosis["columns"] = len(pf.columns)
                diagnosis["expected_rows"] = pf.count
                return diagnosis
            except Exception as e:
                error_str = str(e).lower()
                if "thrift" in error_str or "tprotocolexception" in error_str:
                    diagnosis["thrift_corruption"] = True
                if not diagnosis["error"]:
                    diagnosis["error"] = str(e)
        
        # Additional diagnostics for Thrift corruption
        if diagnosis["thrift_corruption"]:
            logger.info(f"  📋 File structure: Header={diagnosis['has_par1_header']}, Footer={diagnosis['has_par1_footer']}")
            logger.info(f"  💡 Strategy: Need to recover data without metadata or reconstruct metadata")
        
        return diagnosis
    
    def recover_with_pyarrow_row_groups(self, file_path: Path) -> Optional[Any]:
        """Recover data by reading row groups individually."""
        if not PYARROW_AVAILABLE or not PANDAS_AVAILABLE:
            return None
        
        try:
            parquet_file = pq.ParquetFile(file_path)
            row_group_dfs = []
            
            for rg_idx in range(parquet_file.num_row_groups):
                try:
                    rg_table = parquet_file.read_row_group(rg_idx)
                    rg_df = rg_table.to_pandas()
                    row_group_dfs.append(rg_df)
                    logger.debug(f"  Row group {rg_idx}: {len(rg_df)} rows")
                except Exception as rg_err:
                    logger.warning(f"  Row group {rg_idx} failed: {str(rg_err)[:80]}, skipping...")
                    continue
            
            if row_group_dfs:
                df = pd.concat(row_group_dfs, ignore_index=True)
                logger.info(f"  ✅ Recovered {len(df):,} rows from {len(row_group_dfs)}/{parquet_file.num_row_groups} row groups")
                return df
            
        except Exception as e:
            logger.debug(f"Row group recovery failed: {e}")
        
        return None
    
    def _repair_corrupted_footer(self, file_path: Path) -> Optional[Path]:
        """Attempt to repair corrupted footer by finding where readable data ends."""
        try:
            import struct
            data = bytearray(file_path.read_bytes())
            size = len(data)
            
            # Get current footer length
            footer_len = struct.unpack('<I', data[-8:-4])[0]
            footer_start = size - 8 - footer_len
            
            # Look for corruption pattern in last 500 bytes (repeating patterns indicate corruption)
            last_500 = bytes(data[-500:])
            
            # Check for repeating patterns: 1c 00 00 (common corruption pattern)
            corruption_offset = None
            for pattern_bytes in [b'\x1c\x00\x00', b'\x00\x00\x1c']:
                pattern_len = len(pattern_bytes)
                # Find all occurrences of pattern
                pos = 0
                occurrences = []
                while True:
                    pos = last_500.find(pattern_bytes, pos)
                    if pos == -1:
                        break
                    occurrences.append(pos)
                    pos += pattern_len
                
                # Check if occurrences are evenly spaced (indicating repeating pattern)
                if len(occurrences) >= 15:
                    # Check if they're consecutive repeats
                    if all(occurrences[i+1] - occurrences[i] == pattern_len for i in range(min(15, len(occurrences)-1))):
                        corruption_offset = 500 - occurrences[0]
                        repeats = len([o for o in occurrences if o >= occurrences[0] and o < occurrences[0] + pattern_len * 50])
                        logger.info(f"  Found repeating pattern {pattern_bytes.hex()} ({repeats} times) starting {corruption_offset} bytes from file end")
                        break
            
            if corruption_offset:
                # Calculate new footer length (excluding corruption)
                new_footer_len = footer_len - corruption_offset
                if 1000 < new_footer_len < footer_len:  # Sanity check
                    logger.info(f"  Repairing: adjusting footer length from {footer_len} to {new_footer_len}")
                    
                    # Truncate file to remove corruption, keeping last 8 bytes (length + PAR1)
                    new_size = size - corruption_offset
                    # Remove corrupted bytes but preserve footer structure
                    repaired_data = data[:new_size]
                    
                    # Update footer length field in the last 8 bytes
                    repaired_data[-8:-4] = struct.pack('<I', new_footer_len)
                    # Ensure PAR1 marker is present
                    repaired_data[-4:] = b'PAR1'
                    
                    # Write repaired file
                    repaired_path = file_path.parent / f"{file_path.stem}_REPAIRED.parquet"
                    repaired_path.write_bytes(repaired_data)
                    logger.info(f"  Created repaired file: {repaired_path.name} ({new_size} bytes, footer_len={new_footer_len})")
                    return repaired_path
        except Exception as e:
            logger.debug(f"  Footer repair failed: {e}")
        return None
    
    def recover_with_pyarrow_columns(self, file_path: Path) -> Optional[Any]:
        """Recover data by reading columns individually with error isolation."""
        if not PYARROW_AVAILABLE or not PANDAS_AVAILABLE:
            return None
        
        # First try to repair corrupted footer
        repaired_file = self._repair_corrupted_footer(file_path)
        file_to_read = repaired_file if repaired_file else file_path
        
        try:
            # Try to open file - this might fail if metadata is completely corrupted
            try:
                parquet_file = pq.ParquetFile(file_to_read)
                schema = parquet_file.schema
                column_names = [schema[i].name for i in range(len(schema))]
            except Exception as e:
                # If we can't even get schema from metadata, try extracting from file bytes
                logger.debug(f"  Cannot read schema from metadata, trying byte extraction: {e}")
                column_names = self._extract_schema_from_bytes(file_path)
                if not column_names:
                    return None
            
            recovered_data = {}
            failed_columns = []
            
            # Try reading with error isolation per column
            for col_name in column_names:
                try:
                    parquet_file = pq.ParquetFile(file_path)
                    col_table = parquet_file.read(columns=[col_name])
                    recovered_data[col_name] = col_table.column(0).to_pandas()
                except Exception as col_err:
                    failed_columns.append(col_name)
                    logger.debug(f"  Column '{col_name}' failed: {str(col_err)[:80]}")
                    # Don't create null column yet - wait to see what we recover
            
            if recovered_data:
                # Find minimum length (most reliable)
                lengths = [len(v) for v in recovered_data.values() if v is not None]
                if lengths:
                    min_len = min(lengths)
                    # Truncate all columns to minimum length
                    recovered_data_truncated = {
                        k: v.iloc[:min_len] if v is not None else pd.Series([None] * min_len)
                        for k, v in recovered_data.items()
                    }
                    # Fill missing columns with None
                    for col_name in column_names:
                        if col_name not in recovered_data_truncated:
                            recovered_data_truncated[col_name] = pd.Series([None] * min_len)
                    
                    df = pd.DataFrame(recovered_data_truncated)
                    logger.info(f"  ✅ Recovered {len(df):,} rows from {len(recovered_data)}/{len(column_names)} columns")
                    return df
            
        except Exception as e:
            logger.debug(f"Column recovery failed: {e}")
        
        return None
    
    def _extract_schema_from_bytes(self, file_path: Path) -> List[str]:
        """Extract column names from readable schema JSON in file bytes."""
        try:
            data = file_path.read_bytes()
            # Look for schema JSON
            json_start = data.rfind(b'{"index_columns"')
            if json_start > 0:
                import json
                json_str = data[json_start:]
                brace_count = 0
                json_end = 0
                for i, b in enumerate(json_str):
                    if b == ord('{'):
                        brace_count += 1
                    elif b == ord('}'):
                        brace_count -= 1
                        if brace_count == 0:
                            json_end = i + 1
                            break
                
                if json_end > 0:
                    schema_json = json_str[:json_end]
                    schema = json.loads(schema_json)
                    columns = schema.get('columns', [])
                    column_names = [col.get('name') for col in columns if col.get('name')]
                    if column_names:
                        logger.debug(f"  Extracted {len(column_names)} column names from embedded schema")
                        return column_names
        except Exception as e:
            logger.debug(f"  Schema extraction from bytes failed: {e}")
        return []
    
    def recover_with_fastparquet(self, file_path: Path) -> Optional[Any]:
        """Recover data using FastParquet library."""
        if not FASTPARQUET_AVAILABLE or not PANDAS_AVAILABLE:
            return None
        
        try:
            pf = fastparquet.ParquetFile(str(file_path))
            df = pf.to_pandas()
            logger.info(f"  ✅ Recovered {len(df):,} rows with FastParquet")
            return df
        except Exception as e:
            logger.debug(f"FastParquet recovery failed: {e}")
            return None
    
    def find_original_binary_file(self, parquet_path: Path) -> Optional[Path]:
        """Try to find the original binary file that created this parquet.
        
        Based on binary_to_parquet_parquet.py, parquet files are named as:
        output_file = output_dir / f"{input_file.stem}.parquet"
        
        So the binary file should have the SAME stem, just with .dat or .bin extension.
        """
        # The parquet file stem IS the binary file stem (same name, different extension)
        stem = parquet_path.stem
        
        # Binary files use different naming pattern:
        # Parquet: data_mining_20251031_113048.parquet
        # Binary: binary_data_20251031_113048.dat
        
        # Extract date and time from parquet stem
        # Format: {crawler}_YYYYMMDD_HHMMSS
        import re
        # Match date (8 digits) and time (6 digits) pattern
        full_match = re.search(r'(\d{8})_(\d{6})', stem)
        if full_match:
            date = full_match.group(1)
            time = full_match.group(2)
        else:
            # Fallback: separate searches
            date_match = re.search(r'(\d{8})', stem)
            time_match = re.search(r'_(\d{6})(?:\.|$)', stem)  # Match 6 digits after underscore
            date = date_match.group(1) if date_match else None
            time = time_match.group(1) if time_match else None
        
        project_root = Path(__file__).parent.parent
        
        # Also search in external volume copies
        external_dirs = [
            project_root / "temp_parquet" / "windowsdata_source",
            project_root / "temp_parquet" / "macdata_source",
        ]
        
        # First check external volume copies explicitly
        for ext_dir in external_dirs:
            if ext_dir.exists() and date and time:
                # Pattern search in external dirs
                for pattern_name, pattern in [
                    ("binary_data", f"binary_data_{date}_{time}.*\\.(dat|bin)$"),
                    ("date+time", f".*{date}_{time}.*\\.(dat|bin)$"),
                    ("date_only", f".*{date}.*\\.(dat|bin)$") if date else None,
                ]:
                    if pattern:
                        try:
                            result = subprocess.run(
                                ['fd', '--type', 'f', pattern, str(ext_dir)],
                                capture_output=True,
                                text=True,
                                timeout=5
                            )
                            if result.returncode == 0 and result.stdout.strip():
                                found_path = Path(result.stdout.strip().split('\n')[0])
                                logger.info(f"  Found binary source (external {pattern_name}): {found_path}")
                                return found_path
                        except Exception:
                            pass
        
        # Try pattern: binary_data_{date}_{time}.dat
        if date and time:
            
            # Pattern 1: binary_data_{date}_{time}.dat
            binary_pattern = f"binary_data_{date}_{time}.*\\.(dat|bin)$"
            try:
                result = subprocess.run(
                    ['fd', '--max-depth', '25', '--type', 'f', binary_pattern, str(project_root)],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0 and result.stdout.strip():
                    found_path = Path(result.stdout.strip().split('\n')[0])
                    logger.info(f"  Found binary source (binary_data pattern): {found_path}")
                    return found_path
            except Exception as e:
                logger.debug(f"  fd binary_data search failed: {e}")
            
            # Pattern 2: {crawler}_data_{date}_{time}.dat (e.g., intraday_data_20251029_143155.dat)
            if 'intraday' in stem.lower():
                intraday_pattern = f"intraday_data_{date}_{time}.*\\.(dat|bin)$"
                try:
                    result = subprocess.run(
                        ['fd', '--max-depth', '25', '--type', 'f', intraday_pattern, str(project_root)],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        found_path = Path(result.stdout.strip().split('\n')[0])
                        logger.info(f"  Found binary source (intraday_data pattern): {found_path}")
                        return found_path
                except Exception as e:
                    logger.debug(f"  fd intraday_data search failed: {e}")
        
        # Try exact stem match with fd
        try:
            result = subprocess.run(
                ['fd', '--max-depth', '25', '--type', 'f', f'^{stem}\\.(dat|bin)$', str(project_root)],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0 and result.stdout.strip():
                found_path = Path(result.stdout.strip().split('\n')[0])
                logger.info(f"  Found binary source (fd exact match): {found_path}")
                return found_path
        except Exception as e:
            logger.debug(f"  fd exact search failed: {e}")
        
        # Try date + time pattern search
        if date and time:
            pattern = f".*{date}_{time}.*\\.(dat|bin)$"
            try:
                result = subprocess.run(
                    ['fd', '--max-depth', '25', '--type', 'f', pattern, str(project_root)],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0 and result.stdout.strip():
                    matches = [Path(p) for p in result.stdout.strip().split('\n') if p.strip()]
                    if matches:
                        logger.info(f"  Found binary source (date+time pattern): {matches[0]}")
                        return matches[0]
            except Exception as e:
                logger.debug(f"  fd date+time search failed: {e}")
        
        # Fallback: search by date only
        if date:
            pattern = f".*{date}.*\\.(dat|bin)$"
            try:
                result = subprocess.run(
                    ['fd', '--max-depth', '25', '--type', 'f', pattern, str(project_root)],
                    capture_output=True,
                    text=True,
                    timeout=15
                )
                if result.returncode == 0 and result.stdout.strip():
                    matches = [Path(p) for p in result.stdout.strip().split('\n') if p.strip()]
                    # Prefer files with time pattern
                    if time:
                        time_matches = [f for f in matches if time in f.stem]
                        if time_matches:
                            logger.info(f"  Found binary source (date pattern, time matched): {time_matches[0]}")
                            return time_matches[0]
                    if matches:
                        logger.info(f"  Found binary source (date pattern): {matches[0]}")
                        return matches[0]
            except Exception as e:
                logger.debug(f"  fd date-only search failed: {e}")
        
        # Last resort: search all .dat/.bin files with find (more comprehensive)
        try:
            # Use find for even deeper search
            find_cmd = ['find', str(project_root), '-maxdepth', '25', '-type', 'f']
            if date:
                find_cmd.extend(['-name', f'*{date}*.dat', '-o', '-name', f'*{date}*.bin'])
            else:
                find_cmd.extend(['-name', '*.dat', '-o', '-name', '*.bin'])
            
            result = subprocess.run(
                find_cmd,
                capture_output=True,
                text=True,
                timeout=20
            )
            if result.returncode == 0 and result.stdout.strip():
                matches = [Path(p.strip()) for p in result.stdout.strip().split('\n') if p.strip() and project_root in Path(p.strip()).parents]
                if matches:
                    # Filter out .venv, __pycache__, node_modules
                    matches = [m for m in matches if '.venv' not in str(m) and '__pycache__' not in str(m) and 'node_modules' not in str(m)]
                    if matches:
                        logger.info(f"  Found binary source (find fallback): {matches[0]}")
                        return matches[0]
        except Exception as e:
            logger.debug(f"  find fallback search failed: {e}")
        
        logger.debug(f"  Could not find binary file for {stem}")
        if date and time:
            logger.debug(f"    Searched patterns: binary_data_{date}_{time}, {stem}, *{date}_{time}*, *{date}*")
        else:
            logger.debug(f"    Searched patterns: {stem}, *{date}*")
        
        return None
    
    def reparse_from_binary(self, binary_path: Path, original_parquet_path: Path) -> Optional[Any]:
        """Re-parse the original binary file to recreate parquet data."""
        if not self.binary_converter or not PANDAS_AVAILABLE:
            return None
        
        try:
            logger.info(f"  Attempting to re-parse from binary: {binary_path.name}")
            
            # Parse binary file
            raw_data = binary_path.read_bytes()
            # Use the packet detection method
            packets = self.binary_converter._detect_packet_format(raw_data)
            
            if not packets:
                logger.warning(f"  No packets extracted from binary file")
                return None
            
            logger.info(f"  Parsed {len(packets):,} packets from binary")
            
            # Convert packets to list of records for DataFrame
            from binary_to_parquet.production_binary_converter import COLUMN_ORDER
            all_records = []
            
            for packet in packets:
                # Extract key fields from packet
                record = {}
                # Extract all available fields from packet
                if 'instrument_token' in packet:
                    record['instrument_token'] = packet.get('instrument_token')
                if 'last_price' in packet:
                    record['last_price'] = packet.get('last_price')
                if 'volume' in packet:
                    record['volume'] = packet.get('volume')
                if 'exchange_timestamp' in packet:
                    record['exchange_timestamp'] = packet.get('exchange_timestamp')
                if 'last_traded_timestamp' in packet:
                    record['last_traded_timestamp'] = packet.get('last_traded_timestamp')
                # Add market depth if present
                if 'market_depth' in packet:
                    depth = packet.get('market_depth', [])
                    for i, entry in enumerate(depth[:5]):  # bid side
                        record[f'bid_{i+1}_price'] = entry.get('price')
                        record[f'bid_{i+1}_quantity'] = entry.get('quantity')
                    for i, entry in enumerate(depth[5:10]):  # ask side
                        record[f'ask_{i+1}_price'] = entry.get('price')
                        record[f'ask_{i+1}_quantity'] = entry.get('quantity')
                
                if record:  # Only add non-empty records
                    all_records.append(record)
            
            if all_records:
                df = pd.DataFrame(all_records)
                logger.info(f"  ✅ Re-created {len(df):,} rows from binary")
                return df
            
        except Exception as e:
            logger.warning(f"  Binary re-parse failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return None
    
    def recover_file(self, file_path: Path) -> Dict[str, Any]:
        """Attempt to recover data from a corrupted parquet file using multiple strategies."""
        result = {
            "file_path": str(file_path),
            "success": False,
            "strategy_used": None,
            "rows_recovered": 0,
            "original_rows": 0,
            "recovered_file": None,
            "error": None,
            "diagnosis": None
        }
        
        logger.info(f"\n🔍 Diagnosing: {file_path.name}")
        
        # Step 1: Diagnose
        diagnosis = self.diagnose_file(file_path)
        result["diagnosis"] = diagnosis
        result["original_rows"] = diagnosis.get("expected_rows", 0)
        
        if not diagnosis["exists"]:
            result["error"] = "File does not exist"
            return result
        
        if diagnosis["file_size"] == 0:
            result["error"] = "File is empty"
            return result
        
        # Step 2: Try recovery strategies in order
        df = None
        
        # Strategy 1: Standard read (might work for partially corrupted files)
        if PANDAS_AVAILABLE:
            try:
                df = pd.read_parquet(file_path)
                result["success"] = True
                result["strategy_used"] = "standard_read"
                result["rows_recovered"] = len(df)
                logger.info(f"  ✅ Standard read successful: {len(df):,} rows")
            except Exception:
                pass
        
        # Strategy 2: PyArrow row-group recovery
        if df is None and PYARROW_AVAILABLE:
            logger.info("  Trying PyArrow row-group recovery...")
            df = self.recover_with_pyarrow_row_groups(file_path)
            if df is not None and len(df) > 0:
                result["success"] = True
                result["strategy_used"] = "pyarrow_row_groups"
                result["rows_recovered"] = len(df)
        
        # Strategy 3: PyArrow column recovery
        if df is None and PYARROW_AVAILABLE:
            logger.info("  Trying PyArrow column recovery...")
            df = self.recover_with_pyarrow_columns(file_path)
            if df is not None and len(df) > 0:
                result["success"] = True
                result["strategy_used"] = "pyarrow_columns"
                result["rows_recovered"] = len(df)
        
        # Strategy 4: FastParquet
        if df is None and FASTPARQUET_AVAILABLE:
            logger.info("  Trying FastParquet recovery...")
            df = self.recover_with_fastparquet(file_path)
            if df is not None and len(df) > 0:
                result["success"] = True
                result["strategy_used"] = "fastparquet"
                result["rows_recovered"] = len(df)
        
        # Strategy 5: Re-parse from original binary
        if df is None and self.reparse_from_binary:
            logger.info("  Trying to find and re-parse from original binary...")
            binary_path = self.find_original_binary_file(file_path)
            if binary_path and binary_path.exists():
                df = self.reparse_from_binary(binary_path, file_path)
                if df is not None and len(df) > 0:
                    result["success"] = True
                    result["strategy_used"] = "binary_reparse"
                    result["rows_recovered"] = len(df)
            else:
                logger.info("  Original binary file not found")
        
        # Step 3: Save recovered data
        if df is not None and len(df) > 0:
            try:
                recovered_file = self.output_dir / f"{file_path.stem}_RECOVERED.parquet"
                
                # Ensure required columns exist before saving
                if 'instrument_token' not in df.columns:
                    logger.warning("  Recovered data missing instrument_token column")
                    result["error"] = "Recovered data missing required columns"
                else:
                    df.to_parquet(recovered_file, compression='snappy', index=False)
                    result["recovered_file"] = str(recovered_file)
                    logger.info(f"  ✅ Saved recovered file: {recovered_file.name}")
            except Exception as e:
                logger.error(f"  Failed to save recovered file: {e}")
                result["error"] = f"Save failed: {e}"
        else:
            result["error"] = "All recovery strategies failed"
            logger.warning(f"  ❌ Could not recover any data from {file_path.name}")
        
        return result


def batch_recover_files(
    input_dir: Path,
    output_dir: Optional[Path] = None,
    max_files: Optional[int] = None,
    dry_run: bool = False
) -> Dict[str, Any]:
    """Batch recover all parquet files in a directory."""
    logger.info(f"\n{'='*80}")
    logger.info(f"BATCH PARQUET RECOVERY")
    logger.info(f"Input: {input_dir}")
    logger.info(f"{'='*80}\n")
    
    # Find all parquet files
    parquet_files = sorted(input_dir.glob("*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found in {input_dir}")
        return {"success": False, "error": "No files found"}
    
    if max_files:
        parquet_files = parquet_files[:max_files]
    
    logger.info(f"Found {len(parquet_files)} parquet files to recover")
    
    if dry_run:
        logger.info("DRY RUN - Files that would be processed:")
        for f in parquet_files[:10]:
            logger.info(f"  {f.name}")
        if len(parquet_files) > 10:
            logger.info(f"  ... and {len(parquet_files) - 10} more")
        return {"success": True, "dry_run": True, "files_found": len(parquet_files)}
    
    # Initialize recovery tool
    recovery_tool = ParquetRecoveryTool(output_dir=output_dir)
    
    # Process files
    results = []
    successful = 0
    failed = 0
    
    for i, file_path in enumerate(parquet_files, 1):
        logger.info(f"\n[{i}/{len(parquet_files)}] Processing: {file_path.name}")
        try:
            result = recovery_tool.recover_file(file_path)
            results.append(result)
            
            if result["success"]:
                successful += 1
                logger.info(f"  ✅ SUCCESS ({result['strategy_used']}): {result['rows_recovered']:,} rows recovered")
            else:
                failed += 1
                logger.warning(f"  ❌ FAILED: {result.get('error', 'Unknown error')}")
        
        except Exception as e:
            failed += 1
            logger.error(f"  ❌ EXCEPTION: {e}", exc_info=True)
            results.append({
                "file_path": str(file_path),
                "success": False,
                "error": str(e)
            })
    
    # Summary
    logger.info(f"\n{'='*80}")
    logger.info(f"RECOVERY SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Total files: {len(parquet_files)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Success rate: {successful/len(parquet_files)*100:.1f}%")
    
    total_rows_recovered = sum(r.get("rows_recovered", 0) for r in results if r.get("success"))
    logger.info(f"Total rows recovered: {total_rows_recovered:,}")
    
    # Strategy breakdown
    strategies = {}
    for r in results:
        if r.get("success"):
            strat = r.get("strategy_used", "unknown")
            strategies[strat] = strategies.get(strat, 0) + 1
    
    if strategies:
        logger.info(f"\nStrategy breakdown:")
        for strat, count in strategies.items():
            logger.info(f"  {strat}: {count} files")
    
    return {
        "success": True,
        "total_files": len(parquet_files),
        "successful": successful,
        "failed": failed,
        "total_rows_recovered": total_rows_recovered,
        "results": results,
        "strategies": strategies
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Recover corrupted parquet files from temp_parquet directory"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="temp_parquet",
        help="Input directory with corrupted parquet files"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="temp_parquet/recovered",
        help="Output directory for recovered files"
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum number of files to process (for testing)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually processing"
    )
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else None
    
    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    result = batch_recover_files(
        input_dir=input_dir,
        output_dir=output_dir,
        max_files=args.max_files,
        dry_run=args.dry_run
    )
    
    if not result.get("success"):
        sys.exit(1)

