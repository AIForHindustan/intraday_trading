# safe_data_writer.py
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import json

class SafeDataWriter:
    """Safe data writer that prevents corruption and provides recovery"""
    
    def __init__(self, base_path: str, max_buffer_size: int = 1000):
        self.base_path = Path(base_path)
        self.max_buffer_size = max_buffer_size
        self.buffer = []
        self.logger = logging.getLogger(__name__)
        
        # Create directory structure
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Statistics
        self.stats = {
            'records_written': 0,
            'files_created': 0,
            'write_errors': 0
        }
    
    def add_data(self, data: List[Dict[str, Any]]):
        """Add data to buffer and write if buffer is full"""
        self.buffer.extend(data)
        
        if len(self.buffer) >= self.max_buffer_size:
            self.flush()
    
    def flush(self):
        """Write buffered data to disk safely"""
        if not self.buffer:
            return
        
        try:
            # Create filename with timestamp (preserve original naming convention)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Try to detect crawler type from path
            if "intraday" in str(self.base_path).lower():
                filename = f"intraday_{timestamp}.parquet"
            elif "data_mining" in str(self.base_path).lower():
                filename = f"data_mining_{timestamp}.parquet"
            elif "research" in str(self.base_path).lower():
                filename = f"research_{timestamp}.parquet"
            else:
                filename = f"ticks_{timestamp}.parquet"
            filepath = self.base_path / filename
            
            # Convert to DataFrame
            df = self._create_dataframe(self.buffer)
            
            # Write with corruption protection
            self._safe_write_parquet(df, filepath)
            
            # Verify write was successful
            if self._verify_file(filepath):
                self.stats['records_written'] += len(self.buffer)
                self.stats['files_created'] += 1
                self.buffer.clear()
                self.logger.info(f"Successfully wrote {len(df)} records to {filename}")
            else:
                raise Exception("File verification failed")
                
        except Exception as e:
            self.stats['write_errors'] += 1
            self.logger.error(f"Failed to write data: {e}")
            # Keep data in buffer for retry
    
    def _create_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert data to DataFrame with proper type handling"""
        # Normalize data structure
        normalized_data = []
        
        for record in data:
            try:
                normalized = self._normalize_record(record)
                if normalized:
                    normalized_data.append(normalized)
            except Exception as e:
                self.logger.warning(f"Failed to normalize record: {e}")
                continue
        
        return pd.DataFrame(normalized_data)
    
    def _normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize record structure and handle data types"""
        normalized = {}
        
        # Copy basic fields
        for key, value in record.items():
            if key == 'depth':
                # Handle depth data separately
                continue
            elif isinstance(value, (int, float, str, bool)) and value is not None:
                normalized[key] = value
            else:
                # Convert complex objects to JSON string
                normalized[f"{key}_json"] = json.dumps(value, default=str)
        
        # Add timestamp for indexing
        if 'exchange_timestamp' in record:
            normalized['timestamp'] = record['exchange_timestamp']
        else:
            normalized['timestamp'] = int(datetime.now().timestamp())
        
        return normalized
    
    def _safe_write_parquet(self, df: pd.DataFrame, filepath: Path):
        """Safely write Parquet file with corruption protection"""
        # Ensure directory exists (handle race conditions)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temporary file first
        temp_path = filepath.with_suffix('.parquet.tmp')
        
        try:
            # Write with compression and proper settings
            table = pa.Table.from_pandas(df)
            
            pq.write_table(
                table,
                temp_path,
                compression='snappy',
                version='2.6',
                data_page_version='2.0'
            )
            
            # Verify temp file was created before renaming
            if not temp_path.exists():
                raise FileNotFoundError(f"Temp file was not created: {temp_path}")
            
            # Atomic rename to final filename
            temp_path.rename(filepath)
            
            # Verify final file exists after rename
            if not filepath.exists():
                raise FileNotFoundError(f"Final file was not created after rename: {filepath}")
            
        except Exception as e:
            # Clean up temporary file on error
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass  # Ignore cleanup errors
            raise e
    
    def _verify_file(self, filepath: Path) -> bool:
        """Verify Parquet file is not corrupt"""
        try:
            # Try to read the file
            table = pq.read_table(filepath)
            return len(table) > 0
        except Exception as e:
            self.logger.error(f"File verification failed for {filepath}: {e}")
            # Move corrupt file to quarantine
            quarantine_path = filepath.with_suffix('.parquet.corrupt')
            filepath.rename(quarantine_path)
            return False
    
    def emergency_save(self):
        """Emergency save of buffer data in case of shutdown"""
        if self.buffer:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_EMERGENCY")
            filename = f"emergency_save_{timestamp}.json"
            filepath = self.base_path / filename
            
            try:
                with open(filepath, 'w') as f:
                    json.dump(self.buffer, f, default=str)
                self.logger.info(f"Emergency saved {len(self.buffer)} records")
            except Exception as e:
                self.logger.error(f"Emergency save failed: {e}")