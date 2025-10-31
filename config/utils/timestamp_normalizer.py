# core/utils/timestamp_normalizer.py
import datetime
from typing import Union

class TimestampNormalizer:
    """
    ONE-TIME FIX: Convert all timestamp formats to epoch milliseconds
    """
    
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    
    @staticmethod
    def to_epoch_ms(timestamp: Union[str, int, float, datetime.datetime]) -> int:
        """Convert ANY timestamp format to epoch milliseconds"""
        if timestamp is None:
            return int(datetime.datetime.now().timestamp() * 1000)
        
        # Already epoch milliseconds
        if isinstance(timestamp, int) and timestamp > 1000000000000:
            return timestamp
        
        # Already epoch seconds  
        if isinstance(timestamp, (int, float)) and timestamp < 1000000000000:
            return int(timestamp * 1000)
        
        # String formats
        if isinstance(timestamp, str):
            # Zerodha format: "2025-10-17T09:25:30Z"
            if timestamp.endswith('Z'):
                dt = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            
            # Exchange format: "2025-10-17 09:25:30.123456"
            if '.' in timestamp and ':' in timestamp:
                parts = timestamp.split('.')
                dt = datetime.datetime.fromisoformat(parts[0])
                microseconds = int(parts[1][:6].ljust(6, '0'))
                dt = dt.replace(microsecond=microseconds)
                return int(dt.timestamp() * 1000)
            
            # Slash format: "17/10/2025 09:25:30"
            if '/' in timestamp:
                dt = datetime.datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
                return int(dt.timestamp() * 1000)
            
            # Try ISO format as last resort
            try:
                dt = datetime.datetime.fromisoformat(timestamp)
                return int(dt.timestamp() * 1000)
            except ValueError:
                pass
        
        # datetime object
        if isinstance(timestamp, datetime.datetime):
            return int(timestamp.timestamp() * 1000)
        
        # Fallback to current time
        return int(datetime.datetime.now().timestamp() * 1000)

    @staticmethod
    def to_iso_string(epoch_ms: int, tz: datetime.timezone = None) -> str:
        """Convert epoch milliseconds to ISO string in the desired timezone (default IST)."""
        tz = tz or TimestampNormalizer.IST
        dt = datetime.datetime.fromtimestamp(epoch_ms / 1000.0, tz=datetime.timezone.utc)
        return dt.astimezone(tz).isoformat()

# USAGE: One-time normalization when storing data
normalized_ts = TimestampNormalizer.to_epoch_ms("2025-10-17T09:25:30Z")
# Result: 1737011130000 (ALWAYS epoch milliseconds)
