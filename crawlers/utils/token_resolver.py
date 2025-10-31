"""
Token Resolver for Binary Data Processing
Converts instrument tokens to proper symbols for downstream processing
"""

import json
from pathlib import Path
from typing import Dict, Optional

# Global cache for token mapping
_TOKEN_MAPPING_CACHE: Optional[Dict[int, Dict]] = None


def resolve_token_to_symbol(instrument_token: int) -> Optional[str]:
    """
    Resolve instrument token to trading symbol
    
    Args:
        instrument_token: Zerodha instrument token
        
    Returns:
        Trading symbol in format 'EXCHANGE:SYMBOL' or None if not found
    """
    try:
        token_mapping = _load_token_mapping()
        
        if instrument_token in token_mapping:
            instrument_info = token_mapping[instrument_token]
            
            exchange = instrument_info.get('exchange', 'NSE')
            symbol = instrument_info.get('tradingsymbol', '')
            
            if symbol:
                return f"{exchange}:{symbol}"
        
        return None
        
    except Exception as e:
        print(f"Error resolving token {instrument_token}: {e}")
        return None


def _load_token_mapping() -> Dict[int, Dict]:
    """
    Load token mapping from token_lookup.json with caching
    
    Returns:
        Dictionary mapping token -> instrument info
    """
    global _TOKEN_MAPPING_CACHE
    
    if _TOKEN_MAPPING_CACHE is not None:
        return _TOKEN_MAPPING_CACHE
    
    try:
        # Load from token_lookup_enriched.json in core/data (project root relative)
        project_root = Path(__file__).parent.parent.parent
        lookup_file = project_root / 'core' / 'data' / 'token_lookup_enriched.json'
        
        if not lookup_file.exists():
            print(f"❌ Token lookup file not found: {lookup_file}")
            return {}
        
        print(f"⚡ Loading token lookup from {lookup_file} (one-time load)...")
        
        with open(lookup_file, 'r') as f:
            token_lookup = json.load(f)
        
        token_mapping = {}
        
        # Convert string keys to int and restructure data
        for token_str, info in token_lookup.items():
            try:
                token = int(token_str)
                
                # Extract key fields for downstream processing
                # Use 'key' field for tradingsymbol as it contains full symbol with expiry
                key_field = info.get('key', '')
                exchange_from_key = key_field.split(':')[0] if ':' in key_field else info.get('exchange', '')
                
                token_mapping[token] = {
                    'tradingsymbol': key_field.split(':')[1] if ':' in key_field else info.get('name', ''),
                    'exchange': exchange_from_key,
                    'name': info.get('name', ''),
                    'instrument_type': info.get('instrument_type', ''),
                    'segment': key_field,
                    'symbol': info.get('name', ''),
                    'source': info.get('source', 'UNKNOWN')
                }
                
            except (ValueError, KeyError):
                continue
        
        _TOKEN_MAPPING_CACHE = token_mapping
        
        print(f"⚡ Token mapping cached: {len(token_mapping)} instruments")
        return token_mapping
        
    except Exception as e:
        print(f"Error loading token mapping: {e}")
        return {}


if __name__ == "__main__":
    # Test with some common tokens
    test_tokens = [408065, 2953217, 13413890]
    
    for token in test_tokens:
        symbol = resolve_token_to_symbol(token)
        print(f"Token {token} -> {symbol}")
