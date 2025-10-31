#!/usr/bin/env python3
"""
Metadata Resolver for Crawlers
=============================

Purpose:
--------
Centralized metadata resolution for all crawlers that write to disk.
Provides instrument token to symbol/metadata mapping using the master token_lookup.json.

Dependent Scripts:
-----------------
- run_data_mining_crawler.py: Uses metadata_resolver for data mining crawler
- run_intraday_crawler.py: Uses metadata_resolver for intraday crawler  
- run_research_crawler.py: Uses metadata_resolver for research crawler
- data_mining_crawler.py: Replaces InstrumentMapper with metadata_resolver
- intraday_crawler.py: Replaces InstrumentMapper with metadata_resolver
- research_crawler.py: Replaces InstrumentMapper with metadata_resolver

Important Aspects:
-----------------
- Single source of truth for instrument metadata across all crawlers
- Uses core/data/token_lookup.json (58MB, 231K+ instruments)
- Provides fallback for unknown tokens
- Thread-safe singleton pattern for performance
- Caches metadata in memory for fast lookups
- Integrates with existing crawler architecture
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from threading import Lock

logger = logging.getLogger(__name__)


class MetadataResolver:
    """
    Centralized metadata resolver for all crawlers.
    
    Provides instrument token to symbol/metadata mapping using the master
    token_lookup.json file from core/data directory.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        """Singleton pattern for thread-safe access"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'token_cache'):
            self.token_cache = {}
            self._load_token_mapping()
    
    def _load_token_mapping(self):
        """Load token mapping from core/data/token_lookup.json"""
        try:
            # Point to the master token_lookup.json in core/data
            project_root = Path(__file__).parent.parent
            lookup_file = project_root / 'core' / 'data' / 'token_lookup.json'
            
            if not lookup_file.exists():
                logger.error(f"❌ Token lookup file not found: {lookup_file}")
                return
            
            logger.info(f"📋 Loading token metadata from: {lookup_file}")
            
            with open(lookup_file, 'r') as f:
                token_data = json.load(f)
            
            for token_str, info in token_data.items():
                try:
                    token = int(token_str)
                    self.token_cache[token] = {
                        'symbol': info.get('name', ''),
                        'exchange': info.get('exchange', 'NSE'),
                        'instrument_type': info.get('instrument_type', 'EQ'),
                        'segment': info.get('segment', ''),
                        'tradingsymbol': info.get('name', ''),
                        'key': info.get('key', ''),
                        'source': info.get('source', 'Zerodha_API')
                    }
                except (ValueError, TypeError):
                    # Skip non-integer keys (instrument identifiers like MCX:MCXBULLDEX26JAN31800CE)
                    continue
            
            logger.info(f"✅ Loaded metadata for {len(self.token_cache)} instruments")
            
        except Exception as e:
            logger.error(f"❌ Error loading token mapping: {e}")
            raise
    
    def get_metadata(self, instrument_token: int) -> Dict[str, Any]:
        """
        Get metadata for instrument token
        
        Args:
            instrument_token: Zerodha instrument token
            
        Returns:
            Dict containing symbol, exchange, instrument_type, segment, tradingsymbol
        """
        metadata = self.token_cache.get(instrument_token, {
            'symbol': f'UNKNOWN_{instrument_token}',
            'exchange': 'UNKNOWN',
            'instrument_type': 'UNKNOWN',
            'segment': 'UNKNOWN',
            'tradingsymbol': f'UNKNOWN_{instrument_token}',
            'key': f'UNKNOWN_{instrument_token}',
            'source': 'UNKNOWN'
        })
        
        return metadata
    
    def token_to_symbol(self, instrument_token: int) -> str:
        """
        Convert instrument token to symbol
        
        Args:
            instrument_token: Zerodha instrument token
            
        Returns:
            Symbol string
        """
        metadata = self.get_metadata(instrument_token)
        return metadata.get('symbol', f'UNKNOWN_{instrument_token}')
    
    def get_token_metadata(self, instrument_token: int) -> Dict[str, Any]:
        """
        Get complete token metadata (alias for get_metadata)
        
        Args:
            instrument_token: Zerodha instrument token
            
        Returns:
            Complete metadata dictionary
        """
        return self.get_metadata(instrument_token)
    
    def get_instrument_info(self, instrument_token: int) -> Dict[str, Any]:
        """
        Get formatted instrument info for crawlers
        
        Args:
            instrument_token: Zerodha instrument token
            
        Returns:
            Formatted instrument info dictionary
        """
        metadata = self.get_metadata(instrument_token)
        symbol = metadata.get('symbol', f'UNKNOWN_{instrument_token}')
        tradingsymbol = metadata.get('tradingsymbol', symbol)
        
        return {
            "token": instrument_token,
            "symbol": symbol,
            "tradingsymbol": tradingsymbol,
            "name": metadata.get('name', tradingsymbol),
            "exchange": metadata.get('exchange', 'NSE'),
            "segment": metadata.get('segment') or metadata.get('instrument_type', 'EQ'),
            "asset_class": metadata.get('instrument_type', 'unknown'),
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get resolver statistics"""
        return {
            'total_instruments': len(self.token_cache),
            'exchanges': list(set(info.get('exchange', 'UNKNOWN') for info in self.token_cache.values())),
            'instrument_types': list(set(info.get('instrument_type', 'UNKNOWN') for info in self.token_cache.values()))
        }


# Global instance for backward compatibility
metadata_resolver = MetadataResolver()