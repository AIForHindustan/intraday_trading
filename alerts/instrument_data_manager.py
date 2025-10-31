"""
Professional Instrument Data Manager
Handles all instrument data storage, retrieval, and real-time updates
"""

import json
import asyncio
import redis.asyncio as aioredis
import orjson
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class InstrumentDataManager:
    """
    Centralized instrument data management using Redis + RedisJSON
    Provides real-time instrument data for the trading dashboard
    """
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis = aioredis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.redis_json = aioredis.Redis(host=redis_host, port=redis_port, decode_responses=False)
        
        # Redis keys for different data types
        self.keys = {
            'instruments': 'instruments:master',
            'equity_cash': 'instruments:equity_cash',
            'equity_futures': 'instruments:equity_futures', 
            'index_futures': 'instruments:index_futures',
            'index_options': 'instruments:index_options',
            'metadata': 'instruments:metadata',
            'last_updated': 'instruments:last_updated'
        }
    
    async def initialize_data(self):
        """Initialize instrument data from existing sources"""
        try:
            # Load from existing token_lookup_enriched.json
            await self._load_from_enriched_json()
            
            # Create Redis Search indexes for fast queries
            await self._create_search_indexes()
            
            logger.info("Instrument data initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize instrument data: {e}")
            raise
    
    async def _load_from_enriched_json(self):
        """Load data from existing token_lookup_enriched.json"""
        try:
            with open('core/data/token_lookup_enriched.json', 'r') as f:
                data = json.load(f)
            
            # Categorize instruments by asset class
            categorized = self._categorize_instruments(data)
            
            # Store in Redis with different keys for fast access
            for asset_class, instruments in categorized.items():
                key = self.keys.get(asset_class)
                if key:
                    await self.redis_json.json().set(key, '$', instruments)
            
            # Store master data
            await self.redis_json.json().set(self.keys['instruments'], '$', data)
            
            # Update metadata
            metadata = {
                'total_instruments': len(data),
                'last_updated': datetime.now().isoformat(),
                'source': 'token_lookup_enriched.json',
                'categories': {k: len(v) for k, v in categorized.items()}
            }
            await self.redis_json.json().set(self.keys['metadata'], '$', metadata)
            
        except Exception as e:
            logger.error(f"Failed to load from enriched JSON: {e}")
            raise
    
    def _categorize_instruments(self, data: Dict) -> Dict[str, List[Dict]]:
        """Categorize instruments by asset class for fast retrieval"""
        categorized = {
            'equity_cash': [],
            'equity_futures': [],
            'index_futures': [],
            'index_options': []
        }
        
        for token, instrument in data.items():
            try:
                # Determine asset class based on instrument properties
                asset_class = self._determine_asset_class(instrument)
                
                if asset_class in categorized:
                    instrument_data = {
                        'token': token,
                        'symbol': instrument.get('key', ''),
                        'name': instrument.get('name', ''),
                        'exchange': instrument.get('exchange', ''),
                        'expiry': instrument.get('expiry', ''),
                        'strike_price': instrument.get('strike_price'),
                        'option_type': instrument.get('option_type', ''),
                        'sector': instrument.get('sector', ''),
                        'lot_size': instrument.get('lot_size'),
                        'tick_size': instrument.get('tick_size')
                    }
                    categorized[asset_class].append(instrument_data)
                    
            except Exception as e:
                logger.warning(f"Failed to categorize instrument {token}: {e}")
                continue
        
        return categorized
    
    def _determine_asset_class(self, instrument: Dict) -> str:
        """Determine asset class from instrument data"""
        exchange = instrument.get('exchange', '').upper()
        instrument_type = instrument.get('instrument_type', '').upper()
        
        if exchange == 'NSE' and instrument_type == 'EQ':
            return 'equity_cash'
        elif exchange in ['NFO', 'BFO'] and instrument_type == 'FUT':
            if 'NIFTY' in instrument.get('name', '') or 'BANKNIFTY' in instrument.get('name', ''):
                return 'index_futures'
            else:
                return 'equity_futures'
        elif exchange in ['NFO', 'BFO'] and instrument_type == 'OPT':
            return 'index_options'
        
        return 'equity_cash'  # Default fallback
    
    async def _create_search_indexes(self):
        """Create Redis Search indexes for fast queries"""
        try:
            # Create index for instruments search
            await self.redis.ft('instruments_idx').create_index(
                fields=[
                    aioredis.commands.search.TagField("$.symbol", as_name="symbol"),
                    aioredis.commands.search.TagField("$.exchange", as_name="exchange"),
                    aioredis.commands.search.TagField("$.sector", as_name="sector"),
                    aioredis.commands.search.NumericField("$.strike_price", as_name="strike_price"),
                ],
                definition=aioredis.commands.search.IndexDefinition(
                    prefix=["instruments:"],
                    index_type=["JSON"]
                )
            )
        except Exception as e:
            logger.warning(f"Search index creation failed (may already exist): {e}")
    
    async def get_instruments_by_asset_class(self, asset_class: str) -> List[Dict]:
        """Get instruments by asset class with caching"""
        try:
            key = self.keys.get(asset_class)
            if not key:
                return []
            
            # Try to get from Redis first
            cached_data = await self.redis_json.json().get(key)
            if cached_data:
                return cached_data
            
            # Fallback to master data
            master_data = await self.redis_json.json().get(self.keys['instruments'])
            if master_data:
                categorized = self._categorize_instruments(master_data)
                return categorized.get(asset_class, [])
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get instruments for {asset_class}: {e}")
            return []
    
    async def search_instruments(self, query: str, asset_class: Optional[str] = None) -> List[Dict]:
        """Search instruments using Redis Search"""
        try:
            search_query = f"@symbol:*{query}*"
            if asset_class:
                search_query += f" @exchange:{asset_class.upper()}"
            
            results = await self.redis.ft('instruments_idx').search(
                search_query,
                limit=50
            )
            
            return [doc.json for doc in results.docs]
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    async def get_instrument_metadata(self) -> Dict:
        """Get instrument metadata and statistics"""
        try:
            metadata = await self.redis_json.json().get(self.keys['metadata'])
            if metadata:
                return metadata
            return {}
        except Exception as e:
            logger.error(f"Failed to get metadata: {e}")
            return {}
    
    async def refresh_data(self):
        """Refresh instrument data from external sources"""
        try:
            # This would integrate with your existing update scripts
            # For now, just reload from existing sources
            await self._load_from_enriched_json()
            logger.info("Instrument data refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh data: {e}")
            raise

# Global instance
instrument_manager = InstrumentDataManager()
