"""
Simple Instrument Data Manager using regular Redis
Fallback for systems without RedisJSON module
"""

import json
import asyncio
import time
import redis.asyncio as aioredis
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SimpleInstrumentManager:
    """
    Simple instrument data management using regular Redis
    Stores data as JSON strings in Redis keys
    """
    
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.redis = aioredis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Redis keys for different data types
        self.keys = {
            'instruments': 'instruments:master',
            'equity_cash': 'instruments:equity_cash',
            'equity_futures': 'instruments:equity_futures', 
            'index_futures': 'instruments:index_futures',
            'index_options': 'instruments:index_options',
            'metadata': 'instruments:metadata',
        }

        # Ultra-fast in-memory caches (TTL-based)
        self._memory_cache: Dict[str, Tuple[float, List[Dict]]] = {}
        self._memory_cache_ttl_seconds: int = 300  # 5 minutes per asset-class bucket
        self._master_cache: Optional[Dict] = None
        self._master_cache_loaded_at: float = 0.0
        self._master_cache_ttl_seconds: int = 900  # 15 minutes
    
    async def initialize_data(self):
        """Initialize instrument data from preferred sources (binary crawler → Redis → enriched json)."""
        try:
            # 1) Prefer Binary Crawler JSON or Redis snapshot if available
            loaded = await self._load_from_binary_crawler_sources()
            if not loaded:
                # 2) Fallback to enriched lookup
                await self._load_from_enriched_json()
            # Warm in-memory caches for fastest first paint
            await self._warm_memory_caches()
            logger.info("Instrument data initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize instrument data: {e}")
            raise
    
    async def _load_from_enriched_json(self):
        """Load data from existing token_lookup_enriched.json"""
        try:
            with open('core/data/token_lookup_enriched.json', 'r') as f:
                data = json.load(f)
            # Keep a master copy in memory for ultra-fast categorization
            self._master_cache = data
            self._master_cache_loaded_at = time.time()
            
            # Categorize instruments by asset class
            categorized = self._categorize_instruments(data)
            
            # Store in Redis as JSON strings
            for asset_class, instruments in categorized.items():
                key = self.keys.get(asset_class)
                if key:
                    await self.redis.set(key, json.dumps(instruments))
            
            # Store master data
            await self.redis.set(self.keys['instruments'], json.dumps(data))
            
            # Update metadata
            metadata = {
                'total_instruments': len(data),
                'last_updated': datetime.now().isoformat(),
                'source': 'token_lookup_enriched.json',
                'categories': {k: len(v) for k, v in categorized.items()}
            }
            await self.redis.set(self.keys['metadata'], json.dumps(metadata))
            
        except Exception as e:
            logger.error(f"Failed to load from enriched JSON: {e}")
            raise

    async def _load_from_binary_crawler_sources(self) -> bool:
        """Try to load instruments from Binary Crawler outputs.
        Supported sources:
          - Redis key 'binary_crawler:instruments' as JSON list
          - File 'crawlers/binary_crawler1.json' as JSON list
        Returns True if loaded successfully, else False.
        """
        # Try Redis snapshot first
        try:
            snapshot = await self.redis.get('binary_crawler:instruments')
            if snapshot:
                data_list = json.loads(snapshot)
                if isinstance(data_list, list) and data_list:
                    await self._store_list_as_categorized(data_list, source='binary_crawler:redis')
                    return True
        except Exception as e:
            logger.warning(f"Binary crawler Redis load failed: {e}")

        # Try local JSON file
        try:
            import os
            # Common locations
            candidate_paths = [
                'crawlers/binary_crawler1.json',
                'crawlers/zerodha_websocket/binary_crawler1.json',
                'crawlers/binary_crawler.json'
            ]
            for path in candidate_paths:
                if os.path.exists(path):
                    with open(path, 'r') as f:
                        data_list = json.load(f)
                    if isinstance(data_list, list) and data_list:
                        await self._store_list_as_categorized(data_list, source=path)
                        return True
        except Exception as e:
            logger.warning(f"Binary crawler file load failed: {e}")

        return False

    async def _store_list_as_categorized(self, data_list: List[Dict], source: str) -> None:
        """Store a simple list of instrument dicts (from binary crawler) into categorized Redis keys."""
        # Convert list->dict keyed by token if token present
        data_as_dict = {}
        for inst in data_list:
            token = str(inst.get('token') or inst.get('instrument_token') or inst.get('id') or '')
            data_as_dict[token or str(len(data_as_dict)+1)] = inst

        # Keep a master copy in memory and Redis
        self._master_cache = data_as_dict
        self._master_cache_loaded_at = time.time()

        categorized = self._categorize_instruments(data_as_dict)
        for asset_class, instruments in categorized.items():
            key = self.keys.get(asset_class)
            if key:
                await self.redis.set(key, json.dumps(instruments))

        await self.redis.set(self.keys['instruments'], json.dumps(data_as_dict))
        metadata = {
            'total_instruments': len(data_as_dict),
            'last_updated': datetime.now().isoformat(),
            'source': source,
            'categories': {k: len(v) for k, v in categorized.items()}
        }
        await self.redis.set(self.keys['metadata'], json.dumps(metadata))

    async def _warm_memory_caches(self) -> None:
        """Populate in-memory caches from Redis for ultra-fast reads."""
        try:
            for asset_class in ('equity_cash', 'equity_futures', 'index_futures', 'index_options'):
                key = self.keys.get(asset_class)
                if not key:
                    continue
                cached_data = await self.redis.get(key)
                if cached_data:
                    self._memory_cache[asset_class] = (time.time(), json.loads(cached_data))
        except Exception as e:
            logger.warning(f"Memory cache warm failed: {e}")
    
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
        name = instrument.get('name', '').upper()
        
        if exchange == 'NSE' and instrument_type == 'EQ':
            return 'equity_cash'
        elif exchange in ['NFO', 'BFO'] and instrument_type == 'FUT':
            if 'NIFTY' in name or 'BANKNIFTY' in name or 'FINNIFTY' in name:
                return 'index_futures'
            else:
                return 'equity_futures'
        elif exchange in ['NFO', 'BFO'] and instrument_type == 'OPT':
            return 'index_options'
        
        return 'equity_cash'  # Default fallback
    
    async def get_instruments_by_asset_class(self, asset_class: str) -> List[Dict]:
        """Get instruments by asset class with caching"""
        try:
            # 1) Try ultra-fast in-memory cache first
            now = time.time()
            mem_hit = self._memory_cache.get(asset_class)
            if mem_hit and (now - mem_hit[0]) < self._memory_cache_ttl_seconds:
                return mem_hit[1]

            key = self.keys.get(asset_class)
            if not key:
                return []
            
            # 2) Try to get from Redis next
            cached_data = await self.redis.get(key)
            if cached_data:
                data = json.loads(cached_data)
                # Update memory cache
                self._memory_cache[asset_class] = (now, data)
                return data
            
            # 3) Fallback to in-process master cache or Redis master
            master_data = None
            if self._master_cache and (now - self._master_cache_loaded_at) < self._master_cache_ttl_seconds:
                master_data = self._master_cache
            else:
                master_str = await self.redis.get(self.keys['instruments'])
                if master_str:
                    master_data = json.loads(master_str)
                    self._master_cache = master_data
                    self._master_cache_loaded_at = now
            if master_data:
                categorized = self._categorize_instruments(master_data)
                data = categorized.get(asset_class, [])
                # Update caches
                self._memory_cache[asset_class] = (now, data)
                return data
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get instruments for {asset_class}: {e}")
            return []

    async def get_options_chain(self, underlying: str) -> Dict[str, List]:
        """Build a lightweight options chain index from cached index_options.
        Returns expiries and strikes arrays for fast UI filters.
        """
        try:
            options = await self.get_instruments_by_asset_class('index_options')
            expiries: List[str] = []
            strikes: List[float] = []
            u_norm = underlying.upper()
            for inst in options:
                sym = (inst.get('symbol') or '').upper()
                name = (inst.get('name') or '').upper()
                if u_norm in sym or u_norm in name:
                    exp = inst.get('expiry')
                    if exp and exp not in expiries:
                        expiries.append(exp)
                    strike_val = inst.get('strike_price')
                    if isinstance(strike_val, (int, float)) and strike_val not in strikes:
                        strikes.append(strike_val)
            strikes.sort()
            expiries.sort()
            return {"expiries": expiries, "strikes": strikes}
        except Exception as e:
            logger.error(f"Failed to build options chain for {underlying}: {e}")
            return {"expiries": [], "strikes": []}
    
    async def search_instruments(self, query: str, asset_class: Optional[str] = None) -> List[Dict]:
        """Search instruments using simple pattern matching"""
        try:
            # Get all instruments for the asset class
            if asset_class:
                instruments = await self.get_instruments_by_asset_class(asset_class)
            else:
                # Search across all asset classes
                all_instruments = []
                for ac in ['equity_cash', 'equity_futures', 'index_futures', 'index_options']:
                    all_instruments.extend(await self.get_instruments_by_asset_class(ac))
                instruments = all_instruments
            
            # Simple pattern matching
            query_lower = query.lower()
            results = []
            for instrument in instruments:
                symbol = instrument.get('symbol', '').lower()
                name = instrument.get('name', '').lower()
                if query_lower in symbol or query_lower in name:
                    results.append(instrument)
            
            return results[:50]  # Limit results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    async def get_instrument_metadata(self) -> Dict:
        """Get instrument metadata and statistics"""
        try:
            metadata_str = await self.redis.get(self.keys['metadata'])
            if metadata_str:
                return json.loads(metadata_str)
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
            await self._warm_memory_caches()
            logger.info("Instrument data refreshed successfully")
        except Exception as e:
            logger.error(f"Failed to refresh data: {e}")
            raise

# Global instance
instrument_manager = SimpleInstrumentManager()
