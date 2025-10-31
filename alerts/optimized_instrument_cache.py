# dashboard/optimized_instrument_cache.py
import redis
import json
import time
from typing import Dict, List
import asyncio

class InstrumentCache:
    def __init__(self):
        self.redis = redis.Redis(decode_responses=True)
        self.cache_ttl = 3600  # 1 hour cache
        
    async def preload_instruments(self):
        """Preload all instruments at startup - FAST"""
        instruments = {
            "index_fut": [
                {"symbol": "NIFTY", "name": "NIFTY 50", "token": "53490439", "lot_size": 50},
                {"symbol": "BANKNIFTY", "name": "BANK NIFTY", "token": "53490947", "lot_size": 25},
                {"symbol": "FINNIFTY", "name": "FIN NIFTY", "token": "53491455", "lot_size": 40}
            ],
            "index_opt": self._load_options_chain(),
            "equity": self._load_equity_instruments()
        }
        
        # Store as single JSON for instant access
        await self.redis.setex(
            "instruments:cache", 
            self.cache_ttl, 
            json.dumps(instruments)
        )
        
        # Also store individual sets for fast filtering
        for asset_class, inst_list in instruments.items():
            symbols = [inst["symbol"] for inst in inst_list]
            await self.redis.sadd(f"instruments:{asset_class}", *symbols)
            
        print("✅ Instruments preloaded and cached")
    
    def _load_options_chain(self) -> List[Dict]:
        """Load NIFTY options chain - OPTIMIZED"""
        base_symbol = "NIFTY"
        current_price = 21500  # You'd get this dynamically
        strikes = self._generate_strikes(current_price, 20)  # ±20 strikes
        
        options = []
        for strike in strikes:
            for option_type in ["CE", "PE"]:
                options.append({
                    "symbol": f"{base_symbol}{strike}{option_type}",
                    "name": f"{base_symbol} {strike} {option_type}",
                    "token": self._get_option_token(base_symbol, strike, option_type),
                    "strike": strike,
                    "option_type": option_type,
                    "expiry": "2024-11-28"  # Dynamic expiry
                })
        return options
    
    def _generate_strikes(self, current_price: int, count: int) -> List[int]:
        """Generate strike prices around current price"""
        step = 100  # NIFTY strike step
        start = current_price - (count // 2) * step
        return [start + (i * step) for i in range(count)]
    
    async def get_instruments(self, asset_class: str) -> List[Dict]:
        """ULTRA-FAST instrument retrieval"""
        start_time = time.time()
        
        # Try cache first
        cached = await self.redis.get("instruments:cache")
        if cached:
            instruments = json.loads(cached)
            load_time = (time.time() - start_time) * 1000
            print(f"⚡ Instruments loaded from cache: {load_time:.2f}ms")
            return instruments.get(asset_class, [])
        
        # Fallback - rebuild cache
        await self.preload_instruments()
        cached = await self.redis.get("instruments:cache")
        instruments = json.loads(cached)
        return instruments.get(asset_class, [])
    
    async def search_instruments(self, query: str, asset_class: str = None) -> List[Dict]:
        """Fast search with Redis sets"""
        start_time = time.time()
        
        if asset_class:
            # Search within specific asset class
            all_symbols = await self.redis.smembers(f"instruments:{asset_class}")
        else:
            # Search across all asset classes
            all_symbols = await self.redis.sunion(
                "instruments:index_fut",
                "instruments:index_opt", 
                "instruments:equity"
            )
        
        # Filter symbols matching query
        matching_symbols = [sym for sym in all_symbols if query.upper() in sym]
        
        # Get full instrument data for matches
        cached_data = await self.redis.get("instruments:cache")
        all_instruments = json.loads(cached_data) if cached_data else {}
        
        results = []
        for asset_class, instruments in all_instruments.items():
            for inst in instruments:
                if inst["symbol"] in matching_symbols:
                    results.append(inst)
        
        search_time = (time.time() - start_time) * 1000
        print(f"🔍 Instrument search completed: {search_time:.2f}ms")
        return results