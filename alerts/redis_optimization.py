# redis_optimization.py
class RedisOptimizer:
    def __init__(self):
        self.redis = redis.Redis(decode_responses=True)
    
    async def optimize_redis_for_dashboard(self):
        """Apply Redis optimizations for dashboard performance"""
        
        # 1. Enable compression for large datasets
        await self.redis.config_set('hash-max-ziplist-entries', 512)
        await self.redis.config_set('hash-max-ziplist-value', 64)
        
        # 2. Increase memory efficiency
        await self.redis.config_set('maxmemory-policy', 'allkeys-lru')
        
        # 3. Create indexes for fast lookups
        await self.create_redis_indexes()
        
        # 4. Pre-warm cache with frequently accessed data
        await self.prewarm_cache()
    
    async def create_redis_indexes(self):
        """Create Redis search indexes for fast queries"""
        # Create FT index for instruments
        try:
            await self.redis.ft().create_index(
                [
                    redis.TextField("symbol"),
                    redis.TagField("asset_class"),
                    redis.NumericField("strike"),
                    redis.TagField("option_type"),
                    redis.TextField("expiry")
                ],
                prefix = ["instrument:"]
            )
        except Exception as e:
            print(f"Index might already exist: {e}")
    
    async def prewarm_cache(self):
        """Preload frequently accessed data"""
        cache_manager = InstrumentCache()
        await cache_manager.preload_instruments()
        
        # Preload current market data for top instruments
        top_instruments = ["NIFTY", "BANKNIFTY", "RELIANCE", "TCS"]
        for symbol in top_instruments:
            await self.redis.get(f"current:{symbol}")  # Trigger cache