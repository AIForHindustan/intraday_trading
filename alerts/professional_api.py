# dashboard/optimized_api.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from fastapi_cache.decorator import cache

app = FastAPI()

# Initialize cache
@app.on_event("startup")
async def startup():
    FastAPICache.init(RedisBackend(app.state.redis), prefix="fastapi-cache")
    # Preload instruments on startup
    cache_manager = InstrumentCache()
    await cache_manager.preload_instruments()

@app.get("/api/instruments/{asset_class}")
@cache(expire=3600)  # Cache for 1 hour
async def get_instruments_fast(asset_class: str):
    """Cached instrument endpoint - SUB-MILLISECOND response"""
    cache_manager = InstrumentCache()
    return await cache_manager.get_instruments(asset_class)

@app.get("/api/instruments/search/{query}")
async def search_instruments_fast(query: str, asset_class: str = None):
    """Fast instrument search"""
    cache_manager = InstrumentCache()
    return await cache_manager.search_instruments(query, asset_class)

@app.get("/api/options-chain/{underlying}")
@cache(expire=300)  # 5 minute cache for options chain
async def get_options_chain_fast(underlying: str):
    """Fast options chain with Greeks"""
    # Your existing options chain logic, but cached
    return await get_cached_options_chain(underlying)