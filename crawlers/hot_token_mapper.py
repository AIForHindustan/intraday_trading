# hot_token_mapper.py (Redis-optimized)
import json
import logging
import os
from typing import Dict, Optional, Set, Any
from datetime import datetime

# Try to import redis, fallback to file-based if not available
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

class HotTokenMapper:
    """
    Singleton token mapper that uses Redis as primary cache, falls back to file I/O
    Maintains backward compatibility with original interface
    """
    _instance = None
    _initialized = False
    
    def __new__(cls, redis_client=None):
        if cls._instance is None:
            cls._instance = super(HotTokenMapper, cls).__new__(cls)
            cls._instance._pending_redis_client = redis_client
        elif redis_client is not None and not cls._initialized:
            # If instance exists but not initialized, update pending redis_client
            cls._instance._pending_redis_client = redis_client
        return cls._instance
    
    def __init__(self, redis_client=None):
        if not self._initialized:
            # Use pending redis_client from __new__ if available
            if hasattr(self, '_pending_redis_client') and self._pending_redis_client is not None:
                redis_client = self._pending_redis_client
                delattr(self, '_pending_redis_client')  # Clean up
            self.logger = logging.getLogger(__name__)
            self.hot_token_map: Dict[str, Dict] = {}
            self.hot_symbol_map: Dict[str, Dict] = {}
            self.active_tokens: Set[str] = set()
            self.full_mapper = None  # Lazy load full mapper only if needed
            self.redis_client = None
            
            # Use provided Redis client or try to initialize one
            if redis_client:
                self.redis_client = redis_client
                try:
                    self.redis_client.ping()
                    self.logger.info("✅ Using provided Redis client for HotTokenMapper")
                except Exception as e:
                    self.logger.warning(f"⚠️ Provided Redis client not available, will try to create new: {e}")
                    self.redis_client = None
            
            # Try to initialize Redis client if not provided
            if not self.redis_client and REDIS_AVAILABLE:
                try:
                    # ✅ CRITICAL FIX: Use centralized RedisConnectionManager singleton
                    from redis_files.redis_client import redis_manager
                    self.redis_client = redis_manager.get_client()
                    self.redis_client.ping()
                    self.logger.info(f"✅ Redis connected for HotTokenMapper using singleton (DB 1)")
                except Exception as e:
                    self.logger.warning(f"⚠️ Redis not available for HotTokenMapper, using file-based: {e}")
                    self.redis_client = None
            
            self._load_hot_tokens()
            self._initialized = True
    
    def _load_active_tokens_from_configs(self):
        """Load active tokens from ALL crawler configs"""
        self.active_tokens = set()
        crawler_dir = os.path.dirname(__file__)
        
        # Load from binary_crawler1.json (intraday crawler)
        crawler1_path = os.path.join(crawler_dir, 'binary_crawler1', 'binary_crawler1.json')
        if os.path.exists(crawler1_path):
            with open(crawler1_path, 'r') as f:
                crawler1_data = json.load(f)
                tokens = crawler1_data.get('tokens', [])
                self.active_tokens.update(str(t) for t in tokens)
                self.logger.info(f"✅ Loaded {len(tokens)} tokens from binary_crawler1.json")
        
        # Note: crawler2_volatility and crawler3_sso_xvenue have been removed from this codebase
        # They run independently on a separate machine
        
        self.logger.info(f"✅ Total {len(self.active_tokens)} active tokens loaded from all crawler configs")
    
    def _load_hot_tokens(self):
        """Load only active tokens - try Redis first, fallback to files
        
        NOTE: Data mining and research crawlers are file-only (no Redis).
        Always load from files to ensure all crawler tokens are included.
        """
        start_time = datetime.now()
        
        try:
            # Try to load from Redis cache first (only for intraday crawler)
            # Skip Redis for data_mining/research crawlers as they don't use Redis
            if self.redis_client:
                try:
                    if self.redis_client.exists("hot_token_cache_ready"):
                        # Check if Redis cache has all crawler tokens by loading from files first
                        # to compare token counts
                        self.logger.info("📁 Loading active tokens from all crawler configs...")
                        self._load_active_tokens_from_configs()
                        
                        # If we have many active tokens (>500), likely includes data_mining/research
                        # In that case, always rebuild from files to ensure completeness
                        if len(self.active_tokens) > 500:
                            self.logger.info(f"📊 Detected {len(self.active_tokens)} active tokens - "
                                           f"rebuilding from files (includes data_mining/research crawlers)")
                            # Continue to file loading below
                        else:
                            # Only intraday crawler tokens - can use Redis cache
                            self.logger.info("✅ Loading hot tokens from Redis cache...")
                            self._load_from_redis()
                            load_time = (datetime.now() - start_time).total_seconds() * 1000
                            self.logger.info(f"🚀 HotTokenMapper initialized from Redis in {load_time:.1f}ms "
                                           f"({len(self.hot_token_map)} hot tokens, "
                                           f"{len(self.hot_symbol_map)} symbols)")
                            return
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to load from Redis, falling back to files: {e}")
            else:
                # No Redis client - load active tokens first
                self._load_active_tokens_from_configs()
            
            # Fallback to file-based loading (or if Redis cache incomplete)
            if not self.active_tokens:
                self._load_active_tokens_from_configs()
            
            self.logger.info("📁 Loading hot token mappings from files...")
            
            # Step 2: Load mappings from token_lookup_enriched.json AND metadata_resolver
            project_root = os.path.dirname(os.path.dirname(__file__))
            enriched_path = os.path.join(project_root, 'core', 'data', 'token_lookup_enriched.json')
            
            # Load from enriched file first
            tokens_from_enriched = set()
            if os.path.exists(enriched_path):
                with open(enriched_path, 'r') as f:
                    token_lookup = json.load(f)
                    
                    # Iterate through tokens in lookup file
                    for token_str, token_data in token_lookup.items():
                        # Only keep tokens that are active
                        if token_str in self.active_tokens:
                            self.hot_token_map[token_str] = token_data
                            tokens_from_enriched.add(token_str)
                            
                            # Extract symbol from token_data structure
                            key_field = token_data.get('key', '')
                            if ':' in key_field:
                                symbol = key_field.split(':', 1)[1]
                            else:
                                symbol = token_data.get('name', '')
                            
                            if symbol:
                                self.hot_symbol_map[symbol] = token_data
            
            # Step 3: For active tokens NOT in enriched file, use metadata_resolver
            missing_tokens = self.active_tokens - tokens_from_enriched
            if missing_tokens:
                try:
                    from crawlers.metadata_resolver import metadata_resolver
                    self.logger.info(f"🔍 Resolving {len(missing_tokens)} tokens via metadata_resolver...")
                    
                    resolved_count = 0
                    for token_str in missing_tokens:
                        try:
                            token_int = int(token_str)
                            metadata = metadata_resolver.get_metadata(token_int)
                            
                            # Create token_data structure compatible with hot_token_map
                            symbol = metadata.get('symbol', metadata.get('tradingsymbol', ''))
                            if symbol and not symbol.startswith('UNKNOWN'):
                                key_field = metadata.get('key', f"{metadata.get('exchange', 'NSE')}:{symbol}")
                                token_data = {
                                    'key': key_field,
                                    'name': symbol,
                                    'symbol': symbol,
                                    'tradingsymbol': symbol,
                                    'exchange': metadata.get('exchange', 'NSE'),
                                    'instrument_type': metadata.get('instrument_type', ''),
                                    'segment': metadata.get('segment', ''),
                                    'source': metadata.get('source', 'metadata_resolver')
                                }
                                
                                self.hot_token_map[token_str] = token_data
                                self.hot_symbol_map[symbol] = token_data
                                resolved_count += 1
                        except (ValueError, Exception) as e:
                            continue
                    
                    if resolved_count > 0:
                        self.logger.info(f"✅ Resolved {resolved_count} tokens via metadata_resolver")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to resolve tokens via metadata_resolver: {e}")
            
            # Cache to Redis if available
            if self.redis_client:
                try:
                    self._cache_to_redis()
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to cache to Redis: {e}")
            
            load_time = (datetime.now() - start_time).total_seconds() * 1000
            self.logger.info(f"🚀 HotTokenMapper initialized in {load_time:.1f}ms "
                           f"({len(self.hot_token_map)} hot tokens, "
                           f"{len(self.hot_symbol_map)} symbols)")
                           
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize HotTokenMapper: {e}")
            # Fallback: will lazy load full mapper if needed
    
    def _load_from_redis(self):
        """Load hot tokens from Redis cache"""
        # Load token->symbol mappings
        token_to_symbol = self.redis_client.hgetall("hot_token:token_to_symbol")
        symbol_to_token = self.redis_client.hgetall("hot_token:symbol_to_token")
        
        # Reconstruct hot_token_map and hot_symbol_map
        for token_str in token_to_symbol.keys():
            token_data_str = self.redis_client.hget("hot_token:token_data", token_str)
            if token_data_str:
                token_data = json.loads(token_data_str)
                self.hot_token_map[token_str] = token_data
                self.active_tokens.add(token_str)
                
                # Reconstruct symbol map
                symbol = token_to_symbol[token_str]
                if symbol:
                    self.hot_symbol_map[symbol] = token_data
    
    def _cache_to_redis(self):
        """Cache hot tokens to Redis for faster future loads"""
        if not self.redis_client:
            return
        
        try:
            pipe = self.redis_client.pipeline()
            
            # Cache token->symbol and symbol->token mappings
            for token_str, token_data in self.hot_token_map.items():
                key_field = token_data.get('key', '')
                if ':' in key_field:
                    symbol = key_field.split(':', 1)[1]
                else:
                    symbol = token_data.get('name', '')
                
                if symbol:
                    pipe.hset("hot_token:token_to_symbol", token_str, symbol)
                    pipe.hset("hot_token:symbol_to_token", symbol, token_str)
                    pipe.hset("hot_token:token_data", token_str, json.dumps(token_data))
            
            # Mark cache as ready
            pipe.set("hot_token_cache_ready", "1")
            pipe.execute()
            
            self.logger.info(f"✅ Cached {len(self.hot_token_map)} hot tokens to Redis")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to cache to Redis: {e}")
    
    def token_to_symbol(self, instrument_token: int) -> str:
        """Return the mapped symbol for a given instrument token (matches InstrumentMapper interface)"""
        try:
            token_str = str(int(instrument_token))
        except (TypeError, ValueError):
            self.logger.warning("Invalid instrument token: %s", instrument_token)
            return "UNKNOWN"
        
        # Check hot token map first (O(1) lookup)
        if token_str in self.hot_token_map:
            token_data = self.hot_token_map[token_str]
            # Extract symbol from token_data structure
            key_field = token_data.get('key', '')
            if ':' in key_field:
                symbol = key_field.split(':', 1)[1]
            else:
                symbol = token_data.get('name', '')
            
            if symbol:
                return symbol
        
        # Fallback to full mapper for unknown tokens (rare)
        return self._get_full_mapper().token_to_symbol(instrument_token)
    
    def get_symbol(self, token: str) -> Optional[str]:
        """Get symbol for token (O(1) lookup) - alias for compatibility"""
        return self.token_to_symbol(int(token)) if token.isdigit() else None
    
    def get_token(self, symbol: str) -> Optional[str]:
        """Get token for symbol (O(1) lookup)"""
        if symbol in self.hot_symbol_map:
            return self.hot_symbol_map[symbol].get('token')
        
        # Fallback to full mapper for unknown symbols (rare)
        return self._get_full_mapper().get_token(symbol)
    
    def get_token_data(self, token: str) -> Optional[Dict]:
        """Get full token data (O(1) lookup)"""
        if token in self.hot_token_map:
            return self.hot_token_map[token]
        
        # Fallback to full mapper for unknown tokens (rare)
        return self._get_full_mapper().get_token_data(token)
    
    def get_token_metadata(self, instrument_token: int) -> Dict[str, Any]:
        """Get metadata for a token (compatible with InstrumentMapper interface)"""
        try:
            token_str = str(int(instrument_token))
        except (TypeError, ValueError):
            self.logger.warning("Invalid instrument token: %s", instrument_token)
            return {}
        
        # Check hot token map first (O(1) lookup)
        if token_str in self.hot_token_map:
            token_data = self.hot_token_map[token_str]
            # Return in the format expected by InstrumentMapper.get_token_metadata()
            return {
                "tradingsymbol": token_data.get('tradingsymbol') or token_data.get('symbol') or self.token_to_symbol(instrument_token),
                "symbol": token_data.get('symbol') or token_data.get('tradingsymbol') or self.token_to_symbol(instrument_token),
                "name": token_data.get('name', ''),
                "exchange": token_data.get('exchange', 'NSE'),
                "instrument_type": token_data.get('instrument_type', ''),
                "segment": token_data.get('segment', ''),
                "key": token_data.get('key', ''),
            }
        
        # Fallback to full mapper for unknown tokens (rare)
        return self._get_full_mapper().get_token_metadata(instrument_token)
    
    def get_instrument_info(self) -> Dict[int, Dict]:
        """Get instrument info dictionary for websocket parsers (backward compatibility)"""
        instrument_info = {}
        for token_str, token_data in self.hot_token_map.items():
            try:
                token_int = int(token_str)
                # Convert token_data to format expected by parsers
                key_field = token_data.get('key', '')
                if ':' in key_field:
                    exchange, symbol = key_field.split(':', 1)
                else:
                    exchange = token_data.get('exchange', 'NSE')
                    symbol = token_data.get('name', '')
                
                instrument_info[token_int] = {
                    'instrument_token': token_int,
                    'exchange_token': token_data.get('exchange_token', ''),
                    'tradingsymbol': symbol,
                    'name': symbol,
                    'last_price': token_data.get('last_price', 0),
                    'expiry': token_data.get('expiry', ''),
                    'strike': token_data.get('strike', 0),
                    'tick_size': token_data.get('tick_size', 0.05),
                    'lot_size': token_data.get('lot_size', 1),
                    'instrument_type': token_data.get('instrument_type', 'EQ'),
                    'segment': token_data.get('segment', 'NSE'),
                    'exchange': exchange,
                }
            except (ValueError, KeyError):
                continue
        
        return instrument_info
    
    def _get_full_mapper(self):
        """Lazy load full InstrumentMapper only when needed"""
        if self.full_mapper is None:
            from crawlers.utils.instrument_mapper import InstrumentMapper
            self.logger.warning("🔄 Falling back to full InstrumentMapper for unknown token")
            self.full_mapper = InstrumentMapper()
        return self.full_mapper
    
    def is_hot_token(self, token: str) -> bool:
        """Check if token is in hot set"""
        return token in self.hot_token_map
    
    def get_hot_token_count(self) -> int:
        """Get number of hot tokens"""
        return len(self.hot_token_map)

# Global singleton instance
_hot_token_mapper = None

def get_hot_token_mapper(redis_client=None) -> HotTokenMapper:
    """Get singleton HotTokenMapper instance
    
    Args:
        redis_client: Optional Redis client to use (if provided, will use this instead of creating new connection)
    """
    global _hot_token_mapper
    if _hot_token_mapper is None:
        _hot_token_mapper = HotTokenMapper(redis_client=redis_client)
    return _hot_token_mapper


# Alias for backward compatibility and explicit Redis usage
def get_redis_token_mapper(redis_client) -> HotTokenMapper:
    """Get HotTokenMapper instance with explicit Redis client
    
    Args:
        redis_client: Redis client to use
        
    Returns:
        HotTokenMapper instance
    """
    return get_hot_token_mapper(redis_client=redis_client)