import logging
from datetime import datetime
from typing import Dict, Optional
import redis

logger = logging.getLogger(__name__)

class VolumeStateManager:
    """
    SINGLE SOURCE OF TRUTH for volume state across entire system
    Uses Redis for persistence across process restarts
    Integrates with VolumeProfileManager for enhanced pattern detection
    """
    
    def __init__(self, redis_client: redis.Redis, token_resolver=None):
        # Ensure we use DB 0 (system) for session data and volume state
        # VolumeStateManager stores: volume_state:* (session metadata) and session:* keys
        if redis_client and hasattr(redis_client, 'get_client'):
            try:
                self.redis = redis_client.get_client(0)  # DB 0: system (session_data)
            except Exception as e:
                logger.warning(f"Failed to get Redis client for DB 0: {e}")
                self.redis = None
        elif redis_client:
            self.redis = redis_client  # Fallback for raw redis-py clients
        else:
            self.redis = None  # No Redis client available
        self.token_resolver = token_resolver
        self.local_cache: Dict[str, Dict] = {}  # instrument -> {last_cumulative, session_date}
        
        # NEW: Volume Profile Integration (only if Redis is available)
        self.volume_profile_manager = None
        if self.redis is not None:
            self._initialize_volume_profile_manager()
    
    def _initialize_volume_profile_manager(self):
        """Initialize VolumeProfileManager with proper error handling"""
        try:
            from patterns.volume_profile_manager import VolumeProfileManager
            self.volume_profile_manager = VolumeProfileManager(self.redis, self.token_resolver)
            logger.info("✅ VolumeProfileManager integrated with VolumeStateManager")
        except Exception as e:
            logger.warning(f"⚠️ VolumeProfileManager not available: {e}")
            self.volume_profile_manager = None
    
    def calculate_incremental(self, instrument_token: str, current_cumulative: int, 
                           exchange_timestamp: datetime) -> int:
        """
        Calculate incremental volume with proper session reset handling
        Integrates with VolumeProfileManager for enhanced pattern detection
        
        Returns: incremental volume (never uses cumulative as incremental)
        """
        # Get or initialize instrument state
        state_key = f"volume_state:{instrument_token}"
        state = self.local_cache.get(instrument_token)
        
        if not state:
            # Try Redis first, then initialize (only if Redis is available)
            if self.redis is not None:
                try:
                    redis_state = self.redis.hgetall(state_key)
                    if redis_state and b'last_cumulative' in redis_state and b'session_date' in redis_state:
                        state = {
                            'last_cumulative': int(redis_state[b'last_cumulative']),
                            'session_date': redis_state[b'session_date'].decode()
                        }
                    else:
                        state = {'last_cumulative': None, 'session_date': None}
                except Exception as e:
                    logger.debug(f"Failed to get Redis state for {instrument_token}: {e}")
                    state = {'last_cumulative': None, 'session_date': None}
            else:
                state = {'last_cumulative': None, 'session_date': None}
            self.local_cache[instrument_token] = state
        
        current_date = exchange_timestamp.strftime('%Y-%m-%d')
        
        # Check session reset
        if state['session_date'] != current_date:
            logger.info(f"Session reset for {instrument_token} on {current_date} (was {state['session_date']})")
            state['last_cumulative'] = None
            state['session_date'] = current_date
        else:
            logger.debug(f"No session reset for {instrument_token} - session_date: {state['session_date']}, current_date: {current_date}")
        
        # Calculate incremental
        if state['last_cumulative'] is not None and current_cumulative >= state['last_cumulative']:
            incremental = current_cumulative - state['last_cumulative']
            logger.debug(f"Incremental calculation for {instrument_token}: {current_cumulative} - {state['last_cumulative']} = {incremental}")
        else:
            # Session start or data anomaly - use conservative approach
            incremental = 0  # ✅ NEVER use cumulative as incremental
            logger.debug(f"Session start/anomaly for {instrument_token}: last_cumulative={state['last_cumulative']}, current_cumulative={current_cumulative}, incremental=0")
        
        # Update state
        state['last_cumulative'] = current_cumulative
        
        # Persist to Redis (survives process restarts) - only if Redis is available
        if self.redis is not None:
            try:
                self.redis.hset(state_key, 'last_cumulative', current_cumulative)
                self.redis.hset(state_key, 'session_date', current_date)
                # Set 24-hour TTL to auto-cleanup
                self.redis.expire(state_key, 86400)
            except Exception as e:
                logger.debug(f"Failed to persist state to Redis for {instrument_token}: {e}")
        
        # NEW: Volume Profile Integration
        if self.volume_profile_manager and incremental > 0:
            try:
                # Get symbol for volume profile
                symbol = self._get_symbol_from_token(instrument_token)
                if symbol:
                    # Use last_price from session data for volume profile
                    last_price = self._get_last_price(instrument_token)
                    if last_price:
                        self.volume_profile_manager.update_price_volume(
                            symbol, last_price, incremental, exchange_timestamp
                        )
            except Exception as e:
                logger.debug(f"Volume profile update skipped for {instrument_token}: {e}")
        
        return incremental
    
    def _get_symbol_from_token(self, instrument_token: str) -> Optional[str]:
        """Get symbol from instrument token using token resolver"""
        try:
            if self.token_resolver and hasattr(self.token_resolver, 'resolve_token_to_symbol'):
                return self.token_resolver.resolve_token_to_symbol(instrument_token)
            
            # Fallback: try to get symbol from Redis session data (only if Redis is available)
            if self.redis is not None:
                try:
                    session_key = f"session:TOKEN_{instrument_token}:{datetime.now().strftime('%Y-%m-%d')}"
                    session_data = self.redis.hgetall(session_key)
                    if session_data and b'tradingsymbol' in session_data:
                        return session_data[b'tradingsymbol'].decode()
                except Exception as e:
                    logger.debug(f"Redis symbol lookup failed for token {instrument_token}: {e}")
            
            return None
        except Exception as e:
            logger.debug(f"Symbol resolution failed for token {instrument_token}: {e}")
            return None
    
    def _get_last_price(self, instrument_token: str) -> Optional[float]:
        """Get last price from Redis session data"""
        try:
            if self.redis is None:
                return None
            session_key = f"session:TOKEN_{instrument_token}:{datetime.now().strftime('%Y-%m-%d')}"
            session_data = self.redis.hgetall(session_key)
            if session_data and b'last_price' in session_data:
                return float(session_data[b'last_price'])
            return None
        except Exception as e:
            logger.debug(f"Price retrieval failed for token {instrument_token}: {e}")
            return None
    
    def get_volume_profile_data(self, symbol: str) -> Dict:
        """
        Get volume profile data for pattern detection
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with volume profile data (POC, Value Area, etc.)
        """
        if self.volume_profile_manager:
            try:
                profile_data = self.volume_profile_manager.get_volume_nodes(symbol)
                return profile_data if profile_data is not None else {}
            except Exception as e:
                logger.debug(f"Volume profile data retrieval failed for {symbol}: {e}")
                return {}
        return {}
    
    def track_straddle_volume(self, underlying_symbol: str, ce_symbol: str, pe_symbol: str, 
                            ce_volume: int, pe_volume: int, exchange_timestamp: datetime) -> Dict[str, int]:
        """
        Track volume for straddle strategy (CE + PE combined)
        Returns: Dictionary with individual and combined volume metrics
        """
        try:
            # Track CE volume
            ce_incremental = self.calculate_incremental(ce_symbol, ce_volume, exchange_timestamp)
            
            # Track PE volume  
            pe_incremental = self.calculate_incremental(pe_symbol, pe_volume, exchange_timestamp)
            
            # Calculate combined metrics
            combined_incremental = ce_incremental + pe_incremental
            combined_cumulative = ce_volume + pe_volume
            
            # Store straddle-specific metrics in Redis DB 1 (realtime) - real-time trading data
            straddle_key = f"straddle_volume:{underlying_symbol}:{exchange_timestamp.strftime('%Y%m%d')}"
            straddle_data = {
                'ce_incremental': ce_incremental,
                'pe_incremental': pe_incremental,
                'combined_incremental': combined_incremental,
                'ce_cumulative': ce_volume,
                'pe_cumulative': pe_volume,
                'combined_cumulative': combined_cumulative,
                'timestamp': exchange_timestamp.isoformat(),
                'underlying': underlying_symbol
            }
            
            # Get realtime client (DB 1) for straddle volume data
            if hasattr(self, '_redis_wrapper') and hasattr(self._redis_wrapper, 'get_client'):
                realtime_client = self._redis_wrapper.get_client(1)  # DB 1: realtime
                if realtime_client:
                    # Store in Redis DB 1 (realtime) with 1-hour TTL
                    for key, value in straddle_data.items():
                        realtime_client.hset(straddle_key, key, value)
                    realtime_client.expire(straddle_key, 3600)
                else:
                    # Fallback to system DB if can't get realtime client
                    for key, value in straddle_data.items():
                        self.redis.hset(straddle_key, key, value)
                    self.redis.expire(straddle_key, 3600)
            else:
                # Fallback for raw redis-py clients
                for key, value in straddle_data.items():
                    self.redis.hset(straddle_key, key, value)
                self.redis.expire(straddle_key, 3600)
            
            logger.debug(f"Straddle volume tracked: {underlying_symbol} - CE: {ce_incremental}, PE: {pe_incremental}, Combined: {combined_incremental}")
            
            return {
                'ce_incremental': ce_incremental,
                'pe_incremental': pe_incremental,
                'combined_incremental': combined_incremental,
                'ce_cumulative': ce_volume,
                'pe_cumulative': pe_volume,
                'combined_cumulative': combined_cumulative
            }
            
        except Exception as e:
            logger.error(f"Error tracking straddle volume for {underlying_symbol}: {e}")
            return {
                'ce_incremental': 0,
                'pe_incremental': 0,
                'combined_incremental': 0,
                'ce_cumulative': ce_volume,
                'pe_cumulative': pe_volume,
                'combined_cumulative': ce_volume + pe_volume
            }

# Global instance cache (per redis_client)
_volume_manager_cache: Dict[int, VolumeStateManager] = {}

def get_volume_manager(redis_client=None, token_resolver=None) -> VolumeStateManager:
    """
    Get volume manager instance with optional Redis client and token resolver.
    
    If redis_client is provided, uses that client (avoids duplicate connections).
    If not provided, creates a new client using get_redis_client().
    
    Uses a cache keyed by redis_client id to avoid creating duplicate instances.
    """
    global _volume_manager_cache
    
    # If no redis_client provided, get default one
    if redis_client is None:
        from redis_files.redis_client import get_redis_client
        redis_client = get_redis_client()
    
    # Create cache key based on redis_client identity
    # For wrapped clients, use the underlying client
    actual_client = redis_client
    if hasattr(redis_client, 'get_client'):
        try:
            actual_client = redis_client.get_client(0)  # Get DB 0 client
        except Exception:
            actual_client = redis_client
    
    cache_key = id(actual_client) if actual_client else 'default'
    
    # Return cached instance if available
    if cache_key in _volume_manager_cache:
        return _volume_manager_cache[cache_key]
    
    # Create new instance with provided client
    volume_manager = VolumeStateManager(redis_client, token_resolver)
    _volume_manager_cache[cache_key] = volume_manager
    
    return volume_manager