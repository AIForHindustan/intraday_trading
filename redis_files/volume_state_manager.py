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
        """
        Initialize VolumeStateManager with proper DB separation:
        - DB 0 (system): Session state and volume_state metadata (session_data)
        - DB 2 (analytics): Volume profile data (analytics_data) - passed to VolumeProfileManager
        """
        # ✅ DB 0: Session state and volume_state metadata (session_data)
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
        
        # ✅ DB 2: Analytics client for volume profile data (analytics_data)
        # Volume profile data belongs to DB 2 (analytics/historical) per redis_config.py
        self.redis_analytics = None
        if redis_client and hasattr(redis_client, 'get_client'):
            try:
                self.redis_analytics = redis_client.get_client(2)  # DB 2: analytics (volume profiles)
                logger.debug("✅ DB 2 (analytics) client initialized for volume profile storage")
            except Exception as e:
                logger.warning(f"Failed to get Redis client for DB 2: {e}")
                self.redis_analytics = None
        elif redis_client:
            # For raw redis-py clients, try to select DB 2
            try:
                self.redis_analytics = redis_client
                self.redis_analytics.select(2)  # Select DB 2 for volume profiles
                logger.debug("✅ DB 2 (analytics) selected for volume profile storage")
            except Exception as e:
                logger.warning(f"Failed to select DB 2 for volume profiles: {e}")
                self.redis_analytics = None
        
        self.token_resolver = token_resolver
        self.local_cache: Dict[str, Dict] = {}  # instrument -> {last_cumulative, session_date}
        
        # ✅ Volume Profile Integration: Use DB 2 (analytics) client for volume profile storage
        self.volume_profile_manager = None
        if self.redis_analytics is not None:
            self._initialize_volume_profile_manager()
    
    def _initialize_volume_profile_manager(self):
        """Initialize VolumeProfileManager with DB 2 (analytics) client for volume profile storage"""
        try:
            from patterns.volume_profile_manager import VolumeProfileManager
            # ✅ Pass DB 2 (analytics) client to VolumeProfileManager
            # VolumeProfileManager expects DB 2 for analytics/historical data per redis_config.py
            self.volume_profile_manager = VolumeProfileManager(self.redis_analytics, self.token_resolver)
            logger.info("✅ VolumeProfileManager integrated with VolumeStateManager (DB 2 analytics)")
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
        # ✅ Use centralized TTL from redis_config.py
        if self.redis is not None:
            try:
                from redis_files.redis_config import get_ttl_for_data_type
                self.redis.hset(state_key, 'last_cumulative', current_cumulative)
                self.redis.hset(state_key, 'session_date', current_date)
                # Use centralized TTL for session_data (DB 0: 57600 seconds = 16 hours)
                session_ttl = get_ttl_for_data_type("session_data")
                self.redis.expire(state_key, session_ttl)
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
            
            # ✅ Straddle volumes: Store in DB 2 (analytics) for consistency with volume profile data
            # Straddle volumes are analytics/historical data, not real-time trading signals
            # ✅ Use centralized TTL from redis_config.py
            from redis_files.redis_config import get_ttl_for_data_type
            
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
            
            # Get centralized TTL for analytics_data (DB 2: 57600 seconds = 16 hours)
            analytics_ttl = get_ttl_for_data_type("analytics_data")
            
            # ✅ Use DB 2 (analytics) for straddle volume data - consistent with volume profile storage
            if self.redis_analytics is not None:
                # Store in Redis DB 2 (analytics) with centralized TTL
                for key, value in straddle_data.items():
                    self.redis_analytics.hset(straddle_key, key, value)
                self.redis_analytics.expire(straddle_key, analytics_ttl)
            elif hasattr(self, '_redis_wrapper') and hasattr(self._redis_wrapper, 'get_client'):
                # Fallback: Try to get DB 2 client from wrapper
                analytics_client = self._redis_wrapper.get_client(2)  # DB 2: analytics
                if analytics_client:
                    for key, value in straddle_data.items():
                        analytics_client.hset(straddle_key, key, value)
                    analytics_client.expire(straddle_key, analytics_ttl)
                else:
                    logger.warning(f"Could not get DB 2 client for straddle volume storage, falling back to DB 0")
                    # Last resort: fallback to system DB (not ideal)
                    session_ttl = get_ttl_for_data_type("session_data")  # DB 0: 57600 seconds
                    for key, value in straddle_data.items():
                        self.redis.hset(straddle_key, key, value)
                    self.redis.expire(straddle_key, session_ttl)
            else:
                # Fallback for raw redis-py clients
                logger.warning(f"Using DB 0 fallback for straddle volume (should be DB 2)")
                session_ttl = get_ttl_for_data_type("session_data")  # DB 0: 57600 seconds
                for key, value in straddle_data.items():
                    self.redis.hset(straddle_key, key, value)
                self.redis.expire(straddle_key, session_ttl)
            
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