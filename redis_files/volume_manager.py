"""
Unified Volume Manager - Single Source of Truth
===============================================

Combines VolumeStateManager and CorrectVolumeCalculator into one unified manager.
Handles: incremental volume, volume profiles, volume ratios, and baselines.

✅ Uses yaml_field_loader for field mapping to avoid parsing issues
✅ Maintains backward compatibility with existing code
✅ Single source of truth for all volume calculations
✅ Unified database (DB 1) for ALL volume data - eliminates data silos
✅ Real-time baseline adjustments using unified database
"""

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple, Any, List
import redis
import statistics
import re

from redis_files.redis_key_standards import RedisKeyStandards

logger = logging.getLogger(__name__)

class VolumeManager:
    """
    Unified Volume Manager - Single source of truth for all volume calculations.
    Combines functionality from:
    - VolumeStateManager: Incremental volume calculation, session state management
    - CorrectVolumeCalculator: Volume ratio calculation, baseline integration
    """
    
    def __init__(self, redis_client: redis.Redis = None, token_resolver=None):
        """
        Initialize VolumeManager with unified DB 1 storage (via redis_gateway):
        - DB 1 (unified): ALL volume data consolidated in one place
          - Volume state (vol:state:{token}) - session state for incremental calculation
          - Volume profile data (vol:profile:*) - POC, Value Area, distribution
          - Straddle volumes (vol:straddle:{underlying}:{date}) - analytics data
        """
        # ✅ SOURCE OF TRUTH: Use RedisManager82 for client creation
        if redis_client and hasattr(redis_client, 'get_client'):
            # Use provided client wrapper
            self.redis = redis_client.get_client(1) if hasattr(redis_client, 'get_client') else redis_client
        else:
            try:
                from redis_files.redis_client import RedisManager82
                # ✅ SOURCE OF TRUTH: Use RedisManager82 for DB 1 (unified structure)
                self.redis = RedisManager82.get_client(process_name="volume_manager", db=1)
                logger.debug(f"✅ [VOLUME_MANAGER_INIT] Using RedisManager82 (DB 1 unified)")
            except Exception as e:
                # Fallback to redis_gateway if RedisManager82 unavailable
                try:
                    from redis_files.redis_client import redis_gateway
                    self.redis = redis_gateway.redis if hasattr(redis_gateway, 'redis') else redis_gateway
                    logger.warning(f"⚠️ [VOLUME_MANAGER_INIT] Fallback to redis_gateway: {e}")
                except ImportError:
                    self.redis = None
                    logger.error(f"❌ [VOLUME_MANAGER_INIT] No Redis client available")
        
        self.redis_analytics = self.redis  # Same client for all volume data
        
        # ✅ SOURCE OF TRUTH: Initialize key builder
        from redis_files.redis_key_standards import get_key_builder
        self.key_builder = get_key_builder()
        
        # ✅ Store redis_client_wrapper reference for token lookup and session data access
        self._redis_client_wrapper = redis_client if hasattr(redis_client, 'get_client') else None
        
        self.token_resolver = token_resolver
        self.local_cache: Dict[str, Dict] = {}  # instrument -> {last_cumulative, session_date}
        
        # ✅ Token-to-symbol cache for fast lookups
        self._token_symbol_cache: Dict[str, Optional[str]] = {}
        self._load_token_cache()
        
        # ✅ Initialize UnifiedTimeAwareBaseline - uses DB 1 for all baselines with time window logic
        # Note: UnifiedTimeAwareBaseline is created per-symbol, so we store the factory here
        try:
            # Ensure project root is in sys.path for imports
            import sys
            import importlib
            from pathlib import Path
            project_root = Path(__file__).resolve().parents[1]
            project_root_str = str(project_root)
            # Remove any incorrect patterns module from sys.modules if it's from wrong location
            if 'patterns' in sys.modules:
                patterns_mod = sys.modules['patterns']
                if hasattr(patterns_mod, '__file__') and 'core/patterns' in patterns_mod.__file__:
                    del sys.modules['patterns']
            # Ensure project root is FIRST in sys.path (before core/)
            if project_root_str in sys.path:
                sys.path.remove(project_root_str)
            sys.path.insert(0, project_root_str)
            
            # Force import using importlib to avoid cache issues
            from utils.time_aware_volume_baseline import UnifiedTimeAwareBaseline
            
            # Import patterns module first to ensure it's available
            if 'patterns' not in sys.modules:
                import patterns
            # Verify it's from the correct location
            patterns_mod = sys.modules['patterns']
            if hasattr(patterns_mod, '__file__') and 'core/patterns' in patterns_mod.__file__:
                # Wrong location, force reimport
                del sys.modules['patterns']
                import patterns
            # Now import the submodule
            volume_thresholds_module = importlib.import_module('patterns.volume_thresholds')
            VolumeThresholdCalculator = volume_thresholds_module.VolumeThresholdCalculator
            
            # ✅ CRITICAL FIX: self.redis is already the singleton client (DB 1)
            # Use it directly for UnifiedTimeAwareBaseline
            self._baseline_db1_client = self.redis
            self.baseline_calc = None  # Will be created per-symbol when needed
            
            # ✅ UPDATED: VolumeThresholdCalculator now uses UnifiedTimeAwareBaseline via redis_client
            # Pass redis_client directly so it can create UnifiedTimeAwareBaseline instances per symbol
            self.threshold_calc = VolumeThresholdCalculator(redis_client=self._baseline_db1_client) if self._baseline_db1_client else None
            logger.debug("✅ [VOLUME_MANAGER_INIT] UnifiedTimeAwareBaseline (DB 1, time-aware) and threshold calculators initialized")
        except Exception as e:
            logger.error(f"❌ [VOLUME_MANAGER_INIT] Failed to initialize baseline/threshold calculators: {e}", exc_info=True)
            self.baseline_calc = None
            self.threshold_calc = None
        
        # ✅ Volume Profile Integration: Use DB 1 (unified) client for volume profile storage
        self.volume_profile_manager = None
        if self.redis_analytics is not None:
            logger.debug(f"🔍 [VOLUME_MANAGER_INIT] Initializing VolumeProfileManager")
            self._initialize_volume_profile_manager()
        else:
            logger.debug(f"⚠️ [VOLUME_MANAGER_INIT] Skipping VolumeProfileManager init - no redis_analytics")
        
        # ✅ UNIFIED DATABASE: Enable real-time data access for baseline adjustments
        self._enable_realtime_adjustments = True
        
        # ✅ PHASE 3: Real-Time Adjustment Monitoring
        self._realtime_metrics = {
            'total_calculations': 0,
            'realtime_adjusted': 0,
            'historical_only': 0,
            'ratios_capped_at_50': 0,
            'ratios_below_50': 0,
            'baseline_sources': {'real_time_adjusted': 0, 'historical': 0, 'fallback': 0},
            'avg_volume_ratio': 0.0,
            'symbols_processed': set()
        }
        
        logger.debug("✅ [VOLUME_MANAGER_INIT] Unified database enabled - real-time baseline adjustments available")
        logger.debug("✅ [PHASE_3] Real-time adjustment monitoring enabled")
    
    def _initialize_volume_profile_manager(self):
        """Initialize VolumeProfileManager with DB 1 (unified) client for volume profile storage"""
        try:
            # Ensure project root is in sys.path for imports
            import sys
            import importlib
            from pathlib import Path
            project_root = Path(__file__).resolve().parents[1]
            project_root_str = str(project_root)
            # Remove any incorrect patterns module from sys.modules if it's from wrong location
            if 'patterns' in sys.modules:
                patterns_mod = sys.modules['patterns']
                if hasattr(patterns_mod, '__file__') and 'core/patterns' in patterns_mod.__file__:
                    del sys.modules['patterns']
            # Ensure project root is FIRST in sys.path (before core/)
            if project_root_str in sys.path:
                sys.path.remove(project_root_str)
            sys.path.insert(0, project_root_str)
            
            # Force import using importlib to avoid cache issues
            # Import patterns module first to ensure it's available
            if 'patterns' not in sys.modules:
                import patterns
            # Verify it's from the correct location
            patterns_mod = sys.modules['patterns']
            if hasattr(patterns_mod, '__file__') and 'core/patterns' in patterns_mod.__file__:
                # Wrong location, force reimport
                del sys.modules['patterns']
                import patterns
            # Now import the submodule
            volume_profile_module = importlib.import_module('patterns.volume_profile_manager')
            VolumeProfileManager = volume_profile_module.VolumeProfileManager
            logger.debug(f"🔍 [VOLUME_MANAGER_INIT] Creating VolumeProfileManager with redis_analytics type: {type(self.redis_analytics).__name__}")
            self.volume_profile_manager = VolumeProfileManager(self.redis_analytics, self.token_resolver)
            logger.debug("✅ [VOLUME_MANAGER_INIT] VolumeProfileManager integrated with VolumeManager (DB 1 unified)")
        except Exception as e:
            logger.error(f"❌ [VOLUME_MANAGER_INIT] VolumeProfileManager initialization failed: {e}", exc_info=True)
            self.volume_profile_manager = None
    
    # ============================================================================
    # VOLUME STATE MANAGER METHODS (Incremental Volume Calculation)
    # ============================================================================
    
    def calculate_incremental(self, instrument_token: str, current_cumulative: int, 
                           exchange_timestamp: datetime, symbol: Optional[str] = None) -> int:
        """
        Calculate incremental volume with proper session reset handling
        Integrates with VolumeProfileManager for enhanced pattern detection
        
        Returns: incremental volume (never uses cumulative as incremental)
        """
        # ✅ SOURCE OF TRUTH: Use key builder for volume state key
        state_key = self.key_builder.live_volume_state(instrument_token)
        state = self.local_cache.get(instrument_token)
        
        if not state:
            # Try Redis first, then initialize (only if Redis is available)
            if self.redis is not None:
                try:
                    # ✅ CRITICAL FIX: self.redis is already the singleton client
                    redis_state = self.redis.hgetall(state_key)
                    if redis_state and b'last_cumulative' in redis_state and b'session_date' in redis_state:
                        state = {
                            'last_cumulative': int(redis_state[b'last_cumulative']),
                            'session_date': redis_state[b'session_date'].decode()
                        }
                    else:
                        state = {'last_cumulative': None, 'session_date': None}
                except Exception as e:
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
        
        # Calculate incremental
        if state['last_cumulative'] is not None and current_cumulative >= state['last_cumulative']:
            incremental = current_cumulative - state['last_cumulative']
        else:
            # Session start or data anomaly - use conservative approach
            incremental = 0  # ✅ NEVER use cumulative as incremental
        
        # Update state
        state['last_cumulative'] = current_cumulative
        
        # ✅ Persist to Redis DB 1 (unified) - unified volume storage
        # Use centralized TTL from redis_config.py for analytics_data
        if self.redis is not None:
            try:
                from redis_files.redis_client import get_ttl_for_data_type
                # ✅ CRITICAL FIX: self.redis is already the singleton client
                self.redis.hset(state_key, 'last_cumulative', current_cumulative)
                self.redis.hset(state_key, 'session_date', current_date)
                # ✅ Use centralized TTL for analytics_data (DB 1: 57600 seconds = 16 hours)
                analytics_ttl = get_ttl_for_data_type("analytics_data")
                self.redis.expire(state_key, analytics_ttl)
            except Exception as e:
                pass
        
        # Volume Profile Integration
        if self.volume_profile_manager and incremental > 0:
            try:
                # ✅ Use provided symbol if available, otherwise resolve from token
                if not symbol:
                    symbol = self._get_symbol_from_token(instrument_token)
                if symbol:
                    # Use last_price from session data for volume profile
                    last_price = self._get_last_price(instrument_token)
                    if last_price:
                        self.volume_profile_manager.update_price_volume(
                            symbol, last_price, incremental, exchange_timestamp
                        )
            except Exception as e:
                logger.error(f"❌ [VOLUME_PROFILE_FLOW] Error in volume profile update: {e}", exc_info=True)
        
        return incremental
    
    def _load_token_cache(self):
        """Pre-load token-to-symbol cache from Redis session data"""
        try:
            if not self._redis_client_wrapper:
                logger.debug("🔍 [TOKEN_CACHE] No redis_client_wrapper - skipping cache preload")
                return
            
            # Get DB 0 client for session data
            session_client = None
            if hasattr(self._redis_client_wrapper, 'get_client'):
                session_client = self._redis_client_wrapper.get_client(0)  # DB 0 for session data
            
            if not session_client:
                logger.debug("🔍 [TOKEN_CACHE] No session client - skipping cache preload")
                return
            
            # Scan for session keys and extract token->symbol mappings
            try:
                # ✅ Check if Redis connection is valid before scanning
                try:
                    # Test connection with a simple ping
                    session_client.ping()
                except (OSError, ValueError, ConnectionError) as conn_error:
                    logger.debug(f"⚠️ [TOKEN_CACHE] Redis connection not available: {conn_error}, skipping cache preload")
                    return
                
                session_date = datetime.now().strftime('%Y-%m-%d')
                # ✅ SOURCE OF TRUTH: Use SCAN instead of KEYS for pattern matching
                pattern = f"session:TOKEN_*:{session_date}"
                cursor = 0
                cache_count = 0
                
                # ✅ Safely import yaml_field_loader with error handling
                try:
                    from utils.yaml_field_loader import resolve_session_field
                    use_yaml_field_loader = True
                except Exception as import_error:
                    logger.debug(f"⚠️ [TOKEN_CACHE] Could not import yaml_field_loader: {import_error}, using fallback")
                    use_yaml_field_loader = False
                
                # Use SCAN to avoid blocking Redis (with connection error handling)
                max_iterations = 1000  # Safety limit
                iteration = 0
                while iteration < max_iterations:
                    try:
                        cursor, keys = session_client.scan(cursor, match=pattern, count=100)
                    except (OSError, ValueError, ConnectionError) as scan_error:
                        # Connection closed or invalid during scan
                        logger.debug(f"⚠️ [TOKEN_CACHE] Redis connection error during scan: {scan_error}, stopping cache preload")
                        break
                    
                    for key in keys:
                        try:
                            if isinstance(key, bytes):
                                key_str = key.decode()
                            else:
                                key_str = str(key)
                            
                            if 'TOKEN_' in key_str:
                                token_str = key_str.split('TOKEN_')[1].split(':')[0]
                                try:
                                    session_data = session_client.hgetall(key_str)
                                except (OSError, ValueError, ConnectionError):
                                    # Connection closed during hgetall, skip this key
                                    continue
                                
                                # Try to get tradingsymbol field
                                symbol = None
                                if session_data:
                                    # First try direct 'tradingsymbol' key
                                    if b'tradingsymbol' in session_data:
                                        symbol = session_data[b'tradingsymbol'].decode()
                                    # Then try yaml_field_loader if available
                                    elif use_yaml_field_loader:
                                        try:
                                            tradingsymbol_field = resolve_session_field('tradingsymbol')
                                            if tradingsymbol_field and tradingsymbol_field.encode() in session_data:
                                                symbol = session_data[tradingsymbol_field.encode()].decode()
                                        except Exception:
                                            pass  # Fallback to direct lookup
                                
                                if symbol:
                                    self._token_symbol_cache[token_str] = symbol
                                    cache_count += 1
                        except Exception as e:
                            continue
                    
                    if cursor == 0:
                        break
                    
                    iteration += 1
                
                if cache_count > 0:
                    logger.info(f"✅ [TOKEN_CACHE] Pre-loaded {cache_count} token-to-symbol mappings from Redis")
                else:
                    logger.debug(f"🔍 [TOKEN_CACHE] No token mappings found for date {session_date}")
            except (OSError, ValueError, ConnectionError) as e:
                # Connection errors - don't log as warning, just debug
                logger.debug(f"⚠️ [TOKEN_CACHE] Redis connection error during cache preload: {e}")
            except Exception as e:
                logger.warning(f"⚠️ [TOKEN_CACHE] Failed to pre-load cache: {e}", exc_info=True)
        except Exception as e:
            logger.warning(f"⚠️ [TOKEN_CACHE] Error loading token cache: {e}")
    
    def _get_symbol_from_token(self, instrument_token: str) -> Optional[str]:
        """Get symbol from instrument token using cached lookup"""
        try:
            token_str = str(instrument_token)
            
            # ✅ Check cache first
            if token_str in self._token_symbol_cache:
                return self._token_symbol_cache[token_str]
            
            # Try token resolver
            if self.token_resolver:
                if hasattr(self.token_resolver, 'resolve_token_to_symbol'):
                    symbol = self.token_resolver.resolve_token_to_symbol(int(instrument_token) if instrument_token.isdigit() else instrument_token)
                    if symbol:
                        self._token_symbol_cache[token_str] = symbol
                        return symbol
                elif hasattr(self.token_resolver, 'token_to_symbol'):
                    symbol = self.token_resolver.token_to_symbol(int(instrument_token) if instrument_token.isdigit() else instrument_token)
                    if symbol:
                        self._token_symbol_cache[token_str] = symbol
                        return symbol
            
            # ✅ Fallback: Use hot_token_mapper (no Redis dependency)
            try:
                from crawlers.hot_token_mapper import get_hot_token_mapper
                mapper = get_hot_token_mapper()
                token_int = int(instrument_token) if instrument_token.isdigit() else instrument_token
                symbol = mapper.token_to_symbol(token_int)
                if symbol and not symbol.startswith("UNKNOWN"):
                    self._token_symbol_cache[token_str] = symbol
                    return symbol
            except Exception as e:
                logger.debug(f"Error resolving token with hot_token_mapper: {e}")
            
            # Cache None to avoid repeated lookups
            self._token_symbol_cache[token_str] = None
            return None
        except Exception as e:
            logger.debug(f"Error resolving token {instrument_token}: {e}")
            return None
    
    def _get_last_price(self, instrument_token: str) -> Optional[float]:
        """Get last price from multiple Redis sources (DB 0 session data, DB 1 realtime ticks)"""
        try:
            symbol = self._get_symbol_from_token(instrument_token)
            
            # ✅ PRIMARY: Check DB 1 (realtime) - where intraday crawler writes ticks
            if symbol and hasattr(self, '_redis_client_wrapper') and self._redis_client_wrapper:
                try:
                    if hasattr(self._redis_client_wrapper, 'get_client'):
                        realtime_client = self._redis_client_wrapper.get_client(1)  # DB 1 for realtime
                        
                        if realtime_client:
                            # ✅ Use yaml_field_loader for field mapping
                            from utils.yaml_field_loader import resolve_session_field
                            last_price_field = resolve_session_field('last_price')
                            
                            # ✅ SOURCE OF TRUTH: Use key builder for tick hash key
                            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                            tick_hash_key = self.key_builder.live_ticks_hash(canonical_symbol)
                            tick_data = realtime_client.hgetall(tick_hash_key)
                            if tick_data:
                                price_key = b'last_price' if b'last_price' in tick_data else last_price_field.encode() if last_price_field.encode() in tick_data else 'last_price'
                                if price_key in tick_data:
                                    price_val = tick_data[price_key]
                                    if isinstance(price_val, bytes):
                                        price_val = price_val.decode('utf-8')
                                    return float(price_val)
                            
                            # ✅ SOURCE OF TRUTH: Use canonical symbol for price key
                            price_key = f"price:latest:{canonical_symbol}"
                            price_val = realtime_client.get(price_key)
                            if price_val:
                                return float(price_val)
                except Exception as e:
                    logger.debug(f"Error checking DB 1 for last_price: {e}")
            
            # ✅ FALLBACK: Check DB 0 session data
            if hasattr(self, '_redis_client_wrapper') and self._redis_client_wrapper:
                session_client = None
                if hasattr(self._redis_client_wrapper, 'get_client'):
                    session_client = self._redis_client_wrapper.get_client(0)  # DB 0 for session data
                
                if session_client:
                    # ✅ SOURCE OF TRUTH: Use key builder for session key
                    # Note: Session key uses token format, need to resolve to symbol first
                    try:
                        symbol = self._get_symbol_from_token(instrument_token)
                        if symbol:
                            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
                            date_str = datetime.now().strftime('%Y-%m-%d')
                            session_key = self.key_builder.live_session(canonical_symbol, date_str)
                        else:
                            # Fallback to token-based key if symbol resolution fails
                            session_key = f"session:TOKEN_{instrument_token}:{datetime.now().strftime('%Y-%m-%d')}"
                    except Exception:
                        # Fallback to token-based key
                        session_key = f"session:TOKEN_{instrument_token}:{datetime.now().strftime('%Y-%m-%d')}"
                    session_data = session_client.hgetall(session_key)
                    if session_data:
                        from utils.yaml_field_loader import resolve_session_field
                        last_price_field = resolve_session_field('last_price')
                        price_key = b'last_price' if b'last_price' in session_data else last_price_field.encode() if last_price_field.encode() in session_data else 'last_price'
                        if price_key in session_data:
                            price_val = session_data[price_key]
                            if isinstance(price_val, bytes):
                                price_val = price_val.decode('utf-8')
                            return float(price_val)
            
            return None
        except Exception as e:
            logger.debug(f"Error in _get_last_price for token {instrument_token}: {e}")
            return None
    
    def get_volume_profile_data(self, symbol: str) -> Dict:
        """Get volume profile data for pattern detection"""
        if self.volume_profile_manager:
            try:
                profile_data = self.volume_profile_manager.get_volume_nodes(symbol)
                return profile_data if profile_data is not None else {}
            except Exception as e:
                return {}
        return {}
    
    def track_straddle_volume(self, underlying_symbol: str, ce_symbol: str, pe_symbol: str, 
                            ce_volume: int, pe_volume: int, exchange_timestamp: datetime) -> Dict[str, int]:
        """Track volume for straddle strategy (CE + PE combined)"""
        try:
            ce_incremental = self.calculate_incremental(ce_symbol, ce_volume, exchange_timestamp, symbol=ce_symbol)
            pe_incremental = self.calculate_incremental(pe_symbol, pe_volume, exchange_timestamp, symbol=pe_symbol)
            
            combined_incremental = ce_incremental + pe_incremental
            combined_cumulative = ce_volume + pe_volume
            
            from redis_files.redis_client import get_ttl_for_data_type
            from redis_files.redis_key_standards import RedisKeyStandards
            
            # ✅ SOURCE OF TRUTH: Use key builder for straddle volume key
            date_str = exchange_timestamp.strftime('%Y%m%d')
            # Note: Straddle volume key format: vol:straddle:{underlying}:{date}
            # Using key builder pattern for consistency
            canonical_underlying = RedisKeyStandards.canonical_symbol(underlying_symbol)
            straddle_key = f"vol:straddle:{canonical_underlying}:{date_str}"
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
            
            analytics_ttl = get_ttl_for_data_type("analytics_data")
            
            if self.redis_analytics is not None:
                for key, value in straddle_data.items():
                    self.redis_analytics.hset(straddle_key, key, value)
                self.redis_analytics.expire(straddle_key, analytics_ttl)
            elif hasattr(self, '_redis_client_wrapper') and hasattr(self._redis_client_wrapper, 'get_client'):
                analytics_client = self._redis_client_wrapper.get_client(2)
                if analytics_client:
                    for key, value in straddle_data.items():
                        analytics_client.hset(straddle_key, key, value)
                    analytics_client.expire(straddle_key, analytics_ttl)
            
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
    
    # ============================================================================
    # CORRECT VOLUME CALCULATOR METHODS (Volume Ratio Calculation)
    # ============================================================================
    
    def _get_incremental_volume(self, tick_data: dict) -> float:
        """
        Extract incremental volume from tick data using ONLY incremental volume fields.
        
        ✅ CRITICAL: NEVER use cumulative volume (zerodha_cumulative_volume) as incremental.
        VolumeManager.calculate_incremental() calculates: incremental = current_cumulative - last_cumulative
        This is per-tick incremental, not cumulative.
        
        If bucket_incremental_volume is 0 or missing, return 0.0 (do NOT use cumulative as fallback).
        """
        try:
            # ✅ Use yaml_field_loader for field mapping
            from utils.yaml_field_loader import resolve_session_field
            
            # ✅ ONLY incremental volume fields - NEVER use cumulative
            volume_fields = [
                resolve_session_field("bucket_incremental_volume"),  # Primary: calculated by VolumeManager
                resolve_session_field("incremental_volume"),         # Alias
                "bucket_incremental_volume",  # Fallback to direct name
                "incremental_volume",         # Fallback to direct name
                "volume",                     # Generic incremental
                resolve_session_field("zerodha_last_traded_quantity"),  # Per-trade quantity
                "zerodha_last_traded_quantity",  # Fallback
                "last_traded_quantity",       # Alias
                "quantity",                   # Generic quantity
            ]
            
            # ✅ DEBUG: Log all volume field values found in tick_data
            found_fields = {}
            for field in volume_fields:
                if field in tick_data:
                    found_fields[field] = tick_data[field]
            
            if found_fields:
                logger.debug(
                    f"🔍 [VOLUME_DEBUG] _get_incremental_volume: Found volume fields: {found_fields}"
                )
            else:
                logger.debug(
                    f"⚠️ [VOLUME_DEBUG] _get_incremental_volume: No volume fields found. "
                    f"Available keys: {list(tick_data.keys())[:10]}"
                )
            
            for field in volume_fields:
                if field in tick_data and tick_data[field] is not None:
                    try:
                        volume = float(tick_data[field])
                        if volume > 0:
                            logger.debug(
                                f"✅ [VOLUME_DEBUG] _get_incremental_volume: Using field '{field}' = {volume:.0f}"
                            )
                            return volume
                        else:
                            logger.debug(
                                f"⚠️ [VOLUME_DEBUG] _get_incremental_volume: Field '{field}' = {volume:.0f} (zero, skipping)"
                            )
                    except (ValueError, TypeError) as e:
                        logger.debug(
                            f"⚠️ [VOLUME_DEBUG] _get_incremental_volume: Field '{field}' = {tick_data[field]} "
                            f"cannot be converted to float: {e}"
                        )
                        continue
            
            # ✅ Return 0.0 if no incremental volume found (do NOT use cumulative as fallback)
            logger.debug(
                f"❌ [VOLUME_DEBUG] _get_incremental_volume: No valid incremental volume found. "
                f"All checked fields returned 0 or None. Available keys: {list(tick_data.keys())}"
            )
            return 0.0
            
        except Exception as e:
            logger.error(
                f"❌ [VOLUME_DEBUG] _get_incremental_volume exception: {e}", exc_info=True
            )
            return 0.0
    
    def calculate_volume_ratio(self, symbol: str, current_tick: dict) -> float:
        """
        ENHANCED: Now uses UnifiedTimeAwareBaseline with 55-day OHLC support.
        
        Calculate volume ratio with time-aware baseline from DB 1.
        
        ✅ CRITICAL FIX: Now uses time-aware baselines (applies session multipliers).
        ✅ Uses UnifiedTimeAwareBaseline to get time-adjusted baseline from DB 1.
        ✅ ENHANCED: Includes 55-day OHLC calculation if Redis baseline is missing.
        ✅ Applies reasonable caps based on symbol type (not 50.0 for everything).
        
        Priority:
        1. Redis cached baseline (vol:baseline:{symbol})
        2. Calculate from 55-day OHLC data (if Redis missing)
        3. Emergency baseline fallback
        
        Formula: volume_ratio = current_volume / (raw_baseline × session_multiplier)
        """
        try:
            if not isinstance(symbol, str):
                symbol = str(symbol)
            
            # 1. Get CURRENT incremental volume (from this tick)
            current_volume = self._get_incremental_volume(current_tick)
            
            # 2. Get TIME-AWARE EXPECTED volume from DB 1 using UnifiedTimeAwareBaseline
            if not self._baseline_db1_client:
                logger.warning(f"❌ [VOLUME_DEBUG] {symbol}: No baseline calculator available")
                return 1.0
            
            # ✅ Parse timestamp properly for time window detection
            timestamp = current_tick.get('exchange_timestamp') or current_tick.get('timestamp')
            if isinstance(timestamp, str):
                try:
                    # Try to parse ISO format to datetime
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    # Keep as string if parsing fails (UnifiedTimeAwareBaseline handles it)
                    pass
            elif not isinstance(timestamp, datetime) and timestamp is not None:
                # Convert other types to datetime if possible
                try:
                    timestamp = datetime.fromisoformat(str(timestamp))
                except:
                    timestamp = datetime.now()
            
            # ✅ Create UnifiedTimeAwareBaseline instance for this symbol (takes redis_client, symbol)
            from utils.time_aware_volume_baseline import UnifiedTimeAwareBaseline
            baseline_calc = UnifiedTimeAwareBaseline(self._baseline_db1_client, symbol)
            
            # ✅ Get time-aware baseline (includes session multiplier) - now includes time windows!
            expected_volume = baseline_calc.get_baseline_volume(timestamp)
            if expected_volume <= 0:
                cached_baseline = self._get_cached_dynamic_baseline(symbol)
                if cached_baseline > 0:
                    logger.debug(
                        f"✅ [VOLUME_DEBUG] {symbol}: Using cached dynamic baseline={cached_baseline:.0f}"
                    )
                    expected_volume = cached_baseline
            
            # ✅ DEBUG LOGGING: Always log current_volume and baseline_volume for diagnosis
            logger.debug(
                f"🔍 [VOLUME_DEBUG] {symbol}: current_volume={current_volume:.0f}, "
                f"baseline_volume={expected_volume:.0f}"
            )
            
            # ✅ FIX: If current_volume is 0 but baseline exists, don't return 0.0
            # Instead, return 1.0 (neutral) or use last known volume_ratio from Redis
            if current_volume <= 0:
                if expected_volume > 0:
                    logger.debug(
                        f"⚠️ [VOLUME_DEBUG] {symbol}: current_volume=0, baseline={expected_volume:.0f} exists, "
                        f"returning minimal ratio 0.01"
                    )
                    return 0.01
                else:
                    logger.debug(
                        f"❌ [VOLUME_DEBUG] {symbol}: current_volume=0, baseline=0, returning 0.0"
                    )
                    return 0.0
            
            # 3. Validate baseline is reasonable - only reject if it's clearly invalid (0 or negative)
            # ✅ FIXED: Don't use arbitrary thresholds - trust calculated baselines from valid sources (55-day OHLC, Redis, etc.)
            # Only reject if baseline is 0 or negative (invalid), not based on arbitrary volume thresholds
            if expected_volume <= 0:
                logger.warning(f"⚠️ [VOLUME_DEBUG] {symbol}: Invalid baseline={expected_volume:.0f} (0 or negative), trying emergency baseline")
                # Try emergency baseline with time multiplier (from volume profile POC)
                emergency_baseline = baseline_calc._get_emergency_baseline(baseline_calc.symbol)
                if emergency_baseline > 0:
                    # Apply time multiplier to emergency baseline too
                    session_mult = baseline_calc._get_session_multiplier(timestamp)
                    expected_volume = emergency_baseline * session_mult
                    logger.debug(
                        f"✅ [VOLUME_DEBUG] {symbol}: Using emergency baseline from volume profile POC={emergency_baseline:.0f} "
                        f"× session_mult={session_mult:.2f} = {expected_volume:.0f}"
                    )
                else:
                    logger.warning(f"⚠️ [VOLUME_DEBUG] {symbol}: No emergency baseline available either, using 1.0 (neutral)")
                    expected_volume = 1.0  # Neutral fallback
            
            # 4. Calculate ratio
            if expected_volume > 0:
                volume_ratio = current_volume / expected_volume
            else:
                logger.warning(f"❌ [VOLUME_DEBUG] {symbol}: expected_volume=0, returning 1.0")
                volume_ratio = 1.0
            
            # 5. Apply reasonable cap (not 50.0 for everything!)
            capped_ratio = self._reasonable_cap(volume_ratio, symbol)
            
            # ✅ ENHANCED LOGGING: Log when there's an issue, high volume ratio, or when storing
            if expected_volume < 1000 or capped_ratio > 10.0 or capped_ratio == 0.0:
                logger.debug(
                    f"✅ [VOLUME_RATIO] {symbol}: current={current_volume:.0f}, "
                    f"time_aware_baseline={expected_volume:.0f}, ratio={volume_ratio:.2f}, "
                    f"capped={capped_ratio:.2f}"
                )
            elif capped_ratio > 0:
                # Log successful calculation (at debug level)
                logger.debug(
                    f"✅ [VOLUME_DEBUG] {symbol}: Stored volume_ratio={capped_ratio:.2f} "
                    f"(current={current_volume:.0f}, baseline={expected_volume:.0f})"
                )
            
            return capped_ratio
            
        except Exception as e:
            logger.error(f"❌ [VOLUME_DEBUG] Failed to calculate volume ratio for {symbol}: {e}", exc_info=True)
            return 0.0
    
    def _reasonable_cap(self, ratio: float, symbol: str) -> float:
        """
        Apply reasonable cap based on symbol type and volume profile POC data.
        Uses volume profile POC to determine appropriate cap, not hardcoded 50.0.
        
        Note: get_volume_nodes() returns poc_price, value_area_high, value_area_low, 
        profile_strength, support_levels, resistance_levels, but NOT poc_volume.
        poc_volume is only available in get_profile_data() or from Redis directly.
        """
        try:
            # ✅ Use volume profile POC data to determine cap
            if self.volume_profile_manager:
                try:
                    # get_volume_nodes() returns: poc_price, value_area_high, value_area_low,
                    # profile_strength, support_levels, resistance_levels
                    volume_nodes = self.volume_profile_manager.get_volume_nodes(symbol)
                    if volume_nodes:
                        profile_strength = volume_nodes.get('profile_strength', 0)
                        poc_price = volume_nodes.get('poc_price', 0)
                        
                        # If we have valid POC data, use profile_strength to determine cap
                        if poc_price > 0 and profile_strength > 0:
                            # Strong profile (>0.5): allow higher ratios (up to 100x for extreme spikes)
                            # Weak profile: cap lower (50x)
                            max_cap = 100.0 if profile_strength > 0.5 else 50.0
                            return min(ratio, max_cap)
                except Exception as e:
                    logger.debug(f"Could not get volume profile for cap calculation: {e}")
            
            # Fallback: symbol-based caps (not 50.0 for everything)
            symbol_upper = symbol.upper()
            if "BANKNIFTY" in symbol_upper:
                return min(ratio, 100.0)  # Higher cap for index options
            elif "NIFTY" in symbol_upper and "BANK" not in symbol_upper:
                return min(ratio, 100.0)  # Higher cap for index options
            elif any(stock in symbol_upper for stock in ["RELIANCE", "TCS", "HDFC", "INFY", "HDFCBANK"]):
                return min(ratio, 75.0)  # Medium cap for high-volume stocks
            else:
                return min(ratio, 50.0)  # Default cap for others
                
        except Exception as e:
            logger.debug(f"Error in _reasonable_cap for {symbol}: {e}")
            return min(ratio, 50.0)  # Safe fallback
    
    def calculate_bucket_volume_ratio(self, symbol: str, bucket_data: dict) -> float:
        """Calculate volume ratio for 5-minute buckets"""
        try:
            from utils.yaml_field_loader import resolve_session_field
            bucket_volume_field = resolve_session_field('bucket_incremental_volume')
            bucket_volume = bucket_data.get(bucket_volume_field) or bucket_data.get('bucket_incremental_volume', 0)
            
            if bucket_volume <= 0:
                return 0.0
            
            # ✅ UPDATED: Use UnifiedTimeAwareBaseline for baseline calculation
            if not self._baseline_db1_client:
                return 1.0
            
            from utils.time_aware_volume_baseline import UnifiedTimeAwareBaseline
            baseline_calc = UnifiedTimeAwareBaseline(self._baseline_db1_client, symbol)
            minute_baseline = baseline_calc.get_baseline_volume(timestamp=datetime.now())
            five_minute_expected = minute_baseline * 5  # Scale to 5 minutes
            
            if five_minute_expected <= 0:
                logger.warning(f"No 5-minute baseline for {symbol}")
                return 1.0
                
            ratio = bucket_volume / five_minute_expected
            return self._reasonable_cap(ratio, symbol)
            
        except Exception as e:
            logger.error(f"Failed to calculate bucket volume ratio for {symbol}: {e}")
            return 0.0
    
    def calculate_volume_metrics(self, symbol: str, tick_data: dict) -> Dict[str, float]:
        """
        Calculate comprehensive volume metrics with real-time baseline adjustments.
        
        ✅ UNIFIED DATABASE: Stores real-time tick data in DB 1 for baseline adjustments.
        """
        try:
            # Get volume components using VolumeResolver
            from utils.correct_volume_calculator import VolumeResolver
            incremental_volume = self._get_incremental_volume(tick_data)
            cumulative_volume = VolumeResolver.get_cumulative_volume(tick_data)
            
            # ✅ UNIFIED DATABASE: Store real-time tick data in DB 1
            if self._enable_realtime_adjustments and self.redis is not None:
                self._store_realtime_tick_data(symbol, tick_data)
                logger.debug(f"✅ [UNIFIED_DB] Stored real-time tick data for {symbol}")
            
            # ✅ Get DYNAMIC baseline (historical + real-time adjusted)
            dynamic_baseline = self._get_dynamic_baseline(symbol, tick_data)
            
            # ✅ CRITICAL: Log baseline details to debug 0.0 volume_ratio issue
            if dynamic_baseline <= 0:
                logger.warning(
                    f"⚠️ [VOLUME_DEBUG] {symbol}: dynamic_baseline={dynamic_baseline:.2f} "
                    f"(incremental={incremental_volume:.0f}, cumulative={cumulative_volume:.0f})"
                )
            else:
                logger.debug(f"✅ [UNIFIED_DB] Dynamic baseline for {symbol}: {dynamic_baseline:.2f}")
            
            # Calculate volume ratio with real-time context
            volume_ratio = self._calculate_real_time_ratio(tick_data, dynamic_baseline)
            
            # ✅ CRITICAL: Log volume_ratio calculation details
            if volume_ratio == 0.0:
                logger.warning(
                    f"⚠️ [VOLUME_DEBUG] {symbol}: volume_ratio=0.0, "
                    f"incremental={incremental_volume:.0f}, baseline={dynamic_baseline:.2f}"
                )
            # Reduced verbosity - only log if ratio is extreme
            if volume_ratio > 10.0 or volume_ratio < 0.1:
                logger.debug(f"✅ [UNIFIED_DB] Volume ratio for {symbol}: {volume_ratio:.2f}")
            
            # ✅ Update volume profile with real-time data
            if self._enable_realtime_adjustments and self.redis is not None:
                self._update_volume_profile_realtime(symbol, tick_data)
                # Reduced verbosity - volume profile updates are frequent
                # logger.debug(f"✅ [UNIFIED_DB] Updated volume profile for {symbol}")
            
            # ✅ Update dynamic baseline in unified database
            if self._enable_realtime_adjustments and self.redis is not None:
                self._update_dynamic_baseline_realtime(symbol, tick_data, dynamic_baseline)
                logger.debug(f"✅ [UNIFIED_DB] Updated dynamic baseline for {symbol}: {dynamic_baseline:.2f}")
            
            return {
                'incremental_volume': incremental_volume,
                'cumulative_volume': cumulative_volume,
                'volume_ratio': volume_ratio,
                'dynamic_baseline': dynamic_baseline,
                'baseline_source': 'real_time_adjusted' if self._enable_realtime_adjustments else 'historical',
                'timestamp': tick_data.get('exchange_timestamp', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate volume metrics for {symbol}: {e}")
            return {
                'incremental_volume': 0.0,
                'cumulative_volume': 0.0,
                'volume_ratio': 0.0,
                'dynamic_baseline': 0.0,
                'baseline_source': 'fallback',
                'timestamp': 0
            }
    
    def calculate_volume_spike_threshold(self, symbol: str, bucket_resolution: str, 
                                       current_time: datetime, vix_regime: str,
                                       confidence_level: float = 0.95) -> float:
        """Calculate volume spike threshold using statistical methods"""
        if not self.threshold_calc:
            return 0.0
        return self.threshold_calc.calculate_volume_spike_threshold(
            symbol, bucket_resolution, current_time, vix_regime, confidence_level
        )
    
    def calculate_volume_ratio_advanced(self, current_volume: float, threshold: float) -> float:
        """Calculate volume ratio with advanced statistical methods"""
        if not self.threshold_calc:
            return 0.0
        return self.threshold_calc.calculate_volume_ratio(current_volume, threshold)
    
    def detect_volume_anomaly(self, current_volume: float, historical_volumes: list,
                            bucket_resolution: str, confidence: float = 0.99) -> bool:
        """Detect volume anomalies using statistical methods"""
        if not self.threshold_calc:
            return False
        return self.threshold_calc.detect_volume_anomaly(
            current_volume, historical_volumes, bucket_resolution, confidence
        )
    
    def get_optimal_bucket_resolution(self, symbol: str, current_time: datetime) -> str:
        """Get optimal bucket resolution for symbol and time"""
        if not self.threshold_calc:
            return "5min"
        return self.threshold_calc.get_optimal_bucket_resolution(symbol, current_time)
    
    def override_baseline_for_symbol(self, symbol: str, baseline_value: float):
        """Manual baseline override for critical symbols."""
        try:
            from utils.time_aware_volume_baseline import UnifiedTimeAwareBaseline
            from redis_files.redis_key_standards import RedisKeyStandards, get_key_builder
            
            if not self._baseline_db1_client:
                logger.warning("⚠️ [BASELINE_OVERRIDE] No DB1 client available for override")
                return
            
            UnifiedTimeAwareBaseline(self._baseline_db1_client, symbol)  # Ensure symbol normalized internally
            key_builder = get_key_builder()
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            baseline_key = key_builder.live_volume_baseline(canonical_symbol)
            
            override_data = {
                'baseline_1min': str(baseline_value),
                'source': 'manual_override',
                'override_timestamp': datetime.now().isoformat()
            }
            self._baseline_db1_client.hset(baseline_key, mapping=override_data)
            self._baseline_db1_client.expire(baseline_key, 86400)
            logger.info(f"✅ [BASELINE_OVERRIDE] Set manual baseline for {symbol}: {baseline_value:.0f}")
        except Exception as e:
            logger.error(f"❌ [BASELINE_OVERRIDE] Failed to override baseline for {symbol}: {e}", exc_info=True)
    
    # ============================================================================
    # UNIFIED DATABASE METHODS (Real-Time Data Storage & Baseline Adjustments)
    # ============================================================================
    
    def _store_realtime_tick_data(self, symbol: str, tick_data: dict):
        """
        Store real-time tick data in unified database (DB 1) for baseline adjustments.
        Uses yaml_field_loader for field mapping.
        """
        try:
            if not self.redis:
                return
            
            from utils.yaml_field_loader import resolve_session_field
            
            timestamp = tick_data.get('exchange_timestamp')
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
            
            # ✅ SOURCE OF TRUTH: Use canonical symbol for realtime tick key
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            # Use timestamp as key component
            timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S_%f')
            tick_key = f"realtime:{canonical_symbol}:{timestamp_str}"
            
            # Get volume fields using yaml_field_loader
            volume_field = resolve_session_field('bucket_incremental_volume')
            price_field = resolve_session_field('last_price')
            
            incremental_volume = self._get_incremental_volume(tick_data)
            price = tick_data.get(price_field) or tick_data.get('last_price') or tick_data.get('price', 0)
            volume_ratio = tick_data.get('volume_ratio', 0.0)
            
            # Store in unified database (DB 1)
            # ✅ CRITICAL FIX: self.redis is already the singleton client (not a gateway wrapper)
            # Use context manager to ensure connection is released back to pool
            # ✅ Add connection error handling to prevent crashes during connection exhaustion
            try:
                with self.redis.pipeline() as pipeline:
                    pipeline.hset(tick_key, mapping={
                        'volume': str(incremental_volume),
                        'incremental_volume': str(incremental_volume),
                        'price': str(price),
                        'volume_ratio': str(volume_ratio),
                        'timestamp': timestamp.isoformat(),
                        'symbol': symbol
                    })
                    pipeline.expire(tick_key, 864000)  # ✅ INCREASED: Keep for 10 days for ClickHouse recovery
                    pipeline.execute()
            except redis.exceptions.MaxConnectionsError as e:
                # Connection pool exhausted - log but don't crash
                logger.warning(f"⚠️ [VOLUME_MANAGER] Connection pool exhausted for {symbol}, skipping real-time tick storage: {e}")
                # Don't re-raise - allow processing to continue
                return
            except redis.exceptions.ConnectionError as e:
                # Connection error - log but don't crash
                logger.warning(f"⚠️ [VOLUME_MANAGER] Connection error for {symbol}, skipping real-time tick storage: {e}")
                return
            
        except Exception as e:
            # Only log non-connection errors with full traceback
            if not isinstance(e, (redis.exceptions.MaxConnectionsError, redis.exceptions.ConnectionError)):
                logger.error(f"❌ Error storing real-time tick data for {symbol}: {e}", exc_info=True)
            else:
                # Connection errors already handled above, just log warning
                logger.warning(f"⚠️ [VOLUME_MANAGER] Connection issue for {symbol}: {e}")
    
    def _update_volume_profile_realtime(self, symbol: str, tick_data: dict):
        """Update volume profile with real-time tick data using VolumeProfileManager."""
        try:
            if not self.volume_profile_manager:
                return
            
            from utils.yaml_field_loader import resolve_session_field
            
            price_field = resolve_session_field('last_price')
            price = tick_data.get(price_field) or tick_data.get('last_price') or tick_data.get('price')
            volume = self._get_incremental_volume(tick_data)
            
            # Get timestamp
            timestamp = tick_data.get('exchange_timestamp')
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
            
            if price and volume > 0:
                # ✅ Use VolumeProfileManager for proper volume profile updates
                self.volume_profile_manager.update_price_volume(
                    symbol=symbol,
                    price=float(price),
                    volume=float(volume),
                    timestamp=timestamp
                )
                # Reduced verbosity - volume profile updates are frequent
                # logger.debug(f"✅ [VOLUME_PROFILE] Updated volume profile for {symbol} via VolumeProfileManager")
                
        except Exception as e:
            logger.debug(f"Error updating volume profile for {symbol}: {e}")
    
    def _update_dynamic_baseline_realtime(self, symbol: str, tick_data: dict, dynamic_baseline: float):
        """Update dynamic baseline in unified database."""
        try:
            if not self.redis:
                return
            
            timestamp = tick_data.get('exchange_timestamp')
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
            
            # ✅ SOURCE OF TRUTH: Use key builder for volume baseline key
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            baseline_key = self.key_builder.live_volume_baseline(canonical_symbol)
            # ✅ CRITICAL FIX: self.redis is already the singleton client
            self.redis.hset(baseline_key, mapping={
                'current': str(dynamic_baseline),
                'updated': timestamp.isoformat(),
                'source': 'real_time_adjusted'
            })
            self.redis.expire(baseline_key, 86400)  # Keep for 24 hours
            
        except Exception as e:
            logger.debug(f"Error updating dynamic baseline for {symbol}: {e}")
    
    def _get_cached_dynamic_baseline(self, symbol: str) -> float:
        """Fetch dynamic baseline cached in Redis (baseline:{symbol})."""
        if not self.redis:
            return 0.0
        try:
            candidates = [symbol]
            try:
                from redis_files.redis_key_standards import normalize_symbol
                normalized = normalize_symbol(symbol)
                if normalized and normalized not in candidates:
                    candidates.append(normalized)
            except Exception:
                pass
            
            for candidate in candidates:
                # ✅ SOURCE OF TRUTH: Use key builder for volume baseline key
                canonical_candidate = RedisKeyStandards.canonical_symbol(candidate)
                baseline_key = self.key_builder.live_volume_baseline(canonical_candidate)
                data = self.redis.hgetall(baseline_key)
                if not data:
                    continue
                # Decode bytes if needed
                sample_value = next(iter(data.values()), None)
                if isinstance(sample_value, bytes):
                    data = {
                        k.decode() if isinstance(k, bytes) else k:
                        v.decode() if isinstance(v, bytes) else v
                        for k, v in data.items()
                    }
                current = data.get('current')
                if current:
                    try:
                        baseline = float(current)
                        if baseline > 0:
                            return baseline
                    except (ValueError, TypeError):
                        continue
        except Exception as e:
            logger.debug(f"Error fetching cached dynamic baseline for {symbol}: {e}")
        return 0.0
    
    def _get_dynamic_baseline(self, symbol: str, tick_data: dict) -> float:
        """
        Get baseline that combines historical patterns with real-time activity.
        Uses unified database to access real-time data.
        """
        try:
            # Historical baseline from TimeAwareVolumeBaseline
            timestamp = tick_data.get('exchange_timestamp')
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
            
            # ✅ UPDATED: Use UnifiedTimeAwareBaseline for baseline calculation
            if not self._baseline_db1_client:
                return 0.0
            
            from utils.time_aware_volume_baseline import UnifiedTimeAwareBaseline
            baseline_calc = UnifiedTimeAwareBaseline(self._baseline_db1_client, symbol)
            historical_baseline = baseline_calc.get_baseline_volume(timestamp=timestamp)
            
            # Real-time average from unified database
            realtime_avg = self._get_realtime_average(symbol)
            
            # If we have sufficient real-time data, use dynamic weighting
            if realtime_avg > historical_baseline * 0.1:  # At least 10% of historical
                weight = self._calculate_realtime_weight(timestamp)
                dynamic_baseline = (historical_baseline * (1 - weight)) + (realtime_avg * weight)
                return dynamic_baseline
            else:
                # Not enough real-time data, rely on historical
                return historical_baseline
                
        except Exception as e:
            logger.debug(f"Error getting dynamic baseline for {symbol}: {e}")
            return 0.0
    
    def _get_realtime_average(self, symbol: str) -> float:
        """Calculate average volume from recent real-time data in unified database."""
        try:
            if not self.redis:
                return 0.0
            
            # ✅ SOURCE OF TRUTH: Use canonical symbol and SCAN instead of KEYS
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            pattern = f"realtime:{canonical_symbol}:*"
            # ✅ SOURCE OF TRUTH: Use SCAN instead of KEYS for efficiency
            keys = []
            cursor = 0
            while True:
                cursor, batch = self.redis.scan(cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            
            if not keys or len(keys) == 0:
                return 0.0
            
            # Use last 100 ticks for efficiency
            recent_keys = keys[-100:] if len(keys) > 100 else keys
            
            # Use pipeline for efficient batch retrieval
            # ✅ CRITICAL FIX: self.redis is already the singleton client
            # Use context manager to ensure connection is released back to pool
            try:
                with self.redis.pipeline() as pipeline:
                    for key in recent_keys:
                        pipeline.hget(key, 'incremental_volume')
                    
                    results = pipeline.execute()
            except (redis.exceptions.MaxConnectionsError, redis.exceptions.ConnectionError) as e:
                logger.warning(f"⚠️ [VOLUME_MANAGER] Connection error getting recent volume for {symbol}, returning 0.0: {e}")
                return 0.0
            
            total_volume = 0.0
            count = 0
            
            for result in results:
                if result:
                    try:
                        total_volume += float(result)
                        count += 1
                    except (TypeError, ValueError):
                        continue
            
            return total_volume / count if count > 0 else 0.0
            
        except Exception as e:
            logger.debug(f"Error getting real-time average for {symbol}: {e}")
            return 0.0
    
    def _calculate_realtime_weight(self, timestamp: datetime) -> float:
        """
        Calculate how much weight to give real-time data vs historical.
        Higher weight during active trading hours.
        """
        try:
            hour = timestamp.hour
            
            if 9 <= hour <= 15:  # Trading hours (9:15 AM - 3:30 PM IST)
                return 0.7  # 70% weight to real-time data
            else:
                return 0.3  # 30% weight to real-time data
        except:
            return 0.5  # Default: equal weight
    
    def _calculate_real_time_ratio(self, tick_data: dict, baseline: float) -> float:
        """Calculate volume ratio with real-time validation."""
        try:
            incremental_volume = self._get_incremental_volume(tick_data)
            symbol = tick_data.get('symbol', 'UNKNOWN')
            
            # ✅ FIX: If incremental_volume is 0 but baseline exists, don't return 0.0
            # Instead, return 1.0 (neutral) or use last known volume_ratio from Redis
            if incremental_volume <= 0:
                if baseline and baseline > 0:
                    # Baseline exists but no current volume - check if we have historical volume_ratio
                    try:
                        from redis_files.redis_client import redis_gateway
                        if redis_gateway:
                            # Try to get last known volume_ratio from Redis
                            last_ratio = redis_gateway.get_indicator(symbol, 'volume_ratio')
                            if last_ratio and float(last_ratio) > 0:
                                logger.debug(
                                    f"✅ [VOLUME_DEBUG] {symbol}: current_volume=0, using last known volume_ratio={last_ratio} (real-time)"
                                )
                                return float(last_ratio)
                    except Exception:
                        pass
                    
                    # If no last known ratio, return 1.0 (neutral) instead of 0.0
                    logger.debug(
                        f"⚠️ [VOLUME_DEBUG] {symbol}: current_volume=0, baseline={baseline:.0f} exists, "
                        f"returning 1.0 (neutral) instead of 0.0 (real-time)"
                    )
                    return 1.0
                else:
                    # No baseline and no current volume
                    logger.debug(
                        f"❌ [VOLUME_DEBUG] {symbol}: current_volume=0, baseline=0, returning 0.0 (real-time)"
                    )
                    return 0.0
            
            if baseline and baseline > 0:
                raw_ratio = incremental_volume / baseline
                
                # Apply real-time sanity checks
                return self._apply_real_time_validation(raw_ratio, symbol, tick_data)
            else:
                # Emergency calculation using real-time data
                return self._emergency_real_time_ratio(tick_data)
                
        except Exception as e:
            logger.debug(f"Error calculating real-time ratio: {e}")
            return 1.0
    
    def _apply_real_time_validation(self, ratio: float, symbol: str, tick_data: dict) -> float:
        """Validate ratio against real-time market conditions."""
        try:
            if not self.redis or not symbol:
                # ✅ Use centralized volume profile logic via _reasonable_cap
                return self._reasonable_cap(ratio, symbol)
            
            # Get recent ratios for this symbol
            recent_ratios = self._get_recent_ratios(symbol)
            
            if recent_ratios and len(recent_ratios) > 5:
                try:
                    avg_recent = statistics.mean(recent_ratios)
                    std_recent = statistics.stdev(recent_ratios) if len(recent_ratios) > 1 else 0
                    
                    # Cap extreme outliers (beyond 3 standard deviations)
                    if std_recent > 0 and abs(ratio - avg_recent) > (3 * std_recent):
                        capped_ratio = avg_recent + (3 * std_recent * (1 if ratio > avg_recent else -1))
                        logger.warning(f"Capped extreme volume ratio for {symbol}: {ratio:.2f} -> {capped_ratio:.2f}")
                        # ✅ Use centralized volume profile logic via _reasonable_cap
                        return self._reasonable_cap(capped_ratio, symbol)
                except:
                    pass
            
            # ✅ Use centralized volume profile logic via _reasonable_cap (uses volume_profile_manager.get_volume_nodes())
            return self._reasonable_cap(ratio, symbol)
            
        except Exception as e:
            logger.debug(f"Error applying real-time validation: {e}")
            # ✅ Use centralized volume profile logic via _reasonable_cap even on error
            return self._reasonable_cap(ratio, symbol)
    
    def _get_recent_ratios(self, symbol: str) -> List[float]:
        """Get recent volume ratios from real-time data in unified database."""
        try:
            if not self.redis:
                return []
            
            # ✅ SOURCE OF TRUTH: Use canonical symbol and SCAN instead of KEYS
            canonical_symbol = RedisKeyStandards.canonical_symbol(symbol)
            pattern = f"realtime:{canonical_symbol}:*"
            # ✅ SOURCE OF TRUTH: Use SCAN instead of KEYS for efficiency
            keys = []
            cursor = 0
            while True:
                cursor, batch = self.redis.scan(cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            
            if not keys:
                return []
            
            # Use last 50 ticks
            recent_keys = keys[-50:] if len(keys) > 50 else keys
            
            ratios = []
            # ✅ CRITICAL FIX: self.redis is already the singleton client
            # Use context manager to ensure connection is released back to pool
            try:
                with self.redis.pipeline() as pipeline:
                    for key in recent_keys:
                        pipeline.hget(key, 'volume_ratio')
                    
                    results = pipeline.execute()
            except (redis.exceptions.MaxConnectionsError, redis.exceptions.ConnectionError) as e:
                logger.warning(f"⚠️ [VOLUME_MANAGER] Connection error getting recent ratios for {symbol}, returning empty list: {e}")
                return []
            
            for result in results:
                if result:
                    try:
                        ratios.append(float(result))
                    except (TypeError, ValueError):
                        continue
            
            return ratios
            
        except Exception as e:
            logger.debug(f"Error getting recent ratios for {symbol}: {e}")
            return []
    
    def _emergency_real_time_ratio(self, tick_data: dict) -> float:
        """Emergency ratio calculation when main logic fails."""
        try:
            volume = self._get_incremental_volume(tick_data)
            symbol = tick_data.get('symbol')
            
            if symbol:
                baseline = self._get_realtime_average(symbol)
                if baseline > 0:
                    return min(volume / baseline, 50.0)
            
            return 1.0
            
        except:
            return 1.0
    
    # ============================================================================
    # PHASE 3: Real-Time Adjustment Monitoring & Control
    # ============================================================================
    
    def enable_realtime_adjustments(self, enable: bool = True):
        """
        Enable or disable real-time baseline adjustments.
        
        Args:
            enable: If True, enable real-time adjustments (default: True)
        """
        self._enable_realtime_adjustments = enable
        status = "ENABLED" if enable else "DISABLED"
        logger.info(f"✅ [PHASE_3] Real-time volume adjustments {status}")
        
        if enable:
            logger.info("✅ Expected improvements:")
            logger.info("   • Volume ratios will reflect current market conditions")
            logger.info("   • No more 50.0 fallback values (when real-time data available)")
            logger.info("   • Dynamic baselines adapt to market volatility")
            logger.info("   • Pattern detection becomes more accurate")
    
    def get_realtime_metrics(self) -> Dict[str, Any]:
        """
        Get real-time adjustment metrics and statistics.
        
        Returns:
            Dict with metrics including:
            - total_calculations: Total volume ratio calculations
            - realtime_adjusted: Count using real-time baselines
            - historical_only: Count using historical baselines only
            - ratios_capped_at_50: Count of ratios hitting 50.0 cap
            - ratios_below_50: Count of ratios below 50.0
            - baseline_sources: Distribution of baseline sources
            - avg_volume_ratio: Average volume ratio
            - symbols_processed: Number of unique symbols
        """
        metrics = self._realtime_metrics.copy()
        metrics['symbols_processed'] = len(metrics['symbols_processed'])
        
        # Calculate average ratio
        if metrics['total_calculations'] > 0:
            # Estimate average (would need to track sum for exact)
            metrics['capped_percentage'] = (metrics['ratios_capped_at_50'] / metrics['total_calculations']) * 100
            metrics['realtime_percentage'] = (metrics['realtime_adjusted'] / metrics['total_calculations']) * 100
        else:
            metrics['capped_percentage'] = 0.0
            metrics['realtime_percentage'] = 0.0
        
        return metrics
    
    def _update_realtime_metrics(self, symbol: str, volume_ratio: float, baseline_source: str):
        """Update real-time adjustment metrics."""
        try:
            metrics = self._realtime_metrics
            metrics['total_calculations'] += 1
            metrics['symbols_processed'].add(symbol)
            
            # Track baseline source
            if baseline_source in metrics['baseline_sources']:
                metrics['baseline_sources'][baseline_source] += 1
            
            # Track real-time vs historical
            if baseline_source == 'real_time_adjusted':
                metrics['realtime_adjusted'] += 1
            else:
                metrics['historical_only'] += 1
            
            # Track ratio distribution
            if volume_ratio >= 50.0:
                metrics['ratios_capped_at_50'] += 1
            else:
                metrics['ratios_below_50'] += 1
            
            # Update average (simple moving average approximation)
            if metrics['total_calculations'] == 1:
                metrics['avg_volume_ratio'] = volume_ratio
            else:
                # Exponential moving average
                alpha = 0.01  # Smoothing factor
                metrics['avg_volume_ratio'] = (alpha * volume_ratio) + ((1 - alpha) * metrics['avg_volume_ratio'])
                
        except Exception as e:
            logger.debug(f"Error updating real-time metrics: {e}")
    
    def print_realtime_status(self):
        """Print current real-time adjustment status and metrics."""
        status = "ENABLED" if self._enable_realtime_adjustments else "DISABLED"
        print("=" * 70)
        print("REAL-TIME VOLUME ADJUSTMENT STATUS")
        print("=" * 70)
        print(f"Status: {status}")
        print()
        
        if self._enable_realtime_adjustments:
            print("✅ Real-time volume adjustments ENABLED")
            print("Expected improvements:")
            print("  ✅ Volume ratios will reflect current market conditions")
            print("  ✅ No more 50.0 fallback values (when real-time data available)")
            print("  ✅ Dynamic baselines adapt to market volatility")
            print("  ✅ Pattern detection becomes more accurate")
        else:
            print("⚠️  Real-time volume adjustments DISABLED")
            print("   Using historical baselines only")
        
        print()
        metrics = self.get_realtime_metrics()
        if metrics['total_calculations'] > 0:
            print("Metrics:")
            print(f"  Total calculations: {metrics['total_calculations']}")
            print(f"  Real-time adjusted: {metrics['realtime_adjusted']} ({metrics['realtime_percentage']:.1f}%)")
            print(f"  Historical only: {metrics['historical_only']}")
            print(f"  Ratios capped at 50.0: {metrics['ratios_capped_at_50']} ({metrics['capped_percentage']:.1f}%)")
            print(f"  Ratios below 50.0: {metrics['ratios_below_50']}")
            print(f"  Average volume ratio: {metrics['avg_volume_ratio']:.2f}")
            print(f"  Symbols processed: {metrics['symbols_processed']}")
            print()
            print("Baseline Sources:")
            for source, count in metrics['baseline_sources'].items():
                percentage = (count / metrics['total_calculations']) * 100 if metrics['total_calculations'] > 0 else 0
                print(f"  {source}: {count} ({percentage:.1f}%)")
        else:
            print("No metrics available yet (no calculations performed)")
        
        print("=" * 70)
    
    @staticmethod
    def validate_volume_data(tick_data: dict) -> Dict[str, bool]:
        """Validate that volume data is present and reasonable"""
        from utils.correct_volume_calculator import VolumeResolver
        validation = {
            'has_incremental': False,
            'has_cumulative': False,
            'has_ratio': False,
            'is_valid': False
        }
        
        try:
            incremental = VolumeResolver.get_incremental_volume(tick_data)
            validation['has_incremental'] = incremental > 0
            
            cumulative = VolumeResolver.get_cumulative_volume(tick_data)
            validation['has_cumulative'] = cumulative > 0
            
            ratio = VolumeResolver.get_volume_ratio(tick_data)
            validation['has_ratio'] = ratio > 0
            
            validation['is_valid'] = (validation['has_incremental'] and 
                                    validation['has_cumulative'])
        except Exception as e:
            pass
        
        return validation


# Global instance cache (per redis_client)
_volume_manager_cache: Dict[int, VolumeManager] = {}

def get_volume_manager(redis_client=None, token_resolver=None) -> VolumeManager:
    """
    Get volume manager instance with optional Redis client and token resolver.
    
    ✅ UNIFIED: Returns VolumeManager (combines VolumeStateManager + CorrectVolumeCalculator)
    
    If redis_client is provided, uses that client (avoids duplicate connections).
    If not provided, creates a new client using RedisManager82.get_client().
    
    Uses a cache keyed by redis_client id to avoid creating duplicate instances.
    """
    global _volume_manager_cache
    
    # ✅ FIX: If redis_client is provided but doesn't have get_client(), 
    # use redis_gateway for DB 1 (unified structure)
    if redis_client is not None and not hasattr(redis_client, 'get_client'):
        logger.debug(f"🔍 [VOLUME_MANAGER] Raw redis client provided, using redis_gateway for DB 1")
        try:
            from redis_files.redis_client import redis_gateway
            redis_client = redis_gateway  # Use unified DB 1 gateway
            logger.debug(f"🔍 [VOLUME_MANAGER] Using redis_gateway (DB 1 unified)")
        except Exception as e:
            logger.warning(f"⚠️ [VOLUME_MANAGER] Failed to use redis_gateway, falling back to RedisManager82: {e}")
            from redis_files.redis_client import RedisManager82
            redis_client = RedisManager82.get_client(
                process_name="volume_manager",
                db=1  # DB 1 unified
            )
    
    # If no redis_client provided, use redis_gateway for DB 1
    if redis_client is None:
        try:
            from redis_files.redis_client import redis_gateway
            redis_client = redis_gateway  # Use unified DB 1 gateway
            logger.debug(f"🔍 [VOLUME_MANAGER] Using default redis_gateway (DB 1 unified)")
        except Exception as e:
            logger.warning(f"⚠️ [VOLUME_MANAGER] Failed to use redis_gateway, falling back to RedisManager82: {e}")
            from redis_files.redis_client import RedisManager82
            redis_client = RedisManager82.get_client(
                process_name="volume_manager",
                db=1  # DB 1 unified
            )
    
    # Create cache key based on redis_client identity
    actual_client = redis_client
    if hasattr(redis_client, 'get_client'):
        try:
            actual_client = redis_client.get_client(0)
        except Exception:
            actual_client = redis_client
    
    cache_key = id(actual_client) if actual_client else 'default'
    
    # Return cached instance if available
    if cache_key in _volume_manager_cache:
        return _volume_manager_cache[cache_key]
    
    # Create new instance with provided client
    volume_manager = VolumeManager(redis_client, token_resolver)
    _volume_manager_cache[cache_key] = volume_manager
    
    return volume_manager
