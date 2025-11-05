"""
Redis Storage Layer - Enhanced with Indicator Publishing & Pattern Detection
Uses exact Zerodha field names from optimized_field_mapping.yaml

Redis Database Architecture (Consolidated):
- DB 0 (system): Session data, volume baselines, metadata
- DB 1 (realtime): Real-time streams (ticks, indicators, patterns, alerts, news, prices)
- DB 2 (analytics): Volume buckets, cumulative data, historical data, metrics

Volume Baseline Storage:
- Format: volume:baseline:SYMBOL:HH:MM (e.g., volume:baseline:BANKNIFTY:09:30)
- Data: Aggregated averages across multiple trading days
- Coverage: 288 time buckets (00:00-23:55) for 6,085+ symbols
- Usage: Pattern detection volume ratio calculations

MIGRATION NOTE: Replace direct redis.Redis(...) calls with get_redis_client(db=N)
Example:
  BEFORE: r = redis.Redis(host='localhost', port=6379, db=1)
  AFTER:  from redis_files.redis_client import get_redis_client
          r = get_redis_client()  # Uses singleton with RESP3 + retry/backoff
          # Or for specific DB: r = get_redis_client().get_client(1)
"""

import json
import time
import asyncio
import logging
from typing import Dict, List, Any, Optional
from config.utils.timestamp_normalizer import TimestampNormalizer
from redis_files.redis_client import RobustRedisClient
from utils.yaml_field_loader import (
    get_field_mapping_manager,
    resolve_session_field,
)

# Import TA-Lib calculations and ICT patterns
try:
    from intraday_scanner.calculations import HybridCalculations
    from patterns.ict import (
        ICTPatternDetector,
        ICTLiquidityDetector,
        ICTFVGDetector,
        ICTOTECalculator,
        ICTPremiumDiscountDetector,
        ICTKillzoneDetector,
    )
    from patterns.mm_exploitation_strategies import MMExploitationStrategies
    from paper_trading.range_bound_analyzer import RangeBoundAnalyzer
    from patterns.market_maker_trap_detector import MarketMakerTrapDetector
    from patterns.pattern_detector import PatternDetector
    
    # Import pattern registry configuration
    import json
    from pathlib import Path
    try:
        # Path from redis_files/ to patterns/data/ (go up one level to project root)
        _pattern_registry_path = Path(__file__).parent.parent / "patterns" / "data" / "pattern_registry_config.json"
        with open(_pattern_registry_path, 'r') as f:
            PATTERN_REGISTRY = json.load(f)
        PATTERN_REGISTRY_AVAILABLE = True
    except FileNotFoundError:
        PATTERN_REGISTRY = {}
        PATTERN_REGISTRY_AVAILABLE = False
        logging.warning("Pattern registry config file not found")
    
    CALCULATIONS_AVAILABLE = True
except ImportError as e:
    CALCULATIONS_AVAILABLE = False
    PATTERN_REGISTRY = {}
    PATTERN_REGISTRY_AVAILABLE = False
    logging.warning(f"ICT patterns not available: {e}")
except FileNotFoundError:
    CALCULATIONS_AVAILABLE = False
    PATTERN_REGISTRY = {}
    PATTERN_REGISTRY_AVAILABLE = False
    logging.warning("Pattern registry config not found")

logger = logging.getLogger(__name__)

# --- High-performance Redis Streams (Consumer Groups) ---
try:
    import redis.asyncio as aioredis  # Redis 8.2 async client
    import orjson  # ultra-fast JSON
    _ASYNC_STREAMS_AVAILABLE = True
except Exception:
    _ASYNC_STREAMS_AVAILABLE = False

# --- RedisJSON + RediSearch for instant state queries ---
try:
    import redis.asyncio as aioredis_json
    import orjson as _orjson
    _JSON_SEARCH_AVAILABLE = True
except Exception:
    _JSON_SEARCH_AVAILABLE = False


class HighPerformanceAlertStream:
    """Async Redis Streams publisher/consumer using consumer groups.

    Usage gated via _ASYNC_STREAMS_AVAILABLE and optional runtime choice.
    Does not alter existing storage flows.
    """

    def __init__(self, host: str = 'localhost', port: int = 6379,
                 stream_key: str = 'alerts:stream', consumer_group: str = 'dashboard_consumers'):
        if not _ASYNC_STREAMS_AVAILABLE:
            raise RuntimeError("Async Redis streams not available (redis.asyncio/orjson missing)")
        self.redis = aioredis.Redis(host=host, port=port, decode_responses=False, health_check_interval=30)
        self.stream_key = stream_key
        self.consumer_group = consumer_group

    async def setup_streams(self) -> None:
        """Create stream and consumer group if not exists."""
        try:
            await self.redis.xgroup_create(self.stream_key, self.consumer_group, "$", mkstream=True)
        except Exception:
            # Group already exists
            pass

    async def publish_alert(self, alert_data: dict) -> None:
        """Ultra-fast alert publishing using binary JSON."""
        # Ensure alert_id exists for frontend compatibility
        if 'alert_id' not in alert_data or not alert_data.get('alert_id'):
            import time as time_module
            timestamp_ms = alert_data.get('timestamp_ms') or int(alert_data.get('timestamp', time_module.time()) * 1000)
            symbol = alert_data.get('symbol', 'UNKNOWN')
            alert_data['alert_id'] = f"{symbol}_{timestamp_ms}"
        
        binary_data = orjson.dumps(alert_data)
        await self.redis.xadd(
            self.stream_key,
            {b"data": binary_data},
            maxlen=10000,
            approximate=True,
        )

    async def consume_alerts(self, consumer_name: str):
        """High-performance consumption with small batches and short block."""
        import asyncio
        while True:
            try:
                messages = await self.redis.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=consumer_name,
                    streams={self.stream_key: ">"},
                    count=10,
                    block=100,
                )
                for _stream, message_list in messages or []:
                    for message_id, fields in message_list:
                        binary_data = fields.get(b"data") or fields.get("data")
                        if binary_data is None:
                            continue
                        alert = orjson.loads(binary_data)
                        yield alert
                        await self.redis.xack(self.stream_key, self.consumer_group, message_id)
            except Exception:
                await asyncio.sleep(0.1)


class RedisStorage:
    """
    Enhanced Redis Storage with Indicator Publishing & Pattern Detection
    Uses exact Zerodha field names from optimized_field_mapping.yaml
    """
    
    def __init__(self, redis_client: RobustRedisClient, hybrid_calculations: HybridCalculations | None = None):
        self.redis = redis_client
        self.field_mapping_manager = get_field_mapping_manager()
        
        # Initialize TA-Lib calculations and ICT pattern detectors
        if CALCULATIONS_AVAILABLE:
            self.hybrid_calculations = hybrid_calculations or HybridCalculations()
            self.hybrid_calculations.redis_client = redis_client
            
            # Initialize all 8 ICT pattern detectors
            self.ict_pattern_detector = ICTPatternDetector(redis_client)
            self.ict_liquidity_detector = ICTLiquidityDetector(redis_client)
            self.ict_fvg_detector = ICTFVGDetector()
            self.ict_ote_calculator = ICTOTECalculator()
            self.ict_premium_discount_detector = ICTPremiumDiscountDetector(redis_client)
            self.ict_killzone_detector = ICTKillzoneDetector()
            
            # Initialize straddle strategy detectors
            self.mm_exploitation_strategies = MMExploitationStrategies(redis_client)
            self.range_bound_analyzer = RangeBoundAnalyzer(data_source="redis")
            
            # Initialize Kow Signal Straddle strategy
            from patterns.kow_signal_straddle import VWAPStraddleStrategy
            self.kow_signal = VWAPStraddleStrategy(redis_client)
            
            # Initialize additional pattern detectors
            self.market_maker_trap_detector = MarketMakerTrapDetector(redis_client)
            # Initialize expected move calculator first
            # expected_move_calculator = ExpectedMoveCalculator(redis_client)
            # self.premium_collection_strategy = PremiumCollectionStrategy(redis_client, expected_move_calculator)
            
            # Initialize core pattern detector (8 core patterns from registry)
            self.core_pattern_detector = PatternDetector(redis_client=redis_client)
            
            # Complete pattern detectors list with ALL strategies (11 total)
            self.pattern_detectors = [
                # ICT Patterns (6)
                self.ict_pattern_detector,
                self.ict_liquidity_detector,
                self.ict_fvg_detector,
                self.ict_ote_calculator,
                self.ict_premium_discount_detector,
                self.ict_killzone_detector,
                # Straddle Strategies (3)
                self.mm_exploitation_strategies,
                self.range_bound_analyzer,
                self.kow_signal,
                # Additional Pattern Detectors (1)
                self.market_maker_trap_detector,
                # Core Pattern Detector (1) - 8 patterns from registry
                self.core_pattern_detector
            ]
        else:
            self.hybrid_calculations = hybrid_calculations
            self.pattern_detectors = []
        
        # Resolve canonical field names from optimized_field_mapping.yaml
        self.FIELD_LAST_PRICE = resolve_session_field("last_price")
        self.FIELD_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_ZERODHA_CUMULATIVE_VOLUME = resolve_session_field("zerodha_cumulative_volume")
        self.FIELD_ZERODHA_LAST_TRADED_QUANTITY = resolve_session_field("zerodha_last_traded_quantity")
        self.FIELD_BUCKET_INCREMENTAL_VOLUME = resolve_session_field("bucket_incremental_volume")
        self.FIELD_BUCKET_CUMULATIVE_VOLUME = resolve_session_field("bucket_cumulative_volume")
        self.FIELD_HIGH = resolve_session_field("high")
        self.FIELD_LOW = resolve_session_field("low")
        self.FIELD_SESSION_DATE = resolve_session_field("session_date")
        self.FIELD_UPDATE_COUNT = resolve_session_field("update_count")
        self.FIELD_LAST_UPDATE_TIMESTAMP = resolve_session_field("last_update_timestamp")
        self.FIELD_FIRST_UPDATE = resolve_session_field("first_update")
        self.FIELD_AVERAGE_PRICE = resolve_session_field("average_price")
        
        # Pattern registry integration
        self.pattern_registry = PATTERN_REGISTRY if PATTERN_REGISTRY_AVAILABLE else {}
        self.registry_available = PATTERN_REGISTRY_AVAILABLE
        
        # Try to load pattern registry if not already loaded
        if not self.registry_available:
            try:
                import json
                from pathlib import Path
                _pattern_registry_path = Path(__file__).parent.parent.parent / "patterns" / "data" / "pattern_registry_config.json"
                with open(_pattern_registry_path, 'r') as f:
                    self.pattern_registry = json.load(f)
                self.registry_available = True
                logger.info("Pattern registry loaded successfully")
            except Exception as e:
                logger.warning(f"Failed to load pattern registry: {e}")
                self.pattern_registry = {}
                self.registry_available = False
        self.FIELD_OHLC = resolve_session_field("ohlc")
        self.FIELD_NET_CHANGE = resolve_session_field("net_change")
    
    async def store_tick(self, symbol: str, tick_data: dict):
        """Store normalized tick data using Zerodha field names"""
        # Resolve token to symbol BEFORE storing
        symbol = self._resolve_token_to_symbol_for_storage(symbol)
        
        # Normalize timestamp FIRST using Zerodha field names
        normalized_tick = tick_data.copy()
        
        # Zerodha timestamp priority: exchange_timestamp > timestamp_ns > timestamp
        if 'exchange_timestamp' in tick_data and tick_data['exchange_timestamp']:
            normalized_tick['exchange_timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['exchange_timestamp'])
        elif 'timestamp_ns' in tick_data and tick_data['timestamp_ns']:
            normalized_tick['timestamp_ns_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp_ns'])
        elif 'timestamp' in tick_data and tick_data['timestamp']:
            normalized_tick['timestamp_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['timestamp'])
        
        # Last trade time - Zerodha specific field
        if 'last_trade_time' in tick_data and tick_data['last_trade_time']:
            normalized_tick['last_trade_time_ms'] = TimestampNormalizer.to_epoch_ms(tick_data['last_trade_time'])
        
        # Store in Redis Streams using Zerodha field names in DB 1 (realtime)
        stream_key = f"ticks:{symbol}"
        # Use process-specific client if available
        try:
            from redis_files.redis_manager import RedisManager82
            # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
            realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
        except Exception:
            # Fallback to existing method
            realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
        
        # Convert dict to proper stream format
        stream_data = {
            'data': json.dumps(normalized_tick, default=str),
            'timestamp': str(int(time.time() * 1000)),
            'symbol': symbol
        }
        # ✅ FIXED: ALWAYS use maxlen to prevent unbounded stream growth
        realtime_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)
    
    async def get_recent_ticks(self, symbol: str, count: int = 100) -> List[dict]:
        """Get recent ticks - already normalized timestamps using Zerodha field names"""
        stream_key = f"ticks:{symbol}"
        # Use process-specific client if available
        try:
            from redis_files.redis_manager import RedisManager82
            # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
            realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
        except Exception:
            # Fallback to existing method
            realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
        ticks = realtime_client.xrevrange(stream_key, count=count)
        return [tick_data for tick_id, tick_data in ticks]
    
    # ============ Enhanced Indicator Publishing & Pattern Detection ============
    
    # ============ TA-Lib Indicator Publishing System ============
    
    async def publish_indicator(self, symbol: str, indicator_type: str, data: Dict):
        """Publish indicator to Redis stream for real-time consumption"""
        if not self.redis or not self.hybrid_calculations:
            return
            
        try:
            stream_key = f"indicators:{indicator_type}:{symbol}"
            
            message = {
                'symbol': symbol,
                'type': indicator_type,
                'timestamp': int(time.time() * 1000),
                'data': json.dumps(data),
                'pattern_ready': 'true',  # Convert boolean to string
                'source': 'redis_storage_enhanced'
            }
            
            # Publish to DB 1 (realtime) for indicators
            # Use process-specific client if available
            try:
                from redis_files.redis_manager import RedisManager82
                # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
                realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
            except Exception:
                # Fallback to existing method
                realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
            # ✅ FIXED: ALWAYS use maxlen to prevent unbounded stream growth
            realtime_client.xadd(stream_key, message, maxlen=5000, approximate=True)
            
            # Also publish to global indicator stream
            realtime_client.xadd(f"indicators:{indicator_type}:global", message, maxlen=5000, approximate=True)
            
            logger.debug(f"Published {indicator_type} for {symbol} to {stream_key}")
            
        except Exception as e:
            logger.error(f"Failed to publish indicator {indicator_type} for {symbol}: {e}")
    
    async def calculate_and_publish_rsi(self, symbol: str, prices: List[float], period: int = 14):
        """Calculate RSI and publish to stream"""
        if not self.hybrid_calculations:
            return 50.0
            
        rsi_value = self.hybrid_calculations.calculate_rsi(prices, period)
        
        rsi_data = {
            'value': rsi_value,
            'period': period,
            'levels': {
                'oversold': 30,
                'overbought': 70
            },
            'signal': 'OVERSOLD' if rsi_value <= 30 else 'OVERBOUGHT' if rsi_value >= 70 else 'NEUTRAL'
        }
        
        await self.publish_indicator(symbol, 'rsi', rsi_data)
        return rsi_value
    
    async def calculate_and_publish_ema(self, symbol: str, prices: List[float], period: int = 20):
        """Calculate EMA and publish to stream"""
        if not self.hybrid_calculations:
            return prices[-1] if prices else 0.0
            
        ema_value = self.hybrid_calculations.calculate_ema(prices, period)
        
        # Publish individual EMA values for scanner compatibility
        await self.publish_indicator(symbol, f'ema_{period}', ema_value)
        
        ema_data = {
            'value': ema_value,
            'period': period,
            'trend': 'BULLISH' if prices[-1] > ema_value else 'BEARISH' if prices[-1] < ema_value else 'NEUTRAL'
        }
        
        await self.publish_indicator(symbol, 'ema', ema_data)
        return ema_value
    
    async def calculate_and_publish_macd(self, symbol: str, prices: List[float]):
        """Calculate MACD and publish to stream"""
        if not self.hybrid_calculations:
            return {}
            
        macd_data = self.hybrid_calculations.calculate_macd(prices)
        
        if isinstance(macd_data, dict):
            macd_value = macd_data.get('macd', 0)
            signal_value = macd_data.get('signal', 0)
            histogram_value = macd_data.get('histogram', 0)
            crossover = 'BULLISH' if macd_value > signal_value else 'BEARISH' if macd_value < signal_value else 'NONE'
            
            # Publish individual MACD components for scanner compatibility
            await self.publish_indicator(symbol, 'macd_signal', signal_value)
            await self.publish_indicator(symbol, 'macd_histogram', histogram_value)
            await self.publish_indicator(symbol, 'macd_crossover', crossover)
            
            macd_publish_data = {
                'macd': macd_value,
                'signal': signal_value,
                'histogram': histogram_value,
                'crossover': crossover
            }
        else:
            macd_publish_data = {'macd': macd_data, 'signal': 0, 'histogram': 0, 'crossover': 'NONE'}
            # Publish individual components even for fallback
            await self.publish_indicator(symbol, 'macd_signal', 0)
            await self.publish_indicator(symbol, 'macd_histogram', 0)
            await self.publish_indicator(symbol, 'macd_crossover', 'NONE')
        
        await self.publish_indicator(symbol, 'macd', macd_publish_data)
        return macd_data
    
    async def calculate_and_publish_bollinger_bands(self, symbol: str, prices: List[float]):
        """Calculate Bollinger Bands and publish to stream"""
        if not self.hybrid_calculations:
            return {}
            
        bb_data = self.hybrid_calculations.calculate_bollinger_bands(prices)
        
        if isinstance(bb_data, dict):
            current_price = prices[-1] if prices else 0
            upper = bb_data.get('upper', 0)
            middle = bb_data.get('middle', 0)
            lower = bb_data.get('lower', 0)
            
            # Determine position and signal
            if current_price > upper:
                position = 'UPPER'
                signal = 'OVERBOUGHT'
            elif current_price < lower:
                position = 'LOWER'
                signal = 'OVERSOLD'
            else:
                position = 'MIDDLE'
                signal = 'NEUTRAL'
            
            # Publish individual BB components for scanner compatibility
            await self.publish_indicator(symbol, 'bb_upper', upper)
            await self.publish_indicator(symbol, 'bb_middle', middle)
            await self.publish_indicator(symbol, 'bb_lower', lower)
            await self.publish_indicator(symbol, 'bb_position', position)
            await self.publish_indicator(symbol, 'bb_signal', signal)
            
            bb_publish_data = {
                'upper': upper,
                'middle': middle,
                'lower': lower,
                'position': position,
                'signal': signal
            }
        else:
            bb_publish_data = {'upper': 0, 'middle': 0, 'lower': 0, 'position': 'UNKNOWN', 'signal': 'NONE'}
            # Publish individual components even for fallback
            await self.publish_indicator(symbol, 'bb_upper', 0)
            await self.publish_indicator(symbol, 'bb_middle', 0)
            await self.publish_indicator(symbol, 'bb_lower', 0)
            await self.publish_indicator(symbol, 'bb_position', 'UNKNOWN')
            await self.publish_indicator(symbol, 'bb_signal', 'NONE')
        
        await self.publish_indicator(symbol, 'bollinger_bands', bb_publish_data)
        return bb_data
    
    async def calculate_and_publish_missing_indicators(self, symbol: str, tick_data: List[Dict]):
        """Calculate and publish missing indicators that scanner expects"""
        try:
            if not tick_data:
                return {}
            
            latest_tick = tick_data[-1] if tick_data else {}
            prices = [float(tick.get('last_price', 0)) for tick in tick_data]
            
            if not prices:
                return {}
            
            current_price = prices[-1]
            
            # Calculate RSI signal
            rsi_value = latest_tick.get('rsi', 50)
            if rsi_value <= 30:
                rsi_signal = 'OVERSOLD'
            elif rsi_value >= 70:
                rsi_signal = 'OVERBOUGHT'
            else:
                rsi_signal = 'NEUTRAL'
            await self.publish_indicator(symbol, 'rsi_signal', rsi_signal)
            
            # Calculate momentum (price change over last 5 periods)
            if len(prices) >= 5:
                momentum = ((current_price - prices[-5]) / prices[-5]) * 100 if prices[-5] > 0 else 0
            else:
                momentum = 0
            await self.publish_indicator(symbol, 'momentum', momentum)
            
            # Calculate volatility (standard deviation of returns)
            if len(prices) >= 20:
                returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices)) if prices[i-1] > 0]
                if returns:
                    import statistics
                    volatility = statistics.stdev(returns) * 100  # Convert to percentage
                else:
                    volatility = 0
            else:
                volatility = 0
            await self.publish_indicator(symbol, 'volatility', volatility)
            
            # Calculate support and resistance (simplified)
            if len(prices) >= 20:
                recent_prices = prices[-20:]
                support = min(recent_prices)
                resistance = max(recent_prices)
            else:
                support = current_price * 0.95  # 5% below current
                resistance = current_price * 1.05  # 5% above current
            
            await self.publish_indicator(symbol, 'support', support)
            await self.publish_indicator(symbol, 'resistance', resistance)
            
            # Calculate price change (from previous close)
            if len(prices) >= 2:
                price_change = ((current_price - prices[-2]) / prices[-2]) * 100 if prices[-2] > 0 else 0
            else:
                price_change = 0
            await self.publish_indicator(symbol, 'price_change', price_change)
            
            return {
                'rsi_signal': rsi_signal,
                'momentum': momentum,
                'volatility': volatility,
                'support': support,
                'resistance': resistance,
                'price_change': price_change
            }
            
        except Exception as e:
            logger.error(f"Missing indicators calculation failed for {symbol}: {e}")
            return {}
    
    async def calculate_and_publish_expected_move(self, symbol: str, pattern_type: str, entry_price: float, indicators: Dict):
        """Calculate expected move using PatternDetector and publish to Redis"""
        try:
            if not self.core_pattern_detector:
                return {}
            
            # Use PatternDetector's expected move calculation
            expected_move = self.core_pattern_detector.calculate_expected_move(
                pattern_type=pattern_type,
                entry_price=entry_price,
                indicators=indicators
            )
            
            # Publish individual expected move components
            await self.publish_indicator(symbol, 'expected_move_points', expected_move.get('points', 0))
            await self.publish_indicator(symbol, 'expected_move_percent', expected_move.get('percent', 0))
            await self.publish_indicator(symbol, 'expected_move_direction', expected_move.get('direction', 'UNKNOWN'))
            await self.publish_indicator(symbol, 'expected_move_timeframe', expected_move.get('timeframe', '60min'))
            await self.publish_indicator(symbol, 'expected_move_confidence', expected_move.get('confidence', 0.5))
            
            return expected_move
            
        except Exception as e:
            logger.error(f"Expected move calculation failed for {symbol}: {e}")
            return {}
    
    async def calculate_and_publish_atr(self, symbol: str, high: List[float], low: List[float], close: List[float], period: int = 14):
        """Calculate ATR and publish to stream"""
        if not self.hybrid_calculations:
            return 0.0
            
        atr_value = self.hybrid_calculations.calculate_atr(high, low, close, period)
        
        atr_data = {
            'value': atr_value,
            'period': period,
            'volatility': 'HIGH' if atr_value > 2.0 else 'MEDIUM' if atr_value > 1.0 else 'LOW'
        }
        
        await self.publish_indicator(symbol, 'atr', atr_data)
        return atr_value
    
    async def calculate_and_publish_vwap(self, symbol: str, tick_data: List[dict]):
        """Calculate VWAP and publish to stream"""
        if not self.hybrid_calculations or not tick_data:
            return 0.0
            
        # Extract price and volume data
        prices = [float(tick.get('last_price', 0)) for tick in tick_data]
        volumes = [float(tick.get('bucket_incremental_volume', 0)) for tick in tick_data]
        
        vwap_value = self.hybrid_calculations.calculate_vwap(prices, volumes)
        
        vwap_data = {
            'value': vwap_value,
            'price_above_vwap': prices[-1] > vwap_value if prices else False,
            'premium_discount': 'PREMIUM' if prices[-1] > vwap_value else 'DISCOUNT' if prices[-1] < vwap_value else 'FAIR'
        }
        
        await self.publish_indicator(symbol, 'vwap', vwap_data)
        return vwap_value
    
    async def calculate_and_publish_volume_indicators(self, symbol: str, tick_data: List[dict]):
        """Calculate volume-based indicators and publish to stream"""
        if not tick_data:
            return {}
            
        # Extract volume data - use bucket_incremental_volume as primary method
        volumes = [float(tick.get('bucket_incremental_volume', 0)) for tick in tick_data]
        cumulative_volumes = [float(tick.get('zerodha_cumulative_volume', 0)) for tick in tick_data]
        
        # Calculate volume ratio
        if len(volumes) >= 2:
            current_volume = volumes[-1]
            previous_volume = volumes[-2] if len(volumes) > 1 else volumes[-1]
            volume_ratio = (current_volume - previous_volume) / previous_volume if previous_volume > 0 else 0
        else:
            volume_ratio = 0
        
        # Calculate volume trend
        avg_volume = sum(volumes) / len(volumes) if volumes else 0
        current_incremental = volumes[-1] if volumes else 0
        volume_trend = 'ACCUMULATION' if current_incremental > avg_volume * 1.5 else 'DISTRIBUTION' if current_incremental < avg_volume * 0.5 else 'NORMAL'
        
        volume_data = {
            'volume_ratio': volume_ratio,
            'current_volume': current_volume,
            'incremental_volume': current_incremental,
            'volume_trend': volume_trend,
            'volume_signal': 'HIGH' if volume_ratio > 2.0 else 'MEDIUM' if volume_ratio > 1.0 else 'LOW'
        }
        
        await self.publish_indicator(symbol, 'volume', volume_data)
        return volume_data
    
    def _is_option_symbol(self, symbol: str) -> bool:
        """Check if symbol is an F&O option"""
        if not symbol:
            return False
        symbol_upper = symbol.upper()
        # Check for NFO segment and option indicators (CE/PE) followed by numeric value
        import re
        return (symbol_upper.startswith('NFO:') and 
                re.search(r'(CE|PE)\d+$', symbol_upper))
    
    async def calculate_and_publish_fno_basic_data(self, symbol: str, tick_data: List[Dict]):
        """Calculate and publish basic F&O data (strike_price, underlying_price, option_type, etc.)"""
        try:
            if not tick_data:
                return {}
            
            # Get latest tick data
            latest_tick = tick_data[-1] if tick_data else {}
            
            # Extract basic F&O data from tick
            fno_data = {}
            
            # Basic F&O fields
            if latest_tick.get('strike_price'):
                fno_data['strike_price'] = float(latest_tick.get('strike_price', 0))
                await self.publish_indicator(symbol, 'strike_price', fno_data['strike_price'])
            
            if latest_tick.get('underlying_price'):
                fno_data['underlying_price'] = float(latest_tick.get('underlying_price', 0))
                await self.publish_indicator(symbol, 'underlying_price', fno_data['underlying_price'])
            
            if latest_tick.get('option_type'):
                fno_data['option_type'] = str(latest_tick.get('option_type', ''))
                await self.publish_indicator(symbol, 'option_type', fno_data['option_type'])
            
            if latest_tick.get('days_to_expiry'):
                fno_data['days_to_expiry'] = int(latest_tick.get('days_to_expiry', 0))
                await self.publish_indicator(symbol, 'days_to_expiry', fno_data['days_to_expiry'])
            
            # Calculate moneyness if we have strike and underlying
            if fno_data.get('strike_price') and fno_data.get('underlying_price'):
                moneyness = fno_data['underlying_price'] / fno_data['strike_price']
                fno_data['moneyness'] = moneyness
                await self.publish_indicator(symbol, 'moneyness', moneyness)
            
            # Calculate OI change if available
            if latest_tick.get('oi') and latest_tick.get('oi_day_low'):
                oi_change = latest_tick.get('oi', 0) - latest_tick.get('oi_day_low', 0)
                fno_data['oi_change'] = oi_change
                await self.publish_indicator(symbol, 'oi_change', oi_change)
            
            # Calculate OI volume ratio if available
            if latest_tick.get('oi') and latest_tick.get('zerodha_cumulative_volume'):
                oi_volume_ratio = latest_tick.get('oi', 0) / max(latest_tick.get('zerodha_cumulative_volume', 1), 1)
                fno_data['oi_volume_ratio'] = oi_volume_ratio
                await self.publish_indicator(symbol, 'oi_volume_ratio', oi_volume_ratio)
            
            # Determine build_up based on OI and volume
            if latest_tick.get('oi') and latest_tick.get('zerodha_cumulative_volume'):
                oi = latest_tick.get('oi', 0)
                volume = latest_tick.get('zerodha_cumulative_volume', 0)
                if oi > volume * 1.5:
                    build_up = 'LONG_BUILDUP'
                elif oi < volume * 0.5:
                    build_up = 'SHORT_BUILDUP'
                else:
                    build_up = 'NEUTRAL'
                fno_data['build_up'] = build_up
                await self.publish_indicator(symbol, 'build_up', build_up)
            
            # Calculate implied volatility if we have enough data
            if (fno_data.get('strike_price') and fno_data.get('underlying_price') and 
                fno_data.get('days_to_expiry') and latest_tick.get('last_price')):
                try:
                    # Simple IV calculation using Black-Scholes inverse
                    # This is a simplified version - in production, use proper IV calculation
                    spot = fno_data['underlying_price']
                    strike = fno_data['strike_price']
                    time_to_expiry = fno_data['days_to_expiry'] / 365.0
                    option_price = float(latest_tick.get('last_price', 0))
                    risk_free_rate = 0.05  # 5% risk-free rate
                    
                    if time_to_expiry > 0 and option_price > 0:
                        # Simplified IV calculation (this is a placeholder - use proper IV solver)
                        # For now, use a basic approximation
                        moneyness = spot / strike
                        if 0.8 <= moneyness <= 1.2:  # Near the money
                            implied_vol = 0.2 + (abs(moneyness - 1.0) * 0.1)  # 20-30% IV range
                        else:
                            implied_vol = 0.15 + (abs(moneyness - 1.0) * 0.05)  # 15-25% IV range
                        
                        fno_data['implied_volatility'] = implied_vol
                        await self.publish_indicator(symbol, 'implied_volatility', implied_vol)
                except Exception as iv_error:
                    logger.debug(f"IV calculation failed for {symbol}: {iv_error}")
            
            # Publish combined F&O data
            if fno_data:
                await self.publish_indicator(symbol, 'fno_basic', fno_data)
            
            return fno_data
            
        except Exception as e:
            logger.error(f"F&O basic data calculation failed for {symbol}: {e}")
            return {}

    async def calculate_and_publish_greeks(self, symbol: str, tick_data: List[Dict]):
        """Calculate and publish Greek indicators for F&O options"""
        try:
            if not tick_data:
                return {}
            
            # Get latest tick data for Greek calculations
            latest_tick = tick_data[-1] if tick_data else {}
            
            # Import Greek calculator
            from intraday_scanner.calculations import greek_calculator
            
            # Calculate Greeks
            greeks = greek_calculator.calculate_greeks_for_tick_data(latest_tick)
            
            if greeks and any(greeks.get(greek, 0) != 0 for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']):
                # Publish individual Greeks
                await self.publish_indicator(symbol, 'delta', greeks.get('delta', 0.0))
                await self.publish_indicator(symbol, 'gamma', greeks.get('gamma', 0.0))
                await self.publish_indicator(symbol, 'theta', greeks.get('theta', 0.0))
                await self.publish_indicator(symbol, 'vega', greeks.get('vega', 0.0))
                await self.publish_indicator(symbol, 'rho', greeks.get('rho', 0.0))
                
                # Publish combined Greeks data
                greeks_data = {
                    'delta': greeks.get('delta', 0.0),
                    'gamma': greeks.get('gamma', 0.0),
                    'theta': greeks.get('theta', 0.0),
                    'vega': greeks.get('vega', 0.0),
                    'rho': greeks.get('rho', 0.0),
                    'dte_years': greeks.get('dte_years', 0.0),
                    'trading_dte': greeks.get('trading_dte', 0),
                    'expiry_series': greeks.get('expiry_series', 'UNKNOWN')
                }
                
                await self.publish_indicator(symbol, 'greeks', greeks_data)
                return greeks_data
            
            return {}
            
        except Exception as e:
            logger.error(f"Greek calculation failed for {symbol}: {e}")
            return {}
    
    async def calculate_and_publish_volume_profile(self, symbol: str, tick_data: List[Dict]):
        """
        Calculate and publish volume profile indicators using standardized Redis streams
        
        Redis Stream Patterns:
        - indicators:poc_price:SYMBOL → POC price indicator stream
        - indicators:poc_volume:SYMBOL → POC volume indicator stream
        - indicators:value_area_high:SYMBOL → Value Area high indicator stream
        - indicators:value_area_low:SYMBOL → Value Area low indicator stream
        - indicators:profile_strength:SYMBOL → Profile strength indicator stream
        - indicators:support_levels:SYMBOL → Support levels indicator stream
        - indicators:resistance_levels:SYMBOL → Resistance levels indicator stream
        """
        try:
            # Get volume profile data from VolumeStateManager
            from redis_files.volume_state_manager import get_volume_manager
            
            volume_manager = get_volume_manager()
            profile_data = volume_manager.get_volume_profile_data(symbol)
            
            if not profile_data:
                return {}
            
            # Publish volume profile indicators using standardized Redis streams
            volume_profile_indicators = {
                'poc_price': profile_data.get('poc_price'),
                'poc_volume': profile_data.get('poc_volume'),
                'value_area_high': profile_data.get('value_area_high'),
                'value_area_low': profile_data.get('value_area_low'),
                'profile_strength': profile_data.get('profile_strength'),
                'support_levels': profile_data.get('support_levels', []),
                'resistance_levels': profile_data.get('resistance_levels', [])
            }
            
            # Publish each indicator individually using standardized indicator streams
            published_indicators = {}
            for indicator_name, value in volume_profile_indicators.items():
                if value is not None:
                    # Create indicator data structure following system standards
                    indicator_data = {
                        'value': value,
                        'symbol': symbol,
                        'timestamp': int(time.time() * 1000),
                        'source': 'volume_profile_manager',
                        'data_type': 'volume_profile'
                    }
                    
                    await self.publish_indicator(symbol, indicator_name, indicator_data)
                    published_indicators[indicator_name] = value
            
            logger.debug(f"Published volume profile indicators for {symbol}: {list(published_indicators.keys())}")
            return published_indicators
            
        except Exception as e:
            logger.error(f"Error calculating volume profile for {symbol}: {e}")
            return {}
    
    async def batch_publish_all_indicators(self, symbol: str, tick_data: List[Dict]):
        """Batch calculate and publish ALL indicators for comprehensive pattern detection"""
        if not tick_data or len(tick_data) < 20:
            return {}
        
        # Extract price data
        prices = [float(tick.get('last_price', 0)) for tick in tick_data]
        highs = [float(tick.get('high', tick.get('last_price', 0))) for tick in tick_data]
        lows = [float(tick.get('low', tick.get('last_price', 0))) for tick in tick_data]
        closes = prices  # Use last_price as close
        
        # Calculate and publish all indicators concurrently
        tasks = [
            self.calculate_and_publish_rsi(symbol, prices),
            # ✅ ALL EMA WINDOWS: Add all EMA periods for comprehensive coverage
            self.calculate_and_publish_ema(symbol, prices, 5),
            self.calculate_and_publish_ema(symbol, prices, 10),
            self.calculate_and_publish_ema(symbol, prices, 20),
            self.calculate_and_publish_ema(symbol, prices, 50),
            self.calculate_and_publish_ema(symbol, prices, 100),
            self.calculate_and_publish_ema(symbol, prices, 200),
            self.calculate_and_publish_macd(symbol, prices),
            self.calculate_and_publish_bollinger_bands(symbol, prices),
            self.calculate_and_publish_atr(symbol, highs, lows, closes),
            self.calculate_and_publish_vwap(symbol, tick_data),
            self.calculate_and_publish_volume_indicators(symbol, tick_data),
            self.calculate_and_publish_missing_indicators(symbol, tick_data),
            self.calculate_and_publish_volume_profile(symbol, tick_data)  # NEW: Volume Profile Integration
        ]
        
        # 🎯 ADD F&O CALCULATIONS FOR OPTIONS
        if self._is_option_symbol(symbol):
            tasks.append(self.calculate_and_publish_fno_basic_data(symbol, tick_data))
            tasks.append(self.calculate_and_publish_greeks(symbol, tick_data))
        
        # 🎯 ADD EXPECTED MOVE CALCULATION FOR ALL SYMBOLS
        if self.core_pattern_detector and tick_data:
            latest_tick = tick_data[-1] if tick_data else {}
            entry_price = latest_tick.get('last_price', 0.0)
            if entry_price > 0:
                # Create indicators dict for expected move calculation
                indicators = {
                    'atr': latest_tick.get('atr', entry_price * 0.015),
                    'volume_ratio': latest_tick.get('volume_ratio', 1.0),
                    'rsi': latest_tick.get('rsi', 50),
                    'price_change': latest_tick.get('price_change', 0)
                }
                # Calculate expected move for common pattern types
                for pattern_type in ['volume_spike', 'volume_breakout', 'breakout', 'reversal']:
                    tasks.append(self.calculate_and_publish_expected_move(symbol, pattern_type, entry_price, indicators))
        
        # Run all calculations concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Compile results
        # Base tasks: rsi(0), ema_5(1), ema_10(2), ema_20(3), ema_50(4), ema_100(5), ema_200(6), 
        # macd(7), bb(8), atr(9), vwap(10), volume(11), missing_indicators(12), volume_profile(13)
        base_task_count = 14
        indicator_results = {
            'rsi': results[0] if not isinstance(results[0], Exception) else 50.0,
            # ✅ ALL EMA WINDOWS: Include all EMA periods in cached results
            'ema_5': results[1] if not isinstance(results[1], Exception) else 0.0,
            'ema_10': results[2] if not isinstance(results[2], Exception) else 0.0,
            'ema_20': results[3] if not isinstance(results[3], Exception) else 0.0,
            'ema_50': results[4] if not isinstance(results[4], Exception) else 0.0,
            'ema_100': results[5] if not isinstance(results[5], Exception) else 0.0,
            'ema_200': results[6] if not isinstance(results[6], Exception) else 0.0,
            'macd': results[7] if not isinstance(results[7], Exception) else {},
            'bollinger_bands': results[8] if not isinstance(results[8], Exception) else {},
            'atr': results[9] if not isinstance(results[9], Exception) else 0.0,
            'vwap': results[10] if not isinstance(results[10], Exception) else 0.0,
            'volume': results[11] if not isinstance(results[11], Exception) else {},
            'volume_profile': results[13] if len(results) > 13 and not isinstance(results[13], Exception) else {}
        }
        
        # Add F&O Greeks and basic data if option symbol (results at positions after base_task_count)
        # For options: fno_basic_data is at index base_task_count, greeks at base_task_count + 1
        if self._is_option_symbol(symbol) and len(results) > base_task_count:
            fno_idx = base_task_count
            greeks_idx = base_task_count + 1
            
            if len(results) > fno_idx:
                fno_data = results[fno_idx] if not isinstance(results[fno_idx], Exception) else {}
                if fno_data:
                    indicator_results['fno_basic'] = fno_data
            
            if len(results) > greeks_idx:
                greeks_data = results[greeks_idx] if not isinstance(results[greeks_idx], Exception) else {}
                if greeks_data:
                    indicator_results['greeks'] = greeks_data
                    # Also store individual Greeks for easy access
                    if isinstance(greeks_data, dict):
                        indicator_results['delta'] = greeks_data.get('delta', 0.0)
                        indicator_results['gamma'] = greeks_data.get('gamma', 0.0)
                        indicator_results['theta'] = greeks_data.get('theta', 0.0)
                        indicator_results['vega'] = greeks_data.get('vega', 0.0)
                        indicator_results['rho'] = greeks_data.get('rho', 0.0)
        
        # Publish comprehensive indicator summary
        await self.publish_indicator(symbol, 'comprehensive', {
            'symbol': symbol,
            'timestamp': int(time.time() * 1000),
            'indicators': indicator_results,
            'pattern_ready': True
        })
        
        logger.info(f"✅ Published {len(indicator_results)} indicators for {symbol}")
        return indicator_results
    
    async def publish_indicators_to_redis(self, symbol: str, tick_data: List[dict]):
        """Publish TA-Lib indicators to Redis for downstream consumers"""
        if not self.hybrid_calculations:
            return
        
        try:
            # Use the comprehensive indicator publishing system
            indicators = await self.batch_publish_all_indicators(symbol, tick_data)
            
            if indicators:
                # ✅ SINGLE SOURCE OF TRUTH: Store indicators in DB 5 (indicators_cache_db) per redis_config.py
                # Data type: "indicators_cache" maps to DB 5 (primary location for indicators and Greeks)
                # TTL: 300 seconds (5 minutes) to match calculation cache
                for indicator_name, value in indicators.items():
                    redis_key = f"indicators:{symbol}:{indicator_name}"
                    
                    if isinstance(value, dict) and value:  # Non-empty dict
                        # Complex indicators (MACD, Bollinger Bands, Volume, Volume Profile POC, Greeks)
                        indicator_data = {
                            'value': value,
                            'timestamp': int(time.time() * 1000),
                            'symbol': symbol,
                            'indicator_type': indicator_name
                        }
                        # ✅ Use indicators_cache (DB 5) - single source of truth
                        await self.redis.store_by_data_type("indicators_cache", redis_key, json.dumps(indicator_data), ttl=300)
                    elif value is not None and value != {}:  # Simple indicators (RSI, EMA, ATR, VWAP, individual Greeks)
                        # ✅ Use indicators_cache (DB 5) - single source of truth
                        await self.redis.store_by_data_type("indicators_cache", redis_key, str(value), ttl=300)
                
                # Additionally store volume profile POC separately for reversal detection
                if 'volume_profile' in indicators and isinstance(indicators['volume_profile'], dict):
                    vp_data = indicators['volume_profile']
                    if vp_data.get('poc_price'):
                        # Store POC price individually for easy dashboard access
                        poc_key = f"indicators:{symbol}:poc_price"
                        # ✅ Use indicators_cache (DB 5) - single source of truth
                        await self.redis.store_by_data_type("indicators_cache", poc_key, str(vp_data['poc_price']), ttl=300)
                    if vp_data.get('value_area_high'):
                        vah_key = f"indicators:{symbol}:value_area_high"
                        # ✅ Use indicators_cache (DB 5) - single source of truth
                        await self.redis.store_by_data_type("indicators_cache", vah_key, str(vp_data['value_area_high']), ttl=300)
                    if vp_data.get('value_area_low'):
                        val_key = f"indicators:{symbol}:value_area_low"
                        # ✅ Use indicators_cache (DB 5) - single source of truth
                        await self.redis.store_by_data_type("indicators_cache", val_key, str(vp_data['value_area_low']), ttl=300)
                
                logger.info(f"✅ Stored {len(indicators)} indicators in Redis DB 5 (indicators_cache) for {symbol} (including volume profile POC and Greeks)")
                
        except Exception as e:
            logger.error(f"Error publishing indicators for {symbol}: {e}")
    
    async def detect_and_store_patterns(self, symbol: str, tick_data: List[dict]):
        """Detect ICT patterns and store in Redis"""
        if not self.pattern_detectors:
            return
        
        try:
            all_patterns = []
            
            # Run all 8 ICT pattern detectors
            for detector in self.pattern_detectors:
                try:
                    patterns = await self.run_pattern_detection(detector, symbol, tick_data)
                    if patterns:
                        all_patterns.extend(patterns)
                except Exception as e:
                    logger.warning(f"Pattern detector {detector.__class__.__name__} failed for {symbol}: {e}")
            
            # Store patterns in Redis
            if all_patterns:
                await self.store_patterns_in_redis(symbol, all_patterns)
                await self.publish_patterns_stream(symbol, all_patterns)
                
                logger.info(f"✅ Detected {len(all_patterns)} patterns for {symbol}")
                
        except Exception as e:
            logger.error(f"Error detecting patterns for {symbol}: {e}")
    
    async def run_pattern_detection(self, detector, symbol: str, tick_data: List[dict]):
        """Run pattern detection for a specific detector"""
        try:
            # Extract price and volume data - use bucket_incremental_volume
            prices = [float(tick.get('last_price', 0)) for tick in tick_data]
            volumes = [float(tick.get('bucket_incremental_volume', 0)) for tick in tick_data]
            highs = [float(tick.get('high', tick.get('last_price', 0))) for tick in tick_data]
            lows = [float(tick.get('low', tick.get('last_price', 0))) for tick in tick_data]
            
            # Create symbol data for pattern detection
            symbol_data = {
                'symbol': symbol,
                'last_price': prices[-1] if prices else 0,
                'prices': prices,
                'volumes': volumes,
                'highs': highs,
                'lows': lows,
                'timestamp': int(time.time() * 1000)
            }
            
            # Run pattern detection based on detector type
            if hasattr(detector, 'detect_patterns'):
                return detector.detect_patterns(symbol_data)
            elif hasattr(detector, 'detect_liquidity_patterns'):
                return detector.detect_liquidity_patterns(symbol_data)
            elif hasattr(detector, 'detect_fvg_patterns'):
                return detector.detect_fvg_patterns(symbol_data)
            elif hasattr(detector, 'calculate_ote_zones'):
                return detector.calculate_ote_zones(symbol_data)
            elif hasattr(detector, 'detect_premium_discount'):
                return detector.detect_premium_discount(symbol_data)
            elif hasattr(detector, 'detect_killzone'):
                return detector.detect_killzone(symbol_data)
            elif hasattr(detector, 'iv_crush_play'):
                # Straddle strategy detection
                return await self.detect_straddle_patterns(detector, symbol_data)
            elif hasattr(detector, 'generate_options_strategies'):
                # Range bound analyzer for straddle
                return await self.detect_range_bound_straddle(detector, symbol_data)
            # Note: detect_market_maker_traps and detect_premium_collection are handled by MarketMakerTrapDetector
            # and PatternDetector classes directly, not via these undefined methods
            elif hasattr(detector, 'detect_patterns'):
                # Core pattern detector (8 patterns from registry)
                return await self.detect_core_patterns(detector, symbol_data)
            
        except Exception as e:
            logger.warning(f"Pattern detection failed for {detector.__class__.__name__}: {e}")
            return []
    
    async def detect_straddle_patterns(self, detector, symbol_data: dict):
        """Detect straddle patterns using MM exploitation strategies"""
        try:
            # Check for IV crush play conditions
            straddle_patterns = []
            
            # Get current price and volatility indicators
            current_price = symbol_data.get('last_price', 0)
            prices = symbol_data.get('prices', [])
            volumes = symbol_data.get('volumes', [])
            
            if len(prices) < 20:
                return []
            
            # Calculate volatility metrics for straddle detection
            price_volatility = self.calculate_price_volatility(prices)
            volume_spike = self.detect_volume_spike(volumes)
            
            # Check for straddle setup conditions
            if price_volatility > 0.02 and volume_spike > 1.5:  # High volatility + volume spike
                straddle_pattern = {
                    'pattern_type': 'straddle_setup',
                    'strategy': 'iv_crush_play',
                    'symbol': symbol_data.get('symbol'),
                    'current_price': current_price,
                    'volatility': price_volatility,
                    'volume_spike': volume_spike,
                    'confidence': min(0.9, price_volatility * 10),
                    'signal': 'SELL_STRADDLE',
                    'expected_move': price_volatility * current_price,
                    'timestamp': symbol_data.get('timestamp')
                }
                straddle_patterns.append(straddle_pattern)
            
            return straddle_patterns
            
        except Exception as e:
            logger.warning(f"Straddle pattern detection failed: {e}")
            return []
    
    async def detect_range_bound_straddle(self, detector, symbol_data: dict):
        """Detect range-bound straddle opportunities"""
        try:
            straddle_patterns = []
            
            # Extract price data
            prices = symbol_data.get('prices', [])
            highs = symbol_data.get('highs', [])
            lows = symbol_data.get('lows', [])
            
            if len(prices) < 20:
                return []
            
            # Calculate range metrics
            price_range = max(highs) - min(lows) if highs and lows else 0
            current_price = symbol_data.get('last_price', 0)
            range_percentage = (price_range / current_price) * 100 if current_price > 0 else 0
            
            # Check for range-bound conditions (low volatility, sideways movement)
            if range_percentage < 3.0:  # Less than 3% range
                straddle_pattern = {
                    'pattern_type': 'range_bound_straddle',
                    'strategy': 'strangle_strategy',
                    'symbol': symbol_data.get('symbol'),
                    'current_price': current_price,
                    'range_percentage': range_percentage,
                    'confidence': min(0.85, (3.0 - range_percentage) / 3.0),
                    'signal': 'SELL_STRANGLE',
                    'expected_move': range_percentage * current_price / 100,
                    'timestamp': symbol_data.get('timestamp')
                }
                straddle_patterns.append(straddle_pattern)
            
            return straddle_patterns
            
        except Exception as e:
            logger.warning(f"Range-bound straddle detection failed: {e}")
            return []
    
    def calculate_price_volatility(self, prices: List[float]) -> float:
        """Calculate price volatility for straddle detection"""
        if len(prices) < 10:
            return 0.0
        
        # Calculate standard deviation of price changes
        price_changes = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                change = (prices[i] - prices[i-1]) / prices[i-1]
                price_changes.append(change)
        
        if not price_changes:
            return 0.0
        
        # Calculate standard deviation
        mean_change = sum(price_changes) / len(price_changes)
        variance = sum((change - mean_change) ** 2 for change in price_changes) / len(price_changes)
        return (variance ** 0.5) * 100  # Return as percentage
    
    def detect_volume_spike(self, volumes: List[float]) -> float:
        """Detect volume spike for straddle confirmation"""
        if len(volumes) < 10:
            return 1.0
        
        # Calculate average volume
        avg_volume = sum(volumes[:-5]) / len(volumes[:-5]) if len(volumes) > 5 else sum(volumes) / len(volumes)
        current_volume = volumes[-1] if volumes else 0
        
        if avg_volume > 0:
            return current_volume / avg_volume
        return 1.0
    
    async def detect_core_patterns(self, detector, symbol_data: dict):
        """Detect core patterns from the pattern registry (8 patterns)"""
        try:
            # Get active patterns from registry
            active_patterns = self.get_patterns_from_registry()
            if not active_patterns:
                logger.warning("No active patterns found in registry")
                return []
            
            # Convert symbol_data to indicators format expected by PatternDetector
            indicators = {
                'symbol': symbol_data.get('symbol'),
                'last_price': symbol_data.get('last_price', 0),
                'volume': symbol_data.get('volumes', [0])[-1] if symbol_data.get('volumes') else 0,
                'price_change': 0,  # Will be calculated by detector
                'volume_ratio': 1.0,  # Will be calculated by detector
                'high': symbol_data.get('highs', [0])[-1] if symbol_data.get('highs') else 0,
                'low': symbol_data.get('lows', [0])[-1] if symbol_data.get('lows') else 0,
                'timestamp': symbol_data.get('timestamp', int(time.time() * 1000))
            }
            
            # Run core pattern detection
            patterns = detector.detect_patterns(indicators)
            
            # Convert patterns to our format and filter by registry
            core_patterns = []
            for pattern in patterns:
                pattern_type = pattern.get('pattern', 'unknown')
                
                # Check if pattern is active in registry
                if pattern_type in active_patterns:
                    # Get pattern config from registry
                    pattern_config = self.get_pattern_config(pattern_type)
                    
                    # Apply registry thresholds
                    confidence = pattern.get('confidence', 0.5)
                    registry_threshold = pattern_config.get('confidence_threshold', 0.0)
                    
                    if confidence >= registry_threshold:
                        core_pattern = {
                            'pattern_type': pattern_type,
                            'strategy': 'core_pattern',
                            'symbol': symbol_data.get('symbol'),
                            'current_price': symbol_data.get('last_price', 0),
                            'confidence': confidence,
                            'signal': pattern.get('signal', 'NEUTRAL'),
                            'expected_move': pattern.get('expected_move', 0),
                            'timestamp': symbol_data.get('timestamp'),
                            'registry_config': pattern_config
                        }
                        core_patterns.append(core_pattern)
            
            return core_patterns
            
        except Exception as e:
            logger.warning(f"Core pattern detection failed: {e}")
            return []
    
    async def store_patterns_in_redis(self, symbol: str, patterns: List[dict]):
        """Store detected patterns in Redis"""
        try:
            for i, pattern in enumerate(patterns):
                pattern_key = f"patterns:{symbol}:{pattern.get('pattern_type', 'unknown')}:{int(time.time() * 1000)}"
                
                # Add metadata
                pattern_data = {
                    'symbol': symbol,
                    'pattern': pattern,
                    'timestamp': int(time.time() * 1000),
                    'pattern_id': f"{symbol}_{int(time.time() * 1000)}_{i}"
                }
                
                # Store in Redis (DB 1 - realtime via analysis_cache)
                await self.redis.store_by_data_type("analysis_cache", pattern_key, json.dumps(pattern_data))
                
        except Exception as e:
            logger.error(f"Error storing patterns for {symbol}: {e}")
    
    async def publish_patterns_stream(self, symbol: str, patterns: List[dict]):
        """Publish patterns to Redis stream for real-time alerts"""
        try:
            # Get realtime client (DB 1) for pattern streams
            # Use process-specific client if available
            try:
                from redis_files.redis_manager import RedisManager82
                # ✅ STANDARDIZED: Use RedisManager82 instead of legacy get_optimized_client
                realtime_client = RedisManager82.get_client(process_name="redis_storage", db=1)
            except Exception:
                # Fallback to existing method
                realtime_client = self.redis.get_client(1) if hasattr(self.redis, 'get_client') else self.redis
            
            for pattern in patterns:
                stream_data = {
                    'symbol': symbol,
                    'timestamp': int(time.time() * 1000),
                    'pattern': json.dumps(pattern),
                    'source': 'redis_storage'
                }
                
                # Publish to pattern stream
                stream_key = f"patterns:{symbol}"
                # ✅ FIXED: ALWAYS use maxlen to prevent unbounded stream growth
                realtime_client.xadd(stream_key, stream_data, maxlen=5000, approximate=True)
                
                # Also publish to global patterns stream
                realtime_client.xadd("patterns:global", stream_data, maxlen=5000, approximate=True)

                # Additionally publish compact alert to alerts:stream for SSE
                try:
                    # Build minimal alert payload with alert_id for frontend compatibility
                    timestamp_ms = int(time.time() * 1000)
                    alert_id = f"{symbol}_{timestamp_ms}"
                    alert_payload = {
                        'alert_id': alert_id,  # Required for frontend DataGrid
                        'symbol': symbol,
                        'pattern': pattern.get('pattern_type') or pattern.get('pattern') or 'unknown',
                        'confidence': float(pattern.get('confidence', 0.0)),
                        'last_price': pattern.get('current_price', 0.0),
                        'timestamp': timestamp_ms,
                        'direction': pattern.get('signal', 'NEUTRAL'),
                        'signal': pattern.get('signal', 'NEUTRAL'),  # Add signal for frontend
                    }
                    # Serialize to binary using orjson if available
                    try:
                        import orjson as _oj
                        binary = _oj.dumps(alert_payload)
                    except Exception:
                        binary = json.dumps(alert_payload).encode()
                    # ✅ TIER 1: Keep stream trimmed for performance (last 1k alerts)
                    realtime_client.xadd('alerts:stream', { 'data': binary }, maxlen=1000, approximate=True)
                except Exception as _alert_err:
                    logger.debug(f"Alerts stream publish skipped: {_alert_err}")
                
        except Exception as e:
            logger.error(f"Error publishing patterns stream for {symbol}: {e}")
    
    def get_patterns_from_registry(self, category: str = None) -> List[str]:
        """Get patterns from pattern registry config"""
        if not self.registry_available:
            return []
        
        if category:
            categories = self.pattern_registry.get("categories", {})
            category_data = categories.get(category, {})
            return category_data.get("patterns", [])
        else:
            # Return all patterns from all categories
            all_patterns = []
            categories = self.pattern_registry.get("categories", {})
            for cat_data in categories.values():
                all_patterns.extend(cat_data.get("patterns", []))
            return all_patterns
    
    def get_pattern_config(self, pattern_name: str) -> dict:
        """Get configuration for a specific pattern from registry"""
        if not self.registry_available:
            return {}
        
        pattern_configs = self.pattern_registry.get("pattern_configs", {})
        return pattern_configs.get(pattern_name, {})
    
    def _resolve_token_to_symbol_for_storage(self, symbol):
        """Resolve token to symbol for Redis storage - delegates to redis client"""
        try:
            # Use the redis client's token resolution method if available
            if hasattr(self.redis, '_resolve_token_to_symbol_for_storage'):
                resolved = self.redis._resolve_token_to_symbol_for_storage(symbol)
                if resolved and resolved != symbol:
                    logger.debug(f"✅ RedisStorage: Resolved token {symbol} to {resolved}")
                return resolved
            
            # Fallback: basic resolution check
            symbol_str = str(symbol) if symbol else ""
            if ":" in symbol_str and not symbol_str.isdigit():
                return symbol  # Already a symbol
            return symbol  # Return original if resolution not available
        except Exception as e:
            logger.debug(f"Token resolution failed in RedisStorage for {symbol}: {e}")
            return symbol


class RedisJSONStateManager:
    """RedisJSON-backed state manager with RediSearch indexing.

    - Stores alert docs as JSON for fast field queries
    - Indexes via FT for instant filtering/sorting
    - All operations are async and optional (no hard dependency)
    """

    def __init__(self, host: str = 'localhost', port: int = 6379, index_name: str = 'alerts_idx'):
        if not _JSON_SEARCH_AVAILABLE:
            raise RuntimeError("RedisJSON/RediSearch not available (redis.asyncio/orjson missing)")
        self.redis = aioredis_json.Redis(host=host, port=port, decode_responses=False, health_check_interval=30)
        self.index_name = index_name

    async def ensure_index(self) -> None:
        """Create RediSearch index on JSON alerts if missing."""
        try:
            # FT.CREATE alerts_idx ON JSON PREFIX 1 alert: SCHEMA $.symbol AS symbol TAG $.pattern AS pattern TAG $.confidence AS confidence NUMERIC $.timestamp AS timestamp NUMERIC
            await self.redis.execute_command(
                'FT.CREATE', self.index_name, 'ON', 'JSON', 'PREFIX', 1, 'alert:',
                'SCHEMA',
                '$.symbol', 'AS', 'symbol', 'TAG',
                '$.pattern', 'AS', 'pattern', 'TAG',
                '$.confidence', 'AS', 'confidence', 'NUMERIC',
                '$.timestamp', 'AS', 'timestamp', 'NUMERIC'
            )
        except Exception:
            # Assume exists
            pass

    async def store_alert_json(self, alert_data: dict) -> str:
        """Store alert document as RedisJSON with TTL and index it."""
        alert_id = f"alert:{alert_data.get('symbol','UNKNOWN')}:{alert_data.get('timestamp',0)}"
        await self.redis.json().set(alert_id, '$', alert_data)
        await self.redis.expire(alert_id, 3600)
        return alert_id

    async def search_alerts(self, symbol: str | None = None, pattern: str | None = None, min_confidence: float = 0.0, limit: int = 100):
        """Search alerts via RediSearch with simple filters and recent-first sorting."""
        # Ensure index before querying
        await self.ensure_index()
        parts = []
        if symbol:
            parts.append(f"@symbol:{{{symbol}*}}")
        if pattern:
            parts.append(f"@pattern:{{{pattern}}}")
        if min_confidence > 0:
            parts.append(f"@confidence:[{min_confidence} 1]")
        query = ' '.join(parts) if parts else '*'
        # FT.SEARCH alerts_idx query SORTBY timestamp DESC LIMIT 0 limit
        res = await self.redis.execute_command(
            'FT.SEARCH', self.index_name, query, 'SORTBY', 'timestamp', 'DESC', 'LIMIT', 0, limit
        )
        # Parse flat RediSearch response: [total, key1, ["$", json], key2, ["$", json], ...]
        docs = []
        if isinstance(res, (list, tuple)) and len(res) >= 1:
            it = iter(res[1:])
            for key in it:
                try:
                    fields = next(it)
                    if isinstance(fields, (list, tuple)):
                        # JSON from JSON.GET is stored under "$"
                        for i in range(0, len(fields), 2):
                            if fields[i] in (b'$','$'):
                                raw = fields[i+1]
                                if isinstance(raw, bytes):
                                    docs.append(_orjson.loads(raw))
                                elif isinstance(raw, str):
                                    docs.append(_orjson.loads(raw.encode()))
                except StopIteration:
                    break
        return docs
