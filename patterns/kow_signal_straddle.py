''' 
Kow Signal Straddle Strategy: A VWAP-Based Options Trading System

Strategy Overview
=================

The Kow Signal Straddle is an intraday options trading strategy designed for NIFTY and BANKNIFTY indices. 
It uses Volume Weighted Average Price (VWAP) as the primary signal generator to identify optimal entry 
and exit points for At-The-Money (ATM) straddles.

Core Concept
------------

A straddle involves simultaneously buying (or selling) both a Call Option (CE) and Put Option (PE) at 
the same strike price. The strategy profits when the underlying moves significantly in either direction.

The Kow Signal strategy specifically:
- Sells ATM straddles when combined premium (CE + PE) drops below VWAP
- Uses VWAP as a dynamic reference point for fair value
- Trades only during active market hours (9:30 AM - 3:15 PM)
- Requires high confidence (85%+) before generating signals

Data Integration
----------------

Field Name Standardization:
- Uses standardized field names from optimized_field_mapping.yaml
- Primary fields: bucket_incremental_volume, last_price
- Fallback fields: volume, zerodha_cumulative_volume, current_price
- All tick data processing follows standard field mapping conventions

Redis Integration:
- DB 0 (system): Strategy state storage (position, entry_premium, current_strike)
- DB 1 (realtime): Alert publishing (alerts:stream), market data (market:nifty_spot, market:banknifty_spot)
- DB 2 (analytics): OHLC data lookup (ohlc_latest:* keys) for underlying price fallback
- All Redis keys follow standard naming patterns from redis_key_standards

Alert System Integration:
- Publishes signals to alerts:stream (DB 1) with binary JSON format
- Uses orjson for fast binary serialization (falls back to standard json)
- Signal format includes all required fields for alert manager:
  * symbol, pattern, pattern_type, confidence, signal, action
  * last_price, price, timestamp, timestamp_ms
  * volume_ratio, expected_move, strategy_type
- Signals are automatically processed by alert manager and notifiers
- Telegram notifications sent for high-confidence signals (≥90%)
- Signal bot routing for Kow Signal patterns

End-to-End Flow:
1. Tick data received → Standardized field mapping
2. Combined premium calculated → VWAP computed from session start
3. Signal generated → All required fields included
4. Published to alerts:stream → Binary JSON format
5. Alert manager processes → Confidence filtering
6. Notifiers dispatch → Telegram/other channels
7. Dashboard displays → Real-time alert visualization

Dependencies:
- orjson (optional): Faster binary JSON serialization
- redis: Redis client for data storage and streams
- Standard library: asyncio, json, logging, datetime
- Project modules: patterns, alerts, config, redis_files

Created: September 2025
Updated: November 2025 - Enhanced Redis integration and alert system compatibility
'''
import asyncio
import pandas as pd
from typing import Dict, Optional, Tuple
import logging
import time
import json
from patterns.utils.pattern_schema import create_pattern

# Try to import orjson for faster binary serialization
try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False
    orjson = None
from intraday_scanner.math_dispatcher import MathDispatcher
from patterns.pattern_mathematics import PatternMathematics
from config.thresholds import get_kow_signal_strategy_config
from datetime import datetime, time as dt_time, timedelta
from alerts.risk_manager import RiskManager
from alerts.notifiers import TelegramNotifier

class KowSignalStraddleStrategy:
    """
    Intraday Combined VWAP Straddle Strategy for Nifty/BankNifty
    
    This strategy uses VWAP of combined premium (CE+PE) with session anchoring to generate
    entry and exit signals for ATM straddles. Fully integrated with the alert system.
    
    Key Features:
    - VWAP-based signal generation from session start (9:15 AM)
    - Standardized field name mappings (optimized_field_mapping.yaml)
    - Redis integration (DB 0: state, DB 1: alerts, DB 2: analytics)
    - Alert system integration (alerts:stream, Telegram, dashboard)
    - Binary JSON serialization for fast alert publishing
    
    Signal Format:
    All signals include required fields for alert manager compatibility:
    - Core: symbol, pattern, confidence, signal, action, last_price, timestamp
    - Strategy: strike, ce_symbol, pe_symbol, entry_premium, current_vwap
    - Metadata: volume_ratio, expected_move, strategy_type
    
    Redis Storage:
    - Strategy state: DB 0 (strategy:kow:* keys)
    - Alert publishing: DB 1 (alerts:stream)
    - Market data: DB 1 (market:nifty_spot, market:banknifty_spot)
    - OHLC fallback: DB 2 (ohlc_latest:* keys)
    """

    def __init__(self, redis_client=None, config: Dict = None):
        """Initialize Kow Signal Straddle Strategy with Redis client"""
        # Initialize Redis client using RedisManager82
        if redis_client is None:
            try:
                from redis_files.redis_manager import RedisManager82
                # Use DB 1 for realtime data, DB 0 for strategy state
                self.redis_realtime = RedisManager82.get_client(process_name="kow_signal_strategy", db=1, decode_responses=False)
                self.redis_system = RedisManager82.get_client(process_name="kow_signal_strategy", db=0, decode_responses=True)
                self.redis = self.redis_realtime  # Default to realtime for backward compatibility
            except Exception as e:
                logging.error(f"Failed to initialize Redis client: {e}")
                self.redis_realtime = None
                self.redis_system = None
                self.redis = None
        else:
            self.redis = redis_client
            self.redis_realtime = redis_client
            self.redis_system = redis_client
        
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        try:
            from config.thresholds import get_kow_signal_strategy_config
            self.config = config or get_kow_signal_strategy_config()
        except ImportError:
            self.config = config or {
                'entry_time': '09:30',
                'exit_time': '15:15',
                'confirmation_periods': 1,
                'max_reentries': 3,
                'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
                'strike_selection': 'atm',
                'max_risk_per_trade': 0.03,
                'confidence_threshold': 0.85,
                'telegram_routing': True,
                'lot_size': 50
            }
        
        # Strategy state - per underlying (supports multiple underlyings simultaneously)
        self.position_states = {}  # {underlying: position_state_dict}
        self.combined_premium_data = {}  # {underlying: [data_list]}
        self.ce_premiums = {}  # {underlying: premium}
        self.pe_premiums = {}  # {underlying: premium}
        self.ce_volumes = {}  # {underlying: volume}
        self.pe_volumes = {}  # {underlying: volume}
        self.atm_strikes = {}  # {underlying: strike}
        
        # Initialize per-underlying tracking
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        for underlying in underlying_symbols:
            self.position_states[underlying] = {
                'current_position': None,  # 'BOTH_LEGS', 'CE_ONLY', 'PE_ONLY', 'NONE'
                'entry_time': None,
                'entry_combined_premium': 0.0,
                'reentry_count': 0,
                'current_profit': 0.0,
                'stop_loss_level': 1000,
                'current_strike': None
            }
            self.combined_premium_data[underlying] = []
            self.ce_premiums[underlying] = None
            self.pe_premiums[underlying] = None
            self.ce_volumes[underlying] = 0
            self.pe_volumes[underlying] = 0
            self.atm_strikes[underlying] = None
        
        # Legacy single-strike tracking (for backward compatibility)
        self.current_atm_strike = None
        
        # Underlying price tracking (fix missing attribute)
        self.underlying_prices = []  # Stores {'timestamp', 'price', 'symbol'}
        
        # Leg tracking for P&L
        self.ce_entry_price = None
        self.pe_entry_price = None
        self.ce_exit_time = None
        self.pe_exit_time = None
        
        # Initialize risk manager and math dispatcher
        self.risk_manager = RiskManager(redis_client=self.redis_realtime)
        self.math_dispatcher = MathDispatcher(PatternMathematics, self.risk_manager)
        self.pattern_math = PatternMathematics()
        
        # Initialize notifier
        self.notifier = TelegramNotifier()
        
        # Load strategy state from Redis
        self._load_strategy_state_from_redis()
        
        self.logger.info("Kow Signal Straddle Strategy initialized")

    def _get_atm_strike(self, underlying_price: float, underlying: str = None) -> int:
        """Calculate ATM strike based on current underlying price and underlying symbol"""
        if underlying is None:
            # Try to determine from underlying_symbols
            underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        
        # NIFTY uses 50-point intervals, BANKNIFTY uses 100-point intervals
        if "NIFTY" in underlying.upper() and "BANK" not in underlying.upper():
            strike_interval = 50
        else:  # BANKNIFTY
            strike_interval = 100
        return round(underlying_price / strike_interval) * strike_interval

    def _calculate_combined_vwap(self, underlying: str) -> float:
        """Calculate VWAP for combined premium from session start (9:15 AM) for specific underlying"""
        if underlying not in self.combined_premium_data or not self.combined_premium_data[underlying]:
            return 0.0
        
        # Filter data from session start (9:15 AM)
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.combined_premium_data[underlying] 
                       if d['timestamp'] >= session_start]
        
        if not session_data:
            return 0.0
            
        cumulative_pv = sum(d['premium'] * d['volume'] for d in session_data)
        cumulative_volume = sum(d['volume'] for d in session_data)
        
        return cumulative_pv / cumulative_volume if cumulative_volume else 0.0

    def _get_option_symbols(self, underlying: str, strike: int, expiry: str = None) -> Tuple[str, str]:
        """Generate CE and PE symbols based on strike and expiry"""
        if expiry is None:
            expiry = self._get_current_expiry()
        
        # Format: NIFTY25NOV25900CE, NIFTY25NOV25900PE
        # Or NFO:NIFTY25NOV25900CE for exchange prefix
        ce_symbol = f"{underlying}{expiry}{strike}CE"
        pe_symbol = f"{underlying}{expiry}{strike}PE"
        return ce_symbol, pe_symbol
    
    def _is_atm_option(self, symbol: str, strike: int, underlying: str = None) -> bool:
        """Check if symbol matches ATM strike for the given underlying"""
        if underlying:
            # Check if symbol belongs to this underlying and matches strike
            if underlying.upper() not in symbol.upper():
                return False
        return str(strike) in symbol
    
    def _get_underlying_from_symbol(self, symbol: str) -> Optional[str]:
        """Extract underlying name from symbol (NIFTY or BANKNIFTY)"""
        symbol_upper = symbol.upper()
        if "BANKNIFTY" in symbol_upper:
            return "BANKNIFTY"
        elif "NIFTY" in symbol_upper and "BANK" not in symbol_upper:
            return "NIFTY"
        return None

    async def on_tick(self, tick_data: Dict) -> Optional[Dict]:
        """Process incoming tick data from Zerodha WebSocket"""
        try:
            # Filter relevant instruments
            if not self._is_relevant_tick(tick_data):
                return None

            # Update data trackers
            self._update_market_data(tick_data)

            # Check if market hours
            if not self._is_trading_hours():
                return None

            # Generate signals
            signal = await self._generate_straddle_signals(tick_data)
            
            if signal:
                await self._send_alert(signal)
                # Store signal in Redis for tracking
                await self._store_signal_in_redis(signal)
                
            return signal

        except Exception as e:
            self.logger.error(f"Error in on_tick: {e}")
            return None


    def _is_relevant_tick(self, tick_data: Dict) -> bool:
        """Check if tick is relevant for straddle strategy"""
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        
        # Check if it's underlying index or options
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol for us in underlying_symbols)
        is_option = 'CE' in symbol or 'PE' in symbol
        
        return is_underlying or is_option

    def _update_market_data(self, tick_data: Dict):
        """Update market data for combined premium calculation using standard field names"""
        # Use standard field names from optimized_field_mapping.yaml
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        # Use standardized field: last_price (primary) with fallback
        last_price = tick_data.get('last_price', tick_data.get('current_price', 0.0))
        # Use standardized field: bucket_incremental_volume (primary) with fallback
        volume = tick_data.get('bucket_incremental_volume', tick_data.get('volume', tick_data.get('zerodha_cumulative_volume', 0)))
        
        # Update underlying price to determine ATM strike
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol.upper() for us in underlying_symbols)
        
        if is_underlying and last_price > 0:
            # Determine which underlying this is
            underlying_name = None
            if 'BANKNIFTY' in symbol.upper():
                underlying_name = 'BANKNIFTY'
            elif 'NIFTY' in symbol.upper() and 'BANK' not in symbol.upper():
                underlying_name = 'NIFTY'
            
            if underlying_name and underlying_name in self.atm_strikes:
                # Update ATM strike for this underlying
                self.atm_strikes[underlying_name] = self._get_atm_strike(last_price, underlying_name)
                
                # Update legacy single-strike (for backward compatibility - use first available)
                if self.current_atm_strike is None:
                    self.current_atm_strike = self.atm_strikes[underlying_name]
            
            # Store underlying price for tracking
            self.underlying_prices.append({
                'timestamp': datetime.now(),
                'price': last_price,
                'symbol': symbol
            })
            
            # Keep only last hour of prices
            cutoff_time = datetime.now() - timedelta(hours=1)
            self.underlying_prices = [p for p in self.underlying_prices if p['timestamp'] > cutoff_time]
            
            # Store in Redis using standard key pattern
            if self.redis_realtime and underlying_name:
                try:
                    # Store spot price: market:nifty_spot or market:banknifty_spot
                    spot_key = f"market:{underlying_name.lower()}_spot"
                    self.redis_realtime.set(spot_key, str(last_price))
                    self.redis_realtime.expire(spot_key, 60)  # 60 second TTL
                except Exception as e:
                    self.logger.debug(f"Error storing spot price: {e}")

        # Update option premium data for ATM strikes of all underlyings
        underlying_from_symbol = self._get_underlying_from_symbol(symbol)
        if underlying_from_symbol and underlying_from_symbol in self.atm_strikes:
            atm_strike = self.atm_strikes[underlying_from_symbol]
            if atm_strike and self._is_atm_option(symbol, atm_strike, underlying_from_symbol):
                self._update_combined_premium(tick_data, underlying_from_symbol)

    def _update_combined_premium(self, tick_data: Dict, underlying: str):
        """Track combined premium (CE + PE) for ATM straddle using standard field names per underlying"""
        # Handle both 'symbol' and 'tradingsymbol' fields (standardized field mapping)
        symbol = tick_data.get('symbol', tick_data.get('tradingsymbol', ''))
        # Use standardized field: last_price (primary) with fallback
        premium = tick_data.get('last_price', tick_data.get('current_price', 0))
        # Use standardized field: bucket_incremental_volume (primary) with fallback
        volume = tick_data.get('bucket_incremental_volume', tick_data.get('volume', tick_data.get('zerodha_cumulative_volume', 0)))
        
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return
        
        atm_strike = self.atm_strikes[underlying]
        
        # Update CE or PE premium for this underlying
        if 'CE' in symbol and premium > 0:
            self.ce_premiums[underlying] = premium
            self.ce_volumes[underlying] = volume
            # Store option price in Redis using standard key pattern (per underlying)
            if self.redis_realtime:
                underlying_lower = underlying.lower()
                option_key = f"options:{underlying_lower}:{atm_strike}CE:last_price"
                self.redis_realtime.set(option_key, str(premium))
                self.redis_realtime.set(f"options:{underlying_lower}:{atm_strike}CE:volume", str(volume))
                
        elif 'PE' in symbol and premium > 0:
            self.pe_premiums[underlying] = premium
            self.pe_volumes[underlying] = volume
            # Store option price in Redis using standard key pattern (per underlying)
            if self.redis_realtime:
                underlying_lower = underlying.lower()
                option_key = f"options:{underlying_lower}:{atm_strike}PE:last_price"
                self.redis_realtime.set(option_key, str(premium))
                self.redis_realtime.set(f"options:{underlying_lower}:{atm_strike}PE:volume", str(volume))
        
        # Calculate combined premium when both CE and PE available for this underlying
        if (self.ce_premiums[underlying] and self.pe_premiums[underlying] and 
            self.ce_premiums[underlying] > 0 and self.pe_premiums[underlying] > 0):
            combined_premium = self.ce_premiums[underlying] + self.pe_premiums[underlying]
            total_volume = (self.ce_volumes[underlying] + self.pe_volumes[underlying]) // 2  # Average volume
            
            self.combined_premium_data[underlying].append({
                'timestamp': datetime.now(),
                'premium': combined_premium,
                'volume': total_volume
            })
            
            # Keep only last 4 hours of data
            self._clean_old_premium_data(underlying)
        
    def _clean_old_premium_data(self, underlying: str):
        """Remove data older than 4 hours to manage memory for specific underlying"""
        if underlying not in self.combined_premium_data:
            return
        cutoff_time = datetime.now() - timedelta(hours=4)
        self.combined_premium_data[underlying] = [
            d for d in self.combined_premium_data[underlying] 
            if d['timestamp'] > cutoff_time
        ]
    
    def _get_current_combined_premium(self, underlying: str) -> float:
        """Get current combined premium for specific underlying"""
        if (underlying in self.ce_premiums and underlying in self.pe_premiums and
            self.ce_premiums[underlying] and self.pe_premiums[underlying]):
            return self.ce_premiums[underlying] + self.pe_premiums[underlying]
        return 0.0


    async def _generate_straddle_signals(self, tick_data: Dict) -> Optional[Dict]:
        """Generate straddle entry/exit signals based on combined premium VWAP for all underlyings"""
        current_time = datetime.now().time()
        
        # Only trade between 9:30 AM and 3:15 PM
        if current_time < dt_time(9, 30) or current_time > dt_time(15, 15):
            return None

        # Process each underlying separately to generate signals for all ATM straddles
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        
        for underlying in underlying_symbols:
            # Get current combined premium and VWAP for this underlying
            combined_premium = self._get_current_combined_premium(underlying)
            current_vwap = self._calculate_combined_vwap(underlying)
            
            if combined_premium == 0 or current_vwap == 0:
                continue  # Skip if no data for this underlying

            # Calculate confidence
            confidence = self._calculate_confidence(combined_premium, current_vwap, underlying)

            if confidence < self.config.get('confidence_threshold', 0.85):
                continue  # Skip if confidence too low

            # Get position state for this underlying
            position_state = self.position_states[underlying]
            
            # Generate signals based on position state for this underlying
            signal = None
            if position_state['current_position'] is None:
                signal = await self._check_entry_signal(combined_premium, current_vwap, confidence, underlying)
            elif position_state['current_position'] == 'BOTH_LEGS':
                signal = await self._check_exit_signal(combined_premium, current_vwap, confidence, underlying)
            else:  # Single leg position
                # Check re-entry first, then exit
                reentry_signal = await self._check_reentry_signal(combined_premium, current_vwap, confidence, underlying)
                if reentry_signal:
                    signal = reentry_signal
                else:
                    signal = await self._check_exit_signal(combined_premium, current_vwap, confidence, underlying)
            
            # Return first signal found (can be enhanced to return multiple signals)
            if signal:
                return signal
        
        return None
    

    def _calculate_confidence(self, combined_premium: float, current_vwap: float, underlying: str = None) -> float:
        """Calculate confidence score for the signal"""
        try:
            # Get underlying price for this specific underlying
            underlying_price = self._get_current_underlying_price(underlying)
            
            math_context = {
                "pattern_type": "kow_signal_straddle",
                "vwap": current_vwap,
                "combined_premium": combined_premium,
                "underlying_price": underlying_price,
            }
            if self.math_dispatcher:
                return self.math_dispatcher.calculate_confidence('kow_signal_straddle', math_context)
        except Exception as e:
            self.logger.debug(f"MathDispatcher error: {e}")
        
        # Fallback confidence calculation
        premium_ratio = abs(combined_premium - current_vwap) / current_vwap if current_vwap > 0 else 0
        base_confidence = 0.9 if premium_ratio > 0.02 else 0.7
        return min(base_confidence, 0.95)

    async def _check_entry_signal(self, combined_premium: float, current_vwap: float, confidence: float, underlying: str) -> Optional[Dict]:
        """Check for straddle entry signal for specific underlying"""
        # Entry condition: Combined premium below VWAP with 1-minute confirmation
        if combined_premium < current_vwap:
            if self._confirm_below_vwap(period_minutes=1, underlying=underlying):
                return self._create_entry_signal(combined_premium, confidence, underlying)
        return None


    async def _check_exit_signal(self, combined_premium: float, current_vwap: float, confidence: float, underlying: str) -> Optional[Dict]:
        """Check for exit or leg management signals for specific underlying"""
        # Exit condition: Combined premium above VWAP with 2-minute confirmation
        if combined_premium > current_vwap:
            if self._confirm_above_vwap(period_minutes=2, underlying=underlying):
                return await self._manage_leg_exit(underlying)

        # Check stop loss
        sl_signal = self._check_stop_loss(combined_premium, underlying)
        if sl_signal:
            return sl_signal

        return None
    
    async def _check_reentry_signal(self, combined_premium: float, current_vwap: float, confidence: float, underlying: str) -> Optional[Dict]:
        """Check if we should re-enter a previously exited leg for specific underlying"""
        position_state = self.position_states[underlying]
        current_position = position_state['current_position']
        
        # Get exit times for this underlying (need to track per underlying)
        # For now, use position state to determine re-entry
        if combined_premium < current_vwap and self._confirm_below_vwap(period_minutes=1, underlying=underlying):
            if current_position == 'CE_ONLY':
                # Re-enter PE leg
                return self._create_reentry_signal('PE', confidence, underlying)
            elif current_position == 'PE_ONLY':
                # Re-enter CE leg
                return self._create_reentry_signal('CE', confidence, underlying)
                
        return None

    async def _manage_leg_exit(self, underlying: str) -> Dict:
        """Exit loss-making leg based on current P&L for specific underlying"""
        position_state = self.position_states[underlying]
        
        if position_state['current_position'] == 'BOTH_LEGS':
            ce_pnl = self._calculate_leg_pnl('CE', underlying)
            pe_pnl = self._calculate_leg_pnl('PE', underlying)
            
            # Exit the leg with higher loss
            if ce_pnl < pe_pnl:
                action = "EXIT_CE_LEG"
                new_position = "PE_ONLY"
            else:
                action = "EXIT_PE_LEG" 
                new_position = "CE_ONLY"
        else:
            # Exit remaining leg
            if position_state['current_position'] == 'CE_ONLY':
                action = "EXIT_CE_LEG"
                new_position = 'NONE'
            else:
                action = "EXIT_PE_LEG"
                new_position = 'NONE'
        
        position_state['current_position'] = new_position
        return self._create_leg_signal(action, f"EXIT_LOSSMAKING_LEG_{action}", underlying)

    def _check_stop_loss(self, current_premium: float, underlying: str) -> Optional[Dict]:
        """Check stop loss and trailing stop conditions for specific underlying"""
        position_state = self.position_states[underlying]
        
        if position_state['current_position'] is None:
            return None
            
        entry_premium = position_state['entry_combined_premium']
        current_profit = (entry_premium - current_premium) * self.config.get('lot_size', 50)
        
        # Update trailing SL based on profit levels
        if current_profit >= 1000 and position_state['stop_loss_level'] == 1000:
            position_state['stop_loss_level'] = 500  # Lock in ₹500 profit
        elif current_profit >= 1500:
            position_state['stop_loss_level'] = 1000  # Lock in ₹1000 profit
            
        # Check if stop loss hit
        if current_profit <= -position_state['stop_loss_level']:
            return self._create_exit_signal("STOP_LOSS_HIT", underlying)
            
        return None


    def _create_entry_signal(self, combined_premium: float, confidence: float, underlying: str) -> Dict:
        """Create entry signal for straddle for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            self.logger.warning(f"No ATM strike available for {underlying} entry signal")
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        # Get current underlying price for last_price field
        underlying_price = self._get_current_underlying_price(underlying)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        current_vwap = self._calculate_combined_vwap(underlying)
        
        # Create symbol representation for alerts (combine both legs)
        primary_symbol = f"{underlying} {strike} STRADDLE"
        
        signal = {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,  # Primary symbol for alerts
            "pattern": "kow_signal_straddle",  # Pattern type
            "pattern_type": "kow_signal_straddle",  # Alternative pattern field
            "confidence": confidence,
            "signal": "SELL",  # Signal direction (SELL for straddle)
            "action": "SELL_STRADDLE",  # Specific action
            "last_price": underlying_price,  # Underlying price
            "price": underlying_price,  # Alternative price field
            "timestamp": current_time.isoformat(),  # ISO timestamp
            "timestamp_ms": timestamp_ms,  # Millisecond timestamp for compatibility
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50 if underlying == 'NIFTY' else 15,  # NIFTY: 50, BANKNIFTY: 15
            "entry_premium": combined_premium,
            "current_vwap": current_vwap,
            "underlying": underlying,
            "expiry": expiry,
            "reason": "COMBINED_PREMIUM_BELOW_VWAP",
            "strategy_type": "kow_signal_straddle",
            
            # Additional fields for enrichment
            "volume_ratio": 1.0,  # Placeholder - can be calculated from actual volume
            "expected_move": abs(combined_premium - current_vwap) / current_vwap * 100 if current_vwap > 0 else 0.0
        }
        
        # Update position state for this underlying
        position_state = self.position_states[underlying]
        position_state.update({
            'current_position': 'BOTH_LEGS',
            'entry_time': datetime.now(),
            'entry_combined_premium': combined_premium,
            'reentry_count': 0,
            'current_strike': strike
        })
        
        # Track entry prices for P&L calculation (per underlying - would need to add dict tracking)
        # For now, use current premiums
        if underlying in self.ce_premiums:
            # Would need to track per underlying: self.ce_entry_prices[underlying] = self.ce_premiums[underlying]
            pass
        
        return signal
    

    def _create_reentry_signal(self, leg_type: str, confidence: float, underlying: str) -> Dict:
        """Create re-entry signal for closed leg for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        action = f"REENTER_{leg_type}_LEG"
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {strike} {leg_type} REENTRY"
        
        # Update position back to both legs for this underlying
        position_state = self.position_states[underlying]
        position_state['current_position'] = 'BOTH_LEGS'
        
        return {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": confidence,
            "signal": "REENTER",
            "action": action,
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50 if underlying == 'NIFTY' else 15,  # NIFTY: 50, BANKNIFTY: 15
            "reason": f"REENTER_{leg_type}_BELOW_VWAP",
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            "leg_type": leg_type,
            "volume_ratio": 1.0,
            "expected_move": 0.0
        }
    
    def _create_leg_signal(self, action: str, reason: str, underlying: str) -> Dict:
        """Create leg management signal for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {strike} STRADDLE"
        
        return {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": 0.75,
            "signal": action.split("_")[0] if "_" in action else action,  # Extract signal from action
            "action": action,
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50 if underlying == 'NIFTY' else 15,  # NIFTY: 50, BANKNIFTY: 15
            "reason": reason,
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            "volume_ratio": 1.0,
            "expected_move": 0.0
        }
    
    def _create_exit_signal(self, reason: str, underlying: str) -> Dict:
        """Create full exit signal for specific underlying"""
        if underlying not in self.atm_strikes or not self.atm_strikes[underlying]:
            return None
        
        strike = self.atm_strikes[underlying]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        current_time = datetime.now()
        timestamp_ms = int(current_time.timestamp() * 1000)
        underlying_price = self._get_current_underlying_price(underlying)
        primary_symbol = f"{underlying} {strike} STRADDLE"
        
        signal = {
            # Core fields required by alert manager and notifiers
            "symbol": primary_symbol,
            "pattern": "kow_signal_straddle",
            "pattern_type": "kow_signal_straddle",
            "confidence": 0.8,
            "signal": "EXIT",
            "action": "EXIT_STRADDLE",
            "last_price": underlying_price,
            "price": underlying_price,
            "timestamp": current_time.isoformat(),
            "timestamp_ms": timestamp_ms,
            
            # Strategy-specific fields
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50 if underlying == 'NIFTY' else 15,  # NIFTY: 50, BANKNIFTY: 15
            "reason": reason,
            "underlying": underlying,
            "expiry": expiry,
            "strategy_type": "kow_signal_straddle",
            "volume_ratio": 1.0,
            "expected_move": 0.0
        }
        
        # Reset position state for this underlying
        position_state = self.position_states[underlying]
        position_state.update({
            'current_position': None,
            'entry_time': None,
            'entry_combined_premium': 0.0,
            'reentry_count': 0,
            'stop_loss_level': 1000
        })
        
        # Update Redis state
        self._save_strategy_state_to_redis()
        
        return signal

    def _create_signal(self, action: str, strike: int, reason: str) -> Dict:
        """Create standardized signal format"""
        expiry = self._get_current_expiry()
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        return {
            "action": action,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": self.position_state.get('current_quantity', 50),
            "reason": reason,
            "confidence": 0.75,
            "timestamp": datetime.now().isoformat(),
            "underlying": underlying,
            "expiry": expiry
        }
    
    def _confirm_below_vwap(self, period_minutes: int, underlying: str) -> bool:
        """Check if combined premium remains below VWAP for X minutes for specific underlying"""
        if underlying not in self.combined_premium_data or len(self.combined_premium_data[underlying]) < 5:
            return False
            
        cutoff_time = datetime.now() - timedelta(minutes=period_minutes)
        recent_data = [d for d in self.combined_premium_data[underlying] 
                      if d['timestamp'] >= cutoff_time]
        
        if len(recent_data) < 3:  # Minimum 3 ticks required
            return False
            
        current_vwap = self._calculate_combined_vwap(underlying)
        
        # Check all recent ticks were below VWAP
        return all(d['premium'] < current_vwap for d in recent_data)

    def _confirm_above_vwap(self, period_minutes: int, underlying: str) -> bool:
        """Check if combined premium remains above VWAP for X minutes for specific underlying"""
        if underlying not in self.combined_premium_data or len(self.combined_premium_data[underlying]) < 5:
            return False

        cutoff_time = datetime.now() - timedelta(minutes=period_minutes)
        recent_data = [d for d in self.combined_premium_data[underlying] 
                      if d['timestamp'] >= cutoff_time]
        
        if len(recent_data) < 3:
            return False
            
        current_vwap = self._calculate_combined_vwap(underlying)
        
        return all(d['premium'] > current_vwap for d in recent_data)

    def _calculate_leg_pnl(self, option_type: str, underlying: str) -> float:
        """Calculate current P&L for individual leg for specific underlying"""
        if underlying not in self.ce_premiums or underlying not in self.pe_premiums:
            return 0.0
        
        current_premium = self.ce_premiums[underlying] if option_type == 'CE' else self.pe_premiums[underlying]
        
        # For now, use current premium as entry (would need to track entry prices per underlying)
        # TODO: Track entry prices per underlying: self.ce_entry_prices[underlying]
        if current_premium:
            lot_size = 50 if underlying == 'NIFTY' else 15
            # Simplified P&L calculation (would need entry prices tracked per underlying)
            return 0.0  # Placeholder - would calculate based on entry vs current
        
        return 0.0
    
    def _get_current_underlying_price(self, underlying: str = None) -> float:
        """Get current underlying price from tracked data or Redis for specific underlying"""
        # First try in-memory tracking
        if self.underlying_prices:
            if underlying:
                # Filter by underlying symbol
                matching_prices = [p for p in self.underlying_prices if underlying.upper() in p['symbol'].upper()]
                if matching_prices:
                    return matching_prices[-1]['price']
            else:
                # Return most recent price
                return self.underlying_prices[-1]['price']
        
        # Fallback to Redis (DB 1 - realtime)
        if self.redis_realtime:
            try:
                # Try NIFTY first, then BANKNIFTY (from market:* keys)
                nifty_price = self.redis_realtime.get("market:nifty_spot")
                if nifty_price:
                    if isinstance(nifty_price, bytes):
                        nifty_price = nifty_price.decode('utf-8')
                    return float(nifty_price)
                
                banknifty_price = self.redis_realtime.get("market:banknifty_spot")
                if banknifty_price:
                    if isinstance(banknifty_price, bytes):
                        banknifty_price = banknifty_price.decode('utf-8')
                    return float(banknifty_price)
                
                # Try index data from Redis (ohlc_latest:* keys in DB 2)
                try:
                    from redis_files.redis_manager import RedisManager82
                    redis_analytics = RedisManager82.get_client(process_name="kow_signal_strategy", db=2, decode_responses=False)
                    underlying_symbols = self.config.get('underlying_symbols', ['NIFTY'])
                    underlying = underlying_symbols[0] if underlying_symbols else 'NIFTY'
                    
                    # Try ohlc_latest key pattern
                    from redis_files.redis_ohlc_keys import ohlc_latest_hash, normalize_symbol
                    normalized_symbol = normalize_symbol(f"NSE:{underlying}")
                    ohlc_key = ohlc_latest_hash(normalized_symbol)
                    ohlc_data = redis_analytics.hget(ohlc_key, 'close')
                    if ohlc_data:
                        if isinstance(ohlc_data, bytes):
                            ohlc_data = ohlc_data.decode('utf-8')
                        return float(ohlc_data)
                except Exception as ohlc_error:
                    self.logger.debug(f"Error getting price from ohlc_latest: {ohlc_error}")
                
                # Try legacy index data from Redis
                import json
                index_data = self.redis_realtime.get("index:NSE:NIFTY 50")
                if index_data:
                    if isinstance(index_data, bytes):
                        index_data = index_data.decode('utf-8')
                    index_json = json.loads(index_data)
                    return float(index_json.get('last_price', 0))
            except Exception as e:
                self.logger.debug(f"Error getting underlying price from Redis: {e}")
        
        return 0.0
    
    def _get_current_expiry(self) -> str:
        """Get current expiry in format like '25NOV' or '25DEC'"""
        current_date = datetime.now().date()
        current_year = str(current_date.year)[-2:]  # Last 2 digits
        
        # Determine current month's expiry
        month = current_date.month
        month_names = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 
                      'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
        
        # For NIFTY, expiry is typically last Thursday of the month
        # For simplicity, use current month + year format
        return f"{current_year}{month_names[month-1]}"

    async def _store_signal_in_redis(self, signal: Dict):
        """Store signal in Redis for tracking and dashboard using standard key patterns"""
        try:
            import json
            import orjson
            signal_date = datetime.now().strftime('%Y%m%d')
            signal_time = datetime.now().strftime('%H%M%S')
            
            # Store signal in DB 1 (realtime) for alerts stream
            signal_key = f"signals:kow:{signal_date}:{signal['action'].lower()}"
            
            if self.redis_realtime:
                # Store as JSON string in Redis
                signal_json = json.dumps(signal, default=str)
                self.redis_realtime.set(signal_key, signal_json)
                self.redis_realtime.expire(signal_key, 86400)  # Keep for 24 hours
                
                # Also publish to alerts stream for real-time processing (DB 1 - realtime)
                try:
                    alerts_stream = "alerts:stream"
                    
                    # Serialize signal to binary JSON for alerts:stream (matches alert manager expectations)
                    if ORJSON_AVAILABLE and orjson:
                        try:
                            # Use orjson for faster binary serialization if available
                            signal_binary = orjson.dumps(signal, option=orjson.OPT_SERIALIZE_NUMPY)
                        except Exception:
                            # Fallback to standard json if orjson fails
                            signal_binary = json.dumps(signal, default=str).encode('utf-8')
                    else:
                        # Fallback to standard json
                        signal_binary = json.dumps(signal, default=str).encode('utf-8')
                    
                    # Publish to alerts:stream with binary JSON in 'data' field (standard format)
                    stream_data = {
                        b'data': signal_binary,  # Binary JSON for compatibility with alert manager
                    }
                    # Use DB 1 (realtime) for alerts:stream
                    self.redis_realtime.xadd(alerts_stream, stream_data, maxlen=1000, approximate=True)
                    self.logger.debug(f"✅ Published Kow Signal to alerts:stream: {signal.get('symbol', 'UNKNOWN')}")
                except Exception as e:
                    self.logger.error(f"Error publishing to alerts stream: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
            
            # Store strategy state in DB 0 (system)
            if self.redis_system:
                state_key = "strategy:kow:position"
                self.redis_system.set(state_key, self.position_state.get('current_position', 'NONE'))
                
                if self.position_state.get('entry_combined_premium'):
                    self.redis_system.set("strategy:kow:entry_premium", str(self.position_state['entry_combined_premium']))
                
                if self.current_atm_strike:
                    self.redis_system.set("strategy:kow:current_strike", str(self.current_atm_strike))
                    
        except Exception as e:
            self.logger.error(f"Failed to store signal in Redis: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        
    async def _send_alert(self, signal: Dict):
        """Send alert to Telegram"""
        try:
            # TelegramNotifier.send_alert is synchronous, but we call it from async context
            # Use asyncio.to_thread to avoid blocking
            await asyncio.to_thread(self.notifier.send_alert, signal)
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")
    
    def _is_trading_hours(self) -> bool:
        """Check if current time is within trading hours (9:30 AM - 3:15 PM)"""
        current_time = datetime.now().time()
        return current_time >= dt_time(9, 30) and current_time <= dt_time(15, 15)
    
    def _load_strategy_state_from_redis(self):
        """Load strategy state from Redis on startup"""
        if not self.redis_system:
            return
        
        try:
            position = self.redis_system.get("strategy:kow:position")
            if position:
                self.position_state['current_position'] = position
            
            entry_premium = self.redis_system.get("strategy:kow:entry_premium")
            if entry_premium:
                self.position_state['entry_combined_premium'] = float(entry_premium)
            
            current_strike = self.redis_system.get("strategy:kow:current_strike")
            if current_strike:
                self.current_atm_strike = int(current_strike)
                self.position_state['current_strike'] = int(current_strike)
        except Exception as e:
            self.logger.debug(f"Error loading strategy state from Redis: {e}")
    
    def _save_strategy_state_to_redis(self):
        """Save current strategy state to Redis"""
        if not self.redis_system:
            return
        
        try:
            self.redis_system.set("strategy:kow:position", self.position_state.get('current_position', 'NONE'))
            
            if self.position_state.get('entry_combined_premium'):
                self.redis_system.set("strategy:kow:entry_premium", str(self.position_state['entry_combined_premium']))
            
            if self.current_atm_strike:
                self.redis_system.set("strategy:kow:current_strike", str(self.current_atm_strike))
        except Exception as e:
            self.logger.debug(f"Error saving strategy state to Redis: {e}")
    
    async def start_tick_listener(self):
        """Start listening to Redis tick streams for real-time data"""
        if not self.redis_realtime:
            self.logger.error("Redis client not available, cannot start tick listener")
            return
        
        self.logger.info("Starting Kow Signal Straddle tick listener...")
        
        # Subscribe to tick streams for underlying and options
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        
        while True:
            try:
                # Read from tick streams using XREAD
                streams_to_read = {}
                for underlying in underlying_symbols:
                    streams_to_read[f"ticks:{underlying}"] = "$"  # Read new messages
                
                # Read from streams (blocking for 1 second)
                messages = self.redis_realtime.xread(streams_to_read, count=10, block=1000)
                
                if messages:
                    for stream_name, stream_messages in messages:
                        stream_name_str = stream_name.decode() if isinstance(stream_name, bytes) else stream_name
                        
                        for msg_id, msg_data in stream_messages:
                            try:
                                # Parse tick data from stream - standard format
                                tick_data_raw = msg_data.get(b'data') or msg_data.get('data')
                                if tick_data_raw:
                                    if isinstance(tick_data_raw, bytes):
                                        tick_data = json.loads(tick_data_raw.decode('utf-8'))
                                    else:
                                        tick_data = json.loads(tick_data_raw)
                                    
                                    # Process tick through strategy
                                    await self.on_tick(tick_data)
                            except Exception as e:
                                self.logger.debug(f"Error processing tick from stream {stream_name_str}: {e}")
                
                # Save state periodically
                self._save_strategy_state_to_redis()
                
            except Exception as e:
                self.logger.error(f"Error in tick listener: {e}")
                import traceback
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(1)
    
    @staticmethod
    async def run_strategy():
        """Main entry point to run the strategy"""
        strategy = KowSignalStraddleStrategy()
        await strategy.start_tick_listener()


if __name__ == "__main__":
    asyncio.run(KowSignalStraddleStrategy.run_strategy())
        