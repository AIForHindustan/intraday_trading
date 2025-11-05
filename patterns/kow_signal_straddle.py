''' 
Kow Signal Straddle Strategy: A VWAP-Based Options Trading System

Strategy Overview

The Kow Signal Straddle is an intraday options trading strategy designed for NIFTY and BANKNIFTY indices. It uses Volume Weighted Average Price (VWAP) as the primary signal generator to identify optimal entry and exit points for At-The-Money (ATM) straddles.

Core Concept

A straddle involves simultaneously buying (or selling) both a Call Option (CE) and Put Option (PE) at the same strike price. The strategy profits when the underlying moves significantly in either direction.

The Kow Signal strategy specifically:
- Sells ATM straddles when combined premium (CE + PE) drops below VWAP
- Uses VWAP as a dynamic reference point for fair value
- Trades only during active market hours (9:30 AM - 3:15 PM)
- Requires high confidence (85%+) before generating signals
'''
import asyncio
import pandas as pd
from typing import Dict, Optional, Tuple
import logging
import time
import json
from patterns.utils.pattern_schema import create_pattern
from intraday_scanner.math_dispatcher import MathDispatcher
from patterns.pattern_mathematics import PatternMathematics
from config.thresholds import get_kow_signal_strategy_config
from datetime import datetime, time as dt_time, timedelta
from alerts.risk_manager import RiskManager
from alerts.notifiers import TelegramNotifier

class KowSignalStraddleStrategy:
    """
    Intraday Combined VWAP Straddle Strategy for Nifty/BankNifty
    Uses VWAP of combined premium (CE+PE) with session anchoring
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
        self.config = config or get_kow_signal_strategy_config()

        # Load configuration
        try:
            from config.thresholds import get_kow_signal_strategy_config
            self.config = get_kow_signal_strategy_config()
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
        
        # Strategy state - will be loaded from Redis on startup
        self.position_state = {
            'current_position': None,  # 'BOTH_LEGS', 'CE_ONLY', 'PE_ONLY', 'NONE'
            'entry_time': None,
            'entry_combined_premium': 0.0,
            'reentry_count': 0,
            'current_profit': 0.0,
            'stop_loss_level': 1000,
            'current_strike': None
        }
        
        # Data tracking for combined premium VWAP
        self.combined_premium_data = []  # Stores {'timestamp', 'premium', 'volume'}
        self.ce_premium = None
        self.pe_premium = None
        self.ce_volume = 0
        self.pe_volume = 0
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

    def _get_atm_strike(self, underlying_price: float) -> int:
        """Calculate ATM strike based on current underlying price"""
        strike_interval = 50 if "NIFTY" in self.config.get('underlying_symbols', ['NIFTY'])[0] else 100
        return round(underlying_price / strike_interval) * strike_interval

    def _calculate_combined_vwap(self) -> float:
        """Calculate VWAP for combined premium from session start (9:15 AM)"""
        if not self.combined_premium_data:
            return 0.0
        
        # Filter data from session start (9:15 AM)
        session_start = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
        session_data = [d for d in self.combined_premium_data 
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
    
    def _is_atm_option(self, symbol: str, strike: int) -> bool:
        """Check if symbol matches current ATM strike"""
        return str(strike) in symbol

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
        symbol = tick_data.get('tradingsymbol', '')
        
        # Check if it's underlying index or options
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol for us in underlying_symbols)
        is_option = 'CE' in symbol or 'PE' in symbol
        
        return is_underlying or is_option

    def _update_market_data(self, tick_data: Dict):
        """Update market data for combined premium calculation using standard field names"""
        # Use standard field names from optimized_field_mapping.yaml
        symbol = tick_data.get('tradingsymbol', '')
        last_price = tick_data.get('last_price', 0.0)
        volume = tick_data.get('volume', tick_data.get('bucket_incremental_volume', 0))
        
        # Update underlying price to determine ATM strike
        underlying_symbols = self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])
        is_underlying = any(us in symbol.upper() for us in underlying_symbols)
        
        if is_underlying and last_price > 0:
            self.current_atm_strike = self._get_atm_strike(last_price)
            
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
            if self.redis_realtime:
                try:
                    # Store spot price: market:nifty_spot or market:banknifty_spot
                    underlying_name = 'NIFTY' if 'NIFTY' in symbol.upper() and 'BANK' not in symbol.upper() else 'BANKNIFTY'
                    spot_key = f"market:{underlying_name.lower()}_spot"
                    self.redis_realtime.set(spot_key, str(last_price))
                    self.redis_realtime.expire(spot_key, 60)  # 60 second TTL
                except Exception as e:
                    self.logger.debug(f"Error storing spot price: {e}")

        # Update option premium data for current ATM strike
        if self.current_atm_strike and self._is_atm_option(symbol, self.current_atm_strike):
            self._update_combined_premium(tick_data)

    def _update_combined_premium(self, tick_data: Dict):
        """Track combined premium (CE + PE) for ATM straddle using standard field names"""
        symbol = tick_data.get('tradingsymbol', '')
        premium = tick_data.get('last_price', 0)
        volume = tick_data.get('volume', tick_data.get('bucket_incremental_volume', 0))
        
        # Determine instrument type from symbol
        instrument_type = 'CE' if 'CE' in symbol else 'PE' if 'PE' in symbol else 'UNDERLYING'
        
        if 'CE' in symbol and premium > 0:
            self.ce_premium = premium
            self.ce_volume = volume
            # Store option price in Redis using standard key pattern
            if self.redis_realtime and self.current_atm_strike:
                option_key = f"options:nifty:{self.current_atm_strike}CE:last_price"
                self.redis_realtime.set(option_key, str(premium))
                self.redis_realtime.set(f"options:nifty:{self.current_atm_strike}CE:volume", str(volume))
                
        elif 'PE' in symbol and premium > 0:
            self.pe_premium = premium
            self.pe_volume = volume
            # Store option price in Redis using standard key pattern
            if self.redis_realtime and self.current_atm_strike:
                option_key = f"options:nifty:{self.current_atm_strike}PE:last_price"
                self.redis_realtime.set(option_key, str(premium))
                self.redis_realtime.set(f"options:nifty:{self.current_atm_strike}PE:volume", str(volume))
        
        # Calculate combined premium when both available
        if self.ce_premium and self.pe_premium and self.ce_premium > 0 and self.pe_premium > 0:
            combined_premium = self.ce_premium + self.pe_premium
            total_volume = (self.ce_volume + self.pe_volume) // 2  # Average volume
            
            self.combined_premium_data.append({
                'timestamp': datetime.now(),
                'premium': combined_premium,
                'volume': total_volume
            })
            
            # Keep only last 4 hours of data
            self._clean_old_premium_data()
        
    def _clean_old_premium_data(self):
        """Remove data older than 4 hours to manage memory"""
        cutoff_time = datetime.now() - timedelta(hours=4)
        self.combined_premium_data = [
            d for d in self.combined_premium_data 
            if d['timestamp'] > cutoff_time
        ]
    
    def _get_current_combined_premium(self) -> float:
        """Get current combined premium"""
        if self.ce_premium and self.pe_premium:
            return self.ce_premium + self.pe_premium
        return 0.0


    async def _generate_straddle_signals(self, tick_data: Dict) -> Optional[Dict]:
        """Generate straddle entry/exit signals based on combined premium VWAP"""
        current_time = datetime.now().time()
        
        # Only trade between 9:30 AM and 3:15 PM
        if current_time < dt_time(9, 30) or current_time > dt_time(15, 15):
            return None

        # Get current combined premium and VWAP
        combined_premium = self._get_current_combined_premium()
        current_vwap = self._calculate_combined_vwap()
        
        if combined_premium == 0 or current_vwap == 0:
            return None

        # Calculate confidence
        confidence = self._calculate_confidence(combined_premium, current_vwap)

        if confidence < self.config.get('confidence_threshold', 0.85):
            return None

        # Generate signals based on position state
        if self.position_state['current_position'] is None:
            return await self._check_entry_signal(combined_premium, current_vwap, confidence)
        elif self.position_state['current_position'] == 'BOTH_LEGS':
            return await self._check_exit_signal(combined_premium, current_vwap, confidence)
        else:  # Single leg position
            # Check re-entry first, then exit
            reentry_signal = await self._check_reentry_signal(combined_premium, current_vwap, confidence)
            if reentry_signal:
                return reentry_signal
            return await self._check_exit_signal(combined_premium, current_vwap, confidence)
    

    def _calculate_confidence(self, combined_premium: float, current_vwap: float) -> float:
        """Calculate confidence score for the signal"""
        try:
            math_context = {
                "pattern_type": "kow_signal_straddle",
                "vwap": current_vwap,
                "combined_premium": combined_premium,
                "underlying_price": self._get_current_underlying_price(),
            }
            if self.math_dispatcher:
                return self.math_dispatcher.calculate_confidence('kow_signal_straddle', math_context)
        except Exception as e:
            self.logger.debug(f"MathDispatcher error: {e}")
        
        # Fallback confidence calculation
        premium_ratio = abs(combined_premium - current_vwap) / current_vwap
        base_confidence = 0.9 if premium_ratio > 0.02 else 0.7
        return min(base_confidence, 0.95)

    async def _check_entry_signal(self, combined_premium: float, current_vwap: float, confidence: float) -> Optional[Dict]:
        """Check for straddle entry signal"""
        # Entry condition: Combined premium below VWAP with 1-minute confirmation
        if combined_premium < current_vwap:
            if self._confirm_below_vwap(period_minutes=1):
                return self._create_entry_signal(combined_premium, confidence)
        return None


    async def _check_exit_signal(self, combined_premium: float, current_vwap: float, confidence: float) -> Optional[Dict]:
        """Check for exit or leg management signals"""
        # Exit condition: Combined premium above VWAP with 2-minute confirmation
        if combined_premium > current_vwap:
            if self._confirm_above_vwap(period_minutes=2):
                return await self._manage_leg_exit()

        # Check stop loss
        sl_signal = self._check_stop_loss(combined_premium)
        if sl_signal:
            return sl_signal

        return None
    
    async def _check_reentry_signal(self, combined_premium: float, current_vwap: float, confidence: float) -> Optional[Dict]:
        """Check if we should re-enter a previously exited leg"""
        current_position = self.position_state['current_position']
        
        if combined_premium < current_vwap and self._confirm_below_vwap(period_minutes=1):
            if current_position == 'CE_ONLY' and self.pe_exit_time:
                # Re-enter PE leg
                return self._create_reentry_signal('PE', confidence)
            elif current_position == 'PE_ONLY' and self.ce_exit_time:
                # Re-enter CE leg
                return self._create_reentry_signal('CE', confidence)
                
        return None

    async def _manage_leg_exit(self) -> Dict:
        """Exit loss-making leg based on current P&L"""
        if self.position_state['current_position'] == 'BOTH_LEGS':
            ce_pnl = self._calculate_leg_pnl('CE')
            pe_pnl = self._calculate_leg_pnl('PE')
            
            # Exit the leg with higher loss
            if ce_pnl < pe_pnl:
                action = "EXIT_CE_LEG"
                new_position = "PE_ONLY"
                self.ce_exit_time = datetime.now()
            else:
                action = "EXIT_PE_LEG" 
                new_position = "CE_ONLY"
                self.pe_exit_time = datetime.now()
        else:
            # Exit remaining leg
            if self.position_state['current_position'] == 'CE_ONLY':
                action = "EXIT_CE_LEG"
                new_position = 'NONE'
            else:
                action = "EXIT_PE_LEG"
                new_position = 'NONE'
        
        self.position_state['current_position'] = new_position
        return self._create_leg_signal(action, f"EXIT_LOSSMAKING_LEG_{action}")

    def _check_stop_loss(self, current_premium: float) -> Optional[Dict]:
        """Check stop loss and trailing stop conditions"""
        if self.position_state['current_position'] is None:
            return None
            
        entry_premium = self.position_state['entry_combined_premium']
        current_profit = (entry_premium - current_premium) * self.config.get('lot_size', 50)
        
        # Update trailing SL based on profit levels
        if current_profit >= 1000 and self.position_state['stop_loss_level'] == 1000:
            self.position_state['stop_loss_level'] = 500  # Lock in ₹500 profit
        elif current_profit >= 1500:
            self.position_state['stop_loss_level'] = 1000  # Lock in ₹1000 profit
            
        # Check if stop loss hit
        if current_profit <= -self.position_state['stop_loss_level']:
            return self._create_exit_signal("STOP_LOSS_HIT")
            
        return None


    def _create_entry_signal(self, combined_premium: float, confidence: float) -> Dict:
        """Create entry signal for straddle"""
        strike = self.current_atm_strike
        if not strike:
            self.logger.warning("No ATM strike available for entry signal")
            return None
        
        # Get underlying symbol from config
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        signal = {
            "action": "SELL_STRADDLE",
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50,  # Nifty lot size
            "entry_premium": combined_premium,
            "current_vwap": self._calculate_combined_vwap(),
            "underlying": underlying,
            "expiry": expiry,
            "reason": "COMBINED_PREMIUM_BELOW_VWAP",
            "confidence": confidence,
            "timestamp": datetime.now().isoformat(),
            "strategy_type": "kow_signal_straddle"
        }
        
        if signal is None:
            return None
        
        # Update position state
        self.position_state.update({
            'current_position': 'BOTH_LEGS',
            'entry_time': datetime.now(),
            'entry_combined_premium': combined_premium,
            'reentry_count': 0,
            'current_strike': strike
        })
        
        # Track entry prices for P&L calculation
        self.ce_entry_price = self.ce_premium
        self.pe_entry_price = self.pe_premium
        
        return signal
    

    def _create_reentry_signal(self, leg_type: str, confidence: float) -> Dict:
        """Create re-entry signal for closed leg"""
        strike = self.current_atm_strike
        if not strike:
            return None
        
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        action = f"REENTER_{leg_type}_LEG"
        
        # Update position back to both legs
        self.position_state['current_position'] = 'BOTH_LEGS'
        
        # Clear exit tracking for this leg
        if leg_type == 'CE':
            self.ce_entry_price = self.ce_premium
            self.ce_exit_time = None
        else:
            self.pe_entry_price = self.pe_premium
            self.pe_exit_time = None
        
        return {
            "action": action,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50,
            "reason": f"REENTER_{leg_type}_BELOW_VWAP",
            "confidence": confidence,
            "timestamp": datetime.now().isoformat(),
            "underlying": underlying,
            "expiry": expiry
        }
    
    def _create_leg_signal(self, action: str, reason: str) -> Dict:
        """Create leg management signal"""
        strike = self.current_atm_strike
        if not strike:
            return None
        
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        return {
            "action": action,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50,
            "reason": reason,
            "confidence": 0.75,
            "timestamp": datetime.now().isoformat(),
            "underlying": underlying,
            "expiry": expiry
        }
    
    def _create_exit_signal(self, reason: str) -> Dict:
        """Create full exit signal"""
        strike = self.current_atm_strike
        if not strike:
            return None
        
        underlying = self.config.get('underlying_symbols', ['NIFTY'])[0]
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols(underlying, strike, expiry)
        
        signal = {
            "action": "EXIT_STRADDLE",
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": 50,
            "reason": reason,
            "confidence": 0.8,
            "timestamp": datetime.now().isoformat(),
            "underlying": underlying,
            "expiry": expiry
        }
        
        # Reset position state
        self.position_state.update({
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
    
    def _confirm_below_vwap(self, period_minutes: int) -> bool:
        """Check if combined premium remains below VWAP for X minutes"""
        if len(self.combined_premium_data) < 5:
            return False
            
        cutoff_time = datetime.now() - timedelta(minutes=period_minutes)
        recent_data = [d for d in self.combined_premium_data 
                      if d['timestamp'] >= cutoff_time]
        
        if len(recent_data) < 3:  # Minimum 3 ticks required
            return False
            
        current_vwap = self._calculate_combined_vwap()
        
        # Check all recent ticks were below VWAP
        return all(d['premium'] < current_vwap for d in recent_data)

    def _confirm_above_vwap(self, period_minutes: int) -> bool:
        """Check if combined premium remains above VWAP for X minutes"""
        if len(self.combined_premium_data) < 5:
            return False

        cutoff_time = datetime.now() - timedelta(minutes=period_minutes)
        recent_data = [d for d in self.combined_premium_data 
                      if d['timestamp'] >= cutoff_time]
        
        if len(recent_data) < 3:
            return False
            
        current_vwap = self._calculate_combined_vwap()
        
        return all(d['premium'] > current_vwap for d in recent_data)

    def _calculate_leg_pnl(self, option_type: str) -> float:
        """Calculate current P&L for individual leg"""
        current_premium = self.ce_premium if option_type == 'CE' else self.pe_premium
        entry_price = self.ce_entry_price if option_type == 'CE' else self.pe_entry_price
        
        if entry_price and current_premium:
            return (entry_price - current_premium) * 50  # Profit for sold options
        return 0.0
    
    def _get_current_underlying_price(self) -> float:
        """Get current underlying price from tracked data or Redis"""
        # First try in-memory tracking
        if self.underlying_prices:
            return self.underlying_prices[-1]['price']
        
        # Fallback to Redis
        if self.redis_realtime:
            try:
                # Try NIFTY first, then BANKNIFTY
                nifty_price = self.redis_realtime.get("market:nifty_spot")
                if nifty_price:
                    return float(nifty_price)
                
                banknifty_price = self.redis_realtime.get("market:banknifty_spot")
                if banknifty_price:
                    return float(banknifty_price)
                
                # Try index data from Redis
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
            signal_date = datetime.now().strftime('%Y%m%d')
            signal_time = datetime.now().strftime('%H%M%S')
            
            # Store signal in DB 1 (realtime) for alerts stream
            signal_key = f"signals:kow:{signal_date}:{signal['action'].lower()}"
            
            if self.redis_realtime:
                # Store as JSON string in Redis
                signal_json = json.dumps(signal, default=str)
                self.redis_realtime.set(signal_key, signal_json)
                self.redis_realtime.expire(signal_key, 86400)  # Keep for 24 hours
                
                # Also publish to alerts stream for real-time processing
                try:
                    alerts_stream = "alerts:stream"
                    stream_data = {
                        'data': signal_json,
                        'timestamp': str(int(time.time() * 1000)),
                        'pattern': 'kow_signal_straddle',
                        'symbol': signal.get('ce_symbol', 'UNKNOWN'),
                        'strategy': 'kow_signal_straddle'
                    }
                    self.redis_realtime.xadd(alerts_stream, stream_data, maxlen=10000, approximate=True)
                except Exception as e:
                    self.logger.debug(f"Error publishing to alerts stream: {e}")
            
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
            await self.notifier.send_alert(signal)
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
        