# vwap_straddle_strategy.py
import asyncio
import pandas as pd
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple
import logging

from intraday_scanner.math_dispatcher import MathDispatcher
from patterns.pattern_mathematics import PatternMathematics

class VWAPStraddleStrategy:
    """
    Intraday Combined VWAP Straddle Strategy for Nifty/BankNifty
    Integrates with existing Zerodha WebSocket and Redis infrastructure
    """
    
    def __init__(self, redis_client, config: Dict = None):
        self.redis = redis_client
        self.logger = logging.getLogger(__name__)
        
        # Load configuration from thresholds.py
        try:
            from config.thresholds import get_kow_signal_strategy_config
            self.config = get_kow_signal_strategy_config()
        except ImportError:
            # Fallback to provided config or defaults
            self.config = config or {
                'entry_time': '09:30',
                'exit_time': '15:15',
                'confirmation_periods': 1,
                'max_reentries': 2,
                'vwap_period': 20,
                'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
                'strike_selection': 'atm',
                'expiry_preference': 'weekly',
                'max_risk_per_trade': 0.03,
                'position_sizing': 'turtle_trading',
                'confidence_threshold': 0.85,
                'telegram_routing': True
            }
        
        # Strategy state
        self.position_state = {
            'current_position': None,  # 'BOTH_LEGS', 'CE_ONLY', 'PE_ONLY', 'NONE'
            'entry_time': None,
            'entry_combined_premium': 0.0,
            'reentry_count': 0,
            'current_profit': 0.0,
            'stop_loss_level': 0.0
        }
        
        # Data tracking
        self.combined_premium_data = []
        self.vwap_data = []
        self.underlying_prices = []
        
        # Use existing risk manager
        from alerts.risk_manager import RiskManager
        self.risk_manager = RiskManager(redis_client=redis_client)
        self.math_dispatcher = MathDispatcher(PatternMathematics, self.risk_manager)
        self.pattern_math = PatternMathematics
        
        # Use existing notifier
        from alerts.notifiers import TelegramNotifier
        self.notifier = TelegramNotifier()
        
        self.logger.info("VWAP Straddle Strategy initialized")

    def _get_atm_strike(self, underlying_price: float) -> int:
        """Calculate ATM strike based on current underlying price"""
        strike_interval = 50 if "NIFTY" in self.config.get('underlying_symbols', ['NIFTY'])[0] else 100
        return round(underlying_price / strike_interval) * strike_interval

    def _calculate_vwap(self, prices: List[float], volumes: List[int]) -> float:
        """Calculate VWAP from price and volume data"""
        if not prices or not volumes:
            return 0.0
        
        cumulative_volume = sum(volumes)
        if cumulative_volume == 0:
            return prices[-1] if prices else 0.0
            
        cumulative_pv = sum(p * v for p, v in zip(prices, volumes))
        return cumulative_pv / cumulative_volume

    def _get_option_symbols(self, underlying: str, strike: int, expiry: str) -> Tuple[str, str]:
        """Generate CE and PE symbols based on strike and expiry"""
        # Format: NIFTY25OCT25900CE
        ce_symbol = f"{underlying}{expiry}{strike}CE"
        pe_symbol = f"{underlying}{expiry}{strike}PE"
        return ce_symbol, pe_symbol

    async def on_tick(self, tick_data: Dict) -> Optional[Dict]:
        """
        Process incoming tick data from Zerodha WebSocket
        Follows existing on_tick pattern
        """
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
        """Update combined premium and VWAP calculations using existing field names"""
        symbol = tick_data.get('tradingsymbol', '')
        
        # Update underlying price using existing field names
        if any(us in symbol for us in self.config.get('underlying_symbols', ['NIFTY', 'BANKNIFTY'])):
            self.underlying_prices.append({
                'timestamp': tick_data.get('timestamp'),
                'price': tick_data.get('last_price', 0.0),
                'volume': tick_data.get('bucket_incremental_volume', 0)  # Use existing volume field
            })
            
            # Keep only last 1000 ticks for memory management
            if len(self.underlying_prices) > 1000:
                self.underlying_prices.pop(0)

        # Update option premium data (you'll need to track CE and PE separately)
        if 'CE' in symbol or 'PE' in symbol:
            self._update_combined_premium(tick_data)

    def _update_combined_premium(self, tick_data: Dict):
        """Update combined premium calculation for straddle"""
        # This requires tracking both CE and PE premiums for the same strike
        # Implementation depends on how you map CE/PE pairs
        pass

    async def _generate_straddle_signals(self, tick_data: Dict) -> Optional[Dict]:
        """Generate straddle entry/exit signals based on VWAP crossover"""
        current_time = datetime.now().time()
        
        # Only trade between 9:30 AM and 3:15 PM
        if current_time < time(9, 30) or current_time > time(15, 15):
            return None

        # Get current ATM strike
        underlying_price = self._get_current_underlying_price()
        if not underlying_price:
            return None
            
        atm_strike = self._get_atm_strike(underlying_price)
        
        # Calculate combined premium and VWAP
        combined_premium = self._get_combined_premium(atm_strike)
        current_vwap = self._get_current_vwap()
        
        if combined_premium == 0 or current_vwap == 0:
            return None

        confidence = None
        math_context = {
            "pattern_type": "kow_signal_straddle",
            "vwap": current_vwap,
            "combined_premium": combined_premium,
            "underlying_price": underlying_price,
        }
        if self.math_dispatcher:
            try:
                confidence = self.math_dispatcher.calculate_confidence('kow_signal_straddle', math_context)
            except Exception as dispatch_error:
                self.logger.debug(f"MathDispatcher confidence error (KOW): {dispatch_error}")

        if confidence is None:
            confidence = self.config.get('confidence_threshold', 0.85)

        if confidence < self.config.get('confidence_threshold', 0.85):
            return None

        # Generate signals based on strategy rules
        if self.position_state['current_position'] is None:
            return await self._check_entry_signal(combined_premium, current_vwap, atm_strike, confidence)
        else:
            return await self._check_exit_signal(combined_premium, current_vwap, atm_strike, confidence)

    async def _check_entry_signal(self, combined_premium: float, current_vwap: float, strike: int, confidence: float) -> Optional[Dict]:
        """Check for straddle entry signal"""
        # Entry condition: Combined premium crosses below VWAP and remains below for 1 minute
        if combined_premium < current_vwap:
            # Check if it remains below for confirmation period
            if self._confirm_below_vwap(period_minutes=1):
                return self._create_entry_signal(strike, combined_premium, confidence)

        return None

    async def _check_exit_signal(self, combined_premium: float, current_vwap: float, strike: int, confidence: float) -> Optional[Dict]:
        """Check for exit or leg management signals"""
        position = self.position_state['current_position']
        
        # Exit condition: Combined premium crosses above VWAP
        if combined_premium > current_vwap:
            if self._confirm_above_vwap(period_minutes=2):
                return await self._manage_leg_exit(strike)

        # Check stop loss and trailing SL
        sl_signal = self._check_stop_loss(combined_premium)
        if sl_signal:
            return sl_signal

        return None

    async def _manage_leg_exit(self, strike: int) -> Dict:
        """Exit loss-making leg based on current P&L"""
        ce_pnl = self._calculate_leg_pnl(strike, 'CE')
        pe_pnl = self._calculate_leg_pnl(strike, 'PE')
        
        # Exit the leg with higher loss
        if ce_pnl < pe_pnl:
            action = "EXIT_CE_LEG"
            new_position = "PE_ONLY"
        else:
            action = "EXIT_PE_LEG" 
            new_position = "CE_ONLY"
            
        # Update position state
        self.position_state['current_position'] = new_position
        self.position_state['reentry_count'] = 0
        
        return self._create_signal(action, strike, f"EXIT_LOSSMAKING_LEG_{action}")

    def _check_stop_loss(self, current_premium: float) -> Optional[Dict]:
        """Check stop loss and trailing stop conditions"""
        entry_premium = self.position_state['entry_combined_premium']
        current_profit = entry_premium - current_premium  # Since we're selling
        
        # Convert to monetary value (Nifty lot size = 50)
        profit_rs = current_profit * 50
        
        # Update trailing SL based on profit levels
        if profit_rs >= 1000 and self.position_state['stop_loss_level'] == 0:
            self.position_state['stop_loss_level'] = 500
        elif profit_rs >= 1500:
            self.position_state['stop_loss_level'] = 1000
            
        # Check if stop loss hit
        if profit_rs <= -self.position_state.get('stop_loss_level', 1000):
            return self._create_exit_signal("STOP_LOSS_HIT")
            
        return None

    def _create_entry_signal(self, strike: int, combined_premium: float, confidence: float) -> Dict:
        """Create entry signal using unified pattern schema with enhanced validation"""
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols("NIFTY", strike, expiry)
        
        # Calculate position size using existing risk management
        position_size = self.risk_manager.calculate_turtle_position_size(
            "NIFTY",
            self._get_atr_data(),
            {"pattern": "kow_signal_straddle", "confidence": confidence}
        )
        
        # Use unified pattern schema
        from patterns.utils.pattern_schema import create_pattern
        
        signal = create_pattern(
            symbol="NIFTY",
            pattern_type="kow_signal_straddle",
            signal="BUY",
            confidence=confidence,
            last_price=self._get_current_underlying_price(),
            expected_move=combined_premium * 0.5,
            target_price=combined_premium * 0.5,
            stop_loss=combined_premium * 1.0,
            # Enhanced fields for straddle strategy
            action="SELL_STRADDLE",
            position_size=position_size,
            strike=strike,
            ce_symbol=ce_symbol,
            pe_symbol=pe_symbol,
            entry_premium=combined_premium,
            underlying="NIFTY",
            expiry=expiry,
            strategy_type="straddle",
            vwap_price=self._get_current_vwap(),
            reason="COMBINED_PREMIUM_BELOW_VWAP"
        )
        
        # Update position state
        self.position_state.update({
            'current_position': 'BOTH_LEGS',
            'entry_time': datetime.now(),
            'entry_combined_premium': combined_premium,
            'reentry_count': 0,
            'stop_loss_level': 1000  # Initial SL of ₹1000
        })
        
        return signal

    def _create_signal(self, action: str, strike: int, reason: str) -> Dict:
        """Create standardized signal format"""
        expiry = self._get_current_expiry()
        ce_symbol, pe_symbol = self._get_option_symbols("NIFTY", strike, expiry)
        
        return {
            "action": action,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "quantity": self.position_state.get('current_quantity', 50),
            "reason": reason,
            "confidence": 0.75,
            "timestamp": datetime.now().isoformat(),
            "underlying": "NIFTY",
            "expiry": expiry
        }

    # Helper methods (implement based on your existing data)
    def _get_current_underlying_price(self) -> float:
        """Get current underlying price from tracked data"""
        if self.underlying_prices:
            return self.underlying_prices[-1]['price']
        return 0.0

    def _get_combined_premium(self, strike: int) -> float:
        """Get combined premium for given strike"""
        # Implement based on your CE/PE premium tracking
        return 0.0

    def _get_current_vwap(self) -> float:
        """Get current VWAP value"""
        if not self.underlying_prices:
            return 0.0
            
        prices = [p['price'] for p in self.underlying_prices[-20:]]  # Last 20 periods
        volumes = [p['volume'] for p in self.underlying_prices[-20:]]
        
        return self._calculate_vwap(prices, volumes)

    def _confirm_below_vwap(self, period_minutes: int) -> bool:
        """Check if price remains below VWAP for specified period"""
        # Implement time-based confirmation logic
        return True

    def _confirm_above_vwap(self, period_minutes: int) -> bool:
        """Check if price remains above VWAP for specified period"""
        # Implement time-based confirmation logic  
        return True

    def _calculate_leg_pnl(self, strike: int, option_type: str) -> float:
        """Calculate P&L for individual leg"""
        # Implement P&L calculation
        return 0.0

    def _get_current_expiry(self) -> str:
        """Get current expiry in format like '25OCT'"""
        # Implement based on your expiry logic
        return "25OCT"

    def _get_atr_data(self) -> Dict:
        """Get ATR data for position sizing"""
        # Implement or use existing ATR calculation
        return {}

    async def _send_alert(self, signal: Dict):
        """Send alert using existing notifier"""
        try:
            await self.notifier.send_alert(signal)
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")

    def _is_trading_hours(self) -> bool:
        """Check if current time is within trading hours"""
        current_time = datetime.now().time()
        return time(9, 15) <= current_time <= time(15, 30)
