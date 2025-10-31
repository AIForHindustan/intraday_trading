# Position sizing, stop-loss logic

import logging
from typing import Dict, Optional, Any, List
from datetime import datetime, time, timedelta
import pytz
import json

from intraday_scanner.math_dispatcher import MathDispatcher
from patterns.pattern_mathematics import PatternMathematics

logger = logging.getLogger(__name__)

# Import Redis client for real-time data access
try:
    from redis_files.redis_client import get_redis_client
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis client not available - using fallback data")


class RiskManager:
    """
    Risk management module for position sizing and risk metrics
    """
    
    def __init__(self, config=None, redis_client=None):
        """Initialize risk manager with configuration and Redis client"""
        self.config = config or {}
        self.redis_client = redis_client or (get_redis_client() if REDIS_AVAILABLE else None)
        
        # Account parameters
        self.account_balance = self.config.get('account_balance', 500000)  # ₹5 lakh account
        self.max_position_size = self.config.get('max_position_size', 150000)  # ₹1.5 lakh max per position
        self.max_risk_per_trade = self.config.get('max_risk_per_trade', 0.03)  # 3% risk per trade
        self.max_portfolio_risk = self.config.get('max_portfolio_risk', 0.06)  # 6% total portfolio risk
        self.default_stop_loss_pct = self.config.get('default_stop_loss', 0.02)  # 2% stop loss
        
        # Turtle Trading parameters
        self.turtle_risk_per_trade = 0.03  # 3% risk per trade
        self.turtle_max_units = 4  # Maximum 4 units per position
        self.turtle_add_interval = 0.5  # Add at 0.5N intervals
        self.turtle_stop_loss = 2.0  # Stop loss at 2N

        self.math_dispatcher = MathDispatcher(PatternMathematics, None)
        
        # Contract multipliers for different asset classes
        self.contract_multipliers = {
            'NIFTY': 50,
            'BANKNIFTY': 25,
            'FINNIFTY': 40,
            'SENSEX': 10,
            'BANKEX': 5,
            'OPTIONS': 100,  # All options
            'EQUITY': 1,     # Cash equity
            'FUTURES': 1     # Stock futures
        }
        
        # Leverage limits
        self.max_leverage = {
            'equity': 1.0,      # No leverage for equity
            'futures': 2.0,     # 2x leverage for futures
            'options': 0.5      # 50% allocation for options
        }
        
        # Liquidity classification factors
        self.liquidity_factors = {
            'HFT': 1.2,        # High-frequency dominated
            'Institutional': 1.0,
            'Retail': 0.8      # Retail-dominated
        }
    
    @staticmethod
    def _normalise_expected_move(value: Any) -> float:
        """Convert expected move to decimal representation."""
        try:
            move = float(value)
        except (TypeError, ValueError):
            return 0.0
        # Pattern detector now sends percentage values (1.0 = 1%, 2.5 = 2.5%)
        # Always convert to decimal (1.0 -> 0.01, 2.5 -> 0.025)
        return move / 100.0
        
    def calculate_risk_metrics(self, pattern_data: Dict) -> Dict:
        """
        Calculate risk metrics for a detected pattern using real-time Redis data
        
        Args:
            pattern_data: Pattern detection data with price, volume, confidence
            
        Returns:
            Pattern data enriched with risk metrics
        """
        symbol = pattern_data.get('symbol', '')
        try:
            # Get real-time price from Redis if not provided
            price = float(pattern_data.get('price') or pattern_data.get('last_price') or 0.0)
            if price <= 0:
                # Try to get current price from Redis
                redis_price = self._get_current_price(symbol)
                if redis_price > 0:
                    price = redis_price
                    pattern_data['last_price'] = price
                    logger.debug(f"Updated price for {symbol} from Redis: {price}")
                else:
                    logger.warning(f"No price available for {symbol} - using fallback")
                    price = 100.0  # Fallback price for calculation
                    pattern_data['last_price'] = price

            confidence = float(pattern_data.get('confidence', 0.0))
            pattern = pattern_data.get('pattern', '')
            signal = (pattern_data.get('signal') or 'BULLISH').upper()

            expected_move_pct = self._normalise_expected_move(pattern_data.get('expected_move', 0.0))
            
            # Get real-time market data for enhanced risk calculation
            market_data = self._get_market_data(symbol)
            if market_data:
                pattern_data['market_data'] = market_data
            if expected_move_pct == 0.0:
                expected_move_pct = self._normalise_expected_move(pattern_data.get('price_change'))
            expected_move_pct = abs(expected_move_pct)

            direction_factor = -1 if signal in ('BEARISH', 'SELL', 'SHORT') else 1

            position_size = self._calculate_position_size(price, confidence, pattern_data)
            stop_loss = self._calculate_stop_loss(price, pattern, signal, pattern_data)
            if isinstance(pattern_data, dict):
                pattern_data['calculated_stop_loss'] = stop_loss
            target_price = price * (1 + direction_factor * expected_move_pct)

            quantity = position_size / price if price > 0 else 0
            stop_distance = abs(price - stop_loss)
            reward_distance = abs(target_price - price)

            risk_amount = quantity * stop_distance
            reward_amount = quantity * reward_distance
            rr_ratio = reward_amount / risk_amount if risk_amount > 0 else 0

            pattern_data['risk_metrics'] = {
                'position_size': position_size,
                'stop_loss': stop_loss,
                'target_price': target_price,
                'risk_amount': risk_amount,
                'reward_amount': reward_amount,
                'rr_ratio': rr_ratio,
                'max_loss': risk_amount,
                'expected_move_percent': expected_move_pct * 100.0,
                'direction': signal,
            }
            
            logger.debug(f"Risk metrics calculated for {symbol}: position_size={position_size:.0f}, stop_loss={stop_loss:.2f}, target_price={target_price:.2f}, rr_ratio={rr_ratio:.2f}")

            if quantity > 0 and stop_loss > 0 and target_price > 0:
                pattern_data['risk_summary'] = (
                    f"Qty {quantity:.1f} | Target ₹{target_price:.2f} | SL ₹{stop_loss:.2f} | RR {rr_ratio:.2f}x"
                )
            
            risk_score = self._calculate_risk_score(pattern_data)
            pattern_data['risk_score'] = risk_score
            
            # Add trade recommendation
            if risk_score < 30 and confidence > 0.7 and rr_ratio > 2:
                pattern_data['recommendation'] = 'STRONG_BUY' if direction_factor > 0 else 'STRONG_SELL'
            elif risk_score < 50 and confidence > 0.6 and rr_ratio > 1.5:
                pattern_data['recommendation'] = 'BUY' if direction_factor > 0 else 'SELL'
            elif risk_score > 70 or confidence < 0.4:
                pattern_data['recommendation'] = 'AVOID'
            else:
                pattern_data['recommendation'] = 'HOLD'
                
        except Exception as e:
            logger.error(f"Error calculating risk metrics for {symbol}: {e}")
            logger.error(f"Pattern data keys: {list(pattern_data.keys())}")
            logger.error(f"Pattern data values: {pattern_data}")
            pattern_data['risk_metrics'] = {
                'error': str(e),
                'position_size': 0,
                'stop_loss': 0
            }
            
        return pattern_data

    def get_kelly_position_size(self, pattern_data: Dict) -> Dict:
        """
        Public method to get Kelly Criterion position sizing for a pattern
        
        Args:
            pattern_data: Pattern data with price, confidence, pattern type
            
        Returns:
            Dictionary with Kelly Criterion position sizing details
        """
        try:
            price = pattern_data.get('price', 0)
            confidence = pattern_data.get('confidence', 0)
            
            if price <= 0:
                return {'error': 'Invalid price', 'position_size': 0}
            
            kelly_result = self._kelly_position_size(price, confidence, pattern_data)
            return kelly_result or {'error': 'Kelly calculation failed', 'position_size': 0}
            
        except Exception as e:
            logger.error(f"Error getting Kelly position size: {e}")
            return {'error': str(e), 'position_size': 0}
        
    def _calculate_position_size(self, price: float, confidence: float, pattern_data: Dict = None) -> float:
        """Calculate position size using dispatcher with layered fallbacks."""
        if price <= 0:
            return 0

        pattern_type = (pattern_data or {}).get('pattern', 'generic')
        stop_loss = (pattern_data or {}).get('calculated_stop_loss') or (pattern_data or {}).get('stop_loss')
        context = {
            "pattern_type": pattern_type,
            "entry_price": price,
            "stop_loss": stop_loss,
            "account_size": self.account_balance,
            "risk_per_trade": self.max_risk_per_trade,
            "confidence": confidence,
        }

        if stop_loss and self.math_dispatcher:
            try:
                dispatched_size = self.math_dispatcher.calculate_position_size(pattern_type, context)
                if dispatched_size:
                    return dispatched_size
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher position size error for {pattern_type}: {dispatch_error}")

        # Check if we should trade cash equity (only if expected gain > 2%)
        if pattern_data and not self.should_trade_cash_equity(pattern_data):
            return 0  # Reject cash equity with low expected gain
        
        # Try Turtle Trading first if we have ATR data
        if pattern_data and 'atr' in pattern_data:
            atr_data = pattern_data['atr']
            if atr_data.get('turtle_ready', False) and atr_data.get('atr_20', 0) > 0:
                turtle_result = self.calculate_turtle_position_size(
                    pattern_data.get('symbol', ''), 
                    atr_data, 
                    pattern_data
                )
                if turtle_result.get('position_size', 0) > 0:
                    return turtle_result['position_size']
        
        # Try Kelly Criterion if we have pattern data
        if pattern_data:
            kelly_result = self._kelly_position_size(price, confidence, pattern_data)
            if kelly_result and kelly_result['position_size'] > 0:
                return kelly_result['position_size']
        
        # Fallback to confidence-based sizing
        base_size = self.max_position_size * 0.5  # Start with 50% of max
        
        # Adjust based on confidence (0.5 to 1.0 multiplier)
        confidence_multiplier = 0.5 + (confidence * 0.5)
        
        # Calculate final position size
        position_size = base_size * confidence_multiplier
        
        # Apply max limits
        position_size = min(position_size, self.max_position_size)
        
        # Apply risk adjustments based on pattern type
        if pattern_data:
            pattern_type = pattern_data.get('pattern', 'unknown')
            risk_adjusted_size = self._apply_pattern_risk_adjustments(position_size, pattern_type)
            if risk_adjusted_size != position_size:
                logger.info(f"Applied risk adjustment for {pattern_type}: {position_size:.0f} → {risk_adjusted_size:.0f}")
                position_size = risk_adjusted_size

        return position_size

    def _apply_pattern_risk_adjustments(self, position_size: float, pattern_type: str) -> float:
        """
        Apply pattern-specific risk adjustments to position size
        
        Args:
            position_size: Original position size
            pattern_type: Pattern type
            
        Returns:
            Risk-adjusted position size
        """
        try:
            # Import risk adjustment functions from config.thresholds
            from config.thresholds import get_risk_adjusted_position_multiplier
            
            # Get risk-adjusted multiplier for this pattern type
            risk_multiplier = get_risk_adjusted_position_multiplier(pattern_type)
            
            # Apply the multiplier
            adjusted_size = position_size * risk_multiplier
            
            # Round to nearest 1000
            adjusted_size = round(adjusted_size / 1000) * 1000
            
            return adjusted_size
            
        except Exception as e:
            logger.error(f"Error applying pattern risk adjustments: {e}")
            return position_size  # Return original size if adjustment fails

    def calculate_turtle_position_size(self, symbol: str, atr_data: Dict = None, pattern_data: Dict = None) -> Dict:
        """
        Calculate Turtle Trading position size using ATR and contract multipliers
        
        Formula: Position Size = (Account Risk × Account Balance) ÷ (N × Contract Size)
        Where N = ATR (20-day)
        
        Args:
            symbol: Trading symbol
            atr_data: ATR data from indicators (optional, will fetch from Redis if not provided)
            pattern_data: Pattern information
            
        Returns:
            Dict with position size and risk metrics
        """
        try:
            # Get ATR data from Redis if not provided
            if not atr_data:
                atr_data = self._get_atr_data(symbol)
            
            # Get ATR (N value) - use 20-day ATR for Turtle Trading
            n_value = atr_data.get('atr_20', 0)
            if n_value <= 0:
                logger.warning(f"No valid ATR data for Turtle Trading calculation for {symbol}")
                return {
                    'position_size': 0,
                    'error': 'Invalid ATR data',
                    'method': 'turtle_trading'
                }
            
            # Get contract multiplier based on symbol and asset class
            contract_multiplier = self._get_contract_multiplier(symbol, pattern_data)
            
            # Calculate account risk (3% of account balance)
            account_risk = self.account_balance * self.turtle_risk_per_trade
            
            # Turtle Trading position size formula
            position_size = account_risk / (n_value * contract_multiplier)
            
            # Apply maximum position size limit
            max_contracts = self.max_position_size / (n_value * contract_multiplier)
            position_size = min(position_size, max_contracts)
            
            # Round to nearest whole number (no fractional contracts)
            position_size = max(1, round(position_size))
            
            # Calculate actual risk amount
            actual_risk = position_size * n_value * contract_multiplier
            risk_percentage = (actual_risk / self.account_balance) * 100
            
            # Calculate stop loss level (2N from entry)
            current_price = pattern_data.get('last_price', 0) if pattern_data else 0
            stop_loss_distance = n_value * self.turtle_stop_loss
            
            return {
                'position_size': position_size,
                'contract_multiplier': contract_multiplier,
                'n_value': n_value,
                'account_risk': account_risk,
                'actual_risk': actual_risk,
                'risk_percentage': risk_percentage,
                'stop_loss_distance': stop_loss_distance,
                'method': 'turtle_trading',
                'turtle_ready': atr_data.get('turtle_ready', False)
            }
            
        except Exception as e:
            logger.error(f"Turtle position sizing failed for {symbol}: {e}")
            return {
                'position_size': 0,
                'error': str(e),
                'method': 'turtle_trading'
            }

    def should_trade_cash_equity(self, pattern_data: Dict) -> bool:
        """
        Check if cash equity should be traded based on expected gain
        
        Only trade cash equity if expected gain > 2%
        
        Args:
            pattern_data: Pattern information with expected_move
            
        Returns:
            True if should trade, False otherwise
        """
        if not pattern_data:
            return False
            
        expected_move = pattern_data.get('expected_move', 0)
        asset_class = pattern_data.get('asset_class', '')
        
        # Only apply to cash equity
        if asset_class != 'equity_cash':
            return True  # Allow all other asset classes
            
        # For cash equity, check if expected gain > 2%
        if expected_move >= 2.0:  # 2% expected gain
            return True
        else:
            logger.info(f"Cash equity {pattern_data.get('symbol', '')} rejected: expected gain {expected_move:.2f}% < 2%")
            return False

    def _kelly_position_size(self, price: float, confidence: float, pattern_data: Dict) -> Dict:
        """
        AION Kelly Criterion Position Sizing
        Calculate optimal position size using fractional Kelly criterion
        
        Args:
            price: Current price
            confidence: Pattern confidence (0-1)
            pattern_data: Pattern information
            
        Returns:
            Dict with position sizing information
        """
        try:
            # Extract pattern information
            pattern = pattern_data.get('pattern', 'unknown')
            expected_move = pattern_data.get('expected_move', 0)
            asset_class = pattern_data.get('asset_class', '')
            
            # Base win rate from confidence
            base_win_rate = confidence
            
            # Pattern-specific adjustments
            pattern_multipliers = {
                'reversal': 1.2,
                'breakout': 1.1,
                'volume_spike': 0.9,
                'momentum': 1.0
            }
            
            win_prob = base_win_rate * pattern_multipliers.get(pattern, 1.0)
            win_prob = min(win_prob, 0.95)  # Cap at 95%
            
            # Calculate average win/loss based on expected move
            avg_win = abs(expected_move) / 100  # Convert percentage to decimal
            avg_loss = avg_win * 0.5  # Assume 50% of expected move as loss
            
            # Kelly criterion: f = (bp - q) / b
            # where b = odds, p = win probability, q = loss probability
            b = avg_win / avg_loss if avg_loss > 0 else 1.0
            q = 1 - win_prob
            
            full_kelly = (b * win_prob - q) / b if b > 0 else 0
            full_kelly = max(0, min(full_kelly, 0.25))  # Cap at 25%
            
            # Fractional Kelly (use 25% of full Kelly)
            fractional_kelly = full_kelly * 0.25
            
            # Calculate position size
            position_pct = fractional_kelly
            position_size = int(price * position_pct) if price > 0 else 0
            
            return {
                'position_size': position_size,
                'position_pct': position_pct * 100,
                'kelly_full': full_kelly * 100,
                'win_probability': win_prob,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'odds': b,
                'max_loss': position_size * (avg_loss / (avg_win + avg_loss)),
                'pattern': pattern,
                'base_win_rate': base_win_rate
            }
            
        except Exception as e:
            logger.error(f"Error in Kelly position sizing: {e}")
            return None

    def _calculate_stop_loss(self, price: float, pattern: str, signal: str = 'BULLISH', pattern_data: Dict = None) -> float:
        """Calculate stop loss using dispatcher with legacy fallback."""
        context = {
            "entry_price": price,
            "pattern_type": pattern,
            "signal": signal,
            "atr": (pattern_data or {}).get('atr_20') if isinstance(pattern_data, dict) else None,
        }

        if self.math_dispatcher:
            try:
                dispatched_stop = self.math_dispatcher.calculate_stop_loss(pattern, context)
                if dispatched_stop is not None:
                    return dispatched_stop
            except Exception as dispatch_error:
                logger.debug(f"MathDispatcher stop loss error for {pattern}: {dispatch_error}")

        # Pattern-specific stop losses
        stop_loss_pct = {
            'BREAKOUT': 0.015,          # 1.5% for breakouts
            'BREAKDOWN': 0.015,         # 1.5% for breakdowns
            'REVERSAL': 0.02,           # 2% for reversals
            'VOLUME_SPIKE': 0.025,      # 2.5% for volume spikes
            'MOMENTUM': 0.01,           # 1% for momentum
            'FALSE_BREAKOUT': 0.03,     # 3% for false breakouts
            'ACCUMULATION': 0.02,       # 2% for accumulation
            'DISTRIBUTION': 0.02,       # 2% for distribution
            'INSTITUTIONAL_BUY': 0.015, # 1.5% for institutional buys
            'SPOOF_ORDER': 0.035        # 3.5% for spoof orders
        }
        
        # Get pattern-specific stop loss or use default
        stop_loss_percentage = stop_loss_pct.get(pattern.upper(), self.default_stop_loss_pct)
        stop_loss_percentage = max(stop_loss_percentage, 0.005)

        if signal.upper() in ('BEARISH', 'SELL', 'SHORT'):
            return price * (1 + stop_loss_percentage)
        return price * (1 - stop_loss_percentage)

    def _calculate_risk_score(self, pattern_data: Dict) -> float:
        """
        Calculate overall risk score (0-100, lower is better)
        """
        score = 0
        
        # Volume risk (low volume = high risk)
        volume_ratio = pattern_data.get('volume_ratio', 1)
        if volume_ratio < 0.5:
            score += 30
        elif volume_ratio < 1.0:
            score += 15
            
        # Confidence risk (low confidence = high risk)
        confidence = pattern_data.get('confidence', 0)
        if confidence < 0.5:
            score += 30
        elif confidence < 0.7:
            score += 15
            
        # Pattern risk
        high_risk_patterns = ['SPOOF_ORDER', 'BEAR_TRAP', 'BULL_TRAP', 'FAKE_PRESSURE']
        if pattern_data.get('pattern') in high_risk_patterns:
            score += 20
            
        # Market condition risk
        if pattern_data.get('market_regime') == 'VOLATILE':
            score += 10
            
        return min(score, 100)

    def _get_market_data(self, symbol: str) -> Dict[str, Any]:
        """Get real-time market data from Redis for risk calculations"""
        if not self.redis_client:
            logger.warning(f"No Redis client available for market data for {symbol}")
            return {}
        
        try:
            market_data = {
                'session_data': {},
                'price_movement': {},
                'pattern_data': {},
                'timestamp': datetime.now().isoformat()
            }
            
            # Try to get session data if method exists
            if hasattr(self.redis_client, 'get_cumulative_data'):
                try:
                    session_data = self.redis_client.get_cumulative_data(symbol)
                    if session_data:
                        market_data['session_data'] = session_data
                except Exception as e:
                    logger.debug(f"Could not get cumulative data for {symbol}: {e}")
            
            # Try to get price movement if method exists
            if hasattr(self.redis_client, 'get_session_price_movement'):
                try:
                    price_movement = self.redis_client.get_session_price_movement(symbol, minutes_back=30)
                    if price_movement:
                        market_data['price_movement'] = price_movement
                except Exception as e:
                    logger.debug(f"Could not get price movement for {symbol}: {e}")
            
            # Try to get pattern data if method exists
            if hasattr(self.redis_client, 'get_pattern_data'):
                try:
                    pattern_data = self.redis_client.get_pattern_data(symbol)
                    if pattern_data:
                        market_data['pattern_data'] = pattern_data
                except Exception as e:
                    logger.debug(f"Could not get pattern data for {symbol}: {e}")
            
            logger.debug(f"Retrieved market data for {symbol}: {len(market_data)} fields")
            return market_data
            
        except Exception as e:
            logger.error(f"Error getting market data for {symbol}: {e}")
            return {}
    
    def _get_current_price(self, symbol: str) -> float:
        """Get current price from Redis"""
        if not self.redis_client:
            return 0.0
        
        try:
            # Try to get from session data first if method exists
            if hasattr(self.redis_client, 'get_cumulative_data'):
                try:
                    session_data = self.redis_client.get_cumulative_data(symbol)
                    if session_data and session_data.get('last_price'):
                        return float(session_data['last_price'])
                except Exception as e:
                    logger.debug(f"Could not get cumulative data for {symbol}: {e}")
            
            # Fallback: get from recent time buckets if method exists
            if hasattr(self.redis_client, 'get_time_buckets'):
                try:
                    recent_buckets = self.redis_client.get_time_buckets(symbol, lookback_minutes=5)
                    if recent_buckets:
                        latest_bucket = recent_buckets[0]
                        price = (
                            latest_bucket.get('last_price') or
                            latest_bucket.get('close') or
                            latest_bucket.get('price') or
                            0
                        )
                        if price:
                            return float(price)
                except Exception as e:
                    logger.debug(f"Could not get time buckets for {symbol}: {e}")
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Error getting current price for {symbol}: {e}")
            return 0.0


class PremiumRiskManager:
    """Risk manager for premium collection strategies."""

    def __init__(self, account_size: float = 100000.0):
        self.account_size = account_size
        self.max_position_risk = 0.02  # 2% per trade
        self.max_portfolio_risk = 0.10  # 10% total
        self.active_trades: Dict[str, Dict] = {}

    def calculate_position_size(self, trade: Dict) -> int:
        try:
            max_risk_per_trade = self.account_size * self.max_position_risk
            trade_risk = float(trade.get("max_risk", 0.0))
            if trade_risk <= 0:
                return 1
            lots = max(1, int(max_risk_per_trade / trade_risk))
            # Portfolio cap
            available = (self.account_size * self.max_portfolio_risk) - self._current_portfolio_risk()
            max_by_portfolio = int(available / trade_risk) if trade_risk > 0 else 1
            return max(1, min(lots, max_by_portfolio, 10))
        except Exception as e:
            logger.debug(f"PremiumRiskManager sizing failed: {e}")
            return 1

    def _current_portfolio_risk(self) -> float:
        total = 0.0
        for t in self.active_trades.values():
            try:
                total += float(t.get("current_risk", 0.0))
            except Exception:
                continue
        return total
    
    def _get_contract_multiplier(self, symbol: str, pattern_data: Dict = None) -> int:
        """Get contract multiplier for a symbol based on asset class"""
        try:
            # Try to get asset class from pattern data
            if pattern_data:
                asset_class = pattern_data.get('asset_class', '')
                if asset_class == 'equity_options':
                    return self.contract_multipliers['OPTIONS']
                elif asset_class == 'equity_cash':
                    return self.contract_multipliers['EQUITY']
                elif asset_class == 'equity_futures':
                    return self.contract_multipliers['FUTURES']
                elif asset_class == 'indices':
                    # Check if it's an index future
                    if 'NIFTY' in symbol.upper():
                        return self.contract_multipliers['NIFTY']
                    elif 'BANKNIFTY' in symbol.upper():
                        return self.contract_multipliers['BANKNIFTY']
                    elif 'FINNIFTY' in symbol.upper():
                        return self.contract_multipliers['FINNIFTY']
                    elif 'SENSEX' in symbol.upper():
                        return self.contract_multipliers['SENSEX']
                    elif 'BANKEX' in symbol.upper():
                        return self.contract_multipliers['BANKEX']
            
            # Fallback based on symbol name
            symbol_upper = symbol.upper()
            if 'NIFTY' in symbol_upper and 'FUT' in symbol_upper:
                return self.contract_multipliers['NIFTY']
            elif 'BANKNIFTY' in symbol_upper and 'FUT' in symbol_upper:
                return self.contract_multipliers['BANKNIFTY']
            elif 'FINNIFTY' in symbol_upper and 'FUT' in symbol_upper:
                return self.contract_multipliers['FINNIFTY']
            elif 'CE' in symbol_upper or 'PE' in symbol_upper:
                return self.contract_multipliers['OPTIONS']
            else:
                return self.contract_multipliers['EQUITY']  # Default to equity
                
        except Exception as e:
            logger.error(f"Error getting contract multiplier for {symbol}: {e}")
            return 1  # Safe fallback
    
    def calculate_turtle_stop_loss(self, entry_price: float, n_value: float, direction: str) -> float:
        """
        Calculate Turtle Trading stop loss
        
        Stop Loss = Entry ± (2 × N)
        
        Args:
            entry_price: Entry price
            n_value: ATR value (N)
            direction: 'BUY' or 'SELL'
            
        Returns:
            Stop loss price
        """
        stop_distance = n_value * self.turtle_stop_loss
        
        if direction.upper() in ['BUY', 'LONG']:
            return entry_price - stop_distance
        else:  # SELL, SHORT
            return entry_price + stop_distance
    
    def calculate_turtle_pyramid_levels(self, entry_price: float, n_value: float, max_units: int = None) -> list:
        """
        Calculate Turtle Trading pyramid add levels
        
        Add positions at 0.5N intervals
        
        Args:
            entry_price: Initial entry price
            n_value: ATR value (N)
            max_units: Maximum units (default: turtle_max_units)
            
        Returns:
            List of pyramid levels
        """
        if max_units is None:
            max_units = self.turtle_max_units
            
        pyramid_levels = []
        add_interval = n_value * self.turtle_add_interval
        
        for unit in range(2, max_units + 1):  # Start from unit 2 (unit 1 is initial entry)
            add_price = entry_price + (add_interval * (unit - 1))
            stop_loss = add_price - (n_value * self.turtle_stop_loss)
            
            pyramid_levels.append({
                'unit': unit,
                'add_price': add_price,
                'stop_loss': stop_loss,
                'add_interval': add_interval
            })
        
        return pyramid_levels
    
    def classify_liquidity(self, symbol, avg_volume):
        """Classify symbol into liquidity tier"""
        if avg_volume > 1000000:  # 1 million
            return 'HFT'
        elif avg_volume > 500000:
            return 'Institutional'
        return 'Retail'
    
    def adjust_position_size(self, symbol, size):
        """Liquidity-based position sizing"""
        try:
            # Get average volume for the symbol
            avg_volume = self._get_avg_volume(symbol)
            liquidity_class = self.classify_liquidity(symbol, avg_volume)
            factor = self.liquidity_factors.get(liquidity_class, 1.0)
            return size * factor
        except Exception as e:
            logger.error(f"Liquidity adjustment failed: {e}")
            return size
    
    def _get_avg_volume(self, symbol: str, lookback_days: int = 20) -> float:
        """Get average volume for symbol from Redis historical data"""
        if not self.redis_client:
            logger.warning(f"No Redis client available for volume data for {symbol}")
            return 100000  # Conservative fallback
        
        try:
            # Get time buckets for the last N days
            end_time = datetime.now()
            start_time = end_time - timedelta(days=lookback_days)
            
            # Get volume data from Redis using rolling window method
            volume_data = self.redis_client.get_rolling_window_buckets(
                symbol=symbol,
                lookback_minutes=lookback_days * 24 * 60  # Convert days to minutes
            )
            
            if not volume_data:
                logger.warning(f"No volume data found for {symbol} in Redis")
                return 100000  # Conservative fallback
            
            # Calculate average volume from historical data
            total_volume = 0
            valid_buckets = 0
            
            for bucket in volume_data:
                # Try multiple volume field names
                volume = (
                    bucket.get('bucket_incremental_volume') or
                    bucket.get('zerodha_cumulative_volume') or
                    bucket.get('volume') or
                    bucket.get('cumulative_volume') or
                    0
                )
                
                if volume and float(volume) > 0:
                    total_volume += float(volume)
                    valid_buckets += 1
            
            if valid_buckets > 0:
                avg_volume = total_volume / valid_buckets
                logger.debug(f"Calculated average volume for {symbol}: {avg_volume:.0f} from {valid_buckets} buckets")
                return avg_volume
            else:
                logger.warning(f"No valid volume data found for {symbol}")
                return 100000  # Conservative fallback
                
        except Exception as e:
            logger.error(f"Error getting average volume for {symbol}: {e}")
            return 100000  # Conservative fallback
    
    def _get_atr_data(self, symbol: str, period: int = 20) -> Dict[str, float]:
        """Get ATR data from Redis for Turtle Trading calculations"""
        if not self.redis_client:
            logger.warning(f"No Redis client available for ATR data for {symbol}")
            return {'atr_20': 0.0, 'atr_14': 0.0}
        
        try:
            # Try to get ATR from Redis indicators using retrieve_by_data_type
            atr_key = f"indicators:{symbol}:atr"
            atr_data = self.redis_client.retrieve_by_data_type(atr_key, "analysis_cache")
            
            if atr_data:
                try:
                    atr_dict = json.loads(atr_data)
                    return {
                        'atr_20': float(atr_dict.get('atr_20', 0)),
                        'atr_14': float(atr_dict.get('atr_14', 0)),
                        'atr': float(atr_dict.get('atr', 0))
                    }
                except (json.JSONDecodeError, ValueError) as e:
                    logger.debug(f"Error parsing ATR data for {symbol}: {e}")
            
            # Fallback: Calculate ATR from price data
            return self._calculate_atr_from_prices(symbol, period)
            
        except Exception as e:
            logger.error(f"Error getting ATR data for {symbol}: {e}")
            return {'atr_20': 0.0, 'atr_14': 0.0}
    
    def _calculate_atr_from_prices(self, symbol: str, period: int = 20) -> Dict[str, float]:
        """Calculate ATR from historical price data in Redis"""
        try:
            # Get recent price data
            price_data = self.redis_client.get_time_buckets(
                symbol=symbol,
                lookback_minutes=period * 60  # Get enough data for ATR calculation
            )
            
            if len(price_data) < period:
                logger.warning(f"Insufficient price data for ATR calculation for {symbol}")
                return {'atr_20': 0.0, 'atr_14': 0.0}
            
            # Extract high, low, close prices
            highs = []
            lows = []
            closes = []
            
            for bucket in price_data:
                high = bucket.get('high') or bucket.get('last_price', 0)
                low = bucket.get('low') or bucket.get('last_price', 0)
                close = bucket.get('close') or bucket.get('last_price', 0)
                
                if high and low and close:
                    highs.append(float(high))
                    lows.append(float(low))
                    closes.append(float(close))
            
            if len(highs) < period:
                return {'atr_20': 0.0, 'atr_14': 0.0}
            
            # Calculate True Range and ATR
            true_ranges = []
            for i in range(1, len(highs)):
                tr1 = highs[i] - lows[i]
                tr2 = abs(highs[i] - closes[i-1])
                tr3 = abs(lows[i] - closes[i-1])
                true_ranges.append(max(tr1, tr2, tr3))
            
            if true_ranges:
                atr_20 = sum(true_ranges[-20:]) / min(20, len(true_ranges))
                atr_14 = sum(true_ranges[-14:]) / min(14, len(true_ranges))
                
                return {
                    'atr_20': atr_20,
                    'atr_14': atr_14,
                    'atr': atr_20
                }
            else:
                return {'atr_20': 0.0, 'atr_14': 0.0}
                
        except Exception as e:
            logger.error(f"Error calculating ATR from prices for {symbol}: {e}")
            return {'atr_20': 0.0, 'atr_14': 0.0}
    
    def get_time_based_position_rules(self, current_time=None):
        """
        Get time-based position management rules as per DeepSeek protocol
        
        Args:
            current_time: Current time (defaults to now)
            
        Returns:
            Dict with position management rules for current time
        """
        if current_time is None:
            current_time = datetime.now()
        
        # Convert to IST
        ist = pytz.timezone('Asia/Kolkata')
        if current_time.tzinfo is None:
            current_time = ist.localize(current_time)
        else:
            current_time = current_time.astimezone(ist)
        
        current_time_only = current_time.time()
        
        # DeepSeek time-based rules
        rules = {
            'allow_new_entries': True,
            'position_trim_percentage': 0.0,  # 0% = no trimming
            'max_position_size_multiplier': 1.0,  # 1.0 = normal size
            'stop_new_entries': False,
            'exit_all_positions': False,
            'time_rule': 'NORMAL_TRADING'
        }
        
        # 2:30 PM: Trim 50% positions
        if time(14, 30) <= current_time_only < time(14, 45):
            rules.update({
                'position_trim_percentage': 0.5,
                'time_rule': 'TRIMMING_PHASE'
            })
            logger.info("🕐 2:30 PM - Trimming 50% of positions")
        
        # 2:45 PM: Stop new entries
        elif time(14, 45) <= current_time_only < time(15, 15):
            rules.update({
                'allow_new_entries': False,
                'stop_new_entries': True,
                'time_rule': 'NO_NEW_ENTRIES'
            })
            logger.info("🕐 2:45 PM - Stopping new entries")
        
        # 3:15 PM: Exit all intraday positions
        elif time(15, 15) <= current_time_only < time(15, 30):
            rules.update({
                'allow_new_entries': False,
                'exit_all_positions': True,
                'time_rule': 'EXIT_ALL_POSITIONS'
            })
            logger.info("🕐 3:15 PM - Exiting all intraday positions")
        
        # Market close: No trading
        elif current_time_only >= time(15, 30):
            rules.update({
                'allow_new_entries': False,
                'stop_new_entries': True,
                'exit_all_positions': True,
                'time_rule': 'MARKET_CLOSED'
            })
            logger.info("🕐 Market closed - No trading allowed")
        
        # Check for expiry day adjustments
        if self._is_expiry_day(current_time):
            rules['max_position_size_multiplier'] = 0.5  # 50% position size on expiry
            rules['time_rule'] += '_EXPIRY_DAY'
            logger.info("📅 Expiry day - Reducing position sizes by 50%")
        
        return rules
    
    def _is_expiry_day(self, current_time):
        """Check if current day is F&O expiry day (simple Thursday check)"""
        try:
            # Simple Thursday check for F&O expiry (last Thursday of month)
            if current_time.weekday() == 3:  # Thursday
                # Check if it's the last Thursday of the month
                next_week = current_time + timedelta(days=7)
                return next_week.month != current_time.month
            return False
            
        except Exception as e:
            logger.error(f"Expiry day check failed: {e}")
            # Fallback to simple Thursday check
            return current_time.weekday() == 3 and current_time.day >= 25
    
    def apply_time_based_adjustments(self, pattern_data, current_time=None):
        """
        Apply time-based adjustments to pattern data
        
        Args:
            pattern_data: Pattern detection data
            current_time: Current time (defaults to now)
            
        Returns:
            Adjusted pattern data with time-based modifications
        """
        try:
            rules = self.get_time_based_position_rules(current_time)
            
            # Apply position size multiplier
            if 'position_size' in pattern_data:
                pattern_data['position_size'] *= rules['max_position_size_multiplier']
                pattern_data['time_adjusted_size'] = True
            
            # Add time-based flags
            pattern_data['time_rules'] = rules
            
            # Add warnings for time-based restrictions
            if rules['stop_new_entries']:
                pattern_data['warning'] = "⚠️ No new entries allowed at this time"
            elif rules['exit_all_positions']:
                pattern_data['warning'] = "⚠️ Exit all positions - market closing soon"
            elif rules['position_trim_percentage'] > 0:
                pattern_data['warning'] = f"⚠️ Trim {rules['position_trim_percentage']*100:.0f}% of positions"
            
            return pattern_data
            
        except Exception as e:
            logger.error(f"Time-based adjustments failed: {e}")
            return pattern_data
