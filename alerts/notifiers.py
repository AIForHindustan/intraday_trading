"""
Alert Notifications and Templates
=================================

Notification templates and delivery systems for alerts.
Consolidated from scanner/alert_manager.py HumanReadableAlertTemplates and notification logic.

Classes:
- AlertTemplate: Alert message templates
- TelegramNotifier: Telegram notification delivery
- RedisNotifier: Redis notification publishing
- HumanReadableAlertTemplates: Enhanced alert message templates

Created: October 9, 2025
"""

import sys
import os
import json
import time
import logging
import threading
import requests
import redis
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo
import pytz

# Add project root to Python path for proper imports
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Initialize logger
logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Telegram notification delivery system."""
    
    def __init__(self, config_path: Optional[str] = None, redis_client=None):
        """Initialize Telegram notifier with configuration."""
        self.config = self._load_config(config_path)
        self.rate_limits = {}  # {chat_id: last_send_time}
        self.failure_count = 0
        # In-process short-window de-dup cache: key -> expiry_epoch
        self._dedup: Dict[str, float] = {}
        self.redis_client = redis_client  # Store Redis client for news enrichment
        logger.info("🔄 Telegram notifier initialized - circuit breaker disabled")
        
    def _load_config(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Telegram configuration."""
        if config_path is None:
            config_path = Path(__file__).parent / "config" / "telegram_config.json"
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info(f"✅ Telegram config loaded: {config}")
                return config
        except Exception as e:
            logger.error(f"Could not load Telegram config: {e}")
            logger.error(f"Config path: {config_path}")
            return {}
    
    def send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Send alert via Telegram."""
        try:
            # Debug: Check what we received
            logger.debug(f"TelegramNotifier received alert_data: {type(alert_data)}, {alert_data}")
            
            # Check if config is loaded
            if not self.config:
                logger.error("Telegram config not loaded - cannot send alert")
                return False
            
            # Check if alert_data is None
            if alert_data is None:
                logger.error("❌ TelegramNotifier received None alert_data")
                return False
            
            # Fast in-process de-dup (10s window)
            try:
                symbol = str(alert_data.get('symbol', '')).upper()
                pattern = str(alert_data.get('pattern', alert_data.get('pattern_type', ''))).lower()
                action = str(alert_data.get('action', ''))
                # 10-second bucket
                bucket = int(time.time() // 10)
                dedup_key = f"tg:dedup:{symbol}:{pattern}:{action}:{bucket}"
                now = time.time()
                # prune expired entries
                for k in list(self._dedup.keys()):
                    if self._dedup[k] <= now:
                        self._dedup.pop(k, None)
                if dedup_key in self._dedup:
                    logger.info(f"🛑 Telegram de-dup hit, skipping duplicate: {symbol} {pattern}")
                    return True
                # set 12s ttl
                self._dedup[dedup_key] = now + 12.0
            except Exception:
                pass
            
            # Check confidence threshold (90%+ for Telegram), but ALWAYS send NEWS_ALERT
            confidence = float(alert_data.get('confidence', 0.0))
            is_news_alert = str(alert_data.get('pattern', '')).upper() == 'NEWS_ALERT' or bool(alert_data.get('news_context'))
            if not is_news_alert and confidence < 0.90:
                logger.debug(f"Telegram alert skipped: confidence {confidence:.1%} < 90% (non-news)")
                return True  # Don't count as failure
            
            # Determine if this should use the signal bot (high-priority alerts only)
            use_signal_bot = self._should_use_signal_bot(alert_data)
            
            # Circuit breaker disabled - allowing all Telegram alerts
            
            # Enrich alert with news before formatting (if not already enriched)
            try:
                if not alert_data.get('news') and not alert_data.get('news_context'):
                    from alerts.news_enrichment_integration import enrich_alert_with_news
                    if self.redis_client:
                        alert_data = enrich_alert_with_news(alert_data, self.redis_client, lookback_minutes=60, top_k=3)
                        if alert_data.get('news'):
                            logger.debug(f"✅ Enriched {alert_data.get('symbol')} alert with {len(alert_data.get('news'))} news items")
            except Exception as e:
                logger.debug(f"News enrichment skipped: {e}")
            
            # Format message using the proper HumanReadableAlertTemplates
            message = HumanReadableAlertTemplates.format_telegram_alert(alert_data)
            
            # Send to BOTH bots when appropriate
            success = True
            
            # Always send to main bot
            main_chat_ids = self.config.get('chat_ids', [])
            if main_chat_ids:
                logger.info(f"📡 Sending to main bot: {main_chat_ids}")
                for chat_id in main_chat_ids:
                    sent = self._send_to_chat(chat_id, message, use_signal_bot=False)
                    if not sent:
                        success = False
            
            # Also send to signal bot if it's a high-priority alert
            if use_signal_bot and 'signal_bot' in self.config:
                signal_chat_ids = self.config['signal_bot'].get('chat_ids', [])
                if signal_chat_ids:
                    logger.info(f"🚨 Sending to signal bot: {signal_chat_ids}")
                    for chat_id in signal_chat_ids:
                        sent = self._send_to_chat(chat_id, message, use_signal_bot=True)
                        if not sent:
                            success = False
            elif not main_chat_ids:
                logger.warning("No chat IDs configured for either bot")
                success = False
            
            if success:
                self.failure_count = 0  # Reset failure count on success
            else:
                self.failure_count += 1
                
            return success
            
        except Exception as e:
            logger.error(f"Error sending Telegram alert: {e}")
            self.failure_count += 1
            return False
    
    def _should_use_signal_bot(self, alert_data: Dict[str, Any]) -> bool:
        """Determine if alert should use the signal bot (Kow Signal Straddle + NIFTY/BANKNIFTY futures only)."""
        try:
            # Check if signal bot is configured
            if 'signal_bot' not in self.config:
                logger.debug("Signal bot not configured")
                return False
            
            # Get alert details
            symbol = alert_data.get('symbol', '').upper()
            pattern = alert_data.get('pattern', '').lower()
            strategy_type = alert_data.get('strategy_type', '').lower()
            confidence = float(alert_data.get('confidence', 0.0))
            
            # Check confidence threshold for signal bot
            signal_bot_config = self.config.get('signal_bot', {})
            min_confidence = signal_bot_config.get('minimum_confidence', 0.85)
            
            if confidence < min_confidence:
                logger.debug(f"Signal bot skipped: {symbol} confidence {confidence:.2f} < {min_confidence:.2f}")
                return False
            
            # Kow Signal Straddle Strategy filtering
            is_kow_signal = (
                'kow_signal_straddle' in pattern or
                'kow_signal_straddle' in strategy_type or
                'straddle' in pattern or
                symbol == 'KOW_SIGNAL_STRADDLE'
            )
            
            # NIFTY and BANKNIFTY futures filtering - use startswith for exact matching
            is_target_symbol = (
                symbol.startswith('NIFTY') or
                symbol.startswith('BANKNIFTY') or
                symbol == 'NIFTY' or
                symbol == 'BANKNIFTY'
            )
            
            # Extract expiry from symbol name if expiry field is empty
            expiry = alert_data.get('expiry', '')
            if not expiry and symbol:
                # Extract expiry from symbol like "NIFTY25NOVFUT" -> "25NOV"
                import re
                expiry_match = re.search(r'(\d{2}[A-Z]{3})', symbol)
                if expiry_match:
                    expiry = expiry_match.group(1)
            
            # Check if it's November 25th expiry (if expiry info available)
            is_nov_25_expiry = '25NOV' in expiry.upper() or 'NOV25' in expiry.upper()
            
            # Use signal bot for Kow Signal Straddle OR NIFTY/BANKNIFTY futures (any expiry)
            use_signal = is_kow_signal or is_target_symbol
            
            if use_signal:
                logger.info(f"🎯 KOW SIGNAL FILTER: {symbol} {pattern} (confidence: {confidence:.2f}) -> Using signal bot")
            else:
                logger.debug(f"Signal bot skipped: {symbol} {pattern} - not target symbol or kow signal")
            
            return use_signal
            
        except Exception as e:
            logger.error(f"Error determining signal bot usage: {e}")
            return False
    
    def _send_to_chat(self, chat_id: int, message: str, use_signal_bot: bool = False) -> bool:
        """Send message to specific chat ID."""
        try:
            # Choose bot token based on priority
            if use_signal_bot and 'signal_bot' in self.config:
                bot_token = self.config['signal_bot'].get('bot_token')
                logger.info("📡 Using signal bot for high-priority alert")
            else:
                bot_token = self.config.get('bot_token')
                logger.info("📡 Using main bot for standard alert")
            
            if not bot_token:
                logger.error("No bot token configured")
                return False
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            
            retry_attempts = 0
            backoff_seconds = 1.5
            while retry_attempts < 3:
                wait_for = max(0.0, self.rate_limits.get(chat_id, 0.0) - time.time())
                if wait_for > 0:
                    time.sleep(wait_for)

                response = requests.post(url, data=data, timeout=10)
                if response.status_code == 429:
                    retry_attempts += 1
                    retry_after = 0.0
                    try:
                        payload = response.json()
                        retry_after = float(payload.get("parameters", {}).get("retry_after", 0))
                    except Exception:
                        retry_after = 0.0
                    if response.headers.get("Retry-After"):
                        try:
                            retry_after = max(retry_after, float(response.headers["Retry-After"]))
                        except ValueError:
                            pass
                    retry_after = max(retry_after, backoff_seconds)
                    logger.warning(
                        "Telegram rate limit hit for chat %s. Backing off %.1fs (attempt %d/3)",
                        chat_id,
                        retry_after,
                        retry_attempts,
                    )
                    self.rate_limits[chat_id] = time.time() + retry_after
                    time.sleep(retry_after)
                    continue

                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    logger.error(f"Failed to send to chat {chat_id}: {exc} ({response.text})")
                    retry_attempts += 1
                    self.rate_limits[chat_id] = time.time() + backoff_seconds * retry_attempts
                    time.sleep(backoff_seconds * retry_attempts)
                    continue

                # Success
                self.rate_limits[chat_id] = time.time() + 1.5
                logger.info(f"Telegram alert sent to chat {chat_id}")
                return True

            logger.error(f"Giving up on chat {chat_id} after {retry_attempts} attempts")
            return False
            
        except Exception as e:
            logger.error(f"Failed to send to chat {chat_id}: {e}")
            return False
    
    def _check_rate_limit(self, chat_id: int) -> bool:
        """Check if rate limit allows sending to chat."""
        current_time = time.time()
        last_send = self.rate_limits.get(chat_id, 0)
        
        # Rate limit: 0.5 seconds between messages (minimal for intraday trading)
        if current_time - last_send < 0.5:
            logger.debug(f"Rate limited for chat {chat_id}: {current_time - last_send:.1f}s since last send")
            return False
        
        return True
    
    # Circuit breaker methods removed - disabled permanently
    
    def test_connection(self):
        """Test Telegram connection."""
        try:
            bot_token = self.config.get('bot_token')
            if not bot_token:
                logger.error("No bot token configured")
                return False
            
            # Test with first chat ID
            chat_id = self.config.get('chat_ids', [])[0] if self.config.get('chat_ids') else None
            if not chat_id:
                logger.error("No chat IDs configured")
                return False
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                'chat_id': chat_id,
                'text': '🔄 Telegram connection test',
                'parse_mode': 'HTML'
            }
            
            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()
            
            logger.info("✅ Telegram connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Telegram connection test failed: {e}")
            return False


class MacOSNotifier:
    """macOS notification delivery system using osascript."""
    
    def __init__(self):
        self.enabled = True
        logger.info("macOS Notifier initialized")
    
    def send_alert(self, pattern_data):
        """Send macOS notification using osascript."""
        try:
            # Debug: Check what we received
            logger.debug(f"MacOSNotifier received pattern_data: {type(pattern_data)}, {pattern_data}")
            
            if not self.enabled:
                return

            # Check if pattern_data is None
            if pattern_data is None:
                logger.error("❌ MacOSNotifier received None pattern_data")
                return

            # Check confidence threshold (85%+ for macOS)
            confidence = float(pattern_data.get('confidence', 0.0))
            if confidence < 0.90:
                logger.debug(f"macOS alert skipped: confidence {confidence:.1%} < 90%")
                return

            symbol = pattern_data.get('symbol', 'Unknown')
            signal = pattern_data.get('signal', 'Unknown')
            confidence_pct = confidence * 100

            # Use the proper HumanReadableAlertTemplates
            try:
                message = HumanReadableAlertTemplates.format_telegram_alert(pattern_data)
                logger.debug(f"format_telegram_alert completed for {symbol}")
            except Exception as e:
                logger.error(f"Error in format_telegram_alert for {symbol}: {e}")
                # Fallback to simple message
                message = f"{signal} signal for {symbol} with {confidence_pct:.1f}% confidence"
            
            # Create notification title and message
            title = f"🚨 Trading Alert: {symbol}"
            # Truncate message for macOS notification (it has character limits)
            if len(message) > 200:
                message = message[:197] + "..."

            # Use osascript to send macOS notification
            import subprocess
            script = f'''
            display notification "{message}" with title "{title}" sound name "Submarine"
            '''

            result = subprocess.run(['osascript', '-e', script],
                                  capture_output=True, text=True, timeout=5)

            if result.returncode == 0:
                logger.info(f"✅ macOS notification sent: {symbol} {signal}")
            else:
                logger.error(f"macOS notification failed: {result.stderr}")

        except Exception as e:
            logger.error(f"macOS notification error: {e}")


class RedisNotifier:
    """Redis notification publishing system with multi-channel support."""
    
    def __init__(self, redis_client=None):
        """Initialize Redis notifier."""
        self.redis_client = redis_client
        self.channel = "alerts:notifications"
        
        # Single source of truth - NSE Algo Trading channel
        self.CHANNEL_STRUCTURE = {
            'main_signals': 'NSEAlgoTrading',           # Single source for all alerts
            'news_alerts': 'NSEAlgoTrading',           # Same channel for news
        }
        
    def send_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Publish alert to appropriate Redis channels based on content type."""
        try:
            if not self.redis_client:
                logger.warning("No Redis client available")
                return False
            
            # Get realtime client (DB 1) for alert publishing
            realtime_client = self.redis_client.get_client(1) if hasattr(self.redis_client, 'get_client') else self.redis_client
            
            # Redis-based de-dup within short window (avoid double-publish)
            try:
                symbol = str(alert_data.get('symbol', '')).upper()
                pattern = str(alert_data.get('pattern', alert_data.get('pattern_type', ''))).lower()
                action = str(alert_data.get('action', ''))
                # Coarse time bucket to collapse near-duplicates (10s)
                bucket = int(time.time() // 10)
                dedup_key = f"alerts:dedup:{symbol}:{pattern}:{action}:{bucket}"
                # NX with expiry 90s
                if hasattr(realtime_client, 'set') and not realtime_client.set(dedup_key, '1', nx=True, ex=90):
                    logger.info(f"🛑 Redis de-dup hit, skipping publish: {symbol} {pattern}")
                    return True
            except Exception:
                pass
            
            # Add metadata
            alert_data['published_at'] = datetime.now().isoformat()
            alert_data['source'] = 'alert_manager'
            
            # Enrich alert with news before publishing (if not already enriched)
            try:
                if not alert_data.get('news') and not alert_data.get('news_context'):
                    from alerts.news_enrichment_integration import enrich_alert_with_news
                    alert_data = enrich_alert_with_news(alert_data, self.redis_client, lookback_minutes=60, top_k=3)
                    if alert_data.get('news'):
                        logger.debug(f"✅ Enriched {alert_data.get('symbol')} alert with {len(alert_data.get('news'))} news items for Redis")
            except Exception as e:
                logger.debug(f"News enrichment skipped in RedisNotifier: {e}")
            
            # Enhance alert with channel-specific content
            enhanced_alert = self.enhance_alert_for_channels(alert_data)
            
            # Determine target channels based on alert type
            target_channels = self._determine_channels(enhanced_alert)
            
            # Publish to all relevant channels
            message = json.dumps(enhanced_alert)
            success_count = 0
            
            for channel_type, channel_name in target_channels.items():
                try:
                    realtime_client.publish(f"channel:{channel_name}", message)
                    logger.info(f"Alert published to {channel_type}: {channel_name}")
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to publish to {channel_name}: {e}")
            
            # Also publish to main alerts channel for backward compatibility
            realtime_client.publish(self.channel, message)
            
            # CRITICAL: Also publish to Redis Stream for dashboard (alerts.optimized_main)
            # HighPerformanceAlertStream expects binary data in b"data" field using orjson
            try:
                # Use orjson for binary JSON encoding (matches HighPerformanceAlertStream format)
                try:
                    import orjson
                    binary_data = orjson.dumps(enhanced_alert)
                    stream_result = realtime_client.xadd(
                        'alerts:stream',
                        {b'data': binary_data},  # Binary data using orjson - matches HighPerformanceAlertStream.consume_alerts
                        maxlen=10000,
                        approximate=True
                    )
                except ImportError:
                    # Fallback to json if orjson not available
                    import json
                    binary_data = json.dumps(enhanced_alert).encode('utf-8')
                    stream_result = realtime_client.xadd(
                        'alerts:stream',
                        {b'data': binary_data},  # Binary data (utf-8 encoded JSON)
                        maxlen=10000,
                        approximate=True
                    )
                logger.debug(f"✅ Alert published to alerts:stream (dashboard) - ID: {stream_result}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to publish to alerts:stream (dashboard may be static): {e}")
            
            logger.info(f"Alert published to {success_count} channels + main channel + alerts:stream")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error publishing to Redis: {e}")
            return False
    
    def _determine_channels(self, alert_data: Dict[str, Any]) -> Dict[str, str]:
        """Determine which channels to publish to based on alert content."""
        channels = {}
        
        # Always include main signals for pattern alerts
        if alert_data.get('pattern') or alert_data.get('pattern_type'):
            channels['main_signals'] = self.CHANNEL_STRUCTURE['main_signals']
        
        # Check for news-related content (positive or negative sentiment)
        if (alert_data.get('news_context') or 
            alert_data.get('sentiment') or 
            alert_data.get('market_news')):
            channels['news_alerts'] = self.CHANNEL_STRUCTURE['news_alerts']
        
        # If no specific channels determined, default to main signals
        if not channels:
            channels['main_signals'] = self.CHANNEL_STRUCTURE['main_signals']
        
        return channels
    
    def enhance_alert_for_channels(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance alert data with channel-specific content."""
        enhanced_alert = alert_data.copy()
        
        # Add news context for MarketNews_Alerts channel (positive or negative sentiment)
        if alert_data.get('news_context') or alert_data.get('sentiment'):
            enhanced_alert['news_alerts'] = {
                'sentiment': alert_data.get('sentiment', 'neutral'),
                'news_context': alert_data.get('news_context', ''),
                'market_impact': self._assess_market_impact(alert_data)
            }
        
        return enhanced_alert
    
    
    def _assess_market_impact(self, alert_data: Dict[str, Any]) -> str:
        """Assess market impact level for news alerts."""
        confidence = alert_data.get('confidence', 0.0)
        strength = alert_data.get('strength', 0.0)
        
        if confidence > 0.8 and strength > 0.8:
            return "HIGH"
        elif confidence > 0.6 or strength > 0.6:
            return "MEDIUM"
        else:
            return "LOW"


class HumanReadableAlertTemplates:
    """Enhanced alert message templates for better human readability"""

    # Pattern descriptions with actionable trading instructions
    PATTERN_DESCRIPTIONS = {
        # Core patterns from PATTERN_PRIORITIES (MM patterns removed)
        "reversal": {
            "title": "🔄 Trend Reversal",
            "description": "Trend changing direction",
            "action_explanation": "Counter-move confirmed starting",
            "urgency": "high",
            "trading_instruction": "🔄 REVERSAL: Enter opposite to previous trend",
            "directional_action": "BUY-FOLIO",
            "move_type": "TREND_REVERSAL",
        },
        "volume_breakout": {
            "title": "💥 Volume Explosion",
            "description": "Massive volume with price move",
            "action_explanation": "Institutional money moving",
            "urgency": "high",
            "trading_instruction": "💥 VOLUME EXPLOSION: Follow institutions - Enter immediately",
            "directional_action": "BUY-FOLIO",
            "move_type": "VOLUME_EXPLOSION",
        },
        "volume_spike": {
            "title": "📊 Volume Spike",
            "description": "Unusual volume activity detected",
            "action_explanation": "Significant volume increase indicating institutional activity",
            "urgency": "medium",
            "trading_instruction": "WATCH CLOSELY - Be ready to follow volume",
            "directional_action": "WATCH",
            "move_type": "VOLUME_SPIKE",
        },
        "volume_price_divergence": {
            "title": "⚖️ Volume-Price Divergence",
            "description": "Volume and price moving in opposite directions",
            "action_explanation": "Smart money divergence - price vs volume mismatch",
            "urgency": "high",
            "trading_instruction": "⚖️ DIVERGENCE: Follow volume direction - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "VOLUME_PRICE_DIVERGENCE",
        },
        "hidden_accumulation": {
            "title": "🕵️ Hidden Accumulation",
            "description": "Stealth institutional buying during weakness",
            "action_explanation": "Smart money accumulating during apparent weakness",
            "urgency": "medium",
            "trading_instruction": "🕵️ HIDDEN ACCUMULATION: Enter long position - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "HIDDEN_ACCUMULATION",
        },
        "upside_momentum": {
            "title": "🚀 Upside Momentum",
            "description": "Strong upward momentum building",
            "action_explanation": "Price and volume confirming bullish momentum",
            "urgency": "high",
            "trading_instruction": "🚀 MOMENTUM: Enter long position - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "UPSIDE_MOMENTUM",
        },
        "downside_momentum": {
            "title": "📉 Downside Momentum",
            "description": "Strong downward momentum building",
            "action_explanation": "Price and volume confirming bearish momentum",
            "urgency": "high",
            "trading_instruction": "📉 MOMENTUM: Enter short position - Target -1-2%, Stop +0.5%",
            "directional_action": "SHORT-FOLIO",
            "move_type": "DOWNSIDE_MOMENTUM",
        },
        "breakout": {
            "title": "🚀 Breakout",
            "description": "Price breaking out of resistance with volume",
            "action_explanation": "Strong breakout with institutional participation",
            "urgency": "high",
            "trading_instruction": "🚀 BREAKOUT: Enter long position - Target +1.5-3%, Stop -0.5%",
            "short_trading_instruction": "🛑 BREAKDOWN: Enter short position - Target -1.5-3%, Stop +0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "BREAKOUT",
        },
        "scalper_opportunity": {
            "title": "⚡ Scalper Opportunity",
            "description": "High-probability scalping setup detected",
            "action_explanation": "Quick scalping opportunity with tight risk/reward",
            "urgency": "high",
            "trading_instruction": "⚡ SCALP: Quick entry - Target +5 ticks, Stop -3 ticks",
            "directional_action": "SCALP",
            "move_type": "SCALPER_OPPORTUNITY",
        },
        # ICT Patterns
        "ict_liquidity_pools": {
            "title": "🏊 ICT Liquidity Pools",
            "description": "Smart money liquidity grab detected",
            "action_explanation": "Institutional liquidity grab - follow the break",
            "urgency": "high",
            "trading_instruction": "🏊 LIQUIDITY: Enter on liquidity break - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_LIQUIDITY_POOLS",
        },
        "ict_fair_value_gaps": {
            "title": "⚖️ ICT Fair Value Gaps",
            "description": "Price gap indicating fair value imbalance",
            "action_explanation": "Fair value gap - price likely to fill the gap",
            "urgency": "medium",
            "trading_instruction": "⚖️ FVG: Enter gap fill - Target gap close, Stop beyond gap",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_FAIR_VALUE_GAPS",
        },
        "ict_optimal_trade_entry": {
            "title": "🎯 ICT Optimal Trade Entry",
            "description": "Optimal trade entry zone identified",
            "action_explanation": "OTE zone - high probability entry point",
            "urgency": "high",
            "trading_instruction": "🎯 OTE: Enter at optimal zone - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_OPTIMAL_TRADE_ENTRY",
        },
        "ict_premium_discount": {
            "title": "💰 ICT Premium/Discount",
            "description": "Price trading at premium or discount to fair value",
            "action_explanation": "Premium/discount zone - mean reversion expected",
            "urgency": "medium",
            "trading_instruction": "💰 PREM/DISC: Enter mean reversion - Target fair value, Stop beyond zone",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_PREMIUM_DISCOUNT",
        },
        "ict_killzone": {
            "title": "⏰ ICT Killzone",
            "description": "High-probability time window for moves",
            "action_explanation": "Killzone active - optimal time for entries",
            "urgency": "high",
            "trading_instruction": "⏰ KILLZONE: Enter during killzone - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_KILLZONE",
        },
        "ict_momentum": {
            "title": "📊 ICT Momentum",
            "description": "ICT-based momentum pattern detected",
            "action_explanation": "ICT momentum - follow the institutional flow",
            "urgency": "high",
            "trading_instruction": "📊 ICT MOMENTUM: Follow institutional flow - Target +1-2%, Stop -0.5%",
            "directional_action": "BUY-FOLIO",
            "move_type": "ICT_MOMENTUM",
        },
        # Kow Signal Straddle
        "kow_signal_straddle": {
            "title": "🎯 Kow Signal Straddle",
            "description": "VWAP-based options straddle strategy",
            "action_explanation": "VWAP straddle - premium collection opportunity",
            "urgency": "high",
            "trading_instruction": "🎯 STRADDLE: Enter VWAP straddle - Collect premium, Manage Greeks",
            "directional_action": "STRADDLE",
            "move_type": "KOW_SIGNAL_STRADDLE",
        },
    }
    
    @classmethod
    def format_telegram_alert(cls, alert_data: Dict[str, Any]) -> str:
        """
        Format alert data for Telegram using actionable format.
        
        All production alerts should have trading parameters. If missing, actionable format
        will handle gracefully with defaults.
        
        CRITICAL: Dashboard URL is hardcoded for consumer access:
        - Dashboard URL: http://122.167.83.133:53056
        - DO NOT CHANGE without approval
        """
        try:
            # Resolve token to symbol if needed (fallback if not resolved earlier)
            alert_data = cls._ensure_symbol_resolved(alert_data)
            
            # Always use actionable format - it handles missing params gracefully
            return cls.format_actionable_alert(alert_data)
            
        except Exception as e:
            logger.error(f"Error formatting Telegram alert: {e}")
            return f"🚨 Alert: {alert_data.get('symbol', 'UNKNOWN')} - {alert_data.get('pattern', 'unknown')}"
    
    @classmethod
    def get_pattern_description(cls, pattern: str) -> Dict[str, str]:
        """Get pattern description by pattern name."""
        return cls.PATTERN_DESCRIPTIONS.get(pattern, {
            "title": f"📊 {pattern.replace('_', ' ').title()}",
            "description": f"Pattern: {pattern}",
            "action_explanation": "Pattern detected",
            "urgency": "medium",
            "trading_instruction": "Monitor closely",
            "directional_action": "WATCH",
            "move_type": pattern.upper(),
        })
    
    @classmethod
    def format_actionable_alert(cls, signal: Dict[str, Any]) -> str:
        """Transform pattern data into actionable trading alerts with detailed trading parameters.
        
        Handles both full actionable alerts (with trading params) and informational alerts gracefully.
        """
        try:
            # Extract core trading parameters
            action = signal.get('action', 'MONITOR')
            entry = float(signal.get('entry_price', signal.get('last_price', 0)) or 0)
            stop_loss = float(signal.get('stop_loss', 0) or 0)
            target = float(signal.get('target', 0) or 0)
            quantity = float(signal.get('quantity', 1) or 1)
            timeframe = signal.get('timeframe', 'INTRADAY')
            
            # Check if we have actionable trading parameters
            has_trading_params = (entry > 0 and stop_loss > 0 and target > 0 and action and action != 'MONITOR')
            
            # Calculate real risk metrics (from your pattern data)
            risk_points = abs(entry - stop_loss) if entry and stop_loss else 0
            risk_percent = (risk_points / entry * 100) if entry > 0 else 0
            reward_points = abs(target - entry) if target else 0
            reward_ratio = reward_points / risk_points if risk_points > 0 else 0
            
            # Determine order type from action
            order_type = "LIMIT ORDER" if 'LIMIT' in str(action).upper() else "MARKET ORDER"
            
            # Get pattern info
            pattern_type = signal.get('pattern_type', signal.get('pattern', 'Unknown'))
            pattern_desc = cls.get_pattern_description(pattern_type)
            pattern_title = pattern_desc.get('title', f"📊 {pattern_type.replace('_', ' ').title()}")
            trading_instruction = signal.get('trading_instruction', pattern_desc.get('trading_instruction', 'Monitor closely'))
            
            # Extract core alert data
            symbol = signal.get('symbol', 'UNKNOWN')
            confidence = float(signal.get('confidence', 0.0))
            last_price = float(signal.get('last_price', entry or 0))
            expected_move = float(signal.get('expected_move', 0.0))
            volume_ratio = float(signal.get('volume_ratio', 1.0))
            risk_summary = signal.get('risk_summary', '')
            
            # ✅ Extract indicators and Greeks from signal data
            # Indicators can be in: signal['indicators'], signal['rsi'], etc., or nested
            indicators = signal.get('indicators', {})
            if not isinstance(indicators, dict):
                indicators = {}
            # Also check top-level indicator fields
            if not indicators.get('rsi'):
                indicators['rsi'] = signal.get('rsi')
            if not indicators.get('atr'):
                indicators['atr'] = signal.get('atr')
            if not indicators.get('vwap'):
                indicators['vwap'] = signal.get('vwap')
            if not indicators.get('ema_20'):
                indicators['ema_20'] = signal.get('ema_20')
            if not indicators.get('macd'):
                indicators['macd'] = signal.get('macd')
            
            # Greeks can be in: signal['indicators']['greeks'], signal['delta'], etc.
            greeks = {}
            if isinstance(indicators.get('greeks'), dict):
                greeks = indicators['greeks']
            else:
                # Try top-level fields
                for greek in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                    if signal.get(greek) is not None:
                        greeks[greek] = signal.get(greek)
            
            # Format sections
            indicators_section = cls._format_indicators_section(indicators)
            greeks_section = cls._format_greeks_section(greeks, symbol)
            news_section = cls._format_news_section(signal)
            
            # CRITICAL: Dashboard URL for consumer access (external via Cloudflare Tunnel)
            # Dashboard runs on port 53056, exposed via Cloudflare Tunnel (primary) with Mullvad fallback
            # Public URL: https://award-inf-filed-cruz.trycloudflare.com
            DASHBOARD_URL = "https://award-inf-filed-cruz.trycloudflare.com"  # Public Cloudflare Tunnel URL (free, unlimited bandwidth)
            dashboard_link = f"📊 <a href='{DASHBOARD_URL}'>View Dashboard</a>"
            
            # Build actionable alert with Telegram HTML formatting
            if has_trading_params:
                # Full actionable format with trading parameters
                alert = f"""
🎯 <b>TRADE ALERT: {symbol}</b>

<b>ACTION:</b> <code>{action}</code>
<b>ENTRY:</b> <code>{entry:.2f}</code> ({order_type})
<b>STOP LOSS:</b> <code>{stop_loss:.2f}</code> ({risk_percent:.1f}% risk)
<b>TARGET:</b> <code>{target:.2f}</code> ({reward_ratio:.1f}R)
<b>QUANTITY:</b> <code>{quantity:.0f}</code> shares/lots
<b>TIME FRAME:</b> <code>{timeframe}</code>

<b>PATTERN:</b> {pattern_title}
<b>CONFIDENCE:</b> {confidence*100:.0f}%
<b>REASON:</b> {signal.get('reason', signal.get('description', pattern_desc.get('action_explanation', '')))}

<b>VOLUME RATIO:</b> {volume_ratio:.1f}x
<b>LAST PRICE:</b> ₹{last_price:.2f}

{indicators_section}
{greeks_section}
{news_section}
{risk_summary}

{dashboard_link}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                """.strip()
            else:
                # Informational format (no trading params) - streamlined actionable format
                alert = f"""
🚨 <b>{pattern_title}</b>
📊 <b>Symbol:</b> {symbol}
💰 <b>Price:</b> ₹{last_price:.2f}
📈 <b>Expected Move:</b> {expected_move:.2f}%
📊 <b>Volume Ratio:</b> {volume_ratio:.1f}x
🎯 <b>Confidence:</b> {confidence:.1%}
⚡ <b>Action:</b> {action if action else 'MONITOR'}

{trading_instruction}

{indicators_section}
{greeks_section}
{news_section}
{risk_summary}

{dashboard_link}
⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}
                """.strip()
            
            return alert
            
        except Exception as e:
            logger.error(f"Error formatting actionable alert: {e}")
            return f"Alert for {signal.get('symbol', 'Unknown')}: {signal.get('action', 'MONITOR')}"
    
    @classmethod
    def _format_news_context(cls, news_context: Optional[Dict[str, Any]]) -> str:
        """Format news context for inclusion in actionable alerts (markdown format)."""
        if not news_context:
            return ""
        
        try:
            if isinstance(news_context, dict):
                news_title = news_context.get('title', '')
                news_sentiment = news_context.get('sentiment', 'neutral')
                news_impact = news_context.get('impact', 'medium')
                
                if news_title:
                    return f"**NEWS:** {news_title[:100]}{'...' if len(news_title) > 100 else ''}\n**SENTIMENT:** {news_sentiment} | **IMPACT:** {news_impact}"
            
            # Fallback for string news context
            if isinstance(news_context, str):
                return f"**NEWS:** {news_context[:150]}"
            
            return ""
        except Exception as e:
            logger.debug(f"Error formatting news context: {e}")
            return ""
    
    @classmethod
    def _format_news_section(cls, signal: Dict[str, Any]) -> str:
        """Format integrated news section for actionable alerts (HTML format for Telegram)."""
        try:
            # Check for news enrichment data first (new format)
            news_items = signal.get('news', [])
            if news_items and isinstance(news_items, list) and len(news_items) > 0:
                return cls._format_news_enrichment_html(news_items)
            
            # Fallback to legacy news context
            news_context = signal.get('news_context')
            if news_context:
                return cls._format_news_context_html(news_context)
            
            return ""
            
        except Exception as e:
            logger.debug(f"Error formatting news section: {e}")
            return ""
    
    @classmethod
    def _format_news_context_html(cls, news_context: Optional[Dict[str, Any]]) -> str:
        """Format news context for inclusion in actionable alerts (HTML format for Telegram)."""
        if not news_context:
            return ""
        
        try:
            if isinstance(news_context, dict):
                news_title = news_context.get('title', '')
                news_sentiment = news_context.get('sentiment', 'neutral')
                news_impact = news_context.get('impact', 'medium')
                
                if news_title:
                    return f"<b>📰 NEWS:</b> {news_title[:100]}{'...' if len(news_title) > 100 else ''}\n<b>SENTIMENT:</b> {news_sentiment} | <b>IMPACT:</b> {news_impact}"
            
            # Fallback for string news context
            if isinstance(news_context, str):
                return f"<b>📰 NEWS:</b> {news_context[:150]}"
            
            return ""
        except Exception as e:
            logger.debug(f"Error formatting news context HTML: {e}")
            return ""
    
    @classmethod
    def _format_indicators_section(cls, indicators: Dict[str, Any]) -> str:
        """Format technical indicators section for actionable alerts (HTML format for Telegram)."""
        if not indicators or not isinstance(indicators, dict):
            return ""
        
        try:
            indicator_lines = []
            
            # RSI
            rsi = indicators.get('rsi')
            if rsi is not None:
                try:
                    rsi_val = float(rsi)
                    rsi_signal = "🟢" if rsi_val > 70 else "🔴" if rsi_val < 30 else "🟡"
                    indicator_lines.append(f"<b>RSI:</b> {rsi_val:.1f} {rsi_signal}")
                except (ValueError, TypeError):
                    pass
            
            # MACD
            macd = indicators.get('macd')
            if macd is not None:
                try:
                    if isinstance(macd, dict):
                        macd_val = macd.get('macd', macd.get('value', 0))
                        signal_val = macd.get('signal', 0)
                        histogram = macd.get('histogram', 0)
                        indicator_lines.append(f"<b>MACD:</b> {macd_val:.2f} | Signal: {signal_val:.2f} | Hist: {histogram:.2f}")
                    else:
                        indicator_lines.append(f"<b>MACD:</b> {float(macd):.2f}")
                except (ValueError, TypeError):
                    pass
            
            # EMAs
            ema_20 = indicators.get('ema_20')
            ema_50 = indicators.get('ema_50')
            if ema_20 is not None or ema_50 is not None:
                ema_parts = []
                if ema_20 is not None:
                    try:
                        ema_parts.append(f"EMA20: {float(ema_20):.2f}")
                    except (ValueError, TypeError):
                        pass
                if ema_50 is not None:
                    try:
                        ema_parts.append(f"EMA50: {float(ema_50):.2f}")
                    except (ValueError, TypeError):
                        pass
                if ema_parts:
                    indicator_lines.append(f"<b>EMAs:</b> {' | '.join(ema_parts)}")
            
            # ATR
            atr = indicators.get('atr')
            if atr is not None:
                try:
                    indicator_lines.append(f"<b>ATR:</b> {float(atr):.2f}")
                except (ValueError, TypeError):
                    pass
            
            # VWAP
            vwap = indicators.get('vwap')
            if vwap is not None:
                try:
                    indicator_lines.append(f"<b>VWAP:</b> {float(vwap):.2f}")
                except (ValueError, TypeError):
                    pass
            
            if indicator_lines:
                return "<b>📊 INDICATORS:</b>\n" + "\n".join(indicator_lines)
            return ""
            
        except Exception as e:
            logger.debug(f"Error formatting indicators section: {e}")
            return ""
    
    @classmethod
    def _format_greeks_section(cls, greeks: Dict[str, Any], symbol: str) -> str:
        """Format Options Greeks section for actionable alerts (HTML format for Telegram)."""
        if not greeks or not isinstance(greeks, dict):
            return ""
        
        # Only show Greeks for F&O instruments (options/futures)
        is_option = any(x in symbol.upper() for x in ['CE', 'PE', 'FUT']) or 'NFO:' in symbol.upper()
        if not is_option:
            return ""
        
        try:
            greek_lines = []
            
            # Delta
            delta = greeks.get('delta')
            if delta is not None:
                try:
                    delta_val = float(delta)
                    greek_lines.append(f"<b>Delta:</b> {delta_val:.3f}")
                except (ValueError, TypeError):
                    pass
            
            # Gamma
            gamma = greeks.get('gamma')
            if gamma is not None:
                try:
                    gamma_val = float(gamma)
                    greek_lines.append(f"<b>Gamma:</b> {gamma_val:.4f}")
                except (ValueError, TypeError):
                    pass
            
            # Theta
            theta = greeks.get('theta')
            if theta is not None:
                try:
                    theta_val = float(theta)
                    greek_lines.append(f"<b>Theta:</b> {theta_val:.2f}")
                except (ValueError, TypeError):
                    pass
            
            # Vega
            vega = greeks.get('vega')
            if vega is not None:
                try:
                    vega_val = float(vega)
                    greek_lines.append(f"<b>Vega:</b> {vega_val:.2f}")
                except (ValueError, TypeError):
                    pass
            
            # Rho
            rho = greeks.get('rho')
            if rho is not None:
                try:
                    rho_val = float(rho)
                    greek_lines.append(f"<b>Rho:</b> {rho_val:.2f}")
                except (ValueError, TypeError):
                    pass
            
            if greek_lines:
                return "<b>📈 GREEKS:</b>\n" + "\n".join(greek_lines)
            return ""
            
        except Exception as e:
            logger.debug(f"Error formatting Greeks section: {e}")
            return ""
    
    @classmethod
    def _format_news_enrichment_html(cls, news_items: Optional[List[Dict[str, Any]]]) -> str:
        """Format news enrichment data for inclusion in actionable alerts (HTML format for Telegram)."""
        if not news_items or not isinstance(news_items, list):
            return ""
        
        try:
            if len(news_items) == 0:
                return ""
            
            # Format first news item as primary
            primary_news = news_items[0]
            headline = primary_news.get('headline', '')
            news_source = primary_news.get('news_source', '')
            sentiment_score = primary_news.get('sentiment_score')
            
            if not headline:
                return ""
            
            # Format sentiment
            sentiment_text = "🟡 Neutral"  # Default
            if sentiment_score is not None:
                try:
                    score = float(sentiment_score)
                    if score > 0.3:
                        sentiment_text = "🟢 Positive"
                    elif score < -0.3:
                        sentiment_text = "🔴 Negative"
                    else:
                        sentiment_text = "🟡 Neutral"
                except (ValueError, TypeError):
                    # Handle string sentiment values
                    if isinstance(sentiment_score, str):
                        sentiment_lower = sentiment_score.lower()
                        if 'positive' in sentiment_lower:
                            sentiment_text = "🟢 Positive"
                        elif 'negative' in sentiment_lower:
                            sentiment_text = "🔴 Negative"
                        else:
                            sentiment_text = "🟡 Neutral"
            
            # Format news source
            source_text = f" ({news_source})" if news_source else ""
            
            # Format additional news items if present
            additional_news = ""
            if len(news_items) > 1:
                additional_news = f"\n<b>Related News:</b> {len(news_items)-1} more items"
            
            return f"<b>📰 NEWS:</b> {headline[:120]}{'...' if len(headline) > 120 else ''}{source_text}\n<b>SENTIMENT:</b> {sentiment_text}{additional_news}"
            
        except Exception as e:
            logger.debug(f"Error formatting news enrichment HTML: {e}")
            return ""
    
    @classmethod
    def _ensure_symbol_resolved(cls, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure symbol is resolved to human-readable format (fallback token resolution)."""
        try:
            symbol = alert_data.get('symbol', '')
            symbol_str = str(symbol) if symbol else ""
            
            # Check if symbol needs resolution (numeric token or UNKNOWN token)
            if not symbol_str or symbol_str == "UNKNOWN":
                # Try to get instrument_token from alert_data
                instrument_token = alert_data.get("instrument_token") or alert_data.get("token")
                if instrument_token:
                    try:
                        from crawlers.utils.instrument_mapper import InstrumentMapper
                        mapper = InstrumentMapper()
                        resolved = mapper.token_to_symbol(int(instrument_token))
                        if resolved and not resolved.startswith("UNKNOWN_"):
                            logger.debug(f"✅ Notifier: Resolved token {instrument_token} to {resolved}")
                            alert_data["symbol"] = resolved
                            return alert_data
                    except Exception as e:
                        logger.debug(f"Notifier token resolution failed for {instrument_token}: {e}")
            
            # Check if symbol itself is a token number
            elif symbol_str.isdigit() or symbol_str.startswith("TOKEN_") or symbol_str.startswith("UNKNOWN_"):
                try:
                    # Extract token number
                    if symbol_str.startswith("TOKEN_"):
                        token_str = symbol_str.replace("TOKEN_", "")
                    elif symbol_str.startswith("UNKNOWN_"):
                        token_str = symbol_str.replace("UNKNOWN_", "")
                    else:
                        token_str = symbol_str
                    
                    token = int(token_str)
                    from crawlers.utils.instrument_mapper import InstrumentMapper
                    mapper = InstrumentMapper()
                    resolved = mapper.token_to_symbol(token)
                    if resolved and not resolved.startswith("UNKNOWN_"):
                        logger.debug(f"✅ Notifier: Resolved token {token} to {resolved}")
                        alert_data["symbol"] = resolved
                        return alert_data
                except (ValueError, TypeError) as e:
                    logger.debug(f"Notifier: Could not parse token from symbol '{symbol_str}': {e}")
                except Exception as e:
                    logger.debug(f"Notifier: Token resolution failed for '{symbol_str}': {e}")
            
            # Return alert_data with original symbol if already resolved or resolution failed
            return alert_data
            
        except Exception as e:
            logger.debug(f"Notifier: Error in _ensure_symbol_resolved: {e}")
            return alert_data  # Return original on error