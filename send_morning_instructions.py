#!/usr/bin/env python3
"""
Send Morning Dashboard Instructions
Sends comprehensive morning instructions to both Telegram channels
"""

import asyncio
import json
import logging
from pathlib import Path
from community_bots.telegram_bot import AIONTelegramBot

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MorningInstructionsSender:
    def __init__(self):
        self.telegram_bot = AIONTelegramBot()
        
        # Load Telegram config to get all channels
        config_path = Path("alerts/config/telegram_config.json")
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logger.error(f"Could not load Telegram config: {e}")
            self.config = {}
    
    async def send_morning_instructions(self):
        """Send comprehensive morning instructions to all channels"""
        
        # Try to get public URL from ngrok if available
        public_url = self.get_public_url()
        dashboard_url = public_url or "http://localhost:8000"
        
        message = f"""🌅 *GOOD MORNING TRADERS!* 🌅

🚀 *PROFESSIONAL TRADING DASHBOARD IS LIVE!*

🔗 *ACCESS YOUR DASHBOARD:*
{dashboard_url}

📊 *MORNING SETUP CHECKLIST:*

*1. DASHBOARD ACCESS:*
• Open the URL above in your browser
• Bookmark it for quick access
• Ensure you have a stable internet connection

*2. INSTRUMENT SELECTION:*
• *Index Futures:* NIFTY, BANKNIFTY (most active)
• *Index Options:* For options strategies
• *Equity Cash:* Individual stocks
• *Equity Futures:* Stock futures

*3. PATTERN FILTERING:*
• *Volume Spike:* High volume alerts
• *KOW Straddle:* Kowshik's signature strategy
• *Reversal:* Market reversal signals
• *Breakout:* Price breakout confirmations

*4. REAL-TIME MONITORING:*
• Watch the Technical Indicators section
• Monitor the alerts table for new signals
• Use chart controls for different timeframes
• Toggle pattern overlays as needed

⚡ *KEY FEATURES TO USE TODAY:*

*📈 CHART ANALYSIS:*
• Switch between 1m, 5m, 15m, 1h timeframes
• Enable/disable pattern overlays
• Watch real-time price updates

*📊 TECHNICAL INDICATORS:*
• RSI: Overbought/oversold levels
• EMA 20/50: Trend direction
• VWAP: Volume-weighted average price
• MACD: Momentum signals
• Bollinger Bands: Volatility levels

*🚨 ALERT SYSTEM:*
• Real-time alerts with confidence scores
• Color-coded by reliability (High/Medium/Low)
• Latency display shows processing speed
• Click "⚡ Trade" for quick actions

*📱 MOBILE TRADING:*
• Dashboard works on mobile browsers
• Use landscape mode for better charts
• All features available on mobile

🎯 *TRADING TIPS FOR TODAY:*

*MORNING SESSION (9:15 AM - 12:00 PM):*
• Focus on NIFTY/BANKNIFTY futures
• Watch for volume spikes in first 30 minutes
• Monitor KOW Straddle signals closely

*AFTERNOON SESSION (12:00 PM - 3:30 PM):*
• Switch to equity options if needed
• Use 5m/15m timeframes for better signals
• Watch for reversal patterns

*EVENING SESSION (3:30 PM - 4:00 PM):*
• Monitor closing patterns
• Watch for end-of-day breakouts
• Prepare for next day's setup

⚠️ *IMPORTANT REMINDERS:*
• Always verify signals with multiple indicators
• Use proper risk management
• Don't trade on signals alone - use your judgment
• Keep stop losses tight during volatile periods

🔧 *TROUBLESHOOTING:*
• If dashboard doesn't load: Refresh the page
• If indicators are empty: Wait for alerts to populate
• If chart is blank: Select an instrument first
• For mobile issues: Use landscape mode

📞 *SUPPORT:*
• Contact the development team for technical issues
• Check the usage instructions in the previous message
• All features are documented in the dashboard

*Happy Trading! May the markets be in your favor!* 🎯

_Dashboard powered by AION Algorithmic Trading System_"""
        
        # Send to all configured channels
        success_count = 0
        total_channels = 0
        
        # Send to main channel
        main_channel = self.config.get('chat_ids', ['@NSEAlgoTrading'])[0]
        success = await self.telegram_bot.send_message(message, main_channel)
        if success:
            success_count += 1
            logger.info(f"✅ Morning instructions sent to main channel: {main_channel}")
        else:
            logger.error(f"❌ Failed to send to main channel: {main_channel}")
        total_channels += 1
        
        # Send to signal bot channels
        signal_bot_config = self.config.get('signal_bot', {})
        signal_channels = signal_bot_config.get('chat_ids', [])
        
        for channel in signal_channels:
            if channel != main_channel:  # Avoid duplicate to main channel
                success = await self.telegram_bot.send_message(message, channel)
                if success:
                    success_count += 1
                    logger.info(f"✅ Morning instructions sent to: {channel}")
                else:
                    logger.error(f"❌ Failed to send to: {channel}")
                total_channels += 1
        
        logger.info(f"📊 Sent to {success_count}/{total_channels} channels successfully")
        return success_count == total_channels
    
    def get_public_url(self):
        """Try to get public URL from ngrok if running"""
        try:
            import requests
            response = requests.get('http://localhost:4040/api/tunnels', timeout=2)
            if response.status_code == 200:
                data = response.json()
                tunnels = data.get('tunnels', [])
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'https':
                        return tunnel.get('public_url')
                # Fallback to http
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'http':
                        return tunnel.get('public_url')
        except Exception:
            pass
        return None
    
    async def send_quick_start_guide(self):
        """Send a quick start guide for immediate use"""
        
        quick_guide = """⚡ *QUICK START GUIDE* ⚡

*IMMEDIATE ACTION ITEMS:*

1️⃣ *OPEN DASHBOARD NOW:*
• Click the dashboard URL
• Bookmark it in your browser
• Keep it open during trading hours

2️⃣ *SELECT YOUR INSTRUMENTS:*
• Choose "Index Futures" for NIFTY/BANKNIFTY
• Pick "Index Options" for options strategies
• Select specific symbols and expiries

3️⃣ *ENABLE PATTERN FILTERS:*
• Turn on "Volume Spikes" for high-volume alerts
• Enable "KOW Straddle" for signature signals
• Watch for "Reversal" patterns

4️⃣ *MONITOR INDICATORS:*
• RSI above 70 = Overbought (Sell signal)
• RSI below 30 = Oversold (Buy signal)
• EMA 20 > EMA 50 = Uptrend
• VWAP = Key support/resistance level

5️⃣ *WATCH ALERTS TABLE:*
• High confidence (90%+) = Strong signal
• Medium confidence (80-90%) = Good signal
• Low confidence (<80%) = Weak signal

*READY TO TRADE!* 🚀

_Start with small positions and build confidence_"""
        
        # Send to main channel
        main_channel = self.config.get('chat_ids', ['@NSEAlgoTrading'])[0]
        success = await self.telegram_bot.send_message(quick_guide, main_channel)
        
        if success:
            logger.info("✅ Quick start guide sent to main channel")
        else:
            logger.error("❌ Failed to send quick start guide")
        
        return success

async def main():
    """Main function to send morning instructions"""
    sender = MorningInstructionsSender()
    
    print("🌅 Sending Morning Dashboard Instructions...")
    
    # Send comprehensive morning instructions
    success1 = await sender.send_morning_instructions()
    
    # Wait a moment
    await asyncio.sleep(2)
    
    # Send quick start guide
    success2 = await sender.send_quick_start_guide()
    
    if success1 and success2:
        print("✅ All morning instructions sent successfully!")
    else:
        print("❌ Some instructions failed to send")
    
    return success1 and success2

if __name__ == "__main__":
    asyncio.run(main())
