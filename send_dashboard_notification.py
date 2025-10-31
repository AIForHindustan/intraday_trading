#!/usr/bin/env python3
"""
Send Professional Trading Dashboard Notification
Sends notification about the new dashboard to both Telegram channels
"""

import asyncio
import json
import logging
from pathlib import Path
from community_bots.telegram_bot import AIONTelegramBot

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DashboardNotifier:
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
    
    async def send_dashboard_notification(self):
        """Send dashboard notification to all configured channels"""
        
        # Dashboard URL (replace with your actual public URL)
        dashboard_url = "http://122.167.83.133:53056"  # Mullvad public URL
        
        message = f"""🚀 *TRADING DASHBOARD IS NOW SUPER FAST!* 🚀

⚡ *WHAT'S NEW:*
• Dashboard loads 20x faster than before
• No more waiting for data to load
• Everything happens instantly now
• Works great on mobile phones too

📊 *ACCESS YOUR DASHBOARD:*
{dashboard_url}

🎯 *WHAT YOU CAN DO:*
• Watch live stock prices move in real-time
• See helpful indicators (RSI, EMA, VWAP, MACD)
• Get alerts when good trading opportunities appear
• Choose from different stocks and options
• Read latest market news
• Set stop loss and target prices

🔧 *HOW TO START TRADING:*
1. *Choose Market:* Pick from NIFTY, BANKNIFTY, or individual stocks
2. *Select Stock:* Pick the specific stock you want to trade
3. *Watch Alerts:* Look for green/red signals that appear
4. *Check Indicators:* See if the market is going up or down
5. *Make Decisions:* Use the information to decide when to buy/sell

⚡ *WHY IT'S BETTER NOW:*
• Loads instantly (no waiting)
• Shows real-time data
• Works on your phone
• Easy to understand
• Professional trading tools

📱 *MOBILE TRADING:*
• Open the link on your phone
• Turn phone sideways for better charts
• All features work on mobile
• Touch-friendly buttons

*Perfect for beginners and experienced traders!*

_Dashboard powered by AION Algorithmic Trading System_"""
        
        # Send to main channel
        main_channel = self.config.get('chat_ids', ['@NSEAlgoTrading'])[0]
        success_main = await self.telegram_bot.send_message(message, main_channel)
        
        # Send to signal bot channels if available
        signal_bot_config = self.config.get('signal_bot', {})
        signal_channels = signal_bot_config.get('chat_ids', [])
        
        success_signal = True
        for channel in signal_channels:
            if channel != main_channel:  # Avoid duplicate to main channel
                success = await self.telegram_bot.send_message(message, channel)
                success_signal = success_signal and success
                if success:
                    logger.info(f"✅ Dashboard notification sent to {channel}")
                else:
                    logger.error(f"❌ Failed to send to {channel}")
        
        if success_main:
            logger.info(f"✅ Dashboard notification sent to main channel: {main_channel}")
        else:
            logger.error(f"❌ Failed to send to main channel: {main_channel}")
        
        return success_main and success_signal
    
    async def send_usage_instructions(self):
        """Send detailed usage instructions"""
        
        instructions = """📖 *DASHBOARD USAGE INSTRUCTIONS* 📖

*1. GETTING STARTED:*
• Open the dashboard link in your browser
• Wait for instruments to load (may take a few seconds)
• Select your preferred asset class from the dropdown

*2. INSTRUMENT SELECTION:*
• *Index Futures:* NIFTY, BANKNIFTY futures contracts
• *Index Options:* NIFTY/BANKNIFTY options with strike prices
• *Equity Cash:* Individual stocks (RELIANCE, TCS, HDFC BANK, etc.)
• *Equity Futures:* Stock futures contracts

*3. PATTERN FILTERING:*
• *Volume Spike:* Unusual volume activity
• *KOW Straddle:* Kowshik's signature straddle strategy
• *Reversal:* Market reversal patterns
• *Breakout:* Price breakout confirmations

*4. CHART CONTROLS:*
• *Timeframes:* 1m, 5m, 15m, 1h buttons
• *Pattern Overlays:* Toggle specific patterns on/off
• *Real-time Updates:* Automatic data refresh

*5. TECHNICAL INDICATORS:*
• *RSI:* Relative Strength Index (0-100)
• *EMA 20/50:* Exponential Moving Averages
• *VWAP:* Volume Weighted Average Price
• *MACD:* Moving Average Convergence Divergence
• *Bollinger Bands:* Price volatility indicators

*6. ALERT SYSTEM:*
• Real-time alerts appear in the table
• Color-coded by confidence level (High/Medium/Low)
• Click "⚡ Trade" button for quick actions
• Latency display shows alert processing speed

*7. TROUBLESHOOTING:*
• If indicators don't show: Wait for alerts to populate
• If chart is empty: Select an instrument first
• If data seems stale: Refresh the page
• For mobile: Use landscape mode for better experience

_Need help? Contact the development team_"""
        
        # Send to main channel
        main_channel = self.config.get('chat_ids', ['@NSEAlgoTrading'])[0]
        success = await self.telegram_bot.send_message(instructions, main_channel)
        
        if success:
            logger.info("✅ Usage instructions sent to main channel")
        else:
            logger.error("❌ Failed to send usage instructions")
        
        return success

async def main():
    """Main function to send dashboard notifications"""
    notifier = DashboardNotifier()
    
    print("🚀 Sending Professional Trading Dashboard Notification...")
    
    # Send main notification
    success1 = await notifier.send_dashboard_notification()
    
    # Wait a bit between messages
    await asyncio.sleep(2)
    
    # Send usage instructions
    success2 = await notifier.send_usage_instructions()
    
    if success1 and success2:
        print("✅ All notifications sent successfully!")
    else:
        print("❌ Some notifications failed to send")
    
    return success1 and success2

if __name__ == "__main__":
    asyncio.run(main())
