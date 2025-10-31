#!/usr/bin/env python3
"""
Send Dashboard Notification via Community Bot
Uses the community bot system to send notifications
"""

import asyncio
import json
import logging
from pathlib import Path
from community_bots.telegram_bot import AIONTelegramBot

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def send_dashboard_update():
    """Send dashboard update via community bot"""
    
    # Dashboard URL
    dashboard_url = "http://localhost:8000"
    
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
    
    # Initialize community bot
    bot = AIONTelegramBot()
    
    # Load config to get all channels
    config_path = Path("alerts/config/telegram_config.json")
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Could not load config: {e}")
        return False
    
    success_count = 0
    total_channels = 0
    
    # Send to main channel
    main_channel = config.get('chat_ids', ['@NSEAlgoTrading'])[0]
    success = await bot.send_message(message, main_channel)
    if success:
        success_count += 1
        logger.info(f"✅ Dashboard update sent to main channel: {main_channel}")
    else:
        logger.error(f"❌ Failed to send to main channel: {main_channel}")
    total_channels += 1
    
    # Send to signal bot channels
    signal_bot_config = config.get('signal_bot', {})
    signal_channels = signal_bot_config.get('chat_ids', [])
    
    for channel in signal_channels:
        if channel != main_channel:
            success = await bot.send_message(message, channel)
            if success:
                success_count += 1
                logger.info(f"✅ Dashboard update sent to: {channel}")
            else:
                logger.error(f"❌ Failed to send to: {channel}")
        total_channels += 1
    
    logger.info(f"📊 Sent to {success_count}/{total_channels} channels successfully")
    return success_count == total_channels

async def main():
    """Main function"""
    print("🚀 Sending Dashboard Update via Community Bot...")
    
    success = await send_dashboard_update()
    
    if success:
        print("✅ All notifications sent successfully via community bot!")
    else:
        print("❌ Some notifications failed to send")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())
