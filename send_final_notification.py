#!/usr/bin/env python3
"""
Send Final Dashboard Notification
Sends final notification with public access instructions
"""

import asyncio
import json
import logging
from pathlib import Path
from community_bots.telegram_bot import AIONTelegramBot

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinalNotificationSender:
    def __init__(self):
        self.telegram_bot = AIONTelegramBot()
        
        # Load Telegram config
        config_path = Path("alerts/config/telegram_config.json")
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logger.error(f"Could not load Telegram config: {e}")
            self.config = {}
    
    async def send_final_notification(self):
        """Send final notification with public access instructions"""
        
        message = """🎉 *DASHBOARD IS NOW LIVE AND READY!* 🎉

🔗 *ACCESS YOUR PROFESSIONAL TRADING DASHBOARD:*
http://localhost:8000

📱 *FOR PUBLIC ACCESS (RECOMMENDED):*

*Option 1: Using ngrok (Easiest)*
1. Install ngrok: `brew install ngrok/ngrok/ngrok`
2. Run: `ngrok http 8000`
3. Share the public URL with your community

*Option 2: Using your server*
1. Deploy to your VPS/cloud server
2. Configure domain and SSL
3. Share the public URL

*Option 3: Local network access*
1. Find your local IP: `ifconfig | grep inet`
2. Access via: `http://YOUR_IP:8000`
3. Share with users on same network

🚀 *DASHBOARD FEATURES READY:*

✅ *Real-time Charts* - Live price updates
✅ *Technical Indicators* - RSI, EMA, VWAP, MACD
✅ *Pattern Detection* - Volume spikes, KOW straddle, reversals
✅ *Alert System* - Ultra low-latency alerts
✅ *Mobile Support* - Works on all devices
✅ *Professional UI* - Modern trading interface

📊 *INSTRUMENTS COVERED:*
• NIFTY & BANKNIFTY Futures
• NIFTY & BANKNIFTY Options
• Equity Cash (RELIANCE, TCS, HDFC BANK, etc.)
• Equity Futures

🎯 *PATTERN TYPES:*
• Volume Spike alerts
• KOW Straddle signals
• Market reversal patterns
• Price breakout confirmations

⚡ *PERFORMANCE:*
• < 10ms alert latency
• Real-time WebSocket data
• Redis Streams for high performance
• Professional charting library

📱 *MOBILE TRADING:*
• Responsive design
• Landscape mode for charts
• All features available on mobile
• Touch-friendly interface

🔧 *QUICK START:*
1. Open the dashboard URL
2. Select your asset class
3. Choose instruments
4. Watch real-time alerts
5. Use chart controls for analysis

⚠️ *IMPORTANT NOTES:*
• Always verify signals with multiple indicators
• Use proper risk management
• Don't trade on signals alone
• Keep stop losses tight

📞 *SUPPORT:*
• Contact development team for issues
• Check previous messages for detailed instructions
• All features are documented in the dashboard

*Happy Trading! The dashboard is ready for action!* 🚀

_Dashboard powered by AION Algorithmic Trading System_"""
        
        # Send to all configured channels
        success_count = 0
        total_channels = 0
        
        # Send to main channel
        main_channel = self.config.get('chat_ids', ['@NSEAlgoTrading'])[0]
        success = await self.telegram_bot.send_message(message, main_channel)
        if success:
            success_count += 1
            logger.info(f"✅ Final notification sent to main channel: {main_channel}")
        else:
            logger.error(f"❌ Failed to send to main channel: {main_channel}")
        total_channels += 1
        
        # Send to signal bot channels
        signal_bot_config = self.config.get('signal_bot', {})
        signal_channels = signal_bot_config.get('chat_ids', [])
        
        for channel in signal_channels:
            if channel != main_channel:
                success = await self.telegram_bot.send_message(message, channel)
                if success:
                    success_count += 1
                    logger.info(f"✅ Final notification sent to: {channel}")
                else:
                    logger.error(f"❌ Failed to send to: {channel}")
                total_channels += 1
        
        logger.info(f"📊 Sent to {success_count}/{total_channels} channels successfully")
        return success_count == total_channels

async def main():
    """Main function to send final notification"""
    sender = FinalNotificationSender()
    
    print("🎉 Sending Final Dashboard Notification...")
    
    success = await sender.send_final_notification()
    
    if success:
        print("✅ Final notification sent successfully!")
    else:
        print("❌ Some notifications failed to send")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())
