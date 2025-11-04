#!/usr/bin/env python3
"""
Send Dashboard Link Fix Notification to Both Telegram Bots
Sends notification about the fixed dashboard link to main bot and signal bot
"""

import sys
import json
import logging
import requests
import time
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_dashboard_link_notification():
    """Send dashboard link fix notification to both Telegram bots"""
    
    # Dashboard URL - public ngrok tunnel (accessible from anywhere)
    DASHBOARD_URL = "https://jere-unporous-magan.ngrok-free.dev"  # Public ngrok URL
    
    message = f"""✅ <b>DASHBOARD LINK FIXED!</b> ✅

📊 <b>Dashboard is now accessible:</b>
🔗 <a href="{DASHBOARD_URL}">{DASHBOARD_URL}</a>

⚠️ <b>Note:</b> First visit shows ngrok warning page - click "Visit Site" to proceed. This is normal for free ngrok accounts.

🎯 <b>What's Available:</b>
• Real-time alerts with indicators (RSI, MACD, EMA, VWAP, ATR)
• Options Greeks (Delta, Gamma, Theta, Vega, Rho)
• News enrichment for each alert
• Interactive price charts
• Filter by symbol and pattern
• Expected move calculations
• Entry prices, stop loss, and targets

⚡ <b>Features:</b>
✅ Technical indicators for equity/futures
✅ Options Greeks for F&O instruments
✅ News context integration
✅ Live price action visualization
✅ Pattern distribution analysis
✅ Alert filtering and sorting

📱 <b>Mobile Friendly:</b>
• Works on mobile browsers
• Responsive design
• Touch-optimized charts

🔧 <b>How to Use:</b>
1. Click the dashboard link above or in alerts
2. Select symbol from dropdown
3. Choose pattern type to filter
4. Click on alerts to see detailed charts
5. View indicators and news context

🚀 <b>Real-time Updates:</b>
• Dashboard refreshes automatically
• New alerts appear instantly
• Indicators update live
• News enriched automatically

<i>Dashboard powered by AION Trading System</i>"""

    try:
        # Load Telegram config
        config_path = Path(__file__).parent / "alerts" / "config" / "telegram_config.json"
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        main_bot_token = config.get("bot_token")
        main_chat_ids = config.get("chat_ids", [])
        
        # Signal bot config
        signal_bot_config = config.get("signal_bot", {})
        signal_chat_ids = signal_bot_config.get("chat_ids", [])
        signal_bot_token = signal_bot_config.get("bot_token")
        
        success_count = 0
        
        # Send to main bot channels
        if main_bot_token:
            for chat_id in main_chat_ids:
                try:
                    url = f"https://api.telegram.org/bot{main_bot_token}/sendMessage"
                    payload = {
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": False
                    }
                    response = requests.post(url, json=payload, timeout=10)
                    if response.status_code == 200:
                        success_count += 1
                        logger.info(f"✅ Sent to main bot channel: {chat_id}")
                    else:
                        logger.warning(f"⚠️ Failed to send to main bot channel {chat_id}: {response.status_code} - {response.text}")
                except Exception as e:
                    logger.error(f"Error sending to main bot channel {chat_id}: {e}")
        
        # Send to signal bot channels
        if signal_bot_token and signal_chat_ids:
            for chat_id in signal_chat_ids:
                try:
                    url = f"https://api.telegram.org/bot{signal_bot_token}/sendMessage"
                    payload = {
                        "chat_id": chat_id,
                        "text": message,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": False
                    }
                    response = requests.post(url, json=payload, timeout=10)
                    if response.status_code == 200:
                        success_count += 1
                        logger.info(f"✅ Sent to signal bot channel: {chat_id}")
                    else:
                        logger.warning(f"⚠️ Failed to send to signal bot channel {chat_id}: {response.status_code} - {response.text}")
                except Exception as e:
                    logger.error(f"Error sending to signal bot channel {chat_id}: {e}")
        
        if success_count > 0:
            logger.info(f"✅ Successfully sent dashboard link notification to {success_count} channel(s)")
            return True
        else:
            logger.error("❌ Failed to send to any channels")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error sending dashboard link notification: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = send_dashboard_link_notification()
    sys.exit(0 if success else 1)

