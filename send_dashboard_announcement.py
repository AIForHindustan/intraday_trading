#!/usr/bin/env python3
"""
Send dashboard announcement to Telegram bots
"""

import json
import sys
import requests
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_telegram_message(bot_token: str, chat_id: str, message: str) -> bool:
    """Send message to Telegram chat"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info(f"✅ Message sent to {chat_id}")
            return True
        else:
            logger.error(f"❌ Failed to send to {chat_id}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Error sending to {chat_id}: {e}")
        return False

def main():
    # Load Telegram config
    config_path = Path(__file__).parent / "alerts" / "config" / "telegram_config.json"
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return
    
    # Dashboard announcement message
    message = """📊 <b>NEW DASHBOARD AVAILABLE - BETA TESTING</b> 📊

🎯 <b>You now have 2 sources for alerts:</b>

1️⃣ <b>Telegram Alerts</b> (Current)
   • All symbols & patterns
   • Real-time notifications
   • Auto-delivered to your chat/channel
   
2️⃣ <b>Interactive Dashboard</b> (NEW - Beta)
   • <b>Selective filtering</b> - Choose which symbols/patterns to view
   • Visual charts & indicators
   • News enrichment
   • Options Greeks for options
   • Technical indicators (RSI, MACD, EMA, etc.)
   • Cross-check alerts before trading
   
🔗 <b>Dashboard URL:</b>
http://122.167.83.133:53056

⚠️ <b>Beta Testing Phase</b>
• Dashboard is in beta - use it alongside Telegram for verification
• Compare dashboard alerts with Telegram alerts
• Report any discrepancies or issues
• Full features coming soon!

📈 <b>Dashboard Features:</b>
✅ Filter by symbol (e.g., NIFTY, BANKNIFTY, RELIANCE)
✅ Filter by pattern (e.g., volume_spike, kow_signal_straddle)
✅ View price charts with alert timestamps
✅ See news context for news-driven alerts
✅ Technical indicators for equity/futures
✅ Options Greeks (Delta, Gamma, Theta, Vega) for options
✅ Entry price & expected move

💡 <b>How to Use:</b>
1. Open dashboard in browser
2. Use filters to select symbols/patterns
3. Click on alerts to see detailed charts
4. Cross-reference with Telegram alerts
5. Make informed trading decisions

🔄 <b>Both Sources Update in Real-Time:</b>
• Telegram: All alerts (90%+ confidence)
• Dashboard: All alerts (with filtering options)

<i>Happy Trading! 🚀</i>"""
    
    success_count = 0
    total_count = 0
    
    # Send to main bot channels
    main_bot_token = config.get('bot_token')
    main_chat_ids = config.get('chat_ids', [])
    
    if main_bot_token and main_chat_ids:
        for chat_id in main_chat_ids:
            total_count += 1
            if send_telegram_message(main_bot_token, chat_id, message):
                success_count += 1
    
    # Send to signal bot channels
    signal_bot_config = config.get('signal_bot', {})
    signal_bot_token = signal_bot_config.get('bot_token')
    signal_chat_ids = signal_bot_config.get('chat_ids', [])
    
    if signal_bot_token and signal_chat_ids:
        for chat_id in signal_chat_ids:
            total_count += 1
            if send_telegram_message(signal_bot_token, chat_id, message):
                success_count += 1
    
    logger.info(f"\n📊 Summary: Sent to {success_count}/{total_count} channels")
    
    if success_count == total_count:
        logger.info("✅ All notifications sent successfully!")
        return 0
    else:
        logger.warning(f"⚠️ Some notifications failed ({success_count}/{total_count})")
        return 1

if __name__ == '__main__':
    sys.exit(main())

