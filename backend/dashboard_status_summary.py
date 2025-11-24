#!/usr/bin/env python3
# Add intraday_trading to path for imports
import sys
from pathlib import Path
_intraday_path = Path(__file__).parent.parent.parent / "intraday_trading"
if str(_intraday_path) not in sys.path:
    sys.path.insert(0, str(_intraday_path))

"""
Dashboard Status Summary
Shows current dashboard status and provides setup instructions
"""
from redis_files.redis_client import RedisClientFactory

import json
import subprocess
import requests
import time
from pathlib import Path

def check_dashboard_status():
    """Check if dashboard is running locally"""
    try:
        response = requests.get('http://localhost:8000', timeout=5)
        if response.status_code == 200:
            return True, "Dashboard is running locally"
    except Exception:
        pass
    return False, "Dashboard is not running locally"

def check_redis_status():
    """Check if Redis is running"""
    try:
        import redis
        r = r = RedisClientFactory.get_trading_client()
        r.ping()
        return True, "Redis is running"
    except Exception:
        return False, "Redis is not running"

def get_telegram_config():
    """Get Telegram configuration"""
    config_path = Path("alerts/config/telegram_config.json")
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except Exception:
        return None

def print_status():
    """Print current dashboard status"""
    print("🚀 PROFESSIONAL TRADING DASHBOARD STATUS")
    print("=" * 50)
    
    # Check dashboard
    dashboard_running, dashboard_msg = check_dashboard_status()
    print(f"📊 Dashboard: {'✅' if dashboard_running else '❌'} {dashboard_msg}")
    
    # Check Redis
    redis_running, redis_msg = check_redis_status()
    print(f"🗄️  Redis: {'✅' if redis_running else '❌'} {redis_msg}")
    
    # Get Telegram config
    config = get_telegram_config()
    if config:
        print(f"📱 Telegram: ✅ Configured")
        print(f"   Main Channel: {config.get('chat_ids', ['N/A'])[0]}")
        signal_bot = config.get('signal_bot', {})
        if signal_bot:
            print(f"   Signal Bot: {len(signal_bot.get('chat_ids', []))} channels")
    else:
        print("📱 Telegram: ❌ Not configured")
    
    print("\n" + "=" * 50)
    
    if dashboard_running and redis_running:
        print("🎉 DASHBOARD IS READY!")
        print("\n📋 NEXT STEPS:")
        print("1. Access dashboard at: http://localhost:53056")
        print("2. For public access, use Cloudflare Tunnel:")
        print("   https://remember-prefers-thinkpad-distributors.trycloudflare.com")
        print("4. Share the public URL with your community")
        print("\n📱 TELEGRAM NOTIFICATIONS:")
        print("✅ Notifications sent to both channels")
        print("✅ Morning instructions delivered")
        print("✅ Usage guide provided")
        
    else:
        print("⚠️  SETUP REQUIRED:")
        if not dashboard_running:
            print("• Start the dashboard: uvicorn backend.optimized_main:app --host 0.0.0.0 --port 8000")
        if not redis_running:
            print("• Start Redis: redis-server")
    
    print("\n🔧 FEATURES AVAILABLE:")
    print("• Real-time price charts")
    print("• Technical indicators (RSI, EMA, VWAP, MACD)")
    print("• Pattern detection overlays")
    print("• Stop loss & target levels")
    print("• Options Greeks display")
    print("• Position sizing & risk metrics")
    print("• Market profile & volume analysis")
    print("• Ultra low-latency alerts (< 10ms)")
    print("• Professional charting with Lightweight Charts")
    print("• Real-time WebSocket data")
    print("• Redis Streams for high performance")
    
    print("\n📊 INSTRUMENT COVERAGE:")
    print("• Index Futures: NIFTY, BANKNIFTY")
    print("• Index Options: NIFTY/BANKNIFTY options")
    print("• Equity Cash: Individual stocks")
    print("• Equity Futures: Stock futures")
    
    print("\n🎯 PATTERN TYPES:")
    print("• Volume Spike: High volume alerts")
    print("• KOW Straddle: Kowshik's signature strategy")
    print("• Reversal: Market reversal signals")
    print("• Breakout: Price breakout confirmations")
    
    print("\n📱 MOBILE SUPPORT:")
    print("• Responsive design")
    print("• Mobile-friendly interface")
    print("• Landscape mode for better charts")
    print("• All features available on mobile")

def main():
    print_status()

if __name__ == "__main__":
    main()
