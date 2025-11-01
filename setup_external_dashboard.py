#!/usr/bin/env python3
"""
Setup External Dashboard Access for Port 53056
Creates a public ngrok URL for external consumers
"""

import subprocess
import time
import requests
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExternalDashboardSetup:
    def __init__(self, port=53056):
        self.local_port = port
        self.ngrok_process = None
        self.public_url = None
    
    def check_ngrok_installed(self):
        """Check if ngrok is installed"""
        try:
            result = subprocess.run(['ngrok', 'version'], capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("✅ ngrok is installed")
                return True
        except FileNotFoundError:
            logger.error("❌ ngrok is not installed")
            return False
        return False
    
    def stop_existing_ngrok(self):
        """Stop any existing ngrok processes for this port"""
        try:
            subprocess.run(['pkill', '-f', f'ngrok http {self.local_port}'], 
                         stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            time.sleep(1)
            logger.info("✅ Stopped any existing ngrok tunnels")
        except Exception:
            pass
    
    def start_ngrok_tunnel(self):
        """Start ngrok tunnel for the dashboard"""
        try:
            # Stop existing tunnels first
            self.stop_existing_ngrok()
            
            logger.info(f"🚀 Starting ngrok tunnel on port {self.local_port}...")
            
            # Start ngrok process in background
            log_file = Path("logs/ngrok.log")
            log_file.parent.mkdir(exist_ok=True)
            
            self.ngrok_process = subprocess.Popen(
                ['ngrok', 'http', str(self.local_port), '--log=stdout'],
                stdout=open(log_file, 'w'),
                stderr=subprocess.STDOUT
            )
            
            # Wait for ngrok to start
            logger.info("⏳ Waiting for ngrok to initialize...")
            time.sleep(5)
            
            # Get the public URL
            max_retries = 10
            for i in range(max_retries):
                self.public_url = self.get_ngrok_url()
                if self.public_url:
                    logger.info(f"✅ Public URL created: {self.public_url}")
                    return True
                logger.info(f"⏳ Retrying... ({i+1}/{max_retries})")
                time.sleep(2)
            
            logger.error("❌ Failed to get ngrok URL after retries")
            return False
                
        except Exception as e:
            logger.error(f"❌ Error starting ngrok: {e}")
            return False
    
    def get_ngrok_url(self):
        """Get the public URL from ngrok API"""
        try:
            response = requests.get('http://localhost:4040/api/tunnels', timeout=5)
            if response.status_code == 200:
                data = response.json()
                tunnels = data.get('tunnels', [])
                
                # Prefer HTTPS tunnel
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'https':
                        public_url = tunnel.get('public_url')
                        if public_url:
                            return public_url
                
                # Fallback to HTTP tunnel
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'http':
                        public_url = tunnel.get('public_url')
                        if public_url:
                            return public_url
            else:
                logger.debug(f"ngrok API returned status {response.status_code}")
        except requests.exceptions.ConnectionError:
            logger.debug("ngrok API not ready yet")
        except Exception as e:
            logger.debug(f"Error getting ngrok URL: {e}")
        return None
    
    def verify_url_accessibility(self, url):
        """Verify the public URL is accessible"""
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Verified public URL is accessible: {url}")
                return True
            else:
                logger.warning(f"⚠️ Public URL returned status {response.status_code}")
                return False
        except Exception as e:
            logger.warning(f"⚠️ Could not verify public URL: {e}")
            return False
    
    def update_dashboard_url_in_code(self, public_url):
        """Update dashboard URL in alerts/notifiers.py"""
        try:
            notifiers_path = Path("alerts/notifiers.py")
            if notifiers_path.exists():
                content = notifiers_path.read_text()
                
                # Find and replace the DASHBOARD_URL line
                import re
                pattern = r'DASHBOARD_URL = "[^"]*"'
                replacement = f'DASHBOARD_URL = "{public_url}"'
                
                updated_content = re.sub(pattern, replacement, content)
                notifiers_path.write_text(updated_content)
                logger.info(f"✅ Updated dashboard URL in alerts/notifiers.py to: {public_url}")
                return True
        except Exception as e:
            logger.error(f"❌ Error updating dashboard URL in code: {e}")
        return False
    
    def send_notification(self, public_url):
        """Send notification with public URL to both Telegram bots"""
        try:
            import requests
            
            config_path = Path("alerts/config/telegram_config.json")
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            message = f"""🌐 <b>DASHBOARD NOW ACCESSIBLE EXTERNALLY!</b> 🌐

🔗 <b>Public URL (Works from anywhere):</b>
<a href="{public_url}">{public_url}</a>

✅ <b>Features:</b>
• Real-time alerts with indicators
• Options Greeks for F&O
• News enrichment
• Interactive charts
• Mobile-friendly

📱 <b>Access from:</b>
• Mobile phones (anywhere)
• Desktop browsers
• Tablets
• No VPN required

🚀 <b>How to Use:</b>
1. Click the dashboard link above
2. Select symbol and pattern
3. View real-time alerts and charts
4. Check indicators and news

<i>Dashboard powered by AION Trading System</i>"""

            success_count = 0
            
            # Send to main bot
            main_bot_token = config.get("bot_token")
            main_chat_ids = config.get("chat_ids", [])
            
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
                    except Exception as e:
                        logger.error(f"Error sending to main bot {chat_id}: {e}")
            
            # Send to signal bot
            signal_bot_config = config.get("signal_bot", {})
            signal_bot_token = signal_bot_config.get("bot_token")
            signal_chat_ids = signal_bot_config.get("chat_ids", [])
            
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
                    except Exception as e:
                        logger.error(f"Error sending to signal bot {chat_id}: {e}")
            
            if success_count > 0:
                logger.info(f"✅ Notifications sent to {success_count} channel(s)")
                return True
            else:
                logger.error("❌ Failed to send to any channels")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending notification: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def setup(self):
        """Main setup function"""
        logger.info("🚀 Setting up external dashboard access...")
        
        # Check if ngrok is installed
        if not self.check_ngrok_installed():
            logger.error("Please install ngrok: brew install ngrok/ngrok/ngrok")
            return False
        
        # Start ngrok tunnel
        if not self.start_ngrok_tunnel():
            return False
        
        # Verify URL is accessible
        self.verify_url_accessibility(self.public_url)
        
        # Update code with new URL
        self.update_dashboard_url_in_code(self.public_url)
        
        # Send notifications
        self.send_notification(self.public_url)
        
        logger.info(f"\n🎉 SUCCESS! Dashboard is now externally accessible at:")
        logger.info(f"   {self.public_url}")
        logger.info(f"\n📱 This URL has been:")
        logger.info(f"   ✅ Updated in alerts/notifiers.py")
        logger.info(f"   ✅ Sent to both Telegram bots")
        logger.info(f"\n⚠️  Keep this script running to maintain the tunnel")
        logger.info(f"   Press Ctrl+C to stop")
        
        return True
    
    def cleanup(self):
        """Clean up ngrok process"""
        if self.ngrok_process:
            try:
                self.ngrok_process.terminate()
                self.ngrok_process.wait(timeout=5)
                logger.info("✅ ngrok tunnel stopped")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping ngrok: {e}")

def main():
    setup = ExternalDashboardSetup(port=53056)
    
    try:
        success = setup.setup()
        if success:
            # Keep running to maintain tunnel
            try:
                while True:
                    time.sleep(60)
                    # Verify tunnel is still active
                    current_url = setup.get_ngrok_url()
                    if current_url != setup.public_url:
                        logger.warning("⚠️ Tunnel URL changed, updating...")
                        setup.public_url = current_url
                        if current_url:
                            setup.update_dashboard_url_in_code(current_url)
            except KeyboardInterrupt:
                logger.info("\n🛑 Stopping ngrok tunnel...")
                setup.cleanup()
        else:
            logger.error("❌ Failed to set up external dashboard")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\n🛑 Setup interrupted")
        setup.cleanup()
        sys.exit(0)

if __name__ == "__main__":
    main()

