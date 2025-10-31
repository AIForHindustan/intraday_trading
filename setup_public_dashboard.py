#!/usr/bin/env python3
"""
Setup Public Dashboard Access
Creates a public URL for the trading dashboard using ngrok
"""

import subprocess
import time
import requests
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PublicDashboardSetup:
    def __init__(self):
        self.local_port = 8000
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
    
    def install_ngrok(self):
        """Install ngrok using brew (macOS)"""
        try:
            logger.info("Installing ngrok...")
            subprocess.run(['brew', 'install', 'ngrok/ngrok/ngrok'], check=True)
            logger.info("✅ ngrok installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to install ngrok: {e}")
            return False
        except FileNotFoundError:
            logger.error("❌ brew not found. Please install ngrok manually from https://ngrok.com/")
            return False
    
    def start_ngrok_tunnel(self):
        """Start ngrok tunnel for the dashboard"""
        try:
            logger.info(f"Starting ngrok tunnel on port {self.local_port}...")
            
            # Start ngrok process
            self.ngrok_process = subprocess.Popen(
                ['ngrok', 'http', str(self.local_port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait a moment for ngrok to start
            time.sleep(3)
            
            # Get the public URL
            self.public_url = self.get_ngrok_url()
            
            if self.public_url:
                logger.info(f"✅ Public URL created: {self.public_url}")
                return True
            else:
                logger.error("❌ Failed to get ngrok URL")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error starting ngrok: {e}")
            return False
    
    def get_ngrok_url(self):
        """Get the public URL from ngrok API"""
        try:
            response = requests.get('http://localhost:4040/api/tunnels')
            if response.status_code == 200:
                data = response.json()
                tunnels = data.get('tunnels', [])
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'https':
                        return tunnel.get('public_url')
                # Fallback to http if https not available
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'http':
                        return tunnel.get('public_url')
        except Exception as e:
            logger.error(f"Error getting ngrok URL: {e}")
        return None
    
    def update_dashboard_url(self, public_url):
        """Update the dashboard notification script with the public URL"""
        try:
            script_path = Path("send_dashboard_notification.py")
            if script_path.exists():
                content = script_path.read_text()
                # Replace the localhost URL with public URL
                updated_content = content.replace(
                    'dashboard_url = "http://localhost:8000"',
                    f'dashboard_url = "{public_url}"'
                )
                script_path.write_text(updated_content)
                logger.info(f"✅ Updated dashboard URL to: {public_url}")
                return True
        except Exception as e:
            logger.error(f"Error updating dashboard URL: {e}")
        return False
    
    def send_public_notification(self, public_url):
        """Send notification with public URL to Telegram"""
        try:
            from community_bots.telegram_bot import AIONTelegramBot
            import asyncio
            
            bot = AIONTelegramBot()
            
            message = f"""🌐 *DASHBOARD NOW PUBLICLY ACCESSIBLE!* 🌐

🔗 *PUBLIC URL:* {public_url}

✅ *ACCESS FROM ANYWHERE:*
• No VPN required
• Mobile-friendly
• Real-time data
• All features available

📱 *MOBILE USERS:*
• Open the URL in your mobile browser
• Use landscape mode for better chart viewing
• All indicators work on mobile

🚀 *QUICK START:*
1. Open the URL above
2. Select your preferred asset class
3. Choose an instrument
4. Watch real-time alerts and indicators

_Professional Trading Dashboard - Now Live!_"""
            
            async def send_notification():
                success = await bot.send_message(message)
                if success:
                    logger.info("✅ Public URL notification sent to Telegram")
                else:
                    logger.error("❌ Failed to send public URL notification")
                return success
            
            return asyncio.run(send_notification())
            
        except Exception as e:
            logger.error(f"Error sending public notification: {e}")
            return False
    
    def setup_public_dashboard(self):
        """Main function to set up public dashboard access"""
        logger.info("🚀 Setting up public dashboard access...")
        
        # Check if ngrok is installed
        if not self.check_ngrok_installed():
            logger.info("Installing ngrok...")
            if not self.install_ngrok():
                return False
        
        # Start ngrok tunnel
        if not self.start_ngrok_tunnel():
            return False
        
        # Update dashboard URL in notification script
        self.update_dashboard_url(self.public_url)
        
        # Send public notification
        self.send_public_notification(self.public_url)
        
        logger.info(f"🎉 Dashboard is now publicly accessible at: {self.public_url}")
        logger.info("📱 Share this URL with your community!")
        
        return True
    
    def cleanup(self):
        """Clean up ngrok process"""
        if self.ngrok_process:
            self.ngrok_process.terminate()
            logger.info("✅ ngrok tunnel stopped")

def main():
    setup = PublicDashboardSetup()
    
    try:
        success = setup.setup_public_dashboard()
        if success:
            print(f"\n🎉 SUCCESS! Dashboard is live at: {setup.public_url}")
            print("📱 Share this URL with your Telegram community!")
            print("\nPress Ctrl+C to stop the tunnel...")
            
            # Keep the script running to maintain the tunnel
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n🛑 Stopping ngrok tunnel...")
                setup.cleanup()
        else:
            print("❌ Failed to set up public dashboard")
    
    except KeyboardInterrupt:
        print("\n🛑 Setup interrupted")
        setup.cleanup()

if __name__ == "__main__":
    main()
