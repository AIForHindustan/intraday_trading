"""
Ngrok Tunnel Manager with Auto-Reconnection
===========================================

Manages ngrok tunnel for dashboard external access with:
- Auto-restart on failure
- Health monitoring
- Reconnection logic
- Background daemon mode

Usage:
    python scripts/ngrok_manager.py start    # Start ngrok daemon
    python scripts/ngrok_manager.py stop     # Stop ngrok daemon
    python scripts/ngrok_manager.py status   # Check status
    python scripts/ngrok_manager.py restart  # Restart ngrok
"""

import os
import sys
import time
import signal
import subprocess
import logging
import json
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
DASHBOARD_PORT = 53056
NGROK_EXECUTABLE = os.getenv("NGROK_EXECUTABLE", "ngrok")
NGROK_API_URL = "http://127.0.0.1:4040"
PID_FILE = project_root / "logs" / "ngrok.pid"
LOG_FILE = project_root / "logs" / "ngrok_manager.log"
MAX_RESTART_ATTEMPTS = 10
RESTART_DELAY = 5  # seconds
HEALTH_CHECK_INTERVAL = 30  # seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class NgrokManager:
    """Ngrok tunnel manager with auto-reconnection."""
    
    def __init__(self, port: int = DASHBOARD_PORT):
        self.port = port
        self.process: Optional[subprocess.Popen] = None
        self.running = False
        self.restart_count = 0
        self.last_restart = None
        
        # Ensure logs directory exists
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def is_ngrok_running(self) -> bool:
        """Check if ngrok process is running."""
        try:
            # Check if our managed process is running
            if self.process:
                return self.process.poll() is None
            
            # Fallback: Check if any ngrok process is running
            try:
                result = subprocess.run(['pgrep', '-f', 'ngrok'], 
                                      capture_output=True, text=True)
                if result.returncode == 0 and len(result.stdout.strip()) > 0:
                    return True
                
                # Also check via ngrok API
                tunnel_url = self.get_ngrok_tunnel_url()
                if tunnel_url:
                    return True
            except Exception:
                pass
            return False
        except Exception:
            return False
    
    def get_ngrok_tunnel_url(self) -> Optional[str]:
        """Get current ngrok tunnel URL from API."""
        try:
            import requests
            response = requests.get(f"{NGROK_API_URL}/api/tunnels", timeout=2)
            if response.status_code == 200:
                data = response.json()
                tunnels = data.get('tunnels', [])
                for tunnel in tunnels:
                    if tunnel.get('proto') == 'https':
                        public_url = tunnel.get('public_url')
                        if public_url:
                            return public_url
        except ImportError:
            # Fallback: try urllib if requests not available
            try:
                import urllib.request
                import json
                with urllib.request.urlopen(f"{NGROK_API_URL}/api/tunnels", timeout=2) as response:
                    data = json.loads(response.read().decode())
                    tunnels = data.get('tunnels', [])
                    for tunnel in tunnels:
                        if tunnel.get('proto') == 'https':
                            public_url = tunnel.get('public_url')
                            if public_url:
                                return public_url
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"Could not fetch ngrok URL: {e}")
        return None
    
    def start_ngrok(self) -> bool:
        """Start ngrok tunnel."""
        try:
            # Kill any existing ngrok processes
            subprocess.run(['pkill', '-9', 'ngrok'], capture_output=True)
            time.sleep(1)
            
            # Start ngrok in background
            cmd = [NGROK_EXECUTABLE, 'http', str(self.port), '--log=stdout']
            
            # Check if ngrok config file exists
            ngrok_config = Path.home() / ".ngrok2" / "ngrok.yml"
            if not ngrok_config.exists():
                ngrok_config = Path.home() / ".config" / "ngrok" / "ngrok.yml"
            
            if ngrok_config.exists():
                cmd.extend(['--config', str(ngrok_config)])
            
            logger.info(f"🚀 Starting ngrok tunnel: {' '.join(cmd)}")
            
            # Start process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Wait a bit for ngrok to start
            time.sleep(3)
            
            # Check if process is still running
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                logger.error(f"❌ Ngrok failed to start: {stderr}")
                return False
            
            # Get tunnel URL
            time.sleep(2)  # Give ngrok API time to register
            tunnel_url = self.get_ngrok_tunnel_url()
            
            if tunnel_url:
                logger.info(f"✅ Ngrok tunnel started: {tunnel_url}")
                # Save PID
                with open(PID_FILE, 'w') as f:
                    f.write(str(self.process.pid))
                return True
            else:
                logger.warning("⚠️ Ngrok started but tunnel URL not available yet")
                return True  # Assume it will be available soon
            
        except FileNotFoundError:
            logger.error(f"❌ Ngrok executable not found: {NGROK_EXECUTABLE}")
            logger.error("   Install ngrok: https://ngrok.com/download")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to start ngrok: {e}")
            return False
    
    def stop_ngrok(self) -> bool:
        """Stop ngrok tunnel."""
        try:
            self.running = False
            
            # Kill process if running
            if self.process:
                try:
                    self.process.terminate()
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                except Exception:
                    pass
                self.process = None
            
            # Kill any other ngrok processes
            subprocess.run(['pkill', '-9', 'ngrok'], capture_output=True)
            time.sleep(1)
            
            # Remove PID file
            if PID_FILE.exists():
                PID_FILE.unlink()
            
            logger.info("✅ Ngrok stopped")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to stop ngrok: {e}")
            return False
    
    def restart_ngrok(self) -> bool:
        """Restart ngrok tunnel."""
        logger.info("🔄 Restarting ngrok tunnel...")
        self.stop_ngrok()
        time.sleep(2)
        return self.start_ngrok()
    
    def check_dashboard_running(self) -> bool:
        """Check if dashboard is actually listening on the port."""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except Exception as e:
            logger.debug(f"Error checking dashboard port: {e}")
            return False
    
    def test_tunnel_connection(self) -> bool:
        """Test if ngrok tunnel can actually reach the dashboard."""
        try:
            tunnel_url = self.get_ngrok_tunnel_url()
            if not tunnel_url:
                return False
            
            import urllib.request
            import urllib.error
            # Test connection through ngrok
            test_url = f"{tunnel_url}/"
            req = urllib.request.Request(test_url, headers={'User-Agent': 'ngrok-health-check'})
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    return response.getcode() in [200, 302, 404]  # Any valid HTTP response
            except urllib.error.HTTPError as e:
                # HTTP errors mean the connection worked, server just returned error
                return e.code in [200, 302, 404, 500, 503]
            except urllib.error.URLError:
                # Connection errors mean tunnel is broken
                return False
        except Exception as e:
            logger.debug(f"Error testing tunnel connection: {e}")
            return False
    
    def health_check(self) -> bool:
        """Check if ngrok tunnel is healthy and dashboard is accessible."""
        # Step 1: Check if ngrok process is running
        if not self.is_ngrok_running():
            logger.debug("❌ Health check failed: ngrok process not running")
            return False
        
        # Step 2: Check if dashboard is listening on local port
        if not self.check_dashboard_running():
            logger.debug(f"❌ Health check failed: dashboard not listening on port {self.port}")
            return False
        
        # Step 3: Check if tunnel URL is available
        tunnel_url = self.get_ngrok_tunnel_url()
        if not tunnel_url:
            logger.debug("❌ Health check failed: tunnel URL not available")
            return False
        
        # Step 4: Test if tunnel can actually connect to dashboard
        if not self.test_tunnel_connection():
            logger.debug("❌ Health check failed: tunnel connection test failed")
            return False
        
        return True
    
    def run_daemon(self):
        """Run ngrok manager as daemon with auto-reconnection."""
        logger.info(f"🚀 Starting ngrok manager daemon (port {self.port})")
        
        # Check if ngrok is already running (from previous daemon)
        if self.is_ngrok_running():
            tunnel_url = self.get_ngrok_tunnel_url()
            if tunnel_url:
                logger.info(f"✅ Ngrok already running: {tunnel_url}, monitoring it...")
            else:
                logger.info("⚠️ Ngrok process found but URL not available, restarting...")
                self.restart_ngrok()
        else:
            # Start ngrok fresh
            if not self.start_ngrok():
                logger.error("❌ Failed to start ngrok initially")
                return
        
        self.running = True
        self.restart_count = 0
        
        try:
            consecutive_failures = 0
            while self.running:
                time.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.health_check():
                    consecutive_failures += 1
                    logger.warning(f"⚠️ Ngrok tunnel unhealthy (failure #{consecutive_failures}), attempting restart...")
                    
                    if self.restart_count >= MAX_RESTART_ATTEMPTS:
                        logger.error(f"❌ Max restart attempts ({MAX_RESTART_ATTEMPTS}) reached, stopping")
                        break
                    
                    # Check if dashboard is running - if not, that's the issue
                    if not self.check_dashboard_running():
                        logger.error(f"⚠️ Dashboard not running on port {self.port}! Please start the dashboard first.")
                        logger.error(f"   Start dashboard: python alert_validation/alert_dashboard.py")
                        # Don't restart ngrok if dashboard is down - wait for dashboard to come up
                        time.sleep(RESTART_DELAY * 2)
                        continue
                    
                    # Restart ngrok
                    if self.restart_ngrok():
                        self.restart_count = 0
                        consecutive_failures = 0
                        self.last_restart = datetime.now()
                        logger.info("✅ Ngrok tunnel restored")
                    else:
                        self.restart_count += 1
                        logger.error(f"❌ Restart failed (attempt {self.restart_count}/{MAX_RESTART_ATTEMPTS})")
                        time.sleep(RESTART_DELAY)
                else:
                    # Reset counters on success
                    if consecutive_failures > 0:
                        consecutive_failures = 0
                        logger.info("✅ Ngrok tunnel restored and healthy")
                    if self.restart_count > 0:
                        self.restart_count = 0
                    
                    # Log tunnel URL periodically
                    check_time = int(time.time())
                    if check_time % 300 == 0:  # Every 5 minutes
                        tunnel_url = self.get_ngrok_tunnel_url()
                        if tunnel_url:
                            logger.info(f"📡 Ngrok tunnel active: {tunnel_url}")
                            logger.info(f"✅ Dashboard health check passed")
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"❌ Fatal error in daemon: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.stop_ngrok()
            logger.info("✅ Ngrok manager daemon stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ngrok tunnel manager")
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'status', 'daemon'],
                       help='Action to perform')
    parser.add_argument('--port', type=int, default=DASHBOARD_PORT,
                       help=f'Dashboard port (default: {DASHBOARD_PORT})')
    
    args = parser.parse_args()
    
    manager = NgrokManager(port=args.port)
    
    if args.action == 'start':
        if manager.start_ngrok():
            tunnel_url = manager.get_ngrok_tunnel_url()
            if tunnel_url:
                print(f"✅ Ngrok started: {tunnel_url}")
            else:
                print("✅ Ngrok started (URL pending)")
            sys.exit(0)
        else:
            print("❌ Failed to start ngrok")
            sys.exit(1)
    
    elif args.action == 'stop':
        if manager.stop_ngrok():
            print("✅ Ngrok stopped")
            sys.exit(0)
        else:
            print("❌ Failed to stop ngrok")
            sys.exit(1)
    
    elif args.action == 'restart':
        if manager.restart_ngrok():
            tunnel_url = manager.get_ngrok_tunnel_url()
            if tunnel_url:
                print(f"✅ Ngrok restarted: {tunnel_url}")
            else:
                print("✅ Ngrok restarted (URL pending)")
            sys.exit(0)
        else:
            print("❌ Failed to restart ngrok")
            sys.exit(1)
    
    elif args.action == 'status':
        # Create a new manager instance for status check (may not have process ref)
        manager = NgrokManager(port=args.port)
        if manager.is_ngrok_running():
            tunnel_url = manager.get_ngrok_tunnel_url()
            if tunnel_url:
                print(f"✅ Ngrok running: {tunnel_url}")
            else:
                print("⚠️ Ngrok running but URL not available yet")
        else:
            print("❌ Ngrok not running")
        sys.exit(0)
    
    elif args.action == 'daemon':
        manager.run_daemon()
        sys.exit(0)


if __name__ == "__main__":
    main()

