"""
Cloudflare Tunnel Manager with Auto-Reconnection
================================================

Manages Cloudflare Tunnel for dashboard external access with:
- Auto-restart on failure
- Health monitoring
- Reconnection logic
- Background daemon mode
- FREE unlimited bandwidth with HTTPS

Cloudflare Tunnel is superior to ngrok because:
- ✅ Free unlimited bandwidth
- ✅ Custom domain support
- ✅ HTTPS included
- ✅ No URL changes on restart
- ✅ Better reliability

Usage:
    python scripts/cloudflare_tunnel_manager.py start    # Start tunnel daemon
    python scripts/cloudflare_tunnel_manager.py stop     # Stop tunnel daemon
    python scripts/cloudflare_tunnel_manager.py status   # Check status
    python scripts/cloudflare_tunnel_manager.py restart  # Restart tunnel
    python scripts/cloudflare_tunnel_manager.py daemon   # Run as daemon with auto-reconnect
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
CLOUDFLARED_EXECUTABLE = os.getenv("CLOUDFLARED_EXECUTABLE", "cloudflared")
PID_FILE = project_root / "logs" / "cloudflare_tunnel.pid"
LOG_FILE = project_root / "logs" / "cloudflare_tunnel_manager.log"
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


class CloudflareTunnelManager:
    """Cloudflare Tunnel manager with auto-reconnection."""
    
    def __init__(self, port: int = DASHBOARD_PORT):
        self.port = port
        self.process: Optional[subprocess.Popen] = None
        self.running = False
        self.restart_count = 0
        self.last_restart = None
        self.tunnel_url = None
        
        # Ensure logs directory exists
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def check_cloudflared_installed(self) -> bool:
        """Check if cloudflared is installed."""
        try:
            result = subprocess.run(
                [CLOUDFLARED_EXECUTABLE, '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def is_tunnel_running(self) -> bool:
        """Check if cloudflare tunnel process is running."""
        try:
            # Check if our managed process is running
            if self.process:
                return self.process.poll() is None
            
            # Fallback: Check if any cloudflared process is running
            try:
                result = subprocess.run(['pgrep', '-f', 'cloudflared'], 
                                      capture_output=True, text=True)
                if result.returncode == 0 and len(result.stdout.strip()) > 0:
                    return True
            except Exception:
                pass
            return False
        except Exception:
            return False
    
    def get_tunnel_url(self) -> Optional[str]:
        """Get current cloudflare tunnel URL from process output."""
        # Cloudflare Tunnel prints URL to stderr in format:
        # "https://xxxx-xxxx-xxxx.trycloudflare.com"
        # or if using a custom domain, the configured domain
        
        if self.tunnel_url:
            return self.tunnel_url
        
        # Try to read from process output if available
        if self.process and self.process.stderr:
            try:
                # Read any available output
                # Note: cloudflared outputs URL to stderr
                output = self.process.stderr.read()
                if output:
                    import re
                    # Look for URL pattern
                    url_pattern = r'https://[\w\-]+\.(trycloudflare\.com|[\w\-]+\.cfargotunnel\.com)'
                    matches = re.findall(url_pattern, output.decode('utf-8', errors='ignore'))
                    if matches:
                        return matches[0]
            except Exception:
                pass
        
        return None
    
    def start_tunnel(self) -> bool:
        """Start cloudflare tunnel."""
        try:
            # Check if cloudflared is installed
            if not self.check_cloudflared_installed():
                logger.error(f"❌ cloudflared executable not found: {CLOUDFLARED_EXECUTABLE}")
                logger.error("   Install: brew install cloudflare/cloudflare/cloudflared")
                logger.error("   Or download: https://github.com/cloudflare/cloudflared/releases")
                return False
            
            # Kill any existing cloudflared processes
            subprocess.run(['pkill', '-9', 'cloudflared'], capture_output=True)
            time.sleep(1)
            
            # Start cloudflared tunnel
            cmd = [CLOUDFLARED_EXECUTABLE, 'tunnel', '--url', f'http://localhost:{self.port}']
            
            logger.info(f"🚀 Starting Cloudflare Tunnel: {' '.join(cmd)}")
            
            # Start process
            log_file = LOG_FILE.parent / "cloudflared_output.log"
            self.process = subprocess.Popen(
                cmd,
                stdout=open(log_file, 'w'),
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Wait a bit for tunnel to start
            time.sleep(5)
            
            # Check if process is still running
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                logger.error(f"❌ Cloudflare Tunnel failed to start: {stderr}")
                return False
            
            # Extract URL from stderr (cloudflared prints URL there)
            time.sleep(3)  # Give cloudflared time to print URL
            try:
                if self.process.stderr:
                    # Read URL from stderr
                    import select
                    import re
                    if hasattr(self.process.stderr, 'readable'):
                        output = ""
                        # Try to read available data
                        for _ in range(10):
                            time.sleep(0.5)
                            # Check if there's data (non-blocking)
                            import fcntl
                            try:
                                flags = fcntl.fcntl(self.process.stderr, fcntl.F_GETFL)
                                fcntl.fcntl(self.process.stderr, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                                chunk = self.process.stderr.read()
                                if chunk:
                                    output += chunk
                                    url_pattern = r'https://[\w\-]+\.(trycloudflare\.com|[\w\-]+\.cfargotunnel\.com)'
                                    matches = re.findall(url_pattern, output)
                                    if matches:
                                        self.tunnel_url = matches[0]
                                        break
                            except Exception:
                                pass
            except Exception as e:
                logger.debug(f"Could not extract URL from stderr: {e}")
            
            # Also check log file
            if not self.tunnel_url and log_file.exists():
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        import re
                        url_pattern = r'https://[\w\-]+\.(trycloudflare\.com|[\w\-]+\.cfargotunnel\.com)'
                        matches = re.findall(url_pattern, content)
                        if matches:
                            self.tunnel_url = matches[0]
                except Exception:
                    pass
            
            if self.tunnel_url:
                logger.info(f"✅ Cloudflare Tunnel started: {self.tunnel_url}")
                # Save PID
                with open(PID_FILE, 'w') as f:
                    f.write(str(self.process.pid))
                return True
            else:
                logger.warning("⚠️ Cloudflare Tunnel started but URL not available yet")
                logger.warning("   Check logs/cloudflared_output.log for the tunnel URL")
                return True  # Assume it will be available soon
            
        except FileNotFoundError:
            logger.error(f"❌ cloudflared executable not found: {CLOUDFLARED_EXECUTABLE}")
            logger.error("   Install: brew install cloudflare/cloudflare/cloudflared")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to start Cloudflare Tunnel: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def stop_tunnel(self) -> bool:
        """Stop cloudflare tunnel."""
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
            
            # Kill any other cloudflared processes
            subprocess.run(['pkill', '-9', 'cloudflared'], capture_output=True)
            time.sleep(1)
            
            # Remove PID file
            if PID_FILE.exists():
                PID_FILE.unlink()
            
            self.tunnel_url = None
            logger.info("✅ Cloudflare Tunnel stopped")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to stop Cloudflare Tunnel: {e}")
            return False
    
    def restart_tunnel(self) -> bool:
        """Restart cloudflare tunnel."""
        logger.info("🔄 Restarting Cloudflare Tunnel...")
        self.stop_tunnel()
        time.sleep(2)
        return self.start_tunnel()
    
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
        """Test if tunnel can actually reach the dashboard."""
        try:
            tunnel_url = self.get_tunnel_url()
            if not tunnel_url:
                return False
            
            import urllib.request
            import urllib.error
            # Test connection through tunnel
            test_url = f"{tunnel_url}/"
            req = urllib.request.Request(test_url, headers={'User-Agent': 'cloudflare-tunnel-health-check'})
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
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
        """Check if tunnel is healthy and dashboard is accessible."""
        # Step 1: Check if tunnel process is running
        if not self.is_tunnel_running():
            logger.debug("❌ Health check failed: tunnel process not running")
            return False
        
        # Step 2: Check if dashboard is listening on local port
        if not self.check_dashboard_running():
            logger.debug(f"❌ Health check failed: dashboard not listening on port {self.port}")
            return False
        
        # Step 3: Check if tunnel URL is available
        tunnel_url = self.get_tunnel_url()
        if not tunnel_url:
            logger.debug("❌ Health check failed: tunnel URL not available")
            return False
        
        # Step 4: Test if tunnel can actually connect to dashboard
        if not self.test_tunnel_connection():
            logger.debug("❌ Health check failed: tunnel connection test failed")
            return False
        
        return True
    
    def run_daemon(self):
        """Run tunnel manager as daemon with auto-reconnection."""
        logger.info(f"🚀 Starting Cloudflare Tunnel manager daemon (port {self.port})")
        
        # Check if tunnel is already running
        if self.is_tunnel_running():
            tunnel_url = self.get_tunnel_url()
            if tunnel_url:
                logger.info(f"✅ Tunnel already running: {tunnel_url}, monitoring it...")
            else:
                logger.info("⚠️ Tunnel process found but URL not available, restarting...")
                self.restart_tunnel()
        else:
            # Start tunnel fresh
            if not self.start_tunnel():
                logger.error("❌ Failed to start tunnel initially")
                return
        
        self.running = True
        self.restart_count = 0
        
        try:
            consecutive_failures = 0
            while self.running:
                time.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.health_check():
                    consecutive_failures += 1
                    logger.warning(f"⚠️ Tunnel unhealthy (failure #{consecutive_failures}), attempting restart...")
                    
                    if self.restart_count >= MAX_RESTART_ATTEMPTS:
                        logger.error(f"❌ Max restart attempts ({MAX_RESTART_ATTEMPTS}) reached, stopping")
                        break
                    
                    # Check if dashboard is running - if not, that's the issue
                    if not self.check_dashboard_running():
                        logger.error(f"⚠️ Dashboard not running on port {self.port}! Please start the dashboard first.")
                        logger.error(f"   Start dashboard: python alert_validation/alert_dashboard.py")
                        # Don't restart tunnel if dashboard is down - wait for dashboard to come up
                        time.sleep(RESTART_DELAY * 2)
                        continue
                    
                    # Restart tunnel
                    if self.restart_tunnel():
                        self.restart_count = 0
                        consecutive_failures = 0
                        self.last_restart = datetime.now()
                        logger.info("✅ Tunnel restored")
                    else:
                        self.restart_count += 1
                        logger.error(f"❌ Restart failed (attempt {self.restart_count}/{MAX_RESTART_ATTEMPTS})")
                        time.sleep(RESTART_DELAY)
                else:
                    # Reset counters on success
                    if consecutive_failures > 0:
                        consecutive_failures = 0
                        logger.info("✅ Tunnel restored and healthy")
                    if self.restart_count > 0:
                        self.restart_count = 0
                    
                    # Log tunnel URL periodically
                    check_time = int(time.time())
                    if check_time % 300 == 0:  # Every 5 minutes
                        tunnel_url = self.get_tunnel_url()
                        if tunnel_url:
                            logger.info(f"📡 Cloudflare Tunnel active: {tunnel_url}")
                            logger.info(f"✅ Dashboard health check passed")
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"❌ Fatal error in daemon: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.stop_tunnel()
            logger.info("✅ Cloudflare Tunnel manager daemon stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cloudflare Tunnel manager")
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'status', 'daemon'],
                       help='Action to perform')
    parser.add_argument('--port', type=int, default=DASHBOARD_PORT,
                       help=f'Dashboard port (default: {DASHBOARD_PORT})')
    
    args = parser.parse_args()
    
    manager = CloudflareTunnelManager(port=args.port)
    
    if args.action == 'start':
        if manager.start_tunnel():
            tunnel_url = manager.get_tunnel_url()
            if tunnel_url:
                print(f"✅ Cloudflare Tunnel started: {tunnel_url}")
            else:
                print("✅ Cloudflare Tunnel started (URL pending - check logs/cloudflared_output.log)")
            sys.exit(0)
        else:
            print("❌ Failed to start Cloudflare Tunnel")
            sys.exit(1)
    
    elif args.action == 'stop':
        if manager.stop_tunnel():
            print("✅ Cloudflare Tunnel stopped")
            sys.exit(0)
        else:
            print("❌ Failed to stop Cloudflare Tunnel")
            sys.exit(1)
    
    elif args.action == 'restart':
        if manager.restart_tunnel():
            tunnel_url = manager.get_tunnel_url()
            if tunnel_url:
                print(f"✅ Cloudflare Tunnel restarted: {tunnel_url}")
            else:
                print("✅ Cloudflare Tunnel restarted (URL pending)")
            sys.exit(0)
        else:
            print("❌ Failed to restart Cloudflare Tunnel")
            sys.exit(1)
    
    elif args.action == 'status':
        if manager.is_tunnel_running():
            tunnel_url = manager.get_tunnel_url()
            if tunnel_url:
                print(f"✅ Cloudflare Tunnel running: {tunnel_url}")
            else:
                print("⚠️ Tunnel running but URL not available yet (check logs)")
        else:
            print("❌ Cloudflare Tunnel not running")
        sys.exit(0)
    
    elif args.action == 'daemon':
        manager.run_daemon()
        sys.exit(0)


if __name__ == "__main__":
    main()

