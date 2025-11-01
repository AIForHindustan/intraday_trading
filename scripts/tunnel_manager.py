"""
Unified Tunnel Manager with Automatic Failover
==============================================

Primary: Cloudflare Tunnel (free, unlimited bandwidth, HTTPS)
Fallback: Mullvad SSH Tunnel (via VPS when Cloudflare is down)

Features:
- Automatic failover from Cloudflare to Mullvad
- Health monitoring of both tunnels
- Auto-switch back to Cloudflare when it recovers
- Comprehensive logging and status reporting

Usage:
    python scripts/tunnel_manager.py start    # Start with auto-failover
    python scripts/tunnel_manager.py stop     # Stop all tunnels
    python scripts/tunnel_manager.py status   # Check current status
    python scripts/tunnel_manager.py daemon   # Run daemon with auto-failover
"""

import os
import sys
import time
import signal
import subprocess
import logging
import json
import re
from pathlib import Path
from typing import Optional, Literal
from datetime import datetime
from enum import Enum

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
DASHBOARD_PORT = 53056
CLOUDFLARED_EXECUTABLE = os.getenv("CLOUDFLARED_EXECUTABLE", "cloudflared")
VPS_HOST = os.getenv("VPS_HOST", "")  # For Mullvad fallback
VPS_USER = os.getenv("VPS_USER", "")  # For Mullvad fallback
SSH_KEY = os.getenv("SSH_KEY_PATH", "")  # Optional SSH key
REMOTE_PORT = int(os.getenv("REMOTE_PORT", str(DASHBOARD_PORT)))

PID_FILE = project_root / "logs" / "tunnel_manager.pid"
LOG_FILE = project_root / "logs" / "tunnel_manager.log"
CLOUDFLARED_LOG = project_root / "logs" / "cloudflared_output.log"
MAX_RESTART_ATTEMPTS = 10
RESTART_DELAY = 5  # seconds
HEALTH_CHECK_INTERVAL = 30  # seconds
FAILOVER_DELAY = 10  # seconds before switching to fallback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class TunnelType(Enum):
    """Tunnel type enumeration."""
    CLOUDFLARE = "cloudflare"
    MULLVAD = "mullvad"
    NONE = "none"


class UnifiedTunnelManager:
    """Unified tunnel manager with Cloudflare primary and Mullvad fallback."""
    
    def __init__(self, port: int = DASHBOARD_PORT):
        self.port = port
        self.cloudflare_process: Optional[subprocess.Popen] = None
        self.mullvad_process: Optional[subprocess.Popen] = None
        self.running = False
        self.current_tunnel: TunnelType = TunnelType.NONE
        self.cloudflare_url: Optional[str] = None
        self.mullvad_available = bool(VPS_HOST and VPS_USER)
        self.restart_count = 0
        self.last_failover = None
        
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
    
    def check_autossh_installed(self) -> bool:
        """Check if autossh is installed for Mullvad fallback."""
        try:
            result = subprocess.run(['autossh', '-V'], capture_output=True, timeout=5)
            return result.returncode == 0 or 'autossh' in str(result.stderr)
        except Exception:
            return False
    
    def check_dashboard_running(self) -> bool:
        """Check if dashboard is listening on port."""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', self.port))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def start_cloudflare_tunnel(self) -> bool:
        """Start Cloudflare Tunnel (primary)."""
        try:
            if not self.check_cloudflared_installed():
                logger.warning("⚠️ cloudflared not installed, skipping Cloudflare Tunnel")
                return False
            
            # Kill any existing cloudflared processes
            subprocess.run(['pkill', '-9', 'cloudflared'], capture_output=True)
            time.sleep(1)
            
            cmd = [CLOUDFLARED_EXECUTABLE, 'tunnel', '--url', f'http://localhost:{self.port}']
            
            logger.info(f"🚀 Starting Cloudflare Tunnel (primary)...")
            
            self.cloudflare_process = subprocess.Popen(
                cmd,
                stdout=open(CLOUDFLARED_LOG, 'w'),
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            time.sleep(5)
            
            if self.cloudflare_process.poll() is not None:
                stderr = self.cloudflare_process.stderr.read() if self.cloudflare_process.stderr else ""
                logger.error(f"❌ Cloudflare Tunnel failed to start: {stderr}")
                return False
            
            # Extract URL from log file
            time.sleep(3)
            self.cloudflare_url = self._extract_cloudflare_url()
            
            if self.cloudflare_url:
                logger.info(f"✅ Cloudflare Tunnel started: {self.cloudflare_url}")
                return True
            else:
                logger.warning("⚠️ Cloudflare Tunnel started but URL not available yet")
                return True  # Assume it will be available soon
            
        except Exception as e:
            logger.error(f"❌ Failed to start Cloudflare Tunnel: {e}")
            return False
    
    def _extract_cloudflare_url(self) -> Optional[str]:
        """Extract Cloudflare tunnel URL from log output."""
        try:
            if CLOUDFLARED_LOG.exists():
                with open(CLOUDFLARED_LOG, 'r') as f:
                    content = f.read()
                    # Look for URL pattern
                    url_pattern = r'https://[\w\-]+\.(trycloudflare\.com|[\w\-]+\.cfargotunnel\.com)'
                    matches = re.findall(url_pattern, content)
                    if matches:
                        return matches[0]
            
            # Also try reading from stderr
            if self.cloudflare_process and self.cloudflare_process.stderr:
                try:
                    output = self.cloudflare_process.stderr.read()
                    if output:
                        url_pattern = r'https://[\w\-]+\.(trycloudflare\.com|[\w\-]+\.cfargotunnel\.com)'
                        matches = re.findall(url_pattern, output.decode('utf-8', errors='ignore'))
                        if matches:
                            return matches[0]
                except Exception:
                    pass
        except Exception:
            pass
        return None
    
    def start_mullvad_tunnel(self) -> bool:
        """Start Mullvad SSH tunnel (fallback)."""
        if not self.mullvad_available:
            logger.warning("⚠️ Mullvad fallback not configured (VPS_HOST/VPS_USER not set)")
            return False
        
        try:
            use_autossh = self.check_autossh_installed()
            if not use_autossh:
                logger.warning("⚠️ autossh not installed, using ssh (no auto-reconnect)")
            
            # Build SSH command
            if use_autossh:
                cmd = [
                    'autossh', '-M', '0', '-f', '-N',
                    '-o', 'ServerAliveInterval=60',
                    '-o', 'ServerAliveCountMax=3',
                    '-o', 'ExitOnForwardFailure=yes',
                    '-R', f'{REMOTE_PORT}:localhost:{self.port}',
                ]
            else:
                cmd = [
                    'ssh', '-N',
                    '-o', 'ServerAliveInterval=60',
                    '-o', 'ServerAliveCountMax=3',
                    '-o', 'ExitOnForwardFailure=yes',
                    '-R', f'{REMOTE_PORT}:localhost:{self.port}',
                ]
            
            if SSH_KEY and Path(SSH_KEY).exists():
                cmd.extend(['-i', SSH_KEY])
            
            cmd.append(f'{VPS_USER}@{VPS_HOST}')
            
            logger.info(f"🔄 Starting Mullvad SSH Tunnel (fallback)...")
            
            self.mullvad_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            time.sleep(2)
            
            if self.mullvad_process.poll() is not None:
                stderr = self.mullvad_process.stderr.read() if self.mullvad_process.stderr else ""
                logger.error(f"❌ Mullvad SSH tunnel failed: {stderr}")
                return False
            
            logger.info(f"✅ Mullvad SSH Tunnel started: http://{VPS_HOST}:{REMOTE_PORT}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start Mullvad tunnel: {e}")
            return False
    
    def stop_cloudflare_tunnel(self):
        """Stop Cloudflare tunnel."""
        if self.cloudflare_process:
            try:
                self.cloudflare_process.terminate()
                self.cloudflare_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.cloudflare_process.kill()
            except Exception:
                pass
            self.cloudflare_process = None
        
        subprocess.run(['pkill', '-9', 'cloudflared'], capture_output=True)
        self.cloudflare_url = None
    
    def stop_mullvad_tunnel(self):
        """Stop Mullvad tunnel."""
        if self.mullvad_process:
            try:
                self.mullvad_process.terminate()
                self.mullvad_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.mullvad_process.kill()
            except Exception:
                pass
            self.mullvad_process = None
        
        subprocess.run(['pkill', '-f', f'autossh.*{VPS_HOST}'], capture_output=True)
        subprocess.run(['pkill', '-f', f'ssh.*-R.*{REMOTE_PORT}'], capture_output=True)
    
    def test_tunnel_connection(self, url: Optional[str] = None) -> bool:
        """Test if tunnel can reach dashboard."""
        try:
            if not url:
                if self.current_tunnel == TunnelType.CLOUDFLARE:
                    url = self.cloudflare_url
                elif self.current_tunnel == TunnelType.MULLVAD:
                    url = f"http://{VPS_HOST}:{REMOTE_PORT}"
                else:
                    return False
            
            if not url:
                return False
            
            import urllib.request
            import urllib.error
            req = urllib.request.Request(url, headers={'User-Agent': 'tunnel-health-check'})
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.getcode() in [200, 302, 404]
            except urllib.error.HTTPError as e:
                return e.code in [200, 302, 404, 500, 503]
            except urllib.error.URLError:
                return False
        except Exception:
            return False
    
    def health_check_cloudflare(self) -> bool:
        """Check Cloudflare tunnel health."""
        if not self.cloudflare_process or self.cloudflare_process.poll() is not None:
            return False
        
        if not self.cloudflare_url:
            self.cloudflare_url = self._extract_cloudflare_url()
        
        if not self.cloudflare_url:
            return False
        
        return self.test_tunnel_connection(self.cloudflare_url)
    
    def health_check_mullvad(self) -> bool:
        """Check Mullvad tunnel health."""
        if not self.mullvad_available:
            return False
        
        if not self.mullvad_process or self.mullvad_process.poll() is not None:
            return False
        
        return self.test_tunnel_connection(f"http://{VPS_HOST}:{REMOTE_PORT}")
    
    def get_active_tunnel_url(self) -> Optional[str]:
        """Get URL of currently active tunnel."""
        if self.current_tunnel == TunnelType.CLOUDFLARE:
            return self.cloudflare_url
        elif self.current_tunnel == TunnelType.MULLVAD:
            return f"http://{VPS_HOST}:{REMOTE_PORT}"
        return None
    
    def start_tunnels(self) -> bool:
        """Start primary tunnel (Cloudflare)."""
        # Always start with Cloudflare as primary
        if self.start_cloudflare_tunnel():
            self.current_tunnel = TunnelType.CLOUDFLARE
            # Pre-start Mullvad in background as standby (if available)
            if self.mullvad_available:
                logger.info("🔄 Pre-starting Mullvad tunnel as standby...")
                self.start_mullvad_tunnel()
            return True
        else:
            # Cloudflare failed, try Mullvad immediately
            logger.warning("⚠️ Cloudflare Tunnel failed, switching to Mullvad fallback...")
            if self.start_mullvad_tunnel():
                self.current_tunnel = TunnelType.MULLVAD
                return True
            return False
    
    def stop_all_tunnels(self):
        """Stop all tunnels."""
        self.stop_cloudflare_tunnel()
        self.stop_mullvad_tunnel()
        self.current_tunnel = TunnelType.NONE
    
    def handle_failover(self):
        """Handle failover from Cloudflare to Mullvad or vice versa."""
        cloudflare_healthy = self.health_check_cloudflare()
        mullvad_healthy = self.health_check_mullvad()
        
        # If Cloudflare is primary and fails, switch to Mullvad
        if self.current_tunnel == TunnelType.CLOUDFLARE and not cloudflare_healthy:
            logger.warning("⚠️ Cloudflare Tunnel unhealthy, failing over to Mullvad...")
            
            # Ensure Mullvad is running
            if not mullvad_healthy:
                logger.info("🔄 Restarting Mullvad tunnel...")
                self.stop_mullvad_tunnel()
                time.sleep(2)
                if self.start_mullvad_tunnel():
                    mullvad_healthy = True
            
            if mullvad_healthy:
                self.current_tunnel = TunnelType.MULLVAD
                self.last_failover = datetime.now()
                logger.info(f"✅ Switched to Mullvad fallback: http://{VPS_HOST}:{REMOTE_PORT}")
                return True
        
        # If Mullvad is active but Cloudflare recovers, switch back
        elif self.current_tunnel == TunnelType.MULLVAD and cloudflare_healthy:
            # Wait a bit to ensure Cloudflare is stable
            time.sleep(FAILOVER_DELAY)
            if self.health_check_cloudflare():
                logger.info("✅ Cloudflare Tunnel recovered, switching back to primary...")
                self.current_tunnel = TunnelType.CLOUDFLARE
                logger.info(f"✅ Active tunnel: {self.cloudflare_url}")
                return True
        
        return False
    
    def run_daemon(self):
        """Run tunnel manager as daemon with auto-failover."""
        logger.info(f"🚀 Starting Unified Tunnel Manager (port {self.port})")
        logger.info(f"   Primary: Cloudflare Tunnel")
        if self.mullvad_available:
            logger.info(f"   Fallback: Mullvad SSH Tunnel ({VPS_USER}@{VPS_HOST})")
        else:
            logger.warning(f"   Fallback: Mullvad not configured")
        
        if not self.check_dashboard_running():
            logger.error(f"⚠️ Dashboard not running on port {self.port}!")
            logger.error(f"   Start dashboard: python alert_validation/alert_dashboard.py")
            return
        
        if not self.start_tunnels():
            logger.error("❌ Failed to start any tunnel")
            return
        
        self.running = True
        self.restart_count = 0
        
        try:
            consecutive_failures = 0
            while self.running:
                time.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.check_dashboard_running():
                    logger.error(f"⚠️ Dashboard stopped! Waiting for restart...")
                    time.sleep(RESTART_DELAY * 2)
                    continue
                
                # Handle failover
                self.handle_failover()
                
                # Check current tunnel health
                is_healthy = False
                if self.current_tunnel == TunnelType.CLOUDFLARE:
                    is_healthy = self.health_check_cloudflare()
                elif self.current_tunnel == TunnelType.MULLVAD:
                    is_healthy = self.health_check_mullvad()
                
                if not is_healthy:
                    consecutive_failures += 1
                    logger.warning(f"⚠️ Active tunnel unhealthy (failure #{consecutive_failures})")
                    
                    if consecutive_failures >= 3:
                        logger.error("❌ Tunnel failures exceeded threshold")
                        # Try to restart current tunnel
                        if self.current_tunnel == TunnelType.CLOUDFLARE:
                            self.stop_cloudflare_tunnel()
                            if not self.start_cloudflare_tunnel():
                                # Failover to Mullvad
                                self.handle_failover()
                        elif self.current_tunnel == TunnelType.MULLVAD:
                            self.stop_mullvad_tunnel()
                            self.start_mullvad_tunnel()
                        
                        consecutive_failures = 0
                else:
                    if consecutive_failures > 0:
                        consecutive_failures = 0
                        logger.info("✅ Tunnel health restored")
                    
                    # Log status periodically
                    check_time = int(time.time())
                    if check_time % 300 == 0:  # Every 5 minutes
                        active_url = self.get_active_tunnel_url()
                        tunnel_name = self.current_tunnel.value.upper()
                        logger.info(f"📡 Active tunnel ({tunnel_name}): {active_url}")
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"❌ Fatal error in daemon: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.stop_all_tunnels()
            logger.info("✅ Tunnel manager stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Unified Tunnel Manager (Cloudflare + Mullvad)")
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'status', 'daemon'],
                       help='Action to perform')
    parser.add_argument('--port', type=int, default=DASHBOARD_PORT,
                       help=f'Dashboard port (default: {DASHBOARD_PORT})')
    
    args = parser.parse_args()
    
    manager = UnifiedTunnelManager(port=args.port)
    
    if args.action == 'start':
        if manager.start_tunnels():
            url = manager.get_active_tunnel_url()
            tunnel_type = manager.current_tunnel.value
            print(f"✅ Tunnel started ({tunnel_type}): {url}")
            sys.exit(0)
        else:
            print("❌ Failed to start tunnel")
            sys.exit(1)
    
    elif args.action == 'stop':
        manager.stop_all_tunnels()
        print("✅ All tunnels stopped")
        sys.exit(0)
    
    elif args.action == 'restart':
        manager.stop_all_tunnels()
        time.sleep(2)
        if manager.start_tunnels():
            url = manager.get_active_tunnel_url()
            print(f"✅ Tunnels restarted: {url}")
            sys.exit(0)
        else:
            print("❌ Failed to restart tunnels")
            sys.exit(1)
    
    elif args.action == 'status':
        # Check what's running
        cloudflare_running = manager.cloudflare_process and manager.cloudflare_process.poll() is None
        mullvad_running = manager.mullvad_process and manager.mullvad_process.poll() is None
        
        if cloudflare_running or mullvad_running:
            if cloudflare_running:
                url = manager._extract_cloudflare_url()
                print(f"✅ Cloudflare Tunnel: {url or 'starting...'}")
            if mullvad_running and manager.mullvad_available:
                print(f"✅ Mullvad Tunnel: http://{VPS_HOST}:{REMOTE_PORT}")
            print(f"   Active: {manager.current_tunnel.value if manager.current_tunnel != TunnelType.NONE else 'none'}")
        else:
            print("❌ No tunnels running")
        sys.exit(0)
    
    elif args.action == 'daemon':
        manager.run_daemon()
        sys.exit(0)


if __name__ == "__main__":
    main()

