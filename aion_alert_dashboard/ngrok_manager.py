"""
Ngrok Tunnel Manager with Auto-Reconnection
===========================================

Manages ngrok tunnel for dashboard external access with:
- Auto-restart on failure
- Health monitoring
- Reconnection logic
- Background daemon mode

Usage:
    python aion_alert_dashboard/ngrok_manager.py start    # Start ngrok daemon
    python aion_alert_dashboard/ngrok_manager.py stop     # Stop ngrok daemon
    python aion_alert_dashboard/ngrok_manager.py status   # Check status
    python aion_alert_dashboard/ngrok_manager.py restart  # Restart ngrok
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
NGROK_PROCESS_LOG = project_root / "logs" / "ngrok_process.log"
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
        self._shutting_down = False
        
        # Ensure logs directory exists
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully - but only if explicitly shutting down."""
        # Ignore SIGTERM if not explicitly shutting down (allows launchd to manage restarts)
        # Only exit on SIGINT (Ctrl+C) or explicit shutdown
        if signum == signal.SIGTERM and not self._shutting_down:
            logger.info(f"⚠️ Received SIGTERM (signal {signum}) - ignoring (launchd may be restarting)")
            # Don't exit, let launchd handle the restart
            return
        logger.info(f"🛑 Received signal {signum}, shutting down...")
        self._shutting_down = True
        self.stop_ngrok()
        sys.exit(0)
    
    def is_ngrok_running(self) -> bool:
        """Check if ngrok process is running reliably."""
        try:
            # First check our managed process
            if self.process and self.process.poll() is None:
                return True
            
            # Fallback: Check if any ngrok process is running on our port
            try:
                # Use lsof to check for ngrok processes on our specific port
                result = subprocess.run([
                    'lsof', '-i', f':{self.port}',
                    '-c', 'ngrok'
                ], capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0 and result.stdout.strip():
                    return True
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
            
            # Also check via ngrok API as last resort
            tunnel_url = self.get_ngrok_tunnel_url()
            if tunnel_url:
                logger.debug("Ngrok API responding but process not managed")
                return True
                
            return False
        except Exception as e:
            logger.debug(f"Error checking ngrok process: {e}")
            return False
    
    def get_ngrok_tunnel_url(self) -> Optional[str]:
        """Get current ngrok tunnel URL from API with retry."""
        for attempt in range(3):
            try:
                import requests
                response = requests.get(f"{NGROK_API_URL}/api/tunnels", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    tunnels = data.get('tunnels', [])
                    for tunnel in tunnels:
                        if tunnel.get('proto') == 'https' and tunnel.get('public_url'):
                            return tunnel.get('public_url')
                time.sleep(1)  # Brief delay before retry
            except Exception as e:
                logger.debug(f"Attempt {attempt + 1} to get ngrok URL failed: {e}")
                if attempt < 2:  # Don't sleep after last attempt
                    time.sleep(1)
        return None
    
    def start_ngrok(self) -> bool:
        """Start ngrok tunnel with proper process management."""
        try:
            # Kill any existing ngrok processes more carefully
            self._kill_existing_ngrok()
            time.sleep(2)
            
            # Start ngrok with output redirected to log file
            cmd = [NGROK_EXECUTABLE, 'http', str(self.port), '--log=stdout']
            
            # Add header to skip browser warning page (Option 1 from fix_ngrok_warning.md)
            cmd.extend(['--request-header-add', 'ngrok-skip-browser-warning: true'])
            
            # Handle ngrok config
            ngrok_config = self._find_ngrok_config()
            if ngrok_config:
                cmd.extend(['--config', str(ngrok_config)])
            
            logger.info(f"🚀 Starting ngrok tunnel: {' '.join(cmd)}")
            
            # Open log file for ngrok process output
            log_file = open(NGROK_PROCESS_LOG, 'a')
            log_file.write(f"\n{'='*50}\n")
            log_file.write(f"Ngrok started at {datetime.now()}\n")
            log_file.write(f"Command: {' '.join(cmd)}\n")
            log_file.flush()
            
            # Start process with output redirection
            self.process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                preexec_fn=os.setsid  # Create process group for better signal handling
            )
            
            # Wait for ngrok to initialize
            for i in range(10):
                time.sleep(2)
                if self.process.poll() is not None:
                    # Process died immediately
                    stderr = "Check ngrok_process.log for details"
                    logger.error(f"❌ Ngrok process died immediately: {stderr}")
                    return False
                
                # Check if tunnel URL is available
                tunnel_url = self.get_ngrok_tunnel_url()
                if tunnel_url:
                    logger.info(f"✅ Ngrok tunnel started: {tunnel_url}")
                    # Save PID
                    with open(PID_FILE, 'w') as f:
                        f.write(str(self.process.pid))
                    return True
            
            logger.warning("⚠️ Ngrok started but tunnel URL not available after 20 seconds")
            return True  # Process is running, URL might come later
            
        except FileNotFoundError:
            logger.error(f"❌ Ngrok executable not found: {NGROK_EXECUTABLE}")
            logger.error("   Install ngrok: https://ngrok.com/download")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to start ngrok: {e}")
            return False
    
    def _kill_existing_ngrok(self):
        """Kill existing ngrok processes more carefully."""
        try:
            # Try graceful shutdown first
            subprocess.run(['pkill', '-f', f'ngrok.*{self.port}'], 
                         capture_output=True, timeout=10)
            time.sleep(2)
            
            # Force kill any remaining
            subprocess.run(['pkill', '-9', '-f', f'ngrok.*{self.port}'], 
                         capture_output=True, timeout=5)
            time.sleep(1)
        except Exception as e:
            logger.debug(f"Error killing existing ngrok: {e}")
    
    def _find_ngrok_config(self) -> Optional[Path]:
        """Find ngrok config file if it exists and is valid."""
        config_paths = [
            Path.home() / ".ngrok2" / "ngrok.yml",
            Path.home() / ".config" / "ngrok" / "ngrok.yml",
        ]
        
        for config_path in config_paths:
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        config_content = f.read()
                        # Skip if config has placeholder token
                        if 'YOUR_NGROK_AUTHTOKEN' not in config_content:
                            return config_path
                except Exception:
                    pass
        return None
    
    def stop_ngrok(self) -> bool:
        """Stop ngrok tunnel gracefully."""
        try:
            self.running = False
            
            # Stop our managed process first
            if self.process:
                try:
                    # Send SIGTERM to process group
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                    try:
                        self.process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        # Force kill if not responding
                        os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                        self.process.wait()
                except (ProcessLookupError, OSError):
                    pass  # Process already dead
                finally:
                    self.process = None
            
            # Clean up any other ngrok processes
            self._kill_existing_ngrok()
            
            # Remove PID file
            if PID_FILE.exists():
                PID_FILE.unlink()
            
            logger.info("✅ Ngrok stopped")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to stop ngrok: {e}")
            return False
    
    def restart_ngrok(self) -> bool:
        """Restart ngrok tunnel with delay."""
        logger.info("🔄 Restarting ngrok tunnel...")
        self.stop_ngrok()
        time.sleep(3)  # Give time for ports to be released
        return self.start_ngrok()
    
    def check_dashboard_running(self) -> bool:
        """Check if dashboard is actually listening on the port."""
        try:
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                result = sock.connect_ex(('127.0.0.1', self.port))
                return result == 0
        except Exception as e:
            logger.debug(f"Error checking dashboard port: {e}")
            return False
    
    def test_tunnel_connection(self) -> bool:
        """Test if ngrok tunnel can actually reach the dashboard - more lenient."""
        try:
            tunnel_url = self.get_ngrok_tunnel_url()
            if not tunnel_url:
                return False
            
            import urllib.request
            import urllib.error
            
            # Try health endpoint first
            health_url = f"{tunnel_url}/health"
            req = urllib.request.Request(health_url, headers={
                'User-Agent': 'ngrok-health-check',
                'Accept': 'application/json'
            })
            
            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.getcode() == 200
            except urllib.error.HTTPError as e:
                # HTTP errors mean the tunnel is working but dashboard returned error
                logger.debug(f"Tunnel working but dashboard returned HTTP {e.code}")
                return e.code in [200, 404, 500]  # Consider these as "working"
            except urllib.error.URLError as e:
                # Connection errors mean tunnel is broken
                logger.debug(f"Tunnel connection error: {e}")
                return False
                
        except Exception as e:
            logger.debug(f"Error testing tunnel connection: {e}")
            return False
    
    def health_check(self) -> bool:
        """Health check that properly detects failures to trigger restarts."""
        # Step 1: Check if dashboard is listening on local port (CRITICAL - fail immediately)
        if not self.check_dashboard_running():
            logger.warning(f"❌ Health check failed: dashboard not listening on port {self.port}")
            return False
        
        # Step 2: Check if ngrok process is running (CRITICAL - fail if process dead)
        if not self.is_ngrok_running():
            logger.warning("❌ Health check failed: ngrok process not running")
            return False
        
        # Step 3: Check if tunnel URL is available (CRITICAL - fail if no URL)
        tunnel_url = self.get_ngrok_tunnel_url()
        if not tunnel_url:
            logger.warning("❌ Health check failed: tunnel URL not available")
            return False
        
        # Step 4: Test tunnel connection (CRITICAL - fail if tunnel not accessible)
        if not self.test_tunnel_connection():
            logger.warning("❌ Health check failed: tunnel connection test failed")
            return False
        
        logger.debug("✅ Health check passed")
        return True
    
    def wait_for_dashboard(self, max_wait_seconds=60):
        """Wait for dashboard to be ready before starting ngrok."""
        logger.info(f"⏳ Waiting for dashboard to be ready on port {self.port}...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_seconds:
            if self.check_dashboard_running():
                # Test health endpoint
                try:
                    import urllib.request
                    health_url = f"http://127.0.0.1:{self.port}/health"
                    req = urllib.request.Request(health_url, headers={
                        'User-Agent': 'ngrok-manager',
                        'Accept': 'application/json'
                    })
                    with urllib.request.urlopen(req, timeout=5) as response:
                        if response.getcode() == 200:
                            logger.info(f"✅ Dashboard is ready on port {self.port}")
                            return True
                except Exception as e:
                    logger.debug(f"Dashboard health check failed: {e}")
            
            if time.time() - start_time > max_wait_seconds - 5:
                logger.warning("⚠️ Dashboard taking longer than expected to start...")
            
            time.sleep(2)
        
        logger.error(f"❌ Dashboard not ready after {max_wait_seconds}s")
        return False
    
    def run_daemon(self):
        """Run ngrok manager as daemon with improved auto-reconnection."""
        logger.info(f"🚀 Starting ngrok manager daemon (port {self.port})")
        
        # Wait for dashboard to be ready first - keep waiting indefinitely
        logger.info("⏳ Waiting for dashboard to be ready before starting ngrok...")
        while not self.check_dashboard_running() and not self._shutting_down:
            logger.debug(f"Dashboard not ready on port {self.port}, waiting...")
            time.sleep(5)
        
        if self._shutting_down:
            return
        
        # Now wait for health endpoint to be ready
        logger.info(f"✅ Dashboard is listening, waiting for health endpoint...")
        self.wait_for_dashboard(max_wait_seconds=60)
        
        # Check if ngrok is already running
        if self.is_ngrok_running():
            tunnel_url = self.get_ngrok_tunnel_url()
            if tunnel_url:
                logger.info(f"✅ Ngrok already running: {tunnel_url}")
            else:
                logger.warning("⚠️ Ngrok process found but URL not available, restarting...")
                if not self.restart_ngrok():
                    logger.error("❌ Failed to restart existing ngrok process")
                    return
        else:
            # Start ngrok fresh
            if not self.start_ngrok():
                logger.error("❌ Failed to start ngrok initially")
                return
        
        self.running = True
        self.restart_count = 0
        consecutive_failures = 0
        max_consecutive_failures = 3  # Allow some temporary failures
        
        try:
            while self.running and not self._shutting_down:
                time.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.health_check():
                    consecutive_failures += 1
                    logger.warning(f"⚠️ Ngrok tunnel unhealthy (failure #{consecutive_failures})")
                    
                    # Only restart after multiple consecutive failures
                    if consecutive_failures >= max_consecutive_failures:
                        logger.error(f"❌ {consecutive_failures} consecutive failures, restarting ngrok...")
                        
                        if self.restart_count >= MAX_RESTART_ATTEMPTS:
                            logger.error(f"❌ Max restart attempts ({MAX_RESTART_ATTEMPTS}) reached, stopping")
                            break
                        
                        # Check if dashboard is still running
                        if not self.check_dashboard_running():
                            logger.error(f"❌ Dashboard not running on port {self.port}!")
                            logger.info("⏳ Waiting for dashboard to restart (will wait indefinitely)...")
                            # Wait indefinitely for dashboard to come back
                            while not self.check_dashboard_running() and not self._shutting_down:
                                time.sleep(5)
                                if int(time.time()) % 30 == 0:  # Log every 30 seconds
                                    logger.info("⏳ Still waiting for dashboard to restart...")
                            
                            if self._shutting_down:
                                break
                                
                            # Wait for health endpoint
                            if self.wait_for_dashboard(max_wait_seconds=60):
                                logger.info("✅ Dashboard is back, restarting ngrok...")
                                consecutive_failures = 0
                            else:
                                logger.warning("⚠️ Dashboard listening but health endpoint not ready yet, continuing to wait...")
                                continue  # Keep waiting instead of breaking
                        
                        # Restart ngrok
                        if self.restart_ngrok():
                            self.restart_count += 1
                            consecutive_failures = 0
                            self.last_restart = datetime.now()
                            logger.info(f"✅ Ngrok tunnel restored (restart #{self.restart_count})")
                        else:
                            self.restart_count += 1
                            logger.error(f"❌ Restart failed (attempt {self.restart_count}/{MAX_RESTART_ATTEMPTS})")
                            time.sleep(RESTART_DELAY * 2)
                else:
                    # Reset counters on success
                    if consecutive_failures > 0:
                        logger.info("✅ Ngrok tunnel recovered")
                        consecutive_failures = 0
                    if self.restart_count > 0:
                        self.restart_count = 0
                    
                    # Log tunnel status periodically
                    check_time = int(time.time())
                    if check_time % 300 == 0:  # Every 5 minutes
                        tunnel_url = self.get_ngrok_tunnel_url()
                        if tunnel_url:
                            logger.info(f"📡 Ngrok tunnel active: {tunnel_url}")
                        logger.info(f"✅ System healthy - {consecutive_failures} consecutive failures")
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"❌ Fatal error in daemon: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if not self._shutting_down:
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
            time.sleep(3)  # Wait for URL to be available
            tunnel_url = manager.get_ngrok_tunnel_url()
            if tunnel_url:
                print(f"✅ Ngrok started: {tunnel_url}")
            else:
                print("✅ Ngrok started (URL pending)")
            sys.exit(0)
        else:
            print("❌ Failed to start ngrok - check logs/ngrok_manager.log")
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
            time.sleep(3)
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
        manager = NgrokManager(port=args.port)
        if manager.is_ngrok_running():
            tunnel_url = manager.get_ngrok_tunnel_url()
            if tunnel_url:
                print(f"✅ Ngrok running: {tunnel_url}")
                # Test connection
                if manager.test_tunnel_connection():
                    print("✅ Tunnel connection: HEALTHY")
                else:
                    print("⚠️ Tunnel connection: UNHEALTHY")
            else:
                print("⚠️ Ngrok running but URL not available yet")
        else:
            print("❌ Ngrok not running")
        
        # Check dashboard
        if manager.check_dashboard_running():
            print("✅ Dashboard: RUNNING")
        else:
            print("❌ Dashboard: NOT RUNNING")
        
        sys.exit(0)
    
    elif args.action == 'daemon':
        manager.run_daemon()
        sys.exit(0)


if __name__ == "__main__":
    main()