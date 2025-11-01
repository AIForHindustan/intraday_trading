"""
Mullvad SSH Tunnel Manager for Dashboard Exposure
=================================================

Manages SSH reverse tunnel through VPS for dashboard access.
Works with Mullvad VPN for enhanced privacy/security.

Prerequisites:
- VPS/server with SSH access
- Mullvad VPN (optional but recommended)
- autossh installed (for auto-reconnect)

Usage:
    python scripts/mullvad_ssh_tunnel_manager.py start
    python scripts/mullvad_ssh_tunnel_manager.py stop
    python scripts/mullvad_ssh_tunnel_manager.py status
    python scripts/mullvad_ssh_tunnel_manager.py daemon

Configuration:
    Set VPS_HOST, VPS_USER, and SSH_KEY in environment variables or config file.
"""

import os
import sys
import time
import signal
import subprocess
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configuration
DASHBOARD_PORT = 53056
VPS_HOST = os.getenv("VPS_HOST", "")  # Set via environment variable
VPS_USER = os.getenv("VPS_USER", "")  # Set via environment variable
SSH_KEY = os.getenv("SSH_KEY_PATH", "")  # Optional: path to SSH private key
REMOTE_PORT = int(os.getenv("REMOTE_PORT", str(DASHBOARD_PORT)))  # Port on VPS

PID_FILE = project_root / "logs" / "mullvad_ssh_tunnel.pid"
LOG_FILE = project_root / "logs" / "mullvad_ssh_tunnel_manager.log"
HEALTH_CHECK_INTERVAL = 60  # seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class MullvadSSHTunnelManager:
    """SSH tunnel manager for Mullvad VPN setup."""
    
    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.running = False
        
        # Validate configuration
        if not VPS_HOST or not VPS_USER:
            logger.error("❌ VPS_HOST and VPS_USER must be set via environment variables")
            logger.error("   Example: export VPS_HOST=your-vps-ip && export VPS_USER=username")
            raise ValueError("VPS configuration missing")
    
    def check_autossh_installed(self) -> bool:
        """Check if autossh is installed."""
        try:
            result = subprocess.run(['autossh', '-V'], capture_output=True, timeout=5)
            return result.returncode == 0 or 'autossh' in str(result.stderr)
        except Exception:
            return False
    
    def is_tunnel_running(self) -> bool:
        """Check if SSH tunnel is running."""
        try:
            if self.process:
                return self.process.poll() is None
            
            # Check for autossh process
            result = subprocess.run(['pgrep', '-f', f'autossh.*{VPS_HOST}'], 
                                  capture_output=True, text=True)
            if result.returncode == 0 and len(result.stdout.strip()) > 0:
                return True
            
            # Also check for ssh process with reverse port forwarding
            result = subprocess.run(['pgrep', '-f', f'ssh.*-R.*{REMOTE_PORT}'], 
                                  capture_output=True, text=True)
            return result.returncode == 0 and len(result.stdout.strip()) > 0
        except Exception:
            return False
    
    def start_tunnel(self) -> bool:
        """Start SSH reverse tunnel."""
        try:
            # Check if autossh is installed
            if not self.check_autossh_installed():
                logger.error("❌ autossh not found")
                logger.error("   Install: brew install autossh")
                logger.error("   Or use: ssh instead of autossh (no auto-reconnect)")
                # Fall back to regular ssh
                use_autossh = False
            else:
                use_autossh = True
            
            # Build SSH command
            if use_autossh:
                cmd = [
                    'autossh',
                    '-M', '0',  # No monitoring port (use TCP keepalive)
                    '-f',  # Fork to background
                    '-N',  # No command execution
                    '-o', 'ServerAliveInterval=60',
                    '-o', 'ServerAliveCountMax=3',
                    '-o', 'ExitOnForwardFailure=yes',
                    '-R', f'{REMOTE_PORT}:localhost:{DASHBOARD_PORT}',
                ]
            else:
                cmd = [
                    'ssh',
                    '-N',  # No command execution
                    '-o', 'ServerAliveInterval=60',
                    '-o', 'ServerAliveCountMax=3',
                    '-o', 'ExitOnForwardFailure=yes',
                    '-R', f'{REMOTE_PORT}:localhost:{DASHBOARD_PORT}',
                ]
            
            # Add SSH key if specified
            if SSH_KEY and Path(SSH_KEY).exists():
                cmd.extend(['-i', SSH_KEY])
            
            # Add VPS connection
            cmd.append(f'{VPS_USER}@{VPS_HOST}')
            
            logger.info(f"🚀 Starting SSH tunnel: {' '.join(cmd)}")
            
            # Start process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait a moment
            time.sleep(2)
            
            # Check if process is still running
            if self.process.poll() is not None:
                stderr = self.process.stderr.read() if self.process.stderr else ""
                logger.error(f"❌ SSH tunnel failed to start: {stderr}")
                return False
            
            # Save PID
            with open(PID_FILE, 'w') as f:
                f.write(str(self.process.pid))
            
            logger.info(f"✅ SSH tunnel started (connecting to {VPS_USER}@{VPS_HOST})")
            logger.info(f"   Dashboard accessible at: http://{VPS_HOST}:{REMOTE_PORT}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to start SSH tunnel: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def stop_tunnel(self) -> bool:
        """Stop SSH tunnel."""
        try:
            self.running = False
            
            if self.process:
                try:
                    self.process.terminate()
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                except Exception:
                    pass
                self.process = None
            
            # Kill any autossh/ssh processes for this tunnel
            subprocess.run(['pkill', '-f', f'autossh.*{VPS_HOST}'], capture_output=True)
            subprocess.run(['pkill', '-f', f'ssh.*-R.*{REMOTE_PORT}'], capture_output=True)
            time.sleep(1)
            
            if PID_FILE.exists():
                PID_FILE.unlink()
            
            logger.info("✅ SSH tunnel stopped")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to stop SSH tunnel: {e}")
            return False
    
    def check_dashboard_running(self) -> bool:
        """Check if dashboard is listening locally."""
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', DASHBOARD_PORT))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def health_check(self) -> bool:
        """Check if tunnel is healthy."""
        if not self.is_tunnel_running():
            return False
        
        if not self.check_dashboard_running():
            return False
        
        return True
    
    def run_daemon(self):
        """Run tunnel manager as daemon with auto-reconnection."""
        logger.info(f"🚀 Starting Mullvad SSH Tunnel manager daemon")
        logger.info(f"   VPS: {VPS_USER}@{VPS_HOST}")
        logger.info(f"   Remote port: {REMOTE_PORT}")
        
        if not self.start_tunnel():
            logger.error("❌ Failed to start tunnel initially")
            return
        
        self.running = True
        
        try:
            while self.running:
                time.sleep(HEALTH_CHECK_INTERVAL)
                
                if not self.health_check():
                    logger.warning("⚠️ Tunnel unhealthy, attempting restart...")
                    self.stop_tunnel()
                    time.sleep(5)
                    if not self.start_tunnel():
                        logger.error("❌ Failed to restart tunnel")
                        time.sleep(30)  # Wait longer before retry
                else:
                    logger.debug("✅ Tunnel health check passed")
        
        except KeyboardInterrupt:
            logger.info("🛑 Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"❌ Fatal error in daemon: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.stop_tunnel()
            logger.info("✅ SSH Tunnel manager daemon stopped")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Mullvad SSH Tunnel manager")
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'status', 'daemon'],
                       help='Action to perform')
    
    args = parser.parse_args()
    
    try:
        manager = MullvadSSHTunnelManager()
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nSetup instructions:")
        print("  1. Set VPS_HOST: export VPS_HOST=your-vps-ip")
        print("  2. Set VPS_USER: export VPS_USER=your-username")
        print("  3. Optional: Set SSH_KEY_PATH: export SSH_KEY_PATH=/path/to/key")
        sys.exit(1)
    
    if args.action == 'start':
        if manager.start_tunnel():
            print(f"✅ SSH Tunnel started")
            print(f"   Dashboard: http://{VPS_HOST}:{REMOTE_PORT}")
            sys.exit(0)
        else:
            print("❌ Failed to start SSH Tunnel")
            sys.exit(1)
    
    elif args.action == 'stop':
        if manager.stop_tunnel():
            print("✅ SSH Tunnel stopped")
            sys.exit(0)
        else:
            print("❌ Failed to stop SSH Tunnel")
            sys.exit(1)
    
    elif args.action == 'restart':
        manager.stop_tunnel()
        time.sleep(2)
        if manager.start_tunnel():
            print(f"✅ SSH Tunnel restarted")
            sys.exit(0)
        else:
            print("❌ Failed to restart SSH Tunnel")
            sys.exit(1)
    
    elif args.action == 'status':
        if manager.is_tunnel_running():
            print(f"✅ SSH Tunnel running")
            print(f"   Dashboard: http://{VPS_HOST}:{REMOTE_PORT}")
        else:
            print("❌ SSH Tunnel not running")
        sys.exit(0)
    
    elif args.action == 'daemon':
        manager.run_daemon()
        sys.exit(0)


if __name__ == "__main__":
    main()

