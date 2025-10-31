#!/usr/bin/env python3
"""
Crawler Launcher - Specialized Crawler System
=============================================

Launches two specialized crawlers with proper data flow:

1. Data Mining Crawler (Crawler 2): Historical data collection (writes to files only)
2. Research Crawler (Crawler 3): Cross-venue analysis (writes to files only)

NOTE: Intraday Crawler (Crawler 1) now runs independently via run_intraday_crawler.py
      for better modularity and independent management.

Features:
- Graceful shutdown with signal handling
- Health monitoring and status reporting
- Comprehensive logging
- Resource cleanup
"""

import logging
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

# Add project root to Python path
project_root = Path(__file__).parent.parent  # Go up one level from crawlers/ to project root
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
# Intraday crawler now runs independently
from crawlers.zerodha_websocket.data_mining_crawler import create_data_mining_crawler
from crawlers.zerodha_websocket.research_crawler import create_research_crawler
# Order flow crawler now managed with intraday crawler
from crawlers.metadata_resolver import metadata_resolver


# EnhancedIntradayCrawler class removed - now managed independently


# Premarket watchlist function removed - now managed with intraday crawler


class CrawlerManager:
    """Manages the three specialized crawlers"""

    def __init__(self, token_metadata: Optional[Dict[int, Dict[str, Any]]] = None):
        self.crawlers = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        self.logger.info("🔧 Initializing CrawlerManager...")

        # Intraday crawler attributes removed - now managed independently
        # Using metadata_resolver instead of InstrumentMapper

        # Load Zerodha credentials
        self.logger.info("🔑 Loading Zerodha credentials...")
        self.api_key, self.access_token = self._load_zerodha_credentials()

        # Load instrument configurations
        self.logger.info("📋 Loading instrument configurations...")
        self.instrument_configs = self._load_instrument_configs()
        self.logger.info(f"✅ Loaded {len(self.instrument_configs)} configs: {list(self.instrument_configs.keys())}")

    def _load_zerodha_credentials(self):
        """Load Zerodha API credentials"""
        try:
            token_data = ZerodhaConfig.get_token_data()
            if not token_data:
                raise ValueError("No Zerodha token data found")

            api_key = token_data.get("api_key")
            access_token = token_data.get("access_token")

            if not api_key or not access_token:
                raise ValueError("Missing API key or access token")

            self.logger.info("✅ Zerodha credentials loaded successfully")
            return api_key, access_token

        except Exception as e:
            self.logger.error(f"❌ Failed to load Zerodha credentials: {e}")
            sys.exit(1)

    def _load_instrument_configs(self):
        """Load instrument configurations for each crawler"""
        configs = {}

        try:
            print("DEBUG: Starting config loading...")
            self.logger.info("🔍 Loading instrument configurations...")
            # Intraday Crawler - Now runs independently via run_intraday_crawler.py
            # Configuration loading removed to allow independent management

            # Data Mining Crawler - Broader market coverage
            data_mining_config_path = (
                project_root
                / "crawlers"
                / "crawler2_volatility"
                / "crawler2_volatility.json"
            )
            if data_mining_config_path.exists():
                import json

                with open(data_mining_config_path, "r") as f:
                    data_mining_config = json.load(f)
                    instruments = []
                    for inst in data_mining_config.get("instruments", {}).values():
                        token_value = inst.get("token")
                        try:
                            instruments.append(int(token_value))
                        except (TypeError, ValueError):
                            continue
                    configs["data_mining"] = {
                        "tokens": instruments,
                        "instrument_info": self._create_instrument_info(instruments),
                        "name": "data_mining_crawler",
                        "data_directory": "crawlers/raw_data/data_mining",
                    }

            # Research Crawler - Cross-venue instruments
            research_config_path = (
                project_root
                / "crawlers"
                / "crawler3_sso_xvenue"
                / "crawler3_sso_xvenue.json"
            )
            if research_config_path.exists():
                import json

                with open(research_config_path, "r") as f:
                    research_config = json.load(f)
                    instruments = []
                    for inst in research_config.get("instruments", {}).values():
                        token_value = inst.get("token")
                        try:
                            instruments.append(int(token_value))
                        except (TypeError, ValueError):
                            continue
                    configs["research"] = {
                        "tokens": instruments,
                        "instrument_info": self._create_instrument_info(instruments),
                        "name": "research_crawler",
                        "data_directory": "research_data",
                    }

            self.logger.info("✅ Instrument configurations loaded")
            return configs

        except Exception as e:
            self.logger.error(f"❌ Failed to load instrument configurations: {e}")
            return {}

    def _create_instrument_info(self, tokens: List[int]) -> Dict[int, Dict]:
        """Create instrument info mapping using metadata_resolver."""
        instrument_info = {}
        for token in tokens:
            meta = metadata_resolver.get_metadata(token)
            instrument_info[token] = {
                "token": token,
                "symbol": meta.get("symbol", f"TOKEN_{token}"),
                "tradingsymbol": meta.get("tradingsymbol", f"TOKEN_{token}"),
                "name": meta.get("name", f"TOKEN_{token}"),
                "exchange": meta.get("exchange", "NSE"),
                "segment": meta.get("segment", "EQ"),
                "asset_class": meta.get("instrument_type", "unknown"),
            }
        return instrument_info

    def initialize_crawlers(self, targets: Optional[Iterable[str]] = None):
        """Initialize crawlers filtered by targets (None = all)."""
        try:
            self.logger.info("🚀 Initializing crawlers...")
            self.logger.info(f"📊 Available configs: {list(self.instrument_configs.keys())}")
            target_set = set(t.lower() for t in targets) if targets else None
            # Intraday Crawler - Now runs independently via run_intraday_crawler.py
            # Removed from launch_crawlers.py to allow independent management

            # Data Mining Crawler - File writing only
            if "data_mining" in self.instrument_configs and (
                target_set is None or "data_mining" in target_set
            ):
                config = self.instrument_configs["data_mining"]
                self.crawlers["data_mining"] = create_data_mining_crawler(
                    api_key=self.api_key,
                    access_token=self.access_token,
                    instruments=config["tokens"],
                    instrument_info=config["instrument_info"],
                    data_directory=config.get("data_directory", "crawlers/raw_data/data_mining"),
                    name=config["name"],
                )
                self.logger.info("✅ Data Mining Crawler initialized (File writing)")

            # Research Crawler - File writing only
            if "research" in self.instrument_configs and (
                target_set is None or "research" in target_set
            ):
                config = self.instrument_configs["research"]
                self.crawlers["research"] = create_research_crawler(
                    api_key=self.api_key,
                    access_token=self.access_token,
                    instruments=config["tokens"],
                    instrument_info=config["instrument_info"],
                    data_directory=config.get("data_directory", "research_data"),
                    name=config["name"],
                )
                self.logger.info("✅ Research Crawler initialized (File writing)")

            if not self.crawlers:
                raise ValueError("No crawlers were initialized")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize crawlers: {e}")
            sys.exit(1)

    def start(self):
        """Start all crawlers"""
        try:
            self.running = True

            for name, crawler in self.crawlers.items():
                crawler.start()
                self.logger.info(f"🚀 Started {name} crawler")

            self.logger.info("🎯 All crawlers started successfully")

        except Exception as e:
            self.logger.error(f"❌ Failed to start crawlers: {e}")
            self.stop()

    def stop(self):
        """Stop all crawlers gracefully"""
        try:
            self.running = False

            for name, crawler in self.crawlers.items():
                try:
                    crawler.stop()
                finally:
                    self.logger.info(f"🛑 Stopped {name} crawler")

            # Order flow crawler now managed independently with intraday crawler

            self.logger.info("✅ All crawlers stopped successfully")

        except Exception as e:
            self.logger.error(f"❌ Error stopping crawlers: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get status of all crawlers"""
        status = {
            "running": self.running,
            "crawler_count": len(self.crawlers),
            "crawler_status": {},
        }

        for name, crawler in self.crawlers.items():
            try:
                if hasattr(crawler, "get_intraday_status"):
                    status["crawler_status"][name] = crawler.get_intraday_status()
                elif hasattr(crawler, "get_data_mining_status"):
                    status["crawler_status"][name] = crawler.get_data_mining_status()
                elif hasattr(crawler, "get_research_status"):
                    status["crawler_status"][name] = crawler.get_research_status()
                else:
                    status["crawler_status"][name] = crawler.get_status()
            except Exception as e:
                status["crawler_status"][name] = {"error": str(e)}

        return status

def setup_logging():
    """Setup comprehensive logging configuration with organized structure"""
    # Create main logs directory
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create crawler-specific log directories
    crawler_log_dir = log_dir / "crawlers"
    crawler_log_dir.mkdir(exist_ok=True)
    
    scanner_log_dir = log_dir / "scanner"
    scanner_log_dir.mkdir(exist_ok=True)

    # Setup main launcher logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / "crawler_launcher.log"),
        ],
    )
    
    # Setup crawler-specific loggers
    crawler_logger = logging.getLogger('crawlers')
    crawler_logger.setLevel(logging.INFO)
    crawler_handler = logging.FileHandler(crawler_log_dir / "crawler_main.log")
    crawler_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    crawler_logger.addHandler(crawler_handler)
    
    # Setup scanner logger
    scanner_logger = logging.getLogger('scanner')
    scanner_logger.setLevel(logging.INFO)
    scanner_handler = logging.FileHandler(scanner_log_dir / "scanner_main.log")
    scanner_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    scanner_logger.addHandler(scanner_handler)


def main():
    """Main launcher function"""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting Specialized Crawler System...")
    logger.info("🔧 Setting up logging and imports...")
    logger.info("=" * 60)
    logger.info("SPECIALIZED CRAWLER ARCHITECTURE:")
    logger.info(
        "1. Data Mining Crawler - Binary file writing for historical data"
    )
    logger.info("2. Research Crawler    - Binary file writing for cross-venue analysis")
    logger.info("NOTE: Intraday Crawler runs independently via run_intraday_crawler.py")
    logger.info("=" * 60)

    # Create crawler manager
    try:
        logger.info("🔧 Creating CrawlerManager...")
        crawler_manager = CrawlerManager()
        logger.info("🔧 Initializing crawlers...")
        crawler_manager.initialize_crawlers()
        logger.info("✅ Crawler manager created successfully")
    except Exception as e:
        logger.error(f"❌ Failed to create crawler manager: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        logger.info(f"🛑 Received signal {signum}, initiating shutdown...")
        crawler_manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start the crawlers
    try:
        crawler_manager.start()
        logger.info("🎯 All specialized crawlers started successfully")

        # Keep main thread alive and monitor status
        while crawler_manager.running:
            try:
                # Check status every 30 seconds
                status = crawler_manager.get_status()
                active_crawlers = len(
                    [
                        s
                        for s in status["crawler_status"].values()
                        if s.get("state") == "running"
                    ]
                )

                logger.info(
                    f"📊 Status: {active_crawlers}/{status['crawler_count']} crawlers active"
                )

                # Log detailed status for each crawler
                for name, crawler_status in status["crawler_status"].items():
                    state = crawler_status.get("state", "unknown")
                    if name == "intraday" and "tick_count" in crawler_status:
                        logger.info(
                            f"   {name}: {state} - {crawler_status['tick_count']} ticks"
                        )
                    elif (
                        name == "data_mining"
                        and "total_message_count" in crawler_status
                    ):
                        logger.info(
                            f"   {name}: {state} - {crawler_status['total_message_count']} messages"
                        )
                    elif (
                        name == "research"
                        and "total_research_messages" in crawler_status
                    ):
                        logger.info(
                            f"   {name}: {state} - {crawler_status['total_research_messages']} messages"
                        )
                    else:
                        logger.info(f"   {name}: {state}")

                time.sleep(30)

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                logger.error(f"Status check error: {e}")
                time.sleep(10)

    except Exception as e:
        logger.error(f"❌ Fatal error in crawler manager: {e}")
    finally:
        logger.info("🛑 Shutting down crawler system...")
        crawler_manager.stop()
        logger.info("✅ Crawler system shutdown complete")


if __name__ == "__main__":
    main()
