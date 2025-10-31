#!/usr/bin/env python3
"""
Intraday Crawler Runner
=======================

Purpose:
--------
Runs the intraday crawler as a separate process for real-time trading data.
This crawler publishes data to Redis for real-time analysis and pattern detection.

Dependent Scripts:
-----------------
- crawlers/metadata_resolver.py: Provides instrument metadata resolution
- crawlers/zerodha_websocket/intraday_crawler.py: Main intraday crawler implementation
- config/zerodha_config.py: Zerodha API credentials and configuration
- crawlers/binary_crawler1/binary_crawler1.json: Intraday instrument configuration

Important Aspects:
-----------------
- Uses metadata_resolver for centralized instrument metadata
- Publishes parsed tick data to Redis DB 4 (continuous_market)
- Writes binary data to crawlers/raw_data/intraday_data directory
- Optimized for low-latency processing
- Handles 218+ intraday data files
- Supports separate process execution for system stability
- Integrates with real-time pattern detection system
"""

import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
from crawlers.zerodha_websocket.intraday_crawler import create_intraday_crawler
from crawlers.metadata_resolver import metadata_resolver

def main():
    """Run intraday crawler as separate process"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Intraday Crawler (Separate Process)")
    logger.info("=" * 60)
    
    try:
        # Load Zerodha credentials
        logger.info("🔑 Loading Zerodha credentials...")
        token_data = ZerodhaConfig.get_token_data()
        if not token_data:
            raise ValueError("No Zerodha token data found")
        
        api_key = token_data.get("api_key")
        access_token = token_data.get("access_token")
        
        if not api_key or not access_token:
            raise ValueError("Missing API key or access token")
        
        logger.info("✅ Zerodha credentials loaded")
        
        # Load instrument configuration
        logger.info("📋 Loading intraday instrument configuration...")
        intraday_config_path = project_root / "crawlers" / "binary_crawler1" / "binary_crawler1.json"
        
        if not intraday_config_path.exists():
            raise FileNotFoundError(f"Intraday config not found: {intraday_config_path}")
        
        import json
        with open(intraday_config_path, "r") as f:
            intraday_config = json.load(f)
            # The config has a direct 'tokens' array
            raw_tokens = intraday_config.get("tokens", [])
            instruments = []
            for token in raw_tokens:
                try:
                    instruments.append(int(token))
                except (TypeError, ValueError):
                    continue
        
        logger.info(f"📊 Loaded {len(instruments)} intraday instruments")
        
        # Create instrument info mapping using metadata_resolver
        logger.info("🔧 Creating instrument info mapping using metadata_resolver...")
        instrument_info = {}
        for token in instruments:
            instrument_info[token] = metadata_resolver.get_instrument_info(token)
        
        logger.info(f"✅ Created instrument info for {len(instrument_info)} instruments")
        
        # Create and start intraday crawler
        logger.info("🔧 Creating intraday crawler...")
        crawler = create_intraday_crawler(
            api_key=api_key,
            access_token=access_token,
            instruments=instruments,
            instrument_info=instrument_info,
            data_directory="crawlers/raw_data/intraday_data",
            name="intraday_crawler",
        )
        
        logger.info("🚀 Starting intraday crawler...")
        crawler.start()
        
        # Keep running until interrupted
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping intraday crawler...")
            crawler.stop()
            logger.info("✅ Intraday crawler stopped")
    
    except Exception as e:
        logger.error(f"❌ Failed to start intraday crawler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
