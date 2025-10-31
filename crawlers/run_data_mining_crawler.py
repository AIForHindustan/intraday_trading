#!/usr/bin/env python3
"""
Data Mining Crawler Runner
=========================

Purpose:
--------
Runs the data mining crawler as a separate process for historical data collection.
This crawler writes binary data to files for historical analysis.

Dependent Scripts:
-----------------
- crawlers/metadata_resolver.py: Provides instrument metadata resolution
- crawlers/zerodha_websocket/data_mining_crawler.py: Main data mining crawler implementation
- config/zerodha_config.py: Zerodha API credentials and configuration
- crawlers/crawler2_volatility/crawler2_volatility.json: Data mining instrument configuration

Important Aspects:
-----------------
- Uses metadata_resolver for centralized instrument metadata
- Writes binary data to crawlers/raw_data/data_mining directory
- No external data publishing (file writing only)
- Optimized for data integrity and storage efficiency
- Handles 166+ historical data files
- Supports separate process execution for system stability
"""

import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
from crawlers.zerodha_websocket.data_mining_crawler import create_data_mining_crawler
from crawlers.metadata_resolver import metadata_resolver

def main():
    """Run data mining crawler as separate process"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Data Mining Crawler (Separate Process)")
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
        logger.info("📋 Loading data mining instrument configuration...")
        data_mining_config_path = project_root / "crawlers" / "crawler2_volatility" / "crawler2_volatility.json"
        
        if not data_mining_config_path.exists():
            raise FileNotFoundError(f"Data mining config not found: {data_mining_config_path}")
        
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
        
        logger.info(f"📊 Loaded {len(instruments)} data mining instruments")
        
        # Create instrument info mapping using metadata_resolver
        logger.info("🔧 Creating instrument info mapping using metadata_resolver...")
        instrument_info = {}
        for token in instruments:
            instrument_info[token] = metadata_resolver.get_instrument_info(token)
        
        logger.info(f"✅ Created instrument info for {len(instrument_info)} instruments")
        
        # Create and start data mining crawler
        logger.info("🔧 Creating data mining crawler...")
        crawler = create_data_mining_crawler(
            api_key=api_key,
            access_token=access_token,
            instruments=instruments,
            instrument_info=instrument_info,
            data_directory="crawlers/raw_data/data_mining",
            name="data_mining_crawler",
        )
        
        logger.info("🚀 Starting data mining crawler...")
        crawler.start()
        
        # Keep running until interrupted
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping data mining crawler...")
            crawler.stop()
            logger.info("✅ Data mining crawler stopped")
    
    except Exception as e:
        logger.error(f"❌ Failed to start data mining crawler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
