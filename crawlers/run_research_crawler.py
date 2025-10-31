#!/usr/bin/env python3
"""
Research Crawler Runner
======================

Purpose:
--------
Runs the research crawler as a separate process for cross-venue analysis.
This crawler writes binary data to files for research analysis.

Dependent Scripts:
-----------------
- crawlers/metadata_resolver.py: Provides instrument metadata resolution
- crawlers/zerodha_websocket/research_crawler.py: Main research crawler implementation
- config/zerodha_config.py: Zerodha API credentials and configuration
- crawlers/crawler3_sso_xvenue/crawler3_sso_xvenue.json: Research instrument configuration

Important Aspects:
-----------------
- Uses metadata_resolver for centralized instrument metadata
- Writes binary data to research_data directory
- No external data publishing (file writing only)
- Optimized for data completeness and research flexibility
- Supports SSO-XVENUE cross-venue analysis
- Handles research data files for analysis
- Supports separate process execution for system stability
"""

import sys
import logging
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
from crawlers.zerodha_websocket.research_crawler import create_research_crawler
from crawlers.metadata_resolver import metadata_resolver

def main():
    """Run research crawler as separate process"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Research Crawler (Separate Process)")
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
        logger.info("📋 Loading research instrument configuration...")
        research_config_path = project_root / "crawlers" / "crawler3_sso_xvenue" / "crawler3_sso_xvenue.json"
        
        if not research_config_path.exists():
            raise FileNotFoundError(f"Research config not found: {research_config_path}")
        
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
        
        logger.info(f"📊 Loaded {len(instruments)} research instruments")
        
        # Create instrument info mapping using metadata_resolver
        logger.info("🔧 Creating instrument info mapping using metadata_resolver...")
        instrument_info = {}
        for token in instruments:
            instrument_info[token] = metadata_resolver.get_instrument_info(token)
        
        logger.info(f"✅ Created instrument info for {len(instrument_info)} instruments")
        
        # Create and start research crawler
        logger.info("🔧 Creating research crawler...")
        crawler = create_research_crawler(
            api_key=api_key,
            access_token=access_token,
            instruments=instruments,
            instrument_info=instrument_info,
            data_directory="research_data",
            name="research_crawler",
        )
        
        logger.info("🚀 Starting research crawler...")
        crawler.start()
        
        # Keep running until interrupted
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping research crawler...")
            crawler.stop()
            logger.info("✅ Research crawler stopped")
    
    except Exception as e:
        logger.error(f"❌ Failed to start research crawler: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
