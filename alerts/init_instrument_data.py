#!/usr/bin/env python3
"""
Initialize instrument data in Redis for the trading dashboard
Run this script to populate Redis with instrument data from existing sources
"""

import asyncio
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from alerts.simple_instrument_manager import instrument_manager

async def main():
    """Initialize instrument data in Redis"""
    print("🚀 Initializing instrument data for trading dashboard...")
    
    try:
        # Initialize the data manager
        await instrument_manager.initialize_data()
        
        # Get metadata to verify
        metadata = await instrument_manager.get_instrument_metadata()
        print(f"✅ Successfully loaded {metadata.get('total_instruments', 0)} instruments")
        print(f"📊 Categories: {metadata.get('categories', {})}")
        print(f"🕒 Last updated: {metadata.get('last_updated', 'Unknown')}")
        
        # Test each asset class
        asset_classes = ['equity_cash', 'equity_futures', 'index_futures', 'index_options']
        for asset_class in asset_classes:
            instruments = await instrument_manager.get_instruments_by_asset_class(asset_class)
            print(f"📈 {asset_class}: {len(instruments)} instruments")
        
        print("\n🎯 Instrument data ready for dashboard!")
        print("💡 Access via: http://localhost:8000/api/instruments/{asset_class}")
        
    except Exception as e:
        print(f"❌ Failed to initialize instrument data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
