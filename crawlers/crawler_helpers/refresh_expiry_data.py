#!/usr/bin/env python3
"""
Refresh expiry data for all instruments in data_mining_crawler3.json
Fetches expiry from Zerodha API and updates the config
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add config directory to path
sys.path.append(str(Path(__file__).parent.parent / "config"))

from zerodha_config import ZerodhaConfig

def refresh_expiry_data():
    """Add expiry data to all instruments"""
    print("=" * 60)
    print("REFRESHING EXPIRY DATA")
    print("=" * 60)
    
    try:
        # Get KiteConnect instance
        kite = ZerodhaConfig.get_kite_instance()
        print("✅ Connected to Zerodha API")
        
        # Fetch all instruments
        print("📥 Fetching all instruments from Zerodha...")
        all_instruments = kite.instruments()
        print(f"✅ Fetched {len(all_instruments)} instruments")
        
        # Create token -> instrument mapping
        token_map = {}
        for inst in all_instruments:
            token = inst.get('instrument_token')
            if token:
                token_map[token] = inst
        
        # Load config
        config_path = Path(__file__).parent.parent / "config" / "data_mining_crawler3.json"
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        print(f"\n📊 Updating {len(config['instruments'])} instruments...")
        
        updated_count = 0
        
        for key, inst in config['instruments'].items():
            token = inst.get('token')
            if token and token in token_map:
                zerodha_inst = token_map[token]
                expiry = zerodha_inst.get('expiry')
                
                if expiry:
                    # Update with expiry data
                    inst['expiry'] = expiry.isoformat() if hasattr(expiry, 'isoformat') else str(expiry)
                    updated_count += 1
        
        print(f"  ✅ Updated {updated_count} instruments with expiry data")
        
        # Update metadata
        config['metadata']['last_expiry_refresh'] = datetime.now().isoformat()
        
        # Save
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print("\n✅ Expiry data refresh complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    refresh_expiry_data()

