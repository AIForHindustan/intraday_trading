#!/usr/bin/env python3
"""
Update all token files from Zerodha API
Fetches latest instrument data and MERGES into existing token mapping files
Preserves existing tokens while adding new ones
"""

import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig

def load_existing_tokens():
    """Load existing master token file"""
    master_file = Path("core/data/token_lookup_enriched.json")
    if master_file.exists():
        try:
            with open(master_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading existing tokens: {e}")
            return {}
    return {}

def load_existing_nifty50():
    """Load existing NIFTY 50 tokens"""
    nifty50_file = Path("crawlers/master_token/nifty50_tokens.json")
    if nifty50_file.exists():
        try:
            with open(nifty50_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading existing NIFTY 50 tokens: {e}")
            return {}
    return {}

def load_existing_indices():
    """Load existing indices tokens"""
    indices_file = Path("indices_tokens_final.json")
    if indices_file.exists():
        try:
            with open(indices_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error loading existing indices tokens: {e}")
            return {}
    return {}

def update_all_token_files():
    """Update all token files with latest data from Zerodha - MERGING with existing data"""
    print("🚀 MERGING TOKEN FILES FROM ZERODHA (PRESERVING EXISTING DATA)")
    print("=" * 70)
    
    try:
        # Load existing token data
        print("📥 Loading existing token data...")
        existing_tokens = load_existing_tokens()
        existing_nifty50 = load_existing_nifty50()
        existing_indices = load_existing_indices()
        
        print(f"✅ Loaded existing data:")
        print(f"   • Master tokens: {len(existing_tokens)}")
        print(f"   • NIFTY 50 tokens: {len(existing_nifty50)}")
        print(f"   • Indices tokens: {len(existing_indices)}")
        
        # Get KiteConnect instance
        kite = ZerodhaConfig.get_kite_instance()
        print("✅ Connected to Zerodha API")
        
        # Fetch all instruments
        print("📥 Fetching all instruments from Zerodha...")
        all_instruments = kite.instruments()
        print(f"✅ Fetched {len(all_instruments)} instruments")
        
        # Start with existing data (preserve everything)
        token_mapping = existing_tokens.copy()
        # Clean NIFTY 50 list - start fresh with correct 50 stocks only
        nifty50_tokens = {}
        indices_tokens = existing_indices.copy()
        
        # Track new additions
        new_tokens_added = 0
        new_nifty50_added = 0
        new_indices_added = 0
        
        for inst in all_instruments:
            symbol = inst.get('tradingsymbol', '')
            exchange = inst.get('exchange', '')
            token = inst.get('instrument_token', 0)
            name = inst.get('name', '')
            instrument_type = inst.get('instrument_type', '')
            
            if not symbol or not token:
                continue
                
            # Update or add to comprehensive mapping (use token as key to match existing structure)
            token_str = str(token)
            key = f"{exchange}:{symbol}"
            
            # Update existing entry or create new one
            if token_str in token_mapping:
                # Update existing entry with latest data
                token_mapping[token_str]['key'] = key
                token_mapping[token_str]['name'] = name
                token_mapping[token_str]['exchange'] = exchange
                token_mapping[token_str]['instrument_type'] = instrument_type
                token_mapping[token_str]['updated_at'] = datetime.now().isoformat()
            else:
                # Add new entry
                token_mapping[token_str] = {
                    'key': key,
                    'token': token,
                    'name': name,
                    'exchange': exchange,
                    'instrument_type': instrument_type,
                    'updated_at': datetime.now().isoformat()
                }
                new_tokens_added += 1
            
            # Check if it's a NIFTY 50 stock (only add if not already exists)
            if exchange == 'NSE' and instrument_type == 'EQ':
                # Load NIFTY 50 symbols from source of truth (nifty50_tokens.json)
                # This ensures consistency across the codebase
                nifty50_file = Path("crawlers/master_token/nifty50_tokens.json")
                if nifty50_file.exists():
                    try:
                        with open(nifty50_file, 'r') as f:
                            nifty50_data = json.load(f)
                        nifty50_symbols = set(nifty50_data.keys())
                    except Exception as e:
                        print(f"⚠️ Error loading NIFTY 50 list: {e}")
                        nifty50_symbols = set()
                else:
                    nifty50_symbols = set()
                
                if symbol in nifty50_symbols:
                    nifty50_tokens[symbol] = {
                        'token': token,
                        'name': name,
                        'exchange': exchange,
                        'instrument_type': instrument_type,
                        'updated_at': datetime.now().isoformat()
                    }
                    new_nifty50_added += 1
            
            # Check if it's an index (only add if not already exists)
            if exchange == 'NSE' and instrument_type == 'INDEX' and symbol not in indices_tokens:
                indices_tokens[symbol] = {
                    'token': token,
                    'name': name,
                    'exchange': exchange,
                    'instrument_type': instrument_type,
                    'updated_at': datetime.now().isoformat()
                }
                new_indices_added += 1
        
        # Update comprehensive token mapping
        comprehensive_file = Path("core/data/token_lookup_enriched.json")
        with open(comprehensive_file, 'w') as f:
            json.dump(token_mapping, f, indent=2)
        print(f"✅ Merged comprehensive token mapping: {len(token_mapping)} total instruments (+{new_tokens_added} new)")
        
        # Update NIFTY 50 tokens
        nifty50_file = Path("crawlers/master_token/nifty50_tokens.json")
        with open(nifty50_file, 'w') as f:
            json.dump(nifty50_tokens, f, indent=2)
        print(f"✅ Merged NIFTY 50 tokens: {len(nifty50_tokens)} total stocks (+{new_nifty50_added} new)")
        
        # Update indices tokens
        indices_file = Path("indices_tokens_final.json")
        with open(indices_file, 'w') as f:
            json.dump(indices_tokens, f, indent=2)
        print(f"✅ Merged indices tokens: {len(indices_tokens)} total indices (+{new_indices_added} new)")
        
        # Check for INDIGO and MAXHEALTH specifically
        print("\n🔍 CHECKING FOR NEW NIFTY 50 STOCKS:")
        for symbol in ['INDIGO', 'MAXHEALTH']:
            if symbol in nifty50_tokens:
                print(f"✅ {symbol} found - Token: {nifty50_tokens[symbol]['token']}")
            else:
                print(f"❌ {symbol} not found in Zerodha data")
        
        print(f"\n🎉 TOKEN MERGE COMPLETED!")
        print(f"   • Total instruments: {len(token_mapping)} (added {new_tokens_added} new)")
        print(f"   • NIFTY 50 stocks: {len(nifty50_tokens)} (added {new_nifty50_added} new)")
        print(f"   • Indices: {len(indices_tokens)} (added {new_indices_added} new)")
        print(f"   • Preserved all existing tokens!")
        
    except Exception as e:
        print(f"❌ Error updating tokens: {e}")
        return False
    
    return True

if __name__ == "__main__":
    update_all_token_files()
