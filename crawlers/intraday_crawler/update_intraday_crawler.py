#!/usr/bin/env python3
"""
Update Intraday Crawler Configuration
=====================================

This script modifies the intraday_crawler configuration to:
1. Remove all instruments expiring on 28th October 2025
2. Add December futures and options with ATM ±2 strikes from token_lookup.json

The script focuses on NSE F&O instruments for the intraday crawler.
"""

import json
import re
from collections import defaultdict
from pathlib import Path

def load_configs():
    """Load current configuration and token lookup"""
    # Load current intraday crawler config
    config_path = Path("crawlers/binary_crawler1/binary_crawler1.json")
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Load token lookup
    token_lookup_path = Path("core/data/token_lookup.json")
    with open(token_lookup_path, 'r') as f:
        token_lookup = json.load(f)
    
    return config, token_lookup

def get_current_tokens(config):
    """Get current tokens from config"""
    return set(config.get("tokens", []))

def find_october_expiry_tokens_in_config(current_tokens, token_lookup):
    """Find tokens expiring on 28OCT that are currently in the config"""
    oct_tokens = []
    for token in current_tokens:
        token_str = str(token)
        if token_str in token_lookup:
            data = token_lookup[token_str]
            if '28OCT' in data.get('key', ''):
                oct_tokens.append(token)
    return oct_tokens

def find_december_instruments(token_lookup):
    """Find all December instruments"""
    dec_instruments = {}
    for token_str, data in token_lookup.items():
        key = data.get('key', '')
        if '25DEC' in key:
            dec_instruments[int(token_str)] = data
    return dec_instruments

def group_by_underlying(dec_instruments):
    """Group December instruments by underlying asset"""
    asset_groups = defaultdict(list)
    for token, data in dec_instruments.items():
        name = data.get('name', '')
        asset_groups[name].append((token, data))
    return asset_groups

def extract_strike_price(key):
    """Extract strike price from instrument key"""
    # Pattern for options: ASSET25DECSTRIKETYPE
    # Example: NFO:RELIANCE25DEC1370CE
    match = re.search(r'(\d+(?:\.\d+)?)(CE|PE)$', key)
    if match:
        return float(match.group(1))
    return None

def select_atm_instruments(asset_groups, target_count, atm_range=2):
    """Select ATM ±2 instruments for each asset until we reach target count"""
    selected_tokens = []
    
    for asset, instruments in asset_groups.items():
        if len(selected_tokens) >= target_count:
            break
            
        # Separate futures and options
        futures = []
        options = []
        
        for token, data in instruments:
            inst_type = data.get('instrument_type', '')
            if inst_type == 'FUT':
                futures.append((token, data))
            elif inst_type in ['CE', 'PE']:
                options.append((token, data))
        
        # Add all futures first
        for token, data in futures:
            if len(selected_tokens) < target_count:
                selected_tokens.append(token)
        
        # For options, find ATM strikes
        if options and len(selected_tokens) < target_count:
            strikes = []
            for token, data in options:
                strike = extract_strike_price(data.get('key', ''))
                if strike is not None:
                    strikes.append((token, data, strike))
            
            if strikes:
                # Sort by strike price
                strikes.sort(key=lambda x: x[2])
                
                # Find ATM range (middle strikes)
                mid_idx = len(strikes) // 2
                start_idx = max(0, mid_idx - atm_range)
                end_idx = min(len(strikes), mid_idx + atm_range + 1)
                
                # Add ATM ±2 strikes
                for token, data, strike in strikes[start_idx:end_idx]:
                    if len(selected_tokens) < target_count:
                        selected_tokens.append(token)
    
    return selected_tokens

def update_config(config, new_tokens, token_lookup):
    """Update the configuration with new tokens"""
    config["tokens"] = sorted(new_tokens)
    config["total_instruments"] = len(new_tokens)
    
    # Update expiry policy
    config["expiry_policy"] = {
        "rule": "December month expiry",
        "current_expiry": "2025-12-25",
        "days_to_expiry": 58,
        "included_patterns": [
            "25DECFUT",
            "25DEC"
        ]
    }
    
    # Update instrument breakdown
    config["instrument_breakdown"] = {
        "nifty50_stocks": {
            "description": "NIFTY 50 constituent stocks",
            "count": 50,
            "exchange": "NSE",
            "asset_class": "equity_cash"
        },
        "december_futures": {
            "description": "December futures (F&O)",
            "count": len([t for t in new_tokens if str(t) in token_lookup and 'FUT' in token_lookup[str(t)].get('instrument_type', '')]),
            "exchange": "NFO",
            "asset_class": "equity_futures"
        },
        "december_options": {
            "description": "December options ATM ±2",
            "count": len([t for t in new_tokens if str(t) in token_lookup and token_lookup[str(t)].get('instrument_type', '') in ['CE', 'PE']]),
            "exchange": "NFO",
            "asset_class": "equity_options"
        }
    }
    
    return config

def main():
    """Main function to update intraday crawler configuration"""
    print("🔄 Updating Intraday Crawler Configuration")
    print("=" * 50)
    
    # Load configurations
    config, token_lookup = load_configs()
    
    print(f"📊 Current configuration:")
    print(f"  Total instruments: {config['total_instruments']}")
    print(f"  Current tokens: {len(config['tokens'])}")
    
    # Get current tokens
    current_tokens = get_current_tokens(config)
    
    # Find October expiry tokens to remove (only from current config)
    oct_tokens = find_october_expiry_tokens_in_config(current_tokens, token_lookup)
    print(f"🗑️  Found {len(oct_tokens)} tokens expiring on 28OCT in current config to remove")
    
    # Find December instruments
    dec_instruments = find_december_instruments(token_lookup)
    print(f"📅 Found {len(dec_instruments)} December instruments available")
    
    # Group by underlying asset
    asset_groups = group_by_underlying(dec_instruments)
    print(f"📈 Grouped into {len(asset_groups)} underlying assets")
    
    # Select ATM ±2 instruments (equal to number of October tokens to remove)
    selected_dec_tokens = select_atm_instruments(asset_groups, len(oct_tokens), atm_range=2)
    print(f"🎯 Selected {len(selected_dec_tokens)} December instruments (ATM ±2)")
    
    # Create new token list - replace October with December
    new_tokens = current_tokens - set(oct_tokens)
    new_tokens.update(selected_dec_tokens)
    
    print(f"✅ New configuration:")
    print(f"  Removed October tokens: {len(oct_tokens)}")
    print(f"  Added December tokens: {len(selected_dec_tokens)}")
    print(f"  Total instruments: {len(new_tokens)}")
    
    # Update configuration
    updated_config = update_config(config, list(new_tokens), token_lookup)
    
    # Save updated configuration
    config_path = Path("crawlers/binary_crawler1/binary_crawler1.json")
    with open(config_path, 'w') as f:
        json.dump(updated_config, f, indent=2)
    
    print(f"💾 Configuration saved to {config_path}")
    
    # Show sample of selected instruments
    print("\n📋 Sample of selected December instruments:")
    sample_count = 0
    for token in sorted(selected_dec_tokens)[:20]:
        if str(token) in token_lookup:
            data = token_lookup[str(token)]
            print(f"  {token}: {data['key']} ({data['instrument_type']})")
            sample_count += 1
    
    if len(selected_dec_tokens) > 20:
        print(f"  ... and {len(selected_dec_tokens) - 20} more")

if __name__ == "__main__":
    main()
