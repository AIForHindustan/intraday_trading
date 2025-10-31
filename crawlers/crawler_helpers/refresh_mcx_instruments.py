#!/usr/bin/env python3
"""
MCX Commodity Instrument Refresh Tool
=====================================

Purpose:
--------
Refresh expired MCX commodity instruments with upcoming months instead of removing them.
Updates MCX instruments to the next 2 months (NOV 2025, DEC 2025).

Dependent Scripts:
----------------
- crawlers/crawler2_volatility/crawler2_volatility.json: Data mining crawler config
- core/data/token_lookup.json: Token validation and lookup reference

Important Aspects:
-----------------
- Identifies expired MCX commodity instruments
- Finds corresponding instruments for next 2 months
- Updates crawler configuration with new tokens
- Preserves non-MCX instruments unchanged
- Creates backup before making changes
"""

import json
import shutil
from datetime import date, timedelta
from pathlib import Path
import re

def get_next_months(current_date: date, count: int = 2):
    """Get the next N months from current date."""
    months = []
    next_month = current_date.replace(day=1) + timedelta(days=32)
    next_month = next_month.replace(day=1)
    
    month_names = {
        1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
        7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DEC'
    }
    
    for i in range(count):
        month_date = next_month.replace(month=((next_month.month - 1 + i) % 12) + 1)
        if i > 0 and month_date.month < next_month.month:
            month_date = month_date.replace(year=month_date.year + 1)
        
        months.append((month_date.month, month_names[month_date.month]))
    
    return months

def find_mcx_instruments(token_lookup: dict, commodity_name: str, target_months: list):
    """Find MCX instruments for a commodity in target months."""
    found_instruments = {}
    
    for token_str, info in token_lookup.items():
        if isinstance(info, dict):
            key = info.get('key', '')
            if f'MCX:{commodity_name}' in key and 'FUT' in key:
                # Extract month from key
                month_match = re.search(r'(\d{2})([A-Z]{3})', key)
                if month_match:
                    day = month_match.group(1)
                    month_name = month_match.group(2)
                    
                    # Check if this month is in our target months
                    for month_num, target_month in target_months:
                        if month_name == target_month:
                            found_instruments[f"{day}{month_name}"] = {
                                'token': token_str,
                                'key': key,
                                'info': info
                            }
                            break
    
    return found_instruments

def refresh_mcx_instruments(config_path: Path, token_lookup: dict, current_date: date) -> int:
    """Refresh MCX instruments in crawler configuration."""
    print(f"🔄 Refreshing MCX instruments in {config_path.name}...")
    
    # Create backup
    backup_path = config_path.with_suffix('.json.backup')
    shutil.copy2(config_path, backup_path)
    print(f"  📁 Backup created: {backup_path.name}")
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    instruments = config.get('instruments', {})
    
    # Get next 2 months
    target_months = get_next_months(current_date, 2)
    print(f"  🎯 Target months: {[f'{month_name} 2025' for _, month_name in target_months]}")
    
    # Find expired MCX instruments
    expired_mcx = {}
    for inst_key, inst_data in instruments.items():
        if 'MCX:' in inst_key and isinstance(inst_data, dict):
            # Extract commodity name
            commodity_match = re.search(r'MCX:([A-Z]+)', inst_key)
            if commodity_match:
                commodity_name = commodity_match.group(1)
                if commodity_name not in expired_mcx:
                    expired_mcx[commodity_name] = []
                expired_mcx[commodity_name].append((inst_key, inst_data))
    
    print(f"  📊 Found {len(expired_mcx)} MCX commodity types to refresh")
    
    refreshed_count = 0
    new_instruments = {}
    
    # Process each commodity
    for commodity_name, expired_instruments in expired_mcx.items():
        print(f"  🔄 Refreshing {commodity_name} ({len(expired_instruments)} instruments)...")
        
        # Find new instruments for this commodity
        new_mcx_instruments = find_mcx_instruments(token_lookup, commodity_name, target_months)
        
        if new_mcx_instruments:
            print(f"    ✅ Found {len(new_mcx_instruments)} new instruments")
            
            # Add new instruments
            for month_key, instrument_data in new_mcx_instruments.items():
                new_key = instrument_data['key']
                new_token = instrument_data['token']
                
                new_instruments[new_key] = {
                    'token': new_token,
                    'exchange': 'MCX',
                    'instrument_type': 'FUT'
                }
                refreshed_count += 1
        else:
            print(f"    ⚠️  No new instruments found for {commodity_name}")
    
    # Remove expired MCX instruments and add new ones
    updated_instruments = {}
    for inst_key, inst_data in instruments.items():
        if 'MCX:' not in inst_key:
            updated_instruments[inst_key] = inst_data
    
    # Add new MCX instruments
    updated_instruments.update(new_instruments)
    
    # Update configuration
    config['instruments'] = updated_instruments
    
    # Save updated configuration
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    original_count = len(instruments)
    new_count = len(updated_instruments)
    
    print(f"  ✅ Refreshed {refreshed_count} MCX instruments")
    print(f"  📊 Original instruments: {original_count}")
    print(f"  📊 Updated instruments: {new_count}")
    print(f"  📊 Net change: {new_count - original_count}")
    
    return refreshed_count

def main():
    """Main function to refresh MCX instruments."""
    print("🔄 MCX COMMODITY INSTRUMENT REFRESH")
    print("=" * 50)
    
    current_date = date.today()
    print(f"Current date: {current_date}")
    
    # Load token lookup
    token_lookup_path = Path("core/data/token_lookup.json")
    with open(token_lookup_path, 'r') as f:
        token_lookup = json.load(f)
    
    print(f"Token lookup loaded: {len(token_lookup)} tokens")
    
    # Configuration files to process
    config_files = [
        Path("crawlers/crawler2_volatility/crawler2_volatility.json"),
    ]
    
    total_refreshed = 0
    
    for config_path in config_files:
        if config_path.exists():
            refreshed_count = refresh_mcx_instruments(config_path, token_lookup, current_date)
            total_refreshed += refreshed_count
            print()
        else:
            print(f"⚠️  Configuration file not found: {config_path}")
    
    print("📊 REFRESH SUMMARY:")
    print(f"  Total MCX instruments refreshed: {total_refreshed}")
    print(f"  Configuration files processed: {len(config_files)}")
    print()
    
    if total_refreshed > 0:
        print("✅ MCX INSTRUMENT REFRESH COMPLETE")
        print("  • Expired MCX instruments replaced with upcoming months")
        print("  • Original configurations backed up")
        print("  • Crawlers will now process active MCX instruments")
        print("  • MCX commodity trading ready for next 2 months")
    else:
        print("✅ NO MCX INSTRUMENTS TO REFRESH")
        print("  • All MCX instruments are already current")
        print("  • No refresh required")

if __name__ == "__main__":
    main()
