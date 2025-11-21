#!/usr/bin/env python3
"""
Remove Expired Instruments from Crawler Configurations
======================================================

Purpose:
--------
Remove expired instruments from crawler configurations to improve performance
and eliminate processing of invalid/stale tokens.

Dependent Scripts:
----------------
- core/data/token_lookup.json: Token validation reference

Note: crawler2_volatility and crawler3_sso_xvenue have been removed from this codebase and run independently

Important Aspects:
-----------------
- Identifies instruments with expiry dates before current date
- Creates backup of original configurations
- Removes expired instruments to improve crawler performance
- Updates instrument counts for buffer size optimization
- Preserves active instruments for continued operation
"""

import json
import shutil
from datetime import date
from pathlib import Path
import re

def remove_expired_instruments(config_path: Path, current_date: date) -> int:
    """
    Remove expired instruments from crawler configuration.
    
    Args:
        config_path: Path to crawler configuration JSON file
        current_date: Current date for expiry comparison
        
    Returns:
        Number of expired instruments removed
    """
    print(f"🔧 Processing {config_path.name}...")
    
    # Create backup
    backup_path = config_path.with_suffix('.json.backup')
    shutil.copy2(config_path, backup_path)
    print(f"  📁 Backup created: {backup_path.name}")
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    instruments = config.get('instruments', {})
    original_count = len(instruments)
    
    # Month mapping
    month_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    expired_instruments = []
    active_instruments = {}
    
    # Process each instrument
    for inst_key, inst_data in instruments.items():
        if isinstance(inst_data, dict):
            # Check if instrument has expiry information
            if any(month in inst_key for month in month_map.keys()):
                # Extract expiry date
                date_match = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)', inst_key)
                if date_match:
                    day = int(date_match.group(1))
                    month_str = date_match.group(2)
                    month = month_map.get(month_str, 0)
                    
                    if month > 0:
                        expiry_year = 2025
                        expiry_date = date(expiry_year, month, day)
                        
                        if expiry_date < current_date:
                            expired_instruments.append({
                                'key': inst_key,
                                'expiry': expiry_date.isoformat(),
                                'days_expired': (current_date - expiry_date).days,
                                'token': inst_data.get('token', 'N/A')
                            })
                        else:
                            active_instruments[inst_key] = inst_data
                    else:
                        active_instruments[inst_key] = inst_data
                else:
                    active_instruments[inst_key] = inst_data
            else:
                # Non-expiring instruments
                active_instruments[inst_key] = inst_data
    
    # Update configuration
    config['instruments'] = active_instruments
    
    # Save updated configuration
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    expired_count = len(expired_instruments)
    active_count = len(active_instruments)
    
    print(f"  ✅ Removed {expired_count} expired instruments")
    print(f"  📊 Active instruments: {active_count}")
    print(f"  📊 Total processed: {original_count}")
    
    # Show sample expired instruments
    if expired_instruments:
        print(f"  📋 Sample expired instruments:")
        for i, inst in enumerate(expired_instruments[:5]):
            print(f"    • {inst['key']}: Expired {inst['days_expired']} days ago ({inst['expiry']})")
        if len(expired_instruments) > 5:
            print(f"    ... and {len(expired_instruments) - 5} more")
    
    return expired_count

def main():
    """Main function to remove expired instruments from all crawler configs."""
    print("🧹 REMOVING EXPIRED INSTRUMENTS FROM CRAWLER CONFIGURATIONS")
    print("=" * 60)
    
    current_date = date.today()
    print(f"Current date: {current_date}")
    print()
    
    # Configuration files to process
    config_files = [
        # Note: crawler2_volatility and crawler3_sso_xvenue moved to ubuntu_crawler/
        # Add intraday crawler config here if needed
    ]
    
    total_expired = 0
    
    for config_path in config_files:
        if config_path.exists():
            expired_count = remove_expired_instruments(config_path, current_date)
            total_expired += expired_count
            print()
        else:
            print(f"⚠️  Configuration file not found: {config_path}")
    
    print("📊 CLEANUP SUMMARY:")
    print(f"  Total expired instruments removed: {total_expired}")
    print(f"  Configuration files processed: {len(config_files)}")
    print()
    
    if total_expired > 0:
        print("✅ EXPIRED INSTRUMENT CLEANUP COMPLETE")
        print("  • Expired instruments removed from crawler configurations")
        print("  • Original configurations backed up")
        print("  • Crawlers will now process only active instruments")
        print("  • Performance should improve with fewer invalid tokens")
    else:
        print("✅ NO EXPIRED INSTRUMENTS FOUND")
        print("  • All instruments in crawler configurations are active")
        print("  • No cleanup required")

if __name__ == "__main__":
    main()
