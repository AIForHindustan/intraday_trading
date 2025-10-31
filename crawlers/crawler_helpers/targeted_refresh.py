#!/usr/bin/env python3
"""
Targeted MCX/CDS Instrument Refresh Tool
========================================

Purpose:
--------
Refresh only the specific expired MCX commodity and CDS currency instruments
with their corresponding NOV 2025 and DEC 2025 contracts.

Dependent Scripts:
----------------
- crawlers/crawler2_volatility/crawler2_volatility.json: Data mining crawler config
- core/data/token_lookup.json: Token validation and lookup reference

Important Aspects:
-----------------
- Only refreshes specific expired instruments (not all MCX/CDS)
- Maps expired instruments to their NOV/DEC 2025 equivalents
- Preserves all other instruments unchanged
- Creates backup before making changes
"""

import json
import shutil
from pathlib import Path
import re

def refresh_specific_instruments(config_path: Path, token_lookup: dict) -> int:
    """Refresh specific expired MCX and CDS instruments."""
    print(f"🔄 Refreshing specific expired instruments in {config_path.name}...")
    
    # Create backup
    backup_path = config_path.with_suffix('.json.backup')
    shutil.copy2(config_path, backup_path)
    print(f"  📁 Backup created: {backup_path.name}")
    
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    instruments = config.get('instruments', {})
    
    # Define specific expired instruments and their replacements
    expired_replacements = {
        # MCX Commodities - Map expired to NOV/DEC 2025
        'MCX:ALUMINIUM26JANFUT': 'MCX:ALUMINIUM25NOVFUT',
        'MCX:ALUMINIUM26FEBFUT': 'MCX:ALUMINIUM25DECFUT',
        'MCX:COPPER26JANFUT': 'MCX:COPPER25NOVFUT',
        'MCX:COPPER26FEBFUT': 'MCX:COPPER25DECFUT',
        'MCX:CRUDEOIL26JANFUT': 'MCX:CRUDEOIL25NOVFUT',
        'MCX:CRUDEOIL26FEBFUT': 'MCX:CRUDEOIL25DECFUT',
        'MCX:CRUDEOIL26MARFUT': 'MCX:CRUDEOIL25NOVFUT',
        'MCX:CRUDEOILM26JANFUT': 'MCX:CRUDEOILM25NOVFUT',
        'MCX:CRUDEOILM26FEBFUT': 'MCX:CRUDEOILM25DECFUT',
        'MCX:CRUDEOILM26MARFUT': 'MCX:CRUDEOILM25NOVFUT',
        'MCX:GOLD26FEBFUT': 'MCX:GOLD25NOVFUT',
        'MCX:GOLD26APRFUT': 'MCX:GOLD25DECFUT',
        'MCX:GOLD26JUNFUT': 'MCX:GOLD25NOVFUT',
        'MCX:GOLD26AUGFUT': 'MCX:GOLD25DECFUT',
        'MCX:GOLDGUINEA26JANFUT': 'MCX:GOLDGUINEA25NOVFUT',
        'MCX:GOLDGUINEA26FEBFUT': 'MCX:GOLDGUINEA25DECFUT',
        'MCX:GOLDGUINEA26MARFUT': 'MCX:GOLDGUINEA25NOVFUT',
        'MCX:GOLDM26JANFUT': 'MCX:GOLDM25NOVFUT',
        'MCX:GOLDM26FEBFUT': 'MCX:GOLDM25DECFUT',
        'MCX:GOLDM26MARFUT': 'MCX:GOLDM25NOVFUT',
        'MCX:GOLDM26APRFUT': 'MCX:GOLDM25DECFUT',
        'MCX:GOLDPETAL26JANFUT': 'MCX:GOLDPETAL25NOVFUT',
        'MCX:GOLDPETAL26FEBFUT': 'MCX:GOLDPETAL25DECFUT',
        'MCX:GOLDPETAL26MARFUT': 'MCX:GOLDPETAL25NOVFUT',
        
        # CDS Currencies - Map expired to NOV/DEC 2025
        'CDS:EURINR26JANFUT': 'CDS:EURINR25NOVFUT',
        'CDS:EURINR26FEBFUT': 'CDS:EURINR25DECFUT',
        'CDS:EURINR26MARFUT': 'CDS:EURINR25NOVFUT',
        'CDS:EURINR26APRFUT': 'CDS:EURINR25DECFUT',
        'CDS:EURINR26MAYFUT': 'CDS:EURINR25NOVFUT',
        'CDS:EURINR26JUNFUT': 'CDS:EURINR25DECFUT',
        'CDS:EURINR26JULFUT': 'CDS:EURINR25NOVFUT',
        'CDS:EURINR26AUGFUT': 'CDS:EURINR25DECFUT',
        'CDS:EURINR26SEPFUT': 'CDS:EURINR25NOVFUT',
    }
    
    print(f"  🎯 Target: Replace {len(expired_replacements)} expired instruments")
    
    # Find tokens for replacement instruments
    replacement_tokens = {}
    for expired_key, new_key in expired_replacements.items():
        # Find token for the new instrument
        for token_str, info in token_lookup.items():
            if isinstance(info, dict):
                if info.get('key') == new_key:
                    replacement_tokens[expired_key] = {
                        'new_key': new_key,
                        'new_token': token_str,
                        'info': info
                    }
                    break
    
    print(f"  ✅ Found {len(replacement_tokens)} replacement instruments")
    
    # Update instruments
    updated_instruments = {}
    refreshed_count = 0
    
    for inst_key, inst_data in instruments.items():
        if inst_key in expired_replacements:
            # Replace expired instrument
            if inst_key in replacement_tokens:
                new_data = replacement_tokens[inst_key]
                updated_instruments[new_data['new_key']] = {
                    'token': new_data['new_token'],
                    'exchange': new_data['info'].get('exchange', ''),
                    'instrument_type': new_data['info'].get('instrument_type', 'FUT')
                }
                refreshed_count += 1
                print(f"    🔄 {inst_key} → {new_data['new_key']}")
            else:
                print(f"    ⚠️  No replacement found for {inst_key}")
        else:
            # Keep existing instrument
            updated_instruments[inst_key] = inst_data
    
    # Update configuration
    config['instruments'] = updated_instruments
    
    # Save updated configuration
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    original_count = len(instruments)
    new_count = len(updated_instruments)
    
    print(f"  ✅ Refreshed {refreshed_count} instruments")
    print(f"  📊 Original instruments: {original_count}")
    print(f"  📊 Updated instruments: {new_count}")
    print(f"  📊 Net change: {new_count - original_count}")
    
    return refreshed_count

def main():
    """Main function to refresh specific expired instruments."""
    print("🔄 TARGETED INSTRUMENT REFRESH")
    print("=" * 40)
    
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
            refreshed_count = refresh_specific_instruments(config_path, token_lookup)
            total_refreshed += refreshed_count
            print()
        else:
            print(f"⚠️  Configuration file not found: {config_path}")
    
    print("📊 REFRESH SUMMARY:")
    print(f"  Total instruments refreshed: {total_refreshed}")
    print(f"  Configuration files processed: {len(config_files)}")
    print()
    
    if total_refreshed > 0:
        print("✅ TARGETED REFRESH COMPLETE")
        print("  • Specific expired MCX/CDS instruments replaced")
        print("  • Original configurations backed up")
        print("  • Crawlers will now process active instruments")
        print("  • Commodity and currency trading ready for NOV/DEC 2025")
    else:
        print("✅ NO INSTRUMENTS TO REFRESH")
        print("  • All instruments are already current")
        print("  • No refresh required")

if __name__ == "__main__":
    main()
