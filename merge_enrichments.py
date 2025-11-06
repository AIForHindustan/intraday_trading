#!/usr/bin/env python3
"""
Merge backup token lookup file (Oct 29) with comprehensive metadata
and enrich with sector/classification data
"""

import json
from datetime import datetime

print("🔄 MERGING BACKUP WITH COMPREHENSIVE & ENRICHING")
print("=" * 70)

# Load backup file from Oct 29
backup_file = 'core/data/token_lookup_backup_20251029_130718.json'
print(f"📂 Loading backup file: {backup_file}")
with open(backup_file, 'r') as f:
    backup = json.load(f)

print(f"  Loaded {len(backup):,} tokens from backup")

# Load comprehensive file
print(f"\n📂 Loading comprehensive file...")
with open('zerodha_tokens_metadata_comprehensive.json', 'r') as f:
    comprehensive = json.load(f)

print(f"  Loaded {len(comprehensive):,} tokens from comprehensive")

# Create lookup maps for matching
# Backup uses 'key' field (can be string identifier or token), comprehensive uses 'token' as key
print(f"\n🔍 Creating lookup maps for matching...")
backup_token_map = {}  # token_val -> (backup_key, backup_data)
comprehensive_token_map = {}  # token -> comprehensive_data

# Map backup entries by token value for matching with comprehensive
for backup_key, backup_data in backup.items():
    token_val = backup_data.get('key')  # The token value or identifier is in 'key' field
    if token_val:
        token_str = str(token_val)
        backup_token_map[token_str] = (backup_key, backup_data)

for comp_key, comp_data in comprehensive.items():
    token_val = comp_data.get('token')
    if token_val:
        comprehensive_token_map[str(token_val)] = comp_data

print(f"  Backup entries indexed: {len(backup_token_map):,}")
print(f"  Comprehensive entries mapped: {len(comprehensive_token_map):,}")

# Merge: Start with backup structure, enrich with comprehensive data
# KEEP BACKUP ORIGINAL FORMAT - just add metadata fields
print(f"\n🔄 Merging data (keeping backup original format + metadata)...")
merged = {}
enrichments_added = 0
matched_count = 0

# Process all backup entries, keeping their ORIGINAL keys exactly as they are
for backup_key, backup_data in backup.items():
    merged_entry = backup_data.copy()  # Start with backup data
    
    # Try to match with comprehensive by token value
    token_val = backup_data.get('key')
    token_str = str(token_val) if token_val else None
    
    # Add metadata from comprehensive if available
    if token_str and token_str in comprehensive_token_map:
        comp_data = comprehensive_token_map[token_str]
        matched_count += 1
        
        # Add all enrichment fields from comprehensive
        enrichment_fields = ['sector', 'state', 'bond_type', 'maturity_year', 
                           'coupon_rate', 'asset_class', 'sub_category']
        
        for field in enrichment_fields:
            if field in comp_data:
                comp_value = comp_data[field]
                # Only override if current value is empty/Unknown/Missing
                current_value = merged_entry.get(field, '')
                if not current_value or current_value in ['Unknown', 'Missing', '']:
                    merged_entry[field] = comp_value
                    enrichments_added += 1
    
    # Keep original backup key exactly as it was (preserving format)
    merged[backup_key] = merged_entry

print(f"  Merged entries: {len(merged):,}")
print(f"  Enrichments added: {enrichments_added:,}")
print(f"  Tokens matched with comprehensive: {matched_count:,}")
print(f"  Total backup entries preserved: {len(merged):,}")

# Save merged file
output_file = 'zerodha_tokens_metadata_merged.json'
print(f"\n💾 Saving merged file: {output_file}")
with open(output_file, 'w') as f:
    json.dump(merged, f, indent=2)

print(f"✅ Merged file saved: {output_file}")

# Now run enrichment on the merged file
print(f"\n🔧 Running enrichment on merged file...")
print("=" * 70)

# Import enrichment functions (consolidated in parquet_ingester)
from parquet_ingester import (
    enrich_metadata_comprehensive,
    generate_final_statistics,
    save_enriched_metadata
)

# Enrich the merged data
enriched_merged = enrich_metadata_comprehensive(merged)

# Save enriched result
enriched_output = 'zerodha_tokens_metadata_merged_enriched.json'
save_enriched_metadata(enriched_merged, enriched_output)

# Generate statistics
generate_final_statistics(enriched_merged)

print(f"\n✅ MERGE & ENRICHMENT COMPLETE!")
print(f"📁 Merged file: {output_file}")
print(f"📁 Enriched file: {enriched_output}")
