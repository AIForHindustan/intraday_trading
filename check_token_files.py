#!/usr/bin/env python3
"""
Check and compare token lookup files to find the best one
"""

import json
from pathlib import Path
from collections import Counter

def analyze_token_file(file_path: Path):
    """Analyze a token file"""
    print(f"\n{'='*80}")
    print(f"Analyzing: {file_path}")
    print(f"{'='*80}")
    
    if not file_path.exists():
        print(f"❌ File not found")
        return None
    
    file_size_mb = file_path.stat().st_size / (1024**2)
    print(f"File size: {file_size_mb:.2f} MB")
    
    try:
        with open(file_path) as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error loading: {e}")
        return None
    
    stats = {
        'file': str(file_path),
        'size_mb': file_size_mb,
        'format': None,
        'token_count': 0,
        'structure': None,
    }
    
    if isinstance(data, dict):
        stats['format'] = 'dict'
        stats['token_count'] = len(data)
        
        # Check key types
        key_types = Counter(type(k).__name__ for k in list(data.keys())[:1000])
        print(f"Top-level keys: {len(data):,}")
        print(f"Key types (sample): {dict(key_types)}")
        
        # Check value structure
        if data:
            sample_val = list(data.values())[0]
            if isinstance(sample_val, dict):
                stats['structure'] = 'dict_of_dicts'
                print(f"Value structure: dict with keys: {list(sample_val.keys())[:10]}")
            else:
                stats['structure'] = type(sample_val).__name__
                print(f"Value structure: {stats['structure']}")
        
        # Count integer keys
        int_keys = sum(1 for k in data.keys() if isinstance(k, (str, int)) and str(k).isdigit())
        print(f"Integer token keys: {int_keys:,}")
        
    elif isinstance(data, list):
        stats['format'] = 'list'
        stats['token_count'] = len(data)
        print(f"Entries: {len(data):,}")
        
        if data and isinstance(data[0], dict):
            stats['structure'] = 'list_of_dicts'
            print(f"Structure: list of dicts with keys: {list(data[0].keys())[:10]}")
            
            # Check if tokens are in 'token' field
            tokens_with_token_field = sum(1 for item in data if 'token' in item)
            print(f"Items with 'token' field: {tokens_with_token_field:,}")
            
            # Sample tokens
            sample_tokens = [item.get('token') for item in data[:5] if 'token' in item]
            print(f"Sample tokens: {sample_tokens}")
    
    return stats

def main():
    """Compare all token files"""
    
    token_files = [
        Path('core/data/token_lookup_enriched.json'),
        Path('zerodha_token_list/zerodha_tokens_metadata_merged_enriched.json'),
        Path('zerodha_token_list/all_extracted_tokens_merged.json'),
        Path('zerodha_token_list/zerodha_tokens_metadata_comprehensive.json'),
    ]
    
    print("="*80)
    print("TOKEN FILE ANALYSIS")
    print("="*80)
    
    results = []
    for file_path in token_files:
        stats = analyze_token_file(file_path)
        if stats:
            results.append(stats)
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print(f"{'File':<60} {'Tokens':>12} {'Size (MB)':>10}")
    print("-"*80)
    
    for stats in sorted(results, key=lambda x: x['token_count'], reverse=True):
        file_name = Path(stats['file']).name
        print(f"{file_name:<60} {stats['token_count']:>12,} {stats['size_mb']:>10.2f}")
    
    # Recommendation
    if results:
        best = max(results, key=lambda x: x['token_count'])
        print(f"\n✅ Best file: {best['file']}")
        print(f"   Tokens: {best['token_count']:,}")
        print(f"   Format: {best['format']}")
        print(f"   Structure: {best['structure']}")

if __name__ == "__main__":
    main()

