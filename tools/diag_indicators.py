#!/usr/bin/env python3
"""
Diagnostic tool to pinpoint indicator visibility mismatches.

Usage:
    python tools/diag_indicators.py

Checks:
- IND_DB and IND_PREFIX configuration
- Key enumeration under canonical prefix
- JSON parsing and schema validation
- DB/prefix matches between writers and readers
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.data.indicators_store import (
    IND_DB,
    IND_PREFIX,
    list_indicators,
    get_indicator,
)


def main() -> int:
    print(f"[diag] IND_DB={IND_DB} IND_PREFIX='{IND_PREFIX}'")
    
    keys = list_indicators(pattern="*")
    print(f"[diag] Found {len(keys)} keys under prefix")
    
    if not keys:
        print("[hint] No keys under current prefix/DB. Check IND_DB/IND_PREFIX/env.")
        print(f"[hint] Tried: DB={IND_DB}, prefix='{IND_PREFIX}:*'")
        return 1
    
    sample = keys[:10]
    print("[diag] Sample keys:")
    for k in sample:
        print(f"  - {k}")
    
    # Try a few reads
    misses = 0
    hits = 0
    
    for k in sample:
        try:
            parts = k.split(":")
            if len(parts) >= 3:
                sym = parts[1]
                tf = parts[2]
            else:
                print(f"[warn] Unexpected key format: {k}")
                continue
        except ValueError:
            print(f"[warn] Unexpected key format: {k}")
            continue
        
        doc = get_indicator(sym, tf)
        if doc is None:
            print(f"[MISS] {sym}:{tf} -> None")
            misses += 1
        else:
            # Show only top-level fields to avoid noise
            fields = list(doc.keys())[:8]
            print(f"[HIT] {sym}:{tf} -> fields={fields}")
            hits += 1
    
    print(f"\n[diag] Summary: {hits} hits, {misses} misses")
    
    if misses > 0:
        print(f"[diag] {misses} misses. Likely legacy writers or wrong decode_responses.")
        print("[hint] Check:")
        print("  - Writers using correct DB and prefix?")
        print("  - Encoding: decode_responses=True for reads?")
        print("  - Schema: Hash (HSET) vs String (SET)?")
    
    if hits > 0:
        print(f"[diag] ✅ {hits} indicators visible - dashboard should work!")
    
    print("[diag] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

