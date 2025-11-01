"""
Canonical, typed Redis access for indicators.

Fixes dashboard visibility issues caused by:
- DB drift: writers using different DBs than dashboard expects
- Prefix drift: inconsistent key naming (indicator: vs indicators:)
- Encoding drift: bytes vs str/JSON parsing mismatches
- Schema drift: SET key=JSON vs HSET key data=JSON

Usage:
    from core.data.indicators_store import put_indicator, get_indicator, list_indicators
    
    # Write
    put_indicator("RELIANCE", "5m", {"rsi": 65.5, "macd": 1.2})
    
    # Read
    data = get_indicator("RELIANCE", "5m")
    
    # List all
    keys = list_indicators(pattern="*")
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from redis_files.redis_client import get_redis_client

# Environment-configurable defaults
# Note: Scanner writes to DB 1 with prefix "indicators" (plural)
IND_DB = int(os.getenv("IND_DB", os.getenv("REDIS_DB_INDICATORS", "1")))  # default DB 1 (realtime) for indicators
IND_PREFIX = os.getenv("IND_PREFIX", "indicators")                         # keyspace prefix (plural to match scanner)
IND_TTL_SEC = int(os.getenv("IND_TTL_SEC", "0"))                          # 0 = no TTL


def _key(symbol: str, timeframe: str = None) -> str:
    """
    Single source of truth for dashboard + writers.
    Note: Existing scanner uses format 'indicators:SYMBOL:INDICATOR_NAME' (no timeframe).
    For backward compatibility, timeframe is optional.
    """
    sym = symbol.upper().strip()
    if timeframe:
        tf = timeframe.lower().strip()
        return f"{IND_PREFIX}:{sym}:{tf}"
    else:
        # Legacy format without timeframe: indicators:SYMBOL (for aggregate indicator hash)
        return f"{IND_PREFIX}:{sym}"


def _coerce_json(value: Any) -> Optional[Dict[str, Any]]:
    """
    Accept legacy bytes/str/dict. Avoids dashboard blind spots due to encoding drift.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return json.loads(value)
        except Exception:
            # Legacy Python-literal dicts (not recommended)
            try:
                import ast
                obj = ast.literal_eval(value)
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None
    return None  # unsupported type


def put_indicator(symbol: str, timeframe: str, payload: Dict[str, Any]) -> None:
    """
    Store indicator snapshot as JSON in a single Hash 'data' field + metadata fields.
    Why Hash: allows partial reads/fields later; dashboard reads 'data' key.
    """
    key = _key(symbol, timeframe)
    now = int(time.time())
    r = get_redis_client(db=IND_DB, decode_responses=True)
    
    # Atomic write
    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "data": json.dumps(payload, separators=(",", ":")),
        "updated_ts": str(now),
        "symbol": symbol.upper(),
        "timeframe": timeframe.lower(),
        "schema": "v1",
    })
    if IND_TTL_SEC > 0:
        pipe.expire(key, IND_TTL_SEC)
    pipe.execute()


def get_indicator(symbol: str, timeframe: str = None, indicator_name: str = None) -> Optional[Dict[str, Any]]:
    """
    Get indicator data with backward compatibility for legacy formats.
    
    Scanner stores individual indicators as: indicators:SYMBOL:INDICATOR_NAME (e.g., indicators:RELIANCE:rsi)
    This function aggregates all indicators for a symbol, or returns a specific one if indicator_name is provided.
    """
    r = get_redis_client(db=IND_DB, decode_responses=True)
    sym = symbol.upper().strip()
    
    # If specific indicator requested, return just that one
    if indicator_name:
        key = f"{IND_PREFIX}:{sym}:{indicator_name.lower()}"
        if r.exists(key):
            try:
                raw = r.get(key)
                data = _coerce_json(raw)
                if data and isinstance(data, dict) and 'value' in data:
                    # Scanner format: {"value": 65.5, "timestamp": ...}
                    return data.get('value')
                return data
            except Exception:
                pass
        return None
    
    # Aggregate all indicators for the symbol
    # Scanner format: indicators:SYMBOL:INDICATOR_NAME
    pattern = f"{IND_PREFIX}:{sym}:*"
    cursor = 0
    all_indicators = {}
    
    try:
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
            for key in keys:
                try:
                    # Extract indicator name from key
                    parts = key.split(":")
                    if len(parts) >= 3:
                        ind_name = parts[-1]  # e.g., "rsi" from "indicators:RELIANCE:rsi"
                        
                        # Get value
                        raw = r.get(key)
                        data = _coerce_json(raw)
                        if data:
                            if isinstance(data, dict) and 'value' in data:
                                # Scanner format: {"value": 65.5, ...}
                                all_indicators[ind_name] = data.get('value')
                            elif isinstance(data, (int, float)):
                                all_indicators[ind_name] = data
                            else:
                                all_indicators[ind_name] = data
                except Exception:
                    continue
            if cursor == 0:
                break
    except Exception as e:
        # Fallback: try individual indicator keys
        pass
    
    return all_indicators if all_indicators else None


def list_indicators(pattern: str = "*") -> List[str]:
    """
    List keys under the canonical prefix; compatible with RESP3 SCAN returns.
    """
    r = get_redis_client(db=IND_DB, decode_responses=True)
    cursor = 0
    keys: List[str] = []
    match = f"{IND_PREFIX}:{pattern}"
    
    while True:
        cursor, batch = r.scan(cursor=cursor, match=match, count=1000)
        keys.extend(batch)
        if cursor == 0:
            break
    
    return keys


def mget_indicators(pairs: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], Optional[Dict[str, Any]]]:
    """Batch get multiple indicators"""
    out: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
    for sym, tf in pairs:
        out[(sym, tf)] = get_indicator(sym, tf)
    return out

