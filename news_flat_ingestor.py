from __future__ import annotations

import json, time, re, hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Callable, Tuple

# ---- helpers ----

def _ts_ms_from_iso(s: str) -> Optional[int]:
    try:
        # Handle "2025-10-30T15:07:36.514238" (naive -> UTC)
        dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp()*1000)
    except Exception:
        return None

_REL_PAT = re.compile(r"^\s*(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago\s*$", re.I)
def _ts_ms_from_relative(s: str, now_ms: Optional[int]=None) -> Optional[int]:
    m = _REL_PAT.match(s or "")
    if not m: return None
    val, unit = int(m.group(1)), m.group(2).lower()
    now = now_ms or int(time.time()*1000)
    mult = {"second":1, "minute":60, "hour":3600, "day":86400, "week":604800, "month":2592000, "year":31536000}[unit]
    return now - val*mult*1000

def _coerce_ts_ms(date_str: Optional[str], collected_iso: Optional[str]) -> int:
    if collected_iso:
        ts = _ts_ms_from_iso(collected_iso)
        if ts: return ts
    if date_str:
        # try iso then relative
        ts = _ts_ms_from_iso(date_str) or _ts_ms_from_relative(date_str)
        if ts: return ts
    return int(time.time()*1000)

def _hash_dedupe(fields: Dict[str, Any]) -> str:
    # Stable minimal identity to avoid dupes (source+headline+url+ts bucket)
    h = hashlib.sha256()
    h.update((fields.get("news_source","") or "").encode())
    h.update((fields.get("headline","") or "").encode())
    h.update((fields.get("url","") or "").encode())
    h.update(str(int(fields.get("ts",0)//(5*60*1000))).encode())  # 5m bucket
    return h.hexdigest()[:32]

# ---- ingestor ----

class NewsIngestorFlat:
    """
    Flat-JSON news ingestor:
      - Reads string JSON values under keys like news:item:* / news:latest:*
      - Maps fields via field_map to standard schema
      - XADD to news:{symbol} and news:global (bounded)
      - Dedupe via a Redis SET
    """
    def __init__(
        self,
        redis_client: Any,
        news_db: int = 1,
        per_symbol_maxlen: int = 2000,
        global_maxlen: int = 10000,
        field_map: Optional[Dict[str,str]] = None,
        symbol_resolver: Optional[Callable[[Dict[str,Any]], List[str]]] = None,
        dedupe_key: str = "news:ingested:hashes",
    ):
        self._root = redis_client
        self._r = redis_client.get_client(news_db) if hasattr(redis_client, "get_client") else redis_client
        self._per_symbol_maxlen = int(per_symbol_maxlen)
        self._global_maxlen = int(global_maxlen)
        # Defaults aligned to your sample
        self._map = field_map or {
            "source": "news_source",
            "title": "headline", 
            "link": "url",
            "date": "published_time",
            "publisher": "source_name",
            "collected_at": "ingestion_timestamp",
            "sentiment": "sentiment_score",
        }
        self._resolve_symbols = symbol_resolver or (lambda item: [])
        self._dedupe_key = dedupe_key

    # ---- mapping for one item ----
    def _map_fields(self, item: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for src_key, dst_key in self._map.items():
            if src_key in item and item[src_key] not in (None, ""):
                out[dst_key] = item[src_key]
        # timestamps
        ts = _coerce_ts_ms(
            item.get("date") or item.get("published_time"),
            item.get("collected_at") or item.get("ingestion_timestamp"),
        )
        out["ts"] = ts
        # standard fields for streams
        if "headline" not in out and "title" in item:
            out["headline"] = item["title"]
        if "news_source" not in out and "source" in item:
            out["news_source"] = item["source"]
        # sentiment normalization (optional)
        if "sentiment_score" in out:
            try:
                # allow strings like "positive"/"negative"
                s = out["sentiment_score"]
                if isinstance(s, str):
                    mapping = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
                    out["sentiment_score"] = mapping.get(s.lower(), float(s))
                else:
                    out["sentiment_score"] = float(s)
            except Exception:
                pass
        return out

    # ---- ingest one redis key ----
    def ingest_key(self, key: str) -> Optional[Tuple[str,str]]:
        raw = self._r.get(key)
        if not raw: return None
        try:
            item = json.loads(raw)
        except Exception:
            return None
        fields = self._map_fields(item)
        if not fields.get("headline"):  # must have a title/headline
            return None
        # keep raw
        try:
            fields["raw"] = json.dumps(item, ensure_ascii=False, separators=(",",":"), default=str)
        except Exception:
            pass
        # dedupe
        sig = _hash_dedupe(fields)
        if self._r.sismember(self._dedupe_key, sig):
            return None
        # symbol routing
        syms = self._resolve_symbols(item) or []
        sym_ids = []
        # write streams
        if syms:
            for sym in syms:
                skey = f"news:{sym}"
                sym_id = self._r.xadd(skey, {"symbol": sym, **fields}, maxlen=self._per_symbol_maxlen, approximate=True)
                sym_ids.append(sym_id)
        gid = self._r.xadd("news:global", fields, maxlen=self._global_maxlen, approximate=True)
        self._r.sadd(self._dedupe_key, sig)
        return (sym_ids[0] if sym_ids else None, gid)

    # ---- SCAN loop (non-blocking) ----
    def scan_and_ingest(self, patterns: List[str], count: int = 500) -> int:
        total = 0
        for pattern in patterns:
            cur = 0
            while True:
                cur, keys = self._r.scan(cursor=cur, match=pattern, count=count)
                for k in keys:
                    try:
                        if self.ingest_key(k): total += 1
                    except Exception:
                        pass
                if cur == 0: break
        return total

# ---- alert side enrichment ----

class NewsEnricher:
    def __init__(self, redis_client: Any, news_db: int = 1):
        self._r = redis_client.get_client(news_db) if hasattr(redis_client, "get_client") else redis_client

    @staticmethod
    def _ms_to_xid(ms: int) -> str: return f"{ms}-0"

    def fetch_recent_news(self, symbol: str, since_ms: int, until_ms: Optional[int]=None, limit: int = 3) -> List[Dict[str, Any]]:
        start = self._ms_to_xid(since_ms); end = "+" if until_ms is None else self._ms_to_xid(until_ms)
        rows = self._r.xrange(f"news:{symbol}", min=start, max=end, count=limit*4 or None)
        out: List[Dict[str, Any]] = []
        for _id, data in reversed(rows):
            # Handle sentiment score conversion safely
            sentiment_score = None
            if "sentiment_score" in data:
                try:
                    s = data["sentiment_score"]
                    if isinstance(s, str):
                        mapping = {"positive": 1.0, "neutral": 0.0, "negative": -1.0}
                        sentiment_score = mapping.get(s.lower(), float(s))
                    else:
                        sentiment_score = float(s)
                except (ValueError, TypeError):
                    sentiment_score = None
            
            out.append({
                "id": _id,
                "ts": int(data.get("ts")) if "ts" in data else None,
                "headline": data.get("headline"),
                "news_source": data.get("news_source"),
                "url": data.get("url"),
                "sentiment_score": sentiment_score,
            })
            if len(out) >= limit: break
        return out

    def enrich_alert(self, alert: Dict[str, Any], symbol: str, lookback_minutes: int = 30, top_k: int = 3) -> Dict[str, Any]:
        now_ms = int(time.time()*1000); since = now_ms - lookback_minutes*60*1000
        items = self.fetch_recent_news(symbol, since, now_ms, top_k)
        if items:
            alert.setdefault("news", items)
            alert.setdefault("top_headline", items[0].get("headline"))
        return alert

# ---- example usage ----

"""
# 0) field mapping config (based on your sample)
field_map = {
  "source": "news_source",
  "title": "headline",
  "link": "url", 
  "date": "published_time",
  "publisher": "source_name",
  "collected_at": "ingestion_timestamp",
  "sentiment": "sentiment_score",
}

# 1) optional: a symbol resolver (map publisher/article -> symbols). For now empty → global only.
def resolve_symbols(item: dict) -> list[str]:
    # e.g., use a dict: {"Canara Bank": ["CANBK", "CANBK-FUT"]} matched from title/publisher
    t = (item.get("title") or "").lower()
    syms = []
    if "canara bank" in t: syms += ["CANBK"]
    return syms

# 2) init
ing = NewsIngestorFlat(redis_client, news_db=1, field_map=field_map, symbol_resolver=resolve_symbols)

# 3) offline/cron bridge from key-value store -> streams
ing.scan_and_ingest(["news:item:*", "news:latest:*"])

# 4) enrich an alert before publishing
enr = NewsEnricher(redis_client, news_db=1)
alert = {..., "symbol": "CANBK"}
alert = enr.enrich_alert(alert, symbol=alert["symbol"], lookback_minutes=60, top_k=3)
"""
