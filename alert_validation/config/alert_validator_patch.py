
# file: alert_validator_patch.py
"""
Patch for AlertValidator:
- Resolve price symbol (option -> underlying) for forward validation.
- Add robust price fallbacks to avoid infinite retries.
- Ensure bucket metrics path runs; add INFO logs for visibility.
Usage:
  import alert_validator_patch  # after importing alert_validator
"""
from __future__ import annotations
import json
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
import pytz

try:
    from alert_validator import AlertValidator  # noqa
except Exception as exc:  # pragma: no cover
    raise

def _resolve_price_symbol(self: 'AlertValidator', symbol: str) -> str:
    """Map option symbols to underlying. Fallback: original symbol."""
    try:
        # Prefer centralized redis helper if exists
        if hasattr(self, "redis") and hasattr(self.redis, "map_to_underlying"):
            base = self.redis.map_to_underlying(symbol) or symbol
            return base
    except Exception:
        pass
    try:
        # Config-driven mapping
        mapping = (self.config or {}).get("symbol_mapping", {})
        return mapping.get(symbol, symbol)
    except Exception:
        return symbol

def _get_current_price(self: 'AlertValidator', symbol: str) -> Optional[float]:
    """Get best-effort current price with multiple fallbacks."""
    base = _resolve_price_symbol(self, symbol)
    # 1) direct last price
    price = None
    try:
        price = self.redis.get_last_price(base)
    except Exception:
        price = None
    if price is not None:
        return float(price)

    # 2) last candle close
    try:
        candle = getattr(self.redis, "get_last_candle", None)
        if callable(candle):
            c = candle(base)
            if c and isinstance(c, dict):
                for key in ("close", "last_price", "price"):
                    if key in c and c[key] is not None:
                        return float(c[key])
    except Exception:
        pass

    # 3) last trade tick
    try:
        tick = getattr(self.redis, "get_last_trade", None)
        if callable(tick):
            t = tick(base)
            if t and isinstance(t, dict) and t.get("price") is not None:
                return float(t["price"])
    except Exception:
        pass

    return None

def _evaluate_forward_window(self: 'AlertValidator', state: Dict[str, Any], window_minutes: int):
    """Wrapper: use robust price getter; avoid indefinite None result."""
    symbol = state.get("symbol", "UNKNOWN")
    entry_price = float(state.get("entry_price") or 0.0)
    direction = int(state.get("direction") or 0)

    if entry_price <= 0:
        return {
            "status": "INCONCLUSIVE",
            "reason": "INVALID_ENTRY_PRICE",
            "window_minutes": window_minutes,
        }

    current_price = _get_current_price(self, symbol)
    if current_price is None:
        # Don't force endless retries; mark inconclusive immediately
        return {
            "status": "INCONCLUSIVE",
            "reason": "PRICE_UNAVAILABLE",
            "window_minutes": window_minutes,
        }

    # Defer to original logic by calling the unpatched core if available
    # We reconstruct minimal expected context: price change vs expected
    # Find movement after window by sampling last price now; this is a quick fix.
    # If original method exists under a different name, call it.
    orig = getattr(self, "_evaluate_forward_window__orig", None)
    if callable(orig):
        # Temporarily monkey-patch redis.get_last_price to return our resolved price for this call
        class _ProxyRedis:
            def __init__(self, r, base_symbol, current_price):
                self._r = r; self._base = base_symbol; self._price = current_price
            def __getattr__(self, k): return getattr(self._r, k)
            def get_last_price(self, s):  # ensure consistent for the evaluated symbol
                if s == self._base: return self._price
                try: return self._r.get_last_price(s)
                except Exception: return None
        saved = self.redis
        try:
            self.redis = _ProxyRedis(self.redis, _resolve_price_symbol(self, symbol), current_price)
            return orig(state, window_minutes)
        finally:
            self.redis = saved

    # Fallback: simple threshold evaluation (keeps compatibility)
    move = (current_price - entry_price) / max(entry_price, 1e-9)
    target = float(self.forward_config.get("expected_move", 0.002))  # 0.2% default
    status = "SUCCESS" if (direction >= 0 and move >= target) or (direction < 0 and -move >= target) else "FAILURE"
    return {
        "status": status,
        "reason": "BASIC_THRESHOLD",
        "window_minutes": window_minutes,
        "entry_price": entry_price,
        "current_price": current_price,
        "move": move,
        "target": target,
        "evaluated_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
    }

def _get_rolling_metrics(self: 'AlertValidator', symbol: str, windows: List[int]) -> Dict:
    """Wrapper: force bucket path when configured; add INFO logs for visibility."""
    force_buckets = bool((self.config or {}).get("validation", {}).get("force_bucket_metrics", False))
    metrics = {}
    for window in windows:
        used = "cache"
        data = None
        if not force_buckets:
            try:
                key = f"metrics:{symbol}:{window}min"
                store_client = self.rolling_store_client or self.redis_client
                data = store_client.get(key) if store_client else None
                if data:
                    metrics[window] = json.loads(data)
            except Exception:
                data = None
        if not data:
            used = "buckets"
            metrics[window] = self._calculate_from_volume_buckets(symbol, window)
        # Log once per window at INFO for debugging
        try:
            self.logger.info(f"[metrics] {symbol} {window}min via {used} -> keys={list(metrics[window].keys()) if isinstance(metrics[window], dict) else 'n/a'}")
        except Exception:
            pass
    return metrics

def _calculate_from_volume_buckets(self: 'AlertValidator', symbol: str, window_minutes: int):
    """Wrapper: add INFO entry/exit logs to confirm invocation."""
    try:
        self.logger.info(f"↗️  computing bucket metrics for {symbol} over {window_minutes}m")
    except Exception:
        pass
    return self.__class__._calculate_from_volume_buckets.__wrapped__(self, symbol, window_minutes)  # type: ignore

# --- apply monkey patches ---
# keep original for delegation
if not hasattr(AlertValidator, "_evaluate_forward_window__orig"):
    AlertValidator._evaluate_forward_window__orig = AlertValidator._evaluate_forward_window
AlertValidator._evaluate_forward_window = _evaluate_forward_window
AlertValidator.get_rolling_metrics = _get_rolling_metrics

# Wrap _calculate_from_volume_buckets to add logs only if method exists
import functools
if hasattr(AlertValidator, "_calculate_from_volume_buckets"):
    original = getattr(AlertValidator, "_calculate_from_volume_buckets")
    if not hasattr(original, "__wrapped__"):
        setattr(AlertValidator, "_calculate_from_volume_buckets",
                functools.wraps(original)(_calculate_from_volume_buckets))
