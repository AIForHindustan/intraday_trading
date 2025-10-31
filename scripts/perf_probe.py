#!/usr/bin/env python3
"""
Non-invasive performance probes for existing pipeline.

- No architecture changes; just timing + minimal telemetry.
- Writes optional metrics to Redis Stream: metrics:perf (bounded).

Usage:
  from perf_probe import patch_redis_client, timed_block, slow_tick_guard, PerfSink
  perf = PerfSink(redis_client=get_db_client(1))  # or None to log-only
  patch_redis_client(perf, redis_client)          # wrap xadd/publish/evalsha/... at runtime

  with slow_tick_guard(perf, symbol, threshold_ms=30):  # around per-tick processing
      ... existing tick work ...

  @timed_block("talib.rsi")   # decorate your TA-Lib/volume profile compute functions
  def compute_rsi(...): ...
"""

from __future__ import annotations

import functools
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Optional


# ---------- sink that can write to Redis Stream (bounded) or just keep counters ----------
class PerfSink:
    def __init__(self, redis_client=None, stream: str = "metrics:perf", maxlen: int = 2000, approximate: bool = True):
        self.r = redis_client   # may be None
        self.stream = stream
        self.maxlen = maxlen
        self.approximate = approximate
        self._lock = threading.Lock()
        self._counters: Dict[str, Dict[str, float]] = {}  # name -> {count, sum_ms, max_ms}
        self.enabled = str(os.getenv("PERF_PROBE", "1")).lower() not in {"0", "false", "off"}

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def add(self, name: str, dur_ms: float, extra: Optional[Dict[str, Any]] = None):
        if not self.enabled:
            return
        with self._lock:
            st = self._counters.setdefault(name, {"count": 0, "sum_ms": 0.0, "max_ms": 0.0})
            st["count"] += 1
            st["sum_ms"] += dur_ms
            if dur_ms > st["max_ms"]:
                st["max_ms"] = dur_ms
        if self.r:
            try:
                fields: Dict[str, Any] = {
                    "ts": self._now_ms(),
                    "name": name,
                    "dur_ms": f"{dur_ms:.3f}",
                }
                if extra:
                    for k, v in extra.items():
                        fields[k] = v if isinstance(v, (str, int, float)) else json.dumps(v, default=str)
                # bounded stream write
                self.r.xadd(self.stream, fields, maxlen=self.maxlen, approximate=self.approximate)
            except Exception:
                pass

    def snapshot(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            # shallow copy
            return {k: dict(v) for k, v in self._counters.items()}


# ---------- decorators / context managers ----------
def timed_block(name: str) -> Callable:
    """Decorate any compute function (e.g., TA-Lib / volume profile) to record duration."""
    def deco(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            sink: Optional[PerfSink] = kwargs.pop("_perf_sink", None) or getattr(wrapper, "_perf_sink", None)
            t0 = time.perf_counter_ns()
            try:
                return fn(*args, **kwargs)
            finally:
                if sink:
                    dt_ms = (time.perf_counter_ns() - t0) / 1e6
                    sink.add(name, dt_ms)
        return wrapper
    return deco


@contextmanager
def slow_tick_guard(sink: PerfSink, symbol: str, threshold_ms: Optional[float] = None, label: str = "tick"):
    """Wrap one full tick processing path; emits slow event if threshold exceeded."""
    thr = float(os.getenv("SLOW_TICK_MS", threshold_ms or 50))
    t0 = time.perf_counter_ns()
    try:
        yield
    finally:
        dt_ms = (time.perf_counter_ns() - t0) / 1e6
        if sink:
            sink.add(f"{label}.total", dt_ms, {"symbol": symbol})
            if dt_ms >= thr:
                sink.add(f"{label}.slow", dt_ms, {"symbol": symbol, "threshold_ms": thr})


# ---------- redis client patch (monkey-patch methods only; no behavior change) ----------
def patch_redis_client(sink: PerfSink, client: Any):
    """
    Wrap frequent methods: xadd, publish, evalsha, get, hset, hmset, lpush.
    Call once after client construction. Safe if method missing.
    """

    def _wrap_method(obj, meth: str, label: str):
        if not hasattr(obj, meth):
            return
        original = getattr(obj, meth)
        # avoid double-wrapping
        if getattr(original, "__perf_wrapped__", False):
            return

        @functools.wraps(original)
        def wrapped(*args, **kwargs):
            t0 = time.perf_counter_ns()
            try:
                return original(*args, **kwargs)
            finally:
                dt_ms = (time.perf_counter_ns() - t0) / 1e6
                extra: Dict[str, Any] = {}
                if meth == "xadd":
                    try:
                        stream = args[0] if args else kwargs.get("name") or kwargs.get("stream")
                        extra["stream"] = stream
                    except Exception:
                        pass
                sink.add(f"redis.{label}", dt_ms, extra or None)

        setattr(wrapped, "__perf_wrapped__", True)
        setattr(obj, meth, wrapped)

    # Common hot-path commands
    _wrap_method(client, "xadd", "xadd")
    _wrap_method(client, "publish", "publish")
    _wrap_method(client, "evalsha", "evalsha")
    _wrap_method(client, "eval", "eval")
    _wrap_method(client, "get", "get")
    _wrap_method(client, "hset", "hset")
    _wrap_method(client, "hmset", "hmset")
    _wrap_method(client, "lpush", "lpush")


# ---------- tiny CLI to print rolling summary ----------
def _fmt(ms: float) -> str:
    return f"{ms:.2f}ms"


def print_summary(sink: PerfSink):
    snap = sink.snapshot()
    if not snap:
        print("(no metrics yet)")
        return
    lines = []
    for name, st in sorted(snap.items()):
        cnt = int(st["count"])
        avg = (st["sum_ms"] / cnt) if cnt else 0.0
        mx = st["max_ms"]
        lines.append(f"{name:<20}  n={cnt:<6}  avg={_fmt(avg):>8}  max={_fmt(mx):>8}")
    print("\n".join(lines))


if __name__ == "__main__":
    # Demo: local in-memory sink; simulate a few calls
    s = PerfSink(redis_client=None)

    @timed_block("demo.block")
    def fake(n, _perf_sink=None):
        time.sleep(n / 1000.0)

    fake(5, _perf_sink=s)
    fake(7, _perf_sink=s)
    s.add("redis.xadd", 1.2, {"stream": "ticks:NIFTY"})
    print_summary(s)


