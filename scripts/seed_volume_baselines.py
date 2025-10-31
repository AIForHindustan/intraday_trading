#!/usr/bin/env python3
"""
Seed Volume Baselines From 20-Day Averages
=========================================

Bootstrap Redis `volume:baseline:*` keys using the canonical
`config/volume_averages_20d.json` file.  This is useful when DB0/DB5
have been flushed and the trading system needs baseline data before
real-time statistics can rebuild themselves.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

from redis_files.redis_ohlc_keys import normalize_symbol
from utils.time_aware_volume_baseline import TimeAwareVolumeBaseline
from redis_files.redis_client import get_redis_client


TRADING_MINUTES_PER_DAY = 6 * 60 + 15  # 9:15 – 15:30 IST -> 375 minutes


def _resolve_base_symbol(symbol: str) -> str:
    """Collapse instrument identifiers down to their base trading symbol."""
    upper = symbol.upper()
    import re

    expiry_match = re.search(r"\d{2}[A-Z]{3}", upper)
    if expiry_match:
        upper = upper.replace(expiry_match.group(), "")

    if upper.endswith("FUT"):
        upper = upper[:-3]

    if re.search(r"\d+(CE|PE)$", upper):
        upper = re.sub(r"\d+(CE|PE)$", "", upper)

    return upper


def seed_baselines(volume_file: Path) -> None:
    if not volume_file.exists():
        raise FileNotFoundError(f"Volume averages file not found: {volume_file}")

    with volume_file.open("r", encoding="utf-8") as handle:
        volume_data = json.load(handle)

    redis_wrapper = get_redis_client()
    baseline_client = redis_wrapper.get_client(2) or redis_wrapper.redis_client  # analytics DB
    if baseline_client is None:
        raise RuntimeError("Unable to acquire Redis baseline client (DB 2 - analytics)")

    seeded = 0
    skipped = 0

    for raw_symbol, payload in volume_data.items():
        try:
            avg_volume = float(payload.get("avg_volume_20d") or 0.0)
            if not math.isfinite(avg_volume) or avg_volume <= 0:
                skipped += 1
                continue

            base_symbol = _resolve_base_symbol(raw_symbol)
            normalized = normalize_symbol(base_symbol)
            if not normalized:
                skipped += 1
                continue

            baseline_1min = avg_volume / TRADING_MINUTES_PER_DAY
            baseline_key = f"volume:baseline:{normalized}:1min"
            baseline_client.set(baseline_key, baseline_1min)

            # Pre-compute other bucket resolutions for faster cold starts.
            for bucket, multiplier in TimeAwareVolumeBaseline.BUCKET_MULTIPLIERS.items():
                bucket_key = f"volume:baseline:{normalized}:{bucket}"
                bucket_value = baseline_1min * multiplier
                baseline_client.set(bucket_key, bucket_value)

            seeded += 1
        except Exception as exc:  # pragma: no cover - defensive logging only
            print(f"⚠️  Failed to seed baseline for {raw_symbol}: {exc}", file=sys.stderr)
            skipped += 1

    print(f"✅ Seeded baselines for {seeded} symbols (skipped {skipped})")


if __name__ == "__main__":
    volume_file = Path(__file__).resolve().parents[1] / "config" / "volume_averages_20d.json"
    seed_baselines(volume_file)
