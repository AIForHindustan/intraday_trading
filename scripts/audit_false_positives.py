#!/usr/bin/env python3
"""Audit recent volume-spike alerts against actual outcomes."""

import json
from datetime import datetime, timedelta
from typing import Optional, Tuple

from alert_validation.alert_validator import AlertValidator
from utils.correct_volume_calculator import VolumeResolver
from config.utils.timestamp_normalizer import TimestampNormalizer

# Constants
PRICE_STREAM_KEY = "ticks:intraday:processed"
ALERT_LIST_KEY = "alerts:volume_spike:recent"
PRICE_WINDOW_MINUTES = 30
AUDIT_LIMIT = 10


def _parse_alert(alert_blob: str) -> Optional[dict]:
    try:
        return json.loads(alert_blob)
    except json.JSONDecodeError:
        return None


def _get_price_movement(
    redis_client,
    symbol: str,
    alert_time: datetime,
    window_minutes: int = PRICE_WINDOW_MINUTES,
) -> float:
    """Compute percent move over the post-alert window using the shared stream."""
    if not redis_client:
        return 0.0

    start_ms = int(alert_time.timestamp() * 1000)
    end_ms = start_ms + window_minutes * 60_000

    try:
        entries = redis_client.xrange(
            PRICE_STREAM_KEY,
            f"{start_ms}-0",
            f"{end_ms}-0",
            count=1000,
        )
    except Exception:
        return 0.0

    start_price = None
    end_price = None

    for entry_id, fields in entries:
        data = fields.get("data")
        if not data:
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            continue
        if payload.get("symbol") != symbol:
            continue
        price = payload.get("last_price") or payload.get("price")
        if price is None:
            continue
        price = float(price)
        if start_price is None:
            start_price = price
        end_price = price

    if start_price is None or end_price is None or start_price == 0:
        return 0.0

    return ((end_price - start_price) / start_price) * 100.0


def _get_volume_context(validator: AlertValidator, symbol: str, alert_data: dict, alert_time: datetime) -> Tuple[Optional[float], Optional[float]]:
    """Return actual incremental volume and expected baseline at alert time."""
    actual_volume = VolumeResolver.get_incremental_volume(alert_data)

    try:
        expected = validator.time_aware_baseline.get_expected_volume(symbol, alert_time)
    except Exception:
        expected = None

    return actual_volume, expected


def audit_recent_signals(limit: int = AUDIT_LIMIT):
    validator = AlertValidator()

    # Alert list lives in the alert datastore (typically DB 6)
    alert_store = validator.alert_store_client or validator.redis_client
    recent_alerts = alert_store.lrange(ALERT_LIST_KEY, 0, -1) if alert_store else []

    print(f"🔍 Auditing {len(recent_alerts)} recent volume spikes...")

    price_stream_client = validator.redis.get_client(0) or validator.redis_client

    for alert_blob in recent_alerts[-limit:]:
        alert = _parse_alert(alert_blob)
        if not alert:
            continue

        symbol = alert.get("symbol", "UNKNOWN")
        timestamp_raw = alert.get("timestamp")
        if not timestamp_raw:
            continue

        try:
            alert_time = datetime.fromisoformat(timestamp_raw)
        except ValueError:
            # Fallback for epoch timestamps
            alert_time = datetime.fromtimestamp(TimestampNormalizer.to_epoch_ms(timestamp_raw) / 1000.0)

        price_move_pct = _get_price_movement(price_stream_client, symbol, alert_time)
        actual_volume, expected_volume = _get_volume_context(validator, symbol, alert, alert_time)
        volume_ratio = (
            (actual_volume / expected_volume) if expected_volume and expected_volume > 0 else None
        )

        print(f"\n{symbol} at {alert_time.strftime('%Y-%m-%d %H:%M')}:")
        reported_ratio = alert.get('volume_ratio')
        if reported_ratio is not None:
            print(f"  Reported Volume Ratio: {float(reported_ratio):.2f}x")
        if volume_ratio is not None:
            print(f"  Recomputed Volume Ratio: {volume_ratio:.2f}x")
        if actual_volume is not None:
            print(f"  Incremental Volume: {actual_volume:.0f}")
        if expected_volume is not None:
            print(f"  Expected Volume: {expected_volume:.0f}")
        print(f"  Price Move (next {PRICE_WINDOW_MINUTES} min): {price_move_pct:.2f}%")
        print(f"  Valid Signal: {abs(price_move_pct) > 0.5}")


if __name__ == "__main__":
    audit_recent_signals()
