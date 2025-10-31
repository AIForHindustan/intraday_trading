#!/usr/bin/env python3
"""
Load OHLC snapshots into Redis 8 structures aligned with the new key standards.

Creates or updates:
- Time Series: ohlc:{symbol}:1d / 1h / 5m
- Sorted Sets: ohlc_daily:{symbol}, ohlc_hourly:{symbol}
- Hashes: ohlc_latest:{symbol}, ohlc_stats:{symbol}
- Streams: ohlc_updates:{symbol}
"""

import argparse
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import redis

SUPPORTED_TIMESERIES_INTERVALS = ("1d", "1h", "5m")


def _sanitize_symbol(symbol: str) -> str:
    """Return a Redis-safe uppercase symbol."""
    return "".join(ch for ch in symbol.upper().replace(" ", "_") if ch.isalnum() or ch == "_")


def _to_epoch_ms(date_str: str) -> int:
    """Convert ISO-like date string (with timezone) to epoch milliseconds."""
    dt = datetime.fromisoformat(date_str)
    return int(dt.timestamp() * 1000)


def _ensure_timeseries(conn: redis.Redis, key: str, labels: Dict[str, str]) -> None:
    """Create TS key with duplicate policy if it does not exist."""
    try:
        args = ["TS.CREATE", key, "DUPLICATE_POLICY", "last"]
        if labels:
            args.append("LABELS")
            for label_key, label_value in labels.items():
                args.extend([label_key, label_value])
        conn.execute_command(*args)
    except redis.ResponseError as exc:
        if "already exists" not in str(exc):
            raise


def _update_hashes(conn: redis.Redis, symbol: str, rows: List[Dict[str, object]]) -> None:
    """Populate ohlc_latest and ohlc_stats hashes."""
    if not rows:
        return

    latest = rows[-1]
    timestamp_ms = _to_epoch_ms(latest["date"])

    conn.hset(
        f"ohlc_latest:{symbol}",
        mapping={
            "timestamp": timestamp_ms,
            "open": latest.get("open", 0),
            "high": latest.get("high", 0),
            "low": latest.get("low", 0),
            "close": latest.get("close", 0),
            "volume": latest.get("volume", 0),
        },
    )

    highs = [float(row.get("high", 0) or 0.0) for row in rows]
    lows = [float(row.get("low", 0) or 0.0) for row in rows]
    closes = [float(row.get("close", 0) or 0.0) for row in rows]
    volumes = [float(row.get("volume", 0) or 0.0) for row in rows]

    stats_mapping = {
        "records": len(rows),
        "high_30d": max(highs) if highs else 0.0,
        "low_30d": min(lows) if lows else 0.0,
        "avg_close_30d": statistics.fmean(closes) if closes else 0.0,
        "avg_volume_30d": statistics.fmean(volumes) if volumes else 0.0,
        "total_volume_30d": sum(volumes),
    }

    conn.hset(f"ohlc_stats:{symbol}", mapping=stats_mapping)


def _push_interval_data(
    conn: redis.Redis,
    symbol: str,
    interval: str,
    rows: List[Dict[str, object]],
) -> Tuple[int, int, int]:
    """Insert OHLC rows into TimeSeries / Streams / Sorted Sets for a given interval."""
    series_key = f"ohlc:{symbol}:{interval}"
    stream_key = f"ohlc_updates:{symbol}"
    if interval == "1d":
        zset_key = f"ohlc_daily:{symbol}"
    else:
        zset_key = f"ohlc_hourly:{symbol}"

    _ensure_timeseries(
        conn,
        series_key,
        labels={"symbol": symbol, "interval": interval, "source": "nifty_30day"},
    )

    # Only insert data for 1d interval from the daily dataset.
    if interval != "1d":
        return 0, 0, 0

    ts_count = stream_count = zset_count = 0

    for row in rows:
        timestamp_ms = _to_epoch_ms(row["date"])
        open_price = float(row.get("open", 0.0) or 0.0)
        high_price = float(row.get("high", 0.0) or 0.0)
        low_price = float(row.get("low", 0.0) or 0.0)
        close_price = float(row.get("close", 0.0) or 0.0)
        volume = float(row.get("volume", 0.0) or 0.0)

        conn.execute_command("TS.ADD", series_key, timestamp_ms, close_price)
        ts_count += 1

        conn.xadd(
            stream_key,
            {
                "symbol": symbol,
                "timestamp": str(timestamp_ms),
                "interval": interval,
                "open": f"{open_price:.4f}",
                "high": f"{high_price:.4f}",
                "low": f"{low_price:.4f}",
                "close": f"{close_price:.4f}",
                "volume": f"{volume:.0f}",
                "date": row.get("date", ""),
            },
            maxlen=1000,
            approximate=True,
        )
        stream_count += 1

        payload = json.dumps(
            {
                "o": open_price,
                "h": high_price,
                "l": low_price,
                "c": close_price,
                "v": volume,
                "d": row.get("date"),
            }
        )
        conn.zadd(zset_key, {payload: timestamp_ms})
        zset_count += 1

    return ts_count, stream_count, zset_count


def _load_symbol(conn: redis.Redis, symbol: str, rows: List[Dict[str, object]]) -> Tuple[int, int, int]:
    """Load every interval structure for a given symbol."""
    total_ts = total_stream = total_zset = 0

    for interval in SUPPORTED_TIMESERIES_INTERVALS:
        ts, stream, zset = _push_interval_data(conn, symbol, interval, rows)
        total_ts += ts
        total_stream += stream
        total_zset += zset

    _update_hashes(conn, symbol, rows)
    return total_ts, total_stream, total_zset


def load_ohlc(json_path: Path, host: str, port: int, password: str | None, db: int) -> None:
    """Import OHLC JSON payload into Redis."""
    data = json.loads(json_path.read_text(encoding="utf-8"))
    conn = redis.Redis(host=host, port=port, password=password, db=db)

    total_ts = total_stream = total_zset = 0

    index_block = data.get("nifty_50_index", {})
    index_symbol = _sanitize_symbol(index_block.get("symbol", "NIFTY50"))
    index_rows = index_block.get("data", []) or []
    if index_rows:
        ts, stream, zset = _load_symbol(conn, index_symbol, index_rows)
        total_ts += ts
        total_stream += stream
        total_zset += zset

    for raw_symbol, rows in data.get("nifty_50_stocks", {}).items():
        if not rows:
            continue
        clean_symbol = _sanitize_symbol(raw_symbol)
        ts, stream, zset = _load_symbol(conn, clean_symbol, rows)
        total_ts += ts
        total_stream += stream
        total_zset += zset

    print(
        f"Loaded OHLC data: {total_ts} TS points, {total_stream} stream entries, "
        f"{total_zset} sorted-set members"
    )


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load OHLC JSON data into Redis.")
    parser.add_argument("json_path", type=Path, help="Path to OHLC JSON export.")
    parser.add_argument("--host", default="127.0.0.1", help="Redis host.")
    parser.add_argument("--port", default=6379, type=int, help="Redis port.")
    parser.add_argument("--password", default=None, help="Redis password, if any.")
    parser.add_argument("--db", default=0, type=int, help="Redis database number.")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    load_ohlc(args.json_path, args.host, args.port, args.password, args.db)


if __name__ == "__main__":
    main()
