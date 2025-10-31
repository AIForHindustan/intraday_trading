#!/usr/bin/env python3
"""
Daily OHLC updater for NIFTY universe.

This script fetches the previous trading day's candles from Zerodha and stores them in the
Redis 8 structures introduced for OHLC data:
  - TimeSeries (`ohlc:{symbol}:1d`)
  - Sorted sets (`ohlc_daily:{symbol}`)
  - Latest snapshot hash (`ohlc_latest:{symbol}`)
  - Rolling statistics hash (`ohlc_stats:{symbol}`)
  - Real-time stream (`ohlc_updates:{symbol}`)
"""

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from config.zerodha_config import ZerodhaConfig
from redis_files.redis_ohlc_keys import (
    normalize_symbol,
    ohlc_timeseries_key,
    ohlc_daily_zset,
    ohlc_latest_hash,
    ohlc_stats_hash,
    ohlc_stream,
)

DATA_DIR = Path(__file__).resolve().parent
NIFTY_LIST_FILE = DATA_DIR / "nifty_50_official_list.json"
SYMBOL_TTL_SECONDS = 90 * 24 * 3600  # 90 days
MAX_STREAM_LENGTH = 2000


def _midnight_epoch_ms(target_date: datetime) -> int:
    """Return midnight epoch milliseconds for the provided date."""
    midnight = datetime.combine(target_date.date(), datetime.min.time())
    return int(midnight.timestamp() * 1000)


@dataclass(frozen=True)
class SymbolInfo:
    tradingsymbol: str
    instrument_token: int
    normalized: str
    exchange: str = "NSE"


class OHLCUpdater:
    """Coordinates fetching OHLC data from Zerodha and storing it in Redis."""

    def __init__(
        self,
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: Optional[str] = None,
        concurrency: int = 8,
    ):
        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
        )
        self.kite = ZerodhaConfig.get_kite_instance()
        self.symbols = self._load_symbols()
        self.symbol_map = self._build_symbol_map(self.symbols)
        self.concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)

    def _load_symbols(self) -> List[str]:
        """Load tradingsymbols from the curated NIFTY 50 list."""
        if not NIFTY_LIST_FILE.exists():
            raise FileNotFoundError(f"Nifty list not found at {NIFTY_LIST_FILE}")
        data = json.loads(NIFTY_LIST_FILE.read_text(encoding="utf-8"))
        return data.get("nifty_50_constituents", [])

    def _build_symbol_map(self, tradingsymbols: Iterable[str]) -> Dict[str, SymbolInfo]:
        """Resolve Zerodha instrument tokens for the configured symbols."""
        instruments = self.kite.instruments("NSE")
        lookup = {item["tradingsymbol"]: item for item in instruments}

        symbol_map: Dict[str, SymbolInfo] = {}
        for symbol in tradingsymbols:
            instrument = lookup.get(symbol)
            if not instrument:
                print(f"⚠️  Instrument token not found for {symbol}, skipping.")
                continue
            normalized = normalize_symbol(symbol)
            symbol_map[symbol] = SymbolInfo(
                tradingsymbol=symbol,
                instrument_token=instrument["instrument_token"],
                normalized=normalized,
                exchange=instrument.get("exchange", "NSE"),
            )

        # Include NIFTY index manually (primary index token: 256265)
        symbol_map["NIFTY 50"] = SymbolInfo(
            tradingsymbol="NIFTY 50",
            instrument_token=256265,
            normalized=normalize_symbol("NIFTY 50"),
            exchange="NSE",
        )
        return symbol_map

    async def update_daily_ohlc(self, target_date: Optional[datetime] = None) -> None:
        """Fetch and store OHLC for the provided date (defaults to previous calendar day)."""
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)

        # Align to date-only (strip time component)
        target_date = datetime.combine(target_date.date(), datetime.min.time())
        print(f"📅 Updating OHLC for {target_date.date().isoformat()} ({len(self.symbol_map)} symbols)")

        symbols = list(self.symbol_map.values())
        batch_size = 10

        for batch_start in range(0, len(symbols), batch_size):
            batch = symbols[batch_start : batch_start + batch_size]
            tasks = [
                asyncio.create_task(self._update_symbol(symbol_info, target_date))
                for symbol_info in batch
            ]
            await asyncio.gather(*tasks)

            if batch_start + batch_size < len(symbols):
                await asyncio.sleep(5)
        print("✅ Daily OHLC update complete.")

    async def _update_symbol(self, symbol_info: SymbolInfo, target_date: datetime) -> None:
        async with self._semaphore:
            try:
                ohlc = await self._fetch_zerodha_ohlc(symbol_info, target_date)
                if not ohlc:
                    print(f"⚠️  No OHLC data for {symbol_info.tradingsymbol} on {target_date.date()}")
                    return

                await self._store_ohlc(symbol_info, target_date, ohlc)
                print(f"✅ Stored OHLC for {symbol_info.tradingsymbol} ({target_date.date()})")
            except Exception as exc:
                print(f"❌ Failed to update {symbol_info.tradingsymbol}: {exc}")

    async def _fetch_zerodha_ohlc(self, symbol_info: SymbolInfo, target_date: datetime) -> Optional[Dict[str, float]]:
        """Fetch a single day's candle from Zerodha."""
        from_date = target_date
        to_date = target_date + timedelta(days=1)

        def _fetch() -> List[Dict[str, object]]:
            return self.kite.historical_data(
                instrument_token=symbol_info.instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval="day",
            )

        records: List[Dict[str, object]] = await asyncio.to_thread(_fetch)
        if not records:
            return None

        candle = records[-1]
        return {
            "open": float(candle.get("open", 0.0) or 0.0),
            "high": float(candle.get("high", 0.0) or 0.0),
            "low": float(candle.get("low", 0.0) or 0.0),
            "close": float(candle.get("close", 0.0) or 0.0),
            "volume": float(candle.get("volume", 0.0) or 0.0),
        }

    async def _store_ohlc(self, symbol_info: SymbolInfo, target_date: datetime, ohlc: Dict[str, float]) -> None:
        """Persist OHLC data in Redis using the new schema."""
        timestamp_ms = _midnight_epoch_ms(target_date)
        symbol_key = symbol_info.normalized

        ts_key = ohlc_timeseries_key(symbol_key, "1d")
        zset_key = ohlc_daily_zset(symbol_key)
        hash_key = ohlc_latest_hash(symbol_key)
        stats_key = ohlc_stats_hash(symbol_key)
        stream_key = ohlc_stream(symbol_key)

        await self._ensure_timeseries(ts_key, symbol_key, "1d")

        await self.redis.execute_command("TS.ADD", ts_key, timestamp_ms, ohlc["close"])

        payload = json.dumps(
            {
                "o": ohlc["open"],
                "h": ohlc["high"],
                "l": ohlc["low"],
                "c": ohlc["close"],
                "v": ohlc["volume"],
                "d": target_date.strftime("%Y-%m-%d"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        await self.redis.zadd(zset_key, mapping={payload: timestamp_ms})
        await self.redis.expire(zset_key, SYMBOL_TTL_SECONDS)

        await self.redis.xadd(
            stream_key,
            {
                "symbol": symbol_key,
                "timestamp": str(timestamp_ms),
                "interval": "1d",
                "open": f"{ohlc['open']:.4f}",
                "high": f"{ohlc['high']:.4f}",
                "low": f"{ohlc['low']:.4f}",
                "close": f"{ohlc['close']:.4f}",
                "volume": f"{ohlc['volume']:.0f}",
                "date": target_date.strftime("%Y-%m-%d"),
            },
            maxlen=MAX_STREAM_LENGTH,
            approximate=True,
        )

        await self.redis.hset(
            hash_key,
            mapping={
                "open": ohlc["open"],
                "high": ohlc["high"],
                "low": ohlc["low"],
                "close": ohlc["close"],
                "volume": ohlc["volume"],
                "date": target_date.strftime("%Y-%m-%d"),
                "timestamp": timestamp_ms,
                "updated_at": int(datetime.utcnow().timestamp() * 1000),
            },
        )
        await self.redis.expire(hash_key, SYMBOL_TTL_SECONDS)

        await self._refresh_stats(zset_key, stats_key)

    async def _ensure_timeseries(self, ts_key: str, symbol_key: str, interval: str) -> None:
        """Create TS key with labels if it does not exist."""
        try:
            await self.redis.execute_command(
                "TS.CREATE",
                ts_key,
                "DUPLICATE_POLICY",
                "last",
                "LABELS",
                "symbol",
                symbol_key,
                "interval",
                interval,
                "source",
                "zerodha",
            )
        except ResponseError as exc:
            if "already exists" not in str(exc):
                raise

    async def _refresh_stats(self, zset_key: str, stats_key: str) -> None:
        """Recalculate rolling statistics using the last 30 daily entries."""
        entries = await self.redis.zrevrange(zset_key, 0, 29)
        if not entries:
            return

        highs = []
        lows = []
        closes = []
        volumes = []

        for entry in entries:
            try:
                data = json.loads(entry)
            except json.JSONDecodeError:
                continue
            highs.append(float(data.get("h", 0.0) or 0.0))
            lows.append(float(data.get("l", 0.0) or 0.0))
            closes.append(float(data.get("c", 0.0) or 0.0))
            volumes.append(float(data.get("v", 0.0) or 0.0))

        mapping = {
            "records": len(closes),
            "high_30d": max(highs) if highs else 0.0,
            "low_30d": min(lows) if lows else 0.0,
            "avg_close_30d": sum(closes) / len(closes) if closes else 0.0,
            "avg_volume_30d": sum(volumes) / len(volumes) if volumes else 0.0,
            "total_volume_30d": sum(volumes),
        }
        await self.redis.hset(stats_key, mapping=mapping)
        await self.redis.expire(stats_key, SYMBOL_TTL_SECONDS)

    async def close(self) -> None:
        await self.redis.close()


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily OHLC updater for Zerodha instruments.")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD (defaults to previous calendar day).",
    )
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--redis-db", default=0, type=int)
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--concurrency", default=8, type=int)
    return parser.parse_args(argv)


async def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    target_date = datetime.strptime(args.date, "%Y-%m-%d") if args.date else None

    updater = OHLCUpdater(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        concurrency=args.concurrency,
    )
    try:
        await updater.update_daily_ohlc(target_date)
    finally:
        await updater.close()


if __name__ == "__main__":
    asyncio.run(main())
