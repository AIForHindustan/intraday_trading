#!/usr/bin/env python3
"""
OHLC updater for Intraday Crawler instruments.

This script fetches historical OHLC data from Zerodha for all instruments in binary_crawler1.json
and stores them in Redis 8 structures:
  - TimeSeries (`ohlc:{symbol}:1d`)
  - Sorted sets (`ohlc_daily:{symbol}`)
  - Latest snapshot hash (`ohlc_latest:{symbol}`)
  - Rolling statistics hash (`ohlc_stats:{symbol}`)
  - Real-time stream (`ohlc_updates:{symbol}`)

Supports:
  - Single date: --date YYYY-MM-DD
  - Date range: --start-date YYYY-MM-DD --end-date YYYY-MM-DD
  - All intraday crawler instruments (246 tokens from binary_crawler1.json)
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from redis.asyncio import Redis
from redis.exceptions import ResponseError

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from config.zerodha_config import ZerodhaConfig
from redis_files.redis_ohlc_keys import (
    normalize_symbol,
    ohlc_timeseries_key,
    ohlc_daily_zset,
    ohlc_latest_hash,
    ohlc_stats_hash,
    ohlc_stream,
)
from crawlers.utils.instrument_mapper import InstrumentMapper

DATA_DIR = Path(__file__).resolve().parent
CRAWLER_CONFIG_FILE = project_root / "crawlers" / "binary_crawler1" / "binary_crawler1.json"
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
        concurrency: int = 3,  # Reduced default concurrency to avoid rate limits
    ):
        self.redis = Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password,
            decode_responses=True,
        )
        
        # Initialize and verify Kite connection
        print("🔌 Initializing Zerodha Kite API connection...")
        try:
            self.kite = ZerodhaConfig.get_kite_instance()
            profile = self.kite.profile()
            print(f"✅ Connected to Zerodha API (User: {profile.get('user_name', 'N/A')})")
        except Exception as e:
            print(f"❌ Failed to initialize Zerodha API: {e}")
            raise
        
        self.symbols = self._load_symbols()
        self.symbol_map = self._build_symbol_map(self.symbols)
        self.concurrency = concurrency
        self._semaphore = asyncio.Semaphore(concurrency)

    def _load_symbols(self) -> List[int]:
        """Load instrument tokens from intraday crawler config (binary_crawler1.json)."""
        if not CRAWLER_CONFIG_FILE.exists():
            raise FileNotFoundError(f"Intraday crawler config not found at {CRAWLER_CONFIG_FILE}")
        data = json.loads(CRAWLER_CONFIG_FILE.read_text(encoding="utf-8"))
        tokens = data.get("tokens", [])
        # Convert to integers
        return [int(token) if isinstance(token, str) else token for token in tokens]

    def _build_symbol_map(self, tokens: Iterable[int]) -> Dict[str, SymbolInfo]:
        """Resolve instrument tokens to symbol info using token resolver."""
        print(f"🔍 Resolving {len(tokens)} instrument tokens...")
        
        symbol_map: Dict[str, SymbolInfo] = {}
        instrument_mapper = InstrumentMapper()
        
        # Try InstrumentMapper first (faster, uses cached data)
        resolved_count = 0
        unresolved_tokens = []
        
        for token in tokens:
            try:
                metadata = instrument_mapper.get_token_metadata(token)
                if metadata:
                    tradingsymbol = metadata.get("tradingsymbol") or metadata.get("symbol") or metadata.get("name")
                    exchange = metadata.get("exchange", "NSE")
                    
                    if tradingsymbol:
                        normalized = normalize_symbol(tradingsymbol)
                        symbol_map[str(token)] = SymbolInfo(
                            tradingsymbol=tradingsymbol,
                            instrument_token=token,
                            normalized=normalized,
                            exchange=exchange,
                        )
                        resolved_count += 1
                        continue
            except Exception:
                pass
            
            # Mark as unresolved for Zerodha API lookup
            unresolved_tokens.append(token)
        
        # Fallback to Zerodha API for unresolved tokens (slower, but comprehensive)
        if unresolved_tokens:
            print(f"   📡 Fetching {len(unresolved_tokens)} tokens from Zerodha API...")
            all_instruments = {}
            for exchange in ["NSE", "NFO", "BSE", "BFO"]:
                try:
                    instruments = self.kite.instruments(exchange)
                    for item in instruments:
                        all_instruments[item["instrument_token"]] = item
                except Exception as e:
                    print(f"⚠️  Warning: Could not load instruments from {exchange}: {e}")
            
            for token in unresolved_tokens:
                instrument = all_instruments.get(token)
                if instrument:
                    tradingsymbol = instrument["tradingsymbol"]
                    exchange = instrument.get("exchange", "NSE")
                    normalized = normalize_symbol(tradingsymbol)
                    
                    symbol_map[str(token)] = SymbolInfo(
                        tradingsymbol=tradingsymbol,
                        instrument_token=token,
                        normalized=normalized,
                        exchange=exchange,
                    )
                    resolved_count += 1
                else:
                    print(f"⚠️  Could not resolve token {token}, skipping.")
        
        print(f"✅ Resolved {resolved_count}/{len(tokens)} instruments")
        if unresolved_tokens:
            print(f"   ⚠️  {len(unresolved_tokens)} tokens could not be resolved")
        return symbol_map

    async def update_daily_ohlc(self, target_date: Optional[datetime] = None, 
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None) -> None:
        """Fetch and store OHLC for date(s).
        
        Args:
            target_date: Single date to fetch (if start_date/end_date not provided)
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive)
        """
        # Determine date range
        if start_date and end_date:
            # Date range mode
            dates = []
            current = start_date
            while current <= end_date:
                # Skip weekends
                if current.weekday() < 5:
                    dates.append(datetime.combine(current.date(), datetime.min.time()))
                current += timedelta(days=1)
            
            # Check if we're fetching November/December data
            nov_dates = [d for d in dates if d.month == 11]
            dec_dates = [d for d in dates if d.month == 12]
            
            date_range_str = f"{start_date.date()} to {end_date.date()}"
            if nov_dates and dec_dates:
                print(f"📅 Fetching OHLC for {len(dates)} dates ({date_range_str})")
                print(f"   📆 November: {len(nov_dates)} days | December: {len(dec_dates)} days")
            elif nov_dates:
                print(f"📅 Fetching OHLC for {len(dates)} dates ({date_range_str})")
                print(f"   📆 November: {len(nov_dates)} days")
            elif dec_dates:
                print(f"📅 Fetching OHLC for {len(dates)} dates ({date_range_str})")
                print(f"   📆 December: {len(dec_dates)} days")
            else:
                print(f"📅 Fetching OHLC for {len(dates)} dates ({date_range_str})")
        elif target_date:
            # Single date mode
            target_date = datetime.combine(target_date.date(), datetime.min.time())
            dates = [target_date]
            date_range_str = f"{target_date.date().isoformat()}"
            print(f"📅 Updating OHLC for {date_range_str}")
        else:
            # Default to previous calendar day
            target_date = datetime.now() - timedelta(days=1)
            target_date = datetime.combine(target_date.date(), datetime.min.time())
            dates = [target_date]
            print(f"📅 Updating OHLC for {target_date.date().isoformat()}")

        resolved_symbols = len(self.symbol_map)
        
        # Analyze expiry months for F&O contracts
        expiry_analysis = {"NOV": 0, "DEC": 0, "OTHER": 0, "EQ": 0}
        for symbol_info in self.symbol_map.values():
            symbol = symbol_info.tradingsymbol
            if "25NOV" in symbol or "NOV25" in symbol:
                expiry_analysis["NOV"] += 1
            elif "25DEC" in symbol or "DEC25" in symbol:
                expiry_analysis["DEC"] += 1
            elif symbol_info.exchange in ["NFO", "BFO"]:
                expiry_analysis["OTHER"] += 1
            else:
                expiry_analysis["EQ"] += 1
        
        print(f"\n📊 Contract Breakdown:")
        if expiry_analysis["NOV"] > 0:
            print(f"   November contracts: {expiry_analysis['NOV']}")
        if expiry_analysis["DEC"] > 0:
            print(f"   December contracts: {expiry_analysis['DEC']}")
        if expiry_analysis["OTHER"] > 0:
            print(f"   Other F&O: {expiry_analysis['OTHER']}")
        if expiry_analysis["EQ"] > 0:
            print(f"   Equity: {expiry_analysis['EQ']}")
        
        print(f"\n   Processing {resolved_symbols} symbols (from {len(self.symbols)} tokens loaded)")
        
        # Smart filtering: Only fetch dates relevant to each contract's expiry month
        symbols_with_dates = []
        for symbol_info in self.symbol_map.values():
            symbol = symbol_info.tradingsymbol
            # Determine which dates this contract should have data for
            relevant_dates = []
            for date in dates:
                date_month = date.month
                # November contracts should only have Nov data
                if "25NOV" in symbol or "NOV25" in symbol:
                    if date_month == 11:
                        relevant_dates.append(date)
                # December contracts should only have Dec data  
                elif "25DEC" in symbol or "DEC25" in symbol:
                    if date_month == 12:
                        relevant_dates.append(date)
                # Equity or other - fetch all dates
                else:
                    relevant_dates.append(date)
            
            if relevant_dates:
                symbols_with_dates.append((symbol_info, relevant_dates))
        
        total_ops = sum(len(dts) for _, dts in symbols_with_dates)
        print(f"   📊 Smart filtering: {total_ops} operations (vs {resolved_symbols * len(dates)} without filtering)")
        print(f"   💡 Only fetching relevant dates per contract expiry month")
        
        symbols = symbols_with_dates
        batch_size = 5  # Reduced from 20 to avoid rate limits

        total_updates = 0
        processed_count = 0
        
        # Process each symbol with its relevant dates
        for symbol_idx, (symbol_info, relevant_dates) in enumerate(symbols):
            for date in relevant_dates:
                # Check if data already exists to avoid redundant API calls
                symbol_key = symbol_info.normalized
                zset_key = f"ohlc_daily:{symbol_key}"
                date_str = date.strftime("%Y-%m-%d")
                timestamp_ms = int(date.timestamp() * 1000)
                
                # Quick check if this date already exists
                existing = await self.redis.zrangebyscore(zset_key, timestamp_ms - 1000, timestamp_ms + 1000)
                skip_fetch = False
                if existing:
                    # Check if the date field matches
                    for entry in existing:
                        try:
                            entry_data = json.loads(entry)
                            if entry_data.get("d") == date_str or entry_data.get("date") == date_str:
                                # Data already exists, skip API call
                                skip_fetch = True
                                break
                        except:
                            pass
                
                if skip_fetch:
                    processed_count += 1
                    continue
                
                # Fetch and store
                success = await self._update_symbol(symbol_info, date)
                if success:
                    total_updates += 1
                
                processed_count += 1
                if processed_count % 20 == 0 or processed_count == total_ops:
                    progress_pct = (processed_count / total_ops) * 100
                    success_rate = (total_updates / processed_count * 100) if processed_count > 0 else 0
                    print(f"   Progress: {progress_pct:.1f}% ({processed_count}/{total_ops}) - Updated: {total_updates} ({success_rate:.1f}% success)")
                
                # Rate limiting - balanced to avoid limits but still reasonable speed
                await asyncio.sleep(0.3)  # ~3 req/sec
        
        print(f"\n✅ OHLC update complete. Updated {total_updates} symbol-date combinations.")

    async def _update_symbol(self, symbol_info: SymbolInfo, target_date: datetime) -> bool:
        """Update OHLC for a symbol on a specific date. Returns True if successful."""
        async with self._semaphore:
            try:
                ohlc = await self._fetch_zerodha_ohlc(symbol_info, target_date)
                if not ohlc:
                    # Silently skip - contract may not have existed on this date
                    return False

                await self._store_ohlc(symbol_info, target_date, ohlc)
                return True
            except Exception as exc:
                error_msg = str(exc).lower()
                # Handle rate limiting errors
                if "too many requests" in error_msg or "rate limit" in error_msg:
                    # Wait much longer and retry once
                    await asyncio.sleep(10)
                    try:
                        ohlc = await self._fetch_zerodha_ohlc(symbol_info, target_date)
                        if ohlc:
                            await self._store_ohlc(symbol_info, target_date, ohlc)
                            return True
                    except:
                        pass
                    # Don't spam warnings - just continue
                    return False
                # Only log non-expected errors
                if "invalid" not in error_msg and "not found" not in error_msg and "instrument" not in error_msg:
                    print(f"❌ Failed to update {symbol_info.tradingsymbol}: {exc}")
                return False

    def _find_historical_token(self, symbol_info: SymbolInfo, target_date: datetime) -> Optional[int]:
        """Find the correct instrument token for a given historical date.
        
        For F&O contracts, Zerodha API returns data if the token existed at that time.
        If token doesn't exist for that date, returns None (caller should skip).
        """
        # For now, try using the current token - Zerodha API will return data
        # if the token existed on that date, or None if it didn't
        return symbol_info.instrument_token
    
    async def _fetch_zerodha_ohlc(self, symbol_info: SymbolInfo, target_date: datetime) -> Optional[Dict[str, float]]:
        """Fetch a single day's candle from Zerodha.
        
        Handles the case where token might not have existed on that date.
        """
        from_date = target_date
        to_date = target_date + timedelta(days=1)

        def _fetch() -> Optional[List[Dict[str, object]]]:
            try:
                # Try to find the correct token for this historical date
                token = self._find_historical_token(symbol_info, target_date)
                if not token:
                    return None
                
                data = self.kite.historical_data(
                    instrument_token=token,
                    from_date=from_date,
                    to_date=to_date,
                    interval="day",
                )
                return data if data else None
            except Exception as e:
                # Check if error is due to token not existing on that date
                error_msg = str(e).lower()
                if any(phrase in error_msg for phrase in ["invalid", "not found", "bad request", "instrument"]):
                    # Token didn't exist on this date - this is expected for new contracts
                    return None
                # Re-raise other errors
                raise

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

        # ✅ FIXED: ALWAYS use maxlen to prevent unbounded stream growth
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
                "updated_at": int(datetime.now().timestamp() * 1000),
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
    parser = argparse.ArgumentParser(
        description="OHLC updater for Intraday Crawler instruments (all 246 tokens from binary_crawler1.json)."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Single target date in YYYY-MM-DD (defaults to previous calendar day).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for range in YYYY-MM-DD (use with --end-date).",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for range in YYYY-MM-DD (use with --start-date).",
    )
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--redis-db", default=5, type=int, help="Redis database (default: 5 for OHLC data)")
    parser.add_argument("--redis-password", default=None)
    parser.add_argument("--concurrency", default=8, type=int, help="Concurrent API calls (default: 8)")
    return parser.parse_args(argv)


async def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    
    # Parse dates
    target_date = None
    start_date = None
    end_date = None
    
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    elif args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        # Default: 1 month before today to today
        today = datetime.now().date()
        one_month_ago = today - timedelta(days=30)
        start_date = datetime.combine(one_month_ago, datetime.min.time())
        end_date = datetime.combine(today, datetime.min.time())
        print(f"📅 Using default range: 1 month before today ({start_date.date()}) to today ({end_date.date()})")

    updater = OHLCUpdater(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        concurrency=args.concurrency,
    )
    try:
        await updater.update_daily_ohlc(target_date=target_date, start_date=start_date, end_date=end_date)
    finally:
        await updater.close()


if __name__ == "__main__":
    asyncio.run(main())
