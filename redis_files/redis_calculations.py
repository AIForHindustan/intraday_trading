"""High-performance Redis-backed technical indicator calculations."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Dict, List, Optional

from redis.exceptions import ResponseError


class OptimizedRedisCalculations:
    """
    Redis calculation helper that evaluates a Lua script via SCRIPT LOAD / EVALSHA.
    Falls back to the original Python implementations when Redis is unavailable.
    """

    def __init__(self, redis_client: Any):
        self.logger = logging.getLogger(__name__)
        self._script_sha: Optional[str] = None

        self.redis = self._resolve_client(redis_client)
        if not self.redis:
            self.logger.warning(
                "Redis client unavailable; falling back to Python calculations."
            )
            return

        # Lua script is in the same directory as this file (redis_files/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self._script_path = os.path.normpath(os.path.join(script_dir, "redis_calculations.lua"))
        self._load_script()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _resolve_client(self, redis_client: Any) -> Optional[Any]:
        """Resolve a redis-py client capable of script_load / evalsha."""
        if redis_client is None:
            return None

        # RobustRedisClient exposes .redis_client for raw access
        if hasattr(redis_client, "redis_client") and redis_client.redis_client:
            return redis_client.redis_client

        # Some helpers expose get_client(db)
        if hasattr(redis_client, "get_client"):
            try:
                client = redis_client.get_client(0)
                if client:
                    return client
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.debug("Failed to resolve Redis client: %s", exc)

        # Lastly, accept a raw redis-py client (with script_load capability)
        if hasattr(redis_client, "script_load") and hasattr(redis_client, "evalsha"):
            return redis_client

        return None

    def _load_script(self) -> None:
        """Load the Lua script into Redis and cache its SHA."""
        try:
            with open(self._script_path, "r", encoding="utf-8") as lua_file:
                script = lua_file.read()
        except FileNotFoundError:
            self.logger.warning("Lua script not found at %s", self._script_path)
            self._script_sha = None
            return
        except OSError as exc:
            self.logger.warning("Unable to read Lua script: %s", exc)
            self._script_sha = None
            return

        try:
            self._script_sha = self.redis.script_load(script)
            self.logger.info("✅ Redis indicator script loaded")
        except Exception as exc:  # pragma: no cover - defensive
            self._script_sha = None
            self.logger.warning("Could not load Lua script, using Python fallbacks: %s", exc)

    def _encode_arg(self, value: Any) -> Any:
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        if isinstance(value, bool):
            return "1" if value else "0"
        return value

    def _call_script(self, command: str, *args: Any) -> Any:
        if not self.redis or not self._script_sha:
            raise RuntimeError("Lua script not loaded")

        encoded_args = [command] + [self._encode_arg(arg) for arg in args]

        try:
            result = self.redis.evalsha(self._script_sha, 0, *encoded_args)
        except ResponseError as exc:
            if "NOSCRIPT" in str(exc).upper():
                self.logger.warning("Redis script missing; reloading.")
                self._load_script()
                if not self._script_sha:
                    raise
                result = self.redis.evalsha(self._script_sha, 0, *encoded_args)
            else:
                raise
        if isinstance(result, (bytes, bytearray)):
            return result.decode("utf-8")
        return result

    # ------------------------------------------------------------------ #
    # Public API (mirrors legacy RedisCalculations interface)
    # ------------------------------------------------------------------ #

    def calculate_atr_redis(
        self,
        high_data: List[float],
        low_data: List[float],
        close_data: List[float],
        period: int = 14,
    ) -> float:
        try:
            result = self._call_script("ATR", high_data, low_data, close_data, period)
            return float(result)
        except Exception as exc:
            self.logger.debug("Redis ATR fallback: %s", exc)
            return self._calculate_atr_python(high_data, low_data, close_data, period)

    def calculate_ema_redis(self, prices: List[float], period: int = 20) -> float:
        try:
            result = self._call_script("EMA", prices, period)
            return float(result)
        except Exception as exc:
            self.logger.debug("Redis EMA fallback: %s", exc)
            return self._calculate_ema_python(prices, period)

    def calculate_ema_all_redis(self, prices: List[float]) -> Dict[str, float]:
        """Calculate all EMA windows (5, 10, 20, 50, 100, 200) in one Redis call"""
        try:
            result = self._call_script("EMA_ALL", prices)
            if isinstance(result, str):
                return json.loads(result)
            return result
        except Exception as exc:
            self.logger.debug("Redis EMA_ALL fallback: %s", exc)
            return self._calculate_ema_all_python(prices)

    def calculate_rsi_redis(self, prices: List[float], period: int = 14) -> float:
        try:
            result = self._call_script("RSI", prices, period)
            return float(result)
        except Exception as exc:
            self.logger.debug("Redis RSI fallback: %s", exc)
            return self._calculate_rsi_python(prices, period)

    def calculate_vwap_redis(self, tick_data: List[Dict[str, Any]]) -> float:
        try:
            result = self._call_script("VWAP", tick_data)
            return float(result)
        except Exception as exc:
            self.logger.debug("Redis VWAP fallback: %s", exc)
            return self._calculate_vwap_python(tick_data)


    def calculate_indicators_batch(
        self, symbol: str, price_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not price_data:
            return {}
        try:
            response = self._call_script(
                "BATCH",
                price_data.get("prices", []),
                price_data.get("highs", []),
                price_data.get("lows", []),
                price_data.get("volumes", []),
            )
            if isinstance(response, str):
                return json.loads(response or "{}")
            if isinstance(response, dict):
                return response
        except Exception as exc:
            self.logger.debug("Redis batch calculation fallback: %s", exc)
        return self._python_indicators_fallback(price_data)

    # ------------------------------------------------------------------ #
    # Python fallbacks (existing implementations)
    # ------------------------------------------------------------------ #


    def _calculate_vwap_python(self, tick_data: List[Dict[str, Any]]) -> float:
        total_volume = 0.0
        weighted_sum = 0.0
        for tick in tick_data or []:
            # Try different volume fields in order of preference
            volume = (float(tick.get("zerodha_cumulative_volume", 0) or 0) or
                     float(tick.get("bucket_incremental_volume", 0) or 0) or
                     float(tick.get("bucket_cumulative_volume", 0) or 0) or 0)
            price = float(tick.get("last_price", 0) or 0)
            if volume > 0 and price > 0:
                total_volume += volume
                weighted_sum += volume * price
        return weighted_sum / total_volume if total_volume > 0 else 0.0

    def _calculate_atr_python(
        self,
        high_data: List[float],
        low_data: List[float],
        close_data: List[float],
        period: int,
    ) -> float:
        if (
            len(high_data) < period + 1
            or len(low_data) < period + 1
            or len(close_data) < period + 1
        ):
            return 0.0

        true_ranges = []
        for i in range(1, len(high_data)):
            high = float(high_data[i])
            low = float(low_data[i])
            prev_close = float(close_data[i - 1])
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            true_ranges.append(max(tr1, tr2, tr3))

        if len(true_ranges) < period:
            return 0.0

        return sum(true_ranges[-period:]) / period

    def _calculate_rsi_python(self, prices: List[float], period: int) -> float:
        if len(prices) < period + 1:
            return 50.0

        gains = 0.0
        losses = 0.0
        for i in range(1, period + 1):
            change = float(prices[i]) - float(prices[i - 1])
            if change > 0:
                gains += change
            else:
                losses -= change

        avg_gain = gains / period
        avg_loss = losses / period

        for i in range(period + 1, len(prices)):
            change = float(prices[i]) - float(prices[i - 1])
            if change > 0:
                avg_gain = (avg_gain * (period - 1) + change) / period
                avg_loss = (avg_loss * (period - 1)) / period
            else:
                avg_gain = (avg_gain * (period - 1)) / period
                avg_loss = (avg_loss * (period - 1) - change) / period

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calculate_ema_python(self, prices: List[float], period: int) -> float:
        if not prices:
            return 0.0
        if len(prices) < period:
            return float(prices[-1])

        alpha = 2.0 / (period + 1.0)
        ema = float(prices[0])
        for price in prices[1:]:
            ema = float(price) * alpha + ema * (1 - alpha)
        return ema

    def _calculate_ema_all_python(self, prices: List[float]) -> Dict[str, float]:
        """Calculate all EMA windows using Python fallback"""
        periods = [5, 10, 20, 50, 100, 200]
        result = {}
        for period in periods:
            result[f"ema_{period}"] = self._calculate_ema_python(prices, period)
        return result


    def _python_indicators_fallback(self, price_data: Dict[str, Any]) -> Dict[str, Any]:
        prices = price_data.get("prices", []) or []
        highs = price_data.get("highs", []) or []
        lows = price_data.get("lows", []) or []
        volumes = price_data.get("volumes", []) or []

        close_values = prices or highs or lows

        result = {
            "ema_5": self._calculate_ema_python(prices, 5) if prices else 0.0,
            "ema_10": self._calculate_ema_python(prices, 10) if prices else 0.0,
            "ema_20": self._calculate_ema_python(prices, 20) if prices else 0.0,
            "ema_50": self._calculate_ema_python(prices, 50) if prices else 0.0,
            "ema_100": self._calculate_ema_python(prices, 100) if prices else 0.0,
            "ema_200": self._calculate_ema_python(prices, 200) if prices else 0.0,
            "rsi": self._calculate_rsi_python(prices, 14) if prices else 50.0,
            "atr": self._calculate_atr_python(highs, lows, close_values, 14)
            if highs and lows and close_values
            else 0.0,
            "vwap": self._calculate_vwap_python(
                [{"price": p, "volume": v} for p, v in zip(prices, volumes)]
            )
            if prices and volumes
            else 0.0,
        }
        return result


RedisCalculations = OptimizedRedisCalculations

__all__ = ["OptimizedRedisCalculations", "RedisCalculations"]
