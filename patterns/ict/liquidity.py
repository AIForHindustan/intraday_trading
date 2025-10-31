#!/usr/bin/env python3
"""
ICT Liquidity Detector utilities.
Updated: 2025-10-27
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List, Optional, Any


class ICTLiquidityDetector:
    """Detects previous day/week highs/lows and simple stop-hunt zones."""

    def __init__(self, redis_client=None):
        self.redis_client = redis_client

    def detect_liquidity_pools(self, symbol: str) -> Dict[str, Optional[float]]:
        prev_day = self._get_previous_trading_day()
        weekly_dates = self._get_recent_trading_days(5)

        pd_high = self._get_session_high(symbol, prev_day)
        pd_low = self._get_session_low(symbol, prev_day)

        # Weekly from last up to 5 sessions (excluding today)
        w_high, w_low = self._get_week_extremes(symbol, weekly_dates)

        pools = {
            "previous_day_high": pd_high,
            "previous_day_low": pd_low,
            "weekly_high": w_high,
            "weekly_low": w_low,
        }
        pools["stop_hunt_zones"] = self._calculate_stop_hunt_zones(pools)
        return pools

    def _get_previous_trading_day(self) -> str:
        # Simple: yesterday; if weekend, walk back
        d = date.today() - timedelta(days=1)
        # Walk back up to 3 days to skip weekend
        for _ in range(3):
            if d.weekday() < 5:
                break
            d = d - timedelta(days=1)
        return d.isoformat()

    def _get_recent_trading_days(self, count: int) -> List[str]:
        days = []
        d = date.today() - timedelta(days=1)
        while len(days) < count and (date.today() - d).days <= 10:
            if d.weekday() < 5:
                days.append(d.isoformat())
            d = d - timedelta(days=1)
        return days

    def _get_session_high(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        if not session_date or not self.redis_client:
            return None
        try:
            data = self.redis_client.get_cumulative_data(symbol, session_date)
            if data and data.get("high") not in (None, float("-inf")):
                return float(data.get("high"))
        except Exception:
            pass
        return None

    def _get_session_low(self, symbol: str, session_date: Optional[str]) -> Optional[float]:
        if not session_date or not self.redis_client:
            return None
        try:
            data = self.redis_client.get_cumulative_data(symbol, session_date)
            if data and data.get("low") not in (None, float("inf")):
                return float(data.get("low"))
        except Exception:
            pass
        return None

    def _get_week_extremes(self, symbol: str, session_dates: List[str]) -> (Optional[float], Optional[float]):
        highs: List[float] = []
        lows: List[float] = []
        if not self.redis_client:
            return None, None
        for s in session_dates:
            try:
                data = self.redis_client.get_cumulative_data(symbol, s)
                if not data:
                    continue
                hp = data.get("high")
                lp = data.get("low")
                if isinstance(hp, (int, float)):
                    highs.append(float(hp))
                if isinstance(lp, (int, float)):
                    lows.append(float(lp))
            except Exception:
                continue
        return (max(highs) if highs else None, min(lows) if lows else None)

    def _calculate_stop_hunt_zones(self, pools: Dict[str, Optional[float]]) -> List[float]:
        levels = [
            pools.get("previous_day_high"),
            pools.get("previous_day_low"),
            pools.get("weekly_high"),
            pools.get("weekly_low"),
        ]
        hunt_zones: List[float] = []
        for level in levels:
            try:
                if level and level > 0:
                    hunt_zones.append(level * 1.005)
                    hunt_zones.append(level * 0.995)
            except Exception:
                continue
        return hunt_zones
