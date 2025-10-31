#!/usr/bin/env python3
"""
ICT Killzone detector helpers.
Updated: 2025-10-27
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Optional, Dict


class ICTKillzoneDetector:
    """Time-of-day killzone detector for Indian market hours (IST)."""

    def __init__(self):
        self.killzones = {
            "opening_drive": {
                "start": time(9, 15),
                "end": time(10, 30),
                "bias": "volatile",
                "priority": "high",
            },
            "midday_drift": {
                "start": time(12, 0),
                "end": time(13, 30),
                "bias": "consolidation",
                "priority": "low",
            },
            "closing_power": {
                "start": time(14, 30),
                "end": time(15, 30),
                "bias": "directional",
                "priority": "high",
            },
        }

    def _time_remaining(self, end_t: time) -> int:
        now = datetime.now()
        end_dt = datetime.combine(now.date(), end_t)
        if now > end_dt:
            return 0
        delta = end_dt - now
        return int(delta.total_seconds() // 60)

    def get_current_killzone(self) -> Optional[Dict]:
        now_t = datetime.now().time()
        for zone_name, info in self.killzones.items():
            if info["start"] <= now_t <= info["end"]:
                return {
                    "zone": zone_name,
                    "bias": info["bias"],
                    "priority": info["priority"],
                    "time_remaining": self._time_remaining(info["end"]),
                }
        return None
