#!/usr/bin/env python3
"""
ICT Fair Value Gap detector helpers.
Updated: 2025-10-27
"""
from __future__ import annotations

from typing import Dict, List


class ICTFVGDetector:
    """Detect Fair Value Gaps (FVG) from simple OHLC candles."""

    def detect_fvg(self, candles: List[Dict]) -> List[Dict]:
        fvgs: List[Dict] = []
        if not candles or len(candles) < 3:
            return fvgs

        # Expect each candle to have standardized keys: open, high, low, close, timestamp
        for i in range(2, len(candles)):
            prior = candles[i - 2]
            previous = candles[i - 1]
            current = candles[i]

            try:
                # Bullish FVG: current low > prior high
                if float(current["low"]) > float(prior["high"]):
                    fvgs.append(
                        {
                            "type": "bullish",
                            "gap_low": float(prior["high"]),
                            "gap_high": float(current["low"]),
                            "strength": (float(current["low"]) - float(prior["high"]))
                            / max(1e-9, float(prior["high"])),
                            "timestamp": current.get("timestamp"),
                        }
                    )

                # Bearish FVG: current high < prior low
                elif float(current["high"]) < float(prior["low"]):
                    fvgs.append(
                        {
                            "type": "bearish",
                            "gap_low": float(current["high"]),
                            "gap_high": float(prior["low"]),
                            "strength": (float(prior["low"]) - float(current["high"]))
                            / max(1e-9, float(current["high"])),
                            "timestamp": current.get("timestamp"),
                        }
                    )
            except Exception:
                continue

        return fvgs

    def is_price_in_fvg(self, current_price: float, fvg: Dict) -> bool:
        try:
            return float(fvg["gap_low"]) <= float(current_price) <= float(fvg["gap_high"])
        except Exception:
            return False
