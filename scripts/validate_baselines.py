#!/usr/bin/env python3
"""
Validate that baseline calculations are working correctly.

This script exercises both HistoricalVolumeBaseline and VolumeBaselineManager
against a representative set of symbols to ensure the Redis-driven baseline
pipeline is functioning.
"""

from __future__ import annotations

from datetime import datetime

from core.data.redis_client import get_redis_client
from utils.historical_volume_baseline import HistoricalVolumeBaseline
from utils.volume_baselines import VolumeBaselineManager


def validate_baseline_calculations() -> None:
    """Test baseline calculations for key symbols."""
    redis_client = get_redis_client()
    historical_baseline = HistoricalVolumeBaseline(redis_client)
    baseline_manager = VolumeBaselineManager(redis_client)

    test_symbols = ["RELIANCE", "TCS", "INFY", "NIFTY 50", "NIFTY BANK"]
    current_time = datetime.now()

    print("🔍 Validating Baseline Calculations...")
    print("=" * 60)

    for symbol in test_symbols:
        try:
            hist_baseline = historical_baseline.get_session_aware_baseline(
                symbol, current_time
            )
            mgr_baseline = baseline_manager.get_baseline(
                symbol, "5min", 0, current_time
            )

            print(f"\n📊 {symbol}:")
            print(f"   Historical Baseline: {hist_baseline:,.0f}")
            print(f"   Manager Baseline:    {mgr_baseline:,.0f}")
            if hist_baseline > 0:
                print(f"   Ratio:               {mgr_baseline / hist_baseline:.2f}x")
            else:
                print("   Ratio:               N/A")

        except Exception as exc:
            print(f"\n❌ {symbol}: ERROR - {exc}")

    print("\n" + "=" * 60)
    print("✅ Validation complete")


if __name__ == "__main__":
    validate_baseline_calculations()
