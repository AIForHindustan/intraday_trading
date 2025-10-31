"""
Centralized routing layer between pattern mathematics and risk calculators.

Provides a unified interface so legacy detectors can request confidence,
position sizing, and news impact without hard-coding the underlying engine.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class MathDispatcher:
    """Thin adapter that delegates to the configured math and risk engines."""

    def __init__(self, pattern_math: Any, risk_calculator: Any):
        self.pattern_math = pattern_math
        self.risk_calculator = risk_calculator

    def calculate_position_size(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        Determine position sizing using the preferred engine.

        Falls back to the risk calculator if the pattern math object does not
        implement the requested method.
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_position_size"):
            return self.pattern_math.calculate_position_size(pattern_type, context)

        if self.risk_calculator and hasattr(self.risk_calculator, "calculate_position_size"):
            return self.risk_calculator.calculate_position_size(pattern_type, context)

        return None

    def calculate_confidence(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """
        Compute pattern confidence scores using the centralized math engine.
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_confidence"):
            return self.pattern_math.calculate_confidence(pattern_type, context)

        return None

    def calculate_news_boost(self, instrument: str, news_data: Dict[str, Any]) -> Optional[float]:
        """
        Route news impact calculations to the math engine. Returns None when the
        pattern math implementation does not expose a helper.
        """
        if self.pattern_math and hasattr(self.pattern_math, "calculate_news_impact"):
            return self.pattern_math.calculate_news_impact(instrument, news_data)

        return None

    def calculate_stop_loss(self, pattern_type: str, context: Dict[str, Any]) -> Optional[float]:
        """Delegate stop loss calculations."""
        if self.pattern_math and hasattr(self.pattern_math, "calculate_stop_loss"):
            return self.pattern_math.calculate_stop_loss(pattern_type, context)

        if self.risk_calculator and hasattr(self.risk_calculator, "calculate_stop_loss"):
            return self.risk_calculator.calculate_stop_loss(pattern_type, context)

        return None

    def calculate_price_targets(self, pattern_type: str, context: Dict[str, Any]) -> Optional[Dict[str, float]]:
        """Delegate price target calculations."""
        if self.pattern_math and hasattr(self.pattern_math, "calculate_price_targets"):
            return self.pattern_math.calculate_price_targets(pattern_type, context)

        return None
