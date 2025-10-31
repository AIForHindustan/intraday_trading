#!/usr/bin/env python3
from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Dict, List

import logging

logger = logging.getLogger(__name__)


class PremiumCollectionStrategy:
    """
    Generate premium collection trades using Expected Move and ICT context.
    Strategies: Strangle, Iron Condor, Credit Spreads.
    """

    def __init__(self, redis_client, expected_move_calculator):
        self.redis_client = redis_client
        self.em_calculator = expected_move_calculator
        self.min_credit_pct = 0.015  # 1.5% of underlying
        self.target_probability = 0.70
        self.max_risk_reward = 0.33  # Risk 1 to make >=0.33 (1:3)

    def generate_premium_trades(self, symbol: str, ctx: Dict) -> List[Dict]:
        trades: List[Dict] = []
        price = float(ctx.get("last_price") or 0)
        em_ctx = ctx.get("expected_move_context") or {}
        if not em_ctx:
            # Build from detector fields as fallback
            base_em_percent = float(ctx.get("em_base_percent") or 0.0) / 100.0
            em_ctx = {
                "expected_move": price * base_em_percent,
                "expected_move_percent": base_em_percent,
                "confidence": float(ctx.get("em_confidence") or 0.0),
            }
        if price <= 0 or not em_ctx:
            return trades

        pattern_type = ctx.get("pattern") or ctx.get("pattern_type") or ""
        killzone = ctx.get("killzone") or "unknown"

        # Range-bound: strangle
        if self._is_range_bound_market(symbol, ctx):
            trades += self._generate_strangle(symbol, price, em_ctx, ctx)

        # High vol: iron condor
        if self._is_high_vol_market(symbol, ctx):
            trades += self._generate_iron_condor(symbol, price, em_ctx, ctx)

        # ICT premium/discount → directional credit spreads
        if "ict_premium_zone_short" in pattern_type:
            trades += self._generate_credit_spread(symbol, price, em_ctx, ctx, direction="short")
        if "ict_discount_zone_long" in pattern_type:
            trades += self._generate_credit_spread(symbol, price, em_ctx, ctx, direction="long")

        # Viability filter
        return [t for t in trades if self._is_trade_viable(price, t)]

    # -------------------- Strategies --------------------
    def _generate_strangle(self, symbol: str, price: float, em: Dict, ctx: Dict) -> List[Dict]:
        trades: List[Dict] = []
        em_pct = float(em.get("expected_move_percent") or 0.02)
        call_strike = price * (1 + em_pct * 1.2)
        put_strike = price * (1 - em_pct * 1.2)
        call_prem = self._estimate_premium(price, call_strike, "CE", em, ctx)
        put_prem = self._estimate_premium(price, put_strike, "PE", em, ctx)
        total_credit = call_prem + put_prem
        if total_credit >= price * self.min_credit_pct:
            trades.append(
                {
                    "strategy": "strangle",
                    "symbol": symbol,
                    "actions": [
                        {"type": "SELL", "option_type": "CE", "strike": call_strike, "premium": call_prem},
                        {"type": "SELL", "option_type": "PE", "strike": put_strike, "premium": put_prem},
                    ],
                    "total_credit": total_credit,
                    "max_risk": self._strangle_risk(price, call_strike, put_strike),
                    "probability_profit": self._prob_between(price, call_strike, put_strike, em),
                    "breakevens": {"upper": call_strike + total_credit, "lower": put_strike - total_credit},
                    "expiry": self._get_next_expiry(symbol),
                    "context": {"expected_move": em, "killzone": ctx.get("killzone"), "pattern_type": ctx.get("pattern") or ctx.get("pattern_type")},
                }
            )
        return trades

    def _generate_iron_condor(self, symbol: str, price: float, em: Dict, ctx: Dict) -> List[Dict]:
        trades: List[Dict] = []
        em_pct = float(em.get("expected_move_percent") or 0.02)
        sc = price * (1 + em_pct * 0.8)
        sp = price * (1 - em_pct * 0.8)
        lc = price * (1 + em_pct * 1.5)
        lp = price * (1 - em_pct * 1.5)
        sc_p = self._estimate_premium(price, sc, "CE", em, ctx)
        sp_p = self._estimate_premium(price, sp, "PE", em, ctx)
        lc_p = self._estimate_premium(price, lc, "CE", em, ctx)
        lp_p = self._estimate_premium(price, lp, "PE", em, ctx)
        total_credit = (sc_p + sp_p) - (lc_p + lp_p)
        width = (lc - sc)
        max_risk = max(0.0, width - total_credit)
        if total_credit > 0 and max_risk > 0 and (total_credit / max_risk) >= self.max_risk_reward:
            trades.append(
                {
                    "strategy": "iron_condor",
                    "symbol": symbol,
                    "actions": [
                        {"type": "SELL", "option_type": "CE", "strike": sc, "premium": sc_p},
                        {"type": "BUY", "option_type": "CE", "strike": lc, "premium": -lc_p},
                        {"type": "SELL", "option_type": "PE", "strike": sp, "premium": sp_p},
                        {"type": "BUY", "option_type": "PE", "strike": lp, "premium": -lp_p},
                    ],
                    "total_credit": total_credit,
                    "max_risk": max_risk,
                    "probability_profit": self._prob_condor(price, sc, sp, em),
                    "risk_reward": total_credit / max_risk,
                    "expiry": self._get_next_expiry(symbol),
                    "context": ctx,
                }
            )
        return trades

    def _generate_credit_spread(self, symbol: str, price: float, em: Dict, ctx: Dict, direction: str) -> List[Dict]:
        trades: List[Dict] = []
        em_pct = float(em.get("expected_move_percent") or 0.02)
        if direction == "short":
            s = price * (1 + em_pct * 0.5)
            l = price * (1 + em_pct * 1.0)
            sp = self._estimate_premium(price, s, "CE", em, ctx)
            lp = self._estimate_premium(price, l, "CE", em, ctx)
        else:
            s = price * (1 - em_pct * 0.5)
            l = price * (1 - em_pct * 1.0)
            sp = self._estimate_premium(price, s, "PE", em, ctx)
            lp = self._estimate_premium(price, l, "PE", em, ctx)
        credit = sp - lp
        width = abs(s - l)
        max_risk = max(0.0, width - credit)
        rr = (credit / max_risk) if max_risk > 0 else 0.0
        if credit > 0 and rr >= self.max_risk_reward:
            trades.append(
                {
                    "strategy": f"credit_spread_{direction}",
                    "symbol": symbol,
                    "actions": [
                        {"type": "SELL", "option_type": ("CE" if direction == "short" else "PE"), "strike": s, "premium": sp},
                        {"type": "BUY", "option_type": ("CE" if direction == "short" else "PE"), "strike": l, "premium": -lp},
                    ],
                    "total_credit": credit,
                    "max_risk": max_risk,
                    "risk_reward": rr,
                    "probability_profit": self._prob_spread(price, s, direction, em),
                    "expiry": self._get_next_expiry(symbol),
                    "context": ctx,
                }
            )
        return trades

    # -------------------- Helpers --------------------
    def _estimate_premium(self, spot: float, strike: float, opt_type: str, em: Dict, ctx: Dict) -> float:
        days = max(1, self._get_days_to_expiry(ctx.get("symbol") or ""))
        vol = float(em.get("expected_move_percent") or 0.02) * math.sqrt(365)  # annualized proxy
        t = math.sqrt(days / 365.0)
        intrinsic = max(spot - strike, 0.0) if opt_type == "CE" else max(strike - spot, 0.0)
        extrinsic = spot * vol * t * 0.4
        return intrinsic + extrinsic

    def _strangle_risk(self, spot: float, call_strike: float, put_strike: float) -> float:
        return max(call_strike - spot, 0.0) + max(spot - put_strike, 0.0)

    def _prob_between(self, spot: float, call_strike: float, put_strike: float, em: Dict) -> float:
        em_abs = float(em.get("expected_move") or 0.0)
        width = max(1e-6, (call_strike - put_strike))
        return max(0.5, 1.0 - (em_abs / width))

    def _prob_condor(self, spot: float, short_call_strike: float, short_put_strike: float, em: Dict) -> float:
        return self._prob_between(spot, short_call_strike, short_put_strike, em)

    def _prob_spread(self, spot: float, short_strike: float, direction: str, em: Dict) -> float:
        em_abs = float(em.get("expected_move") or 0.0)
        distance = abs(spot - short_strike)
        return max(0.5, 1.0 - (em_abs / max(1e-6, distance * 2)))

    def _get_next_expiry(self, symbol: str) -> str:
        # Next Thursday for indices by convention (placeholder calculation)
        today = datetime.now()
        # find next Thursday
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        exp = today + timedelta(days=days_ahead)
        return exp.strftime("%Y-%m-%d")

    def _get_days_to_expiry(self, symbol: str) -> int:
        # Placeholder: 7 days; replace with actual symbol parsing if available
        return 7

    def _is_range_bound_market(self, symbol: str, ctx: Dict) -> bool:
        atr_percent = float(ctx.get("atr_percent") or 0.0)
        vix = float(ctx.get("vix") or 0.0)
        return (atr_percent < 0.015 and vix < 18 and ctx.get("killzone") != "opening_drive")

    def _is_high_vol_market(self, symbol: str, ctx: Dict) -> bool:
        vix = float(ctx.get("vix") or 0.0)
        return vix > 20

    def _is_trade_viable(self, spot: float, trade: Dict) -> bool:
        return (
            trade.get("probability_profit", 0) >= self.target_probability
            and trade.get("total_credit", 0) >= spot * self.min_credit_pct
            and trade.get("risk_reward", trade.get("total_credit", 0) / max(trade.get("max_risk", 1e-6), 1e-6))
            <= 1.0 / 2.5
        )

