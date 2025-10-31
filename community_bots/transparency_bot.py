"""
Transparency bot wrapper used by the automatic validator.

This keeps the implementation lightweight and resilient: if community bots
are unavailable, messages are simply logged.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional


logger = logging.getLogger(__name__)


class TransparencyBot:
    """Dispatches validation updates to community channels when available."""

    def __init__(self, telegram_bot=None, reddit_bot=None):
        self.telegram_bot = telegram_bot
        self.reddit_bot = reddit_bot

    async def publish_validation_report(self, alert_data: dict, validation_result: dict):
        """Publish validation result to community channels."""
        message = self._format_validation_message(alert_data, validation_result)

        await asyncio.gather(
            self._send_telegram(message),
            self._send_reddit(alert_data, message),
            return_exceptions=True,
        )

    async def publish_daily_report(self, message: str):
        """Publish daily summary."""
        await asyncio.gather(
            self._send_telegram(message),
            self._send_reddit({"pattern": "DAILY_REPORT"}, message),
            return_exceptions=True,
        )

    async def _send_telegram(self, message: str):
        if not self.telegram_bot:
            logger.debug("Telegram bot unavailable for transparency message")
            return
        try:
            await self.telegram_bot.send_message(message)
        except Exception as exc:
            logger.debug(f"Telegram transparency send failed: {exc}")

    async def _send_reddit(self, alert_data: dict, message: str):
        if not self.reddit_bot:
            logger.debug("Reddit bot unavailable for transparency message")
            return
        try:
            title = f"Alert Validation: {alert_data.get('symbol', 'UNKNOWN')} {alert_data.get('pattern', '')}"
            await self.reddit_bot.post_update(title=title, text=message)
        except Exception as exc:
            logger.debug(f"Reddit transparency send failed: {exc}")

    def _format_validation_message(self, alert: dict, result: dict) -> str:
        symbol = alert.get("symbol", "UNKNOWN")
        pattern = alert.get("pattern", "unknown")
        signal = alert.get("signal", "NEUTRAL")
        confidence = float(alert.get("confidence", 0.0)) * 100

        status_emoji = "✅" if result.get("status") == "SUCCESS" else "❌"
        movement = float(result.get("price_movement_pct", 0.0))

        return (
            f"{status_emoji} <b>ALERT VALIDATION RESULT</b>\n\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>Pattern:</b> {pattern}\n"
            f"<b>Signal:</b> {signal}\n"
            f"<b>Confidence:</b> {confidence:.0f}%\n\n"
            f"<b>Outcome:</b> {result.get('status', 'UNKNOWN')}\n"
            f"<b>Price Movement:</b> {movement:+.2f}%\n"
            f"<b>Validation Window:</b> {result.get('duration_minutes', 0)} minutes\n"
            f"<b>Details:</b> {result.get('details', 'N/A')}\n\n"
            "<i>Automated validation • Zero human intervention</i>"
        )
