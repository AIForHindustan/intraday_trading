"""
Alert stream utilities for validation transparency.

Provides a shared interface for publishing alerts that require outcome
validation and for recording the eventual validation results. Everything
persists through Redis using the standardized key structure.

Enhanced with:
- Full rolling window data capture
- Chart visualization integration
- Human-readable publishing format
- Success/failure tracking with justification
"""

from __future__ import annotations

import asyncio
import json
import time
import base64
import io
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta

from config.utils.timestamp_normalizer import TimestampNormalizer

# Chart visualization imports
try:
    import matplotlib
    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    import pandas as pd
    import numpy as np
    CHART_AVAILABLE = True
except ImportError:
    CHART_AVAILABLE = False
    matplotlib = None
    pandas = None
    numpy = None


class AlertStream:
    """Redis-backed alert stream helper."""

    def __init__(self, redis_client):
        self.redis = redis_client
        self.pending_alerts_stream = "alerts:pending:validation"
        self.validation_results_stream = "alerts:validation:results"
        self.performance_stats_key = "alert_performance:stats"
        self.alert_metadata_prefix = "alert:metadata"
        self.rolling_windows_key = "alert:rolling_windows"
        self.chart_cache_key = "alert:charts"

    # ------------------------------------------------------------------ #
    # Public publishing helpers (sync + async)
    # ------------------------------------------------------------------ #

    def publish_alert_for_validation_sync(self, alert_data: Dict) -> str:
        """Synchronously register an alert for outcome validation with full rolling window data."""
        alert_id = alert_data.get("alert_id") or self._generate_alert_id(
            alert_data.get("symbol", "UNKNOWN")
        )
        stream_payload = self._build_stream_payload(alert_id, alert_data)

        # Capture rolling window data
        rolling_data = self._capture_rolling_windows(alert_data)
        stream_payload.update(rolling_data)

        # Write to stream (bounded length)
        self.redis.xadd(
            self.pending_alerts_stream,
            stream_payload,
            maxlen=1000,
            approximate=True,
        )

        # Persist metadata for later stats aggregation
        metadata_key = self._metadata_key(alert_id)
        metadata_payload = {
            "alert_id": alert_id,
            "symbol": stream_payload["symbol"],
            "pattern": stream_payload["pattern"],
            "confidence": stream_payload.get("confidence", 0.0),
            "signal": stream_payload.get("signal", "NEUTRAL"),
            "timestamp_ms": stream_payload["timestamp_ms"],
            "rolling_windows": json.dumps(rolling_data.get("rolling_windows", {})),
        }

        # ✅ CONSISTENCY: Use RedisManager82 for direct client access
        from redis_files.redis_manager import RedisManager82
        redis_core = getattr(self.redis, "redis_client", None) or RedisManager82.get_client(
            process_name="alert_validator",
            db=0,
            max_connections=None
        )
        if redis_core:
            pipe = redis_core.pipeline()
            pipe.hset(metadata_key, mapping=metadata_payload)
            pipe.expire(metadata_key, 86400)  # keep for 24h
            pipe.execute()

        return alert_id

    async def publish_alert_for_validation(self, alert_data: Dict) -> str:
        """Async wrapper for publish_alert_for_validation_sync."""
        return await asyncio.to_thread(self.publish_alert_for_validation_sync, alert_data)

    def publish_validation_result_sync(
        self, alert_id: str, outcome: Dict, metadata: Optional[Dict] = None
    ) -> None:
        """Synchronously record the validation outcome with full data and chart."""
        metadata = metadata or self._load_metadata(alert_id)
        
        # Generate chart if data is available
        chart_data = None
        if CHART_AVAILABLE and metadata:
            chart_data = self._generate_validation_chart(alert_id, outcome, metadata)
        
        result_payload = {
            "alert_id": alert_id,
            "status": outcome.get("status", "INCONCLUSIVE"),
            "price_movement_pct": float(outcome.get("price_movement_pct", 0.0)),
            "duration_minutes": int(outcome.get("duration_minutes", 0)),
            "max_move_pct": float(outcome.get("max_move_pct", 0.0)),
            "details": outcome.get("details", ""),
            "validation_timestamp_ms": int(time.time() * 1000),
            "rolling_windows": metadata.get("rolling_windows", "{}") if metadata else "{}",
            "chart_data": chart_data,
            "human_readable": self._format_human_readable_result(alert_id, outcome, metadata),
        }

        self.redis.xadd(
            self.validation_results_stream,
            result_payload,
            maxlen=1000,
            approximate=True,
        )

        if metadata:
            self._update_performance_stats_sync(result_payload, metadata)

    async def publish_validation_result(
        self, alert_id: str, outcome: Dict, metadata: Optional[Dict] = None
    ) -> None:
        """Async wrapper for publish_validation_result_sync."""
        await asyncio.to_thread(
            self.publish_validation_result_sync, alert_id, outcome, metadata
        )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _generate_alert_id(self, symbol: str) -> str:
        return f"alert_{int(time.time() * 1000)}_{symbol}"

    def _metadata_key(self, alert_id: str) -> str:
        return f"{self.alert_metadata_prefix}:{alert_id}"

    def _load_metadata(self, alert_id: str) -> Optional[Dict]:
        # ✅ CONSISTENCY: Use RedisManager82 for direct client access
        from redis_files.redis_manager import RedisManager82
        redis_core = getattr(self.redis, "redis_client", None) or RedisManager82.get_client(
            process_name="alert_validator",
            db=0,
            max_connections=None
        )
        if not redis_core:
            return None
        metadata = redis_core.hgetall(self._metadata_key(alert_id))
        if not metadata:
            return None
        return {
            key: self._coerce_metadata_value(value)
            for key, value in metadata.items()
        }

    def _coerce_metadata_value(self, value):
        if value is None:
            return value
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8")
        try:
            return float(value)
        except (TypeError, ValueError):
            return value

    def _build_stream_payload(self, alert_id: str, alert_data: Dict) -> Dict:
        symbol = alert_data.get("symbol", "UNKNOWN")
        pattern = alert_data.get("pattern", "unknown")
        confidence = float(alert_data.get("confidence", 0.0) or 0.0)
        signal = alert_data.get("signal", "NEUTRAL")

        timestamp_ms = alert_data.get("timestamp_ms")
        if timestamp_ms is None:
            timestamp_ms = TimestampNormalizer.to_epoch_ms(
                alert_data.get("timestamp") or time.time() * 1000
            )

        last_price = alert_data.get("last_price") or alert_data.get("price") or 0.0
        volume_ratio = alert_data.get("volume_ratio", 0.0)
        expected_move = alert_data.get("expected_move", 0.0)

        return {
            "alert_id": alert_id,
            "symbol": symbol,
            "pattern": pattern,
            "confidence": confidence,
            "signal": signal,
            "timestamp_ms": int(timestamp_ms),
            "price_at_alert": float(last_price),
            "volume_ratio": float(volume_ratio or 0.0),
            "expected_move": float(expected_move or 0.0),
        }

    def _update_performance_stats_sync(self, result_payload: Dict, metadata: Dict) -> None:
        # ✅ CONSISTENCY: Use RedisManager82 for direct client access
        from redis_files.redis_manager import RedisManager82
        redis_core = getattr(self.redis, "redis_client", None) or RedisManager82.get_client(
            process_name="alert_validator",
            db=0,
            max_connections=None
        )
        if not redis_core:
            return

        status = str(result_payload.get("status", "INCONCLUSIVE")).upper()
        movement = float(result_payload.get("price_movement_pct", 0.0))
        pattern = str(metadata.get("pattern", "unknown")).lower()

        total_key = self.performance_stats_key
        pattern_key = f"{self.performance_stats_key}:{pattern}"

        pipe = redis_core.pipeline()
        pipe.hincrby(total_key, "total_alerts", 1)
        if status == "SUCCESS":
            pipe.hincrby(total_key, "success_count", 1)
        elif status == "FAILURE":
            pipe.hincrby(total_key, "failure_count", 1)
        else:
            pipe.hincrby(total_key, "inconclusive_count", 1)

        pipe.hset(total_key, mapping={"last_updated": int(time.time() * 1000)})

        pipe.hincrby(pattern_key, "total_alerts", 1)
        if status == "SUCCESS":
            pipe.hincrby(pattern_key, "success_count", 1)
        elif status == "FAILURE":
            pipe.hincrby(pattern_key, "failure_count", 1)
        else:
            pipe.hincrby(pattern_key, "inconclusive_count", 1)
        pipe.hincrbyfloat(pattern_key, "total_movement", movement)
        pipe.hset(pattern_key, mapping={"last_updated": int(time.time() * 1000)})

        pipe.execute()

    # Convenience async wrappers for direct stat updates
    async def update_performance_stats(self, outcome: Dict, metadata: Dict) -> None:
        await asyncio.to_thread(self._update_performance_stats_sync, outcome, metadata)

    # ------------------------------------------------------------------ #
    # Enhanced methods for rolling windows, charts, and human-readable format
    # ------------------------------------------------------------------ #

    def _capture_rolling_windows(self, alert_data: Dict) -> Dict:
        """Capture rolling window data for comprehensive analysis."""
        symbol = alert_data.get("symbol", "UNKNOWN")
        windows = [5, 10, 15, 30, 60]  # minutes
        
        rolling_data = {
            "rolling_windows": {},
            "volume_profile": {},
            "price_profile": {},
        }
        
        try:
            # ✅ CONSISTENCY: Use RedisManager82 for direct client access
            from redis_files.redis_manager import RedisManager82
            redis_core = getattr(self.redis, "redis_client", None) or RedisManager82.get_client(
                process_name="alert_validator",
                db=0,
                max_connections=None
            )
            if not redis_core:
                return rolling_data
            
            # Get recent price data for rolling windows
            price_key = f"ohlc_latest:{symbol}"
            price_data = redis_core.hgetall(price_key)
            
            if price_data:
                rolling_data["price_profile"] = {
                    "current_price": float(price_data.get("close", 0)),
                    "open": float(price_data.get("open", 0)),
                    "high": float(price_data.get("high", 0)),
                    "low": float(price_data.get("low", 0)),
                    "volume": int(price_data.get("volume", 0)),
                }
            
            # Get volume baseline data
            volume_key = f"volume_averages:{symbol}"
            volume_data = redis_core.hgetall(volume_key)
            
            if volume_data:
                rolling_data["volume_profile"] = {
                    "avg_volume_20d": float(volume_data.get("avg_volume_20d", 0)),
                    "avg_volume_55d": float(volume_data.get("avg_volume_55d", 0)),
                    "turtle_ready": volume_data.get("turtle_ready", "false") == "true",
                }
            
            # Calculate rolling window metrics
            for window in windows:
                rolling_data["rolling_windows"][f"{window}m"] = {
                    "window_minutes": window,
                    "volume_ratio": alert_data.get("volume_ratio", 0.0),
                    "confidence": alert_data.get("confidence", 0.0),
                    "signal_strength": self._calculate_signal_strength(alert_data, window),
                }
                
        except Exception as e:
            print(f"Warning: Could not capture rolling windows for {symbol}: {e}")
        
        return rolling_data

    def _calculate_signal_strength(self, alert_data: Dict, window: int) -> float:
        """Calculate signal strength for a given time window."""
        confidence = alert_data.get("confidence", 0.0)
        volume_ratio = alert_data.get("volume_ratio", 0.0)
        
        # Weighted calculation based on window size
        window_weight = min(window / 60.0, 1.0)  # Normalize to 1-hour max
        signal_strength = (confidence * 0.6 + volume_ratio * 0.4) * window_weight
        
        return min(signal_strength, 1.0)

    def _generate_validation_chart(self, alert_id: str, outcome: Dict, metadata: Dict) -> Optional[str]:
        """Generate chart visualization for validation result."""
        if not CHART_AVAILABLE:
            return None
        
        try:
            symbol = metadata.get("symbol", "UNKNOWN")
            pattern = metadata.get("pattern", "unknown")
            status = outcome.get("status", "INCONCLUSIVE")
            
            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            fig.suptitle(f"Alert Validation: {symbol} - {pattern}", fontsize=14, fontweight='bold')
            
            # Price movement chart
            price_movement = outcome.get("price_movement_pct", 0.0)
            max_move = outcome.get("max_move_pct", 0.0)
            
            ax1.bar(['Price Movement', 'Max Movement'], [price_movement, max_move], 
                   color=['green' if price_movement > 0 else 'red', 'blue'])
            ax1.set_title(f"Price Movement Analysis - Status: {status}")
            ax1.set_ylabel("Percentage (%)")
            ax1.grid(True, alpha=0.3)
            
            # Rolling windows analysis
            rolling_windows = json.loads(metadata.get("rolling_windows", "{}"))
            if rolling_windows:
                windows = list(rolling_windows.keys())
                # ✅ FIX: Handle case where rolling_windows[w] might be an int/dict
                confidences = []
                signal_strengths = []
                for w in windows:
                    window_data = rolling_windows[w]
                    if isinstance(window_data, dict):
                        confidences.append(window_data.get("confidence", 0.0))
                        signal_strengths.append(window_data.get("signal_strength", 0.0))
                    else:
                        # If window_data is not a dict (e.g., int/float), use defaults
                        confidences.append(0.0)
                        signal_strengths.append(0.0)
                
                x = range(len(windows))
                ax2.plot(x, confidences, 'o-', label='Confidence', color='blue')
                ax2.plot(x, signal_strengths, 's-', label='Signal Strength', color='red')
                ax2.set_title("Rolling Windows Analysis")
                ax2.set_xlabel("Time Windows")
                ax2.set_ylabel("Score")
                ax2.set_xticks(x)
                ax2.set_xticklabels(windows)
                ax2.legend()
                ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # Convert to base64 string
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            chart_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close(fig)
            
            return chart_base64
            
        except Exception as e:
            print(f"Warning: Could not generate chart for {alert_id}: {e}")
            return None

    def _format_human_readable_result(self, alert_id: str, outcome: Dict, metadata: Optional[Dict]) -> str:
        """Format validation result in human-readable format for Telegram/Reddit."""
        if not metadata:
            return f"Alert {alert_id}: No metadata available"
        
        symbol = metadata.get("symbol", "UNKNOWN")
        pattern = metadata.get("pattern", "unknown")
        status = outcome.get("status", "INCONCLUSIVE")
        price_movement = outcome.get("price_movement_pct", 0.0)
        duration = outcome.get("duration_minutes", 0)
        max_move = outcome.get("max_move_pct", 0.0)
        details = outcome.get("details", "")
        
        # Format status with appropriate indicators
        status_icon = "✅" if status == "SUCCESS" else "❌" if status == "FAILURE" else "⏳"
        
        result_text = f"""
ALERT VALIDATION RESULT
======================

Symbol: {symbol}
Pattern: {pattern}
Status: {status_icon} {status}
Price Movement: {price_movement:+.2f}%
Duration: {duration} minutes
Max Movement: {max_move:+.2f}%

Justification:
{details}

Rolling Windows Analysis:"""
        
        # Add rolling windows data if available
        try:
            rolling_windows = json.loads(metadata.get("rolling_windows", "{}"))
            for window, data in rolling_windows.items():
                # ✅ FIX: Handle case where data might be an int/dict
                if isinstance(data, dict):
                    confidence = data.get("confidence", 0.0)
                    signal_strength = data.get("signal_strength", 0.0)
                else:
                    # If data is not a dict (e.g., int/float), use defaults
                    confidence = 0.0
                    signal_strength = 0.0
                result_text += f"\n{window}: Confidence={confidence:.2f}, Signal={signal_strength:.2f}"
        except:
            result_text += "\nRolling windows data not available"
        
        result_text += f"\n\nAlert ID: {alert_id}"
        result_text += f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        return result_text.strip()

    def get_performance_summary(self) -> Dict:
        """Get comprehensive performance summary with rolling windows data."""
        try:
            # ✅ CONSISTENCY: Use RedisManager82 for direct client access
            from redis_files.redis_manager import RedisManager82
            redis_core = getattr(self.redis, "redis_client", None) or RedisManager82.get_client(
                process_name="alert_validator",
                db=0,
                max_connections=None
            )
            if not redis_core:
                return {}
            
            # Get overall stats
            total_stats = redis_core.hgetall(self.performance_stats_key)
            
            # Get pattern-specific stats
            pattern_keys = list(redis_core.scan_iter(match=f"{self.performance_stats_key}:*", count=500))
            pattern_stats = {}
            
            for key in pattern_keys:
                pattern_key = key.decode() if isinstance(key, (bytes, bytearray)) else key
                pattern_name = pattern_key.split(":")[-1]
                pattern_data = redis_core.hgetall(pattern_key)
                pattern_stats[pattern_name] = {
                    "total_alerts": int(pattern_data.get("total_alerts", 0)),
                    "success_count": int(pattern_data.get("success_count", 0)),
                    "failure_count": int(pattern_data.get("failure_count", 0)),
                    "inconclusive_count": int(pattern_data.get("inconclusive_count", 0)),
                    "total_movement": float(pattern_data.get("total_movement", 0.0)),
                    "success_rate": self._calculate_success_rate(pattern_data),
                }
            
            return {
                "overall": {
                    "total_alerts": int(total_stats.get("total_alerts", 0)),
                    "success_count": int(total_stats.get("success_count", 0)),
                    "failure_count": int(total_stats.get("failure_count", 0)),
                    "inconclusive_count": int(total_stats.get("inconclusive_count", 0)),
                    "success_rate": self._calculate_success_rate(total_stats),
                },
                "patterns": pattern_stats,
                "last_updated": int(time.time() * 1000),
            }
            
        except Exception as e:
            print(f"Warning: Could not get performance summary: {e}")
            return {}

    def _calculate_success_rate(self, stats: Dict) -> float:
        """Calculate success rate from stats."""
        total = int(stats.get("total_alerts", 0))
        success = int(stats.get("success_count", 0))
        
        if total == 0:
            return 0.0
        
        return (success / total) * 100.0
