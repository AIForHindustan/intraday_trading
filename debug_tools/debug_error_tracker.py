from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import DefaultDict, Dict, List, Optional


@dataclass
class ErrorSample:
    timestamp: datetime
    context: Optional[str]
    packet_preview: Optional[str]


class DebugErrorTracker:
    """Collect error categories and lightweight samples for post-run analysis."""

    def __init__(self, sample_limit: int = 5) -> None:
        self.sample_limit = sample_limit
        self.error_counts: DefaultDict[str, int] = defaultdict(int)
        self.error_samples: DefaultDict[str, List[ErrorSample]] = defaultdict(list)

    def reset(self) -> None:
        self.error_counts.clear()
        self.error_samples.clear()

    def track_error(
        self,
        error_type: str,
        context: Optional[str] = None,
        packet_data: Optional[bytes] = None,
    ) -> None:
        """Increment an error bucket and retain the first few examples."""
        self.error_counts[error_type] += 1
        if len(self.error_samples[error_type]) >= self.sample_limit:
            return

        preview: Optional[str] = None
        if isinstance(packet_data, bytes):
            preview = packet_data[:100].hex()
        elif packet_data is not None:
            preview = str(packet_data)[:200]

        self.error_samples[error_type].append(
            ErrorSample(
                timestamp=datetime.utcnow(),
                context=context,
                packet_preview=preview,
            )
        )

    def analyze(self) -> Dict[str, Dict[str, object]]:
        """Return a structured snapshot of errors collected so far."""
        analysis: Dict[str, Dict[str, object]] = {}
        for error_type, count in self.error_counts.items():
            samples = [
                {
                    "timestamp": sample.timestamp.isoformat(timespec="seconds"),
                    "context": sample.context,
                    "packet_preview": sample.packet_preview,
                }
                for sample in self.error_samples.get(error_type, [])
            ]
            analysis[error_type] = {
                "count": count,
                "samples": samples,
            }
        return analysis

    def print_report(self) -> None:
        """Emit a human-readable summary of errors and sample contexts."""
        total = sum(self.error_counts.values())
        print("🔍 ERROR ANALYSIS REPORT")
        print("=" * 60)
        print(f"Total errors tracked: {total}")

        if total == 0:
            print("No errors recorded.")
            return

        for error_type, details in sorted(
            self.analyze().items(), key=lambda item: item[1]["count"], reverse=True
        ):
            count = details["count"]
            percentage = (count / total) * 100 if total else 0.0
            print(f"\n❌ {error_type}: {count} ({percentage:.1f}%)")
            for idx, sample in enumerate(details["samples"][:2], start=1):
                context = sample.get("context")
                preview = sample.get("packet_preview")
                print(f"   Sample {idx}: {context}")
                if preview:
                    print(f"      Packet: {preview[:80]}{'…' if len(preview or '') > 80 else ''}")


# Shared singleton tracker for ad-hoc debugging runs.
error_tracker = DebugErrorTracker()


def reset_error_tracker() -> DebugErrorTracker:
    error_tracker.reset()
    return error_tracker
