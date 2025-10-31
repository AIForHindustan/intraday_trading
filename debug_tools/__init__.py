"""Utility package with debugging aids for binary ingestion."""

from .debug_error_tracker import DebugErrorTracker, error_tracker, reset_error_tracker

__all__ = [
    "DebugErrorTracker",
    "error_tracker",
    "reset_error_tracker",
]
