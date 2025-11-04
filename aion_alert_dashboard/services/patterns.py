"""
Pattern normalisation helpers for the alert dashboard.

Centralises the mapping between raw pattern names coming from the
pipeline and the snake_case keys we use for filtering and lookups.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional


@lru_cache(maxsize=256)
def normalize_pattern_name(pattern: Optional[str]) -> str:
    """Normalise any pattern identifier to snake_case."""
    if not pattern:
        return "unknown"
    normalized = pattern.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized or "unknown"


@lru_cache(maxsize=256)
def pattern_display_label(pattern_key: str, original: Optional[str] = None) -> str:
    """Return a human-friendly label for a pattern key."""
    source = original or pattern_key or "unknown"
    text = source.replace("_", " ").strip()
    if not text:
        text = "unknown"
    words = []
    for word in text.split():
        if word.isupper():
            words.append(word)
        else:
            words.append(word.title())
    return " ".join(words)

