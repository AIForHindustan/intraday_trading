"""
Unified Key Builder (Foundation Day 1)
--------------------------------------

Centralizes Redis key prefix definitions and symbol normalization.
This module is the groundwork for converging all Redis access patterns
onto a single source of truth backed by a YAML configuration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from redis_files.redis_key_standards import RedisKeyStandards


class UnifiedSymbolParser:
    """Thin wrapper that delegates to RedisKeyStandards for normalization."""

    def normalize(self, symbol: Optional[str]) -> str:
        if not symbol:
            return ""
        return RedisKeyStandards.canonical_symbol(symbol)


class UnifiedKeyBuilder:
    """
    Declarative Redis key builder that reads prefixes/resolutions from YAML.

    The initial scope focuses on ticks and volume buckets so downstream
    modules can start migrating without touching hard-coded strings.
    """

    def __init__(self, yaml_config_path: Optional[str] = None) -> None:
        self.config_path = self._resolve_config_path(yaml_config_path)
        self.config = self._load_yaml_config(self.config_path)
        self.symbol_parser = UnifiedSymbolParser()

    def normalize_symbol(self, symbol: str) -> str:
        """SINGLE source of truth for symbol normalization."""
        return self.symbol_parser.normalize(symbol)

    def tick_key(self, symbol: str, window: str = "1min", *, normalize: bool = True) -> str:
        if normalize:
            normalized = self.normalize_symbol(symbol)
        else:
            normalized = (symbol or "").strip()
        tick_prefix = self.config["prefixes"]["tick"]
        return f"{tick_prefix}:{normalized}:{window}"

    def volume_bucket_key(self, symbol: str, resolution: str) -> str:
        """
        Map to existing bucket structure without breaking.

        Caller is expected to append hour + bucket index:
        `{builder.volume_bucket_key(...)}:{hour}:{bucket_index}`
        """
        normalized = self.normalize_symbol(symbol)
        resolution_config = self._get_resolution_config(resolution)
        return f"{resolution_config['prefix']}:{normalized}:buckets"

    def volume_history_key(self, symbol: str, resolution: str) -> str:
        normalized = self.normalize_symbol(symbol)
        resolution_config = self._get_resolution_config(resolution)
        history_prefix = resolution_config["history"]
        return f"{history_prefix}:{resolution}:{normalized}"

    def daily_volume_key(self, symbol: str, date_str: str) -> str:
        normalized = self.normalize_symbol(symbol)
        daily_prefix = self.config["prefixes"]["daily_volume"]
        return f"{daily_prefix}:{normalized}:daily:{date_str}"

    def volume_profile_poc_key(self, symbol: str) -> str:
        """Build volume profile Point of Control (POC) key."""
        normalized = self.normalize_symbol(symbol)
        vp_prefix = self.config["prefixes"]["volume_profile"]
        return f"{vp_prefix}:poc:{normalized}"

    def volume_profile_nodes_key(self, symbol: str) -> str:
        """Build volume profile nodes (support/resistance levels) key."""
        normalized = self.normalize_symbol(symbol)
        vp_prefix = self.config["prefixes"]["volume_profile"]
        return f"{vp_prefix}:nodes:{normalized}"

    def volume_profile_session_key(self, symbol: str, date_str: str) -> str:
        """Build volume profile session key for a specific date."""
        normalized = self.normalize_symbol(symbol)
        vp_prefix = self.config["prefixes"]["volume_profile"]
        return f"{vp_prefix}:session:{normalized}:{date_str}"

    def pattern_analysis_key(self, symbol: str, pattern_type: str, window: str) -> str:
        """DB2 pattern analysis list key."""
        normalized = self.normalize_symbol(symbol)
        analysis_prefix = self.config["prefixes"].get("pattern_analysis", "pattern_analysis")
        return f"{analysis_prefix}:{pattern_type}:{normalized}:{window}"

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    def _resolve_config_path(self, yaml_config_path: Optional[str]) -> Path:
        if yaml_config_path:
            candidate = Path(yaml_config_path)
        else:
            candidate = Path("config/key_standards.yaml")

        if not candidate.is_absolute():
            project_root = Path(__file__).resolve().parents[1]
            candidate = project_root / candidate
        return candidate

    def _load_yaml_config(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"Unified key builder config missing: {path}")

        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}

        if "prefixes" not in data or "resolutions" not in data:
            raise ValueError(
                f"Unified key builder config invalid (missing prefixes/resolutions): {path}"
            )
        return data

    def _get_resolution_config(self, resolution: str) -> Dict[str, Any]:
        try:
            return self.config["resolutions"][str(resolution)]
        except KeyError as err:
            raise KeyError(f"Unknown bucket resolution '{resolution}' in key builder") from err


_key_builder_singleton: Optional[UnifiedKeyBuilder] = None


def get_key_builder() -> UnifiedKeyBuilder:
    global _key_builder_singleton
    if _key_builder_singleton is None:
        _key_builder_singleton = UnifiedKeyBuilder()
    return _key_builder_singleton


__all__ = [
    "UnifiedKeyBuilder",
    "UnifiedSymbolParser",
    "get_key_builder",
]

