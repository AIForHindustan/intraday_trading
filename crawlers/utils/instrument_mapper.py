"""
Instrument mapper utilities for Zerodha instrument tokens.

Provides a consistent interface to resolve instrument tokens into human-readable
symbols using the project's instrument reference data.

Used by:
- intraday_scanner/data_pipeline.py and intraday_scanner/scanner_main.py (token→symbol resolution)
- crawlers/websocket_message_parser.py (live stream normalization)
- redis_files/redis_client.py (key generation and metadata lookups)
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union


class InstrumentMapper:
    """Resolve Zerodha instrument tokens to canonical symbols."""

    def __init__(
        self,
        instruments: Optional[Union[Dict, Iterable[Dict]]] = None,
        master_mapping_path: Optional[Path] = None,
        token_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.token_to_symbol_map: Dict[int, str] = {}
        self.token_metadata_map: Dict[int, Dict[str, Any]] = {}
        self.unknown_tokens: set[int] = set()

        # ✅ ALWAYS load from token lookup file first (master source)
        # This ensures we have complete coverage for all tokens
        self._load_default_tokens(master_mapping_path)
        
        # Then overlay with provided metadata (if any) - takes precedence
        if token_metadata:
            self._load_from_metadata(token_metadata)

        # Finally, overlay with instruments (if any) - takes precedence
        self._load_instruments(instruments)

    def token_to_symbol(self, instrument_token: int) -> str:
        """Return the mapped symbol for a given instrument token."""
        try:
            token_int = int(instrument_token)
        except (TypeError, ValueError):
            self.logger.warning("Invalid instrument token: %s", instrument_token)
            return "UNKNOWN"

        symbol = self.token_to_symbol_map.get(token_int)
        if symbol:
            return symbol

        self.unknown_tokens.add(token_int)
        self.logger.warning("Unknown instrument token encountered: %s", token_int)
        return f"UNKNOWN_{token_int}"

    def get_token_metadata(self, instrument_token: int) -> Dict[str, Any]:
        """Return stored metadata for a token, if available."""
        try:
            token_int = int(instrument_token)
        except (TypeError, ValueError):
            return {}
        return self.token_metadata_map.get(token_int, {})

    def export_token_metadata(self) -> Dict[int, Dict[str, Any]]:
        """Export current token metadata for sharing across processes."""
        return dict(self.token_metadata_map)

    def _load_from_metadata(self, metadata: Dict[int, Dict[str, Any]]):
        """Populate mapper from pre-loaded metadata."""
        for key, meta in metadata.items():
            try:
                token_int = int(key)
            except (TypeError, ValueError):
                continue

            clean_meta = dict(meta) if isinstance(meta, dict) else {}
            symbol = (
                clean_meta.get("tradingsymbol")
                or clean_meta.get("symbol")
                or clean_meta.get("name")
            )
            if symbol:
                self.token_to_symbol_map[token_int] = symbol
            self.token_metadata_map[token_int] = clean_meta

    def _load_default_tokens(self, master_mapping_path: Optional[Path]):
        """Populate mapping from master token catalog."""
        token_file = (
            master_mapping_path
            if master_mapping_path is not None
            else Path(__file__).resolve().parents[2] / "core" / "data" / "token_lookup_enriched.json"
        )

        if not token_file.exists():
            self.logger.debug("Token catalog not found at %s", token_file)
            return

        try:
            with open(token_file, "r") as fh:
                raw_mapping = json.load(fh)
        except Exception as exc:
            self.logger.error("Failed to load token catalog %s: %s", token_file, exc)
            return

        # Handle token_lookup.json structure where keys are tokens
        for token_str, entry in raw_mapping.items():
            # In token_lookup.json, the key is the token and entry has different structure
            token = token_str
            # Extract symbol from the 'key' field (e.g., "BFO:BANKEX25OCTFUT" -> "BANKEX")
            key_field = entry.get("key", "")
            if ":" in key_field:
                tradingsymbol = key_field.split(":", 1)[1]
            else:
                tradingsymbol = entry.get("name", "")

            if not token or not tradingsymbol:
                continue

            try:
                token_int = int(token)
            except (TypeError, ValueError):
                continue

            self.token_to_symbol_map.setdefault(token_int, tradingsymbol)
            meta = {
                "tradingsymbol": tradingsymbol,
                "symbol": tradingsymbol,
                "exchange": entry.get("exchange"),
                "instrument_type": entry.get("instrument_type"),
                "segment": entry.get("source"),
                "name": entry.get("name"),
            }
            self.token_metadata_map.setdefault(token_int, meta)

    def _load_instruments(self, instruments: Optional[Union[Dict, Iterable[Dict]]]):
        """Populate internal mapping from the provided instrument data."""
        if instruments is None:
            return

        if isinstance(instruments, dict):
            iterable = instruments.items()
        else:
            iterable = enumerate(instruments)

        for key, meta in iterable:
            token = meta if isinstance(meta, int) else meta.get("instrument_token", key)
            try:
                token_int = int(token)
            except (TypeError, ValueError):
                continue

            if isinstance(meta, dict):
                symbol = (
                    meta.get("symbol")
                    or meta.get("tradingsymbol")
                    or meta.get("name")
                    or meta.get("exchange_token")
                )
                self.token_metadata_map.setdefault(token_int, dict(meta))
            else:
                symbol = None

            if symbol:
                self.token_to_symbol_map[token_int] = symbol
