from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

from binary_to_parquet.production_binary_converter import (
    ProductionZerodhaBinaryConverter,
    VALID_PACKET_LENGTHS,
)

from .debug_error_tracker import DebugErrorTracker, error_tracker, reset_error_tracker


class DebugBinaryConverter(ProductionZerodhaBinaryConverter):
    """Augmented converter that records detailed packet and enrichment failures."""

    def __init__(
        self,
        *args,
        tracker: Optional[DebugErrorTracker] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._tracker = tracker or error_tracker

    # ------------------------------------------------------------------
    # Packet parsing instrumentation
    # ------------------------------------------------------------------

    def _detect_packet_format(self, raw_data: bytes) -> List[Dict]:
        data_len = len(raw_data)
        print(f"🔍 Analyzing {data_len} bytes of data")

        if self._looks_like_websocket_frame(raw_data):
            print("📦 Detected WebSocket frame format")
            packets = super()._parse_websocket_frames(raw_data)
            print(f"📊 Detection stats: {len(packets)} websocket packets parsed")
            return packets

        packets: List[Dict] = []
        position = 0
        detection_attempts = 0
        successful = 0

        while position + 8 <= data_len:
            parsed = False
            for packet_length in VALID_PACKET_LENGTHS:
                end_pos = position + packet_length
                if end_pos > data_len:
                    continue
                detection_attempts += 1
                packet = self._parse_single_packet(raw_data[position:end_pos], packet_length)
                if packet:
                    packets.append(packet)
                    position = end_pos
                    successful += 1
                    parsed = True
                    break
            if not parsed:
                self._tracker.track_error(
                    "PACKET_DETECTION_FAILED",
                    context=f"position={position}, remaining={data_len - position}",
                    packet_data=raw_data[position : position + 16],
                )
                position += 1

            if packets and len(packets) % 10_000 == 0:
                print(f"  ↳ Progress: {len(packets)} packets detected")

        print(f"📊 Detection stats: {successful}/{detection_attempts or 1} successful")
        return packets

    def _parse_single_packet(self, packet_data: bytes, packet_length: int) -> Optional[Dict]:
        try:
            packet = super()._parse_single_packet(packet_data, packet_length)
        except Exception as exc:  # noqa: BLE001
            self._tracker.track_error(
                "PACKET_PARSE_EXCEPTION",
                context=f"length={packet_length}, error={exc}",
                packet_data=packet_data,
            )
            return None

        if not packet:
            self._tracker.track_error(
                "PACKET_PARSE_EMPTY",
                context=f"length={packet_length}",
                packet_data=packet_data,
            )
        return packet

    # ------------------------------------------------------------------
    # Metadata enrichment instrumentation
    # ------------------------------------------------------------------

    def _enrich_packets(
        self,
        batch: List[Dict],
        file_path: Path,
        conn,
    ) -> Tuple[List[Dict], int]:
        enriched: List[Dict] = []
        errors = 0

        for idx, packet in enumerate(batch):
            if not packet:
                errors += 1
                self._tracker.track_error(
                    "EMPTY_PACKET_OBJECT",
                    context=f"file={file_path}, batch_index={idx}",
                )
                continue
            try:
                result = super()._enrich_with_complete_metadata(conn, packet, file_path)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self._tracker.track_error(
                    "METADATA_ENRICHMENT_EXCEPTION",
                    context=f"file={file_path.name}, token={packet.get('instrument_token')}, error={exc}",
                )
                continue

            if not result:
                errors += 1
                self._tracker.track_error(
                    "METADATA_ENRICHMENT_EMPTY",
                    context=f"file={file_path.name}, token={packet.get('instrument_token')}",
                )
                continue

            enriched.append(result)

        return enriched, errors


def get_debug_converter(**kwargs) -> DebugBinaryConverter:
    reset_error_tracker()
    return DebugBinaryConverter(**kwargs, tracker=error_tracker)
