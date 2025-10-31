"""
Production-ready Zerodha binary converter.

This module provides a resilient pipeline to convert raw Zerodha WebSocket
binary captures (.dat / .bin) into a normalized DuckDB table with complete
market-depth, token enrichment, and session intelligence.
"""

from __future__ import annotations

import hashlib
import json
import logging
import struct
from collections import defaultdict
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from uuid import uuid4

import duckdb
from typing import TYPE_CHECKING

try:
    import pyarrow as pa
except ImportError as exc:  # pragma: no cover - environment dependency
    pa = None  # type: ignore[assignment]
    PYARROW_IMPORT_ERROR = exc
else:
    PYARROW_IMPORT_ERROR = None

if TYPE_CHECKING:
    from token_cache import TokenCacheManager
from singleton_db import DatabaseConnectionManager

logger = logging.getLogger(__name__)


VALID_PACKET_LENGTHS = (184, 44, 32, 8)
DEFAULT_BATCH_SIZE = 1000

COLUMN_ORDER = [
    "instrument_token",
    "symbol",
    "exchange",
    "segment",
    "instrument_type",
    "expiry",
    "strike_price",
    "option_type",
    "lot_size",
    "tick_size",
    "is_expired",
    "timestamp",
    "exchange_timestamp",
    "exchange_timestamp_ns",
    "last_traded_timestamp",
    "last_traded_timestamp_ns",
    "last_price",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "average_traded_price",
    "volume",
    "last_traded_quantity",
    "total_buy_quantity",
    "total_sell_quantity",
    "open_interest",
    "oi_day_high",
    "oi_day_low",
    "bid_1_price",
    "bid_1_quantity",
    "bid_1_orders",
    "bid_2_price",
    "bid_2_quantity",
    "bid_2_orders",
    "bid_3_price",
    "bid_3_quantity",
    "bid_3_orders",
    "bid_4_price",
    "bid_4_quantity",
    "bid_4_orders",
    "bid_5_price",
    "bid_5_quantity",
    "bid_5_orders",
    "ask_1_price",
    "ask_1_quantity",
    "ask_1_orders",
    "ask_2_price",
    "ask_2_quantity",
    "ask_2_orders",
    "ask_3_price",
    "ask_3_quantity",
    "ask_3_orders",
    "ask_4_price",
    "ask_4_quantity",
    "ask_4_orders",
    "ask_5_price",
    "ask_5_quantity",
    "ask_5_orders",
    "packet_type",
    "data_quality",
    "session_type",
    "source_file",
    "processing_batch",
    "session_id",
]

TIMESTAMP_COLUMNS = {"timestamp", "exchange_timestamp", "last_traded_timestamp"}
DATE_COLUMNS = {"expiry"}
BOOL_COLUMNS = {"is_expired"}
INT32_COLUMNS = {
    *(f"bid_{level}_orders" for level in range(1, 6)),
    *(f"ask_{level}_orders" for level in range(1, 6)),
}
FLOAT_COLUMNS = {
    "strike_price",
    "tick_size",
    "last_price",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "average_traded_price",
    *(f"bid_{level}_price" for level in range(1, 6)),
    *(f"ask_{level}_price" for level in range(1, 6)),
}
INT64_COLUMNS = {
    "instrument_token",
    "lot_size",
    "exchange_timestamp_ns",
    "last_traded_timestamp_ns",
    "volume",
    "last_traded_quantity",
    "total_buy_quantity",
    "total_sell_quantity",
    "open_interest",
    "oi_day_high",
    "oi_day_low",
    *(f"bid_{level}_quantity" for level in range(1, 6)),
    *(f"ask_{level}_quantity" for level in range(1, 6)),
}
STRING_COLUMNS = set(COLUMN_ORDER) - (
    TIMESTAMP_COLUMNS | DATE_COLUMNS | BOOL_COLUMNS | INT32_COLUMNS | FLOAT_COLUMNS | INT64_COLUMNS
)


def _build_column_arrow_types() -> Dict[str, pa.DataType]:
    mapping: Dict[str, pa.DataType] = {}
    for column in COLUMN_ORDER:
        if column in TIMESTAMP_COLUMNS:
            mapping[column] = pa.timestamp("us")
        elif column in DATE_COLUMNS:
            mapping[column] = pa.date32()
        elif column in BOOL_COLUMNS:
            mapping[column] = pa.bool_()
        elif column in INT32_COLUMNS:
            mapping[column] = pa.int32()
        elif column in INT64_COLUMNS:
            mapping[column] = pa.int64()
        elif column in FLOAT_COLUMNS:
            mapping[column] = pa.float64()
        else:
            mapping[column] = pa.string()
    return mapping


if pa is not None:
    COLUMN_ARROW_TYPES = _build_column_arrow_types()
else:
    COLUMN_ARROW_TYPES: Dict[str, "pa.DataType"] = {}


@dataclass
class ProcessingResult:
    """Simple dataclass to capture batch processing statistics."""

    file_path: Path
    total_packets: int
    successful_inserts: int


def ensure_production_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create production-grade tables required by the converter."""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS tick_data_corrected (
        instrument_token BIGINT NOT NULL,
        symbol VARCHAR,
        exchange VARCHAR,
        segment VARCHAR,
        instrument_type VARCHAR,
        expiry DATE,
        strike_price DOUBLE,
        option_type VARCHAR,
        lot_size INTEGER,
        tick_size DOUBLE,
        is_expired BOOLEAN,

        timestamp TIMESTAMP,
        exchange_timestamp TIMESTAMP,
        exchange_timestamp_ns BIGINT,
        last_traded_timestamp TIMESTAMP,
        last_traded_timestamp_ns BIGINT,

        last_price DOUBLE,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        average_traded_price DOUBLE,

        volume BIGINT,
        last_traded_quantity BIGINT,
        total_buy_quantity BIGINT,
        total_sell_quantity BIGINT,

        open_interest BIGINT,
        oi_day_high BIGINT,
        oi_day_low BIGINT,

        bid_1_price DOUBLE, bid_1_quantity BIGINT, bid_1_orders INTEGER,
        bid_2_price DOUBLE, bid_2_quantity BIGINT, bid_2_orders INTEGER,
        bid_3_price DOUBLE, bid_3_quantity BIGINT, bid_3_orders INTEGER,
        bid_4_price DOUBLE, bid_4_quantity BIGINT, bid_4_orders INTEGER,
        bid_5_price DOUBLE, bid_5_quantity BIGINT, bid_5_orders INTEGER,

        ask_1_price DOUBLE, ask_1_quantity BIGINT, ask_1_orders INTEGER,
        ask_2_price DOUBLE, ask_2_quantity BIGINT, ask_2_orders INTEGER,
        ask_3_price DOUBLE, ask_3_quantity BIGINT, ask_3_orders INTEGER,
        ask_4_price DOUBLE, ask_4_quantity BIGINT, ask_4_orders INTEGER,
        ask_5_price DOUBLE, ask_5_quantity BIGINT, ask_5_orders INTEGER,

        packet_type VARCHAR,
        data_quality VARCHAR,
        session_type VARCHAR,

        source_file VARCHAR,
        processing_batch VARCHAR,
        session_id VARCHAR,

        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        parser_version VARCHAR DEFAULT 'v2.0',

        PRIMARY KEY (instrument_token, exchange_timestamp_ns, source_file)
    );

    CREATE TABLE IF NOT EXISTS processed_files_log (
        file_path VARCHAR PRIMARY KEY,
        file_size BIGINT,
        file_hash VARCHAR,
        records_processed INTEGER,
        processing_started TIMESTAMP,
        processing_completed TIMESTAMP,
        status VARCHAR,
        error_message VARCHAR
    );
    """

    conn.execute(schema_sql)


class ProductionZerodhaBinaryConverter:
    """Converter that rewrites raw WebSocket captures into normalized DuckDB tables."""

    def __init__(
        self,
        db_path: Optional[str] = "tick_data_production.db",
        batch_size: int = DEFAULT_BATCH_SIZE,
        token_cache: Optional["TokenCacheManager"] = None,
        ensure_schema: bool = True,
        drop_unknown_tokens: bool = False,
    ) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.token_cache = token_cache
        self.drop_unknown_tokens = drop_unknown_tokens
        self._token_metadata_cache: Dict[int, Dict] = {}
        self._arrow_schema: Optional[pa.Schema] = None
        self._missing_token_counts: Dict[int, int] = defaultdict(int)
        self._logged_missing_tokens: Set[int] = set()
        if self.db_path and ensure_schema:
            with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
                ensure_production_schema(conn)
        self.session_metadata_cache = self._load_all_session_metadata()

    # ------------------------------------------------------------------
    # Session metadata utilities
    # ------------------------------------------------------------------

    def _load_all_session_metadata(self) -> Dict[str, Dict]:
        """Load session metadata from research_data directories."""
        metadata_cache: Dict[str, Dict] = {}
        base_dir = Path("research_data")
        if not base_dir.exists():
            return metadata_cache

        for research_dir in base_dir.glob("research_*"):
            meta_file = research_dir / "session_metadata.json"
            if not meta_file.exists():
                continue
            try:
                with meta_file.open("r", encoding="utf-8") as fh:
                    session_data = json.load(fh)
                session_id = session_data.get("session_id", research_dir.name)
                metadata_cache[session_id] = session_data
                logger.debug("Loaded session metadata for %s", session_id)
            except json.JSONDecodeError as exc:
                logger.warning("Failed to parse %s: %s", meta_file, exc)
        return metadata_cache

    # ------------------------------------------------------------------
    # File processing orchestration
    # ------------------------------------------------------------------

    def process_all_binary_sources(self) -> List[ProcessingResult]:
        """Process every configured binary source in order of priority."""
        results: List[ProcessingResult] = []

        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            dat_paths = [
                Path("crawlers/raw_data/intraday_data"),
                Path("crawlers/raw_data/data_mining"),
                Path("/Users/lokeshgupta/Desktop/backtesting/data_mining"),
            ]
            for base_path in dat_paths:
                if not base_path.exists():
                    continue
                for dat_file in sorted(base_path.glob("*.dat")):
                    results.append(self._process_file_with_connection(conn, dat_file))

            bin_path = Path("crawlers/raw_data/zerodha_websocket/raw_binary")
            if bin_path.exists():
                for bin_file in sorted(bin_path.glob("*.bin")):
                    results.append(self._process_file_with_connection(conn, bin_file))

        return results

    def process_file_with_metadata_capture(self, file_path: Union[str, Path]) -> ProcessingResult:
        """Process a single binary capture file with managed connection."""
        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            return self._process_file_with_connection(conn, file_path)

    def _process_file_with_connection(
        self,
        conn: duckdb.DuckDBPyConnection,
        file_path: Union[str, Path],
    ) -> ProcessingResult:
        """Core file-processing logic that uses a supplied DuckDB connection."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)

        if self._is_file_processed(conn, path):
            logger.info("Skipping already processed file: %s", path)
            return ProcessingResult(path, total_packets=0, successful_inserts=0)

        processing_id = self._start_processing_file(conn, path)
        total_packets = 0
        successful_inserts = 0
        error_count = 0
        max_errors_before_abort = 50

        try:
            raw_data = path.read_bytes()
            packets = self._detect_packet_format(raw_data)
            total_packets = len(packets)

            logger.info("Processing %s: %s packets", path, total_packets)

            batch_size = 500
            for batch_index, start in enumerate(range(0, total_packets, batch_size), start=1):
                batch = packets[start : start + batch_size]
                batch_success, batch_errors = self._process_batch_with_error_isolation(conn, list(batch), path)
                successful_inserts += batch_success
                error_count += batch_errors
                logger.info(
                    "Batch %s for %s: %s/%s succeeded, %s errors (total errors %s)",
                    batch_index,
                    path.name,
                    batch_success,
                    len(batch),
                    batch_errors,
                    error_count,
                )
                if error_count > max_errors_before_abort:
                    logger.warning(
                        "Aborting %s due to excessive errors (%s)",
                        path.name,
                        error_count,
                    )
                    break

            if error_count == 0:
                self._complete_processing_file(conn, processing_id, successful_inserts, total_packets)
                logger.info(
                    "Processed %s: %s/%s packets",
                    path,
                    successful_inserts,
                    total_packets,
                )
            else:
                self._partial_processing_file(conn, processing_id, successful_inserts, total_packets, error_count)
                logger.warning(
                    "Partially processed %s: %s/%s packets (%s errors)",
                    path,
                    successful_inserts,
                    total_packets,
                    error_count,
                )
            return ProcessingResult(path, total_packets, successful_inserts)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process %s", path)
            self._fail_processing_file(conn, processing_id, str(exc))
            return ProcessingResult(path, total_packets, successful_inserts)

    # ------------------------------------------------------------------
    # Packet parsing
    # ------------------------------------------------------------------

    def _detect_packet_format(self, raw_data: bytes) -> List[Dict]:
        """Detect frame format and parse accordingly."""
        if self._looks_like_websocket_frame(raw_data):
            return self._parse_websocket_frames(raw_data)
        return self._parse_raw_packet_stream(raw_data)

    def _looks_like_websocket_frame(self, data: bytes) -> bool:
        if len(data) < 4:
            return False
        try:
            num_packets = struct.unpack(">H", data[0:2])[0]
            first_packet_length = struct.unpack(">H", data[2:4])[0]
            if num_packets <= 0 or num_packets > 10_000:
                return False
            if first_packet_length not in VALID_PACKET_LENGTHS:
                return False
            return True
        except struct.error:
            return False

    def _parse_websocket_frames(self, data: bytes) -> List[Dict]:
        packets: List[Dict] = []
        position = 0
        try:
            num_packets = struct.unpack(">H", data[0:2])[0]
            position += 2
            for _ in range(num_packets):
                if position + 2 > len(data):
                    break
                packet_length = struct.unpack(">H", data[position : position + 2])[0]
                position += 2
                if packet_length not in VALID_PACKET_LENGTHS:
                    break
                packet_end = position + packet_length
                if packet_end > len(data):
                    break
                packet = self._parse_single_packet(data[position:packet_end], packet_length)
                if packet:
                    packets.append(packet)
                position = packet_end
        except struct.error as exc:
            logger.warning("WebSocket frame parse error: %s", exc)
        return packets

    def _parse_raw_packet_stream(self, data: bytes) -> List[Dict]:
        packets: List[Dict] = []
        position = 0
        data_len = len(data)

        while position + 8 <= data_len:
            parsed = False
            for packet_length in VALID_PACKET_LENGTHS:
                end_pos = position + packet_length
                if end_pos > data_len:
                    continue
                packet = self._parse_single_packet(data[position:end_pos], packet_length)
                if packet:
                    packets.append(packet)
                    position = end_pos
                    parsed = True
                    break
            if not parsed:
                position += 1
        return packets

    def _parse_single_packet(self, packet_data: bytes, packet_length: int) -> Optional[Dict]:
        if packet_length == 184:
            return self._parse_full_mode_packet(packet_data)
        if packet_length == 44:
            return self._parse_quote_mode_packet(packet_data)
        if packet_length == 32:
            return self._parse_index_mode_packet(packet_data)
        if packet_length == 8:
            return self._parse_ltp_mode_packet(packet_data)
        return None

    def _parse_full_mode_packet(self, data: bytes) -> Optional[Dict]:
        if len(data) < 184:
            return None
        try:
            fields = struct.unpack(">IIIIIIIIIIIIIIII", data[:64])
            depth_entries: List[Dict[str, int]] = []
            depth_start = 64
            for i in range(10):
                entry_start = depth_start + i * 12
                if entry_start + 10 > len(data):
                    break
                quantity, price, orders = struct.unpack(">IIH", data[entry_start : entry_start + 10])
                depth_entries.append(
                    {
                        "quantity": quantity,
                        "price": price,
                        "orders": orders,
                    }
                )
            return {
                "instrument_token": fields[0],
                "last_price": fields[1],
                "last_quantity": fields[2],
                "average_price": fields[3],
                "volume": fields[4],
                "total_buy_quantity": fields[5],
                "total_sell_quantity": fields[6],
                "open": fields[7],
                "high": fields[8],
                "low": fields[9],
                "close": fields[10],
                "last_traded_timestamp": self._coerce_timestamp_ns(fields[11]),
                "open_interest": fields[12],
                "oi_day_high": fields[13],
                "oi_day_low": fields[14],
                "exchange_timestamp": self._coerce_timestamp_ns(fields[15]),
                "market_depth": depth_entries,
                "packet_type": "full",
                "data_quality": "complete",
            }
        except struct.error as exc:
            logger.debug("Full packet parse error: %s", exc)
            return None

    def _parse_quote_mode_packet(self, data: bytes) -> Optional[Dict]:
        if len(data) < 44:
            return None
        try:
            fields = struct.unpack(">IIIIIIIIIII", data)
            return {
                "instrument_token": fields[0],
                "last_price": fields[1],
                "last_quantity": fields[2],
                "average_price": fields[3],
                "volume": fields[4],
                "total_buy_quantity": fields[5],
                "total_sell_quantity": fields[6],
                "open": fields[7],
                "high": fields[8],
                "low": fields[9],
                "close": fields[10],
                "packet_type": "quote",
                "data_quality": "partial",
            }
        except struct.error as exc:
            logger.debug("Quote packet parse error: %s", exc)
            return None

    def _parse_index_mode_packet(self, data: bytes) -> Optional[Dict]:
        if len(data) < 32:
            return None
        try:
            fields = struct.unpack(">IIIIIIII", data)
            return {
                "instrument_token": fields[0],
                "last_price": fields[1],
                "high": fields[2],
                "low": fields[3],
                "open": fields[4],
                "close": fields[5],
                "price_change": fields[6],
                "exchange_timestamp": self._coerce_timestamp_ns(fields[7]),
                "packet_type": "index",
                "data_quality": "index_data",
            }
        except struct.error as exc:
            logger.debug("Index packet parse error: %s", exc)
            return None

    def _parse_ltp_mode_packet(self, data: bytes) -> Optional[Dict]:
        if len(data) < 8:
            return None
        try:
            instrument_token, last_price = struct.unpack(">II", data)
            return {
                "instrument_token": instrument_token,
                "last_price": last_price,
                "packet_type": "ltp",
                "data_quality": "minimal",
            }
        except struct.error as exc:
            logger.debug("LTP packet parse error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Metadata enrichment & insertion
    # ------------------------------------------------------------------

    def _process_batch_with_error_isolation(
        self,
        conn: duckdb.DuckDBPyConnection,
        batch: List[Dict],
        file_path: Path,
    ) -> Tuple[int, int]:
        if not batch:
            return 0, 0

        enriched_records, errors = self._enrich_packets(batch, file_path, conn)
        if not enriched_records:
            return 0, max(errors, len(batch))

        try:
            arrow_table = self._records_to_arrow_table(enriched_records)
            inserted = self._write_arrow_table(conn, arrow_table)
            return inserted, errors
        except Exception as exc:  # noqa: BLE001
            logger.warning("Batch insert failed for %s: %s", file_path.name, exc)
            return 0, len(batch)

    def _enrich_packets(
        self,
        batch: List[Dict],
        file_path: Path,
        conn: Optional[duckdb.DuckDBPyConnection],
    ) -> Tuple[List[Dict], int]:
        errors = 0
        enriched_records: List[Dict] = []

        for packet in batch:
            if not packet:
                errors += 1
                continue
            try:
                enriched = self._enrich_with_complete_metadata(conn, packet, file_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Packet enrichment error in %s: %s", file_path.name, exc)
                errors += 1
                continue

            if not enriched:
                errors += 1
                continue

            enriched_records.append(enriched)

        return enriched_records, errors

    def _write_arrow_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        arrow_table: pa.Table,
    ) -> int:
        if pa is None:
            raise ImportError("pyarrow is required for bulk insert operations") from PYARROW_IMPORT_ERROR

        view_name = f"arrow_batch_{uuid4().hex}"
        column_list = ", ".join(COLUMN_ORDER)
        try:
            conn.register(view_name, arrow_table)
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                f"INSERT INTO tick_data_corrected ({column_list}) "
                f"SELECT {column_list} FROM {view_name}"
            )
            conn.execute("COMMIT")
            return arrow_table.num_rows
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:  # noqa: BLE001
                pass
            raise
        finally:
            try:
                conn.unregister(view_name)
            except Exception:  # noqa: BLE001
                pass

    def _records_to_arrow_table(self, records: List[Dict]) -> pa.Table:
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions") from PYARROW_IMPORT_ERROR

        schema = self._get_arrow_schema()
        columns = []
        for field in schema:
            column_name = field.name
            values = [self._normalize_value_for_arrow(column_name, record.get(column_name)) for record in records]
            columns.append(pa.array(values, type=field.type, from_pandas=True))
        return pa.Table.from_arrays(columns, schema=schema)

    def _get_arrow_schema(self) -> pa.Schema:
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions") from PYARROW_IMPORT_ERROR

        if self._arrow_schema is None:
            fields = [pa.field(column, COLUMN_ARROW_TYPES[column]) for column in COLUMN_ORDER]
            self._arrow_schema = pa.schema(fields)
        return self._arrow_schema

    def _table_to_ipc_bytes(self, table: pa.Table) -> bytes:
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions") from PYARROW_IMPORT_ERROR

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()

    def _ipc_bytes_to_table(self, payload: bytes) -> pa.Table:
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions") from PYARROW_IMPORT_ERROR

        reader = pa.ipc.open_stream(pa.BufferReader(payload))
        return reader.read_all()

    def convert_file_to_arrow_batches(
        self,
        file_path: Union[str, Path],
        batch_size: Optional[int] = None,
        use_db_metadata: bool = False,
    ) -> Tuple[List[Dict[str, Union[str, int, bytes]]], int, int]:
        """Convert a binary capture into Arrow IPC batches without writing to DuckDB."""
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions") from PYARROW_IMPORT_ERROR

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(path)

        self._missing_token_counts.clear()

        raw_data = path.read_bytes()
        packets = self._detect_packet_format(raw_data)
        total_packets = len(packets)

        if total_packets == 0:
            return [], 0, 0

        batch_len = batch_size or self.batch_size
        arrow_batches: List[Dict[str, Union[str, int, bytes]]] = []
        total_errors = 0

        connection_context = (
            DatabaseConnectionManager.connection_scope(self.db_path)  # type: ignore[arg-type]
            if use_db_metadata and self.db_path
            else nullcontext(None)
        )

        with connection_context as conn:
            for batch_index, start in enumerate(range(0, total_packets, batch_len)):
                batch_packets = packets[start : start + batch_len]
                enriched_records, errors = self._enrich_packets(batch_packets, path, conn)
                total_errors += errors
                if not enriched_records:
                    continue

                arrow_table = self._records_to_arrow_table(enriched_records)
                arrow_batches.append(
                    {
                        "file_path": str(path),
                        "batch_index": batch_index,
                        "row_count": arrow_table.num_rows,
                        "packet_count": len(batch_packets),
                        "errors": errors,
                        "arrow_ipc": self._table_to_ipc_bytes(arrow_table),
                    }
                )

        if self._missing_token_counts:
            missing_total = sum(self._missing_token_counts.values())
            top_examples = sorted(self._missing_token_counts.items(), key=lambda item: item[1], reverse=True)[:5]
            logger.warning(
                "Missing metadata for %s tokens (%s packets) in %s; top tokens: %s",
                len(self._missing_token_counts),
                missing_total,
                path.name,
                ", ".join(f"{token}:{count}" for token, count in top_examples),
            )

        return arrow_batches, total_packets, total_errors

    def _normalize_value_for_arrow(self, column: str, value):
        if value is None:
            return None

        if column in TIMESTAMP_COLUMNS:
            if isinstance(value, datetime):
                return value
            if isinstance(value, (int, float)):
                try:
                    seconds = float(value) / 1_000_000_000
                    return datetime.utcfromtimestamp(seconds)
                except (OverflowError, ValueError):
                    return None
            return value

        if column in DATE_COLUMNS:
            if isinstance(value, date):
                return value
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, str):
                try:
                    return datetime.strptime(value[:10], "%Y-%m-%d").date()
                except ValueError:
                    return None
            return None

        if column in BOOL_COLUMNS:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in {"1", "true", "yes"}
            return bool(value)

        if column in FLOAT_COLUMNS:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        if column in INT32_COLUMNS or column in INT64_COLUMNS:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        if column in STRING_COLUMNS:
            return str(value)

        return value

    def _get_token_metadata(
        self,
        conn: Optional[duckdb.DuckDBPyConnection],
        instrument_token: int,
    ) -> Dict:
        cached = self._token_metadata_cache.get(instrument_token)
        if cached:
            return cached

        metadata: Dict[str, Optional[Union[str, float, int, bool, date]]] = {
            "symbol": f"UNKNOWN_{instrument_token}",
            "exchange": None,
            "segment": None,
            "instrument_type": None,
            "expiry": None,
            "strike_price": None,
            "option_type": None,
            "lot_size": None,
            "tick_size": None,
            "is_expired": False,
        }

        if self.token_cache:
            cache_info = self.token_cache.get_instrument_info(instrument_token)
            for key in ("symbol", "exchange", "segment", "instrument_type", "expiry", "strike_price", "option_type", "lot_size", "tick_size", "is_expired"):
                if cache_info is None:
                    continue
                value = cache_info.get(key)
                if value is not None:
                    metadata[key] = value

        db_row = None
        if conn is not None:
            try:
                db_row = conn.execute(
                    """
                    SELECT symbol, exchange, segment, instrument_type, expiry,
                           strike_price, option_type, lot_size, tick_size, is_expired
                    FROM token_mappings
                    WHERE instrument_token = ?
                    """,
                    [instrument_token],
                ).fetchone()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Token metadata lookup failed for %s: %s", instrument_token, exc)

        if db_row:
            db_keys = [
                "symbol",
                "exchange",
                "segment",
                "instrument_type",
                "expiry",
                "strike_price",
                "option_type",
                "lot_size",
                "tick_size",
                "is_expired",
            ]
            for idx, key in enumerate(db_keys):
                value = db_row[idx]
                if value is not None:
                    metadata[key] = value

        metadata = self._normalize_token_metadata(metadata)
        self._token_metadata_cache[instrument_token] = metadata
        return metadata

    def _normalize_token_metadata(self, metadata: Dict) -> Dict:
        expiry = metadata.get("expiry")
        if isinstance(expiry, str):
            try:
                metadata["expiry"] = datetime.strptime(expiry[:10], "%Y-%m-%d").date()
            except ValueError:
                metadata["expiry"] = None
        elif isinstance(expiry, datetime):
            metadata["expiry"] = expiry.date()
        elif expiry and not isinstance(expiry, date):
            metadata["expiry"] = None

        tick_size = metadata.get("tick_size")
        if isinstance(tick_size, str):
            try:
                metadata["tick_size"] = float(tick_size)
            except ValueError:
                metadata["tick_size"] = None

        lot_size = metadata.get("lot_size")
        if isinstance(lot_size, str):
            try:
                metadata["lot_size"] = int(float(lot_size))
            except ValueError:
                metadata["lot_size"] = None

        strike_price = metadata.get("strike_price")
        if isinstance(strike_price, str):
            try:
                metadata["strike_price"] = float(strike_price)
            except ValueError:
                metadata["strike_price"] = None

        is_expired = metadata.get("is_expired")
        if isinstance(is_expired, str):
            metadata["is_expired"] = is_expired.lower() in {"1", "true", "yes"}

        return metadata

    def _enrich_with_complete_metadata(
        self,
        conn: Optional[duckdb.DuckDBPyConnection],
        packet: Dict,
        file_path: Optional[Path],
    ) -> Optional[Dict]:
        token = packet.get("instrument_token")
        if token is None:
            return None

        metadata = self._get_token_metadata(conn, token)

        symbol = metadata.get("symbol")
        if not symbol or (isinstance(symbol, str) and symbol.startswith("UNKNOWN_")):
            self._missing_token_counts[token] += 1
            if token not in self._logged_missing_tokens and len(self._logged_missing_tokens) < 10:
                logger.warning(
                    "Missing metadata for instrument token %s in %s",
                    token,
                    file_path.name if isinstance(file_path, Path) else str(file_path) if file_path else "<unknown>",
                )
                self._logged_missing_tokens.add(token)
            if self.drop_unknown_tokens and self.token_cache:
                return None

        price_divisor = self._get_price_divisor_safe(metadata)
        exchange_ts_ns = packet.get("exchange_timestamp") or 0
        last_traded_ts_ns = packet.get("last_traded_timestamp") or 0

        exchange_ts = self._ns_to_timestamp(exchange_ts_ns)
        processed_ts = datetime.utcnow()

        enriched = {
            **packet,
            "symbol": metadata.get("symbol"),
            "exchange": metadata.get("exchange"),
            "segment": metadata.get("segment"),
            "instrument_type": metadata.get("instrument_type"),
            "expiry": metadata.get("expiry"),
            "strike_price": metadata.get("strike_price"),
            "option_type": metadata.get("option_type"),
            "lot_size": metadata.get("lot_size"),
            "tick_size": metadata.get("tick_size"),
            "is_expired": metadata.get("is_expired", False),
            "timestamp": processed_ts,
            "exchange_timestamp_ns": exchange_ts_ns,
            "exchange_timestamp": exchange_ts,
            "last_traded_timestamp_ns": last_traded_ts_ns,
            "last_traded_timestamp": self._ns_to_timestamp(last_traded_ts_ns),
            "last_price": self._scale_price(packet.get("last_price"), price_divisor),
            "open_price": self._scale_price(packet.get("open"), price_divisor),
            "high_price": self._scale_price(packet.get("high"), price_divisor),
            "low_price": self._scale_price(packet.get("low"), price_divisor),
            "close_price": self._scale_price(packet.get("close"), price_divisor),
            "average_traded_price": self._scale_price(packet.get("average_price"), price_divisor),
            "volume": packet.get("volume", 0),
            "last_traded_quantity": packet.get("last_quantity", 0),
            "total_buy_quantity": packet.get("total_buy_quantity", 0),
            "total_sell_quantity": packet.get("total_sell_quantity", 0),
            "open_interest": packet.get("open_interest", 0),
            "oi_day_high": packet.get("oi_day_high", 0),
            "oi_day_low": packet.get("oi_day_low", 0),
            "packet_type": packet.get("packet_type", "unknown"),
            "data_quality": packet.get("data_quality", "unknown"),
            "session_type": self.classify_session(exchange_ts, metadata.get("exchange")),
            "source_file": str(file_path) if file_path else None,
            "processing_batch": "production_v2",
            "session_id": self._find_session_for_file(file_path),
        }

        enriched.update(self._flatten_market_depth(packet.get("market_depth", []), price_divisor))
        return enriched

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _flatten_market_depth(self, depth_entries: List[Dict], price_divisor: float) -> Dict[str, Optional[Union[int, float]]]:
        flattened: Dict[str, Optional[Union[int, float]]] = {}
        for level in range(1, 6):
            flattened[f"bid_{level}_price"] = None
            flattened[f"bid_{level}_quantity"] = 0
            flattened[f"bid_{level}_orders"] = 0
            flattened[f"ask_{level}_price"] = None
            flattened[f"ask_{level}_quantity"] = 0
            flattened[f"ask_{level}_orders"] = 0

        for idx, entry in enumerate(depth_entries):
            price = entry.get("price", 0)
            qty = entry.get("quantity", 0)
            orders = entry.get("orders", 0)
            if idx < 5:
                level = idx + 1
                flattened[f"bid_{level}_price"] = self._scale_price(price, price_divisor)
                flattened[f"bid_{level}_quantity"] = qty
                flattened[f"bid_{level}_orders"] = orders
            elif idx < 10:
                level = idx - 4
                flattened[f"ask_{level}_price"] = self._scale_price(price, price_divisor)
                flattened[f"ask_{level}_quantity"] = qty
                flattened[f"ask_{level}_orders"] = orders
        return flattened

    def _scale_price(self, value: Optional[int], divisor: float) -> Optional[float]:
        if value in (None, 0):
            return 0.0
        return float(value) / float(divisor)

    def _coerce_timestamp_ns(self, value: int) -> int:
        if value is None:
            return 0
        if value >= 10**15:  # Already nanoseconds
            return value
        if value >= 10**12:  # milliseconds
            return int(value * 1_000_000)
        if value >= 10**9:  # seconds
            return int(value * 1_000_000_000)
        return int(value)

    def _ns_to_timestamp(self, nanoseconds: int) -> Optional[datetime]:
        if not nanoseconds:
            return None
        seconds = nanoseconds / 1_000_000_000
        try:
            return datetime.utcfromtimestamp(seconds)
        except (OverflowError, ValueError):
            return None

    def _get_price_divisor_safe(self, token_info: Dict) -> float:
        tick_size_value = token_info.get("tick_size")
        segment = token_info.get("segment")
        instrument_type = token_info.get("instrument_type")

        try:
            tick_size = float(tick_size_value) if tick_size_value not in (None, 0) else 0.0
        except (TypeError, ValueError):
            tick_size = 0.0
        if tick_size > 0:
            if tick_size >= 1.0:
                return 1.0
            divisor = 1.0 / tick_size
            if divisor > 10_000_000:
                divisor = 10_000_000.0
            return float(divisor)

        if segment == "CDS":
            return 10_000_000.0
        if segment == "MCX":
            return 100.0
        if segment in {"NSE", "BSE"}:
            return 100.0
        return 100.0

    def classify_session(self, ts: Optional[datetime], exchange: Optional[str]) -> str:
        if ts is None:
            return "unknown"
        hour = ts.hour
        minute = ts.minute

        # Convert to UTC windows for NSE (IST offset already accounted for by using UTC)
        if exchange in {None, "NSE", "BSE"}:
            if hour == 3 and 30 <= minute < 38:
                return "pre_market"
            if (hour == 3 and minute >= 45) or (4 <= hour < 10) or (hour == 10 and minute == 0):
                return "regular"
            if hour == 10 and 10 <= minute < 30:
                return "post_market"
            return "off_market"
        return "unknown"

    def _extract_timestamp_from_filename(self, file_path: Optional[Path]) -> Optional[datetime]:
        import re  # pylint: disable=import-outside-toplevel

        if file_path is None:
            return None

        match = re.search(r"(\d{8}_\d{4})", file_path.stem)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y%m%d_%H%M")
            except ValueError:
                return None
        return None

    def _find_session_for_file(self, file_path: Optional[Path]) -> Optional[str]:
        if file_path is None:
            return None
        file_ts = self._extract_timestamp_from_filename(file_path)
        if not file_ts:
            try:
                file_ts = datetime.utcfromtimestamp(file_path.stat().st_mtime)
            except OSError:
                file_ts = None
        if not file_ts:
            return None

        for session_id, metadata in self.session_metadata_cache.items():
            start = self._parse_session_timestamp(metadata.get("start_time"))
            end = self._parse_session_timestamp(metadata.get("end_time"))
            if start and end and start <= file_ts <= end:
                return session_id
        return None

    def _parse_session_timestamp(self, value: Optional[Union[int, float]]) -> Optional[datetime]:
        if value is None:
            return None
        try:
            return datetime.utcfromtimestamp(float(value))
        except (TypeError, ValueError, OverflowError):
            return None

    # ------------------------------------------------------------------
    # Processed file ledger helpers
    # ------------------------------------------------------------------

    def _is_file_processed(self, conn: duckdb.DuckDBPyConnection, file_path: Path) -> bool:
        row = conn.execute(
            "SELECT status FROM processed_files_log WHERE file_path = ?",
            [str(file_path)],
        ).fetchone()
        return row is not None and row[0] == "completed"

    def _start_processing_file(self, conn: duckdb.DuckDBPyConnection, file_path: Path) -> str:
        file_stat = file_path.stat()
        # Use file metadata rather than loading the entire payload into memory to fingerprint work.
        fingerprint = f"{file_stat.st_size}:{file_stat.st_mtime_ns}"
        file_hash = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
        now = datetime.utcnow()
        conn.execute(
            """
            INSERT OR REPLACE INTO processed_files_log
            (file_path, file_size, file_hash, records_processed,
             processing_started, processing_completed, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(file_path),
                file_stat.st_size,
                file_hash,
                0,
                now,
                None,
                "in_progress",
                None,
            ],
        )
        return str(file_path)

    def _complete_processing_file(
        self,
        conn: duckdb.DuckDBPyConnection,
        processing_id: str,
        records: int,
        total: int,
    ) -> None:
        conn.execute(
            """
            UPDATE processed_files_log
            SET records_processed = ?,
                processing_completed = ?,
                status = ?
            WHERE file_path = ?
            """,
            [
                records,
                datetime.utcnow(),
                "completed",
                processing_id,
            ],
        )

    def _fail_processing_file(
        self,
        conn: duckdb.DuckDBPyConnection,
        processing_id: str,
        error_message: str,
    ) -> None:
        conn.execute(
            """
            UPDATE processed_files_log
            SET processing_completed = ?,
                status = ?,
                error_message = ?
            WHERE file_path = ?
            """,
            [
                datetime.utcnow(),
                "failed",
                error_message[:512],
                processing_id,
            ],
        )

    def _partial_processing_file(
        self,
        conn: duckdb.DuckDBPyConnection,
        processing_id: str,
        records: int,
        total: int,
        errors: int,
    ) -> None:
        conn.execute(
            """
            UPDATE processed_files_log
            SET records_processed = ?,
                processing_completed = ?,
                status = ?,
                error_message = ?
            WHERE file_path = ?
            """,
            [
                records,
                datetime.utcnow(),
                "partial",
                f"errors={errors} total_packets={total}",
                processing_id,
            ],
        )


def run_comprehensive_validation(
    converter: ProductionZerodhaBinaryConverter,
    *_,
) -> None:
    """Run quick sanity checks on parser utilities before bulk processing."""
    print("🧪 COMPREHENSIVE PRODUCTION VALIDATION")

    test_packets = {
        "full": b"\x00" * 184,
        "quote": b"\x00" * 44,
        "index": b"\x00" * 32,
        "ltp": b"\x00" * 8,
    }

    for ptype, payload in test_packets.items():
        parser = getattr(converter, f"_parse_{ptype}_mode_packet")
        result = parser(payload)
        status = "✅" if result and result.get("packet_type") == ptype else "❌"
        label = result.get("packet_type") if result else "FAILED"
        print(f"  {status} {ptype.upper()} mode parser → {label}")

    print("\n🧮 PRICE SCALING SAFETY")
    scaling_tests = [
        (0.05, 20.0),
        (0.0, 100.0),
        (0.0000001, 10_000_000.0),
        (100.0, 1.0),
    ]
    for tick_size, expected in scaling_tests:
        token_info = {
            "tick_size": tick_size,
            "segment": "NSE",
            "instrument_type": "EQ",
        }
        divisor = converter._get_price_divisor_safe(token_info)  # noqa: SLF001
        status = "✅" if abs(divisor - expected) < 1e-3 else "❌"
        print(f"  {status} tick_size={tick_size} → divisor={divisor} (expected {expected})")

    print("\n🕒 SESSION CLASSIFICATION")
    session_tests = [
        (datetime(2024, 1, 1, 3, 35, tzinfo=timezone.utc), "pre_market"),
        (datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc), "regular"),
        (datetime(2024, 1, 1, 10, 20, tzinfo=timezone.utc), "post_market"),
    ]
    for ts, expected in session_tests:
        session = converter.classify_session(ts, "NSE")
        status = "✅" if session == expected else "❌"
        print(f"  {status} {ts.strftime('%H:%M')} UTC → {session} (expected {expected})")

    print("\n🎯 VALIDATION COMPLETE - Ready for production processing!")


def validate_and_fix_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Report schema mismatches for critical columns."""
    required_types = {
        "bid_1_orders": "INTEGER",
        "bid_2_orders": "INTEGER",
        "bid_3_orders": "INTEGER",
        "bid_4_orders": "INTEGER",
        "bid_5_orders": "INTEGER",
        "ask_1_orders": "INTEGER",
        "ask_2_orders": "INTEGER",
        "ask_3_orders": "INTEGER",
        "ask_4_orders": "INTEGER",
        "ask_5_orders": "INTEGER",
    }
    for column, expected in required_types.items():
        row = conn.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = 'tick_data_corrected' AND column_name = ?
            """,
            [column],
        ).fetchone()
        if not row:
            print(f"❌ Column {column} missing from tick_data_corrected")
        elif row[0] != expected:
            print(f"⚠️ Column {column} type {row[0]}, expected {expected}")


def recreate_table_with_correct_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop and recreate tick_data_corrected with enforced INTEGER order columns."""
    print("🔄 Recreating tick_data_corrected with corrected schema …")
    conn.execute("DROP TABLE IF EXISTS tick_data_corrected")
    ensure_production_schema(conn)
    print("✅ tick_data_corrected recreated.")


def run_audit_queries(
    conn: duckdb.DuckDBPyConnection,
    source_file: Optional[str] = None,
) -> None:
    """Run basic audit metrics for converted data."""
    where_clause = ""
    params: List = []
    if source_file:
        where_clause = "WHERE source_file = ?"
        params = [source_file]

    basic_stats = conn.execute(
        f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT instrument_token) AS unique_instruments,
            COUNT(DISTINCT source_file) AS unique_files,
            MIN(exchange_timestamp) AS earliest_ts,
            MAX(exchange_timestamp) AS latest_ts
        FROM tick_data_corrected
        {where_clause}
        """,
        params,
    ).fetchone()

    if not basic_stats or basic_stats[0] == 0:
        print("📊 No records found for audit.")
        return

    print(
        "📊 Basic stats: records={:,}, unique instruments={:,}, files={}, range={} → {}".format(
            basic_stats[0],
            basic_stats[1],
            basic_stats[2],
            basic_stats[3],
            basic_stats[4],
        )
    )

    packet_stats = conn.execute(
        f"""
        SELECT packet_type, COUNT(*) AS records
        FROM tick_data_corrected
        {where_clause}
        GROUP BY packet_type
        ORDER BY records DESC
        """,
        params,
    ).fetchdf()
    print("\n📦 Packet distribution:")
    for _, row in packet_stats.iterrows():
        print(f"  {row['packet_type']}: {row['records']:,}")

    depth_stats = conn.execute(
        f"""
        SELECT
            COUNT(CASE WHEN bid_1_price IS NOT NULL THEN 1 END),
            COUNT(CASE WHEN bid_5_price IS NOT NULL THEN 1 END),
            COUNT(CASE WHEN ask_1_price IS NOT NULL THEN 1 END),
            COUNT(CASE WHEN ask_5_price IS NOT NULL THEN 1 END),
            COUNT(CASE WHEN bid_1_orders > 32767 THEN 1 END)
        FROM tick_data_corrected
        {where_clause}
        """,
        params,
    ).fetchone()
    print(
        "\n🎯 Depth completeness: bid1={}, bid5={}, ask1={}, ask5={}, large_order_counts={}".format(
            depth_stats[0],
            depth_stats[1],
            depth_stats[2],
            depth_stats[3],
            depth_stats[4],
        )
    )

    quality_stats = conn.execute(
        f"""
        SELECT
            COUNT(CASE WHEN last_price <= 0 THEN 1 END),
            COUNT(CASE WHEN volume < 0 THEN 1 END),
            COUNT(CASE WHEN total_buy_quantity < 0 OR total_sell_quantity < 0 THEN 1 END),
            COUNT(CASE WHEN exchange_timestamp IS NULL THEN 1 END)
        FROM tick_data_corrected
        {where_clause}
        """,
        params,
    ).fetchone()
    print(
        "\n🔍 Quality checks: invalid_price={}, negative_volume={}, negative_flow={}, missing_ts={}".format(
            quality_stats[0],
            quality_stats[1],
            quality_stats[2],
            quality_stats[3],
        )
    )


def diagnose_invalid_prices(conn: duckdb.DuckDBPyConnection):
    """Identify instruments exhibiting invalid price patterns."""
    print("🔍 Diagnosing invalid prices...")
    query = """
        SELECT
            instrument_token,
            symbol,
            packet_type,
            COUNT(*) AS invalid_count,
            AVG(last_price) AS avg_price,
            MIN(last_price) AS min_price,
            MAX(last_price) AS max_price
        FROM tick_data_corrected
        WHERE last_price <= 0 OR last_price IS NULL
        GROUP BY instrument_token, symbol, packet_type
        HAVING COUNT(*) > 100
        ORDER BY invalid_count DESC
        LIMIT 20
    """
    df = conn.execute(query).fetchdf()
    if df.empty:
        print("✅ No significant invalid price patterns detected.")
        return df

    print("Top instruments with invalid prices:")
    for _, row in df.iterrows():
        print(
            f"  {row['symbol']} ({row['packet_type']}): "
            f"{row['invalid_count']} invalid, avg price {row['avg_price']}"
        )
    return df


def fix_price_scaling_issues(conn: duckdb.DuckDBPyConnection) -> None:
    """Attempt corrective scaling for instruments with systematic price issues."""
    print("🔄 Fixing price scaling for problematic instruments...")
    scaling_query = """
        SELECT
            instrument_token,
            symbol,
            segment,
            instrument_type,
            COUNT(*) AS total_ticks,
            COUNT(CASE WHEN last_price <= 0 THEN 1 END) AS invalid_prices,
            AVG(CASE WHEN last_price > 0 THEN last_price END) AS avg_valid_price
        FROM tick_data_corrected
        GROUP BY instrument_token, symbol, segment, instrument_type
        HAVING invalid_prices > 100 AND (avg_valid_price IS NULL OR avg_valid_price < 1.0)
    """
    issues = conn.execute(scaling_query).fetchdf()
    if issues.empty:
        print("✅ No scaling anomalies detected.")
        return

    for _, row in issues.iterrows():
        token = row["instrument_token"]
        symbol = row["symbol"]
        segment = row["segment"]
        invalid_count = row["invalid_prices"]
        print(f"  Adjusting {symbol} ({segment}): {invalid_count} invalid prices")

        if segment == "CDS":
            conn.execute(
                """
                UPDATE tick_data_corrected
                SET last_price = NULLIF(open_price, 0),
                    open_price = NULLIF(open_price, 0),
                    high_price = NULLIF(high_price, 0),
                    low_price = NULLIF(low_price, 0),
                    close_price = NULLIF(close_price, 0)
                WHERE instrument_token = ?
                  AND last_price <= 0
                """,
                [token],
            )
        else:
            conn.execute(
                """
                UPDATE tick_data_corrected
                SET last_price = NULLIF(open_price, 0)
                WHERE instrument_token = ? AND last_price <= 0
                """,
                [token],
            )


def diagnose_missing_timestamps(conn: duckdb.DuckDBPyConnection):
    """Report timestamp completeness per packet type."""
    print("🔍 Diagnosing missing timestamps...")
    query = """
        SELECT
            packet_type,
            COUNT(*) AS total_records,
            COUNT(CASE WHEN exchange_timestamp IS NULL THEN 1 END) AS missing_exchange_ts,
            COUNT(CASE WHEN last_traded_timestamp IS NULL THEN 1 END) AS missing_traded_ts,
            COUNT(CASE WHEN timestamp IS NULL THEN 1 END) AS missing_processing_ts
        FROM tick_data_corrected
        GROUP BY packet_type
    """
    df = conn.execute(query).fetchdf()
    if df.empty:
        print("No data present.")
        return df

    for _, row in df.iterrows():
        missing_pct = (row["missing_exchange_ts"] / row["total_records"]) * 100
        print(
            f"  {row['packet_type']}: {row['missing_exchange_ts']} missing exchange timestamps "
            f"({missing_pct:.1f}%)"
        )
    return df


def backfill_timestamps(conn: duckdb.DuckDBPyConnection) -> None:
    """Use fallback sources to populate missing exchange timestamps."""
    print("🔄 Backfilling missing timestamps...")
    missing_before = conn.execute(
        "SELECT COUNT(*) FROM tick_data_corrected WHERE exchange_timestamp IS NULL"
    ).fetchone()[0]
    conn.execute(
        """
        UPDATE tick_data_corrected
        SET exchange_timestamp = last_traded_timestamp
        WHERE exchange_timestamp IS NULL AND last_traded_timestamp IS NOT NULL
        """
    )
    conn.execute(
        """
        UPDATE tick_data_corrected
        SET exchange_timestamp = timestamp
        WHERE exchange_timestamp IS NULL AND timestamp IS NOT NULL
        """
    )
    missing_after = conn.execute(
        "SELECT COUNT(*) FROM tick_data_corrected WHERE exchange_timestamp IS NULL"
    ).fetchone()[0]
    print(f"  ✅ Backfilled {missing_before - missing_after} rows with missing exchange timestamps")


__all__ = [
    "ProductionZerodhaBinaryConverter",
    "run_comprehensive_validation",
    "validate_and_fix_schema",
    "recreate_table_with_correct_schema",
    "run_audit_queries",
    "diagnose_invalid_prices",
    "fix_price_scaling_issues",
    "diagnose_missing_timestamps",
    "backfill_timestamps",
    "ensure_production_schema",
    "ProcessingResult",
]
