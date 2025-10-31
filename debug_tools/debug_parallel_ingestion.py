#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from binary_to_parquet.production_binary_converter import ensure_production_schema
from debug_tools.debug_binary_converter import get_debug_converter
from debug_tools.debug_error_tracker import error_tracker, reset_error_tracker
from singleton_db import DatabaseConnectionManager


def clear_previous_results(db_path: str, file_path: Path) -> None:
    with DatabaseConnectionManager.connection_scope(db_path) as conn:
        ensure_production_schema(conn)
        conn.execute("DELETE FROM tick_data_corrected WHERE source_file = ?", [str(file_path)])
        conn.execute("DELETE FROM processed_files_log WHERE file_path = ?", [str(file_path)])


def inspect_results(db_path: str, file_path: Path) -> None:
    with DatabaseConnectionManager.connection_scope(db_path) as conn:
        ensure_production_schema(conn)
        inserted = conn.execute(
            "SELECT COUNT(*) FROM tick_data_corrected WHERE source_file = ?",
            [str(file_path)],
        ).fetchone()[0]
        log_row = conn.execute(
            """
            SELECT status, records_processed, error_message
            FROM processed_files_log
            WHERE file_path = ?
            """,
            [str(file_path)],
        ).fetchone()

    print("\n📊 PROCESSING RESULTS")
    print("=" * 60)
    print(f"File: {file_path}")
    print(f"Rows in tick_data_corrected: {inserted}")
    print(f"processed_files_log entry: {log_row}")


def run_debug(
    file_path: Path,
    db_path: str,
    batch_size: int,
    clear_previous: bool,
) -> None:
    if clear_previous:
        print("🧹 Clearing previous rows for this file...")
        clear_previous_results(db_path, file_path)

    reset_error_tracker()

    converter = get_debug_converter(
        db_path=db_path,
        batch_size=batch_size,
        ensure_schema=True,
    )

    print(f"🔍 DEBUG PROCESSING: {file_path}")
    result = converter.process_file_with_metadata_capture(file_path)
    print(
        f"ProcessingResult(total_packets={result.total_packets}, "
        f"successful_inserts={result.successful_inserts})"
    )

    inspect_results(db_path, file_path)
    error_tracker.print_report()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run single-file ingestion with detailed debugging.")
    parser.add_argument("file", type=Path, help="Target .bin/.dat capture to process.")
    parser.add_argument(
        "--db-path",
        default="tick_data_production.db",
        help="DuckDB database to inspect (default: %(default)s).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size for Arrow conversion (default: %(default)s).",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Skip clearing prior inserts/log entries for the file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    file_path: Path = args.file.resolve()
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return 1

    run_debug(
        file_path=file_path,
        db_path=args.db_path,
        batch_size=args.batch_size,
        clear_previous=not args.keep_existing,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
