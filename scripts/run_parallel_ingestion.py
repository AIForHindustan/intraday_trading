#!/usr/bin/env python3
"""
CLI wrapper for ParallelBinaryIngestionDriver.

Multiprocessing on macOS requires the entrypoint to live in a real file;
running via ``python - <<'PY'`` leaves worker processes unable to respawn the
parent module. This script provides a concrete file-based entrypoint so the
driver can use the ``spawn`` start method without stalls.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from binary_to_parquet.parallel_ingestion_driver import ParallelBinaryIngestionDriver
from binary_to_parquet.production_binary_converter import DEFAULT_BATCH_SIZE


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parallel binary ingestion driver.")
    parser.add_argument(
        "paths",
        nargs="+",
        help="Directories or files to scan for .bin/.dat captures.",
    )
    parser.add_argument(
        "--db-path",
        default="tick_data_production.db",
        help="Target DuckDB database path (default: %(default)s).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker processes (default: 4 for MacBook safety).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Override converter batch size (default: 5000).",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=16,
        help="Maximum result queue size (default: 16).",
    )
    parser.add_argument(
        "--token-cache",
        default=None,
        help="Optional token cache file to reuse.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-file progress output.",
    )
    parser.add_argument(
        "--min-file-age",
        type=int,
        default=0,
        help="Minimum age (seconds) a file must have before ingestion (default: %(default)s).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover files and exit without ingestion.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    directories: List[str] = [str(Path(p)) for p in args.paths]

    driver = ParallelBinaryIngestionDriver(
        db_path=args.db_path,
        workers=args.workers,
        batch_size=args.batch_size,
        result_queue_size=args.queue_size,
        token_cache_path=args.token_cache,
        verbose=not args.quiet,
        min_file_age_seconds=args.min_file_age,
    )

    if args.dry_run:
        files = driver.discover_files(directories)
        print(f"📂 Discovered {len(files)} files:")
        for path in files:
            print(path)
        return 0

    summary = driver.run(directories)
    print(f"✅ Ingestion summary: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
