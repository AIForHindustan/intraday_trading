"""
High-throughput multiprocessing driver for Zerodha binary ingestion.

This driver coordinates worker processes that parse binary captures into
Arrow IPC batches and a dedicated writer process that performs the DuckDB
inserts. It builds on the Arrow-enabled converter to maximise throughput
while preserving the processed_files_log guarantees.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from token_cache import TokenCacheManager
from singleton_db import DatabaseConnectionManager
from binary_to_parquet.production_binary_converter import DEFAULT_BATCH_SIZE, ProductionZerodhaBinaryConverter, ensure_production_schema
from smart_upsert_converter import SmartUpsertConverter


BatchPayload = Dict[str, object]
SummaryPayload = Dict[str, int]


def _worker_loop(
    task_queue: "mp.JoinableQueue",
    result_queue: "mp.Queue",
    batch_size: int,
    token_cache_path: str,
) -> None:
    """Worker process: parse files and emit Arrow IPC batches."""
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
    converter = ProductionZerodhaBinaryConverter(
        db_path=None,
        batch_size=batch_size,
        token_cache=token_cache,
        ensure_schema=False,
        drop_unknown_tokens=True,
    )

    while True:
        file_path = task_queue.get()
        if file_path is None:
            task_queue.task_done()
            break

        try:
            batches, total_packets, total_errors = converter.convert_file_to_arrow_batches(
                file_path,
                batch_size=batch_size,
                use_db_metadata=False,
            )
            for batch in batches:
                payload: BatchPayload = {
                    "type": "batch",
                    "file_path": batch["file_path"],
                    "batch_index": batch["batch_index"],
                    "row_count": batch["row_count"],
                    "packet_count": batch["packet_count"],
                    "errors": batch.get("errors", 0),
                    "arrow_ipc": batch["arrow_ipc"],
                }
                result_queue.put(payload)

            result_queue.put(
                {
                    "type": "file_complete",
                    "file_path": str(file_path),
                    "total_packets": total_packets,
                    "total_errors": total_errors,
                    "batches": len(batches),
                }
            )
        except Exception as exc:  # noqa: BLE001
            result_queue.put(
                {
                    "type": "file_error",
                    "file_path": str(file_path),
                    "error": repr(exc),
                }
            )
        finally:
            task_queue.task_done()


def _writer_loop(
    result_queue: "mp.Queue",
    summary_queue: "mp.Queue",
    db_path: str,
    batch_size: int,
    token_cache_path: str,
    verbose: bool,
) -> None:
    """Writer process: consume Arrow batches and persist them to DuckDB."""
    token_cache = TokenCacheManager(cache_path=token_cache_path, verbose=False)
    converter = SmartUpsertConverter(
        db_path=db_path,
        batch_size=batch_size,
        upsert_strategy="replace",
        token_cache=token_cache,
        ensure_schema=True,
    )

    files_completed = 0
    total_rows = 0
    total_errors = 0
    file_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"rows": 0, "packets": 0, "errors": 0})

    with DatabaseConnectionManager.connection_scope(db_path) as conn:
        ensure_production_schema(conn)

        while True:
            message = result_queue.get()
            if message is None:
                break

            mtype = message.get("type")
            file_path = message.get("file_path")

            if mtype == "batch":
                arrow_bytes = message["arrow_ipc"]
                try:
                    arrow_table = converter._ipc_bytes_to_table(arrow_bytes)  # noqa: SLF001
                    inserted_rows = converter._write_arrow_table(conn, arrow_table)  # noqa: SLF001
                except Exception as exc:  # noqa: BLE001
                    converter._fail_processing_file(conn, str(file_path), f"writer insert failed: {exc}")  # noqa: SLF001
                    file_stats.pop(str(file_path), None)
                    total_errors += int(message.get("packet_count", 0))
                    continue

                stats = file_stats[str(file_path)]
                stats["rows"] += inserted_rows
                stats["packets"] += int(message.get("packet_count", inserted_rows))
                stats["errors"] += int(message.get("errors", 0))
                total_rows += inserted_rows

            elif mtype == "file_complete":
                stats = file_stats.pop(str(file_path), {"rows": 0, "packets": 0, "errors": 0})
                total_packets = int(message.get("total_packets", stats["packets"]))
                file_errors = stats["errors"] + int(message.get("total_errors", 0))
                total_errors += file_errors

                if file_errors == 0 and stats["rows"] > 0:
                    converter._complete_processing_file(  # noqa: SLF001
                        conn,
                        str(file_path),
                        stats["rows"],
                        total_packets,
                    )
                else:
                    converter._partial_processing_file(  # noqa: SLF001
                        conn,
                        str(file_path),
                        stats["rows"],
                        total_packets,
                        file_errors,
                    )
                if verbose:
                    print(
                        f"📝 {file_path} → rows={stats['rows']}, packets={total_packets}, errors={file_errors}",
                        flush=True,
                    )
                files_completed += 1

            elif mtype == "file_error":
                converter._fail_processing_file(conn, str(file_path), str(message.get("error")))  # noqa: SLF001
                file_stats.pop(str(file_path), None)
                total_errors += 1
                if verbose:
                    print(f"❌ {file_path} failed: {message.get('error')}", flush=True)

    summary_queue.put(
        {
            "files_completed": files_completed,
            "rows_inserted": total_rows,
            "errors": total_errors,
        }
    )


class ParallelBinaryIngestionDriver:
    """Coordinate high-throughput processing across binary archives."""

    def __init__(
        self,
        db_path: str = "tick_data_production.db",
        workers: Optional[int] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        result_queue_size: int = 16,
        token_cache_path: Optional[str] = None,
        verbose: bool = True,
        min_file_age_seconds: int = 0,
    ) -> None:
        self.db_path = db_path
        self.workers = workers or max(1, min(4, os.cpu_count() - 1))  # Conservative default
        self.batch_size = batch_size
        self.result_queue_size = result_queue_size
        self.mp_ctx = mp.get_context("spawn")
        self.verbose = verbose
        self.min_file_age_seconds = max(0, min_file_age_seconds)

        cache_manager = TokenCacheManager(cache_path=token_cache_path)
        self.token_cache_path = str(cache_manager.cache_path)

    def discover_files(self, directories: Iterable[str]) -> List[Path]:
        """Return sorted file list recursively scanning provided directories."""
        supported_extensions = {".bin", ".dat", ".parquet", ".jsonl", ".json"}
        file_set: set[Path] = set()
        for directory in directories:
            base = Path(directory)
            if not base.exists():
                continue
            if base.is_file() and base.suffix in supported_extensions:
                file_set.add(base)
                continue
            for pattern in ("*.bin", "*.dat", "*.parquet", "*.jsonl"):
                for file_path in base.rglob(pattern):
                    if file_path.is_file():
                        file_set.add(file_path)
        return sorted(file_set)

    def run(self, directories: Iterable[str]) -> SummaryPayload:
        """Run the multiprocessing pipeline and return summary stats."""
        task_queue: mp.JoinableQueue = self.mp_ctx.JoinableQueue()
        result_queue: mp.Queue = self.mp_ctx.Queue(maxsize=self.result_queue_size)
        summary_queue: mp.Queue = self.mp_ctx.Queue(maxsize=1)

        converter = ProductionZerodhaBinaryConverter(
            db_path=self.db_path,
            batch_size=self.batch_size,
            token_cache=TokenCacheManager(cache_path=self.token_cache_path, verbose=False),
            drop_unknown_tokens=True,
        )

        files_to_process = self.discover_files(directories)
        if self.verbose:
            print(f"📁 Discovered {len(files_to_process)} files to process")

        files_to_queue: List[Path] = []
        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            ensure_production_schema(conn)
            for file_path in files_to_process:
                if self.min_file_age_seconds > 0 and not self._is_file_stable(file_path):
                    if self.verbose:
                        print(
                            f"⏳ Skipping {file_path} (file age < {self.min_file_age_seconds}s; will retry later)"
                        )
                    continue
                if converter._is_file_processed(conn, file_path):  # noqa: SLF001
                    continue
                converter._start_processing_file(conn, file_path)  # noqa: SLF001
                files_to_queue.append(file_path)

        if not files_to_queue:
            if self.verbose:
                print("✅ No new files to ingest")
            return {"files_completed": 0, "rows_inserted": 0, "errors": 0}

        skipped = len(files_to_process) - len(files_to_queue)
        if skipped and self.verbose:
            print(f"⏭️  Skipped {skipped} already completed files")

        writer = self.mp_ctx.Process(
            target=_writer_loop,
            args=(result_queue, summary_queue, self.db_path, self.batch_size, self.token_cache_path, self.verbose),
            name="binary-writer",
        )
        writer.start()

        workers: List[mp.Process] = []
        for idx in range(self.workers):
            worker = self.mp_ctx.Process(
                target=_worker_loop,
                args=(task_queue, result_queue, self.batch_size, self.token_cache_path),
                name=f"binary-worker-{idx+1}",
            )
            worker.start()
            workers.append(worker)

        for file_path in files_to_queue:
            task_queue.put(str(file_path))
            if self.verbose:
                print(f"  ↪ queued {file_path}")

        task_queue.join()

        for _ in workers:
            task_queue.put(None)

        for worker in workers:
            worker.join()

        result_queue.put(None)
        writer.join()

        summary = summary_queue.get() if not summary_queue.empty() else {"files_completed": 0, "rows_inserted": 0, "errors": 0}
        if self.verbose:
            print(f"✅ Ingestion summary: {summary}")
        return summary

    def _is_file_stable(self, file_path: Path) -> bool:
        try:
            file_stat = file_path.stat()
        except OSError:
            return False
        age = time.time() - file_stat.st_mtime
        return age >= self.min_file_age_seconds
