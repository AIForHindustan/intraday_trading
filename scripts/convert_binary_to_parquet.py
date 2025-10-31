#!/usr/bin/env python3
"""
Convert Zerodha .bin/.dat captures to Parquet with metadata enrichment.

The converter applies the ProductionZerodhaBinaryConverter pipeline so that
only packets with resolvable metadata are retained. Each input file is split
into one or more Parquet batches under the requested output directory while
preserving the relative directory structure from the provided roots.
"""

from __future__ import annotations

import argparse
import sys
import multiprocessing as mp
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pyarrow.parquet as pq

from token_cache import TokenCacheManager
from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter


SUPPORTED_EXTENSIONS = {".bin", ".dat"}

_WORKER_CONVERTER: Optional[ProductionZerodhaBinaryConverter] = None
_WORKER_OUTPUT_ROOT: Optional[Path] = None


def discover_files(paths: Sequence[Path]) -> List[Tuple[Path, Path]]:
    """Return (file_path, base_dir) pairs for all supported files under the inputs."""
    discovered: List[Tuple[Path, Path]] = []

    for root in paths:
        if root.is_file():
            if root.suffix.lower() in SUPPORTED_EXTENSIONS:
                discovered.append((root, root.parent))
            continue

        if not root.exists():
            print(f"⚠️  Skipping missing path: {root}", file=sys.stderr)
            continue

        for pattern in ("*.bin", "*.dat"):
            for file_path in root.rglob(pattern):
                if file_path.is_file():
                    discovered.append((file_path, root))

    return sorted(discovered, key=lambda pair: pair[0])


def ensure_directory(path: Path) -> None:
    """Create the parent directory for a target file if necessary."""
    path.parent.mkdir(parents=True, exist_ok=True)


def convert_file_to_parquet(
    converter: ProductionZerodhaBinaryConverter,
    file_path: Path,
    base_dir: Path,
    output_root: Path,
    quiet: bool = False,
) -> Tuple[int, int]:
    """Convert a single binary capture into one or more Parquet batches."""
    batches, total_packets, total_errors = converter.convert_file_to_arrow_batches(file_path)
    kept_rows = 0

    if not batches:
        return total_packets, kept_rows

    relative_dir = Path()
    try:
        relative_dir = file_path.parent.relative_to(base_dir)
    except ValueError:
        # Fallback when the file_path is not under base_dir (e.g. direct file input)
        relative_dir = Path()

    for batch in batches:
        table = converter._ipc_bytes_to_table(batch["arrow_ipc"])  # noqa: SLF001
        kept_rows += table.num_rows

        batch_name = f"{file_path.stem}_batch{batch['batch_index']:03}.parquet"
        target_path = output_root / relative_dir / batch_name
        ensure_directory(target_path)
        pq.write_table(table, target_path, compression="zstd")

    dropped_rows = total_errors
    if dropped_rows and total_packets and not quiet:
        print(
            f"   ↳ dropped {dropped_rows:,} packets without metadata "
            f"({dropped_rows / total_packets:.1%} of file)",
            file=sys.stderr,
        )
    return total_packets, kept_rows


def _init_worker(
    cache_path_str: str,
    batch_size: int,
    drop_unknown: bool,
    max_mem_bytes: Optional[int],
    output_root_str: str,
) -> None:
    """Initializer for multiprocessing workers."""
    global _WORKER_CONVERTER, _WORKER_OUTPUT_ROOT
    if max_mem_bytes:
        try:
            import resource

            resource.setrlimit(resource.RLIMIT_AS, (max_mem_bytes, max_mem_bytes))
        except Exception:  # noqa: BLE001
            pass
    token_cache = TokenCacheManager(cache_path=cache_path_str, verbose=False)
    _WORKER_CONVERTER = ProductionZerodhaBinaryConverter(
        db_path=None,
        batch_size=batch_size,
        token_cache=token_cache,
        ensure_schema=False,
        drop_unknown_tokens=drop_unknown,
    )
    _WORKER_OUTPUT_ROOT = Path(output_root_str)


def _worker_task(args_tuple: Tuple[str, str]) -> Tuple[str, int, int]:
    """Process a single file in a worker process."""
    if _WORKER_CONVERTER is None or _WORKER_OUTPUT_ROOT is None:
        raise RuntimeError("Worker converter not initialised")
    file_path_str, base_dir_str = args_tuple
    file_path = Path(file_path_str)
    base_dir = Path(base_dir_str)
    packets, rows = convert_file_to_parquet(
        _WORKER_CONVERTER,
        file_path,
        base_dir,
        _WORKER_OUTPUT_ROOT,
        quiet=True,
    )
    return file_path_str, packets, rows


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Zerodha .bin/.dat captures to Parquet with metadata enrichment.",
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Files or directories containing .bin/.dat captures.",
    )
    parser.add_argument(
        "--output-dir",
        default="parquet_output",
        help="Directory where Parquet batches will be written (default: %(default)s).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Override converter batch size (default: %(default)s).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes (default: %(default)s).",
    )
    parser.add_argument(
        "--max-mem-gb",
        type=float,
        default=None,
        help="Optional per-worker memory cap in GB (uses RLIMIT_AS if supported).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-file progress messages.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    input_paths = [Path(p).expanduser().resolve() for p in args.paths]
    output_root = Path(args.output_dir).expanduser().resolve()

    pairs = discover_files(input_paths)
    if not pairs:
        print("No .bin/.dat files discovered under the provided paths.", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"📂 Discovered {len(pairs)} files to convert")
        print(f"📤 Output directory: {output_root}")

    token_cache = TokenCacheManager(verbose=not args.quiet)
    cache_path = str(token_cache.cache_path)
    max_mem_bytes = int(args.max_mem_gb * (1024**3)) if args.max_mem_gb else None

    tasks = [(file_path, base_dir) for file_path, base_dir in pairs]
    total_packets = 0
    total_rows = 0

    if args.workers <= 1:
        converter = ProductionZerodhaBinaryConverter(
            db_path=None,
            batch_size=args.batch_size,
            token_cache=token_cache,
            ensure_schema=False,
            drop_unknown_tokens=True,
        )
        for file_path, base_dir in tasks:
            if not args.quiet:
                print(f"→ {file_path}")
            packets, rows = convert_file_to_parquet(converter, file_path, base_dir, output_root, quiet=args.quiet)
            total_packets += packets
            total_rows += rows
    else:
        ctx = mp.get_context("spawn")
        iterable = [(str(fp), str(bd)) for fp, bd in tasks]
        with ctx.Pool(
            processes=args.workers,
            initializer=_init_worker,
            initargs=(cache_path, args.batch_size, True, max_mem_bytes, str(output_root)),
        ) as pool:
            for file_path_str, packets, rows in pool.imap_unordered(_worker_task, iterable):
                total_packets += packets
                total_rows += rows
                if not args.quiet:
                    retained_ratio = rows / packets if packets else 0.0
                    dropped = packets - rows
                    print(f"✓ {file_path_str} → kept {rows:,}/{packets:,} ({retained_ratio:.1%})")
                    if dropped and packets:
                        print(
                            f"   ↳ dropped {dropped:,} packets without metadata ({dropped / packets:.1%})",
                            file=sys.stderr,
                        )

    dropped = total_packets - total_rows
    retained_ratio = 1.0
    if total_packets:
        retained_ratio = total_rows / total_packets
    if not args.quiet:
        print(
            f"\n✅ Conversion complete: kept {total_rows:,} rows out of {total_packets:,} packets "
            f"({retained_ratio:.1%} retained)",
        )
    else:
        print(
            f"{total_rows},{total_packets},{dropped}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
