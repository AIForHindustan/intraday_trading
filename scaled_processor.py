from pathlib import Path
import time

from singleton_db import DatabaseConnectionManager
from binary_to_parquet.production_binary_converter import (
    ProductionZerodhaBinaryConverter,
    run_audit_queries,
)
from token_cache import TokenCacheManager


class ScaledDataProcessor:
    def __init__(self, db_path: str = "tick_data_production.db", quality_threshold: float = 0.95):
        self.db_path = db_path
        token_cache = TokenCacheManager()
        self.converter = ProductionZerodhaBinaryConverter(db_path, token_cache=token_cache)
        self.quality_threshold = quality_threshold

    def process_directory_batch(self, directory_path: str, batch_size: int = 50, max_files: int = 1000) -> int:
        directory = Path(directory_path)
        if not directory.exists():
            print(f"❌ Directory not found: {directory_path}")
            return 0

        candidates = list(directory.glob("*.dat")) + list(directory.glob("*.bin"))
        total_files = min(len(candidates), max_files)
        if total_files == 0:
            print(f"⚠️ No files to process in {directory_path}")
            return 0

        print(f"🎯 Processing {total_files} files from {directory_path}")
        processed_count = 0

        for batch_index in range(0, total_files, batch_size):
            batch = candidates[batch_index : batch_index + batch_size]
            success = self._process_batch_with_quality_check(batch)
            processed_count += success
            print(f"📦 Batch {batch_index // batch_size + 1}: {success}/{len(batch)} successful")

            if processed_count and processed_count % 100 == 0:
                self._run_quality_audit()

            time.sleep(0.5)

        print(f"✅ Completed {processed_count}/{total_files} files from {directory_path}")
        return processed_count

    def _process_batch_with_quality_check(self, file_batch) -> int:
        successful_files = 0
        for file_path in file_batch:
            try:
                self.converter.process_file_with_metadata_capture(file_path)
                if self._check_file_quality(file_path):
                    successful_files += 1
                else:
                    print(f"⚠️ Quality below threshold for {file_path.name}")
            except Exception as exc:  # noqa: BLE001
                print(f"❌ Failed {file_path.name}: {exc}")
        return successful_files

    def _check_file_quality(self, file_path: Path) -> bool:
        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            metrics = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_records,
                    COUNT(CASE WHEN last_price > 0 THEN 1 END) AS valid_prices,
                    COUNT(CASE WHEN exchange_timestamp IS NOT NULL THEN 1 END) AS valid_timestamps,
                    COUNT(CASE WHEN bid_1_price IS NOT NULL THEN 1 END) AS depth_records
                FROM tick_data_corrected
                WHERE source_file = ?
                """,
                [str(file_path)],
            ).fetchone()

            if not metrics or metrics[0] == 0:
                return False

            price_quality = metrics[1] / metrics[0]
            timestamp_quality = metrics[2] / metrics[0]
            return price_quality >= self.quality_threshold and timestamp_quality >= self.quality_threshold

    def _run_quality_audit(self) -> None:
        with DatabaseConnectionManager.connection_scope(self.db_path) as conn:
            print("\n" + "=" * 60)
            print("PERIODIC QUALITY AUDIT")
            print("=" * 60)
            run_audit_queries(conn)
            print("=" * 60 + "\n")
