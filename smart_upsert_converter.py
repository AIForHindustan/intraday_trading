from __future__ import annotations

import duckdb

from binary_to_parquet.production_binary_converter import (
    COLUMN_ORDER,
    ProductionZerodhaBinaryConverter,
)

from uuid import uuid4

try:
    import pyarrow as pa
except ImportError:  # pragma: no cover - handled at runtime
    pa = None  # type: ignore[assignment]


class SmartUpsertConverter(ProductionZerodhaBinaryConverter):
    """Drop-in converter that supports configurable DuckDB conflict handling."""

    def __init__(
        self,
        db_path: str = "tick_data_production.db",
        upsert_strategy: str = "replace",
        **kwargs,
    ) -> None:
        super().__init__(db_path=db_path, **kwargs)
        self.upsert_strategy = upsert_strategy.lower()

    def set_strategy(self, strategy: str) -> None:
        self.upsert_strategy = strategy.lower()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _insert_prefix(self) -> str:
        strategy = self.upsert_strategy
        if strategy == "replace":
            return "INSERT OR REPLACE"
        if strategy == "skip":
            return "INSERT OR IGNORE"
        if strategy == "merge":
            # DuckDB lacks MERGE, fallback to REPLACE semantics.
            return "INSERT OR REPLACE"
        return "INSERT"

    # ------------------------------------------------------------------
    # Overridden Arrow writer
    # ------------------------------------------------------------------

    def _write_arrow_table(  # type: ignore[override]
        self,
        conn: duckdb.DuckDBPyConnection,
        arrow_table: pa.Table,
    ) -> int:
        if pa is None:
            raise ImportError("pyarrow is required for Arrow conversions")

        view_name = f"arrow_batch_{uuid4().hex}"
        column_list = ", ".join(COLUMN_ORDER)
        insert_prefix = self._insert_prefix()

        try:
            conn.register(view_name, arrow_table)
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                f"""
                {insert_prefix} INTO tick_data_corrected ({column_list})
                SELECT {column_list} FROM {view_name}
                """
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
