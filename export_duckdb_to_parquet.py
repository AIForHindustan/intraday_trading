#!/usr/bin/env python3
"""
Export DuckDB to Parquet for Correlation Analysis
Converts tick_data_production.db to partitioned parquet files
"""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DuckDBToParquetExporter:
    """Export DuckDB tick data to partitioned Parquet files"""
    
    def __init__(self, db_path: str, output_dir: str):
        self.db_path = Path(db_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def export_tick_data(
        self,
        table_name: str = "tick_data_corrected",
        date_partition: bool = True,
        instrument_partition: bool = False,
        date_filter: Optional[str] = None,
        batch_size: int = 1000000
    ) -> Dict:
        """Export tick_data_corrected to partitioned parquet files"""
        
        logger.info(f"Connecting to database: {self.db_path}")
        conn = duckdb.connect(str(self.db_path))
        
        # Build query with date filtering
        base_query = f"SELECT * FROM {table_name}"
        if date_filter:
            base_query += f" WHERE {date_filter}"
        else:
            # Filter out invalid dates (1970 epoch)
            base_query += " WHERE timestamp >= '2025-01-01' AND timestamp <= CURRENT_TIMESTAMP"
        
        # Check row count
        count_query = base_query.replace("SELECT *", "SELECT COUNT(*)")
        total_rows = conn.execute(count_query).fetchone()[0]
        logger.info(f"Total rows to export: {total_rows:,}")
        
        if total_rows == 0:
            logger.warning("No rows to export")
            return {"success": False, "error": "No rows to export"}
        
        # Export with partitioning
        if date_partition:
            return self._export_with_date_partition(conn, base_query, batch_size)
        elif instrument_partition:
            return self._export_with_instrument_partition(conn, base_query, batch_size)
        else:
            return self._export_single_file(conn, base_query, batch_size)
    
    def _export_with_date_partition(
        self,
        conn: duckdb.DuckDBPyConnection,
        base_query: str,
        batch_size: int
    ) -> Dict:
        """Export partitioned by date"""
        
        logger.info("Exporting with date partitioning...")
        
        # Get date range
        date_query = f"""
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as row_count
            FROM ({base_query}) sub
            GROUP BY DATE(timestamp)
            ORDER BY date
        """
        
        date_stats = conn.execute(date_query).fetchdf()
        logger.info(f"Found {len(date_stats)} unique dates")
        
        total_exported = 0
        files_created = []
        
        for _, date_row in date_stats.iterrows():
            date = date_row['date']
            row_count = date_row['row_count']
            
            logger.info(f"Exporting date {date} ({row_count:,} rows)...")
            
            # Create date partition directory
            date_dir = self.output_dir / f"date={date}"
            date_dir.mkdir(parents=True, exist_ok=True)
            
            # Query data for this date
            date_query_full = f"""
                SELECT * FROM ({base_query}) sub
                WHERE DATE(timestamp) = '{date}'
                ORDER BY timestamp, instrument_token
            """
            
            # Export in batches
            offset = 0
            batch_num = 0
            
            while offset < row_count:
                batch_query = f"{date_query_full} LIMIT {batch_size} OFFSET {offset}"
                df = conn.execute(batch_query).fetchdf()
                
                if len(df) == 0:
                    break
                
                # Convert to Arrow table
                table = pa.Table.from_pandas(df)
                
                # Write parquet file
                output_file = date_dir / f"batch_{batch_num:04d}.parquet"
                pq.write_table(
                    table,
                    output_file,
                    compression='zstd',
                    use_dictionary=True,
                    write_statistics=True
                )
                
                files_created.append(str(output_file))
                total_exported += len(df)
                batch_num += 1
                offset += batch_size
                
                if batch_num % 10 == 0:
                    logger.info(f"  Exported {total_exported:,} rows...")
        
        conn.close()
        
        logger.info(f"✅ Export complete: {total_exported:,} rows in {len(files_created)} files")
        
        return {
            "success": True,
            "total_rows": total_exported,
            "files_created": len(files_created),
            "output_directory": str(self.output_dir),
            "partitioning": "date"
        }
    
    def _export_with_instrument_partition(
        self,
        conn: duckdb.DuckDBPyConnection,
        base_query: str,
        batch_size: int
    ) -> Dict:
        """Export partitioned by instrument_token (for correlation analysis)"""
        
        logger.info("Exporting with instrument partitioning...")
        
        # Get instrument list
        inst_query = f"""
            SELECT 
                instrument_token,
                symbol,
                COUNT(*) as row_count
            FROM ({base_query}) sub
            GROUP BY instrument_token, symbol
            ORDER BY row_count DESC
        """
        
        inst_stats = conn.execute(inst_query).fetchdf()
        logger.info(f"Found {len(inst_stats)} unique instruments")
        
        total_exported = 0
        files_created = []
        
        for _, inst_row in inst_stats.iterrows():
            inst_token = inst_row['instrument_token']
            symbol = inst_row['symbol']
            row_count = inst_row['row_count']
            
            if row_count < 100:  # Skip instruments with very few rows
                continue
            
            # Create instrument partition directory
            inst_dir = self.output_dir / f"instrument_token={inst_token}"
            inst_dir.mkdir(parents=True, exist_ok=True)
            
            # Query data for this instrument
            inst_query_full = f"""
                SELECT * FROM ({base_query}) sub
                WHERE instrument_token = {inst_token}
                ORDER BY timestamp
            """
            
            # Export in batches
            offset = 0
            batch_num = 0
            
            while offset < row_count:
                batch_query = f"{inst_query_full} LIMIT {batch_size} OFFSET {offset}"
                df = conn.execute(batch_query).fetchdf()
                
                if len(df) == 0:
                    break
                
                # Convert to Arrow table
                table = pa.Table.from_pandas(df)
                
                # Write parquet file
                output_file = inst_dir / f"{symbol}_batch_{batch_num:04d}.parquet"
                pq.write_table(
                    table,
                    output_file,
                    compression='zstd',
                    use_dictionary=True,
                    write_statistics=True
                )
                
                files_created.append(str(output_file))
                total_exported += len(df)
                batch_num += 1
                offset += batch_size
        
        conn.close()
        
        logger.info(f"✅ Export complete: {total_exported:,} rows in {len(files_created)} files")
        
        return {
            "success": True,
            "total_rows": total_exported,
            "files_created": len(files_created),
            "output_directory": str(self.output_dir),
            "partitioning": "instrument"
        }
    
    def _export_single_file(
        self,
        conn: duckdb.DuckDBPyConnection,
        base_query: str,
        batch_size: int
    ) -> Dict:
        """Export to single parquet file (batched)"""
        
        logger.info("Exporting to single file (batched)...")
        
        total_rows = conn.execute(f"SELECT COUNT(*) FROM ({base_query}) sub").fetchone()[0]
        offset = 0
        batch_num = 0
        files_created = []
        
        while offset < total_rows:
            batch_query = f"{base_query} ORDER BY timestamp, instrument_token LIMIT {batch_size} OFFSET {offset}"
            df = conn.execute(batch_query).fetchdf()
            
            if len(df) == 0:
                break
            
            # Convert to Arrow table
            table = pa.Table.from_pandas(df)
            
            # Write parquet file
            output_file = self.output_dir / f"tick_data_batch_{batch_num:04d}.parquet"
            pq.write_table(
                table,
                output_file,
                compression='zstd',
                use_dictionary=True,
                write_statistics=True
            )
            
            files_created.append(str(output_file))
            offset += batch_size
            batch_num += 1
            
            if batch_num % 10 == 0:
                logger.info(f"  Exported {offset:,}/{total_rows:,} rows...")
        
        conn.close()
        
        logger.info(f"✅ Export complete: {offset:,} rows in {len(files_created)} files")
        
        return {
            "success": True,
            "total_rows": offset,
            "files_created": len(files_created),
            "output_directory": str(self.output_dir),
            "partitioning": "none"
        }


def main():
    """Main export function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Export DuckDB to Parquet")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--output", default="correlation_analysis_db/tick_data_parquet", help="Output directory")
    parser.add_argument("--partition", choices=["date", "instrument", "none"], default="date", help="Partitioning strategy")
    parser.add_argument("--date-filter", help="SQL WHERE clause for date filtering")
    parser.add_argument("--batch-size", type=int, default=1000000, help="Batch size for export")
    
    args = parser.parse_args()
    
    exporter = DuckDBToParquetExporter(args.db, args.output)
    
    if args.partition == "date":
        result = exporter.export_tick_data(
            date_partition=True,
            instrument_partition=False,
            date_filter=args.date_filter,
            batch_size=args.batch_size
        )
    elif args.partition == "instrument":
        result = exporter.export_tick_data(
            date_partition=False,
            instrument_partition=True,
            date_filter=args.date_filter,
            batch_size=args.batch_size
        )
    else:
        result = exporter.export_tick_data(
            date_partition=False,
            instrument_partition=False,
            date_filter=args.date_filter,
            batch_size=args.batch_size
        )
    
    if result.get("success"):
        print(f"\n✅ Export successful!")
        print(f"   Rows: {result['total_rows']:,}")
        print(f"   Files: {result['files_created']}")
        print(f"   Output: {result['output_directory']}")
    else:
        print(f"\n❌ Export failed: {result.get('error')}")


if __name__ == "__main__":
    main()

