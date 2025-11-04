#!/usr/bin/env python3
"""
Database Analysis Script
Analyzes all database files to understand their structure, content, and differences
"""

import duckdb
from pathlib import Path
from datetime import datetime
import json

def analyze_duckdb(db_path: Path):
    """Analyze a DuckDB database"""
    if not db_path.exists():
        return None
    
    print(f"\n{'='*80}")
    print(f"Database: {db_path.name}")
    print(f"Size: {db_path.stat().st_size / (1024**3):.2f} GB")
    print(f"Modified: {datetime.fromtimestamp(db_path.stat().st_mtime)}")
    print(f"{'='*80}")
    
    try:
        conn = duckdb.connect(str(db_path))
        
        # Get all tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"\nTables ({len(tables)}):")
        
        db_info = {
            'path': str(db_path),
            'size_gb': db_path.stat().st_size / (1024**3),
            'modified': datetime.fromtimestamp(db_path.stat().st_mtime).isoformat(),
            'tables': {}
        }
        
        for table_name_tuple in tables:
            table_name = table_name_tuple[0]
            print(f"\n  📊 Table: {table_name}")
            
            # Get schema
            try:
                schema = conn.execute(f"DESCRIBE {table_name}").df()
                print(f"    Columns ({len(schema)}):")
                for _, col in schema.iterrows():
                    print(f"      - {col['column_name']}: {col['column_type']}")
                
                # Get row count
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"    Row Count: {row_count:,}")
                
                # Get sample data date range if timestamp column exists
                timestamp_cols = ['timestamp', 'exchange_timestamp', 'last_traded_timestamp', 'date']
                date_range = None
                for ts_col in timestamp_cols:
                    try:
                        result = conn.execute(f"""
                            SELECT 
                                MIN({ts_col}) as min_date,
                                MAX({ts_col}) as max_date
                            FROM {table_name}
                            WHERE {ts_col} IS NOT NULL
                        """).fetchone()
                        if result and result[0]:
                            date_range = {
                                'column': ts_col,
                                'min': str(result[0]),
                                'max': str(result[1])
                            }
                            print(f"    Date Range ({ts_col}): {result[0]} to {result[1]}")
                            break
                    except:
                        continue
                
                # Get unique instruments if applicable
                if 'instrument_token' in schema['column_name'].values:
                    unique_instruments = conn.execute(
                        f"SELECT COUNT(DISTINCT instrument_token) FROM {table_name}"
                    ).fetchone()[0]
                    print(f"    Unique Instruments: {unique_instruments:,}")
                
                if 'symbol' in schema['column_name'].values:
                    unique_symbols = conn.execute(
                        f"SELECT COUNT(DISTINCT symbol) FROM {table_name}"
                    ).fetchone()[0]
                    print(f"    Unique Symbols: {unique_symbols:,}")
                
                db_info['tables'][table_name] = {
                    'columns': schema.to_dict('records'),
                    'row_count': int(row_count),
                    'date_range': date_range,
                    'unique_instruments': unique_instruments if 'instrument_token' in schema['column_name'].values else None,
                    'unique_symbols': unique_symbols if 'symbol' in schema['column_name'].values else None
                }
                
            except Exception as e:
                print(f"    ⚠️ Error analyzing table: {e}")
                db_info['tables'][table_name] = {'error': str(e)}
        
        conn.close()
        return db_info
        
    except Exception as e:
        print(f"  ❌ Error connecting to database: {e}")
        return None

def main():
    """Main analysis function"""
    print("="*80)
    print("DATABASE STRUCTURE ANALYSIS")
    print("="*80)
    
    project_root = Path(__file__).parent
    db_files = [
        project_root / "tick_data_production.db",
        project_root / "nse_tick_data.duckdb",
        project_root / "intraday_trading.db",
        project_root / "new_tick_data.db",
        project_root / "old_tick_data.db",
    ]
    
    all_results = {}
    
    for db_file in db_files:
        if db_file.exists():
            result = analyze_duckdb(db_file)
            if result:
                all_results[db_file.name] = result
    
    # Save results to JSON
    output_file = project_root / "database_analysis_report.json"
    with open(output_file, 'w') as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n{'='*80}")
    print(f"Analysis complete! Results saved to: {output_file}")
    print(f"{'='*80}")
    
    # Summary
    print("\n📊 SUMMARY:")
    print("\nDatabase Files:")
    for db_name, info in all_results.items():
        print(f"  • {db_name}: {info['size_gb']:.2f} GB, {len(info['tables'])} tables")
        if 'tick_data_corrected' in info['tables']:
            table_info = info['tables']['tick_data_corrected']
            print(f"    → tick_data_corrected: {table_info.get('row_count', 0):,} rows")
            if table_info.get('date_range'):
                print(f"    → Date range: {table_info['date_range']['min']} to {table_info['date_range']['max']}")

if __name__ == "__main__":
    main()

