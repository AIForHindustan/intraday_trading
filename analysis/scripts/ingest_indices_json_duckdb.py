#!/usr/bin/env python3
"""
Ingest Indices JSONL Files into DuckDB
- Parses indices data from JSONL files
- Uses DuckDB native SQL for efficient insertion
- Handles news sentiment aggregation
"""

import sys
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import duckdb
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ensure_indices_schema(conn: duckdb.DuckDBPyConnection):
    """Ensure daily_indices table exists with correct schema"""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS daily_indices (
        instrument_token BIGINT,
        symbol VARCHAR,
        tradingsymbol VARCHAR,
        timestamp TIMESTAMP,
        last_price DOUBLE,
        net_change DOUBLE,
        percent_change DOUBLE,
        exchange VARCHAR,
        segment VARCHAR,
        mode VARCHAR,
        ohlc_open DOUBLE,
        ohlc_high DOUBLE,
        ohlc_low DOUBLE,
        ohlc_close DOUBLE,
        timestamp_ns BIGINT,
        news_sentiment_positive INTEGER,
        news_sentiment_negative INTEGER,
        news_sentiment_neutral INTEGER,
        news_total_count INTEGER,
        news_sentiment_score DOUBLE,
        market_news_available BOOLEAN,
        news_sources VARCHAR,
        news_last_updated TIMESTAMP,
        processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (instrument_token, timestamp_ns)
    );
    """
    try:
        conn.execute(schema_sql)
    except Exception as e:
        # Table might exist without PRIMARY KEY, try to add it
        if "already exists" in str(e).lower():
            try:
                conn.execute("""
                    ALTER TABLE daily_indices 
                    ADD PRIMARY KEY (instrument_token, timestamp_ns)
                """)
            except:
                pass  # PRIMARY KEY might already exist


def parse_timestamp(ts: Any) -> tuple:
    """Parse timestamp to (datetime, nanoseconds)"""
    if isinstance(ts, datetime):
        ns = int(ts.timestamp() * 1e9)
        return (ts, ns)
    
    if isinstance(ts, (int, float)):
        # Assume nanoseconds if > 1e15, else seconds
        if ts > 1e15:
            dt = datetime.fromtimestamp(ts / 1e9)
            return (dt, int(ts))
        else:
            dt = datetime.fromtimestamp(ts)
            return (dt, int(ts * 1e9))
    
    if isinstance(ts, str):
        try:
            # Try ISO format
            if 'T' in ts:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00').replace('+00:00', ''))
            else:
                dt = datetime.fromisoformat(ts)
            ns = int(dt.timestamp() * 1e9)
            return (dt, ns)
        except:
            pass
    
    # Default to now
    dt = datetime.now()
    return (dt, int(dt.timestamp() * 1e9))


def normalize_news_sentiment(news_data: Any) -> dict:
    """Normalize news sentiment data from various formats"""
    if isinstance(news_data, dict):
        return {
            'positive': int(news_data.get('positive', news_data.get('news_sentiment_positive', 0))),
            'negative': int(news_data.get('negative', news_data.get('news_sentiment_negative', 0))),
            'neutral': int(news_data.get('neutral', news_data.get('news_sentiment_neutral', 0))),
            'total': int(news_data.get('total', news_data.get('news_total_count', 0))),
            'score': float(news_data.get('score', news_data.get('news_sentiment_score', 0.0))),
            'available': bool(news_data.get('available', news_data.get('market_news_available', False))),
            'sources': news_data.get('sources', news_data.get('news_sources', '[]')),
            'last_updated': news_data.get('last_updated', news_data.get('news_last_updated'))
        }
    
    return {
        'positive': 0,
        'negative': 0,
        'neutral': 0,
        'total': 0,
        'score': 0.0,
        'available': False,
        'sources': '[]',
        'last_updated': None
    }


def ingest_indices_jsonl_file(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    token_cache=None
) -> dict:
    """Ingest a JSONL file containing indices data"""
    try:
        # Read JSONL file
        indices_items = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        indices_items.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        
        if not indices_items:
            return {"success": False, "error": "No indices items found", "row_count": 0}
        
        logger.info(f"Found {len(indices_items)} indices items in {file_path.name}")
        
        # Process and insert indices items
        inserted_count = 0
        for item in indices_items:
            try:
                # Extract fields
                instrument_token = item.get('instrument_token') or item.get('token') or None
                symbol = item.get('symbol') or item.get('tradingsymbol') or ''
                tradingsymbol = item.get('tradingsymbol') or symbol
                exchange = item.get('exchange') or 'NSE'
                segment = item.get('segment') or 'INDEX'
                mode = item.get('mode') or 'quote'
                
                # Parse timestamp
                ts = item.get('timestamp') or item.get('timestamp_ns')
                timestamp, timestamp_ns = parse_timestamp(ts)
                
                # Price data
                last_price = float(item.get('last_price') or item.get('price') or 0.0)
                net_change = float(item.get('net_change') or item.get('change') or 0.0)
                percent_change = float(item.get('percent_change') or item.get('percent_change') or 0.0)
                
                # OHLC data - handle nested ohlc object or flat fields
                ohlc_obj = item.get('ohlc', {})
                ohlc_open = float(
                    item.get('ohlc_open') or 
                    (ohlc_obj.get('open') if isinstance(ohlc_obj, dict) else None) or 
                    item.get('open') or 
                    item.get('open_price') or 
                    0.0
                )
                ohlc_high = float(
                    item.get('ohlc_high') or 
                    (ohlc_obj.get('high') if isinstance(ohlc_obj, dict) else None) or 
                    item.get('high') or 
                    item.get('high_price') or 
                    0.0
                )
                ohlc_low = float(
                    item.get('ohlc_low') or 
                    (ohlc_obj.get('low') if isinstance(ohlc_obj, dict) else None) or 
                    item.get('low') or 
                    item.get('low_price') or 
                    0.0
                )
                ohlc_close = float(
                    item.get('ohlc_close') or 
                    (ohlc_obj.get('close') if isinstance(ohlc_obj, dict) else None) or 
                    item.get('close') or 
                    item.get('close_price') or 
                    last_price or 
                    0.0
                )
                
                # News sentiment data - can be nested or flat
                news_data = {}
                if 'news' in item and isinstance(item['news'], dict):
                    news_data = item['news']
                elif 'news_sentiment' in item:
                    news_data = item['news_sentiment']
                else:
                    # Try to extract news fields directly from item
                    news_data = {
                        'positive': item.get('news_sentiment_positive'),
                        'negative': item.get('news_sentiment_negative'),
                        'neutral': item.get('news_sentiment_neutral'),
                        'total': item.get('news_total_count'),
                        'score': item.get('news_sentiment_score'),
                        'available': item.get('market_news_available'),
                        'sources': item.get('news_sources'),
                        'last_updated': item.get('news_last_updated')
                    }
                news_norm = normalize_news_sentiment(news_data)
                
                # Parse news sources
                news_sources = news_norm['sources']
                if isinstance(news_sources, list):
                    news_sources = json.dumps(news_sources)
                elif isinstance(news_sources, str):
                    try:
                        # Validate it's JSON
                        json.loads(news_sources)
                    except:
                        news_sources = json.dumps([news_sources]) if news_sources else '[]'
                else:
                    news_sources = '[]'
                
                # Parse news_last_updated
                news_last_updated = news_norm['last_updated']
                if news_last_updated:
                    if isinstance(news_last_updated, str):
                        try:
                            news_last_updated = datetime.fromisoformat(news_last_updated.replace('Z', '+00:00').replace('+00:00', ''))
                        except:
                            news_last_updated = timestamp
                    elif not isinstance(news_last_updated, datetime):
                        news_last_updated = timestamp
                else:
                    news_last_updated = timestamp
                
                # Insert into DuckDB - use MERGE for upsert
                # First, delete existing row if it exists
                delete_query = """
                    DELETE FROM daily_indices 
                    WHERE instrument_token = ? AND timestamp_ns = ?
                """
                conn.execute(delete_query, (instrument_token, timestamp_ns))
                
                # Then insert
                insert_query = """
                    INSERT INTO daily_indices 
                    (instrument_token, symbol, tradingsymbol, timestamp, last_price,
                     net_change, percent_change, exchange, segment, mode,
                     ohlc_open, ohlc_high, ohlc_low, ohlc_close, timestamp_ns,
                     news_sentiment_positive, news_sentiment_negative, news_sentiment_neutral,
                     news_total_count, news_sentiment_score, market_news_available,
                     news_sources, news_last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                conn.execute(insert_query, (
                    instrument_token,
                    symbol,
                    tradingsymbol,
                    timestamp,
                    last_price,
                    net_change,
                    percent_change,
                    exchange,
                    segment,
                    mode,
                    ohlc_open,
                    ohlc_high,
                    ohlc_low,
                    ohlc_close,
                    timestamp_ns,
                    news_norm['positive'],
                    news_norm['negative'],
                    news_norm['neutral'],
                    news_norm['total'],
                    news_norm['score'],
                    news_norm['available'],
                    news_sources,
                    news_last_updated
                ))
                
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error processing indices item: {e}")
                continue
        
        return {
            "success": True,
            "row_count": inserted_count,
            "original_rows": len(indices_items)
        }
        
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "row_count": 0
        }


def get_pending_indices_files(db_path: str, search_dirs: List[str] = None) -> List[Path]:
    """Get list of indices JSONL files that haven't been ingested"""
    if search_dirs is None:
        search_dirs = ["config/data/indices", "crawlers/raw_data/data_mining"]
    
    # Find indices JSONL files
    indices_files = []
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            for f in dir_path.rglob("indices_data_*.jsonl"):
                if f.is_file() and f not in indices_files:
                    indices_files.append(f)
    
    return sorted(indices_files)


def main():
    import argparse
    from token_cache import TokenCacheManager
    
    parser = argparse.ArgumentParser(description="Ingest indices JSONL files into DuckDB")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--file", help="Specific indices JSONL file to ingest")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for indices files")
    parser.add_argument("--token-cache", default="zerodha_token_list/all_extracted_tokens_merged.json",
                       help="Token cache for symbol validation (optional)")
    parser.add_argument("--auto", action="store_true", help="Auto-find and ingest pending files")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    
    args = parser.parse_args()
    
    # Load token cache (optional for indices)
    token_cache = None
    if args.token_cache and Path(args.token_cache).exists():
        try:
            token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False)
            logger.info(f"Loaded token cache: {len(token_cache.token_map):,} tokens")
        except:
            logger.warning("Could not load token cache, continuing without it")
    
    # Connect to DuckDB
    conn = duckdb.connect(args.db)
    ensure_indices_schema(conn)
    
    if args.file:
        # Ingest specific file
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        
        result = ingest_indices_jsonl_file(conn, file_path, token_cache)
        if result.get("success"):
            logger.info(f"✅ Ingested {result['row_count']} indices items from {file_path.name}")
        else:
            logger.error(f"❌ Failed: {result.get('error')}")
    
    elif args.auto:
        # Find and ingest pending files
        search_dirs = args.search_dirs or ["config/data/indices", "crawlers/raw_data/data_mining"]
        indices_files = get_pending_indices_files(args.db, search_dirs)
        
        logger.info(f"Found {len(indices_files)} indices JSONL files")
        
        if args.dry_run:
            logger.info("DRY RUN - Would ingest these files:")
            for f in indices_files[:10]:
                logger.info(f"  {f}")
            return
        
        total_rows = 0
        files_completed = 0
        errors = 0
        
        for file_path in indices_files:
            result = ingest_indices_jsonl_file(conn, file_path, token_cache)
            if result.get("success"):
                total_rows += result.get("row_count", 0)
                files_completed += 1
                logger.info(f"✅ {file_path.name}: {result.get('row_count', 0):,} items")
                if files_completed % 10 == 0:
                    logger.info(f"Progress: {files_completed}/{len(indices_files)} files, {total_rows:,} indices items")
            else:
                errors += 1
                if errors <= 10:
                    logger.error(f"❌ Failed {file_path.name}: {result.get('error')}")
        
        logger.info(f"✅ Complete: {files_completed} files, {total_rows:,} indices items, {errors} errors")
    
    else:
        parser.print_help()
    
    conn.close()


if __name__ == "__main__":
    main()

