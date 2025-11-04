#!/usr/bin/env python3
"""
Ingest News JSON Files into DuckDB
- Parses symbols from news content
- Uses DuckDB native SQL for efficient insertion
"""

import sys
import json
import re
import hashlib
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


def ensure_news_schema(conn: duckdb.DuckDBPyConnection):
    """Ensure news_data table exists with correct schema"""
    schema_sql = """
    CREATE TABLE IF NOT EXISTS news_data (
        id VARCHAR PRIMARY KEY,
        source VARCHAR,
        title VARCHAR,
        link VARCHAR,
        date VARCHAR,
        publisher VARCHAR,
        collected_at TIMESTAMP,
        sentiment VARCHAR,
        sentiment_score DOUBLE,
        sentiment_confidence DOUBLE,
        market_impact VARCHAR,
        sector_relevance VARCHAR,
        written_at TIMESTAMP,
        processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn.execute(schema_sql)


def extract_symbols_from_text(text: str, token_cache=None) -> List[str]:
    """Extract stock symbols from news text using token cache - optimized"""
    if not text:
        return []
    
    symbols = set()
    text_upper = text.upper()
    text_words = set(text_upper.split())
    
    # If token_cache available, use optimized lookup
    if token_cache:
        # Build word set for fast lookup
        # Check symbols directly from token cache
        # Only check symbols that are likely to be in the text
        checked_symbols = set()
        
        # First, extract potential symbol patterns from text (uppercase words 2-10 chars)
        potential_symbols = set()
        pattern = r'\b([A-Z]{2,10})\b'
        matches = re.findall(pattern, text_upper)
        common_words = {
            'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ONE',
            'OUR', 'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW',
            'OLD', 'SEE', 'TWO', 'WAY', 'WHO', 'BOY', 'DID', 'LET', 'PUT', 'SAY',
            'SHE', 'TOO', 'USE', 'NSE', 'BSE', 'FII', 'DII', 'IPO', 'EPS', 'PEG', 'PE', 'ROI'
        }
        
        for match in matches:
            if match not in common_words and len(match) >= 2:
                potential_symbols.add(match)
        
        # Now check if these potential symbols exist in token cache
        for potential in potential_symbols:
            if potential in checked_symbols:
                continue
            checked_symbols.add(potential)
            
            # Check if symbol exists in token cache
            token = token_cache.symbol_to_token(potential)
            if token:
                symbols.add(potential)
        
        # Also check for common index patterns
        if 'BANKNIFTY' in text_upper or 'BANK NIFTY' in text_upper:
            symbols.add('BANKNIFTY')
        if 'NIFTY' in text_upper and '50' in text_upper:
            symbols.add('NIFTY')
        
        return list(symbols)[:20]  # Limit to 20 symbols per news item
    
    # Fallback: basic pattern matching
    pattern = r'\b([A-Z]{2,10})\b'
    matches = re.findall(pattern, text_upper)
    
    common_words = {
        'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ONE',
        'OUR', 'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW',
        'OLD', 'SEE', 'TWO', 'WAY', 'WHO', 'BOY', 'DID', 'LET', 'PUT', 'SAY',
        'SHE', 'TOO', 'USE', 'NSE', 'BSE', 'FII', 'DII', 'IPO', 'EPS', 'PEG', 'PE', 'ROI'
    }
    
    for match in matches:
        if match not in common_words and len(match) >= 2:
            symbols.add(match)
    
    return list(symbols)[:10]


def normalize_sentiment(sentiment: Any) -> tuple:
    """Normalize sentiment to (sentiment_string, score, confidence)"""
    if isinstance(sentiment, (int, float)):
        score = float(sentiment)
        if score > 0.1:
            return ("positive", score, abs(score))
        elif score < -0.1:
            return ("negative", score, abs(score))
        else:
            return ("neutral", score, abs(score))
    elif isinstance(sentiment, str):
        sentiment_lower = sentiment.lower()
        if sentiment_lower in ["positive", "bullish", "up", "gain"]:
            return ("positive", 1.0, 0.8)
        elif sentiment_lower in ["negative", "bearish", "down", "loss", "decline"]:
            return ("negative", -1.0, 0.8)
        else:
            return ("neutral", 0.0, 0.5)
    else:
        return ("neutral", 0.0, 0.5)


def parse_date(date_str: Any, default=None) -> Optional[datetime]:
    """Parse date string to datetime"""
    if not date_str:
        return default or datetime.now()
    
    if isinstance(date_str, datetime):
        return date_str
    
    try:
        # Try ISO format
        if isinstance(date_str, str):
            # Handle relative dates like "44 minutes ago"
            if "ago" in date_str.lower():
                # Parse relative time
                import re as re_module
                rel_match = re_module.match(r'(\d+)\s+(minute|hour|day|week|month|year)', date_str.lower())
                if rel_match:
                    val, unit = int(rel_match.group(1)), rel_match.group(2)
                    delta_map = {
                        "minute": 60, "hour": 3600, "day": 86400,
                        "week": 604800, "month": 2592000, "year": 31536000
                    }
                    seconds = val * delta_map.get(unit, 60)
                    return datetime.now() - timedelta(seconds=seconds)
            
            # Try ISO format
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00').replace('+00:00', ''))
            else:
                return datetime.fromisoformat(date_str)
    except:
        pass
    
    return default or datetime.now()


def generate_news_id(item: Dict[str, Any]) -> str:
    """Generate unique ID for news item"""
    # Use hash of source + title + link for deduplication
    key_str = f"{item.get('source', '')}_{item.get('title', '')}_{item.get('link', '')}"
    hash_str = hashlib.md5(key_str.encode()).hexdigest()[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{hash_str}_{timestamp}"


def ingest_news_json_file(
    conn: duckdb.DuckDBPyConnection,
    file_path: Path,
    token_cache=None
) -> dict:
    """Ingest a JSON or JSONL file containing news items"""
    try:
        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
        
        # Try to parse as JSON array or JSONL
        news_items = []
        try:
            # Try as JSON array first
            data = json.loads(content)
            if isinstance(data, list):
                news_items = data
            elif isinstance(data, dict):
                # Check if it has a news array
                if 'news' in data:
                    news_items = data['news']
                elif 'items' in data:
                    news_items = data['items']
                else:
                    news_items = [data]
        except json.JSONDecodeError:
            # Try as JSONL (one JSON object per line)
            for line in content.split('\n'):
                line = line.strip()
                if line:
                    try:
                        news_items.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        
        if not news_items:
            return {"success": False, "error": "No news items found", "row_count": 0}
        
        logger.info(f"Found {len(news_items)} news items in {file_path.name}")
        
        # Process and insert news items
        inserted_count = 0
        for item in news_items:
            try:
                # Extract fields
                title = item.get('title') or item.get('headline') or ''
                source = item.get('source') or item.get('news_source') or 'unknown'
                link = item.get('link') or item.get('url') or ''
                publisher = item.get('publisher') or item.get('source_name') or ''
                date_str = item.get('date') or item.get('published_time') or ''
                
                # Extract symbols from title and content
                content = title
                if 'content' in item:
                    content += ' ' + str(item['content'])
                if 'description' in item:
                    content += ' ' + str(item['description'])
                
                symbols = extract_symbols_from_text(content, token_cache)
                
                # Parse sentiment
                sentiment_data = item.get('sentiment') or item.get('sentiment_score')
                sentiment, sentiment_score, sentiment_confidence = normalize_sentiment(sentiment_data)
                
                # Parse dates
                collected_at = parse_date(
                    item.get('collected_at') or item.get('ingestion_timestamp'),
                    default=datetime.now()
                )
                written_at = parse_date(
                    item.get('written_at') or item.get('date') or item.get('published_time'),
                    default=collected_at
                )
                
                # Market impact and sector
                market_impact = item.get('market_impact', 'LOW')
                
                # Handle sector_relevance - could be list or string
                sector_relevance_raw = item.get('sector_relevance') or item.get('sector') or []
                if isinstance(sector_relevance_raw, list):
                    sector_relevance = json.dumps(sector_relevance_raw)
                elif isinstance(sector_relevance_raw, str):
                    # Try to parse if it's JSON string
                    try:
                        parsed = json.loads(sector_relevance_raw)
                        if isinstance(parsed, list):
                            sector_relevance = json.dumps(parsed)
                        else:
                            sector_relevance = json.dumps([sector_relevance_raw])
                    except:
                        sector_relevance = json.dumps([sector_relevance_raw])
                else:
                    sector_relevance = json.dumps([])
                
                # Add extracted symbols to sector_relevance
                if symbols:
                    sector_list = json.loads(sector_relevance) if sector_relevance else []
                    # Add symbols if not already present
                    for sym in symbols:
                        if sym not in sector_list:
                            sector_list.append(sym)
                    sector_relevance = json.dumps(sector_list)
                
                # Generate ID
                news_id = item.get('id') or generate_news_id(item)
                
                insert_query = f"""
                    INSERT OR REPLACE INTO news_data 
                    (id, source, title, link, date, publisher, collected_at, 
                     sentiment, sentiment_score, sentiment_confidence, 
                     market_impact, sector_relevance, written_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                conn.execute(insert_query, (
                    news_id,
                    source,
                    title,
                    link,
                    date_str,
                    publisher,
                    collected_at,
                    sentiment,
                    sentiment_score,
                    sentiment_confidence,
                    market_impact,
                    sector_relevance,
                    written_at
                ))
                
                inserted_count += 1
                
            except Exception as e:
                logger.error(f"Error processing news item: {e}")
                continue
        
        return {
            "success": True,
            "row_count": inserted_count,
            "original_rows": len(news_items)
        }
        
    except Exception as e:
        logger.error(f"Error ingesting {file_path}: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "row_count": 0
        }


def get_pending_news_files(db_path: str, search_dirs: List[str] = None) -> List[Path]:
    """Get list of news JSON files that haven't been ingested"""
    if search_dirs is None:
        search_dirs = ["config/data/indices/news", "."]
    
    conn = duckdb.connect(db_path)
    
    # Get already ingested news files (tracked by source + title hash)
    # Since we don't track source_file for news, we'll check by title/source combination
    try:
        ingested = conn.execute("""
            SELECT DISTINCT CONCAT(source, '_', title) as news_key
            FROM news_data
        """).fetchall()
        ingested_keys = {row[0] for row in ingested if row[0]}
    except:
        ingested_keys = set()
    
    conn.close()
    
    # Find news JSON files
    news_files = []
    for directory in search_dirs:
        dir_path = Path(directory)
        if dir_path.exists():
            # Look for news*.json and news*.jsonl files
            for pattern in ["news*.json", "news*.jsonl"]:
                for f in dir_path.rglob(pattern):
                    if f.is_file() and f not in news_files:
                        news_files.append(f)
    
    return sorted(news_files)


def main():
    import argparse
    from token_cache import TokenCacheManager
    
    parser = argparse.ArgumentParser(description="Ingest news JSON files into DuckDB")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--file", help="Specific news JSON file to ingest")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for news files")
    parser.add_argument("--token-cache", default="zerodha_token_list/all_extracted_tokens_merged.json",
                       help="Token cache for symbol validation")
    parser.add_argument("--auto", action="store_true", help="Auto-find and ingest pending files")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    
    args = parser.parse_args()
    
    # Load token cache for symbol validation
    token_cache = None
    if args.token_cache and Path(args.token_cache).exists():
        token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False)
        logger.info(f"Loaded token cache: {len(token_cache.token_map):,} tokens")
    
    # Connect to DuckDB
    conn = duckdb.connect(args.db)
    ensure_news_schema(conn)
    
    if args.file:
        # Ingest specific file
        file_path = Path(args.file)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return
        
        result = ingest_news_json_file(conn, file_path, token_cache)
        if result.get("success"):
            logger.info(f"✅ Ingested {result['row_count']} news items from {file_path.name}")
        else:
            logger.error(f"❌ Failed: {result.get('error')}")
    
    elif args.auto:
        # Find and ingest pending files
        search_dirs = args.search_dirs or ["config/data/indices/news", "."]
        news_files = get_pending_news_files(args.db, search_dirs)
        
        logger.info(f"Found {len(news_files)} news JSON files")
        
        if args.dry_run:
            logger.info("DRY RUN - Would ingest these files:")
            for f in news_files[:10]:
                logger.info(f"  {f}")
            return
        
        total_rows = 0
        files_completed = 0
        errors = 0
        
        for file_path in news_files:
            result = ingest_news_json_file(conn, file_path, token_cache)
            if result.get("success"):
                total_rows += result.get("row_count", 0)
                files_completed += 1
                if files_completed % 10 == 0:
                    logger.info(f"Progress: {files_completed}/{len(news_files)} files, {total_rows:,} news items")
            else:
                errors += 1
                if errors <= 10:
                    logger.error(f"Failed {file_path.name}: {result.get('error')}")
        
        logger.info(f"✅ Complete: {files_completed} files, {total_rows:,} news items, {errors} errors")
    
    else:
        parser.print_help()
    
    conn.close()


if __name__ == "__main__":
    from datetime import timedelta
    main()

