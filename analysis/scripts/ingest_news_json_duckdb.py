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


def extract_underlying_symbol(symbol: str) -> str:
    """Extract underlying symbol from option/future symbols"""
    if not symbol:
        return symbol
    
    symbol_upper = symbol.upper()
    
    # Remove exchange prefixes
    if ':' in symbol_upper:
        symbol_upper = symbol_upper.split(':')[-1]
    
    # Check for index names first (BANKNIFTY, NIFTY, etc.)
    index_match = re.search(r'(BANKNIFTY|NIFTY|FINNIFTY|MIDCPNIFTY)', symbol_upper)
    if index_match:
        return index_match.group(1)
    
    # For options (CE/PE), extract everything before the date pattern
    if 'CE' in symbol_upper or 'PE' in symbol_upper:
        # Pattern: SYMBOL + DDMMM + STRIKE + CE/PE (e.g., BANKNIFTY25OCT23400CE)
        match = re.search(r'^([A-Z]+)\d{2}[A-Z]{3}', symbol_upper)
        if match:
            return match.group(1)
        # Fallback: extract before digits
        match = re.search(r'^([A-Z]+)\d', symbol_upper)
        if match:
            return match.group(1)
    
    # For futures (FUT), extract everything before the date
    if symbol_upper.endswith('FUT'):
        match = re.search(r'^([A-Z]+)\d', symbol_upper)
        if match:
            return match.group(1)
    
    return symbol_upper


def get_sector_from_symbol(symbol: str, token_cache=None) -> str:
    """Map symbol to sector based on naming patterns and token cache"""
    if not symbol:
        return ""
    
    symbol_upper = symbol.upper()
    
    # Bank sector indicators
    bank_keywords = ['BANK', 'HDFC', 'ICICI', 'SBI', 'AXIS', 'KOTAK', 'INDUSIND', 
                     'YESBANK', 'FEDERAL', 'IDFC', 'BANDHAN', 'RBL', 'UNION',
                     'PNB', 'CANARA', 'BANKOFBARODA', 'BANKOFINDIA']
    
    # IT sector indicators
    it_keywords = ['INFOSYS', 'TCS', 'WIPRO', 'HCL', 'TECHMAHINDRA', 'LTIM', 
                   'LTTS', 'PERSISTENT', 'MINDTREE', 'COFORGE', 'MPHASIS']
    
    # Auto sector indicators
    auto_keywords = ['MARUTI', 'M&M', 'TATA', 'MOTORS', 'BAJAJ', 'HERO', 
                     'EICHER', 'ASHOK', 'LEYLAND', 'TVS', 'MOTHERSUM']
    
    # Pharma sector indicators
    pharma_keywords = ['SUNPHARMA', 'DRREDDY', 'CIPLA', 'LUPIN', 'TORRENT', 
                       'AUROBINDO', 'DIVIS', 'GLENMARK', 'CADILA']
    
    # FMCG sector indicators
    fmcg_keywords = ['HUL', 'ITC', 'NESTLE', 'DABUR', 'MARICO', 'BRITANNIA', 
                     'GODREJ', 'TATA', 'CONSUMER']
    
    # Energy sector indicators
    energy_keywords = ['RELIANCE', 'ONGC', 'GAIL', 'IOC', 'BPCL', 'HPCL', 
                       'OIL', 'PETRONET']
    
    # Metal sector indicators
    metal_keywords = ['TATASTEEL', 'JSWSTEEL', 'SAIL', 'JINDAL', 'VEDANTA', 
                      'HINDALCO', 'NMDC']
    
    # Check symbol against keywords
    for keyword in bank_keywords:
        if keyword in symbol_upper:
            return "BANK"
    
    for keyword in it_keywords:
        if keyword in symbol_upper:
            return "IT"
    
    for keyword in auto_keywords:
        if keyword in symbol_upper:
            return "AUTO"
    
    for keyword in pharma_keywords:
        if keyword in symbol_upper:
            return "PHARMA"
    
    for keyword in fmcg_keywords:
        if keyword in symbol_upper:
            return "FMCG"
    
    for keyword in energy_keywords:
        if keyword in symbol_upper:
            return "ENERGY"
    
    for keyword in metal_keywords:
        if keyword in symbol_upper:
            return "METAL"
    
    # Check token cache for instrument_type if available
    if token_cache:
        token = token_cache.symbol_to_token(symbol)
        if token:
            info = token_cache.get_instrument_info(token)
            inst_type = info.get('instrument_type', '')
            if inst_type == 'EQ':
                # Try to infer from symbol name patterns
                if any(kw in symbol_upper for kw in bank_keywords):
                    return "BANK"
                elif any(kw in symbol_upper for kw in it_keywords):
                    return "IT"
    
    return ""


def extract_symbols_from_text(text: str, token_cache=None) -> Dict[str, Any]:
    """
    Extract stock symbols from news text with sector categorization.
    Returns dict with 'symbols', 'sectors', and 'sub_categories'.
    """
    if not text:
        return {'symbols': [], 'sectors': [], 'sub_categories': []}
    
    symbols = set()
    sectors = set()
    sub_categories = set()
    text_upper = text.upper()
    
    # If token_cache available, use optimized lookup
    if token_cache:
        checked_symbols = set()
        
        # Extract potential symbol patterns from text (uppercase words 2-15 chars)
        potential_symbols = set()
        pattern = r'\b([A-Z]{2,15})\b'
        matches = re.findall(pattern, text_upper)
        common_words = {
            'THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ONE',
            'OUR', 'OUT', 'DAY', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS', 'MAY', 'NEW', 'NOW',
            'OLD', 'SEE', 'TWO', 'WAY', 'WHO', 'BOY', 'DID', 'LET', 'PUT', 'SAY',
            'SHE', 'TOO', 'USE', 'NSE', 'BSE', 'FII', 'DII', 'IPO', 'EPS', 'PEG', 'PE', 'ROI',
            'WITH', 'FROM', 'THIS', 'THAT', 'THAN', 'THEY', 'WHAT', 'WHEN', 'WHERE', 'WHICH'
        }
        
        for match in matches:
            if match not in common_words and len(match) >= 2:
                potential_symbols.add(match)
        
        # Check for option/future symbols (longer patterns)
        option_pattern = r'\b([A-Z]+\d{2}[A-Z]{3}\d+(?:CE|PE|FUT))\b'
        option_matches = re.findall(option_pattern, text_upper)
        for opt_match in option_matches:
            potential_symbols.add(opt_match)
        
        # Now process each potential symbol
        for potential in potential_symbols:
            if potential in checked_symbols:
                continue
            checked_symbols.add(potential)
            
            # Check if it's an option/future and extract underlying
            underlying = extract_underlying_symbol(potential)
            
            # Check if underlying exists in token cache
            token = token_cache.symbol_to_token(underlying)
            if token:
                symbols.add(underlying)  # Use underlying symbol, not the option
                
                # Get sector for underlying
                sector = get_sector_from_symbol(underlying, token_cache)
                if sector:
                    sectors.add(sector)
                    sub_categories.add(underlying)  # Add underlying as sub-category
            elif token_cache.symbol_to_token(potential):
                # Direct symbol match (equity)
                symbols.add(potential)
                sector = get_sector_from_symbol(potential, token_cache)
                if sector:
                    sectors.add(sector)
                    sub_categories.add(potential)
        
        # Check for common index patterns
        if 'BANKNIFTY' in text_upper or 'BANK NIFTY' in text_upper:
            symbols.add('BANKNIFTY')
            sectors.add('BANK')
        if 'NIFTY' in text_upper:
            symbols.add('NIFTY')
            sectors.add('INDEX')
        if 'FINNIFTY' in text_upper:
            symbols.add('FINNIFTY')
            sectors.add('INDEX')
        
        return {
            'symbols': list(symbols)[:20],
            'sectors': list(sectors),
            'sub_categories': list(sub_categories)[:20]
        }
    
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
            sector = get_sector_from_symbol(match)
            if sector:
                sectors.add(sector)
                sub_categories.add(match)
    
    return {
        'symbols': list(symbols)[:10],
        'sectors': list(sectors),
        'sub_categories': list(sub_categories)[:10]
    }


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
                
                # Extract symbols with sector categorization
                symbol_data = extract_symbols_from_text(content, token_cache)
                symbols = symbol_data.get('symbols', [])
                extracted_sectors = symbol_data.get('sectors', [])
                sub_categories = symbol_data.get('sub_categories', [])
                
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
                
                # Build comprehensive sector_relevance structure
                # Format: {"sectors": ["BANK", "IT"], "symbols": ["HDFCBANK", "INFY"], "sub_categories": ["HDFCBANK", "ICICIBANK"]}
                sector_relevance_dict = {
                    "sectors": list(extracted_sectors),
                    "symbols": symbols,
                    "sub_categories": sub_categories
                }
                
                # Merge with existing sector_relevance if present
                sector_relevance_raw = item.get('sector_relevance') or item.get('sector') or []
                if isinstance(sector_relevance_raw, list):
                    # Merge existing sectors
                    for existing in sector_relevance_raw:
                        if isinstance(existing, str):
                            if existing not in sector_relevance_dict["sectors"]:
                                sector_relevance_dict["sectors"].append(existing)
                            if existing not in sector_relevance_dict["symbols"]:
                                sector_relevance_dict["symbols"].append(existing)
                elif isinstance(sector_relevance_raw, str):
                    try:
                        parsed = json.loads(sector_relevance_raw)
                        if isinstance(parsed, dict):
                            # Merge with existing structure
                            if "sectors" in parsed:
                                sector_relevance_dict["sectors"].extend(parsed["sectors"])
                            if "symbols" in parsed:
                                sector_relevance_dict["symbols"].extend(parsed["symbols"])
                            if "sub_categories" in parsed:
                                sector_relevance_dict["sub_categories"].extend(parsed["sub_categories"])
                        elif isinstance(parsed, list):
                            for item in parsed:
                                if isinstance(item, str):
                                    if item not in sector_relevance_dict["sectors"]:
                                        sector_relevance_dict["sectors"].append(item)
                                    if item not in sector_relevance_dict["symbols"]:
                                        sector_relevance_dict["symbols"].append(item)
                    except:
                        # Treat as plain string
                        if sector_relevance_raw not in sector_relevance_dict["sectors"]:
                            sector_relevance_dict["sectors"].append(sector_relevance_raw)
                
                # Remove duplicates and convert to JSON
                sector_relevance_dict["sectors"] = list(set(sector_relevance_dict["sectors"]))
                sector_relevance_dict["symbols"] = list(set(sector_relevance_dict["symbols"]))
                sector_relevance_dict["sub_categories"] = list(set(sector_relevance_dict["sub_categories"]))
                
                sector_relevance = json.dumps(sector_relevance_dict)
                
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


def update_existing_news_sectors(conn: duckdb.DuckDBPyConnection, token_cache=None, batch_size: int = 1000, dry_run: bool = False) -> dict:
    """Update existing news records with new sector categorization"""
    logger.info("Starting update of existing news records with new sector categorization...")
    
    # Get all news items that need updating
    try:
        news_items = conn.execute("""
            SELECT id, title, link, sector_relevance
            FROM news_data
            WHERE sector_relevance IS NOT NULL
        """).fetchall()
    except Exception as e:
        logger.error(f"Error fetching news items: {e}")
        return {"success": False, "error": str(e), "updated": 0}
    
    total_items = len(news_items)
    logger.info(f"Found {total_items:,} news items to potentially update")
    
    updated_count = 0
    skipped_count = 0
    
    for i, (news_id, title, link, old_sector_rel) in enumerate(news_items):
        try:
            # Check if already in new format
            if old_sector_rel:
                try:
                    parsed = json.loads(old_sector_rel)
                    if isinstance(parsed, dict) and "sectors" in parsed and "symbols" in parsed:
                        skipped_count += 1
                        if i % 10000 == 0:
                            logger.info(f"Progress: {i}/{total_items} (skipped: {skipped_count}, updated: {updated_count})")
                        continue
                except:
                    pass
            
            # Re-extract symbols and sectors from title
            content = title or ""
            if link:
                # Sometimes link contains useful info
                content += " " + str(link)
            
            # Extract symbols with sector categorization
            symbol_data = extract_symbols_from_text(content, token_cache)
            symbols = symbol_data.get('symbols', [])
            extracted_sectors = symbol_data.get('sectors', [])
            sub_categories = symbol_data.get('sub_categories', [])
            
            # Build new sector_relevance structure
            sector_relevance_dict = {
                "sectors": list(extracted_sectors),
                "symbols": symbols,
                "sub_categories": sub_categories
            }
            
            # Try to merge with old data if it exists
            if old_sector_rel:
                try:
                    old_parsed = json.loads(old_sector_rel)
                    if isinstance(old_parsed, list):
                        # Old format: list of sectors
                        for old_sector in old_parsed:
                            if isinstance(old_sector, str) and old_sector.upper() not in [s.upper() for s in sector_relevance_dict["sectors"]]:
                                sector_relevance_dict["sectors"].append(old_sector.upper())
                    elif isinstance(old_parsed, str):
                        # Plain string
                        if old_parsed.upper() not in [s.upper() for s in sector_relevance_dict["sectors"]]:
                            sector_relevance_dict["sectors"].append(old_parsed.upper())
                except:
                    # If parsing fails, ignore old data
                    pass
            
            # Remove duplicates
            sector_relevance_dict["sectors"] = list(set(sector_relevance_dict["sectors"]))
            sector_relevance_dict["symbols"] = list(set(sector_relevance_dict["symbols"]))
            sector_relevance_dict["sub_categories"] = list(set(sector_relevance_dict["sub_categories"]))
            
            new_sector_rel = json.dumps(sector_relevance_dict)
            
            if not dry_run:
                # Update the record
                conn.execute("""
                    UPDATE news_data
                    SET sector_relevance = ?
                    WHERE id = ?
                """, (new_sector_rel, news_id))
            
            updated_count += 1
            
            if (i + 1) % batch_size == 0:
                if not dry_run:
                    conn.commit()
                logger.info(f"Progress: {i+1}/{total_items} (updated: {updated_count}, skipped: {skipped_count})")
        
        except Exception as e:
            logger.error(f"Error updating news item {news_id}: {e}")
            continue
    
    if not dry_run:
        conn.commit()
    
    logger.info(f"✅ Update complete: {updated_count:,} updated, {skipped_count:,} skipped (already in new format)")
    
    return {
        "success": True,
        "updated": updated_count,
        "skipped": skipped_count,
        "total": total_items
    }


def main():
    import argparse
    from token_cache import TokenCacheManager
    
    parser = argparse.ArgumentParser(description="Ingest news JSON files into DuckDB")
    parser.add_argument("--db", default="tick_data_production.db", help="Database path")
    parser.add_argument("--file", help="Specific news JSON file to ingest")
    parser.add_argument("--search-dirs", nargs="+", help="Directories to search for news files")
    parser.add_argument("--token-cache", default="core/data/token_lookup_enriched.json",
                       help="Token cache for symbol validation")
    parser.add_argument("--auto", action="store_true", help="Auto-find and ingest pending files")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--update-existing", action="store_true", help="Update existing news records with new sector categorization")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for updates")
    
    args = parser.parse_args()
    
    # Load token cache for symbol validation
    token_cache = None
    if args.token_cache and Path(args.token_cache).exists():
        token_cache = TokenCacheManager(cache_path=args.token_cache, verbose=False)
        logger.info(f"Loaded token cache: {len(token_cache.token_map):,} tokens")
    
    # Connect to DuckDB
    conn = duckdb.connect(args.db)
    ensure_news_schema(conn)
    
    if args.update_existing:
        # Update existing news records
        result = update_existing_news_sectors(conn, token_cache, args.batch_size, args.dry_run)
        if result.get("success"):
            logger.info(f"✅ Updated {result['updated']:,} news records")
        else:
            logger.error(f"❌ Update failed: {result.get('error')}")
        conn.close()
        return
    
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

