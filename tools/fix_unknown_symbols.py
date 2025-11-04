#!/usr/bin/env python3
"""
Fix UNKNOWN symbols by re-parsing binary files with improved offset detection.

This script:
1. Finds parquet files with UNKNOWN symbols in the database
2. Locates the original binary files
3. Re-parses them with comprehensive offset testing
4. Updates the database with correct tokens and metadata
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set
import duckdb
import re
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
from token_cache import TokenCacheManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UnknownSymbolFixer:
    """Fix UNKNOWN symbols by re-parsing binary files with correct offset detection."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.token_cache = TokenCacheManager(
            cache_path="core/data/token_lookup_enriched.json",
            verbose=False
        )
        self.converter = ProductionZerodhaBinaryConverter(
            db_path=None,  # We'll handle DB operations separately
            token_cache=self.token_cache,
            drop_unknown_tokens=False,
            ensure_schema=False
        )
        
        # Search paths for binary files
        self.search_paths = [
            Path('temp_parquet/windowsdata_source'),
            Path('temp_parquet/macdata_source'),
            Path('temp'),
            Path('temp_parquet'),
            Path('crawlers/raw_data/intraday_data'),
            Path('crawlers/raw_data/data_mining'),
        ]
    
    def find_unknown_symbols(self, limit: int = 100) -> List[Dict]:
        """Find source files with UNKNOWN symbols in database."""
        conn = duckdb.connect(self.db_path, read_only=True)
        
        query = """
            SELECT 
                source_file,
                instrument_token as current_token,
                symbol,
                COUNT(*) as row_count,
                MIN(exchange_timestamp) as first_seen,
                MAX(exchange_timestamp) as last_seen
            FROM tick_data_corrected
            WHERE exchange_timestamp >= '2025-01-01' AND exchange_timestamp < '2026-01-01'
              AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
            GROUP BY source_file, instrument_token, symbol
            ORDER BY row_count DESC
            LIMIT ?
        """
        
        results = conn.execute(query, [limit]).fetchdf()
        conn.close()
        
        return results.to_dict('records')
    
    def find_original_binary_file(self, parquet_name: str) -> Optional[Path]:
        """Find the original binary file that created this parquet file."""
        stem = Path(parquet_name).stem
        
        # Extract date and time from parquet name
        # Format: intraday_data_20251009_123305
        match = re.search(r'(\d{8})_(\d{6})', stem)
        if match:
            date = match.group(1)
            time = match.group(2)
        else:
            date = None
            time = None
        
        # Search for binary files
        for search_path in self.search_paths:
            if not search_path.exists():
                continue
            
            # Try different patterns
            patterns = [
                f"*{date}*{time}*.bin" if date and time else None,
                f"*{date}*{time}*.dat" if date and time else None,
                f"*{stem}*.bin",
                f"*{stem}*.dat",
            ]
            
            for pattern in patterns:
                if pattern is None:
                    continue
                matches = list(search_path.rglob(pattern))
                if matches:
                    return matches[0]
        
        return None
    
    def reparse_binary_file(self, binary_file: Path) -> List[Dict]:
        """Re-parse binary file with improved offset detection."""
        logger.info(f"Re-parsing {binary_file.name}")
        
        try:
            raw_data = binary_file.read_bytes()
            packets = self.converter._detect_packet_format(raw_data)
            
            logger.info(f"  Parsed {len(packets)} packets")
            
            # Enrich with metadata
            enriched_packets = []
            for packet in packets:
                token = packet.get('instrument_token', 0)
                if 50 <= token < 50_000_000:  # Valid token range
                    # Get metadata
                    metadata = self.converter._get_token_metadata(None, token, packet)
                    
                    # Update packet with metadata
                    packet['symbol'] = metadata.get('symbol', f'UNKNOWN_{token}')
                    packet['exchange'] = metadata.get('exchange')
                    packet['segment'] = metadata.get('segment')
                    packet['instrument_type'] = metadata.get('instrument_type')
                    
                    enriched_packets.append(packet)
            
            logger.info(f"  Enriched {len(enriched_packets)} packets with valid tokens")
            return enriched_packets
            
        except Exception as e:
            logger.error(f"Error re-parsing {binary_file.name}: {e}")
            return []
    
    def create_token_mapping_from_database(
        self,
        source_file: str,
        wrong_token: int,
        packets: List[Dict]
    ) -> Dict[int, int]:
        """Create token mapping by matching database rows with parsed packets.
        
        Matches based on:
        1. exchange_timestamp_ns (most reliable)
        2. last_price (if timestamp matches)
        3. volume (if price matches)
        """
        conn = duckdb.connect(self.db_path, read_only=True)
        
        # Get sample rows from database with wrong token
        query = """
            SELECT 
                exchange_timestamp_ns,
                last_price,
                volume,
                exchange_timestamp
            FROM tick_data_corrected
            WHERE source_file = ?
              AND instrument_token = ?
              AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
            ORDER BY exchange_timestamp_ns
            LIMIT 100
        """
        
        db_rows = conn.execute(query, [source_file, wrong_token]).fetchdf()
        conn.close()
        
        if len(db_rows) == 0:
            return {}
        
        # Create mapping: wrong_token -> correct_token
        # Match db rows to parsed packets by timestamp
        token_matches = defaultdict(int)
        
        for _, db_row in db_rows.iterrows():
            db_ts_ns = db_row.get('exchange_timestamp_ns', 0)
            db_price = db_row.get('last_price', 0)
            db_volume = db_row.get('volume', 0)
            
            # Find matching packet (exact timestamp match preferred)
            best_match = None
            best_score = 0
            
            for packet in packets:
                packet_ts_ns = packet.get('exchange_timestamp', 0)
                packet_price = packet.get('last_price', 0)
                packet_volume = packet.get('volume', 0)
                
                # Exact timestamp match
                if packet_ts_ns == db_ts_ns and packet_ts_ns > 0:
                    score = 100
                    # Bonus for price match
                    if abs(packet_price - db_price) < 0.01:
                        score += 50
                    # Bonus for volume match
                    if packet_volume == db_volume and db_volume > 0:
                        score += 25
                    
                    if score > best_score:
                        best_score = score
                        best_match = packet.get('instrument_token')
            
            # If no exact match, try approximate timestamp (within 1 second)
            if best_match is None:
                for packet in packets:
                    packet_ts_ns = packet.get('exchange_timestamp', 0)
                    packet_price = packet.get('last_price', 0)
                    
                    if packet_ts_ns > 0 and db_ts_ns > 0:
                        ts_diff = abs(packet_ts_ns - db_ts_ns)
                        if ts_diff < 1_000_000_000:  # Within 1 second
                            score = 50
                            if abs(packet_price - db_price) < 0.01:
                                score += 50
                            
                            if score > best_score:
                                best_score = score
                                best_match = packet.get('instrument_token')
            
            if best_match and 50 <= best_match < 50_000_000:
                # Filter out suspicious tokens (powers of 2, round numbers) unless they're in token cache
                is_suspicious = (
                    (best_match & (best_match - 1) == 0 and best_match > 256) or  # Power of 2 > 256
                    (best_match % 1000 == 0 and best_match < 50000)  # Round thousands
                )
                
                # Check if token is in cache
                metadata = self.converter._get_token_metadata(None, best_match)
                is_valid_token = (
                    metadata.get('symbol') and 
                    not metadata.get('symbol', '').startswith('UNKNOWN')
                )
                
                # Only count if it's a valid token OR not suspicious
                if is_valid_token or not is_suspicious:
                    token_matches[best_match] += 1
                elif best_match == wrong_token:
                    # If wrong token matches itself but is suspicious, still try to find alternative
                    logger.debug(f"  Token {best_match} is suspicious (power of 2 or round number)")
        
        # Return the most common correct token
        if token_matches:
            # Prefer tokens that are NOT the wrong token (unless wrong token is valid)
            valid_matches = {
                k: v for k, v in token_matches.items()
                if k != wrong_token or (k in token_matches and self.token_cache.get_instrument_info(k) and 
                                       not self.token_cache.get_instrument_info(k).get('symbol', '').startswith('UNKNOWN'))
            }
            
            if valid_matches:
                correct_token = max(valid_matches.items(), key=lambda x: x[1])[0]
            else:
                # Fallback to all matches
                correct_token = max(token_matches.items(), key=lambda x: x[1])[0]
            
            match_count = token_matches[correct_token]
            logger.info(f"  Matched {match_count}/{len(db_rows)} rows: token {wrong_token} -> {correct_token}")
            return {wrong_token: correct_token}
        
        return {}
    
    def update_database_with_correct_tokens(
        self,
        source_file: str,
        token_mappings: Dict[int, int],
        correct_packets: List[Dict]
    ) -> int:
        """Update database rows with correct tokens and metadata.
        
        Uses DELETE + INSERT to handle primary key conflicts.
        """
        conn = duckdb.connect(self.db_path)
        
        updated_count = 0
        
        try:
            # For each token mapping
            for wrong_token, correct_token in token_mappings.items():
                # Find the correct metadata
                correct_metadata = None
                for packet in correct_packets:
                    if packet.get('instrument_token') == correct_token:
                        correct_metadata = {
                            'symbol': packet.get('symbol'),
                            'exchange': packet.get('exchange'),
                            'segment': packet.get('segment'),
                            'instrument_type': packet.get('instrument_type'),
                        }
                        break
                
                if not correct_metadata:
                    continue
                
                # Get rows to update (read them first)
                select_query = """
                    SELECT 
                        instrument_token,
                        exchange_timestamp_ns,
                        source_file,
                        last_price,
                        volume,
                        exchange_timestamp,
                        last_traded_timestamp,
                        last_traded_timestamp_ns,
                        average_traded_price,
                        total_buy_quantity,
                        total_sell_quantity,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        open_interest,
                        oi_day_high,
                        oi_day_low,
                        last_traded_quantity,
                        packet_type,
                        data_quality,
                        session_type,
                        processing_batch,
                        session_id,
                        processed_at,
                        parser_version
                    FROM tick_data_corrected
                    WHERE source_file = ?
                      AND instrument_token = ?
                      AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
                """
                
                rows_to_update = conn.execute(select_query, [source_file, wrong_token]).fetchdf()
                
                if len(rows_to_update) == 0:
                    continue
                
                # Delete old rows
                delete_query = """
                    DELETE FROM tick_data_corrected
                    WHERE source_file = ?
                      AND instrument_token = ?
                      AND (symbol LIKE 'UNKNOWN%' OR symbol IS NULL OR symbol = '')
                """
                conn.execute(delete_query, [source_file, wrong_token])
                
                # Insert updated rows with correct token
                for _, row in rows_to_update.iterrows():
                    # Check if row with new token+timestamp already exists (might have been inserted correctly)
                    check_query = """
                        SELECT COUNT(*) 
                        FROM tick_data_corrected
                        WHERE instrument_token = ?
                          AND exchange_timestamp_ns = ?
                          AND source_file = ?
                    """
                    exists = conn.execute(
                        check_query,
                        [correct_token, row['exchange_timestamp_ns'], source_file]
                    ).fetchone()[0]
                    
                    if exists > 0:
                        # Row already exists with correct token, skip to avoid duplicate
                        continue
                    
                    # Insert with correct token
                    insert_query = """
                        INSERT INTO tick_data_corrected (
                            instrument_token, symbol, exchange, segment, instrument_type,
                            exchange_timestamp_ns, source_file, last_price, volume,
                            exchange_timestamp, last_traded_timestamp, last_traded_timestamp_ns,
                            average_traded_price, total_buy_quantity, total_sell_quantity,
                            open_price, high_price, low_price, close_price,
                            open_interest, oi_day_high, oi_day_low, last_traded_quantity,
                            packet_type, data_quality, session_type, processing_batch,
                            session_id, processed_at, parser_version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    conn.execute(
                        insert_query,
                        [
                            correct_token,
                            correct_metadata['symbol'],
                            correct_metadata['exchange'],
                            correct_metadata['segment'],
                            correct_metadata['instrument_type'],
                            row['exchange_timestamp_ns'],
                            source_file,
                            row.get('last_price'),
                            row.get('volume'),
                            row.get('exchange_timestamp'),
                            row.get('last_traded_timestamp'),
                            row.get('last_traded_timestamp_ns'),
                            row.get('average_traded_price'),
                            row.get('total_buy_quantity'),
                            row.get('total_sell_quantity'),
                            row.get('open_price'),
                            row.get('high_price'),
                            row.get('low_price'),
                            row.get('close_price'),
                            row.get('open_interest'),
                            row.get('oi_day_high'),
                            row.get('oi_day_low'),
                            row.get('last_traded_quantity'),
                            row.get('packet_type'),
                            row.get('data_quality'),
                            row.get('session_type'),
                            row.get('processing_batch'),
                            row.get('session_id'),
                            row.get('processed_at'),
                            row.get('parser_version'),
                        ]
                    )
                    
                    updated_count += 1
            
            conn.commit()
            logger.info(f"  Updated {updated_count} rows for {source_file}")
            
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            try:
                conn.rollback()
            except:
                pass  # Ignore rollback errors
        finally:
            conn.close()
        
        return updated_count
    
    def fix_unknown_symbols_for_file(self, source_file: str, unknown_tokens: List[int]) -> Dict:
        """Fix UNKNOWN symbols for a single source file."""
        logger.info(f"\n{'='*70}")
        logger.info(f"Processing: {source_file}")
        logger.info(f"Unknown tokens: {unknown_tokens}")
        
        # Find original binary file
        binary_file = self.find_original_binary_file(source_file)
        if not binary_file:
            logger.warning(f"  ❌ Could not find original binary file for {source_file}")
            return {'success': False, 'error': 'Binary file not found'}
        
        logger.info(f"  ✅ Found binary file: {binary_file}")
        
        # Re-parse binary file
        packets = self.reparse_binary_file(binary_file)
        if not packets:
            logger.warning(f"  ❌ No valid packets parsed from {binary_file.name}")
            return {'success': False, 'error': 'No packets parsed'}
        
        # Create token mappings by matching database rows with parsed packets
        token_mappings = {}
        for wrong_token in unknown_tokens:
            mapping = self.create_token_mapping_from_database(source_file, wrong_token, packets)
            token_mappings.update(mapping)
        
        if not token_mappings:
            logger.warning(f"  ❌ Could not determine correct tokens")
            return {'success': False, 'error': 'No token mappings'}
        
        # Update database
        updated_count = self.update_database_with_correct_tokens(
            source_file,
            token_mappings,
            packets
        )
        
        return {
            'success': True,
            'updated_count': updated_count,
            'token_mappings': token_mappings,
        }
    
    def fix_all_unknown_symbols(self, limit: int = 10):
        """Fix UNKNOWN symbols for all affected files."""
        logger.info("="*70)
        logger.info("FIXING UNKNOWN SYMBOLS")
        logger.info("="*70)
        
        # Find unknown symbols
        unknown_data = self.find_unknown_symbols(limit=limit)
        
        # Group by source file
        files_with_unknowns = defaultdict(list)
        for record in unknown_data:
            files_with_unknowns[record['source_file']].append(record['current_token'])
        
        logger.info(f"\nFound {len(files_with_unknowns)} files with UNKNOWN symbols")
        
        # Process each file
        results = []
        for source_file, tokens in list(files_with_unknowns.items())[:limit]:
            unique_tokens = list(set(tokens))
            result = self.fix_unknown_symbols_for_file(source_file, unique_tokens)
            results.append(result)
        
        # Summary
        logger.info(f"\n{'='*70}")
        logger.info("SUMMARY")
        logger.info("="*70)
        successful = sum(1 for r in results if r.get('success'))
        total_updated = sum(r.get('updated_count', 0) for r in results)
        
        logger.info(f"Successfully processed: {successful}/{len(results)} files")
        logger.info(f"Total rows updated: {total_updated:,}")
        
        return results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix UNKNOWN symbols by re-parsing binary files')
    parser.add_argument('--db-path', default='analysis/tick_data_production.db',
                       help='Path to DuckDB database')
    parser.add_argument('--limit', type=int, default=10,
                       help='Maximum number of files to process')
    
    args = parser.parse_args()
    
    db_path = Path(args.db_path)
    if not db_path.exists():
        db_path = Path('tick_data_production.db')
    
    fixer = UnknownSymbolFixer(str(db_path))
    fixer.fix_all_unknown_symbols(limit=args.limit)


if __name__ == '__main__':
    main()

