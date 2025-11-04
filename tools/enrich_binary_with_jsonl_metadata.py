#!/usr/bin/env python3
"""
Enrich Binary Files with Metadata from JSONL.zst Files

Reads JSONL.zst files to extract metadata (symbol, exchange, segment, etc.)
and uses it to enrich binary file parsing results.
This solves the "missing metadata" issue that causes packets to be skipped.
"""

import sys
from pathlib import Path
import json
import zstandard as zstd
import logging
from typing import Dict, Optional, List
from collections import defaultdict
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class JSONLMetadataEnricher:
    """Extract and provide metadata from JSONL.zst files"""
    
    def __init__(self, jsonl_dir: Path):
        self.jsonl_dir = jsonl_dir
        self.metadata_by_token: Dict[int, Dict] = {}
        self.metadata_by_symbol: Dict[str, Dict] = {}
        self.timestamps_by_token: Dict[int, List[str]] = defaultdict(list)
        
    def load_jsonl_files(self, date_pattern: Optional[str] = None):
        """Load all JSONL.zst files and extract metadata"""
        jsonl_files = list(self.jsonl_dir.glob("*.jsonl.zst"))
        
        if date_pattern:
            jsonl_files = [f for f in jsonl_files if date_pattern in f.name]
        
        logger.info(f"Loading metadata from {len(jsonl_files)} JSONL.zst files...")
        
        dctx = zstd.ZstdDecompressor()
        
        for jsonl_file in jsonl_files:
            try:
                with open(jsonl_file, 'rb') as f:
                    compressed_data = f.read()
                    
                    # Try decompressing - handle partial/corrupted files
                    try:
                        # First try normal decompression
                        decompressed = dctx.decompress(compressed_data)
                    except Exception as decomp_error:
                        # Try streaming decompression for partial files
                        try:
                            import io
                            stream = io.BytesIO(compressed_data)
                            reader = dctx.stream_reader(stream)
                            decompressed = reader.read()
                        except Exception as stream_error:
                            # Last resort: try to decompress in chunks (partial file)
                            try:
                                max_decompress_size = 100 * 1024 * 1024  # 100MB max
                                chunk_size = min(len(compressed_data), max_decompress_size)
                                decompressed = dctx.decompress(compressed_data[:chunk_size])
                                logger.debug(f"Partially decompressed {jsonl_file.name} ({chunk_size} bytes)")
                            except:
                                logger.debug(f"Could not decompress {jsonl_file.name}: {decomp_error}")
                                continue
                    
                    lines = decompressed.decode('utf-8').split('\n')
                    
                    for line in lines:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            token = record.get('instrument_token')
                            
                            if token:
                                # Store metadata
                                if token not in self.metadata_by_token:
                                    symbol = record.get('symbol', '')
                                    self.metadata_by_token[token] = {
                                        'symbol': symbol,
                                        'asset_class': record.get('asset_class', ''),
                                        'instrument_token': token,
                                    }
                                    
                                    if symbol:
                                        self.metadata_by_symbol[symbol] = self.metadata_by_token[token]
                                
                                # Store timestamp for validation
                                ts = record.get('timestamp')
                                if ts:
                                    self.timestamps_by_token[token].append(ts)
                                    
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                logger.warning(f"Error reading {jsonl_file.name}: {e}")
        
        logger.info(f"Loaded metadata for {len(self.metadata_by_token):,} instruments")
        return len(self.metadata_by_token)
    
    def get_metadata(self, instrument_token: int) -> Optional[Dict]:
        """Get metadata for an instrument token"""
        return self.metadata_by_token.get(instrument_token)
    
    def enrich_packet(self, packet: Dict) -> Dict:
        """Enrich a binary packet with JSONL metadata"""
        token = packet.get('instrument_token')
        if not token:
            return packet
        
        metadata = self.get_metadata(token)
        if metadata:
            # Add metadata fields that are missing
            if 'symbol' not in packet or not packet.get('symbol') or packet.get('symbol', '').startswith('UNKNOWN'):
                packet['symbol'] = metadata.get('symbol', '')
            
            # Add asset_class/exchange info
            asset_class = metadata.get('asset_class', '')
            if asset_class:
                if 'equity' in asset_class.lower():
                    packet['instrument_type'] = packet.get('instrument_type') or 'EQ'
                    packet['segment'] = packet.get('segment') or 'NSE'
                elif 'fut' in asset_class.lower():
                    packet['instrument_type'] = packet.get('instrument_type') or 'FUT'
                    packet['segment'] = packet.get('segment') or 'NFO'
                elif 'option' in asset_class.lower():
                    packet['instrument_type'] = packet.get('instrument_type') or 'OPT'
                    packet['segment'] = packet.get('segment') or 'NFO'
        
        return packet


def enrich_binary_conversion_with_jsonl(
    binary_file: Path,
    jsonl_enricher: JSONLMetadataEnricher,
    token_cache
) -> Dict:
    """Convert binary file to parquet using JSONL metadata for enrichment"""
    from analysis.scripts.binary_to_parquet_parquet import convert_binary_to_parquet
    from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
    
    logger.info(f"Converting {binary_file.name} with JSONL metadata enrichment...")
    
    # Parse binary file
    converter = ProductionZerodhaBinaryConverter(
        db_path=None,
        token_cache=token_cache,
        drop_unknown_tokens=False,
        ensure_schema=False,
    )
    
    raw_data = binary_file.read_bytes()
    packets = converter._detect_packet_format(raw_data)
    
    logger.info(f"  Parsed {len(packets):,} packets")
    
    # Enrich packets with JSONL metadata
    enriched_packets = []
    for packet in packets:
        enriched = jsonl_enricher.enrich_packet(packet.copy())
        enriched_packets.append(enriched)
    
    # Count how many got metadata
    got_metadata = sum(1 for p in enriched_packets if p.get('symbol') and not p.get('symbol', '').startswith('UNKNOWN'))
    logger.info(f"  Enriched {got_metadata}/{len(enriched_packets)} packets with metadata")
    
    # Now process enriched packets through normal pipeline
    # We need to manually enrich and convert since convert_binary_to_parquet expects raw parsing
    
    # For now, return the enriched packets - we'll need to integrate this into the conversion pipeline
    return {
        'packets': enriched_packets,
        'enriched_count': got_metadata,
        'total_packets': len(packets)
    }


if __name__ == "__main__":
    import argparse
    from token_cache import TokenCacheManager
    
    parser = argparse.ArgumentParser(description="Enrich binary files with JSONL metadata")
    parser.add_argument("--jsonl-dir", default="temp_parquet/jsonl_source", help="Directory with JSONL.zst files")
    parser.add_argument("--binary-file", help="Binary file to test enrichment")
    parser.add_argument("--date", help="Date pattern to match JSONL files (e.g., 20250930)")
    
    args = parser.parse_args()
    
    jsonl_dir = Path(args.jsonl_dir)
    enricher = JSONLMetadataEnricher(jsonl_dir)
    enricher.load_jsonl_files(args.date)
    
    if args.binary_file:
        binary_file = Path(args.binary_file)
        token_cache = TokenCacheManager(cache_path="core/data/token_lookup_enriched.json", verbose=True)
        result = enrich_binary_conversion_with_jsonl(binary_file, enricher, token_cache)
        print(f"\n✅ Enrichment result: {result['enriched_count']}/{result['total_packets']} packets got metadata")
    else:
        print(f"\n✅ Loaded metadata for {len(enricher.metadata_by_token):,} instruments")
        print(f"   Ready to enrich binary files!")

