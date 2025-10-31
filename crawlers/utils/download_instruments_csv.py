#!/usr/bin/env python3
"""
Download entire instruments list from Zerodha in CSV format with token mapping
Uses existing Zerodha configuration to fetch all instruments and export to CSV
"""

import csv
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add config directory to path
sys.path.append(str(Path(__file__).parent / "config"))

from zerodha_config import ZerodhaConfig

def download_instruments_csv():
    """Download all instruments from Zerodha and save as CSV with token mapping"""
    print("🚀 DOWNLOADING ZERODHA INSTRUMENTS LIST")
    print("=" * 60)
    
    try:
        # Test Zerodha connection
        print("🔗 Testing Zerodha API connection...")
        kite = ZerodhaConfig.get_kite_instance()
        print("✅ Connected to Zerodha API successfully")
        
        # Fetch all instruments
        print("📥 Fetching all instruments from Zerodha...")
        all_instruments = kite.instruments()
        print(f"✅ Fetched {len(all_instruments)} instruments")
        
        # Prepare CSV data
        csv_data = []
        exchange_counts = {}
        instrument_type_counts = {}
        
        for inst in all_instruments:
            # Extract all relevant fields
            instrument_data = {
                'instrument_token': inst.get('instrument_token', ''),
                'exchange_token': inst.get('exchange_token', ''),
                'tradingsymbol': inst.get('tradingsymbol', ''),
                'name': inst.get('name', ''),
                'last_price': inst.get('last_price', ''),
                'expiry': inst.get('expiry', ''),
                'strike': inst.get('strike', ''),
                'tick_size': inst.get('tick_size', ''),
                'lot_size': inst.get('lot_size', ''),
                'instrument_type': inst.get('instrument_type', ''),
                'segment': inst.get('segment', ''),
                'exchange': inst.get('exchange', ''),
                'created_at': datetime.now().isoformat()
            }
            
            csv_data.append(instrument_data)
            
            # Count by exchange
            exchange = inst.get('exchange', 'Unknown')
            exchange_counts[exchange] = exchange_counts.get(exchange, 0) + 1
            
            # Count by instrument type
            inst_type = inst.get('instrument_type', 'Unknown')
            instrument_type_counts[inst_type] = instrument_type_counts.get(inst_type, 0) + 1
        
        # Generate CSV filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"zerodha_instruments_{timestamp}.csv"
        
        # Create zerodha_token_list directory if it doesn't exist
        token_list_dir = Path("zerodha_token_list")
        token_list_dir.mkdir(exist_ok=True)
        
        csv_path = token_list_dir / csv_filename
        
        # Write CSV file
        print(f"📝 Writing instruments to CSV: {csv_path}")
        
        if csv_data:
            fieldnames = csv_data[0].keys()
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(csv_data)
        
        # Generate summary report
        summary_filename = f"instruments_summary_{timestamp}.json"
        summary_path = Path(summary_filename)
        
        summary_data = {
            'download_timestamp': datetime.now().isoformat(),
            'total_instruments': len(csv_data),
            'exchanges': exchange_counts,
            'instrument_types': instrument_type_counts,
            'csv_file': str(csv_path),
            'top_exchanges': sorted(exchange_counts.items(), key=lambda x: x[1], reverse=True)[:10],
            'top_instrument_types': sorted(instrument_type_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        }
        
        with open(summary_path, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        # Print summary
        print(f"\n🎉 DOWNLOAD COMPLETED!")
        print(f"   📄 CSV File: {csv_filename}")
        print(f"   📊 Summary: {summary_filename}")
        print(f"   📈 Total Instruments: {len(csv_data):,}")
        
        print(f"\n📊 EXCHANGE BREAKDOWN:")
        for exchange, count in sorted(exchange_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {exchange}: {count:,} instruments")
        
        print(f"\n📊 INSTRUMENT TYPE BREAKDOWN:")
        for inst_type, count in sorted(instrument_type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {inst_type}: {count:,} instruments")
        
        # Show sample data
        print(f"\n📋 SAMPLE INSTRUMENTS (first 5):")
        for i, inst in enumerate(csv_data[:5]):
            print(f"   {i+1}. {inst['tradingsymbol']} ({inst['exchange']}) - Token: {inst['instrument_token']} - Type: {inst['instrument_type']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error downloading instruments: {e}")
        import traceback
        traceback.print_exc()
        return False

def filter_instruments_by_criteria(csv_file: str, output_file: str = None, 
                                 exchanges: List[str] = None, 
                                 instrument_types: List[str] = None,
                                 min_lot_size: int = None) -> str:
    """
    Filter instruments from CSV based on criteria
    
    Args:
        csv_file: Path to input CSV file
        output_file: Path to output filtered CSV file
        exchanges: List of exchanges to include (e.g., ['NSE', 'BSE'])
        instrument_types: List of instrument types to include (e.g., ['EQ', 'FUT', 'CE', 'PE'])
        min_lot_size: Minimum lot size filter
    
    Returns:
        Path to filtered CSV file
    """
    if not output_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"filtered_instruments_{timestamp}.csv"
    
    filtered_data = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Apply filters
            if exchanges and row.get('exchange') not in exchanges:
                continue
            if instrument_types and row.get('instrument_type') not in instrument_types:
                continue
            if min_lot_size and int(row.get('lot_size', 0)) < min_lot_size:
                continue
            
            filtered_data.append(row)
    
    # Write filtered data
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        if filtered_data:
            fieldnames = filtered_data[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered_data)
    
    print(f"✅ Filtered {len(filtered_data)} instruments to {output_file}")
    return output_file

if __name__ == "__main__":
    print("Zerodha Instruments CSV Downloader")
    print("=" * 40)
    
    # Download all instruments
    success = download_instruments_csv()
    
    if success:
        print("\n🔧 FILTERING OPTIONS:")
        print("You can filter the downloaded CSV using the filter_instruments_by_criteria function:")
        print("Example usage:")
        print("  # Filter for NSE equities only")
        print("  filter_instruments_by_criteria('zerodha_instruments_YYYYMMDD_HHMMSS.csv',")
        print("                                 exchanges=['NSE'], instrument_types=['EQ'])")
        print("")
        print("  # Filter for F&O instruments")
        print("  filter_instruments_by_criteria('zerodha_instruments_YYYYMMDD_HHMMSS.csv',")
        print("                                 instrument_types=['FUT', 'CE', 'PE'])")
    else:
        print("❌ Download failed. Please check your Zerodha credentials and try again.")
        sys.exit(1)
