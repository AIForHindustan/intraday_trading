#!/usr/bin/env python3
"""
Fetch Historical Data for Binary Tokens
=======================================

Fetches historical OHLC data from Zerodha API for the token numbers 
found in our binary data analysis.

Usage:
    python fetch_binary_tokens_historical.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from config.zerodha_config import ZerodhaConfig

# Binary tokens from our analysis
BINARY_TOKENS = {
    409: "STURAKL (MCX)",
    0: "GOLDSTAR-SM (NSE)", 
    131072: "KPITTECH25NOV1180PE (NFO)",
    410: "SUGARSKLP (MCX)",
    1: "GOLDSTAR-SM (NSE)",
    29900: "OMFURN-SM (NSE)",
    5711200: "MINDSPACE-RR (NSE)",
    254214144: "DuckDB Token 254214144",
    6880000: "DuckDB Token 6880000",
    2097156: "DuckDB Token 2097156 (Most Active)",
    90241: "DuckDB Token 90241",
    # Unknown tokens
    2928017409: "UNKNOWN_TOKEN_2928017409",
    4198000876: "UNKNOWN_TOKEN_4198000876",  
    42598402: "UNKNOWN_TOKEN_42598402"
}

def fetch_token_historical_data(kite, token: int, symbol_name: str, days_back: int = 30) -> List[Dict]:
    """Fetch historical data for a specific token"""
    try:
        print(f"   📊 Fetching {symbol_name} (token {token})...", end=" ")
        
        # Calculate date range
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days_back)
        
        # Fetch historical data
        historical_data = kite.historical_data(
            instrument_token=token,
            from_date=from_date.date(),
            to_date=to_date.date(),
            interval="day"
        )
        
        if historical_data:
            # Convert to our format
            ohlc_data = []
            for day in historical_data:
                ohlc_data.append({
                    'date': day['date'].strftime('%Y-%m-%d'),
                    'open': day['open'],
                    'high': day['high'],
                    'low': day['low'],
                    'close': day['close'],
                    'volume': day['volume']
                })
            
            print(f"✅ {len(ohlc_data)} days")
            return ohlc_data
        else:
            print("❌ No data")
            return []
            
    except Exception as e:
        print(f"❌ Error: {str(e)[:50]}")
        return []

def main():
    """Main execution function"""
    print("🚀 FETCHING HISTORICAL DATA FOR BINARY TOKENS")
    print("=" * 70)
    print()
    
    # Initialize Zerodha API
    try:
        token_data = ZerodhaConfig.get_token_data()
        if not token_data:
            print("❌ Failed to load Zerodha API credentials")
            sys.exit(1)
        
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=token_data['api_key'])
        kite.set_access_token(token_data['access_token'])
        kite.profile()  # Test connection
        print("✅ Zerodha API connected")
    except Exception as e:
        print(f"❌ Failed to connect to Zerodha API: {e}")
        sys.exit(1)
    print()
    
    # Fetch data for each token
    print("📊 FETCHING HISTORICAL DATA FOR BINARY TOKENS")
    print("-" * 50)
    
    results = {}
    successful_fetches = 0
    failed_fetches = 0
    
    for token, symbol_name in BINARY_TOKENS.items():
        print(f"\n🔍 Token {token}: {symbol_name}")
        
        # Skip unknown tokens for now
        if "UNKNOWN" in symbol_name:
            print(f"   ⏭️  Skipping unknown token")
            failed_fetches += 1
            continue
        
        # Fetch historical data
        ohlc_data = fetch_token_historical_data(kite, token, symbol_name, days_back=30)
        
        if ohlc_data:
            results[token] = {
                'symbol_name': symbol_name,
                'token': token,
                'ohlc_data': ohlc_data,
                'data_points': len(ohlc_data),
                'date_range': f"{ohlc_data[0]['date']} to {ohlc_data[-1]['date']}" if ohlc_data else "No data",
                'fetched_at': datetime.now().isoformat()
            }
            successful_fetches += 1
        else:
            failed_fetches += 1
    
    print("\n" + "=" * 70)
    print("📊 FETCH RESULTS SUMMARY")
    print("=" * 70)
    print(f"✅ Successful fetches: {successful_fetches}")
    print(f"❌ Failed fetches: {failed_fetches}")
    print(f"📈 Total tokens processed: {len(BINARY_TOKENS)}")
    
    if results:
        print(f"\n📋 SUCCESSFUL FETCHES:")
        for token, data in results.items():
            print(f"   • Token {token}: {data['symbol_name']}")
            print(f"     - Data points: {data['data_points']}")
            print(f"     - Date range: {data['date_range']}")
        
        # Save results
        output_file = f"binary_tokens_historical_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        output_data = {
            'metadata': {
                'total_tokens': len(BINARY_TOKENS),
                'successful_fetches': successful_fetches,
                'failed_fetches': failed_fetches,
                'fetch_date': datetime.now().isoformat(),
                'days_back': 30
            },
            'token_data': results
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        # Show sample data for first successful token
        if results:
            first_token = list(results.keys())[0]
            first_data = results[first_token]
            print(f"\n📄 SAMPLE DATA (Token {first_token}):")
            print(f"   Symbol: {first_data['symbol_name']}")
            print(f"   Data points: {first_data['data_points']}")
            if first_data['ohlc_data']:
                sample_day = first_data['ohlc_data'][-1]  # Latest day
                print(f"   Latest day: {sample_day['date']}")
                print(f"   OHLC: O={sample_day['open']}, H={sample_day['high']}, L={sample_day['low']}, C={sample_day['close']}")
                print(f"   Volume: {sample_day['volume']}")
    
    print("\n🎯 HISTORICAL DATA FETCH COMPLETED!")

if __name__ == "__main__":
    main()






