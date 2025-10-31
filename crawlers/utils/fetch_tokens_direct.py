#!/usr/bin/env python3
"""
Fetch Historical Data Directly by Token Numbers
===============================================

Fetches historical OHLC data from Zerodha API using token numbers directly,
without name resolution.

Usage:
    python fetch_tokens_direct.py
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from config.zerodha_config import ZerodhaConfig

# Binary tokens from our analysis - just the numbers
BINARY_TOKENS = [
    409, 0, 131072, 410, 1, 29900, 5711200, 254214144, 6880000, 2097156, 90241,
    2928017409, 4198000876, 42598402
]

def fetch_token_direct(kite, token: int, days_back: int = 30) -> Dict:
    """Fetch historical data for a token number directly"""
    try:
        print(f"   📊 Token {token}...", end=" ")
        
        # Calculate date range
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days_back)
        
        # Fetch historical data directly by token
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
            return {
                'token': token,
                'success': True,
                'data_points': len(ohlc_data),
                'ohlc_data': ohlc_data,
                'date_range': f"{ohlc_data[0]['date']} to {ohlc_data[-1]['date']}" if ohlc_data else "No data"
            }
        else:
            print("❌ No data")
            return {
                'token': token,
                'success': False,
                'error': 'No historical data returned'
            }
            
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error: {error_msg[:50]}")
        return {
            'token': token,
            'success': False,
            'error': error_msg
        }

def main():
    """Main execution function"""
    print("🚀 FETCHING HISTORICAL DATA BY TOKEN NUMBERS")
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
    print("📊 FETCHING HISTORICAL DATA BY TOKEN NUMBERS")
    print("-" * 50)
    
    results = {}
    successful_fetches = 0
    failed_fetches = 0
    
    for token in BINARY_TOKENS:
        print(f"\n🔍 Processing Token {token}")
        
        # Fetch historical data directly
        result = fetch_token_direct(kite, token, days_back=30)
        results[token] = result
        
        if result['success']:
            successful_fetches += 1
        else:
            failed_fetches += 1
    
    print("\n" + "=" * 70)
    print("📊 FETCH RESULTS SUMMARY")
    print("=" * 70)
    print(f"✅ Successful fetches: {successful_fetches}")
    print(f"❌ Failed fetches: {failed_fetches}")
    print(f"📈 Total tokens processed: {len(BINARY_TOKENS)}")
    
    if successful_fetches > 0:
        print(f"\n📋 SUCCESSFUL FETCHES:")
        for token, result in results.items():
            if result['success']:
                print(f"   • Token {token}: {result['data_points']} days ({result['date_range']})")
        
        # Show sample data for first successful token
        for token, result in results.items():
            if result['success'] and result['ohlc_data']:
                print(f"\n📄 SAMPLE DATA (Token {token}):")
                sample_day = result['ohlc_data'][-1]  # Latest day
                print(f"   Latest day: {sample_day['date']}")
                print(f"   OHLC: O={sample_day['open']}, H={sample_day['high']}, L={sample_day['low']}, C={sample_day['close']}")
                print(f"   Volume: {sample_day['volume']}")
                break
    
    if failed_fetches > 0:
        print(f"\n❌ FAILED FETCHES:")
        for token, result in results.items():
            if not result['success']:
                print(f"   • Token {token}: {result['error']}")
    
    # Save results
    output_file = f"tokens_direct_fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    output_data = {
        'metadata': {
            'total_tokens': len(BINARY_TOKENS),
            'successful_fetches': successful_fetches,
            'failed_fetches': failed_fetches,
            'fetch_date': datetime.now().isoformat(),
            'days_back': 30
        },
        'results': results
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    print("\n🎯 DIRECT TOKEN FETCH COMPLETED!")

if __name__ == "__main__":
    main()






