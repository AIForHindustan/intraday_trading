#!/usr/bin/env python3
"""
Fetch 180 days of historical candlestick data for BFO unknown tokens
to identify what instruments they belong to
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from config.zerodha_config import ZerodhaConfig

def fetch_token_historical_data(kite, token: int, days_back: int = 180) -> Optional[List[Dict]]:
    """Fetch historical data for a specific token"""
    try:
        # Calculate date range
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days_back)
        
        # Fetch historical data (using day interval for 180 days)
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
            return ohlc_data
        return None
            
    except Exception as e:
        # Check for specific error types - return None for invalid tokens
        error_str = str(e)
        if any(phrase in error_str.lower() for phrase in ["invalid token", "instrument not found", "invalid request", "bad request"]):
            return None
        # Re-raise for other errors (rate limits, network, etc.)
        raise

def get_instrument_info(kite, token: int) -> Optional[Dict]:
    """Get instrument information using token"""
    try:
        # Try to get instrument from all exchanges
        exchanges = ['BFO', 'NSE', 'NFO', 'BSE', 'MCX', 'CDS']
        
        for exchange in exchanges:
            instruments = kite.instruments(exchange)
            for inst in instruments:
                if inst.get('instrument_token') == token:
                    return {
                        'instrument_token': inst.get('instrument_token'),
                        'exchange_token': inst.get('exchange_token'),
                        'tradingsymbol': inst.get('tradingsymbol'),
                        'name': inst.get('name'),
                        'exchange': inst.get('exchange'),
                        'instrument_type': inst.get('instrument_type'),
                        'segment': inst.get('segment'),
                        'expiry': inst.get('expiry').strftime('%Y-%m-%d') if inst.get('expiry') else None,
                        'strike': inst.get('strike'),
                        'lot_size': inst.get('lot_size'),
                    }
        return None
    except Exception as e:
        return None

def main():
    """Main execution function"""
    print("🚀 FETCHING 180-DAY HISTORICAL DATA FOR BFO UNKNOWN TOKENS")
    print("=" * 70)
    print()
    
    # Load Zerodha tokens to fetch
    token_list_file = Path("zerodha_tokens_for_historical_fetch.json")
    if not token_list_file.exists():
        print("❌ zerodha_tokens_for_historical_fetch.json not found")
        print("   Run the token mapping script first")
        sys.exit(1)
    
    with open(token_list_file, 'r') as f:
        token_data = json.load(f)
    
    tokens = token_data['tokens']
    print(f"📋 Total Zerodha tokens to process: {len(tokens):,}")
    
    # Load mappings for reference
    bfo_mappings = token_data.get('bfo_mappings', {})
    unknown_mappings = token_data.get('unknown_mappings', {})
    
    print(f"   - {len(bfo_mappings)} from BFO unknown tokens")
    print(f"   - {len(unknown_mappings)} from general unknown tokens")
    
    # Initialize Zerodha API
    try:
        token_data_config = ZerodhaConfig.get_token_data()
        if not token_data_config:
            print("❌ Failed to load Zerodha API credentials")
            sys.exit(1)
        
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=token_data_config['api_key'])
        kite.set_access_token(token_data_config['access_token'])
        kite.profile()  # Test connection
        print("✅ Zerodha API connected")
    except Exception as e:
        print(f"❌ Failed to connect to Zerodha API: {e}")
        sys.exit(1)
    
    print(f"\n📊 Fetching historical data for {len(tokens)} tokens (180 days each)...")
    print("   ⚠️  This may take a while due to rate limits...\n")
    
    results = {}
    successful = 0
    failed = 0
    invalid_tokens = 0
    rate_limit_delay = 0.2  # 200ms between requests to avoid rate limits
    
    start_time = time.time()
    
    for i, token in enumerate(tokens, 1):
        if i % 50 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            remaining = (len(tokens) - i) / rate if rate > 0 else 0
            print(f"   Progress: {i}/{len(tokens)} ({i/len(tokens)*100:.1f}%) - ETA: {remaining/60:.1f} min")
        
        # Try to get instrument info first (faster than historical data)
        try:
            instrument_info = get_instrument_info(kite, token)
        except Exception as e:
            instrument_info = None
        
        # Fetch historical data (only if instrument info found or we want to try anyway)
        try:
            historical_data = fetch_token_historical_data(kite, token, days_back=180)
        except Exception as e:
            error_str = str(e).lower()
            if any(phrase in error_str for phrase in ["invalid token", "instrument not found", "bad request", "invalid request"]):
                historical_data = None
            else:
                # Other errors (rate limit, network) - skip for now
                print(f"   ⚠️  Token {token}: Error - {str(e)[:50]}")
                time.sleep(1)  # Longer delay on error
                continue
        
        if historical_data:
            results[token] = {
                'token': token,
                'instrument_info': instrument_info,
                'historical_data': historical_data,
                'data_points': len(historical_data),
                'date_range': {
                    'from': historical_data[0]['date'],
                    'to': historical_data[-1]['date']
                },
                'price_range': {
                    'min': min(d['low'] for d in historical_data),
                    'max': max(d['high'] for d in historical_data),
                    'latest_close': historical_data[-1]['close']
                },
                'fetched_at': datetime.now().isoformat()
            }
            successful += 1
            
            # Show success for first few
            if successful <= 10:
                symbol = instrument_info.get('tradingsymbol', 'Unknown') if instrument_info else 'Unknown'
                print(f"   ✅ Token {token}: {symbol} - {len(historical_data)} days")
        elif instrument_info:
            # Instrument exists but no historical data (may be expired or inactive)
            results[token] = {
                'token': token,
                'instrument_info': instrument_info,
                'historical_data': None,
                'status': 'instrument_found_no_data',
                'fetched_at': datetime.now().isoformat()
            }
            successful += 1
        else:
            invalid_tokens += 1
        
        # Rate limiting
        time.sleep(rate_limit_delay)
    
    elapsed_time = time.time() - start_time
    
    print(f"\n" + "=" * 70)
    print(f"📊 FETCH RESULTS SUMMARY")
    print(f"=" * 70)
    print(f"✅ Successful: {successful:,} ({successful/len(tokens)*100:.1f}%)")
    print(f"❌ Invalid tokens: {invalid_tokens:,} ({invalid_tokens/len(tokens)*100:.1f}%)")
    print(f"⏱️  Time elapsed: {elapsed_time/60:.1f} minutes")
    print(f"📈 Total tokens processed: {len(tokens):,}")
    
    if results:
        # Analyze results
        with_data = sum(1 for r in results.values() if r.get('historical_data'))
        with_info = sum(1 for r in results.values() if r.get('instrument_info'))
        
        print(f"\n📋 Breakdown:")
        print(f"  Tokens with historical data: {with_data:,}")
        print(f"  Tokens with instrument info: {with_info:,}")
        
        # Show samples
        print(f"\n📋 Sample identified instruments:")
        sample_count = 0
        for token, data in list(results.items())[:10]:
            if data.get('instrument_info'):
                info = data['instrument_info']
                symbol = info.get('tradingsymbol', 'N/A')
                exchange = info.get('exchange', 'N/A')
                inst_type = info.get('instrument_type', 'N/A')
                expiry = info.get('expiry', 'N/A')
                
                print(f"    Token {token}: {symbol} ({exchange}, {inst_type})")
                if expiry and expiry != 'N/A':
                    print(f"      Expiry: {expiry}")
                if data.get('historical_data'):
                    print(f"      Historical data: {data['data_points']} days")
                sample_count += 1
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"bfo_tokens_historical_{timestamp}.json"
        
        output_data = {
            'metadata': {
                'total_tokens': len(tokens),
                'successful_fetches': successful,
                'invalid_tokens': invalid_tokens,
                'tokens_with_data': with_data,
                'tokens_with_info': with_info,
                'fetch_date': datetime.now().isoformat(),
                'days_back': 180,
                'time_elapsed_seconds': elapsed_time
            },
            'results': results
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        # Create summary CSV
        import csv
        csv_file = f"bfo_tokens_identified_{timestamp}.csv"
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Token', 'TradingSymbol', 'Name', 'Exchange', 'InstrumentType', 
                           'Expiry', 'Strike', 'LotSize', 'DataPoints', 'PriceRange', 'Status'])
            
            for token, data in sorted(results.items(), key=lambda x: x[0]):
                info = data.get('instrument_info') or {}
                price_info = data.get('price_range') or {}
                price_str = f"{price_info.get('min', 'N/A')}-{price_info.get('max', 'N/A')}" if price_info else 'N/A'
                status = 'has_data' if data.get('historical_data') else data.get('status', 'no_data')
                
                writer.writerow([
                    token,
                    info.get('tradingsymbol', '') if info else '',
                    info.get('name', '') if info else '',
                    info.get('exchange', '') if info else '',
                    info.get('instrument_type', '') if info else '',
                    info.get('expiry', '') if info else '',
                    info.get('strike', '') if info else '',
                    info.get('lot_size', '') if info else '',
                    data.get('data_points', 0),
                    price_str,
                    status
                ])
        
        print(f"💾 Summary CSV saved to: {csv_file}")
    
    print("\n🎯 HISTORICAL DATA FETCH COMPLETED!")

if __name__ == "__main__":
    main()

