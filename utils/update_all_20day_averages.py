#!/usr/bin/env python3
"""
Update 20-day averages for Trading Crawler instruments
Optimized for intraday core trading with token resolver integration

USAGE:
    - Used by: Intraday Crawler (262 instruments only)
    - Called from: Production scheduler, manual execution
    - Saves to: config/volume_averages_20d.json, Redis cache
    - Dependencies: crawlers/binary_crawler1/binary_crawler1.json, config/zerodha_config.py, token_resolver

PURPOSE:
    - Updates 20-day average volumes and prices for intraday crawler's 262 instruments ONLY
    - Uses token resolver for instrument token and name resolution
    - Covers NIFTY 50 + NIFTY 50 Futures + NIFTY Index Futures + BANKNIFTY Futures
    - Uses centralized master token mapping (single source of truth)
    - Maintains data consistency for intraday trading patterns
    - Auto-updates with F&O availability changes
    - UPDATED: Now includes November/December expiry instruments (25NOV/25DEC) instead of October (28OCT)

REDIS CONFIGURATION:
    - Redis 8.0 with centralized configuration (config/redis_config.py)
    - TTL: 57,600 seconds (16 hours) for all databases
    - Database segmentation (consolidated structure): 
      * DB 1 (realtime): OHLC data storage (ohlc_latest:* keys)
      * DB 2 (analytics): Volume averages and historical data (volume_averages:* keys)
    - Automatic expiration: Volume averages (24h), OHLC data (36h)
    - Client tracking: Enabled for volume data (DB 2)
    - Health monitoring: 30-second health check intervals

SCHEDULING:
    - DAILY EXECUTION REQUIRED: Run every trading day before market open
    - Recommended time: 8:00 AM IST (pre-market start)
    - Redis data expires after 16 hours, so daily refresh is essential
    - Manual execution: python utils/update_all_20day_averages.py
    - Production scheduler: Integrated with crawler launch system
    - Market calendar aware: Skips weekends
    - OHLC data: Fetches current market data for all 262 instruments
    - Symbol format: Uses NSE: prefix for OHLC API compatibility

REDIS DATA STORAGE:
    - ohlc_latest:* - Current OHLC data (36-hour TTL) in DB 1 (realtime)
    - ohlc_stats:* - 20-day and 55-day averages (24-hour TTL) in DB 2 (analytics)
    - market_data:averages:* - Comprehensive averages data (24-hour TTL) in DB 2 (analytics)
    - Database 2 (analytics): Volume statistics and historical data
    - Database 1 (realtime): Pattern detection cache and analysis data

CREATED: September 18, 2025
UPDATED: October 23, 2025 - Enhanced Redis integration with daily scheduling requirements
UPDATED: October 29, 2025 - Updated for November/December expiry instruments (25NOV/25DEC) instead of October (28OCT)
"""

import sys
from pathlib import Path
from typing import Optional
# Add parent directory to path to import from config
sys.path.append(str(Path(__file__).parent.parent))

from config.zerodha_config import ZerodhaConfig
from datetime import datetime, timedelta
import json
import time
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("⚠️ Redis not available, will skip Redis caching")

from redis_files.redis_ohlc_keys import normalize_symbol, ohlc_latest_hash, ohlc_stats_hash, ohlc_daily_zset, ohlc_timeseries_key

def get_intraday_crawler_instruments():
    """
    Get instruments from binary_crawler1.json configuration (intraday crawler's 262 instruments)
    Uses token resolver for instrument token and name resolution
    UPDATED: Now includes November/December expiry instruments (25NOV/25DEC) instead of October (28OCT)
    
    RETURNS:
        list: List of instrument dictionaries with symbol, token, exchange, etc.
    """
    try:
        # Load from binary_crawler1.json (intraday crawler configuration)
        project_root = Path(__file__).parent.parent
        crawler_config_file = project_root / 'crawlers' / 'binary_crawler1' / 'binary_crawler1.json'
        
        if not crawler_config_file.exists():
            print(f"❌ Intraday crawler config not found: {crawler_config_file}")
            return []
        
        with open(crawler_config_file, 'r') as f:
            config_data = json.load(f)
        
        # Get token list (262 instruments for intraday crawler)
        tokens = config_data.get('tokens', [])
        if not tokens:
            print("❌ No tokens found in intraday crawler config")
            return []
        
        # Convert string tokens to integers for processing
        tokens = [int(token) if isinstance(token, str) else token for token in tokens]
        
        print(f"⚡ Processing {len(tokens)} intraday crawler instruments...")
        print("🔍 Updated configuration: NIFTY 50 stocks + November/December futures/options (25NOV/25DEC)")
        
        # Load token lookup once for all instruments using file descriptor
        token_lookup_file = project_root / "core" / "data" / "token_lookup_enriched.json"
        token_lookup = {}
        if token_lookup_file.exists():
            with open(token_lookup_file, 'r') as fd:
                token_lookup = json.load(fd)
            print(f"✅ Loaded token lookup once: {len(token_lookup)} instruments")
        
        def resolve_token_to_symbol_direct(token: int) -> Optional[str]:
            """Resolve token to symbol directly from enriched lookup"""
            try:
                token_str = str(token)
                if token_str in token_lookup:
                    data = token_lookup[token_str]
                    key_field = data.get('key', '')
                    if key_field and ':' in key_field:
                        return key_field  # Already in EXCHANGE:SYMBOL format
                    exchange = data.get('exchange', 'NSE')
                    symbol = data.get('name', '')
                    if symbol:
                        return f"{exchange}:{symbol}"
            except Exception:
                pass
            return None
        
        # Process each token using token resolver
        processed_instruments = []
        resolved_count = 0
        unresolved_count = 0
        
        for token in tokens:
            try:
                # Use direct token resolution from enriched lookup
                resolved_symbol = resolve_token_to_symbol_direct(token)
                
                if resolved_symbol:
                    # Parse resolved symbol (format: "NSE:RELIANCE" or "NFO:ABB25DECFUT")
                    if ':' in resolved_symbol:
                        exchange, tradingsymbol = resolved_symbol.split(':', 1)
                    else:
                        exchange = 'NSE'
                        tradingsymbol = resolved_symbol
                    
                    # Determine asset class based on exchange and symbol pattern
                    if exchange == 'NFO':
                        if 'FUT' in tradingsymbol or any(idx in tradingsymbol for idx in ['NIFTY', 'BANKNIFTY']):
                            if any(idx in tradingsymbol for idx in ['NIFTY', 'BANKNIFTY']):
                                asset_class = 'indices'
                                instrument_type = 'FUT'
                            else:
                                asset_class = 'equity_futures'
                                instrument_type = 'FUT'
                        else:
                            asset_class = 'derivatives'
                            instrument_type = 'OPT'
                    else:
                        asset_class = 'equity_cash'
                        instrument_type = 'EQ'
                    
                    processed_instruments.append({
                        'symbol': tradingsymbol,
                        'token': token,
                        'exchange': exchange,
                        'instrument_type': instrument_type,
                        'asset_class': asset_class,
                        'name': tradingsymbol,  # Use tradingsymbol as name
                        'current_price': 0,  # Will be fetched from API
                        'resolved_symbol': resolved_symbol  # Keep full resolved symbol
                    })
                    resolved_count += 1
                else:
                    # Use pre-loaded token_lookup as fallback
                    token_str = str(token)
                    fallback_symbol = f'TOKEN_{token}'
                    fallback_exchange = 'NSE'
                    fallback_type = 'EQ'
                    
                    if token_str in token_lookup:
                        data = token_lookup[token_str]
                        fallback_symbol = data.get('name', f'TOKEN_{token}')
                        fallback_exchange = data.get('exchange', 'NSE')
                        fallback_type = data.get('instrument_type', 'EQ')
                    
                    processed_instruments.append({
                        'symbol': fallback_symbol,
                        'token': token,
                        'exchange': fallback_exchange,
                        'instrument_type': fallback_type,
                        'asset_class': 'equity_cash',
                        'name': fallback_symbol,
                        'current_price': 0,
                        'resolved_symbol': f'{fallback_exchange}:{fallback_symbol}'
                    })
                    unresolved_count += 1
                    
            except Exception as e:
                print(f"⚠️ Error processing token {token}: {e}")
                unresolved_count += 1
                processed_instruments.append({
                    'symbol': f'TOKEN_{token}',
                    'token': token,
                    'exchange': 'NSE',
                    'instrument_type': 'EQ',
                    'asset_class': 'equity_cash',
                    'name': f'Token {token}',
                    'current_price': 0,
                    'resolved_symbol': f'NSE:TOKEN_{token}'
                })
        
        # Count by asset class
        cash_count = len([i for i in processed_instruments if i['asset_class'] == 'equity_cash'])
        futures_count = len([i for i in processed_instruments if i['asset_class'] in ['equity_futures', 'indices']])
        derivatives_count = len([i for i in processed_instruments if i['asset_class'] == 'derivatives'])
        
        print(f"✅ Loaded {len(processed_instruments)} trading crawler instruments")
        print(f"   • Cash Stocks: {cash_count}")
        print(f"   • Futures: {futures_count}")
        print(f"   • Derivatives: {derivatives_count}")
        print(f"   • Total: {len(processed_instruments)}")
        print(f"   • Resolved: {resolved_count}")
        print(f"   • Unresolved: {unresolved_count}")
        
        return processed_instruments
        
    except Exception as e:
        print(f"❌ Failed to load trading crawler instruments: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_bulk_ohlc_data(instruments: list, kite, tokens: list) -> dict:
    """
    Get current OHLC data for multiple instruments using appropriate APIs:
    - OHLC API for cash stocks (bulk, fast)
    - Historical data API for futures (individual calls)
    
    ARGS:
        instruments: List of instrument dictionaries with resolved_symbol
        kite: KiteConnect instance
        
    RETURNS:
        dict: OHLC data by symbol {'NSE:RELIANCE': {'open': float, 'high': float, 'low': float, 'close': float, 'last_price': float}, ...}
    """
    try:
        # Separate cash stocks from futures
        cash_symbols = []
        futures_instruments = []
        
        # Load token lookup file to get trading symbols using file descriptor
        project_root = Path(__file__).parent.parent
        token_lookup_file = project_root / "core" / "data" / "token_lookup_enriched.json"
        token_lookup = {}
        if token_lookup_file.exists():
            with open(token_lookup_file, 'r') as fd:
                token_lookup = json.load(fd)
        
        # Resolve tokens directly from enriched lookup
        def resolve_token_direct(token: int) -> Optional[str]:
            """Resolve token to symbol directly from enriched lookup"""
            token_str = str(token)
            if token_str in token_lookup:
                data = token_lookup[token_str]
                key_field = data.get('key', '')
                if key_field and ':' in key_field:
                    return key_field
                exchange = data.get('exchange', 'NSE')
                symbol = data.get('name', '')
                if symbol:
                    return f"{exchange}:{symbol}"
            return None
        
        # Use tokens directly from binary_crawler1.json to create OHLC symbols
        for token in tokens:
            # Find the correct trading symbol from token lookup for OHLC API
            ohlc_symbol = None
            # In token_lookup.json, the key IS the token (as string), not a field
            token_str = str(token)
            if token_str in token_lookup:
                data = token_lookup[token_str]
                if (data.get('exchange') == 'NSE' and 
                    data.get('instrument_type') == 'EQ'):
                    # Use the 'key' field which contains the proper symbol format
                    ohlc_symbol = data.get('key', '')
                    if ohlc_symbol and ':' in ohlc_symbol:
                        ohlc_symbol = ohlc_symbol  # Already in correct format like "NSE:TATAMOTORS"
                    else:
                        ohlc_symbol = f"NSE:{data.get('name', '')}"
                    
                    # Debug for TATA MOTORS
                    if token == 884737:
                        print(f"🔍 DEBUG TATA MOTORS: token={token}, key={data.get('key')}, name={data.get('name')}, ohlc_symbol={ohlc_symbol}")
            
            if ohlc_symbol:
                cash_symbols.append(ohlc_symbol)
            else:
                # Check if it's a futures/options instrument
                if token_str in token_lookup:
                    data = token_lookup[token_str]
                    if data.get('exchange') == 'NFO':
                        # Use the 'key' field which contains the proper symbol format with expiry
                        resolved_symbol = data.get('key', f"NFO:{data.get('name', '')}")
                        if resolved_symbol:  # Only add if resolved_symbol is not None
                            futures_instruments.append({
                                'token': token,
                                'resolved_symbol': resolved_symbol
                            })
                            
                            # Debug for F&O instruments
                            if token in [13356546, 9809154]:  # ADANIENT futures
                                print(f"🔍 DEBUG F&O: token={token}, key={data.get('key')}, name={data.get('name')}, resolved_symbol={resolved_symbol}")
        
        processed_ohlc = {}
        
        # 1. Get OHLC data for cash stocks (bulk API)
        if cash_symbols:
            print(f"📊 Fetching OHLC data for {len(cash_symbols)} cash stocks...")
            print(f"🔍 DEBUG: First 5 cash symbols: {cash_symbols[:5]}")
            # Retry logic for bulk OHLC API calls
            ohlc_data = None
            max_retries = 3
            retry_delay = 2.0
            
            for attempt in range(max_retries):
                try:
                    ohlc_data = kite.ohlc(cash_symbols)
                    break  # Success, exit retry loop
                except Exception as api_error:
                    error_msg = str(api_error)
                    if "503 Service Unavailable" in error_msg and attempt < max_retries - 1:
                        print(f"⚠️ Bulk OHLC API Error (attempt {attempt + 1}/{max_retries}): {error_msg}")
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                        continue
                    else:
                        print(f"❌ Bulk OHLC API Error: {error_msg}")
                        break
            
            print(f"🔍 DEBUG: OHLC API returned {len(ohlc_data) if ohlc_data else 0} results")
            if ohlc_data:
                print(f"🔍 DEBUG: OHLC keys: {list(ohlc_data.keys())[:5]}")
                for symbol, data in ohlc_data.items():
                    if 'ohlc' in data:
                        # Use symbol directly since it's already in the correct format
                        processed_ohlc[symbol] = {
                            'open': float(data['ohlc'].get('open', 0)),
                            'high': float(data['ohlc'].get('high', 0)),
                            'low': float(data['ohlc'].get('low', 0)),
                            'close': float(data['ohlc'].get('close', 0)),
                            'last_price': float(data.get('last_price', 0)),
                            'instrument_token': data.get('instrument_token'),  # PRESERVE THE TOKEN!
                            'timestamp': datetime.now().isoformat(),
                            'source': 'ohlc_api'
                        }
                print(f"✅ Retrieved OHLC data for {len(processed_ohlc)} cash stocks")
        
        # 2. Get OHLC data for futures/options using OHLC API (bulk)
        if futures_instruments:
            print(f"📊 Fetching OHLC data for {len(futures_instruments)} futures/options...")
            futures_symbols = [inst['resolved_symbol'] for inst in futures_instruments]
            
            # Debug: Show first 10 symbols being requested
            print(f"🔍 DEBUG: First 10 futures symbols: {futures_symbols[:10]}")
            
            # Retry logic for futures OHLC API calls
            futures_ohlc_data = None
            max_retries = 3
            retry_delay = 2.0
            
            for attempt in range(max_retries):
                try:
                    futures_ohlc_data = kite.ohlc(futures_symbols)
                    break  # Success, exit retry loop
                except Exception as api_error:
                    error_msg = str(api_error)
                    if "503 Service Unavailable" in error_msg and attempt < max_retries - 1:
                        print(f"⚠️ Futures OHLC API Error (attempt {attempt + 1}/{max_retries}): {error_msg}")
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                        continue
                    else:
                        print(f"❌ Futures OHLC API Error: {error_msg}")
                        break
            
            print(f"🔍 DEBUG: Futures OHLC API returned {len(futures_ohlc_data) if futures_ohlc_data else 0} results")
            
            if futures_ohlc_data:
                for symbol, data in futures_ohlc_data.items():
                    if 'ohlc' in data:
                        processed_ohlc[symbol] = {
                            'open': float(data['ohlc'].get('open', 0)),
                            'high': float(data['ohlc'].get('high', 0)),
                            'low': float(data['ohlc'].get('low', 0)),
                            'close': float(data['ohlc'].get('close', 0)),
                            'last_price': float(data.get('last_price', 0)),
                            'instrument_token': data.get('instrument_token'),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'ohlc_api'
                        }
                print(f"✅ Retrieved OHLC data for {len(futures_ohlc_data)} futures/options")
                
                # Debug: Show which symbols got data
                print(f"🔍 DEBUG: Symbols with OHLC data: {list(futures_ohlc_data.keys())[:10]}")
            else:
                print("⚠️ No futures OHLC data returned from API")
                # Fallback to individual historical data calls
                futures_ohlc_count = 0
                for instrument in futures_instruments:
                    try:
                        token = instrument['token']
                        resolved_symbol = instrument['resolved_symbol']
                        
                        # Get last 2 days of historical data
                        from_date = (datetime.now() - timedelta(days=3)).date()
                        to_date = datetime.now().date()
                        
                        try:
                            historical_data = kite.historical_data(
                                instrument_token=token,
                                from_date=from_date,
                                to_date=to_date,
                                interval='day'
                            )
                        except Exception as api_error:
                            print(f"❌ Historical API Error for {resolved_symbol}: {api_error}")
                            continue
                        
                        if historical_data and len(historical_data) > 0:
                            # Get the latest day's data
                            latest_day = historical_data[-1]
                            processed_ohlc[resolved_symbol] = {
                                'open': float(latest_day.get('open', 0)),
                                'high': float(latest_day.get('high', 0)),
                                'low': float(latest_day.get('low', 0)),
                                'close': float(latest_day.get('close', 0)),
                                'last_price': float(latest_day.get('close', 0)),
                                'volume': int(latest_day.get('volume', 0)),
                                'timestamp': datetime.now().isoformat(),
                                'source': 'historical_api'
                            }
                            futures_ohlc_count += 1
                        
                        # Rate limiting for futures
                        time.sleep(0.1)  # 10 calls per second
                        
                    except Exception as e:
                        print(f"⚠️ Error fetching futures OHLC for {resolved_symbol}: {e}")
                        continue
                
                print(f"✅ Retrieved OHLC data for {futures_ohlc_count} futures (fallback method)")
        
        # 3. FALLBACK: Get OHLC data for any missing instruments using individual calls
        missing_instruments = []
        for instrument in instruments:
            token = instrument.get('token')
            resolved_symbol = instrument.get('resolved_symbol', '')
            
            # Check if we already have OHLC data for this instrument
            has_ohlc = False
            for ohlc_key in processed_ohlc.keys():
                if (resolved_symbol == ohlc_key or 
                    resolved_symbol.replace(' ', '') == ohlc_key.replace(' ', '')):
                    has_ohlc = True
                    break
            
            if not has_ohlc:
                missing_instruments.append(instrument)
        
        if missing_instruments:
            print(f"📊 FALLBACK: Getting OHLC data for {len(missing_instruments)} missing instruments...")
            fallback_count = 0
            
            for instrument in missing_instruments:
                try:
                    token = instrument.get('token')
                    resolved_symbol = instrument.get('resolved_symbol', '')
                    
                    # Try individual OHLC call
                    try:
                        individual_ohlc = kite.ohlc([resolved_symbol])
                        if individual_ohlc and resolved_symbol in individual_ohlc:
                            data = individual_ohlc[resolved_symbol]
                            if 'ohlc' in data:
                                processed_ohlc[resolved_symbol] = {
                                    'open': float(data['ohlc'].get('open', 0)),
                                    'high': float(data['ohlc'].get('high', 0)),
                                    'low': float(data['ohlc'].get('low', 0)),
                                    'close': float(data['ohlc'].get('close', 0)),
                                    'last_price': float(data.get('last_price', 0)),
                                    'instrument_token': data.get('instrument_token'),
                                    'timestamp': datetime.now().isoformat(),
                                    'source': 'individual_ohlc_api'
                                }
                                fallback_count += 1
                                continue
                    except Exception as ohlc_error:
                        pass
                    
                    # Try historical data as last resort
                    try:
                        from_date = (datetime.now() - timedelta(days=3)).date()
                        to_date = datetime.now().date()
                        
                        historical_data = kite.historical_data(
                            instrument_token=token,
                            from_date=from_date,
                            to_date=to_date,
                            interval='day'
                        )
                        
                        if historical_data and len(historical_data) > 0:
                            latest_day = historical_data[-1]
                            processed_ohlc[resolved_symbol] = {
                                'open': float(latest_day.get('open', 0)),
                                'high': float(latest_day.get('high', 0)),
                                'low': float(latest_day.get('low', 0)),
                                'close': float(latest_day.get('close', 0)),
                                'last_price': float(latest_day.get('close', 0)),
                                'volume': int(latest_day.get('volume', 0)),
                                'timestamp': datetime.now().isoformat(),
                                'source': 'historical_api_fallback'
                            }
                            fallback_count += 1
                    except Exception as hist_error:
                        pass
                    
                    # Rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"⚠️ Fallback failed for {resolved_symbol}: {e}")
                    continue
            
            print(f"✅ Fallback retrieved OHLC data for {fallback_count} additional instruments")
        
        total_ohlc = len(processed_ohlc)
        print(f"📊 Total OHLC data retrieved: {total_ohlc} instruments")
        return processed_ohlc
        
    except Exception as e:
        print(f"⚠️ Could not fetch OHLC data: {e}")
        return {}

def get_30_day_historical_ohlc(instrument_token: int, kite) -> list:
    """
    Get 30-day historical OHLC data using Zerodha historical_data API (for individual instruments)
    
    ARGS:
        instrument_token: Zerodha instrument token
        kite: KiteConnect instance
        
    RETURNS:
        list: 30 days of OHLC data in format [{'date': 'YYYY-MM-DD', 'open': float, 'high': float, 'low': float, 'close': float, 'volume': int}, ...]
    """
    try:
        # Calculate date range (35 days buffer for non-trading days)
        to_date = datetime.now().date()
        from_date = to_date - timedelta(days=35)
        
        # Fetch historical data
        try:
            historical_data = kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval="day"
            )
        except Exception as api_error:
            print(f"❌ 30-day API Error for token {instrument_token}: {api_error}")
            return []
        
        if not historical_data or len(historical_data) < 30:
            return []
        
        # Convert to our format and take last 30 days
        ohlc_formatted = []
        for day_data in historical_data[-30:]:
            # Handle date formatting
            date_str = day_data['date']
            if hasattr(date_str, 'strftime'):
                date_str = date_str.strftime('%Y-%m-%d')
            elif isinstance(date_str, datetime):
                date_str = date_str.strftime('%Y-%m-%d')
            
            ohlc_formatted.append({
                'date': date_str,
                'open': float(day_data.get('open', 0)),
                'high': float(day_data.get('high', 0)),
                'low': float(day_data.get('low', 0)),
                'close': float(day_data.get('close', 0)),
                'volume': int(day_data.get('volume', 0))
            })
        
        return ohlc_formatted
        
    except Exception as e:
        print(f"⚠️ Could not fetch 30-day historical OHLC for token {instrument_token}: {e}")
        return []

def update_trading_crawler_averages_with_ohlc():
    """
    Enhanced version: Update 20-day averages + 30-day OHLC data for Trading Crawler instruments
    
    PROCESS:
        1. Load trading crawler instruments (updated configuration)
        2. Fetch historical data from Zerodha API
        3. Calculate 20-day averages for volume and price
        4. Get 30-day OHLC data for each instrument
        5. Save combined data to config/market_data_combined_{date}.json
        6. Update Redis cache with both averages and OHLC data
        
    RETURNS:
        dict: Combined averages + OHLC data
    """
    print("🚀 UPDATING 20-DAY AVERAGES + 30-DAY OHLC FOR INTRADAY CRAWLER")
    print("=" * 70)
    print("Processing intraday crawler's 262 instruments with token resolver")
    print("Covers NIFTY 50 + NIFTY 50 Futures + NIFTY Index Futures + BANKNIFTY Futures")
    print("Using token resolver for instrument token and name resolution")
    print("🔍 Updated configuration: November/December expiry instruments (25NOV/25DEC)")
    print()

    # Get intraday crawler instruments (262 instruments only)
    instruments = get_intraday_crawler_instruments()
    if not instruments:
        print("❌ No instruments found in intraday crawler")
        return {}

    # Load Zerodha token data directly from JSON file
    project_root = Path(__file__).parent.parent
    token_file = project_root / 'config' / 'zerodha_token.json'
    
    if not token_file.exists():
        print(f"❌ Token file not found: {token_file}")
        return {}
    
    with open(token_file, 'r') as f:
        token_data = json.load(f)
    
    api_key = token_data.get('api_key')
    access_token = token_data.get('access_token')
    
    if not api_key or not access_token:
        print("❌ API key or access token not found in zerodha_token.json")
        return {}
    
    if access_token == "YOUR_ACCESS_TOKEN":
        print("❌ Access token is a placeholder in zerodha_token.json")
        print("Please run 'python config/zerodha_config.py token <request_token>' to generate one.")
        return {}
    
    print("✅ Loaded Zerodha credentials from JSON file")
    print(f"API Key: {api_key}")
    print(f"Access Token: {access_token[:10]}...")
    
    # Initialize KiteConnect for API calls
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # Test the connection first
    try:
        profile = kite.profile()
        print(f"✅ Zerodha API connection successful - User: {profile.get('user_name', 'Unknown')}")
    except Exception as e:
        print(f"❌ Zerodha API connection failed: {e}")
        print("The access token may be expired. Please generate a new one.")
        print("Run: python crawlers/config/zerodha_config.py token <new_request_token>")
        return {}

    # Initialize Redis client using centralized configuration
    redis_client = None
    if REDIS_AVAILABLE:
        try:
            from redis_files.redis_client import get_redis_client
            redis_client = get_redis_client()
            redis_client.ping()  # Test connection
            print("✅ Connected to Redis using centralized configuration")
            print("📊 Using DB 5 for volume averages and OHLC data (cumulative_volume)")
        except Exception as e:
            print(f"⚠️ Redis connection failed: {e}")
            print("ℹ️ Continuing without Redis caching")
            redis_client = None
    else:
        print("ℹ️ Redis not available, skipping Redis caching")

    # Load existing averages
    project_root = Path(__file__).parent.parent
    # Write to config directory where the code expects to load from
    config_dir = project_root / 'config'
    config_dir.mkdir(exist_ok=True)
    averages_file = project_root / 'config' / 'volume_averages_20d.json'
    try:
        with open(averages_file, 'r') as f:
            existing_averages = json.load(f)
        print(f"✅ Loaded existing averages for {len(existing_averages)} instruments")
    except FileNotFoundError:
        existing_averages = {}
        print("ℹ️ No existing averages file found, starting fresh")
    except Exception as e:
        print(f"⚠️ Error loading existing averages: {e}")
        existing_averages = {}

    # NEW: Get bulk OHLC data for all instruments at once
    print(f"\n📊 FETCHING BULK OHLC DATA FOR {len(instruments)} INSTRUMENTS")
    print("=" * 70)
    # Get tokens from binary_crawler1.json for OHLC API
    project_root = Path(__file__).parent.parent
    crawler_config_file = project_root / 'crawlers' / 'binary_crawler1' / 'binary_crawler1.json'
    with open(crawler_config_file, 'r') as f:
        crawler_config = json.load(f)
    tokens = crawler_config.get('tokens', [])
    
    bulk_ohlc_data = get_bulk_ohlc_data(instruments, kite, tokens)
    
    # Create mapping from tokens to OHLC keys using the tokens from instruments
    token_to_ohlc_mapping = {}
    print(f"🔍 DEBUG: Creating token mapping from {len(bulk_ohlc_data)} OHLC entries")
    
    # Use the tokens from instruments to find matching OHLC data
    for instrument in instruments:
        token = instrument.get('token')
        resolved_symbol = instrument.get('resolved_symbol', '')
        
        # Try to find OHLC data by resolved symbol first
        if resolved_symbol and resolved_symbol in bulk_ohlc_data:
            token_to_ohlc_mapping[token] = resolved_symbol
            if len(token_to_ohlc_mapping) <= 5:  # Debug first 5
                print(f"🔍 DEBUG: Token {token} -> OHLC key: {resolved_symbol}")
        else:
            # Try fuzzy matching for symbols with spaces vs without spaces
            # e.g., "NSE:TATA MOTORS" vs "NSE:TATAMOTORS"
            found_match = False
            if resolved_symbol:
                for ohlc_key in bulk_ohlc_data.keys():
                    if ohlc_key:
                        # Remove spaces and compare
                        resolved_clean = resolved_symbol.replace(' ', '')
                        ohlc_clean = ohlc_key.replace(' ', '')
                        if resolved_clean == ohlc_clean:
                            token_to_ohlc_mapping[token] = ohlc_key
                            if len(token_to_ohlc_mapping) <= 5:  # Debug first 5
                                print(f"🔍 DEBUG: Token {token} -> OHLC key: {ohlc_key} (fuzzy match)")
                            found_match = True
                            break
            
            if not found_match:
                # Fallback: try to match by instrument_token if available
                for ohlc_key, ohlc_data in bulk_ohlc_data.items():
                    if ohlc_data.get('instrument_token') == token:
                        token_to_ohlc_mapping[token] = ohlc_key
                        if len(token_to_ohlc_mapping) <= 5:  # Debug first 5
                            print(f"🔍 DEBUG: Token {token} -> OHLC key: {ohlc_key}")
                        break
    
    print(f"🔍 DEBUG: Created mapping for {len(token_to_ohlc_mapping)} tokens")
    
    # Process each trading crawler instrument
    updated_count = 0
    failed_count = 0
    new_averages = existing_averages.copy()
    combined_data = {}  # New: Store combined averages + OHLC data

    print(f"\n📊 PROCESSING {len(instruments)} TRADING CRAWLER INSTRUMENTS")
    print("=" * 70)

    for i, instrument in enumerate(instruments, 1):
        symbol = instrument['symbol']
        token = instrument['token']
        name = instrument['name']
        exchange = instrument['exchange']
        instrument_type = instrument['instrument_type']
        current_price = instrument.get('current_price', 0)
        resolved_symbol = instrument.get('resolved_symbol', '')
        
        # Show resolved symbol if available
        display_symbol = resolved_symbol if resolved_symbol else symbol
        print(f"[{i:3d}/{len(instruments)}] Processing {display_symbol:20s} ({instrument_type:3s}) - Token: {str(token):8s}", end=" ")
        
        try:
            # Get historical data from Zerodha API
            from_date = (datetime.now() - timedelta(days=90)).date()  # 90 days to ensure 55+ trading days (accounting for weekends)
            to_date = datetime.now().date()
            
            # Add rate limiting (max 3 calls per second)
            time.sleep(0.35)  # ~3 calls per second
            
            # Retry logic for API calls (handle 503 Service Unavailable)
            historical_data = None
            max_retries = 3
            retry_delay = 2.0  # Start with 2 seconds
            
            # Convert token to integer if it's a string
            token_int = int(token) if isinstance(token, str) else token
            
            for attempt in range(max_retries):
                try:
                    historical_data = kite.historical_data(
                        instrument_token=token_int,
                        from_date=from_date,
                        to_date=to_date,
                        interval="day"
                    )
                    break  # Success, exit retry loop
                except Exception as api_error:
                    error_msg = str(api_error)
                    if "503 Service Unavailable" in error_msg and attempt < max_retries - 1:
                        print(f"⚠️ API Error (attempt {attempt + 1}/{max_retries}) for {resolved_symbol}: {error_msg}")
                        print(f"⏳ Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 1.5  # Exponential backoff
                        continue
                    else:
                        print(f"❌ API Error for {resolved_symbol}: {error_msg}")
                        break
            
            if historical_data and len(historical_data) > 0:  # Accept any available historical data
                # Calculate both 20-day and 55-day averages from API data
                # Ensure we have enough data for 55-day calculation
                min_days_needed = min(55, len(historical_data))
                
                # Calculate available days for averages
                available_days = len(historical_data)
                days_20d = min(20, available_days)
                days_55d = min(55, available_days)
                
                # Initialize variables for tracking actual days used
                actual_days_20d = 0
                actual_days_55d = 0
                
                # 20-day averages (use available days if less than 20)
                volumes_20d = [day.get('volume', 0) for day in historical_data[-days_20d:] if day.get('volume', 0) > 0]
                prices_20d = [day.get('close', 0) for day in historical_data[-days_20d:] if day.get('close', 0) > 0]
                
                # 55-day averages (use available days if less than 55)
                volumes_55d = [day.get('volume', 0) for day in historical_data[-days_55d:] if day.get('volume', 0) > 0]
                prices_55d = [day.get('close', 0) for day in historical_data[-days_55d:] if day.get('close', 0) > 0]
                
                # Extract OHLC data for ATR calculations
                ohlc_20d = []
                ohlc_55d = []
                
                for day in historical_data[-days_20d:]:
                    if all(day.get(field, 0) > 0 for field in ['open', 'high', 'low', 'close']):
                        # Convert date to string if it's a datetime object
                        date_str = day.get('date', '')
                        if hasattr(date_str, 'strftime'):
                            date_str = date_str.strftime('%Y-%m-%d')
                        elif isinstance(date_str, datetime):
                            date_str = date_str.strftime('%Y-%m-%d')
                        
                        ohlc_20d.append({
                            'open': day.get('open', 0),
                            'high': day.get('high', 0),
                            'low': day.get('low', 0),
                            'close': day.get('close', 0),
                            'volume': day.get('volume', 0),
                            'date': date_str
                        })
                
                for day in historical_data[-days_55d:]:
                    if all(day.get(field, 0) > 0 for field in ['open', 'high', 'low', 'close']):
                        # Convert date to string if it's a datetime object
                        date_str = day.get('date', '')
                        if hasattr(date_str, 'strftime'):
                            date_str = date_str.strftime('%Y-%m-%d')
                        elif isinstance(date_str, datetime):
                            date_str = date_str.strftime('%Y-%m-%d')
                        
                        ohlc_55d.append({
                            'open': day.get('open', 0),
                            'high': day.get('high', 0),
                            'low': day.get('low', 0),
                            'close': day.get('close', 0),
                            'volume': day.get('volume', 0),
                            'date': date_str
                        })
                
                if volumes_20d and prices_20d:
                    # Calculate 20-day averages
                    avg_volume_20d = sum(volumes_20d) / len(volumes_20d)
                    avg_price_20d = sum(prices_20d) / len(prices_20d)
                    actual_days_20d = len(volumes_20d)
                    
                    # Calculate 55-day averages (use available data, fallback to 20-day if insufficient)
                    if len(volumes_55d) >= 20:  # Use 55-day if we have at least 20 days
                        avg_volume_55d = sum(volumes_55d) / len(volumes_55d)
                        avg_price_55d = sum(prices_55d) / len(prices_55d)
                        actual_days_55d = len(volumes_55d)
                    else:
                        # Fallback to 20-day data if insufficient for 55-day
                        avg_volume_55d = avg_volume_20d
                        avg_price_55d = avg_price_20d
                        actual_days_55d = actual_days_20d
                    
                    # Create proper key with exchange prefix
                    key = f"{exchange}:{symbol}"
                    new_averages[key] = {
                        # 20-day averages
                        'avg_volume_20d': round(avg_volume_20d, 2),
                        'avg_price_20d': round(avg_price_20d, 2),
                        'data_points_20d': len(volumes_20d),
                        
                        # 55-day averages (for Turtle Trading)
                        'avg_volume_55d': round(avg_volume_55d, 2),
                        'avg_price_55d': round(avg_price_55d, 2),
                        'data_points_55d': actual_days_55d,
                        
                        # Metadata
                        'last_updated': datetime.now().isoformat(),
                        'name': name,
                        'current_price': current_price,
                        'instrument_type': instrument_type,
                        'exchange': exchange,
                        'source': 'trading_crawler',
                        'resolved_symbol': resolved_symbol,  # Store resolved symbol for reference
                        'turtle_ready': actual_days_55d >= 55  # Flag for full Turtle Trading capability
                    }
                    
                    # Update Redis cache (if available) - FOLLOW README ARCHITECTURE
                    if redis_client:
                        normalized_symbol = normalize_symbol(key)
                        try:
                            # FOLLOW README: DB 5 for cumulative volume (line 162)
                            # Store volume averages in DB 5 as per README architecture
                            redis_client.select(5)  # DB 5: Cumulative volume
                            
                            # Store volume averages in proper format
                            volume_key = f"volume_averages:{normalized_symbol}"
                            volume_data = {
                                'avg_volume_20d': avg_volume_20d,
                                'avg_volume_55d': avg_volume_55d,
                                'data_points_20d': len(volumes_20d),
                                'data_points_55d': actual_days_55d,
                                'last_updated': datetime.now().isoformat(),
                                'turtle_ready': str(actual_days_55d >= 55),
                                'source': 'intraday_crawler'
                            }
                            # Store each field individually (DB 2 analytics)
                            for field, value in volume_data.items():
                                redis_client.get_client(2).hset(volume_key, field, value)
                            redis_client.get_client(2).expire(volume_key, 86400)  # 24 hours
                            
                            # 2. Store historical OHLC data in daily zset (for volume baseline calculations)
                            daily_zset_key = ohlc_daily_zset(normalized_symbol)
                            for day_data in ohlc_20d:
                                # Create timestamp for sorting
                                date_str = day_data.get('date', '')
                                if date_str:
                                    try:
                                        # Parse date and create timestamp
                                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                                        timestamp = int(date_obj.timestamp())
                                        
                                        # Create payload for zset
                                        payload = {
                                            'o': day_data.get('open', 0),
                                            'h': day_data.get('high', 0),
                                            'l': day_data.get('low', 0),
                                            'c': day_data.get('close', 0),
                                            'v': day_data.get('volume', 0),
                                            'date': date_str
                                        }
                                        
                                        # Store in zset with timestamp as score
                                        redis_client.zadd(daily_zset_key, {json.dumps(payload): timestamp})
                                        
                                    except Exception as date_error:
                                        print(f"⚠️ Date parsing error for {symbol}: {date_error}")
                                        continue
                            
                            # Set TTL for daily zset (30 days)
                            redis_client.expire(daily_zset_key, 2592000)  # 30 days
                            
                            # 3. Store in legacy market_data format (for backward compatibility)
                            redis_key = f"market_data:averages:{key}"
                            redis_data = {
                                'avg_volume_20d': avg_volume_20d,
                                'avg_price_20d': avg_price_20d,
                                'data_points_20d': len(volumes_20d),
                                'avg_volume_55d': avg_volume_55d,
                                'avg_price_55d': avg_price_55d,
                                'data_points_55d': actual_days_55d,
                                'last_updated': datetime.now().isoformat(),
                                'turtle_ready': str(actual_days_55d >= 55),
                                'source': 'intraday_crawler'
                            }
                            # Store each field individually using hset (DB 5)
                            for field, value in redis_data.items():
                                redis_client.get_client(2).hset(redis_key, field, value)  # analytics DB
                            redis_client.get_client(2).expire(redis_key, 86400)  # 24 hours
                                
                        except Exception as e:
                            print(f"⚠️ Redis update failed for {symbol}: {e}")
                    
                    # NEW: Get current OHLC data from bulk fetch
                    # Use token to find the correct OHLC data
                    instrument_token = instrument.get('token')
                    ohlc_key = token_to_ohlc_mapping.get(instrument_token)
                    if ohlc_key:
                        current_ohlc = bulk_ohlc_data.get(ohlc_key, {})
                    else:
                        current_ohlc = {}
                        # Debug: Check if token mapping is working
                        if i <= 5 or instrument_token == 884737:  # Debug first 5 instruments + TATA MOTORS
                            print(f"🔍 DEBUG: Token {instrument_token} -> OHLC key: {ohlc_key}")
                            print(f"🔍 DEBUG: Resolved symbol: {instrument.get('resolved_symbol')}")
                            print(f"🔍 DEBUG: Available OHLC keys: {list(bulk_ohlc_data.keys())[:5]}")
                            print(f"🔍 DEBUG: Available tokens in mapping: {list(token_to_ohlc_mapping.keys())[:5]}")
                    
                    # Store combined data (averages + current OHLC)
                    combined_data[key] = {
                        **new_averages[key],  # Include all existing averages data
                        'current_ohlc': current_ohlc,  # Add current OHLC data
                        'has_ohlc': bool(current_ohlc)
                    }
                    
                    # Update Redis with current OHLC data - FOLLOW README ARCHITECTURE
                    if redis_client and current_ohlc:
                        try:
                            # FOLLOW README: DB 2 for price data (line 160)
                            # Store OHLC data in DB 2 as per README architecture
                            redis_client.select(2)  # DB 2: Price data
                            
                            # Store current OHLC data in proper format with None value handling
                            price_key = f"ohlc_latest:{normalized_symbol}"
                            price_data = {
                                'open': current_ohlc.get('open', 0.0) or 0.0,
                                'high': current_ohlc.get('high', 0.0) or 0.0,
                                'low': current_ohlc.get('low', 0.0) or 0.0,
                                'close': current_ohlc.get('close', 0.0) or 0.0,
                                'volume': current_ohlc.get('volume', 0) or 0,
                                'last_price': current_ohlc.get('close', current_ohlc.get('last_price', 0.0)) or 0.0,
                                'date': current_ohlc.get('date', datetime.now().strftime('%Y-%m-%d')) or datetime.now().strftime('%Y-%m-%d'),
                                'updated_at': datetime.now().isoformat(),
                                'source': 'intraday_crawler'
                            }
                            # Store each field individually (DB 2 for OHLC data) - convert None to appropriate defaults
                            for field, value in price_data.items():
                                # Convert None values to appropriate defaults
                                if value is None:
                                    if field in ['open', 'high', 'low', 'close', 'last_price']:
                                        value = 0.0
                                    elif field == 'volume':
                                        value = 0
                                    elif field == 'date':
                                        value = datetime.now().strftime('%Y-%m-%d')
                                    else:
                                        value = ''
                                redis_client.get_client(1).hset(price_key, field, value)  # realtime DB (prices)
                            redis_client.get_client(1).expire(price_key, 129600)  # 36 hours
                            
                            # 2. Store in daily zset for historical access (if we have a valid date)
                            if current_ohlc.get('date'):
                                try:
                                    daily_zset_key = ohlc_daily_zset(normalized_symbol)
                                    date_str = current_ohlc.get('date')
                                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                                    timestamp = int(date_obj.timestamp())
                                    
                                    # Create payload for zset
                                    payload = {
                                        'o': current_ohlc.get('open', 0),
                                        'h': current_ohlc.get('high', 0),
                                        'l': current_ohlc.get('low', 0),
                                        'c': current_ohlc.get('close', 0),
                                        'v': current_ohlc.get('volume', 0),
                                        'date': date_str
                                    }
                                    
                                    # Store in zset with timestamp as score (DB 5)
                                    redis_client.get_client(2).zadd(daily_zset_key, {json.dumps(payload): timestamp})  # analytics DB
                                    redis_client.get_client(2).expire(daily_zset_key, 2592000)  # 30 days
                                    
                                except Exception as date_error:
                                    print(f"⚠️ Date parsing error for current OHLC {symbol}: {date_error}")
                            
                            # 3. Store in timeseries format (for technical analysis) with None value handling
                            timeseries_key = ohlc_timeseries_key(normalized_symbol, "1d")
                            timeseries_data = {
                                'timestamp': int(datetime.now().timestamp()),
                                'open': current_ohlc.get('open', 0.0) or 0.0,
                                'high': current_ohlc.get('high', 0.0) or 0.0,
                                'low': current_ohlc.get('low', 0.0) or 0.0,
                                'close': current_ohlc.get('close', 0.0) or 0.0,
                                'volume': current_ohlc.get('volume', 0) or 0,
                                'date': current_ohlc.get('date', datetime.now().strftime('%Y-%m-%d')) or datetime.now().strftime('%Y-%m-%d')
                            }
                            
                            # Store as JSON string in timeseries
                            redis_client.set(timeseries_key, json.dumps(timeseries_data))
                            redis_client.expire(timeseries_key, 129600)  # 36 hours
                            
                        except Exception as e:
                            print(f"⚠️ Redis OHLC update failed for {symbol}: {e}")
                    
                    updated_count += 1
                    ohlc_status = f" | OHLC: ✅" if current_ohlc else " | OHLC: ❌"
                    days_info = f"({actual_days_20d}d/{actual_days_55d}d)" if actual_days_20d < 20 or actual_days_55d < 55 else ""
                    print(f"✅ 20d: Vol {avg_volume_20d:,.0f}, Price ₹{avg_price_20d:.2f} | 55d: Vol {avg_volume_55d:,.0f}, Price ₹{avg_price_55d:.2f} {days_info}{ohlc_status}")
                else:
                    print("❌ No valid data")
                    failed_count += 1
            else:
                print("❌ Insufficient historical data")
                failed_count += 1
                
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
            failed_count += 1

    # Save updated averages to file
    try:
        with open(averages_file, 'w') as f:
            json.dump(new_averages, f, indent=2)
        print(f"\n💾 Saved updated averages to {averages_file}")
    except Exception as e:
        print(f"❌ Failed to save averages file: {e}")
    
    # NEW: Save combined data (averages + OHLC) to JSON file
    if combined_data:
        try:
            combined_file = project_root / 'config' / f'market_data_combined_{datetime.now().strftime("%Y%m%d")}.json'
            with open(combined_file, 'w') as f:
                json.dump(combined_data, f, indent=2)
            print(f"💾 Saved combined data (averages + OHLC) to {combined_file}")
            
            # Show OHLC data summary
            ohlc_count = sum(1 for data in combined_data.values() if data.get('has_ohlc'))
            print(f"📊 OHLC Summary: {ohlc_count} instruments with current OHLC data")
            
        except Exception as e:
            print(f"❌ Failed to save combined data file: {e}")
    else:
        print("⚠️ No combined data to save")

    # Summary
    print(f"\n📈 PROCESSING SUMMARY")
    print("=" * 70)
    print(f"Total instruments processed: {len(instruments)}")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed: {failed_count}")
    print(f"Success rate: {(updated_count/len(instruments)*100):.1f}%")
    print(f"Total averages in file: {len(new_averages)}")
    
    # Show sample of updated data by instrument type
    if updated_count > 0:
        print(f"\n📊 SAMPLE UPDATED DATA BY INSTRUMENT TYPE:")
        
        # Group by instrument type
        by_type = {}
        for key, data in new_averages.items():
            if data.get('source') == 'trading_crawler':
                inst_type = data.get('instrument_type', 'UNKNOWN')
                if inst_type not in by_type:
                    by_type[inst_type] = []
                by_type[inst_type].append((key, data))
        
        # Show samples for each type
        for inst_type, items in by_type.items():
            print(f"\n  {inst_type} ({len(items)} instruments):")
            for key, data in items[:3]:  # Show first 3 of each type
                resolved_sym = data.get('resolved_symbol', key)
                print(f"    {resolved_sym:20s} - Vol: {data['avg_volume_20d']:8,.0f}, Price: ₹{data['avg_price_20d']:8.2f}")
            if len(items) > 3:
                print(f"    ... and {len(items) - 3} more {inst_type} instruments")

    print(f"\n✅ INTRADAY CRAWLER 20-DAY AVERAGES + CURRENT OHLC UPDATE COMPLETED")
    print(f"📁 Output: config/volume_averages_20d.json")
    print(f"📁 Combined: config/market_data_combined_{datetime.now().strftime('%Y%m%d')}.json")
    print(f"🗄️ Redis Storage Summary:")
    print(f"   • ohlc_stats:* - Volume averages (20d/55d) for indicators")
    print(f"   • ohlc_latest:* - Current OHLC data for real-time analysis")
    print(f"   • ohlc_daily:* - Historical OHLC data for volume baselines")
    print(f"   • ohlc:*:1d - Timeseries data for technical analysis")
    print(f"   • market_data:averages:* - Legacy format for backward compatibility")
    print(f"⚡ Used bulk OHLC API for fast data retrieval (262 instruments only)")
    print(f"🔧 All Redis keys use normalized symbols and proper TTL for indicators")
    print(f"🔄 Updated for November/December expiry instruments (25NOV/25DEC) - no more October expiry")
    
    return combined_data if combined_data else new_averages

def main():
    """Main execution function - Enhanced version with current OHLC data"""
    print("🔄 Starting enhanced 20-day averages + current OHLC update for Trading Crawler...")
    print("🔄 Updated for November/December expiry instruments (25NOV/25DEC) - no more October expiry")
    result = update_trading_crawler_averages_with_ohlc()
    
    if result:
        print("\n🎉 Enhanced update completed successfully!")
        print("📊 Now includes current OHLC data for better trading strategies")
        print("⚡ Uses bulk OHLC API for fast retrieval (262 instruments)")
        print("🔄 Updated configuration: November/December expiry instruments (25NOV/25DEC)")
        return True
    else:
        print("\n❌ Enhanced update failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
