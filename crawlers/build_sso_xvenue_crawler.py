#!/usr/bin/env python3
"""
Build SSO-XVENUE Crawler Configuration
Single-Stock F&O + BSE Cross-Venue

Focus:
- NSE Single-Stock Futures: 140 (active F&O, near + next month)
- NSE Single-Stock Options: 120 (top 60 F&O names, ATM ±3 weekly)
- BSE Index Derivatives: 20-30 (SENSEX/BANKEX options + futures)
- BSE Cash/ETF: 30 (SENSEX 30 + ETFs)
- Spare: 10-20 for temporary inclusions

Target: ~320 instruments
Hours: 09:05-15:35
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.append(str(Path(__file__).parent.parent))
from config.zerodha_config import ZerodhaConfig

def get_single_stock_futures(kite, max_count=140):
    """Get NSE single-stock futures (near + next month)"""
    print("\n📈 Fetching NSE Single-Stock Futures...")
    
    all_instruments = kite.instruments("NFO")
    
    # Get current and next month expiries
    today = datetime.now()
    cutoff = today + timedelta(days=60)
    
    # Group by underlying
    futures_by_underlying = defaultdict(list)
    
    for inst in all_instruments:
        if inst['instrument_type'] != 'FUT':
            continue
        
        symbol = inst['tradingsymbol']
        expiry = inst.get('expiry')
        
        # Skip index futures
        if any(idx in symbol for idx in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY']):
            continue
        
        if expiry:
            expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
            if expiry_date < cutoff.date():
                underlying = symbol.replace('FUT', '').rstrip('0123456789')
                futures_by_underlying[underlying].append({
                    'symbol': symbol,
                    'token': inst['instrument_token'],
                    'expiry': expiry,
                    'name': inst.get('name', ''),
                    'exchange': 'NFO',
                    'instrument_type': 'FUT'
                })
    
    # Keep only near + next month for each underlying
    selected = []
    for underlying, futs in futures_by_underlying.items():
        # Sort by expiry
        futs.sort(key=lambda x: x['expiry'])
        # Take first 2 (near + next)
        selected.extend(futs[:2])
    
    # Limit to max_count
    selected = selected[:max_count]
    
    print(f"  ✅ Found {len(selected)} single-stock futures ({len(futures_by_underlying)} underlyings)")
    return selected

def get_single_stock_options(kite, max_count=120):
    """Get NSE single-stock options (top 60 F&O names, ATM ±3)"""
    print("\n📊 Fetching NSE Single-Stock Options...")
    
    # Top 60 F&O stocks by liquidity
    top_fo_stocks = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'HINDUNILVR', 'ITC', 'SBIN',
        'BHARTIARTL', 'KOTAKBANK', 'LT', 'AXISBANK', 'ASIANPAINT', 'MARUTI', 'BAJFINANCE',
        'HCLTECH', 'TITAN', 'SUNPHARMA', 'ULTRACEMCO', 'NESTLEIND', 'WIPRO', 'TECHM',
        'M&M', 'TATAMOTORS', 'POWERGRID', 'NTPC', 'ONGC', 'TATASTEEL', 'BAJAJFINSV',
        'ADANIENT', 'COALINDIA', 'JSWSTEEL', 'INDUSINDBK', 'DRREDDY', 'HINDALCO',
        'GRASIM', 'CIPLA', 'DIVISLAB', 'EICHERMOT', 'BRITANNIA', 'APOLLOHOSP',
        'SHREECEM', 'UPL', 'BPCL', 'HEROMOTOCO', 'ADANIPORTS', 'TATACONSUM', 'BAJAJ-AUTO',
        'SBILIFE', 'HDFCLIFE', 'LTIM', 'PIDILITIND', 'SIEMENS', 'DLF', 'GODREJCP',
        'AMBUJACEM', 'DABUR', 'HAVELLS', 'ICICIPRULI', 'INDIGO'
    ][:60]
    
    all_instruments = kite.instruments("NFO")
    
    # Find nearest 2 expiries from actual stock options data (not index)
    today = datetime.now()
    cutoff = today + timedelta(days=90)  # Look at next 3 months
    
    # Collect all unique expiries for our target stocks
    stock_expiries = set()
    
    for inst in all_instruments:
        if inst['instrument_type'] not in ['CE', 'PE']:
            continue
        
        symbol = inst['tradingsymbol']
        
        # Check if it's for one of our top stocks (not an index)
        is_target_stock = False
        for stock in top_fo_stocks:
            if symbol.startswith(stock):
                is_target_stock = True
                break
        
        if not is_target_stock:
            continue
        
        expiry = inst.get('expiry')
        if expiry:
            expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
            if today.date() <= expiry_date < cutoff.date():
                stock_expiries.add(expiry_date)
    
    valid_expiries = sorted(list(stock_expiries))[:2]  # Take nearest 2
    print(f"  Target expiries (stock options): {valid_expiries}")
    
    # Group options by underlying and strike (we already filtered by expiry above)
    options_by_stock = defaultdict(lambda: defaultdict(list))
    
    # Debug: Track what we find
    total_options_nfo = 0
    matched_underlying = 0
    matched_expiry = 0
    
    for inst in all_instruments:
        if inst['instrument_type'] not in ['CE', 'PE']:
            continue
        
        total_options_nfo += 1
        symbol = inst['tradingsymbol']
        
        # Check if it's for one of our top stocks
        underlying = None
        for stock in top_fo_stocks:
            if symbol.startswith(stock):
                underlying = stock
                break
        
        if not underlying:
            continue
        
        matched_underlying += 1
        
        # Check if current or next month expiry
        expiry = inst.get('expiry')
        if not expiry:
            continue
        
        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
        if expiry_date not in valid_expiries:
            continue
        
        matched_expiry += 1
        
        # Extract strike
        strike = inst.get('strike', 0)
        
        options_by_stock[underlying][strike].append({
            'symbol': symbol,
            'token': inst['instrument_token'],
            'strike': strike,
            'expiry': expiry,
            'underlying': underlying,
            'option_type': inst['instrument_type'],
            'name': inst.get('name', ''),
            'exchange': 'NFO',
            'instrument_type': inst['instrument_type']
        })
    
    # Debug output
    print(f"  Debug: Total NFO options={total_options_nfo}, Matched underlying={matched_underlying}, Matched expiry={matched_expiry}")
    
    # For each stock, select ATM ±3 strikes (current month only for now)
    selected_options = []
    
    for underlying, strikes_dict in options_by_stock.items():
        if not strikes_dict:
            continue
        
        # Get all strikes
        strikes = sorted(strikes_dict.keys())
        
        if len(strikes) == 0:
            continue
        
        # Estimate ATM (middle strike)
        mid_idx = len(strikes) // 2
        atm_strike = strikes[mid_idx]
        
        # Get ATM ±3 strikes
        start_idx = max(0, mid_idx - 3)
        end_idx = min(len(strikes), mid_idx + 4)  # +4 because slice is exclusive
        
        selected_strikes = strikes[start_idx:end_idx]
        
        # Add all CE/PE for these strikes
        for strike in selected_strikes:
            selected_options.extend(strikes_dict[strike])
    
    # Limit to max_count
    selected_options = selected_options[:max_count]
    
    print(f"  ✅ Found {len(selected_options)} single-stock options from {len(options_by_stock)} stocks")
    return selected_options

def get_bse_index_derivatives(kite, max_count=30):
    """Get BSE index derivatives (SENSEX, BANKEX)"""
    print("\n🏛️  Fetching BSE Index Derivatives...")
    
    try:
        all_instruments = kite.instruments("BFO")
    except:
        print("  ⚠️ BFO not accessible, using empty set")
        return []
    
    derivatives = []
    for inst in all_instruments:
        symbol = inst['tradingsymbol']
        
        # SENSEX or BANKEX derivatives
        if any(idx in symbol for idx in ['SENSEX', 'BANKEX']):
            derivatives.append({
                'symbol': symbol,
                'token': inst['instrument_token'],
                'expiry': inst.get('expiry'),
                'name': inst.get('name', ''),
                'exchange': 'BFO',
                'instrument_type': inst['instrument_type']
            })
    
    derivatives = derivatives[:max_count]
    print(f"  ✅ Found {len(derivatives)} BSE index derivatives")
    return derivatives

def get_bse_cash_etf(kite, max_count=30):
    """Get BSE cash (SENSEX 30) and ETFs"""
    print("\n💰 Fetching BSE Cash & ETFs...")
    
    # SENSEX 30 constituents
    sensex_30 = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'INFY', 'ICICIBANK', 'HINDUNILVR', 'ITC', 'SBIN',
        'BHARTIARTL', 'KOTAKBANK', 'LT', 'AXISBANK', 'ASIANPAINT', 'MARUTI', 'BAJFINANCE',
        'HCLTECH', 'TITAN', 'SUNPHARMA', 'ULTRACEMCO', 'NESTLEIND', 'WIPRO', 'TECHM',
        'M&M', 'TATAMOTORS', 'POWERGRID', 'NTPC', 'ONGC', 'TATASTEEL', 'BAJAJFINSV', 'ADANIENT'
    ]
    
    # Top ETFs
    top_etfs = ['SENSEXETF', 'BANKBEES', 'NIFTYBEES', 'JUNIORBEES', 'GOLDBEES']
    
    all_instruments = kite.instruments("BSE")
    
    cash_etf = []
    for inst in all_instruments:
        symbol = inst['tradingsymbol']
        
        # Check if SENSEX 30 or ETF
        if symbol in sensex_30 or symbol in top_etfs:
            if inst['instrument_type'] == 'EQ':
                cash_etf.append({
                    'symbol': symbol,
                    'token': inst['instrument_token'],
                    'name': inst.get('name', ''),
                    'exchange': 'BSE',
                    'instrument_type': 'EQ'
                })
    
    cash_etf = cash_etf[:max_count]
    print(f"  ✅ Found {len(cash_etf)} BSE cash/ETF instruments")
    return cash_etf

def build_sso_xvenue_config():
    """Build complete SSO-XVENUE crawler configuration"""
    print("=" * 60)
    print("BUILDING SSO-XVENUE CRAWLER CONFIGURATION")
    print("=" * 60)
    
    try:
        kite = ZerodhaConfig.get_kite_instance()
        print("✅ Connected to Zerodha API")
        
        # Collect all instruments
        instruments = []
        
        # 1. NSE Single-Stock Futures (140)
        instruments.extend(get_single_stock_futures(kite, max_count=140))
        
        # 2. NSE Single-Stock Options (120)
        instruments.extend(get_single_stock_options(kite, max_count=120))
        
        # 3. BSE Index Derivatives (20-30)
        instruments.extend(get_bse_index_derivatives(kite, max_count=30))
        
        # 4. BSE Cash/ETF (30)
        instruments.extend(get_bse_cash_etf(kite, max_count=30))
        
        print(f"\n📋 Total instruments collected: {len(instruments)}")
        
        # Build config
        config = {
            "metadata": {
                "name": "SSO-XVENUE",
                "description": "Single-Stock F&O + BSE Cross-Venue Crawler",
                "version": "1.0",
                "created": datetime.now().isoformat(),
                "total_instruments": len(instruments),
                "focus": "NSE single-stock derivatives + BSE index derivatives + BSE cash for cross-venue routing",
                "architecture": "binary_only",
                "processing": "Offline - separate from data capture"
            },
            "crawler_settings": {
                "max_instruments": 350,
                "buffer_size": 3000,
                "enable_depth": True,
                "enable_volume": True,
                "active_hours": {
                    "start": "09:05",
                    "end": "15:35",
                    "timezone": "Asia/Kolkata",
                    "reason": "F&O + BSE day session window"
                },
                "streaming_mode": "websocket_binary",
                "data_push": "real-time (event-driven)",
                "protocol": "Zerodha WebSocket Binary (wss://ws.kite.trade)",
                "websocket_mode": "FULL",
                "packet_size": "184 bytes",
                "data_storage": {
                    "format": "raw_binary",
                    "directory": "raw_ticks/sso_xvenue",
                    "rotation": "hourly",
                    "filename_pattern": "sso_xvenue_YYYYMMDD_HHMM.bin",
                    "compression": None,
                    "note": "Raw binary WebSocket packets - NO processing, just save to disk",
                    "architecture": {
                        "capture": "WebSocket binary packets saved directly to disk",
                        "processing": "Separate consumer reads .bin files and processes",
                        "separation": "Clean separation: crawlers capture, consumers process"
                    }
                },
                "binary_format": {
                    "header": "timestamp (8 bytes) + length (4 bytes)",
                    "header_format": ">QI (big-endian, uint64 timestamp_ms, uint32 length)",
                    "payload": "raw WebSocket binary packet (variable length)"
                },
                "note": "RAW TICK CAPTURE ONLY - No processing, no parsing, just binary storage"
            },
            "instrument_breakdown": {
                "nse_stock_futures": {
                    "count": len([i for i in instruments if i['exchange'] == 'NFO' and i['instrument_type'] == 'FUT']),
                    "description": "Single-stock futures (near + next month)"
                },
                "nse_stock_options": {
                    "count": len([i for i in instruments if i['exchange'] == 'NFO' and i['instrument_type'] in ['CE', 'PE']]),
                    "description": "Top 60 F&O stocks, ATM ±3 weekly"
                },
                "bse_index_derivatives": {
                    "count": len([i for i in instruments if i['exchange'] == 'BFO']),
                    "description": "SENSEX/BANKEX options + futures"
                },
                "bse_cash_etf": {
                    "count": len([i for i in instruments if i['exchange'] == 'BSE']),
                    "description": "SENSEX 30 cash + ETFs for cross-venue analysis"
                }
            },
            "instruments": {}
        }
        
        # Add instruments
        for inst in instruments:
            key = f"{inst['exchange']}:{inst['symbol']}"
            config['instruments'][key] = {
                'token': inst['token'],
                'tradingsymbol': inst['symbol'],
                'exchange': inst['exchange'],
                'instrument_type': inst['instrument_type'],
                'segment': inst['exchange'],
                'name': inst['name'],
                'expiry': inst.get('expiry').isoformat() if inst.get('expiry') else None
            }
        
        # Save config
        config_path = Path(__file__).parent.parent / "config" / "sso_xvenue_crawler.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print("\n" + "=" * 60)
        print("✅ SSO-XVENUE CONFIGURATION COMPLETE!")
        print("=" * 60)
        print(f"Total Instruments: {len(instruments)}")
        print(f"Config saved: {config_path}")
        print("\nBreakdown:")
        for key, value in config['instrument_breakdown'].items():
            print(f"  • {key}: {value['count']} - {value['description']}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    build_sso_xvenue_config()

