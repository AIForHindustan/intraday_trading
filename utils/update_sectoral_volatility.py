#!/usr/bin/env python3
"""
Update Sectoral Volatility Data for Baseline Understanding
Fetches sectoral indices (not constituents) and calculates their overall volatility for baseline market regime analysis

USAGE:
    - Used by: Market regime analysis, baseline volatility understanding
    - Called from: Production scheduler, manual execution
    - Saves to: config/sector_volatility.json
    - Dependencies: Zerodha Kite Connect API, json, datetime

PURPOSE:
    - Updates overall volatility for 9 NSE sectoral indices (the indices themselves, not constituents)
    - Fetches historical index data from Zerodha API
    - Calculates 20-day and 55-day volatility for each index
    - Provides baseline volatility understanding for market regime analysis
    - Covers: NIFTY 50, NIFTY BANK, NIFTY AUTO, NIFTY IT, NIFTY PHARMA, NIFTY FMCG, NIFTY METAL, NIFTY ENERGY, NIFTY REALTY

NSE SECTORAL INDICES (Index Symbols):
    - NIFTY 50: NSE:NIFTY 50 (broad market index)
    - NIFTY BANK: NSE:NIFTY BANK (banking sector index)
    - NIFTY AUTO: NSE:NIFTY AUTO (automotive sector index)
    - NIFTY IT: NSE:NIFTY IT (IT sector index)
    - NIFTY PHARMA: NSE:NIFTY PHARMA (pharmaceutical sector index)
    - NIFTY FMCG: NSE:NIFTY FMCG (FMCG sector index)
    - NIFTY METAL: NSE:NIFTY METAL (metals sector index)
    - NIFTY ENERGY: NSE:NIFTY ENERGY (energy sector index)
    - NIFTY REALTY: NSE:NIFTY REALTY (realty sector index)

SCHEDULING:
    - WEEKLY EXECUTION: Run every Sunday for weekly sector updates
    - Recommended time: 6:00 PM IST (after market close)
    - Manual execution: python utils/update_sectoral_volatility.py
    - Production scheduler: Integrated with weekly data refresh
    - Market calendar aware: Skips holidays

CREATED: October 31, 2025
UPDATED: November 6, 2025 - Initial implementation for NSE sectoral indices
"""

import sys
from pathlib import Path
import json
from datetime import datetime, timedelta
import time
import math

# Add parent directory to path to import from config
sys.path.append(str(Path(__file__).parent.parent))

from config.zerodha_config import ZerodhaConfig

def get_index_instrument_tokens(kite):
    """
    Get instrument tokens for NSE sectoral indices
    
    ARGS:
        kite: Zerodha Kite Connect instance
    
    RETURNS:
        dict: Mapping of index name to instrument token
    """
    # NSE sectoral index symbols (Zerodha tradingsymbol format)
    index_symbols = {
        'NIFTY_50': 'NIFTY 50',
        'NIFTY_BANK': 'NIFTY BANK',
        'NIFTY_AUTO': 'NIFTY AUTO',
        'NIFTY_IT': 'NIFTY IT',
        'NIFTY_PHARMA': 'NIFTY PHARMA',
        'NIFTY_FMCG': 'NIFTY FMCG',
        'NIFTY_METAL': 'NIFTY METAL',
        'NIFTY_ENERGY': 'NIFTY ENERGY',
        'NIFTY_REALTY': 'NIFTY REALTY'
    }
    
    try:
        # Get all instruments from Zerodha
        instruments = kite.instruments("NSE")
        
        # Map index symbols to tokens
        index_tokens = {}
        for index_name, symbol in index_symbols.items():
            # Find matching instrument by exact tradingsymbol match
            for inst in instruments:
                if inst.get('tradingsymbol') == symbol:
                    index_tokens[index_name] = inst['instrument_token']
                    print(f"✅ Found {index_name}: token {inst['instrument_token']} ({symbol})")
                    break
            else:
                print(f"⚠️ Could not find token for {index_name} ({symbol})")
        
        return index_tokens
        
    except Exception as e:
        print(f"❌ Error fetching instrument tokens: {e}")
        return {}

def fetch_index_historical_data(kite, instrument_token, days_back=60):
    """
    Fetch historical data for an index
    
    ARGS:
        kite: Zerodha Kite Connect instance
        instrument_token: Instrument token for the index
        days_back: Number of days of historical data to fetch
        
    RETURNS:
        list: List of historical OHLC data points
    """
    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=days_back)
        
        historical_data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date.date(),
            to_date=to_date.date(),
            interval="day"
        )
        
        return historical_data if historical_data else []
        
    except Exception as e:
        print(f"⚠️ Error fetching historical data for token {instrument_token}: {e}")
        return []

def calculate_index_volatility(historical_data, window_days=20):
    """
    Calculate volatility for an index using historical price data
    
    ARGS:
        historical_data: List of historical OHLC data (from Zerodha API)
        window_days: Number of days for volatility calculation window
        
    RETURNS:
        float: Annualized volatility percentage
    """
    try:
        if not historical_data or len(historical_data) < window_days:
            return 0.0
        
        # Extract closing prices from historical data
        closes = [day['close'] for day in historical_data if day.get('close')]
        
        if len(closes) < window_days:
            return 0.0
        
        # Use last window_days for calculation
        recent_closes = closes[-window_days:]
        
        # Calculate daily returns
        returns = []
        for i in range(1, len(recent_closes)):
            if recent_closes[i-1] > 0:
                daily_return = (recent_closes[i] - recent_closes[i-1]) / recent_closes[i-1]
                returns.append(daily_return)
        
        if len(returns) < 2:
            return 0.0
        
        # Calculate standard deviation of returns
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        std_dev = math.sqrt(variance)
        
        # Annualize volatility (assuming 252 trading days)
        annualized_volatility = std_dev * math.sqrt(252) * 100
        
        return round(annualized_volatility, 2)
        
    except Exception as e:
        print(f"⚠️ Error calculating volatility: {e}")
        return 0.0

def update_sectoral_volatility():
    """
    Update sectoral index volatility data from Zerodha API
    
    PROCESS:
        1. Initialize Zerodha Kite Connect API
        2. Get instrument tokens for sectoral indices
        3. Fetch historical data for each index
        4. Calculate 20-day and 55-day volatility for each index
        5. Update sector_volatility.json with index volatility data
        6. Maintain metadata and structure
        
    RETURNS:
        dict: Updated sectoral volatility data with index-level volatility
    """
    print("🚀 UPDATING SECTORAL INDEX VOLATILITY DATA")
    print("=" * 70)
    print("Fetching 9 NSE sectoral indices for baseline volatility understanding")
    print("Calculating overall index volatility (not individual constituents)")
    print()

    # Initialize Zerodha API
    try:
        kite = ZerodhaConfig.get_kite_instance()
        print("✅ Connected to Zerodha API")
    except Exception as e:
        print(f"❌ Failed to connect to Zerodha API: {e}")
        return {}

    # Load existing sector_volatility.json
    project_root = Path(__file__).parent.parent
    sector_file = project_root / 'config' / 'sector_volatility.json'
    
    existing_data = {}
    if sector_file.exists():
        try:
            with open(sector_file, 'r') as f:
                existing_data = json.load(f)
            print(f"✅ Loaded existing sector_volatility.json")
        except Exception as e:
            print(f"⚠️ Error loading existing data: {e}")
            existing_data = {}
    else:
        print("ℹ️ No existing sector_volatility.json found, creating new one")
        existing_data = {}

    # Get index instrument tokens
    print("\n🔍 Fetching index instrument tokens...")
    index_tokens = get_index_instrument_tokens(kite)
    
    if not index_tokens:
        print("❌ No index tokens found, using existing data")
        return existing_data

    # Fetch historical data and calculate volatility for each index
    print("\n📊 Calculating index volatility...")
    index_volatility = {}
    
    for index_name, token in index_tokens.items():
        print(f"  📈 Processing {index_name} (token {token})...", end=" ")
        
        try:
            # Fetch 60 days of historical data (for both 20d and 55d calculations)
            historical_data = fetch_index_historical_data(kite, token, days_back=60)
            
            if not historical_data:
                print("❌ No data")
                continue
            
            # Calculate 20-day and 55-day volatility
            vol_20d = calculate_index_volatility(historical_data, window_days=20)
            vol_55d = calculate_index_volatility(historical_data, window_days=55)
            
            # Get current price
            current_price = historical_data[-1]['close'] if historical_data else 0.0
            
            index_volatility[index_name] = {
                'symbol': index_name,
                'instrument_token': token,
                'current_price': round(current_price, 2),
                'volatility_20d_pct': vol_20d,
                'volatility_55d_pct': vol_55d,
                'data_points': len(historical_data),
                'last_updated': datetime.now().isoformat()
            }
            
            print(f"✅ Vol 20d: {vol_20d}%, Vol 55d: {vol_55d}%")
            
            # Rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            continue

    # Preserve existing sectors data if it exists, but update metadata
    existing_sectors = existing_data.get('sectors', {})
    
    # Update metadata with index volatility data
    updated_data = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'source': 'Zerodha Kite Connect API',
            'window_days': 20,
            'sectors': len(index_volatility),
            'formula': 'volatility = sqrt(252) * std_dev(returns)',
            'data_type': 'index_volatility',
            'description': 'Overall volatility for sectoral indices (baseline understanding)',
            'straddle_patterns': {
                'description': 'Straddle strategy patterns for NIFTY/BANKNIFTY options',
                'underlying_symbols': ['NIFTY', 'BANKNIFTY'],
                'strike_selection': 'atm',
                'expiry_preference': 'weekly',
                'confidence_threshold': 0.85,
                'telegram_routing': True
            }
        },
        'index_volatility': index_volatility,  # New: Index-level volatility
        'sectors': existing_sectors  # Preserve existing constituent data if any
    }

    # Save updated data
    try:
        with open(sector_file, 'w') as f:
            json.dump(updated_data, f, indent=2)
        print(f"\n💾 Saved updated sectoral volatility data to {sector_file}")
    except Exception as e:
        print(f"❌ Failed to save sectoral volatility data: {e}")
        return existing_data

    # Summary
    print(f"\n📈 SECTORAL INDEX VOLATILITY UPDATE SUMMARY")
    print("=" * 70)
    print(f"Total indices updated: {len(index_volatility)}")
    print(f"Data source: Zerodha Kite Connect API")
    print(f"Use case: Baseline volatility understanding for market regime analysis")
    print(f"Data type: Index-level volatility (not individual constituents)")
    
    # Show index breakdown
    if index_volatility:
        print(f"\n📊 INDEX VOLATILITY BREAKDOWN:")
        for index_name, data in index_volatility.items():
            print(f"  • {index_name}:")
            print(f"    Price: {data['current_price']}")
            print(f"    20d Vol: {data['volatility_20d_pct']}%")
            print(f"    55d Vol: {data['volatility_55d_pct']}%")

    print(f"\n✅ SECTORAL INDEX VOLATILITY UPDATE COMPLETED")
    print(f"🎯 Ready for baseline volatility analysis")
    print(f"📁 Output: config/sector_volatility.json")
    print(f"🔄 Recommended: Weekly execution (Sundays at 6:00 PM IST)")
    
    return updated_data

def main():
    """Main execution function"""
    print("🔄 Starting sectoral index volatility update...")
    result = update_sectoral_volatility()
    
    if result:
        print("\n🎉 Sectoral index volatility update completed successfully!")
        print("📊 Ready for baseline volatility analysis")
        return True
    else:
        print("\n❌ Sectoral index volatility update failed!")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
