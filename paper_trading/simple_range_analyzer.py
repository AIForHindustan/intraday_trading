#!/usr/bin/env python3
"""
Simple Range-Bound Analyzer using market data JSON
"""

import json
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def analyze_range_bound_markets():
    """Analyze range-bound conditions using market data JSON"""
    
    # Load market data
    market_data_file = project_root / "config" / "market_data_combined_20251017.json"
    
    if not market_data_file.exists():
        print("❌ Market data file not found")
        return
    
    print("📊 Loading market data...")
    with open(market_data_file, 'r') as f:
        market_data = json.load(f)
    
    print(f"✅ Loaded data for {len(market_data)} symbols")
    
    # Analyze range-bound conditions
    range_bound_symbols = []
    total_symbols = 0
    
    for symbol, data in market_data.items():
        total_symbols += 1
        
        # Check if symbol has OHLC data
        if not data.get('has_ohlc', False) or 'current_ohlc' not in data:
            continue
            
        ohlc = data['current_ohlc']
        high = ohlc.get('high', 0)
        low = ohlc.get('low', 0)
        close = ohlc.get('close', 0)
        
        if high <= 0 or low <= 0 or close <= 0:
            continue
        
        # Calculate range width
        range_width = (high - low) / low
        
        # Define range-bound as < 5% daily range
        if range_width < 0.05:
            range_bound_symbols.append({
                'symbol': symbol,
                'name': data.get('name', symbol),
                'high': high,
                'low': low,
                'close': close,
                'range_width_pct': range_width * 100,
                'avg_price_55d': data.get('avg_price_55d', 0)
            })
    
    # Sort by range width (tightest ranges first)
    range_bound_symbols.sort(key=lambda x: x['range_width_pct'])
    
    print(f"\n📈 RANGE-BOUND ANALYSIS RESULTS:")
    print(f"   Total symbols: {total_symbols}")
    print(f"   Range-bound symbols: {len(range_bound_symbols)}")
    print(f"   Range-bound percentage: {len(range_bound_symbols)/total_symbols:.1%}")
    
    print(f"\n🎯 TOP RANGE-BOUND OPPORTUNITIES:")
    for i, symbol_data in enumerate(range_bound_symbols[:10]):
        print(f"   {i+1}. {symbol_data['name']} ({symbol_data['symbol']})")
        print(f"      Range: {symbol_data['low']:.1f} - {symbol_data['high']:.1f} ({symbol_data['range_width_pct']:.1f}%)")
        print(f"      Current: {symbol_data['close']:.1f}")
        print(f"      55d Avg: {symbol_data['avg_price_55d']:.1f}")
        print()
    
    return range_bound_symbols

def generate_options_strategies(range_bound_symbols):
    """Generate options strategies for range-bound symbols"""
    
    print(f"📋 GENERATING OPTIONS STRATEGIES")
    print("=" * 50)
    
    strategies = []
    
    for symbol_data in range_bound_symbols[:5]:  # Top 5 opportunities
        symbol = symbol_data['symbol']
        name = symbol_data['name']
        high = symbol_data['high']
        low = symbol_data['low']
        close = symbol_data['close']
        range_width = symbol_data['range_width_pct']
        
        # Calculate Iron Condor strikes
        short_put = low * 0.98    # 2% below support
        short_call = high * 1.02  # 2% above resistance
        long_put = short_put * 0.96    # 2% further down
        long_call = short_call * 1.04   # 2% further up
        
        # Calculate strategy metrics
        max_profit = (short_put - long_put) + (long_call - short_call)
        max_loss = (high - low) - max_profit
        
        strategy = {
            'symbol': symbol,
            'name': name,
            'current_price': close,
            'range_width_pct': range_width,
            'strikes': {
                'long_put': long_put,
                'short_put': short_put,
                'short_call': short_call,
                'long_call': long_call
            },
            'max_profit': max_profit,
            'max_loss': max_loss,
            'probability': min(0.85, 0.6 + (0.05 - range_width/100) * 5)  # Higher for tighter ranges
        }
        
        strategies.append(strategy)
    
    # Display strategies
    for i, strategy in enumerate(strategies, 1):
        print(f"{i}. {strategy['name']} ({strategy['symbol']}) - Iron Condor")
        print(f"   Current Price: {strategy['current_price']:.1f}")
        print(f"   Range Width: {strategy['range_width_pct']:.1f}%")
        print(f"   Probability: {strategy['probability']:.1%}")
        print(f"   Max Profit: {strategy['max_profit']:.1f}")
        print(f"   Max Loss: {strategy['max_loss']:.1f}")
        print(f"   Strikes:")
        print(f"     • Long PUT: {strategy['strikes']['long_put']:.0f}")
        print(f"     • Short PUT: {strategy['strikes']['short_put']:.0f}")
        print(f"     • Short CALL: {strategy['strikes']['short_call']:.0f}")
        print(f"     • Long CALL: {strategy['strikes']['long_call']:.0f}")
        print()
    
    return strategies

def main():
    """Main analysis function"""
    print("🚀 Simple Range-Bound Market Analysis")
    print("=" * 50)
    
    # Analyze range-bound markets
    range_bound_symbols = analyze_range_bound_markets()
    
    if range_bound_symbols:
        # Generate options strategies
        strategies = generate_options_strategies(range_bound_symbols)
        
        print(f"💡 MANUAL EXECUTION STEPS:")
        print(f"   1. Check these symbols in your broker")
        print(f"   2. Verify option strikes are available")
        print(f"   3. Execute Iron Condor strategies")
        print(f"   4. Set profit targets at 50% of max profit")
        print(f"   5. Set stop-loss at 2x max loss")
    else:
        print("❌ No range-bound opportunities found today")

if __name__ == "__main__":
    main()
