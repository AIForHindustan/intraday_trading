#!/usr/bin/env python3
"""
Real 30-Day Range-Bound Analyzer
Uses actual historical OHLC data from JSONL files
"""

import json
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime, timedelta
from collections import defaultdict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class Real30DayRangeAnalyzer:
    def __init__(self):
        """Initialize with 30-day historical data"""
        self.historical_data = {}
        self.load_30day_data()
    
    def load_30day_data(self):
        """Load 30 days of OHLC data from our NIFTY 50 JSON file"""
        print("📊 Loading 30-day NIFTY 50 OHLC data...")
        
        # Load our fetched NIFTY 50 data
        data_file = project_root / "nifty_30day_ohlc_oct28.json"
        
        if not data_file.exists():
            print(f"❌ Data file not found: {data_file}")
            return
        
        try:
            with open(data_file, 'r') as f:
                nifty_data = json.load(f)
            
            # Process NIFTY 50 index data
            if 'nifty_50_index' in nifty_data:
                index_data = nifty_data['nifty_50_index']['data']
                processed_index = []
                
                for day in index_data:
                    try:
                        # Parse date
                        date_str = day['date']
                        if '+05:30' in date_str:
                            dt = datetime.fromisoformat(date_str)
                        elif 'T' in date_str:
                            dt = datetime.fromisoformat(date_str.replace('+05:30', ''))
                        else:
                            dt = datetime.strptime(date_str, '%Y-%m-%d')
                        
                        processed_index.append({
                            'date': dt.date(),
                            'timestamp': dt,
                            'open': float(day['open']),
                            'high': float(day['high']),
                            'low': float(day['low']),
                            'close': float(day['close']),
                            'volume': int(day.get('volume', 0)) or 0
                        })
                    except (ValueError, KeyError) as e:
                        continue
                
                if processed_index:
                    self.historical_data['NIFTY_50'] = processed_index
                    print(f"✅ Loaded NIFTY 50 index: {len(processed_index)} days")
            
            # Process NIFTY 50 stocks data
            if 'nifty_50_stocks' in nifty_data:
                stocks_data = nifty_data['nifty_50_stocks']
                
                for symbol, stock_data in stocks_data.items():
                    if not stock_data:
                        continue
                    
                    processed_stock = []
                    
                    for day in stock_data:
                        try:
                            # Parse date
                            date_str = day['date']
                            if '+05:30' in date_str:
                                dt = datetime.fromisoformat(date_str)
                            elif 'T' in date_str:
                                dt = datetime.fromisoformat(date_str.replace('+05:30', ''))
                            else:
                                dt = datetime.strptime(date_str, '%Y-%m-%d')
                            
                            processed_stock.append({
                                'date': dt.date(),
                                'timestamp': dt,
                                'open': float(day['open']),
                                'high': float(day['high']),
                                'low': float(day['low']),
                                'close': float(day['close']),
                                'volume': int(day.get('volume', 0)) or 0
                            })
                        except (ValueError, KeyError) as e:
                            continue
                    
                    if len(processed_stock) >= 15:  # Need at least 15 days
                        self.historical_data[symbol] = processed_stock
                        print(f"✅ Loaded {symbol}: {len(processed_stock)} days")
            
            # Process NIFTY futures data
            if 'futures' in nifty_data:
                futures_data = nifty_data['futures']
                
                for future_symbol, future_data in futures_data.items():
                    if not future_data:
                        continue
                    
                    processed_future = []
                    
                    for day in future_data:
                        try:
                            # Parse date
                            date_str = day['date']
                            if '+05:30' in date_str:
                                dt = datetime.fromisoformat(date_str)
                            elif 'T' in date_str:
                                dt = datetime.fromisoformat(date_str.replace('+05:30', ''))
                            else:
                                dt = datetime.strptime(date_str, '%Y-%m-%d')
                            
                            processed_future.append({
                                'date': dt.date(),
                                'timestamp': dt,
                                'open': float(day['open']),
                                'high': float(day['high']),
                                'low': float(day['low']),
                                'close': float(day['close']),
                                'volume': int(day.get('volume', 0)) or 0
                            })
                        except (ValueError, KeyError) as e:
                            continue
                    
                    if len(processed_future) >= 15:  # Need at least 15 days
                        self.historical_data[future_symbol] = processed_future
                        print(f"✅ Loaded {future_symbol}: {len(processed_future)} days")
            
            print(f"✅ Loaded data for {len(self.historical_data)} symbols")
            
            # Show sample data
            if self.historical_data:
                sample_symbol = list(self.historical_data.keys())[0]
                sample_data = self.historical_data[sample_symbol][-3:]
                print(f"📊 Sample data for {sample_symbol}:")
                for day in sample_data:
                    print(f"   {day['date']}: O={day['open']:.1f} H={day['high']:.1f} L={day['low']:.1f} C={day['close']:.1f}")
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return
    
    def analyze_range_bound_percentage(self, window_days=30, range_threshold=0.08):
        """Analyze how often stocks are range-bound over 30-day windows"""
        print(f"\n📊 Analyzing Range-Bound Conditions ({window_days}-day windows)")
        print(f"   Window: {window_days} days, Threshold: {range_threshold:.1%}")
        
        range_bound_days = 0
        total_days = 0
        symbol_stats = {}
        
        for symbol in self.historical_data:
            data_length = len(self.historical_data[symbol])
            print(f"   📊 {symbol}: {data_length} days available")
            
            if data_length < window_days + 1:
                print(f"      ❌ Not enough data (need {window_days + 1}, have {data_length})")
                continue
                
            symbol_range_days = 0
            symbol_total_days = 0
            
            # Analyze rolling 20-day windows
            for i in range(window_days, data_length):
                # Get 20-day window
                window = self.historical_data[symbol][i-window_days:i]
                
                # Calculate range metrics
                valid_days = [day for day in window if day['high'] > 0 and day['low'] > 0 and day['close'] > 0]
                
                if len(valid_days) < window_days * 0.8:  # Need at least 80% valid data
                    continue
                
                high_20 = max([day['high'] for day in valid_days])
                low_20 = min([day['low'] for day in valid_days])
                current_price = valid_days[-1]['close']
                
                range_width = (high_20 - low_20) / low_20
                
                # Define range-bound as < threshold
                if range_width < range_threshold:
                    range_bound_days += 1
                    symbol_range_days += 1
                
                total_days += 1
                symbol_total_days += 1
            
            if symbol_total_days > 0:
                symbol_range_pct = symbol_range_days / symbol_total_days
                symbol_stats[symbol] = {
                    'range_percentage': symbol_range_pct,
                    'total_days': symbol_total_days,
                    'range_days': symbol_range_days
                }
                print(f"      ✅ {symbol}: {symbol_range_pct:.1%} range-bound ({symbol_range_days}/{symbol_total_days} days)")
            else:
                print(f"      ❌ {symbol}: No valid windows found")
        
        # Calculate overall statistics
        overall_range_pct = range_bound_days / total_days if total_days > 0 else 0
        
        # Top range-bound symbols
        top_range_symbols = sorted(
            symbol_stats.items(), 
            key=lambda x: x[1]['range_percentage'], 
            reverse=True
        )[:10]
        
        print(f"\n📈 OVERALL RANGE ANALYSIS:")
        print(f"   Range-bound time: {overall_range_pct:.1%}")
        print(f"   Total days analyzed: {total_days:,}")
        print(f"   Range-bound days: {range_bound_days:,}")
        
        print(f"\n🏆 TOP RANGE-BOUND SYMBOLS:")
        for symbol, stats in top_range_symbols:
            print(f"   {symbol}: {stats['range_percentage']:.1%} ({stats['range_days']}/{stats['total_days']} days)")
        
        return {
            'overall_percentage': overall_range_pct,
            'total_days': total_days,
            'range_bound_days': range_bound_days,
            'symbol_stats': symbol_stats,
            'top_symbols': top_range_symbols
        }
    
    def detect_current_ranges(self, min_range_days=30, max_range_width=0.10):
        """Detect current tradable ranges for options strategies"""
        print(f"\n🎯 Detecting Current Tradable Ranges")
        print(f"   Min range days: {min_range_days}, Max width: {max_range_width:.1%}")
        
        tradable_ranges = []
        
        for symbol in self.historical_data:
            if len(self.historical_data[symbol]) < min_range_days:
                continue
            
            # Get recent data
            recent_data = self.historical_data[symbol][-min_range_days:]
            
            # Calculate range metrics
            recent_high = max([day['high'] for day in recent_data if day['high'] > 0])
            recent_low = min([day['low'] for day in recent_data if day['low'] > 0])
            current_price = recent_data[-1]['close']
            
            if recent_high <= 0 or recent_low <= 0 or current_price <= 0:
                continue
            
            range_width = (recent_high - recent_low) / recent_low
            
            # Check if in tradable range
            if (range_width < max_range_width and 
                recent_low * 1.02 <= current_price <= recent_high * 0.98):
                
                tradable_ranges.append({
                    'symbol': symbol,
                    'support': recent_low,
                    'resistance': recent_high,
                    'current_price': current_price,
                    'range_width_pct': range_width * 100,
                    'days_in_range': min_range_days,
                    'range_center': (recent_high + recent_low) / 2
                })
        
        # Sort by range width (tighter ranges first)
        tradable_ranges.sort(key=lambda x: x['range_width_pct'])
        
        print(f"✅ Found {len(tradable_ranges)} tradable ranges")
        
        # Show top opportunities
        for i, range_data in enumerate(tradable_ranges[:5]):
            print(f"   {i+1}. {range_data['symbol']}: {range_data['range_width_pct']:.1f}% range "
                  f"({range_data['support']:.1f}-{range_data['resistance']:.1f})")
        
        return tradable_ranges
    
    def generate_options_strategies(self, tradable_ranges):
        """Generate specific options strategies for each range"""
        print(f"\n📋 Generating Options Strategies")
        
        strategies = []
        
        for range_data in tradable_ranges:
            symbol = range_data['symbol']
            support = range_data['support']
            resistance = range_data['resistance']
            current_price = range_data['current_price']
            range_width = range_data['range_width_pct']
            
            # Calculate optimal strikes
            short_put = support * 0.98    # 2% below support
            short_call = resistance * 1.02  # 2% above resistance
            long_put = short_put * 0.96    # 2% further down
            long_call = short_call * 1.04   # 2% further up
            
            # Calculate strategy metrics
            max_profit = (short_put - long_put) + (long_call - short_call)
            max_loss = (resistance - support) - max_profit
            
            # Iron Condor Strategy
            iron_condor = {
                'symbol': symbol,
                'strategy': 'Iron Condor',
                'current_price': current_price,
                'range_width_pct': range_width,
                'strikes': {
                    'long_put': long_put,
                    'short_put': short_put,
                    'short_call': short_call,
                    'long_call': long_call
                },
                'actions': [
                    f"SELL PUT {short_put:.0f}",
                    f"BUY PUT {long_put:.0f}",
                    f"SELL CALL {short_call:.0f}",
                    f"BUY CALL {long_call:.0f}"
                ],
                'max_profit': max_profit,
                'max_loss': max_loss,
                'probability': min(0.85, 0.6 + (0.1 - range_width/100) * 2.5),
                'risk_reward': max_profit / abs(max_loss) if max_loss != 0 else 0
            }
            
            strategies.append(iron_condor)
        
        return strategies
    
    def analyze_full_30day_ranges(self):
        """Analyze the full 30-day period as single ranges for each symbol"""
        print(f"\n📊 FULL 30-DAY RANGE ANALYSIS")
        print("=" * 50)
        
        full_ranges = []
        
        for symbol in self.historical_data:
            if len(self.historical_data[symbol]) < 30:
                continue
                
            # Get all 30 days of data
            full_data = self.historical_data[symbol]
            
            # Calculate full 30-day range
            high_30 = max([day['high'] for day in full_data if day['high'] > 0])
            low_30 = min([day['low'] for day in full_data if day['low'] > 0])
            current_price = full_data[-1]['close']
            
            if high_30 <= 0 or low_30 <= 0 or current_price <= 0:
                continue
            
            range_width = (high_30 - low_30) / low_30
            
            full_ranges.append({
                'symbol': symbol,
                'support': low_30,
                'resistance': high_30,
                'current_price': current_price,
                'range_width_pct': range_width * 100,
                'days_in_range': len(full_data),
                'range_center': (high_30 + low_30) / 2,
                'start_date': full_data[0]['date'],
                'end_date': full_data[-1]['date']
            })
        
        # Sort by range width (tighter ranges first)
        full_ranges.sort(key=lambda x: x['range_width_pct'])
        
        print(f"✅ Analyzed {len(full_ranges)} symbols for full 30-day ranges")
        
        # Show top 10 tightest ranges
        print(f"\n🏆 TOP 30-DAY RANGE OPPORTUNITIES:")
        for i, range_data in enumerate(full_ranges[:10], 1):
            print(f"   {i}. {range_data['symbol']}: {range_data['range_width_pct']:.1f}% range "
                  f"({range_data['support']:.1f}-{range_data['resistance']:.1f}) "
                  f"Current: {range_data['current_price']:.1f}")
        
        return full_ranges

    def generate_daily_scan(self):
        """Generate daily options scan for manual execution"""
        print(f"\n🎯 DAILY OPTIONS SCAN - REAL 30-DAY DATA")
        print("=" * 60)
        
        # Detect current ranges
        ranges = self.detect_current_ranges()
        
        if not ranges:
            print("❌ No tradable ranges found today")
            return
        
        # Generate strategies
        strategies = self.generate_options_strategies(ranges[:5])  # Top 5 opportunities
        
        print(f"\n📋 TOP OPTIONS OPPORTUNITIES:")
        
        for i, strategy in enumerate(strategies, 1):
            print(f"\n{i}. {strategy['symbol']} - Iron Condor")
            print(f"   Current Price: {strategy['current_price']:.1f}")
            print(f"   Range Width: {strategy['range_width_pct']:.1f}%")
            print(f"   Probability: {strategy['probability']:.1%}")
            print(f"   Max Profit: {strategy['max_profit']:.1f}")
            print(f"   Max Loss: {strategy['max_loss']:.1f}")
            print(f"   Risk-Reward: {strategy['risk_reward']:.2f}")
            print(f"   Actions:")
            for action in strategy['actions']:
                print(f"     • {action}")
        
        print(f"\n💡 MANUAL EXECUTION STEPS:")
        print(f"   1. Check these symbols in your broker")
        print(f"   2. Verify option strikes are available")
        print(f"   3. Execute Iron Condor strategies")
        print(f"   4. Set profit targets at 50% of max profit")
        print(f"   5. Set stop-loss at 2x max loss")
        
        return strategies

def main():
    """Main analysis function"""
    print("🚀 Real 30-Day Range-Bound Market Analysis")
    print("=" * 60)
    
    # Initialize analyzer
    analyzer = Real30DayRangeAnalyzer()
    
    if not analyzer.historical_data:
        print("❌ No data loaded")
        return
    
    # 1. Analyze range-bound percentage (30-day windows)
    range_stats = analyzer.analyze_range_bound_percentage()
    
    # 2. Analyze full 30-day ranges
    full_30day_ranges = analyzer.analyze_full_30day_ranges()
    
    # 3. Detect current tradable ranges
    tradable_ranges = analyzer.detect_current_ranges()
    
    # 4. Generate options strategies
    if tradable_ranges:
        strategies = analyzer.generate_options_strategies(tradable_ranges)
        
        # 5. Generate daily scan
        daily_scan = analyzer.generate_daily_scan()
    
    print(f"\n✅ Analysis Complete!")
    print(f"📊 Range-bound time: {range_stats['overall_percentage']:.1%}")
    print(f"🎯 Tradable ranges found: {len(tradable_ranges)}")

if __name__ == "__main__":
    main()
