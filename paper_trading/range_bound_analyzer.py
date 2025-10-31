#!/usr/bin/env python3
"""
Range-Bound Market Analyzer
Detects range-bound conditions and suggests options strategies
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import sys
from datetime import datetime, timedelta
import redis

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class RangeBoundAnalyzer:
    def __init__(self, data_source="duckdb"):
        """Initialize range-bound analyzer"""
        self.data_source = data_source
        self.historical_data = {}
        self.redis_client = None
        self.load_data()
    
    def load_data(self):
        """Load data from available sources"""
        if self.data_source == "duckdb":
            self._load_duckdb_data()
        elif self.data_source == "redis":
            self._load_redis_data()
        else:
            print("❌ No data source specified")
    
    def _load_duckdb_data(self):
        """Load from DuckDB database"""
        try:
            import duckdb
        except ImportError:
            print("❌ DuckDB not available")
            return
            
        db_path = project_root / "nse_tick_data.duckdb"
        
        if db_path.exists():
            print(f"📁 Loading from DuckDB: {db_path}")
            
            conn = duckdb.connect(str(db_path))
            
            # Query for daily OHLC data
            query = """
            SELECT 
                instrument,
                date_partition,
                MIN(last_price) as low,
                MAX(last_price) as high,
                FIRST(last_price) as open,
                LAST(last_price) as close,
                SUM(volume) as volume
            FROM tick_data 
            WHERE instrument IS NOT NULL 
            GROUP BY instrument, date_partition
            ORDER BY instrument, date_partition
            """
            
            try:
                result = conn.execute(query).fetchall()
                conn.close()
                
                # Process results into daily OHLC format
                for row in result:
                    symbol, date, low, high, open_price, close, volume = row
                    
                    # Skip invalid data
                    if not all([low, high, open_price, close]) or low <= 0 or high <= 0:
                        continue
                    
                    if symbol not in self.historical_data:
                        self.historical_data[symbol] = []
                    
                    self.historical_data[symbol].append({
                        'date': date,
                        'open': float(open_price),
                        'high': float(high),
                        'low': float(low),
                        'close': float(close),
                        'volume': int(volume) if volume else 0
                    })
                
                print(f"✅ Loaded data for {len(self.historical_data)} symbols")
                
                # Debug: Show sample data
                if self.historical_data:
                    sample_symbol = list(self.historical_data.keys())[0]
                    sample_data = self.historical_data[sample_symbol][:3]
                    print(f"📊 Sample data for {sample_symbol}:")
                    for day in sample_data:
                        print(f"   {day['date']}: O={day['open']:.1f} H={day['high']:.1f} L={day['low']:.1f} C={day['close']:.1f}")
                
            except Exception as e:
                print(f"❌ Error querying DuckDB: {e}")
    
    def _load_redis_data(self):
        """Load from Redis cache"""
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
            # Redis connection established
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
    
    def analyze_range_bound_percentage(self, window_days=20, range_threshold=0.08):
        """Analyze how often stocks are range-bound"""
        print(f"\n📊 Analyzing Range-Bound Conditions")
        print(f"   Window: {window_days} days, Threshold: {range_threshold:.1%}")
        
        range_bound_days = 0
        total_days = 0
        symbol_stats = {}
        
        for symbol in self.historical_data:
            if len(self.historical_data[symbol]) < window_days + 1:
                continue
                
            symbol_range_days = 0
            symbol_total_days = 0
            
            # Analyze rolling windows
            for i in range(window_days, len(self.historical_data[symbol])):
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
    
    def detect_tradable_ranges(self, min_range_days=15, max_range_width=0.10):
        """Detect current tradable ranges for options strategies"""
        print(f"\n🎯 Detecting Tradable Ranges")
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
                'probability': min(0.85, 0.6 + (0.1 - range_width/100) * 2.5),  # Higher probability for tighter ranges
                'risk_reward': max_profit / abs(max_loss) if max_loss != 0 else 0
            }
            
            # Strangle Strategy (alternative)
            strangle = {
                'symbol': symbol,
                'strategy': 'Strangle',
                'current_price': current_price,
                'range_width_pct': range_width,
                'strikes': {
                    'put_strike': support * 0.99,
                    'call_strike': resistance * 1.01
                },
                'actions': [
                    f"SELL PUT {support * 0.99:.0f}",
                    f"SELL CALL {resistance * 1.01:.0f}"
                ],
                'max_profit': (support * 0.99 + resistance * 1.01) / 2,  # Simplified
                'max_loss': (resistance - support) * 0.5,  # Simplified
                'probability': min(0.80, 0.5 + (0.1 - range_width/100) * 3),
                'risk_reward': 1.5  # Typical for strangles
            }
            
            strategies.append({
                'range_data': range_data,
                'iron_condor': iron_condor,
                'strangle': strangle
            })
        
        return strategies
    
    def manual_test_options_strategies(self, test_months=6):
        """Test options strategies on historical data"""
        print(f"\n🧪 Testing Options Strategies on Historical Data")
        print(f"   Testing last {test_months} months")
        
        test_results = []
        
        # Focus on major indices for testing
        test_symbols = ['NIFTY 50', 'NIFTY BANK', 'NSE:NIFTY 50', 'NSE:NIFTY BANK']
        
        for symbol in test_symbols:
            if symbol not in self.historical_data:
                continue
                
            print(f"\n📈 Testing {symbol}...")
            
            # Get recent months
            recent_data = self.historical_data[symbol][-test_months:]
            
            for i, month_data in enumerate(recent_data):
                if i < 20:  # Need at least 20 days for range calculation
                    continue
                    
                # Get 20-day window ending at this month
                window_start = max(0, i - 20)
                window = self.historical_data[symbol][window_start:i+1]
                
                if len(window) < 20:
                    continue
                
                high = max([day['high'] for day in window if day['high'] > 0])
                low = min([day['low'] for day in window if day['low'] > 0])
                close = month_data['close']
                
                if high <= 0 or low <= 0 or close <= 0:
                    continue
                
                # Simulate iron condor
                short_call = high * 1.02
                short_put = low * 0.98
                
                if short_put <= close <= short_call:
                    result = "✅ PROFIT - Price in range"
                    pnl = 0.12  # 12% average credit
                else:
                    result = "❌ LOSS - Price broke range"
                    pnl = -0.25  # 25% max loss
                
                test_results.append({
                    'symbol': symbol,
                    'month': month_data.get('date', f'Month {i}'),
                    'result': result,
                    'pnl': pnl,
                    'high': high,
                    'low': low,
                    'close': close
                })
                
                print(f"   {month_data.get('date', f'Month {i}')}: {result} (PnL: {pnl:+.1%})")
        
        # Calculate overall performance
        if test_results:
            total_pnl = sum([r['pnl'] for r in test_results])
            wins = len([r for r in test_results if r['pnl'] > 0])
            total_trades = len(test_results)
            win_rate = wins / total_trades if total_trades > 0 else 0
            
            print(f"\n📊 OVERALL TEST RESULTS:")
            print(f"   Total trades: {total_trades}")
            print(f"   Win rate: {win_rate:.1%}")
            print(f"   Total PnL: {total_pnl:+.1%}")
            print(f"   Average PnL per trade: {total_pnl/total_trades:+.1%}")
        
        return test_results
    
    def generate_daily_scan(self):
        """Generate daily options scan for manual execution"""
        print(f"\n🎯 DAILY OPTIONS SCAN")
        print("=" * 50)
        
        # Detect current ranges
        ranges = self.detect_tradable_ranges()
        
        if not ranges:
            print("❌ No tradable ranges found today")
            return
        
        # Generate strategies
        strategies = self.generate_options_strategies(ranges[:5])  # Top 5 opportunities
        
        print(f"\n📋 TOP OPTIONS OPPORTUNITIES:")
        
        for i, strategy in enumerate(strategies, 1):
            range_data = strategy['range_data']
            iron_condor = strategy['iron_condor']
            
            print(f"\n{i}. {range_data['symbol']} - Iron Condor")
            print(f"   Current Price: {range_data['current_price']:.1f}")
            print(f"   Range: {range_data['support']:.1f} - {range_data['resistance']:.1f} ({range_data['range_width_pct']:.1f}%)")
            print(f"   Probability: {iron_condor['probability']:.1%}")
            print(f"   Max Profit: {iron_condor['max_profit']:.1f}")
            print(f"   Max Loss: {iron_condor['max_loss']:.1f}")
            print(f"   Actions:")
            for action in iron_condor['actions']:
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
    print("🚀 Starting Range-Bound Market Analysis")
    print("=" * 60)
    
    # Initialize analyzer
    analyzer = RangeBoundAnalyzer(data_source="duckdb")
    
    if not analyzer.historical_data:
        print("❌ No data loaded")
        return
    
    # 1. Analyze range-bound percentage
    range_stats = analyzer.analyze_range_bound_percentage()
    
    # 2. Detect current tradable ranges
    tradable_ranges = analyzer.detect_tradable_ranges()
    
    # 3. Generate options strategies
    if tradable_ranges:
        strategies = analyzer.generate_options_strategies(tradable_ranges)
        
        # 4. Test on historical data
        test_results = analyzer.manual_test_options_strategies()
        
        # 5. Generate daily scan
        daily_scan = analyzer.generate_daily_scan()
    
    print(f"\n✅ Analysis Complete!")
    print(f"📊 Range-bound time: {range_stats['overall_percentage']:.1%}")
    print(f"🎯 Tradable ranges found: {len(tradable_ranges)}")

if __name__ == "__main__":
    main()
