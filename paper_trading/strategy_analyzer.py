#!/usr/bin/env python3
"""
Simple Paper Trading Strategy Analyzer
Uses historical data to test trading strategies
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
from datetime import datetime, timedelta
import sys
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

class StrategyAnalyzer:
    def __init__(self, data_source="parquet"):
        """Initialize with data source"""
        self.data_source = data_source
        self.historical_data = {}
        self.load_historical_data()
    
    def load_historical_data(self):
        """Load historical OHLC data from available sources"""
        print("📊 Loading historical data...")
        
        if self.data_source == "parquet":
            self._load_parquet_data()
        elif self.data_source == "json":
            self._load_json_data()
        elif self.data_source == "duckdb":
            self._load_duckdb_data()
        else:
            self._load_sample_data()
    
    def _load_parquet_data(self):
        """Load from parquet files"""
        try:
            import polars as pl
        except ImportError:
            print("❌ Polars not available, falling back to sample data")
            self._load_sample_data()
            return
            
        parquet_dir = project_root / "binary_to_parquet"
        
        if parquet_dir.exists():
            print(f"📁 Found parquet data in {parquet_dir}")
            
            # Load all parquet files
            parquet_files = list(parquet_dir.glob("**/*.parquet"))
            print(f"   📊 Found {len(parquet_files)} parquet files")
            
            if parquet_files:
                # Load and process parquet data
                self._process_parquet_files(parquet_files)
            else:
                print("⚠️  No parquet files found, using sample data")
                self._load_sample_data()
        else:
            print("⚠️  No parquet directory found, using sample data")
            self._load_sample_data()
    
    def _process_parquet_files(self, parquet_files):
        """Process parquet files and extract OHLC data"""
        print("🔄 Processing parquet files...")
        
        # Load first few files to get structure
        sample_files = parquet_files[:5]  # Process first 5 files
        
        for file_path in sample_files:
            try:
                print(f"   📄 Loading {file_path.name}")
                df = pl.read_parquet(file_path)
                
                # Group by instrument and extract OHLC
                if 'instrument' in df.columns and 'last_price' in df.columns:
                    ohlc_data = df.group_by('instrument').agg([
                        pl.col('last_price').first().alias('open'),
                        pl.col('last_price').max().alias('high'),
                        pl.col('last_price').min().alias('low'),
                        pl.col('last_price').last().alias('close'),
                        pl.col('volume').sum().alias('volume')
                    ])
                    
                    # Store in historical_data format
                    for row in ohlc_data.iter_rows(named=True):
                        symbol = row['instrument']
                        if symbol not in self.historical_data:
                            self.historical_data[symbol] = []
                        
                        self.historical_data[symbol].append({
                            'month': len(self.historical_data[symbol]),
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume']
                        })
                        
            except Exception as e:
                print(f"   ❌ Error loading {file_path.name}: {e}")
                continue
        
        print(f"✅ Loaded data for {len(self.historical_data)} symbols")
        
        # If no data loaded, fall back to sample
        if not self.historical_data:
            print("⚠️  No valid data loaded, using sample data")
            self._load_sample_data()
    
    def _load_json_data(self):
        """Load from JSON files"""
        print("📁 Loading JSON data...")
        
        # Look for JSON data in crawlers/raw_data
        json_dirs = [
            project_root / "crawlers" / "raw_data" / "gift_nifty",
            project_root / "crawlers" / "raw_data" / "trading_crawler" / "binary_crawler",
            project_root / "crawlers" / "raw_data" / "zerodha_websocket" / "binary_crawler"
        ]
        
        json_files = []
        for json_dir in json_dirs:
            if json_dir.exists():
                json_files.extend(list(json_dir.glob("*.jsonl")))
        
        if json_files:
            print(f"   📊 Found {len(json_files)} JSON files")
            self._process_json_files(json_files[:10])  # Process first 10 files
        else:
            print("⚠️  No JSON files found, using sample data")
            self._load_sample_data()
    
    def _process_json_files(self, json_files):
        """Process JSON files and extract OHLC data"""
        print("🔄 Processing JSON files...")
        
        for file_path in json_files:
            try:
                print(f"   📄 Loading {file_path.name}")
                
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                
                # Process first 100 lines to get structure
                sample_lines = lines[:100]
                
                for line in sample_lines:
                    try:
                        data = json.loads(line.strip())
                        
                        # Extract symbol and price data
                        symbol = data.get('tradingsymbol', data.get('symbol', 'UNKNOWN'))
                        if symbol == 'UNKNOWN':
                            continue
                            
                        price = data.get('last_price', 0)
                        if price <= 0:
                            continue
                        
                        # Initialize symbol data if not exists
                        if symbol not in self.historical_data:
                            self.historical_data[symbol] = []
                        
                        # Add to historical data (simplified OHLC)
                        if len(self.historical_data[symbol]) == 0:
                            self.historical_data[symbol].append({
                                'month': 0,
                                'open': price,
                                'high': price,
                                'low': price,
                                'close': price,
                                'volume': data.get('volume', 0)
                            })
                        else:
                            # Update current month's data
                            current = self.historical_data[symbol][-1]
                            current['high'] = max(current['high'], price)
                            current['low'] = min(current['low'], price)
                            current['close'] = price
                            current['volume'] += data.get('volume', 0)
                            
                    except json.JSONDecodeError:
                        continue
                        
            except Exception as e:
                print(f"   ❌ Error loading {file_path.name}: {e}")
                continue
        
        print(f"✅ Loaded data for {len(self.historical_data)} symbols")
        
        # If no data loaded, fall back to sample
        if not self.historical_data:
            print("⚠️  No valid data loaded, using sample data")
            self._load_sample_data()
    
    def _load_duckdb_data(self):
        """Load from DuckDB database"""
        try:
            import duckdb
        except ImportError:
            print("❌ DuckDB not available, falling back to sample data")
            self._load_sample_data()
            return
            
        db_path = project_root / "nse_tick_data.duckdb"
        
        if db_path.exists():
            print(f"📁 Found DuckDB database: {db_path}")
            
            conn = duckdb.connect(str(db_path))
            
            # Query for OHLC data
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
            LIMIT 1000
            """
            
            try:
                result = conn.execute(query).fetchall()
                conn.close()
                
                # Process results
                for row in result:
                    symbol, date, low, high, open_price, close, volume = row
                    
                    if symbol not in self.historical_data:
                        self.historical_data[symbol] = []
                    
                    self.historical_data[symbol].append({
                        'month': len(self.historical_data[symbol]),
                        'open': open_price,
                        'high': high,
                        'low': low,
                        'close': close,
                        'volume': volume
                    })
                
                print(f"✅ Loaded data for {len(self.historical_data)} symbols from DuckDB")
                
            except Exception as e:
                print(f"❌ Error querying DuckDB: {e}")
                self._load_sample_data()
        else:
            print("⚠️  No DuckDB database found, using sample data")
            self._load_sample_data()
    
    def _load_sample_data(self):
        """Create sample historical data for testing"""
        print("📊 Creating sample historical data...")
        
        # Sample symbols
        symbols = ['NIFTY', 'BANKNIFTY', 'RELIANCE', 'TCS', 'HDFC', 'INFY']
        
        for symbol in symbols:
            self.historical_data[symbol] = []
            
            # Generate 24 months of data
            base_price = 18000 if 'NIFTY' in symbol else 40000 if 'BANK' in symbol else 2000
            
            for month in range(24):
                # Generate realistic OHLC data
                volatility = 0.15 if 'NIFTY' in symbol else 0.20
                
                # Random walk with trend
                trend = np.random.normal(0, 0.02)  # 2% monthly trend
                noise = np.random.normal(0, volatility)
                
                if month == 0:
                    open_price = base_price
                else:
                    open_price = self.historical_data[symbol][month-1]['close']
                
                # Generate OHLC
                daily_returns = np.random.normal(trend, volatility/30, 30)  # 30 days
                prices = [open_price]
                
                for ret in daily_returns:
                    prices.append(prices[-1] * (1 + ret))
                
                high = max(prices)
                low = min(prices)
                close = prices[-1]
                
                self.historical_data[symbol].append({
                    'month': month,
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': np.random.randint(1000000, 5000000)
                })
        
        print(f"✅ Loaded data for {len(symbols)} symbols, 24 months each")
    
    def _create_sample_data_structure(self):
        """Create sample data structure matching parquet format"""
        self._load_sample_data()
    
    def analyze_iron_condor_performance(self, otm_percent=0.02, credit_percent=0.15, max_loss_percent=0.30):
        """Analyze Iron Condor strategy performance"""
        print(f"\n🎯 Analyzing Iron Condor Strategy")
        print(f"   OTM: {otm_percent:.1%}, Credit: {credit_percent:.1%}, Max Loss: {max_loss_percent:.1%}")
        
        profitable_trades = 0
        total_trades = 0
        total_return = 0
        monthly_returns = []
        
        for symbol in self.historical_data:
            print(f"\n📈 Analyzing {symbol}...")
            
            # Get the number of months available for this symbol
            months_available = len(self.historical_data[symbol])
            months_to_analyze = min(24, months_available)  # Use available months or 24, whichever is smaller
            
            for month in range(months_to_analyze):
                monthly_data = self.historical_data[symbol][month]
                monthly_high = monthly_data.get('high', 0) or 0
                monthly_low = monthly_data.get('low', 0) or 0
                monthly_close = monthly_data.get('close', 0) or 0
                monthly_open = monthly_data.get('open', 0) or 0
                
                # Skip if we don't have valid price data
                if monthly_high <= 0 or monthly_low <= 0 or monthly_close <= 0 or monthly_open <= 0:
                    continue
                
                # Iron Condor Setup
                short_call_strike = monthly_high * (1 - otm_percent)  # 2% OTM from high
                short_put_strike = monthly_low * (1 + otm_percent)    # 2% OTM from low
                long_call_strike = short_call_strike * (1 + otm_percent)
                long_put_strike = short_put_strike * (1 - otm_percent)
                
                # Check if price stayed in profitable range
                if short_put_strike <= monthly_close <= short_call_strike:
                    profitable_trades += 1
                    monthly_return = credit_percent
                    total_return += credit_percent
                else:
                    # Calculate actual loss based on how far price moved
                    if monthly_close > short_call_strike:
                        loss = min(max_loss_percent, (monthly_close - short_call_strike) / short_call_strike)
                    else:  # monthly_close < short_put_strike
                        loss = min(max_loss_percent, (short_put_strike - monthly_close) / short_put_strike)
                    
                    monthly_return = -loss
                    total_return -= loss
                
                monthly_returns.append(monthly_return)
                total_trades += 1
        
        # Calculate metrics
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0
        avg_return = total_return / total_trades if total_trades > 0 else 0
        
        # Risk metrics
        monthly_returns_array = np.array(monthly_returns)
        sharpe_ratio = np.mean(monthly_returns_array) / np.std(monthly_returns_array) if np.std(monthly_returns_array) > 0 else 0
        max_drawdown = self._calculate_max_drawdown(monthly_returns_array)
        
        return {
            'win_rate': win_rate,
            'avg_return': avg_return,
            'total_trades': total_trades,
            'profitable_trades': profitable_trades,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'monthly_returns': monthly_returns
        }
    
    def _calculate_max_drawdown(self, returns):
        """Calculate maximum drawdown"""
        cumulative = np.cumprod(1 + returns)
        running_max = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - running_max) / running_max
        return np.min(drawdown)
    
    def analyze_covered_call_performance(self, otm_percent=0.02, premium_percent=0.05):
        """Analyze Covered Call strategy performance"""
        print(f"\n📞 Analyzing Covered Call Strategy")
        print(f"   OTM: {otm_percent:.1%}, Premium: {premium_percent:.1%}")
        
        profitable_trades = 0
        total_trades = 0
        total_return = 0
        monthly_returns = []
        
        for symbol in self.historical_data:
            print(f"\n📈 Analyzing {symbol}...")
            
            # Get the number of months available for this symbol
            months_available = len(self.historical_data[symbol])
            months_to_analyze = min(24, months_available)  # Use available months or 24, whichever is smaller
            
            for month in range(months_to_analyze):
                monthly_data = self.historical_data[symbol][month]
                monthly_high = monthly_data.get('high', 0) or 0
                monthly_close = monthly_data.get('close', 0) or 0
                monthly_open = monthly_data.get('open', 0) or 0
                
                # Skip if we don't have valid price data
                if monthly_high <= 0 or monthly_close <= 0 or monthly_open <= 0:
                    continue
                
                # Covered Call Setup
                strike_price = monthly_open * (1 + otm_percent)  # 2% OTM from open
                premium_received = monthly_open * premium_percent
                
                # Calculate return
                if monthly_close <= strike_price:
                    # Option expires worthless, keep premium + stock appreciation
                    stock_return = (monthly_close - monthly_open) / monthly_open
                    total_return += stock_return + premium_percent
                    monthly_return = stock_return + premium_percent
                    profitable_trades += 1
                else:
                    # Stock called away at strike
                    stock_return = (strike_price - monthly_open) / monthly_open
                    total_return += stock_return + premium_percent
                    monthly_return = stock_return + premium_percent
                    profitable_trades += 1
                
                monthly_returns.append(monthly_return)
                total_trades += 1
        
        # Calculate metrics
        win_rate = profitable_trades / total_trades if total_trades > 0 else 0
        avg_return = total_return / total_trades if total_trades > 0 else 0
        
        monthly_returns_array = np.array(monthly_returns)
        sharpe_ratio = np.mean(monthly_returns_array) / np.std(monthly_returns_array) if np.std(monthly_returns_array) > 0 else 0
        max_drawdown = self._calculate_max_drawdown(monthly_returns_array)
        
        return {
            'win_rate': win_rate,
            'avg_return': avg_return,
            'total_trades': total_trades,
            'profitable_trades': profitable_trades,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'monthly_returns': monthly_returns
        }
    
    def analyze_buy_and_hold_performance(self):
        """Analyze simple buy and hold performance"""
        print(f"\n📈 Analyzing Buy & Hold Strategy")
        
        total_returns = []
        
        for symbol in self.historical_data:
            if len(self.historical_data[symbol]) >= 2:
                first_price = self.historical_data[symbol][0].get('open', 0) or 0
                last_price = self.historical_data[symbol][-1].get('close', 0) or 0
                
                # Skip if we don't have valid price data
                if first_price <= 0 or last_price <= 0:
                    continue
                    
                total_return = (last_price - first_price) / first_price
                total_returns.append(total_return)
                months_held = len(self.historical_data[symbol])
                print(f"   {symbol}: {total_return:.1%} over {months_held} months")
        
        avg_return = np.mean(total_returns) if total_returns else 0
        annualized_return = (1 + avg_return) ** (12/24) - 1  # Annualize 24-month return
        
        return {
            'total_return': avg_return,
            'annualized_return': annualized_return,
            'symbol_returns': total_returns
        }
    
    def generate_report(self):
        """Generate comprehensive strategy analysis report"""
        print("\n" + "="*80)
        print("📊 PAPER TRADING STRATEGY ANALYSIS REPORT")
        print("="*80)
        
        # Iron Condor Analysis
        iron_condor_results = self.analyze_iron_condor_performance()
        
        print(f"\n🎯 IRON CONDOR STRATEGY RESULTS:")
        print(f"   Win Rate: {iron_condor_results['win_rate']:.1%}")
        print(f"   Average Monthly Return: {iron_condor_results['avg_return']:.1%}")
        print(f"   Total Trades: {iron_condor_results['total_trades']}")
        print(f"   Profitable Trades: {iron_condor_results['profitable_trades']}")
        print(f"   Sharpe Ratio: {iron_condor_results['sharpe_ratio']:.2f}")
        print(f"   Max Drawdown: {iron_condor_results['max_drawdown']:.1%}")
        
        # Covered Call Analysis
        covered_call_results = self.analyze_covered_call_performance()
        
        print(f"\n📞 COVERED CALL STRATEGY RESULTS:")
        print(f"   Win Rate: {covered_call_results['win_rate']:.1%}")
        print(f"   Average Monthly Return: {covered_call_results['avg_return']:.1%}")
        print(f"   Total Trades: {covered_call_results['total_trades']}")
        print(f"   Profitable Trades: {covered_call_results['profitable_trades']}")
        print(f"   Sharpe Ratio: {covered_call_results['sharpe_ratio']:.2f}")
        print(f"   Max Drawdown: {covered_call_results['max_drawdown']:.1%}")
        
        # Buy & Hold Analysis
        buy_hold_results = self.analyze_buy_and_hold_performance()
        
        print(f"\n📈 BUY & HOLD STRATEGY RESULTS:")
        print(f"   Total Return (24 months): {buy_hold_results['total_return']:.1%}")
        print(f"   Annualized Return: {buy_hold_results['annualized_return']:.1%}")
        
        # Strategy Comparison
        print(f"\n🏆 STRATEGY COMPARISON:")
        print(f"   Iron Condor: {iron_condor_results['avg_return']:.1%} monthly return")
        print(f"   Covered Call: {covered_call_results['avg_return']:.1%} monthly return")
        print(f"   Buy & Hold: {buy_hold_results['annualized_return']/12:.1%} monthly return")
        
        # Risk-Adjusted Returns
        print(f"\n⚖️  RISK-ADJUSTED RETURNS (Sharpe Ratio):")
        print(f"   Iron Condor: {iron_condor_results['sharpe_ratio']:.2f}")
        print(f"   Covered Call: {covered_call_results['sharpe_ratio']:.2f}")
        
        return {
            'iron_condor': iron_condor_results,
            'covered_call': covered_call_results,
            'buy_hold': buy_hold_results
        }

def main():
    """Main analysis function"""
    print("🚀 Starting Paper Trading Strategy Analysis")
    print("="*50)
    
    # Try different data sources in order of preference
    data_sources = ["duckdb", "parquet", "json", "sample"]
    
    analyzer = None
    for source in data_sources:
        try:
            print(f"\n🔄 Trying data source: {source}")
            analyzer = StrategyAnalyzer(data_source=source)
            
            # Check if we got real data
            if len(analyzer.historical_data) > 0:
                print(f"✅ Successfully loaded data from {source}")
                break
            else:
                print(f"⚠️  No data loaded from {source}, trying next...")
                
        except Exception as e:
            print(f"❌ Error with {source}: {e}")
            continue
    
    if not analyzer or len(analyzer.historical_data) == 0:
        print("❌ Failed to load any data, using sample data")
        analyzer = StrategyAnalyzer(data_source="sample")
    
    # Generate comprehensive report
    results = analyzer.generate_report()
    
    print(f"\n✅ Analysis Complete!")
    print(f"📊 Analyzed {len(analyzer.historical_data)} symbols")
    
    return results

if __name__ == "__main__":
    main()
