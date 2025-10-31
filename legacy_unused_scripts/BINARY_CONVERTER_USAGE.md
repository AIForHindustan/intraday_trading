# Binary to Parquet Converter with Token Resolution

## 🎯 Overview

This system converts your 20.29 GB of binary tick data (159 .dat files + 66 .bin files) into optimized Parquet format with proper instrument names using the token resolver.

## 📊 Data Inventory

- **📁 .dat files**: 159 files (13.43 GB) - Processed tick data
- **📁 .bin files**: 66 files (6.86 GB) - Raw tick data from crawlers
- **💾 Total size**: 20.29 GB of historical tick data
- **🔧 Token resolver**: 103,475 instruments loaded

## 🚀 Quick Start

### 1. Convert All Data (Recommended)

```bash
# Convert all binary files and create DuckDB database
python run_binary_converter.py --input-dir crawlers/raw_data/data_mining --output-dir parquet_ticks --create-db --max-workers 6

# Convert raw_ticks directory as well
python run_binary_converter.py --input-dir raw_ticks --output-dir parquet_ticks --create-db --max-workers 6
```

### 2. Convert Specific Files

```bash
# Convert a specific .dat file
python run_binary_converter.py --file crawlers/raw_data/data_mining/binary_data_20251013_165346.dat --output-dir parquet_ticks

# Convert a specific .bin file
python run_binary_converter.py --file raw_ticks/crawler1_binary/binary_crawler/raw_binary/ticks_20251007_1600.bin --output-dir parquet_ticks
```

### 3. Convert with Custom Settings

```bash
# Use more workers for faster processing (M4 has 8+ cores)
python run_binary_converter.py --input-dir crawlers/raw_data/data_mining --output-dir parquet_ticks --max-workers 8 --create-db

# Convert without creating database (just Parquet files)
python run_binary_converter.py --input-dir crawlers/raw_data/data_mining --output-dir parquet_ticks --max-workers 6
```

## 📈 Query and Analysis

### Basic Usage

```python
from query_utils import TickDataAnalyzer

# Initialize analyzer
analyzer = TickDataAnalyzer("nse_tick_data.duckdb")

# Get instrument data
nifty_data = analyzer.get_instrument_data("NSE:NIFTY 50")
print(f"Retrieved {len(nifty_data)} NIFTY50 records")

# Calculate alpha signals
alpha_signals = analyzer.calculate_alpha_signals("NSE:NIFTY 50")
print(f"Generated {len(alpha_signals)} alpha signals")

# Get daily OHLC for backtesting
daily_ohlc = analyzer.get_daily_ohlc(["NSE:NIFTY 50"])
print(f"Retrieved {len(daily_ohlc)} daily OHLC records")

# Close connection
analyzer.close()
```

### Advanced Analysis

```python
# Market depth analysis
depth_analysis = analyzer.get_market_depth_analysis("NSE:NIFTY 50")

# Volume profile analysis
volume_profile = analyzer.get_volume_profile("NSE:NIFTY 50", price_bins=100)

# Correlation analysis
correlation_matrix = analyzer.get_correlation_matrix([
    "NSE:NIFTY 50", "NSE:RELIANCE", "NSE:TCS", "NSE:INFY"
])
```

## 🔧 Key Features

### Token Resolution
- ✅ **103,475 instruments** loaded from token mapping
- ✅ **Instrument names** saved instead of token numbers
- ✅ **Automatic fallback** to token numbers if resolution fails

### Performance Optimizations
- ✅ **Apple M4 optimized** with parallel processing
- ✅ **Memory management** with chunked processing (100K records per chunk)
- ✅ **Zstd compression** for optimal speed/size balance
- ✅ **DuckDB indexing** for fast analytical queries
- ✅ **Polars vectorization** for data manipulation

### Analytical Capabilities
- ✅ **Alpha signal generation** with multiple lookback periods
- ✅ **Volume profile analysis** for price-volume relationships
- ✅ **Market depth analysis** for order book insights
- ✅ **Correlation analysis** for multi-instrument strategies
- ✅ **Daily OHLC aggregation** for backtesting workflows

## 📊 Expected Output

### Parquet Files
- **Format**: Optimized Parquet with Zstd compression
- **Schema**: 40+ columns including OHLC, volume, bid/ask depth
- **Partitioning**: By date and hour for fast queries
- **Size**: ~50-70% smaller than original binary files

### DuckDB Database
- **Tables**: `tick_data` (main table), `daily_ohlc` (aggregated view)
- **Indexes**: Optimized for fast queries by instrument and time
- **Views**: Pre-computed daily OHLC for backtesting

## 🎯 Usage Examples

### Convert and Analyze NIFTY50 Data

```bash
# 1. Convert data
python run_binary_converter.py --input-dir crawlers/raw_data/data_mining --output-dir parquet_ticks --create-db

# 2. Analyze in Python
python -c "
from query_utils import TickDataAnalyzer
analyzer = TickDataAnalyzer('nse_tick_data.duckdb')
nifty_data = analyzer.get_instrument_data('NSE:NIFTY 50')
print(f'NIFTY50 records: {len(nifty_data)}')
analyzer.close()
"
```

### Generate Alpha Signals

```python
from query_utils import generate_alpha_signals

# Generate alpha signals for backtesting
alpha_signals, daily_ohlc = generate_alpha_signals()

# Save for further analysis
alpha_signals.write_parquet("alpha_signals.parquet")
daily_ohlc.write_parquet("daily_ohlc.parquet")
```

## 🔍 Troubleshooting

### Common Issues

1. **Token resolution fails**: System falls back to token numbers automatically
2. **Memory issues**: Reduce `--max-workers` or chunk size in converter
3. **Database errors**: Ensure Parquet files exist before creating database

### Performance Tips

1. **Use SSD storage** for faster I/O
2. **Parallel processing** works best with 6-8 workers on M4
3. **Chunked processing** prevents memory overflow on large files
4. **DuckDB indexes** speed up analytical queries significantly

## 📈 Expected Performance

### Conversion Speed
- **M4 MacBook**: ~2-3 GB/hour conversion speed
- **Memory usage**: ~8-12 GB peak during conversion
- **Parallel processing**: 6-8 workers optimal

### Query Performance
- **Instrument queries**: <100ms for 1M+ records
- **Alpha signal calculation**: <1s for daily data
- **Correlation analysis**: <5s for 10+ instruments

## 🎯 Next Steps

1. **Run conversion** on your binary data
2. **Test queries** with sample instruments
3. **Generate alpha signals** for your trading strategies
4. **Set up backtesting** workflows with daily OHLC data

The system is now ready to process your 20.29 GB of binary tick data into optimized, queryable format with proper instrument names! 🚀
