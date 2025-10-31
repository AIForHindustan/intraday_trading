# Bayesian Statistical Methods for Redis Data Analysis

## Overview

This guide explains how to use Bayesian statistical methods with the cumulative volume and price data stored in Redis buckets from October 3rd, 2024. The implementation provides enhanced uncertainty quantification and probabilistic trading indicators.

## Key Components

### 1. Redis Data Structure

The Redis data is organized in the following structure:

```
Redis DB 5 (cumulative_volume):
├── session:SYMBOL:2024-10-03
│   ├── cumulative_volume: Total daily volume
│   ├── first_price: Opening price
│   ├── last_price: Current price
│   ├── high_price: Day high
│   ├── low_price: Day low
│   └── time_buckets: Hourly/minute buckets
│       ├── bucket:SYMBOL:2024-10-03:HOUR:MINUTE
│       │   ├── open, high, low, close
│       │   ├── total_volume
│       │   ├── count
│       │   └── order_book_snapshots
```

### 2. Bayesian Statistical Framework

#### Prior Distributions

```python
# VWAP Prior: μ ~ Normal(μ₀, τ₀⁻¹)
vwap_prior_mean = 0.0
vwap_prior_precision = 1.0

# RSI Prior: Beta(α₀, β₀) - symmetric around 0.5
rsi_prior_alpha = 2.0
rsi_prior_beta = 2.0

# Volume Prior: μ ~ Normal(1.0, 1.0) - normal volume ratio
volume_prior_mean = 1.0
volume_prior_precision = 1.0
```

#### Likelihood Functions

```python
# Price Model: price ~ Normal(μ, σ²)
# Volume Model: volume_ratio ~ Normal(μ, σ²)
# RSI Model: rsi ~ Beta(α, β) transformed to [0, 100]
```

#### Posterior Distributions

The framework uses conjugate priors for analytical solutions:

- **Normal-Normal**: For price and volume parameters
- **Beta-Beta**: For RSI and probability parameters
- **Normal-Gamma**: For mean and variance parameters

## Usage Examples

### 1. Extract October 3rd Data

```python
from bayesian_redis_analysis import RedisDataExtractor

# Initialize extractor
extractor = RedisDataExtractor()

# Get available symbols
symbols = extractor.get_available_symbols()
print(f"Found {len(symbols)} symbols with data")

# Extract time series for a symbol
df = extractor.create_time_series("RELIANCE")
print(f"Extracted {len(df)} data points")
```

### 2. Run Bayesian Analysis

```python
from bayesian_redis_analysis import BayesianVolumePriceAnalyzer, BayesianParameters

# Initialize analyzer
analyzer = BayesianVolumePriceAnalyzer(BayesianParameters())

# Fit volume-price model
model_results = analyzer.fit_volume_price_model(df)

# Calculate correlation
correlation_results = analyzer.calculate_volume_price_correlation(df)

# Detect anomalies
anomaly_results = analyzer.detect_volume_anomalies(df)
```

### 3. Use Bayesian Indicators

```python
from scanner.bayesian_indicators import BayesianIndicatorManager, BayesianIndicatorConfig

# Initialize Bayesian indicator manager
config = BayesianIndicatorConfig()
manager = BayesianIndicatorManager(config)

# Update indicators with new data
result = manager.update_indicators("RELIANCE", price=100.0, volume=1000, avg_volume=1000)

# Get trading signals
signal = manager.get_trading_signals("RELIANCE")
print(f"Signal: {signal['signal']}, Confidence: {signal['confidence']:.2f}")
```

### 4. Integration with Existing Framework

```python
from bayesian_redis_integration import BayesianRedisIntegration

# Initialize integration
integration = BayesianRedisIntegration()

# Run historical analysis
historical_results = integration.run_historical_analysis()

# Integrate with tick processor
integration.integrate_with_tick_processor()

# Generate enhanced signals
signal_result = integration.generate_enhanced_signals("RELIANCE", tick_data)
```

## Bayesian Methods Explained

### 1. Bayesian VWAP

The Bayesian VWAP provides uncertainty quantification around the standard VWAP:

```python
# Model: price ~ Normal(μ, σ²) where μ is true VWAP
# Prior: μ ~ Normal(μ₀, τ₀⁻¹), σ² ~ InverseGamma(α₀, β₀)
# Posterior: μ ~ Normal(μₙ, τₙ⁻¹), σ² ~ InverseGamma(αₙ, βₙ)

# Where:
# τₙ = τ₀ + n
# μₙ = (τ₀μ₀ + nȳ) / τₙ
# αₙ = α₀ + n/2
# βₙ = β₀ + 0.5 * (n*s² + τ₀n(ȳ-μ₀)²/τₙ)
```

**Benefits:**
- Provides credible intervals for VWAP
- Quantifies uncertainty in price levels
- Enables probabilistic trading decisions

### 2. Bayesian RSI

The Bayesian RSI models the RSI as a Beta distribution:

```python
# Model: RSI ~ Beta(α, β) transformed to [0, 100]
# Prior: α₀ = 2, β₀ = 2 (symmetric around 50)
# Posterior: αₙ = α₀ + successes, βₙ = β₀ + failures
```

**Benefits:**
- Accounts for uncertainty in RSI values
- Provides probabilistic overbought/oversold signals
- More robust than point estimates

### 3. Volume Anomaly Detection

Uses hierarchical Bayesian models for anomaly detection:

```python
# Model: volume_ratio ~ Normal(μ, σ²)
# Prior: μ ~ Normal(1.0, 1.0), σ² ~ InverseGamma(1, 1)
# Anomaly Score: -log P(ratio | posterior)
```

**Benefits:**
- Detects unusual volume patterns
- Provides anomaly probability scores
- Adapts to changing market conditions

### 4. Volume-Price Relationship

Models the correlation between volume and price movements:

```python
# Model: correlation ~ Fisher's z transformation
# Prior: z ~ Normal(0, 1)
# Posterior: z ~ Normal(z_sample, 1/√(n-3))
```

**Benefits:**
- Quantifies volume-price relationships
- Provides uncertainty in correlations
- Enables dynamic relationship modeling

## Key Features

### 1. Uncertainty Quantification

All indicators provide:
- **Point estimates**: Best guess values
- **Uncertainty measures**: Standard deviations
- **Credible intervals**: Probability ranges
- **Confidence levels**: Statistical confidence

### 2. Probabilistic Trading Signals

Trading signals include:
- **Signal type**: BUY, SELL, HOLD, ALERT
- **Confidence score**: 0.0 to 1.0
- **Reasoning**: Explanation of signal
- **Uncertainty metrics**: Risk assessment

### 3. Anomaly Detection

Volume anomaly detection provides:
- **Anomaly flag**: Boolean indicator
- **Anomaly score**: Numerical severity
- **Probability**: Statistical probability
- **Context**: Market conditions

## Performance Considerations

### 1. Computational Efficiency

- Uses conjugate priors for analytical solutions
- Avoids expensive MCMC sampling where possible
- Caches results for repeated calculations
- Limits history to prevent memory bloat

### 2. Memory Management

- Keeps only recent data (100 observations)
- Uses efficient data structures
- Implements proper cleanup
- Monitors memory usage

### 3. Real-time Performance

- Optimized for streaming data
- Minimal computational overhead
- Fast convergence
- Scalable architecture

## Integration Points

### 1. Redis Data Access

```python
# Access cumulative data
cum_data = redis_client.get_cumulative_data(symbol)

# Access time buckets
buckets = redis_client.get_time_buckets(symbol, session_date)

# Access session data
session_data = redis_client.get_session_data(symbol, session_date)
```

### 2. Indicator Framework

```python
# Enhanced tick processing
indicators = tick_processor.process_tick(symbol, tick_data)

# Bayesian indicators included
bayesian_vwap = indicators.get('bayesian_vwap')
vwap_uncertainty = indicators.get('vwap_uncertainty')
bayesian_signal = indicators.get('bayesian_signal')
```

### 3. Alert System

```python
# Generate alerts based on Bayesian indicators
if indicators.get('volume_anomaly'):
    alert_manager.create_alert('VOLUME_ANOMALY', symbol, indicators)

if indicators.get('bayesian_signal') == 'ALERT':
    alert_manager.create_alert('BAYESIAN_ALERT', symbol, indicators)
```

## Best Practices

### 1. Data Quality

- Validate Redis data before analysis
- Handle missing values appropriately
- Check for data consistency
- Monitor data freshness

### 2. Parameter Tuning

- Adjust prior parameters based on market conditions
- Calibrate uncertainty thresholds
- Optimize for specific trading strategies
- Backtest parameter choices

### 3. Risk Management

- Use uncertainty measures for position sizing
- Implement stop-losses based on credible intervals
- Monitor model performance
- Update priors based on new information

### 4. Monitoring

- Track indicator performance
- Monitor uncertainty levels
- Alert on model degradation
- Maintain audit trails

## Troubleshooting

### 1. Common Issues

**No data found:**
- Check Redis connection
- Verify date format (YYYY-MM-DD)
- Ensure data exists for target date

**High uncertainty:**
- Increase sample size
- Adjust prior parameters
- Check data quality

**Poor performance:**
- Optimize parameters
- Increase computational resources
- Check for data issues

### 2. Debugging

```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Check Redis data
redis_client.ping()

# Verify data structure
session_data = redis_client.get_cumulative_data(symbol)
print(json.dumps(session_data, indent=2))
```

## Future Enhancements

### 1. Advanced Models

- Dynamic linear models
- State space models
- Hierarchical models
- Machine learning integration

### 2. Real-time Updates

- Streaming Bayesian updates
- Online learning
- Adaptive priors
- Continuous calibration

### 3. Visualization

- Uncertainty plots
- Credible interval charts
- Anomaly visualizations
- Performance dashboards

## Conclusion

The Bayesian statistical framework provides a powerful approach to analyzing Redis data with proper uncertainty quantification. By integrating these methods with the existing indicator framework, traders can make more informed decisions with better risk assessment.

The implementation is designed to be:
- **Statistically sound**: Uses proper Bayesian methods
- **Computationally efficient**: Optimized for real-time use
- **Practically useful**: Provides actionable insights
- **Extensible**: Easy to add new methods

For questions or issues, refer to the code documentation or contact the development team.
