# 🧮 Mathematical Equations Reference
*Complete reference of all mathematical equations implemented in the enhanced pattern detection system*

## Table of Contents
1. [VPIN (Volume-Synchronized Probability of Informed Trading)](#vpin)
2. [Granger Causality](#granger-causality)
3. [DBSCAN Clustering](#dbscan-clustering)
4. [Dynamic Time Warping (DTW)](#dynamic-time-warping)
5. [Fake Order Wall Detection](#fake-order-wall-detection)
6. [Spoofing Detection](#spoofing-detection)
7. [Order Flow Toxicity](#order-flow-toxicity)
8. [Cross-Asset Correlation](#cross-asset-correlation)
9. [Institutional Manipulation Detection](#institutional-manipulation)
10. [Market Maker Pattern Detection](#market-maker-patterns)

---

## VPIN (Volume-Synchronized Probability of Informed Trading)

### Formula
```
VPIN = Σ|V_buy - V_sell| / Σ(V_buy + V_sell)
```

### Implementation
```python
def calculate_vpin(buy_volumes, sell_volumes):
    volume_imbalance = [abs(b - s) for b, s in zip(buy_volumes, sell_volumes)]
    total_volume = sum(buy_volumes) + sum(sell_volumes)
    return sum(volume_imbalance) / total_volume if total_volume > 0 else 0
```

### Toxicity Levels
- **Normal**: VPIN < 0.7
- **High**: 0.7 ≤ VPIN < 0.85
- **Extreme**: VPIN ≥ 0.85

---

## Granger Causality

### Formula
```
Y_t = Σ(α_i × Y_{t-i}) + Σ(β_i × X_{t-i}) + ε_t
```

### Test Statistic
```
F = (RSS_restricted - RSS_unrestricted) / p
    RSS_unrestricted / (T - 2p - 1)
```

### Implementation
```python
def granger_causality_test(series1, series2, max_lag=3):
    from statsmodels.tsa.stattools import grangercausalitytests
    
    data = pd.DataFrame({'series1': series1, 'series2': series2})
    result = grangercausalitytests(data, maxlag=max_lag, verbose=False)
    
    p_values = [result[lag][0]['ssr_ftest'][1] for lag in range(1, max_lag+1)]
    return min(p_values) < 0.05
```

---

## DBSCAN Clustering

### Distance Metric
```
d(x, y) = √(Σ(x_i - y_i)²)
```

### DBSCAN Algorithm
```
1. For each point p:
   - If p is unvisited:
     - Mark p as visited
     - If p has ≥ min_samples neighbors:
       - Create new cluster C
       - Add p to C
       - Add all neighbors to queue Q
       - While Q is not empty:
         - Remove q from Q
         - If q is unvisited:
           - Mark q as visited
           - If q has ≥ min_samples neighbors:
             - Add q's neighbors to Q
           - Add q to C
```

### Implementation
```python
def cluster_patterns(features):
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    dbscan = DBSCAN(eps=0.5, min_samples=3)
    clusters = dbscan.fit_predict(features_scaled)
    
    return clusters
```

---

## Dynamic Time Warping (DTW)

### Formula
```
DTW(X,Y) = min_π Σ((i,j)∈π) |X_i - Y_j|²
```

### Recurrence Relation
```
DTW(i,j) = |X_i - Y_j|² + min(
    DTW(i-1, j),    # insertion
    DTW(i, j-1),    # deletion
    DTW(i-1, j-1)   # match
)
```

### Implementation
```python
def dtw_distance(series1, series2):
    n, m = len(series1), len(series2)
    dtw_matrix = np.full((n+1, m+1), np.inf)
    dtw_matrix[0, 0] = 0
    
    for i in range(1, n+1):
        for j in range(1, m+1):
            cost = abs(series1[i-1] - series2[j-1])
            dtw_matrix[i, j] = cost + min(
                dtw_matrix[i-1, j],    # insertion
                dtw_matrix[i, j-1],    # deletion
                dtw_matrix[i-1, j-1]   # match
            )
    
    return dtw_matrix[n, m]
```

---

## Fake Order Wall Detection

### Large Order Threshold
```
Large_Order_Threshold = Average_Order_Size × 3
```

### Spread Analysis
```
Spread_Percentage = (Ask_Price - Bid_Price) / Bid_Price × 100
```

### Detection Criteria
```
Fake_Wall = (Order_Size > Large_Order_Threshold) AND (Spread_Percentage < 1%)
```

### Implementation
```python
def detect_fake_walls(bid_price, ask_price, bid_qty, ask_qty, avg_qty):
    spread_pct = (ask_price - bid_price) / bid_price * 100
    large_threshold = avg_qty * 3
    
    if bid_qty > large_threshold and spread_pct < 1:
        return "FAKE_BID_WALL"
    elif ask_qty > large_threshold and spread_pct < 1:
        return "FAKE_ASK_WALL"
    
    return None
```

---

## Spoofing Detection

### Order Imbalance Ratio
```
Imbalance_Ratio = (Buy_Quantity - Sell_Quantity) / (Buy_Quantity + Sell_Quantity)
```

### Spoofing Threshold
```
Spoofing_Detected = |Imbalance_Ratio| > 0.8
```

### Implementation
```python
def detect_spoofing(buy_qty, sell_qty, threshold=0.8):
    total_qty = buy_qty + sell_qty
    if total_qty == 0:
        return False
    
    imbalance = (buy_qty - sell_qty) / total_qty
    return abs(imbalance) > threshold
```

---

## Order Flow Toxicity

### Toxicity Score
```
Toxicity_Score = |Imbalance_Ratio| × (Volume / 10000)
```

### Order Flow Imbalance
```
Imbalance_Ratio = (Buy_Quantity - Sell_Quantity) / (Buy_Quantity + Sell_Quantity)
```

### Implementation
```python
def calculate_order_flow_toxicity(buy_qty, sell_qty, volume, threshold=0.7):
    total_qty = buy_qty + sell_qty
    if total_qty == 0:
        return 0
    
    imbalance = (buy_qty - sell_qty) / total_qty
    toxicity = abs(imbalance) * (volume / 10000)
    
    return toxicity > threshold
```

---

## Cross-Asset Correlation

### Pearson Correlation
```
ρ = Σ((X_i - X̄)(Y_i - Ȳ)) / √(Σ(X_i - X̄)² × Σ(Y_i - Ȳ)²)
```

### Rolling Correlation
```
Rolling_Corr(t) = Corr(X[t-w:t], Y[t-w:t])
```

### Implementation
```python
def calculate_rolling_correlation(series1, series2, window=30):
    returns1 = np.diff(np.log(series1))
    returns2 = np.diff(np.log(series2))
    
    rolling_corr = []
    for i in range(window, len(returns1)):
        corr = np.corrcoef(returns1[i-window:i], returns2[i-window:i])[0, 1]
        rolling_corr.append(corr)
    
    return rolling_corr
```

---

## Institutional Manipulation Detection

### Range-Bound Manipulation
```
Range_Percentage = (Max_Price - Min_Price) / Min_Price × 100
Volume_Spike = Max_Volume / Average_Volume

Range_Bound_Manipulation = (Range_Percentage < 2%) AND (Volume_Spike > 2.0)
```

### Volume Manipulation
```
Volume_Manipulation = (Volume_Spike > 3.0) AND (Range_Percentage < 1%)
```

### Implementation
```python
def detect_institutional_manipulation(prices, volumes):
    price_range = (max(prices) - min(prices)) / min(prices) * 100
    avg_volume = np.mean(volumes)
    max_volume = max(volumes)
    volume_spike = max_volume / avg_volume if avg_volume > 0 else 0
    
    if price_range < 2 and volume_spike > 2:
        return "RANGE_BOUND_MANIPULATION"
    elif volume_spike > 3 and price_range < 1:
        return "VOLUME_MANIPULATION"
    
    return None
```

---

## Market Maker Pattern Detection

### Quote Update Frequency
```
Quote_Frequency = Quote_Updates / Total_Periods
```

### Market Maker Detection
```
Market_Maker_Activity = Quote_Frequency > 0.7
```

### Implementation
```python
def detect_market_maker_activity(quote_data):
    quote_updates = 0
    for i in range(1, len(quote_data)):
        if (quote_data[i-1]['bid_price'] != quote_data[i]['bid_price'] or
            quote_data[i-1]['ask_price'] != quote_data[i]['ask_price']):
            quote_updates += 1
    
    frequency = quote_updates / (len(quote_data) - 1)
    return frequency > 0.7
```

---

## Pattern Classification Summary

### Pattern Types by Category

**Volume-Based Patterns:**
- VPIN Toxicity
- Volume Manipulation
- Order Flow Toxicity

**Correlation-Based Patterns:**
- Granger Causality
- Cross-Asset Correlation
- DTW Similarity

**Manipulation Patterns:**
- Fake Order Walls
- Spoofing Detection
- Institutional Manipulation

**Market Microstructure:**
- Market Maker Activity
- Order Book Imbalance
- Pattern Clustering

### Confidence Scoring
```
Confidence = Base_Confidence × Data_Quality × Regime_Adjustment × Time_Adjustment
```

### Expected Move Calculation
```
Expected_Move = Pattern_Strength × Confidence × Market_Volatility
```

---

## Integration Status

### ✅ Implemented
- VPIN Toxicity Detection
- Granger Causality Analysis
- DBSCAN Clustering
- Dynamic Time Warping
- Fake Order Wall Detection
- Spoofing Detection
- Order Flow Toxicity
- Cross-Asset Correlation
- Institutional Manipulation
- Market Maker Patterns

### 🔄 Enhanced
- Pattern Coherence Filtering
- Dynamic Threshold Adjustment
- Time-Based Filtering
- Confidence Scoring

### 📊 Total Pattern Types: 34+
- Basic Patterns: 24
- Enhanced Patterns: 10+
- Mathematical Methods: 10
