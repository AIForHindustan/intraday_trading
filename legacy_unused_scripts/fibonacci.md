# Fibonacci Sequence in Algorithms & Trading

## 🔢 **Core Mathematical Properties**

The Fibonacci sequence is a series where each number is the sum of the two preceding ones:
```
0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987...
```

**Mathematical Formula:**
```
F(n) = F(n-1) + F(n-2)
F(0) = 0
F(1) = 1
```

## 🎯 **Trading Applications**

### **1. Fibonacci Retracement Levels**

Fibonacci retracements are horizontal lines that indicate where support and resistance are likely to occur. They are based on the key numbers identified by mathematician Leonardo Fibonacci in the thirteenth century.

```python
def calculate_fibonacci_retracement(high, low):
    """
    Calculate Fibonacci retracement levels for trading
    
    Args:
        high (float): Recent high price
        low (float): Recent low price
    
    Returns:
        dict: Fibonacci retracement levels
    """
    diff = high - low
    
    levels = {
        '0%': high,
        '23.6%': high - (0.236 * diff),
        '38.2%': high - (0.382 * diff),
        '50%': high - (0.5 * diff),
        '61.8%': high - (0.618 * diff),
        '78.6%': high - (0.786 * diff),
        '100%': low
    }
    
    return levels

# Example usage:
# high_price = 100
# low_price = 80
# fib_levels = calculate_fibonacci_retracement(high_price, low_price)
# print(f"61.8% retracement: {fib_levels['61.8%']}")
```

### **2. Fibonacci Extension Levels**

Fibonacci extensions are used to determine profit targets and potential reversal points.

```python
def calculate_fibonacci_extension(high, low, retracement_low):
    """
    Calculate Fibonacci extension levels for profit targets
    
    Args:
        high (float): Original high price
        low (float): Original low price
        retracement_low (float): Low point of retracement
    
    Returns:
        dict: Fibonacci extension levels
    """
    diff = high - low
    retracement_diff = high - retracement_low
    
    extensions = {
        '127.2%': retracement_low - (0.272 * retracement_diff),
        '161.8%': retracement_low - (0.618 * retracement_diff),
        '200%': retracement_low - retracement_diff,
        '261.8%': retracement_low - (1.618 * retracement_diff)
    }
    
    return extensions
```

### **3. Fibonacci Time Zones**

Fibonacci time zones are vertical lines that help identify potential reversal points based on time.

```python
from datetime import datetime, timedelta

def fibonacci_time_zones(start_date, periods=21):
    """
    Calculate Fibonacci time zones for trend analysis
    
    Args:
        start_date (datetime): Starting date for analysis
        periods (int): Number of periods to analyze
    
    Returns:
        list: Fibonacci time zone dates
    """
    fib_numbers = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89]
    time_zones = []
    
    for fib_num in fib_numbers:
        if fib_num <= periods:
            time_zones.append(start_date + timedelta(days=fib_num))
    
    return time_zones
```

### **4. Trading Signal Generation**

```python
def fibonacci_trading_signals(price_data):
    """
    Generate trading signals based on Fibonacci levels
    
    Args:
        price_data (list): List of price data points
    
    Returns:
        list: Trading signals (BUY/SELL/HOLD)
    """
    signals = []
    
    for i in range(20, len(price_data)):  # Need at least 20 periods
        # Find recent high and low
        recent_high = max(price_data[i-20:i])
        recent_low = min(price_data[i-20:i])
        
        # Calculate Fibonacci levels
        fib_levels = calculate_fibonacci_retracement(recent_high, recent_low)
        current_price = price_data[i]
        
        # Generate signals
        if current_price <= fib_levels['61.8%']:
            signals.append('BUY')  # Strong support level
        elif current_price >= fib_levels['38.2%']:
            signals.append('SELL')  # Resistance level
        else:
            signals.append('HOLD')
    
    return signals
```

## 🚀 **Algorithm Efficiency Applications**

### **1. Fibonacci Search Algorithm**

Fibonacci search is more efficient than binary search in scenarios where division operations are costly.

```python
def fibonacci_search(arr, target):
    """
    Fibonacci search algorithm - more efficient than binary search in some cases
    
    Args:
        arr (list): Sorted array to search
        target: Value to find
    
    Returns:
        int: Index of target, -1 if not found
    """
    n = len(arr)
    
    # Find smallest Fibonacci number >= n
    fib_m2 = 0  # (m-2)'th Fibonacci number
    fib_m1 = 1  # (m-1)'th Fibonacci number
    fib_m = fib_m1 + fib_m2  # m'th Fibonacci number
    
    while fib_m < n:
        fib_m2 = fib_m1
        fib_m1 = fib_m
        fib_m = fib_m1 + fib_m2
    
    offset = -1
    
    while fib_m > 1:
        i = min(offset + fib_m2, n - 1)
        
        if arr[i] < target:
            fib_m = fib_m1
            fib_m1 = fib_m2
            fib_m2 = fib_m - fib_m1
            offset = i
        elif arr[i] > target:
            fib_m = fib_m2
            fib_m1 = fib_m1 - fib_m2
            fib_m2 = fib_m - fib_m1
        else:
            return i
    
    if fib_m1 and arr[offset + 1] == target:
        return offset + 1
    
    return -1
```

### **2. Optimized Fibonacci Calculation**

#### **A. Memoization (Dynamic Programming)**
```python
from functools import lru_cache

@lru_cache(maxsize=None)
def fibonacci_memoized(n):
    """
    Optimized Fibonacci calculation with memoization
    
    Args:
        n (int): Fibonacci number to calculate
    
    Returns:
        int: nth Fibonacci number
    """
    if n < 2:
        return n
    return fibonacci_memoized(n-1) + fibonacci_memoized(n-2)
```

#### **B. Matrix Exponentiation (O(log n))**
```python
def fibonacci_matrix(n):
    """
    Calculate Fibonacci using matrix exponentiation - O(log n)
    
    Args:
        n (int): Fibonacci number to calculate
    
    Returns:
        int: nth Fibonacci number
    """
    if n == 0:
        return 0
    
    def matrix_multiply(a, b):
        """Multiply two 2x2 matrices"""
        return [
            [a[0][0]*b[0][0] + a[0][1]*b[1][0], a[0][0]*b[0][1] + a[0][1]*b[1][1]],
            [a[1][0]*b[0][0] + a[1][1]*b[1][0], a[1][0]*b[0][1] + a[1][1]*b[1][1]]
        ]
    
    def matrix_power(matrix, power):
        """Calculate matrix to the power of n"""
        if power == 1:
            return matrix
        
        if power % 2 == 0:
            half = matrix_power(matrix, power // 2)
            return matrix_multiply(half, half)
        else:
            return matrix_multiply(matrix, matrix_power(matrix, power - 1))
    
    # Fibonacci matrix: [[1,1],[1,0]]
    fib_matrix = [[1, 1], [1, 0]]
    result_matrix = matrix_power(fib_matrix, n)
    
    return result_matrix[0][1]
```

#### **C. Iterative Approach (O(n))**
```python
def fibonacci_iterative(n):
    """
    Calculate Fibonacci using iterative approach - O(n)
    
    Args:
        n (int): Fibonacci number to calculate
    
    Returns:
        int: nth Fibonacci number
    """
    if n < 2:
        return n
    
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    
    return b
```

### **3. Fibonacci Heap (Advanced Data Structure)**

```python
class FibonacciNode:
    """Node for Fibonacci heap"""
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.parent = None
        self.child = None
        self.left = None
        self.right = None
        self.degree = 0
        self.marked = False

class FibonacciHeap:
    """
    Fibonacci heap for efficient priority queue operations
    Used in Dijkstra's algorithm and other graph algorithms
    
    Time Complexity:
    - Insert: O(1) amortized
    - Extract Min: O(log n) amortized
    - Decrease Key: O(1) amortized
    """
    def __init__(self):
        self.min_node = None
        self.count = 0
        self.root_list = []
    
    def insert(self, key, value):
        """Insert with O(1) amortized time"""
        node = FibonacciNode(key, value)
        self._add_to_root_list(node)
        
        if self.min_node is None or key < self.min_node.key:
            self.min_node = node
        
        self.count += 1
    
    def extract_min(self):
        """Extract minimum with O(log n) amortized time"""
        if self.min_node is None:
            return None
        
        min_node = self.min_node
        self._remove_from_root_list(min_node)
        
        if min_node.child:
            self._add_children_to_root_list(min_node.child)
        
        if self.root_list:
            self._consolidate()
        else:
            self.min_node = None
        
        self.count -= 1
        return min_node
    
    def _add_to_root_list(self, node):
        """Add node to root list"""
        if not self.root_list:
            self.root_list = [node]
        else:
            self.root_list.append(node)
    
    def _remove_from_root_list(self, node):
        """Remove node from root list"""
        if node in self.root_list:
            self.root_list.remove(node)
    
    def _add_children_to_root_list(self, child):
        """Add children to root list"""
        current = child
        while True:
            next_child = current.right
            current.parent = None
            self._add_to_root_list(current)
            current = next_child
            if current == child:
                break
    
    def _consolidate(self):
        """Consolidate trees of same degree"""
        degree_table = {}
        
        for node in self.root_list[:]:
            degree = node.degree
            while degree in degree_table:
                other = degree_table[degree]
                if node.key > other.key:
                    node, other = other, node
                self._link(other, node)
                degree_table.pop(degree)
                degree += 1
            degree_table[degree] = node
        
        self.min_node = None
        for node in degree_table.values():
            if self.min_node is None or node.key < self.min_node.key:
                self.min_node = node
    
    def _link(self, child, parent):
        """Link child to parent"""
        self._remove_from_root_list(child)
        child.parent = parent
        parent.degree += 1
        parent.child = child
```

## 📊 **Trading Strategy Implementation**

### **Complete Fibonacci Trading System**

```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class FibonacciTradingSystem:
    """
    Complete Fibonacci-based trading system
    """
    
    def __init__(self, price_data):
        self.price_data = price_data
        self.signals = []
        self.fib_levels = {}
    
    def calculate_fibonacci_levels(self, lookback_period=20):
        """
        Calculate Fibonacci levels for each data point
        
        Args:
            lookback_period (int): Number of periods to look back for high/low
        """
        for i in range(lookback_period, len(self.price_data)):
            # Get recent high and low
            recent_data = self.price_data[i-lookback_period:i]
            high = max(recent_data)
            low = min(recent_data)
            
            # Calculate Fibonacci levels
            fib_levels = calculate_fibonacci_retracement(high, low)
            self.fib_levels[i] = fib_levels
    
    def generate_signals(self):
        """
        Generate trading signals based on Fibonacci levels
        """
        for i in range(20, len(self.price_data)):
            if i not in self.fib_levels:
                self.signals.append('HOLD')
                continue
            
            current_price = self.price_data[i]
            fib_levels = self.fib_levels[i]
            
            # Signal logic
            if current_price <= fib_levels['61.8%']:
                self.signals.append('BUY')
            elif current_price >= fib_levels['38.2%']:
                self.signals.append('SELL')
            else:
                self.signals.append('HOLD')
    
    def calculate_profit_targets(self, entry_price, direction):
        """
        Calculate profit targets using Fibonacci extensions
        
        Args:
            entry_price (float): Entry price
            direction (str): 'LONG' or 'SHORT'
        """
        if direction == 'LONG':
            # For long positions, calculate extensions above entry
            targets = {
                '127.2%': entry_price * 1.272,
                '161.8%': entry_price * 1.618,
                '200%': entry_price * 2.0,
                '261.8%': entry_price * 2.618
            }
        else:
            # For short positions, calculate extensions below entry
            targets = {
                '127.2%': entry_price * 0.728,
                '161.8%': entry_price * 0.382,
                '200%': entry_price * 0.5,
                '261.8%': entry_price * 0.382
            }
        
        return targets
    
    def backtest_strategy(self, initial_capital=10000):
        """
        Backtest the Fibonacci trading strategy
        
        Args:
            initial_capital (float): Starting capital
        
        Returns:
            dict: Backtest results
        """
        capital = initial_capital
        position = 0
        trades = []
        
        for i, signal in enumerate(self.signals):
            if signal == 'BUY' and position == 0:
                # Enter long position
                position = capital / self.price_data[i]
                capital = 0
                trades.append({
                    'type': 'BUY',
                    'price': self.price_data[i],
                    'timestamp': i
                })
            elif signal == 'SELL' and position > 0:
                # Exit long position
                capital = position * self.price_data[i]
                position = 0
                trades.append({
                    'type': 'SELL',
                    'price': self.price_data[i],
                    'timestamp': i
                })
        
        # Calculate final capital
        if position > 0:
            capital = position * self.price_data[-1]
        
        return {
            'final_capital': capital,
            'total_return': (capital - initial_capital) / initial_capital * 100,
            'trades': trades,
            'total_trades': len(trades)
        }
```

## 🎯 **Key Applications in Trading Systems**

### **1. Support/Resistance Levels**
- **23.6%**: Weak support/resistance
- **38.2%**: Moderate support/resistance  
- **50%**: Psychological level
- **61.8%**: Strong support/resistance (Golden Ratio)
- **78.6%**: Very strong support/resistance

### **2. Profit Targets**
- **127.2%**: First profit target
- **161.8%**: Golden ratio extension
- **200%**: Double target
- **261.8%**: Maximum target

### **3. Risk Management**
- Use Fibonacci levels to set stop losses
- Position sizing based on Fibonacci ratios
- Time-based exits using Fibonacci time zones

### **4. Algorithm Efficiency**
- **Fibonacci Search**: O(log n) search algorithm
- **Fibonacci Heap**: O(1) insert, O(log n) extract-min
- **Matrix Exponentiation**: O(log n) Fibonacci calculation

## 📈 **Performance Comparison**

| Method | Time Complexity | Space Complexity | Best Use Case |
|--------|----------------|------------------|---------------|
| Recursive | O(2^n) | O(n) | Not recommended |
| Memoization | O(n) | O(n) | Small to medium n |
| Iterative | O(n) | O(1) | General purpose |
| Matrix | O(log n) | O(1) | Large n values |
| Fibonacci Search | O(log n) | O(1) | Search in sorted arrays |
| Fibonacci Heap | O(1) insert | O(n) | Priority queues |

## 🔧 **Integration with Trading System**

```python
# Example integration with your trading system
def integrate_fibonacci_with_trading_system():
    """
    Example of how to integrate Fibonacci analysis with your trading system
    """
    # Load your price data
    price_data = load_price_data()  # Your existing function
    
    # Initialize Fibonacci trading system
    fib_system = FibonacciTradingSystem(price_data)
    
    # Calculate Fibonacci levels
    fib_system.calculate_fibonacci_levels()
    
    # Generate signals
    fib_system.generate_signals()
    
    # Backtest strategy
    results = fib_system.backtest_strategy()
    
    # Print results
    print(f"Final Capital: ${results['final_capital']:.2f}")
    print(f"Total Return: {results['total_return']:.2f}%")
    print(f"Total Trades: {results['total_trades']}")
    
    return results
```

## 📚 **References**

1. **Fibonacci Sequence**: Mathematical foundation
2. **Golden Ratio**: 1.618 (φ) - key ratio in Fibonacci analysis
3. **Technical Analysis**: Using Fibonacci in trading
4. **Algorithm Design**: Fibonacci in computer science
5. **Data Structures**: Fibonacci heaps and search algorithms

---

**Note**: This document provides a comprehensive guide to using Fibonacci sequences in both algorithmic trading and computer science applications. The implementations are optimized for performance and can be integrated into your existing trading system.
