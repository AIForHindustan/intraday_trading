// charting/professional-chart.js

class ProfessionalTradingChart {
    constructor(containerId, symbol) {
        this.container = document.getElementById(containerId);
        this.symbol = symbol;
        this.chart = null;
        this.indicators = new Map();
        this.patternMarkers = [];
        
        this.initChart();
    }
    
    initChart() {
        this.chart = LightweightCharts.createChart(this.container, {
            width: this.container.clientWidth,
            height: 500,
            layout: {
                background: { type: 'solid', color: '#0f172a' },
                textColor: '#cbd5e1',
            },
            grid: {
                vertLines: { color: '#1e293b' },
                horzLines: { color: '#1e293b' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#334155',
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.1,
                },
            },
            timeScale: {
                borderColor: '#334155',
                timeVisible: true,
                secondsVisible: false,
                barSpacing: 6,
                minBarSpacing: 1,
            },
            handleScroll: {
                mouseWheel: true,
                pressedMouseMove: true,
            },
            handleScale: {
                axisPressedMouseMove: true,
                mouseWheel: true,
                pinch: true,
            },
        });

        // Candlestick series
        this.candlestickSeries = this.chart.addCandlestickSeries({
            upColor: '#10b981',
            downColor: '#ef4444',
            borderVisible: false,
            wickUpColor: '#10b981',
            wickDownColor: '#ef4444',
        });

        // Volume series
        this.volumeSeries = this.chart.addHistogramSeries({
            color: '#26a69a',
            priceFormat: {
                type: 'volume',
            },
            priceScaleId: 'volume',
            scaleMargins: {
                top: 0.8,
                bottom: 0,
            },
        });

        this.setupRealTimeUpdates();
    }
    
    setupRealTimeUpdates() {
        // Connect to WebSocket for real-time data
        this.ws = new WebSocket(`ws://localhost:8000/ws/professional/${this.symbol}`);
        
        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.updateChart(data);
            this.updateIndicators(data.technical_indicators);
            this.updatePatternMarkers(data.active_patterns);
        };
    }
    
    updateChart(data) {
        // Update candlestick data
        const ohlc = data.ohlc_data.map(item => ({
            time: item.timestamp / 1000,
            open: item.open,
            high: item.high, 
            low: item.low,
            close: item.close,
        }));
        
        this.candlestickSeries.setData(ohlc);
        
        // Update volume
        const volumeData = data.ohlc_data.map(item => ({
            time: item.timestamp / 1000,
            value: item.volume,
            color: item.close >= item.open ? '#10b981' : '#ef4444',
        }));
        
        this.volumeSeries.setData(volumeData);
    }
    
    addIndicator(indicatorName, data, options = {}) {
        const series = this.chart.addLineSeries({
            color: options.color || '#f59e0b',
            lineWidth: 1,
            priceScaleId: options.priceScaleId || 'right',
        });
        
        series.setData(data);
        this.indicators.set(indicatorName, series);
    }
    
    updateIndicators(indicators) {
        // Update existing indicators
        for (const [name, indicator] of Object.entries(indicators)) {
            if (this.indicators.has(name)) {
                const series = this.indicators.get(name);
                const data = indicator.data.map(item => ({
                    time: item.timestamp / 1000,
                    value: item.value
                }));
                series.setData(data);
            } else {
                // Add new indicator
                this.addIndicator(name, indicator.data, {
                    color: this.getIndicatorColor(name),
                    priceScaleId: indicator.priceScaleId || 'right'
                });
            }
        }
    }
    
    updatePatternMarkers(patterns) {
        const markers = patterns.map(pattern => ({
            time: pattern.timestamp / 1000,
            position: pattern.direction === 'long' ? 'belowBar' : 'aboveBar',
            color: this.getPatternColor(pattern.type),
            shape: pattern.direction === 'long' ? 'arrowUp' : 'arrowDown',
            text: `${pattern.type} (${(pattern.confidence * 100).toFixed(0)}%)`,
        }));
        
        this.patternMarkers = [...this.patternMarkers, ...markers];
        this.candlestickSeries.setMarkers(this.patternMarkers);
    }
    
    getPatternColor(patternType) {
        const colors = {
            'kow_signal_straddle': '#8b5cf6',
            'reversal': '#ef4444',
            'volume_spike': '#10b981',
            'breakout': '#f59e0b',
            'ict_liquidity_pools': '#06b6d4',
            'ict_fvg': '#ec4899',
            'ict_order_block': '#f97316'
        };
        return colors[patternType] || '#6b7280';
    }
    
    getIndicatorColor(indicatorName) {
        const colors = {
            'rsi': '#f59e0b',
            'ema20': '#3b82f6',
            'ema50': '#8b5cf6',
            'vwap': '#10b981',
            'macd': '#ef4444',
            'bollinger_upper': '#6b7280',
            'bollinger_lower': '#6b7280'
        };
        return colors[indicatorName] || '#f59e0b';
    }
    
    setTimeframe(timeframe) {
        // This would typically fetch new data based on timeframe
        console.log(`Switching to ${timeframe} timeframe`);
    }
    
    destroy() {
        if (this.chart) {
            this.chart.remove();
        }
        if (this.ws) {
            this.ws.close();
        }
    }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ProfessionalTradingChart;
} else {
    window.ProfessionalTradingChart = ProfessionalTradingChart;
}
