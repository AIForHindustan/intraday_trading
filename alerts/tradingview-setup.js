// charting/tradingview-setup.js
class ProfessionalChartManager {
    constructor(container) {
        this.chart = LightweightCharts.createChart(container, {
            width: container.clientWidth,
            height: container.clientHeight,
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
            },
            timeScale: {
                borderColor: '#334155',
                timeVisible: true,
                secondsVisible: false,
            },
        });

        this.candlestickSeries = this.chart.addCandlestickSeries();
        this.volumeSeries = this.chart.addHistogramSeries({
            color: '#26a69a',
            priceFormat: {
                type: 'volume',
            },
            priceScaleId: 'volume',
        });

        this.patternMarkers = [];
    }

    addPatternMarker(pattern) {
        const marker = {
            time: pattern.timestamp / 1000,
            position: 'aboveBar',
            color: this.getPatternColor(pattern.type),
            shape: this.getPatternShape(pattern.type),
            text: `${pattern.type} (${(pattern.confidence * 100).toFixed(0)}%)`,
        };

        this.candlestickSeries.setMarkers([...this.patternMarkers, marker]);
        this.patternMarkers.push(marker);
    }

    getPatternColor(patternType) {
        const colors = {
            'kow_signal_straddle': '#8b5cf6',
            'reversal': '#ef4444', 
            'volume_spike': '#10b981',
            'breakout': '#f59e0b'
        };
        return colors[patternType] || '#6b7280';
    }

    getPatternShape(patternType) {
        const shapes = {
            'kow_signal_straddle': 'arrowDown',
            'reversal': 'circle',
            'volume_spike': 'square',
            'breakout': 'arrowUp'
        };
        return shapes[patternType] || 'circle';
    }
}