<!DOCTYPE html>
<html>
<head>
    <title>Professional Trading Dashboard</title>
    <script src="https://unpkg.com/vue@3/dist/vue.global.js"></script>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/tailwindcss/2.2.19/tailwind.min.css" rel="stylesheet">
</head>
<body>
    <div id="app" class="flex h-screen bg-gray-900 text-white">
        <!-- Left Sidebar - Instrument Selection -->
        <div class="w-80 bg-gray-800 p-4">
            <h2 class="text-xl font-bold mb-4">🎯 Trading Dashboard</h2>
            
            <!-- Asset Class Selection -->
            <div class="mb-6">
                <label class="block text-sm font-medium mb-2">Asset Class</label>
                <select v-model="selectedAssetClass" @change="loadInstruments" 
                        class="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2">
                    <option value="eq">Equity Cash</option>
                    <option value="eq_fut">Equity Futures</option>
                    <option value="index_fut">Index Futures</option>
                    <option value="index_opt">Index Options</option>
                </select>
            </div>
            
            <!-- Instrument Selection -->
            <div class="mb-6">
                <label class="block text-sm font-medium mb-2">Instrument</label>
                <select v-model="selectedInstrument" @change="loadChartData"
                        class="w-full bg-gray-700 border border-gray-600 rounded px-3 py-2">
                    <option v-for="inst in instruments" :value="inst.symbol">
                        {{ inst.name }}
                    </option>
                </select>
            </div>
            
            <!-- Options Selector (Conditional) -->
            <div v-if="selectedAssetClass === 'index_opt'" class="mb-6 grid grid-cols-2 gap-2">
                <div>
                    <label class="block text-sm font-medium mb-1">Expiry</label>
                    <select v-model="selectedExpiry" class="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm">
                        <option v-for="expiry in expiries" :value="expiry">{{ expiry }}</option>
                    </select>
                </div>
                <div>
                    <label class="block text-sm font-medium mb-1">Strike</label>
                    <select v-model="selectedStrike" class="w-full bg-gray-700 border border-gray-600 rounded px-2 py-1 text-sm">
                        <option v-for="strike in strikes" :value="strike">{{ strike }}</option>
                    </select>
                </div>
            </div>
            
            <!-- Pattern Filters -->
            <div class="mb-6">
                <h3 class="text-lg font-medium mb-3">Pattern Filters</h3>
                <div class="space-y-2">
                    <label class="flex items-center">
                        <input type="checkbox" v-model="filters.patterns.volumeSpike" class="mr-2">
                        Volume Spike
                    </label>
                    <label class="flex items-center">
                        <input type="checkbox" v-model="filters.patterns.kowStraddle" class="mr-2">
                        KOW Straddle
                    </label>
                    <label class="flex items-center">
                        <input type="checkbox" v-model="filters.patterns.reversal" class="mr-2">
                        Reversal
                    </label>
                    <label class="flex items-center">
                        <input type="checkbox" v-model="filters.patterns.breakout" class="mr-2">
                        Breakout
                    </label>
                </div>
            </div>
            
            <!-- Confidence Filter -->
            <div class="mb-6">
                <label class="block text-sm font-medium mb-2">
                    Min Confidence: {{ filters.confidence }}%
                </label>
                <input type="range" v-model="filters.confidence" min="0" max="100" 
                       class="w-full bg-gray-700 rounded-lg appearance-none cursor-pointer">
            </div>
        </div>

        <!-- Main Chart Area -->
        <div class="flex-1 flex flex-col">
            <!-- Chart Controls -->
            <div class="bg-gray-800 p-4 flex items-center justify-between">
                <div class="flex space-x-2">
                    <button v-for="tf in timeframes" 
                            @click="setTimeframe(tf)"
                            :class="['px-3 py-1 rounded', timeframe === tf ? 'bg-blue-600' : 'bg-gray-700']">
                        {{ tf }}
                    </button>
                </div>
                
                <div class="flex items-center space-x-4">
                    <div class="text-sm">
                        <span class="text-green-400">L: {{ lastPrice }}</span>
                        <span :class="['ml-2', priceChange >= 0 ? 'text-green-400' : 'text-red-400']">
                            {{ priceChange >= 0 ? '+' : '' }}{{ priceChange }}%
                        </span>
                    </div>
                    
                    <div class="text-sm text-gray-400">
                        Latency: {{ latency }}ms
                    </div>
                </div>
            </div>
            
            <!-- Chart Container -->
            <div class="flex-1 relative">
                <div id="chart" ref="chartContainer" class="absolute inset-0"></div>
            </div>
            
            <!-- Alerts Panel -->
            <div class="h-64 bg-gray-800 border-t border-gray-700">
                <div class="p-4">
                    <h3 class="text-lg font-medium mb-2">Live Alerts</h3>
                    <div class="overflow-y-auto h-48">
                        <div v-for="alert in filteredAlerts" 
                             :class="['p-2 mb-2 rounded border-l-4', getAlertBorderClass(alert)]">
                            <div class="flex justify-between items-center">
                                <span class="font-medium">{{ alert.symbol }}</span>
                                <span class="text-sm bg-blue-600 px-2 py-1 rounded">
                                    {{ (alert.confidence * 100).toFixed(1) }}%
                                </span>
                            </div>
                            <div class="text-sm text-gray-300">{{ alert.pattern }}</div>
                            <div class="text-sm">Price: {{ alert.price }} | Volume: {{ alert.volume_ratio }}x</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        const { createApp, ref, computed, onMounted } = Vue;
        
        createApp({
            setup() {
                // Reactive state
                const selectedAssetClass = ref('index_fut');
                const selectedInstrument = ref('NIFTY');
                const timeframe = ref('5m');
                const lastPrice = ref(0);
                const priceChange = ref(0);
                const latency = ref(0);
                
                const filters = ref({
                    patterns: {
                        volumeSpike: true,
                        kowStraddle: true,
                        reversal: true,
                        breakout: false
                    },
                    confidence: 80
                });
                
                const instruments = ref([]);
                const chartData = ref([]);
                const liveAlerts = ref([]);
                
                // Computed properties
                const filteredAlerts = computed(() => {
                    return liveAlerts.value.filter(alert => {
                        // Pattern filter
                        const patternMatch = filters.value.patterns[alert.pattern] || false;
                        
                        // Confidence filter
                        const confidenceMatch = (alert.confidence * 100) >= filters.value.confidence;
                        
                        return patternMatch && confidenceMatch;
                    });
                });
                
                // Methods
                const loadInstruments = async () => {
                    const response = await fetch(`/api/instruments/${selectedAssetClass.value}`);
                    instruments.value = await response.json();
                    if (instruments.value.length > 0) {
                        selectedInstrument.value = instruments.value[0].symbol;
                    }
                };
                
                const loadChartData = async () => {
                    const response = await fetch(`/api/chart/${selectedInstrument.value}?timeframe=${timeframe.value}`);
                    const data = await response.json();
                    chartData.value = data;
                    initializeChart(data);
                };
                
                const setTimeframe = (tf) => {
                    timeframe.value = tf;
                    loadChartData();
                };
                
                const initializeChart = (data) => {
                    // Initialize Lightweight Charts or TradingView
                    const chart = LightweightCharts.createChart('chart', {
                        width: document.getElementById('chart').clientWidth,
                        height: document.getElementById('chart').clientHeight,
                        layout: {
                            backgroundColor: '#1e293b',
                            textColor: '#d1d5db',
                        },
                        grid: {
                            vertLines: { color: '#334155' },
                            horzLines: { color: '#334155' },
                        },
                    });
                    
                    const candlestickSeries = chart.addCandlestickSeries();
                    candlestickSeries.setData(data.ohlc);
                    
                    // Add pattern markers
                    data.patterns.forEach(pattern => {
                        // Add markers for patterns
                    });
                };
                
                const getAlertBorderClass = (alert) => {
                    const confidence = alert.confidence;
                    if (confidence >= 0.9) return 'border-green-500 bg-green-900/20';
                    if (confidence >= 0.8) return 'border-yellow-500 bg-yellow-900/20';
                    return 'border-red-500 bg-red-900/20';
                };
                
                // WebSocket for real-time data
                const setupWebSocket = () => {
                    const ws = new WebSocket(`ws://localhost:8000/ws/realtime?symbols=${selectedInstrument.value}`);
                    
                    ws.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        const receivedTime = Date.now();
                        const latency = receivedTime - data.timestamp;
                        
                        if (data.type === 'tick') {
                            lastPrice.value = data.data.last_price;
                            // Update chart with new tick
                        } else if (data.type === 'alert') {
                            liveAlerts.value.unshift(data.data);
                            // Keep only last 50 alerts
                            if (liveAlerts.value.length > 50) {
                                liveAlerts.value.pop();
                            }
                        }
                    };
                };
                
                onMounted(() => {
                    loadInstruments();
                    setupWebSocket();
                });
                
                return {
                    selectedAssetClass,
                    selectedInstrument,
                    timeframe,
                    lastPrice,
                    priceChange,
                    latency,
                    filters,
                    instruments,
                    chartData,
                    liveAlerts,
                    filteredAlerts,
                    loadInstruments,
                    loadChartData,
                    setTimeframe,
                    getAlertBorderClass,
                    timeframes: ['1m', '5m', '15m', '1h', '4h', '1d']
                };
            }
        }).mount('#app');
    </script>
</body>
</html>