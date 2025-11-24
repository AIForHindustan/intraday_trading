import { useEffect, useRef, useState } from 'react';
import { createChart, IChartApi, UTCTimestamp } from 'lightweight-charts';
import { subscribeChart } from '../services/socket';
import { Box, Typography } from '@mui/material';

type Bar = { timestamp: string | number; open: number; high: number; low: number; close: number; volume?: number };
type LinePoint = { time: UTCTimestamp; value: number };

interface Props {
  ohlc: Bar[];
  ema?: Record<string, number[]>;
  vwap?: number[];
  height?: number;
  symbol?: string; // Symbol for real-time updates
  enableRealtime?: boolean; // Enable real-time updates (default: true if symbol provided)
  showVolume?: boolean; // Show volume chart below price chart
  showLegend?: boolean; // Show legend for indicators
}

export default function PriceChart({ ohlc, ema, vwap, height = 380, symbol, enableRealtime = true, showVolume = true, showLegend = true }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<any>(null);
  const lineSeriesRef = useRef<Map<string, any>>(new Map());
  
  // State for merged historical + real-time data
  const [mergedOhlc, setMergedOhlc] = useState<Bar[]>(ohlc);
  const [mergedEma, setMergedEma] = useState<Record<string, number[]>>(ema || {});
  const [mergedVwap, setMergedVwap] = useState<number[]>(vwap || []);

  // Update merged data when props change
  useEffect(() => {
    if (ohlc && ohlc.length > 0) {
      setMergedOhlc(ohlc);
      setMergedEma(ema || {});
      setMergedVwap(vwap || []);
    }
  }, [ohlc, ema, vwap]);

  // Initialize chart
  useEffect(() => {
    if (!ref.current || !mergedOhlc || mergedOhlc.length === 0) {
      console.log('📊 PriceChart: Skipping init - no data', { 
        hasRef: !!ref.current, 
        dataLength: mergedOhlc?.length || 0 
      });
      return;
    }
    
    console.log('📊 PriceChart: Initializing chart with', mergedOhlc.length, 'bars');
    console.log('📊 PriceChart: Sample data', mergedOhlc.slice(0, 2));
    
    // Clean up existing chart safely
    if (chartRef.current) {
      try {
        chartRef.current.remove();
      } catch (e) {
        // Chart already disposed, ignore
        console.warn('Chart already disposed during cleanup');
      }
      chartRef.current = null;
      candleSeriesRef.current = null;
      lineSeriesRef.current.clear();
    }

    // Calculate height for price chart (80% if volume shown, 100% otherwise)
    const priceChartHeight = showVolume ? Math.floor(height * 0.75) : height;
    
    // Calculate time span to determine if we need to show dates
    let timeSpanDays = 0;
    if (mergedOhlc && mergedOhlc.length > 0) {
      const firstBar = mergedOhlc[0];
      const lastBar = mergedOhlc[mergedOhlc.length - 1];
      
      let firstTs: number;
      if (typeof firstBar.timestamp === 'number') {
        firstTs = String(firstBar.timestamp).length <= 10 ? firstBar.timestamp : firstBar.timestamp / 1000;
      } else {
        firstTs = Date.parse(firstBar.timestamp as string) / 1000;
      }
      
      let lastTs: number;
      if (typeof lastBar.timestamp === 'number') {
        lastTs = String(lastBar.timestamp).length <= 10 ? lastBar.timestamp : lastBar.timestamp / 1000;
      } else {
        lastTs = Date.parse(lastBar.timestamp as string) / 1000;
      }
      
      timeSpanDays = (lastTs - firstTs) / (24 * 60 * 60);
    }
    
    const chart = createChart(ref.current, { 
      height: priceChartHeight,
      width: ref.current.clientWidth,
      layout: { 
        background: { color: 'transparent' },
        textColor: '#333',
      }, 
      rightPriceScale: { 
        borderVisible: true,
        scaleMargins: {
          top: 0.1,
          bottom: 0.1,
        },
      },
      leftPriceScale: {
        visible: false, // Hide left scale, use right only
      }, 
      timeScale: { 
        borderVisible: true,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        barSpacing: 3,
        fixLeftEdge: false,
        fixRightEdge: false,
        tickMarkFormatter: (time: number, tickMarkType: any, locale: string) => {
          // Convert UTC timestamp (in seconds) to IST (UTC+5:30)
          // time is already in seconds since epoch (UTC)
          const date = new Date(time * 1000);
          const istOffset = 5.5 * 60 * 60 * 1000; // IST offset in milliseconds
          const istDate = new Date(date.getTime() + istOffset);
          
          // Format based on time span (calculated above)
          if (timeSpanDays > 1) {
            // Multi-day: Show date and time for better clarity
            const day = istDate.getUTCDate().toString().padStart(2, '0');
            const month = (istDate.getUTCMonth() + 1).toString().padStart(2, '0');
            const hours = istDate.getUTCHours().toString().padStart(2, '0');
            const minutes = istDate.getUTCMinutes().toString().padStart(2, '0');
            
            // For major ticks (type 0 = Time, type 1 = Date), show date + time
            // For minor ticks, show just time to avoid clutter
            if (tickMarkType === 0 || tickMarkType === 1) {
              return `${day}/${month} ${hours}:${minutes}`;
            } else {
              return `${hours}:${minutes}`;
            }
          } else {
            // Single day: Show time only
            const hours = istDate.getUTCHours().toString().padStart(2, '0');
            const minutes = istDate.getUTCMinutes().toString().padStart(2, '0');
            return `${hours}:${minutes}`;
          }
        },
      },
      localization: {
        timeFormatter: (time: number) => {
          // Convert UTC timestamp (in seconds) to IST (UTC+5:30) for crosshair tooltip
          const date = new Date(time * 1000);
          const istOffset = 5.5 * 60 * 60 * 1000; // IST offset in milliseconds
          const istDate = new Date(date.getTime() + istOffset);
          
          // Format as HH:MM:SS in IST
          const hours = istDate.getUTCHours().toString().padStart(2, '0');
          const minutes = istDate.getUTCMinutes().toString().padStart(2, '0');
          const seconds = istDate.getUTCSeconds().toString().padStart(2, '0');
          return `${hours}:${minutes}:${seconds}`;
        },
      },
      grid: {
        vertLines: { visible: true },
        horzLines: { visible: true },
      },
      crosshair: {
        mode: 0, // Normal
      },
    });
    
    const candle = chart.addCandlestickSeries({
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderVisible: false,
      wickUpColor: '#26a69a',
      wickDownColor: '#ef5350',
    });
    candleSeriesRef.current = candle;
    
    // Add volume series if volume data is available and showVolume is true
    let volumeSeries: any = null;
    if (showVolume && mergedOhlc.length > 0 && mergedOhlc[0].volume !== undefined) {
      volumeSeries = chart.addHistogramSeries({
        color: '#26a69a',
        priceFormat: {
          type: 'volume',
        },
        priceScaleId: 'volume',
      });
      
      // Set volume data
      const volumeData = mergedOhlc.map(b => {
        let timestamp: number;
        if (typeof b.timestamp === 'number') {
          timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
        } else if (b.timestamp) {
          timestamp = Date.parse(b.timestamp as string) / 1000;
        } else {
          timestamp = Date.now() / 1000;
        }
        const volume = b.volume || 0;
        const color = (b.close || 0) >= (b.open || 0) ? '#26a69a' : '#ef5350';
        return {
          time: timestamp as UTCTimestamp,
          value: volume,
          color: color,
        };
      });
      volumeSeries.setData(volumeData);
      
      // Create separate price scale for volume
      chart.priceScale('volume').applyOptions({
        scaleMargins: {
          top: 0.8,
          bottom: 0,
        },
      });
    }
    
    const convertToChartData = (bars: Bar[]) => {
      const converted = bars.map(b => {
        // Handle timestamp - convert to seconds if needed
        let timestamp: number;
        if (typeof b.timestamp === 'number') {
          timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
        } else if (b.timestamp) {
          timestamp = Date.parse(b.timestamp as string) / 1000;
        } else {
          timestamp = Date.now() / 1000;
        }
        
        // Validate price values
        const open = Number(b.open) || 0;
        const high = Number(b.high) || 0;
        const low = Number(b.low) || 0;
        const close = Number(b.close) || 0;
        
        if (!open && !high && !low && !close) {
          console.warn('📊 PriceChart: Invalid bar data', b);
          return null;
        }
        
        return {
          time: timestamp as UTCTimestamp,
          open, 
          high, 
          low, 
          close
        };
      }).filter(item => item !== null);
      
      console.log('📊 PriceChart: Converted', converted.length, 'bars');
      if (converted.length > 0) {
        console.log('📊 PriceChart: Price range', {
          min: Math.min(...converted.map(b => b.low)),
          max: Math.max(...converted.map(b => b.high)),
        });
      }
      
      return converted;
    };
    
    const chartData = convertToChartData(mergedOhlc);
    if (chartData.length > 0) {
      candle.setData(chartData);
      console.log('📊 PriceChart: Data set successfully');
    } else {
      console.warn('📊 PriceChart: No valid data to display');
    }

    const addLine = (key: string, values?: number[], color?: string) => {
      if (!values || values.length === 0) return;
      let line = lineSeriesRef.current.get(key);
      if (!line) {
        line = chart.addLineSeries({ color, lineWidth: 2 });
        lineSeriesRef.current.set(key, line);
      }
      const points: LinePoint[] = mergedOhlc.map((b, i) => {
        // Handle timestamp - convert to seconds if needed
        let timestamp: number;
        if (typeof b.timestamp === 'number') {
          timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
        } else if (b.timestamp) {
          timestamp = Date.parse(b.timestamp as string) / 1000;
        } else {
          timestamp = Date.now() / 1000;
        }
        return {
          time: timestamp as UTCTimestamp,
          value: values[i] || values[values.length - 1] || 0 // Use last value if array is shorter
        };
      }).filter(p => Number.isFinite(p.value));
      line.setData(points);
    };

    if (mergedEma) {
      if (mergedEma['ema_20']) addLine('ema_20', mergedEma['ema_20'], '#7cb342');
      if (mergedEma['ema_50']) addLine('ema_50', mergedEma['ema_50'], '#42a5f5');
      if (mergedEma['ema_100']) addLine('ema_100', mergedEma['ema_100'], '#ab47bc');
      if (mergedEma['ema_200']) addLine('ema_200', mergedEma['ema_200'], '#ef5350');
    }
    if (mergedVwap && mergedVwap.length > 0) {
      addLine('vwap', mergedVwap, '#ffa726');
    }

    chartRef.current = chart;
    const ro = new ResizeObserver(() => {
      if (ref.current && chartRef.current) {
        try {
          chartRef.current.applyOptions({ width: ref.current.clientWidth });
        } catch (e) {
          // Chart disposed, ignore
        }
      }
    });
    ro.observe(ref.current);
    
    return () => { 
      ro.disconnect();
      if (chartRef.current) {
        try {
          chartRef.current.remove();
        } catch (e) {
          // Chart already disposed, ignore
        }
        chartRef.current = null;
      }
      candleSeriesRef.current = null;
      lineSeriesRef.current.clear();
    };
  }, [mergedOhlc, mergedEma, mergedVwap, height]);

  // Update chart when merged data changes (for real-time updates)
  useEffect(() => {
    if (!candleSeriesRef.current || !mergedOhlc || mergedOhlc.length === 0 || !chartRef.current) return;
    
    const convertToChartData = (bars: Bar[]) => {
      return bars.map(b => {
        let timestamp: number;
        if (typeof b.timestamp === 'number') {
          timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
        } else if (b.timestamp) {
          timestamp = Date.parse(b.timestamp as string) / 1000;
        } else {
          timestamp = Date.now() / 1000;
        }
        return {
          time: timestamp as UTCTimestamp,
          open: b.open, 
          high: b.high, 
          low: b.low, 
          close: b.close
        };
      });
    };
    
    try {
      candleSeriesRef.current.setData(convertToChartData(mergedOhlc));
    } catch (e) {
      console.warn('Failed to update candle data:', e);
      return;
    }
    
    // Update indicator lines
    if (mergedEma) {
      Object.entries(mergedEma).forEach(([key, values]) => {
        const line = lineSeriesRef.current.get(key);
        if (line && values && values.length > 0) {
          try {
            const points: LinePoint[] = mergedOhlc.map((b, i) => {
              let timestamp: number;
              if (typeof b.timestamp === 'number') {
                timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
              } else if (b.timestamp) {
                timestamp = Date.parse(b.timestamp as string) / 1000;
              } else {
                timestamp = Date.now() / 1000;
              }
              return {
                time: timestamp as UTCTimestamp,
                value: values[i] || values[values.length - 1] || 0
              };
            }).filter(p => Number.isFinite(p.value));
            line.setData(points);
          } catch (e) {
            console.warn(`Failed to update line ${key}:`, e);
          }
        }
      });
    }
    
    if (mergedVwap && mergedVwap.length > 0) {
      const vwapLine = lineSeriesRef.current.get('vwap');
      if (vwapLine) {
        try {
          const points: LinePoint[] = mergedOhlc.map((b, i) => {
            let timestamp: number;
            if (typeof b.timestamp === 'number') {
              timestamp = String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000;
            } else if (b.timestamp) {
              timestamp = Date.parse(b.timestamp as string) / 1000;
            } else {
              timestamp = Date.now() / 1000;
            }
            return {
              time: timestamp as UTCTimestamp,
              value: mergedVwap[i] || mergedVwap[mergedVwap.length - 1] || 0
            };
          }).filter(p => Number.isFinite(p.value));
          vwapLine.setData(points);
        } catch (e) {
          console.warn('Failed to update VWAP line:', e);
        }
      }
    }
  }, [mergedOhlc, mergedEma, mergedVwap]);

  // Subscribe to real-time updates
  useEffect(() => {
    if (!symbol || !enableRealtime) return;
    
    const unsubscribe = subscribeChart(symbol, (data: any) => {
      if (data.type === 'chart_update' && data.ohlc) {
        // Merge real-time OHLC with historical data
        setMergedOhlc(prev => {
          // Check if this timestamp already exists (update existing candle)
          const existingIndex = prev.findIndex(b => {
            const bTs = typeof b.timestamp === 'number' 
              ? (String(b.timestamp).length <= 10 ? b.timestamp : b.timestamp / 1000)
              : Date.parse(b.timestamp as string) / 1000;
            const dataTs = typeof data.ohlc.timestamp === 'number'
              ? (String(data.ohlc.timestamp).length <= 10 ? data.ohlc.timestamp : data.ohlc.timestamp / 1000)
              : Date.parse(data.ohlc.timestamp as string) / 1000;
            // Same minute (within 60 seconds)
            return Math.abs(bTs - dataTs) < 60;
          });
          
          const newBar: Bar = {
            timestamp: data.ohlc.timestamp,
            open: data.ohlc.open,
            high: data.ohlc.high,
            low: data.ohlc.low,
            close: data.ohlc.close
          };
          
          if (existingIndex >= 0) {
            // Update existing candle
            const updated = [...prev];
            updated[existingIndex] = newBar;
            return updated;
          } else {
            // Add new candle (append to end)
            return [...prev, newBar];
          }
        });
        
        // Update indicators if provided
        if (data.indicators) {
          setMergedEma(prev => {
            const updated = { ...prev };
            if (data.indicators.ema_20 !== undefined) {
              updated['ema_20'] = [...(prev['ema_20'] || []), data.indicators.ema_20];
            }
            if (data.indicators.ema_50 !== undefined) {
              updated['ema_50'] = [...(prev['ema_50'] || []), data.indicators.ema_50];
            }
            if (data.indicators.ema_100 !== undefined) {
              updated['ema_100'] = [...(prev['ema_100'] || []), data.indicators.ema_100];
            }
            if (data.indicators.ema_200 !== undefined) {
              updated['ema_200'] = [...(prev['ema_200'] || []), data.indicators.ema_200];
            }
            return updated;
          });
          
          if (data.indicators.vwap !== undefined) {
            setMergedVwap(prev => [...prev, data.indicators.vwap]);
          }
        }
      }
    });
    
    return unsubscribe;
  }, [symbol, enableRealtime]);

  // Calculate price change
  const priceChange = mergedOhlc.length > 0 ? (() => {
    const latest = mergedOhlc[mergedOhlc.length - 1];
    const previous = mergedOhlc.length > 1 ? mergedOhlc[mergedOhlc.length - 2] : latest;
    const change = (latest.close || 0) - (previous.close || 0);
    const changePercent = previous.close ? (change / previous.close) * 100 : 0;
    return { change, changePercent, latest: latest.close || 0, previous: previous.close || 0 };
  })() : null;

  return (
    <Box sx={{ width: '100%' }}>
      {/* Price Change Info */}
      {priceChange && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1, px: 1 }}>
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            {priceChange.latest.toFixed(2)}
          </Typography>
          <Typography 
            variant="body2" 
            sx={{ 
              color: priceChange.change >= 0 ? 'success.main' : 'error.main',
              fontWeight: 600
            }}
          >
            {priceChange.change >= 0 ? '+' : ''}{priceChange.change.toFixed(2)} 
            ({priceChange.change >= 0 ? '+' : ''}{priceChange.changePercent.toFixed(2)}%)
          </Typography>
        </Box>
      )}
      
      {/* Legend */}
      {showLegend && (ema || vwap) && (
        <Box sx={{ display: 'flex', gap: 2, mb: 1, px: 1, flexWrap: 'wrap', alignItems: 'center' }}>
          <Typography variant="caption" sx={{ fontWeight: 600 }}>Legend:</Typography>
          {ema?.ema_20 && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 20, height: 2, bgcolor: '#7cb342' }} />
              <Typography variant="caption">EMA 20</Typography>
            </Box>
          )}
          {ema?.ema_50 && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 20, height: 2, bgcolor: '#42a5f5' }} />
              <Typography variant="caption">EMA 50</Typography>
            </Box>
          )}
          {ema?.ema_100 && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 20, height: 2, bgcolor: '#ab47bc' }} />
              <Typography variant="caption">EMA 100</Typography>
            </Box>
          )}
          {ema?.ema_200 && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 20, height: 2, bgcolor: '#ef5350' }} />
              <Typography variant="caption">EMA 200</Typography>
            </Box>
          )}
          {vwap && vwap.length > 0 && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
              <Box sx={{ width: 20, height: 2, bgcolor: '#ffa726' }} />
              <Typography variant="caption">VWAP</Typography>
            </Box>
          )}
        </Box>
      )}
      
      <div ref={ref} className="w-full" />
    </Box>
  );
}

