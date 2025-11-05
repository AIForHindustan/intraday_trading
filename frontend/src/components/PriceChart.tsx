import { useEffect, useRef, useState } from 'react';
import { createChart, IChartApi, UTCTimestamp } from 'lightweight-charts';
import { subscribeChart } from '../services/socket';

type Bar = { timestamp: string | number; open: number; high: number; low: number; close: number };
type LinePoint = { time: UTCTimestamp; value: number };

interface Props {
  ohlc: Bar[];
  ema?: Record<string, number[]>;
  vwap?: number[];
  height?: number;
  symbol?: string; // Symbol for real-time updates
  enableRealtime?: boolean; // Enable real-time updates (default: true if symbol provided)
}

export default function PriceChart({ ohlc, ema, vwap, height = 380, symbol, enableRealtime = true }: Props) {
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
    if (!ref.current || !mergedOhlc || mergedOhlc.length === 0) return;
    if (chartRef.current) { 
      chartRef.current.remove(); 
      chartRef.current = null;
      candleSeriesRef.current = null;
      lineSeriesRef.current.clear();
    }

    const chart = createChart(ref.current, { 
      height, 
      layout: { background: { color: 'transparent' }}, 
      rightPriceScale: { borderVisible: false }, 
      timeScale: { borderVisible: false }
    });
    
    const candle = chart.addCandlestickSeries();
    candleSeriesRef.current = candle;
    
    const convertToChartData = (bars: Bar[]) => {
      return bars.map(b => {
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
          open: b.open, 
          high: b.high, 
          low: b.low, 
          close: b.close
        };
      });
    };
    
    candle.setData(convertToChartData(mergedOhlc));

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
      if (ref.current) {
        chart.applyOptions({ width: ref.current.clientWidth });
      }
    });
    ro.observe(ref.current);
    
    return () => { 
      ro.disconnect(); 
      chart.remove();
      candleSeriesRef.current = null;
      lineSeriesRef.current.clear();
    };
  }, [mergedOhlc, mergedEma, mergedVwap, height]);

  // Update chart when merged data changes (for real-time updates)
  useEffect(() => {
    if (!candleSeriesRef.current || !mergedOhlc || mergedOhlc.length === 0) return;
    
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
    
    candleSeriesRef.current.setData(convertToChartData(mergedOhlc));
    
    // Update indicator lines
    if (mergedEma) {
      Object.entries(mergedEma).forEach(([key, values]) => {
        const line = lineSeriesRef.current.get(key);
        if (line && values && values.length > 0) {
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
        }
      });
    }
    
    if (mergedVwap && mergedVwap.length > 0) {
      const vwapLine = lineSeriesRef.current.get('vwap');
      if (vwapLine) {
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

  return <div ref={ref} className="w-full" />;
}

