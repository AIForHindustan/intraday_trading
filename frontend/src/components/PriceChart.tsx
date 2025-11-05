import { useEffect, useRef } from 'react';
import { createChart, IChartApi, UTCTimestamp } from 'lightweight-charts';

type Bar = { timestamp: string | number; open: number; high: number; low: number; close: number };
type LinePoint = { time: UTCTimestamp; value: number };

interface Props {
  ohlc: Bar[];
  ema?: Record<string, number[]>;
  vwap?: number[];
  height?: number;
}

export default function PriceChart({ ohlc, ema, vwap, height = 380 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!ref.current || !ohlc || ohlc.length === 0) return;
    if (chartRef.current) { 
      chartRef.current.remove(); 
      chartRef.current = null; 
    }

    const chart = createChart(ref.current, { 
      height, 
      layout: { background: { color: 'transparent' }}, 
      rightPriceScale: { borderVisible: false }, 
      timeScale: { borderVisible: false }
    });
    
    const candle = chart.addCandlestickSeries();
    
    candle.setData(ohlc.map(b => ({
      time: (typeof b.timestamp === 'number' ? (b.timestamp / 1000) : Date.parse(b.timestamp as string) / 1000) as UTCTimestamp,
      open: b.open, 
      high: b.high, 
      low: b.low, 
      close: b.close
    })));

    const addLine = (values?: number[], color?: string) => {
      if (!values || values.length === 0) return;
      const line = chart.addLineSeries({ color, lineWidth: 2 });
      const points: LinePoint[] = ohlc.map((b, i) => ({
        time: (typeof b.timestamp === 'number' ? (b.timestamp / 1000) : Date.parse(b.timestamp as string) / 1000) as UTCTimestamp,
        value: values[i]
      })).filter(p => Number.isFinite(p.value));
      line.setData(points);
    };

    if (ema) {
      addLine(ema['ema_20'], '#7cb342');
      addLine(ema['ema_50'], '#42a5f5');
      addLine(ema['ema_100'], '#ab47bc');
      addLine(ema['ema_200'], '#ef5350');
    }
    if (vwap) addLine(vwap, '#ffa726');

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
    };
  }, [ohlc, ema, vwap, height]);

  return <div ref={ref} className="w-full" />;
}

