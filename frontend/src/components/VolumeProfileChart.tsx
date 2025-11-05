import ReactECharts from 'echarts-for-react';

interface Props {
  distribution: Record<string, number>; // price -> volume
  poc_price?: number;
  value_area_low?: number;
  value_area_high?: number;
  height?: number;
}

export default function VolumeProfileChart({ distribution, poc_price, value_area_low, value_area_high, height = 280 }: Props) {
  const prices = Object.keys(distribution).map(parseFloat).sort((a, b) => a - b);
  const vols = prices.map(p => distribution[p.toFixed(2)] ?? distribution[String(p)] ?? 0);

  const option = {
    grid: { left: 90, right: 20, top: 10, bottom: 10 },
    xAxis: { type: 'value', axisLine: { show: false }, splitLine: { show: false } },
    yAxis: { type: 'category', data: prices.map(p => p.toFixed(2)), axisLine: { show: false }},
    series: [{
      type: 'bar',
      data: vols,
      barWidth: '70%',
      itemStyle: { opacity: 0.85 }
    }],
    visualMap: poc_price ? [{
      show: false,
      type: 'piecewise',
      pieces: [{ min: poc_price - 0.0001, max: poc_price + 0.0001, color: '#ef5350' }]
    }] : undefined,
    tooltip: { trigger: 'axis' }
  };

  return <ReactECharts style={{ height }} option={option} notMerge={true} lazyUpdate={true} />;
}

