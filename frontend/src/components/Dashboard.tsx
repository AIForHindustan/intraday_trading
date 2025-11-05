import React from 'react';
import { Box, Grid, Paper, Typography, Card, CardContent, Divider } from '@mui/material';
import AlertList from './AlertList';
import PriceChart from './PriceChart';
import { alertsAPI, newsAPI, chartsAPI } from '../services/api';
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { adaptOhlcPayload, adaptOverlays } from '../services/adapters';

interface QuickStats {
  total_alerts: number;
  today_alerts: number;
  avg_confidence: number;
  top_pattern?: string;
}

interface NewsItem {
  title?: string;
  headline?: string;
  summary?: string;
  description?: string;
  published_at?: string;
  timestamp?: string;
  source?: string;
  url?: string;
}

const Dashboard: React.FC = () => {
  const [quickStats, setQuickStats] = useState<QuickStats | null>(null);
  const [recentNews, setRecentNews] = useState<NewsItem[]>([]);
  const [chartData, setChartData] = useState<any>(null);
  const [chartSymbol, setChartSymbol] = useState<string>('NIFTY');

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        // First fetch stats and alerts to get a real symbol for the chart
        const [statsRes, alertsRes, newsRes] = await Promise.all([
          alertsAPI.getStats(),
          alertsAPI.getAll({ limit: 10 }).catch(() => ({ data: { alerts: [] } })),
          newsAPI.getLatestMarket().catch(() => ({ data: [] }))
        ]);
        if (!mounted) return;
        
        const data = statsRes.data;
        setQuickStats({
          total_alerts: data.total_alerts || 0,
          today_alerts: data.today_alerts || 0,
          avg_confidence: data.avg_confidence || 0,
          top_pattern: data.top_pattern || 'N/A'
        });
        
        // Set recent news (limit to 5)
        const news = Array.isArray(newsRes.data) ? newsRes.data.slice(0, 5) : [];
        setRecentNews(news);
        
        // Get symbol from first alert if available, otherwise use default
        const alerts = alertsRes.data?.alerts || [];
        const symbolForChart = alerts.length > 0 
          ? (alerts[0].base_symbol || alerts[0].symbol || chartSymbol)
          : chartSymbol;
        
        // Fetch chart data for the symbol
        try {
          const chartRes = await chartsAPI.getData(symbolForChart, { include_indicators: true });
          if (mounted && chartRes.data) {
            setChartData(chartRes.data);
            setChartSymbol(symbolForChart);
          }
        } catch (chartErr) {
          console.warn(`Failed to fetch chart data for ${symbolForChart}:`, chartErr);
          // Try with default symbol if first attempt failed
          if (symbolForChart !== chartSymbol) {
            try {
              const fallbackChartRes = await chartsAPI.getData(chartSymbol, { include_indicators: true });
              if (mounted && fallbackChartRes.data) {
                setChartData(fallbackChartRes.data);
              }
            } catch {
              // Ignore fallback errors
            }
          }
        }
      } catch (err) {
        console.error('Failed to fetch dashboard data', err);
        if (mounted) {
          setQuickStats({
            total_alerts: 0,
            today_alerts: 0,
            avg_confidence: 0,
            top_pattern: 'N/A'
          });
        }
      }
    })();
    return () => { mounted = false; };
  }, []);

  // Debug: Log component render
  console.log('Dashboard rendering, quickStats:', quickStats);

  return (
    <Box sx={{ width: '100%' }}>
      {/* Quick Stats Cards - Always show, even if loading */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom variant="body2">
                Total Alerts
              </Typography>
              <Typography variant="h4">
                {quickStats?.total_alerts ?? '...'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom variant="body2">
                Today's Alerts
              </Typography>
              <Typography variant="h4">
                {quickStats?.today_alerts ?? '...'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom variant="body2">
                Avg Confidence
              </Typography>
              <Typography variant="h4">
                {quickStats?.avg_confidence !== undefined && quickStats.avg_confidence > 0 
                  ? `${(quickStats.avg_confidence * 100).toFixed(1)}%` 
                  : quickStats === null ? '...' : '0%'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} sm={6} md={3}>
          <Card>
            <CardContent>
              <Typography color="text.secondary" gutterBottom variant="body2">
                Top Pattern
              </Typography>
              <Typography variant="h4" sx={{ fontSize: '1.5rem' }}>
                {quickStats?.top_pattern || (quickStats === null ? '...' : 'N/A')}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Chart Section */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Market Chart - {chartSymbol}
        </Typography>
        {chartData ? (() => {
          const ohlc = adaptOhlcPayload(chartData);
          const overlay = adaptOverlays(chartData);
          const ema = overlay.ema_20?.length > 0 ? {
            ema_20: overlay.ema_20,
            ema_50: overlay.ema_50,
            ema_100: overlay.ema_100,
            ema_200: overlay.ema_200,
          } : undefined;
          const vwap = overlay.vwap;
          
          if (ohlc.length > 0) {
            return (
              <Box sx={{ width: '100%', height: 400 }}>
                <PriceChart 
                  ohlc={ohlc} 
                  ema={ema} 
                  vwap={vwap} 
                  height={400}
                  symbol={chartSymbol}
                  enableRealtime={true}
                />
              </Box>
            );
          } else {
            return (
              <Box sx={{ p: 3, textAlign: 'center', height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Typography variant="body1" color="text.secondary">
                  No chart data available for {chartSymbol}
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                  Chart data will appear here once market data is available
                </Typography>
              </Box>
            );
          }
        })() : (
          <Box sx={{ p: 3, textAlign: 'center', height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Typography variant="body1" color="text.secondary">
              Loading chart data...
            </Typography>
          </Box>
        )}
      </Paper>

      {/* Two Column Layout: Alerts + News */}
      <Grid container spacing={2}>
        {/* Alerts List - Takes 2/3 width */}
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} md={8}>
          <AlertList />
        </Grid>
        
        {/* Recent News - Takes 1/3 width */}
        {/* @ts-ignore - MUI Grid API mismatch */}
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
              <Typography variant="h6">Recent Market News</Typography>
              <Typography 
                component={Link} 
                to="/news" 
                variant="body2" 
                sx={{ color: 'primary.main', textDecoration: 'none' }}
              >
                View All →
              </Typography>
            </Box>
            <Divider sx={{ mb: 2 }} />
            {recentNews.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No recent news available
              </Typography>
            ) : (
              <Box>
                {recentNews.map((news, idx) => (
                  <Box key={idx} sx={{ mb: 2, pb: 2, borderBottom: idx < recentNews.length - 1 ? '1px solid' : 'none', borderColor: 'divider' }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.5 }}>
                      {news.headline || news.title || 'Untitled'}
                    </Typography>
                    {(news.summary || news.description) && (
                      <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                        {(() => {
                          const text = news.summary || news.description || '';
                          return text.length > 100 ? `${text.substring(0, 100)}...` : text;
                        })()}
                      </Typography>
                    )}
                    {(news.published_at || news.timestamp) && (
                      <Typography variant="caption" color="text.secondary">
                        {new Date(news.published_at || news.timestamp || Date.now()).toLocaleDateString()}
                      </Typography>
                    )}
                  </Box>
                ))}
              </Box>
            )}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard;

