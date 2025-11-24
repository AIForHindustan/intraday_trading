import React from 'react';
import { Box, Grid, Paper, Typography, Card, CardContent, Divider, Button } from '@mui/material';
import AlertList from './AlertList';
import PriceChart from './PriceChart';
import { alertsAPI, newsAPI, chartsAPI, dashboardAPI } from '../services/api';
import { useEffect, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { adaptOhlcPayload, adaptOverlays } from '../services/adapters';
import { formatDistanceToNow } from 'date-fns';
import { BarChart, ShowChart } from '@mui/icons-material';

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
  collected_at?: string;
  written_at?: string;
  date?: string;
  source?: string;
  publisher?: string;
  url?: string;
  link?: string;
}

const Dashboard: React.FC = () => {
  const [quickStats, setQuickStats] = useState<QuickStats | null>(null);
  const [recentNews, setRecentNews] = useState<NewsItem[]>([]);
  const [chartData, setChartData] = useState<any>(null);
  const [chartSymbol, setChartSymbol] = useState<string>('NIFTY 50'); // Default to NIFTY 50 index
  const navigate = useNavigate();

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        // Use consolidated dashboard-stats endpoint (single API call)
        console.log('📊 Dashboard: Fetching dashboard stats...');
        const dashboardRes = await dashboardAPI.getStats().catch(e => {
          console.error('Failed to fetch dashboard stats:', e);
          return { 
            data: { 
              total_alerts: 0, 
              today_alerts: 0, 
              avg_confidence: 0, 
              top_pattern: 'N/A',
              alerts: [],
              news: []
            } 
          };
        });
        
        if (!mounted) return;
        
        const data = dashboardRes.data;
        console.log('📊 Dashboard: Dashboard stats received:', {
          total_alerts: data.total_alerts,
          today_alerts: data.today_alerts,
          alerts_count: data.alerts?.length || 0,
          news_count: data.news?.length || 0
        });
        
        // Set quick stats
        setQuickStats({
          total_alerts: data.total_alerts || 0,
          today_alerts: data.today_alerts || 0,
          avg_confidence: data.avg_confidence || 0,
          top_pattern: data.top_pattern || 'N/A'
        });
        
        // Set recent news (already sorted by backend, newest first)
        const newsData = Array.isArray(data.news) ? data.news : [];
        console.log('📰 Dashboard: News from dashboard-stats:', newsData.length, 'items');
        if (newsData.length > 0) {
          console.log('📰 Dashboard: First news item (should be newest):', {
            title: newsData[0].title || newsData[0].headline,
            timestamp: newsData[0].timestamp || newsData[0].collected_at || newsData[0].written_at || newsData[0].published_at || newsData[0].date
          });
        }
        setRecentNews(newsData.slice(0, 5));
        
        // Get symbol from alerts (for chart selection)
        const alerts = data.alerts || [];
        
        // Helper to check if symbol is an index (not options/futures)
        const isIndexSymbol = (symbol: string): boolean => {
          if (!symbol) return false;
          const upper = symbol.toUpperCase();
          // Check if it's a known index
          const indexNames = ['NIFTY', 'BANKNIFTY', 'NIFTY BANK', 'NIFTY 50', 'FINNIFTY', 'INDIA VIX'];
          if (indexNames.some(idx => upper.includes(idx))) {
            // Make sure it's not an options contract (no numbers/dates after the index name)
            // Options have format like: BANKNIFTY25DEC55400PE
            const indexMatch = upper.match(/^(NIFTY|BANKNIFTY|FINNIFTY|INDIA VIX)/);
            if (indexMatch) {
              const afterIndex = upper.substring(indexMatch[0].length).trim();
              // If there are digits right after, it's likely an options contract
              if (/^\d/.test(afterIndex)) {
                return false; // Options contract
              }
              return true; // Pure index
            }
          }
          return false;
        };
        
        // Use the symbol from the most recent alert (first in the array, since sorted newest first)
        let symbolForChart = chartSymbol; // Default to 'NIFTY 50'
        
        if (alerts.length > 0) {
          // Use the actual symbol from the most recent alert (not base_symbol)
          // This allows showing options contracts, futures, or indices
          const mostRecentAlert = alerts[0];
          symbolForChart = mostRecentAlert.symbol || mostRecentAlert.base_symbol || chartSymbol;
          console.log('📊 Dashboard: Using most recent alert symbol:', symbolForChart, 'from alert:', mostRecentAlert.symbol);
        } else {
          console.log('📊 Dashboard: No alerts, using default:', chartSymbol);
        }
        
        console.log('📊 Dashboard: Final chart symbol:', symbolForChart, 'from', alerts.length, 'alerts');
        
        // Fetch chart data for the symbol with period parameter
        try {
          console.log('📊 Dashboard: Fetching chart data for', symbolForChart);
          const chartRes = await chartsAPI.getData(symbolForChart, { 
            include_indicators: true,
            period: '7d', // Request 7 days of historical data
            resolution: '5m'
          });
          console.log('📊 Dashboard: Chart response received', {
            hasData: !!chartRes.data,
            ohlcCount: chartRes.data?.ohlc?.length || 0,
            dataType: chartRes.data?.data_type
          });
          if (mounted && chartRes.data) {
            // Ensure we have enough data points
            if (chartRes.data.ohlc && chartRes.data.ohlc.length > 0) {
              setChartData(chartRes.data);
              setChartSymbol(symbolForChart);
            } else {
              console.warn('📊 Dashboard: Chart data empty, trying fallback');
              // Try with default symbol if no data
              try {
                const fallbackChartRes = await chartsAPI.getData(chartSymbol, { 
                  include_indicators: true,
                  period: '7d',
                  resolution: '5m'
                });
                if (mounted && fallbackChartRes.data && fallbackChartRes.data.ohlc?.length > 0) {
                  setChartData(fallbackChartRes.data);
                }
              } catch {
                // Ignore fallback errors
              }
            }
          }
        } catch (chartErr) {
          console.error('❌ Dashboard: Failed to fetch chart data', chartErr);
          // Try with default symbol if first attempt failed
          if (symbolForChart !== chartSymbol) {
            try {
              const fallbackChartRes = await chartsAPI.getData(chartSymbol, { 
                include_indicators: true,
                period: '7d',
                resolution: '5m'
              });
              if (mounted && fallbackChartRes.data && fallbackChartRes.data.ohlc?.length > 0) {
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
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="h6">
              Market Chart - {chartSymbol}
            </Typography>
            <Button
              variant="outlined"
              size="small"
              startIcon={<BarChart />}
              onClick={() => {
                console.log('📊 Dashboard: Navigating to instruments page with symbol:', chartSymbol);
                const url = `/instruments${chartSymbol ? `?symbol=${encodeURIComponent(chartSymbol)}` : ''}`;
                console.log('📊 Dashboard: Navigation URL:', url);
                navigate(url);
              }}
              sx={{ textTransform: 'none' }}
            >
              View All Instruments
            </Button>
          </Box>
          {chartData && (
            <Typography variant="caption" color="text.secondary">
              {chartData.ohlc?.length || 0} bars • {chartData.data_type || 'redis'} • {chartData.period || '7d'}
              {chartData.data_days && chartData.data_days > 0 && ` • ${chartData.data_days.toFixed(1)} days of data`}
            </Typography>
          )}
        </Box>
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
                {recentNews.map((news, idx) => {
                  const title = news.headline || news.title || 'Untitled';
                  const url = news.url || news.link || '#';
                  const timestamp = news.timestamp || news.collected_at || news.written_at;
                  const timeAgo = timestamp && typeof timestamp === 'string' && timestamp.includes('T') 
                    ? formatDistanceToNow(new Date(timestamp), { addSuffix: true }) 
                    : '';
                  const publisher = news.publisher || news.source || '';
                  
                  return (
                    <Box 
                      key={idx} 
                      component="a"
                      href={url}
                      target="_blank"
                      rel="noopener noreferrer"
                      sx={{ 
                        mb: 2, 
                        pb: 2, 
                        borderBottom: idx < recentNews.length - 1 ? '1px solid' : 'none', 
                        borderColor: 'divider',
                        display: 'block',
                        textDecoration: 'none',
                        color: 'inherit',
                        '&:hover': {
                          backgroundColor: 'action.hover'
                        }
                      }}
                    >
                      <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.5 }}>
                        {title}
                      </Typography>
                      {(news.summary || news.description) && (
                        <Typography variant="body2" color="text.secondary" sx={{ mb: 0.5 }}>
                          {(() => {
                            const text = news.summary || news.description || '';
                            return text.length > 100 ? `${text.substring(0, 100)}...` : text;
                          })()}
                        </Typography>
                      )}
                      <Typography variant="caption" color="text.secondary">
                        {publisher ? `${publisher} • ` : ''}{news.date || timeAgo || 'Unknown date'}
                      </Typography>
                    </Box>
                  );
                })}
              </Box>
            )}
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default Dashboard;

