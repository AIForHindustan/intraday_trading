import React from 'react';
import { Box, Grid, Paper, Typography, Card, CardContent, Divider } from '@mui/material';
import AlertList from './AlertList';
import { alertsAPI, newsAPI } from '../services/api';
import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';

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

  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const [statsRes, newsRes] = await Promise.all([
          alertsAPI.getStats(),
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

