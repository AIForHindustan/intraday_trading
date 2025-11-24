import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { newsAPI } from '../services/api';
import { Box, Typography, Paper, List, ListItem, ListItemText, TextField, Button } from '@mui/material';
import { formatDistanceToNow } from 'date-fns';

const NewsFeed: React.FC = React.memo(() => {
  const [symbol, setSymbol] = useState('');
  const [news, setNews] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  // Memoized fetch function to avoid recreation on each render
  const fetchNews = useCallback(async () => {
    setLoading(true);
    try {
      console.log('📰 NewsFeed: Fetching news...');
      const response = symbol ? await newsAPI.getBySymbol(symbol, 50, 180) : await newsAPI.getLatestMarket();
      console.log('📰 NewsFeed: Response received:', response?.status, response?.data?.length || 'N/A');
      // API returns array directly, not wrapped in {news: [...]}
      let newsData = Array.isArray(response.data) ? response.data : (response.data?.news || []);
      console.log('📰 NewsFeed: Parsed news items:', newsData.length);
      
      // Sort news by timestamp (newest first)
      newsData = newsData.sort((a, b) => {
        // Get timestamp from various possible fields
        const getTimestamp = (item: any): number => {
          const ts = item.timestamp || item.collected_at || item.written_at || item.published_at || item.date;
          if (!ts) return 0;
          try {
            const date = new Date(ts);
            return isNaN(date.getTime()) ? 0 : date.getTime();
          } catch {
            return 0;
          }
        };
        
        const tsA = getTimestamp(a);
        const tsB = getTimestamp(b);
        // Sort descending (newest first)
        return tsB - tsA;
      });
      
      setNews(newsData); // Always update - React.memo will handle re-render optimization
    } catch (err: any) {
      console.error('❌ NewsFeed: Failed to fetch news', err);
      console.error('❌ Error details:', err?.response?.status, err?.response?.data, err?.message);
      console.error('❌ Request URL:', err?.config?.url, err?.config?.baseURL);
      setNews([]);
    } finally {
      setLoading(false);
    }
  }, [symbol]);

  useEffect(() => {
    fetchNews();
  }, [fetchNews]);

  return (
    <Box>
      <Typography variant="h5" gutterBottom>News Feed</Typography>
      <Paper sx={{ p: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', gap: 2, mb: 2 }}>
          <TextField
            label="Symbol"
            size="small"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
          />
          <Button variant="contained" onClick={fetchNews} disabled={loading}>
            {loading ? 'Loading...' : 'Fetch News'}
          </Button>
        </Box>
        <List>
          {news.map((item, index) => {
            // Handle both backend formats: {title, link} or {headline, url}
            const title = item.headline || item.title || 'No title';
            const url = item.url || item.link || '#';
            const source = item.source || item.publisher || 'Unknown';
            const timestamp = item.timestamp || item.collected_at || item.date || item.written_at || new Date().toISOString();
            const timeAgo = formatDistanceToNow(new Date(timestamp), { addSuffix: true });
            
            return (
              <ListItem key={`${index}-${title}`} component="a" href={url} target="_blank" rel="noopener noreferrer" sx={{ color: 'inherit' }}>
                <ListItemText
                  primary={title}
                  secondary={`${source} — ${timeAgo}`}
                />
              </ListItem>
            );
          })}
        </List>
      </Paper>
    </Box>
  );
});

NewsFeed.displayName = 'NewsFeed';

export default NewsFeed;