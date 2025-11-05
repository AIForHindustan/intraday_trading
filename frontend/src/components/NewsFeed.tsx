import React, { useEffect, useState } from 'react';
import { newsAPI } from '../services/api';
import { Box, Typography, Paper, List, ListItem, ListItemText, TextField, Button } from '@mui/material';
import { formatDistanceToNow } from 'date-fns';

const NewsFeed: React.FC = () => {
  const [symbol, setSymbol] = useState('');
  const [news, setNews] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchNews = async () => {
    setLoading(true);
    try {
      const response = symbol ? await newsAPI.getBySymbol(symbol, 50, 180) : await newsAPI.getLatestMarket();
      // API returns array directly, not wrapped in {news: [...]}
      const newsData = Array.isArray(response.data) ? response.data : (response.data?.news || []);
      setNews(newsData);
    } catch (err) {
      console.error('Failed to fetch news', err);
      setNews([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchNews();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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
          {news.map((item, index) => (
            <ListItem key={index} component="a" href={item.url} target="_blank" rel="noopener noreferrer" sx={{ color: 'inherit' }}>
              <ListItemText
                primary={item.headline}
                secondary={`${item.source} — ${formatDistanceToNow(new Date(item.timestamp), { addSuffix: true })}`}
              />
            </ListItem>
          ))}
        </List>
      </Paper>
    </Box>
  );
};

export default NewsFeed;