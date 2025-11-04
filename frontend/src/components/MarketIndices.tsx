import React, { useEffect, useState } from 'react';
import { marketAPI } from '../services/api';
import { subscribeMarket } from '../services/socket';
import { Box, Typography } from '@mui/material';

interface IndicesData {
  "NIFTY 50": {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
  "NIFTY BANK": {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
  "INDIA VIX": {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
}

const MarketIndices: React.FC = () => {
  const [indices, setIndices] = useState<IndicesData | null>(null);

  // Fetch initial data
  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        const response = await marketAPI.getIndices();
        if (mounted) setIndices(response.data);
      } catch (err) {
        console.error('Failed to fetch market indices', err);
      }
    })();
    // Subscribe to real-time updates via WebSocket
    const unsubscribe = subscribeMarket((data) => {
      setIndices(data);
    });
    return () => {
      mounted = false;
      unsubscribe();
    };
  }, []);

  if (!indices) {
    // Return empty placeholder instead of null to avoid layout issues
    return (
      <Box sx={{ display: 'flex', alignItems: 'center', mr: 2 }}>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Loading indices...
        </Typography>
      </Box>
    );
  }

  const Item = ({ name, data }: { name: string; data: any }) => {
    if (!data || data.last_price === 0) return null;
    const color = data.change >= 0 ? 'success.main' : 'error.main';
    const arrow = data.change >= 0 ? '▲' : '▼';
    return (
      <Box sx={{ mx: 1 }}>
        <Typography variant="subtitle2" component="span" sx={{ fontWeight: 600 }}>
          {name}:
        </Typography>
        <Typography component="span" sx={{ ml: 0.5 }}>
          {data.last_price.toFixed(2)}
        </Typography>
        <Typography component="span" sx={{ ml: 0.5, color }}>
          {arrow} {Math.abs(data.change).toFixed(2)} ({data.change_pct.toFixed(2)}%)
        </Typography>
      </Box>
    );
  };

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', mr: 2 }}>
      <Item name="VIX" data={indices["INDIA VIX"]} />
      <Item name="NIFTY" data={indices["NIFTY 50"]} />
      <Item name="BANKNIFTY" data={indices["NIFTY BANK"]} />
    </Box>
  );
};

export default MarketIndices;