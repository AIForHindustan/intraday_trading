import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { marketAPI } from '../services/api';
import { subscribeMarket } from '../services/socket';
import { Box, Chip, Stack, Typography } from '@mui/material';

interface IndicesData {
  "NIFTY 50"?: {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
  "NIFTY BANK"?: {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
  "INDIA VIX"?: {
    last_price: number;
    prev_close: number;
    change: number;
    change_pct: number;
  };
  "GIFT_NIFTY_GAP"?: {
    gap_points: number;
    gap_percent: number;
    gift_price: number;
    nifty_price: number;
    signal: string;
  };
  // Alternative format
  vix?: { value: number };
  nifty_50?: { value: number };
  banknifty?: { value: number };
}

const MarketIndices: React.FC = React.memo(() => {
  const [indices, setIndices] = useState<IndicesData>({});
  const [error, setError] = useState<string | null>(null);

  // Memoized fetch function to avoid recreating on each render
  const fetchOnce = useCallback(async () => {
    try {
      console.log('📊 MarketIndices: Fetching indices...');
      const response = await marketAPI.getIndices();
      console.log('📊 MarketIndices: Response received:', response?.status, response?.data);
      const data = response?.data;
      if (data) {
        console.log('📊 MarketIndices: Setting indices data:', Object.keys(data));
        setIndices(data); // Always update - React.memo will handle re-render optimization
        setError(null); // Clear any previous errors
      } else {
        console.warn('📊 MarketIndices: No data in response');
        setError('No data received from API');
      }
    } catch (err: any) {
      console.error('❌ MarketIndices: Failed to fetch market indices', err);
      console.error('❌ Error details:', err?.response?.status, err?.response?.data, err?.message);
      console.error('❌ Request URL:', err?.config?.url, err?.config?.baseURL);
      setError(err?.message || 'Failed to fetch indices');
    }
  }, []);

  // REST bootstrap + polling fallback (optimized polling interval)
  useEffect(() => {
    fetchOnce(); // Initial fetch
    const timer = setInterval(fetchOnce, 10000); // Poll every 10 seconds (reduced from 5s)
    return () => clearInterval(timer);
  }, [fetchOnce]);

  // WS live updates (if available) - use subscribeMarket helper
  useEffect(() => {
    const unsubscribe = subscribeMarket((msg: any) => {
      const data = msg?.data || msg || {};
      console.log('📊 MarketIndices: WebSocket update received:', data);
      setIndices(data); // Always update - React.memo will handle re-render optimization
    });
    return unsubscribe;
  }, []);

  // Memoized value extraction to avoid recalculation on every render
  const displayValues = useMemo(() => {
    console.log('📊 MarketIndices: Computing display values from indices:', JSON.stringify(indices));
    const vixValue = indices?.vix?.value ?? indices?.["INDIA VIX"]?.last_price ?? null;
    const niftyValue = indices?.nifty_50?.value ?? indices?.["NIFTY 50"]?.last_price ?? null;
    const bankniftyValue = indices?.banknifty?.value ?? indices?.["NIFTY BANK"]?.last_price ?? null;
    const giftGap = indices?.["GIFT_NIFTY_GAP"];

    console.log('📊 MarketIndices: Extracted values:', { vixValue, niftyValue, bankniftyValue, giftGap });
    console.log('📊 MarketIndices: Full indices object keys:', Object.keys(indices || {}));

    const v = vixValue != null && vixValue > 0 ? vixValue.toFixed(2) : '--';
    const n = niftyValue != null && niftyValue > 0 ? niftyValue.toFixed(2) : '--';
    const b = bankniftyValue != null && bankniftyValue > 0 ? bankniftyValue.toFixed(2) : '--';
    
    // Format Gift Nifty gap
    const gapDisplay = giftGap 
      ? `${giftGap.gap_points >= 0 ? '+' : ''}${giftGap.gap_points.toFixed(0)} (${giftGap.gap_percent >= 0 ? '+' : ''}${giftGap.gap_percent.toFixed(2)}%)`
      : '--';

    const result = { v, n, b, gapDisplay, giftGap };
    console.log('📊 MarketIndices: Final display values:', result);
    return result;
  }, [indices]);

  if (error) {
    return (
      <Chip 
        label={`Error: ${error}`} 
        size="small" 
        color="error"
        sx={{ maxWidth: 300 }}
      />
    );
  }

  return (
    <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
      <Chip label={`VIX: ${displayValues.v}`} size="small" />
      <Chip label={`NIFTY: ${displayValues.n}`} size="small" />
      <Chip label={`BANKNIFTY: ${displayValues.b}`} size="small" />
      <Chip 
        label={`GIFT NIFTY GAP: ${displayValues.gapDisplay}`} 
        size="small"
        color={displayValues.giftGap && Math.abs(displayValues.giftGap.gap_percent) > 0.5 
          ? (displayValues.giftGap.gap_percent > 0 ? 'success' : 'error') 
          : 'default'}
      />
    </Stack>
  );
});

MarketIndices.displayName = 'MarketIndices';

export default MarketIndices;