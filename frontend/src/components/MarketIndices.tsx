import React, { useEffect, useState } from 'react';
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

const MarketIndices: React.FC = () => {
  const [indices, setIndices] = useState<IndicesData>({});

  // REST bootstrap + polling fallback
  useEffect(() => {
    let timer: any;
    const fetchOnce = async () => {
      try {
        const { data } = await marketAPI.getIndices();
        if (data) {
          setIndices(data);
        }
      } catch (err) {
        console.error('Failed to fetch market indices', err);
      }
    };
    fetchOnce();
    timer = setInterval(fetchOnce, 5000); // Poll every 5 seconds
    return () => clearInterval(timer);
  }, []);

  // WS live updates (if available) - use subscribeMarket helper
  useEffect(() => {
    const unsubscribe = subscribeMarket((msg: any) => {
      const data = msg?.data || msg || {};
      setIndices(data);
    });
    return unsubscribe;
  }, []);

  // Extract values with fallbacks
  const vixValue = indices?.vix?.value ?? indices?.["INDIA VIX"]?.last_price ?? null;
  const niftyValue = indices?.nifty_50?.value ?? indices?.["NIFTY 50"]?.last_price ?? null;
  const bankniftyValue = indices?.banknifty?.value ?? indices?.["NIFTY BANK"]?.last_price ?? null;
  const giftGap = indices?.["GIFT_NIFTY_GAP"];

  const v = vixValue != null && vixValue > 0 ? vixValue.toFixed(2) : '--';
  const n = niftyValue != null && niftyValue > 0 ? niftyValue.toFixed(2) : '--';
  const b = bankniftyValue != null && bankniftyValue > 0 ? bankniftyValue.toFixed(2) : '--';
  
  // Format Gift Nifty gap
  const gapDisplay = giftGap 
    ? `${giftGap.gap_points >= 0 ? '+' : ''}${giftGap.gap_points.toFixed(0)} (${giftGap.gap_percent >= 0 ? '+' : ''}${giftGap.gap_percent.toFixed(2)}%)`
    : '--';

  return (
    <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
      <Chip label={`VIX: ${v}`} size="small" />
      <Chip label={`NIFTY: ${n}`} size="small" />
      <Chip label={`BANKNIFTY: ${b}`} size="small" />
      <Chip 
        label={`GIFT NIFTY GAP: ${gapDisplay}`} 
        size="small"
        color={giftGap && Math.abs(giftGap.gap_percent) > 0.5 ? (giftGap.gap_percent > 0 ? 'success' : 'error') : 'default'}
      />
    </Stack>
  );
};

export default MarketIndices;