import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { marketAPI } from '../services/api';
import { subscribeMarket } from '../services/socket';
import { Box, Chip, Stack } from '@mui/material';

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
    const vixData = indices?.["INDIA VIX"];
    const niftyData = indices?.["NIFTY 50"];
    const bankniftyData = indices?.["NIFTY BANK"];
    
    const vixValue = indices?.vix?.value ?? vixData?.last_price ?? null;
    const niftyValue = indices?.nifty_50?.value ?? niftyData?.last_price ?? null;
    const bankniftyValue = indices?.banknifty?.value ?? bankniftyData?.last_price ?? null;
    const giftGap = indices?.["GIFT_NIFTY_GAP"];

    console.log('📊 MarketIndices: Extracted values:', { vixValue, niftyValue, bankniftyValue, giftGap });
    console.log('📊 MarketIndices: Full indices object keys:', Object.keys(indices || {}));

    const v = vixValue != null && vixValue > 0 ? vixValue.toFixed(2) : '--';
    const n = niftyValue != null && niftyValue > 0 ? niftyValue.toFixed(2) : '--';
    const b = bankniftyValue != null && bankniftyValue > 0 ? bankniftyValue.toFixed(2) : '--';
    
    // Get change values for color coding
    const vixChange = vixData?.change ?? 0;
    const niftyChange = niftyData?.change ?? 0;
    const bankniftyChange = bankniftyData?.change ?? 0;
    
    // Format Gift Nifty gap
    const gapDisplay = giftGap 
      ? `${giftGap.gap_points >= 0 ? '+' : ''}${giftGap.gap_points.toFixed(0)} (${giftGap.gap_percent >= 0 ? '+' : ''}${giftGap.gap_percent.toFixed(2)}%)`
      : '--';

    const result = { 
      v, n, b, gapDisplay, giftGap,
      vixChange, niftyChange, bankniftyChange
    };
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

  // Helper to get value color based on change - softer, more readable colors
  const getValueColor = (change: number) => {
    if (change > 0) return '#81c784'; // softer green
    if (change < 0) return '#e57373'; // softer red
    return 'rgba(255,255,255,0.9)'; // slightly dimmed white for neutral
  };

  // Shared chip styling for better readability
  const chipSx = {
    backgroundColor: 'rgba(255,255,255,0.15)', // slightly more opaque for better contrast
    color: 'white',
    height: '36px', // larger than default small (24px)
    fontSize: '0.95rem', // larger font
    '& .MuiChip-label': { 
      px: 2, // more horizontal padding
      py: 0.5,
      fontWeight: 500
    }
  };

  return (
    <Stack direction="row" spacing={2} sx={{ alignItems: 'center' }}>
      <Chip 
        label={
          <Box component="span" sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Box component="span" sx={{ color: 'rgba(255,255,255,0.95)', fontWeight: 600, fontSize: '0.95rem' }}>VIX:</Box>
            <Box component="span" sx={{ color: getValueColor(displayValues.vixChange), fontWeight: 700, fontSize: '1rem' }}>
              {displayValues.v}
            </Box>
          </Box>
        }
        sx={chipSx}
      />
      <Chip 
        label={
          <Box component="span" sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Box component="span" sx={{ color: 'rgba(255,255,255,0.95)', fontWeight: 600, fontSize: '0.95rem' }}>NIFTY:</Box>
            <Box component="span" sx={{ color: getValueColor(displayValues.niftyChange), fontWeight: 700, fontSize: '1rem' }}>
              {displayValues.n}
            </Box>
          </Box>
        }
        sx={chipSx}
      />
      <Chip 
        label={
          <Box component="span" sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Box component="span" sx={{ color: 'rgba(255,255,255,0.95)', fontWeight: 600, fontSize: '0.95rem' }}>BANKNIFTY:</Box>
            <Box component="span" sx={{ color: getValueColor(displayValues.bankniftyChange), fontWeight: 700, fontSize: '1rem' }}>
              {displayValues.b}
            </Box>
          </Box>
        }
        sx={chipSx}
      />
      <Chip 
        label={
          <Box component="span" sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
            <Box component="span" sx={{ color: 'rgba(255,255,255,0.95)', fontWeight: 600, fontSize: '0.95rem' }}>GIFT NIFTY GAP:</Box>
            <Box component="span" sx={{ 
              color: displayValues.giftGap 
                ? (displayValues.giftGap.gap_percent > 0 ? '#81c784' : displayValues.giftGap.gap_percent < 0 ? '#e57373' : 'rgba(255,255,255,0.9)')
                : 'rgba(255,255,255,0.9)',
              fontWeight: 700,
              fontSize: '1rem'
            }}>
              {displayValues.gapDisplay}
            </Box>
          </Box>
        }
        sx={chipSx}
      />
    </Stack>
  );
});

MarketIndices.displayName = 'MarketIndices';

export default MarketIndices;