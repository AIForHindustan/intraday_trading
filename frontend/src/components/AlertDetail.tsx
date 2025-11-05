import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { alertsAPI, chartsAPI, volumeProfileAPI, newsAPI, validationAPI } from '../services/api';
import { subscribeValidation } from '../services/socket';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Chip,
  Divider,
  CircularProgress,
  List,
  ListItem,
  ListItemText,
  Tabs,
  Tab
} from '@mui/material';
import PriceChart from './PriceChart';
import VolumeProfileChart from './VolumeProfileChart';
import { format } from 'date-fns';

interface AlertDetailData {
  alert: any;
  chart_data: any;
  volume_profile: any;
  news: any[];
  validation: any;
}

const AlertDetail: React.FC = () => {
  const { alertId } = useParams<{ alertId: string }>();
  const [data, setData] = useState<AlertDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState(0);

  useEffect(() => {
    if (!alertId) return;
    let mounted = true;
    (async () => {
      try {
        const response = await alertsAPI.getById(alertId);
        if (!mounted) return;
        const alertData = response.data;
        // Extract symbol - use base_symbol if available, otherwise extract from symbol
        const symbol = alertData.alert.base_symbol || alertData.alert.symbol?.split(':')?.[1]?.split(/\d/)?.[0] || alertData.alert.symbol;
        
        // Fetch additional chart and volume data
        const [chartRes, volumeRes, newsRes, validationRes] = await Promise.all([
          chartsAPI.getData(alertData.alert.symbol, { include_indicators: true }),
          volumeProfileAPI.getData(alertData.alert.symbol),
          newsAPI.getBySymbol(symbol),
          validationAPI.getByAlertId(alertId)
        ]);
        if (!mounted) return;
        
        // Handle news response - API returns array directly, not wrapped in {news: [...]}
        const newsData = Array.isArray(newsRes.data) ? newsRes.data : (newsRes.data?.news || []);
        
        setData({
          alert: alertData.alert,
          chart_data: chartRes.data,
          volume_profile: volumeRes.data,
          news: newsData,
          validation: validationRes.data
        });
        setLoading(false);
      } catch (err) {
        console.error('Failed to fetch alert details', err);
        setLoading(false);
      }
    })();
    // Subscribe to validation updates
    const unsubscribe = subscribeValidation(alertId, (validation) => {
      setData((prev) => (prev ? { ...prev, validation } : prev));
    });
    return () => {
      mounted = false;
      unsubscribe();
    };
  }, [alertId]);

  if (loading || !data) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }}>
        <CircularProgress />
      </Box>
    );
  }

  const { alert, chart_data, volume_profile, news, validation } = data;

  // Prepare data for PriceChart
  const ohlc = chart_data?.ohlc || chart_data?.data || [];
  const ema = chart_data?.indicators_overlay ? {
    ema_20: chart_data.indicators_overlay.ema_20,
    ema_50: chart_data.indicators_overlay.ema_50,
    ema_100: chart_data.indicators_overlay.ema_100,
    ema_200: chart_data.indicators_overlay.ema_200,
  } : undefined;
  const vwap = chart_data?.indicators_overlay?.vwap;

  return (
    <Box sx={{ p: 2 }}>
      <Box sx={{ mb: 2 }}>
        <Typography variant="h5" gutterBottom>
          {alert.symbol} - {alert.pattern_label}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Alert ID: {alert.alert_id}
        </Typography>
        <Chip label={alert.signal} color={alert.signal === 'BUY' ? 'success' : 'error'} sx={{ mr: 1, mt: 1 }} />
        <Chip label={`Confidence: ${(alert.confidence * 100).toFixed(1)}%`} sx={{ mr: 1, mt: 1 }} />
        <Chip label={`Last Price: ${alert.last_price.toFixed(2)}`} sx={{ mr: 1, mt: 1 }} />
      </Box>

      <Paper sx={{ p: 2, mb: 2 }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)}>
          <Tab label="Price" />
          <Tab label="Indicators" />
          <Tab label="Volume Profile" />
          <Tab label="Validation" />
        </Tabs>

        {tab === 0 && (
          <Box sx={{ mt: 2 }}>
            <PriceChart ohlc={ohlc} ema={ema} vwap={vwap} />
          </Box>
        )}

        {tab === 1 && (
          <Grid container spacing={2} sx={{ mt: 1 }}>
            {/* @ts-ignore - MUI Grid API mismatch */}
            <Grid item xs={12} md={6}>
              <Paper sx={{ p: 2 }}>
                <Typography variant="h6" gutterBottom>Indicators</Typography>
                {alert.indicators && Object.keys(alert.indicators).length > 0 ? (
                  Object.keys(alert.indicators).map((key) => {
                    const value = alert.indicators[key];
                    let displayValue: string;
                    if (typeof value === 'object' && value !== null) {
                      // Handle complex indicators like MACD, Bollinger Bands
                      displayValue = JSON.stringify(value, null, 2);
                    } else if (typeof value === 'number') {
                      displayValue = value.toFixed(2);
                    } else {
                      displayValue = String(value);
                    }
                    return (
                      <Typography key={key} variant="body2" sx={{ mb: 0.5 }}>
                        <strong>{key.toUpperCase()}:</strong> {displayValue}
                      </Typography>
                    );
                  })
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No indicators available
                  </Typography>
                )}
              </Paper>
            </Grid>
            {/* @ts-ignore - MUI Grid API mismatch */}
            <Grid item xs={12} md={6}>
              {alert.instrument_type === 'OPTIONS' && alert.greeks && Object.keys(alert.greeks).length > 0 && (
                <Paper sx={{ p: 2 }}>
                  <Typography variant="h6" gutterBottom>Greeks</Typography>
                  {Object.keys(alert.greeks).map((key) => {
                    const value = alert.greeks[key];
                    const displayValue = typeof value === 'number' ? value.toFixed(4) : String(value);
                    const color = typeof value === 'number' && value < 0 ? 'error.main' : 'success.main';
                    return (
                      <Typography key={key} variant="body2" sx={{ mb: 0.5 }}>
                        <strong>{key.toUpperCase()}:</strong>{' '}
                        <span style={{ color: color === 'error.main' ? '#f44336' : '#4caf50' }}>
                          {displayValue}
                        </span>
                      </Typography>
                    );
                  })}
                </Paper>
              )}
            </Grid>
          </Grid>
        )}

        {tab === 2 && volume_profile && (
          <Box sx={{ mt: 2 }}>
            <VolumeProfileChart
              distribution={volume_profile.distribution || {}}
              poc_price={volume_profile.poc_price}
              value_area_low={volume_profile.value_area_low}
              value_area_high={volume_profile.value_area_high}
            />
          </Box>
        )}

        {tab === 3 && (
          <Box sx={{ mt: 2 }}>
            <Paper sx={{ p: 2 }}>
              <Typography variant="h6" gutterBottom>Validation</Typography>
              {validation && (
                <Box>
                  <Typography variant="body2">Status: {validation.status}</Typography>
                  {validation.confidence_score != null && (
                    <Typography variant="body2">Confidence Score: {(validation.confidence_score * 100).toFixed(1)}%</Typography>
                  )}
                  {validation.price_movement_pct != null && (
                    <Typography variant="body2">Price Move: {validation.price_movement_pct}%</Typography>
                  )}
                  {validation.duration_minutes != null && (
                    <Typography variant="body2">Duration: {validation.duration_minutes} minutes</Typography>
                  )}
                  {validation.max_move_pct != null && (
                    <Typography variant="body2">Max Move: {validation.max_move_pct}%</Typography>
                  )}
                  {validation.rolling_windows && typeof validation.rolling_windows === 'object' && (
                    <Box sx={{ mt: 2 }}>
                      <Typography variant="subtitle2" gutterBottom>Rolling Windows</Typography>
                      <Typography variant="body2">
                        Success: {validation.rolling_windows.success_count || 0} / {validation.rolling_windows.total_windows || 0}
                      </Typography>
                      <Typography variant="body2">
                        Success Ratio: {((validation.rolling_windows.success_ratio || 0) * 100).toFixed(1)}%
                      </Typography>
                    </Box>
                  )}
                </Box>
              )}
            </Paper>
          </Box>
        )}
      </Paper>
      <Box sx={{ mt: 2 }}>
        <Paper sx={{ p: 2, mb: 2 }}>
          <Typography variant="h6">News</Typography>
          <List>
            {news.map((item, index) => (
              <ListItem key={index} component="a" href={item.url} target="_blank" rel="noopener noreferrer" sx={{ color: 'inherit' }}>
                <ListItemText primary={item.headline} secondary={format(new Date(item.timestamp), 'yyyy-MM-dd HH:mm:ss')} />
              </ListItem>
            ))}
          </List>
        </Paper>
        <Paper sx={{ p: 2 }}>
          <Typography variant="h6">Validation</Typography>
          {validation && (
            <Box>
              <Typography variant="body2">Status: {validation.status}</Typography>
              {validation.confidence_score != null && (
                <Typography variant="body2">Confidence Score: {(validation.confidence_score * 100).toFixed(1)}%</Typography>
              )}
              {validation.price_movement_pct != null && (
                <Typography variant="body2">Price Move: {validation.price_movement_pct}%</Typography>
              )}
            </Box>
          )}
        </Paper>
      </Box>
      <Divider sx={{ my: 3 }} />
      <Box>
        <Typography variant="body2" component={Link} to="/" sx={{ textDecoration: 'none' }}>
          &larr; Back to Alerts
        </Typography>
      </Box>
    </Box>
  );
};

export default AlertDetail;