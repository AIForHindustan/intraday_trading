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
  ListItemText
} from '@mui/material';
import Plot from 'react-plotly.js';
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

  useEffect(() => {
    if (!alertId) return;
    let mounted = true;
    (async () => {
      try {
        const response = await alertsAPI.getById(alertId);
        if (!mounted) return;
        const alertData = response.data;
        // Fetch additional chart and volume data
        const [chartRes, volumeRes, newsRes, validationRes] = await Promise.all([
          chartsAPI.getData(alertData.alert.symbol, { include_indicators: true }),
          volumeProfileAPI.getData(alertData.alert.symbol),
          newsAPI.getBySymbol(alertData.alert.base_symbol),
          validationAPI.getByAlertId(alertId)
        ]);
        if (!mounted) return;
        setData({
          alert: alertData.alert,
          chart_data: chartRes.data,
          volume_profile: volumeRes.data,
          news: newsRes.data.news,
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

  // Prepare candlestick data for Plotly
  const ohlc = chart_data?.data || [];
  const trace = {
    x: ohlc.map((d: any) => d.timestamp),
    open: ohlc.map((d: any) => d.open),
    high: ohlc.map((d: any) => d.high),
    low: ohlc.map((d: any) => d.low),
    close: ohlc.map((d: any) => d.close),
    type: 'candlestick',
    name: alert.symbol,
    increasing: { line: { color: '#4caf50' } },
    decreasing: { line: { color: '#f44336' } }
  };
  const indicatorTraces: any[] = [];
  if (chart_data?.indicators_overlay) {
    Object.keys(chart_data.indicators_overlay).forEach((key) => {
      const arr = chart_data.indicators_overlay[key];
      indicatorTraces.push({
        x: ohlc.map((d: any) => d.timestamp),
        y: arr,
        type: 'scatter',
        mode: 'lines',
        name: key.toUpperCase(),
        line: { width: 1 }
      });
    });
  }
  const layout = {
    title: `${alert.symbol} Price Chart`,
    xaxis: { type: 'date' },
    yaxis: { title: 'Price' },
    showlegend: true,
    margin: { l: 40, r: 40, t: 40, b: 40 },
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent'
  };

  return (
    <Box>
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
      <Grid container spacing={2}>
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 2 }}>
            <Plot
              data={[trace, ...indicatorTraces]}
              layout={layout as any}
              config={{ responsive: true, displaylogo: false }}
              style={{ width: '100%', height: '400px' }}
            />
          </Paper>
        </Grid>
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 2, mb: 2 }}>
            <Typography variant="h6">Indicators</Typography>
            {alert.indicators && Object.keys(alert.indicators).map((key) => (
              <Typography key={key} variant="body2">
                {key.toUpperCase()}: {JSON.stringify(alert.indicators[key])}
              </Typography>
            ))}
          </Paper>
          {alert.instrument_type === 'OPTIONS' && alert.greeks && (
            <Paper sx={{ p: 2, mb: 2 }}>
              <Typography variant="h6">Greeks</Typography>
              {Object.keys(alert.greeks).map((key) => (
                <Typography key={key} variant="body2">
                  {key.toUpperCase()}: {alert.greeks[key]}
                </Typography>
              ))}
            </Paper>
          )}
          <Paper sx={{ p: 2 }}>
            <Typography variant="h6">Volume Profile</Typography>
            {volume_profile && (
              <Box>
                <Typography variant="body2">POC: {volume_profile.poc_price}</Typography>
                <Typography variant="body2">Value Area High: {volume_profile.value_area_high}</Typography>
                <Typography variant="body2">Value Area Low: {volume_profile.value_area_low}</Typography>
              </Box>
            )}
          </Paper>
        </Grid>
      </Grid>
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