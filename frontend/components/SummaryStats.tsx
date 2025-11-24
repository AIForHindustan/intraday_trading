import React, { useEffect, useState } from 'react';
import { alertsAPI, validationAPI } from '../services/api';
import { Box, Paper, Typography, Grid } from '@mui/material';
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Legend } from 'recharts';

interface AlertsSummary {
  total_alerts: number;
  today_alerts: number;
  pattern_distribution: Record<string, number>;
  symbol_ranking: { symbol: string; count: number }[];
  confidence_distribution: Record<string, number>;
  instrument_type_distribution: Record<string, number>;
  news_enrichment_rate: number;
}

interface ValidationStats {
  total_validated: number;
  success_rate: number;
  average_confidence: number;
  pattern_performance: Record<string, { total: number; success: number; success_rate: number; avg_confidence: number }>;
  timeframe_analysis: Record<string, { success_rate: number }>;
}

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8e44ad', '#e74c3c'];

const SummaryStats: React.FC = () => {
  const [alertsSummary, setAlertsSummary] = useState<AlertsSummary | null>(null);
  const [validationStats, setValidationStats] = useState<ValidationStats | null>(null);

  useEffect(() => {
    (async () => {
      try {
        const [alertsRes, validationRes] = await Promise.all([
          alertsAPI.getStats().catch(e => ({ data: null })),
          validationAPI.getStats().catch(e => ({ data: null }))
        ]);
        console.log('📊 SummaryStats: Alerts response:', alertsRes.data);
        console.log('📊 SummaryStats: Validation response:', validationRes.data);
        setAlertsSummary(alertsRes.data);
        setValidationStats(validationRes.data || {
          total_validated: 0,
          success_rate: 0.0,
          average_confidence: 0.0,
          pattern_performance: {},
          timeframe_analysis: {}
        });
      } catch (err) {
        console.error('Failed to fetch summary statistics', err);
      }
    })();
  }, []);

  if (!alertsSummary) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '60vh' }}>
        <Typography>Loading statistics...</Typography>
      </Box>
    );
  }
  
  // Use default validation stats if not available
  const displayValidationStats = validationStats || {
    total_validated: 0,
    success_rate: 0.0,
    average_confidence: 0.0,
    pattern_performance: {},
    timeframe_analysis: {}
  };

  // Prepare chart data
  const patternData = Object.entries(alertsSummary.pattern_distribution || {}).map(([name, value]) => ({ name, value }));
  const confidenceData = Object.entries(alertsSummary.confidence_distribution || {}).map(([name, value]) => ({ name, value }));
  const instrumentData = Object.entries(alertsSummary.instrument_type_distribution || {}).map(([name, value]) => ({ name, value }));
  const symbolData = alertsSummary.symbol_ranking || [];

  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Summary Statistics
      </Typography>
      <Grid container spacing={2}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 300 }}>
            <Typography variant="subtitle1">Pattern Distribution</Typography>
            <ResponsiveContainer width="100%" height="85%">
              <PieChart>
                <Pie data={patternData} dataKey="value" nameKey="name" outerRadius={80} label>
                  {patternData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 300 }}>
            <Typography variant="subtitle1">Confidence Distribution</Typography>
            <ResponsiveContainer width="100%" height="85%">
              <PieChart>
                <Pie data={confidenceData} dataKey="value" nameKey="name" outerRadius={80} label>
                  {confidenceData.map((entry, index) => (
                    <Cell key={`cell2-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 300 }}>
            <Typography variant="subtitle1">Instrument Type Distribution</Typography>
            <ResponsiveContainer width="100%" height="85%">
              <PieChart>
                <Pie data={instrumentData} dataKey="value" nameKey="name" outerRadius={80} label>
                  {instrumentData.map((entry, index) => (
                    <Cell key={`cell3-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 2, height: 300 }}>
            <Typography variant="subtitle1">Top Symbols</Typography>
            <ResponsiveContainer width="100%" height="85%">
              <BarChart data={symbolData} margin={{ left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="symbol" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="count" fill="#8884d8" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default SummaryStats;