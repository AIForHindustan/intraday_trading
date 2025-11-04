import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAlertsStore } from '../store/alerts';
import { subscribeAlerts } from '../services/socket';
import {
  Box,
  Paper,
  TextField,
  MenuItem,
  FormControl,
  InputLabel,
  Select,
  Slider,
  Button,
  Typography
} from '@mui/material';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { format } from 'date-fns';

interface FilterState {
  symbol: string;
  pattern: string;
  min_confidence: number;
  date_from: string;
  date_to: string;
}

const patternOptions = [
  { value: '', label: 'All' },
  { value: 'volume_profile_breakout', label: 'Volume Profile Breakout' },
  { value: 'volume_spike', label: 'Volume Spike' },
  { value: 'breakout', label: 'Breakout' },
  { value: 'volume_breakout', label: 'Volume Breakout' }
];

const AlertList: React.FC = () => {
  const navigate = useNavigate();
  const { alerts, fetchAlerts, appendAlert, loading, total } = useAlertsStore();
  const [filters, setFilters] = useState<FilterState>({
    symbol: '',
    pattern: '',
    min_confidence: 0,
    date_from: '',
    date_to: ''
  });
  const [subscribed, setSubscribed] = useState(false);

  useEffect(() => {
    // Fetch initial alerts with default filters
    fetchAlerts({ limit: 100, offset: 0 });
  }, [fetchAlerts]);

  // Subscribe to new alerts via WebSocket
  useEffect(() => {
    if (!subscribed) {
      const unsubscribe = subscribeAlerts({ symbol: filters.symbol || undefined, pattern: filters.pattern || undefined }, (data) => {
        appendAlert(data);
      });
      setSubscribed(true);
      return () => {
        unsubscribe();
        setSubscribed(false);
      };
    }
  }, [appendAlert, filters, subscribed]);

  const handleFilterChange = <K extends keyof FilterState>(key: K, value: FilterState[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value }));
  };

  const applyFilters = () => {
    const params: any = {};
    if (filters.symbol) params.symbol = filters.symbol;
    if (filters.pattern) params.pattern = filters.pattern;
    if (filters.min_confidence > 0) params.min_confidence = filters.min_confidence;
    if (filters.date_from) params.date_from = filters.date_from;
    if (filters.date_to) params.date_to = filters.date_to;
    fetchAlerts({ ...params, limit: 100, offset: 0 });
  };

  const columns: GridColDef[] = [
    { field: 'symbol', headerName: 'Symbol', width: 130 },
    { field: 'pattern_label', headerName: 'Pattern', width: 200 },
    {
      field: 'confidence',
      headerName: 'Confidence',
      width: 150,
      valueFormatter: ({ value }) => `${(value * 100).toFixed(1)}%`
    },
    {
      field: 'signal',
      headerName: 'Signal',
      width: 100,
      renderCell: ({ value }) => (
        <Typography sx={{ color: value === 'BUY' ? 'success.main' : 'error.main' }}>{value}</Typography>
      )
    },
    { field: 'last_price', headerName: 'Price', width: 120, valueFormatter: ({ value }) => value.toFixed(2) },
    {
      field: 'timestamp',
      headerName: 'Time',
      width: 180,
      valueFormatter: ({ value }) => format(new Date(value), 'yyyy-MM-dd HH:mm:ss')
    },
    {
      field: 'action',
      headerName: 'Action',
      width: 100
    }
  ];

  return (
    <Box>
      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" gutterBottom>
          Alerts ({total})
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2, mb: 2 }}>
          <TextField
            label="Symbol"
            size="small"
            value={filters.symbol}
            onChange={(e) => handleFilterChange('symbol', e.target.value)}
          />
          <FormControl size="small" sx={{ minWidth: 180 }}>
            <InputLabel id="pattern-label">Pattern</InputLabel>
            <Select
              labelId="pattern-label"
              label="Pattern"
              value={filters.pattern}
              onChange={(e) => handleFilterChange('pattern', e.target.value as string)}
            >
              {patternOptions.map((opt) => (
                <MenuItem key={opt.value} value={opt.value}>
                  {opt.label}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <Box sx={{ width: 200 }}>
            <Typography id="confidence-slider" gutterBottom>
              Min Confidence
            </Typography>
            <Slider
              aria-labelledby="confidence-slider"
              min={0}
              max={1}
              step={0.05}
              value={filters.min_confidence}
              onChange={(_, value) => handleFilterChange('min_confidence', value as number)}
              valueLabelDisplay="auto"
              valueLabelFormat={(value) => `${(value * 100).toFixed(0)}%`}
            />
          </Box>
          {/* Date fields could be replaced with DatePicker components */}
          <TextField
            label="From"
            type="datetime-local"
            size="small"
            InputLabelProps={{ shrink: true }}
            value={filters.date_from}
            onChange={(e) => handleFilterChange('date_from', e.target.value)}
          />
          <TextField
            label="To"
            type="datetime-local"
            size="small"
            InputLabelProps={{ shrink: true }}
            value={filters.date_to}
            onChange={(e) => handleFilterChange('date_to', e.target.value)}
          />
          <Button variant="contained" color="primary" onClick={applyFilters}>
            Apply
          </Button>
        </Box>
        <div style={{ height: 600, width: '100%' }}>
          <DataGrid
            rows={alerts}
            columns={columns}
            getRowId={(row) => row.alert_id}
            loading={loading}
            onRowClick={(params) => navigate(`/alerts/${params.row.alert_id}`)}
            pageSizeOptions={[25, 50, 100]}
          />
        </div>
      </Paper>
    </Box>
  );
};

export default AlertList;