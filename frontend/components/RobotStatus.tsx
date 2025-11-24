import React, { useEffect, useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Stack,
  Chip,
  CircularProgress,
  Alert
} from '@mui/material';
import { AccountBalance, TrendingUp, TrendingDown, ShoppingCart } from '@mui/icons-material';
import { robotAPI, RobotStatus } from '../services/api';

/**
 * Robot Status Panel
 * 
 * Displays real-time trading robot status including:
 * - Available margin
 * - Margin used
 * - Safe capital (1/3 rule)
 * - Number of open positions
 * 
 * This is an optional component that can be added to Dashboard.tsx
 */

const RobotStatus: React.FC = () => {
  const [status, setStatus] = useState<RobotStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadStatus = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Backend endpoint: GET /api/robot/status
      const data = await robotAPI.getStatus();
      setStatus(data);
    } catch (err: any) {
      console.error('Failed to load robot status', err);
      // Don't show error if endpoint doesn't exist yet (404)
      if (err?.response?.status !== 404) {
        setError(err?.message || 'Unable to load robot status');
      }
      setStatus(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStatus();
    // Refresh every 30 seconds
    const interval = setInterval(loadStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0
    }).format(amount);
  };

  const calculateSafeCapital = (totalCapital: number) => {
    // 1/3 rule: Only use 1/3 of total capital for trading
    return Math.floor(totalCapital / 3);
  };

  const getMarginUtilizationColor = (pct: number) => {
    if (pct >= 80) return 'error';
    if (pct >= 60) return 'warning';
    return 'success';
  };

  if (loading) {
    return (
      <Card variant="outlined">
        <CardContent>
          <Box sx={{ display: 'flex', justifyContent: 'center', p: 2 }}>
            <CircularProgress size={24} />
          </Box>
        </CardContent>
      </Card>
    );
  }

  if (error) {
    return (
      <Card variant="outlined">
        <CardContent>
          <Alert severity="warning" sx={{ mb: 0 }}>
            {error}
          </Alert>
        </CardContent>
      </Card>
    );
  }

  if (!status) {
    return (
      <Card variant="outlined">
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Robot Status
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Robot status will appear here once the backend endpoint is implemented.
          </Typography>
          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
            Expected endpoint: GET /api/robot/status
          </Typography>
        </CardContent>
      </Card>
    );
  }

  const totalCapital = status.total_capital || (status.available_margin + status.margin_used);
  const safeCapital = status.safe_capital || calculateSafeCapital(totalCapital);
  const marginUtilization = status.margin_utilization_pct || 
    (status.margin_used / totalCapital) * 100;

  return (
    <Card variant="outlined">
      <CardContent>
        <Typography variant="h6" gutterBottom>
          Robot Status
        </Typography>

        <Stack spacing={2} sx={{ mt: 1 }}>
          {/* Available Margin */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <AccountBalance color="primary" fontSize="small" />
              <Typography variant="body2" color="text.secondary">
                Available Margin
              </Typography>
            </Box>
            <Typography variant="body1" fontWeight="medium">
              {formatCurrency(status.available_margin)}
            </Typography>
          </Box>

          {/* Margin Used */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <TrendingUp color="action" fontSize="small" />
              <Typography variant="body2" color="text.secondary">
                Margin Used
              </Typography>
            </Box>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography variant="body1" fontWeight="medium">
                {formatCurrency(status.margin_used)}
              </Typography>
              <Chip
                label={`${marginUtilization.toFixed(1)}%`}
                color={getMarginUtilizationColor(marginUtilization) as any}
                size="small"
              />
            </Box>
          </Box>

          {/* Safe Capital (1/3 Rule) */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <TrendingDown color="success" fontSize="small" />
              <Typography variant="body2" color="text.secondary">
                Safe Capital (1/3 Rule)
              </Typography>
            </Box>
            <Typography variant="body1" fontWeight="medium" color="success.main">
              {formatCurrency(safeCapital)}
            </Typography>
          </Box>

          {/* Open Positions */}
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <ShoppingCart color="primary" fontSize="small" />
              <Typography variant="body2" color="text.secondary">
                Open Positions
              </Typography>
            </Box>
            <Chip
              label={status.open_positions}
              color={status.open_positions > 0 ? 'primary' : 'default'}
              size="small"
            />
          </Box>

          {/* Margin Utilization Bar */}
          <Box sx={{ mt: 1 }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
              <Typography variant="caption" color="text.secondary">
                Margin Utilization
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {formatCurrency(status.margin_used)} / {formatCurrency(totalCapital)}
              </Typography>
            </Box>
            <Box
              sx={{
                width: '100%',
                height: 8,
                backgroundColor: 'grey.200',
                borderRadius: 1,
                overflow: 'hidden'
              }}
            >
              <Box
                sx={{
                  width: `${Math.min(marginUtilization, 100)}%`,
                  height: '100%',
                  backgroundColor:
                    marginUtilization >= 80
                      ? 'error.main'
                      : marginUtilization >= 60
                      ? 'warning.main'
                      : 'success.main',
                  transition: 'width 0.3s ease'
                }}
              />
            </Box>
          </Box>
        </Stack>
      </CardContent>
    </Card>
  );
};

export default RobotStatus;

