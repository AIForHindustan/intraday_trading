import React, { useEffect, useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  CardHeader,
  TextField,
  Button,
  Typography,
  Stack,
  Chip,
  Alert,
  CircularProgress
} from '@mui/material';
import { brokersAPI, BrokerStatus } from '../services/api';

type BrokerKey = 'ZERODHA' | 'ANGEL_ONE';

interface BrokerFormState {
  token: string;
  clientCode?: string;
  loading: boolean;
  error?: string;
  status?: BrokerStatus;
}

interface LocalState {
  ZERODHA: BrokerFormState;
  ANGEL_ONE: BrokerFormState;
}

const emptyLocal: LocalState = {
  ZERODHA: { token: '', clientCode: '', loading: false },
  ANGEL_ONE: { token: '', clientCode: '', loading: false }
};

const formatStatus = (status?: BrokerStatus) => {
  if (!status) return { label: 'Not connected', color: 'default' as const };
  if (status.status === 'ACTIVE') return { label: 'Active', color: 'success' as const };
  if (status.status === 'EXPIRED') return { label: 'Expired', color: 'warning' as const };
  return { label: 'Disconnected', color: 'default' as const };
};

const formatExpiry = (status?: BrokerStatus) => {
  if (!status?.expires_at) return null;
  return `Valid until: ${new Date(status.expires_at).toLocaleString()}`;
};

const BrokerConnections: React.FC = () => {
  const [state, setState] = useState<LocalState>(emptyLocal);
  const [globalError, setGlobalError] = useState<string | null>(null);
  const [loadingInitial, setLoadingInitial] = useState(true);

  const loadStatus = async () => {
    try {
      setLoadingInitial(true);
      console.log('🔌 Loading broker status...');
      const statuses = await brokersAPI.getStatus();
      console.log('🔌 Broker status loaded:', statuses);

      setState((prev) => {
        const next = { ...prev };
        for (const s of statuses) {
          const key = s.broker as BrokerKey;
          next[key] = {
            ...next[key],
            status: s,
            error: undefined
          };
        }
        return next;
      });
    } catch (err: any) {
      console.error('❌ Failed to load broker status', err);
      console.error('❌ Error details:', {
        message: err?.message,
        response: err?.response?.data,
        status: err?.response?.status
      });
      setGlobalError(
        err?.response?.data?.detail || 
        err?.message || 
        'Unable to load broker connection status. Check backend /api/brokers/status.'
      );
      setLoadingInitial(false); // Make sure to set loading to false even on error
    } finally {
      setLoadingInitial(false);
    }
  };

  useEffect(() => {
    loadStatus();
  }, []);

  // Debug: Log when component mounts and unmounts
  useEffect(() => {
    console.log('🔌 BrokerConnections component mounted');
    return () => {
      console.log('🔌 BrokerConnections component unmounted');
    };
  }, []);

  const handleTokenChange = (broker: BrokerKey, token: string) => {
    setState((prev) => ({
      ...prev,
      [broker]: {
        ...prev[broker],
        token
      }
    }));
  };

  const handleClientCodeChange = (broker: BrokerKey, clientCode: string) => {
    setState((prev) => ({
      ...prev,
      [broker]: {
        ...prev[broker],
        clientCode
      }
    }));
  };

  const handleConnect = async (broker: BrokerKey) => {
    const brokerState = state[broker];
    if (!brokerState.token.trim()) {
      setState((prev) => ({
        ...prev,
        [broker]: {
          ...prev[broker],
          error: "Please paste today's access token first."
        }
      }));
      return;
    }
    if (broker === 'ANGEL_ONE' && !brokerState.clientCode?.trim()) {
      setState((prev) => ({
        ...prev,
        [broker]: {
          ...prev[broker],
          error: 'Please enter your Angel One client code.'
        }
      }));
      return;
    }

    setState((prev) => ({
      ...prev,
      [broker]: {
        ...prev[broker],
        loading: true,
        error: undefined
      }
    }));

    try {
      let status: BrokerStatus;

      if (broker === 'ZERODHA') {
        status = await brokersAPI.connectZerodha(brokerState.token.trim());
      } else {
        status = await brokersAPI.connectAngelOne(
          brokerState.token.trim(),
          brokerState.clientCode?.trim() || ''
        );
      }

      setState((prev) => ({
        ...prev,
        [broker]: {
          ...prev[broker],
          loading: false,
          token: '',
          clientCode: broker === 'ANGEL_ONE' ? '' : prev[broker].clientCode,
          status,
          error: status.status === 'ACTIVE' ? undefined : status.last_error || undefined
        }
      }));
    } catch (err: any) {
      console.error(`Failed to connect ${broker}`, err);
      setState((prev) => ({
        ...prev,
        [broker]: {
          ...prev[broker],
          loading: false,
          error:
            err?.response?.data?.detail ||
            err?.message ||
            `Failed to connect to ${broker}.`
        }
      }));
    }
  };

  const renderBrokerCard = (broker: BrokerKey, title: string, helper: string) => {
    const brokerState = state[broker];
    const { label, color } = formatStatus(brokerState.status);

    return (
      <Card variant="outlined" sx={{ flex: 1, minWidth: 320 }}>
        <CardHeader
          title={title}
          subheader={helper}
          action={
            <Chip
              label={label}
              color={color}
              variant={color === 'default' ? 'outlined' : 'filled'}
              size="small"
            />
          }
        />
        <CardContent>
          {formatExpiry(brokerState.status) && (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              {formatExpiry(brokerState.status)}
            </Typography>
          )}

          <TextField
            label="Today's Access Token"
            placeholder="Paste broker access token here"
            fullWidth
            size="small"
            value={brokerState.token}
            onChange={(e) => handleTokenChange(broker, e.target.value)}
            sx={{ mb: 1.5 }}
          />
          {broker === 'ANGEL_ONE' && (
            <TextField
              label="Client Code"
              placeholder="Enter your Angel One client code"
              fullWidth
              size="small"
              value={brokerState.clientCode || ''}
              onChange={(e) => handleClientCodeChange(broker, e.target.value)}
              sx={{ mb: 1.5 }}
            />
          )}

          {brokerState.error && (
            <Alert severity="error" sx={{ mb: 1 }}>
              {brokerState.error}
            </Alert>
          )}

          <Button
            variant="contained"
            color="primary"
            fullWidth
            disabled={brokerState.loading}
            onClick={() => handleConnect(broker)}
          >
            {brokerState.loading ? (
              <>
                <CircularProgress size={18} sx={{ mr: 1 }} />
                Testing & Activating…
              </>
            ) : (
              'Test & Activate'
            )}
          </Button>
        </CardContent>
      </Card>
    );
  };

  if (loadingInitial) {
    return (
      <Box sx={{ p: 3, display: 'flex', justifyContent: 'center' }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h5" gutterBottom>
        Broker Connections (Daily Tokens)
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Paste today&apos;s access token for each broker you want to use. We&apos;ll verify it once,
        store it securely, and use it for your robot during today&apos;s session.
      </Typography>

      {globalError && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          {globalError}
        </Alert>
      )}

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2}>
        {renderBrokerCard(
          'ZERODHA',
          'Zerodha (Kite Connect)',
          "1. Generate today's access token in Zerodha. 2. Paste it here and click Test & Activate."
        )}
        {renderBrokerCard(
          'ANGEL_ONE',
          'Angel One (SmartAPI)',
          "1. Generate today's access token in SmartAPI. 2. Paste it here and click Test & Activate."
        )}
      </Stack>
    </Box>
  );
};

export default BrokerConnections;
