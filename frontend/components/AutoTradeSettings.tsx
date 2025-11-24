import React, { useEffect, useState } from 'react';
import {
  Box,
  Typography,
  Card,
  CardHeader,
  CardContent,
  Grid,
  Switch,
  FormControlLabel,
  TextField,
  MenuItem,
  Button,
  Alert,
  Stack,
  Chip,
  CircularProgress
} from '@mui/material';
import { autoTradeAPI, StrategyRule } from '../services/api';

/**
 * AutoTradeSettings Component
 * 
 * Displays per-pattern strategy controls for auto-trading.
 * 
 * Pattern Names (must match pattern_registry_config.json):
 * - volume_spike
 * - volume_breakout
 * - volume_price_divergence
 * - momentum_continuation
 * - breakout_pattern
 * - reversal_pattern
 * - spring_pattern
 * - coil_pattern
 * - hidden_accumulation
 * - kow_signal_straddle
 * - ict_iron_condor
 * 
 * The backend /auto_trade/rules endpoint should return StrategyRule objects
 * with pattern_type matching these exact names from the pattern registry.
 */

const AutoTradeSettings: React.FC = () => {
  const [rules, setRules] = useState<StrategyRule[]>([]);
  const [loading, setLoading] = useState(true);
  const [savingId, setSavingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);

  const loadRules = async () => {
    try {
      setLoading(true);
      const data = await autoTradeAPI.getStrategyRules();
      setRules(data);
      setError(null);
    } catch (err: any) {
      console.error('Failed to load strategy rules', err);
      setError(
        err?.message ||
          'Unable to load auto-trade settings. Check /auto_trade/rules on backend.'
      );
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadRules();
  }, []);

  const updateRuleField = <K extends keyof StrategyRule>(
    id: string,
    field: K,
    value: StrategyRule[K]
  ) => {
    setRules((prev) =>
      prev.map((r) => (r.id === id ? { ...r, [field]: value } : r))
    );
  };

  const handleSave = async (rule: StrategyRule) => {
    setSavingId(rule.id);
    setSaveMessage(null);
    try {
      const saved = await autoTradeAPI.saveStrategyRule(rule);
      setRules((prev) =>
        prev.map((r) => (r.id === saved.id ? saved : r))
      );
      setSaveMessage(`Saved settings for ${saved.display_name || saved.pattern_type}.`);
    } catch (err: any) {
      console.error('Failed to save strategy rule', err);
      setError(
        err?.response?.data?.detail ||
          err?.message ||
          `Failed to save strategy: ${rule.display_name || rule.pattern_type}.`
      );
    } finally {
      setSavingId(null);
    }
  };

  if (loading) {
    return (
      <Box sx={{ p: 3, display: 'flex', justifyContent: 'center' }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h5" gutterBottom>
        My Algo Strategies
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Enable or disable each pattern, choose your broker, and set conservative risk limits.
        The robot will only place orders within these limits when signals fire.
      </Typography>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {saveMessage && (
        <Alert severity="success" sx={{ mb: 2 }}>
          {saveMessage}
        </Alert>
      )}

      <Grid container spacing={2}>
        {rules.map((rule) => {
          const activeChipColor =
            rule.enabled && rule.auto_execute ? 'success' : rule.enabled ? 'info' : 'default';

          return (
            <Grid item xs={12} md={6} key={rule.id}>
              <Card variant="outlined">
                <CardHeader
                  title={rule.display_name || rule.pattern_type}
                  subheader={rule.strategy_id}
                  action={
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Chip
                        size="small"
                        label={
                          rule.enabled
                            ? rule.auto_execute
                              ? 'Auto mode'
                              : 'Signals only'
                            : 'Disabled'
                        }
                        color={activeChipColor as any}
                        variant={activeChipColor === 'default' ? 'outlined' : 'filled'}
                      />
                    </Stack>
                  }
                />
                <CardContent>
                  <Stack spacing={1.5}>
                    <FormControlLabel
                      control={
                        <Switch
                          checked={rule.enabled}
                          onChange={(e) =>
                            updateRuleField(rule.id, 'enabled', e.target.checked)
                          }
                        />
                      }
                      label="Use this strategy"
                    />

                    <TextField
                      select
                      label="Broker"
                      size="small"
                      fullWidth
                      value={rule.broker || ''}
                      onChange={(e) =>
                        updateRuleField(
                          rule.id,
                          'broker',
                          e.target.value as StrategyRule['broker']
                        )
                      }
                    >
                      <MenuItem value="">(Select broker)</MenuItem>
                      <MenuItem value="ZERODHA">Zerodha</MenuItem>
                      <MenuItem value="ANGEL_ONE">Angel One</MenuItem>
                    </TextField>

                    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                      <TextField
                        label="Max capital per trade (₹)"
                        type="number"
                        size="small"
                        fullWidth
                        value={rule.max_capital_per_trade ?? ''}
                        onChange={(e) =>
                          updateRuleField(
                            rule.id,
                            'max_capital_per_trade',
                            e.target.value ? Number(e.target.value) : null
                          )
                        }
                      />
                      <TextField
                        label="Max lots per trade"
                        type="number"
                        size="small"
                        fullWidth
                        value={rule.max_lots_per_trade ?? ''}
                        onChange={(e) =>
                          updateRuleField(
                            rule.id,
                            'max_lots_per_trade',
                            e.target.value ? Number(e.target.value) : null
                          )
                        }
                      />
                    </Stack>

                    <TextField
                      label="Daily max loss (₹)"
                      type="number"
                      size="small"
                      fullWidth
                      value={rule.daily_loss_limit ?? ''}
                      onChange={(e) =>
                        updateRuleField(
                          rule.id,
                          'daily_loss_limit',
                          e.target.value ? Number(e.target.value) : null
                        )
                      }
                    />

                    <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1}>
                      <TextField
                        select
                        label="Stop-loss mode"
                        size="small"
                        fullWidth
                        value={rule.stop_loss_mode}
                        onChange={(e) =>
                          updateRuleField(
                            rule.id,
                            'stop_loss_mode',
                            e.target.value as StrategyRule['stop_loss_mode']
                          )
                        }
                      >
                        <MenuItem value="FROM_ALERT">Use stop-loss from alert</MenuItem>
                        <MenuItem value="CUSTOM">Custom % (below entry)</MenuItem>
                      </TextField>
                      <TextField
                        label="Custom SL (%)"
                        type="number"
                        size="small"
                        fullWidth
                        disabled={rule.stop_loss_mode !== 'CUSTOM'}
                        value={rule.custom_stop_loss_pct ?? ''}
                        onChange={(e) =>
                          updateRuleField(
                            rule.id,
                            'custom_stop_loss_pct',
                            e.target.value ? Number(e.target.value) : null
                          )
                        }
                      />
                    </Stack>

                    <FormControlLabel
                      control={
                        <Switch
                          checked={rule.auto_execute}
                          onChange={(e) =>
                            updateRuleField(rule.id, 'auto_execute', e.target.checked)
                          }
                        />
                      }
                      label="Auto mode (let robot place orders when signals fire)"
                    />

                    <Box sx={{ textAlign: 'right', mt: 1 }}>
                      <Button
                        variant="contained"
                        size="small"
                        onClick={() => handleSave(rule)}
                        disabled={savingId === rule.id}
                      >
                        {savingId === rule.id ? (
                          <>
                            <CircularProgress size={16} sx={{ mr: 1 }} />
                            Saving…
                          </>
                        ) : (
                          'Save Settings'
                        )}
                      </Button>
                    </Box>
                  </Stack>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>
    </Box>
  );
};

export default AutoTradeSettings;

