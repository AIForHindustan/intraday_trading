import React, { useState, useEffect, useCallback } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Box,
  Paper,
  Typography,
  Grid,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Button,
  Chip,
  CircularProgress,
  Alert,
  Tabs,
  Tab,
} from '@mui/material';
import PriceChart from './PriceChart';
import VolumeProfileChart from './VolumeProfileChart';
import { chartsAPI, indicatorsAPI, greeksAPI, volumeProfileAPI, intradayAPI } from '../services/api';
import { adaptOhlcPayload, adaptOverlays } from '../services/adapters';

interface Instrument {
  token: number;
  symbol: string;
  name: string;
  exchange: string;
  instrument_type: string;
  segment: string;
}

interface InstrumentChartsProps {
  instruments?: Instrument[];
}

const InstrumentCharts: React.FC<InstrumentChartsProps> = ({ instruments: propInstruments }) => {
  const [searchParams, setSearchParams] = useSearchParams();
  const symbolFromUrl = searchParams.get('symbol') || '';
  
  const [instruments, setInstruments] = useState<Instrument[]>(propInstruments || []);
  const [selectedSymbol, setSelectedSymbol] = useState<string>(symbolFromUrl || '');
  const [chartData, setChartData] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [period, setPeriod] = useState<string>('7d');
  const [resolution, setResolution] = useState<string>('5m');
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [tab, setTab] = useState<number>(0); // 0: Price, 1: Indicators, 2: Volume Profile, 3: Greeks
  const [indicators, setIndicators] = useState<any>(null);
  const [greeks, setGreeks] = useState<any>(null);
  const [volumeProfile, setVolumeProfile] = useState<any>(null);
  
  // Update selectedSymbol when URL changes (only on mount or URL change, not when user selects)
  useEffect(() => {
    if (symbolFromUrl && symbolFromUrl !== selectedSymbol) {
      console.log('📊 InstrumentCharts: URL changed, updating selectedSymbol from URL:', symbolFromUrl);
      setSelectedSymbol(symbolFromUrl);
    }
  }, [symbolFromUrl]); // Only depend on symbolFromUrl, not selectedSymbol
  
  // Update URL when symbol changes (but avoid infinite loops)
  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbolFromUrl) {
      console.log('📊 InstrumentCharts: selectedSymbol changed, updating URL:', selectedSymbol);
      setSearchParams({ symbol: selectedSymbol }, { replace: true });
    }
  }, [selectedSymbol]); // Only depend on selectedSymbol, not symbolFromUrl

  // Fetch instruments if not provided (only run once on mount or when propInstruments changes)
  useEffect(() => {
    console.log('📊 InstrumentCharts: Fetching instruments, symbolFromUrl:', symbolFromUrl);
    
    if (!propInstruments || propInstruments.length === 0) {
      const fetchInstruments = async () => {
        try {
          console.log('📊 InstrumentCharts: Fetching instruments from API...');
          const response = await intradayAPI.getInstruments();
          const data = response.data;
          console.log('📊 InstrumentCharts: Fetched', data.instruments?.length || 0, 'instruments');
          
          if (data.instruments) {
            setInstruments(data.instruments);
            // Only set default symbol if no symbol is currently selected
            // Don't override user's selection
            if (data.instruments.length > 0 && !selectedSymbol) {
              if (symbolFromUrl) {
                // Check if symbol from URL exists in instruments
                const found = data.instruments.find((inst: Instrument) => inst.symbol === symbolFromUrl);
                console.log('📊 InstrumentCharts: Looking for symbol', symbolFromUrl, 'found:', !!found);
                if (found) {
                  console.log('📊 InstrumentCharts: Setting selectedSymbol to', symbolFromUrl);
                  setSelectedSymbol(symbolFromUrl);
                } else {
                  console.log('📊 InstrumentCharts: Symbol not found, using first instrument:', data.instruments[0].symbol);
                  setSelectedSymbol(data.instruments[0].symbol);
                }
              } else {
                console.log('📊 InstrumentCharts: No URL symbol, using first instrument:', data.instruments[0].symbol);
                setSelectedSymbol(data.instruments[0].symbol);
              }
            }
          }
        } catch (err) {
          console.error('❌ InstrumentCharts: Failed to fetch instruments:', err);
          setError('Failed to load instruments');
        }
      };
      fetchInstruments();
    } else if (propInstruments.length > 0) {
      console.log('📊 InstrumentCharts: Using propInstruments, count:', propInstruments.length);
      // Only set default symbol if no symbol is currently selected
      // Don't override user's selection
      if (!selectedSymbol) {
        if (symbolFromUrl) {
          const found = propInstruments.find(inst => inst.symbol === symbolFromUrl);
          if (found) {
            console.log('📊 InstrumentCharts: Setting selectedSymbol from URL:', symbolFromUrl);
            setSelectedSymbol(symbolFromUrl);
          } else {
            console.log('📊 InstrumentCharts: Setting selectedSymbol to first prop instrument:', propInstruments[0].symbol);
            setSelectedSymbol(propInstruments[0].symbol);
          }
        } else {
          console.log('📊 InstrumentCharts: Setting selectedSymbol to first prop instrument:', propInstruments[0].symbol);
          setSelectedSymbol(propInstruments[0].symbol);
        }
      }
    }
  }, [propInstruments]); // Only depend on propInstruments, NOT selectedSymbol or symbolFromUrl

  // Fetch all data types for the selected instrument
  const fetchAllData = useCallback(async (symbol: string) => {
    if (!symbol) return;
    
    setLoading(true);
    setError(null);
    
    try {
      // Fetch all data in parallel
      const [chartRes, indicatorsRes, volumeProfileRes, greeksRes] = await Promise.allSettled([
        chartsAPI.getData(symbol, {
          include_indicators: true,
          period,
          resolution,
        }),
        indicatorsAPI.getBySymbol(symbol).catch(() => ({ data: { indicators: {} } })),
        volumeProfileAPI.getData(symbol).catch(() => ({ data: { distribution: {}, poc_price: 0 } })),
        greeksAPI.getBySymbol(symbol).catch(() => ({ data: {} })),
      ]);
      
      // Process chart data
      if (chartRes.status === 'fulfilled' && chartRes.value.data) {
        const chartData = chartRes.value.data;
        if (chartData.ohlc && chartData.ohlc.length > 0) {
          setChartData(chartData);
        } else {
          setError(`No chart data available for ${symbol}`);
          setChartData(null);
        }
      } else {
        setError(`Failed to fetch chart data for ${symbol}`);
        setChartData(null);
      }
      
      // Process indicators
      if (indicatorsRes.status === 'fulfilled' && indicatorsRes.value.data) {
        setIndicators(indicatorsRes.value.data.indicators || indicatorsRes.value.data);
      } else {
        setIndicators(null);
      }
      
      // Process volume profile
      if (volumeProfileRes.status === 'fulfilled' && volumeProfileRes.value.data) {
        setVolumeProfile(volumeProfileRes.value.data);
      } else {
        setVolumeProfile(null);
      }
      
      // Process greeks (only for options)
      if (greeksRes.status === 'fulfilled' && greeksRes.value.data) {
        setGreeks(greeksRes.value.data);
      } else {
        setGreeks(null);
      }
      
    } catch (err: any) {
      console.error('❌ InstrumentCharts: Failed to fetch data', err);
      setError(err?.message || 'Failed to fetch instrument data');
      setChartData(null);
    } finally {
      setLoading(false);
    }
  }, [period, resolution]);

  useEffect(() => {
    if (selectedSymbol) {
      console.log('📊 InstrumentCharts: Fetching data for symbol:', selectedSymbol);
      fetchAllData(selectedSymbol);
    } else {
      console.log('📊 InstrumentCharts: No selectedSymbol, skipping data fetch');
    }
  }, [selectedSymbol, fetchAllData]);

  // Filter instruments based on search
  const filteredInstruments = instruments.filter(inst =>
    inst.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
    inst.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const selectedInstrument = instruments.find(inst => inst.symbol === selectedSymbol);

  return (
    <Box sx={{ width: '100%', p: 2 }}>
      <Typography variant="h5" gutterBottom>
        Intraday Crawler Instruments
      </Typography>
      
      <Paper sx={{ p: 2, mb: 2 }}>
        <Grid container spacing={2} sx={{ mb: 2 }}>
          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              label="Search Instruments"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Type to filter instruments..."
              size="small"
            />
          </Grid>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth>
              <InputLabel id="instrument-select-label">Select Instrument</InputLabel>
              <Select
                labelId="instrument-select-label"
                id="instrument-select"
                value={selectedSymbol || ''}
                onChange={(e) => {
                  const newSymbol = e.target.value as string;
                  console.log('📊 InstrumentCharts: User selected instrument:', newSymbol);
                  // Directly set the symbol - don't let any useEffect override this
                  setSelectedSymbol(newSymbol);
                }}
                label="Select Instrument"
                displayEmpty
                MenuProps={{
                  PaperProps: {
                    style: {
                      maxHeight: 400,
                    },
                  },
                  anchorOrigin: {
                    vertical: 'bottom',
                    horizontal: 'left',
                  },
                  transformOrigin: {
                    vertical: 'top',
                    horizontal: 'left',
                  },
                }}
                sx={{ zIndex: 1300 }}
              >
                {filteredInstruments.length === 0 ? (
                  <MenuItem disabled value="">
                    <em>No instruments found</em>
                  </MenuItem>
                ) : (
                  filteredInstruments.map((inst) => (
                    <MenuItem key={inst.token} value={inst.symbol}>
                      <Box sx={{ display: 'flex', flexDirection: 'column' }}>
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {inst.symbol}
                        </Typography>
                        {inst.name && (
                          <Typography variant="caption" color="text.secondary">
                            {inst.name}
                          </Typography>
                        )}
                      </Box>
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={3}>
            <FormControl fullWidth size="small">
              <InputLabel>Period</InputLabel>
              <Select value={period} onChange={(e) => setPeriod(e.target.value)} label="Period">
                <MenuItem value="1d">1 Day</MenuItem>
                <MenuItem value="7d">7 Days</MenuItem>
                <MenuItem value="1w">1 Week</MenuItem>
                <MenuItem value="1m">1 Month</MenuItem>
                <MenuItem value="3m">3 Months</MenuItem>
                <MenuItem value="1y">1 Year</MenuItem>
              </Select>
            </FormControl>
          </Grid>
          <Grid item xs={12} md={2}>
            <FormControl fullWidth size="small">
              <InputLabel>Resolution</InputLabel>
              <Select value={resolution} onChange={(e) => setResolution(e.target.value)} label="Resolution">
                <MenuItem value="1m">1m</MenuItem>
                <MenuItem value="5m">5m</MenuItem>
                <MenuItem value="15m">15m</MenuItem>
                <MenuItem value="30m">30m</MenuItem>
                <MenuItem value="1h">1h</MenuItem>
                <MenuItem value="day">Day</MenuItem>
              </Select>
            </FormControl>
          </Grid>
        </Grid>

        {selectedInstrument && (
          <Box sx={{ mb: 2 }}>
            <Chip label={`Token: ${selectedInstrument.token}`} size="small" sx={{ mr: 1 }} />
            <Chip label={`Exchange: ${selectedInstrument.exchange}`} size="small" sx={{ mr: 1 }} />
            <Chip label={`Type: ${selectedInstrument.instrument_type}`} size="small" />
          </Box>
        )}
      </Paper>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      {loading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
          <CircularProgress />
        </Box>
      )}

      {selectedSymbol && !loading && (
        <Paper sx={{ p: 2 }}>
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
            <Typography variant="h6">
              {selectedSymbol}
              {chartData?.data_type && (
                <Chip
                  label={chartData.data_type}
                  size="small"
                  color={chartData.data_type === 'zerodha_api' ? 'success' : 'default'}
                  sx={{ ml: 2 }}
                />
              )}
            </Typography>
          </Box>
          
          <Tabs value={tab} onChange={(_, v) => setTab(v)} sx={{ mb: 2 }}>
            <Tab label="Price Chart" />
            <Tab label="Indicators" />
            <Tab label="Volume Profile" />
            {selectedInstrument?.instrument_type === 'OPTIONS' && <Tab label="Greeks" />}
          </Tabs>

          {tab === 0 && (
            <Box sx={{ width: '100%', height: 500, mt: 2 }}>
              {chartData ? (() => {
                const ohlc = adaptOhlcPayload(chartData);
                const overlay = adaptOverlays(chartData);
                const ema = overlay.ema_20?.length > 0 ? {
                  ema_20: overlay.ema_20,
                  ema_50: overlay.ema_50,
                  ema_100: overlay.ema_100,
                  ema_200: overlay.ema_200,
                } : undefined;
                const vwap = overlay.vwap;

                if (ohlc.length > 0) {
                  return (
                    <PriceChart
                      ohlc={ohlc}
                      ema={ema}
                      vwap={vwap}
                      height={500}
                      symbol={selectedSymbol}
                      enableRealtime={true}
                    />
                  );
                } else {
                  return (
                    <Box sx={{ p: 3, textAlign: 'center' }}>
                      <Typography variant="body1" color="text.secondary">
                        No chart data available
                      </Typography>
                    </Box>
                  );
                }
              })() : (
                <Box sx={{ p: 3, textAlign: 'center' }}>
                  <Typography variant="body1" color="text.secondary">
                    Loading chart data...
                  </Typography>
                </Box>
              )}
            </Box>
          )}

          {tab === 1 && (
            <Grid container spacing={2} sx={{ mt: 1 }}>
              <Grid item xs={12} md={6}>
                <Paper sx={{ p: 2 }}>
                  <Typography variant="h6" gutterBottom>Technical Indicators</Typography>
                  {indicators && Object.keys(indicators).length > 0 ? (
                    Object.keys(indicators).map((key) => {
                      const value = indicators[key];
                      let displayValue: string;
                      if (typeof value === 'object' && value !== null) {
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
            </Grid>
          )}

          {tab === 2 && volumeProfile && (
            <Box sx={{ mt: 2, minHeight: 300 }}>
              {volumeProfile.distribution && Object.keys(volumeProfile.distribution).length > 0 ? (
                <VolumeProfileChart
                  distribution={volumeProfile.distribution || {}}
                  poc_price={volumeProfile.poc_price}
                  value_area_low={volumeProfile.value_area_low}
                  value_area_high={volumeProfile.value_area_high}
                  height={300}
                />
              ) : (
                <Paper sx={{ p: 3, textAlign: 'center' }}>
                  <Typography variant="body1" color="text.secondary">
                    No volume profile data available
                  </Typography>
                </Paper>
              )}
            </Box>
          )}

          {tab === 3 && selectedInstrument?.instrument_type === 'OPTIONS' && (
            <Grid container spacing={2} sx={{ mt: 1 }}>
              <Grid item xs={12} md={6}>
                {greeks && Object.keys(greeks).length > 0 ? (
                  <Paper sx={{ p: 2 }}>
                    <Typography variant="h6" gutterBottom>Greeks</Typography>
                    {Object.keys(greeks).map((key) => {
                      const value = greeks[key];
                      const displayValue = typeof value === 'number' ? value.toFixed(4) : String(value);
                      const color = typeof value === 'number' && value < 0 ? '#f44336' : '#4caf50';
                      return (
                        <Typography key={key} variant="body2" sx={{ mb: 0.5 }}>
                          <strong>{key.toUpperCase()}:</strong>{' '}
                          <span style={{ color }}>{displayValue}</span>
                        </Typography>
                      );
                    })}
                  </Paper>
                ) : (
                  <Paper sx={{ p: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      No Greeks data available
                    </Typography>
                  </Paper>
                )}
              </Grid>
            </Grid>
          )}
        </Paper>
      )}

    </Box>
  );
};

export default InstrumentCharts;
