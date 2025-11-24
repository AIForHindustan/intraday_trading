import { create } from 'zustand';
import { alertsAPI, AlertQueryParams } from '../services/api';
import { adaptAlert } from '../services/adapters';

export interface Alert {
  alert_id: string;
  symbol: string;
  base_symbol: string;
  pattern: string;
  pattern_label: string;
  confidence: number;
  signal: string;
  last_price: number;
  timestamp: string;
  action: string;
  stop_loss?: number;
  target?: number;
  expiry_date?: string;
  instrument_type?: string;
  option_type?: string;
  strike?: number;
  indicators?: any;
  greeks?: any;
  volume_ratio?: number;
  expected_move?: number;
  news_count?: number;
  validation_status?: string;
  validation_confidence?: number | null;
}

interface AlertsState {
  alerts: Alert[];
  total: number;
  limit: number;
  offset: number;
  hasMore: boolean;
  loading: boolean;
  error: string | null;
  fetchAlerts: (params?: AlertQueryParams) => Promise<void>;
  appendAlert: (alert: Alert) => void;
  reset: () => void;
}

export const useAlertsStore = create<AlertsState>((set) => ({
  alerts: [],
  total: 0,
  limit: 100,
  offset: 0,
  hasMore: true,
  loading: false,
  error: null,
  fetchAlerts: async (params?: AlertQueryParams) => {
    set({ loading: true, error: null });
    try {
      const response = await alertsAPI.getAll(params);
      const data = response.data;
      // Adapt alerts using adapter to normalize fields
      const rawAlerts = data.alerts || data || [];
      const adaptedAlerts = Array.isArray(rawAlerts) ? rawAlerts.map(adaptAlert) : [];
      set({
        alerts: adaptedAlerts,
        total: data.total || adaptedAlerts.length,
        limit: data.limit,
        offset: data.offset,
        hasMore: data.has_more,
        loading: false
      });
    } catch (err: any) {
      console.error('Error fetching alerts:', err);
      // Extract error message - handle Pydantic validation errors and other formats
      let errorMessage = 'Failed to fetch alerts';
      if (err?.response?.data) {
        const data = err.response.data;
        if (typeof data.detail === 'string') {
          errorMessage = data.detail;
        } else if (Array.isArray(data.detail)) {
          // Pydantic validation errors are arrays
          errorMessage = data.detail.map((e: any) => {
            if (typeof e === 'string') return e;
            if (e?.msg) return e.msg;
            return JSON.stringify(e);
          }).join(', ');
        } else if (data.detail?.msg) {
          errorMessage = data.detail.msg;
        } else if (typeof data === 'string') {
          errorMessage = data;
        } else {
          errorMessage = JSON.stringify(data);
        }
      } else if (err?.message) {
        errorMessage = err.message;
      }
      set({ loading: false, error: errorMessage, alerts: [] });
      // TEMPORARILY DISABLED: Authentication is disabled, no redirect needed
      // if (err?.response?.status === 401) {
      //   localStorage.removeItem('access_token');
      //   window.location.href = '/login';
      // }
    }
  },
  appendAlert: (alert: Alert) => {
    // Ensure alert_id exists for frontend compatibility
    if (!alert.alert_id) {
      const timestamp = alert.timestamp || Date.now();
      alert.alert_id = `${alert.symbol}_${timestamp}`;
    }
    // Ensure required fields - only set signal if it's a valid trading signal
    if (!alert.signal) {
      const validSignals = ['BUY', 'SELL', 'HOLD', 'WATCH'];
      const candidateSignal = (alert as any).direction || (alert as any).action;
      // Only use if it's a valid signal, otherwise leave null
      if (candidateSignal && validSignals.includes(candidateSignal.toUpperCase())) {
        alert.signal = candidateSignal.toUpperCase();
      }
      // Don't default to NEUTRAL - leave as null if no valid signal
    } else if (alert.signal === 'NEUTRAL' || alert.signal === '') {
      // Remove invalid NEUTRAL signals
      alert.signal = '';
    }
    if (!alert.base_symbol) {
      // Extract base symbol (remove dates, strikes, etc.)
      const symbolStr = alert.symbol || '';
      // Remove exchange prefix (NFO:, NSE:, etc.)
      let cleaned = symbolStr.split(':').pop() || symbolStr;
      
      // Extract index name (NIFTY, BANKNIFTY, FINNIFTY, INDIA VIX)
      const indexMatch = cleaned.match(/^(NIFTY|BANKNIFTY|FINNIFTY|INDIA VIX)/i);
      if (indexMatch) {
        const indexName = indexMatch[1].toUpperCase();
        // Check if there's more after the index name (dates/strikes for options)
        const afterIndex = cleaned.substring(indexMatch[0].length).trim();
        if (/^\d/.test(afterIndex)) {
          // Has digits after index name - it's an options contract
          // Return just the index name
          alert.base_symbol = indexName === 'NIFTY' ? 'NIFTY 50' : indexName;
        } else if (afterIndex && !afterIndex.match(/^\s*50\s*$|^\s*BANK\s*$/i)) {
          // Has text after but not just "50" or "BANK" - might be options
          alert.base_symbol = indexName === 'NIFTY' ? 'NIFTY 50' : indexName;
        } else {
          // Pure index or index with standard suffix
          alert.base_symbol = cleaned.toUpperCase().trim();
        }
      } else {
        // Not a recognized index, use first part before any numbers
        const baseMatch = cleaned.match(/^([A-Z]+(?:\s+[A-Z]+)?)/);
        alert.base_symbol = baseMatch ? baseMatch[1] : cleaned;
      }
    }
    set((state) => ({ alerts: [alert, ...state.alerts], total: state.total + 1 }));
  },
  reset: () => set({ alerts: [], total: 0, limit: 100, offset: 0, hasMore: true })
}));