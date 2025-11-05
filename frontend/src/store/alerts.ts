import { create } from 'zustand';
import { alertsAPI, AlertQueryParams } from '../services/api';

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
      set({
        alerts: data.alerts || [],
        total: data.total,
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
      // If 401, clear token and redirect to login
      if (err?.response?.status === 401) {
        localStorage.removeItem('access_token');
        window.location.href = '/login';
      }
    }
  },
  appendAlert: (alert: Alert) => {
    // Ensure alert_id exists for frontend compatibility
    if (!alert.alert_id) {
      const timestamp = alert.timestamp || Date.now();
      alert.alert_id = `${alert.symbol}_${timestamp}`;
    }
    // Ensure required fields
    if (!alert.signal) {
      alert.signal = (alert as any).direction || (alert as any).action || 'NEUTRAL';
    }
    if (!alert.pattern_label) {
      alert.pattern_label = alert.pattern || 'Unknown Pattern';
    }
    if (!alert.base_symbol) {
      // Extract base symbol (remove dates, strikes, etc.)
      const symbolStr = alert.symbol || '';
      const baseMatch = symbolStr.match(/^([A-Z]+)/);
      alert.base_symbol = baseMatch ? baseMatch[1] : symbolStr.split(':').pop() || symbolStr;
    }
    set((state) => ({ alerts: [alert, ...state.alerts], total: state.total + 1 }));
  },
  reset: () => set({ alerts: [], total: 0, limit: 100, offset: 0, hasMore: true })
}));