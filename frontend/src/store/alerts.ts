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
      set({ loading: false, error: err.message || 'Failed to fetch alerts' });
    }
  },
  appendAlert: (alert: Alert) => {
    set((state) => ({ alerts: [alert, ...state.alerts], total: state.total + 1 }));
  },
  reset: () => set({ alerts: [], total: 0, limit: 100, offset: 0, hasMore: true })
}));