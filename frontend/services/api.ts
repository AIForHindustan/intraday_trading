import axios from 'axios';

// Origin-aware API base URL resolution
// Handles local network, external access, and VPN/tunnel scenarios
// Supports explicit Vite overrides via VITE_API_URL
function getApiBaseUrl(): string {
  // ALWAYS use relative path - Cloudflare tunnel ingress handles routing
  // /api/* routes to backend (port 8000), everything else to frontend (port 3000)
  // This works for both localhost and tunnel access
  
  // Only use explicit VITE_API_URL if set (for development/testing overrides)
  if (import.meta.env.VITE_API_URL) {
    const url = import.meta.env.VITE_API_URL.replace(/\/+$/, '');
    console.log('🔧 Using explicit VITE_API_URL override:', url);
    return url;
  }
  
  // Default: use relative path - works with:
  // 1. Vite proxy (localhost development)
  // 2. Cloudflare tunnel ingress routing (production/external access)
  console.log('🔌 Using relative API path: /api');
  return '/api';
}

// Compute API base URL dynamically (not cached at module load)
// This ensures tunnel detection works even if browser cached old code
function getCurrentApiBase(): string {
  return getApiBaseUrl();
}

  // Debug logging for API base URL (helpful for troubleshooting)
  if (typeof window !== 'undefined') {
    const currentBase = getCurrentApiBase();
    const hostname = window.location.hostname;
    const isTunnel = hostname.includes('trycloudflare.com') || 
                     hostname.includes('cloudflare.com') ||
                     hostname.includes('loca.lt');
    
    console.log('🔌 API Base URL:', currentBase);
    console.log('🔌 Hostname:', hostname);
    console.log('🔌 Protocol:', window.location.protocol);
    console.log('🔌 Is Tunnel:', isTunnel);
    console.log('🔌 Full URL:', window.location.href);
    console.log('🔌 VITE_API_URL env:', import.meta.env.VITE_API_URL || 'NOT SET');
    console.log('🔌 import.meta.env keys:', Object.keys(import.meta.env).filter(k => k.startsWith('VITE')));
    
    // Warn if using localhost URL on external access
    if (currentBase.includes('localhost:8000') && isTunnel) {
      console.error('❌ ERROR: Using localhost URL on tunnel! This will fail for external users.');
      console.error('❌ VITE_API_URL should be set to backend tunnel URL');
      console.error('❌ Current VITE_API_URL:', import.meta.env.VITE_API_URL || 'NOT SET');
    }
  }

const api = axios.create({
  baseURL: getCurrentApiBase(), // Compute dynamically
  timeout: 20000,             // 20s timeout
  headers: {
    'Content-Type': 'application/json'
  },
  withCredentials: false      // set true ONLY if using cookie auth
});

// Override baseURL on each request to ensure it's always current
api.interceptors.request.use((config) => {
  // Recompute baseURL on each request to handle tunnel detection
  const newBaseURL = getCurrentApiBase();
  config.baseURL = newBaseURL;
  
  // Debug log for tunnel detection
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname;
    if (hostname.includes('trycloudflare.com') || hostname.includes('cloudflare.com')) {
      console.log('🌐 Tunnel request - baseURL:', newBaseURL, 'for', config.url);
    }
  }
  
  return config;
});

// Attach JWT to every call
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Request interceptor for debugging (runs after baseURL override)
api.interceptors.request.use((config) => {
  if (typeof window !== 'undefined') {
    console.log('🌐 API Request:', config.method?.toUpperCase(), config.url, '→', config.baseURL);
  }
  return config;
}, (error) => {
  console.error('❌ API Request Error:', error);
  return Promise.reject(error);
});

// Error interceptor for better timeout/network error messages
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // Get current API base (always recompute to handle tunnel detection)
    const currentApiBase = getCurrentApiBase();
    
    // Enhance timeout errors with clearer messaging
    if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
      error.timeoutError = true;
      const baseDisplay = currentApiBase.startsWith('/') 
        ? `${window.location.origin}${currentApiBase}` 
        : currentApiBase;
      error.message = `Request timeout: Backend at ${baseDisplay} did not respond within 20 seconds. Check if server is running.`;
    }
    // Enhance network errors
    if (error.code === 'ECONNREFUSED' || error.message?.includes('Network Error') || error.message?.includes('Failed to fetch')) {
      error.networkError = true;
      const hostname = typeof window !== 'undefined' ? window.location.hostname : '';
      const isTunnel = hostname.includes('trycloudflare.com') || hostname.includes('cloudflare.com');
      
      if (isTunnel && currentApiBase === '/api') {
        // Using tunnel with relative path - backend should be accessible via proxy
        error.message = `Cannot connect to backend. The backend server may not be running, or the Vite proxy is not configured correctly.`;
      } else {
        const baseDisplay = currentApiBase.startsWith('/') 
          ? `${window.location.origin}${currentApiBase} (via proxy)` 
          : currentApiBase;
        error.message = `Cannot connect to backend at ${baseDisplay}. Please check if server is running.`;
      }
    }
    return Promise.reject(error);
  }
);

// Dev log so you can see exactly where calls go
console.info('[API] Base URL (computed):', getCurrentApiBase());

// Alert endpoints
export interface AlertQueryParams {
  symbol?: string;
  pattern?: string;
  min_confidence?: number;
  date_from?: string;
  date_to?: string;
  limit?: number;
  offset?: number;
}

export const alertsAPI = {
  getAll: (params?: AlertQueryParams) => api.get('/alerts', { params }),
  getById: (id: string) => api.get(`/alerts/${id}`),
  getStats: () => api.get('/alerts/stats/summary')
};

// Indicators endpoints
export const indicatorsAPI = {
  getBySymbol: (symbol: string, indicators?: string) =>
    api.get(`/indicators/${encodeURIComponent(symbol)}`, { params: { indicators } })
};

// Greeks endpoints
export const greeksAPI = {
  getBySymbol: (symbol: string) => api.get(`/greeks/${encodeURIComponent(symbol)}`)
};

// Charts endpoints
export interface ChartQueryParams {
  date_from?: string;
  date_to?: string;
  resolution?: string;
  include_indicators?: boolean;
  period?: string; // Time period: 1d, 1w, 1m, 3m, 1y
}
export const chartsAPI = {
  getData: (symbol: string, params?: ChartQueryParams) =>
    api.get(`/charts/${encodeURIComponent(symbol)}`, { params })
};

// Intraday instruments endpoints
export const intradayAPI = {
  getInstruments: () => api.get('/intraday/instruments'),
  getHistorical: (symbol: string, period?: string, resolution?: string) =>
    api.get(`/historical/${encodeURIComponent(symbol)}`, { params: { period, resolution } })
};

// Volume profile endpoints
export const volumeProfileAPI = {
  getData: (symbol: string, date?: string) =>
    api.get(`/volume-profile/${encodeURIComponent(symbol)}`, { params: { date } })
};

// News endpoints
export const newsAPI = {
  getBySymbol: (symbol: string, limit?: number, hours_back?: number) =>
    api.get(`/news/${encodeURIComponent(symbol)}`, { params: { limit, hours_back } }),
  getLatestMarket: () => api.get('/news/market/latest')
};

// Validation endpoints
export const validationAPI = {
  getByAlertId: (alertId: string) => api.get(`/validation/${alertId}`),
  getStats: () => api.get('/validation/stats')
};

// Market endpoints
export const marketAPI = {
  getIndices: () => api.get('/market/indices')
};

// Dashboard endpoints
export const dashboardAPI = {
  getStats: () => api.get('/dashboard-stats')
};

// Instruments endpoints
export const instrumentsAPI = {
  getInstruments: (type?: string) => api.get('/instruments', { params: { type } })
};

// Auth endpoints
// Deprecated - use authAPI.login() directly with shaKey or username/password
export interface LoginCredentials {
  username: string;
  password: string;
}

export interface AuthResponse {
  access_token: string;
  token_type: string;
  expires_in?: number;
}

export const authAPI = {
  login: (accountNumberOrUsername: string, password?: string) => {
    // Use FormData for FastAPI Form(...) endpoint
    const formData = new URLSearchParams();
    
    // If password is provided, use username/password login
    // Otherwise, use account number login
    if (password !== undefined) {
      formData.append('username', accountNumberOrUsername);
      formData.append('password', password);
    } else {
      formData.append('account_number', accountNumberOrUsername);
    }
    
    return api.post<AuthResponse>('/auth/login', formData, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });
  },
  register: () => {
    return api.post<{ account_number: string; account_number_raw: string; message: string }>('/auth/register');
  },
  refresh: async (refreshToken?: string) => {
    // Backend expects refresh_token as query parameter or JSON body
    // Using JSON body for consistency with other endpoints
    return api.post<AuthResponse>('/auth/refresh', { refresh_token: refreshToken });
  },
  logout: () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
  }
};

// === Broker & Auto-Trade APIs (UI-facing only) ===

// Broker status for current logged-in user
export interface BrokerStatus {
  broker: 'ZERODHA' | 'ANGEL_ONE';
  status: 'ACTIVE' | 'EXPIRED' | 'DISCONNECTED';
  expires_at?: string | null;
  last_error?: string | null;
}

// Strategy / pattern auto-trade settings for current user
export interface StrategyRule {
  id: string; // backend id or pattern code
  strategy_id: string;
  pattern_type: string;
  display_name: string;

  enabled: boolean;
  broker: 'ZERODHA' | 'ANGEL_ONE' | null;

  max_capital_per_trade: number | null;
  max_lots_per_trade: number | null;
  daily_loss_limit: number | null;

  stop_loss_mode: 'FROM_ALERT' | 'CUSTOM';
  custom_stop_loss_pct: number | null;

  auto_execute: boolean;
}

/**
 * UI-level broker and auto-trade API helpers.
 * Backend endpoints are suggestions; you can adjust paths on the server side
 * and only need to update these functions to match.
 */
export const brokersAPI = {
  getStatus: async (): Promise<BrokerStatus[]> => {
    const { data } = await api.get<BrokerStatus[]>('/brokers/status');
    return data;
  },
  connectZerodha: async (accessToken: string): Promise<BrokerStatus> => {
    const formData = new URLSearchParams();
    formData.append('access_token', accessToken);
    const { data } = await api.post<BrokerStatus>('/brokers/zerodha/token', formData, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });
    return data;
  },
  connectAngelOne: async (accessToken: string, clientCode: string): Promise<BrokerStatus> => {
    const formData = new URLSearchParams();
    formData.append('access_token', accessToken);
    formData.append('client_code', clientCode);
    const { data } = await api.post<BrokerStatus>('/brokers/angelone/token', formData, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });
    return data;
  }
};

export const autoTradeAPI = {
  getStrategyRules: async (): Promise<StrategyRule[]> => {
    const { data } = await api.get<StrategyRule[]>('/auto_trade/rules');
    return data;
  },
  saveStrategyRule: async (rule: StrategyRule): Promise<StrategyRule> => {
    const { data } = await api.post<StrategyRule>('/auto_trade/rules', rule);
    return data;
  }
};

// Robot status interface
export interface RobotStatus {
  available_margin: number;
  margin_used: number;
  safe_capital: number;
  open_positions: number;
  total_capital?: number;
  margin_utilization_pct?: number;
}

export const robotAPI = {
  getStatus: async (): Promise<RobotStatus> => {
    const { data } = await api.get<RobotStatus>('/robot/status');
    return data;
  }
};
