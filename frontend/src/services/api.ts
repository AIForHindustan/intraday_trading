import axios from 'axios';

// Origin-aware API base URL resolution
// Derives from browser origin, defaults to FastAPI's standard port 8000 in local dev
// Supports explicit Vite overrides via VITE_API_URL
function getApiBaseUrl(): string {
  // Explicit override takes precedence
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL.replace(/\/+$/, '');
  }
  
  // Derive from browser origin (works in production and when frontend/backend same origin)
  if (typeof window !== 'undefined') {
    const origin = window.location.origin;
    const port = window.location.port;
    
    // If running on standard Vite dev port (5173) or production, default to backend port 5001
    // But allow explicit override via VITE_API_URL or VITE_API_PORT
    if (!port || port === '5173' || port === '3000') {
      // Default to port 5001 for current backend (or allow env override)
      // Check if we're in development mode (Vite defaults to 5173)
      const isDev = port === '5173' || port === '3000';
      // Default to 5001 for current backend, but allow env override
      const backendPort = import.meta.env.VITE_API_PORT || (isDev ? '5001' : port || '5001');
      return `${origin.split(':').slice(0, 2).join(':')}:${backendPort}/api`;
    }
    
    // If custom port, assume backend is on same port (e.g., localhost:5001)
    return `${origin}/api`;
  }
  
  // Fallback for SSR or non-browser environments
  // Default to 5001 to match current backend, but allow override
  return import.meta.env.VITE_API_URL || 'http://localhost:5001/api';
}

const API_BASE = getApiBaseUrl();

const api = axios.create({
  baseURL: API_BASE,
  timeout: 20000,             // 20s timeout
  headers: {
    'Content-Type': 'application/json'
  },
  withCredentials: false      // set true ONLY if using cookie auth
});

// Attach JWT to every call
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Error interceptor for better timeout/network error messages
api.interceptors.response.use(
  (response) => response,
  (error) => {
    // Enhance timeout errors with clearer messaging
    if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
      error.timeoutError = true;
      error.message = `Request timeout: Backend at ${API_BASE} did not respond within 20 seconds. Check if server is running.`;
    }
    // Enhance network errors
    if (error.code === 'ECONNREFUSED' || error.message?.includes('Network Error')) {
      error.networkError = true;
      error.message = `Cannot connect to backend at ${API_BASE}. Please check if server is running.`;
    }
    return Promise.reject(error);
  }
);

// Dev log so you can see exactly where calls go
console.info('[API] Base URL:', API_BASE);

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
}
export const chartsAPI = {
  getData: (symbol: string, params?: ChartQueryParams) =>
    api.get(`/charts/${encodeURIComponent(symbol)}`, { params })
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

// Instruments endpoints
export const instrumentsAPI = {
  getInstruments: (type?: string) => api.get('/instruments', { params: { type } })
};

// Auth endpoints
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
  login: (credentials: LoginCredentials) => {
    // Use FormData for FastAPI Form(...) endpoint
    const formData = new URLSearchParams();
    formData.append('username', credentials.username);
    formData.append('password', credentials.password);
    return api.post<AuthResponse>('/auth/login', formData, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });
  },
  refresh: (refreshToken?: string) => 
    api.post<AuthResponse>('/auth/refresh', { refresh_token: refreshToken }),
  logout: () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
  }
};