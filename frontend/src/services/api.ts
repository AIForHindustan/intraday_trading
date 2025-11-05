import axios from 'axios';

// Unified API base URL configuration
const RAW = import.meta.env.VITE_API_URL || 'http://localhost:5001/api';

// Remove any trailing slashes once
const API_BASE = RAW.replace(/\/+$/, '');

const api = axios.create({
  baseURL: API_BASE,          // e.g. http://localhost:5001/api
  timeout: 20000,             // bump from 10s → 20s
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

// OPTIONAL: dev log so you can see exactly where calls go
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