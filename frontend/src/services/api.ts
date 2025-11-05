import axios from 'axios';

// Configure base URL for the API
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
});

// ---- JWT auth bearer token interceptor ----
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

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