import { create } from 'zustand';
import { authAPI } from '../services/api';

type User = { username: string } | null;

interface AuthState {
  user: User;
  loading: boolean;
  error?: string;
  login: (accountNumberOrUsername: string, password?: string) => Promise<void>;
  logout: () => void;
  hydrate: () => void;
}

export const useAuth = create<AuthState>((set) => ({
  user: null, // Start with no user - require login
  loading: false,
  login: async (accountNumberOrUsername: string, password?: string) => {
    set({ loading: true, error: undefined });
    try {
      const { data } = await authAPI.login(accountNumberOrUsername, password);
      localStorage.setItem('access_token', data.access_token);
      // Extract username from token if possible, otherwise use a placeholder
      let username = 'user';
      try {
        const payload = JSON.parse(atob(data.access_token.split('.')[1]));
        username = payload.sub || 'user';
      } catch {
        // If we can't decode, use placeholder
      }
      set({ user: { username }, loading: false });
    } catch (e: any) {
      // Enhanced error handling: flag backend timeouts or network errors
      let errorMsg = 'Login failed';
      
      // Network/connection errors (backend unreachable)
      if (e?.networkError || e?.code === 'ECONNREFUSED' || e?.message?.includes('Network Error')) {
        errorMsg = 'Cannot connect to backend server. Please check if the server is running and accessible.';
      }
      // Timeout errors (backend too slow or hanging)
      else if (e?.timeoutError || e?.code === 'ECONNABORTED' || e?.message?.includes('timeout')) {
        errorMsg = 'Request timeout: Backend server did not respond in time. The server may be overloaded or unreachable.';
      }
      // Backend validation errors (check this FIRST to get actual error message)
      if (e?.response?.data) {
        const data = e.response.data;
        if (typeof data.detail === 'string') {
          errorMsg = data.detail;
        } else if (Array.isArray(data.detail)) {
          errorMsg = data.detail.map((err: any) => err?.msg || JSON.stringify(err)).join(', ');
        } else if (data.detail?.msg) {
          errorMsg = data.detail.msg;
        }
      }
      // Authentication errors (fallback if no detail message)
      else if (e?.response?.status === 401) {
        errorMsg = 'Invalid account number or credentials';
      }
      // Generic error message
      else if (e?.message) {
        errorMsg = e.message;
      }
      
      console.error('Login error:', e);
      set({ error: errorMsg, loading: false });
      throw e;
    }
  },
  logout: () => {
    localStorage.removeItem('access_token');
    set({ user: null });
  },
  hydrate: () => {
    const token = localStorage.getItem('access_token');
    if (token) {
      // Try to decode token to get username (basic check)
      try {
        const payload = JSON.parse(atob(token.split('.')[1]));
        set({ user: { username: payload.sub || 'me' } });
      } catch {
        // If decode fails, still set user since token exists
        set({ user: { username: 'me' } });
      }
    }
  }
}));

