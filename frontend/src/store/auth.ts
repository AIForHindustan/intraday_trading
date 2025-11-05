import { create } from 'zustand';
import { authAPI } from '../services/api';

type User = { username: string } | null;

interface AuthState {
  user: User;
  loading: boolean;
  error?: string;
  login: (username: string, password: string) => Promise<void>;
  logout: () => void;
  hydrate: () => void;
}

export const useAuth = create<AuthState>((set) => ({
  // TEMPORARILY DISABLED: Set guest user automatically
  user: { username: 'guest' },
  loading: false,
  login: async (username, password) => {
    set({ loading: true, error: undefined });
    try {
      const { data } = await authAPI.login({ username, password });
      localStorage.setItem('access_token', data.access_token);
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
      // Authentication errors (wrong credentials)
      else if (e?.response?.status === 401) {
        errorMsg = 'Incorrect username or password';
      }
      // Backend validation errors
      else if (e?.response?.data) {
        const data = e.response.data;
        if (typeof data.detail === 'string') {
          errorMsg = data.detail;
        } else if (Array.isArray(data.detail)) {
          errorMsg = data.detail.map((err: any) => err?.msg || JSON.stringify(err)).join(', ');
        } else if (data.detail?.msg) {
          errorMsg = data.detail.msg;
        }
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

