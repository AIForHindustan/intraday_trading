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
  user: null,
  loading: false,
  login: async (username, password) => {
    set({ loading: true, error: undefined });
    try {
      const { data } = await authAPI.login({ username, password });
      localStorage.setItem('access_token', data.access_token);
      set({ user: { username }, loading: false });
    } catch (e: any) {
      // Extract error message - handle Pydantic validation errors
      let errorMsg = 'Login failed';
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
    if (token) set({ user: { username: 'me' } });
  }
}));

