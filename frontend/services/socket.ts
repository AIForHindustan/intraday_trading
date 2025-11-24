// Native WebSocket client (not Socket.IO) - matches FastAPI WebSocket backend
// Origin-aware WebSocket URL resolution (matches REST client logic)

function getWebSocketBaseUrl(): string {
  // Explicit override takes precedence
  if (import.meta.env.VITE_WS_URL) {
    return import.meta.env.VITE_WS_URL.replace(/\/+$/, '');
  }
  
  // Check for tunnel hostnames FIRST (before any other logic)
  // This ensures tunnel access always uses relative paths through Vite proxy
  if (typeof window !== 'undefined') {
    const hostname = window.location.hostname;
    const isTunnel = hostname.includes('trycloudflare.com') || hostname.includes('loca.lt');
    
    // ALWAYS use relative path when accessed via tunnel
    if (isTunnel) {
      console.log('🌐 Tunnel detected, using relative WebSocket path');
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${protocol}//${window.location.host}`;
    }
    
    // In development mode, also use relative path
    const isDev = import.meta.env.DEV;
    if (isDev) {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${protocol}//${window.location.host}`;
    }
  }
  
  // Derive from browser origin (matches REST client logic)
  if (typeof window !== 'undefined') {
    const origin = window.location.origin;
    const port = window.location.port;
    
    // If running on standard Vite dev port (5173) or production, default to backend port 8000
    if (!port || port === '5173' || port === '3000') {
      // Default to port 8000 for current backend, but allow env override
      const isDev = port === '5173' || port === '3000';
      const backendPort = import.meta.env.VITE_WS_PORT || import.meta.env.VITE_API_PORT || (isDev ? '8000' : port || '8000');
      return `${origin.split(':').slice(0, 2).join(':')}:${backendPort}`;
    }
    
    // If custom port, assume backend is on same port
    return origin;
  }
  
  // Fallback for SSR or non-browser environments
  // Default to 8000 to match current backend, but allow override
  return import.meta.env.VITE_WS_URL || 'ws://localhost:8000';
}

const WS_BASE_URL = getWebSocketBaseUrl();
const WS_PATH = '/ws/alerts';

let socket: WebSocket | null = null;
let connected = false;
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
const messageHandlers: Map<string, Set<(data: any) => void>> = new Map();

function getWebSocketUrl(): string {
  // Check if current page is HTTPS or if base URL already uses secure protocol
  const isSecure = typeof window !== 'undefined' 
    ? window.location.protocol === 'https:' 
    : WS_BASE_URL.startsWith('wss://') || WS_BASE_URL.startsWith('https://');
  
  const protocol = isSecure ? 'wss' : 'ws';
  const host = WS_BASE_URL.replace(/^(wss?|https?):\/\//, '').replace(/\/$/, '');
  return `${protocol}://${host}${WS_PATH}`;
}

function connect(): WebSocket {
  if (socket && socket.readyState === WebSocket.OPEN) {
    return socket;
  }

  const token = localStorage.getItem('access_token');
  const url = getWebSocketUrl();
  
  socket = new WebSocket(url);
  
  socket.onopen = () => {
    connected = true;
    console.log('WebSocket connected');
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }
  };

  socket.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);
      const type = message.type || 'new_alert';
      
      // Handle ping/pong
      if (type === 'ping') {
        socket?.send(JSON.stringify({ type: 'pong' }));
        return;
      }
      
      // Call handlers for this message type
      const handlers = messageHandlers.get(type);
      if (handlers) {
        handlers.forEach(handler => handler(message.data || message));
      }
      
      // Also check for legacy format (direct alert object)
      if (type === 'new_alert' && !message.data && message.alert_id) {
        const alertHandlers = messageHandlers.get('new_alert');
        if (alertHandlers) {
          alertHandlers.forEach(handler => handler(message));
        }
      }
    } catch (e) {
      console.error('Error parsing WebSocket message:', e);
    }
  };

  socket.onerror = (error) => {
    console.error('WebSocket error:', error);
    connected = false;
  };

  socket.onclose = () => {
    connected = false;
    socket = null;
    // Auto-reconnect after 2 seconds
    if (!reconnectTimeout) {
      reconnectTimeout = setTimeout(() => {
        reconnectTimeout = null;
        connect();
      }, 2000);
    }
  };

  return socket;
}

export function getSocket(): WebSocket {
  if (!socket || socket.readyState === WebSocket.CLOSED) {
    return connect();
  }
  return socket;
}

export function subscribeAlerts(params: any, onNew: (alert: any) => void, onUpdate?: (u: any) => void) {
  const s = getSocket();
  
  // Add handlers
  if (!messageHandlers.has('new_alert')) {
    messageHandlers.set('new_alert', new Set());
  }
  messageHandlers.get('new_alert')!.add(onNew);
  
  if (onUpdate) {
    if (!messageHandlers.has('alert_updated')) {
      messageHandlers.set('alert_updated', new Set());
    }
    messageHandlers.get('alert_updated')!.add(onUpdate);
  }

  return () => {
    messageHandlers.get('new_alert')?.delete(onNew);
    if (onUpdate) {
      messageHandlers.get('alert_updated')?.delete(onUpdate);
    }
  };
}

export function subscribeMarket(onIndices: (d:any)=>void, onNews?: (n:any)=>void) {
  const s = getSocket();
  
  // Backend doesn't support market data via WebSocket yet - use REST polling
  // This is handled in MarketIndices.tsx component
  // But we can listen for any market-related messages if backend adds them
  if (!messageHandlers.has('indices_update')) {
    messageHandlers.set('indices_update', new Set());
  }
  messageHandlers.get('indices_update')!.add(onIndices);
  
  if (onNews) {
    if (!messageHandlers.has('news_update')) {
      messageHandlers.set('news_update', new Set());
    }
    messageHandlers.get('news_update')!.add(onNews);
  }

  return () => {
    messageHandlers.get('indices_update')?.delete(onIndices);
    if (onNews) {
      messageHandlers.get('news_update')?.delete(onNews);
    }
  };
}

export function subscribeValidation(filter:any, onResult:(d:any)=>void) {
  const s = getSocket();
  
  // Backend doesn't support validation via WebSocket yet - use REST polling
  // But we can listen for validation messages if backend adds them
  if (!messageHandlers.has('validation_result')) {
    messageHandlers.set('validation_result', new Set());
  }
  messageHandlers.get('validation_result')!.add(onResult);

  return () => {
    messageHandlers.get('validation_result')?.delete(onResult);
  };
}

// Real-time chart WebSocket connection per symbol
const chartSockets: Map<string, WebSocket> = new Map();
const chartHandlers: Map<string, Set<(data: any) => void>> = new Map();

export function subscribeChart(symbol: string, onUpdate: (data: any) => void): () => void {
  // Normalize symbol
  const normalizedSymbol = symbol.toUpperCase().trim();
  
  // Create WebSocket URL for chart endpoint (uses same origin-aware resolution)
  const isSecure = typeof window !== 'undefined' 
    ? window.location.protocol === 'https:' 
    : WS_BASE_URL.startsWith('wss://') || WS_BASE_URL.startsWith('https://');
  const protocol = isSecure ? 'wss' : 'ws';
  const host = WS_BASE_URL.replace(/^(wss?|https?):\/\//, '').replace(/\/$/, '');
  const chartUrl = `${protocol}://${host}/ws/professional/${encodeURIComponent(normalizedSymbol)}`;
  
  // Check if socket already exists
  let ws = chartSockets.get(normalizedSymbol);
  
  if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
    // Create new WebSocket connection
    ws = new WebSocket(chartUrl);
    chartSockets.set(normalizedSymbol, ws);
    
    if (!chartHandlers.has(normalizedSymbol)) {
      chartHandlers.set(normalizedSymbol, new Set());
    }
    
    ws.onopen = () => {
      console.log(`Chart WebSocket connected for ${normalizedSymbol}`);
    };
    
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        // Call all handlers for this symbol
        const handlers = chartHandlers.get(normalizedSymbol);
        if (handlers) {
          handlers.forEach(handler => handler(data));
        }
      } catch (e) {
        console.error(`Error parsing chart WebSocket message for ${normalizedSymbol}:`, e);
      }
    };
    
    ws.onerror = (error) => {
      console.error(`Chart WebSocket error for ${normalizedSymbol}:`, error);
    };
    
    ws.onclose = () => {
      console.log(`Chart WebSocket closed for ${normalizedSymbol}`);
      chartSockets.delete(normalizedSymbol);
      // Auto-reconnect after 2 seconds
      setTimeout(() => {
        if (chartHandlers.has(normalizedSymbol) && chartHandlers.get(normalizedSymbol)!.size > 0) {
          subscribeChart(normalizedSymbol, onUpdate);
        }
      }, 2000);
    };
  }
  
  // Add handler
  chartHandlers.get(normalizedSymbol)!.add(onUpdate);
  
  // Return unsubscribe function
  return () => {
    const handlers = chartHandlers.get(normalizedSymbol);
    if (handlers) {
      handlers.delete(onUpdate);
      // Close socket if no more handlers
      if (handlers.size === 0) {
        const wsToClose = chartSockets.get(normalizedSymbol);
        if (wsToClose) {
          wsToClose.close();
          chartSockets.delete(normalizedSymbol);
        }
        chartHandlers.delete(normalizedSymbol);
      }
    }
  };
}
