// Native WebSocket client for FastAPI WebSocket endpoints
const WS_BASE_URL = import.meta.env.VITE_WS_URL || 'http://localhost:5000';
const WS_PROTOCOL = WS_BASE_URL.replace('http://', 'ws://').replace('https://', 'wss://');

let alertsWs: WebSocket | null = null;
let validationWs: WebSocket | null = null;
let marketWs: WebSocket | null = null;

export type AlertEvent = {
  alert_id: string;
  symbol: string;
  pattern: string;
  confidence: number;
  signal: string;
  last_price: number;
  timestamp: string;
  action: string;
  stop_loss?: number;
  target?: number;
};

/**
 * Subscribe to alerts stream via FastAPI WebSocket.
 * Returns an unsubscribe function.
 */
export function subscribeAlerts(filters: { symbol?: string; pattern?: string } | undefined, callback: (alert: any) => void) {
  // Close existing connection if any
  if (alertsWs) {
    alertsWs.close();
  }
  
  // Connect to FastAPI WebSocket endpoint
  alertsWs = new WebSocket(`${WS_PROTOCOL}/ws/alerts`);
  
  alertsWs.onopen = () => {
    console.log('✅ Connected to alerts WebSocket');
    // Send filters if provided (FastAPI WebSocket can receive text messages)
    if (filters) {
      alertsWs?.send(JSON.stringify({ type: 'filter', ...filters }));
    }
  };
  
  alertsWs.onmessage = (event) => {
    try {
      const message = JSON.parse(event.data);
      // Skip ping messages
      if (message.type === 'ping') {
        return;
      }
      // Handle new_alert format: {type: "new_alert", data: {...}}
      // or legacy format: direct alert object
      const alertData = message.type === 'new_alert' ? message.data : message;
      callback(alertData);
    } catch (e) {
      console.error('Error parsing WebSocket message:', e);
    }
  };
  
  alertsWs.onerror = (error) => {
    console.error('WebSocket error:', error);
  };
  
  alertsWs.onclose = () => {
    console.log('WebSocket disconnected');
    alertsWs = null;
  };
  
  return () => {
    if (alertsWs) {
      alertsWs.close();
      alertsWs = null;
    }
  };
}

/**
 * Subscribe to validation stream for a specific alert.
 * Uses polling via REST API for now (WebSocket support can be added later)
 */
export function subscribeValidation(alertId: string, callback: (validation: any) => void) {
  // For now, use polling - can be enhanced with WebSocket later
  const interval = setInterval(async () => {
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL || 'http://localhost:5000/api'}/validation/${alertId}`);
      if (response.ok) {
        const data = await response.json();
        if (data && Object.keys(data).length > 0) {
          callback(data);
        }
      }
    } catch (e) {
      console.error('Error fetching validation:', e);
    }
  }, 5000); // Poll every 5 seconds
  
  return () => {
    clearInterval(interval);
  };
}

/**
 * Subscribe to market data stream.
 * Uses polling via REST API for now
 */
export function subscribeMarket(callback: (indices: any) => void, newsCallback?: (news: any) => void) {
  const interval = setInterval(async () => {
    try {
      // Fetch indices
      const indicesResponse = await fetch(`${import.meta.env.VITE_API_URL || 'http://localhost:5000/api'}/market/indices`);
      if (indicesResponse.ok) {
        const indices = await indicesResponse.json();
        callback(indices);
      }
      
      // Fetch news if callback provided
      if (newsCallback) {
        const newsResponse = await fetch(`${import.meta.env.VITE_API_URL || 'http://localhost:5000/api'}/news/market/latest`);
        if (newsResponse.ok) {
          const news = await newsResponse.json();
          newsCallback(news);
        }
      }
    } catch (e) {
      console.error('Error fetching market data:', e);
    }
  }, 30000); // Poll every 30 seconds
  
  return () => {
    clearInterval(interval);
  };
}