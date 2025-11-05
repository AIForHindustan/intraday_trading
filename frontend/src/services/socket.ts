import { io, Socket } from 'socket.io-client';

const WS_BASE_URL = import.meta.env.VITE_WS_URL || 'http://localhost:5000';

let socket: Socket | null = null;
let connected = false;
const subs: Array<() => void> = []; // re-subscribe callbacks

export function getSocket(): Socket {
  if (!socket) {
    socket = io(WS_BASE_URL, {
      transports: ['websocket'],
      autoConnect: true,
      reconnection: true,
      reconnectionAttempts: Infinity,
      reconnectionDelay: 500,
      reconnectionDelayMax: 5000
    });

    socket.on('connect', () => {
      connected = true;
      // re-subscribe all channels on reconnect
      subs.forEach(fn => fn());
    });

    socket.on('disconnect', () => { connected = false; });
  }
  return socket;
}

export function subscribeAlerts(params: any, onNew: (alert: any) => void, onUpdate?: (u: any) => void) {
  const s = getSocket();
  const sub = () => s.emit('subscribe_alerts', params || {});
  subs.push(sub);
  if (connected) sub();

  s.on('new_alert', (msg: any) => {
    const data = msg?.data || msg;
    onNew?.(data);
  });
  s.on('alert_updated', (msg: any) => {
    const data = msg?.data || msg;
    onUpdate?.(data);
  });

  return () => {
    s.off('new_alert');
    s.off('alert_updated');
  };
}

export function subscribeMarket(onIndices: (d:any)=>void, onNews?: (n:any)=>void) {
  const s = getSocket();
  const sub = () => s.emit('subscribe_market_data');
  subs.push(sub);
  if (connected) sub();
  s.on('indices_update', (msg) => onIndices(msg?.data || msg));
  if (onNews) s.on('news_update', (msg) => onNews(msg?.data || msg));
  return () => { s.off('indices_update'); s.off('news_update'); };
}

export function subscribeValidation(filter:any, onResult:(d:any)=>void) {
  const s = getSocket();
  const sub = () => s.emit('subscribe_validation', filter || {});
  subs.push(sub);
  if (connected) sub();
  s.on('validation_result', (msg) => onResult(msg?.data || msg));
  return () => s.off('validation_result');
}
