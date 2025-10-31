from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import asyncio
import uuid
import json
import time
from typing import Dict, List
from pathlib import Path

# Use uvloop for better async performance if available
try:
    import uvloop  # type: ignore
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

app = FastAPI(
    title="Ultra Low-Latency Trading Dashboard",
    docs_url=None,
    redoc_url=None,
)

# Templates: render existing alerts/dashboard.html
templates = Jinja2Templates(directory="alerts")


@app.middleware("http")
async def add_performance_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Response-Time"] = "ultra-fast"
    response.headers["Cache-Control"] = "no-cache, must-revalidate"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/professional", response_class=HTMLResponse)
async def professional_dashboard(request: Request):
    """Professional trading dashboard with TradingView charts"""
    with open("alerts/vue-trading-dashboard.html", "r") as f:
        content = f.read()
    return HTMLResponse(content=content)


# --- App startup: warm instrument caches for ultra-fast loads ---
@app.on_event("startup")
async def warm_instrument_cache():
    try:
        from alerts.simple_instrument_manager import instrument_manager
        # Initialize and warm caches
        await instrument_manager.initialize_data()
    except Exception as e:
        print(f"Instrument cache warm failed: {e}")


# SSE endpoint backed by HighPerformanceAlertStream
@app.get("/alerts/stream")
async def alert_stream(symbol: str | None = None, pattern: str | None = None):
    from redis_files.redis_storage import HighPerformanceAlertStream  # reuse existing impl
    import orjson

    async def event_generator():
        stream_processor = HighPerformanceAlertStream()
        # Ensure stream + consumer group exist
        try:
            await stream_processor.setup_streams()
        except Exception:
            pass
        consumer_name = f"dashboard_{uuid.uuid4()}"
        async for alert in stream_processor.consume_alerts(consumer_name):
            if symbol and alert.get("symbol") != symbol:
                continue
            if pattern and alert.get("pattern") != pattern:
                continue
            yield f"data: {orjson.dumps(alert).decode()}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# Professional instrument data endpoints
@app.get("/api/instruments/{asset_class}")
async def api_instruments(asset_class: str):
    """Get instruments by asset class from Redis + RedisJSON"""
    try:
        # Normalize frontend asset class to backend keys
        normalization_map = {
            "eq": "equity_cash",
            "eq_fut": "equity_futures",
            "index_fut": "index_futures",
            "index_opt": "index_options",
        }
        normalized = normalization_map.get(asset_class, asset_class)

        from alerts.simple_instrument_manager import instrument_manager
        data = await instrument_manager.get_instruments_by_asset_class(normalized)
        
        # Format for frontend consumption
        formatted_data = []
        for instrument in data:
            formatted_data.append({
                "symbol": instrument.get('symbol', ''),
                "name": instrument.get('name', ''),
                "exchange": instrument.get('exchange', ''),
                "expiry": instrument.get('expiry', ''),
                "strike_price": instrument.get('strike_price'),
                "option_type": instrument.get('option_type', ''),
                "sector": instrument.get('sector', ''),
                "token": instrument.get('token', '')
            })
        
        return formatted_data[:100]  # Limit to 100 for performance
        
    except Exception as e:
        print(f"Error loading instruments: {e}")
        # Fallback to static data
        return await _get_fallback_instruments(asset_class)


@app.get("/api/options-chain/{underlying}")
async def api_options_chain(underlying: str):
    """Return expiries and strikes for a given underlying using cached data"""
    try:
        from alerts.simple_instrument_manager import instrument_manager
        return await instrument_manager.get_options_chain(underlying)
    except Exception as e:
        print(f"Options chain error: {e}")
        return {"expiries": [], "strikes": []}

async def _get_fallback_instruments(asset_class: str):
    """Fallback static instrument data"""
    if asset_class in ("eq", "equity_cash"):
        return [
            {"symbol": "NSE:RELIANCE", "name": "RELIANCE", "exchange": "NSE"},
            {"symbol": "NSE:TCS", "name": "TCS", "exchange": "NSE"},
            {"symbol": "NSE:HDFCBANK", "name": "HDFC BANK", "exchange": "NSE"},
        ]
    elif asset_class in ("index_fut", "index_futures"):
        return [
            {"symbol": "NFO:NIFTY25NOV", "name": "NIFTY 50 NOV FUT", "expiry": "2025-11-25"},
            {"symbol": "NFO:BANKNIFTY25NOV", "name": "BANK NIFTY NOV FUT", "expiry": "2025-11-25"},
        ]
    elif asset_class in ("index_opt", "index_options"):
        return [
            {"symbol": "NFO:NIFTY25NOV24000CE", "name": "NIFTY 24000 CE", "expiry": "2025-11-25"},
            {"symbol": "NFO:NIFTY25NOV24000PE", "name": "NIFTY 24000 PE", "expiry": "2025-11-25"},
        ]
    return []

@app.get("/api/instruments/search/{query}")
async def search_instruments(query: str, asset_class: str = None):
    """Search instruments using Redis Search"""
    try:
        from alerts.simple_instrument_manager import instrument_manager
        results = await instrument_manager.search_instruments(query, asset_class)
        return results[:50]  # Limit results
    except Exception as e:
        print(f"Search error: {e}")
        return []

@app.get("/api/instruments/metadata")
async def get_instrument_metadata():
    """Get instrument metadata and statistics"""
    try:
        from alerts.simple_instrument_manager import instrument_manager
        metadata = await instrument_manager.get_instrument_metadata()
        return metadata
    except Exception as e:
        print(f"Metadata error: {e}")
        return {}


@app.get("/api/news")
async def api_news(limit: int = 25):
    """Serve latest consolidated news items from config/data/indices/news"""
    base = Path("config/data/indices/news")
    items: List[Dict] = []
    try:
        if base.exists():
            files = sorted(base.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
            for fp in files[:5]:
                with fp.open("r") as f:
                    for line in f:
                        try:
                            items.append(json.loads(line))
                        except Exception:
                            continue
                        if len(items) >= limit:
                            break
                if len(items) >= limit:
                    break
    except Exception as e:
        print(f"News load error: {e}")
    return items[:limit]


@app.get("/api/chart/{symbol}")
async def api_chart(symbol: str, timeframe: str = "5m") -> Dict:
    try:
        from alerts.professional_api import ProfessionalDataManager
        mgr = ProfessionalDataManager()
        return await mgr.get_chart_data(symbol, timeframe)
    except Exception:
        # Minimal synthetic OHLC + no patterns
        import time as _t
        now = int(_t.time())
        ohlc = [{"time": now - i*60, "open": 100+i, "high": 101+i, "low": 99+i, "close": 100.5+i} for i in range(60)][::-1]
        return {"ohlc": ohlc, "patterns": [], "indicators": {}, "metadata": {"symbol": symbol, "timeframe": timeframe}}


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.symbol_connections: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, symbol: str = None):
        await websocket.accept()
        self.active_connections.append(websocket)
        if symbol:
            if symbol not in self.symbol_connections:
                self.symbol_connections[symbol] = []
            self.symbol_connections[symbol].append(websocket)

    def disconnect(self, websocket: WebSocket, symbol: str = None):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if symbol and symbol in self.symbol_connections:
            if websocket in self.symbol_connections[symbol]:
                self.symbol_connections[symbol].remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast_to_symbol(self, message: str, symbol: str):
        if symbol in self.symbol_connections:
            for connection in self.symbol_connections[symbol]:
                try:
                    await connection.send_text(message)
                except:
                    # Remove dead connections
                    self.symbol_connections[symbol].remove(connection)

manager = ConnectionManager()


@app.websocket("/ws/professional/{symbol}")
async def websocket_professional_chart(websocket: WebSocket, symbol: str):
    """WebSocket for real-time professional chart data"""
    await manager.connect(websocket, symbol)
    try:
        while True:
            # Keep connection alive and send periodic updates
            await asyncio.sleep(1)
            
            # Simulate real-time data
            chart_data = {
                "timestamp": int(time.time() * 1000),
                "ohlc_data": [{
                    "timestamp": int(time.time() * 1000),
                    "open": 2400 + (time.time() % 100),
                    "high": 2405 + (time.time() % 100),
                    "low": 2395 + (time.time() % 100),
                    "close": 2402 + (time.time() % 100),
                    "volume": 1000000 + int(time.time() % 500000)
                }],
                "technical_indicators": {
                    "rsi": {"value": 65.4, "signal": "NEUTRAL"},
                    "ema20": {"value": 2400.2, "signal": "BULLISH"},
                    "vwap": {"value": 2401.5, "signal": "BULLISH"}
                },
                "active_patterns": []
            }
            
            await manager.send_personal_message(json.dumps(chart_data), websocket)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)


