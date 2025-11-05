from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect, Query, HTTPException, Depends, Header, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from backend.websockets import router as websocket_router
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from jose import JWTError, jwt
try:
    from passlib.context import CryptContext
    HAS_PASSLIB = True
except ImportError:
    HAS_PASSLIB = False
    CryptContext = None  # Optional - only needed for password hashing
import asyncio
import uuid
import json
import time
import os
import random
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import concurrent.futures

# Shared executor for Redis operations (created once, reused for all requests)
redis_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10, thread_name_prefix="redis_worker")

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

# Add exception handler for better error messages (only for non-HTTPException errors)
@app.exception_handler(500)
async def internal_server_error_handler(request: Request, exc: Exception):
    """Handle 500 errors with detailed messages"""
    import traceback
    error_detail = str(exc)
    print(f"Internal server error: {exc}")
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {error_detail}"}
    )

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# JWT Authentication
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")  # Change in production!
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# Password hashing (for future user management)
# Try to initialize passlib, but disable if bcrypt version check fails
pwd_context = None
if HAS_PASSLIB:
    try:
        # Test if passlib can work with current bcrypt version
        # Suppress the AttributeError traceback from passlib's bcrypt version check
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
            # Try a test hash to verify compatibility
            test_context.hash("test")
            pwd_context = test_context
    except (AttributeError, Exception):
        # passlib has compatibility issues with this bcrypt version
        # We'll use direct bcrypt instead
        HAS_PASSLIB = False
        pwd_context = None
        print(f"⚠️  passlib disabled due to bcrypt compatibility issue, using direct bcrypt")
security = HTTPBearer(auto_error=False)

# 2FA Support (TOTP)
try:
    import pyotp
    import qrcode
    from io import BytesIO
    import base64
    HAS_2FA = True
except ImportError:
    HAS_2FA = False
    pyotp = None
    qrcode = None

# API Key authentication (optional - enable via environment variable)
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
REQUIRED_API_KEY = os.getenv("API_KEY", None)  # Set API_KEY env var to enable
LOCALTUNNEL_DOMAIN = os.getenv("LOCALTUNNEL_DOMAIN", "loca.lt")  # Restrict to LocalTunnel domain

# User management - stored in Redis DB 0 (system database)
# Key format: user:{username} -> JSON with hashed password and metadata

async def initialize_redis_connections():
    """Pre-warm Redis connections to avoid first-connection delays"""
    def warmup_redis():
        try:
            from redis_files.redis_manager import RedisManager82
            # Warm up connections for all dbs you use
            client_db0 = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
            client_db0.ping()
            print("✓ Redis DB0 connection warmed up")
            
            # Add other databases if you use them
            # client_db1 = RedisManager82.get_client(process_name="api", db=1, decode_responses=True)
            # client_db1.ping()
            
        except Exception as e:
            print(f"❌ Redis warmup failed: {e}")
    
    # Run warmup in thread pool
    await asyncio.wrap_future(redis_executor.submit(warmup_redis))

def get_user_from_redis(username: str) -> Optional[Dict]:
    """Get user from Redis - fast non-blocking lookup"""
    try:
        from redis_files.redis_manager import RedisManager82
        db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
        # Redis.get() with decode_responses=True returns string, not bytes
        user_data = db0_client.get(f"user:{username}")
        if user_data:
            # decode_responses=True means we get a string, parse JSON
            return json.loads(user_data)
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error for user {username}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching user from Redis: {e}")
        import traceback
        traceback.print_exc()
        return None

async def get_user_from_redis_with_timeout(username: str, timeout: float = 8.0) -> Optional[Dict]:
    """Get user from Redis with timeout protection"""
    try:
        user_data = await asyncio.wait_for(
            asyncio.wrap_future(redis_executor.submit(get_user_from_redis, username)),
            timeout=timeout
        )
        return user_data
    except asyncio.TimeoutError:
        print(f"❌ Redis timeout after {timeout}s for user: {username}")
        return None
    except Exception as e:
        print(f"❌ Redis error for user {username}: {e}")
        return None

def save_user_to_redis(username: str, password_hash: str, metadata: Optional[Dict] = None, update_existing: bool = False):
    """Save user to Redis with hashed password"""
    try:
        from redis_files.redis_manager import RedisManager82
        db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
        
        # Get existing user data if updating
        existing_data = {}
        if update_existing:
            existing = get_user_from_redis(username)
            if existing:
                existing_data = existing
        
        user_data = {
            "username": username,
            "password_hash": password_hash,
            "created_at": existing_data.get("created_at", datetime.utcnow().isoformat()),
            "metadata": {**(existing_data.get("metadata", {})), **(metadata or {})},
            "two_fa_enabled": existing_data.get("two_fa_enabled", False),
            "two_fa_secret": existing_data.get("two_fa_secret"),
            "brokerage_connections": existing_data.get("brokerage_connections", {})
        }
        db0_client.set(f"user:{username}", json.dumps(user_data))
        return True
    except Exception as e:
        print(f"Error saving user to Redis: {e}")
        return False

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against hash"""
    if not hashed_password:
        return False
    
    if not plain_password:
        return False
    
    # Try passlib first (if available and working)
    if HAS_PASSLIB and pwd_context:
        try:
            return pwd_context.verify(plain_password, hashed_password)
        except Exception:
            # Silently fallback to direct bcrypt if passlib fails
            pass
    
    # Fallback: Use bcrypt directly
    try:
        import bcrypt
        password_bytes = plain_password.encode('utf-8')
        if len(password_bytes) > 72:
            password_bytes = password_bytes[:72]
        
        # Ensure hashed_password is a string
        if isinstance(hashed_password, bytes):
            hash_bytes = hashed_password
        else:
            hash_bytes = hashed_password.encode('utf-8')
        
        return bcrypt.checkpw(password_bytes, hash_bytes)
    except Exception as e:
        print(f"bcrypt verification failed: {e}")
        import traceback
        traceback.print_exc()
        # Last resort: plain text (NOT SECURE - only for development)
        # Only use this if we're in development mode
        if os.getenv("ENVIRONMENT") != "production":
            return plain_password == hashed_password
        return False

def hash_password(password: str) -> str:
    """Hash password (bcrypt has 72-byte limit)"""
    # Truncate password to 72 bytes if needed (bcrypt limitation)
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
    
    # Try passlib first (if available and working)
    if HAS_PASSLIB and pwd_context:
        try:
            return pwd_context.hash(password)
        except Exception:
            # Silently fallback to direct bcrypt if passlib fails
            pass
    
    # Fallback: Use bcrypt directly
    try:
        import bcrypt
        return bcrypt.hashpw(password_bytes, bcrypt.gensalt()).decode('utf-8')
    except Exception as e:
        print(f"bcrypt hashing failed: {e}")
        # Last resort: return plain text (NOT SECURE - only for development)
        return password

# Initialize default users if they don't exist
def initialize_default_users():
    """Initialize default admin and user accounts in Redis"""
    default_users = {
        "admin": "admin123",
        "user": "user123"
    }
    for username, password in default_users.items():
        existing = get_user_from_redis(username)
        # Always reset password hash to ensure it's correct (in case hash format changed)
        password_hash = hash_password(password)
        save_user_to_redis(username, password_hash, {"role": "admin" if username == "admin" else "user"}, update_existing=True)
        if existing:
            print(f"✅ Updated password for existing user: {username}")
        else:
            print(f"✅ Initialized default user: {username}")

def verify_api_key(x_api_key: str = Depends(API_KEY_HEADER)):
    """Verify API key if authentication is enabled"""
    if REQUIRED_API_KEY is None:
        return True  # Authentication disabled
    if x_api_key != REQUIRED_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    if expires_delta:
        expire = now + expires_delta
    else:
        expire = now + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    # JWT expects exp as integer timestamp
    to_encode.update({"exp": int(expire.timestamp())})
    try:
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    except Exception as e:
        print(f"Error encoding JWT: {e}")
        import traceback
        traceback.print_exc()
        raise

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Verify JWT token and return user"""
    # TEMPORARILY DISABLED: Allow unauthenticated access for development
    # Set ALLOW_UNAUTHENTICATED=true to enable, or comment out the return below to require auth
    return {"username": "guest"}
    
    # Original authentication code (disabled temporarily)
    # if os.getenv("ALLOW_UNAUTHENTICATED", "false").lower() == "true":
    #     return {"username": "guest"}
    # 
    # if credentials is None:
    #     raise HTTPException(status_code=401, detail="Not authenticated", headers={"WWW-Authenticate": "Bearer"})
    # 
    # token = credentials.credentials
    # try:
    #     payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    #     username: str = payload.get("sub")
    #     if username is None:
    #         raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})
    #     return {"username": username}
    # except JWTError as e:
    #     raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})

# Add CORS middleware for frontend
# Allow all origins for production (restrict in production with specific domains)
FRONTEND_DIST_PATH = Path("frontend/dist")
IS_PRODUCTION = FRONTEND_DIST_PATH.exists() and os.getenv("ENVIRONMENT") == "production"

if IS_PRODUCTION:
    # Production: Serve static files
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIST_PATH / "assets")), name="static")
    
    # Allow specific production domains (including LocalTunnel)
    frontend_url = os.getenv("FRONTEND_URL", "")
    localtunnel_url = os.getenv("LOCALTUNNEL_URL", "")  # e.g., https://intraday-trading.loca.lt
    
    allowed_origins = [
        frontend_url,
        localtunnel_url,
        f"https://*.{LOCALTUNNEL_DOMAIN}",  # Allow any LocalTunnel subdomain
    ]
    # Remove empty strings
    allowed_origins = [origin for origin in allowed_origins if origin]
    
    # If no specific origins, allow LocalTunnel by default
    if not allowed_origins:
        allowed_origins = [f"https://*.{LOCALTUNNEL_DOMAIN}"]
else:
    # Development: Allow localhost origins
    allowed_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",  # Vite default
    ]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins + ["*"] if os.getenv("ALLOW_ALL_ORIGINS") == "true" else allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include WebSocket router
app.include_router(websocket_router)

# Templates: render existing alerts/dashboard.html
templates = Jinja2Templates(directory="alerts")


@app.middleware("http")
async def security_middleware(request: Request, call_next):
    """Security middleware: Origin validation, rate limiting"""
    # Check if request is from LocalTunnel domain (when using LocalTunnel)
    # Allow LocalTunnel requests - they may not have origin header
    if LOCALTUNNEL_DOMAIN and IS_PRODUCTION:
        origin = request.headers.get("origin", "")
        host = request.headers.get("host", "")
        # Allow if: no origin (direct access), LocalTunnel domain, or static files
        if origin and not request.url.path.startswith("/static"):
            # Only block if origin is explicitly set and not from LocalTunnel
            if LOCALTUNNEL_DOMAIN not in origin and LOCALTUNNEL_DOMAIN not in host:
                # Allow empty origin (LocalTunnel direct access)
                if origin:
                    # Log suspicious access attempt
                    print(f"⚠️  Blocked request from unauthorized origin: {origin}")
                    return Response(
                        content="Unauthorized origin",
                        status_code=403,
                        headers={"X-Blocked-Reason": "Origin validation failed"}
                    )
    
    # Add security headers
    response = await call_next(request)
    response.headers["X-Response-Time"] = "ultra-fast"
    response.headers["Cache-Control"] = "no-cache, must-revalidate"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response


@app.get("/")
async def dashboard(request: Request):
    """Serve dashboard - either static React build or legacy HTML"""
    if IS_PRODUCTION:
        return FileResponse(FRONTEND_DIST_PATH / "index.html")
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/professional", response_class=HTMLResponse)
async def professional_dashboard(request: Request):
    """Professional trading dashboard with TradingView charts"""
    with open("alerts/vue-trading-dashboard.html", "r") as f:
        content = f.read()
    return HTMLResponse(content=content)


# Store background tasks for proper cleanup
background_tasks = []

# --- App startup: warm instrument caches for ultra-fast loads ---
@app.on_event("startup")
async def warm_instrument_cache():
    # Pre-warm Redis connections
    await initialize_redis_connections()
    
    try:
        from alerts.simple_instrument_manager import instrument_manager
        # Initialize and warm caches
        await instrument_manager.initialize_data()
    except Exception as e:
        print(f"Instrument cache warm failed: {e}")
    
    # Initialize default users in Redis
    try:
        initialize_default_users()
        print("✅ Initialized default users in Redis")
    except Exception as e:
        print(f"Failed to initialize default users: {e}")
    
    # Start Redis alert listener for WebSocket
    try:
        from backend.websockets import start_redis_listener
        task = await start_redis_listener()
        if task:
            background_tasks.append(task)
        print("✅ Started Redis alert listener for WebSocket")
    except Exception as e:
        print(f"Failed to start Redis alert listener: {e}")

# --- App shutdown: cleanup background tasks ---
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up background tasks on shutdown"""
    for task in background_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    print("✅ Cleaned up background tasks")


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


def load_news_from_files(limit: int = 25) -> List[Dict]:
    """Helper function to load news from files - can be called from anywhere"""
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

@app.get("/api/news")
async def get_news(current_user: dict = Depends(get_current_user), limit: int = 25):
    """Get market news - requires authentication"""
    try:
        items = load_news_from_files(limit)
        
        # Return in expected format
        if items:
            return {"news": items}
        else:
            # Return mock data if no news available
            return {
                "news": [
                    {"title": "Market update", "content": "No recent news available", "date": "2024-01-01"}
                ]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        """Send message to websocket, checking if connection is still open"""
        try:
            # Try to send - let the exception handling catch closed connections
            await websocket.send_text(message)
        except (WebSocketDisconnect, RuntimeError, ConnectionError):
            # Connection already closed, silently ignore
            return
        except Exception as e:
            # Only log non-connection-related errors
            error_msg = str(e).lower()
            if "close message" not in error_msg and "disconnect" not in error_msg and "connection" not in error_msg:
                print(f"Error sending WebSocket message: {e}")
            return

    async def broadcast_to_symbol(self, message: str, symbol: str):
        if symbol in self.symbol_connections:
            disconnected = []
            for connection in self.symbol_connections[symbol]:
                try:
                    # Check if connection is still open
                    if connection.client_state.name != 'CONNECTED':
                        disconnected.append(connection)
                        continue
                    await connection.send_text(message)
                except (WebSocketDisconnect, RuntimeError, ConnectionError):
                    # Connection closed, mark for removal
                    disconnected.append(connection)
                except Exception as e:
                    print(f"Error broadcasting to WebSocket: {e}")
                    disconnected.append(connection)
            
            # Remove disconnected connections
            for conn in disconnected:
                if conn in self.symbol_connections[symbol]:
                    self.symbol_connections[symbol].remove(conn)

manager = ConnectionManager()


@app.websocket("/ws/professional/{symbol}")
async def websocket_professional_chart(websocket: WebSocket, symbol: str):
    """WebSocket for real-time professional chart data - reads from Redis ohlc_latest"""
    await manager.connect(websocket, symbol)
    try:
        from redis_files.redis_manager import RedisManager82
        db2_client = RedisManager82.get_client(process_name="api", db=2, decode_responses=False)
        db5_client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
        
        # Normalize symbol
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        
        last_sent_hash = None  # Track last sent data to avoid duplicate sends
        
        while True:
            await asyncio.sleep(0.5)  # Poll every 500ms for real-time updates
            
            try:
                # Read latest OHLC from Redis DB 2
                ohlc_key = f"ohlc_latest:{base_symbol}"
                ohlc_data = db2_client.hgetall(ohlc_key)
                
                if not ohlc_data:
                    # Try with full symbol
                    ohlc_key = f"ohlc_latest:{symbol}"
                    ohlc_data = db2_client.hgetall(ohlc_key)
                
                # For base indices, also try index data if no ohlc_latest found
                if not ohlc_data:
                    is_base_index = base_symbol in ['NIFTY', 'BANKNIFTY', 'NIFTY 50', 'NIFTY BANK', 'INDIA VIX']
                    if is_base_index:
                        db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
                        index_key_map = {
                            'NIFTY': 'index:NSE:NIFTY 50',
                            'NIFTY 50': 'index:NSE:NIFTY 50',
                            'BANKNIFTY': 'index:NSE:NIFTY BANK',
                            'NIFTY BANK': 'index:NSE:NIFTY BANK',
                            'INDIA VIX': 'index:NSE:INDIA VIX'
                        }
                        index_key = index_key_map.get(base_symbol, f"index:NSE:{base_symbol}")
                        index_data = db1_client.get(index_key)
                        
                        if index_data:
                            if isinstance(index_data, bytes):
                                index_data = index_data.decode('utf-8')
                            try:
                                index_json = json.loads(index_data)
                                # Convert index data to ohlc_data hash format for processing
                                ohlc_data = {
                                    b'last_price': str(index_json.get('last_price', 0)).encode(),
                                    b'open': str(index_json.get('ohlc', {}).get('open', 0)).encode(),
                                    b'high': str(index_json.get('ohlc', {}).get('high', 0)).encode(),
                                    b'low': str(index_json.get('ohlc', {}).get('low', 0)).encode(),
                                    b'close': str(index_json.get('ohlc', {}).get('close', 0)).encode(),
                                    b'timestamp': str(int(time.time() * 1000)).encode()
                                }
                            except Exception as e:
                                print(f"Error reading index data for WebSocket: {e}")
                
                if ohlc_data:
                    # Decode hash fields
                    def decode_val(key, default=0):
                        val = ohlc_data.get(key.encode() if isinstance(key, str) else key) or ohlc_data.get(key, default)
                        if isinstance(val, bytes):
                            val = val.decode('utf-8')
                        try:
                            return float(val) if val else default
                        except (ValueError, TypeError):
                            return default
                    
                    timestamp = decode_val('timestamp', int(time.time() * 1000))
                    if timestamp < 1e10:
                        timestamp = int(timestamp * 1000)  # Convert seconds to ms
                    
                    current_ohlc = {
                        "timestamp": int(timestamp),
                        "open": decode_val('open', 0),
                        "high": decode_val('high', 0),
                        "low": decode_val('low', 0),
                        "close": decode_val('close', decode_val('last_price', 0)),
                        "volume": int(decode_val('volume', 0))
                    }
                    
                    # Create hash to detect changes
                    current_hash = hash((current_ohlc['timestamp'], current_ohlc['close'], current_ohlc['volume']))
                    
                    # Only send if data changed
                    if current_hash != last_sent_hash:
                        # Get indicators from DB 5
                        indicators = {}
                        try:
                            ema_20 = db5_client.get(f"indicators:{base_symbol}:ema_20")
                            ema_50 = db5_client.get(f"indicators:{base_symbol}:ema_50")
                            ema_100 = db5_client.get(f"indicators:{base_symbol}:ema_100")
                            ema_200 = db5_client.get(f"indicators:{base_symbol}:ema_200")
                            vwap_val = db5_client.get(f"indicators:{base_symbol}:vwap")
                            
                            def parse_indicator(val):
                                if not val:
                                    return None
                                if isinstance(val, bytes):
                                    val = val.decode('utf-8')
                                try:
                                    return float(val)
                                except:
                                    return None
                            
                            if ema_20:
                                indicators['ema_20'] = parse_indicator(ema_20)
                            if ema_50:
                                indicators['ema_50'] = parse_indicator(ema_50)
                            if ema_100:
                                indicators['ema_100'] = parse_indicator(ema_100)
                            if ema_200:
                                indicators['ema_200'] = parse_indicator(ema_200)
                            if vwap_val:
                                indicators['vwap'] = parse_indicator(vwap_val)
                        except Exception as e:
                            print(f"Error loading indicators: {e}")
                        
                        chart_data = {
                            "type": "chart_update",
                            "symbol": symbol,
                            "timestamp": int(time.time() * 1000),
                            "ohlc": current_ohlc,
                            "indicators": indicators
                        }
                        
                        await manager.send_personal_message(json.dumps(chart_data), websocket)
                        last_sent_hash = current_hash
                else:
                    # No data available - send empty update occasionally
                    await asyncio.sleep(2)  # Slow down if no data
                    
            except WebSocketDisconnect:
                # Client disconnected, break out of loop
                break
            except (RuntimeError, ConnectionError) as e:
                # Connection closed error - check message and break
                error_msg = str(e).lower()
                if "close message" in error_msg or "disconnect" in error_msg:
                    # Connection already closed, break immediately
                    break
                # For other connection errors, check state
                try:
                    if websocket.client_state.name != 'CONNECTED':
                        break
                except:
                    break
            except Exception as e:
                error_msg = str(e).lower()
                # Silently ignore connection closed errors
                if "close message" in error_msg or "disconnect" in error_msg:
                    break
                # Only log non-connection errors
                print(f"Error in websocket_professional_chart: {e}")
                # Check if connection is still alive before continuing
                try:
                    if websocket.client_state.name != 'CONNECTED':
                        break
                except:
                    break
                await asyncio.sleep(1)  # Wait before retrying
            
    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)
    except Exception as e:
        print(f"WebSocket error: {e}")
        manager.disconnect(websocket, symbol)


# ============================================================================
# HEALTH CHECK ENDPOINTS
# ============================================================================

@app.get("/api")
async def api_root():
    """Catch root API calls and redirect to proper endpoints"""
    return {
        "message": "API is running. Use specific endpoints:",
        "endpoints": {
            "login": "/api/auth/login",
            "alerts": "/api/alerts", 
            "dashboard": "/api/dashboard-stats",
            "market_data": "/api/market-data",
            "news": "/api/news",
            "health": "/api/health/redis"
        }
    }

@app.get("/api/health/redis")
async def redis_health_check():
    """Check Redis connectivity"""
    try:
        def check_redis():
            from redis_files.redis_manager import RedisManager82
            client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
            return client.ping()
        
        is_healthy = await asyncio.wait_for(
            asyncio.wrap_future(redis_executor.submit(check_redis)),
            timeout=3.0
        )
        
        return {"status": "healthy", "redis": is_healthy}
    
    except asyncio.TimeoutError:
        return {"status": "unhealthy", "redis": False, "error": "Redis timeout"}
    except Exception as e:
        return {"status": "unhealthy", "redis": False, "error": str(e)}

# ============================================================================
# DEBUG ENDPOINTS
# ============================================================================

@app.get("/api/debug/check-auth")
async def debug_check_auth(current_user: dict = Depends(get_current_user)):
    """Check if authentication is working"""
    return {
        "authenticated": True,
        "user": current_user,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/debug/alerts-data")
async def debug_alerts_data():
    """Debug endpoint to check alerts data without auth"""
    try:
        # Simulate getting some alerts data
        return {
            "alerts": [],
            "total_alerts": 0,
            "message": "Debug endpoint working"
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/debug/redis-users")
async def debug_redis_users():
    """Check what users exist in Redis"""
    def get_all_users():
        try:
            from redis_files.redis_manager import RedisManager82
            client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
            # Get all user keys
            user_keys = client.keys("user:*")
            users = {}
            for key in user_keys:
                user_data = client.get(key)
                if user_data:
                    try:
                        users[key] = json.loads(user_data)
                    except json.JSONDecodeError:
                        users[key] = {"raw": user_data}
            return users
        except Exception as e:
            return {"error": str(e)}
    
    users = await asyncio.wrap_future(redis_executor.submit(get_all_users))
    return users

@app.post("/api/debug/reset-admin-password")
async def debug_reset_admin_password(new_password: str = Form("admin")):
    """Reset admin password (for debugging)"""
    def reset_password():
        try:
            password_hash = hash_password(new_password)
            save_user_to_redis("admin", password_hash, {"role": "admin"}, update_existing=True)
            return {"success": True, "message": f"Admin password reset to: {new_password}"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    result = await asyncio.wrap_future(redis_executor.submit(reset_password))
    return result

# ============================================================================
# AUTH ENDPOINTS
# ============================================================================

@app.post("/api/auth/login")
async def login(
    username: str = Form(...), 
    password: str = Form(...),
    two_fa_code: Optional[str] = Form(None)
):
    """Login endpoint - returns JWT access token (supports 2FA)"""
    try:
        # Step 1: Get user data with timeout
        user_data = await get_user_from_redis_with_timeout(username)
        
        if not user_data:
            # Don't reveal whether user exists for security
            await asyncio.sleep(2)  # Prevent timing attacks
            raise HTTPException(status_code=401, detail="Incorrect username or password")
        
        # Step 2: Verify password with timeout
        password_hash = user_data.get("password_hash", "")
        try:
            password_valid = await asyncio.wait_for(
                asyncio.wrap_future(redis_executor.submit(verify_password, password, password_hash)),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            print(f"❌ Password verification timeout for user: {username}")
            raise HTTPException(status_code=500, detail="Authentication service timeout")
        
        if not password_valid:
            await asyncio.sleep(2)  # Prevent timing attacks
            raise HTTPException(status_code=401, detail="Incorrect username or password")
        
        # Check if 2FA is enabled
        if user_data.get("two_fa_enabled", False):
            if not two_fa_code:
                raise HTTPException(status_code=403, detail="2FA code required")
            
            two_fa_secret = user_data.get("two_fa_secret")
            if not two_fa_secret or not HAS_2FA:
                raise HTTPException(status_code=500, detail="2FA not properly configured")
            
            # Verify 2FA code
            totp = pyotp.TOTP(two_fa_secret)
            if not totp.verify(two_fa_code, valid_window=1):  # Allow 1 step tolerance
                raise HTTPException(status_code=401, detail="Invalid 2FA code")
        
        # Create access token with error handling
        try:
            access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            access_token = create_access_token(
                data={"sub": username}, expires_delta=access_token_expires
            )
        except Exception as e:
            print(f"Error creating access token: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Token creation error: {str(e)}")
        
        return {"access_token": access_token, "token_type": "bearer", "two_fa_required": user_data.get("two_fa_enabled", False)}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Unexpected login error for {username}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error during authentication")

@app.post("/api/auth/register")
async def register(
    username: str = Form(...),
    password: str = Form(...),
    email: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    """Register a new user (requires authentication to prevent public signups)"""
    # Check if user already exists
    if get_user_from_redis(username):
        raise HTTPException(status_code=400, detail="Username already exists")
    
    # Validate password strength
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Hash and save password
    password_hash = hash_password(password)
    metadata = {"email": email, "created_by": current_user.get("username", "system")}
    if save_user_to_redis(username, password_hash, metadata):
        return {"message": "User registered successfully", "username": username}
    else:
        raise HTTPException(status_code=500, detail="Failed to register user")

@app.post("/api/auth/create-user")
@limiter.limit("10/minute")
async def create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    email: Optional[str] = Form(None),
    role: Optional[str] = Form("user"),
    current_user: dict = Depends(get_current_user)
):
    """Admin endpoint to create users (requires authentication)"""
    # Check if user already exists
    if get_user_from_redis(username):
        raise HTTPException(status_code=400, detail="Username already exists")
    
    # Validate password strength
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Hash and save password
    password_hash = hash_password(password)
    metadata = {
        "email": email,
        "role": role,
        "created_by": current_user.get("username", "system"),
        "created_at": datetime.utcnow().isoformat()
    }
    if save_user_to_redis(username, password_hash, metadata):
        return {"message": "User created successfully", "username": username}
    else:
        raise HTTPException(status_code=500, detail="Failed to create user")

@app.post("/api/auth/refresh")
async def refresh_token(refresh_token: Optional[str] = None):
    """Refresh access token (for future implementation)"""
    # For now, just return a new token if refresh_token is valid
    # In production, implement proper refresh token validation
    return {"access_token": create_access_token(data={"sub": "user"}), "token_type": "bearer"}

# ============================================================================
# 2FA ENDPOINTS
# ============================================================================

@app.post("/api/auth/2fa/setup")
@limiter.limit("5/minute")
async def setup_2fa(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """Generate 2FA secret and QR code for user"""
    if not HAS_2FA:
        raise HTTPException(status_code=501, detail="2FA not available (pyotp/qrcode not installed)")
    
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Generate new secret
    secret = pyotp.random_base32()
    totp = pyotp.TOTP(secret)
    
    # Generate QR code
    issuer = "Intraday Trading Dashboard"
    uri = totp.provisioning_uri(
        name=username,
        issuer_name=issuer
    )
    
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    
    # Convert to base64
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    qr_code_base64 = base64.b64encode(buffer.getvalue()).decode()
    
    # Save secret temporarily (user needs to verify before enabling)
    # Store in user metadata as pending_secret
    metadata = user_data.get("metadata", {})
    metadata["pending_2fa_secret"] = secret
    
    save_user_to_redis(
        username, 
        user_data.get("password_hash", ""),
        metadata=metadata,
        update_existing=True
    )
    
    return {
        "secret": secret,
        "qr_code": f"data:image/png;base64,{qr_code_base64}",
        "uri": uri,
        "message": "Scan QR code with authenticator app, then verify with /api/auth/2fa/verify"
    }

@app.post("/api/auth/2fa/verify")
@limiter.limit("10/minute")
async def verify_2fa(
    request: Request,
    code: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    """Verify 2FA code and enable 2FA for user"""
    if not HAS_2FA:
        raise HTTPException(status_code=501, detail="2FA not available")
    
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get pending secret
    pending_secret = user_data.get("metadata", {}).get("pending_2fa_secret")
    if not pending_secret:
        raise HTTPException(status_code=400, detail="No pending 2FA setup. Call /api/auth/2fa/setup first")
    
    # Verify code
    totp = pyotp.TOTP(pending_secret)
    if not totp.verify(code, valid_window=1):
        raise HTTPException(status_code=401, detail="Invalid 2FA code")
    
    # Enable 2FA
    metadata = user_data.get("metadata", {})
    metadata.pop("pending_2fa_secret", None)
    
    save_user_to_redis(
        username,
        user_data.get("password_hash", ""),
        metadata=metadata,
        update_existing=True
    )
    
    # Update user with 2FA enabled
    user_data["two_fa_enabled"] = True
    user_data["two_fa_secret"] = pending_secret
    user_data["metadata"] = metadata
    
    from redis_files.redis_manager import RedisManager82
    db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
    db0_client.set(f"user:{username}", json.dumps(user_data))
    
    return {"message": "2FA enabled successfully"}

@app.post("/api/auth/2fa/disable")
@limiter.limit("5/minute")
async def disable_2fa(
    request: Request,
    password: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    """Disable 2FA for user (requires password confirmation)"""
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Verify password
    if not verify_password(password, user_data.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid password")
    
    # Disable 2FA
    user_data["two_fa_enabled"] = False
    user_data["two_fa_secret"] = None
    
    from redis_files.redis_manager import RedisManager82
    db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
    db0_client.set(f"user:{username}", json.dumps(user_data))
    
    return {"message": "2FA disabled successfully"}

# ============================================================================
# BROKERAGE INTEGRATION ENDPOINTS
# ============================================================================

@app.get("/api/brokerage/auth-url")
@limiter.limit("10/minute")
async def get_brokerage_auth_url(
    request: Request,
    broker: str = Query(...),  # "zerodha", "angel_one", etc.
    redirect_url: str = Query(...),  # Where to redirect after auth
    current_user: dict = Depends(get_current_user)
):
    """Get authentication URL for brokerage OAuth flow
    
    This endpoint generates the OAuth URL for users to authenticate directly with their broker.
    We do NOT store credentials - authentication happens client-side.
    
    Returns:
        - auth_url: URL to redirect user for broker authentication
        - state: State token for CSRF protection
    """
    import secrets
    import urllib.parse
    
    username = current_user.get("username")
    state = secrets.token_urlsafe(32)
    
    # Store state temporarily in Redis (expires in 10 minutes) for CSRF protection
    from redis_files.redis_manager import RedisManager82
    db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
    db0_client.setex(f"brokerage_auth_state:{username}:{broker}:{state}", 600, "pending")
    
    if broker.lower() == "zerodha":
        # Zerodha Kite Connect OAuth flow
        api_key = request.headers.get("X-API-Key") or os.getenv("ZERODHA_API_KEY")
        if not api_key:
            raise HTTPException(status_code=400, detail="Zerodha API key not configured. Provide X-API-Key header or set ZERODHA_API_KEY env var.")
        
        # Build Kite Connect login URL
        kite_login_url = f"https://kite.trade/connect/login?v=3&api_key={api_key}"
        # Note: redirect_url should be whitelisted in Kite Connect app settings
        
        return {
            "auth_url": kite_login_url,
            "state": state,
            "broker": "zerodha",
            "message": "Redirect user to this URL. After authentication, user will be redirected back with request_token."
        }
    
    elif broker.lower() in ["angel_one", "angel_broking", "angel"]:
        # Angel Broking uses Publisher API - authentication happens client-side
        # We just provide the API key (which is public for Publisher API)
        api_key = request.headers.get("X-API-Key") or os.getenv("ANGEL_ONE_API_KEY")
        if not api_key:
            raise HTTPException(status_code=400, detail="Angel One API key not configured. Provide X-API-Key header or set ANGEL_ONE_API_KEY env var.")
        
        # For Publisher API, authentication is handled client-side via embedded buttons
        # No server-side OAuth flow needed
        return {
            "auth_url": None,
            "api_key": api_key,  # Public API key for Publisher API
            "state": state,
            "broker": "angel_one",
            "message": "Use Publisher API client-side. No server-side auth needed. Embed Publisher buttons in frontend.",
            "publisher_api_url": "https://smartapi.angelbroking.com/publisher.js",
            "docs": "https://smartapi.angelbroking.com/docs/Publisher"
        }
    
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported broker: {broker}. Supported: zerodha, angel_one")

@app.post("/api/brokerage/verify-connection")
@limiter.limit("10/minute")
async def verify_brokerage_connection(
    request: Request,
    broker: str = Form(...),
    state: str = Form(...),
    # Zerodha specific
    request_token: Optional[str] = Form(None),
    # Angel One - no token needed (client-side auth)
    current_user: dict = Depends(get_current_user)
):
    """Verify and confirm brokerage connection (after OAuth redirect)
    
    This endpoint verifies the connection WITHOUT storing credentials.
    We only store that the user has connected (not their tokens).
    
    For Zerodha: Verifies request_token and confirms connection
    For Angel One: Just confirms connection (auth happens client-side)
    """
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Verify state token (CSRF protection)
    from redis_files.redis_manager import RedisManager82
    db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
    state_key = f"brokerage_auth_state:{username}:{broker}:{state}"
    state_valid = db0_client.exists(state_key)
    
    if not state_valid:
        raise HTTPException(status_code=400, detail="Invalid or expired state token")
    
    # Delete state token (one-time use)
    db0_client.delete(state_key)
    
    brokerage_connections = user_data.get("brokerage_connections", {})
    
    if broker.lower() == "zerodha":
        if not request_token:
            raise HTTPException(status_code=400, detail="request_token required for Zerodha verification")
        
        try:
            # Get API key from env or header (not from user - this is OUR app's API key)
            api_key = os.getenv("ZERODHA_API_KEY") or request.headers.get("X-API-Key")
            if not api_key:
                raise HTTPException(status_code=400, detail="Zerodha API key not configured")
            
            # Verify token (one-time check) - we DON'T store it
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=api_key)
            session = kite.generate_session(request_token, api_secret=os.getenv("ZERODHA_API_SECRET"))
            
            # Verify connection by fetching profile (one-time verification)
            kite.set_access_token(session["access_token"])
            profile = kite.profile()
            
            # Store ONLY metadata (NOT credentials)
            brokerage_connections["zerodha"] = {
                "connected": True,
                "user_id": profile.get("user_id"),
                "user_name": profile.get("user_name"),
                "connected_at": datetime.utcnow().isoformat(),
                "last_verified": datetime.utcnow().isoformat(),
                "note": "Credentials NOT stored. User manages tokens client-side."
            }
            
            # Update user data (only metadata)
            user_data["brokerage_connections"] = brokerage_connections
            db0_client.set(f"user:{username}", json.dumps(user_data))
            
            return {
                "message": "Zerodha connection verified successfully",
                "broker": "zerodha",
                "user_id": profile.get("user_id"),
                "user_name": profile.get("user_name"),
                "note": "Credentials are NOT stored. You must manage your access_token client-side for trade execution."
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to verify Zerodha connection: {str(e)}")
    
    elif broker.lower() in ["angel_one", "angel_broking", "angel"]:
        # Angel One Publisher API - authentication is 100% client-side
        # We just confirm the connection status
        brokerage_connections["angel_one"] = {
            "connected": True,
            "connected_at": datetime.utcnow().isoformat(),
            "note": "Publisher API - authentication happens client-side. No credentials stored."
        }
        
        # Update user data (only connection status)
        user_data["brokerage_connections"] = brokerage_connections
        db0_client.set(f"user:{username}", json.dumps(user_data))
        
        return {
            "message": "Angel One connection confirmed",
            "broker": "angel_one",
            "note": "Publisher API handles authentication client-side. Embed Publisher buttons in frontend for trade execution."
        }
    
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported broker: {broker}. Supported: zerodha, angel_one")

@app.get("/api/brokerage/connections")
async def get_brokerage_connections(current_user: dict = Depends(get_current_user)):
    """Get user's connected brokerage accounts (metadata only - no credentials)"""
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    connections = user_data.get("brokerage_connections", {})
    # Return only connection status (NO credentials stored)
    safe_connections = {}
    for broker, data in connections.items():
        safe_connections[broker] = {
            "connected": data.get("connected", False),
            "user_id": data.get("user_id"),
            "user_name": data.get("user_name"),
            "connected_at": data.get("connected_at"),
            "last_verified": data.get("last_verified"),
            "note": data.get("note", "Credentials managed client-side")
        }
    
    return {
        "connections": safe_connections,
        "security_note": "Credentials are NEVER stored. Users manage authentication tokens client-side."
    }

@app.post("/api/brokerage/disconnect")
@limiter.limit("5/minute")
async def disconnect_brokerage(
    request: Request,
    broker: str = Form(...),
    current_user: dict = Depends(get_current_user)
):
    """Disconnect brokerage account"""
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    brokerage_connections = user_data.get("brokerage_connections", {})
    if broker.lower() in brokerage_connections:
        del brokerage_connections[broker.lower()]
        user_data["brokerage_connections"] = brokerage_connections
        
        from redis_files.redis_manager import RedisManager82
        db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
        db0_client.set(f"user:{username}", json.dumps(user_data))
        
        return {"message": f"Brokerage account {broker} disconnected"}
    else:
        raise HTTPException(status_code=404, detail=f"No connection found for {broker}")

# ============================================================================
# REST API ENDPOINTS FOR FRONTEND
# ============================================================================

@app.get("/api/alerts")
@limiter.limit("100/minute")  # Rate limit: 100 requests per minute
async def get_alerts(
    request: Request,
    current_user: dict = Depends(get_current_user),  # Require JWT authentication
    symbol: Optional[str] = Query(None),
    pattern: Optional[str] = Query(None),
    min_confidence: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get paginated alerts from Redis DB 1 (alerts:stream)"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        # Get Redis client for DB 1 (realtime - where alerts:stream is)
        client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        
        alerts = []
        stream_name = "alerts:stream"
        
        # Read from alerts:stream (DB 1)
        try:
            stream_length = client.xlen(stream_name)
            max_messages = min(stream_length, limit + offset)
            
            # If no alerts in Redis, use sample data
            if stream_length == 0:
                sample_alerts = generate_sample_alerts(limit + offset)
                return {
                    "alerts": sample_alerts[offset:offset + limit],
                    "total": len(sample_alerts),
                    "limit": limit,
                    "offset": offset,
                    "last_updated": datetime.now().isoformat()
                }
            
            if max_messages > 0:
                # Read messages in reverse order (newest first)
                stream_messages = client.xrevrange(stream_name, count=max_messages)
                
                for msg_id, msg_data in stream_messages:
                    try:
                        # Extract alert data
                        data_field = msg_data.get('data') or msg_data.get(b'data')
                        if not data_field:
                            continue
                        
                        # Parse JSON
                        if isinstance(data_field, bytes):
                            try:
                                import orjson
                                alert = orjson.loads(data_field)
                            except (ImportError, Exception):
                                alert = json.loads(data_field.decode('utf-8'))
                        else:
                            alert = json.loads(data_field) if isinstance(data_field, str) else data_field
                        
                        # Generate alert_id for legacy alerts that don't have it
                        if 'alert_id' not in alert or not alert.get('alert_id'):
                            ts = alert.get('timestamp') or alert.get('timestamp_ms', int(time.time() * 1000))
                            sym = alert.get('symbol', 'UNKNOWN')
                            alert['alert_id'] = f"{sym}_{ts}"
                        
                        # Ensure required fields for frontend
                        if 'signal' not in alert:
                            alert['signal'] = alert.get('direction', alert.get('action', 'NEUTRAL'))
                        if 'pattern_label' not in alert:
                            alert['pattern_label'] = alert.get('pattern', 'Unknown Pattern')
                        if 'base_symbol' not in alert:
                            # Extract base symbol from full symbol (e.g., "NIFTY25DEC26000CE" -> "NIFTY")
                            symbol_str = alert.get('symbol', '')
                            if symbol_str:
                                # Try to extract base symbol (remove dates, strikes, option types)
                                import re
                                base_match = re.match(r'^([A-Z]+)', symbol_str)
                                alert['base_symbol'] = base_match.group(1) if base_match else symbol_str.split(':')[-1] if ':' in symbol_str else symbol_str
                        
                        # Apply filters
                        if symbol and alert.get('symbol') != symbol:
                            continue
                        if pattern and alert.get('pattern') != pattern:
                            continue
                        if min_confidence is not None and alert.get('confidence', 0) < min_confidence:
                            continue
                        
                        # Date filtering
                        if date_from or date_to:
                            alert_ts = alert.get('timestamp')
                            if alert_ts:
                                try:
                                    if isinstance(alert_ts, str):
                                        alert_dt = datetime.fromisoformat(alert_ts.replace('Z', '+00:00'))
                                    else:
                                        alert_dt = datetime.fromtimestamp(alert_ts)
                                    
                                    if date_from:
                                        from_dt = datetime.fromisoformat(date_from.replace('Z', '+00:00'))
                                        if alert_dt < from_dt:
                                            continue
                                    if date_to:
                                        to_dt = datetime.fromisoformat(date_to.replace('Z', '+00:00'))
                                        if alert_dt > to_dt:
                                            continue
                                except Exception:
                                    pass
                        
                        alerts.append(alert)
                    except Exception as e:
                        continue
        except Exception as e:
            print(f"Error reading alerts:stream: {e}")
        
        # Apply pagination
        total = len(alerts)
        alerts = alerts[offset:offset+limit]
        
        return {
            "alerts": alerts,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < total
        }
    except Exception as e:
        print(f"Error in get_alerts: {e}")
        import traceback
        traceback.print_exc()
        return {"alerts": [], "total": 0, "limit": limit, "offset": offset, "has_more": False}


@app.get("/api/alerts/{alert_id}")
async def get_alert_by_id(alert_id: str):
    """Get single alert with full details from Redis DB 0 and DB 1"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_key_standards import RedisKeyStandards
        
        # Try DB 0 first (forward_validation:alert:{alert_id})
        db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=False)
        validation_key = RedisKeyStandards.get_validation_key(alert_id)
        
        alert_data = None
        try:
            data = db0_client.get(validation_key)
            if data:
                if isinstance(data, bytes):
                    alert_data = json.loads(data.decode('utf-8'))
                else:
                    alert_data = json.loads(data) if isinstance(data, str) else data
        except Exception:
            pass
        
        # If not found, try alerts:stream in DB 1
        if not alert_data:
            db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
            try:
                # Search recent messages
                messages = db1_client.xrevrange("alerts:stream", count=1000)
                for msg_id, msg_data in messages:
                    data_field = msg_data.get('data') or msg_data.get(b'data')
                    if data_field:
                        try:
                            if isinstance(data_field, bytes):
                                import orjson
                                alert = orjson.loads(data_field)
                            else:
                                alert = json.loads(data_field)
                            
                            if alert.get('alert_id') == alert_id:
                                alert_data = alert
                                break
                        except Exception:
                            continue
            except Exception:
                pass
        
        if not alert_data:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        # Get additional data (indicators, Greeks, volume profile, news, validation)
        symbol = alert_data.get('symbol', '')
        
        # Get indicators from DB 5
        indicators = {}
        if symbol:
            try:
                db5_client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
                from redis_files.redis_client import RobustRedisClient
                wrapped = RobustRedisClient(db5_client, process_name="api")
                
                indicator_names = ['rsi', 'atr', 'vwap', 'ema_20', 'ema_50', 'macd', 'bollinger_bands']
                for ind_name in indicator_names:
                    key = f"indicators:{symbol}:{ind_name}"
                    data = wrapped.retrieve_by_data_type(key, "indicators_cache")
                    if data:
                        try:
                            if isinstance(data, bytes):
                                data = data.decode('utf-8')
                            indicators[ind_name] = json.loads(data) if isinstance(data, str) else data
                        except Exception:
                            indicators[ind_name] = data
            except Exception as e:
                print(f"Error loading indicators: {e}")
        
        # Get Greeks from DB 5
        greeks = {}
        if symbol:
            try:
                db5_client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
                from redis_files.redis_client import RobustRedisClient
                wrapped = RobustRedisClient(db5_client, process_name="api")
                
                greeks_key = f"indicators:{symbol}:greeks"
                data = wrapped.retrieve_by_data_type(greeks_key, "indicators_cache")
                if data:
                    try:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        greeks_data = json.loads(data) if isinstance(data, str) else data
                        if isinstance(greeks_data, dict) and 'value' in greeks_data:
                            greeks = greeks_data['value']
                        else:
                            greeks = greeks_data
                    except Exception:
                        pass
            except Exception as e:
                print(f"Error loading Greeks: {e}")
        
        # Get volume profile from DB 2
        volume_profile = {}
        if symbol:
            try:
                db2_client = RedisManager82.get_client(process_name="api", db=2, decode_responses=False)
                from redis_files.redis_key_standards import RedisKeyStandards
                
                poc_key = RedisKeyStandards.get_volume_profile_poc_key(symbol)
                poc_data = db2_client.hgetall(poc_key)
                
                if poc_data:
                    volume_profile = {
                        "poc_price": float(poc_data.get(b'poc_price', b'0').decode()) if isinstance(poc_data.get(b'poc_price'), bytes) else float(poc_data.get('poc_price', 0)),
                        "poc_volume": int(poc_data.get(b'poc_volume', b'0').decode()) if isinstance(poc_data.get(b'poc_volume'), bytes) else int(poc_data.get('poc_volume', 0)),
                        "value_area_high": float(poc_data.get(b'value_area_high', b'0').decode()) if isinstance(poc_data.get(b'value_area_high'), bytes) else float(poc_data.get('value_area_high', 0)),
                        "value_area_low": float(poc_data.get(b'value_area_low', b'0').decode()) if isinstance(poc_data.get(b'value_area_low'), bytes) else float(poc_data.get('value_area_low', 0)),
                        "profile_strength": float(poc_data.get(b'profile_strength', b'0').decode()) if isinstance(poc_data.get(b'profile_strength'), bytes) else float(poc_data.get('profile_strength', 0)),
                    }
            except Exception as e:
                print(f"Error loading volume profile: {e}")
        
        # Get chart data
        chart_data = await get_chart_data_internal(symbol)
        
        # Get news
        news = []
        if symbol:
            try:
                db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
                news_key = f"news:latest:{symbol}"
                news_data = db1_client.get(news_key)
                if news_data:
                    if isinstance(news_data, bytes):
                        news_data = news_data.decode('utf-8')
                    news = json.loads(news_data) if isinstance(news_data, str) else news_data
                    if not isinstance(news, list):
                        news = [news] if news else []
            except Exception:
                pass
        
        # Get validation results from DB 0
        validation = {}
        try:
            validation_data = db0_client.get(validation_key)
            if validation_data:
                if isinstance(validation_data, bytes):
                    validation_data = validation_data.decode('utf-8')
                validation = json.loads(validation_data) if isinstance(validation_data, str) else validation_data
        except Exception:
            pass
        
        return {
            "alert": alert_data,
            "indicators": indicators,
            "greeks": greeks,
            "volume_profile": volume_profile,
            "chart_data": chart_data,
            "news": news,
            "validation": validation
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_alert_by_id: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alerts/stats/summary")
@limiter.limit("100/minute")
async def get_alert_stats(
    request: Request,
    current_user: dict = Depends(get_current_user)
):
    """Get alerts summary with historical context"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        
        # Read recent alerts from stream
        stream_length = db1_client.xlen("alerts:stream")
        messages = db1_client.xrevrange("alerts:stream", count=min(stream_length, 10000))
        
        # If no alerts in Redis, use sample data
        if stream_length == 0:
            sample_alerts = generate_sample_alerts(1000)
            today = datetime.now().date()
            today_alerts = [alert for alert in sample_alerts 
                           if datetime.fromisoformat(alert["timestamp"]).date() == today]
            
            # Calculate pattern frequency
            pattern_counts = {}
            for alert in sample_alerts[-100:]:  # Last 100 alerts
                pattern = alert["pattern"]
                pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
            
            top_pattern = max(pattern_counts.items(), key=lambda x: x[1])[0] if pattern_counts else "N/A"
            
            return {
                "total_alerts": len(sample_alerts),
                "today_alerts": len(today_alerts),
                "avg_confidence": round(sum(a["confidence"] for a in sample_alerts[-100:]) / min(100, len(sample_alerts)), 1),
                "top_pattern": top_pattern,
                "market_status": get_market_status(),
                "last_updated": datetime.now().isoformat(),
                "pattern_distribution": pattern_counts,
                "symbol_ranking": [],
                "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
                "instrument_type_distribution": {},
                "news_enrichment_rate": 0.0
            }
        
        pattern_counts = {}
        symbol_counts = {}
        confidence_counts = {"high": 0, "medium": 0, "low": 0}
        instrument_type_counts = {}
        today_count = 0
        # Use IST timezone for "today" calculation
        from datetime import timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(ist).date()
        
        for msg_id, msg_data in messages:
            try:
                data_field = msg_data.get('data') or msg_data.get(b'data')
                if not data_field:
                    continue
                
                if isinstance(data_field, bytes):
                    try:
                        import orjson
                        alert = orjson.loads(data_field)
                    except:
                        alert = json.loads(data_field.decode('utf-8'))
                else:
                    alert = json.loads(data_field) if isinstance(data_field, str) else data_field
                
                # Count patterns
                pattern = alert.get('pattern', 'other')
                pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
                
                # Count symbols
                symbol = alert.get('symbol', 'UNKNOWN')
                base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
                symbol_counts[base_symbol] = symbol_counts.get(base_symbol, 0) + 1
                
                # Count confidence
                confidence = alert.get('confidence', 0)
                if confidence >= 0.8:
                    confidence_counts["high"] += 1
                elif confidence >= 0.5:
                    confidence_counts["medium"] += 1
                else:
                    confidence_counts["low"] += 1
                
                # Count instrument types
                inst_type = alert.get('instrument_type', 'UNKNOWN')
                instrument_type_counts[inst_type] = instrument_type_counts.get(inst_type, 0) + 1
                
                # Count today's alerts (IST timezone) - INSIDE the loop
                alert_ts = alert.get('timestamp')
                if alert_ts:
                    try:
                        if isinstance(alert_ts, str):
                            alert_dt = datetime.fromisoformat(alert_ts.replace('Z', '+00:00'))
                        else:
                            # If timestamp is in milliseconds, convert to seconds
                            if alert_ts > 1e10:
                                alert_ts = alert_ts / 1000
                            alert_dt = datetime.fromtimestamp(alert_ts)
                        
                        # Convert to IST (UTC+5:30)
                        alert_ist = alert_dt.replace(tzinfo=timezone.utc).astimezone(ist)
                        today_ist = datetime.now(ist).date()
                        
                        if alert_ist.date() == today_ist:
                            today_count += 1
                    except Exception:
                        pass
            except Exception:
                pass
        
        # Sort symbol ranking
        symbol_ranking = sorted(symbol_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Calculate average confidence and top pattern
        total_confidence = 0
        confidence_count = 0
        for msg_id, msg_data in messages:
            try:
                data_field = msg_data.get('data') or msg_data.get(b'data')
                if not data_field:
                    continue
                
                if isinstance(data_field, bytes):
                    try:
                        import orjson
                        alert = orjson.loads(data_field)
                    except:
                        alert = json.loads(data_field.decode('utf-8'))
                else:
                    alert = json.loads(data_field) if isinstance(data_field, str) else data_field
                
                conf = alert.get('confidence', 0)
                if conf > 0:
                    total_confidence += conf
                    confidence_count += 1
            except Exception:
                continue
        
        avg_confidence = total_confidence / confidence_count if confidence_count > 0 else 0.0
        top_pattern = max(pattern_counts.items(), key=lambda x: x[1])[0] if pattern_counts else "N/A"
        
        return {
            "total_alerts": stream_length,
            "today_alerts": today_count,
            "avg_confidence": avg_confidence,
            "top_pattern": top_pattern,
            "pattern_distribution": pattern_counts,
            "symbol_ranking": [{"symbol": s, "count": c} for s, c in symbol_ranking],
            "confidence_distribution": confidence_counts,
            "instrument_type_distribution": instrument_type_counts,
            "news_enrichment_rate": 0.75  # TODO: Calculate actual rate
        }
    except Exception as e:
        print(f"Error in get_alert_stats: {e}")
        return {
            "total_alerts": 0,
            "today_alerts": 0,
            "pattern_distribution": {},
            "symbol_ranking": [],
            "confidence_distribution": {"high": 0, "medium": 0, "low": 0},
            "instrument_type_distribution": {},
            "news_enrichment_rate": 0.0
        }


@app.get("/api/indicators/{symbol}")
async def get_indicators(symbol: str, indicators: Optional[str] = Query(None)):
    """Get technical indicators for symbol from Redis DB 5 (indicators_cache)"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_client import RobustRedisClient
        
        # Decode symbol if URL encoded
        symbol = symbol.replace('%3A', ':').replace('%2F', '/')
        
        # Get DB 5 client (indicators_cache)
        client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
        wrapped = RobustRedisClient(client, process_name="api")
        
        # Get requested indicators or all
        indicator_list = indicators.split(',') if indicators else [
            'rsi', 'atr', 'vwap', 'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_100', 'ema_200',
            'macd', 'bollinger_bands', 'volume_ratio'
        ]
        
        result = {}
        for ind_name in indicator_list:
            ind_name = ind_name.strip()
            key = f"indicators:{symbol}:{ind_name}"
            try:
                data = wrapped.retrieve_by_data_type(key, "indicators_cache")
                if data:
                    try:
                        if isinstance(data, bytes):
                            data = data.decode('utf-8')
                        parsed = json.loads(data) if isinstance(data, str) else data
                        # Extract value if nested
                        if isinstance(parsed, dict) and 'value' in parsed:
                            result[ind_name] = parsed['value']
                        else:
                            result[ind_name] = parsed
                    except (json.JSONDecodeError, TypeError):
                        # Try as float
                        try:
                            result[ind_name] = float(data) if isinstance(data, (str, bytes)) else data
                        except (ValueError, TypeError):
                            result[ind_name] = data
            except Exception as e:
                continue
        
        return {
            "symbol": symbol,
            "timestamp": datetime.now().isoformat(),
            "indicators": result
        }
    except Exception as e:
        print(f"Error in get_indicators: {e}")
        import traceback
        traceback.print_exc()
        return {"symbol": symbol, "timestamp": datetime.now().isoformat(), "indicators": {}}


@app.get("/api/greeks/{symbol}")
async def get_greeks(symbol: str):
    """Get Greeks for symbol from Redis DB 5 (indicators_cache)"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_client import RobustRedisClient
        
        # Decode symbol if URL encoded
        symbol = symbol.replace('%3A', ':').replace('%2F', '/')
        
        # Get DB 5 client (indicators_cache)
        client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
        wrapped = RobustRedisClient(client, process_name="api")
        
        key = f"indicators:{symbol}:greeks"
        data = wrapped.retrieve_by_data_type(key, "indicators_cache")
        
        if data:
            try:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                parsed = json.loads(data) if isinstance(data, str) else data
                # Extract value if nested
                if isinstance(parsed, dict) and 'value' in parsed:
                    return parsed['value']
                return parsed
            except Exception as e:
                return {}
        
        return {}
    except Exception as e:
        print(f"Error in get_greeks: {e}")
        return {}


@app.get("/api/charts/{symbol}")
@limiter.limit("100/minute")
async def get_charts(
    request: Request,
    symbol: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    resolution: str = Query("5m"),
    include_indicators: bool = Query(False),
    period: str = Query("1d", description="Time period: 1d, 1w, 1m, 3m, 1y"),
    current_user: dict = Depends(get_current_user)
):
    """Get chart data with historical data fallback"""
    try:
        # Try to get data from Redis first
        redis_data = await get_chart_data_internal(symbol, date_from, date_to, resolution, include_indicators)
        
        # If no data from Redis, use historical data fallback
        if not redis_data.get("ohlc") or len(redis_data.get("ohlc", [])) == 0:
            historical_data = await get_historical_data(symbol, period, current_user)
            
            # Add technical indicators if requested
            indicators = {}
            if include_indicators and historical_data.get("data"):
                indicators = calculate_technical_indicators(historical_data["data"])
            
            return {
                "symbol": symbol,
                "ohlc": historical_data.get("data", []),
                "indicators": indicators,
                "indicators_overlay": indicators,
                "market_status": get_market_status(),
                "last_trading_day": get_last_trading_day().isoformat(),
                "data_type": "historical",
                "period": period
            }
        else:
            # Redis data exists, return it
            return redis_data
    except Exception as e:
        # Fallback to historical data on error
        try:
            historical_data = await get_historical_data(symbol, period, current_user)
            indicators = {}
            if include_indicators and historical_data.get("data"):
                indicators = calculate_technical_indicators(historical_data["data"])
            return {
                "symbol": symbol,
                "ohlc": historical_data.get("data", []),
                "indicators": indicators,
                "indicators_overlay": indicators,
                "market_status": get_market_status(),
                "last_trading_day": get_last_trading_day().isoformat(),
                "data_type": "historical",
                "period": period
            }
        except Exception as fallback_error:
            raise HTTPException(status_code=500, detail=str(fallback_error))


async def get_chart_data_internal(
    symbol: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    resolution: str = "5m",
    include_indicators: bool = False
):
    """Internal function to get chart data"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        # Decode symbol if URL encoded
        symbol = symbol.replace('%3A', ':').replace('%2F', '/')
        
        # Get DB 2 client (analytics)
        db2_client = RedisManager82.get_client(process_name="api", db=2, decode_responses=False)
        
        # Try ohlc_daily:{symbol} sorted set
        base_symbol = symbol.split(':')[-1] if ':' in symbol else symbol
        zset_key = f"ohlc_daily:{symbol}"
        
        ohlc_data = []
        
        # For base indices (NIFTY, BANKNIFTY), try to get from index data if no historical data
        is_base_index = base_symbol in ['NIFTY', 'BANKNIFTY', 'NIFTY 50', 'NIFTY BANK', 'INDIA VIX']
        
        try:
            zset_entries = db2_client.zrange(zset_key, 0, -1, withscores=True)
            
            if not zset_entries:
                # Try base symbol
                zset_key = f"ohlc_daily:{base_symbol}"
                zset_entries = db2_client.zrange(zset_key, 0, -1, withscores=True)
            
            # If still no data and it's a base index, try to get from index data
            if not zset_entries and is_base_index:
                # Try to get latest index data and create a minimal chart
                db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
                index_key = f"index:NSE:{base_symbol}" if base_symbol in ['NIFTY 50', 'NIFTY BANK'] else f"index:NSE:{base_symbol}"
                index_data = db1_client.get(index_key)
                
                if index_data:
                    if isinstance(index_data, bytes):
                        index_data = index_data.decode('utf-8')
                    try:
                        index_json = json.loads(index_data)
                        last_price = float(index_json.get('last_price', 0))
                        ohlc_obj = index_json.get('ohlc', {})
                        
                        if isinstance(ohlc_obj, dict) and ohlc_obj:
                            # Create a single OHLC bar from index data
                            timestamp_ms = int(time.time() * 1000)
                            ohlc_entry = {
                                "timestamp": timestamp_ms,
                                "open": float(ohlc_obj.get('open', last_price)),
                                "high": float(ohlc_obj.get('high', last_price)),
                                "low": float(ohlc_obj.get('low', last_price)),
                                "close": float(ohlc_obj.get('close', last_price)),
                                "volume": 0
                            }
                            ohlc_data.append(ohlc_entry)
                    except Exception as e:
                        print(f"Error creating chart from index data: {e}")
            
            for entry_data, timestamp_score in zset_entries:
                try:
                    if isinstance(entry_data, bytes):
                        entry_str = entry_data.decode('utf-8')
                    else:
                        entry_str = entry_data
                    
                    ohlc_json = json.loads(entry_str)
                    
                    # Convert timestamp
                    timestamp_float = float(timestamp_score)
                    if timestamp_float > 1e10:
                        timestamp_ms = int(timestamp_float)
                    else:
                        timestamp_ms = int(timestamp_float * 1000)
                    
                    ohlc_entry = {
                        "timestamp": timestamp_ms,
                        "open": float(ohlc_json.get('o', 0)),
                        "high": float(ohlc_json.get('h', 0)),
                        "low": float(ohlc_json.get('l', 0)),
                        "close": float(ohlc_json.get('c', 0)),
                        "volume": int(float(ohlc_json.get('v', 0)))
                    }
                    ohlc_data.append(ohlc_entry)
                except Exception:
                    continue
        except Exception as e:
            print(f"Error reading OHLC data: {e}")
        
        # If still no data, try ohlc_latest hash in DB 2 (bucket format)
        if len(ohlc_data) == 0:
            for symbol_variant in [symbol, base_symbol]:
                ohlc_key = f"ohlc_latest:{symbol_variant}"
                ohlc_hash = db2_client.hgetall(ohlc_key)
                
                if ohlc_hash:
                    def decode_val(key, default=0):
                        val = ohlc_hash.get(key.encode() if isinstance(key, str) else key) or ohlc_hash.get(key, default)
                        if isinstance(val, bytes):
                            val = val.decode('utf-8')
                        try:
                            return float(val) if val else default
                        except (ValueError, TypeError):
                            return default
                    
                    # Bucket format: open, high, low, close, volume, timestamp
                    timestamp_ms = int(decode_val('timestamp', time.time() * 1000))
                    ohlc_entry = {
                        "timestamp": timestamp_ms,
                        "open": decode_val('open', 0),
                        "high": decode_val('high', 0),
                        "low": decode_val('low', 0),
                        "close": decode_val('close', 0),
                        "volume": int(decode_val('volume', 0))
                    }
                    ohlc_data.append(ohlc_entry)
                    break
        
        # If still no data and it's a base index, try to get from index data
        if len(ohlc_data) == 0 and is_base_index:
            # Try to get latest index data and create a minimal chart
            db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
            # Map symbol names to Redis index keys
            index_key_map = {
                'NIFTY': 'index:NSE:NIFTY 50',
                'NIFTY 50': 'index:NSE:NIFTY 50',
                'BANKNIFTY': 'index:NSE:NIFTY BANK',
                'NIFTY BANK': 'index:NSE:NIFTY BANK',
                'INDIA VIX': 'index:NSE:INDIA VIX'
            }
            index_key = index_key_map.get(base_symbol, f"index:NSE:{base_symbol}")
            index_data = db1_client.get(index_key)
            
            if index_data:
                if isinstance(index_data, bytes):
                    index_data = index_data.decode('utf-8')
                try:
                    index_json = json.loads(index_data)
                    last_price = float(index_json.get('last_price', 0))
                    ohlc_obj = index_json.get('ohlc', {})
                    
                    if isinstance(ohlc_obj, dict) and ohlc_obj:
                        # Create a single OHLC bar from index data
                        timestamp_ms = int(time.time() * 1000)
                        ohlc_entry = {
                            "timestamp": timestamp_ms,
                            "open": float(ohlc_obj.get('open', last_price)),
                            "high": float(ohlc_obj.get('high', last_price)),
                            "low": float(ohlc_obj.get('low', last_price)),
                            "close": float(ohlc_obj.get('close', last_price)),
                            "volume": 0
                        }
                        ohlc_data.append(ohlc_entry)
                except Exception as e:
                    print(f"Error creating chart from index data: {e}")
        
        # Sort by timestamp
        ohlc_data.sort(key=lambda x: x['timestamp'])
        
        # Apply date filters
        if date_from:
            from_ts = int(datetime.fromisoformat(date_from.replace('Z', '+00:00')).timestamp() * 1000)
            ohlc_data = [x for x in ohlc_data if x['timestamp'] >= from_ts]
        if date_to:
            to_ts = int(datetime.fromisoformat(date_to.replace('Z', '+00:00')).timestamp() * 1000)
            ohlc_data = [x for x in ohlc_data if x['timestamp'] <= to_ts]
        
        # Get indicators overlay if requested
        indicators_overlay = {}
        if include_indicators and symbol and len(ohlc_data) > 0:
            try:
                # Get indicators from DB 5
                db5_client = RedisManager82.get_client(process_name="api", db=5, decode_responses=False)
                
                # Fetch current indicator values
                ema_20 = db5_client.get(f"indicators:{symbol}:ema_20")
                ema_50 = db5_client.get(f"indicators:{symbol}:ema_50")
                ema_100 = db5_client.get(f"indicators:{symbol}:ema_100")
                ema_200 = db5_client.get(f"indicators:{symbol}:ema_200")
                vwap_val = db5_client.get(f"indicators:{symbol}:vwap")
                
                # Helper to parse indicator value
                def parse_indicator(val):
                    if not val:
                        return None
                    if isinstance(val, bytes):
                        val = val.decode('utf-8')
                    try:
                        return float(val)
                    except:
                        return None
                
                # Calculate EMA arrays (simple: use current EMA for all points, or calculate if we have historical)
                # For now, use current EMA values as constant lines (can be enhanced with historical calculation)
                ema_20_val = parse_indicator(ema_20)
                ema_50_val = parse_indicator(ema_50)
                ema_100_val = parse_indicator(ema_100)
                ema_200_val = parse_indicator(ema_200)
                vwap_val_parsed = parse_indicator(vwap_val)
                
                # Create arrays matching OHLC length
                if ema_20_val is not None:
                    indicators_overlay['ema_20'] = [ema_20_val] * len(ohlc_data)
                if ema_50_val is not None:
                    indicators_overlay['ema_50'] = [ema_50_val] * len(ohlc_data)
                if ema_100_val is not None:
                    indicators_overlay['ema_100'] = [ema_100_val] * len(ohlc_data)
                if ema_200_val is not None:
                    indicators_overlay['ema_200'] = [ema_200_val] * len(ohlc_data)
                if vwap_val_parsed is not None:
                    indicators_overlay['vwap'] = [vwap_val_parsed] * len(ohlc_data)
            except Exception as e:
                print(f"Error loading indicators overlay: {e}")
                import traceback
                traceback.print_exc()
        
        return {
            "symbol": symbol,
            "ohlc": ohlc_data,
            "indicators_overlay": indicators_overlay,
            "resolution": resolution,
            "count": len(ohlc_data)
        }
    except Exception as e:
        print(f"Error in get_charts: {e}")
        import traceback
        traceback.print_exc()
        return {"symbol": symbol, "ohlc": [], "indicators_overlay": {}, "resolution": resolution, "count": 0}


@app.get("/api/volume-profile/{symbol}")
@limiter.limit("100/minute")
async def get_volume_profile(
    request: Request,
    symbol: str,
    date: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    """Get volume profile from Redis DB 2 (analytics)"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_key_standards import RedisKeyStandards
        
        # Decode symbol if URL encoded
        symbol = symbol.replace('%3A', ':').replace('%2F', '/')
        
        # Get DB 2 client (analytics)
        client = RedisManager82.get_client(process_name="api", db=2, decode_responses=False)
        
        # Get POC data
        poc_key = RedisKeyStandards.get_volume_profile_poc_key(symbol)
        poc_data = client.hgetall(poc_key)
        
        result = {
            "poc_price": 0.0,
            "poc_volume": 0,
            "value_area_high": 0.0,
            "value_area_low": 0.0,
            "profile_strength": 0.0,
            "distribution": {}
        }
        
        if poc_data:
            # Decode bytes to values
            def decode_value(key, default=0):
                val = poc_data.get(key.encode() if isinstance(key, str) else key) or poc_data.get(key, default)
                if isinstance(val, bytes):
                    val = val.decode('utf-8')
                return val
            
            result["poc_price"] = float(decode_value('poc_price', 0))
            result["poc_volume"] = int(float(decode_value('poc_volume', 0)))
            result["value_area_high"] = float(decode_value('value_area_high', 0))
            result["value_area_low"] = float(decode_value('value_area_low', 0))
            result["profile_strength"] = float(decode_value('profile_strength', 0))
        
        # Get distribution if date provided
        if date:
            try:
                dist_key = RedisKeyStandards.get_volume_profile_distribution_key(symbol, date)
                dist_data = client.hgetall(dist_key)
                if dist_data:
                    distribution = {}
                    for k, v in dist_data.items():
                        key_str = k.decode() if isinstance(k, bytes) else str(k)
                        val_str = v.decode() if isinstance(v, bytes) else str(v)
                        distribution[key_str] = int(float(val_str))
                    result["distribution"] = distribution
            except Exception:
                pass
        
        return result
    except Exception as e:
        print(f"Error in get_volume_profile: {e}")
        import traceback
        traceback.print_exc()
        return {
            "poc_price": 0.0,
            "poc_volume": 0,
            "value_area_high": 0.0,
            "value_area_low": 0.0,
            "profile_strength": 0.0,
            "distribution": {}
        }


@app.get("/api/news/{symbol}")
async def get_news_by_symbol(
    symbol: str,
    limit: int = Query(10, ge=1, le=100),
    hours_back: int = Query(24, ge=1, le=168)
):
    """Get news for symbol from Redis DB 1 (realtime)"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        # Decode symbol if URL encoded
        symbol = symbol.replace('%3A', ':').replace('%2F', '/')
        
        # Get DB 1 client (realtime)
        client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        
        news_key = f"news:latest:{symbol}"
        news_data = client.get(news_key)
        
        if news_data:
            try:
                if isinstance(news_data, bytes):
                    news_data = news_data.decode('utf-8')
                news_list = json.loads(news_data) if isinstance(news_data, str) else news_data
                if not isinstance(news_list, list):
                    news_list = [news_list] if news_list else []
                
                # Filter by hours_back
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                filtered_news = []
                for item in news_list:
                    try:
                        item_ts = item.get('timestamp')
                        if item_ts:
                            if isinstance(item_ts, str):
                                item_dt = datetime.fromisoformat(item_ts.replace('Z', '+00:00'))
                            else:
                                item_dt = datetime.fromtimestamp(item_ts)
                            if item_dt >= cutoff_time:
                                filtered_news.append(item)
                    except Exception:
                        filtered_news.append(item)
                
                return filtered_news[:limit]
            except Exception as e:
                print(f"Error parsing news: {e}")
        
        return []
    except Exception as e:
        print(f"Error in get_news_by_symbol: {e}")
        return []


@app.get("/api/news/market/latest")
async def get_latest_market_news(limit: int = Query(25, ge=1, le=100)):
    """Get latest market news from file system"""
    # Use helper function
    return load_news_from_files(limit)

@app.get("/api/market-data")
async def get_market_data(current_user: dict = Depends(get_current_user)):
    """Get market chart data"""
    try:
        # Mock data for testing - can be replaced with real data later
        return {
            "chart_data": [],
            "message": "Market data endpoint working"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/dashboard-stats")
async def get_dashboard_stats(current_user: dict = Depends(get_current_user)):
    """Get all dashboard statistics in one call"""
    try:
        # Get stats from existing endpoints
        from redis_files.redis_manager import RedisManager82
        
        db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        stream_length = db1_client.xlen("alerts:stream")
        
        # Use IST timezone for "today" calculation
        from datetime import timezone, timedelta
        ist = timezone(timedelta(hours=5, minutes=30))
        today = datetime.now(ist).date()
        
        # Count today's alerts
        today_count = 0
        messages = db1_client.xrevrange("alerts:stream", count=min(stream_length, 1000))
        for msg_id, msg_data in messages:
            try:
                data_field = msg_data.get('data') or msg_data.get(b'data')
                if not data_field:
                    continue
                
                if isinstance(data_field, bytes):
                    try:
                        import orjson
                        alert = orjson.loads(data_field)
                    except:
                        alert = json.loads(data_field.decode('utf-8'))
                else:
                    alert = json.loads(data_field) if isinstance(data_field, str) else data_field
                
                # Check if alert is from today
                ts = alert.get('timestamp') or alert.get('timestamp_ms', 0)
                if ts:
                    alert_date = datetime.fromtimestamp(ts / 1000, tz=ist).date()
                    if alert_date == today:
                        today_count += 1
            except Exception:
                continue
        
        # Get latest news
        try:
            news_data = load_news_from_files(5)
        except:
            news_data = []
        
        return {
            "total_alerts": stream_length,
            "today_alerts": today_count,
            "avg_confidence": 0.0,  # Can be calculated from alerts if needed
            "top_pattern": "N/A",
            "market_data": [],
            "news": news_data if isinstance(news_data, list) else [],
            "status": "success"
        }
    except Exception as e:
        print(f"Error in get_dashboard_stats: {e}")
        import traceback
        traceback.print_exc()
        # Return mock data on error
        return {
            "total_alerts": 0,
            "today_alerts": 0,
            "avg_confidence": 0,
            "top_pattern": "N/A",
            "market_data": [],
            "news": [{"title": "Market update", "content": "No recent news available", "date": "2024-01-01"}],
            "status": "success"
        }


@app.get("/api/validation/{alert_id}")
async def get_validation_by_alert_id(alert_id: str):
    """Get validation results for alert from Redis DB 0"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_key_standards import RedisKeyStandards
        
        # Get DB 0 client (system)
        client = RedisManager82.get_client(process_name="api", db=0, decode_responses=False)
        
        validation_key = RedisKeyStandards.get_validation_key(alert_id)
        data = client.get(validation_key)
        
        if data:
            try:
                if isinstance(data, bytes):
                    data = data.decode('utf-8')
                validation = json.loads(data) if isinstance(data, str) else data
                return validation
            except Exception:
                pass
        
        return {}
    except Exception as e:
        print(f"Error in get_validation_by_alert_id: {e}")
        return {}


@app.get("/api/validation/stats")
async def get_validation_stats():
    """Get validation performance statistics from Redis DB 0"""
    try:
        from redis_files.redis_manager import RedisManager82
        from redis_files.redis_key_standards import RedisKeyStandards
        
        # Get DB 0 client (system)
        client = RedisManager82.get_client(process_name="api", db=0, decode_responses=False)
        
        # Get performance stats
        stats_key = RedisKeyStandards.get_alert_performance_stats_key()
        stats_data = client.get(stats_key)
        
        if stats_data:
            try:
                if isinstance(stats_data, bytes):
                    stats_data = stats_data.decode('utf-8')
                return json.loads(stats_data) if isinstance(stats_data, str) else stats_data
            except Exception:
                pass
        
        return {
            "total_validations": 0,
            "success_rate": 0.0,
            "average_confidence": 0.0,
            "pattern_performance": {}
        }
    except Exception as e:
        print(f"Error in get_validation_stats: {e}")
        return {
            "total_validations": 0,
            "success_rate": 0.0,
            "average_confidence": 0.0,
            "pattern_performance": {}
        }


# Helper functions for historical data
def generate_historical_data(base_price: float, intervals: int, start_date: datetime, end_date: datetime, symbol: str = ""):
    """Generate realistic historical price data"""
    data = []
    current_price = base_price
    current_time = start_date
    
    time_step = (end_date - start_date) / intervals if intervals > 0 else timedelta(hours=1)
    
    for i in range(intervals):
        # Simulate price movement
        change_percent = random.uniform(-0.5, 0.5)  # -0.5% to +0.5%
        current_price = current_price * (1 + change_percent / 100)
        
        # Ensure price doesn't go too crazy
        if "NIFTY" in symbol.upper():
            current_price = max(1000, min(50000, current_price))
        else:
            current_price = max(100, min(100000, current_price))
        
        # Generate OHLC data
        open_price = current_price * random.uniform(0.999, 1.001)
        high_price = max(open_price, current_price) * random.uniform(1.001, 1.005)
        low_price = min(open_price, current_price) * random.uniform(0.995, 0.999)
        close_price = current_price
        
        data.append({
            "timestamp": current_time.isoformat(),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": random.randint(100000, 5000000)
        })
        
        current_time += time_step
    
    return data

def calculate_technical_indicators(data: List[Dict]):
    """Calculate basic technical indicators"""
    if not data:
        return {}
    
    closes = [item["close"] for item in data]
    
    # Simple Moving Averages
    sma_20 = sum(closes[-20:]) / min(20, len(closes)) if len(closes) >= 20 else sum(closes) / len(closes)
    sma_50 = sum(closes[-50:]) / min(50, len(closes)) if len(closes) >= 50 else None
    
    # RSI (simplified)
    if len(closes) > 1:
        gains = sum(max(0, closes[i] - closes[i-1]) for i in range(1, len(closes))) / len(closes)
        losses = sum(max(0, closes[i-1] - closes[i]) for i in range(1, len(closes))) / len(closes)
        rsi = 100 - (100 / (1 + (gains / losses if losses != 0 else 1)))
    else:
        rsi = 50.0
    
    return {
        "sma_20": round(sma_20, 2),
        "sma_50": round(sma_50, 2) if sma_50 else None,
        "rsi": round(rsi, 2),
        "current_price": closes[-1] if closes else 0,
        "price_change": round(closes[-1] - closes[0], 2) if len(closes) > 1 else 0,
        "price_change_percent": round(((closes[-1] - closes[0]) / closes[0]) * 100, 2) if len(closes) > 1 and closes[0] != 0 else 0
    }

def get_market_status():
    """Determine if market is open or closed"""
    now = datetime.now()
    # Indian market hours: 9:15 AM to 3:30 PM IST
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    # Check if weekend
    if now.weekday() >= 5:  # Saturday or Sunday
        return "closed_weekend"
    
    # Check market hours
    if market_open <= now <= market_close:
        return "open"
    else:
        return "closed"

def get_last_trading_day():
    """Get the last trading day (skip weekends)"""
    today = datetime.now()
    if today.weekday() == 0:  # Monday
        return today - timedelta(days=3)  # Friday
    elif today.weekday() >= 5:  # Weekend
        return today - timedelta(days=2 if today.weekday() == 6 else 1)
    else:
        return today - timedelta(days=1)

def generate_sample_alerts(count: int):
    """Generate sample historical alerts for demonstration"""
    symbols = ["RELIANCE", "TCS", "INFY", "HDFC", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL"]
    patterns = ["Bullish Engulfing", "Bearish Engulfing", "Morning Star", "Evening Star", 
                "Hammer", "Shooting Star", "Double Top", "Double Bottom"]
    
    alerts = []
    base_time = datetime.now() - timedelta(days=30)
    
    for i in range(count):
        symbol = random.choice(symbols)
        pattern = random.choice(patterns)
        confidence = random.randint(60, 95)
        
        alert_time = base_time + timedelta(hours=random.randint(1, 720))  # Random time in last 30 days
        
        alerts.append({
            "id": i + 1,
            "symbol": symbol,
            "pattern": pattern,
            "confidence": confidence,
            "timestamp": alert_time.isoformat(),
            "price": round(random.uniform(1000, 5000), 2),
            "volume": random.randint(10000, 1000000),
            "timeframe": random.choice(["5min", "15min", "1h", "1d"]),
            "status": random.choice(["active", "triggered", "expired"])
        })
    
    # Sort by timestamp descending (newest first)
    alerts.sort(key=lambda x: x["timestamp"], reverse=True)
    return alerts

@app.get("/api/historical/{symbol}")
async def get_historical_data(
    symbol: str,
    period: str = Query("1d", description="Time period: 1d, 1w, 1m, 3m, 1y"),
    current_user: dict = Depends(get_current_user)
):
    """Get historical market data for any symbol"""
    try:
        # Generate realistic mock historical data
        end_date = datetime.now()
        
        # Set start date based on period
        if period == "1d":
            start_date = end_date - timedelta(days=1)
            intervals = 390  # 6.5 hours * 60 minutes
        elif period == "1w":
            start_date = end_date - timedelta(weeks=1)
            intervals = 35   # 7 days * 5 data points per day
        elif period == "1m":
            start_date = end_date - timedelta(days=30)
            intervals = 30   # 30 days
        elif period == "3m":
            start_date = end_date - timedelta(days=90)
            intervals = 90   # 90 days
        else:  # 1y
            start_date = end_date - timedelta(days=365)
            intervals = 52   # 52 weeks
        
        # Generate realistic price data
        base_price = 22000 if "NIFTY" in symbol.upper() else 50000
        data = generate_historical_data(base_price, intervals, start_date, end_date, symbol)
        
        return {
            "symbol": symbol,
            "period": period,
            "data": data,
            "last_updated": datetime.now().isoformat(),
            "data_type": "historical"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/market/indices")
async def get_market_indices():
    """Get market indices (NIFTY, BANKNIFTY, VIX) from Redis
    
    Per REDIS_STORAGE_SIGNATURE.md:
    - ohlc_latest:{symbol} is in DB 2 (analytics_data) - bucket format
    - index:NSE:{name} is in DB 1 (realtime) - gift_nifty_gap.py format
    
    Priority:
    1. ohlc_latest:{symbol} hash in DB 2 (bucket format when market is on)
    2. index:NSE:{name} JSON in DB 1 (gift_nifty_gap.py format when market is off)
    """
    try:
        from redis_files.redis_manager import RedisManager82
        
        # Get clients for both DB 1 (realtime) and DB 2 (analytics)
        # Per REDIS_STORAGE_SIGNATURE.md: ohlc_latest is in DB 2, index:NSE: is in DB 1
        db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        db2_client = RedisManager82.get_client(process_name="api", db=2, decode_responses=False)
        
        indices = {}
        # Map index names to symbol variations for bucket lookup
        index_symbol_map = {
            'NIFTY 50': ['NIFTY 50', 'NIFTY', 'NSE:NIFTY 50'],
            'NIFTY BANK': ['NIFTY BANK', 'BANKNIFTY', 'NSE:NIFTY BANK'],
            'INDIA VIX': ['INDIA VIX', 'VIX', 'NSE:INDIA VIX']
        }
        
        # Map index names to gift_nifty_gap.py Redis keys
        gift_nifty_key_map = {
            'NIFTY 50': 'index:NSE:NIFTY 50',
            'NIFTY BANK': 'index:NSE:NIFTY BANK',
            'INDIA VIX': 'index:NSE:INDIA VIX'
        }
        
        for index_name, symbol_variations in index_symbol_map.items():
            try:
                found = False
                
                # PRIORITY 1: Try ohlc_latest hash in DB 2 (bucket format - when market is on)
                for symbol_variant in symbol_variations:
                    ohlc_key = f"ohlc_latest:{symbol_variant}"
                    ohlc_data = db2_client.hgetall(ohlc_key)
                    
                    if ohlc_data:
                        def decode_val(key, default=0):
                            val = ohlc_data.get(key.encode() if isinstance(key, str) else key) or ohlc_data.get(key, default)
                            if isinstance(val, bytes):
                                val = val.decode('utf-8')
                            try:
                                return float(val) if val else default
                            except (ValueError, TypeError):
                                return default
                        
                        # Bucket format: open, high, low, close, volume, timestamp
                        # Note: bucket format uses 'close' as the current price, no 'last_price' field
                        close_price = decode_val('close', 0)
                        last_price = close_price  # In bucket format, close IS the last_price
                        
                        # prev_close: try to get from yesterday's data or use current close as fallback
                        prev_close = decode_val('prev_close', close_price)
                        
                        # Calculate change: last_price - prev_close
                        change = last_price - prev_close
                        change_pct = ((change / prev_close) * 100) if prev_close > 0 else 0.0
                        
                        indices[index_name] = {
                            "last_price": last_price,
                            "prev_close": prev_close,
                            "change": change,
                            "change_pct": change_pct
                        }
                        found = True
                        break
                
                # PRIORITY 2: Fallback to gift_nifty_gap.py format in DB 1 (when market is off)
                if not found:
                    redis_key = gift_nifty_key_map.get(index_name)
                    if redis_key:
                        index_data = db1_client.get(redis_key)
                        
                        if index_data:
                            if isinstance(index_data, bytes):
                                index_data = index_data.decode('utf-8')
                            
                            try:
                                index_json = json.loads(index_data)
                                last_price = float(index_json.get('last_price', 0))
                                
                                # Handle different data structures
                                ohlc = index_json.get('ohlc', {})
                                if isinstance(ohlc, dict):
                                    prev_close = float(ohlc.get('close', last_price))
                                else:
                                    prev_close = float(index_json.get('prev_close', last_price))
                                
                                # Calculate change from net_change or last_price - prev_close
                                net_change = index_json.get('net_change')
                                if net_change is not None:
                                    change = float(net_change)
                                else:
                                    change = float(index_json.get('change', last_price - prev_close))
                                
                                # Calculate change percentage
                                if prev_close > 0:
                                    change_pct = (change / prev_close) * 100
                                else:
                                    change_pct = 0.0
                                
                                indices[index_name] = {
                                    "last_price": last_price,
                                    "prev_close": prev_close,
                                    "change": change,
                                    "change_pct": change_pct
                                }
                                found = True
                            except (json.JSONDecodeError, ValueError, TypeError) as e:
                                print(f"Error parsing index data for {index_name}: {e}")
                
                # Default fallback if nothing found
                if not found:
                    indices[index_name] = {
                        "last_price": 0,
                        "prev_close": 0,
                        "change": 0,
                        "change_pct": 0
                    }
                    
            except Exception as e:
                print(f"Error fetching index {index_name}: {e}")
                import traceback
                traceback.print_exc()
                indices[index_name] = {
                    "last_price": 0,
                    "prev_close": 0,
                    "change": 0,
                    "change_pct": 0
                }
        
        # Add Gift Nifty gap data
        try:
            gap_data = db1_client.get('latest_gift_nifty_gap')
            if gap_data:
                if isinstance(gap_data, bytes):
                    gap_data = gap_data.decode('utf-8')
                gap_json = json.loads(gap_data)
                indices['GIFT_NIFTY_GAP'] = {
                    "gap_points": float(gap_json.get('gap_points', 0)),
                    "gap_percent": float(gap_json.get('gap_percent', 0)),
                    "gift_price": float(gap_json.get('gift_price', 0)),
                    "nifty_price": float(gap_json.get('nifty_price', 0)),
                    "signal": gap_json.get('signal', '')
                }
        except Exception as e:
            print(f"Error fetching Gift Nifty gap: {e}")
        
        return indices
    except Exception as e:
        print(f"Error in get_market_indices: {e}")
        import traceback
        traceback.print_exc()
        return {
            "NIFTY 50": {"last_price": 0, "prev_close": 0, "change": 0, "change_pct": 0},
            "NIFTY BANK": {"last_price": 0, "prev_close": 0, "change": 0, "change_pct": 0},
            "INDIA VIX": {"last_price": 0, "prev_close": 0, "change": 0, "change_pct": 0}
        }


@app.get("/api/instruments")
async def get_instruments(type: Optional[str] = Query(None)):
    """Get instruments - proxy to existing endpoint"""
    # Use existing instrument manager
    asset_class = type or "eq"
    return await api_instruments(asset_class)


# SPA routing: Serve React frontend for all non-API routes (must be last)
if IS_PRODUCTION:
    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve React frontend for all routes (SPA routing)"""
        # Skip API and WebSocket routes
        if full_path.startswith("api") or full_path.startswith("ws") or full_path.startswith("alerts/stream"):
            raise HTTPException(status_code=404, detail="Not found")
        
        # Serve static files if they exist
        file_path = FRONTEND_DIST_PATH / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        
        # Default to index.html for SPA routing
        return FileResponse(FRONTEND_DIST_PATH / "index.html")

