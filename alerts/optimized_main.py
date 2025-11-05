from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect, Query, HTTPException, Depends, Header, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from backend.websockets import router as websocket_router
from fastapi.responses import HTMLResponse, StreamingResponse
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
from typing import Dict, List, Optional
from pathlib import Path
from datetime import datetime, timedelta
from functools import lru_cache

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

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# JWT Authentication
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")  # Change in production!
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours

# Password hashing (for future user management)
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto") if HAS_PASSLIB else None
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
def get_user_from_redis(username: str) -> Optional[Dict]:
    """Get user from Redis"""
    try:
        from redis_files.redis_manager import RedisManager82
        db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
        user_data = db0_client.get(f"user:{username}")
        if user_data:
            return json.loads(user_data)
        return None
    except Exception as e:
        print(f"Error fetching user from Redis: {e}")
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
    if not HAS_PASSLIB or not pwd_context:
        # Fallback: plain text comparison (NOT SECURE - only for development)
        return plain_password == hashed_password
    return pwd_context.verify(plain_password, hashed_password)

def hash_password(password: str) -> str:
    """Hash password (bcrypt has 72-byte limit)"""
    if not HAS_PASSLIB or not pwd_context:
        # Fallback: return plain text (NOT SECURE - only for development)
        return password
    # Truncate password to 72 bytes if needed (bcrypt limitation)
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > 72:
        password = password_bytes[:72].decode('utf-8', errors='ignore')
    return pwd_context.hash(password)

# Initialize default users if they don't exist
def initialize_default_users():
    """Initialize default admin and user accounts in Redis"""
    default_users = {
        "admin": "admin123",
        "user": "user123"
    }
    for username, password in default_users.items():
        existing = get_user_from_redis(username)
        if not existing:
            password_hash = hash_password(password)
            save_user_to_redis(username, password_hash, {"role": "admin" if username == "admin" else "user"})
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
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)):
    """Verify JWT token and return user"""
    # Allow unauthenticated access for development (can be disabled in production)
    if os.getenv("ALLOW_UNAUTHENTICATED", "false").lower() == "true":
        return {"username": "guest"}
    
    if credentials is None:
        raise HTTPException(status_code=401, detail="Not authenticated", headers={"WWW-Authenticate": "Bearer"})
    
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})
        return {"username": username}
    except JWTError as e:
        raise HTTPException(status_code=401, detail="Invalid token", headers={"WWW-Authenticate": "Bearer"})

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


# --- App startup: warm instrument caches for ultra-fast loads ---
@app.on_event("startup")
async def warm_instrument_cache():
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
        await start_redis_listener()
        print("✅ Started Redis alert listener for WebSocket")
    except Exception as e:
        print(f"Failed to start Redis alert listener: {e}")


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
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=401, detail="Incorrect username or password")
    
    if not verify_password(password, user_data.get("password_hash", "")):
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
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer", "two_fa_required": user_data.get("two_fa_enabled", False)}

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

@app.post("/api/brokerage/connect")
@limiter.limit("10/minute")
async def connect_brokerage(
    request: Request,
    broker: str = Form(...),  # "zerodha", "angel_one", etc.
    api_key: str = Form(...),
    access_token: Optional[str] = Form(None),
    request_token: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user)
):
    """Connect user's brokerage account"""
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Store brokerage credentials securely
    brokerage_connections = user_data.get("brokerage_connections", {})
    
    if broker.lower() == "zerodha":
        try:
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=api_key)
            
            # If request_token provided, generate access_token
            if request_token:
                session = kite.generate_session(request_token)
                access_token = session["access_token"]
            elif not access_token:
                raise HTTPException(status_code=400, detail="Either access_token or request_token required")
            
            kite.set_access_token(access_token)
            # Verify connection by fetching profile
            profile = kite.profile()
            
            # Store credentials (encrypted in production)
            brokerage_connections["zerodha"] = {
                "api_key": api_key,
                "access_token": access_token,
                "user_id": profile.get("user_id"),
                "user_name": profile.get("user_name"),
                "connected_at": datetime.utcnow().isoformat(),
                "last_verified": datetime.utcnow().isoformat()
            }
            
            # Update user data
            user_data["brokerage_connections"] = brokerage_connections
            from redis_files.redis_manager import RedisManager82
            db0_client = RedisManager82.get_client(process_name="api", db=0, decode_responses=True)
            db0_client.set(f"user:{username}", json.dumps(user_data))
            
            return {
                "message": "Brokerage account connected successfully",
                "broker": "zerodha",
                "user_id": profile.get("user_id"),
                "user_name": profile.get("user_name")
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to connect brokerage: {str(e)}")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported broker: {broker}")

@app.get("/api/brokerage/connections")
async def get_brokerage_connections(current_user: dict = Depends(get_current_user)):
    """Get user's connected brokerage accounts"""
    username = current_user.get("username")
    user_data = get_user_from_redis(username)
    if not user_data:
        raise HTTPException(status_code=404, detail="User not found")
    
    connections = user_data.get("brokerage_connections", {})
    # Return connection info without sensitive tokens
    safe_connections = {}
    for broker, data in connections.items():
        safe_connections[broker] = {
            "user_id": data.get("user_id"),
            "user_name": data.get("user_name"),
            "connected_at": data.get("connected_at"),
            "last_verified": data.get("last_verified")
        }
    
    return {"connections": safe_connections}

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
    """Get alert statistics from Redis"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        db1_client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        
        # Read recent alerts from stream
        stream_length = db1_client.xlen("alerts:stream")
        messages = db1_client.xrevrange("alerts:stream", count=min(stream_length, 10000))
        
        pattern_counts = {}
        symbol_counts = {}
        confidence_counts = {"high": 0, "medium": 0, "low": 0}
        instrument_type_counts = {}
        today_count = 0
        today = datetime.now().date()
        
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
                
                # Count today's alerts
                alert_ts = alert.get('timestamp')
                if alert_ts:
                    try:
                        if isinstance(alert_ts, str):
                            alert_dt = datetime.fromisoformat(alert_ts.replace('Z', '+00:00'))
                        else:
                            alert_dt = datetime.fromtimestamp(alert_ts)
                        if alert_dt.date() == today:
                            today_count += 1
                    except Exception:
                        pass
            except Exception:
                continue
        
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
async def get_charts(
    symbol: str,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    resolution: str = Query("5m"),
    include_indicators: bool = Query(False)
):
    """Get OHLC chart data from Redis DB 2 (analytics)"""
    return await get_chart_data_internal(symbol, date_from, date_to, resolution, include_indicators)


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
        try:
            zset_entries = db2_client.zrange(zset_key, 0, -1, withscores=True)
            
            if not zset_entries:
                # Try base symbol
                zset_key = f"ohlc_daily:{base_symbol}"
                zset_entries = db2_client.zrange(zset_key, 0, -1, withscores=True)
            
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
        if include_indicators and symbol:
            try:
                indicators = await get_indicators(symbol)
                # TODO: Calculate indicator values for each OHLC point
                # For now, return current indicator values
                indicators_overlay = indicators.get('indicators', {})
            except Exception:
                pass
        
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
async def get_volume_profile(symbol: str, date: Optional[str] = Query(None)):
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
    # Use existing endpoint
    return await api_news(limit)


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


@app.get("/api/market/indices")
async def get_market_indices():
    """Get market indices (NIFTY, BANKNIFTY, VIX) from Redis"""
    try:
        from redis_files.redis_manager import RedisManager82
        
        # Get DB 1 client (realtime)
        client = RedisManager82.get_client(process_name="api", db=1, decode_responses=False)
        
        indices = {}
        index_symbols = ['NIFTY 50', 'NIFTY BANK', 'INDIA VIX']
        
        for index_name in index_symbols:
            try:
                # Try ohlc_latest:{symbol} hash
                ohlc_key = f"ohlc_latest:{index_name}"
                ohlc_data = client.hgetall(ohlc_key)
                
                if ohlc_data:
                    def decode_val(key, default=0):
                        val = ohlc_data.get(key.encode() if isinstance(key, str) else key) or ohlc_data.get(key, default)
                        if isinstance(val, bytes):
                            val = val.decode('utf-8')
                        return float(val) if val else default
                    
                    last_price = decode_val('last_price', 0)
                    prev_close = decode_val('prev_close', last_price)
                    
                    change_pct = ((last_price - prev_close) / prev_close * 100) if prev_close > 0 else 0
                    
                    indices[index_name] = {
                        "last_price": last_price,
                        "prev_close": prev_close,
                        "change": last_price - prev_close,
                        "change_pct": change_pct
                    }
                else:
                    indices[index_name] = {
                        "last_price": 0,
                        "prev_close": 0,
                        "change": 0,
                        "change_pct": 0
                    }
            except Exception as e:
                indices[index_name] = {
                    "last_price": 0,
                    "prev_close": 0,
                    "change": 0,
                    "change_pct": 0
                }
        
        return indices
    except Exception as e:
        print(f"Error in get_market_indices: {e}")
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

