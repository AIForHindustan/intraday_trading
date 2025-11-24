# backend/websockets.py

import asyncio
import logging
from typing import List, Dict, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

try:
    from redis.asyncio import Redis
    import orjson
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("redis.asyncio not available - WebSocket alerts will not work")

router = APIRouter()

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Keeps track of active WebSocket connections and broadcasts messages to them.
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """
        Send the same payload to all connected clients.
        """
        if not self.active_connections:
            return
        
        # Convert message to JSON string
        try:
            if isinstance(message, dict):
                # Use orjson for faster serialization if available
                if REDIS_AVAILABLE:
                    message_json = orjson.dumps(message).decode()
                else:
                    import json
                    message_json = json.dumps(message)
            else:
                message_json = str(message)
        except Exception as e:
            logger.error(f"Error serializing message: {e}")
            return
        
        # Send to all connections
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message if isinstance(message, dict) else {"data": message_json})
            except Exception as e:
                disconnected.append(connection)
        
        # Clean up disconnected connections
        for connection in disconnected:
            self.disconnect(connection)


manager = ConnectionManager()


@router.websocket("/ws/alerts")
async def alerts_ws(websocket: WebSocket):
    """
    WebSocket endpoint for streaming alerts.
    Clients only receive data; they need not send anything.
    """
    await manager.connect(websocket)
    try:
        # Keep the connection alive by sending periodic pings
        while True:
            # Wait for client messages or timeout
            try:
                # Wait for any message from client (or timeout)
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Clients can send filter requests (optional)
                # For now, we just keep the connection alive
            except asyncio.TimeoutError:
                # Send ping to keep connection alive
                await websocket.send_json({"type": "ping", "timestamp": asyncio.get_event_loop().time()})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


async def redis_alert_listener(redis: Optional[Redis] = None):
    """
    Read from Redis streams and broadcast updates to WebSocket clients.
    Run this in a background task (e.g., using FastAPI's startup event).
    
    Args:
        redis: Optional Redis client. If None, will create one using default config.
    """
    if not REDIS_AVAILABLE:
        logger.error("redis.asyncio not available - cannot start alert listener")
        return
    
    # Create Redis client if not provided
    if redis is None:
        try:
            # Try to get Redis config from existing codebase
            from redis_files.redis_client import get_redis_config
            config = get_redis_config()
            redis = Redis(
                host=config.get('host', 'localhost'),
                port=config.get('port', 6379),
                db=1,  # DB 1 for realtime streams (alerts:stream)
                decode_responses=False,  # Keep binary for orjson
                health_check_interval=30
            )
        except Exception as e:
            logger.error(f"Failed to create Redis client: {e}")
            return
    
    # Stream configuration
    stream = "alerts:stream"
    group = "ws-consumer-group"
    consumer = f"alerts-ws-{id(asyncio.current_task())}"
    
    # Create consumer group if it doesn't exist
    try:
        await redis.xgroup_create(
            name=stream,
            groupname=group,
            id="0",  # Start from beginning, or use "$" for new messages only
            mkstream=True
        )
        logger.info(f"Created consumer group '{group}' for stream '{stream}'")
    except Exception as e:
        # Group might already exist
        if "BUSYGROUP" not in str(e):
            logger.warning(f"Error creating consumer group: {e}")
    
    logger.info(f"Starting Redis alert listener for stream '{stream}' (group: {group}, consumer: {consumer})")
    
    # Main consumption loop
    while True:
        try:
            # XREADGROUP with block so we don't spin CPU
            # Read from stream with consumer group
            response = await redis.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},  # ">" means new messages not yet delivered to this consumer
                count=1,
                block=1000  # Block for 1 second
            )
            
            if response:
                # Response format: [(stream_name, [(message_id, payload_dict)])]
                for stream_name, messages in response:
                    for message_id, payload in messages:
                        try:
                            # Convert from Redis bytes to Python dict
                            # Payload format: {b'data': binary_json}
                            if b'data' in payload:
                                # Decode JSON data
                                if REDIS_AVAILABLE:
                                    alert_data = orjson.loads(payload[b'data'])
                                else:
                                    import json
                                    alert_data = json.loads(payload[b'data'].decode())
                            else:
                                # Fallback: try to decode entire payload
                                alert_data = {}
                                for key, value in payload.items():
                                    try:
                                        if REDIS_AVAILABLE:
                                            alert_data[key.decode()] = orjson.loads(value) if isinstance(value, bytes) else value
                                        else:
                                            import json
                                            alert_data[key.decode()] = json.loads(value.decode()) if isinstance(value, bytes) else value
                                    except:
                                        alert_data[key.decode()] = value.decode() if isinstance(value, bytes) else value
                            
                            # Acknowledge message
                            await redis.xack(stream, group, message_id)
                            
                            # Ensure alert_id exists for frontend compatibility
                            if 'alert_id' not in alert_data or not alert_data.get('alert_id'):
                                import time as time_module
                                timestamp_ms = alert_data.get('timestamp_ms') or alert_data.get('timestamp', int(time_module.time() * 1000))
                                symbol = alert_data.get('symbol', 'UNKNOWN')
                                alert_data['alert_id'] = f"{symbol}_{timestamp_ms}"
                            
                            # Broadcast to all WebSocket clients
                            # Wrap in consistent format for frontend compatibility
                            broadcast_data = {
                                "type": "new_alert",
                                "data": alert_data
                            }
                            await manager.broadcast(broadcast_data)
                            
                            
                        except Exception as e:
                            logger.error(f"Error processing alert message {message_id}: {e}")
                            # Still acknowledge to avoid reprocessing
                            try:
                                await redis.xack(stream, group, message_id)
                            except:
                                pass
                            
        except asyncio.CancelledError:
            logger.info("Redis alert listener cancelled")
            break
        except Exception as e:
            logger.error(f"Error in Redis alert listener: {e}")
            # Wait a bit before retrying
            await asyncio.sleep(5)
    
    # Cleanup
    try:
        await redis.aclose()
    except:
        pass
    logger.info("Redis alert listener stopped")


async def start_redis_listener(redis: Optional[Redis] = None):
    """
    Start the Redis alert listener as a background task.
    This should be called during FastAPI startup.
    """
    if not REDIS_AVAILABLE:
        logger.warning("Redis not available - WebSocket alerts will not receive updates")
        return
    
    # Start listener in background
    task = asyncio.create_task(redis_alert_listener(redis))
    logger.info("Started Redis alert listener background task")
    return task

