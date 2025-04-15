from fastapi import FastAPI
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime as dt
import asyncio
from sqlalchemy import text
from typing import Optional
import uuid
# region Monitoring
# from prometheus_client import start_http_server, Gauge
# import time

# # Create a metric to track server uptime
# server_up = Gauge('server_status', 'Server running status (1=up, 0=down)')

# async def monitor_server():
#     while True:
#         server_up.set(1)  # Set to 1 if the server is running
#         await asyncio.sleep(5)

# # Start a separate metrics server
# start_http_server(9100)  # Runs a Prometheus-compatible metrics server

# monitor_server()
# endregion
from Services.kafka_consumer import ConsumerService
from dependencies import bootstrap_servers, sasl_mechanism, sasl_plain_password, sasl_plain_username, security_protocol
from Routes import HistoricalRoute, ScanListRoute, UtilRoute
from Services.db_connection import Base, engine
from Utils.functions import real_time_filter

import json

Base.metadata.create_all(engine)

with engine.connect() as connection:
    connection.execute(text("ALTER SEQUENCE filtering_options_id_seq RESTART WITH 1000000"))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(UtilRoute.router)
app.include_router(HistoricalRoute.router)
app.include_router(ScanListRoute.router)

@app.get('/')
async def main():
    return {"message": "Server running on 8001"}

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, dict] = {}
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        try:
            await websocket.accept()
            websocket_id = str(uuid.uuid4())
            async with self.lock:
                self.active_connections[websocket_id] = {"websocket": websocket, "condition": None}
            print(f"WebSocket connected: {websocket_id}")
            return websocket_id
        except Exception as e:
            print(f"Error during WebSocket connect: {e}")
            return None

    async def disconnect(self, websocket_id: str):
        try:
            async with self.lock:
                connection = self.active_connections.pop(websocket_id, None)
            if connection:
                await connection["websocket"].close()
                print(f"WebSocket disconnected: {websocket_id}")
        except Exception as e:
            print(f"Error closing WebSocket ({websocket_id}): {e}")

    async def update_condition(self, websocket_id: str, condition: Optional[str]):
        try:
            async with self.lock:
                if websocket_id in self.active_connections:
                    self.active_connections[websocket_id]["condition"] = condition
                    print(f"Condition updated for {websocket_id}: {condition}")
        except Exception as e:
            print(f"Error updating condition for {websocket_id}: {e}")

    async def broadcast_to_clients(self, tick):
        try:
            async with self.lock:
                connections_snapshot = list(self.active_connections.items())
        except Exception as e:
            print(f"Error accessing connections: {e}")
            return

        disconnected_ids = []

        for websocket_id, conn_info in connections_snapshot:
            websocket = conn_info["websocket"]
            condition = conn_info["condition"]

            try:
                if websocket.application_state == WebSocketState.DISCONNECTED:
                    disconnected_ids.append(websocket_id)
                    continue

                if condition is not None:
                    filtered = real_time_filter(condition, tick)
                    if filtered:
                        await websocket.send_text(json.dumps(filtered))
            except WebSocketDisconnect:
                disconnected_ids.append(websocket_id)
            except Exception as e:
                print(f"Error sending message to {websocket_id}: {e}")
                disconnected_ids.append(websocket_id)

        for websocket_id in disconnected_ids:
            await self.disconnect(websocket_id)

manager = ConnectionManager()
tick_queue = asyncio.Queue(maxsize=50000)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    websocket_id = await manager.connect(websocket)
    if websocket_id is None:
        return
    try:
        while True:
            try:
                data = await websocket.receive_text()
                print(f"Received data ({websocket_id}): {data}")
                data_json = json.loads(data)
                action = data_json.get("action")
                if action == "update_condition":
                    condition = data_json.get("condition") or None
                    await manager.update_condition(websocket_id, condition)
            except json.JSONDecodeError:
                print(f"Invalid JSON received ({websocket_id}).")
            except WebSocketDisconnect:
                print(f"WebSocket disconnect signal received: {websocket_id}")
                break
            except Exception as e:
                print(f"Error in receive loop ({websocket_id}): {e}")
    finally:
        await manager.disconnect(websocket_id)

@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    consumer = ConsumerService(
        topic_name="feed_tick_trade_format",
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        loop=loop,
    )
    asyncio.create_task(run_consumer(consumer))
    asyncio.create_task(tick_broadcaster())

async def run_consumer(consumer_service: ConsumerService):
    def enqueue_tick(tick):
        try:
            tick_queue.put_nowait(tick)
        except asyncio.QueueFull:
            print("Tick queue full. Dropping tick.")

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, consumer_service.consume_message, enqueue_tick)

async def tick_broadcaster():
    while True:
        tick = await tick_queue.get()
        await manager.broadcast_to_clients(tick)