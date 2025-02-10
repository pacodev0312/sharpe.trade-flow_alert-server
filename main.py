from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime as dt
import asyncio
from sqlalchemy import text
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
from Services.kafka_consumer import Consumerservice
from dependencies import bootstrap_servers, sasl_mechanism, sasl_plain_password, sasl_plain_username, security_protocol
from Routes import HistoricalRoute
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

app.include_router(HistoricalRoute.router)

@app.get('/')
def main():
    return {"message": "Server running on 8001"}

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[dict] = []  # Store active connections with websocket and condition

    async def connect(self, websocket: WebSocket, condition: str):
        await websocket.accept()
        self.active_connections.append({"websocket": websocket, "condition": condition})

    async def disconnect(self, websocket: WebSocket):
        self.active_connections = [
            connection for connection in self.active_connections if connection["websocket"] != websocket
        ]
        try:
            await websocket.close()
        except Exception as e:
            print(f"Error closing WebSocket: {e}")

    async def broadcast(self, message: str):
        for connection in self.active_connections[:]:  # Iterate over a copy of the list
            websocket: WebSocket = connection["websocket"]
            try:
                message = real_time_filter(condition=connection["condition"], data=message)
                if message is not None:
                    await websocket.send_text(message)
            except Exception as e:
                print(f"Error sending message to {websocket}: {e}")
                await self.disconnect(websocket)


manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, condition: str):
    await manager.connect(websocket, condition)
    try:
        while True:
            data = await websocket.receive_text()  # Keep the connection alive
            print(f"Received: {data}")
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        print("Client disconnected")
    except Exception as e:
        print(f"Error in WebSocket connection: {e}")
        await manager.disconnect(websocket)
        
# Background task to send messages every second
async def periodic_message_task():
    test_consumer = Consumerservice(
        topic_name="feed_tick_trade_format",
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
    )
    try:
        await test_consumer.start_consumer()
        await test_consumer.consume_message(manager.broadcast)
    except Exception as e:
        print(f"Error in periodic message task: {e}")
   

# Run the periodic task in the background
@app.on_event("startup")
async def startup_event():
    # asyncio.create_task(monitor_server())
    asyncio.create_task(periodic_message_task())
     
      # try:
    #     while True:
    #         dt_now = dt.now()
    #         timestamp = dt_now.strftime("%Y-%m-%dT%H:%M:%S")
    #         data = {
    #             "ltp": 83889.73,
    #             "lot_size": 1,
    #             "delta_volume": 0,
    #             "exchange": "BSE",
    #             "moneyness": "",
    #             "sweep3": "BuySweep",
    #             "power_sweep_volume": 0,
    #             "power_block": "",
    #             "is_contract_active": 1,
    #             "bid": 0.0,
    #             "symbol": "BSESNXT50",
    #             "delta_volume_value": 0.0,
    #             "tick_size": "0",
    #             "aggressor": "Buy",
    #             "sweep3_volume": 0,
    #             "power_sweep_value": 0.0,
    #             "power_block_volume": "",
    #             "oi_build_up": "",
    #             "timestamp": timestamp,
    #             "bid_qty": 0,
    #             "symbol_name": "BSESNXT50BSE",
    #             "buy_volume": 0,
    #             "expiry": "0001-01-01",
    #             "sweep1": "BuySweep",
    #             "sweep3_value": 0.0,
    #             "block1": "",
    #             "power_block_value": "",
    #             "sentiment": "",
    #             "ask": 0.0,
    #             "exchange_token": 400000048,
    #             "buy_value": 0.0,
    #             "option_type": "IN",
    #             "sweep1_volume": 0,
    #             "sweep4": "BuySweep",
    #             "block1_volume": "",
    #             "unusal_prive_activity": "",
    #             "tick_seq": 4445,
    #             "ask_qty": 0,
    #             "underlier_symbol": "BSESNXT50",
    #             "sell_volume": 0,
    #             "strike": 0,
    #             "sweep1_value": 0.0,
    #             "sweep4_volume": 0,
    #             "block1_value": "",
    #             "strike_difference": "",
    #             "volume": 0,
    #             "underlier_price": 83889.73,
    #             "sell_value": 0,
    #             "precision": "",
    #             "sweep2": "BuySweep",
    #             "sweep4_value": 0.0,
    #             "block2": "",
    #             "symbol_root": "",
    #             "symbol_id": "400000048",
    #             "oi": 0,
    #             "trade_value": 0.0,
    #             "symbol_type": "IN",
    #             "oi_change": 0,
    #             "sweep2_volume": 0,
    #             "power_sweep": "BuySweep",
    #             "block2_volume": "",
    #             "local_id": "BSESNXT50_IN_BSE",
    #             "last_size": 0,
    #             "tag": "Sell",
    #             "sweep2_value": 0.0,
    #             "block2_value": ""
    #         },
    #         await manager.broadcast(message=json.dumps(data))
    #         await asyncio.sleep(1)
    # except Exception as ex:
    #     print(ex)
    