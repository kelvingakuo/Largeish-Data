import asyncio
from datetime import datetime
import websockets
import json
from base_websockets_connector import BaseWebSocketAsync

# https://docs.kraken.com/api/docs/websocket-v2/ticker/
# {'channel': 'ticker', 'type': 'snapshot', 'data': [{'symbol': 'BTC/USD', 'bid': 87794.0, 'bid_qty': 0.08299728, 'ask': 87794.1, 'ask_qty': 2.44593945, 'last': 87794.1, 'volume': 2617.38647876, 'vwap': 87264.8, 'low': 86007.0, 'high': 88822.0, 'change': -749.7, 'change_pct': -0.85, 'volume_usd': 229791090.25, 'timestamp': '2026-01-26T12:31:08.210624Z'}]}

class KrakenConnector(BaseWebSocketAsync):
    def __init__(self, stream_url="wss://ws.kraken.com/v2", pair="BTC/USD", channel="ticker"):
        super().__init__()
        self.stream_url = stream_url
        self.pair = pair
        self.channel = channel
        self.connection = None

    async def connect(self, url: str = None, **kwargs):
        self.connection = await websockets.connect(url or self.stream_url)
        subscribe_message = {
            "method": "subscribe",
            "params": {
                "channel": self.channel,
                "symbol": [self.pair]
            }
        }
        await self.send(json.dumps(subscribe_message))
        print(f"Connected to {url or self.stream_url} and subscribed to {self.channel} for {self.pair}")

    async def send(self, data: str):
        if self.connection is None:
            raise RuntimeError("WebSocket connection not established.")
        await self.connection.send(data)

    async def receive(self) -> str:
        if self.connection is None:
            raise RuntimeError("WebSocket connection not established.")
        return await self.connection.recv()

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Disconnected from Kraken WebSocket.")

    async def run(self, message_handler):
        await self.connect()
        try:
            while True:
                message = await self.receive()
                parsed_message = json.loads(message)
                parsed_message["processing_time"] = datetime.now().timestamp()
                # Skip heartbeat and system status messages
                if isinstance(parsed_message, dict) and parsed_message.get("channel") in ["heartbeat", "systemStatus", "subscriptionStatus"]:
                    continue
                await message_handler(parsed_message)
        finally:
            await self.close()