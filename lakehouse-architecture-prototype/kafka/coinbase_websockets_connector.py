import asyncio
from datetime import datetime
import websockets
import json
from base_websockets_connector import BaseWebSocketAsync

# https://docs.cdp.coinbase.com/exchange/websocket-feed/overview
# https://docs.cdp.coinbase.com/exchange/websocket-feed/channels#ticker-channel
# {'type': 'ticker', 'sequence': 120070317963, 'product_id': 'BTC-USD', 'price': '87759.99', 'open_24h': '88659.35', 'volume_24h': '7066.30302259', 'low_24h': '86000.13', 'high_24h': '88847.56', 'volume_30d': '226641.34738008', 'best_bid': '87759.99', 'best_bid_size': '0.00001532', 'best_ask': '87760.00', 'best_ask_size': '0.06091547', 'side': 'sell', 'time': '2026-01-26T12:37:42.922471Z', 'trade_id': 943525024, 'last_size': '0.0011381'}

class CoinbaseConnector(BaseWebSocketAsync):
    def __init__(self, stream_url="wss://ws-feed.exchange.coinbase.com", product_id="BTC-USD", channel="ticker"):
        super().__init__()
        self.stream_url = stream_url
        self.product_id = product_id
        self.channel = channel
        self.connection = None

    async def connect(self, url: str = None, **kwargs):
        self.connection = await websockets.connect(url or self.stream_url)
        subscribe_message = {
            "type": "subscribe",
            "channels": [
                {
                    "name": self.channel,
                    "product_ids": [self.product_id]
                }
            ]
        }
        await self.send(json.dumps(subscribe_message))
        print(f"Connected to {url or self.stream_url} and subscribed to {self.channel} for {self.product_id}")

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
            print("Disconnected from Coinbase WebSocket.")

    async def run(self, message_handler):
        await self.connect()
        try:
            while True:
                message = await self.receive()
                parsed_message = json.loads(message)
                parsed_message["processing_time"] = datetime.now().timestamp()
                # Skip subscription confirmation and error messages
                if isinstance(parsed_message, dict) and parsed_message.get("type") in ["subscriptions", "error", "heartbeat"]:
                    continue
                await message_handler(parsed_message)
        finally:
            await self.close()