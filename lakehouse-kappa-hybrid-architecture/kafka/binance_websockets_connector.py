import asyncio
from datetime import datetime
import websockets
import json
from base_websockets_connector import BaseWebSocketAsync

# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams
# https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#individual-symbol-ticker-streams
# {'e': '24hrTicker', 'E': 1769431372027, 's': 'BTCUSDT', 'p': '-977.99000000', 'P': '-1.102', 'w': '87434.59573515', 'x': '88713.38000000', 'c': '87735.38000000', 'Q': '0.00053000', 'b': '87735.38000000', 'B': '3.07445000', 'a': '87735.39000000', 'A': '0.98431000', 'o': '88713.37000000', 'h': '88971.89000000', 'l': '86074.72000000', 'v': '20208.81539000', 'q': '1766949603.91101490', 'O': 1769344972003, 'C': 1769431372003, 'F': 5810524766, 'L': 5815548081, 'n': 5023316}

class BinanceConnector(BaseWebSocketAsync):
    def __init__(self, stream_url="wss://stream.binance.com:9443/ws", symbol="btcusdt", channel="ticker"):
        super().__init__()
        self.stream_url = f"{stream_url}/{symbol}@{channel}"
        self.symbol = symbol
        self.channel = channel
        self.connection = None

    async def connect(self, url: str = None, **kwargs):
        self.connection = await websockets.connect(url or self.stream_url)
        print(f"Connected to {url or self.stream_url}")
        # Schedule reconnection before 24hr timeout (23 hours = 82800 seconds)
        asyncio.create_task(self._schedule_reconnect())

    async def _schedule_reconnect(self):
        # Reconnect after 23 hours (before the 24hr disconnect)
        await asyncio.sleep(82800)
        print("Reconnecting before 24hr timeout...")
        # Create new connection first before closing old one to minimize data loss
        old_connection = self.connection
        new_connection = await websockets.connect(self.stream_url)
        self.connection = new_connection
        # Now close the old connection
        if old_connection:
            await old_connection.close()
        self.connection = await websockets.connect(self.stream_url)
        print(f"Reconnected to {self.stream_url}")
        # Schedule next reconnection
        asyncio.create_task(self._schedule_reconnect())

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
            print("Disconnected from Binance WebSocket.")

    async def run(self, message_handler):
        await self.connect()
        while True:
            try:
                message = await self.receive()
                parsed_message = json.loads(message)
                parsed_message["processing_time"] = datetime.now().timestamp()
                # Handle ping frame from Binance
                if isinstance(parsed_message, dict) and parsed_message.get("e") == "ping":
                    await self.send(json.dumps({"pong": parsed_message.get("p", "")}))
                    continue
                await message_handler(parsed_message)
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed, attempting to reconnect...")
                await self.connect()