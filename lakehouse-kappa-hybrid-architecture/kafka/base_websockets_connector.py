from abc import ABC, abstractmethod

# Asynchronous WebSocket Base Class
class BaseWebSocketAsync(ABC):
    @abstractmethod
    async def connect(self, url: str, **kwargs):
        """Establish an asynchronous WebSocket connection."""
        pass

    @abstractmethod
    async def send(self, data: str):
        """Send data asynchronously over the WebSocket."""
        pass

    @abstractmethod
    async def receive(self) -> str:
        """Receive data asynchronously from the WebSocket."""
        pass

    @abstractmethod
    async def close(self):
        """Close the asynchronous WebSocket connection."""
        pass