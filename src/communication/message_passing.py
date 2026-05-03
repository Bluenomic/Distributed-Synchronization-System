import aiohttp
import logging
import asyncio
from typing import Any, Dict, Optional

logger = logging.getLogger("MessagePassing")

class MessagePassing:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def send_post(self, url: str, data: Dict[str, Any], timeout: float = 2.0) -> Optional[Dict[str, Any]]:
        """Sends a POST request and returns the JSON response."""
        session = await self._get_session()
        try:
            async with session.post(url, json=data, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.debug(f"POST to {url} returned status {resp.status}")
        except Exception as e:
            logger.debug(f"Failed to send POST to {url}: {e}")
        return None

    async def send_get(self, url: str, timeout: float = 2.0) -> Optional[Dict[str, Any]]:
        """Sends a GET request and returns the JSON response."""
        session = await self._get_session()
        try:
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.debug(f"GET to {url} returned status {resp.status}")
        except Exception as e:
            logger.debug(f"Failed to send GET to {url}: {e}")
        return None

    async def broadcast_post(self, neighbors: list, path: str, data: Dict[str, Any]) -> list:
        """Broadcasts a POST request to all neighbors and returns list of results."""
        tasks = []
        for neighbor in neighbors:
            url = f"{neighbor.rstrip('/')}/{path.lstrip('/')}"
            tasks.append(self.send_post(url, data))
        
        return await asyncio.gather(*tasks)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
