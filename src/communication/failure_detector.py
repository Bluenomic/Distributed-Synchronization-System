import asyncio
import logging
import time
from typing import Dict, List, Set
from src.communication.message_passing import MessagePassing

logger = logging.getLogger("FailureDetector")

class FailureDetector:
    def __init__(self, node_id: str, neighbors: List[str], messenger: MessagePassing):
        self.node_id = node_id
        self.neighbors = neighbors
        self.messenger = messenger
        
        # State: { neighbor_url: is_alive }
        self.alive_nodes: Dict[str, bool] = {n: True for n in neighbors}
        # Last seen timestamps for more advanced checks if needed
        self.last_seen: Dict[str, float] = {n: time.time() for n in neighbors}
        
        self.check_interval = 2.0  # Heartbeat every 2 seconds
        self.timeout = 5.0         # Mark as dead after 5 seconds of no response

    async def run(self):
        """Main loop to monitor neighbors"""
        logger.info(f"Node {self.node_id}: Failure Detector started.")
        while True:
            tasks = []
            for neighbor in self.neighbors:
                tasks.append(self._check_neighbor(neighbor))
            
            await asyncio.gather(*tasks)
            await asyncio.sleep(self.check_interval)

    async def _check_neighbor(self, neighbor: str):
        """Sends a health check request to a neighbor"""
        # We'll assume nodes have a '/health' endpoint from BaseNode
        result = await self.messenger.send_get(f"{neighbor.rstrip('/')}/health", timeout=1.5)
        
        if result and result.get("status") == "healthy":
            if not self.alive_nodes[neighbor]:
                logger.info(f"Node {self.node_id}: Neighbor {neighbor} is BACK ONLINE.")
            self.alive_nodes[neighbor] = True
            self.last_seen[neighbor] = time.time()
        else:
            # Check for timeout
            if self.alive_nodes[neighbor] and (time.time() - self.last_seen[neighbor] > self.timeout):
                logger.warning(f"Node {self.node_id}: Neighbor {neighbor} DETECTED AS DOWN.")
                self.alive_nodes[neighbor] = False

    def is_alive(self, neighbor_url: str) -> bool:
        """Returns the cached health status of a neighbor"""
        return self.alive_nodes.get(neighbor_url, False)

    def add_neighbor(self, neighbor_url: str):
        """Dynamically adds a new neighbor to monitor"""
        if neighbor_url not in self.neighbors:
            self.neighbors.append(neighbor_url)
            self.alive_nodes[neighbor_url] = True
            self.last_seen[neighbor_url] = time.time()
            logger.info(f"FailureDetector: Now monitoring new neighbor {neighbor_url}")

    def get_active_neighbors(self) -> List[str]:
        """Returns a list of neighbors that are currently alive"""
        return [n for n, alive in self.alive_nodes.items() if alive]
