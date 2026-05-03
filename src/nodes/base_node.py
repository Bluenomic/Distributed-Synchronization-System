import asyncio
import logging
from aiohttp import web
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BaseNode")

class BaseNode:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        
    def setup_routes(self):
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/info', self.get_info)
        self.app.router.add_post('/cluster/join', self.handle_cluster_join)

    async def health_check(self, request):
        return web.json_response({"status": "healthy", "node_id": self.node_id})

    async def get_info(self, request):
        return web.json_response({
            "node_id": self.node_id,
            "type": self.__class__.__name__,
            "host": self.host,
            "port": self.port,
            "neighbors": getattr(self, 'neighbors', [])
        })

    async def handle_cluster_join(self, request):
        """Allows a new node to join the cluster dynamically."""
        data = await request.json()
        new_node_url = data.get("node_url")
        if not new_node_url:
            return web.json_response({"error": "Missing node_url"}, status=400)
        
        if hasattr(self, 'neighbors') and new_node_url not in self.neighbors:
            logger.info(f"Node {self.node_id}: New node {new_node_url} joined the cluster.")
            self.neighbors.append(new_node_url)
            # If we have a failure detector, tell it to monitor the new node
            if hasattr(self, 'failure_detector'):
                if new_node_url not in self.failure_detector.neighbors:
                    self.failure_detector.neighbors.append(new_node_url)
                    self.failure_detector.alive_nodes[new_node_url] = True
                    self.failure_detector.last_seen[new_node_url] = time.time()

        return web.json_response({"status": "joined", "cluster_size": len(self.neighbors) + 1})

    def run(self):
        logger.info(f"Starting {self.__class__.__name__} {self.node_id} on {self.host}:{self.port}")
        web.run_app(self.app, host=self.host, port=self.port)

if __name__ == "__main__":
    node_id = os.getenv("NODE_ID", "node-1")
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))
    
    node = BaseNode(node_id, host, port)
    node.run()
