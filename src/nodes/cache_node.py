import asyncio
import logging
import time
from collections import OrderedDict
from aiohttp import web
from src.nodes.base_node import BaseNode
from src.communication.message_passing import MessagePassing
from src.communication.failure_detector import FailureDetector
from src.utils.config import Config
from src.utils.metrics import MetricsCollector
from src.utils.security import SecurityManager

logger = logging.getLogger("CacheNode")

class CacheNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, neighbors: list):
        super().__init__(node_id, host, port)
        self.neighbors = neighbors
        self.messenger = MessagePassing()
        self.failure_detector = FailureDetector(node_id, neighbors, self.messenger)
        self.metrics_collector = MetricsCollector()
        self.cache = OrderedDict()
        self.cache_limit = Config.CACHE_LIMIT

    def setup_routes(self):
        super().setup_routes()
        self.app.router.add_get('/cache/state/{key}', self.get_cache_state)
        self.app.router.add_get('/cache/{key}', self.handle_cache_get)
        self.app.router.add_post('/cache/{key}', self.handle_cache_put)
        self.app.router.add_post('/cache/internal/snoop', self.handle_cache_snoop)
        self.app.router.add_get('/metrics', self.handle_get_metrics)
        self.app.on_startup.append(self.on_startup)
        self.app.on_cleanup.append(self.on_cleanup)

    async def get_cache_state(self, request):
        key = request.match_info.get('key')
        if key in self.cache:
            return web.json_response({"key": key, "state": self.cache[key]["state"], "node": self.node_id})
        return web.json_response({"error": "Not in cache"}, status=404)

    async def on_startup(self, app):
        asyncio.create_task(self.failure_detector.run())

    async def on_cleanup(self, app):
        await self.messenger.close()

    async def handle_cache_get(self, request):
        start = time.time()
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "cache:get"):
            SecurityManager.log_audit(self.node_id, role, "cache:get", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        key = request.match_info.get('key')
        if key in self.cache and self.cache[key]["state"] != "I":
            self.metrics_collector.increment("cache_hits")
            self.cache.move_to_end(key)
            self.metrics_collector.record_latency("cache_op", time.time() - start)
            SecurityManager.log_audit(self.node_id, role, "cache:get", key, "hit")
            return web.json_response({"status": "hit", "value": self.cache[key]["value"], "state": self.cache[key]["state"]})
        
        self.metrics_collector.increment("cache_misses")
        results = await self.messenger.broadcast_post(self.failure_detector.get_active_neighbors(), "/cache/internal/snoop", {"action": "BusRd", "key": key})
        remote = next((r for r in results if r and r.get("found")), None)
        if remote:
            self._update_local_cache(key, remote["value"], "S")
            self.metrics_collector.record_latency("cache_op", time.time() - start)
            SecurityManager.log_audit(self.node_id, role, "cache:get", key, "remote_hit")
            return web.json_response({"status": "remote_hit", "value": remote["value"], "state": "S"})
        
        SecurityManager.log_audit(self.node_id, role, "cache:get", key, "miss")
        return web.json_response({"status": "miss"}, status=404)

    async def handle_cache_put(self, request):
        start = time.time()
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "cache:put"):
            SecurityManager.log_audit(self.node_id, role, "cache:put", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        key, data = request.match_info.get('key'), await request.json()
        await self.messenger.broadcast_post(self.failure_detector.get_active_neighbors(), "/cache/internal/snoop", {"action": "BusRdX", "key": key})
        self._update_local_cache(key, data.get("value"), "M")
        self.metrics_collector.record_latency("cache_op", time.time() - start)
        SecurityManager.log_audit(self.node_id, role, "cache:put", key, "updated")
        return web.json_response({"status": "updated"})

    async def handle_cache_snoop(self, request):
        data = await request.json()
        action, key = data.get("action"), data.get("key")
        if key not in self.cache or self.cache[key]["state"] == "I": return web.json_response({"found": False})
        val = self.cache[key]["value"]
        if action == "BusRd": self.cache[key]["state"] = "S"
        elif action == "BusRdX": self.cache[key]["state"] = "I"
        return web.json_response({"found": True, "value": val})

    def _update_local_cache(self, key, value, state):
        if len(self.cache) >= self.cache_limit and key not in self.cache: self.cache.popitem(last=False)
        self.cache[key] = {"value": value, "state": state}
        self.cache.move_to_end(key)

    async def handle_get_metrics(self, request): return web.json_response(self.metrics_collector.get_report())

if __name__ == "__main__":
    node = CacheNode(Config.NODE_ID, Config.HOST, Config.PORT, Config.NEIGHBORS)
    node.run()
