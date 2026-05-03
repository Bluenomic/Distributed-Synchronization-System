import asyncio
import logging
import uuid
import time
import json
from aiohttp import web
import aioredis
from src.nodes.base_node import BaseNode
from src.communication.message_passing import MessagePassing
from src.communication.failure_detector import FailureDetector
from src.utils.hashing import ConsistentHashing
from src.utils.config import Config
from src.utils.metrics import MetricsCollector
from src.utils.security import SecurityManager

logger = logging.getLogger("QueueNode")

class QueueNode(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, neighbors: list):
        super().__init__(node_id, host, port)
        self.messenger = MessagePassing()
        self.failure_detector = FailureDetector(node_id, neighbors, self.messenger)
        self.metrics_collector = MetricsCollector()
        self.hash_ring = ConsistentHashing(nodes=neighbors + [f"http://{host}:{port}"])
        self.queues = {}
        self.pending_acks = {}
        self.redis = None

    def setup_routes(self):
        super().setup_routes()
        self.app.router.add_post('/queue/enqueue', self.handle_enqueue)
        self.app.router.add_get('/queue/dequeue/{topic}', self.handle_dequeue)
        self.app.router.add_post('/queue/ack', self.handle_queue_ack)
        self.app.router.add_get('/metrics', self.handle_get_metrics)
        self.app.on_startup.append(self.on_startup)
        self.app.on_cleanup.append(self.on_cleanup)

    async def on_startup(self, app):
        try:
            self.redis = await aioredis.from_url(f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}", decode_responses=True)
            logger.info(f"Node {self.node_id}: Connected to Redis. Starting recovery...")
            await self._recover_from_redis()
        except Exception as e: logger.error(f"Redis error during startup: {e}")
        asyncio.create_task(self.failure_detector.run())
        asyncio.create_task(self.queue_ack_monitor_loop())

    async def _recover_from_redis(self):
        if not self.redis: return
        try:
            ack_keys = await self.redis.keys("pending_ack:*")
            for akey in ack_keys:
                data_json = await self.redis.get(akey)
                if data_json:
                    info = json.loads(data_json)
                    topic = info["topic"]
                    target = self.hash_ring.get_node(topic)
                    if target.startswith(f"http://{self.host}") or target.startswith(f"http://{self.node_id}"):
                        if topic not in self.queues: self.queues[topic] = asyncio.Queue()
                        await self.queues[topic].put(info["message"])
                        await self.redis.delete(akey)
            keys = await self.redis.keys("queue:*")
            for key in keys:
                topic = key.split(":")[1]
                target = self.hash_ring.get_node(topic)
                if target.startswith(f"http://{self.host}") or target.startswith(f"http://{self.node_id}"):
                    messages = await self.redis.lrange(key, 0, -1)
                    if messages:
                        if topic not in self.queues: self.queues[topic] = asyncio.Queue()
                        while not self.queues[topic].empty(): self.queues[topic].get_nowait()
                        for msg in messages: await self.queues[topic].put(msg)
        except Exception as e: logger.error(f"Recovery error: {e}")

    async def on_cleanup(self, app):
        await self.messenger.close()
        if self.redis: await self.redis.close()

    async def handle_enqueue(self, request):
        start = time.time()
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "queue:enqueue"):
            SecurityManager.log_audit(self.node_id, role, "queue:enqueue", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        self.metrics_collector.increment("queue_enqueue")
        data = await request.json()
        topic, message = data.get("topic"), data.get("message")
        target = self.hash_ring.get_node(topic)
        if target.startswith(f"http://{self.host}") or target.startswith(f"http://{self.node_id}"):
            if topic not in self.queues: self.queues[topic] = asyncio.Queue()
            await self.queues[topic].put(message)
            if self.redis: await self.redis.rpush(f"queue:{topic}", message)
            self.metrics_collector.record_latency("queue_op", time.time() - start)
            SecurityManager.log_audit(self.node_id, role, "queue:enqueue", topic, "success")
            return web.json_response({"status": "enqueued"})
        res = await self.messenger.send_post(f"{target}/queue/enqueue", data)
        return web.json_response(res) if res else web.json_response({"error": "unreachable"}, status=502)

    async def handle_dequeue(self, request):
        start = time.time()
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "queue:dequeue"):
            SecurityManager.log_audit(self.node_id, role, "queue:dequeue", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        self.metrics_collector.increment("queue_dequeue")
        topic = request.match_info.get('topic')
        target = self.hash_ring.get_node(topic)
        if target.startswith(f"http://{self.host}") or target.startswith(f"http://{self.node_id}"):
            message = await self.queues[topic].get() if topic in self.queues and not self.queues[topic].empty() else (await self.redis.lpop(f"queue:{topic}") if self.redis else None)
            if message:
                ack_id = str(uuid.uuid4())
                info = {"topic": topic, "message": message, "timestamp": time.time()}
                self.pending_acks[ack_id] = info
                if self.redis: await self.redis.set(f"pending_ack:{ack_id}", json.dumps(info), ex=300)
                self.metrics_collector.record_latency("queue_op", time.time() - start)
                SecurityManager.log_audit(self.node_id, role, "queue:dequeue", topic, "success")
                return web.json_response({"status": "dequeued", "message": message, "ack_id": ack_id})
            SecurityManager.log_audit(self.node_id, role, "queue:dequeue", topic, "empty")
            return web.json_response({"status": "empty"}, status=404)
        res = await self.messenger.send_get(f"{target}/queue/dequeue/{topic}")
        return web.json_response(res) if res else web.json_response({"error": "unreachable"}, status=502)

    async def handle_queue_ack(self, request):
        data = await request.json()
        ack_id = data.get("ack_id")
        if ack_id in self.pending_acks:
            del self.pending_acks[ack_id]
            if self.redis: await self.redis.delete(f"pending_ack:{ack_id}")
            return web.json_response({"status": "acknowledged"})
        if self.redis and await self.redis.exists(f"pending_ack:{ack_id}"):
            await self.redis.delete(f"pending_ack:{ack_id}")
            return web.json_response({"status": "acknowledged (recovered)"})
        return web.json_response({"status": "not_found"}, status=404)

    async def handle_get_metrics(self, request): return web.json_response(self.metrics_collector.get_report())

    async def queue_ack_monitor_loop(self):
        while True:
            await asyncio.sleep(15)
            now = time.time()
            for aid, info in list(self.pending_acks.items()):
                if now - info["timestamp"] > 30:
                    if info["topic"] not in self.queues: self.queues[info["topic"]] = asyncio.Queue()
                    await self.queues[info["topic"]].put(info["message"])
                    del self.pending_acks[aid]

if __name__ == "__main__":
    node = QueueNode(Config.NODE_ID, Config.HOST, Config.PORT, Config.NEIGHBORS)
    node.run()
