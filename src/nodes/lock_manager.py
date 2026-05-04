import asyncio
import logging
import time
from typing import List, Dict, Any
from aiohttp import web
from src.nodes.base_node import BaseNode
from src.consensus.raft import RaftNode
from src.communication.message_passing import MessagePassing
from src.communication.failure_detector import FailureDetector
from src.utils.config import Config
from src.utils.metrics import MetricsCollector
from src.utils.security import SecurityManager

logger = logging.getLogger("LockManager")

class LockManager(BaseNode):
    def __init__(self, node_id: str, host: str, port: int, neighbors: list):
        super().__init__(node_id, host, port)
        self.neighbors = neighbors
        self.messenger = MessagePassing()
        self.failure_detector = FailureDetector(node_id, neighbors, self.messenger)
        self.metrics_collector = MetricsCollector()
        self.raft = RaftNode(node_id, neighbors, self.messenger)
        self.locks = {} # { resource_id: {"owners": [ids], "type": "shared/exclusive", "timestamp": ts} }
        self.waiting_for = {}

    def setup_routes(self):
        super().setup_routes()
        self.app.router.add_post('/raft/request_vote', self.handle_raft_request_vote)
        self.app.router.add_post('/raft/append_entries', self.handle_raft_append_entries)
        self.app.router.add_post('/raft/install_snapshot', self.handle_raft_install_snapshot)
        self.app.router.add_post('/lock/acquire', self.acquire_lock)
        self.app.router.add_post('/lock/release', self.release_lock)
        self.app.router.add_get('/metrics', self.handle_get_metrics)
        self.app.router.add_get('/info', self.get_info) # Explicitly override base
        self.app.on_startup.append(self.on_startup)
        self.app.on_cleanup.append(self.on_cleanup)

    async def get_info(self, request):
        """Extended info with Raft state"""
        return web.json_response({
            "node_id": self.node_id,
            "type": "LockManager",
            "raft_state": self.raft.state.name,
            "current_term": self.raft.current_term,
            "leader_id": self.raft.leader_id,
            "commit_index": self.raft.commit_index,
            "neighbors": self.neighbors
        })

    async def on_startup(self, app):
        # Recover from Raft snapshot if exists
        if self.raft.snapshot_data:
            self.locks = self.raft.snapshot_data
            logger.info(f"Node {self.node_id}: State recovered from Raft snapshot (Index {self.raft.last_included_index})")
        
        asyncio.create_task(self.failure_detector.run())
        asyncio.create_task(self.raft.run())
        asyncio.create_task(self.state_machine_loop())
        asyncio.create_task(self.deadlock_cleanup_loop())
        asyncio.create_task(self.snapshot_monitor_loop())

    async def handle_raft_install_snapshot(self, request):
        res = await self.raft.handle_install_snapshot(await request.json())
        # Apply snapshot to state machine immediately
        if self.raft.snapshot_data:
            self.locks = self.raft.snapshot_data
        return web.json_response(res)

    async def snapshot_monitor_loop(self):
        """Monitor log size and trigger snapshotting"""
        while True:
            await asyncio.sleep(30)
            if len(self.raft.log) > 50: # Threshold for snapshotting
                logger.info(f"Node {self.node_id}: Log size {len(self.raft.log)} exceeds threshold. Taking snapshot.")
                # We snapshot up to the last applied index
                self.raft.create_snapshot(self.locks, self.raft.last_applied)

    async def on_cleanup(self, app):
        await self.messenger.close()

    async def state_machine_loop(self):
        while True:
            if self.raft.commit_index > self.raft.last_applied:
                self.raft.last_applied += 1
                entry = self.raft.log[self.raft.last_applied - 1]
                await self._apply_log_entry(entry)
            await asyncio.sleep(0.1)

    async def _apply_log_entry(self, entry):
        cmd = entry["command"]
        action, res_id, client_id, ltype = cmd.get("action"), cmd.get("resource_id"), cmd.get("client_id"), cmd.get("type")
        if action == "acquire":
            if res_id not in self.locks:
                self.locks[res_id] = {"owners": [client_id], "type": ltype, "timestamp": time.time()}
            elif client_id not in self.locks[res_id]["owners"]:
                self.locks[res_id]["owners"].append(client_id)
            if client_id in self.waiting_for and res_id in self.waiting_for[client_id]:
                self.waiting_for[client_id].remove(res_id)
                if not self.waiting_for[client_id]: del self.waiting_for[client_id]
        elif action == "release":
            if res_id in self.locks and client_id in self.locks[res_id]["owners"]:
                self.locks[res_id]["owners"].remove(client_id)
                if not self.locks[res_id]["owners"]: del self.locks[res_id]

    async def acquire_lock(self, request):
        start = time.time()
        # RBAC Check
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "lock:acquire"):
            SecurityManager.log_audit(self.node_id, role, "lock:acquire", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        self.metrics_collector.increment("lock_requests")
        
        if self.raft.leader_id is None:
            return web.json_response({"error": "No leader elected", "detail": "Election in progress, please retry in a few seconds."}, status=503)

        if self.raft.leader_id != self.node_id:
            leader_url = f"http://{self.raft.leader_id}:8000/lock/acquire"
            return web.json_response(
                {"error": "Not leader", "leader_id": self.raft.leader_id}, 
                status=307,
                headers={"Location": leader_url}
            )
        
        try:
            data = await request.json()
        except Exception as e:
            logger.error(f"Node {self.node_id}: JSON Decode Error: {e}")
            return web.json_response({"error": "Invalid JSON payload", "detail": str(e)}, status=400)
            
        res_id, ltype, client_id = data.get("resource_id"), data.get("type", "exclusive"), data.get("client_id")
        
        if res_id in self.locks:
            current = self.locks[res_id]
            if current["type"] == "exclusive" or ltype == "exclusive":
                if client_id not in current["owners"]:
                    if client_id not in self.waiting_for: self.waiting_for[client_id] = set()
                    self.waiting_for[client_id].add(res_id)
                    SecurityManager.log_audit(self.node_id, role, "lock:acquire", res_id, "conflict_deadlock_tracking")
                    return web.json_response({"status": "denied", "reason": "Conflict - Tracking for Deadlock"}, status=409)
        
        if self.raft.append_command({"action": "acquire", "resource_id": res_id, "type": ltype, "client_id": client_id}):
            await asyncio.sleep(0.05)
            self.metrics_collector.record_latency("lock_acquire", time.time() - start)
            SecurityManager.log_audit(self.node_id, role, "lock:acquire", res_id, "granted")
            return web.json_response({"status": "granted"})
        return web.json_response({"error": "Raft failure"}, status=500)

    async def release_lock(self, request):
        # RBAC Check
        role = request.headers.get("X-Role", "guest")
        if not SecurityManager.authorize(role, "lock:release"):
            SecurityManager.log_audit(self.node_id, role, "lock:release", "unknown", "denied_rbac")
            return web.json_response({"error": "Unauthorized"}, status=403)

        if self.raft.leader_id != self.node_id: return web.json_response({"error": "Not leader"}, status=307)
        data = await request.json()
        res_id = data.get("resource_id")
        self.raft.append_command({"action": "release", "resource_id": res_id, "client_id": data.get("client_id")})
        SecurityManager.log_audit(self.node_id, role, "lock:release", res_id, "released_pending")
        return web.json_response({"status": "released_pending"})

    async def handle_raft_request_vote(self, request): return web.json_response(await self.raft.handle_request_vote(await request.json()))
    async def handle_raft_append_entries(self, request): return web.json_response(await self.raft.handle_append_entries(await request.json()))
    async def handle_get_metrics(self, request): return web.json_response(self.metrics_collector.get_report())

    async def deadlock_cleanup_loop(self):
        while True:
            await asyncio.sleep(10)
            if self.raft.state == self.raft.state.LEADER:
                cycle = self._find_deadlock_cycle()
                if cycle:
                    victim = cycle[0]
                    for rid, info in list(self.locks.items()):
                        if victim in info["owners"]: self.raft.append_command({"action": "release", "resource_id": rid, "client_id": victim})
                    if victim in self.waiting_for: del self.waiting_for[victim]

    def _find_deadlock_cycle(self) -> List[str]:
        graph = {}
        for waiter, res_ids in self.waiting_for.items():
            graph[waiter] = set()
            for rid in res_ids:
                if rid in self.locks:
                    for owner in self.locks[rid]["owners"]:
                        if owner != waiter: graph[waiter].add(owner)
        visited, path = set(), []
        def visit(u):
            if u in path: return path[path.index(u):]
            if u in visited: return None
            visited.add(u)
            path.append(u)
            for v in graph.get(u, []):
                res = visit(v)
                if res: return res
            path.pop()
            return None
        for node in list(graph.keys()):
            res = visit(node)
            if res: return res
        return []

if __name__ == "__main__":
    node = LockManager(Config.NODE_ID, Config.HOST, Config.PORT, Config.NEIGHBORS)
    node.run()
