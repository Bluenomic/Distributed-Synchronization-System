import asyncio
import logging
import time
import json
from enum import Enum
from typing import List, Dict, Any, Set
from src.communication.message_passing import MessagePassing
from src.utils.security import SecurityManager

logger = logging.getLogger("PBFT")

class PBFTConsensus:
    def __init__(self, node_id: str, neighbors: List[str], messenger: MessagePassing):
        self.node_id = node_id
        self.neighbors = neighbors
        self.messenger = messenger
        self.n = len(neighbors) + 1
        # In a 3-node cluster, we can't strictly satisfy n >= 3f + 1 for f=1.
        # However, for this project, we treat f=1 if n=3 to demonstrate the logic.
        self.f = max(1, (self.n - 1) // 3) if self.n >= 3 else 0
        
        self.view_number = 0
        self.sequence_number = 0
        self.state = {} # {seq: {"pre-prepare": msg, "prepares": set(), "commits": set(), "status": str}}
        
        # View Change state
        self.view_change_votes = {} # {view: set(node_ids)}
        self.last_request_time = time.time()
        self.view_change_timeout = 10.0 # Timeout before suspecting leader

    def _get_primary(self, view: int) -> str:
        # Simple round-robin primary selection
        all_nodes = sorted(self.neighbors + [self.node_id])
        return all_nodes[view % self.n]

    def _sign(self, payload: Dict[str, Any]) -> str:
        msg_str = json.dumps(payload, sort_keys=True)
        return SecurityManager.sign_message(self.node_id, msg_str)

    def _verify(self, msg: Dict[str, Any]) -> bool:
        signature = msg.pop("sig", None)
        node_id = msg.get("node_id")
        if not signature or not node_id: return False
        msg_str = json.dumps(msg, sort_keys=True)
        return SecurityManager.verify_node_signature(node_id, msg_str, signature)

    async def propose(self, command: Dict[str, Any]):
        """Primary node starts the 3-phase consensus"""
        if self._get_primary(self.view_number) != self.node_id: return
        
        self.sequence_number += 1
        payload = {
            "type": "pre-prepare",
            "view": self.view_number,
            "seq": self.sequence_number,
            "command": command,
            "node_id": self.node_id
        }
        payload["sig"] = self._sign(payload)
        await self.messenger.broadcast_post(self.neighbors, "/consensus/pbft", payload)

    async def handle_message(self, msg: Dict[str, Any]):
        if not self._verify(msg.copy()):
            logger.warning(f"PBFT {self.node_id}: Invalid signature from {msg.get('node_id')}")
            return False

        msg_type = msg.get("type")
        seq = msg.get("seq")
        view = msg.get("view", 0)

        # 1. Handle View Change messages
        if msg_type == "view-change":
            target_view = msg.get("target_view")
            if target_view not in self.view_change_votes: self.view_change_votes[target_view] = set()
            self.view_change_votes[target_view].add(msg["node_id"])
            if len(self.view_change_votes[target_view]) >= 2 * self.f + 1:
                if target_view > self.view_number:
                    logger.info(f"PBFT {self.node_id}: View Change successful. New view: {target_view}")
                    self.view_number = target_view
                    self.view_change_votes.clear()
            return False

        # 2. Handle Consensus messages (Pre-prepare, Prepare, Commit)
        if view != self.view_number: return False
        
        if seq not in self.state:
            self.state[seq] = {"prepares": set(), "commits": set(), "status": "pending"}

        if msg_type == "pre-prepare":
            # Follower validates primary and sends Prepare
            if msg["node_id"] != self._get_primary(view): return False
            self.state[seq]["pre-prepare"] = msg
            prepare = {"type": "prepare", "view": view, "seq": seq, "node_id": self.node_id}
            prepare["sig"] = self._sign(prepare)
            await self.messenger.broadcast_post(self.neighbors, "/consensus/pbft", prepare)

        elif msg_type == "prepare":
            self.state[seq]["prepares"].add(msg["node_id"])
            if len(self.state[seq]["prepares"]) >= 2 * self.f:
                if self.state[seq]["status"] == "pending":
                    self.state[seq]["status"] = "prepared"
                    SecurityManager.log_audit(self.node_id, "pbft", "consensus:prepared", str(seq), "success")
                    commit = {"type": "commit", "view": view, "seq": seq, "node_id": self.node_id}
                    commit["sig"] = self._sign(commit)
                    await self.messenger.broadcast_post(self.neighbors, "/consensus/pbft", commit)

        elif msg_type == "commit":
            self.state[seq]["commits"].add(msg["node_id"])
            if len(self.state[seq]["commits"]) >= 2 * self.f + 1:
                if self.state[seq]["status"] != "committed":
                    logger.info(f"PBFT {self.node_id}: Seq {seq} COMMITTED (Byzantine Fault Tolerant)")
                    SecurityManager.log_audit(self.node_id, "pbft", "consensus:committed", str(seq), "byzantine_verified")
                    self.state[seq]["status"] = "committed"
                    self.last_request_time = time.time()
                    return True # Executed
        return False

    async def run_view_change_monitor(self):
        """Monitor leader health and trigger view change if needed"""
        while True:
            await asyncio.sleep(5)
            if time.time() - self.last_request_time > self.view_change_timeout:
                if self._get_primary(self.view_number) != self.node_id:
                    logger.warning(f"PBFT {self.node_id}: Leader timeout suspected. Triggering View Change.")
                    target_view = self.view_number + 1
                    msg = {"type": "view-change", "target_view": target_view, "node_id": self.node_id}
                    msg["sig"] = self._sign(msg)
                    await self.messenger.broadcast_post(self.neighbors, "/consensus/pbft", msg)
