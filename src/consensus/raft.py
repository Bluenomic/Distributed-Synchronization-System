import asyncio
import random
import time
import logging
import json
import os
from enum import Enum
from typing import List, Optional, Dict, Any
from src.communication.message_passing import MessagePassing
from src.utils.config import Config

logger = logging.getLogger("Raft")

class NodeState(Enum):
    FOLLOWER = 1
    PRE_CANDIDATE = 2
    CANDIDATE = 3
    LEADER = 4

class RaftNode:
    def __init__(self, node_id: str, neighbors: List[str], messenger: MessagePassing):
        self.node_id = node_id
        self.neighbors = neighbors
        self.messenger = messenger
        
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        
        # Log structure: list of {"term": int, "command": dict}
        self.log: List[Dict[str, Any]] = []
        self.commit_index = 0
        self.last_applied = 0
        
        # Snapshot state
        self.last_included_index = 0
        self.last_included_term = 0
        self.snapshot_data = None
        
        # Persistence path
        self.persist_path = f"data/raft_{node_id}.json"
        self._load_state()
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Timings
        self.election_timeout = random.uniform(Config.ELECTION_TIMEOUT_MIN, Config.ELECTION_TIMEOUT_MAX)
        self.last_heartbeat = time.time()
        self.heartbeat_interval = Config.HEARTBEAT_INTERVAL

    def _get_log_entry(self, index: int) -> Optional[Dict[str, Any]]:
        """Safely get log entry by absolute index (1-based)"""
        if index <= self.last_included_index: return None
        local_idx = index - self.last_included_index - 1
        if 0 <= local_idx < len(self.log): return self.log[local_idx]
        return None

    def _get_last_log_index(self) -> int:
        return self.last_included_index + len(self.log)

    def _get_last_log_term(self) -> int:
        if self.log: return self.log[-1]["term"]
        return self.last_included_term

    def _save_state(self):
        """Persist term, voted_for, log, and snapshot info"""
        try:
            if not os.path.exists("data"): os.makedirs("data")
            state = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "log": self.log,
                "last_included_index": self.last_included_index,
                "last_included_term": self.last_included_term,
                "snapshot_data": self.snapshot_data
            }
            with open(self.persist_path, "w") as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Failed to save Raft state: {e}")

    def _load_state(self):
        if os.path.exists(self.persist_path):
            try:
                with open(self.persist_path, "r") as f:
                    state = json.load(f)
                    self.current_term = state.get("current_term", 0)
                    self.voted_for = state.get("voted_for")
                    self.log = state.get("log", [])
                    self.last_included_index = state.get("last_included_index", 0)
                    self.last_included_term = state.get("last_included_term", 0)
                    self.snapshot_data = state.get("snapshot_data")
                    self.commit_index = self.last_included_index
                    self.last_applied = self.last_included_index
                    logger.info(f"Raft {self.node_id}: State recovered (Snapshot Index: {self.last_included_index})")
            except Exception as e:
                logger.error(f"Failed to load Raft state: {e}")

    async def run(self):
        while True:
            try:
                if self.state == NodeState.FOLLOWER: await self._run_follower()
                elif self.state == NodeState.PRE_CANDIDATE: await self._run_pre_candidate()
                elif self.state == NodeState.CANDIDATE: await self._run_candidate()
                elif self.state == NodeState.LEADER: await self._run_leader()
            except Exception as e: logger.error(f"Error in Raft loop: {e}")
            await asyncio.sleep(0.1)

    async def _run_follower(self):
        if time.time() - self.last_heartbeat > self.election_timeout:
            logger.info(f"Node {self.node_id}: Election timeout reached. Transitioning to PRE_CANDIDATE")
            self.leader_id = None # Clear stale leader
            self.state = NodeState.PRE_CANDIDATE

    async def _run_pre_candidate(self):
        """Pre-vote phase: check if cluster is willing to elect without bumping term"""
        logger.info(f"Node {self.node_id}: Starting PRE-VOTE phase")
        self.leader_id = None
        payload = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self._get_last_log_index(),
            "last_log_term": self._get_last_log_term(),
            "pre_vote": True
        }
        results = await self.messenger.broadcast_post(self.neighbors, "/raft/request_vote", payload)
        votes = 1 + sum(1 for r in results if r and r.get("vote_granted"))
        if votes > (len(self.neighbors) + 1) / 2:
            self.state = NodeState.CANDIDATE
        else:
            self.state = NodeState.FOLLOWER
            self.last_heartbeat = time.time()

    async def _run_candidate(self):
        self.current_term += 1
        self.voted_for = self.node_id
        self._save_state()
        logger.info(f"Node {self.node_id}: Starting election for term {self.current_term}")
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(Config.ELECTION_TIMEOUT_MIN, Config.ELECTION_TIMEOUT_MAX)
        
        payload = {
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self._get_last_log_index(),
            "last_log_term": self._get_last_log_term()
        }
        results = await self.messenger.broadcast_post(self.neighbors, "/raft/request_vote", payload)
        votes = 1 + sum(1 for r in results if r and r.get("vote_granted"))
        if votes > (len(self.neighbors) + 1) / 2:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            last_idx = self._get_last_log_index()
            for n in self.neighbors:
                self.next_index[n] = last_idx + 1
                self.match_index[n] = 0
        else: await asyncio.sleep(0.5)

    async def _run_leader(self):
        for n in self.neighbors:
            asyncio.create_task(self._replicate_log_to(n))
        await asyncio.sleep(self.heartbeat_interval)

    async def _replicate_log_to(self, neighbor: str):
        next_idx = self.next_index.get(neighbor, 1)
        
        # If follower is behind snapshot, send InstallSnapshot
        if next_idx <= self.last_included_index:
            await self._send_install_snapshot(neighbor)
            return

        prev_idx = next_idx - 1
        prev_term = self.last_included_term if prev_idx == self.last_included_index else (self._get_log_entry(prev_idx)["term"] if prev_idx > 0 else 0)
        entries = self.log[next_idx - self.last_included_index - 1:]

        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index
        }
        res = await self.messenger.send_post(f"{neighbor}/raft/append_entries", payload)
        if res:
            if res.get("success"):
                self.next_index[neighbor] = prev_idx + len(entries) + 1
                self.match_index[neighbor] = prev_idx + len(entries)
                self._update_commit_index()
            elif res.get("term") > self.current_term:
                self.current_term = res.get("term")
                self.state = NodeState.FOLLOWER
                self._save_state()
            else:
                self.next_index[neighbor] = max(1, self.next_index[neighbor] - 1)

    async def _send_install_snapshot(self, neighbor: str):
        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "data": self.snapshot_data
        }
        res = await self.messenger.send_post(f"{neighbor}/raft/install_snapshot", payload)
        if res and res.get("term", 0) > self.current_term:
            self.current_term = res["term"]
            self.state = NodeState.FOLLOWER
            self._save_state()
        elif res:
            self.next_index[neighbor] = self.last_included_index + 1
            self.match_index[neighbor] = self.last_included_index

    def _update_commit_index(self):
        last_idx = self._get_last_log_index()
        for n in range(last_idx, self.commit_index, -1):
            count = 1
            for neighbor in self.neighbors:
                if self.match_index.get(neighbor, 0) >= n: count += 1
            if count > (len(self.neighbors) + 1) / 2:
                entry = self._get_log_entry(n)
                if entry and entry["term"] == self.current_term:
                    self.commit_index = n
                    break

    # --- RPC Handlers ---
    async def handle_request_vote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        term, candidate_id = data['term'], data['candidate_id']
        is_pre_vote = data.get("pre_vote", False)
        
        if term > self.current_term and not is_pre_vote:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self._save_state()

        vote_granted = False
        last_idx, last_term = self._get_last_log_index(), self._get_last_log_term()
        req_last_idx, req_last_term = data.get('last_log_index', 0), data.get('last_log_term', 0)

        # Log up-to-date check
        log_ok = (req_last_term > last_term) or (req_last_term == last_term and req_last_idx >= last_idx)
        
        if is_pre_vote:
            # Pre-vote: grant if log is ok and term is current or newer
            if term >= self.current_term and log_ok: vote_granted = True
        elif term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            if log_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                self._save_state()
            
        return {"term": self.current_term, "vote_granted": vote_granted}

    async def handle_append_entries(self, data: Dict[str, Any]) -> Dict[str, Any]:
        term, leader_id = data['term'], data['leader_id']
        if term < self.current_term: return {"term": self.current_term, "success": False}
        
        self.last_heartbeat = time.time()
        self.leader_id = leader_id
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self._save_state()

        prev_idx, prev_term = data.get('prev_log_index', 0), data.get('prev_log_term', 0)
        
        # Check if prev entry matches
        if prev_idx > 0:
            if prev_idx < self.last_included_index: return {"term": self.current_term, "success": False} # Too far back
            if prev_idx == self.last_included_index:
                if self.last_included_term != prev_term: return {"term": self.current_term, "success": False}
            else:
                entry = self._get_log_entry(prev_idx)
                if not entry or entry["term"] != prev_term: return {"term": self.current_term, "success": False}

        # Append entries
        new_entries = data.get('entries', [])
        curr_idx = prev_idx
        for entry in new_entries:
            curr_idx += 1
            if curr_idx <= self.last_included_index: continue
            local_idx = curr_idx - self.last_included_index - 1
            if local_idx < len(self.log):
                if self.log[local_idx]["term"] != entry["term"]:
                    self.log = self.log[:local_idx]
                    self.log.append(entry)
            else: self.log.append(entry)
        
        self._save_state()
        if data.get('leader_commit', 0) > self.commit_index:
            self.commit_index = min(data['leader_commit'], self._get_last_log_index())
        return {"term": self.current_term, "success": True}

    async def handle_install_snapshot(self, data: Dict[str, Any]) -> Dict[str, Any]:
        term = data.get("term", 0)
        if term < self.current_term: return {"term": self.current_term}
        
        self.last_heartbeat = time.time()
        self.last_included_index = data["last_included_index"]
        self.last_included_term = data["last_included_term"]
        self.snapshot_data = data["data"]
        
        # Clear log entries that are now in snapshot
        self.log = [] 
        self.commit_index = max(self.commit_index, self.last_included_index)
        self.last_applied = self.last_included_index
        self._save_state()
        return {"term": self.current_term}

    def create_snapshot(self, state: Any, index: int):
        """Called by state machine to compact log up to index"""
        if index <= self.last_included_index: return
        
        entry = self._get_log_entry(index)
        if not entry: return
        
        self.last_included_term = entry["term"]
        # Keep log entries AFTER index
        self.log = self.log[index - self.last_included_index:]
        self.last_included_index = index
        self.snapshot_data = state
        self._save_state()
        logger.info(f"Node {self.node_id}: Snapshot created at index {index}")

    def append_command(self, command: Dict[str, Any]) -> bool:
        if self.state != NodeState.LEADER: return False
        self.log.append({"term": self.current_term, "command": command})
        self._save_state()
        return True
