import hashlib
import json
import logging
import datetime
import os
from typing import Dict, Any, Optional

logger = logging.getLogger("Security")

class SecurityManager:
    """Handles basic RBAC, digital signatures, and audit logging."""
    
    AUDIT_LOG_FILE = "data/audit.log"

    # Simple RBAC roles
    ROLES = {
        "admin": ["lock:acquire", "lock:release", "queue:enqueue", "queue:dequeue", "cache:put", "cache:get"],
        "producer": ["queue:enqueue"],
        "consumer": ["queue:dequeue"],
        "reader": ["cache:get"]
    }

    # Node identities and cryptographic keys
    NODE_KEYS = {
        "lock-1": "secret-key-l1",
        "lock-2": "secret-key-l2",
        "lock-3": "secret-key-l3",
        "queue-1": "secret-key-q1",
        "queue-2": "secret-key-q2",
        "queue-3": "secret-key-q3",
        "cache-1": "secret-key-c1",
        "cache-2": "secret-key-c2",
        "cache-3": "secret-key-c3"
    }

    @classmethod
    def authorize(cls, role: str, action: str) -> bool:
        """Checks if a role is allowed to perform a specific action."""
        allowed_actions = cls.ROLES.get(role, [])
        return action in allowed_actions

    @classmethod
    def log_audit(cls, node_id: str, role: str, action: str, resource: str, status: str):
        """Records an action to a persistent audit log file."""
        timestamp = datetime.datetime.now().isoformat()
        entry = {
            "timestamp": timestamp,
            "node_id": node_id,
            "role": role,
            "action": action,
            "resource": resource,
            "status": status
        }
        
        # Ensure data directory exists
        if not os.path.exists("data"): os.makedirs("data")
        
        try:
            with open(cls.AUDIT_LOG_FILE, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")

    @classmethod
    def sign_message(cls, node_id: str, message: str) -> str:
        """Signs a message using the node's secret key."""
        key = cls.NODE_KEYS.get(node_id, "default-secret")
        return hashlib.sha256(f"{message}:{key}".encode()).hexdigest()

    @classmethod
    def verify_node_signature(cls, node_id: str, message: str, signature: str) -> bool:
        """Verifies that a message was signed by the specific node_id."""
        key = cls.NODE_KEYS.get(node_id, "default-secret")
        expected = hashlib.sha256(f"{message}:{key}".encode()).hexdigest()
        return expected == signature
