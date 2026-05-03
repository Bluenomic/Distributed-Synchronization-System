import os
from dotenv import load_dotenv
from typing import List

# Load .env file
load_dotenv()

class Config:
    NODE_ID = os.getenv("NODE_ID", "node-1")
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", 8000))
    
    # Neighbors is a list of URLs
    _neighbors_raw = os.getenv("NEIGHBORS", "")
    NEIGHBORS = [n.strip() for n in _neighbors_raw.split(",") if n.strip()]
    
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    
    # Raft Settings
    ELECTION_TIMEOUT_MIN = float(os.getenv("ELECTION_TIMEOUT_MIN", 2.0))
    ELECTION_TIMEOUT_MAX = float(os.getenv("ELECTION_TIMEOUT_MAX", 4.0))
    HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", 1.0))
    
    # Cache Settings
    CACHE_LIMIT = int(os.getenv("CACHE_LIMIT", 100))
    
    @classmethod
    def get_node_url(cls):
        return f"http://{cls.NODE_ID}:{cls.PORT}"
