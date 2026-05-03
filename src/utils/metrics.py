import time
import logging
from collections import deque
from typing import Dict, List, Any

logger = logging.getLogger("MetricsCollector")

class MetricsCollector:
    def __init__(self, window_size: int = 100):
        self.start_time = time.time()
        self.counts: Dict[str, int] = {
            "lock_requests": 0,
            "queue_enqueue": 0,
            "queue_dequeue": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
        # For latency tracking (stores last N durations)
        self.latencies: Dict[str, deque] = {
            "lock_acquire": deque(maxlen=window_size),
            "queue_op": deque(maxlen=window_size),
            "cache_op": deque(maxlen=window_size)
        }

    def increment(self, key: str):
        if key in self.counts:
            self.counts[key] += 1

    def record_latency(self, key: str, duration: float):
        if key in self.latencies:
            self.latencies[key].append(duration)

    def get_report(self) -> Dict[str, Any]:
        uptime = time.time() - self.start_time
        report = {
            "uptime_seconds": round(uptime, 2),
            "counts": self.counts,
            "averages": {}
        }
        
        for key, latencies in self.latencies.items():
            if latencies:
                report["averages"][f"{key}_avg_ms"] = round((sum(latencies) / len(latencies)) * 1000, 2)
            else:
                report["averages"][f"{key}_avg_ms"] = 0.0
                
        # Calculate throughput
        report["throughput"] = {
            "total_ops": sum(self.counts.values()),
            "ops_per_sec": round(sum(self.counts.values()) / uptime, 2) if uptime > 0 else 0
        }
        
        return report
