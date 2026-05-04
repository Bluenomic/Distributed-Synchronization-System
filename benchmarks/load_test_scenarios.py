import time
import random
from locust import HttpUser, task, between

class DistributedSystemUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    # Service Endpoints
    LOCK_NODES = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
    QUEUE_NODES = ["http://localhost:8004", "http://localhost:8005", "http://localhost:8006"]
    CACHE_NODES = ["http://localhost:8007", "http://localhost:8008", "http://localhost:8009"]
    
    # Use admin role to ensure all operations are allowed
    headers = {"X-Role": "admin"}
    
    @task(3)
    def test_cache_operations(self):
        """Test Cache Hit/Miss and MESI propagation"""
        node = random.choice(self.CACHE_NODES)
        key = f"item_{random.randint(1, 100)}"
        
        # Try to GET
        with self.client.get(f"{node}/cache/{key}", headers=self.headers, name="/cache/[key]", catch_response=True) as response:
            if response.status_code in [200, 404]:
                response.success()
        
        # Try to PUT
        self.client.post(f"{node}/cache/{key}", json={"value": "data"}, headers=self.headers, name="/cache/[key]")

    @task(2)
    def test_queue_flow(self):
        """Test Enqueue -> Dequeue -> ACK flow"""
        node = random.choice(self.QUEUE_NODES)
        topic = "orders"
        
        # Enqueue
        self.client.post(f"{node}/queue/enqueue", json={"topic": topic, "message": "job"}, headers=self.headers, name="/queue/enqueue")
        
        # Dequeue
        with self.client.get(f"{node}/queue/dequeue/{topic}", headers=self.headers, name="/queue/dequeue/[topic]", catch_response=True) as response:
            if response.status_code == 200:
                ack_id = response.json().get("ack_id")
                if ack_id:
                    self.client.post(f"{node}/queue/ack", json={"ack_id": ack_id}, headers=self.headers, name="/queue/ack")
                response.success()
            elif response.status_code == 404:
                response.success()

    @task(1)
    def test_lock_manager(self):
        """Test Raft-based Lock acquisition"""
        node = random.choice(self.LOCK_NODES)
        resource = "db_record_1"
        client_id = f"worker_{random.randint(1, 1000)}"
        
        # Acquire
        with self.client.post(f"{node}/lock/acquire", json={
            "resource_id": resource,
            "client_id": client_id,
            "type": "exclusive"
        }, headers=self.headers, name="/lock/acquire", allow_redirects=False, catch_response=True) as res:
            if res.status_code == 200:
                # Release
                self.client.post(f"{node}/lock/release", json={
                    "resource_id": resource,
                    "client_id": client_id
                }, headers=self.headers, name="/lock/release")
                res.success()
            elif res.status_code in [307, 409, 503]:
                res.success()

    @task(1)
    def test_metrics(self):
        node = random.choice(self.LOCK_NODES + self.QUEUE_NODES + self.CACHE_NODES)
        self.client.get(f"{node}/metrics", headers=self.headers, name="/metrics")
