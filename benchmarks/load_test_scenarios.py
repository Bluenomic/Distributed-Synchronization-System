import time
from locust import HttpUser, task, between

class DistributedSystemUser(HttpUser):
    wait_time = between(0.1, 0.5)
    
    # Scenarios for Performance Analysis
    
    @task(3)
    def test_cache_operations(self):
        """Test Cache Hit/Miss and MESI propagation"""
        key = "item_123"
        # Try to GET
        self.client.get(f"/cache/{key}", name="/cache/[key]")
        # Try to PUT
        self.client.post(f"/cache/{key}", json={"value": "some_data"}, name="/cache/[key]")

    @task(2)
    def test_queue_flow(self):
        """Test Enqueue -> Dequeue -> ACK flow"""
        topic = "orders"
        # Enqueue
        res = self.client.post("/queue/enqueue", json={"topic": topic, "message": "order_data"}, name="/queue/enqueue")
        if res.status_code == 200:
            # Dequeue
            deq_res = self.client.get(f"/queue/dequeue/{topic}", name="/queue/dequeue/[topic]")
            if deq_res.status_code == 200:
                ack_id = deq_res.json().get("ack_id")
                if ack_id:
                    # ACK
                    self.client.post("/queue/ack", json={"ack_id": ack_id}, name="/queue/ack")

    @task(1)
    def test_lock_manager(self):
        """Test Raft-based Lock acquisition"""
        resource = "db_record_1"
        client_id = "locust_worker"
        # Acquire
        res = self.client.post("/lock/acquire", json={
            "resource_id": resource,
            "client_id": client_id,
            "type": "exclusive"
        }, name="/lock/acquire")
        
        if res.status_code == 200:
            # Release
            self.client.post("/lock/release", json={
                "resource_id": resource,
                "client_id": client_id
            }, name="/lock/release")
        elif res.status_code == 307:
            # Redirected to leader (handled by client or ignored for simple benchmarking)
            pass

    @task(1)
    def test_metrics(self):
        self.client.get("/metrics", name="/metrics")
