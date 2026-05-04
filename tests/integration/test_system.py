import asyncio
import aiohttp
import pytest
import time

# Service Ports
LOCK_URLS = ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
QUEUE_URLS = ["http://localhost:8004", "http://localhost:8005", "http://localhost:8006"]
CACHE_URLS = ["http://localhost:8007", "http://localhost:8008", "http://localhost:8009"]

HEADERS = {"X-Role": "admin"}

@pytest.mark.asyncio
async def test_distributed_lock_shared_exclusive():
    """Test shared and exclusive lock logic across the cluster"""
    async with aiohttp.ClientSession() as session:
        # 1. Find the leader first (with retries for stability)
        leader_url = None
        for _ in range(10):
            for url in LOCK_URLS:
                try:
                    async with session.get(f"{url}/info", headers=HEADERS) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            lid = data.get("leader_id")
                            if lid:
                                # lid is 'lock-1', 'lock-2', etc. Map to port 8001, 8002...
                                leader_url = f"http://localhost:800{lid[-1]}"
                                break
                except: continue
            if leader_url: break
            await asyncio.sleep(0.5)
        
        assert leader_url is not None, "Could not find Raft leader"

        # 2. Client A acquires Shared Lock
        async with session.post(f"{leader_url}/lock/acquire", json={
            "resource_id": "res_1", "type": "shared", "client_id": "client_a"
        }, headers=HEADERS) as resp:
            assert resp.status == 200
        
        # 3. Client B acquires Shared Lock (Should be GRANTED)
        async with session.post(f"{leader_url}/lock/acquire", json={
            "resource_id": "res_1", "type": "shared", "client_id": "client_b"
        }, headers=HEADERS) as resp:
            assert resp.status == 200

        # 4. Client C tries to acquire Exclusive Lock (Should eventually be 409 Conflict)
        # Polling because state machine application might have slight latency
        conflict_detected = False
        for _ in range(10):
            async with session.post(f"{leader_url}/lock/acquire", json={
                "resource_id": "res_1", "type": "exclusive", "client_id": "client_c"
            }, headers=HEADERS) as resp:
                if resp.status == 409:
                    conflict_detected = True
                    break
                await asyncio.sleep(0.2)
        
        assert conflict_detected, "Exclusive lock was granted while Shared locks existed (Conflict not detected)"

        # 5. Release shared locks
        await session.post(f"{leader_url}/lock/release", json={"resource_id": "res_1", "client_id": "client_a"}, headers=HEADERS)
        await session.post(f"{leader_url}/lock/release", json={"resource_id": "res_1", "client_id": "client_b"}, headers=HEADERS)

        # 6. Client C tries again (Should eventually be GRANTED after release is processed)
        success = False
        for _ in range(10):
            async with session.post(f"{leader_url}/lock/acquire", json={
                "resource_id": "res_1", "type": "exclusive", "client_id": "client_c"
            }, headers=HEADERS) as resp:
                if resp.status == 200:
                    success = True
                    break
                await asyncio.sleep(0.2)
        
        assert success, "Exclusive lock was not granted even after releases"

@pytest.mark.asyncio
async def test_distributed_queue_persistence():
    """Test queue enqueue/dequeue and hash ring routing"""
    async with aiohttp.ClientSession() as session:
        topic = "test_topic"
        message = "hello_distributed_world"

        # Enqueue to Queue Node 1 (8004)
        async with session.post(f"{QUEUE_URLS[0]}/queue/enqueue", json={
            "topic": topic, "message": message
        }, headers=HEADERS) as resp:
            assert resp.status == 200
        
        # Dequeue from Queue Node 3 (8006)
        async with session.get(f"{QUEUE_URLS[2]}/queue/dequeue/{topic}", headers=HEADERS) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["message"] == message
            ack_id = data["ack_id"]
            
            # Send ACK
            async with session.post(f"{QUEUE_URLS[2]}/queue/ack", json={"ack_id": ack_id}, headers=HEADERS) as ack_resp:
                assert ack_resp.status == 200

@pytest.mark.asyncio
async def test_cache_mesi_coherence():
    """Test MESI protocol across nodes"""
    async with aiohttp.ClientSession() as session:
        key = f"test_key_{int(time.time())}"
        val = "v1"

        # 1. Put value on Cache Node 1 (8007)
        await session.post(f"{CACHE_URLS[0]}/cache/{key}", json={"value": val}, headers=HEADERS)

        # 2. Read from Cache Node 2 (8008)
        async with session.get(f"{CACHE_URLS[1]}/cache/{key}", headers=HEADERS) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["value"] == val
            assert data["state"] == "S"

        # 3. Update on Cache Node 3 (8009)
        new_val = "v2"
        await session.post(f"{CACHE_URLS[2]}/cache/{key}", json={"value": new_val}, headers=HEADERS)

        # 4. Read from Cache Node 1 (8007)
        async with session.get(f"{CACHE_URLS[0]}/cache/{key}", headers=HEADERS) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["value"] == new_val
            assert data["status"] == "remote_hit"

@pytest.mark.asyncio
async def test_raft_leader_election_simulation():
    """Verify cluster health and consensus info"""
    async with aiohttp.ClientSession() as session:
        for url in LOCK_URLS:
            try:
                async with session.get(f"{url}/info", headers=HEADERS) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert "leader_id" in data
            except: 
                pytest.fail(f"Node {url} is unreachable")
