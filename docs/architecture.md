# Distributed Synchronization System Architecture

## 1. System Overview
Sistem ini adalah platform sinkronisasi terdistribusi yang menyediakan tiga layanan utama: Distributed Locking, Distributed Queuing, dan Distributed Caching. Sistem dibangun menggunakan arsitektur peer-to-peer di mana setiap node (3 node) memiliki kapabilitas yang sama.

```mermaid
graph TD
    Client[Client/User] -->|HTTP Request| Node1[Main Node 1]
    Client -->|HTTP Request| Node2[Main Node 2]
    Client -->|HTTP Request| Node3[Main Node 3]
    
    subgraph Cluster
        Node1 <-->|Raft / Gossip / MESI| Node2
        Node2 <-->|Raft / Gossip / MESI| Node3
        Node3 <-->|Raft / Gossip / MESI| Node1
    end
    
    Node1 --> Redis[(Redis Persistence)]
    Node2 --> Redis
    Node3 --> Redis
```

## 2. Core Components

### A. Distributed Lock Manager (Raft Consensus)
Menggunakan algoritma Raft untuk memastikan konsistensi dalam pengelolaan lock.
- **Leader Election**: Node berkompetisi untuk menjadi Leader melalui mekanisme voting jika tidak menerima heartbeat.
- **Lock Management**: Hanya Leader yang berhak memberikan `acquire` atau `release` lock.

```mermaid
stateDiagram-v2
    [*] --> Follower
    Follower --> Candidate: Election Timeout
    Candidate --> Candidate: Vote Failed / Split
    Candidate --> Leader: Majority Votes
    Leader --> Follower: New Term / Higher Term
    Candidate --> Follower: Discover Leader
```

### B. Distributed Queue System (Consistent Hashing)
Menggunakan Consistent Hashing untuk mendistribusikan beban penyimpanan antrean.
- **Consistent Hashing**: Memetakan setiap `topic` ke node tertentu pada *hash ring*.
- **Persistence**: Setiap pesan disimpan di Redis.

```mermaid
graph LR
    subgraph HashRing
        T1((Topic A)) --> N1[Node 1]
        T2((Topic B)) --> N2[Node 2]
        T3((Topic C)) --> N3[Node 3]
        T4((Topic D)) --> N1
    end
```

### C. Distributed Cache Coherence (MESI Protocol)
Memastikan integritas data cache di seluruh node menggunakan protokol MESI.

```mermaid
stateDiagram-v2
    I --> S: Local Read (BusRd)
    S --> M: Local Write (BusRdX)
    I --> E: Local Read (No others have it)
    E --> M: Local Write
    M --> S: Remote Read (BusRd)
    S --> I: Remote Write (BusRdX)
    E --> I: Remote Write (BusRdX)
    M --> I: Remote Write (BusRdX)
```

## 3. Communication Pattern
- **Inter-node communication**: Menggunakan HTTP REST API (asynchronous via `aiohttp`).
- **State Management**: Redis digunakan untuk persistensi data antrean.

## 4. Technology Stack
- **Language**: Python 3.9+ (Asyncio)
- **Web Framework**: aiohttp
- **State Store**: Redis
- **Containerization**: Docker & Docker Compose
