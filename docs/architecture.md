# Distributed Synchronization System Architecture

## 1. System Overview
Sistem ini adalah platform sinkronisasi terdistribusi tingkat lanjut yang mencakup layanan **Distributed Locking**, **Distributed Queuing**, dan **Distributed Caching**. Berbeda dengan arsitektur monolitik, sistem ini menggunakan **10-Node Cluster** yang terspesialisasi untuk menjamin ketersediaan tinggi (High Availability), toleransi kesalahan (Fault Tolerance), dan keamanan data.

### System Topology (10-Node Cluster)
```mermaid
graph TB
    subgraph Sys["Distributed Sync System"]
        subgraph LM["🔐 Lock Manager (Raft)"]
            LN1["lock-1:8001"]
            LN2["lock-2:8002"]
            LN3["lock-3:8003"]
        end
        
        subgraph QS["📨 Queue (Hash Ring)"]
            QN1["queue-1:8004"]
            QN2["queue-2:8005"]
            QN3["queue-3:8006"]
        end
        
        subgraph CS["💾 Cache (MESI)"]
            CN1["cache-1:8007"]
            CN2["cache-2:8008"]
            CN3["cache-3:8009"]
        end
        
        Redis["🔴 Redis:6379<br/>(State Store)"]
    end
    
    Network["dist-net (Docker Bridge)"]
    
    LM <-->|Raft Log Sync| Redis
    QS <-->|Persistence| Redis
    CS <-->|Backing Store| Redis
    
    Sys -.-|via| Network
```

---

## 2. Core Components & Protocols

### A. Distributed Lock Manager (Hardened Raft Consensus)
Komponen ini menjamin bahwa resource hanya bisa diakses oleh satu klien pada satu waktu.
- **Protokol:** Raft Consensus.
- **Optimasi Lanjut:**
    - **Pre-vote Phase:** Mencegah gangguan kluster dari node yang baru bangkit.
    - **Log Compaction (Snapshots):** Snapshotting otomatis setiap 50 entri.

```mermaid
stateDiagram-v2
    [*] --> FOLLOWER
    FOLLOWER --> PRE_CANDIDATE: Election Timeout
    PRE_CANDIDATE --> CANDIDATE: Majority Pre-votes
    PRE_CANDIDATE --> FOLLOWER: Discover Leader
    CANDIDATE --> LEADER: Majority Votes
    CANDIDATE --> FOLLOWER: Discover Leader / Higher Term
    LEADER --> FOLLOWER: Higher Term Discovered
    LEADER --> SNAPSHOT: Log Size > 50
    SNAPSHOT --> LEADER: Compaction Done
```

### B. Distributed Queue System (Consistent Hashing & Redis)
Sistem antrean pesan yang terdistribusi secara merata.
- **Sharding:** Menggunakan **Consistent Hashing** untuk memetakan topik pesan.
- **Persistence:** Pesan disimpan di **Redis 7**.

```mermaid
graph TB
    subgraph Ring["Consistent Hash Ring"]
        Q1["Node 1 (vnodes)"]
        Q2["Node 2 (vnodes)"]
        Q3["Node 3 (vnodes)"]
    end
    
    Msg["Message Topic"] --> Hash["SHA256 Hash"]
    Hash --> FindNode["Find Node Clockwise"]
    FindNode -->|Assigns| Ring
    Ring -->|Persist| Redis
```

### C. Distributed Cache Coherence (MESI Protocol)
Manajemen cache lokal yang tetap sinkron menggunakan protokol MESI.

```mermaid
stateDiagram-v2
    [*] --> I: Invalid
    I --> S: Read Hit (BusRd)
    I --> E: Read Miss (Exclusive)
    I --> M: Write Miss (BusRdX)
    E --> M: Write Hit
    S --> M: Write Hit (BusRdX)
    M --> S: BusRd (Snooped)
    S --> I: BusRdX (Snooped)
    E --> I: BusRdX (Snooped)
    M --> I: BusRdX (Snooped)
```

### D. Practical Byzantine Fault Tolerance (PBFT)
Menangani node yang berperilaku aneh atau jahat (Byzantine).

```mermaid
sequenceDiagram
    participant Client
    participant Primary
    participant R2 as Replica 2
    participant R3 as Replica 3
    
    Client->>Primary: REQUEST
    Primary->>R2: PRE-PREPARE
    Primary->>R3: PRE-PREPARE
    R2->>Primary: PREPARE
    R2->>R3: PREPARE
    R3->>Primary: PREPARE
    R3->>R2: PREPARE
    Note over Primary,R3: Quorum reached (2f+1)
    Primary->>R2: COMMIT
    Primary->>R3: COMMIT
    R2->>R3: COMMIT
    R3->>R2: COMMIT
    R2->>Client: REPLY
    R3->>Client: REPLY
```

---

## 3. Security & Observability

### A. Role-Based Access Control (RBAC)
- **Admin:** Akses penuh.
- **Producer/Consumer:** Akses terbatas ke antrean.
- **Reader:** Akses read-only ke cache.

### B. Audit Logging (Tamper-Proof Logic)
Setiap transaksi krusial dicatat secara permanen untuk verifikasi keamanan.

```mermaid
flowchart LR
    E0["Entry 0<br/>hash=H0"] -->|chain| E1["Entry 1<br/>prev_hash=H0"]
    E1 -->|chain| E2["Entry 2<br/>prev_hash=H1"]
    E2 -->|chain| EN["Entry N<br/>prev_hash=HN-1"]
```

### C. Health Monitoring & Self-Healing
Setiap container dilengkapi dengan **Docker Healthchecks** yang memantau endpoint `/health` dan melakukan restart otomatis jika terdeteksi kegagalan.

---

## 4. Technology Stack
- **Language:** Python 3.11 (Asyncio)
- **Communication:** HTTP/REST (aiohttp)
- **Persistence:** Redis 7
- **Infrastructure:** Docker & Docker Compose (10 Containers)
