# Distributed Synchronization System
**Tugas 3 - Sistem Paralel dan Terdistribusi**

**NAMA  : Imam Dzulvan Muffid**
**NIM   : 11231031**

**Link Demo   :https://youtu.be/-yaeUzZygNw**

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/docker-compose-blue.svg)](https://www.docker.com/)
[![Redis](https://img.shields.io/badge/redis-7.0-red.svg)](https://redis.io/)

Platform sinkronisasi terdistribusi yang tangguh, aman, dan berperforma tinggi. Proyek ini mengimplementasikan kluster **10-Node** yang mencakup Distributed Locking, Queuing, dan Caching dengan toleransi kesalahan terhadap kegagalan node dan serangan Byzantine.

---

## Fitur Utama

### 1. Distributed Lock Manager (Hardened Raft)
*   **Leader Election:** Pemilihan pemimpin otomatis dalam < 300ms.
*   **Log Compaction:** Snapshotting otomatis setiap 50 entri untuk efisiensi recovery.
*   **Pre-vote Phase:** Proteksi kluster terhadap gangguan dari node yang *stale*.
*   **Leader Redirection:** Otomatisasi routing request klien ke node Leader (HTTP 307).

### 2. Distributed Queue (Consistent Hashing)
*   **Dynamic Sharding:** Topik pesan didistribusikan merata menggunakan algoritma Consistent Hashing.
*   **At-Least-Once Delivery:** Mekanisme Acknowledgement (ACK) untuk menjamin pesan sampai.
*   **Redis Persistence:** Data antrean tetap aman meskipun seluruh node aplikasi mati.

### 3. Distributed Cache (MESI Protocol)
*   **Cache Coherence:** Sinkronisasi status *Invalid, Shared, Exclusive, Modified* antar-node secara real-time.
*   **LRU Eviction:** Manajemen memori lokal yang cerdas untuk data sementara.

### 4. Advanced Security & Byzantine Tolerance
*   **PBFT Consensus:** Toleransi terhadap node jahat (Byzantine) dengan alur 3-Phase Commit.
*   **RBAC Security:** Kontrol akses berbasis peran (Admin, Producer, Consumer, Reader).
*   **Audit Logging:** Pencatatan setiap transaksi krusial ke dalam file log yang terintegrasi.

---

## Arsitektur Kluster (10 Nodes)

Sistem berjalan di atas jaringan bridge Docker dengan topologi berikut:

| Kelompok Node | Nama Container | Port Host | Protokol Utama |
| :--- | :--- | :--- | :--- |
| **Lock Manager** | `lock-1` s/d `lock-3` | `8001-8003` | Raft |
| **Queue Node** | `queue-1` s/d `queue-3` | `8004-8006` | Consistent Hashing |
| **Cache Node** | `cache-1` s/d `cache-3` | `8007-8009` | MESI |
| **State Store** | `redis-state` | `6379` | Redis Persistence |

---

## Cara Menjalankan

### 1. Menjalankan Cluster
Pastikan Anda berada di direktori akar proyek:
```bash
docker-compose up --build -d
```
Tunggu hingga semua container berstatus `(healthy)`. Cek dengan `docker ps`.

### 2. Menjalankan Unit & Integration Tests (Pytest)
Sistem memiliki skenario pengujian komprehensif untuk memverifikasi Raft, Hashing, dan MESI.
```bash
pip install -r requirements.txt
python -m pytest
```

### 3. Menjalankan Benchmark (Locust)
Gunakan Locust untuk melihat grafik performa secara real-time:
```bash
locust -f benchmarks/load_test_scenarios.py
```
Akses Dashboard di `http://localhost:8089` (Isian `Host` pada web UI bisa dikosongkan karena script sudah menerapkan *Client-Side Load Balancing* secara hardcoded).

---

## Dokumentasi Lanjut
*   **[Architecture Deep-Dive](docs/architecture.md):** Detail algoritma Mermaid.js dan diagram urutan.
*   **[Deployment Guide](docs/deployment_guide.md):** Panduan instalasi dan penggunaan API (curl).
*   **[Testing Protocol](tests/):** Unit testing dan integrasi.

---
