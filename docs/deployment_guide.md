# Deployment & Usage Guide - Distributed Synchronization System

## 1. Prasyarat (Prerequisites)
*   **Docker** & **Docker Compose** v2.0+
*   **Python 3.11+** (Untuk menjalankan benchmark Locust secara lokal)
*   **RAM Minimal 4GB** (Untuk menjalankan 10 container secara bersamaan)

---

## 2. Cara Menjalankan Cluster (10 Nodes)
1.  Buka terminal di folder akar proyek.
2.  Bangun dan nyalakan seluruh layanan (10 container):
    ```bash
    docker-compose up --build -d
    ```
3.  Tunggu sekitar 10-15 detik hingga sistem melakukan pemilihan Leader (Raft).
4.  Cek status kesehatan node:
    ```bash
    docker ps
    ```
    Pastikan semua container memiliki status `(healthy)`.

### Port Mapping Cluster
| Service | Nodes | Host Ports |
| :--- | :--- | :--- |
| **Lock Manager (Raft)** | lock-1, lock-2, lock-3 | 8001, 8002, 8003 |
| **Queue Nodes (Hashing)** | queue-1, queue-2, queue-3 | 8004, 8005, 8006 |
| **Cache Nodes (MESI)** | cache-1, cache-2, cache-3 | 8007, 8008, 8009 |
| **State Store** | redis | 6379 |

---

## 3. Panduan Penggunaan API

Sistem menggunakan **RBAC Security**. Sertakan header `X-Role: admin` (atau `producer`/`consumer`/`reader`) pada setiap request.

### **A. Distributed Lock (Raft)**
Gunakan salah satu port (8001-8003). Jika node bukan Leader, sistem akan mengarahkan (307 Redirect) ke Leader.
```bash
curl -X POST http://localhost:8001/lock/acquire \
     -H "X-Role: admin" \
     -H "Content-Type: application/json" \
     -d '{"resource_id": "file_1", "client_id": "user_A", "type": "exclusive"}'
```

### **B. Distributed Queue (Consistent Hashing)**
Request bisa dikirim ke node mana saja (8004-8006). Sistem akan otomatis me-route ke node yang bertanggung jawab atas topik tersebut.
```bash
# Enqueue
curl -X POST http://localhost:8004/queue/enqueue \
     -H "X-Role: producer" \
     -H "Content-Type: application/json" \
     -d '{"topic": "tasks", "message": "payload_01"}'

# Dequeue
curl -X GET http://localhost:8005/queue/dequeue/tasks \
     -H "X-Role: consumer"
```

### **C. Distributed Cache (MESI)**
Menjaga konsistensi cache di node 8007, 8008, dan 8009 secara otomatis.
```bash
curl -X POST http://localhost:8007/cache/user_1 \
     -H "X-Role: admin" \
     -H "Content-Type: application/json" \
     -d '{"value": "active"}'
```

---

## 4. Verifikasi Keamanan & Byzantine Fault Tolerance

### **A. Mengecek Audit Log**
Semua transaksi penting dicatat di file lokal (jika tidak menggunakan volume) atau di dalam container:
```bash
docker exec lock-1 cat data/audit.log
```

### **B. Simulasi Byzantine (PBFT)**
Node PBFT akan memverifikasi tanda tangan digital antar-node. Jika ada node yang memanipulasi pesan, kuorum $2f+1$ akan menolak transaksi tersebut.

---

## 5. Menjalankan Benchmark (Locust)
Untuk memvisualisasikan performa sistem dalam grafik:
1.  Instal locust secara lokal: `pip install locust requests`
2.  Jalankan Locust:
    ```bash
    locust -f benchmarks/load_test_scenarios.py
    ```
3.  Buka browser di `http://localhost:8089`.
4.  Masukkan Host: `http://localhost:8001` (atau port node lainnya).
5.  Lihat grafik **RPS** dan **Latency** secara real-time.

---

## 6. Troubleshooting
*   **Node Unhealthy:** Jika node `unhealthy`, Docker akan otomatis me-restart. Cek log dengan `docker logs lock-1`.
*   **Raft No Leader:** Jika terjadi pemilihan leader yang gagal, tunggu 5 detik agar timeout memicu pemilihan baru.
*   **Redis Down:** Jika Redis mati, antrean pesan akan berhenti berfungsi. Pastikan container `redis-state` berjalan.
