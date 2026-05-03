# Deployment & Usage Guide - Distributed Synchronization System

## 1. Prasyarat (Prerequisites)
*   **Docker** & **Docker Compose** v2.0+
*   **Python 3.11+**
*   **RAM Minimal 4GB** (Untuk menjalankan 10 container secara bersamaan)

---

## 2. Persiapan Lingkungan (Setup)
Sebelum menjalankan kluster, instal dependensi Python yang diperlukan untuk benchmark dan testing:
```bash
pip install -r requirements.txt
```

---

## 3. Cara Menjalankan Cluster (10 Nodes)
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

## 4. Panduan Penggunaan API

Sistem menggunakan **RBAC Security**. Sertakan header `X-Role: admin` (atau `producer`/`consumer`/`reader`) pada setiap request.

### **A. Distributed Lock (Raft)**
Gunakan salah satu port (8001-8003). Jika node bukan Leader, sistem akan mengarahkan (307 Redirect) ke Leader. Gunakan flag `-L` di curl untuk mengikuti redirect otomatis.
```bash
curl -sL -X POST http://localhost:8001/lock/acquire \
     -H "X-Role: admin" \
     -H "Content-Type: application/json" \
     -d '{"resource_id": "file_1", "client_id": "user_A", "type": "exclusive"}'
```

### **B. Distributed Queue (Consistent Hashing)**
Request bisa dikirim ke node mana saja (8004-8006). Sistem akan otomatis me-route ke node yang bertanggung jawab atas topik tersebut.
```bash
# Enqueue
curl -sL -X POST http://localhost:8004/queue/enqueue \
     -H "X-Role: producer" \
     -H "Content-Type: application/json" \
     -d '{"topic": "tasks", "message": "payload_01"}'

# Dequeue
curl -sL -X GET http://localhost:8005/queue/dequeue/tasks \
     -H "X-Role: consumer"
```

---

## 5. Menjalankan Benchmark (Locust)
Untuk memvisualisasikan performa sistem dalam grafik:
1.  Pastikan dependensi sudah terinstall (lihat Poin 2).
2.  Jalankan Locust:
    ```bash
    locust -f benchmarks/load_test_scenarios.py
    ```
3.  Buka browser di [http://localhost:8089](http://localhost:8089).
4.  Masukkan Host: `http://localhost:8001` (atau port node lainnya).
5.  Lihat grafik **RPS** dan **Latency** secara real-time.

---

## 6. Verifikasi Keamanan & Audit Log
Semua transaksi penting dicatat di file lokal di dalam container:
```bash
docker exec lock-1 cat data/audit.log
```
Log ini mencatat Digital Signatures (HMAC-SHA256) untuk verifikasi PBFT dan status otorisasi RBAC.
