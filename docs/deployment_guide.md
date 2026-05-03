# Deployment Guide - Distributed Synchronization System

## 1. Prasyarat (Prerequisites)
*   **Docker** & **Docker Compose** installed.
*   **Python 3.9+** (Jika ingin menjalankan pengujian secara lokal).
*   **Redis** (Sudah termasuk dalam Docker Compose).

## 2. Cara Menjalankan Cluster
1.  Buka terminal di folder akar proyek.
2.  Jalankan perintah berikut untuk membangun dan menyalakan semua node:
    ```bash
    docker-compose up --build
    ```
3.  Sistem akan menyalakan 4 container:
    *   `redis`: State persistence.
    *   `lock-manager`: Node utama pengelola Lock (Port 8001).
    *   `queue-node`: Node utama pengelola Queue (Port 8002).
    *   `cache-node`: Node utama pengelola Cache (Port 8003).

## 3. Cara Menggunakan API
Sistem ini menggunakan **RBAC Security**. Setiap request harus menyertakan header `X-Role`.

### **A. Distributed Lock**
*   **Acquire Lock:**
    ```bash
    curl -X POST http://localhost:8001/lock/acquire \
         -H "X-Role: admin" \
         -H "Content-Type: application/json" \
         -d '{"resource_id": "file_1", "client_id": "client_A", "type": "exclusive"}'
    ```

### **B. Distributed Queue**
*   **Enqueue:**
    ```bash
    curl -X POST http://localhost:8002/queue/enqueue \
         -H "X-Role: producer" \
         -H "Content-Type: application/json" \
         -d '{"topic": "orders", "message": "payload_data"}'
    ```

### **C. Distributed Cache (MESI)**
*   **Put Cache:**
    ```bash
    curl -X POST http://localhost:8003/cache/config_1 \
         -H "X-Role: admin" \
         -H "Content-Type: application/json" \
         -d '{"value": "12345"}'
    ```

## 4. Menambah Node Baru (Dynamic Scaling)
Jika Anda menyalakan node baru secara manual, Anda bisa menggabungkannya ke cluster tanpa restart:
```bash
curl -X POST http://localhost:8001/cluster/join \
     -d '{"node_url": "http://node-baru:8000"}'
```

## 5. Menjalankan Benchmark Performa
Skrip otomatisasi telah disediakan untuk mengumpulkan metrik:
```bash
python tests/performance/run_benchmarks.py
```
Hasil akan disimpan di `docs/benchmarks/` dalam format CSV.
