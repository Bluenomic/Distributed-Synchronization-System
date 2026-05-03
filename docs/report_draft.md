# DRAF LAPORAN AKHIR (Pindahkan ke PDF)

## 1. Ringkasan Sistem
Sistem Sinkronisasi Terdistribusi ini mengimplementasikan tiga pilar utama: konsensus (Raft), distribusi beban (Consistent Hashing), dan koherensi data (MESI).

## 2. Arsitektur Teknis
*   **Distributed Lock (Raft):** Menggunakan algoritma Raft untuk menjamin Linearizability. Dilengkapi dengan *Wait-for-Graph* untuk deteksi siklus deadlock.
*   **Distributed Queue (Consistent Hashing):** Pesan didistribusikan menggunakan hash ring untuk memastikan scalability. Mendukung *At-least-once delivery* dengan persistent ACK di Redis.
*   **Distributed Cache (MESI):** Protokol snooping di atas bus virtual (HTTP broadcast) untuk menjaga integritas data antar cache node.
*   **Security:** Implementasi RBAC (Role-Based Access Control) pada level endpoint API.

## 3. Analisis Performa
(Gunakan data dari folder `docs/benchmarks/` untuk mengisi bagian ini)

### **A. Throughput**
*   Lock Acquisition: ~XX ops/sec
*   Queue Enqueue/Dequeue: ~YY ops/sec

### **B. Latency**
*   Rata-rata latensi Raft Consensus: ZZ ms (terpengaruh oleh network hop antar node).

## 4. Perbandingan Single-Node vs Distributed
*   **Single-Node:** Latensi rendah, tapi memiliki *Single Point of Failure*.
*   **Distributed:** Latensi lebih tinggi (overhead konsensus), namun memiliki *High Availability* dan data durability yang jauh lebih kuat.

## 5. Kesimpulan & Tantangan
Tantangan terbesar adalah menangani *race condition* saat pemilihan leader Raft dan memastikan sinyal invalidasi MESI sampai ke seluruh node yang aktif tanpa kehilangan paket.
