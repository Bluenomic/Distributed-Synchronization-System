# Distributed Synchronization System

Tugas 3 - Sistem Paralel dan Terdistribusi

## Fitur Utama
- **Distributed Lock**: Implementasi algoritma Raft Consensus.
- **Distributed Queue**: Menggunakan Consistent Hashing dan Redis persistence.
- **Distributed Cache**: Protokol MESI dengan kebijakan LRU.
- **High Availability**: Pemilihan Leader otomatis jika salah satu node mati.

## Persiapan
1. Pastikan Docker dan Docker Compose sudah terinstal.
2. Clone repository ini.

## Cara Menjalankan
1. Masuk ke folder proyek.
2. Jalankan perintah:
   ```bash
   cd docker
   docker-compose up --build
   ```
3. Sistem akan menjalankan 10 container:
   - **Lock Manager 1-3** (Raft Cluster): `http://localhost:8001-8003`
   - **Queue Node 1-3** (Consistent Hashing Cluster): `http://localhost:8004-8006`
   - **Cache Node 1-3** (MESI Coherence Cluster): `http://localhost:8007-8009`
   - 1 Redis instance untuk persistence.

## Endpoint Pengujian
- Lock Manager 1: `http://localhost:8001`
- Lock Manager 2: `http://localhost:8002`
- Lock Manager 3: `http://localhost:8003`
- Queue Node 1: `http://localhost:8004`
- Queue Node 2: `http://localhost:8005`
- Queue Node 3: `http://localhost:8006`
- Cache Node 1: `http://localhost:8007`
- Cache Node 2: `http://localhost:8008`
- Cache Node 3: `http://localhost:8009`


Lihat folder `docs/` untuk dokumentasi arsitektur dan spesifikasi API lengkap.
