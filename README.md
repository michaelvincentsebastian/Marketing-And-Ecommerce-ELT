# 🚀 Marketing & E-Commerce Data Pipeline: Modern ETL/ELT Architecture

![Data Pipeline](https://img.shields.io/badge/Pipeline-ETL%20%2F%20ELT-blue)
![Database](https://img.shields.io/badge/Database-DuckDB-orange)
![Orchestration](https://img.shields.io/badge/Orchestration-SQLMesh-green)
![Data Sharing](https://img.shields.io/badge/Sharing-Apache%20Arrow-red)

## 📌 1. Pendahuluan
Proyek ini mengimplementasikan **Data Pipeline** ujung-ke-ujung (end-to-end) yang dirancang untuk mengelola data pemasaran dan e-commerce. Sistem ini mentransformasi data mentah (CSV) menjadi wawasan bisnis yang siap pakai melalui arsitektur Lakehouse modern. 

Poin unik dari proyek ini adalah penggunaan **Apache Arrow Flight** sebagai protokol distribusi data, yang memungkinkan Analyst menarik data dalam hitungan milidetik tanpa *overhead* serialisasi tradisional.



---

## 💼 2. Business Case & Analisis Strategis
Proyek ini bertujuan untuk mengoptimalkan alokasi anggaran pemasaran dengan menganalisis performa setiap *channel* melalui 4 metrik utama:

| Metrik | Tujuan Analisis |
| :--- | :--- |
| **Total Net Revenue** | Mengukur profitabilitas nyata setelah diskon. |
| **Total Transactions** | Mengukur volume penetrasi pasar dan skalabilitas. |
| **Avg Discount Rate** | Mengukur efisiensi biaya (seberapa "mahal" kita menarik pelanggan). |
| **Refund Rate** | Indikator kualitas target audiens dan kepuasan pelanggan. |



---

## 🛠️ 3. Tech Stack
| Name | Description | Functions |
| :--- | :--- | :--- |
| **Python** | High-level programming | Ingestion script, Flight API, & Migration logic. |
| **SQL** | Structured Query Language | Data cleaning & business logic modeling. |
| **DuckDB** | In-process OLAP DB | Core compute engine & analytical storage. |
| **SQLMesh** | Data Transformation | Model management, dependency tracking, & auditing. |
| **Apache Arrow** | Columnar Memory Format | High-performance data sharing via Arrow Flight. |
| **Ducklake** | Lakehouse Extension | Syncing local database to S3/MinIO. |
| **MinIO** | Object Storage | Data Lake storage (Running via Docker). |
| **Docker** | Containerization | Infrastructure isolation for MinIO. |

---

## 🔄 4. Alur Kerja Pipeline (Flow)

### A. Extraction & Ingestion
Mengekstrak data mentah dari file CSV dan memuatnya ke dalam skema `raw` di DuckDB untuk pemrosesan awal.
- **Script:** `csvToDuckDB.py`

### B. Transformation (SQLMesh)
Menggunakan SQLMesh untuk mengelola siklus hidup data melalui dua lapisan:
1. **Staging Layer:** Membersihkan nilai NULL, string kosong, dan validasi tipe data.
2. **Mart Layer:** Menggabungkan tabel kampanye dan transaksi untuk menghasilkan model `campaign_performance`.



### C. Lakehouse Migration
Sinkronisasi data matang ke **MinIO** dalam format **Parquet**. Hal ini memastikan data tersimpan secara terdistribusi dan siap untuk skalabilitas besar (Big Data).

### D. Data Distribution (Arrow Flight)
Penyediaan data melalui API berperforma tinggi. Flight Server mengeksekusi kueri langsung pada memori DuckDB dan mengirimkan *stream* biner Arrow ke klien.



---

## 📂 5. Struktur Proyek
```text
.
├── arrowFlight/            # Mekanisme Distribusi Data
│   ├── data/               # Output Parquet untuk Analyst
│   ├── flight_server.py    # Server API Middle-man
│   └── flight_client.py    # Client/Analyst Data Fetcher
├── models/                 # Logika SQLMesh (SQL)
│   ├── staging/            # Layer Pembersihan (Clean)
│   └── marts/              # Layer Bisnis (Modelling)
├── notebooks/              # Migrasi Ducklake & EDA
├── rawData/                # Sumber Data CSV Mentah
├── config.yaml             # Konfigurasi Gateway SQLMesh
└── csvToDuckDB.py          # Skrip Ingestion Otomatis

```

---

## ⚙️ 6. Setup & Instalasi

1. Install Library:
```Bash
pip install sqlmesh[duckdb] duckdb pyarrow pandas
```

2. Jalankan Ingestion:
```Bash
python csvToDuckDB.py
```

3. Terapkan Transformasi:
```Bash
sqlmesh plan
sqlmesh apply
```

4. Aktifkan Data Service:
```Bash
python arrowFlight/flight_server.py
```

5. Request Data (Analyst Side):
```Bash
python arrowFlight/flight_client.py
```

---

## 💡 7. Kesimpulan
Pipeline ini bukan sekadar alat ETL, melainkan solusi Modern Data Stack yang mandiri. Dengan mengintegrasikan SQLMesh untuk kontrol kualitas data dan Apache Arrow untuk kecepatan akses, organisasi dapat mengambil keputusan bisnis berbasis data 90% lebih cepat dibandingkan metode manual.
