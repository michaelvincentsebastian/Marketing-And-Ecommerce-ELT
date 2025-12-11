# Dapodik-ELT-DataPipeline

url : https://www.kaggle.com/datasets/geethasagarbonthu/marketing-and-e-commerce-analytics-dataset

# Deskripsi Dataset Analisis E-commerce dan Pemasaran (Terjemahan Bahasa Indonesia)

Dataset ini adalah dataset sintetik multi-tabel yang realistis, dirancang untuk analisis pemasaran, pengujian A/B, pemodelan peningkatan (uplift modeling), dan analisis produk. Ini mensimulasikan lingkungan analitik lengkap perusahaan e-commerce modern, termasuk pelanggan, produk, kampanye pemasaran, peristiwa interaksi pengguna, dan transaksi pembelian.

Dataset ini berisi jutaan interaksi tingkat peristiwa (event-level interactions) di seluruh penelusuran (browsing), klik (clicking), penambahan ke keranjang (carting), dan pembelian. Dataset ini juga mencakup paparan kampanye dan penugasan eksperimen, menjadikannya ideal untuk praktik langsung dengan:
* Analisis Data Eksplorasi (EDA)
* Pembersihan data dan rekayasa fitur (feature engineering)
* Analisis saluran (funnel) dan perilaku
* Pengujian A/B dan pemodelan peningkatan (uplift modeling)
* Penggabungan multi-tabel bergaya SQL (SQL-style multi-table joins)
* Analisis pendapatan, pengembalian dana (refund), dan diskon
* Pola deret waktu dan musiman (seasonality)
* Segmentasi pelanggan dan analisis kinerja produk

Pembelian dalam dataset mengikuti model peningkatan probabilistik (probabilistic uplift model), dipengaruhi oleh paparan kampanye, grup eksperimen, tingkat loyalitas (loyalty tier), sumber lalu lintas (traffic source), tren musiman, dan efek akhir pekan — memungkinkan pengguna untuk menjalankan uji statistik yang bermakna alih-alih bekerja dengan noise acak.

Dataset ini sepenuhnya sintetik, dihasilkan secara terprogram menggunakan Python, dan tidak mengandung informasi pelanggan atau bisnis nyata. Ini dimaksudkan sebagai sumber belajar bagi analis, ilmuwan data (data scientists), pelajar, dan siapa saja yang mempraktikkan analisis pemasaran dan e-commerce.

---

## Detail File CSV

**campaigns.csv**:
- Berisi informasi rinci tentang setiap kampanye pemasaran.
- Mencakup saluran kampanye, tujuan, tanggal mulai/akhir, dan segmen target.
- expected_uplift memengaruhi perilaku pembelian dalam events dataset.
- Mendukung analisis kinerja dan atribusi kampanye.
- Kunci utama (Primary key) campaign_id terhubung ke events dan transactions.

**customers.csv**:
- Berisi satu baris per pelanggan dengan informasi demografi dan profil.
- Mencakup negara, usia, jenis kelamin, dan signup_date untuk segmentasi dan analisis kohort (cohort analysis).
- Melacak nilai pelanggan melalui loyalty_tier (Perunggu → Platinum).
- Menunjukkan bagaimana setiap pelanggan memasuki platform via acquisition_channel.
- Kunci utama (Primary key) customer_id terhubung ke events dan transactions.

**events.csv**:
- Tabel fakta (fact table) besar berisi interaksi pengguna: views, clicks, add-to-cart, bounces, purchases.
- Mencakup metadata seperti device type, traffic source, page category, dan session duration.
- campaign_id dan experiment_group memungkinkan analisis peningkatan (uplift) dan pengujian A/B.
- Mengandung ketidakrapihan yang realistis: missing device types, inconsistent traffic-source casing.
- Kunci utama (Primary key) event_id, dengan kunci asing (foreign keys) ke customers, products, dan campaigns.

**products.csv**:
- Satu baris per produk dalam katalog dengan metadata kategori dan merek (brand).
- base_price values mencerminkan distribusi harga yang realistis berdasarkan kategori.
- Mencakup waktu peluncuran produk untuk menganalisis siklus hidup produk.
- is_premium mengidentifikasi segmen produk dengan harga lebih tinggi.
- Kunci utama (Primary key) product_id terhubung ke events dan transactions.

**transactions.csv**:
- Berisi satu baris per transaksi pembelian yang diselesaikan.
- Mencakup quantity, discounts applied, dan total gross revenue (dengan refunds sebagai nilai negatif).
- Terhubung langsung ke purchase events melalui timestamp dan customer/product IDs.
- Mendukung analisis pendapatan, perilaku pengembalian dana (refund behavior), dan studi efektivitas kampanye.
- Kunci utama (Primary key) transaction_id, merujuk ke customers, products, dan campaigns.