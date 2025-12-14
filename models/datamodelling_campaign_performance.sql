MODEL (
  -- Nama model yang akan dibuat di skema datamodelling Anda
  name datamodelling.campaign_performance,

  -- Tipe model: FULL (dibuat ulang setiap kali data sumber berubah)
  kind FULL,

  -- Definisi Primary Key (Grain)
  -- Karena kita mengagregasi berdasarkan objective, objective adalah grain-nya
  grain (objective)
);

-- Kueri SQL untuk menghasilkan data performa kampanye
SELECT
    -- 1. Dimensi Utama
    c.objective AS objective,
    c.channel AS channel,
    
    -- 2. Metrik Kunci (Profitabilitas & Sales)
    -- METRIK 1: Total Net Revenue (Pendapatan Bersih)
    -- Formula: Gross Revenue * (1 - Discount Applied)
    SUM(t.gross_revenue * (1 - t.discount_applied)) AS total_net_revenue,
    
    -- METRIK 2: Total Transactions (Sales Count)
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    
    -- 3. Metrik Efisiensi & Kualitas
    -- METRIK 3: Average Discount Rate (%)
    -- Rata-rata diskon yang diberikan (Biaya Akuisisi)
    AVG(t.discount_applied) * 100 AS avg_discount_rate_pct,
    
    -- METRIK 4: Refund Rate (Tingkat Pengembalian)
    -- Persentase transaksi yang di-refund (Kualitas Sales)
    (SUM(t.refund_flag) * 1.0) / COUNT(t.transaction_id) AS refund_rate

FROM
    -- Tabel Sumber 1: Kampanye
    datamarketingseeds.campaigns c
JOIN
    -- Tabel Sumber 2: Transaksi
    datamarketingseeds.transactions t 
    -- Menggabungkan berdasarkan ID Kampanye yang sama
    ON c.campaign_id = t.campaign_id
GROUP BY
    -- Agregasi berdasarkan Objective dan Channel untuk analisis yang mendalam
    c.objective,
    c.channel
ORDER BY
    -- Urutkan berdasarkan Net Revenue tertinggi untuk memudahkan peninjauan
    total_net_revenue DESC;