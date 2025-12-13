MODEL (
  name datamodelling.customer_rfm,
  kind FULL,
  owner 'vincent'
);

-- Tentukan tanggal hari ini/maksimum data (misalnya, 1 hari setelah transaksi terakhir)
WITH max_date AS (
    SELECT MAX(transaction_at) AS last_txn_date
    FROM datamodelling.transactions_net_revenue
),

rfm_data AS (
    SELECT
        t.customer_id,
        -- Recency (R): Hari sejak transaksi terakhir
        DATE_DIFF('day', MAX(t.transaction_at), (SELECT last_txn_date FROM max_date)) AS recency,
        -- Frequency (F): Jumlah total transaksi
        COUNT(DISTINCT t.transaction_id) AS frequency,
        -- Monetary (M): Total pendapatan bersih yang dihasilkan
        SUM(t.net_revenue) AS monetary
    FROM
        datamodelling.transactions_net_revenue AS t
    GROUP BY 1
)

SELECT
    r.*,
    c.acquisition_channel, -- Ambil channel akuisisi dari data customer
    c.loyalty_tier
FROM
    rfm_data AS r
JOIN
    datamarketingclean.customers AS c
    ON r.customer_id = c.customer_id