MODEL (
  name datamodelling.transactions_net_revenue,
  kind FULL,
  owner 'vincent'
);

SELECT
  transaction_id,
  customer_id,
  product_id,
  timestamp AS transaction_at,
  gross_revenue,
  discount_applied,
  -- Hitung Net Revenue: Gross - (Gross * Discount)
  gross_revenue * (1 - discount_applied) AS net_revenue
FROM
  datamarketingclean.transactions
WHERE
  refund_flag = 0 -- Hanya sertakan transaksi yang tidak di-refund
  AND gross_revenue > 0 -- Pastikan pendapatan positif