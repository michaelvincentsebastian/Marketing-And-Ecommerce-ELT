MODEL (
  name datamarketingclean.transactions,
  kind FULL,
  owner 'vincent'
);

SELECT
  transaction_id,
  timestamp,
  customer_id,
  product_id,
  quantity,
  discount_applied,
  gross_revenue,
  campaign_id,
  refund_flag
FROM
  datamarketingseeds.transactions
WHERE
  transaction_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND product_id IS NOT NULL
  AND gross_revenue IS NOT NULL -- Memastikan pendapatan tercatat