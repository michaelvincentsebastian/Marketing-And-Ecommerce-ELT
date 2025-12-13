MODEL (
  name datamarketingseeds.transactions,

  kind SEED (
    path '../seeds/transactions.csv'
  ),

  columns (
    transaction_id INT,
    timestamp timestamp,
    customer_id INT,
    product_id INT,
    quantity INT,
    discount_applied FLOAT,
    gross_revenue FLOAT,
    campaign_id INT,
    refund_flag INT
  ),

  grain (transaction_id, customer_id, product_id, campaign_id) -- satu baris dari data mewakili 1 product_id
);
  