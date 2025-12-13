MODEL (
  name datamarketingseeds.customers,

  kind SEED (
    path '../seeds/customers.csv'
  ),

  columns (
    customer_id INT,
    signup_date DATE,
    country VARCHAR,
    age INT,
    gender VARCHAR,
    loyalty_tier VARCHAR,
    acquisition_channel VARCHAR
  ),

  grain (customer_id) -- satu baris dari data mewakili 1 product_id
);
  