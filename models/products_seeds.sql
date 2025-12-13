MODEL (
  name datamarketingseeds.products,

  kind SEED (
    path '../seeds/products.csv'
  ),

  columns (
    product_id INT,
    category VARCHAR,
    brand VARCHAR,
    base_price FLOAT,
    launch_date DATE,
    is_premium INT
  ),

  grain (product_id) -- satu baris dari data mewakili 1 product_id
);
  