MODEL (
  name datamarketingclean.products,
  kind FULL,
  owner 'vincent'
);

SELECT
  product_id,
  category,
  brand,
  base_price,
  launch_date,
  is_premium
FROM
  datamarketingseeds.products
WHERE
  product_id IS NOT NULL -- Memastikan Kunci Utama ada
  AND category IS NOT NULL AND category <> '' -- Memastikan kategori ada dan bukan string kosong
  AND brand IS NOT NULL AND brand <> ''       -- Memastikan brand ada dan bukan string kosong