MODEL (
  name datamarketingclean.customers,
  kind FULL,
  owner 'vincent'
);

SELECT
  customer_id,
  signup_date,
  country,
  age,
  gender,
  loyalty_tier,
  acquisition_channel
FROM
  datamarketingseeds.customers
WHERE
  customer_id IS NOT NULL -- Memastikan Kunci Utama ada
  AND signup_date IS NOT NULL -- Memastikan tanggal penting ada
  AND country <> '' -- Memastikan negara bukan string kosong