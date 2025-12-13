MODEL (
  name datamarketingseeds.events,

  kind SEED (
    path 'C:/Users/Asus/Downloads/Data Andalan Utama (Intern)/Marketing & E-Commerce Analytics/rawData/events.csv'
  ),

  columns (
    event_id INT,
    timestamp timestamp,
    customer_id INT,
    session_id INT,
    event_type VARCHAR,
    product_id INT,
    device_type VARCHAR,
    traffic_source VARCHAR,
    campaign_id INT,
    page_category VARCHAR,
    session_duration_sec FLOAT,
    experiment_group VARCHAR
  ),

  grain (event_id, customer_id, session_id, product_id, campaign_id) -- satu baris dari data mewakili 1 product_id
);
  