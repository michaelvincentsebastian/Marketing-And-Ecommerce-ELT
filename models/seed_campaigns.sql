MODEL (
  name datamarketingseeds.campaigns,

  kind SEED (
    path '../seeds/campaigns.csv'
  ),

  columns (
    campaign_id INT,
    channel VARCHAR,
    objective VARCHAR,
    start_date DATE,
    end_date DATE,
    target_segment VARCHAR,
    expected_uplift FLOAT
  ),

  grain (campaign_id) -- satu baris dari data mewakili 1 product_id
);
  