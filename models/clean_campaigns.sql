MODEL (
  name datamarketingclean.campaigns,
  kind FULL,
  owner 'vincent'
);

SELECT
  campaign_id,
  channel,
  objective,
  start_date,
  end_date,
  target_segment,
  expected_uplift
FROM
  datamarketingseeds.campaigns
WHERE
  campaign_id IS NOT NULL -- Memastikan Kunci Utama ada
  AND start_date IS NOT NULL -- Memastikan tanggal mulai kampanye ada