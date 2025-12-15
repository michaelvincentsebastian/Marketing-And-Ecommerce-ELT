MODEL (
  name datamarketingclean.events,
  kind FULL,
  owner 'events'
);

SELECT
  event_id,
  timestamp,
  customer_id,
  session_id,
  event_type,
  product_id,
  device_type,
  traffic_source,
  campaign_id,
  page_category,
  session_duration_sec,
  experiment_group
FROM
  raw.events
WHERE
  event_id IS NOT NULL
  AND timestamp IS NOT NULL
  AND customer_id IS NOT NULL
  AND event_type IS NOT NULL AND event_type <> '' -- Memastikan jenis event tercatat