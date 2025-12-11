MODEL (
    name staging_db.staging.eventsmobile,
    kind FULL, 
    owner 'vincent'
);

SELECT customer_id FROM ducklake_db.raw.events LIMIT 10;