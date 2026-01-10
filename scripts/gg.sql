-- 1. VIEW RAW DATA (MySQL)
-- Run on mysql_staging
SELECT * FROM raw_flight_data LIMIT 10;

-- 2. VIEW CLEAN DATA (MySQL)
-- Run on mysql_staging
SELECT * FROM clean_flight_data LIMIT 10;

-- 3. VIEW AGGREGATED KPIS (MySQL Staging)
-- Run on mysql_staging
SELECT * FROM staging_kpis LIMIT 10;

-- 4. VIEW FINAL ANALYTICS (Postgres)
-- Run on postgres_analytics
-- (Cannot run directly in this file if using MySQL client, but here is the query)
-- SELECT * FROM flight_kpis LIMIT 10;