-- Data Quality Verification Queries for PostgreSQL
-- Run these queries to verify new data and check for duplicates

-- 1. Check total records loaded per day
SELECT 
    loaded_at::DATE as load_date,
    COUNT(*) as total_records
FROM fact_flights
GROUP BY loaded_at::DATE
ORDER BY load_date DESC;

-- 2. Check for potential duplicates using the business key
-- (Should return 0 rows if deduplication is working)
SELECT
    date_id,
    airline_id,
    departure_time,
    total_fare_bdt,
    COUNT(*) as duplicate_count
FROM fact_flights
GROUP BY
    date_id,
    airline_id,
    departure_time,
    total_fare_bdt
HAVING
    COUNT(*) > 1;

-- 3. View records loaded today
SELECT 
    COUNT(*) as records_loaded_today
FROM fact_flights
WHERE loaded_at::DATE = CURRENT_DATE;

-- 4. Compare record counts between MySQL staging and PostgreSQL analytics
-- Run this in MySQL first:
-- SELECT COUNT(*) as mysql_count FROM fact_flights;

-- Then run this in PostgreSQL:
SELECT COUNT(*) as postgres_count FROM fact_flights;

-- 5. Check dimension table sizes
SELECT 'dim_date' as table_name, COUNT(*) as record_count
FROM dim_date
UNION ALL
SELECT 'dim_airline', COUNT(*)
FROM dim_airline
UNION ALL
SELECT 'dim_location', COUNT(*)
FROM dim_location
UNION ALL
SELECT 'dim_flight_details', COUNT(*)
FROM dim_flight_details
UNION ALL
SELECT 'fact_flights', COUNT(*)
FROM fact_flights;

-- 6. View latest load statistics
SELECT 
    loaded_at::DATE as load_date,
    MIN(loaded_at) as first_load_time,
    MAX(loaded_at) as last_load_time,
    COUNT(*) as records_in_batch
FROM fact_flights
GROUP BY loaded_at::DATE
ORDER BY load_date DESC
LIMIT 10;

-- 7. Check for orphaned records (should return 0 if foreign keys are intact)
SELECT COUNT(*) as orphaned_records
FROM fact_flights f
WHERE
    NOT EXISTS (
        SELECT 1
        FROM dim_date d
        WHERE
            d.date_id = f.date_id
    )
    OR NOT EXISTS (
        SELECT 1
        FROM dim_airline a
        WHERE
            a.airline_id = f.airline_id
    )
    OR NOT EXISTS (
        SELECT 1
        FROM dim_location l
        WHERE
            l.location_id = f.source_location_id
    )
    OR NOT EXISTS (
        SELECT 1
        FROM dim_location l
        WHERE
            l.location_id = f.destination_location_id
    )
    OR NOT EXISTS (
        SELECT 1
        FROM dim_flight_details fd
        WHERE
            fd.detail_id = f.detail_id
    );