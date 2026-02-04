-- ETL Star Schema SQL Script (MySQL Version)
-- Assumes source data is in table 'clean_flight_data'

-- 1. Populate Date Dimension
INSERT IGNORE INTO
    dim_date (
        date,
        day,
        month,
        year,
        day_of_week,
        season,
        is_holiday_window
    )
SELECT DISTINCT
    CAST(departure_date_time AS DATE) as date,
    EXTRACT(
        DAY
        FROM departure_date_time
    ) as day,
    EXTRACT(
        MONTH
        FROM departure_date_time
    ) as month,
    EXTRACT(
        YEAR
        FROM departure_date_time
    ) as year,
    DAYNAME(departure_date_time) as day_of_week,
    CASE
    -- Holiday Logic Check
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2022-04-30' AND '2022-05-07'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2022-07-07' AND '2022-07-14'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2023-04-19' AND '2023-04-26'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2023-06-26' AND '2023-07-03'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2024-04-08' AND '2024-04-15'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2024-06-14' AND '2024-06-21'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2025-03-28' AND '2025-04-04'  THEN 'Peak: Eid Holiday'
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2025-06-04' AND '2025-06-11'  THEN 'Peak: Eid Holiday'
        -- Standard Seasons (Approximation)
        WHEN EXTRACT(
            MONTH
            FROM departure_date_time
        ) IN (11, 12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(
            MONTH
            FROM departure_date_time
        ) IN (3, 4, 5) THEN 'Spring/Pre-Monsoon'
        ELSE 'Monsoon/Summer'
    END as season,
    CASE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2022-04-30' AND '2022-05-07'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2022-07-07' AND '2022-07-14'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2023-04-19' AND '2023-04-26'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2023-06-26' AND '2023-07-03'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2024-04-08' AND '2024-04-15'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2024-06-14' AND '2024-06-21'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2025-03-28' AND '2025-04-04'  THEN TRUE
        WHEN CAST(departure_date_time AS DATE) BETWEEN '2025-06-04' AND '2025-06-11'  THEN TRUE
        ELSE FALSE
    END as is_holiday_window
FROM clean_flight_data;

-- 2. Populate Airline Dimension
INSERT IGNORE INTO
    dim_airline (airline_name)
SELECT DISTINCT
    airline
FROM clean_flight_data;

-- 3. Populate Location Dimension
-- MySQL doesn't support WITH syntax in older versions as easily or clean UNION inside INSERT sometimes,
-- but standard INSERT INTO ... SELECT ... UNION is fine.
INSERT IGNORE INTO
    dim_location (airport_code, city_name)
SELECT source, source_name
FROM clean_flight_data
UNION
SELECT destination, destination_name
FROM clean_flight_data;

-- 4. Populate Flight Details Dimension
INSERT IGNORE INTO
    dim_flight_details (
        class,
        stopovers,
        aircraft_type
    )
SELECT DISTINCT
    class,
    stopovers,
    aircraft_type
FROM clean_flight_data;

-- 5. Populate Fact Table
INSERT INTO
    fact_flights (
        date_id,
        airline_id,
        source_location_id,
        destination_location_id,
        detail_id,
        departure_time,
        arrival_time,
        duration_hrs,
        days_before_departure,
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt
    )
SELECT d.date_id, a.airline_id, loc_src.location_id, loc_dest.location_id, det.detail_id, s.departure_date_time, s.arrival_date_time, s.duration_hrs, s.days_before_departure, s.base_fare_bdt, s.tax_surcharge_bdt, s.total_fare_bdt
FROM
    clean_flight_data s
    LEFT JOIN dim_date d ON CAST(s.departure_date_time AS DATE) = d.date
    LEFT JOIN dim_airline a ON s.airline = a.airline_name
    LEFT JOIN dim_location loc_src ON s.source = loc_src.airport_code
    LEFT JOIN dim_location loc_dest ON s.destination = loc_dest.airport_code
    LEFT JOIN dim_flight_details det ON s.class = det.class
    AND s.stopovers = det.stopovers
    AND s.aircraft_type = det.aircraft_type;