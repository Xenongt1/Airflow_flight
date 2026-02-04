import sqlalchemy
from sqlalchemy import create_engine, text

# Database Connections
# Airflow tasks will use service names (mysql_staging, postgres_analytics).

# MySQL Staging (Internal Docker Network)
mysql_engine = create_engine('mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging')

# Postgres Analytics (Internal Docker Network)
postgres_engine = create_engine('postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics')

def init_mysql():
    print("Creating MySQL table...")
    with mysql_engine.connect() as conn:
        # Drop table if exists to allow schema updates during development
        conn.execute(text("DROP TABLE IF EXISTS clean_flight_data;"))
        conn.execute(text("DROP TABLE IF EXISTS raw_flight_data;"))
        conn.execute(text("DROP TABLE IF EXISTS staging_kpis;")) 
        conn.execute(text("DROP TABLE IF EXISTS seasonal_kpis;"))
        conn.execute(text("DROP TABLE IF EXISTS route_kpis;"))
        
        # Drop Star Schema Tables (MySQL)
        conn.execute(text("DROP TABLE IF EXISTS fact_flights;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_flight_details;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_airline;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_location;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_date;"))
        
        # Raw Data Table
        conn.execute(text("""
            CREATE TABLE raw_flight_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                airline VARCHAR(255),
                source VARCHAR(10),
                source_name VARCHAR(255),
                destination VARCHAR(10),
                destination_name VARCHAR(255),
                departure_date_time DATETIME,
                arrival_date_time DATETIME,
                duration_hrs FLOAT,
                stopovers VARCHAR(50),
                aircraft_type VARCHAR(100),
                class VARCHAR(50),
                booking_source VARCHAR(100),
                base_fare_bdt FLOAT,
                tax_surcharge_bdt FLOAT,
                total_fare_bdt FLOAT,
                seasonality VARCHAR(50),
                days_before_departure INT
            );
        """))
        
        # Clean Data Table
        conn.execute(text("""
            CREATE TABLE clean_flight_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                airline VARCHAR(255),
                source VARCHAR(10),
                source_name VARCHAR(255),
                destination VARCHAR(10),
                destination_name VARCHAR(255),
                departure_date_time DATETIME,
                arrival_date_time DATETIME,
                duration_hrs FLOAT,
                stopovers VARCHAR(50),
                aircraft_type VARCHAR(100),
                class VARCHAR(50),
                booking_source VARCHAR(100),
                base_fare_bdt FLOAT,
                tax_surcharge_bdt FLOAT,
                total_fare_bdt FLOAT,
                seasonality VARCHAR(50),
                days_before_departure INT
            );
        """))

        # Staging KPIs Table
        conn.execute(text("""
            CREATE TABLE staging_kpis (
                airline VARCHAR(255),
                avg_price FLOAT,
                total_bookings INT
            );
        """))
        
        # Seasonal KPIs Table
        conn.execute(text("""
            CREATE TABLE seasonal_kpis (
                season VARCHAR(50),
                avg_price FLOAT,
                variation_from_overall FLOAT
            );
        """))

        # Route KPIs Table
        conn.execute(text("""
            CREATE TABLE route_kpis (
                source VARCHAR(100),
                destination VARCHAR(100),
                booking_count INT,
                avg_price FLOAT
            );
        """))
        
        # --- Star Schema Tables (MySQL Version) ---
        
        # 1. Dimension: Date
        conn.execute(text("""
            CREATE TABLE dim_date (
                date_id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE UNIQUE NOT NULL,
                day INT,
                month INT,
                year INT,
                day_of_week VARCHAR(20),
                season VARCHAR(50),
                is_holiday_window BOOLEAN
            );
        """))

        # 2. Dimension: Airline
        conn.execute(text("""
            CREATE TABLE dim_airline (
                airline_id INT AUTO_INCREMENT PRIMARY KEY,
                airline_name VARCHAR(255) UNIQUE NOT NULL
            );
        """))
        
        # 3. Dimension: Location (Airport/City)
        conn.execute(text("""
            CREATE TABLE dim_location (
                location_id INT AUTO_INCREMENT PRIMARY KEY,
                airport_code VARCHAR(10) UNIQUE NOT NULL,
                city_name VARCHAR(255)
            );
        """))

        # 4. Dimension: Flight Details (Junk Dimension)
        conn.execute(text("""
             CREATE TABLE dim_flight_details (
                 detail_id INT AUTO_INCREMENT PRIMARY KEY,
                 class VARCHAR(50),
                 stopovers VARCHAR(50),
                 aircraft_type VARCHAR(100),
                 CONSTRAINT unique_flight_details UNIQUE(class, stopovers, aircraft_type)
             );
        """))

        # 5. Fact Table
        conn.execute(text("""
            CREATE TABLE fact_flights (
                fact_id INT AUTO_INCREMENT PRIMARY KEY,
                date_id INT,
                airline_id INT,
                source_location_id INT,
                destination_location_id INT,
                detail_id INT,
                departure_time DATETIME,
                arrival_time DATETIME,
                duration_hrs FLOAT,
                days_before_departure INT,
                base_fare_bdt FLOAT,
                tax_surcharge_bdt FLOAT,
                total_fare_bdt FLOAT,
                FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
                FOREIGN KEY (airline_id) REFERENCES dim_airline(airline_id),
                FOREIGN KEY (source_location_id) REFERENCES dim_location(location_id),
                FOREIGN KEY (destination_location_id) REFERENCES dim_location(location_id),
                FOREIGN KEY (detail_id) REFERENCES dim_flight_details(detail_id)
            );
        """))
        
    print("MySQL tables created/reset.")

def init_postgres():
    print("Creating PostgreSQL table...")
    with postgres_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS flight_kpis CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS seasonal_kpis CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS route_kpis CASCADE;"))
        
        # Drop Star Schema Tables (CASCADE to drop dependent views)
        conn.execute(text("DROP TABLE IF EXISTS fact_flights CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_flight_details CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_airline CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_location CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_date CASCADE;"))

        # --- Existing KPI Tables (Preserved) ---
        conn.execute(text("""
            CREATE TABLE flight_kpis (
                kpi_id SERIAL PRIMARY KEY,
                airline VARCHAR(255),
                avg_price NUMERIC,
                total_bookings INT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        conn.execute(text("""
            CREATE TABLE seasonal_kpis (
                kpi_id SERIAL PRIMARY KEY,
                season VARCHAR(50),
                avg_price NUMERIC,
                variation_from_overall NUMERIC,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))

        conn.execute(text("""
            CREATE TABLE route_kpis (
                kpi_id SERIAL PRIMARY KEY,
                source VARCHAR(100),
                destination VARCHAR(100),
                booking_count INT,
                avg_price NUMERIC,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        # --- Star Schema Tables ---
        
        # 1. Dimension: Date
        conn.execute(text("""
            CREATE TABLE dim_date (
                date_id SERIAL PRIMARY KEY,
                date DATE UNIQUE NOT NULL,
                day INT,
                month INT,
                year INT,
                day_of_week VARCHAR(20),
                season VARCHAR(50),
                is_holiday_window BOOLEAN
            );
        """))

        # 2. Dimension: Airline
        conn.execute(text("""
            CREATE TABLE dim_airline (
                airline_id SERIAL PRIMARY KEY,
                airline_name VARCHAR(255) UNIQUE NOT NULL
            );
        """))
        
        # 3. Dimension: Location (Airport/City)
        conn.execute(text("""
            CREATE TABLE dim_location (
                location_id SERIAL PRIMARY KEY,
                airport_code VARCHAR(10) UNIQUE NOT NULL,
                city_name VARCHAR(255)
            );
        """))

        # 4. Dimension: Flight Details (Junk Dimension)
        conn.execute(text("""
             CREATE TABLE dim_flight_details (
                 detail_id SERIAL PRIMARY KEY,
                 class VARCHAR(50),
                 stopovers VARCHAR(50),
                 aircraft_type VARCHAR(100),
                 UNIQUE(class, stopovers, aircraft_type)
             );
        """))

        # 5. Fact Table (with Incremental Load Support)
        conn.execute(text("""
            CREATE TABLE fact_flights (
                fact_id SERIAL PRIMARY KEY,
                date_id INT REFERENCES dim_date(date_id),
                airline_id INT REFERENCES dim_airline(airline_id),
                source_location_id INT REFERENCES dim_location(location_id),
                destination_location_id INT REFERENCES dim_location(location_id),
                detail_id INT REFERENCES dim_flight_details(detail_id),
                departure_time TIMESTAMP,
                arrival_time TIMESTAMP,
                duration_hrs FLOAT,
                days_before_departure INT,
                base_fare_bdt NUMERIC,
                tax_surcharge_bdt NUMERIC,
                total_fare_bdt NUMERIC,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        
        # Create index for efficient time-based queries
        conn.execute(text("""
            CREATE INDEX idx_fact_flights_loaded_at ON fact_flights(loaded_at);
        """))

        # --- KPI Computed Views (Analytics) ---
        
        # 1. KPI: Average Fare by Airline
        conn.execute(text("DROP VIEW IF EXISTS kpi_airline_stats;"))
        conn.execute(text("""
            CREATE VIEW kpi_airline_stats AS
            SELECT 
                a.airline_name,
                COUNT(f.fact_id) as total_bookings,
                ROUND(AVG(f.total_fare_bdt), 2) as avg_fare
            FROM fact_flights f
            JOIN dim_airline a ON f.airline_id = a.airline_id
            GROUP BY a.airline_name;
        """))

        # 2. KPI: Seasonal Variation
        conn.execute(text("DROP VIEW IF EXISTS kpi_seasonal_variation;"))
        conn.execute(text("""
            CREATE VIEW kpi_seasonal_variation AS
            WITH SeasonStats AS (
                SELECT 
                    d.season,
                    ROUND(AVG(f.total_fare_bdt), 2) as avg_season_price
                FROM fact_flights f
                JOIN dim_date d ON f.date_id = d.date_id
                GROUP BY d.season
            ),
            OverallStats AS (
                SELECT AVG(total_fare_bdt) as overall_avg FROM fact_flights
            )
            SELECT 
                s.season,
                s.avg_season_price,
                ROUND(s.avg_season_price - o.overall_avg, 2) as variation_from_overall
            FROM SeasonStats s, OverallStats o;
        """))

        # 3. KPI: Popular Routes
        conn.execute(text("DROP VIEW IF EXISTS kpi_popular_routes;"))
        conn.execute(text("""
            CREATE VIEW kpi_popular_routes AS
            SELECT 
                src.city_name as source_city,
                dst.city_name as destination_city,
                COUNT(f.fact_id) as booking_count,
                ROUND(AVG(f.total_fare_bdt), 2) as avg_route_price
            FROM fact_flights f
            JOIN dim_location src ON f.source_location_id = src.location_id
            JOIN dim_location dst ON f.destination_location_id = dst.location_id
            GROUP BY src.city_name, dst.city_name
            ORDER BY booking_count DESC;
        """))

    print("PostgreSQL tables and views created/reset.")

if __name__ == "__main__":
    try:
        init_mysql()
    except Exception as e:
        print(f"Error creating MySQL table: {e}")
    
    try:
        init_postgres()
    except Exception as e:
        print(f"Error creating Postgres table: {e}")
