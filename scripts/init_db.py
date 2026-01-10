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
        conn.execute(text("DROP TABLE IF EXISTS staging_kpis;")) # Cleanup staging kpis too
        
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
                destination VARCHAR(10),
                total_fare_bdt FLOAT,
                departure_date_time DATETIME
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
        
    print("MySQL tables created/reset.")

def init_postgres():
    print("Creating PostgreSQL table...")
    with postgres_engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS flight_kpis;"))
        conn.execute(text("""
            CREATE TABLE flight_kpis (
                kpi_id SERIAL PRIMARY KEY,
                airline VARCHAR(255),
                avg_price NUMERIC,
                total_bookings INT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
    print("PostgreSQL table 'flight_kpis' created/reset.")

if __name__ == "__main__":
    try:
        init_mysql()
    except Exception as e:
        print(f"Error creating MySQL table: {e}")
    
    try:
        init_postgres()
    except Exception as e:
        print(f"Error creating Postgres table: {e}")
