import pandas as pd
from sqlalchemy import create_engine, text

# Connection Strings (Internal Docker Network)
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'

def load_data():
    print("Starting data load to PostgreSQL...")
    
    # Create Engines
    mysql_engine = create_engine(MYSQL_CONN)
    pg_engine = create_engine(POSTGRES_CONN)
    
    try:
        # Pre-Load: Clean up specific tables for the current day to ensure Idempotency
        # This prevents duplicate entries if the pipeline is run multiple times on the same day.
        # We use a transaction (begin) to ensure safety.
        print("Cleaning up old data for today (Idempotency Check)...")
        with pg_engine.begin() as conn:
            conn.execute(text("DELETE FROM flight_kpis WHERE generated_at::DATE = CURRENT_DATE"))
            conn.execute(text("DELETE FROM seasonal_kpis WHERE generated_at::DATE = CURRENT_DATE"))
            conn.execute(text("DELETE FROM route_kpis WHERE generated_at::DATE = CURRENT_DATE"))
        print("Cleanup complete.")

        # 1. Read KPIs from MySQL Staging
        df_kpi = pd.read_sql("SELECT * FROM staging_kpis", mysql_engine)
        print(f"Read {len(df_kpi)} rows from MySQL 'staging_kpis'.")
        
        if df_kpi.empty:
            print("Warning: No KPI data found.")
            return

        # 2. Write to PostgreSQL Analytics
        # Note: We use 'append' to respect the schema with SERIAL ID and TIMESTAMP defined in init_db.py
        df_kpi.to_sql('flight_kpis', con=pg_engine, if_exists='append', index=False) 
        print("Successfully loaded 'flight_kpis' into PostgreSQL.")

        # 3. Load Seasonal KPIs
        df_seasonal = pd.read_sql("SELECT * FROM seasonal_kpis", mysql_engine)
        if not df_seasonal.empty:
            df_seasonal.to_sql('seasonal_kpis', con=pg_engine, if_exists='append', index=False)
            print("Successfully loaded 'seasonal_kpis' into PostgreSQL.")
            
        # 4. Load Route KPIs
        df_route = pd.read_sql("SELECT * FROM route_kpis", mysql_engine)
        if not df_route.empty:
            df_route.to_sql('route_kpis', con=pg_engine, if_exists='append', index=False)
            print("Successfully loaded 'route_kpis' into PostgreSQL.")
        
        # Verify
        result = pd.read_sql("SELECT count(*) as count FROM flight_kpis", pg_engine)
        print(f"PostgreSQL row count: {result['count'][0]}")
        
    except Exception as e:
        print(f"Error during data load: {e}")
        raise e

if __name__ == "__main__":
    load_data()
