import pandas as pd
from sqlalchemy import create_engine

# Connection Strings (Internal Docker Network)
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'

def load_data():
    print("Starting data load to PostgreSQL...")
    
    # Create Engines
    mysql_engine = create_engine(MYSQL_CONN)
    pg_engine = create_engine(POSTGRES_CONN)
    
    try:
        # 1. Read KPIs from MySQL Staging
        df_kpi = pd.read_sql("SELECT * FROM staging_kpis", mysql_engine)
        print(f"Read {len(df_kpi)} rows from MySQL 'staging_kpis'.")
        
        if df_kpi.empty:
            print("Warning: No KPI data found.")
            return

        # 2. Write to PostgreSQL Analytics
        df_kpi.to_sql('flight_kpis', con=pg_engine, if_exists='replace', index=False)
        
        print("Successfully loaded KPIs into PostgreSQL 'flight_kpis' table.")
        
        # Verify
        result = pd.read_sql("SELECT count(*) as count FROM flight_kpis", pg_engine)
        print(f"PostgreSQL row count: {result['count'][0]}")
        
    except Exception as e:
        print(f"Error during data load: {e}")
        raise e

if __name__ == "__main__":
    load_data()
