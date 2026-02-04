import pandas as pd
from sqlalchemy import create_engine, text
import os

# Connection Strings
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'
# Option B: Transform in MySQL -> Load to Postgres
SQL_SCRIPT_PATH = '/opt/airflow/sql/etl_star_schema_mysql.sql'

def etl_process():
    print("Starting ETL (MySQL-Transform based)...")
    
    mysql_engine = create_engine(MYSQL_CONN)
    pg_engine = create_engine(POSTGRES_CONN)

    # 1. Execute SQL Transformation on MySQL
    print(f"Executing SQL transformation script on MySQL from {SQL_SCRIPT_PATH}...")
    
    if not os.path.exists(SQL_SCRIPT_PATH):
            local_path = os.path.join(os.path.dirname(__file__), '../sql/etl_star_schema_mysql.sql')
            print(f"Container path not found, trying local path: {local_path}")
            with open(local_path, 'r') as f:
                sql_content = f.read()
    else:
        with open(SQL_SCRIPT_PATH, 'r') as f:
            sql_content = f.read()

    statements = sql_content.split(';')
    
    with mysql_engine.connect() as conn:
        trans = conn.begin()
        try:
            for statement in statements:
                if statement.strip():
                    # print(f"Executing statement: {statement[:50]}...")
                    conn.execute(text(statement))
            trans.commit()
            print("MySQL Transformation Complete. Star Schema populated in Staging.")
        except Exception as e:
            trans.rollback()
            print(f"Error executing MySQL Transformation: {e}")
            raise e

    # 2. Transfer Final Tables from MySQL to PostgreSQL (Incremental Strategy)
    print("Transferring Fact & Dimension tables to PostgreSQL...")
    
    from datetime import datetime
    current_time = datetime.now()
    print(f"Load Time: {current_time}")
    
    try:
        # --- Dimensions: Upsert (Insert only new records) ---
        dimension_tables = ['dim_date', 'dim_airline', 'dim_location', 'dim_flight_details']
        
        for table in dimension_tables:
            print(f"Upserting {table}...")
            df = pd.read_sql(f"SELECT * FROM {table}", mysql_engine)
            if df.empty:
                print(f"Warning: {table} is empty.")
                continue
            
            # Fix boolean conversion for dim_date
            if table == 'dim_date' and 'is_holiday_window' in df.columns:
                df['is_holiday_window'] = df['is_holiday_window'].astype(bool)
            
            # Get existing IDs to avoid duplicates
            if table == 'dim_flight_details':
                id_column = 'detail_id'  # Special case
            else:
                id_column = f"{table.replace('dim_', '')}_id"
            existing_ids = pd.read_sql(f"SELECT {id_column} FROM {table}", pg_engine)
            
            if not existing_ids.empty:
                # Filter out records that already exist
                new_records = df[~df[id_column].isin(existing_ids[id_column])]
                if not new_records.empty:
                    new_records.to_sql(table, pg_engine, if_exists='append', index=False)
                    print(f"Inserted {len(new_records)} new records into {table}.")
                else:
                    print(f"No new records for {table}.")
            else:
                # First load - insert all
                df.to_sql(table, pg_engine, if_exists='append', index=False)
                print(f"Inserted {len(df)} records into {table} (initial load).")
        
        # --- Fact Table: Incremental with Timestamp Tracking ---
        print(f"Loading fact_flights...")
        
        # Delete today's data if re-running (idempotency)
        today = current_time.strftime('%Y-%m-%d')
        with pg_engine.begin() as pg_conn:
            result = pg_conn.execute(text(f"DELETE FROM fact_flights WHERE loaded_at::DATE = '{today}'"))
            deleted_count = result.rowcount
            if deleted_count > 0:
                print(f"Deleted {deleted_count} rows from today's previous run (idempotency).")
        
        # Load new data with timestamp
        df_fact = pd.read_sql("SELECT * FROM fact_flights", mysql_engine)
        if not df_fact.empty:
            # Add timestamp
            df_fact['loaded_at'] = current_time
            
            df_fact.to_sql('fact_flights', pg_engine, if_exists='append', index=False)
            print(f"Loaded {len(df_fact)} rows into fact_flights at {current_time}.")
        
        print("ETL Complete: Incremental Data loaded into PostgreSQL.")
        
    except Exception as e:
        print(f"Error transferring data: {e}")
        raise e

if __name__ == "__main__":
    etl_process()
