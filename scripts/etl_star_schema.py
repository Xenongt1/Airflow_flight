import pandas as pd
from sqlalchemy import create_engine, text
import os

# Connection Strings from Environment Variables
DB_USER = os.getenv('DB_USER', 'analytics_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'analytics_password')
DB_HOST = os.getenv('DB_HOST', 'postgres_analytics')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'flight_analytics')

STAGING_USER = os.getenv('STAGING_USER', 'staging_user')
STAGING_PASSWORD = os.getenv('STAGING_PASSWORD', 'staging_password')
STAGING_HOST = os.getenv('STAGING_HOST', 'mysql_staging')

MYSQL_CONN = f'mysql+mysqlconnector://{STAGING_USER}:{STAGING_PASSWORD}@{STAGING_HOST}:3306/flight_staging'
POSTGRES_CONN = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Transform in MySQL -> Load to Postgres
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

            # Sync Sequence (Fix for Duplicate Key Error)
            with pg_engine.begin() as pg_conn:
                pg_conn.execute(text("SELECT setval(pg_get_serial_sequence('fact_flights', 'fact_id'), coalesce(max(fact_id),0) + 1, false) FROM fact_flights;"))
            
            # Drop fact_id to let Postgres generate its own unique IDs
            if 'fact_id' in df_fact.columns:
                df_fact = df_fact.drop(columns=['fact_id'])

            # --- Deduplication Logic: Check against existing data ---
            print("Checking for existing records to prevent duplicates...")
            # We use a composite key of date_id, airline_id, departure_time, and total_fare_bdt as a "Business Key"
            existing_composite = pd.read_sql("SELECT date_id, airline_id, departure_time, total_fare_bdt FROM fact_flights", pg_engine)
            
            if not existing_composite.empty:
                # Create a tuple signature for comparison
                # Ensure types match (convert to string/native types if needed, but pandas usually handles this well for standard types)
                existing_set = set(zip(existing_composite['date_id'], existing_composite['airline_id'], existing_composite['departure_time'], existing_composite['total_fare_bdt']))
                
                # Filter df_fact
                initial_count = len(df_fact)
                df_fact = df_fact[~df_fact.apply(lambda x: (x['date_id'], x['airline_id'], x['departure_time'], x['total_fare_bdt']) in existing_set, axis=1)]
                filtered_count = len(df_fact)
                
                if initial_count != filtered_count:
                    print(f"Skipping {initial_count - filtered_count} duplicate records that already exist in Postgres.")
            
            if not df_fact.empty:
                df_fact.to_sql('fact_flights', pg_engine, if_exists='append', index=False, method='multi', chunksize=5000)
                print(f"Loaded {len(df_fact)} new rows into fact_flights at {current_time}.")
                fact_loaded = len(df_fact)
            else:
                print("No new unique records to load.")
                fact_loaded = 0
        
        print("ETL Complete: Incremental Data loaded into PostgreSQL.")
        
        # Return metrics for tracking
        return {
            'fact_records_loaded': fact_loaded,
            'dimensions_loaded': dimension_tables
        }
        
    except Exception as e:
        print(f"Error transferring data: {e}")
        raise e

def etl_process_wrapper(**context):
    """
    Wrapper function for Airflow with XCom support.
    Calls etl_process() and pushes metrics to XCom.
    """
    ti = context['ti']
    
    # Pull validation metrics from previous task
    records_validated = ti.xcom_pull(task_ids='validate_data', key='records_validated')
    if records_validated:
        print(f"📊 Received from validation: {records_validated} validated records")
    
    result = etl_process()
    
    if result and isinstance(result, dict):
        ti.xcom_push(key='fact_records_loaded', value=result.get('fact_records_loaded', 0))
        ti.xcom_push(key='dimensions_loaded', value=result.get('dimensions_loaded', []))
        
        print(f"✅ XCom Metrics Pushed: {result.get('fact_records_loaded', 0)} fact records loaded")
    
    return result

if __name__ == "__main__":
    etl_process()

