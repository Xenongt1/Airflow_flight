import pandas as pd
from sqlalchemy import create_engine

# Connection String
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'

def validate_data():
    print("Starting data validation...")
    
    engine = create_engine(MYSQL_CONN)
    
    try:
        # Read from Raw Table
        query = "SELECT * FROM raw_flight_data"
        df = pd.read_sql(query, engine)
        print(f"Read {len(df)} rows from 'raw_flight_data'.")
        
        if df.empty:
            print("Warning: No data found in raw table.")
            return

        # 1. Check for Missing Values in Critical Columns
        critical_cols = ['airline', 'source', 'destination', 'total_fare_bdt']
        initial_count = len(df)
        df_clean = df.dropna(subset=critical_cols)
        dropped_count = initial_count - len(df_clean)
        
        if dropped_count > 0:
            print(f"Dropped {dropped_count} rows due to missing values in {critical_cols}.")
        
        # 2. Validate Data Types & Logical Consistencies
        # Ensure Price is numeric and positive
        df_clean['total_fare_bdt'] = pd.to_numeric(df_clean['total_fare_bdt'], errors='coerce')
        df_clean = df_clean[df_clean['total_fare_bdt'] > 0]
        
        # 3. Standardize & Validate Strings
        # Ensure consistent capitalizing
        for col in ['airline', 'source', 'destination', 'source_name', 'destination_name']:
             if col in df_clean.columns:
                 df_clean[col] = df_clean[col].astype(str).str.strip().str.title()
                 
        # Validate Routes (Source != Destination)
        invalid_routes = df_clean[df_clean['source'] == df_clean['destination']]
        if not invalid_routes.empty:
             print(f"Dropped {len(invalid_routes)} rows where Source == Destination.")
             df_clean = df_clean[df_clean['source'] != df_clean['destination']]
        
        # Select only relevant columns
        # cols_to_keep = ['airline', 'source', 'destination', 'total_fare_bdt', 'departure_date_time']
        # existing_cols = [c for c in cols_to_keep if c in df_clean.columns]
        df_final = df_clean # Keep all columns for Star Schema

        print(f"Validation complete. {len(df_final)} valid rows remaining.")
        
        # 4. Save to 'clean_flight_data' table
        df_final.to_sql('clean_flight_data', con=engine, if_exists='replace', index=False)
        print("Validated data saved to 'clean_flight_data' table.")
        
    except Exception as e:
        print(f"Error during validation: {e}")
        raise e

if __name__ == "__main__":
    validate_data()
