import pandas as pd
from sqlalchemy import create_engine
import os
import kagglehub
import glob
import hashlib # For row hashing

# Connection String (Internal Docker Network)
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'

def ingest_data():
    print("Starting data ingestion with KaggleHub...")
    
    try:
        # Download latest version
        print("Downloading dataset from KaggleHub...")
        path = kagglehub.dataset_download("mahatiratusher/flight-price-dataset-of-bangladesh")
        print("Path to dataset files:", path)
        
        # Find the CSV file in the downloaded path
        csv_files = glob.glob(os.path.join(path, "*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV file found in {path}")
            
        downloaded_csv = csv_files[0]
        print(f"Downloaded CSV file: {downloaded_csv}")
        
        # Copy to the data directory so the user can see it
        # /opt/airflow/data maps to flight-airflow/data on the host
        destination_path = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'
        import shutil
        shutil.copy(downloaded_csv, destination_path)
        print(f"Copied CSV to {destination_path} for visibility.")

        csv_path = destination_path # Use the local copy

        # Read CSV
        df = pd.read_csv(csv_path)
        print(f"Successfully read CSV. Shape: {df.shape}")
        
        # Clean Column Names
        df.columns = [c.strip().lower()
                      .replace(" ", "_")
                      .replace("&_", "")
                      .replace("_(hrs)", "_hrs")
                      .replace("_(bdt)", "_bdt")
                      for c in df.columns]
        
        print(f"Mapped Columns: {df.columns.tolist()}")
        
        # Ensure dates are parsed
        df['departure_date_time'] = pd.to_datetime(df['departure_date_time'], errors='coerce')
        df['arrival_date_time'] = pd.to_datetime(df['arrival_date_time'], errors='coerce')

        # Ensure Total Fare exists or is calculated
        # Some rows might vary, so we enforce the calculation to be safe if base/tax exist
        if 'base_fare_bdt' in df.columns and 'tax_surcharge_bdt' in df.columns:
            print("Verifying/Calculating 'total_fare_bdt'...")
            # Fill NaNs with 0 for calculation safety
            df['base_fare_bdt'] = pd.to_numeric(df['base_fare_bdt'], errors='coerce').fillna(0)
            df['tax_surcharge_bdt'] = pd.to_numeric(df['tax_surcharge_bdt'], errors='coerce').fillna(0)
            
            # Recalculate to ensure consistency
            df['total_fare_bdt'] = df['base_fare_bdt'] + df['tax_surcharge_bdt']
        elif 'total_fare_bdt' not in df.columns:
             # If we can't calculate it and it's missing, we have a problem.
             # But let's assume if it's missing schema, we might fail insertion later.
             print("Warning: 'total_fare_bdt' missing and source columns not found.")
        
        # --- Hashing for De-duplication ---
        print("Generating row hashes for idempotency...")
        
        def generate_hash(row):
            # Sort index to ensure consistent column order if that ever changes, though apply(axis=1) usually preserves it.
            # Convert to string, concatenate, encode, then hash.
            row_str = "".join(row.astype(str))
            return hashlib.sha256(row_str.encode("utf-8")).hexdigest()

        # Generate hash for every row in the dataframe
        df['row_hash'] = df.apply(generate_hash, axis=1)

        # Create Engine
        engine = create_engine(MYSQL_CONN)

        # Check existing hashes in the database to prevent duplicates
        try:
            print("Fetching existing hashes from DB...")
            existing_hashes_df = pd.read_sql("SELECT row_hash FROM raw_flight_data", engine)
            existing_hashes = set(existing_hashes_df['row_hash'].tolist())
            print(f"Found {len(existing_hashes)} existing records.")
        except Exception as e:
            # If table doesn't exist or error, assume no history
            print(f"Could not fetch existing hashes (Table might be new): {e}")
            existing_hashes = set()

        # Filter out rows that already exist
        initial_count = len(df)
        df_new = df[~df['row_hash'].isin(existing_hashes)]
        filtered_count = len(df_new)

        if filtered_count < initial_count:
            print(f"Duplicate/Existing rows skipped: {initial_count - filtered_count}")

        if filtered_count > 0:
            print(f"Inserting {filtered_count} new rows into 'raw_flight_data'...")
            df_new.to_sql('raw_flight_data', con=engine, if_exists='append', index=False)
            print("Ingestion complete.")
        else:
            print("No new data found. All rows already exist in DB.")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise e

if __name__ == "__main__":
    ingest_data()
