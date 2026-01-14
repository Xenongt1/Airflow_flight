import pandas as pd
from sqlalchemy import create_engine
import os
import kagglehub
import glob

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
        
        # Create Engine & Insert
        engine = create_engine(MYSQL_CONN)
        df.to_sql('raw_flight_data', con=engine, if_exists='append', index=False)
        
        print(f"Ingested {len(df)} rows into 'raw_flight_data'.")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
        raise e

if __name__ == "__main__":
    ingest_data()
