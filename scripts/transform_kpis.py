import pandas as pd
from sqlalchemy import create_engine

# Connection String
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'

def transform_data():
    print("Starting data transformation & KPI calculation...")
    
    engine = create_engine(MYSQL_CONN)
    
    try:
        # Read Clean Data
        df = pd.read_sql("SELECT * FROM clean_flight_data", engine)
        print(f"Loaded {len(df)} rows for transformation.")
        
        if df.empty:
            print("No data to transform.")
            return

        # KPI 1: Average Fare by Airline
        avg_fare_kpi = df.groupby('airline')['total_fare_bdt'].mean().reset_index()
        avg_fare_kpi.rename(columns={'total_fare_bdt': 'avg_price'}, inplace=True)
        
        # KPI 2: Booking Count by Airline
        booking_count_kpi = df.groupby('airline').size().reset_index(name='total_bookings')
        
        # Merge KPIs into a single summary table
        kpi_summary = pd.merge(avg_fare_kpi, booking_count_kpi, on='airline')
        
        print("\n--- KPI Preview ---")
        print(kpi_summary.head())
        print("-------------------")
        
        # Save to 'staging_kpis'
        kpi_summary.to_sql('staging_kpis', con=engine, if_exists='replace', index=False)
        print("KPIs saved to MySQL 'staging_kpis' table.")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise e

if __name__ == "__main__":
    transform_data()
