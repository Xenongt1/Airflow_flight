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

        # KPI 3: Seasonal Fare Variation
        print("Calculating Seasonal KPIs...")
        def get_season(date):
            if pd.isnull(date): return 'Unknown'
            
            # --- 1. CHECK FOR SPECIFIC HOLIDAYS (EID) FIRST ---
            # Eid dates shift yearly (Lunar). We define a +/- 3 day window for "Peak Holiday Travel".
            # Dates approximated for 2022-2025 context.
            
            eid_windows = [
                # 2022
                (pd.Timestamp('2022-05-02'), pd.Timestamp('2022-05-05')), # Eid-ul-Fitr
                (pd.Timestamp('2022-07-09'), pd.Timestamp('2022-07-12')), # Eid-ul-Adha
                # 2023
                (pd.Timestamp('2023-04-21'), pd.Timestamp('2023-04-24')), # Eid-ul-Fitr
                (pd.Timestamp('2023-06-28'), pd.Timestamp('2023-07-01')), # Eid-ul-Adha
                # 2024
                (pd.Timestamp('2024-04-10'), pd.Timestamp('2024-04-13')), # Eid-ul-Fitr
                (pd.Timestamp('2024-06-16'), pd.Timestamp('2024-06-19')), # Eid-ul-Adha
                 # 2025
                (pd.Timestamp('2025-03-30'), pd.Timestamp('2025-04-02')), # Eid-ul-Fitr
                (pd.Timestamp('2025-06-06'), pd.Timestamp('2025-06-09')), # Eid-ul-Adha
            ]
            
            # Check if date falls in a holiday window
            for start, end in eid_windows:
                # Expand window slightly for travel rush (e.g., 2 days before to 2 days after)
                rush_start = start - pd.Timedelta(days=2)
                rush_end = end + pd.Timedelta(days=2)
                
                if rush_start.date() <= date.date() <= rush_end.date():
                    return 'Peak: Eid Holiday'

            # --- 2. FALLBACK TO METEOROLOGICAL SEASONS ---
            # Winter: Nov-Feb (High Travel Season in BD)
            # Pre-Monsoon: Mar-May
            # Monsoon: Jun-Oct
            month = date.month
            if month in [11, 12, 1, 2]: return 'Winter (High Season)'
            elif month in [3, 4, 5]: return 'Spring/Pre-Monsoon'
            else: return 'Monsoon/Summer'
            
        df['departure_date_time'] = pd.to_datetime(df['departure_date_time'])
        df['season'] = df['departure_date_time'].apply(get_season)
        
        seasonal_kpi = df.groupby('season')['total_fare_bdt'].mean().reset_index(name='avg_price')
        overall_avg = df['total_fare_bdt'].mean()
        seasonal_kpi['variation_from_overall'] = seasonal_kpi['avg_price'] - overall_avg
        
        print("Seasonal KPIs calculated.")
        seasonal_kpi.to_sql('seasonal_kpis', con=engine, if_exists='replace', index=False)
        print("Seasonal KPIs saved to 'seasonal_kpis'.")

        # KPI 4: Most Popular Routes
        print("Calculating Route KPIs...")
        route_kpi = df.groupby(['source', 'destination']).agg(
             booking_count=('airline', 'size'), # Changed from 'id' to 'airline' as id might be lost
             avg_price=('total_fare_bdt', 'mean')
        ).reset_index().sort_values(by='booking_count', ascending=False)
        
        # Limit to reasonable number if needed, but for analytics storing all is fine
        print("Route KPIs calculated.")
        print(route_kpi.head())
        route_kpi.to_sql('route_kpis', con=engine, if_exists='replace', index=False)
        print("Route KPIs saved to 'route_kpis'.")
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise e

if __name__ == "__main__":
    transform_data()
