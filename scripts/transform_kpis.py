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
            # Enhanced Seasonality for Bangladesh context (approximate)
            # Winter: Nov-Feb, Pre-Monsoon: Mar-May, Monsoon: Jun-Oct
            month = date.month
            if month in [11, 12, 1, 2]: return 'Winter'
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
