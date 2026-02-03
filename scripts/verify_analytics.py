import pandas as pd
from sqlalchemy import create_engine, text

# Connection to Analytics Database
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'

def run_analytics_queries():
    print("--- CONNECTING TO DATA WAREHOUSE ---")
    engine = create_engine(POSTGRES_CONN)
    
    # 1. THE WRONG WAY (Summing everything blindly)
    # This will return unexpected huge numbers if you have historical data
    print("\n[BAD QUERY] Summing all booking counts (Ignoring Date):")
    bad_query = "SELECT SUM(total_bookings) as total FROM flight_kpis;"
    try:
        df_bad = pd.read_sql(bad_query, engine)
        print(f"Total: {df_bad['total'][0]} (Likely inflated!)")
    except Exception as e:
        print("Data might be empty.")

    # 2. THE RIGHT WAY (Snapshot: "What are the numbers TODAY?")
    print("\n[GOOD QUERY] Latest Snapshot (Today's Numbers Only):")
    # We filter by the MAX(generated_at) to get the most recent pipeline run
    snapshot_query = """
    SELECT airline, avg_price, total_bookings, generated_at 
    FROM flight_kpis 
    WHERE generated_at::DATE = CURRENT_DATE
    ORDER BY total_bookings DESC;
    """ 
    # Alternative strict version: 
    # WHERE generated_at = (SELECT MAX(generated_at) FROM flight_kpis)
    
    df_snapshot = pd.read_sql(snapshot_query, engine)
    print(df_snapshot)

    # 3. TREND ANALYSIS (Comparing Yesterday vs Today)
    print("\n[GOOD QUERY] Trend Analysis (Comparing prices over time):")
    trend_query = """
    SELECT airline, generated_at::DATE as report_date, avg_price
    FROM flight_kpis
    WHERE airline = 'Biman Bangladesh Airlines' -- Example Airline
    ORDER BY report_date DESC
    LIMIT 5;
    """
    df_trend = pd.read_sql(trend_query, engine)
    if not df_trend.empty:
        print(df_trend)
    else:
        print("No trend data available for Biman.")

if __name__ == "__main__":
    run_analytics_queries()
