from sqlalchemy import create_engine, text

# Postgres Operations
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'
pg_engine = create_engine(POSTGRES_CONN)

def update_views():
    print("Updating KPI Views in Postgres...")
    with pg_engine.connect() as conn:
        # 1. KPI: Average Fare by Airline
        conn.execute(text("DROP VIEW IF EXISTS kpi_airline_stats;"))
        conn.execute(text("""
            CREATE VIEW kpi_airline_stats AS
            SELECT 
                f.loaded_at::DATE as load_date,
                a.airline_name,
                COUNT(f.fact_id) as total_bookings,
                ROUND(AVG(f.total_fare_bdt), 2) as avg_fare
            FROM fact_flights f
            JOIN dim_airline a ON f.airline_id = a.airline_id
            GROUP BY f.loaded_at::DATE, a.airline_name;
        """))
        print("Updated kpi_airline_stats")

        # 2. KPI: Seasonal Variation (Simplified using Window Function)
        conn.execute(text("DROP VIEW IF EXISTS kpi_seasonal_variation;"))
        conn.execute(text("""
            CREATE VIEW kpi_seasonal_variation AS
            SELECT 
                f.loaded_at::DATE as load_date,
                d.season,
                ROUND(AVG(f.total_fare_bdt), 2) as avg_season_price,
                ROUND(AVG(f.total_fare_bdt) - AVG(AVG(f.total_fare_bdt)) OVER (PARTITION BY f.loaded_at::DATE), 2) as variation_from_overall
            FROM fact_flights f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY f.loaded_at::DATE, d.season;
        """))
        print("Updated kpi_seasonal_variation")

        # 3. KPI: Popular Routes (Simplified)
        conn.execute(text("DROP VIEW IF EXISTS kpi_popular_routes;"))
        conn.execute(text("""
            CREATE VIEW kpi_popular_routes AS
            SELECT 
                f.loaded_at::DATE as load_date,
                src.city_name || ' -> ' || dst.city_name as route,
                COUNT(f.fact_id) as booking_count,
                ROUND(AVG(f.total_fare_bdt), 2) as avg_price
            FROM fact_flights f
            JOIN dim_location src ON f.source_location_id = src.location_id
            JOIN dim_location dst ON f.destination_location_id = dst.location_id
            GROUP BY f.loaded_at::DATE, src.city_name, dst.city_name
            ORDER BY f.loaded_at::DATE DESC, booking_count DESC;
        """))
        print("Updated kpi_popular_routes")

if __name__ == "__main__":
    update_views()
