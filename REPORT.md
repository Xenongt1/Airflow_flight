# Flight Price Analysis Pipeline - Final Report

## 1. Pipeline Architecture
The project implements a strict **Staging vs. Data Warehouse** architecture using Apache Airflow. All data processing and transformation occur in the Staging layer (MySQL), ensuring the Analytics layer (PostgreSQL) receives only pristine, modeled data.

### Components:
*   **Orchestration**: Apache Airflow (Dockerized).
*   **Source**: Kaggle Flight Price Dataset (CSV).
*   **Staging Layer**: MySQL 8.0 (Stores Raw Data + Performs Star Schema Transformation).
*   **Analytics Layer**: PostgreSQL (Stores Final Star Schema + KPI Views).

### Data Flow:
1.  **Ingestion** (`ingest_csv.py`): 
    *   Downloads CSV from Kaggle.
    *   Loads raw data into MySQL table `raw_flight_data`.
2.  **Transformation & Modeling** (`etl_star_schema.py`):
    *   Executes SQL Script `sql/etl_star_schema_mysql.sql` **inside MySQL**.
    *   **Dimension Population**: Extracts unique Airlines, Locations, and Dates (including Season/Holiday logic) into `dim_` tables.
    *   **Fact Population**: Joins cleaned data with Dimensions to create `fact_flights`.
3.  **Incremental Loading** (`etl_star_schema.py`):
    *   Transfers the modeled Star Schema from MySQL to PostgreSQL.
    *   Uses a **Timestamp-based Incremental Strategy** for the Fact table.
    *   Dimensions are handled via **Upsert logic** (only new records are added).

## 2. Incremental Loading Logic
To support scalable data growth and history tracking, the pipeline implements an incremental strategy:

-   **`loaded_at` Timestamps**: Every record in `fact_flights` is tagged with the exact time it entered the Data Warehouse.
-   **Idempotency**: If the pipeline is re-run on the same day, it automatically removes existing records for that day before reloading, preventing duplicates while allowing for partial-day updates.
-   **Dimension Upserts**: For Dimension tables (Airline, Location, etc.), the script checks for existing IDs and only inserts records that don't already exist.

## 3. Data Modeling: Star Schema
The schema is standardized across MySQL (Staging) and Postgres (Analytics).

### Tables:
*   **`fact_flights`**: Core metrics (Fare, Duration, FKs) + `loaded_at`.
*   **`dim_date`**: Attributes: `Day`, `Month`, `Season`, `Is_Holiday_Window`.
*   **`dim_airline`**: Unique Airline Names.
*   **`dim_location`**: Unique Source/Destination Cities.
*   **`dim_flight_details`**: Flight Class, Stopovers, Aircraft Type.

## 4. Key Performance Indicators (KPIs)
KPIs are implemented as **SQL Views** in PostgreSQL for real-time computation:

-   **`kpi_airline_stats`**: Average Fare & Booking Count per Airline.
-   **`kpi_seasonal_variation`**: Price comparison between Peak Seasons (Eid/Winter) and Off-Peak.
-   **`kpi_popular_routes`**: Top traveled routes by booking volume.

## 5. Data Verification
The `verify_analytics.py` script provides a quick way to validate the pipeline results and check the load history.

```bash
docker-compose exec airflow-scheduler python /opt/airflow/scripts/verify_analytics.py
```

## 6. How to Run
1.  **Start Services**: `docker-compose up -d --build`
2.  **Initialize DBs**: `docker-compose exec airflow-scheduler python /opt/airflow/scripts/init_db.py`
3.  **Run Pipeline**: Access Airflow at `http://localhost:8081` and toggle `flight_price_analysis_pipeline`.

