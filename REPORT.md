# Flight Price Analysis Pipeline - Final Report

## 1. Pipeline Architecture
The project implements an end-to-end ELT pipeline orchestrating data flow from a CSV source to a PostgreSQL data warehouse using Apache Airflow.

### Components:
*   **Orchestration**: Apache Airflow (Dockerized).
*   **Source**: Kaggle Flight Price Dataset (CSV).
*   **Staging**: MySQL (Handling raw and cleaned data).
*   **Data Warehouse**: PostgreSQL (Storing final KPIs for analytics).
*   **Containerization**: Docker Compose managing all services.

### Data Flow:
1.  **Ingestion**: `ingest_csv.py` downloads the CSV and loads it into MySQL `raw_flight_data`. It also calculates `Total Fare` if missing.
2.  **Validation**: `validate_data.py` cleanses the data (standardizing names, handling nulls, verifying routes) and stores it in `clean_flight_data`.
3.  **Transformation**: `transform_kpis.py` computes business metrics (Avg Fare, Seasonal Trends, Popular Routes) and stores them in MySQL `staging_kpis`, `seasonal_kpis`, and `route_kpis`.
4.  **Loading**: `load_postgres.py` moves the final KPIs to PostgreSQL tables for consumption.

## 2. KPIs Implemented

### A. Average Fare by Airline
*   **Logic**: Group by `Airline` and calculate the mean of `Total Fare`.
*   **Goal**: Benchmarking pricing strategies across carriers.

### B. Seasonal Fare Variation
*   **Logic**:
    *   **Peak Holiday Detection**: Specifically checks for **Eid-ul-Fitr** and **Eid-ul-Adha** windows (approx +/- 3 days) for years 2022-2025.
    *   **Standard Seasons**:
        *   **Winter**: Nov, Dec, Jan, Feb
        *   **Pre-Monsoon (Spring)**: Mar, Apr, May
        *   **Monsoon/Summer**: Jun, Oct
    *   Calculates average price per season and its deviation from the overall average.
*   **Goal**: Identifying high-demand periods (especially Religious Holidays) for dynamic pricing analysis.

### C. Booking Count by Airline
*   **Logic**: Count of total bookings (rows) per airline.
*   **Goal**: Measuring market share and popularity.

### D. Most Popular Routes
*   **Logic**: Group by `Source` and `Destination`, count bookings, and sort descending.
*   **Goal**: Identifying top traveled routes.

## 3. DAG Description
The Airflow DAG `flight_price_analysis_pipeline` defines the dependency chain:

`start_pipeline` >> `ingest_data` >> `validate_data` >> `transform_data` >> `load_postgres`

Each task is implemented as a `PythonOperator` execution of the modular scripts located in `/opt/airflow/scripts`.

## 4. Challenges & Resolutions
*   **Total Fare Calculation**: source data might miss the total fare. Added logic to compute it dynamically (`Base + Tax`) during ingestion.
*   **Data Quality**: Inconsistent capitalization (e.g., "dhaka" vs "Dhaka"). Implemented string standardization in the validation step.
*   **Schema Mismatch**: The intermediate transformation step lost the primary key `id`, causing downstream failures in aggregation. Resolved by using `airline` column for counting instead of `id`.
*   **Database Connectivity**: Docker networking required configuring specific hostnames (`mysql_staging`, `postgres_analytics`) instead of `localhost` for inter-container communication.
*   **Idempotency & Data Duplication**: Initially, re-running the pipeline appended duplicate rows to PostgreSQL. 
    *   *Resolution*: Implemented a "Delete-Write" strategy in `load_postgres.py`. The script now deletes any data generated on the *current day* before loading new records, ensuring the pipeline can be re-run safely multiple times a day without corrupting the analytics.

## 5. Usage
To run the pipeline manually:
1.  Ensure Docker is running.
2.  Trigger the DAG via Airflow UI or run scripts manually:
    ```bash
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/ingest_csv.py
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/validate_data.py
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/transform_kpis.py
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/load_postgres.py
    ```

3. **Verify Data**:
    A verification script is included to demonstrate correct querying patterns:
    ```bash
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/verify_analytics.py
    ```
