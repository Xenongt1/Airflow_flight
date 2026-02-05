# Flight Price Analysis Pipeline - Comprehensive Report

## 1. Pipeline Architecture and Execution Flow

This project implements a robust **ELT (Extract, Load, Transform)** data pipeline orchestrated by **Apache Airflow**. It is designed to ingest raw flight data, ensure data quality through deduplication and validation, and model it into a **Star Schema** for high-performance analytics.

### Architecture Overview
The system follows a "Staging vs. Data Warehouse" architecture:
1.  **Orchestration Layer**: Dockerized **Apache Airflow** manages the workflow dependencies, scheduling, and error alerting.
2.  **Staging Layer (MySQL)**: Acts as the landing zone for raw data. Heavy transformations and data cleaning occur here to offload processing from the final destination.
3.  **Analytics Layer (PostgreSQL)**: Serves as the Data Warehouse. It hosts the clean Star Schema and pre-computed KPI views for reporting.

### Execution Flow
1.  **Extract**: Data is downloaded from Kaggle (CSV format).
2.  **Idempotency Check**: Each row is hashed (SHA256). The system allows only *new* unique hashes into the database, silently skipping duplicates.
3.  **Load (Staging)**: Unique data is loaded into MySQL `raw_flight_data`.
4.  **Validate**: Python scripts clean the data (null checks, logic validation) and move it to `clean_flight_data`.
5.  **Transform (Dimension Modeling)**: SQL logic running inside MySQL transforms flat data into Dimensions (`dim_airline`, `dim_date`, etc.) and Facts (`fact_flights`).
6.  **Load (Warehouse)**: The modeled Star Schema is loaded into PostgreSQL using an incremental strategy (replacing current-day records to allow safe re-runs).

---

## 2. Airflow DAG and Task Descriptions

The pipeline is defined in a single DAG: **`flight_price_analysis_pipeline`**.

### Global Configuration
*   **Schedule**: Manual Trigger (set to `None`).
*   **Error Handling**: A customized `on_failure_callback` is registered. If **ANY** task fails, a notification regarding the specific Task and DAG ID is sent immediately to a **Slack** channel.

### Task Breakdown

#### 1. `ingest_data` (PythonOperator)
*   **Goal**: Securely bring external data into the system.
*   **Logic**:
    *   Downloads the latest dataset using the `kagglehub` API.
    *   Generates a **SHA256 Hash** for every row (Signature of all columns).
    *   Queries `raw_flight_data` for existing hashes.
    *   Filters out existing records and inserts only **new** rows into MySQL.

#### 2. `validate_data` (PythonOperator)
*   **Goal**: Ensure data quality and consistency.
*   **Logic**:
    *   Reads `raw_flight_data`.
    *   **Sanity Checks**: Removes rows with missing prices or where `Source == Destination`.
    *   **Standardization**: Title-cases strings (City names, Airlines).
    *   Writes acceptable data to the `clean_flight_data` table (overwriting previous clean state).

#### 3. `etl_star_schema` (PythonOperator)
*   **Goal**: Model data for analytics and publish to Data Warehouse.
*   **Logic**:
    *   **Step A (MySQL)**: Executes `sql/etl_star_schema_mysql.sql`. This populates the Staging Star Schema (Dimensions & Facts). It includes complex logic to classify dates into "Peak Seasons" (e.g., Eid Holidays) or "Off-Peak".
    *   **Step B (Transfer)**: Reads the Staging Star Schema and loads it into PostgreSQL.
        *   **Dimensions**: Upsert strategy (insert if ID doesn't exist).
        *   **Facts**: Incremental load. Validates idempotency by deleting any data `loaded_at` the current date before inserting the new batch.

---

## 3. KPI Definitions and Computation Logic

Key Performance Indicators are implemented as **Materialized Views** (or standard Views) in PostgreSQL, ensuring real-time consistency with the underlying Star Schema.

### 1. Airline Pricing Performance (`kpi_airline_stats`)
*   **Definition**: Comparison of average fares across different carriers.
*   **Logic**: Aggregates `fact_flights` by `dim_airline`. Calculates `AVG(total_fare_bdt)` and `COUNT(bookings)`.
*   **Usage**: Identifies budget vs. premium carriers.

### 2. Seasonal Price Surge (`kpi_seasonal_variation`)
*   **Definition**: Measures how much flight prices increase during specific holiday windows compared to the yearly average.
*   **Logic**:
    *   `dim_date` classifies dates into 'Eid Holiday', 'Winter', 'Monsoon', etc.
    *   Computes `Average Season Price - Overall Average Price`.
*   **Usage**: Critical for analyzing demand surges during festivals.

### 3. Route Popularity (`kpi_popular_routes`)
*   **Definition**: Identifies high-traffic flight corridors.
*   **Logic**: Group by `Source City` and `Destination City` from `dim_location`. Sorts by `COUNT(*)` descending.
*   **Usage**: Helps in understanding travel patterns (e.g., Dhaka -> Chittagong).

---

## 4. Challenges Encountered and Resolutions

### Challenge 1: Data Duplication
*   **Issue**: Running the pipeline multiple times (e.g., for backfilling or testing) caused the same CSV rows to be stacked on top of each other in the database, skewing analytics.
*   **Resolution**: Implemented a **Row-Level Hashing Strategy**. We calculate a SHA256 fingerprint for every incoming row. The ingestion script checks these fingerprints against the database before insertion, ensuring strict mathematical uniqueness.

### Challenge 2: Monitoring & Observability
*   **Issue**: Pipeline failures (e.g., network issues with Kaggle, DB connection timeouts) were widely unnoticed until someone manually checked the Airflow UI.
*   **Resolution**: Integrated **Slack Webhooks**. We added a python callback function to the DAG that catches any failure event and pushes a formatted alert (with logs link) to the team's Slack channel instantly.

### Challenge 3: Complex Date Logic in Python
*   **Issue**: Calculating "Eid Holidays" (which move every year) inside Pandas was becoming messy and hard to maintain.
*   **Resolution**: Shifted logic to **SQL**. We utilized the `dim_date` generation step in `etl_star_schema_mysql.sql` to handle complex `CASE WHEN` logic for dates. This strictly follows the ELT philosophy—using the database engine for what it does best.

### Challenge 4: Idempotency in the Data Warehouse
*   **Issue**: Even with source deduplication, re-running the ETL step could duplicate rows in the final PostgreSQL Fact table.
*   **Resolution**: Enforced **Time-based Idempotency**. The loading script explicitly deletes records where `loaded_at == Today` in PostgreSQL before inserting the new batch from Staging. This allows safe re-runs of the ETL step.
