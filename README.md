# Flight Price Analysis Pipeline

An end-to-end data engineering pipeline orchestrated by **Apache Airflow** to analyze flight prices in Bangladesh. This project employs a strict **ELT (Extract, Load, Transform)** architecture where data is staged and modeled in MySQL before being loaded into a PostgreSQL Star Schema for analytics.

## Features

*   **Automated Ingestion**: Downloads flight data from Kaggle and loads it into MySQL.
*   **Star Schema Modeling**: Core flight data is organized into Fact and Dimension tables for high-performance querying.
*   **Incremental Loading**: Uses `loaded_at` timestamps to efficiently track and load data history without duplicates.
*   **SQL-Native Transformations**: Business logic (seasons, holidays, joins) is implemented as pure SQL for maintainability.
*   **KPI Views**: Real-time metrics computed via PostgreSQL Views (Avg Fare, Popular Routes, Seasonal Trends).

## Tech Stack

*   **Orchestration**: Apache Airflow 2.x
*   **Databases**: MySQL 8.0 (Staging), PostgreSQL 13 (Analytics)
*   **Language**: Python 3.x (Pandas, SQLAlchemy)
*   **Infrastructure**: Docker & Docker Compose

## Project Structure

```
├── dags/                   # Airflow DAG: Ingest -> Validate -> ETL
├── sql/                    
│   └── etl_star_schema_mysql.sql # Core SQL Transformation logic
├── scripts/                
│   ├── ingest_csv.py       # Ingests Kaggle data to MySQL
│   ├── etl_star_schema.py  # Orchestrates MySQL transformation & loads to Postgres
│   ├── verify_analytics.py # Validates final KPIs in Postgres
│   └── init_db.py          # Schema and Views initialization
├── docker-compose.yaml     # Container orchestration
└── REPORT.md               # Detailed architectural and design report
```

## Usage

### 1. Start the Environment
```bash
docker-compose up -d --build
```

### 2. Initialize Databases
Creates all tables, foreign keys, and KPI views in both MySQL and PostgreSQL.
```bash
docker-compose exec airflow-scheduler python /opt/airflow/scripts/init_db.py
```

### 3. Run the Pipeline
Access the Airflow UI at `http://localhost:8081` (Admin/Admin) and trigger the **`flight_price_analysis_pipeline`**.

Alternatively, run manually:
```bash
# Part 1: Ingest
docker-compose exec airflow-scheduler python /opt/airflow/scripts/ingest_csv.py

# Part 2: Model & Load (Star Schema + Incremental)
docker-compose exec airflow-scheduler python /opt/airflow/scripts/etl_star_schema.py
```

### 4. Verify Results
Check the computed KPIs and load history:
```bash
docker-compose exec airflow-scheduler python /opt/airflow/scripts/verify_analytics.py
```

---
For detailed information on the data model and logic, see **[REPORT.md](./REPORT.md)**.
