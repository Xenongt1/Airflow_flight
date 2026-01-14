# Flight Price Analysis Pipeline

An end-to-end data engineering pipeline orchestrated by **Apache Airflow** to analyze flight prices in Bangladesh. This project ingests raw CSV data, cleansizes and transforms it, and loads business-critical KPIs into a PostgreSQL data warehouse.

## 🚀 Features

*   **Automated Ingestion**: Downloads and ingests flight data from Kaggle/local CSV.
*   **Data Validation**: logical checks for negative fares, missing values, and valid route consistency.
*   **KPI Calculation**:
    *   **Average Fare by Airline**: Benchmarking carrier pricing.
    *   **Seasonal Trends**: Analysis of price variations across Winter, Spring, and Monsoon seasons.
    *   **Route Popularity**: Identifying top-traveled source-destination pairs.
*   **Modern Stack**: Dockerized environment with Airflow, MySQL (Staging), and PostgreSQL (Analytics).

## 🛠️ Tech Stack

*   **Orchestration**: Apache Airflow 2.x
*   **Language**: Python (Pandas, SQLAlchemy)
*   **Database (Staging)**: MySQL 8.0
*   **Database (Analytics)**: PostgreSQL 13
*   **Infrastructure**: Docker & Docker Compose

## 📂 Project Structure

```
├── dags/                   # Airflow DAG definitions
├── data/                   # Raw data storage (mapped to container)
├── scripts/                # ETL Logic
│   ├── ingest_csv.py       # Data Loading & Total Fare Calc
│   ├── validate_data.py    # Cleaning & Validation
│   ├── transform_kpis.py   # Business Logic & Aggregations
│   ├── load_postgres.py    # Loading to Data Warehouse
│   └── init_db.py          # Schema Initialization
├── docker-compose.yaml     # Container orchestration
└── REPORT.md               # Detailed architectural report
```

## ⚡ Usage

### Prerequisites
*   Docker & Docker Compose installed.

### Quick Start

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/Xenongt1/Airflow_flight.git
    cd Airflow_flight
    ```

2.  **Start the environment**:
    ```bash
    docker-compose up -d
    ```

3.  **Initialize Databases**:
    ```bash
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/init_db.py
    ```

4.  **Run the Pipeline**:
    You can trigger the DAG `flight_price_analysis_pipeline` from the Airflow UI at `http://localhost:8081` (User/Pass: `admin`/`admin`), or run the scripts manually:
    ```bash
    # Ingest
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/ingest_csv.py
    
    # Process
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/validate_data.py
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/transform_kpis.py
    
    # Load
    docker-compose exec airflow-scheduler python /opt/airflow/scripts/load_postgres.py
    ```

5.  **Verify Results**:
    Access the PostgreSQL warehouse:
    ```bash
    docker-compose exec postgres_analytics psql -U analytics_user -d flight_analytics -c "SELECT * FROM seasonal_kpis;"
    ```

## 📊 Logic Details

For a deep dive into the transformation logic, schema design, and challenges faced, please refer to [REPORT.md](./REPORT.md).
