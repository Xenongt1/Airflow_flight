from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'flight_fare_retraining',
    default_args=default_args,
    description='Retrain Flight Fare Prediction Model using latest data from Postgres',
    schedule_interval='@daily', # Run once a day
    start_date=days_ago(1),
    catchup=False,
    tags=['ml', 'flight_fare'],
) as dag:

    # Task 1: Run the Training Pipeline
    # We assume the project directory is mounted to /opt/airflow/ml_pipeline
    # We also assume the dependencies are installed in the environment where Airflow runs
    # or inside a virtualenv we can access.
    # For simplicity, we use the system python or a specific venv if known.
    train_model = BashOperator(
        task_id='train_model',
        bash_command='cd /opt/airflow/ml_pipeline && python run_pipeline.py',
        env={
            'DATA_SOURCE': 'postgres', # Force DB source for Airflow runs
            # Pass other env vars if needed, or rely on .env file loading in config.py
        }
    )

    # Future Task: validation, deployment, etc.
    
    train_model
