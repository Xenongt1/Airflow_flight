from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Ensure scripts can be imported
sys.path.append('/opt/airflow/scripts')
from check_retrain_threshold import check_new_data_exists

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'flight_fare_retraining',
    default_args=default_args,
    description='Smart Retraining for Flight Fare Prediction Model',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ml', 'flight_fare', 'smart_retraining'],
) as dag:

    # Task 1: Check if new data meets the threshold for retraining
    check_retrain_task = BranchPythonOperator(
        task_id='check_retrain_threshold',
        python_callable=check_new_data_exists
    )

    # Task 2: Run the Training Pipeline (Only if threshold met)
    train_model_task = BashOperator(
        task_id='train_ml_model',
        bash_command='cd /opt/airflow/ml_pipeline && python run_pipeline.py',
        env={
            'DATA_SOURCE': 'postgres',
            'DB_USER': 'analytics_user',
            'DB_PASSWORD': 'analytics_password',
            'DB_HOST': 'postgres_analytics',
            'DB_PORT': '5432',
            'DB_NAME': 'flight_analytics',
        }
    )

    # Task 3: Skip Training (Dummy Task)
    skip_training_task = DummyOperator(
        task_id='skip_training'
    )

    # DAG Flow
    check_retrain_task >> [train_model_task, skip_training_task]
