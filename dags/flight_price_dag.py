from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Ensure scripts can be imported
sys.path.append('/opt/airflow/scripts')

from ingest_csv import ingest_data
from validate_data import validate_data
from etl_star_schema import etl_process

def start_pipeline():
    print("Flight Price Pipeline Started")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
    dag_id="flight_price_analysis_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["flight", "bangladesh", "etl"],
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    # New ETL Task (Star Schema + SQL Transformation)
    etl_task = PythonOperator(
        task_id="etl_star_schema",
        python_callable=etl_process
    )

    start_task >> ingest_task >> validate_task >> etl_task
