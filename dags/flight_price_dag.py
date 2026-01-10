from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Ensure scripts can be imported
sys.path.append('/opt/airflow/scripts')

from ingest_csv import ingest_data
from validate_data import validate_data
from transform_kpis import transform_data
from load_postgres import load_data

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

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_postgres",
        python_callable=load_data
    )

    start_task >> ingest_task >> validate_task >> transform_task >> load_task
