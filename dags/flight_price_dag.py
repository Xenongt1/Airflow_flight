from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import sys

# Ensure scripts can be imported
sys.path.append('/opt/airflow/scripts')

from ingest_csv import ingest_data
from validate_data import validate_data
from etl_star_schema import etl_process
from check_retrain_threshold import check_new_data_exists

def start_pipeline():
    print("Flight Price Pipeline Started")

import os

# Slack Alert Function
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_fail_slack_alert(context):
    slack_webhook_token = os.environ.get('SLACK_WEBHOOK_URL')
    if not slack_webhook_token:
        print("No SLACK_WEBHOOK_URL defined, skipping alert.")
        return

    # Extract info from context
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    date = context.get('execution_date')
    
    msg = (f":red_circle: *Task Failed*\n"
           f"*Task*: {task_instance.task_id}\n"
           f"*Dag*: {dag_run.dag_id}\n"
           f"*Execution Time*: {date}\n"
           f"*Log Url*: {task_instance.log_url}")

    import requests
    import json
    
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print("No SLACK_WEBHOOK_URL defined, skipping alert.")
        return

    payload = {
        "text": msg,
        "username": "Airflow",
        "icon_emoji": ":airplane:"
    }

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Slack alert sent successfully.")
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    dag_id="flight_price_analysis_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["flight", "bangladesh", "etl", "ml"],
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

    # Check if new data exists before training
    check_new_data_task = BranchPythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data_exists
    )

    # ML Training Task - Only runs if new data exists
    train_model_task = BashOperator(
        task_id='train_ml_model',
        bash_command='cd /opt/airflow/ml_pipeline && python run_pipeline.py',
        env={
            'DATA_SOURCE': 'postgres',  # Force database source
        }
    )

    # Dummy task for when training is skipped
    skip_training_task = DummyOperator(
        task_id='skip_training'
    )

    # Pipeline Flow: ETL → Check New Data → [Train Model OR Skip]
    start_task >> ingest_task >> validate_task >> etl_task >> check_new_data_task
    check_new_data_task >> [train_model_task, skip_training_task]
