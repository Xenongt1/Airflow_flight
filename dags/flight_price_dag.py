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

def send_slack_message(msg):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')
    if not webhook_url:
        print(f"No SLACK_WEBHOOK_URL defined. Log message: {msg}")
        return

    payload = {
        "text": msg,
        "username": "Airflow",
        "icon_emoji": ":airplane:"
    }
    try:
        import requests
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("Slack alert sent successfully.")
    except Exception as e:
        print(f"Failed to send Slack alert: {e}")

def task_fail_slack_alert(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    date = context.get('execution_date')
    
    msg = (f":red_circle: *Task Failed*\n"
           f"*Task*: {task_instance.task_id}\n"
           f"*Dag*: {dag_run.dag_id}\n"
           f"*Execution Time*: {date}\n"
           f"*Log Url*: {task_instance.log_url}")
    send_slack_message(msg)

def notify_training_success():
    msg = ":white_check_mark: *Model Training Success!*\nThe Flight Fare Prediction model has been retrained and the new version is now saved for production."
    send_slack_message(msg)

def notify_skipped():
    msg = ":fast_forward: *Retraining Skipped*\nSmart check determined there is no significant new data to justify a retrain. Using existing model."
    send_slack_message(msg)

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

    # Smart Retraining Check
    check_retrain_threshold_task = BranchPythonOperator(
        task_id='check_retrain_threshold',
        python_callable=check_new_data_exists
    )

    # ML Training Task - Only runs if new data exists
    train_model_task = BashOperator(
        task_id='train_ml_model',
        bash_command='cd /opt/airflow/ml_pipeline && pip install --no-cache-dir -r requirements.txt && python run_pipeline.py',
        env={
            'DATA_SOURCE': 'postgres',  # Force database source
            'DB_USER': 'analytics_user',
            'DB_PASSWORD': 'analytics_password',
            'DB_HOST': 'postgres_analytics',
            'DB_PORT': '5432',
            'DB_NAME': 'flight_analytics',
        }
    )

    # Notification tasks
    success_notif_task = PythonOperator(
        task_id='notify_success',
        python_callable=notify_training_success
    )

    skip_notif_task = PythonOperator(
        task_id='skip_training_notification',
        python_callable=notify_skipped
    )

    # Pipeline Flow: ETL → Check Retraining (Smart Trigger) → [Train Model OR Skip]
    start_task >> ingest_task >> validate_task >> etl_task >> check_retrain_threshold_task
    check_retrain_threshold_task >> train_model_task >> success_notif_task
    check_retrain_threshold_task >> skip_notif_task
