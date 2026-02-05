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

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=None,
        webhook_token=slack_webhook_token,
        message=msg,
        username='Airflow'
    )
    return failed_alert.execute(context=context)

import os

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
