from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure scripts can be imported
sys.path.append('/opt/airflow/scripts')

from ingest_csv import ingest_data_wrapper
from validate_data import validate_data_wrapper
from etl_star_schema import etl_process_wrapper
from check_retrain_threshold import check_new_data_exists

def start_pipeline(**context):
    """Start the pipeline and log initial state"""
    print("=" * 60)
    print("Flight Price Pipeline Started")
    print(f"Execution Date: {context.get('execution_date')}")
    print("=" * 60)
    return "Pipeline started successfully"

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

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert when SLA is missed"""
    task_ids = [t.task_id for t in task_list]
    msg = f":alarm_clock: *SLA Missed*\n*Tasks*: {', '.join(task_ids)}\n*DAG*: {dag.dag_id}"
    send_slack_message(msg)

def notify_training_success(**context):
    """Send success notification with metrics"""
    ti = context['ti']
    
    # Try to pull model metrics from XCom
    r2_score = ti.xcom_pull(task_ids='train_ml_model', key='model_r2_score')
    mse = ti.xcom_pull(task_ids='train_ml_model', key='model_mse')
    
    msg = ":white_check_mark: *Model Training Success!*\nThe Flight Fare Prediction model has been retrained and the new version is now saved for production."
    
    if r2_score is not None:
        msg += f"\n*R² Score*: {r2_score:.4f}"
    if mse is not None:
        msg += f"\n*MSE*: {mse:.2f}"
    
    send_slack_message(msg)

def notify_skipped():
    msg = ":fast_forward: *Retraining Skipped*\nSmart check determined there is no significant new data to justify a retrain. Using existing model."
    send_slack_message(msg)

# Updated default args with retries, timeouts, and SLA
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,  # ✅ FIX: Added retries
    'retry_delay': timedelta(minutes=5),  # ✅ FIX: Retry delay
    'retry_exponential_backoff': True,  # ✅ FIX: Exponential backoff
    'max_retry_delay': timedelta(minutes=30),  # ✅ FIX: Max retry delay
    'execution_timeout': timedelta(hours=2),  # ✅ FIX: Global timeout
    'sla': timedelta(hours=4),  # ✅ FIX: SLA configuration
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    dag_id="flight_price_analysis_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',  # ✅ FIX: Daily automation
    catchup=False,
    tags=["flight", "bangladesh", "etl", "ml"],
    description="Daily flight price analysis pipeline with ML model retraining",
    sla_miss_callback=sla_miss_callback,  # ✅ FIX: SLA monitoring
) as dag:

    start_task = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
        provide_context=True,
        execution_timeout=timedelta(minutes=5)  # ✅ FIX: Task-specific timeout
    )

    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_wrapper,
        provide_context=True,  # ✅ FIX: Enable XCom
        execution_timeout=timedelta(minutes=30)  # ✅ FIX: Task-specific timeout
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data_wrapper,
        provide_context=True,  # ✅ FIX: Enable XCom
        execution_timeout=timedelta(minutes=20)  # ✅ FIX: Task-specific timeout
    )

    # ETL Task (Star Schema + SQL Transformation)
    etl_task = PythonOperator(
        task_id="etl_star_schema",
        python_callable=etl_process_wrapper,
        provide_context=True,
        execution_timeout=timedelta(minutes=45)
    )

    def compute_kpis(**context):
        """Compute pipeline KPIs and log summary results"""
        from sqlalchemy import create_engine, text
        DB_USER = os.getenv('DB_USER', 'analytics_user')
        DB_PASSWORD = os.getenv('DB_PASSWORD', 'analytics_password')
        DB_HOST = os.getenv('DB_HOST', 'postgres_analytics')
        DB_NAME = os.getenv('DB_NAME', 'flight_analytics')
        POSTGRES_CONN = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}'
        
        engine = create_engine(POSTGRES_CONN)
        with engine.begin() as conn:
            # Aggregate Airline Stats
            print("Computing Airline Fare KPIs...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS kpi_airline_summary AS 
                SELECT da.airline_name, AVG(ff.total_fare_bdt) as avg_fare, COUNT(*) as flight_count
                FROM fact_flights ff 
                JOIN dim_airline da ON ff.airline_id = da.airline_id
                GROUP BY 1
            """))
        return "KPIs Computed Successfully"

    kpi_task = PythonOperator(
        task_id="compute_kpis",
        python_callable=compute_kpis,
        provide_context=True
    )

    # Smart Retraining Check
    check_retrain_threshold_task = BranchPythonOperator(
        task_id='check_retrain_threshold',
        python_callable=check_new_data_exists,
        execution_timeout=timedelta(minutes=10)  # ✅ FIX: Task-specific timeout
    )

    # ML Training Task - Only runs if new data exists
    train_model_task = BashOperator(
        task_id='train_ml_model',
        bash_command='cd /opt/airflow/ml_pipeline && pip install --no-cache-dir -r requirements.txt && python run_pipeline.py',
        env={
            'DATA_SOURCE': 'postgres',  # Force database source
            'DB_USER': os.getenv('DB_USER', 'analytics_user'),  # ✅ IMPROVED: Use env vars
            'DB_PASSWORD': os.getenv('DB_PASSWORD', 'analytics_password'),
            'DB_HOST': os.getenv('DB_HOST', 'postgres_analytics'),
            'DB_PORT': os.getenv('DB_PORT', '5432'),
            'DB_NAME': os.getenv('DB_NAME', 'flight_analytics'),
        },
        execution_timeout=timedelta(hours=1)  # ✅ FIX: Task-specific timeout
    )

    # Notification tasks
    success_notif_task = PythonOperator(
        task_id='notify_success',
        python_callable=notify_training_success,
        provide_context=True,  # ✅ FIX: Enable XCom access
        execution_timeout=timedelta(minutes=5)
    )

    skip_notif_task = PythonOperator(
        task_id='skip_training_notification',
        python_callable=notify_skipped,
        execution_timeout=timedelta(minutes=5)
    )

    wait_task = DummyOperator(
        task_id='wait_for_notification',
        trigger_rule='one_success'
    )

    # Pipeline Flow: ETL → KPIs → Check Retraining (Smart Trigger) → [Train Model OR Skip]
    start_task >> ingest_task >> validate_task >> etl_task >> kpi_task >> check_retrain_threshold_task
    check_retrain_threshold_task >> train_model_task >> success_notif_task >> wait_task
    check_retrain_threshold_task >> skip_notif_task >> wait_task
