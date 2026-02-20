# Airflow Flight Price Pipeline - Gaps Analysis

## Executive Summary

After a comprehensive review of your Airflow flight price analysis pipeline and the integrated machine learning model, I've identified **critical gaps** that violate core requirements and best practices. This document outlines these issues in detail with specific recommendations for improvement.

---

## 🔴 CRITICAL GAPS IN DAG CONFIGURATION

### 1. **Missing Daily Automation** ⚠️ HIGH PRIORITY
**Current State:**
```python
# Line 75 in flight_price_dag.py
schedule_interval=None,
```

**Issue:** The DAG is configured for manual triggering only, which defeats the purpose of an automated pipeline.

**Required:** Daily automated execution to continuously ingest new flight data and retrain the model when thresholds are met.

**Impact:** 
- No automated data updates
- Manual intervention required for every pipeline run
- Defeats the purpose of using Airflow for orchestration

**Recommendation:**
```python
schedule_interval='@daily',  # or '0 2 * * *' for 2 AM daily
```

---

### 2. **Insufficient Retry Configuration** ⚠️ HIGH PRIORITY
**Current State:**
```python
# Line 67 in flight_price_dag.py
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,  # ❌ NO RETRIES
    'on_failure_callback': task_fail_slack_alert
}
```

**Issue:** Zero retries means any transient failure (network issues, database locks, API timeouts) will immediately fail the entire pipeline.

**Required:** At least 2 retries with exponential backoff for resilience.

**Impact:**
- Pipeline fails on transient errors
- Reduced reliability
- Increased manual intervention

**Recommendation:**
```python
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert
}
```

---

### 3. **Missing Task Timeout Configuration** ⚠️ HIGH PRIORITY
**Current State:** No timeout configuration on any tasks.

**Issue:** Tasks can hang indefinitely if:
- Database queries get stuck
- Kaggle download hangs
- ML training enters an infinite loop
- Network connections timeout

**Impact:**
- Zombie tasks consuming resources
- DAG runs never complete
- Scheduler gets blocked

**Recommendation:**
```python
from datetime import timedelta

default_args = {
    # ... existing args ...
    'execution_timeout': timedelta(hours=2),  # Global timeout
}

# Task-specific timeouts
ingest_task = PythonOperator(
    task_id="ingest_data",
    python_callable=ingest_data,
    execution_timeout=timedelta(minutes=30)
)

train_model_task = BashOperator(
    task_id='train_ml_model',
    bash_command='...',
    execution_timeout=timedelta(hours=1)
)
```

---

## 🟡 ARCHITECTURAL GAPS

### 4. **KPI Computation Outside Airflow Tasks** ⚠️ MEDIUM PRIORITY
**Current State:** KPIs are computed as PostgreSQL views (created in `init_db.py`), not as Airflow tasks.

**Issue:** 
- KPI computation is not tracked or monitored by Airflow
- No visibility into when KPIs were last updated
- Cannot retry KPI computation if it fails
- Violates the requirement to compute KPIs within Airflow tasks

**Files Affected:**
- `scripts/init_db.py` (creates views)
- `scripts/update_views.py` (refreshes views, but not in DAG)

**Recommendation:**
Create dedicated Airflow tasks for KPI computation:

```python
def compute_kpi_airline_stats(**context):
    """Compute airline statistics KPI"""
    engine = create_engine(POSTGRES_CONN)
    
    query = """
    INSERT INTO kpi_airline_stats (airline_name, avg_fare, total_bookings, computed_at)
    SELECT 
        da.airline_name,
        AVG(ff.total_fare_bdt) as avg_fare,
        COUNT(*) as total_bookings,
        CURRENT_TIMESTAMP as computed_at
    FROM fact_flights ff
    JOIN dim_airline da ON ff.airline_id = da.airline_id
    GROUP BY da.airline_name
    ON CONFLICT (airline_name) DO UPDATE SET
        avg_fare = EXCLUDED.avg_fare,
        total_bookings = EXCLUDED.total_bookings,
        computed_at = EXCLUDED.computed_at
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(query))
        print(f"Updated {result.rowcount} airline statistics")
    
    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='kpi_airline_count', value=result.rowcount)

# Add to DAG
compute_airline_kpi_task = PythonOperator(
    task_id='compute_airline_kpi',
    python_callable=compute_kpi_airline_stats,
    provide_context=True
)

# Update pipeline flow
etl_task >> compute_airline_kpi_task >> check_retrain_threshold_task
```

---

### 5. **No XCom Usage for Inter-Task Communication** ⚠️ MEDIUM PRIORITY
**Current State:** Tasks don't share data or metrics via XCom.

**Issue:**
- Cannot track how many records were ingested
- Cannot pass validation results between tasks
- Cannot share KPI metrics
- Limited observability and debugging capability

**Recommendation:**
Implement XCom for key metrics:

```python
def ingest_data(**context):
    # ... existing ingestion logic ...
    
    # Push metrics to XCom
    context['ti'].xcom_push(key='records_ingested', value=filtered_count)
    context['ti'].xcom_push(key='records_skipped', value=initial_count - filtered_count)
    
    return filtered_count

def validate_data(**context):
    # Pull ingestion metrics
    ti = context['ti']
    records_ingested = ti.xcom_pull(task_ids='ingest_data', key='records_ingested')
    
    # ... validation logic ...
    
    # Push validation results
    ti.xcom_push(key='records_validated', value=len(df_clean))
    ti.xcom_push(key='records_rejected', value=len(df) - len(df_clean))

def etl_process(**context):
    # Pull validation metrics
    ti = context['ti']
    records_validated = ti.xcom_pull(task_ids='validate_data', key='records_validated')
    
    # ... ETL logic ...
    
    # Push ETL results
    ti.xcom_push(key='fact_records_loaded', value=len(df_fact))
    ti.xcom_push(key='dim_records_loaded', value={
        'airlines': airline_count,
        'locations': location_count,
        'dates': date_count
    })
```

---

### 6. **Hardcoded Connection Strings** ⚠️ MEDIUM PRIORITY
**Current State:**
```python
# scripts/etl_star_schema.py - Lines 6-7
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'
POSTGRES_CONN = 'postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/flight_analytics'

# scripts/ingest_csv.py - Line 9
MYSQL_CONN = 'mysql+mysqlconnector://staging_user:staging_password@mysql_staging:3306/flight_staging'

# dags/flight_price_dag.py - Lines 112-117 (partially hardcoded)
env={
    'DATA_SOURCE': 'postgres',
    'DB_USER': 'analytics_user',
    'DB_PASSWORD': 'analytics_password',  # ❌ Hardcoded
    'DB_HOST': 'postgres_analytics',
    'DB_PORT': '5432',
    'DB_NAME': 'flight_analytics',
}
```

**Issue:**
- Security risk (credentials in code)
- Cannot change environments without code changes
- Violates 12-factor app principles
- Not using Airflow Connections feature

**Recommendation:**
Use Airflow Connections and Variables:

```python
from airflow.hooks.base import BaseHook

def get_mysql_connection():
    """Get MySQL connection from Airflow Connections"""
    conn = BaseHook.get_connection('mysql_staging')
    return f'mysql+mysqlconnector://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'

def get_postgres_connection():
    """Get PostgreSQL connection from Airflow Connections"""
    conn = BaseHook.get_connection('postgres_analytics')
    return f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}'

# In tasks
MYSQL_CONN = get_mysql_connection()
POSTGRES_CONN = get_postgres_connection()
```

---

## 🟢 MACHINE LEARNING PIPELINE GAPS

### 7. **Inconsistent Database Configuration** ⚠️ LOW-MEDIUM PRIORITY
**Current State:**
```python
# src/config.py - Lines 28-32
DB_USER = os.getenv("DB_USER", "analytics_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "analytics_password")
DB_HOST = os.getenv("DB_HOST", "localhost")  # ❌ Wrong default
DB_PORT = os.getenv("DB_PORT", "5434")       # ❌ Host port, not container port
DB_NAME = os.getenv("DB_NAME", "flight_analytics")
```

**Issue:**
- Default host is `localhost` but should be `postgres_analytics` when running in Docker
- Port 5434 is the host-mapped port, not the container port (5432)
- Inconsistent with DAG configuration

**Recommendation:**
```python
# src/config.py
DB_USER = os.getenv("DB_USER", "analytics_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "analytics_password")
DB_HOST = os.getenv("DB_HOST", "postgres_analytics")  # Container hostname
DB_PORT = os.getenv("DB_PORT", "5432")                # Container port
DB_NAME = os.getenv("DB_NAME", "flight_analytics")
```

---

### 8. **Data Leakage Risk Still Present** ⚠️ MEDIUM PRIORITY
**Current State:**
```python
# run_pipeline.py - Lines 49-53
drop_cols = [
    target_col, 
    'Base Fare (BDT)',       # LEAKAGE
    'Tax & Surcharge (BDT)',  # LEAKAGE
]
```

**Issue:** While these columns are dropped, the data loader still fetches them from the database:

```python
# src/data_loader.py - Lines 29-31
ff.base_fare_bdt as "Base Fare (BDT)",
ff.tax_surcharge_bdt as "Tax & Surcharge (BDT)",
ff.total_fare_bdt as "Total Fare (BDT)",
```

**Recommendation:**
Don't fetch leakage columns at all:

```python
# src/data_loader.py
query = """
SELECT 
    da.airline_name as "Airline",
    dd.date as "Date",
    dl_src.city_name as "Source",
    dl_dst.city_name as "Destination",
    ff.departure_time as "Departure Date & Time", 
    ff.arrival_time as "Arrival_Time",
    ff.duration_hrs as "Duration (hrs)",
    dfd.stopovers as "Stopovers",
    dfd.class as "Class",
    dfd.aircraft_type as "Aircraft Type",
    -- ❌ REMOVED: ff.base_fare_bdt as "Base Fare (BDT)",
    -- ❌ REMOVED: ff.tax_surcharge_bdt as "Tax & Surcharge (BDT)",
    ff.total_fare_bdt as "Total Fare (BDT)",  -- Target only
    dd.day as "Day",
    dd.month as "Month",
    dd.year as "Year",
    dd.season as "Season",
    dd.day_of_week as "Day_of_Week",
    dd.is_holiday_window as "Is_Holiday"
FROM fact_flights ff
-- ... rest of query
"""
```

---

### 9. **Missing Days Before Departure Feature** ⚠️ MEDIUM PRIORITY
**Current State:** The `days_before_departure` column exists in `fact_flights` but is NOT fetched by the ML data loader.

**Issue:**
- Critical feature for fare prediction is missing
- Model cannot learn booking timing patterns
- Reduced model accuracy

**Recommendation:**
```python
# src/data_loader.py - Add to query
query = """
SELECT 
    -- ... existing columns ...
    ff.days_before_departure as "Days Before Departure",
    -- ... rest of columns ...
FROM fact_flights ff
-- ... rest of query
"""
```

---

### 10. **No Model Performance Tracking in Airflow** ⚠️ LOW-MEDIUM PRIORITY
**Current State:** Model metrics (R2, MSE) are logged to PostgreSQL but not exposed to Airflow.

**Issue:**
- Cannot monitor model performance degradation over time in Airflow UI
- No alerts if model quality drops
- Limited observability

**Recommendation:**
Add a task to pull and log model metrics:

```python
def log_model_metrics(**context):
    """Pull model metrics from database and log to Airflow"""
    engine = create_engine(POSTGRES_CONN)
    
    query = """
    SELECT r2_score, mse, records_trained_on, training_timestamp
    FROM ml_metadata.model_training_log
    ORDER BY training_timestamp DESC
    LIMIT 1
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()
        
        if row:
            r2_score, mse, records, timestamp = row
            
            # Push to XCom
            context['ti'].xcom_push(key='model_r2_score', value=r2_score)
            context['ti'].xcom_push(key='model_mse', value=mse)
            
            # Log to Airflow
            print(f"Model Performance - R²: {r2_score:.4f}, MSE: {mse:.2f}")
            print(f"Trained on {records:,} records at {timestamp}")
            
            # Alert if performance is poor
            if r2_score < 0.7:
                send_slack_message(
                    f"⚠️ Model performance degraded! R²: {r2_score:.4f} (threshold: 0.7)"
                )

# Add to DAG after training
log_metrics_task = PythonOperator(
    task_id='log_model_metrics',
    python_callable=log_model_metrics,
    provide_context=True
)

train_model_task >> log_metrics_task >> success_notif_task
```

---

## 📊 ADDITIONAL IMPROVEMENTS

### 11. **Missing Data Quality Checks**
**Recommendation:** Add data quality validation tasks:

```python
def check_data_quality(**context):
    """Validate data quality after ETL"""
    engine = create_engine(POSTGRES_CONN)
    
    checks = {
        'null_fares': "SELECT COUNT(*) FROM fact_flights WHERE total_fare_bdt IS NULL",
        'negative_fares': "SELECT COUNT(*) FROM fact_flights WHERE total_fare_bdt < 0",
        'future_dates': "SELECT COUNT(*) FROM fact_flights ff JOIN dim_date dd ON ff.date_id = dd.date_id WHERE dd.date > CURRENT_DATE",
        'orphaned_facts': "SELECT COUNT(*) FROM fact_flights WHERE airline_id NOT IN (SELECT airline_id FROM dim_airline)"
    }
    
    issues = []
    with engine.connect() as conn:
        for check_name, query in checks.items():
            count = conn.execute(text(query)).scalar()
            if count > 0:
                issues.append(f"{check_name}: {count} issues")
    
    if issues:
        raise ValueError(f"Data quality issues found: {', '.join(issues)}")
    
    print("✅ All data quality checks passed")
```

---

### 12. **Missing SLA Configuration**
**Recommendation:**
```python
default_args = {
    # ... existing args ...
    'sla': timedelta(hours=4),  # Pipeline should complete within 4 hours
}

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Alert when SLA is missed"""
    msg = f"⏰ SLA Missed for tasks: {[t.task_id for t in task_list]}"
    send_slack_message(msg)

# In DAG
with DAG(
    # ... existing config ...
    sla_miss_callback=sla_miss_callback,
) as dag:
```

---

## 🎯 PRIORITY IMPLEMENTATION ORDER

### Phase 1 - Critical (Implement Immediately)
1. ✅ Add `schedule_interval='@daily'`
2. ✅ Configure `retries=2` with retry delays
3. ✅ Add task timeouts
4. ✅ Move KPI computation to Airflow tasks

### Phase 2 - Important (Next Sprint)
5. ✅ Implement XCom for inter-task communication
6. ✅ Replace hardcoded connections with Airflow Connections
7. ✅ Fix ML pipeline database configuration
8. ✅ Add `days_before_departure` to ML features

### Phase 3 - Enhancement (Future)
9. ✅ Add model performance tracking task
10. ✅ Implement data quality checks
11. ✅ Configure SLA monitoring
12. ✅ Add comprehensive logging and metrics

---

## 📝 SUMMARY OF VIOLATIONS

| Requirement | Current State | Status |
|-------------|---------------|--------|
| Daily automation | `schedule_interval=None` | ❌ VIOLATED |
| Retries (2+) | `retries=0` | ❌ VIOLATED |
| Task timeouts | Not configured | ❌ VIOLATED |
| KPIs in Airflow tasks | PostgreSQL views only | ❌ VIOLATED |
| XCom usage | Not implemented | ❌ VIOLATED |
| No hardcoded credentials | Credentials in code | ❌ VIOLATED |

---

## 🔧 NEXT STEPS

1. **Review this analysis** with your team
2. **Prioritize fixes** based on the phases above
3. **Create implementation tasks** for each gap
4. **Test thoroughly** in development environment
5. **Deploy incrementally** to production

---

**Document Version:** 1.0  
**Created:** 2026-02-17  
**Author:** AI Code Analysis  
**Status:** Ready for Review
