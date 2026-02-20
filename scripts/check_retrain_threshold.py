"""
Smart Model Retraining Trigger - Percentage-Based Approach

This script calculates the percentage of new data added to PostgreSQL
and only triggers model retraining when the change exceeds a configurable threshold.

Used by Airflow DAG to make intelligent retraining decisions.
"""
import os
from sqlalchemy import create_engine, text
from datetime import datetime

def check_new_data_exists():
    """
    Calculate the percentage of new data and decide whether to retrain the model.
    
    Logic:
    1. Count total records in fact_flights (excluding today's load)
    2. Count new records loaded today
    3. Calculate percentage change: (new / total_existing) * 100
    4. Compare against threshold (default: 5%)
    
    Returns:
        str: 'train_ml_model' if change >= threshold, 'skip_training' otherwise
    """
    # Configuration from environment variables
    DB_USER = os.getenv('DB_USER', 'analytics_user')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'analytics_password')
    DB_HOST = os.getenv('DB_HOST', 'postgres_analytics')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'flight_analytics')
    
    # Retraining thresholds
    RETRAIN_THRESHOLD_PERCENT = float(os.getenv('RETRAIN_THRESHOLD_PERCENT', '5.0'))
    MIN_RECORDS_FOR_TRAINING = int(os.getenv('MIN_RECORDS_FOR_TRAINING', '100'))
    
    connection_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    print("=" * 60)
    print("SMART RETRAINING DECISION ENGINE")
    print("=" * 60)
    
    try:
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # 1. Get the last training timestamp from metadata
            last_training_query = text("""
                SELECT training_timestamp, records_trained_on
                FROM ml_metadata.model_training_log
                WHERE model_name = 'flight_fare_predictor'
                ORDER BY training_timestamp DESC
                LIMIT 1
            """)
            result = conn.execute(last_training_query)
            row = result.fetchone()
            
            # 2. Get the total current record count
            total_current_query = text("SELECT COUNT(*) FROM fact_flights")
            total_current = conn.execute(total_current_query).scalar() or 0
            
            if row is None:
                if total_current >= MIN_RECORDS_FOR_TRAINING:
                    print(f"✅ No previous training found. Initial data available: {total_current:,} records.")
                    print("Decision: TRIGGER TRAINING (initial load)")
                    print("=" * 60)
                    return 'train_ml_model'
                else:
                    print(f"⚠️  No previous training found, but insufficient data: {total_current:,}/{MIN_RECORDS_FOR_TRAINING}")
                    print("Decision: SKIP TRAINING")
                    print("=" * 60)
                    return 'skip_training'
            
            last_training_time = row[0]
            records_at_last_train = row[1] or 0
            
            # 3. Count new records added SINCE the last training
            new_records_query = text("""
                SELECT COUNT(*) as new_records
                FROM fact_flights
                WHERE loaded_at > :last_train
            """)
            result = conn.execute(new_records_query, {"last_train": last_training_time})
            new_records = result.fetchone()[0]
            
            # Calculate percentage change based on what the model was LAST trained on
            # If records_at_last_train is 0 for some reason, use total_current - new_records
            base_count = records_at_last_train if records_at_last_train > 0 else (total_current - new_records)
            
            if base_count > 0:
                percentage_change = (new_records / base_count) * 100
            else:
                percentage_change = 100.0 if new_records > 0 else 0.0
            
            # Display statistics
            print(f"🕒 Last training occurred at: {last_training_time}")
            print(f"📊 Records at last training: {base_count:,}")
            print(f"📥 New records accumulated since then: {new_records:,}")
            print(f"📈 Cumulative data growth: {percentage_change:.2f}%")
            print(f"🎯 Threshold for retraining: {RETRAIN_THRESHOLD_PERCENT:.2f}%")
            print(f"🔢 Min new records required: {MIN_RECORDS_FOR_TRAINING:,}")
            print("-" * 60)
            
            # Decision logic
            if new_records < MIN_RECORDS_FOR_TRAINING:
                print(f"⏭️  New records ({new_records:,}) below minimum required ({MIN_RECORDS_FOR_TRAINING:,})")
                print("Decision: SKIP TRAINING")
                print("=" * 60)
                return 'skip_training_notification'
            
            if percentage_change >= RETRAIN_THRESHOLD_PERCENT:
                print(f"✅ Growth ({percentage_change:.2f}%) meets threshold ({RETRAIN_THRESHOLD_PERCENT:.2f}%)")
                print("Decision: TRIGGER TRAINING")
                print("=" * 60)
                return 'train_ml_model'
            else:
                print(f"⏭️  Growth ({percentage_change:.2f}%) below threshold")
                print("Decision: SKIP TRAINING")
                print("=" * 60)
                return 'skip_training_notification'
                
    except Exception as e:
        print(f"❌ Error checking for new data: {e}")
        print("Decision: SKIP TRAINING (error occurred)")
        print("=" * 60)
        # Return skip on error to avoid unnecessary training attempts
        return 'skip_training_notification'

if __name__ == "__main__":
    result = check_new_data_exists()
    print(f"\nFinal Decision: {result}")
