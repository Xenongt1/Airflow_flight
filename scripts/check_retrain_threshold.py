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
            # Get today's date
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Count total existing records (excluding today's load)
            total_existing_query = text("""
                SELECT COUNT(*) as total_existing
                FROM fact_flights
                WHERE loaded_at::DATE < :today
            """)
            result = conn.execute(total_existing_query, {"today": today})
            total_existing = result.fetchone()[0]
            
            # Count new records loaded today
            new_records_query = text("""
                SELECT COUNT(*) as new_records
                FROM fact_flights
                WHERE loaded_at::DATE = :today
            """)
            result = conn.execute(new_records_query, {"today": today})
            new_records = result.fetchone()[0]
            
            # Handle edge cases
            if total_existing == 0 and new_records == 0:
                print("⚠️  No data found in fact_flights table.")
                print("Decision: SKIP TRAINING (no data available)")
                print("=" * 60)
                return 'skip_training'
            
            if total_existing == 0 and new_records > 0:
                print(f"✅ Initial data load detected: {new_records:,} records")
                print("Decision: TRIGGER TRAINING (initial load)")
                print("=" * 60)
                return 'train_ml_model'
            
            if new_records == 0:
                print(f"📊 Total existing records: {total_existing:,}")
                print(f"📥 New records loaded today: 0")
                print("Decision: SKIP TRAINING (no new data)")
                print("=" * 60)
                return 'skip_training'
            
            # Calculate percentage change
            percentage_change = (new_records / total_existing) * 100
            
            # Display statistics
            print(f"📊 Total existing records: {total_existing:,}")
            print(f"📥 New records loaded today: {new_records:,}")
            print(f"📈 Percentage change: {percentage_change:.2f}%")
            print(f"🎯 Configured threshold: {RETRAIN_THRESHOLD_PERCENT:.2f}%")
            print(f"🔢 Minimum records required: {MIN_RECORDS_FOR_TRAINING:,}")
            print("-" * 60)
            
            # Decision logic
            if new_records < MIN_RECORDS_FOR_TRAINING:
                print(f"⚠️  New records ({new_records:,}) below minimum threshold ({MIN_RECORDS_FOR_TRAINING:,})")
                print("Decision: SKIP TRAINING (insufficient new data)")
                print("=" * 60)
                return 'skip_training'
            
            if percentage_change >= RETRAIN_THRESHOLD_PERCENT:
                print(f"✅ Change ({percentage_change:.2f}%) exceeds threshold ({RETRAIN_THRESHOLD_PERCENT:.2f}%)")
                print("Decision: TRIGGER TRAINING (significant data change)")
                print("=" * 60)
                return 'train_ml_model'
            else:
                print(f"⏭️  Change ({percentage_change:.2f}%) below threshold ({RETRAIN_THRESHOLD_PERCENT:.2f}%)")
                print("Decision: SKIP TRAINING (change not significant)")
                print("=" * 60)
                return 'skip_training'
                
    except Exception as e:
        print(f"❌ Error checking for new data: {e}")
        print("Decision: SKIP TRAINING (error occurred)")
        print("=" * 60)
        # Return skip on error to avoid unnecessary training attempts
        return 'skip_training'

if __name__ == "__main__":
    result = check_new_data_exists()
    print(f"\nFinal Decision: {result}")
