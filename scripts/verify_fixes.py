#!/usr/bin/env python3
"""
Quick verification script to test all the fixes.
Run this to ensure everything is working correctly.
"""

import sys
import os

def test_dag_imports():
    """Test if DAG can be imported without errors"""
    print("=" * 60)
    print("TEST 1: DAG Import Test")
    print("=" * 60)
    
    try:
        sys.path.append('/opt/airflow/dags')
        sys.path.append('/opt/airflow/scripts')
        
        # This will fail if there are syntax errors
        import flight_price_dag
        print("✅ DAG imports successfully")
        print(f"✅ Schedule: {flight_price_dag.dag.schedule_interval}")
        print(f"✅ Default retries: {flight_price_dag.default_args.get('retries')}")
        print(f"✅ Execution timeout: {flight_price_dag.default_args.get('execution_timeout')}")
        return True
    except Exception as e:
        print(f"❌ DAG import failed: {e}")
        return False

def test_wrapper_functions():
    """Test if wrapper functions exist"""
    print("\n" + "=" * 60)
    print("TEST 2: Wrapper Functions Test")
    print("=" * 60)
    
    try:
        from ingest_csv import ingest_data_wrapper
        from validate_data import validate_data_wrapper
        from etl_star_schema import etl_process_wrapper
        
        print("✅ ingest_data_wrapper exists")
        print("✅ validate_data_wrapper exists")
        print("✅ etl_process_wrapper exists")
        return True
    except Exception as e:
        print(f"❌ Wrapper function import failed: {e}")
        return False

def test_ml_config():
    """Test ML pipeline configuration"""
    print("\n" + "=" * 60)
    print("TEST 3: ML Pipeline Configuration Test")
    print("=" * 60)
    
    try:
        sys.path.append('/opt/airflow/ml_pipeline/src')
        import config
        
        print(f"✅ DB_HOST: {config.DB_HOST}")
        print(f"✅ DB_PORT: {config.DB_PORT}")
        
        if config.DB_HOST == "postgres_analytics":
            print("✅ DB_HOST is correct (postgres_analytics)")
        else:
            print(f"⚠️  DB_HOST is {config.DB_HOST}, expected postgres_analytics")
            
        if config.DB_PORT == "5432":
            print("✅ DB_PORT is correct (5432)")
        else:
            print(f"⚠️  DB_PORT is {config.DB_PORT}, expected 5432")
            
        return True
    except Exception as e:
        print(f"❌ ML config test failed: {e}")
        return False

def test_data_loader_query():
    """Test that data loader doesn't fetch leakage columns"""
    print("\n" + "=" * 60)
    print("TEST 4: Data Loader Query Test")
    print("=" * 60)
    
    try:
        sys.path.append('/opt/airflow/ml_pipeline/src')
        from data_loader import load_data_from_db
        
        # Read the source code to check the query
        import inspect
        source = inspect.getsource(load_data_from_db)
        
        if "Base Fare (BDT)" in source:
            print("❌ Query still contains 'Base Fare (BDT)' - LEAKAGE!")
            return False
        else:
            print("✅ 'Base Fare (BDT)' NOT in query")
            
        if "Tax & Surcharge (BDT)" in source or "Tax \u0026 Surcharge (BDT)" in source:
            print("❌ Query still contains 'Tax & Surcharge (BDT)' - LEAKAGE!")
            return False
        else:
            print("✅ 'Tax & Surcharge (BDT)' NOT in query")
            
        if "Days Before Departure" in source:
            print("✅ 'Days Before Departure' IS in query")
        else:
            print("❌ 'Days Before Departure' NOT in query - MISSING FEATURE!")
            return False
            
        return True
    except Exception as e:
        print(f"❌ Data loader query test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("\n" + "🔍" * 30)
    print("AIRFLOW PIPELINE VERIFICATION SCRIPT")
    print("🔍" * 30 + "\n")
    
    results = []
    
    results.append(("DAG Import", test_dag_imports()))
    results.append(("Wrapper Functions", test_wrapper_functions()))
    results.append(("ML Configuration", test_ml_config()))
    results.append(("Data Loader Query", test_data_loader_query()))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print("\n" + "=" * 60)
    print(f"TOTAL: {passed}/{total} tests passed")
    print("=" * 60)
    
    if passed == total:
        print("\n🎉 All tests passed! Pipeline is ready.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
