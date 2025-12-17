"""
DAG 3: ETL Staging
Extracts data from MongoDB, stages in Redis for cleaning/transformation
Runs every 60 seconds
This implements the required staging area for ETL
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'fleet-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'etl_staging_dag',
    default_args=default_args,
    description='ETL staging area - Extract from MongoDB, Stage in Redis',
    schedule_interval=timedelta(seconds=60),  # Every 60 seconds
    catchup=False,
    tags=['etl', 'redis-staging', 'critical'],
)

def extract_from_mongodb():
    """STEP 1: Extract data from MongoDB"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        # Get counts
        vehicles = db.dim_vehicles.count_documents({})
        drivers = db.dim_drivers.count_documents({})
        telemetry = db.telemetry_events.count_documents({})
        
        logging.info(f" EXTRACTED: {vehicles} vehicles, {drivers} drivers, {telemetry} telemetry")
        
        client.close()
        return {
            'vehicles': vehicles,
            'drivers': drivers,
            'telemetry': telemetry
        }
    except Exception as e:
        logging.error(f" Extraction failed: {e}")
        raise

def stage_in_redis():
    """STEP 2: Stage data in Redis for cleaning/transformation"""
    import sys
    sys.path.append('/opt/airflow/dags')
    
    try:
        from spark_jobs.etl.redis_staging import RedisETLStaging
        
        staging = RedisETLStaging(
            mongo_uri='mongodb://admin:admin123@mongodb:27017/',
            redis_host='redis',
            redis_port=6379,
            redis_password='redis123'
        )
        
        # Extract and stage dimensions
        staging.extract_and_stage_dimensions()
        
        # Extract and stage recent facts
        staging.extract_and_stage_facts(hours_back=1)
        
        # Validate staged data
        stats = staging.validate_staged_data()
        
        logging.info(f"🧹 STAGED: {sum(stats.values())} records in Redis")
        
        staging.close()
        return stats
        
    except Exception as e:
        logging.error(f" Staging failed: {e}")
        raise

def validate_data_quality():
    """STEP 3: Validate data quality in staging area"""
    import redis
    
    try:
        r = redis.Redis(
            host='redis',
            port=6379,
            password='redis123',
            decode_responses=True
        )
        
        # Count staged records
        staging_keys = len(r.keys('staging:*'))
        
        if staging_keys > 0:
            logging.info(f" VALIDATED: {staging_keys} records in staging area")
            return True
        else:
            logging.warning("  Staging area is empty")
            return False
            
    except Exception as e:
        logging.error(f" Validation failed: {e}")
        raise

# Task 1: Extract from MongoDB
extract_task = PythonOperator(
    task_id='extract_from_mongodb',
    python_callable=extract_from_mongodb,
    dag=dag,
)

# Task 2: Stage in Redis
stage_task = PythonOperator(
    task_id='stage_in_redis',
    python_callable=stage_in_redis,
    dag=dag,
)

# Task 3: Validate data quality
validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Task 4: Log completion
log_task = BashOperator(
    task_id='log_etl_completion',
    bash_command='echo " ETL staging cycle completed - Redis staging area updated"',
    dag=dag,
)

# Define workflow: Extract → Stage → Validate → Log
extract_task >> stage_task >> validate_task >> log_task
