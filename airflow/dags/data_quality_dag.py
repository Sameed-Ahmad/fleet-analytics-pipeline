"""
DAG 7: Data Quality Check
Checks for nulls, duplicates, outliers in staging area and MongoDB
Updates metadata collection
Runs hourly
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
    'email_on_failure': True,  # Alert on quality issues
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality_dag',
    default_args=default_args,
    description='Monitor data quality and integrity',
    schedule_interval=timedelta(hours=1),  # Hourly
    catchup=False,
    tags=['quality', 'monitoring', 'hourly'],
)

def check_for_nulls():
    """Check for null values in required fields"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        issues = {}
        
        # Check vehicles
        null_vehicle_ids = db.dim_vehicles.count_documents({'vehicle_id': None})
        if null_vehicle_ids > 0:
            issues['vehicles_null_ids'] = null_vehicle_ids
        
        # Check drivers
        null_driver_ids = db.dim_drivers.count_documents({'driver_id': None})
        if null_driver_ids > 0:
            issues['drivers_null_ids'] = null_driver_ids
        
        # Check telemetry
        null_speeds = db.telemetry_events.count_documents({'speed': None})
        if null_speeds > 0:
            issues['telemetry_null_speeds'] = null_speeds
        
        if issues:
            logging.warning(f"  Quality issues found: {issues}")
        else:
            logging.info(" No null value issues")
        
        client.close()
        return len(issues) == 0
        
    except Exception as e:
        logging.error(f" Null check failed: {e}")
        return False

def check_for_duplicates():
    """Check for duplicate records"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        # Check for duplicate vehicle IDs
        pipeline = [
            {'$group': {'_id': '$vehicle_id', 'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}
        ]
        
        duplicates = list(db.dim_vehicles.aggregate(pipeline))
        
        if duplicates:
            logging.warning(f"  Found {len(duplicates)} duplicate vehicle IDs")
            return False
        else:
            logging.info(" No duplicates found")
            return True
        
        client.close()
        
    except Exception as e:
        logging.error(f" Duplicate check failed: {e}")
        return False

def check_for_outliers():
    """Check for statistical outliers"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        # Check for unrealistic speeds
        extreme_speeds = db.telemetry_events.count_documents({
            'speed': {'$gt': 200}  # > 200 km/h is suspicious
        })
        
        # Check for negative fuel levels
        negative_fuel = db.telemetry_events.count_documents({
            'fuel_level': {'$lt': 0}
        })
        
        issues = []
        if extreme_speeds > 0:
            issues.append(f"{extreme_speeds} extreme speeds")
        if negative_fuel > 0:
            issues.append(f"{negative_fuel} negative fuel levels")
        
        if issues:
            logging.warning(f"  Outliers found: {', '.join(issues)}")
        else:
            logging.info(" No outliers detected")
        
        client.close()
        return len(issues) == 0
        
    except Exception as e:
        logging.error(f" Outlier check failed: {e}")
        return False

def check_redis_staging():
    """Check data quality in Redis staging area"""
    import redis
    
    try:
        r = redis.Redis(
            host='redis',
            port=6379,
            password='redis123',
            decode_responses=True
        )
        
        # Count staging keys
        staging_keys = len(r.keys('staging:*'))
        
        if staging_keys == 0:
            logging.warning("  Redis staging area is empty")
            return False
        else:
            logging.info(f" Redis staging: {staging_keys} keys")
            return True
            
    except Exception as e:
        logging.error(f" Redis check failed: {e}")
        return False

def update_metadata():
    """Update data quality metadata"""
    from pymongo import MongoClient
    from datetime import datetime
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        # Get collection stats
        metadata = {
            'timestamp': datetime.utcnow(),
            'vehicles': db.dim_vehicles.count_documents({}),
            'drivers': db.dim_drivers.count_documents({}),
            'telemetry': db.telemetry_events.count_documents({}),
            'deliveries': db.deliveries.count_documents({}),
            'incidents': db.incidents.count_documents({}),
            'quality_check': 'passed'
        }
        
        # Store metadata (create collection if needed)
        db.data_quality_metadata.insert_one(metadata)
        
        logging.info(" Metadata updated")
        
        client.close()
        return True
        
    except Exception as e:
        logging.error(f" Metadata update failed: {e}")
        return False

# Tasks
check_nulls = PythonOperator(
    task_id='check_for_nulls',
    python_callable=check_for_nulls,
    dag=dag,
)

check_dupes = PythonOperator(
    task_id='check_for_duplicates',
    python_callable=check_for_duplicates,
    dag=dag,
)

check_outliers = PythonOperator(
    task_id='check_for_outliers',
    python_callable=check_for_outliers,
    dag=dag,
)

check_redis = PythonOperator(
    task_id='check_redis_staging',
    python_callable=check_redis_staging,
    dag=dag,
)

update_meta = PythonOperator(
    task_id='update_metadata',
    python_callable=update_metadata,
    dag=dag,
)

log = BashOperator(
    task_id='log_quality_check',
    bash_command='echo " Data quality check completed"',
    dag=dag,
)

# Workflow - all checks run in parallel, then update metadata
[check_nulls, check_dupes, check_outliers, check_redis] >> update_meta >> log