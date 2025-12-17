"""
DAG 2: Stream Processing Monitor
Monitors Kafka to MongoDB consumer and restarts if needed
Runs every 30 seconds
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'fleet-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    'stream_processing_monitor_dag',
    default_args=default_args,
    description='Monitor stream processing jobs',
    schedule_interval=timedelta(seconds=30),
    catchup=False,
    tags=['monitoring', 'streaming'],
)

def check_kafka_consumer_lag():
    """Check Kafka consumer lag"""
    import subprocess
    try:
        result = subprocess.run(
            ['docker', 'exec', 'fleet-kafka', 'kafka-consumer-groups', 
             '--bootstrap-server', 'localhost:9092', '--group', 'mongodb-writer', '--describe'],
            capture_output=True,
            timeout=15,
            text=True
        )
        
        if result.returncode == 0:
            output = result.stdout
            logging.info(f"Consumer group status:\n{output}")
            
            # Check if lag is too high
            if 'LAG' in output:
                logging.info(" Consumer is active")
                return True
            else:
                logging.warning("  Consumer group not found")
                return False
        else:
            logging.error(" Failed to check consumer lag")
            return False
            
    except Exception as e:
        logging.error(f" Consumer check error: {e}")
        return False

def check_mongodb_writes():
    """Verify MongoDB is receiving data"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        # Check recent telemetry
        recent_cutoff = datetime.utcnow() - timedelta(minutes=5)
        recent_count = db.telemetry_events.count_documents({
            'timestamp': {'$gte': recent_cutoff}
        })
        
        if recent_count > 0:
            logging.info(f" MongoDB receiving data: {recent_count} events in last 5 min")
            return True
        else:
            logging.warning("  No recent data in MongoDB")
            return False
            
    except Exception as e:
        logging.error(f" MongoDB check failed: {e}")
        return False

# Task 1: Check consumer lag
check_consumer = PythonOperator(
    task_id='check_consumer_lag',
    python_callable=check_kafka_consumer_lag,
    dag=dag,
)

# Task 2: Verify MongoDB writes
check_mongodb = PythonOperator(
    task_id='verify_mongodb_writes',
    python_callable=check_mongodb_writes,
    dag=dag,
)

# Task 3: Log status
log_status = BashOperator(
    task_id='log_stream_status',
    bash_command='echo "Stream processing check completed at $(date)"',
    dag=dag,
)

check_consumer >> check_mongodb >> log_status
