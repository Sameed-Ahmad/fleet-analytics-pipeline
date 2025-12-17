"""
DAG 4: Hot-to-Warm Archiving
Archives data from MongoDB (hot) to HDFS warm tier
Runs daily at 2 AM
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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hot_to_warm_archiving_dag',
    default_args=default_args,
    description='Archive data from MongoDB to HDFS warm tier',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['archiving', 'hdfs', 'daily'],
)

def identify_old_data():
    """Identify data older than 48 hours for archiving"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        cutoff = datetime.utcnow() - timedelta(hours=48)
        
        # Count old telemetry events
        old_count = db.telemetry_events.count_documents({
            'timestamp': {'$lt': cutoff}
        })
        
        logging.info(f" Found {old_count} events older than 48 hours")
        
        client.close()
        return old_count
        
    except Exception as e:
        logging.error(f" Failed to identify old data: {e}")
        raise

def export_to_parquet():
    """Export old data to Parquet format"""
    import pandas as pd
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    import os
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        cutoff = datetime.utcnow() - timedelta(hours=48)
        
        # Export telemetry
        old_data = list(db.telemetry_events.find(
            {'timestamp': {'$lt': cutoff}},
            {'_id': 0}
        ).limit(10000))
        
        if old_data:
            df = pd.DataFrame(old_data)
            
            # Create export directory
            os.makedirs('/tmp/archive', exist_ok=True)
            
            # Save as Parquet
            filename = f'/tmp/archive/telemetry_{datetime.now().strftime("%Y%m%d")}.parquet'
            df.to_parquet(filename, compression='snappy')
            
            logging.info(f" Exported {len(old_data)} records to {filename}")
            return filename
        else:
            logging.info("No data to archive")
            return None
            
    except Exception as e:
        logging.error(f" Export failed: {e}")
        raise

def upload_to_hdfs():
    """Upload Parquet file to HDFS warm tier"""
    import subprocess
    import os
    
    try:
        parquet_file = '/tmp/archive/telemetry_*.parquet'
        
        # Check if file exists
        if os.path.exists('/tmp/archive/'):
            # Upload to HDFS
            result = subprocess.run(
                ['docker', 'exec', 'fleet-namenode', 'hdfs', 'dfs', '-put', 
                 '/tmp/archive/*.parquet', '/warehouse/warm/'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                logging.info(" Uploaded to HDFS /warehouse/warm/")
                return True
            else:
                logging.warning(f"  HDFS upload: {result.stderr}")
                return False
        else:
            logging.info("No files to upload")
            return True
            
    except Exception as e:
        logging.error(f" HDFS upload failed: {e}")
        return False

def delete_archived_data():
    """Delete archived data from MongoDB"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    
    try:
        client = MongoClient('mongodb://admin:admin123@mongodb:27017/')
        db = client['fleet_analytics']
        
        cutoff = datetime.utcnow() - timedelta(hours=48)
        
        # Delete old telemetry
        result = db.telemetry_events.delete_many({
            'timestamp': {'$lt': cutoff}
        })
        
        logging.info(f"  Deleted {result.deleted_count} archived records from MongoDB")
        
        client.close()
        return result.deleted_count
        
    except Exception as e:
        logging.error(f" Deletion failed: {e}")
        raise

# Tasks
identify = PythonOperator(
    task_id='identify_old_data',
    python_callable=identify_old_data,
    dag=dag,
)

export = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_to_parquet,
    dag=dag,
)

upload = PythonOperator(
    task_id='upload_to_hdfs',
    python_callable=upload_to_hdfs,
    dag=dag,
)

delete = PythonOperator(
    task_id='delete_archived_data',
    python_callable=delete_archived_data,
    dag=dag,
)

log = BashOperator(
    task_id='log_archiving',
    bash_command='echo " Hot-to-warm archiving completed"',
    dag=dag,
)

# Workflow
identify >> export >> upload >> delete >> log