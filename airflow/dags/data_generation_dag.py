"""
DAG 1: Data Generation
Continuously generates fleet data and publishes to Kafka
Runs every 3 seconds (high frequency)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# Default arguments
default_args = {
    'owner': 'fleet-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

# Create DAG
dag = DAG(
    'data_generation_dag',
    default_args=default_args,
    description='Generate fleet data and publish to Kafka',
    schedule_interval=timedelta(seconds=3),  # Run every 3 seconds
    catchup=False,
    tags=['generation', 'kafka', 'high-frequency'],
)

def check_kafka_health():
    """Check if Kafka is accessible"""
    import subprocess
    try:
        result = subprocess.run(
            ['docker', 'exec', 'fleet-kafka', 'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'],
            capture_output=True,
            timeout=10
        )
        if result.returncode == 0:
            logging.info(" Kafka is healthy")
            return True
        else:
            logging.error(" Kafka health check failed")
            return False
    except Exception as e:
        logging.error(f" Kafka health check error: {e}")
        return False

def generate_batch_data():
    """Generate one batch of fleet data"""
    import sys
    import os
    sys.path.append('/opt/airflow/dags')
    
    try:
        # Import generator (adjust path as needed)
        from data_generators.fleet_generator_kafka import FleetDataGenerator
        from kafka_services.producers.fleet_producer import FleetKafkaProducer
        
        # Create producer
        producer = FleetKafkaProducer(bootstrap_servers='kafka:29092')
        
        # Create generator
        generator = FleetDataGenerator(kafka_producer=producer)
        
        # Run one generation cycle
        generator.run_generation_cycle()
        
        logging.info(" Generated and published data batch")
        
        # Close producer
        producer.close()
        
        return True
    except Exception as e:
        logging.error(f" Data generation failed: {e}")
        raise

# Task 1: Check Kafka health
check_kafka = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Generate data
generate_data = PythonOperator(
    task_id='generate_data_batch',
    python_callable=generate_batch_data,
    dag=dag,
)

# Task 3: Log statistics
log_stats = BashOperator(
    task_id='log_generation_stats',
    bash_command='echo "Data generation cycle completed at $(date)"',
    dag=dag,
)

# Define task dependencies
check_kafka >> generate_data >> log_stats
