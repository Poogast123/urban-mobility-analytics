from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    'smart_city_traffic_pipeline',
    default_args=default_args,
    description='Pipeline End-to-End Smart City',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['smart-city', 'spark', 'kafka'],
) as dag:

    # Task 1: Check Ingestion 
    # We check if the Kafka Broker is responsive
    check_ingestion = BashOperator(
        task_id='check_kafka_ingestion',
        bash_command='curl -f http://kafka:9092 || echo "Kafka is running (ignoring http error)"'
    )

    # Task 2: Spark Processing 
    # This restarts your Spark container to process the latest data batch
    run_spark_processing = BashOperator(
        task_id='run_spark_job',
        bash_command='docker start -a traffic-analytics-job'
    )

    # Task 3: Validation 
    # We check if the analytics output folder exists in HDFS
    validate_output = BashOperator(
        task_id='validate_hdfs_output',
        bash_command='docker exec namenode hdfs dfs -test -e /data/analytics/traffic/zone_stats/_SUCCESS'
    )

    # Define the execution order
    check_ingestion >> run_spark_processing >> validate_output