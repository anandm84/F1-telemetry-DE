import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="f1_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["f1", "telemetry", "medallion"],
) as dag:
    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:9092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=20 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-bronze-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/bronze_consumer.py & "
            "CONSUMER_PID=$! && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:9092 "
            "python ingestion/producer.py && "
            "wait $CONSUMER_PID"
        ),
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python processing/silver_job.py"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python processing/gold_job.py"
        ),
    )

    ingest_to_bronze >> build_silver >> build_gold
