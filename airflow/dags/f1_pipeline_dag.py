import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
SPARK_SUBMIT = "docker exec spark-master /opt/spark/bin/spark-submit"

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
        cwd=PROJECT_ROOT,  # sets working dir for the whole process tree incl. backgrounded children
        bash_command=(
            "mkdir -p data/bronze/laps data/bronze/race_results data/bronze/weather && "
            # Laps consumer
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-laps-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/bronze_consumer.py & "
            "LAPS_PID=$! && "
            # Results consumer
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-results-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/results_consumer.py & "
            "RESULTS_PID=$! && "
            # Weather consumer
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-weather-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/weather_consumer.py & "
            "WEATHER_PID=$! && "
            # Unified producer (blocking)
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "python ingestion/producer.py && "
            # Wait for all consumers to drain and exit
            "wait"
        ),
    )

    build_silver_laps = BashOperator(
        task_id="build_silver_laps",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_job.py",
    )

    build_silver_results = BashOperator(
        task_id="build_silver_results",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_race_results_job.py",
    )

    build_silver_pit_stops = BashOperator(
        task_id="build_silver_pit_stops",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_pit_stops_job.py",
    )

    build_silver_weather = BashOperator(
        task_id="build_silver_weather",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_weather_job.py",
    )

    build_gold_legacy = BashOperator(
        task_id="build_gold_legacy",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/gold_job.py",
    )

    # gold_dimensions_job imports fastf1 (not installed in spark-master) so it
    # runs via Python in the Airflow container, which has both fastf1 and Java/PySpark.
    build_gold_dimensions = BashOperator(
        task_id="build_gold_dimensions",
        cwd=PROJECT_ROOT,
        bash_command="python processing/gold_dimensions_job.py",
    )

    build_gold_facts = BashOperator(
        task_id="build_gold_facts",
        bash_command=f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/gold_facts_job.py",
    )

    # All silver jobs depend on ingest completing
    ingest_to_bronze >> [build_silver_laps, build_silver_results, build_silver_pit_stops, build_silver_weather]

    # Gold legacy (driver_pace, tire_performance, sector_analysis) needs silver laps
    build_silver_laps >> build_gold_legacy

    # Dimensions need silver results; facts need dimensions + all silver
    build_silver_results >> build_gold_dimensions
    [build_silver_laps, build_silver_results, build_silver_pit_stops, build_silver_weather, build_gold_dimensions] >> build_gold_facts
