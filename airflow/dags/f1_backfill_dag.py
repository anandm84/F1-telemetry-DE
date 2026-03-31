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
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="f1_backfill",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["f1", "backfill", "medallion"],
    params={
        "years": "2023",
        "rounds": "all",
        "sessions": "R",
        "data_types": "laps,race_results,weather",
        "force": "false",
    },
) as dag:

    run_backfill = BashOperator(
        task_id="run_backfill",
        cwd=PROJECT_ROOT,
        bash_command=(
            "BACKFILL_YEARS={{ params.years }} "
            "BACKFILL_ROUNDS={{ params.rounds }} "
            "BACKFILL_SESSIONS={{ params.sessions }} "
            "BACKFILL_DATA_TYPES={{ params.data_types }} "
            "BACKFILL_FORCE={{ params.force }} "
            "python ingestion/backfill.py"
        ),
    )

    # Run all silver jobs sequentially (SequentialExecutor limitation)
    # spark-submit uses absolute paths so cwd is not needed here
    build_silver_all = BashOperator(
        task_id="build_silver_all",
        bash_command=(
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_job.py && "
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_race_results_job.py && "
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_pit_stops_job.py && "
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/silver_weather_job.py"
        ),
    )

    build_gold_all = BashOperator(
        task_id="build_gold_all",
        cwd=PROJECT_ROOT,
        bash_command=(
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/gold_job.py && "
            # gold_dimensions_job imports fastf1 (not in spark-master), run locally
            "python processing/gold_dimensions_job.py && "
            f"{SPARK_SUBMIT} {PROJECT_ROOT}/processing/gold_facts_job.py"
        ),
    )

    run_backfill >> build_silver_all >> build_gold_all
