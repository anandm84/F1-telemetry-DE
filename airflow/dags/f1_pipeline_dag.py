import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator # pyright: ignore[reportMissingImports]

PROJECT_ROOT = os.getenv("F1_PROJECT_ROOT", "/opt/project")
DBT_DIR = f"{PROJECT_ROOT}/dbt"
DBT_RUN = f"cd {DBT_DIR} && DBT_PROFILES_DIR=."

# Trigger with conf, e.g.:
#   {"year": 2024, "round": "all", "session": "all"}
# Round and session each accept an int/code, "all", or a comma-separated list.
RACE_YEAR_DEFAULT = "2024"
RACE_ROUND_DEFAULT = "all"
RACE_SESSION_DEFAULT = "all"
SPEED_FACTOR_DEFAULT = "0"  # batch mode: no replay delay

YEAR_TPL = f"{{{{ dag_run.conf.get('year', '{RACE_YEAR_DEFAULT}') }}}}"
ROUND_TPL = f"{{{{ dag_run.conf.get('round', '{RACE_ROUND_DEFAULT}') }}}}"
SESSION_TPL = f"{{{{ dag_run.conf.get('session', '{RACE_SESSION_DEFAULT}') }}}}"
SPEED_TPL = f"{{{{ dag_run.conf.get('speed_factor', '{SPEED_FACTOR_DEFAULT}') }}}}"

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
    params={
        "year": RACE_YEAR_DEFAULT,
        "round": RACE_ROUND_DEFAULT,
        "session": RACE_SESSION_DEFAULT,
        "speed_factor": SPEED_FACTOR_DEFAULT,
    },
) as dag:

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        cwd=PROJECT_ROOT,
        bash_command=(
            "mkdir -p data/bronze/laps data/bronze/race_results data/bronze/weather && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-laps-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/bronze_consumer.py & "
            "LAPS_PID=$! && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-results-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/results_consumer.py & "
            "RESULTS_PID=$! && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            "BRONZE_EXIT_ON_IDLE=true "
            "BRONZE_MAX_IDLE_SECONDS=120 "
            "KAFKA_AUTO_OFFSET_RESET=latest "
            "KAFKA_GROUP_ID=f1-weather-{{ ts_nodash }} "
            "BRONZE_RUN_ID={{ ts_nodash }} "
            "python ingestion/weather_consumer.py & "
            "WEATHER_PID=$! && "
            f"KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
            f"RACE_YEAR={YEAR_TPL} "
            f"RACE_ROUND={ROUND_TPL} "
            f"RACE_SESSION={SESSION_TPL} "
            f"SPEED_FACTOR={SPEED_TPL} "
            "python ingestion/producer.py && "
            "wait"
        ),
    )

    refresh_seeds = BashOperator(
        task_id="refresh_seeds",
        cwd=PROJECT_ROOT,
        bash_command=f"F1_SEED_YEARS={YEAR_TPL} python ingestion/load_schedules.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_RUN} dbt seed && {DBT_RUN} dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_RUN} dbt test",
    )

    [ingest_to_bronze, refresh_seeds] >> dbt_build >> dbt_test
