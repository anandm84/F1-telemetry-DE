# F1 Telemetry Data Engineering Pipeline

End-to-end mini data engineering project for Formula 1 lap timing data using FastF1, Kafka, and Spark with a Bronze/Silver/Gold layout.

## Overview

This project ingests lap data for the **2023 Bahrain Grand Prix race session** and processes it through medallion layers:

1. **Ingestion Producer** fetches lap records from FastF1 and streams them to Kafka.
2. **Bronze Consumer** reads from Kafka and appends raw JSON records to disk.
3. **Silver Job** reads raw JSON with Spark and keeps selected cleaned columns.
4. **Gold Job** aggregates silver data into driver-level summary metrics.

## Architecture

```text
FastF1 API
   |
   v
ingestion/producer.py
   |
   v
Kafka topic: f1_lap_times
   |
   v
ingestion/bronze_consumer.py
   |
   v
data/bronze/laps.json
   |
   v
processing/silver_job.py (Spark)
   |
   v
data/silver/lap_times (Parquet)
   |
   v
processing/gold_job.py (Spark)
   |
   v
data/gold/driver_summary (Parquet)
```

## Tech Stack

- Python
- FastF1
- Kafka (`kafka-python`)
- Apache Spark (PySpark jobs)
- Docker Compose (Kafka, Zookeeper, Spark master/worker)

## Repository Structure

```text
.
|-- airflow/
|   `-- dags/
|       `-- f1_pipeline_dag.py
|-- ingestion/
|   |-- producer.py
|   `-- bronze_consumer.py
|-- processing/
|   |-- silver_job.py
|   `-- gold_job.py
|-- data/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- cache/
|-- compose.yml
|-- requirements.txt
`-- README.md
```

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Internet access (first FastF1 data pull)

## Setup

1. Create and activate a virtual environment.
2. Install Python dependencies:

```bash
pip install -r requirements.txt
pip install pyspark
```

Note: `pyspark` is used by the processing jobs but is not currently listed in `requirements.txt`.

## Start Infrastructure

```bash
docker compose up -d zookeeper kafka spark-master spark-worker
```

Services exposed:

- Kafka: `localhost:9092`
- Spark Master UI: `http://localhost:8080`
- Spark Master endpoint: `spark://localhost:7077`

## Run the Pipeline

Run each step in order.

1. Start bronze consumer (terminal 1):

```bash
python ingestion/bronze_consumer.py
```

2. Start producer (terminal 2):

```bash
python ingestion/producer.py
```

3. Run Silver Spark job (inside Spark container):

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_job.py
```

4. Run Gold Spark job (inside Spark container):

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/gold_job.py
```

## Data Outputs

- Bronze: `data/bronze/laps.json` (newline-delimited JSON)
- Silver: `data/silver/lap_times/` (Parquet)
- Gold: `data/gold/driver_summary/` (Parquet)

## Notes and Current Limitations

- `airflow/dags/f1_pipeline_dag.py` is currently empty; Airflow orchestration is not implemented yet.
- Producer is hardcoded to `fastf1.get_session(2023, 1, 'R')`.
- `lap_time` is stored as string in current flow; averaging in Gold may require explicit time-to-numeric conversion for robust results.
- There is a naming mismatch in repo artifacts: sample data file `laps.ndjson` exists, while consumer writes `laps.json`.

## Next Improvements

- Add configurable race/session parameters (season, round, session type).
- Parse lap times into numeric milliseconds before Silver/Gold aggregation.
- Build a real Airflow DAG for scheduling and dependency management.
- Add tests and data quality checks per layer.
