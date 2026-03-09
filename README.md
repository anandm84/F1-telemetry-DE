# F1 Telemetry Data Engineering Pipeline

End-to-end mini data engineering project for Formula 1 lap timing data using FastF1, Kafka, and Spark with a Bronze/Silver/Gold layout.

## Overview

This project ingests lap data for a configurable FastF1 session and processes it through medallion layers:

1. **Ingestion Producer** fetches lap records from FastF1 and streams them to Kafka.
2. **Bronze Consumer** reads from Kafka and writes raw NDJSON shards grouped by driver+session (with Kafka metadata) to disk.
3. **Silver Job** runs as a compact Spark batch transform with schema enforcement, timestamp normalization, deduplication, and type-safe metrics.
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
data/bronze/laps.ndjson.session-<SESSION>.driver-<DRIVER>.ndjson
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

## How to Run (End-to-End)

From the project root, run these steps in order.

1. Create and activate a virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Start infrastructure:

```bash
docker compose up -d zookeeper kafka spark-master spark-worker
```

4. Confirm services are up:

```bash
docker ps
```

Expected key endpoints:

- Kafka: `localhost:9092`
- Spark Master UI: `http://localhost:8080`
- Spark Master endpoint: `spark://localhost:7077`

5. Start Bronze consumer (terminal 1):

```bash
python ingestion/bronze_consumer.py
```

6. Start Producer (terminal 2):

```bash
python ingestion/producer.py
```

7. Build Silver layer (terminal 3):

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_job.py
```

8. Build Gold layer (terminal 4):

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/gold_job.py
```

9. Verify outputs:

```bash
ls data/bronze
ls data/silver/lap_times
ls data/gold/driver_summary
```

10. Stop infrastructure when done:

```bash
docker compose down
```

## Data Outputs

- Bronze: `data/bronze/laps.ndjson.session-<SESSION>.driver-<DRIVER>.ndjson` (one append-only NDJSON file per driver+session; raw payload + Kafka metadata)
- Silver: `data/silver/lap_times/` (Parquet)
- Gold: `data/gold/driver_summary/` (Parquet)

## Notes and Current Limitations

- `airflow/dags/f1_pipeline_dag.py` is currently empty; Airflow orchestration is not implemented yet.
- Gold aggregation is still batch-style and should be migrated to streaming aggregations in the next phase.
- Bronze flush behavior can be tuned with `BRONZE_BATCH_SIZE` and `BRONZE_FLUSH_INTERVAL_SECONDS` env vars.
- Bronze idle finalize/exit can be tuned with `BRONZE_MAX_IDLE_SECONDS` and `BRONZE_EXIT_ON_IDLE`.
- Bronze consumer offset replay behavior can be tuned with `KAFKA_AUTO_OFFSET_RESET` (default `earliest`).
- Rerun policy options: set `BRONZE_RUN_ID` to write run-specific files, or set `BRONZE_CLEAR_SESSION_ON_START=true` (+ `BRONZE_TARGET_SESSION`) to clear files before replay.
- Silver output file count can be tuned with `SILVER_OUTPUT_FILES` (default `1`).
- Producer emits lap/sector durations as `HH:MM:SS:MMMM` for easier downstream parsing.

## Next Improvements

- Add driver/race dimension tables for richer analytics joins.
- Migrate Gold to watermark-aware streaming aggregations.
- Build a real Airflow DAG for scheduling and dependency management.
- Add tests and data quality checks per layer.
