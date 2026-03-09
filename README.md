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

8. Build Gold layer (terminal 3):

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
