# F1 Telemetry Data Engineering Pipeline

End-to-end data engineering project for Formula 1 telemetry data using FastF1, Kafka, and Spark with a Bronze/Silver/Gold medallion architecture. Designed as a foundation for ML models, analytics dashboards, and an MCP server.

## Overview

The pipeline ingests multiple data types from FastF1 for any configurable session, streams them through Kafka into raw Bronze files, then processes them through typed Silver and analytics-ready Gold layers.

**Data types ingested:**
- **Lap times** — per-driver lap, sector times, tire compound, pit stop flags
- **Race results** — final positions, grid positions, points, DNF status, team, nationality
- **Weather snapshots** — track/air temp, humidity, pressure, wind speed, rainfall

**Gold layer outputs:**
- Aggregation tables — driver pace, tire performance, sector analysis
- Dimension tables — `dim_drivers`, `dim_circuits`, `dim_races`
- Fact tables — `fact_race_results`, `fact_pit_stops`, `fact_weather_snapshots`

## Architecture

```
FastF1 API
    |
    v
ingestion/producer.py  (unified -- loads session once, emits all data types)
    |
    |-->  Kafka: f1_lap_times ----> bronze_consumer.py ----> data/bronze/laps/
    |-->  Kafka: f1_race_results -> results_consumer.py ---> data/bronze/race_results/
    '-->  Kafka: f1_weather ------> weather_consumer.py ---> data/bronze/weather/
                                                                |
                                                                v
                          silver_job.py ---------------  data/silver/lap_times
                          silver_race_results_job.py --  data/silver/race_results
                          silver_pit_stops_job.py -----  data/silver/pit_stops
                          silver_weather_job.py -------  data/silver/weather
                                                                |
                                                                v
                          gold_job.py ----------  data/gold/{driver_pace, tire_performance, sector_analysis}
                          gold_dimensions_job.py   data/gold/dims/{dim_drivers, dim_circuits, dim_races}
                          gold_facts_job.py -----  data/gold/facts/{fact_race_results, fact_pit_stops, ...}
```

**Historical backfill** (bypasses Kafka, writes directly to bronze):
```
ingestion/backfill.py --> data/bronze/{laps, race_results, weather}/
                      '-- data/backfill_manifest.jsonl  (resumable)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | FastF1 Python library |
| Message queue | Apache Kafka (Confluent 7.4.0) |
| Batch processing | Apache Spark / PySpark 3.5.0 |
| Orchestration | Apache Airflow 2.9.3 |
| Containers | Docker Compose |
| Language | Python 3.10+ |
| Formats | NDJSON (bronze), Parquet (silver/gold) |

## Repository Structure

```
.
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── f1_pipeline_dag.py     # Streaming pipeline DAG
│       └── f1_backfill_dag.py     # Historical backfill DAG
├── ingestion/
│   ├── producer.py                # Unified FastF1 -> Kafka producer
│   ├── bronze_consumer.py         # Laps consumer -> data/bronze/laps/
│   ├── results_consumer.py        # Results consumer -> data/bronze/race_results/
│   ├── weather_consumer.py        # Weather consumer -> data/bronze/weather/
│   └── backfill.py                # Historical multi-session backfill
├── processing/
│   ├── silver_job.py              # Bronze laps -> Silver lap_times
│   ├── silver_race_results_job.py # Bronze race_results -> Silver race_results
│   ├── silver_pit_stops_job.py    # Bronze laps (filtered) -> Silver pit_stops
│   ├── silver_weather_job.py      # Bronze weather -> Silver weather
│   ├── gold_job.py                # Silver -> Gold aggregations
│   ├── gold_dimensions_job.py     # Silver -> Gold dim tables (uses FastF1 + PySpark)
│   └── gold_facts_job.py          # Silver -> Gold fact tables
├── data/
│   ├── bronze/                    # Raw NDJSON, append-only
│   │   ├── laps/
│   │   ├── race_results/
│   │   └── weather/
│   ├── silver/                    # Typed Parquet, deduplicated
│   │   ├── lap_times/
│   │   ├── race_results/
│   │   ├── pit_stops/
│   │   └── weather/
│   └── gold/                      # Analytics-ready Parquet
│       ├── driver_pace/
│       ├── tire_performance/
│       ├── sector_analysis/
│       ├── dims/
│       │   ├── dim_drivers/
│       │   ├── dim_circuits/
│       │   └── dim_races/
│       └── facts/
│           ├── fact_race_results/
│           ├── fact_pit_stops/
│           └── fact_weather_snapshots/
├── cache/                         # FastF1 local cache (auto-managed)
├── compose.yml
├── requirements.txt
└── plan.md                        # Detailed implementation plan
```

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Internet access (first FastF1 data pull is cached locally in `cache/`)

## Quick Start (Manual)

This approach runs producers/consumers on the host and Spark jobs inside the `spark-master` container via `docker exec`. The `gold_dimensions_job.py` is an exception — it imports `fastf1` (not available in the Spark image) and runs PySpark in local mode, so it executes on the host.

### 1. Set up environment

```bash
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Start infrastructure

```bash
docker compose up -d zookeeper kafka spark-master spark-worker
```

Key endpoints:
- Kafka: `localhost:9092`
- Spark Master UI: `http://localhost:8080`
- Spark endpoint: `spark://localhost:7077`

### 3. Create bronze directories

```bash
mkdir -p data/bronze/laps data/bronze/race_results data/bronze/weather
```

### 4. Run the streaming pipeline

**Terminal 1 — start all consumers:**

```bash
python ingestion/bronze_consumer.py &
python ingestion/results_consumer.py &
python ingestion/weather_consumer.py &
```

**Terminal 2 — run producer (blocks until done):**

```bash
RACE_YEAR=2024 RACE_ROUND=1 RACE_SESSION=R python ingestion/producer.py
```

Wait for all consumers to exit on idle (~20s after producer finishes).

**Terminal 3 — build Silver:**

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_job.py
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_race_results_job.py
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_pit_stops_job.py
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/silver_weather_job.py
```

**Build Gold:**

```bash
# Aggregation tables (runs inside Spark cluster)
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/gold_job.py

# Dimension tables (runs locally — imports fastf1 which is not in the Spark image)
F1_PROJECT_ROOT=. python processing/gold_dimensions_job.py

# Fact tables (runs inside Spark cluster)
docker exec -it spark-master /opt/spark/bin/spark-submit /opt/project/processing/gold_facts_job.py
```

> **Note:** `gold_dimensions_job.py` imports `fastf1` to fetch circuit/race schedule data and creates a local PySpark session. When running on the host, set `F1_PROJECT_ROOT=.` so paths resolve to the repo root instead of `/opt/project/`.

### 5. Verify outputs

```bash
ls data/bronze/laps/
ls data/bronze/race_results/
ls data/bronze/weather/
ls data/silver/lap_times/
ls data/silver/race_results/
ls data/silver/pit_stops/
ls data/silver/weather/
ls data/gold/dims/dim_drivers/
ls data/gold/facts/fact_race_results/
```

## Automated Run (Airflow)

### Start all services

```bash
docker compose up -d
```

This starts Zookeeper, Kafka, Spark master + worker, and Airflow. Key endpoints:

| Service | URL | Credentials |
|---|---|---|
| Airflow Web UI | `http://localhost:8081` | admin / admin |
| Spark Master UI | `http://localhost:8080` | — |

### Streaming pipeline

Open the Airflow UI and trigger the `f1_pipeline` DAG manually.

**DAG task graph:**
```
ingest_to_bronze
    |-- build_silver_laps
    |-- build_silver_results
    |-- build_silver_pit_stops
    '-- build_silver_weather
            |-- build_gold_legacy
            '-- build_gold_dimensions
                        '-- build_gold_facts
```

The `ingest_to_bronze` task runs three consumers in the background (with unique Kafka group IDs and idle-exit enabled), then runs the producer in the foreground. After the producer completes, it waits for all consumers to drain and exit.

Silver jobs run via `docker exec spark-master spark-submit ...`. The `build_gold_dimensions` task runs via `python` inside the Airflow container (which has both `fastf1` and Java/PySpark installed). All other gold jobs use spark-submit.

> **Note:** The DAG uses `SequentialExecutor`, so "parallel" branches run in topological order. The dependency graph is correct and future-proof for `LocalExecutor` or `CeleryExecutor`.

### Historical backfill

Trigger the `f1_backfill` DAG with params:

| Param | Default | Example |
|---|---|---|
| `years` | `2023` | `2022,2023,2024` |
| `rounds` | `all` | `1,5,10` |
| `sessions` | `R` | `R,Q` |
| `data_types` | `laps,race_results,weather` | `laps` |
| `force` | `false` | `true` |

Or run manually from the host:

```bash
BACKFILL_YEARS=2023,2024 BACKFILL_SESSIONS=R python ingestion/backfill.py
```

The backfill writes directly to bronze (bypasses Kafka) and maintains a resumable manifest at `data/backfill_manifest.jsonl`. Safe to re-run — silver deduplicates on `record_id`.

## Configuration Reference

### Producer

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `RACE_YEAR` | `2024` | F1 season year |
| `RACE_ROUND` | `1` | Race round number |
| `RACE_SESSION` | `R` | Session type (`R`, `Q`, `FP1`, `FP2`, `FP3`, `S`) |
| `DATA_TYPES` | `laps,race_results,weather` | Which data types to emit |
| `SPEED_FACTOR` | `0.02` | Replay speed multiplier (0.02 = 50x faster) |

### Bronze Consumers

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `KAFKA_GROUP_ID` | consumer-specific | Kafka consumer group |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | `earliest` or `latest` |
| `BRONZE_BATCH_SIZE` | `20` (weather: `50`) | Records before flush |
| `BRONZE_FLUSH_INTERVAL_SECONDS` | `3` | Max time between flushes |
| `BRONZE_MAX_IDLE_SECONDS` | `20` | Idle timeout before exit |
| `BRONZE_EXIT_ON_IDLE` | `true` | Exit when idle |
| `BRONZE_RUN_ID` | `""` | Suffix for run-isolated filenames |
| `BRONZE_CLEAR_SESSION_ON_START` | `false` | Delete existing session files on startup |

### Backfill

| Variable | Default | Description |
|---|---|---|
| `BACKFILL_YEARS` | `2023` | Comma-separated years to ingest |
| `BACKFILL_ROUNDS` | `all` | `all` or comma-separated round numbers |
| `BACKFILL_SESSIONS` | `R` | Comma-separated session codes |
| `BACKFILL_DATA_TYPES` | `laps,race_results,weather` | Which data types to write |
| `BACKFILL_MANIFEST_PATH` | `data/backfill_manifest.jsonl` | Resumable manifest file |
| `BRONZE_BASE_DIR` | `data/bronze` | Base path for bronze subdirs |
| `BACKFILL_FORCE` | `false` | Re-ingest already-completed sessions |
| `BACKFILL_SLEEP_SECONDS` | `2` | Pause between sessions |

### Silver / Gold Jobs

| Variable | Default | Description |
|---|---|---|
| `F1_PROJECT_ROOT` | `/opt/project` | Base path for data/cache dirs (set `.` for host execution) |
| `SILVER_OUTPUT_FILES` | `1` | Parquet file count per silver table |
| `GOLD_OUTPUT_FILES` | `1` | Parquet file count per gold table |

## Data Outputs

### Bronze (raw NDJSON, append-only)

| Path | Key | Contents |
|---|---|---|
| `data/bronze/laps/laps.ndjson.session-<S>.driver-<D>.ndjson` | session + driver | Lap times, sector times, pit flags, Kafka metadata |
| `data/bronze/race_results/results.ndjson.session-<S>.driver-<D>.ndjson` | session + driver | Final position, points, status, team, nationality |
| `data/bronze/weather/weather.ndjson.session-<S>.ndjson` | session | Temperature, humidity, pressure, wind, rainfall |

### Silver (typed Parquet, deduplicated on `record_id`)

| Path | Key Columns |
|---|---|
| `data/silver/lap_times/` | driver_id, lap_number, lap_time_ms, sector times, tire_compound |
| `data/silver/race_results/` | driver_id, position, grid_position, points, status, team |
| `data/silver/pit_stops/` | driver_id, lap_number, pit_in_ms, pit_out_ms, tire_compound |
| `data/silver/weather/` | snapshot_index, air/track temp, humidity, pressure, wind, rainfall |

### Gold (Parquet, analytics-ready)

**Aggregations:**
- `data/gold/driver_pace/` — avg/fastest/std dev lap time per driver per session
- `data/gold/tire_performance/` — avg lap time per compound per session
- `data/gold/sector_analysis/` — avg sector times per driver per session

**Dimensions:**
- `data/gold/dims/dim_drivers/` — driver_id (PK), full_name, team, nationality
- `data/gold/dims/dim_circuits/` — circuit_id (PK), circuit_name, country, locality
- `data/gold/dims/dim_races/` — race_key (PK), year, round, name, circuit_id (FK), date

**Facts:**
- `data/gold/facts/fact_race_results/` — positions, points, status per driver per race (FK: race_key, driver_id)
- `data/gold/facts/fact_pit_stops/` — pit timing per driver per lap (FK: race_key, driver_id)
- `data/gold/facts/fact_weather_snapshots/` — weather time-series per session (FK: race_key)

## Roadmap

| Phase | Description | Status |
|---|---|---|
| 1-3 | Expanded ingestion, backfill, dimensional model | Done |
| 4 | DuckDB query layer over Parquet | Planned |
| 5 | Feature engineering layer (rolling averages, track history) | Planned |
| 6 | FastAPI service layer | Planned |
| 7 | Analytics dashboards (Superset / Grafana) | Planned |
| 8 | MCP server wrapping the API | Planned |
| 9 | ML model training and serving (race win prediction) | Planned |

See [plan.md](plan.md) for the detailed implementation plan.
