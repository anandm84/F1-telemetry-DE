# F1 Telemetry Data Engineering & Curation Pipeline

End-to-end data engineering and curation project for Formula 1 telemetry data using FastF1, Kafka, dbt, and DuckDB with a Bronze/Silver/Gold medallion architecture. The repository serves two audiences: (a) engineers wanting a working ingest+transform pipeline, and (b) reviewers / data stewards looking at the curation deliverables under [`docs/curation/`](docs/curation/) (data dictionary, provenance log, licensing memo, preservation plan, FAIR self-assessment, and a reproducible sample bundle).

The curation work is the IS547 (Spring 2026) self-directed course project. See [`docs/curation/REPORT.md`](docs/curation/REPORT.md) for the narrative.

## Overview

The pipeline ingests multiple data types from FastF1 for any configurable year (or single round/session), streams them through Kafka into raw Bronze NDJSON files partitioned by session, and transforms them into typed Silver and analytics-ready Gold tables in DuckDB via dbt.

**Data types ingested:**
- **Lap times** — per-driver lap, sector times, tire compound, pit stop flags
- **Race results** — final positions, grid positions, points, DNF status, team, nationality
- **Weather snapshots** — track/air temp, humidity, pressure, wind speed, rainfall

**Gold layer outputs:**
- Aggregations: `driver_pace`, `tire_performance`, `sector_analysis`
- Dimensions: `dim_drivers`, `dim_circuits`, `dim_races`
- Facts: `fact_race_results`, `fact_pit_stops`, `fact_weather_snapshots`

## Architecture

```
FastF1 API
    |
    v
ingestion/producer.py        (iterates rounds + sessions for a given year)
    |
    |-->  Kafka: f1_lap_times ----> bronze_consumer.py ----> data/bronze/laps/{year}_{round}_{session}/
    |-->  Kafka: f1_race_results -> results_consumer.py ---> data/bronze/race_results/{year}_{round}_{session}/
    '-->  Kafka: f1_weather ------> weather_consumer.py ---> data/bronze/weather/{year}_{round}_{session}/
                                                                 |
                                                                 v
                          dbt staging (silver) -- main_silver.stg_{laps, race_results, pit_stops, weather}
                                                                 |
                                                                 v
                          dbt marts (gold) ----- main_gold.{driver_pace, tire_performance, sector_analysis,
                                                            dim_drivers, dim_circuits, dim_races,
                                                            fact_race_results, fact_pit_stops, fact_weather_snapshots}
                                                                 |
                                                                 v
                                                       data/warehouse.duckdb
```

**Historical backfill** (bypasses Kafka, writes directly to bronze):
```
ingestion/backfill.py --> data/bronze/{laps, race_results, weather}/{year}_{round}_{session}/
                      '-- data/backfill_manifest.jsonl   (resumable)
```

## Tech Stack

| Layer | Technology |
|---|---|
| Data source | FastF1 Python library |
| Message queue | Apache Kafka (Confluent 7.4.0) |
| Transformation | dbt-duckdb 1.10 |
| Warehouse | DuckDB (single-file, in-process) |
| Orchestration | Apache Airflow 2.9.3 |
| Containers | Docker Compose |
| Language | Python 3.10+ |
| Formats | NDJSON (bronze), DuckDB tables (silver/gold) |

## Repository Structure

```
.
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── f1_pipeline_dag.py     # Streaming pipeline DAG (year-parameterized)
│       └── f1_backfill_dag.py     # Historical backfill DAG
├── ingestion/
│   ├── producer.py                # FastF1 -> Kafka, iterates rounds+sessions
│   ├── bronze_consumer.py         # Laps consumer       -> data/bronze/laps/{session_key}/
│   ├── results_consumer.py        # Results consumer    -> data/bronze/race_results/{session_key}/
│   ├── weather_consumer.py        # Weather consumer    -> data/bronze/weather/{session_key}/
│   ├── backfill.py                # Historical multi-session backfill
│   └── load_schedules.py          # FastF1 schedule -> dbt seed CSVs
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml               # DuckDB at data/warehouse.duckdb
│   ├── macros/
│   │   ├── bronze_read.sql        # NDJSON read helper using DuckDB read_json
│   │   └── duration_to_ms.sql     # HH:MM:SS:MMMM parser
│   ├── models/
│   │   ├── staging/               # silver layer (cast, dedup, filter)
│   │   ├── intermediate/          # int_drivers_latest (window pick)
│   │   └── marts/                 # gold layer (dims, facts, aggregations)
│   └── seeds/
│       ├── circuits.csv
│       └── races.csv
├── scripts/
│   ├── build_sample_bundle.py     # Export curated sample to docs/curation/sample_output/
│   ├── checksums.py               # SHA-256 manifest writer/verifier for the bundle
│   ├── cleaning_log.py            # Append row-count snapshot to CLEANING_LOG.md
│   └── parity_snapshot.py         # Snapshot/compare gold table stats
├── docs/
│   └── curation/                  # IS547 curation deliverables (see § Curation Deliverables)
│       ├── REPORT.md              # Final narrative report
│       ├── data_dictionary.{md,csv}
│       ├── metadata.{xml,json}    # Dublin Core
│       ├── PROVENANCE.md
│       ├── prov.jsonld            # W3C PROV-O machine-readable provenance
│       ├── CLEANING_LOG.md
│       ├── LICENSING.md
│       ├── PRESERVATION.md
│       ├── REPRODUCE.md
│       ├── FAIR_assessment.md
│       ├── RISK_REGISTER.md
│       ├── workflow.mmd
│       ├── checksums.sha256
│       └── sample_output/         # 2024 R1 example bundle (Parquet + CSV + bronze excerpts)
├── is-547/                        # Course materials (instructions, plan, peer review)
├── data/
│   ├── bronze/                    # Raw NDJSON, partitioned by session_key
│   │   ├── laps/{year}_{round}_{session}/laps.ndjson.driver-{D}.ndjson
│   │   ├── race_results/{year}_{round}_{session}/results.ndjson
│   │   └── weather/{year}_{round}_{session}/weather.ndjson
│   └── warehouse.duckdb           # Silver + gold tables (built by dbt)
├── cache/                         # FastF1 local cache (auto-managed)
├── compose.yml
├── requirements.txt
├── CITATION.cff                   # Software/dataset citation
├── LICENSE                        # MIT (code)
├── LICENSE-DATA.md                # CC-BY-4.0 (curated data and documentation)
└── plan.md                        # Original engineering plan (Mar 2026)
```

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- Internet access (first FastF1 data pull is cached locally in `cache/`)

## Quick Start

### 1. Install dependencies

```bash
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Backfill one round (no Kafka required)

The simplest end-to-end smoke test bypasses Kafka and writes directly to bronze:

```bash
# Ingest all sessions of round 1, 2024
BACKFILL_YEARS=2024 BACKFILL_ROUNDS=1 BACKFILL_SESSIONS=FP1,FP2,FP3,Q,R \
    python ingestion/backfill.py

# Refresh dbt seeds (FastF1 schedule -> CSVs)
F1_SEED_YEARS=2024 python ingestion/load_schedules.py

# Build silver + gold and run tests
cd dbt && DBT_PROFILES_DIR=. dbt build
```

The warehouse lands at `data/warehouse.duckdb`. Open it with the DuckDB CLI or any client.

### 3. Backfill an entire year

```bash
BACKFILL_YEARS=2024 BACKFILL_ROUNDS=all BACKFILL_SESSIONS=FP1,FP2,FP3,Q,S,SQ,R \
    python ingestion/backfill.py
F1_SEED_YEARS=2024 python ingestion/load_schedules.py
cd dbt && DBT_PROFILES_DIR=. dbt build
```

The backfill manifest at `data/backfill_manifest.jsonl` makes reruns idempotent; sessions already marked `ok` are skipped.

### 4. Streaming pipeline via Kafka

Start Kafka and Airflow:

```bash
docker compose up -d
```

Then in three terminals (or trigger the Airflow DAG, see below):

```bash
# Terminal 1: laps consumer
python ingestion/bronze_consumer.py

# Terminal 2: results + weather consumers
python ingestion/results_consumer.py &
python ingestion/weather_consumer.py

# Terminal 3: producer
RACE_YEAR=2024 RACE_ROUND=1 RACE_SESSION=R SPEED_FACTOR=0 \
    python ingestion/producer.py

# After producer finishes and consumers idle out:
F1_SEED_YEARS=2024 python ingestion/load_schedules.py
cd dbt && DBT_PROFILES_DIR=. dbt build
```

Set `RACE_ROUND=all` and/or `RACE_SESSION=all` to iterate the full season.

## Automated Run (Airflow)

```bash
docker compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| Airflow Web UI | `http://localhost:8081` | admin / admin |

Trigger the `f1_pipeline` DAG. The DAG accepts a JSON config:

```json
{"year": 2024, "round": "all", "session": "all", "speed_factor": "0"}
```

| Param | Default | Description |
|---|---|---|
| `year` | `2024` | Season year |
| `round` | `all` | Round number, comma-separated list, or `all` |
| `session` | `all` | Session code (`FP1`, `FP2`, `FP3`, `SQ`, `S`, `Q`, `R`), comma list, or `all` |
| `speed_factor` | `0` | Replay speed multiplier (`0` = no delay; `0.02` = 50x simulated live) |

**DAG task graph:**
```
ingest_to_bronze ─┐
                  ├─> dbt_build -> dbt_test
refresh_seeds ────┘
```

`ingest_to_bronze` starts the three consumers in the background and runs the producer in the foreground; consumers exit on idle. `refresh_seeds` regenerates `dbt/seeds/circuits.csv` and `races.csv` from the FastF1 schedule. `dbt_build` runs `dbt seed && dbt run`; `dbt_test` runs `dbt test`.

## Configuration Reference

### Producer

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `RACE_YEAR` | `2024` | F1 season year |
| `RACE_ROUND` | `1` | Round number, comma list, or `all` |
| `RACE_SESSION` | `R` | Session code, comma list, or `all` |
| `DATA_TYPES` | `laps,race_results,weather` | Which data types to emit |
| `SPEED_FACTOR` | `0.02` | Replay speed (0 = no delay) |

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

### Backfill

| Variable | Default | Description |
|---|---|---|
| `BACKFILL_YEARS` | `2023` | Comma-separated years |
| `BACKFILL_ROUNDS` | `all` | `all` or comma-separated round numbers |
| `BACKFILL_SESSIONS` | `R` | Comma-separated session codes |
| `BACKFILL_DATA_TYPES` | `laps,race_results,weather` | Which data types to write |
| `BACKFILL_MANIFEST_PATH` | `data/backfill_manifest.jsonl` | Resumable manifest file |
| `BRONZE_BASE_DIR` | `data/bronze` | Base path for bronze subdirs |
| `BACKFILL_FORCE` | `false` | Re-ingest already-completed sessions |

### dbt / Warehouse

| Variable | Default | Description |
|---|---|---|
| `F1_DUCKDB_PATH` | `../data/warehouse.duckdb` | DuckDB file (relative to `dbt/`) |
| `F1_BRONZE_ROOT` | `../data/bronze` | Bronze base used by `bronze_read` macro |
| `F1_SEED_YEARS` | `2023,2024` | Years pulled by `load_schedules.py` |

## Data Outputs

### Bronze (raw NDJSON, append-only, partitioned by session)

| Path | Per-file Key | Contents |
|---|---|---|
| `data/bronze/laps/{year}_{round}_{session}/laps.ndjson.driver-{D}.ndjson` | driver | Lap times, sector times, pit flags |
| `data/bronze/race_results/{year}_{round}_{session}/results.ndjson` | session | Final position, points, status for all drivers |
| `data/bronze/weather/{year}_{round}_{session}/weather.ndjson` | session | Temperature, humidity, pressure, wind, rainfall |

### Silver (DuckDB tables, deduplicated on `record_id`)

`main_silver.stg_laps`, `stg_race_results`, `stg_pit_stops`, `stg_weather` — schema-cleaned typed views over bronze.

### Gold (DuckDB tables, analytics-ready)

**Aggregations** (`main_gold`):
- `driver_pace` — avg/fastest/std-dev lap time per driver per session
- `tire_performance` — avg lap time per compound per session
- `sector_analysis` — avg sector times per driver per session

**Dimensions** (`main_gold`):
- `dim_drivers` — driver_id (PK), full_name, team, nationality
- `dim_circuits` — circuit_id (PK), circuit_name, country, locality
- `dim_races` — race_key (PK), year, round, name, circuit_id (FK), date

**Facts** (`main_gold`):
- `fact_race_results` — positions, points, status per driver per race
- `fact_pit_stops` — pit timing per driver per lap
- `fact_weather_snapshots` — weather time-series per session

### Querying

```bash
duckdb data/warehouse.duckdb
```

```sql
select race_year, race_round, session, driver, fastest_lap_ms
from main_gold.driver_pace
order by fastest_lap_ms
limit 10;
```

## Curation Deliverables

The full IS547 curation package lives under [`docs/curation/`](docs/curation/). Each artifact targets a specific course-concept rubric criterion:

| Artifact | Purpose / course concept |
|---|---|
| [`REPORT.md`](docs/curation/REPORT.md) | Final ~2,400-word narrative tying every decision to course concepts |
| [`data_dictionary.md`](docs/curation/data_dictionary.md) / [`.csv`](docs/curation/data_dictionary.csv) | Column-level schema + interpretive codebook · *Metadata & documentation* |
| [`metadata.xml`](docs/curation/metadata.xml) / [`.json`](docs/curation/metadata.json) | Dublin Core descriptive metadata · *Findability* |
| [`PROVENANCE.md`](docs/curation/PROVENANCE.md) + [`prov.jsonld`](docs/curation/prov.jsonld) | Human prose + W3C PROV-O record · *Provenance & lineage* |
| [`CLEANING_LOG.md`](docs/curation/CLEANING_LOG.md) | Every cleaning rule with rationale + auto-populated row counts · *Data quality* |
| [`LICENSING.md`](docs/curation/LICENSING.md) | Responsible-use memo: F1 timing data redistribution decision · *Ethical/legal compliance* |
| [`PRESERVATION.md`](docs/curation/PRESERVATION.md) | Zenodo deposit plan, format choices, fixity · *Archiving* |
| [`REPRODUCE.md`](docs/curation/REPRODUCE.md) | Clean-environment recipe with parity ranges · *Reproducibility* |
| [`FAIR_assessment.md`](docs/curation/FAIR_assessment.md) | 15-principle FAIR scorecard |
| [`RISK_REGISTER.md`](docs/curation/RISK_REGISTER.md) | Legal / technical / repro / quality / lab risks with mitigations |
| [`workflow.mmd`](docs/curation/workflow.mmd) | Mermaid workflow diagram (render with `mmdc`) |
| [`sample_output/`](docs/curation/sample_output/) | Self-contained 2024 R1 bundle: bronze excerpts + silver Parquet + gold Parquet+CSV |
| [`checksums.sha256`](docs/curation/checksums.sha256) | SHA-256 manifest covering every deposited file |

**Refresh after a build:**

```bash
python scripts/cleaning_log.py            # append row-count snapshot to CLEANING_LOG.md
python scripts/build_sample_bundle.py --year 2024 --round 1
python scripts/checksums.py               # write
python scripts/checksums.py --verify      # verify
```

**Citation and license:** see [`CITATION.cff`](CITATION.cff), [`LICENSE`](LICENSE) (MIT for code), [`LICENSE-DATA.md`](LICENSE-DATA.md) (CC-BY-4.0 for derived data and docs). Raw F1 timing data is **not** redistributed — see [`docs/curation/LICENSING.md`](docs/curation/LICENSING.md).

## Roadmap

| Phase | Description | Status |
|---|---|---|
| 1-3 | Expanded ingestion, backfill, dimensional model | Done |
| 4 | dbt + DuckDB transformation layer | Done |
| 4.5 | IS547 curation package (data dictionary, metadata, provenance, licensing, preservation, FAIR, risk register, sample bundle) | Done |
| 5 | Zenodo Sandbox deposit + DOI replacement in metadata | Planned |
| 6 | Feature engineering layer (rolling averages, track history) | Planned |
| 7 | FastAPI service layer | Planned |
| 8 | Analytics dashboards (Superset / Grafana) | Planned |
| 9 | MCP server wrapping the API | Planned |
| 10 | ML model training and serving (race win prediction) | Planned |
