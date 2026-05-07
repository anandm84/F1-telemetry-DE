# Reproducibility Instructions

This document is the canonical, clean-environment recipe for rebuilding the curated F1 telemetry dataset from scratch. It assumes no prior state on the reviewer's machine. Follow it from a freshly cloned repository.

If you only want to inspect the curated outputs without rebuilding anything, see `sample_output/README.md`, the sample bundle already contains a complete one-race example as Parquet + CSV.

## 1. Environment

| Requirement | Notes |
|---|---|
| Python 3.10+ | Tested on 3.10. |
| Internet access | The first run downloads session data via FastF1; subsequent runs hit the local FastF1 cache in `cache/`. FastF1 enforces a ~500-call/hour rate limit; full-season backfill takes 30-60 minutes. |
| Disk space | ~30 MB bronze + ~50 MB warehouse for one full season; the sample bundle is < 50 MB. |
| Docker (optional) | Only needed if you want to use the streaming Kafka path or the Airflow DAGs. The fastest path uses backfill + dbt and skips Kafka entirely. |

## 2. Clone and install

```bash
git clone <this-repo-url> f1-telemetry
cd f1-telemetry

python -m venv venv
# Linux/macOS:
source venv/bin/activate
# Windows (PowerShell):
# .\venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

`requirements.txt` includes `fastf1`, `duckdb`, `dbt-duckdb`, `pandas`, `kafka-python`, `apache-airflow`.

## 3. Fastest path: full-2024 backfill + dbt build

This path bypasses Kafka and Airflow entirely. Expect ~30-60 minutes total because of FastF1's per-hour rate limit.

```bash
# 1. Acquire bronze for all sessions of 2024 (resumable; safe to re-run)
BACKFILL_YEARS=2024 \
BACKFILL_ROUNDS=all \
BACKFILL_SESSIONS=FP1,FP2,FP3,Q,S,SQ,R \
BACKFILL_DATA_TYPES=laps,race_results,weather \
BACKFILL_SLEEP_SECONDS=8 \
python ingestion/backfill.py

# 2. Refresh dbt seeds from FastF1 schedule (writes dbt/seeds/circuits.csv and races.csv)
F1_SEED_YEARS=2024 python ingestion/load_schedules.py

# 3. Build silver and gold via dbt; runs all tests
cd dbt
DBT_PROFILES_DIR=. F1_DUCKDB_PATH=../data/warehouse.duckdb F1_BRONZE_ROOT=../data/bronze dbt build
cd ..

# 4. (Optional) Refresh row counts in CLEANING_LOG.md
python scripts/cleaning_log.py

# 5. (Optional) Rebuild the sample bundle and checksum manifest
python scripts/build_sample_bundle.py --year 2024 --round 1
python scripts/checksums.py
```

If FastF1 raises `any API: 500 calls/h`, just re-run step 1, the manifest skips already-completed sessions.

Expected outputs after step 3:

```
data/bronze/laps/{year}_{round}_{session}/laps.ndjson.driver-{D}.ndjson
data/bronze/race_results/{year}_{round}_{session}/results.ndjson*
data/bronze/weather/{year}_{round}_{session}/weather.ndjson*
data/warehouse.duckdb           ← single-file DuckDB warehouse
data/backfill_manifest.jsonl    ← resumable manifest
```

The DuckDB warehouse contains:

| Schema | Tables |
|---|---|
| `main_silver` | `stg_laps`, `stg_race_results`, `stg_pit_stops`, `stg_weather` |
| `main_intermediate` | `int_drivers_latest` |
| `main_gold` | `driver_pace`, `tire_performance`, `sector_analysis`, `dim_drivers`, `dim_circuits`, `dim_races`, `fact_race_results`, `fact_pit_stops`, `fact_weather_snapshots` |
| `main_seeds` | `circuits`, `races` |

## 4. Expected row counts (parity check, full 2024 season)

For all 24 rounds × 5-7 sessions of 2024, the build produces:

| Table | Expected | Typical |
|---|---|---|
| `main_silver.stg_laps` | 50,000-60,000 | ~55,300 |
| `main_silver.stg_race_results` | 2,300-2,500 | ~2,400 |
| `main_silver.stg_pit_stops` | 5,500-6,500 | ~6,100 |
| `main_silver.stg_weather` | 10,000-12,500 | ~11,500 |
| `main_gold.driver_pace` | 2,300-2,500 | ~2,360 |
| `main_gold.tire_performance` | 250-350 | ~290 |
| `main_gold.sector_analysis` | 2,300-2,500 | ~2,360 |
| `main_gold.dim_drivers` | 30-35 (full grid + reserves) | ~33 |
| `main_gold.dim_circuits` | 24 (one per round) | 24 |
| `main_gold.dim_races` | 24 | 24 |
| `main_gold.fact_race_results` | 2,300-2,500 | ~2,400 |
| `main_gold.fact_pit_stops` | 5,500-6,500 | ~6,100 |
| `main_gold.fact_weather_snapshots` | 10,000-12,500 | ~11,500 |

If your build is far outside these ranges, check `data/backfill_manifest.jsonl` for unexpected `error` entries and re-run backfill.

## 5. Streaming path (Kafka + Airflow)

Use this path only if you want to verify the streaming code. The output is identical to the backfill path; the path itself is the artifact.

```bash
docker compose up -d
# Airflow UI: http://localhost:8081  (admin / admin)
```

Trigger the `f1_pipeline` DAG with config:

```json
{"year": 2024, "round": 1, "session": "R", "speed_factor": "0"}
```

The DAG's `ingest_to_bronze` task starts the three consumers in the background and the producer in the foreground; consumers exit on idle. `dbt_build` runs `dbt seed && dbt run`; `dbt_test` runs `dbt test`.

## 6. Reading the outputs

A small Python session reproduces a sanity-check query:

```python
import duckdb
con = duckdb.connect("data/warehouse.duckdb", read_only=True)
print(con.execute("""
    SELECT race_year, race_round, session, driver, fastest_lap_ms, laps_completed
      FROM main_gold.driver_pace
      WHERE session = 'R'
      ORDER BY fastest_lap_ms
      LIMIT 10
""").fetchdf())
```

Or use the DuckDB CLI:

```bash
duckdb data/warehouse.duckdb
```

```sql
SELECT race_round, driver, fastest_lap_ms
  FROM main_gold.driver_pace
  WHERE session = 'R'
  ORDER BY fastest_lap_ms
  LIMIT 10;
```

## 7. Verifying integrity

After running the build, verify the checksum manifest:

```bash
python scripts/checksums.py --verify
```

This recomputes SHA-256 over every file in the deposited bundle and compares to `docs/curation/checksums.sha256`. A mismatch indicates either a corrupt download or that you have made local modifications to the sample bundle.

## 8. Verifying parity vs. baseline

For comparing the dbt-DuckDB build against a reference (e.g., a previous Spark build, or someone else's run):

```bash
python scripts/parity_snapshot.py compare-duckdb
```

This compares row counts and key aggregates (sums, mins, maxes) against `scripts/parity_baseline.json` with a 0.1% tolerance.

## 9. Running tests

```bash
cd dbt
DBT_PROFILES_DIR=. F1_DUCKDB_PATH=../data/warehouse.duckdb F1_BRONZE_ROOT=../data/bronze dbt test
```

This runs every test declared in `_stg_models.yml` and `_marts_models.yml`: `not_null`, `unique`, and `relationships`. Failure exits non-zero.

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| FastF1 raises `any API: 500 calls/h` | Hit FastF1's hourly rate limit | Wait an hour and re-run backfill, the manifest skips already-completed sessions. |
| dbt staging models fail with `Out of Memory Error: Allocation failure` on `read_json` | DuckDB OOM scanning many NDJSON files with `union_by_name=true` | Confirm `dbt/macros/bronze_read.sql` does **not** include `union_by_name = true`. (This was the production bug fixed during the IS547 build; see CLEANING_LOG.md "Operational note: bronze_read OOM fix".) |
| `dim_circuits` / `dim_races` rows look wrong or stale | Seeds in `dbt/seeds/` are out of date | Re-run `F1_SEED_YEARS=2024 python ingestion/load_schedules.py` then `dbt build`. |
| Row counts very low | You used `BACKFILL_SESSIONS=R` only, not all five | Re-run with `BACKFILL_SESSIONS=FP1,FP2,FP3,Q,S,SQ,R`. |
| Row counts higher than expected | You re-ran backfill without the manifest, so bronze has duplicate writes | Silver dedup will absorb them; check `silver.stg_laps` is in the expected range. The `qualify` clause keeps the latest write per `record_id`. |
| Spark errors | This branch uses dbt + DuckDB, not Spark | Make sure you are not running scripts under `processing/` (those exist on a previous Spark-based branch). |

## 11. Determinism

- Bronze writes carry stable `record_id` SHA-1 hashes. Silver dedup is therefore deterministic given the same upstream FastF1 cache.
- Gold aggregates are deterministic given silver.
- The dimensions' `_updated_at` columns are wall-clock (`current_timestamp`); expect those to differ on rerun. Other columns are byte-identical.
- Random seeds: not used (no ML in the curation pipeline).

## 12. What constitutes a "successful" reproduction

You can claim successful reproduction if **all** of the following hold:

- `dbt build` exits with code 0 and "Done. PASS=N WARN=0 ERROR=0".
- All 13 silver/gold tables exist in `data/warehouse.duckdb`.
- Row counts fall within the ranges in §4.
- `python scripts/checksums.py --verify` passes against the deposited sample bundle (or you have rebuilt it locally and the new manifest is internally consistent).
- A reading of `main_gold.driver_pace` returns a sensible top-10 (Verstappen / Norris / Leclerc-class times for 2024 races).
