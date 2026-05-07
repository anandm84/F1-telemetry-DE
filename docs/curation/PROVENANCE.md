# Provenance and Data Lineage

This document explains where every data element in the curated dataset came from, what was done to it, when, and by whom. I wrote it for end users (researchers, data stewards, and reviewers), and it is paired with a machine-readable W3C PROV-O record at [`prov.jsonld`](prov.jsonld).

## 1. Sources at a glance

```
                 Formula One Management, F1 timing service (proprietary)
                              │
                              │  reverse-engineered access
                              ▼
                       ┌──────────────┐
                       │   FastF1     │  Python library (MIT)
                       │  (cached)    │  Library version pinned in requirements.txt
                       └──────┬───────┘
                              │
                              │  fastf1.get_session(year, round, session)
                              │  fastf1.get_event_schedule(year)        ← Jolpica-F1
                              ▼
                  ingestion/producer.py  OR  ingestion/backfill.py
                  ingestion/load_schedules.py  (writes dbt seeds)
                              │
                              │   (if streaming) Kafka topics:
                              │     f1_lap_times, f1_race_results, f1_weather
                              ▼
              ingestion/{bronze,results,weather}_consumer.py
                              │
                              ▼
                   data/bronze/{laps,race_results,weather}/{year}_{round}_{session}/...   (NDJSON)
                              │
                              ▼
              dbt staging models (silver)         dbt seeds (circuits.csv, races.csv)
              models/staging/stg_*.sql                      │
                              │                            ▼
                              ▼               dbt marts (gold dimensions)
                   main_silver.stg_*           models/marts/dim_*.sql
                              │                            │
                              ▼                            │
              dbt marts (silver→gold)         ◄────────────┘
              models/marts/{driver_pace, tire_performance, sector_analysis,
                            dim_drivers, dim_circuits, dim_races,
                            fact_race_results, fact_pit_stops, fact_weather_snapshots}
                              │
                              ▼
                   data/warehouse.duckdb (single-file DuckDB warehouse)
```

## 2. Upstream sources

| Upstream | What it provides | Access path | Authoritative? |
|---|---|---|---|
| Formula One Management, F1 live timing | Lap times, sector splits, weather, race results, pit timing | Indirect, via FastF1 | Yes (canonical timing) |
| FastF1 (Python library) | API client; local caching; schedule helpers | Direct dependency in `requirements.txt` | Library is canonical for the project; underlying data is FOM |
| Jolpica-F1 (Ergast successor) | Season schedules and event metadata | Indirect, via FastF1's `get_event_schedule` (called from `ingestion/load_schedules.py`); the resulting CSV seeds are version-controlled in `dbt/seeds/` | Authoritative for schedule metadata |

**Why this distinction matters.** The timing data and the schedule come from different services with different terms. `dim_circuits` and `dim_races` are sourced from the schedule path (Jolpica) and are licensable under more permissive terms. The lap, result, and weather facts come from the timing path and remain bound by F1 Brand Guidelines (see [LICENSING.md](LICENSING.md)).

**dbt seeds lock the schedule lineage.** Because Jolpica data lands as version-controlled CSVs (`dbt/seeds/circuits.csv`, `dbt/seeds/races.csv`), the gold-build is reproducible offline once the seeds have been generated. The `load_schedules.py` step is the only point where the schedule is touched live.

## 3. Retrieval context

Information that lets a reviewer reproduce or evaluate the extraction:

| Field | Value (template) | How it is captured |
|---|---|---|
| FastF1 version | Pinned in [`requirements.txt`](../../requirements.txt) | `pip freeze` at the time of acquisition |
| Python version | 3.10+ (CI baseline) | Recorded in `REPRODUCE.md` |
| OS / platform | Linux container (Airflow worker) or local dev | Recorded in `REPRODUCE.md` |
| Retrieval date(s) | Per-record `event_ts` and `_bronze_written_at` | Stamped at emit and at bronze-flush time |
| Kafka offset & partition | `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` | Captured by bronze consumers |
| Backfill provenance | `data/backfill_manifest.jsonl` lines | Appended by `ingestion/backfill.py` per session |
| FastF1 cache | `cache/` directory | Enabled in `producer.py` and `backfill.py` |
| dbt manifest | `dbt/target/manifest.json` after each `dbt build` | dbt-generated catalog of compiled SQL, sources, and tests |

Each bronze record therefore carries enough lineage to identify (a) when it was emitted, (b) when it was persisted, and (c) which producer (Kafka or backfill) wrote it. The `record_id` (SHA-1) makes idempotent re-ingestion safe. The dbt manifest provides column-level lineage for the silver/gold transformation graph.

## 4. Transformation steps

This is the prose companion to the cleaning log. Each step lists the *rule applied*, the *file that applies it*, and the *rationale*.

### 4.1 Bronze ← FastF1

Files: [`ingestion/producer.py`](../../ingestion/producer.py), [`ingestion/backfill.py`](../../ingestion/backfill.py).

| Step | Rule | Rationale |
|---|---|---|
| Drop laps with null `Driver`, `LapNumber`, or `LapTime` | `laps.dropna(subset=[...])` | Such rows cannot form a stable `record_id` and would be lost in any downstream join. |
| Encode all timedelta values as `HH:MM:SS:NNNN` | `_format_timedelta_hhmmssmmmm` | Preserve sub-second precision while keeping the bronze format text-only and language-agnostic. |
| Compute `record_id` deterministically | SHA-1 of `(year, round, session, driver, lap_number, lap_time)` for laps; analogous tuples for results and weather | Allows resumable backfill and idempotent silver dedup. |
| Stamp `event_ts` at emit and `_bronze_written_at` at flush | UTC ISO-8601 | Two-point timestamp lineage (acquired vs. persisted). |

### 4.2 Bronze → Silver: dbt staging

All silver tables are dbt models under `dbt/models/staging/`. Bronze is read via the `bronze_read()` Jinja macro in [dbt/macros/bronze_read.sql](../../dbt/macros/bronze_read.sql), which wraps DuckDB's `read_json(format='newline_delimited', ignore_errors=true, maximum_object_size=64MB)`. (`union_by_name` was deliberately disabled after it caused OOM on 1000+ files; producers emit a stable schema, so it is unnecessary.)

#### `stg_laps`, file: [dbt/models/staging/stg_laps.sql](../../dbt/models/staging/stg_laps.sql)

| Step | Rule | Rationale |
|---|---|---|
| Cast strings to typed timestamps | `try_cast(event_ts as timestamp)`, `try_cast(_bronze_written_at as timestamp)` | Type-safety; downstream temporal queries; `try_cast` returns NULL on parse failure rather than raising. |
| Normalize driver code | `upper(trim(Driver))` | Defensive. Bronze sometimes carries whitespace in driver fields. |
| Cast `LapNumber` (float in bronze) → INTEGER | `cast(LapNumber as integer)` | Lap indexes are integral by definition; float was forced by FastF1 NaN handling at bronze. |
| Parse `HH:MM:SS:NNNN` → ms | `duration_to_ms` macro at [dbt/macros/duration_to_ms.sql](../../dbt/macros/duration_to_ms.sql) | Analytics need numeric milliseconds. |
| Normalize compound | `upper(trim(Compound))` | Match dimension and aggregation joins. |
| Filter null/empty PK fields | `record_id`, `event_ts`, `Driver`, `LapNumber`, `lap_time_ms` all required | Without these a row cannot be analytically meaningful. |
| Deduplicate | `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1` | Same record may have been written multiple times due to consumer retry or backfill rerun. **Latest wins**, which is different from a simple distinct: it ensures the most recent emit takes precedence. |

#### `stg_race_results`, file: [dbt/models/staging/stg_race_results.sql](../../dbt/models/staging/stg_race_results.sql)

Same pattern: type-cast → normalize → filter (`record_id`, `driver_id`, `race_year`, `race_round` all required) → qualify-dedup latest-wins.

#### `stg_pit_stops`, file: [dbt/models/staging/stg_pit_stops.sql](../../dbt/models/staging/stg_pit_stops.sql)

Reads the *same* bronze laps NDJSON via `bronze_read('laps', …)`, but with a pre-filter `where PitInTime_ms is not null`. Capturing this dependency is important because changes to lap-bronze schemas affect *two* silver outputs.

#### `stg_weather`, file: [dbt/models/staging/stg_weather.sql](../../dbt/models/staging/stg_weather.sql)

Same dedup-on-`record_id` and timestamp-typing pattern.

### 4.3 Silver → Gold: aggregations

Files: [dbt/models/marts/driver_pace.sql](../../dbt/models/marts/driver_pace.sql), [tire_performance.sql](../../dbt/models/marts/tire_performance.sql), [sector_analysis.sql](../../dbt/models/marts/sector_analysis.sql).

| Output | Aggregation | Notes and caveats |
|---|---|---|
| `driver_pace` | per (race_year, race_round, session, driver): `avg`, `min`, `stddev_samp`, `count` of `lap_time_ms` | Includes in-laps, out-laps, safety-car laps. Documented in the data dictionary's interpretive notes. |
| `tire_performance` | per (race_year, race_round, session, tire_compound): `avg`, `count` | Same caveat. |
| `sector_analysis` | per (race_year, race_round, session, driver): `avg` of each sector | Null sectors are ignored by `avg`. |

### 4.4 Silver → Gold: dimensions

- `dim_drivers` (file: [dim_drivers.sql](../../dbt/models/marts/dim_drivers.sql)) selects from the intermediate model `int_drivers_latest` ([int_drivers_latest.sql](../../dbt/models/intermediate/int_drivers_latest.sql)), which uses a per-driver window ordered by `(race_year DESC, race_round DESC)` to pick the latest team and nationality. Type-1 SCD pattern.
- `dim_circuits` (file: [dim_circuits.sql](../../dbt/models/marts/dim_circuits.sql)) reads from the dbt seed [`dbt/seeds/circuits.csv`](../../dbt/seeds/circuits.csv), inner-joined to the set of years that actually have observed race results in silver. Latest seed-year wins per `circuit_id`.
- `dim_races` (file: [dim_races.sql](../../dbt/models/marts/dim_races.sql)) reads from the dbt seed [`dbt/seeds/races.csv`](../../dbt/seeds/races.csv), filtered the same way.

The dbt seeds were generated by [`ingestion/load_schedules.py`](../../ingestion/load_schedules.py) calling `fastf1.get_event_schedule(year)`. **Once the seeds exist, the gold build does not need to call FastF1 again.** That is a meaningful improvement over the earlier Spark-based design, which re-touched FastF1 at gold-build time.

### 4.5 Silver → Gold: facts

Files: [fact_race_results.sql](../../dbt/models/marts/fact_race_results.sql), [fact_pit_stops.sql](../../dbt/models/marts/fact_pit_stops.sql), [fact_weather_snapshots.sql](../../dbt/models/marts/fact_weather_snapshots.sql).

Facts are projections of silver tables with an added `race_key = "{year}_{round}"` synthetic key for joining `dim_races`. No measurements are recomputed at this layer.

## 5. Coverage and known gaps

- **Temporal coverage.** The curated bundle targets the **2024 season**. I attempted backfill across all 24 rounds with five to seven session types per round; the FastF1 service applies a 500-call/hour rate limit that staggers the run. The [`data/backfill_manifest.jsonl`](../../data/backfill_manifest.jsonl) records exact per-session status.
- **Sample bundle coverage.** The sample bundle in `docs/curation/sample_output/` is a single race weekend (2024 round 1, all five sessions). It is illustrative, not a redistribution of season data.
- **Telemetry.** Car telemetry (speed, throttle, brake, gear) is **not** ingested. The producer loads sessions with `telemetry=False` to keep the workload small. Adding telemetry is a known next step.
- **Practice/Sprint coverage.** Producer and backfill accept any session code. Default `BACKFILL_SESSIONS=R`; full multi-session backfill requires explicit `BACKFILL_SESSIONS=FP1,FP2,FP3,Q,S,SQ,R`.
- **Driver code stability.** FastF1 occasionally exposes reserve drivers with codes that vary. `dim_drivers` accepts whatever silver provides and does not attempt cross-season identity reconciliation.
- **`avg_lap_time_ms` is not "pace".** It is a raw mean across all laps including in-laps, out-laps, and safety-car laps. See the data dictionary's interpretive notes.

## 6. Lineage at the column level

Every silver and gold column traces back to a bronze NDJSON field, an aggregation over bronze fields, or a deterministic computation (for example, `race_key`). The full mapping is in the data dictionary's "Source field" column. Combined with the per-record `_kafka_topic`, `_kafka_offset`, `_kafka_timestamp`, and `_bronze_written_at` fields, this gives column-level *and* record-level lineage.

For machine-readable lineage at the model level, dbt's `target/manifest.json` (generated by `dbt build`) provides a complete dependency graph: every `ref()` and `source()` is resolved and timestamped.

## 7. Re-running the pipeline

See [REPRODUCE.md](REPRODUCE.md) for the canonical, clean-environment instructions. The combination of:

- pinned dependency versions (`requirements.txt`),
- the resumable `data/backfill_manifest.jsonl`,
- deterministic `record_id` hashes,
- idempotent silver dedup, and
- version-controlled dbt seeds for the schedule

means a re-run with the same FastF1 cache produces deterministic warehouse contents at silver. The gold layer is deterministic given silver, except for `_updated_at` timestamps in `dim_drivers` and `dim_circuits`, which are wall-clock (`current_timestamp`) and therefore expected to change.
