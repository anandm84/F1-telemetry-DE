# Cleaning Log

This log enumerates every cleaning, normalization, and quality-filter decision applied to the dataset, in the order it is applied. For each rule it records: the layer where it runs, the file that implements it, the rule itself, the rationale, and the analytical impact a downstream user should be aware of. Concrete row counts for the most recent build are produced by [`scripts/cleaning_log.py`](../../scripts/cleaning_log.py) and appended to the bottom of this file.

The log is written *for users of the dataset*, not for engineers debugging the pipeline. It is the document an analyst would consult before publishing a paper based on these tables.

## How to read this log

| Column | Meaning |
|---|---|
| **Step** | Numeric identifier; matches the line in the relevant ingestion script or dbt model. |
| **Layer** | bronze / silver / gold. |
| **Rule** | What is filtered, normalized, or transformed. |
| **Rationale** | Why this was the chosen approach (other options considered, and why rejected). |
| **Impact** | What an analyst loses or gains because of this rule. |

---

## Bronze layer rules

Implemented in [`ingestion/producer.py`](../../ingestion/producer.py) and [`ingestion/backfill.py`](../../ingestion/backfill.py).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| B-1 | Drop laps with null `Driver`, `LapNumber`, or `LapTime`. | These three fields participate in `record_id`. A row missing any of them cannot be deduplicated and would be silently lost downstream anyway. Filtering at bronze makes the loss visible at write time. | Some FastF1 rows representing red-flag-aborted laps appear with null `LapTime` and are intentionally not preserved at bronze. If an analyst wants to count *attempted* laps, they need the FastF1 `Lap.IsAccurate` field directly, not captured here. **Known gap.** |
| B-2 | Encode all timedelta values as `HH:MM:SS:NNNN` strings. | Text encoding survives Kafka serialization without precision loss. The trailing `NNNN` preserves 1/10000-second resolution. | Adds a regex parsing step at silver. Trade-off accepted because alternative (binary timedelta) would couple bronze to a specific deserializer. |
| B-3 | Compute `record_id = SHA1(year\|round\|session\|driver\|lap_number\|lap_time)`. | Deterministic, language-independent, content-addressed. Lets downstream silver dedup be a one-liner. | Two laps with identical `(year, round, session, driver, lap_number, lap_time)` collapse to one row. In practice, lap times to 1/10000 second are unique per driver per lap, so collisions only happen on actual duplicate writes. |
| B-4 | Stamp `event_ts` (emit) and `_bronze_written_at` (flush). UTC ISO-8601. | Two-point timestamp lineage. | None for analytical use. Used by silver dedup and reproducibility tests. |
| B-5 | Bronze writers are append-only per file. | Idempotent reruns are safe, silver dedup absorbs duplicates. | Bronze files can grow on rerun. The backfill manifest prevents this when `BACKFILL_FORCE` is unset. |

## Silver layer rules, laps

File: [`dbt/models/staging/stg_laps.sql`](../../dbt/models/staging/stg_laps.sql). Bronze is read via the `bronze_read('laps', 'laps.ndjson.driver-*.ndjson')` macro.

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| S-LAP-1 | `try_cast(event_ts as timestamp)`, `try_cast(_bronze_written_at as timestamp)`. | Type-safe temporal predicates. `try_cast` returns NULL on parse failure rather than raising, defensive. | None expected; counted in S-LAP-5. |
| S-LAP-2 | `driver_id = upper(trim(Driver))`. Same for `tire_compound = upper(trim(Compound))`. | Defensive normalization. Bronze sometimes carries whitespace; case-folding ensures join keys match `dim_drivers`. | A driver code like `" ver "` becomes `"VER"`. No meaningful loss. |
| S-LAP-3 | `cast(LapNumber as integer)`. | LapNumber is logically integral. Bronze stored it as float because FastF1 returned NaN for unloaded laps. | After this cast, NaN becomes null. Combined with rule S-LAP-5, those rows are dropped. |
| S-LAP-4 | Parse `HH:MM:SS:NNNN` to `lap_time_ms` (BIGINT) via the `duration_to_ms` macro at [`dbt/macros/duration_to_ms.sql`](../../dbt/macros/duration_to_ms.sql). | Aggregations need numeric milliseconds. | If a value cannot be parsed, `lap_time_ms` is null and the row is dropped at the next step. |
| S-LAP-5 | Drop rows where any of `record_id`, `event_ts`, `Driver` (or empty), `LapNumber`, or `lap_time_ms` is null. | These are the analytic primary key plus the core measurement. A row missing any of them cannot be aggregated meaningfully. | Should be 0 in practice if bronze rules held; counted explicitly so any anomaly is visible. |
| S-LAP-6 | `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1`. | Idempotent re-ingestion. **Latest write wins**, different from a simple `distinct`, which would non-deterministically keep one row. | If a producer correction emits the same `record_id` with a different shape (rare; would only happen if the producer code itself changed), the most recent emit takes precedence. |
| S-LAP-7 | Retain `driver` and `lap_time` aliases of the renamed columns. | Backwards compatibility with gold queries. | A small redundancy in storage. Will be removed once gold queries are updated. |

## Silver layer rules, race results

File: [`dbt/models/staging/stg_race_results.sql`](../../dbt/models/staging/stg_race_results.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| S-RR-1 | Drop rows with null `record_id`, empty `driver_id`, null `race_year`, or null `race_round`. | PK + race coordinates required. | None expected; counted defensively. |
| S-RR-2 | `qualify row_number() over (partition by record_id order by event_ts desc) = 1`. | Idempotent. Note: dedup orders by raw `event_ts` string here, not parsed `event_timestamp`, the strings are ISO-8601 so lexicographic order matches chronological order. | Same as S-LAP-6. |
| S-RR-3 | Position, grid_position, points are *not* clamped or imputed. | The dataset preserves whatever FastF1 returned, including nulls for unclassified drivers. Imputing 0 or 21 would erase information. | Analysts must decide how to handle null `position` (DNF, DNS, etc.) themselves. The `status` column carries the textual reason. |

## Silver layer rules, pit stops

File: [`dbt/models/staging/stg_pit_stops.sql`](../../dbt/models/staging/stg_pit_stops.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| S-PS-1 | Pre-filter: keep only rows where `PitInTime_ms IS NOT NULL`. | Only laps where the driver actually pitted are pit-stop facts. | A pit stop that straddles two laps (in-lap end + out-lap begin) is captured as a single row keyed on the in-lap. `pit_out_ms` may be on a subsequent FastF1 row not present here. **Documented gap.** |
| S-PS-2 | `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1` after re-using the lap's `record_id`. | Same SHA-1 hash as the corresponding lap; safe to dedup. | None. |

## Silver layer rules, weather

File: [`dbt/models/staging/stg_weather.sql`](../../dbt/models/staging/stg_weather.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| S-W-1 | Drop rows with null `record_id`, `race_year`, or `race_round`. | PK + coordinates required. | None expected. |
| S-W-2 | `qualify row_number() over (partition by record_id order by event_ts desc) = 1`. | Idempotent. | Weather snapshots have stable `snapshot_index`, so duplicates only appear on rerun. |
| S-W-3 | `is_raining` is preserved as bool (`true`/`false`) or null. | FastF1's `Rainfall` is an integer flag in some formats; the producer normalizes it. | A null `is_raining` means the snapshot did not include a rain reading, **not** that it was dry. Don't impute false. |

## Gold layer rules, aggregates

Files: [`driver_pace.sql`](../../dbt/models/marts/driver_pace.sql), [`tire_performance.sql`](../../dbt/models/marts/tire_performance.sql), [`sector_analysis.sql`](../../dbt/models/marts/sector_analysis.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| G-AGG-1 | `driver_pace` aggregates *all* laps in the session (no in/out/SC filter). | Outlier philosophy: **flag, don't drop.** Filtering safety-car laps would require a session-level state column we do not yet capture. Better to surface the raw mean and let analysts filter via silver. | `avg_lap_time_ms` is sensitive to safety-car laps. Use `main_silver.stg_laps` directly for SC-filtered analyses. |
| G-AGG-2 | `tire_performance` groups by `tire_compound` even when null/`UNKNOWN`. | Surfacing UNKNOWN as its own row is more honest than silently dropping it. | One extra row per session in some sessions; analysts can filter on the fly. |
| G-AGG-3 | `stddev_samp` is the *sample* stddev. | DuckDB-native, matches research convention as an unbiased estimator. | Use `lap_std_dev_ms` only as a rough consistency proxy; with very few laps it is noisy. |

## Gold layer rules, dimensions

Files: [`dim_drivers.sql`](../../dbt/models/marts/dim_drivers.sql), [`dim_circuits.sql`](../../dbt/models/marts/dim_circuits.sql), [`dim_races.sql`](../../dbt/models/marts/dim_races.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| G-DIM-1 | `dim_drivers` keeps the latest `(team, nationality)` per `driver_id` via the intermediate `int_drivers_latest` model (`row_number() OVER (driver_id ORDER BY year DESC, round DESC)`). | Type-1 SCD. The use cases for this dataset (pace analysis, race-strategy aggregates) do not require historical team membership. | A driver who switched teams mid-season appears under their *most recent* team only. For mid-season analyses, join `main_silver.stg_race_results` directly. |
| G-DIM-2 | `dim_circuits` and `dim_races` read from version-controlled dbt seeds (`dbt/seeds/circuits.csv`, `races.csv`). Seeds are refreshed by `ingestion/load_schedules.py`. | Decoupling the schedule from gold-build: once seeds exist, gold rebuilds without network access. Improves reproducibility. | A schedule change after seed-refresh is invisible until `load_schedules.py` runs again. Acceptable: the schedule is low-volatility. |
| G-DIM-3 | Both dim seeds are inner-joined to the set of `race_year` values present in `stg_race_results`. | Surfaces only seasons that actually have data. Prevents an empty dimension row from a future season FastF1 has scheduled but we have not ingested. | If a year is in the seed but no races have been ingested for that year, those circuits and races are filtered out. Documented behavior. |
| G-DIM-4 | `circuit_id` slug from `Location` (lowercase, non-alphanumerics → `_`). | Stable, joinable key derived from human-readable city/locality. | Two distinct circuits in the same city would collide. None known to exist in F1. |
| G-DIM-5 | `dim_races.race_key = "{year}_{round}"`. Dedup on `race_key`. | One race per (year, round) by definition. | None. |

## Gold layer rules, facts

Files: [`fact_race_results.sql`](../../dbt/models/marts/fact_race_results.sql), [`fact_pit_stops.sql`](../../dbt/models/marts/fact_pit_stops.sql), [`fact_weather_snapshots.sql`](../../dbt/models/marts/fact_weather_snapshots.sql).

| Step | Rule | Rationale | Impact |
|---|---|---|---|
| G-FACT-1 | Add `race_key = cast(race_year as varchar) \|\| '_' \|\| cast(race_round as varchar)` to every fact. | Synthetic key for star-schema joins to `dim_races`. | None. |
| G-FACT-2 | No further filtering at fact layer. | Silver has already filtered and deduped; the fact layer is a projection. | None. |

## Quality contract (dbt tests)

The dbt project ships built-in tests that act as the project's data-quality contract. They are declared in [`dbt/models/staging/_stg_models.yml`](../../dbt/models/staging/_stg_models.yml) and [`dbt/models/marts/_marts_models.yml`](../../dbt/models/marts/_marts_models.yml). Run `cd dbt && dbt test` to execute.

Currently asserted:

| Test | Targets |
|---|---|
| `not_null` | All staging PKs (`record_id`), driver/race coord columns; all dim PKs; all fact `record_id`s |
| `unique` | All staging `record_id`s; dim PKs (`driver_id`, `circuit_id`, `race_key`); fact `record_id`s |
| `relationships` | `fact_race_results.driver_id` → `dim_drivers.driver_id` (referential integrity) |

A failing test fails the build; this is intentional.

## Outlier handling philosophy

The dataset's general principle is **flag, don't drop**. Specifically:

- A lap that is 4 standard deviations slower than the median is **not** removed. It might be a safety-car lap, a damaged-car lap, or a backmarker, analytically meaningful in different ways.
- Analysts who want a "pure pace" view should filter from `main_silver.stg_laps` directly using their own outlier policy. The dataset gives them every retained lap; it does not impose a definition of outlier.
- `tire_compound = "UNKNOWN"` is preserved rather than dropped. The frequency of UNKNOWN is itself a quality signal about the upstream feed.

## Edge-case handling

- **Red-flag laps:** typically appear with a null `LapTime`; rule B-1 drops them at bronze. The dataset therefore *under-counts* attempts in red-flag-affected sessions.
- **Safety-car laps:** kept as-is; visible in aggregates as elevated `avg_lap_time_ms` and `lap_std_dev_ms`.
- **Pit-lane crashes / DNFs mid-pit:** the in-lap is recorded; the out-lap may not be. `pit_out_ms` is therefore nullable.
- **Reserve drivers:** appear under whatever 3-letter code FastF1 returns. No reconciliation is attempted.
- **Sprint sessions:** rows for `session = S` and `session = SQ` are present and aggregated separately. Joining sprint rows to a non-sprint dim_races row will succeed because `dim_races` is keyed by `(year, round)` only, not session.

## Operational note: bronze_read OOM fix

During the full-2024-season build, the original `bronze_read` macro (which used `union_by_name=true`) caused DuckDB to OOM when scanning 1300+ NDJSON lap files. The macro was modified to drop `union_by_name=true` (bronze writers emit a stable schema, so it is unnecessary) and to set `maximum_object_size=64MB`. Changing this is itself a curation-quality decision: it is documented here, in [bronze_read.sql](../../dbt/macros/bronze_read.sql) as an inline comment, and in the risk register (T-1).

---

## Automated row-count log

The `scripts/cleaning_log.py` helper appends a dated section here with the row counts at each layer for the most recent build. The first run will populate this section.

### Build at 2026-05-06 23:34:55 UTC

_Warehouse: `data\warehouse.duckdb`_

| Layer / table | Rows in | Rows kept | Rows dropped | Drop reason(s) |
|---|---|---|---|---|
| bronze.laps (NDJSON lines) | 50838 | 50838 | 0 | (append-only; no drop at bronze) |
| silver.stg_laps | 50838 | 31759 | 19079 | null PK or duration parse failure, then qualify-dedup latest |
| silver.stg_race_results | 2257 | 1337 | 920 | null PK, then qualify-dedup latest |
| silver.stg_pit_stops | 50838 | 3336 | 47502 | non-pit laps filtered out, then qualify-dedup latest |
| silver.stg_weather | 10768 | 6321 | 4447 | null PK, then qualify-dedup latest |
| gold.driver_pace | 31759 | 1317 | (aggregate) | groupBy(year, round, session, driver) |
| gold.tire_performance | 31759 | 170 | (aggregate) | groupBy(year, round, session, tire_compound) |
| gold.sector_analysis | 31759 | 1317 | (aggregate) | groupBy(year, round, session, driver) |
| gold.fact_race_results | 1337 | 1337 | 0 | projection (no filtering) |
| gold.fact_pit_stops | 3336 | 3336 | 0 | projection |
| gold.fact_weather_snapshots | 6321 | 6321 | 0 | projection |

_Generated by `scripts/cleaning_log.py`._

### Build at 2026-05-06 23:40:50 UTC

_Warehouse: `data\warehouse.duckdb`_

| Layer / table | Rows in | Rows kept | Rows dropped | Drop reason(s) |
|---|---|---|---|---|
| bronze.laps (NDJSON lines) | 56471 | 56471 | 0 | (append-only; no drop at bronze) |
| silver.stg_laps | 56471 | 55290 | 1181 | null PK or duration parse failure, then qualify-dedup latest |
| silver.stg_race_results | 2477 | 2397 | 80 | null PK, then qualify-dedup latest |
| silver.stg_pit_stops | 56471 | 6113 | 50358 | non-pit laps filtered out, then qualify-dedup latest |
| silver.stg_weather | 11825 | 11502 | 323 | null PK, then qualify-dedup latest |
| gold.driver_pace | 55290 | 2362 | (aggregate) | groupBy(year, round, session, driver) |
| gold.tire_performance | 55290 | 293 | (aggregate) | groupBy(year, round, session, tire_compound) |
| gold.sector_analysis | 55290 | 2362 | (aggregate) | groupBy(year, round, session, driver) |
| gold.fact_race_results | 2397 | 2397 | 0 | projection (no filtering) |
| gold.fact_pit_stops | 6113 | 6113 | 0 | projection |
| gold.fact_weather_snapshots | 11502 | 11502 | 0 | projection |

_Generated by `scripts/cleaning_log.py`._
