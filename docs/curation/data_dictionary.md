# Data Dictionary

This dictionary describes every column in every layer (Bronze → Silver → Gold) of the curated F1 telemetry dataset. Source-system columns from FastF1 are noted where relevant. Units are given explicitly; units that differ from FastF1's native types (e.g., milliseconds vs. `pandas.Timedelta`) reflect a deliberate transformation.

The Silver and Gold layers are dbt-managed tables in a single-file DuckDB warehouse at `data/warehouse.duckdb`. Silver tables live in the `main_silver` schema; Gold tables in `main_gold`. The dbt project is at `dbt/`.

Conventions:
- **PK** = primary key. **FK** = foreign key.
- "Lineage" columns starting with `_` are operational metadata, not analytical.
- "Source field" refers to the underlying FastF1 column name where applicable.
- "Source model" refers to the dbt model that materializes the table.

---

## 1. Bronze layer

Append-only NDJSON, partitioned by *session* (sub-folder per `{year}_{round}_{session}`) and (for laps and results) by driver. Read into dbt staging models via the `bronze_read()` macro at [dbt/macros/bronze_read.sql](../../dbt/macros/bronze_read.sql).

### 1.1 `data/bronze/laps/{year}_{round}_{session}/laps.ndjson.driver-{D}.ndjson`

| Column | Type | Units | Source field | Description |
|---|---|---|---|---|
| `data_type` | string | - | (constant `"lap"`) | Discriminator written by producer; helps if multiple data types share a topic. |
| `Driver` | string | - | `Lap.Driver` | 3-letter driver abbreviation (e.g., `VER`, `HAM`). May contain leading/trailing whitespace at this layer. |
| `LapNumber` | float | laps | `Lap.LapNumber` | Lap index within the session. Float in bronze because FastF1 emits NaN for unloaded laps. |
| `LapTime` | string | - | `Lap.LapTime` | Lap duration formatted `HH:MM:SS:NNNN` (where the last group is 1/10000-second fractional units). Encoded by `_format_timedelta_hhmmssmmmm` in [ingestion/producer.py](../../ingestion/producer.py). |
| `Sector1Time` | string | - | `Lap.Sector1Time` | Same encoding as `LapTime`. |
| `Sector2Time` | string | - | `Lap.Sector2Time` | Same encoding as `LapTime`. |
| `Sector3Time` | string | - | `Lap.Sector3Time` | Same encoding as `LapTime`. |
| `Compound` | string | - | `Lap.Compound` | Tire compound. Allowed values from FastF1: `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`, `UNKNOWN`. May appear in mixed case at bronze. |
| `PitInTime_ms` | int | milliseconds since session start | `Lap.PitInTime` | Time the driver entered pit lane on this lap, or null. |
| `PitOutTime_ms` | int | milliseconds since session start | `Lap.PitOutTime` | Time the driver exited pit lane on this lap, or null. |
| `event_ts` | string (ISO-8601 UTC) | - | (assigned at emit) | Wall-clock timestamp when producer emitted the message. |
| `race_year` | int | - | (assigned) | Season year (e.g., `2024`). |
| `race_round` | int | - | (assigned) | Round number within the season (1 = season opener). |
| `session` | string | - | (assigned) | Session code: `FP1`, `FP2`, `FP3`, `Q`, `S` (sprint), `SQ` (sprint quali), `R` (race). |
| `record_id` | string (40 hex) | - | (computed) | SHA-1 of `\|`-joined `(year, round, session, driver, lap_number, lap_time)`. Stable, deterministic dedup key. |
| `_kafka_topic` | string | - | (consumer) | Kafka topic the record was read from (`f1_lap_times`); set to `"backfill"` for backfill writes. |
| `_kafka_partition` | int | - | (consumer) | Kafka partition; `-1` for backfill. |
| `_kafka_offset` | int | - | (consumer) | Kafka offset; `-1` for backfill. |
| `_kafka_timestamp` | int | ms since epoch | (consumer) | Kafka broker timestamp; `-1` for backfill. |
| `_bronze_written_at` | string (ISO-8601 UTC) | - | (consumer) | Wall-clock timestamp when the bronze writer flushed the record. |

### 1.2 `data/bronze/race_results/{year}_{round}_{session}/results.ndjson*`

| Column | Type | Units | Source field | Description |
|---|---|---|---|---|
| `record_id` | string (40 hex) | - | (computed) | SHA-1 of `(year, round, session, driver_id)`. |
| `race_year` | int | - | (assigned) | Season year. |
| `race_round` | int | - | (assigned) | Round number. |
| `session` | string | - | (assigned) | Session code. |
| `driver_id` | string | - | `Result.Abbreviation` | Upper-cased, trimmed driver abbreviation. PK component. |
| `full_name` | string | - | `Result.FullName` | Driver's full name as supplied by FastF1 results frame. |
| `team` | string | - | `Result.TeamName` | Constructor name at time of race. |
| `nationality` | string | - | `Result.CountryCode` | ISO-style 3-letter country code (FastF1 returns `CountryCode`, not nationality string). |
| `position` | int | - | `Result.Position` | Final classified position. Null if unclassified / data missing. |
| `grid_position` | int | - | `Result.GridPosition` | Starting grid slot. |
| `points` | float | championship points | `Result.Points` | Points awarded for the race. |
| `status` | string | - | `Result.Status` | Free-text status from F1 (e.g., `Finished`, `+1 Lap`, `Engine`, `Accident`). |
| `event_ts`, `_kafka_*`, `_bronze_written_at` | - | - | - | Same as bronze laps. |

### 1.3 `data/bronze/weather/{year}_{round}_{session}/weather.ndjson*`

| Column | Type | Units | Source field | Description |
|---|---|---|---|---|
| `record_id` | string (40 hex) | - | (computed) | SHA-1 of `(year, round, session, snapshot_index)`. |
| `race_year` | int | - | (assigned) | Season year. |
| `race_round` | int | - | (assigned) | Round number. |
| `session` | string | - | (assigned) | Session code. |
| `snapshot_index` | int | - | (assigned, row index) | Zero-based ordinal of the weather snapshot within the session. |
| `time_offset_ms` | int | milliseconds since session start | `Weather.Time` | Snapshot time relative to session start. |
| `air_temp_c` | float | °C | `Weather.AirTemp` | Ambient air temperature. |
| `track_temp_c` | float | °C | `Weather.TrackTemp` | Track surface temperature. |
| `humidity_pct` | float | % | `Weather.Humidity` | Relative humidity. |
| `pressure_mbar` | float | mbar (= hPa) | `Weather.Pressure` | Atmospheric pressure. |
| `wind_speed_ms` | float | m·s⁻¹ | `Weather.WindSpeed` | Wind speed. |
| `is_raining` | bool | - | `Weather.Rainfall` | True if rain present at snapshot. |
| `event_ts`, `_kafka_*`, `_bronze_written_at` | - | - | - | Same as bronze laps. |

---

## 2. Silver layer (DuckDB schema `main_silver`)

dbt-materialized tables, deduplicated on `record_id` via `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1`, types normalized, durations converted to milliseconds via the [`duration_to_ms`](../../dbt/macros/duration_to_ms.sql) macro.

### 2.1 `main_silver.stg_laps`, source model: [dbt/models/staging/stg_laps.sql](../../dbt/models/staging/stg_laps.sql)

| Column | Type | Units | Description |
|---|---|---|---|
| `record_id` | VARCHAR | - | **PK.** Carried from bronze. dbt test: `unique`, `not_null`. |
| `race_year` | INTEGER | - | Season year. dbt test: `not_null`. |
| `race_round` | INTEGER | - | Round number. dbt test: `not_null`. |
| `session` | VARCHAR | - | Session code. |
| `event_timestamp` | TIMESTAMP | UTC | `try_cast(event_ts as timestamp)` from bronze. |
| `bronze_written_at` | TIMESTAMP | UTC | Bronze writer flush time. |
| `driver_id` | VARCHAR | - | Upper-cased, trimmed driver code. dbt test: `not_null`. |
| `driver` | VARCHAR | - | Alias of `driver_id`, retained for backwards compatibility with downstream gold queries. |
| `lap_number` | INTEGER | laps | Lap index within session. dbt test: `not_null`. |
| `lap_time_ms` | BIGINT | milliseconds | Lap duration parsed from `HH:MM:SS:NNNN` via `duration_to_ms` macro. dbt test: `not_null`. |
| `lap_time` | BIGINT | milliseconds | Alias of `lap_time_ms` retained for backwards compatibility. |
| `sector1_ms` | BIGINT | milliseconds | Sector 1 split. |
| `sector2_ms` | BIGINT | milliseconds | Sector 2 split. |
| `sector3_ms` | BIGINT | milliseconds | Sector 3 split. |
| `tire_compound` | VARCHAR | - | Upper-cased compound. Allowed values: `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`, `UNKNOWN`. |
| `PitInTime_ms` | BIGINT | milliseconds | Pit-in time on this lap, if any. |
| `PitOutTime_ms` | BIGINT | milliseconds | Pit-out time on this lap, if any. |
| `_kafka_topic` / `_kafka_partition` / `_kafka_offset` / `_kafka_timestamp` | - | - | Carried from bronze for lineage. |

**Quality filters applied:** `record_id IS NOT NULL`, `event_ts IS NOT NULL`, `Driver IS NOT NULL AND trim(Driver) <> ''`, `LapNumber IS NOT NULL`, `lap_time_ms IS NOT NULL`. Then `qualify row_number() over (partition by record_id order by event_timestamp desc) = 1` keeps the *latest* duplicate by `event_timestamp`.

### 2.2 `main_silver.stg_race_results`, source: [dbt/models/staging/stg_race_results.sql](../../dbt/models/staging/stg_race_results.sql)

| Column | Type | Description |
|---|---|---|
| `record_id` | VARCHAR | **PK.** dbt test: `unique`, `not_null`. |
| `race_year` / `race_round` / `session` | INTEGER / INTEGER / VARCHAR | Race coordinates. dbt: `not_null` on year and round. |
| `event_timestamp` / `bronze_written_at` | TIMESTAMP | Lineage timestamps. |
| `driver_id` | VARCHAR | Upper-cased driver code. dbt: `not_null`. |
| `full_name`, `team`, `nationality` | VARCHAR | Carried from bronze. |
| `position`, `grid_position` | INTEGER | Final / starting position. |
| `points` | DOUBLE | Championship points. |
| `status` | VARCHAR | F1 result status. |
| `_kafka_*` | - | Lineage. |

**Quality filters:** `record_id IS NOT NULL`, `driver_id IS NOT NULL AND trim(driver_id) <> ''`, `race_year IS NOT NULL`, `race_round IS NOT NULL`, then qualify-dedup on latest `event_ts`.

### 2.3 `main_silver.stg_pit_stops`, source: [dbt/models/staging/stg_pit_stops.sql](../../dbt/models/staging/stg_pit_stops.sql)

Built from the same bronze laps NDJSON via the `bronze_read('laps', …)` macro, but only for rows where `PitInTime_ms` is non-null.

| Column | Type | Units | Description |
|---|---|---|---|
| `record_id` | VARCHAR | - | **PK.** Same SHA-1 as the lap row. dbt test: `unique`, `not_null`. |
| `race_year`, `race_round`, `session` | - | - | Race coordinates. |
| `event_timestamp`, `bronze_written_at` | TIMESTAMP | UTC | Lineage. |
| `driver_id` | VARCHAR | - | Upper-cased driver code. dbt: `not_null`. |
| `lap_number` | INTEGER | laps | Lap on which the stop occurred. dbt: `not_null`. |
| `pit_in_ms` | BIGINT | ms since session start | Pit-in time. dbt: `not_null`. |
| `pit_out_ms` | BIGINT | ms since session start | Pit-out time (may be null if the driver did not re-emerge in the same lap row). |
| `tire_compound_before` | VARCHAR | - | The tire compound the driver was running *before* the stop. The compound after is inferred from the next lap. |
| `_kafka_*` | - | - | Lineage. |

### 2.4 `main_silver.stg_weather`, source: [dbt/models/staging/stg_weather.sql](../../dbt/models/staging/stg_weather.sql)

Same columns as bronze weather plus typed `event_timestamp` and `bronze_written_at`. `record_id` is PK; deduped on it (latest wins).

---

## 2.5 Intermediate

### `main_intermediate.int_drivers_latest`, source: [dbt/models/intermediate/int_drivers_latest.sql](../../dbt/models/intermediate/int_drivers_latest.sql)

For each `driver_id`, picks the row with the highest `(race_year, race_round)` from `stg_race_results`. Used by `dim_drivers`. Type-1 SCD pattern.

---

## 3. Gold layer (DuckDB schema `main_gold`, analytics-ready)

### 3.1 Aggregations

`driver_pace`, source: [dbt/models/marts/driver_pace.sql](../../dbt/models/marts/driver_pace.sql)

| Column | Type | Units | Description |
|---|---|---|---|
| `race_year`, `race_round`, `session`, `driver` | INTEGER/INTEGER/VARCHAR/VARCHAR | - | **Composite PK.** dbt test: `not_null` on `driver`. |
| `avg_lap_time_ms` | DOUBLE | ms | Mean lap time across all laps in the session for the driver. **Includes** in/out laps and safety-car laps. |
| `fastest_lap_ms` | BIGINT | ms | Minimum lap time in the session. |
| `lap_std_dev_ms` | DOUBLE | ms | Sample standard deviation of lap times, a rough consistency proxy. |
| `laps_completed` | BIGINT | laps | Row count per group. |

`tire_performance`, source: [dbt/models/marts/tire_performance.sql](../../dbt/models/marts/tire_performance.sql)

| Column | Type | Units | Description |
|---|---|---|---|
| `race_year`, `race_round`, `session`, `tire_compound` | - | - | **Composite PK.** |
| `avg_lap_time_ms` | DOUBLE | ms | Mean lap time on this compound in this session. |
| `laps_run` | BIGINT | laps | Row count per group. |

`sector_analysis`, source: [dbt/models/marts/sector_analysis.sql](../../dbt/models/marts/sector_analysis.sql)

| Column | Type | Units | Description |
|---|---|---|---|
| `race_year`, `race_round`, `session`, `driver` | - | - | **Composite PK.** |
| `avg_sector1_ms`, `avg_sector2_ms`, `avg_sector3_ms` | DOUBLE | ms | Mean sector splits across the session. |

### 3.2 Dimensions

`dim_drivers`, source: [dbt/models/marts/dim_drivers.sql](../../dbt/models/marts/dim_drivers.sql) (built from `int_drivers_latest`; Type-1 SCD)

| Column | Type | Description |
|---|---|---|
| `driver_id` | VARCHAR | **PK.** Upper-cased 3-letter code. dbt: `unique`, `not_null`. |
| `full_name` | VARCHAR | Driver full name from latest result. |
| `team` | VARCHAR | Most recent constructor seen. |
| `nationality` | VARCHAR | Most recent country code seen. |
| `_updated_at` | TIMESTAMP | When this dim row was last refreshed (`current_timestamp`). |

`dim_circuits`, source: [dbt/models/marts/dim_circuits.sql](../../dbt/models/marts/dim_circuits.sql); seeded from [dbt/seeds/circuits.csv](../../dbt/seeds/circuits.csv) (refreshed by `ingestion/load_schedules.py`).

| Column | Type | Description |
|---|---|---|
| `circuit_id` | VARCHAR | **PK.** Slug derived from FastF1 `Location`. dbt: `unique`, `not_null`. |
| `circuit_name` | VARCHAR | `OfficialEventName` from FastF1 schedule. |
| `country` | VARCHAR | Country name. |
| `locality` | VARCHAR | FastF1 `Location` (city / locality). |
| `_updated_at` | TIMESTAMP | Refresh time. |

`dim_races`, source: [dbt/models/marts/dim_races.sql](../../dbt/models/marts/dim_races.sql); seeded from [dbt/seeds/races.csv](../../dbt/seeds/races.csv).

| Column | Type | Description |
|---|---|---|
| `race_key` | VARCHAR | **PK.** `{year}_{round}`. dbt: `unique`, `not_null`. |
| `race_year` | INTEGER | Season year. dbt: `not_null`. |
| `race_round` | INTEGER | Round number. dbt: `not_null`. |
| `race_name` | VARCHAR | `EventName` from FastF1. |
| `circuit_id` | VARCHAR | **FK** → `dim_circuits.circuit_id`. |
| `race_date` | VARCHAR | Event date as `YYYY-MM-DD` string. |

### 3.3 Facts

`fact_race_results`, source: [dbt/models/marts/fact_race_results.sql](../../dbt/models/marts/fact_race_results.sql)

| Column | Type | Description |
|---|---|---|
| `record_id` | VARCHAR | Carried from silver. dbt: `unique`, `not_null`. |
| `race_year`, `race_round`, `session`, `race_key` | - | Race coordinates; `race_key` joins `dim_races`. |
| `driver_id` | VARCHAR | **FK** → `dim_drivers.driver_id`. dbt: `not_null`, `relationships`. |
| `full_name`, `team` | VARCHAR | Denormalized for query convenience. |
| `position`, `grid_position` | INTEGER | Classified / starting positions. |
| `points` | DOUBLE | Championship points awarded. |
| `status` | VARCHAR | F1 result status string. |

`fact_pit_stops`, source: [dbt/models/marts/fact_pit_stops.sql](../../dbt/models/marts/fact_pit_stops.sql)

| Column | Type | Description |
|---|---|---|
| `record_id` | VARCHAR | Carried from silver. dbt: `unique`, `not_null`. |
| `race_year`, `race_round`, `session`, `race_key` | - | Race coordinates. |
| `driver_id` | VARCHAR | **FK** → `dim_drivers.driver_id`. |
| `lap_number` | INTEGER | Lap of the stop. |
| `pit_in_ms`, `pit_out_ms` | BIGINT | Pit timing in ms since session start. |
| `tire_compound_before` | VARCHAR | Compound before the stop. |

`fact_weather_snapshots`, source: [dbt/models/marts/fact_weather_snapshots.sql](../../dbt/models/marts/fact_weather_snapshots.sql)

| Column | Type | Description |
|---|---|---|
| `record_id` | VARCHAR | Carried from silver. dbt: `unique`, `not_null`. |
| `race_year`, `race_round`, `session`, `race_key` | - | Race coordinates. |
| `snapshot_index` | INTEGER | Ordinal within session. |
| `time_offset_ms` | BIGINT | ms since session start. |
| `air_temp_c`, `track_temp_c`, `humidity_pct`, `pressure_mbar`, `wind_speed_ms` | DOUBLE | Weather measurements. |
| `is_raining` | BOOLEAN | Rain flag. |

---

## 4. Interpretive notes (codebook)

These notes go beyond schema and explain *what the analytical units mean*, which schema docs alone cannot convey:

- **`fastest_lap_ms` is a raw minimum.** It does **not** filter out in-laps, out-laps, safety-car laps, or red-flag laps. For "purest pace" analyses, downstream users should filter `main_silver.stg_laps` by `PitInTime_ms IS NULL AND PitOutTime_ms IS NULL` and join against any session-level interruption table (not provided in this dataset).
- **`avg_lap_time_ms` is an arithmetic mean of all kept laps.** It is sensitive to a small number of slow laps (safety car, traffic). For comparing drivers, prefer median or trimmed mean derived from `stg_laps` directly.
- **`tire_compound = UNKNOWN`** appears when FastF1 could not resolve compound for that lap. It is preserved rather than dropped so users see the gap.
- **Sprint sessions are present.** `session` codes `S` (sprint race) and `SQ` (sprint qualifying) are valid alongside `R`/`Q`/`FP1-3`. Aggregates are computed *per session*, so sprint and main race appear as separate rows.
- **Null propagation in aggregates.** DuckDB's `avg`/`stddev_samp` ignore nulls. A driver with one missing sector time still gets a sector average computed from the non-null laps; their `laps_completed` count from `driver_pace` reflects all laps with a non-null `lap_time_ms`.

---

## 5. Cross-references

- See [PROVENANCE.md](PROVENANCE.md) for upstream lineage and retrieval context.
- See [CLEANING_LOG.md](CLEANING_LOG.md) for transformation row-counts.
- See [LICENSING.md](LICENSING.md) for redistribution constraints on these columns.
- dbt test definitions live in [dbt/models/staging/_stg_models.yml](../../dbt/models/staging/_stg_models.yml) and [dbt/models/marts/_marts_models.yml](../../dbt/models/marts/_marts_models.yml). Run `dbt test` from the `dbt/` directory to execute them.
