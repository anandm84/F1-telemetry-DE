# Curated Sample Output, 2024 Round 1

This directory is a small, fully self-describing example of the curated F1
telemetry dataset. It is what would be deposited to Zenodo alongside the
extraction code and documentation.

## Contents

| Subdirectory | Format | Notes |
|---|---|---|
| `bronze_excerpts/` | NDJSON, truncated to ≤200 lines per file | Illustrative only. **Not a redistribution of raw F1 timing data.** See `../LICENSING.md`. |
| `silver/` | Parquet | Type-normalized, deduplicated lap, race-result, pit-stop, and weather tables filtered to 2024 R1. (Silver is the intermediate layer, Parquet only.) |
| `gold/` | Parquet | Aggregations, dimensions, and facts. Aggregations and facts are filtered to 2024 R1; dimensions cover the full warehouse. |
| `gold_csv/` | UTF-8 CSV | CSV derivatives of the gold tables for users without Parquet tooling. |

## Bronze excerpt counts (this build)

- laps files: 100 (≤200 lines each, total 3489)
- race_results files: 104 (total 180)
- weather files: 5 (total 803)

## Curated row counts (this build)

- `dim_circuits`: 24
- `dim_drivers`: 33
- `dim_races`: 24
- `driver_pace`: 100
- `fact_pit_stops`: 341
- `fact_race_results`: 100
- `fact_weather_snapshots`: 480
- `sector_analysis`: 100
- `stg_laps`: 2308
- `stg_pit_stops`: 341
- `stg_race_results`: 100
- `stg_weather`: 480
- `tire_performance`: 10

## Example query (DuckDB CLI)

```sql
ATTACH '../../../data/warehouse.duckdb' AS wh (READ_ONLY);

SELECT session, driver, fastest_lap_ms, laps_completed
  FROM wh.main_gold.driver_pace
  WHERE race_year = 2024 AND race_round = 1
  ORDER BY fastest_lap_ms
  LIMIT 10;
```

…or, against the exported Parquet directly:

```sql
SELECT session, driver, fastest_lap_ms, laps_completed
  FROM read_parquet('gold/driver_pace.parquet')
  ORDER BY fastest_lap_ms
  LIMIT 10;
```

## Verifying integrity

From the repository root:

```bash
python scripts/checksums.py --verify
```

The manifest at `../checksums.sha256` covers every file in this bundle and the
documentation files in `..`.

## Re-creating this bundle

From the repository root, after a successful pipeline build:

```bash
python scripts/build_sample_bundle.py --year 2024 --round 1
python scripts/checksums.py
```

See `../REPRODUCE.md` for prerequisites.
