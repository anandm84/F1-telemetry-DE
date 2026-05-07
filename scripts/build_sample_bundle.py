"""
Build the curated sample output bundle published under docs/curation/sample_output/.

This script reads from the dbt-built DuckDB warehouse at
data/warehouse.duckdb and exports a curated subset for one race weekend
(default: 2024 round 1) as Parquet + CSV. It also includes truncated
bronze NDJSON excerpts (≤200 lines per file) for transparency.

The bundle is what gets deposited to Zenodo. It does NOT include the raw
bronze in full (see LICENSING.md for the redistribution decision).

Usage:
    python scripts/build_sample_bundle.py --year 2024 --round 1
    python scripts/build_sample_bundle.py --year 2024 --round 1 --root /path/to/repo
"""

import argparse
import shutil
import sys
from pathlib import Path


SAMPLE_BRONZE_LINES = 200


SILVER_TABLES = [
    ("main_silver.stg_laps",          "stg_laps"),
    ("main_silver.stg_race_results",  "stg_race_results"),
    ("main_silver.stg_pit_stops",     "stg_pit_stops"),
    ("main_silver.stg_weather",       "stg_weather"),
]

GOLD_TABLES = [
    ("main_gold.driver_pace",                "driver_pace",            True),
    ("main_gold.tire_performance",           "tire_performance",       True),
    ("main_gold.sector_analysis",            "sector_analysis",        True),
    ("main_gold.dim_drivers",                "dim_drivers",            False),
    ("main_gold.dim_circuits",               "dim_circuits",           False),
    ("main_gold.dim_races",                  "dim_races",              False),
    ("main_gold.fact_race_results",          "fact_race_results",      True),
    ("main_gold.fact_pit_stops",             "fact_pit_stops",         True),
    ("main_gold.fact_weather_snapshots",     "fact_weather_snapshots", True),
]


def _truncate_ndjson(src: Path, dst: Path, max_lines: int) -> int:
    dst.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            if written >= max_lines:
                break
            fout.write(line)
            written += 1
    return written


def _bundle_bronze_excerpts(root: Path, sample_dir: Path, year: int, round_num: int) -> dict:
    """Truncated NDJSON excerpts for the target race only. Not a redistribution."""
    counts = {}
    bronze_root = root / "data" / "bronze"

    # The session-keyed sub-folders are 2024_1_FP1, 2024_1_R, etc.
    target_prefix = f"{year}_{round_num}_"

    for kind, glob in [
        ("laps",         "laps.ndjson*"),
        ("race_results", "results.ndjson*"),
        ("weather",      "weather.ndjson*"),
    ]:
        kind_dir = bronze_root / kind
        if not kind_dir.exists():
            continue
        out_dir = sample_dir / "bronze_excerpts" / kind
        files = 0
        lines = 0
        for sess_dir in sorted(kind_dir.iterdir()):
            if not sess_dir.is_dir() or not sess_dir.name.startswith(target_prefix):
                continue
            for src in sorted(sess_dir.glob(glob)):
                rel = sess_dir.name + "/" + src.name
                written = _truncate_ndjson(src, out_dir / rel, SAMPLE_BRONZE_LINES)
                files += 1
                lines += written
        counts[f"{kind}_files"] = files
        counts[f"{kind}_lines"] = lines

    return counts


def _export_table(con, fq_name: str, dst_parquet: Path, dst_csv: Path | None,
                  filter_year: int | None, filter_round: int | None) -> int:
    """Export a single warehouse table (optionally filtered by race) to Parquet (+ CSV)."""
    where = ""
    if filter_year is not None and filter_round is not None:
        # Some dim tables don't carry race coords; check schema first.
        try:
            cols = con.execute(f"DESCRIBE {fq_name}").fetchdf()["column_name"].tolist()
        except Exception:
            cols = []
        if "race_year" in cols and "race_round" in cols:
            where = f" WHERE race_year = {filter_year} AND race_round = {filter_round}"

    dst_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM {fq_name}{where}) "
        f"TO '{dst_parquet.as_posix()}' (FORMAT PARQUET)"
    )
    if dst_csv is not None:
        dst_csv.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY (SELECT * FROM {fq_name}{where}) "
            f"TO '{dst_csv.as_posix()}' (FORMAT CSV, HEADER TRUE)"
        )
    n = con.execute(f"SELECT COUNT(*) FROM {fq_name}{where}").fetchone()[0]
    return n


def _write_readme(sample_dir: Path, year: int, round_num: int, bronze_counts: dict, table_counts: dict) -> None:
    readme = sample_dir / "README.md"
    pace_row_str = "<populated after build>"
    if "driver_pace" in table_counts and table_counts["driver_pace"] > 0:
        pace_row_str = str(table_counts["driver_pace"])

    counts_md = "\n".join(
        f"- `{name}`: {n}"
        for name, n in sorted(table_counts.items()) if n is not None
    ) or "_(no rows; the warehouse may not be built yet)_"

    content = f"""# Curated Sample Output — {year} Round {round_num}

This directory is a small, fully self-describing example of the curated F1
telemetry dataset. It is what would be deposited to Zenodo alongside the
extraction code and documentation.

## Contents

| Subdirectory | Format | Notes |
|---|---|---|
| `bronze_excerpts/` | NDJSON, truncated to ≤{SAMPLE_BRONZE_LINES} lines per file | Illustrative only. **Not a redistribution of raw F1 timing data.** See `../LICENSING.md`. |
| `silver/` | Parquet | Type-normalized, deduplicated lap, race-result, pit-stop, and weather tables filtered to {year} R{round_num}. (Silver is the intermediate layer — Parquet only.) |
| `gold/` | Parquet | Aggregations, dimensions, and facts. Aggregations and facts are filtered to {year} R{round_num}; dimensions cover the full warehouse. |
| `gold_csv/` | UTF-8 CSV | CSV derivatives of the gold tables for users without Parquet tooling. |

## Bronze excerpt counts (this build)

- laps files: {bronze_counts.get("laps_files", 0)} (≤{SAMPLE_BRONZE_LINES} lines each, total {bronze_counts.get("laps_lines", 0)})
- race_results files: {bronze_counts.get("race_results_files", 0)} (total {bronze_counts.get("race_results_lines", 0)})
- weather files: {bronze_counts.get("weather_files", 0)} (total {bronze_counts.get("weather_lines", 0)})

## Curated row counts (this build)

{counts_md}

## Example query (DuckDB CLI)

```sql
ATTACH '../../../data/warehouse.duckdb' AS wh (READ_ONLY);

SELECT session, driver, fastest_lap_ms, laps_completed
  FROM wh.main_gold.driver_pace
  WHERE race_year = {year} AND race_round = {round_num}
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
python scripts/build_sample_bundle.py --year {year} --round {round_num}
python scripts/checksums.py
```

See `../REPRODUCE.md` for prerequisites.
"""
    readme.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Repository root (default: cwd).")
    parser.add_argument("--db", default=None, help="DuckDB warehouse path (default: <root>/data/warehouse.duckdb).")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--round", dest="round_num", type=int, default=1)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    db_path = Path(args.db).resolve() if args.db else (root / "data" / "warehouse.duckdb")
    sample_dir = root / "docs" / "curation" / "sample_output"
    sample_dir.mkdir(parents=True, exist_ok=True)

    print(f"[build_sample_bundle] target: {sample_dir}")
    print(f"[build_sample_bundle] race:   {args.year} R{args.round_num}")
    print(f"[build_sample_bundle] db:     {db_path}")

    bronze_counts = _bundle_bronze_excerpts(root, sample_dir, args.year, args.round_num)
    print(f"[build_sample_bundle] bronze excerpts: {bronze_counts}")

    table_counts: dict[str, int] = {}

    if not db_path.exists():
        print(f"[build_sample_bundle] WARNING: warehouse not found at {db_path} — skipping silver/gold export")
    else:
        try:
            import duckdb
        except ImportError:
            print("[build_sample_bundle] duckdb not installed; cannot export warehouse tables", file=sys.stderr)
            return 2

        con = duckdb.connect(str(db_path), read_only=True)
        try:
            for fq, short in SILVER_TABLES:
                dst_pq = sample_dir / "silver" / f"{short}.parquet"
                # Silver is the intermediate layer; Parquet is enough.
                # CSV derivatives are produced for the gold tables only,
                # which is where most analysts will land first.
                n = _export_table(con, fq, dst_pq, None, args.year, args.round_num)
                table_counts[short] = n
                print(f"[build_sample_bundle] silver/{short}: {n} rows")

            for fq, short, race_filter in GOLD_TABLES:
                dst_pq = sample_dir / "gold" / f"{short}.parquet"
                dst_csv = sample_dir / "gold_csv" / f"{short}.csv"
                yr = args.year if race_filter else None
                rd = args.round_num if race_filter else None
                n = _export_table(con, fq, dst_pq, dst_csv, yr, rd)
                table_counts[short] = n
                print(f"[build_sample_bundle] gold/{short}: {n} rows")
        finally:
            con.close()

    _write_readme(sample_dir, args.year, args.round_num, bronze_counts, table_counts)
    print(f"[build_sample_bundle] wrote README at {sample_dir / 'README.md'}")

    print("[build_sample_bundle] done. Now run scripts/checksums.py to refresh the manifest.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
