"""
Append a row-count snapshot to docs/curation/CLEANING_LOG.md.

Reads bronze NDJSON line counts and silver/gold row counts from the DuckDB
warehouse at data/warehouse.duckdb. Appends a dated section at the end of
the cleaning log describing what was kept vs. dropped at each layer.

Usage:
    python scripts/cleaning_log.py
    python scripts/cleaning_log.py --root /path/to/repo
    python scripts/cleaning_log.py --db data/warehouse.duckdb

The script is read-only against the data layers — it never modifies bronze
files or the warehouse.
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def _count_ndjson_lines(path: Path) -> int:
    """Count non-blank lines across every .ndjson file under `path` (recursive)."""
    if not path.exists():
        return 0
    total = 0
    for f in path.rglob("*.ndjson"):
        with f.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    total += 1
    return total


def _count_duckdb_table(con, fq_name: str) -> int:
    """Return row count for a fully-qualified DuckDB table, or -1 if missing."""
    try:
        return con.execute(f"SELECT COUNT(*) FROM {fq_name}").fetchone()[0]
    except Exception as e:
        print(f"[cleaning_log] WARN could not query {fq_name}: {e}", file=sys.stderr)
        return -1


def _build_table(root: Path, db_path: Path) -> str:
    bronze_laps = _count_ndjson_lines(root / "data" / "bronze" / "laps")
    bronze_results = _count_ndjson_lines(root / "data" / "bronze" / "race_results")
    bronze_weather = _count_ndjson_lines(root / "data" / "bronze" / "weather")

    silver_laps = silver_results = silver_pit = silver_weather = -1
    gold_pace = gold_tire = gold_sector = -1
    gold_results = gold_pit = gold_weather = -1

    if db_path.exists():
        try:
            import duckdb
        except ImportError:
            print("[cleaning_log] duckdb package not installed; skipping warehouse counts", file=sys.stderr)
        else:
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                silver_laps    = _count_duckdb_table(con, "main_silver.stg_laps")
                silver_results = _count_duckdb_table(con, "main_silver.stg_race_results")
                silver_pit     = _count_duckdb_table(con, "main_silver.stg_pit_stops")
                silver_weather = _count_duckdb_table(con, "main_silver.stg_weather")

                gold_pace    = _count_duckdb_table(con, "main_gold.driver_pace")
                gold_tire    = _count_duckdb_table(con, "main_gold.tire_performance")
                gold_sector  = _count_duckdb_table(con, "main_gold.sector_analysis")
                gold_results = _count_duckdb_table(con, "main_gold.fact_race_results")
                gold_pit     = _count_duckdb_table(con, "main_gold.fact_pit_stops")
                gold_weather = _count_duckdb_table(con, "main_gold.fact_weather_snapshots")
            finally:
                con.close()
    else:
        print(f"[cleaning_log] warehouse not found at {db_path}; only bronze counts will populate", file=sys.stderr)

    def _delta(a, b):
        if a < 0 or b < 0:
            return "n/a"
        return str(a - b)

    rows = [
        ("bronze.laps (NDJSON lines)",         bronze_laps,    bronze_laps,   "0",                                    "(append-only; no drop at bronze)"),
        ("silver.stg_laps",                    bronze_laps,    silver_laps,   _delta(bronze_laps, silver_laps),       "null PK or duration parse failure, then qualify-dedup latest"),
        ("silver.stg_race_results",            bronze_results, silver_results,_delta(bronze_results, silver_results), "null PK, then qualify-dedup latest"),
        ("silver.stg_pit_stops",               bronze_laps,    silver_pit,    _delta(bronze_laps, silver_pit),        "non-pit laps filtered out, then qualify-dedup latest"),
        ("silver.stg_weather",                 bronze_weather, silver_weather,_delta(bronze_weather, silver_weather), "null PK, then qualify-dedup latest"),
        ("gold.driver_pace",                   silver_laps,    gold_pace,     "(aggregate)",                          "groupBy(year, round, session, driver)"),
        ("gold.tire_performance",              silver_laps,    gold_tire,     "(aggregate)",                          "groupBy(year, round, session, tire_compound)"),
        ("gold.sector_analysis",               silver_laps,    gold_sector,   "(aggregate)",                          "groupBy(year, round, session, driver)"),
        ("gold.fact_race_results",             silver_results, gold_results,  _delta(silver_results, gold_results),   "projection (no filtering)"),
        ("gold.fact_pit_stops",                silver_pit,     gold_pit,      _delta(silver_pit, gold_pit),           "projection"),
        ("gold.fact_weather_snapshots",        silver_weather, gold_weather,  _delta(silver_weather, gold_weather),   "projection"),
    ]

    header = (
        "| Layer / table | Rows in | Rows kept | Rows dropped | Drop reason(s) |\n"
        "|---|---|---|---|---|\n"
    )
    body = "".join(
        f"| {name} | {rin if rin >= 0 else 'n/a'} | {rkept if rkept >= 0 else 'n/a'} | {rdrop} | {reason} |\n"
        for name, rin, rkept, rdrop, reason in rows
    )
    return header + body


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Repository root (default: cwd).")
    parser.add_argument("--db", default=None, help="DuckDB warehouse path (default: <root>/data/warehouse.duckdb).")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    db_path = Path(args.db).resolve() if args.db else (root / "data" / "warehouse.duckdb")

    log_path = root / "docs" / "curation" / "CLEANING_LOG.md"
    if not log_path.exists():
        print(f"[cleaning_log] {log_path} not found", file=sys.stderr)
        return 1

    table = _build_table(root, db_path)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    section = (
        f"\n### Build at {timestamp}\n\n"
        f"_Warehouse: `{db_path.relative_to(root) if db_path.is_relative_to(root) else db_path}`_\n\n"
        f"{table}\n"
        f"_Generated by `scripts/cleaning_log.py`._\n"
    )

    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(section)

    print(f"[cleaning_log] Appended row-count snapshot to {log_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
