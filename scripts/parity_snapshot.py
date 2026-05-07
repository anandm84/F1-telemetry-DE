"""Snapshot row counts and key aggregates from gold tables for parity testing.

Run once on PySpark output to create the baseline, then again on dbt output to compare.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD = PROJECT_ROOT / "data" / "gold"


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _stats(df: pd.DataFrame, group_cols: list[str], num_cols: list[str]) -> dict:
    out = {"row_count": int(len(df))}
    if not df.empty:
        for c in num_cols:
            if c in df.columns:
                s = pd.to_numeric(df[c], errors="coerce").dropna()
                out[f"{c}_sum"] = float(s.sum()) if not s.empty else 0.0
                out[f"{c}_min"] = float(s.min()) if not s.empty else None
                out[f"{c}_max"] = float(s.max()) if not s.empty else None
        if group_cols:
            present = [c for c in group_cols if c in df.columns]
            if present:
                out["group_count"] = int(df[present].drop_duplicates().shape[0])
    return out


def snapshot() -> dict:
    return {
        "driver_pace": _stats(
            _read(GOLD / "driver_pace"),
            ["race_year", "race_round", "session", "driver"],
            ["avg_lap_time_ms", "fastest_lap_ms", "lap_std_dev_ms", "laps_completed"],
        ),
        "tire_performance": _stats(
            _read(GOLD / "tire_performance"),
            ["race_year", "race_round", "session", "tire_compound"],
            ["avg_lap_time_ms", "laps_run"],
        ),
        "sector_analysis": _stats(
            _read(GOLD / "sector_analysis"),
            ["race_year", "race_round", "session", "driver"],
            ["avg_sector1_ms", "avg_sector2_ms", "avg_sector3_ms"],
        ),
        "dim_drivers": _stats(_read(GOLD / "dims" / "dim_drivers"), ["driver_id"], []),
        "dim_circuits": _stats(_read(GOLD / "dims" / "dim_circuits"), ["circuit_id"], []),
        "dim_races": _stats(_read(GOLD / "dims" / "dim_races"), ["race_key"], []),
        "fact_race_results": _stats(
            _read(GOLD / "facts" / "fact_race_results"),
            ["race_key", "driver_id"],
            ["position", "grid_position", "points"],
        ),
        "fact_pit_stops": _stats(
            _read(GOLD / "facts" / "fact_pit_stops"),
            ["race_key", "driver_id", "lap_number"],
            ["pit_in_ms", "pit_out_ms"],
        ),
        "fact_weather_snapshots": _stats(
            _read(GOLD / "facts" / "fact_weather_snapshots"),
            ["race_key", "snapshot_index"],
            ["air_temp_c", "track_temp_c", "humidity_pct", "pressure_mbar", "wind_speed_ms"],
        ),
    }


def compare(a: dict, b: dict, tol: float = 1e-3) -> list[str]:
    diffs: list[str] = []
    for table, a_stats in a.items():
        if table not in b:
            diffs.append(f"{table}: missing in current snapshot")
            continue
        b_stats = b[table]
        for k, av in a_stats.items():
            bv = b_stats.get(k)
            if av is None and bv is None:
                continue
            if isinstance(av, float) and isinstance(bv, float):
                if math.isnan(av) and math.isnan(bv):
                    continue
                if abs(av - bv) > tol * max(1.0, abs(av)):
                    diffs.append(f"{table}.{k}: baseline={av} current={bv}")
            elif av != bv:
                diffs.append(f"{table}.{k}: baseline={av} current={bv}")
    return diffs


def snapshot_from_duckdb(db_path: Path) -> dict:
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        def fetch(query: str) -> pd.DataFrame:
            return con.execute(query).fetchdf()

        return {
            "driver_pace": _stats(
                fetch("select * from main_gold.driver_pace"),
                ["race_year", "race_round", "session", "driver"],
                ["avg_lap_time_ms", "fastest_lap_ms", "lap_std_dev_ms", "laps_completed"],
            ),
            "tire_performance": _stats(
                fetch("select * from main_gold.tire_performance"),
                ["race_year", "race_round", "session", "tire_compound"],
                ["avg_lap_time_ms", "laps_run"],
            ),
            "sector_analysis": _stats(
                fetch("select * from main_gold.sector_analysis"),
                ["race_year", "race_round", "session", "driver"],
                ["avg_sector1_ms", "avg_sector2_ms", "avg_sector3_ms"],
            ),
            "dim_drivers": _stats(fetch("select * from main_gold.dim_drivers"), ["driver_id"], []),
            "dim_circuits": _stats(fetch("select * from main_gold.dim_circuits"), ["circuit_id"], []),
            "dim_races": _stats(fetch("select * from main_gold.dim_races"), ["race_key"], []),
            "fact_race_results": _stats(
                fetch("select * from main_gold.fact_race_results"),
                ["race_key", "driver_id"],
                ["position", "grid_position", "points"],
            ),
            "fact_pit_stops": _stats(
                fetch("select * from main_gold.fact_pit_stops"),
                ["race_key", "driver_id", "lap_number"],
                ["pit_in_ms", "pit_out_ms"],
            ),
            "fact_weather_snapshots": _stats(
                fetch("select * from main_gold.fact_weather_snapshots"),
                ["race_key", "snapshot_index"],
                ["air_temp_c", "track_temp_c", "humidity_pct", "pressure_mbar", "wind_speed_ms"],
            ),
        }
    finally:
        con.close()


def main() -> None:
    out_path = PROJECT_ROOT / "scripts" / "parity_baseline.json"
    if len(sys.argv) > 1 and sys.argv[1] == "compare":
        baseline = json.loads(out_path.read_text())
        current = snapshot()
        diffs = compare(baseline, current)
        if diffs:
            print("PARITY MISMATCH:")
            for d in diffs:
                print(f"  {d}")
            sys.exit(1)
        print(f"PARITY OK across {len(current)} tables")
        return

    if len(sys.argv) > 1 and sys.argv[1] == "compare-duckdb":
        db_path = PROJECT_ROOT / "data" / "warehouse.duckdb"
        baseline = json.loads(out_path.read_text())
        current = snapshot_from_duckdb(db_path)
        diffs = compare(baseline, current)
        if diffs:
            print("PARITY MISMATCH:")
            for d in diffs:
                print(f"  {d}")
            sys.exit(1)
        print(f"PARITY OK across {len(current)} tables (dbt vs PySpark baseline)")
        return

    snap = snapshot()
    out_path.write_text(json.dumps(snap, indent=2, sort_keys=True))
    print(f"Wrote baseline to {out_path}")
    for table, stats in snap.items():
        print(f"  {table}: {stats['row_count']} rows")


if __name__ == "__main__":
    main()
