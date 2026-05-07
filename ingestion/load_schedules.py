"""Pull FastF1 event schedules and write CSV seeds for dbt.

Replaces the FastF1 portion of the old gold_dimensions_job.py. Run once per
season (or whenever the calendar updates). The CSVs land in dbt/seeds/ and
get loaded into DuckDB via `dbt seed`.
"""

from __future__ import annotations

import csv
import os
import re
from pathlib import Path

import fastf1

PROJECT_ROOT = Path(os.getenv("F1_PROJECT_ROOT", Path(__file__).resolve().parent.parent))
CACHE_DIR = PROJECT_ROOT / "cache"
SEEDS_DIR = PROJECT_ROOT / "dbt" / "seeds"

DEFAULT_YEARS = [2023, 2024]


def _slug(text: str | None) -> str:
    if not text:
        return "unknown"
    return re.sub(r"[^a-z0-9]+", "_", str(text).strip().lower()).strip("_") or "unknown"


def _years_from_env() -> list[int]:
    raw = os.getenv("F1_SEED_YEARS")
    if not raw:
        return DEFAULT_YEARS
    return [int(y.strip()) for y in raw.split(",") if y.strip()]


def main() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    fastf1.Cache.enable_cache(str(CACHE_DIR))

    circuits: list[dict] = []
    races: list[dict] = []

    for year in sorted(_years_from_env()):
        try:
            schedule = fastf1.get_event_schedule(year, include_testing=False)
        except Exception as e:
            print(f"[load_schedules] could not fetch {year}: {e}")
            continue

        for _, event in schedule.iterrows():
            round_num = event.get("RoundNumber")
            if round_num is None or round_num != round_num:
                continue
            round_num = int(round_num)

            location = str(event.get("Location", "") or "")
            circuit_id = _slug(location)
            circuits.append({
                "circuit_id": circuit_id,
                "circuit_name": str(event.get("OfficialEventName", "") or ""),
                "country": str(event.get("Country", "") or ""),
                "locality": location,
                "seed_year": year,
            })

            event_date = event.get("EventDate")
            try:
                race_date = str(event_date.date()) if hasattr(event_date, "date") else str(event_date or "")
            except Exception:
                race_date = ""

            races.append({
                "race_key": f"{year}_{round_num}",
                "race_year": year,
                "race_round": round_num,
                "race_name": str(event.get("EventName", "") or ""),
                "circuit_id": circuit_id,
                "race_date": race_date,
            })

    _write_csv(SEEDS_DIR / "circuits.csv", circuits, ["circuit_id", "circuit_name", "country", "locality", "seed_year"])
    _write_csv(SEEDS_DIR / "races.csv", races, ["race_key", "race_year", "race_round", "race_name", "circuit_id", "race_date"])
    print(f"[load_schedules] wrote {len(circuits)} circuit rows, {len(races)} race rows")


def _write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
