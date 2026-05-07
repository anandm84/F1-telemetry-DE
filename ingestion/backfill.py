"""
Historical backfill — writes directly to bronze subdirectories, bypassing Kafka.

Usage:
    BACKFILL_YEARS=2023,2024 BACKFILL_ROUNDS=all BACKFILL_SESSIONS=R python ingestion/backfill.py

The manifest at BACKFILL_MANIFEST_PATH tracks completed sessions so reruns are safe.
Silver jobs deduplicate on record_id, so partial re-ingestion is also safe.
"""

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone

import fastf1

fastf1.Cache.enable_cache("cache")

# ── Configuration ─────────────────────────────────────────────────────────────

BACKFILL_YEARS = [int(y.strip()) for y in os.getenv("BACKFILL_YEARS", "2023").split(",")]
BACKFILL_ROUNDS_RAW = os.getenv("BACKFILL_ROUNDS", "all").strip()
BACKFILL_SESSIONS = [s.strip() for s in os.getenv("BACKFILL_SESSIONS", "R").split(",")]
BACKFILL_DATA_TYPES = {t.strip() for t in os.getenv("BACKFILL_DATA_TYPES", "laps,race_results,weather").split(",")}
BACKFILL_MANIFEST_PATH = os.getenv("BACKFILL_MANIFEST_PATH", "data/backfill_manifest.jsonl")
BRONZE_BASE_DIR = os.getenv("BRONZE_BASE_DIR", "data/bronze")
BACKFILL_FORCE = os.getenv("BACKFILL_FORCE", "false").strip().lower() in {"1", "true", "yes"}
BACKFILL_SLEEP_SECONDS = float(os.getenv("BACKFILL_SLEEP_SECONDS", "2"))

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_record_id(*parts):
    key = "|".join(str(p) for p in parts)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _safe_token(value, fallback="UNKNOWN"):
    if value is None:
        return fallback
    token = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value).strip())
    return token or fallback


def _session_key(year, round_num, session_code):
    return f"{_safe_token(year, 'UNKNOWN_YEAR')}_{_safe_token(round_num, 'UNKNOWN_ROUND')}_{_safe_token(session_code, 'UNKNOWN_SESSION')}"


def _to_maybe_float(value):
    if value is None:
        return None
    try:
        f = float(value)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _timedelta_to_ms(value):
    if value is None or not hasattr(value, "total_seconds"):
        return None
    try:
        total_seconds = float(value.total_seconds())
        return None if total_seconds != total_seconds else int(round(total_seconds * 1000))
    except (TypeError, ValueError):
        return None


def _format_timedelta_hhmmssmmmm(value):
    if value is None or not hasattr(value, "total_seconds"):
        return None
    total_seconds = float(value.total_seconds())
    if total_seconds != total_seconds:
        return None
    total_seconds_int = int(total_seconds)
    hours = total_seconds_int // 3600
    minutes = (total_seconds_int % 3600) // 60
    seconds = total_seconds_int % 60
    frac_4 = int(round((total_seconds - total_seconds_int) * 10000))
    if frac_4 == 10000:
        frac_4 = 0
        seconds += 1
        if seconds == 60:
            seconds = 0
            minutes += 1
            if minutes == 60:
                minutes = 0
                hours += 1
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}:{frac_4:04d}"


# ── Manifest ──────────────────────────────────────────────────────────────────

def _load_manifest():
    """Return set of (year, round, session) tuples already completed successfully."""
    completed = set()
    if not os.path.exists(BACKFILL_MANIFEST_PATH):
        return completed
    with open(BACKFILL_MANIFEST_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                if entry.get("status") == "ok":
                    key = (entry["race_year"], entry["race_round"], entry["session"])
                    completed.add(key)
            except (json.JSONDecodeError, KeyError):
                pass
    return completed


def _append_manifest(entry):
    os.makedirs(os.path.dirname(BACKFILL_MANIFEST_PATH) or ".", exist_ok=True)
    with open(BACKFILL_MANIFEST_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# ── Session discovery ─────────────────────────────────────────────────────────

def _build_session_list():
    sessions = []
    for year in BACKFILL_YEARS:
        if BACKFILL_ROUNDS_RAW == "all":
            try:
                schedule = fastf1.get_event_schedule(year, include_testing=False)
                rounds = schedule["RoundNumber"].dropna().astype(int).tolist()
            except Exception as e:
                print(f"[backfill] Could not fetch schedule for {year}: {e}")
                continue
        else:
            rounds = [int(r.strip()) for r in BACKFILL_ROUNDS_RAW.split(",")]

        for round_num in rounds:
            for session_type in BACKFILL_SESSIONS:
                sessions.append((year, round_num, session_type))

    return sessions


# ── Bronze writers ─────────────────────────────────────────────────────────────

_BACKFILL_LINEAGE = {
    "_kafka_topic": "backfill",
    "_kafka_partition": -1,
    "_kafka_offset": -1,
    "_kafka_timestamp": -1,
}


def _write_bronze_laps(session_obj, year, round_num, session_code, bronze_base):
    laps_dir = os.path.join(bronze_base, "laps", _session_key(year, round_num, session_code))
    os.makedirs(laps_dir, exist_ok=True)

    try:
        laps = session_obj.laps[
            ["Driver", "LapNumber", "LapTime", "Sector1Time", "Sector2Time",
             "Sector3Time", "Compound", "PitInTime", "PitOutTime"]
        ].copy()
    except Exception as e:
        print(f"[backfill][laps] Could not access laps data: {e}")
        return 0

    laps = laps.dropna(subset=["Driver", "LapNumber", "LapTime"])
    written = 0
    file_handles = {}

    try:
        for lap in laps.to_dict(orient="records"):
            driver = lap.get("Driver")
            lap_number = _to_maybe_float(lap.get("LapNumber"))
            lap_time = _format_timedelta_hhmmssmmmm(lap.get("LapTime"))

            record = {
                "data_type": "lap",
                "Driver": driver,
                "LapNumber": lap_number,
                "LapTime": lap_time,
                "Sector1Time": _format_timedelta_hhmmssmmmm(lap.get("Sector1Time")),
                "Sector2Time": _format_timedelta_hhmmssmmmm(lap.get("Sector2Time")),
                "Sector3Time": _format_timedelta_hhmmssmmmm(lap.get("Sector3Time")),
                "Compound": lap.get("Compound"),
                "PitInTime_ms": _timedelta_to_ms(lap.get("PitInTime")),
                "PitOutTime_ms": _timedelta_to_ms(lap.get("PitOutTime")),
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "race_year": year,
                "race_round": round_num,
                "session": session_code,
                "record_id": _make_record_id(year, round_num, session_code, driver, lap_number, lap_time),
                "_bronze_written_at": datetime.now(timezone.utc).isoformat(),
                **_BACKFILL_LINEAGE,
            }

            driver_token = _safe_token(driver)
            file_name = f"laps.ndjson.driver-{driver_token}.ndjson"
            file_path = os.path.join(laps_dir, file_name)

            if file_path not in file_handles:
                file_handles[file_path] = open(file_path, "a", encoding="utf-8")
            file_handles[file_path].write(json.dumps(record) + "\n")
            written += 1
    finally:
        for fh in file_handles.values():
            fh.close()

    print(f"[backfill][laps] Wrote {written} records for {year} R{round_num} {session_code}")
    return written


def _write_bronze_results(session_obj, year, round_num, session_code, bronze_base):
    results_dir = os.path.join(bronze_base, "race_results", _session_key(year, round_num, session_code))
    os.makedirs(results_dir, exist_ok=True)

    results = session_obj.results
    if results is None or results.empty:
        print(f"[backfill][race_results] No results for {year} R{round_num} {session_code}")
        return 0

    written = 0
    file_path = os.path.join(results_dir, "results.ndjson")

    with open(file_path, "a", encoding="utf-8") as f:
        for row in results.itertuples(index=False):
            driver_id = getattr(row, "Abbreviation", None)
            if not driver_id:
                continue

            position = getattr(row, "Position", None)
            try:
                position = int(position) if position == position else None
            except (TypeError, ValueError):
                position = None

            grid_position = getattr(row, "GridPosition", None)
            try:
                grid_position = int(grid_position) if grid_position == grid_position else None
            except (TypeError, ValueError):
                grid_position = None

            points = getattr(row, "Points", None)
            try:
                points = float(points) if points == points else None
            except (TypeError, ValueError):
                points = None

            record = {
                "record_id": _make_record_id(year, round_num, session_code, driver_id),
                "race_year": year,
                "race_round": round_num,
                "session": session_code,
                "driver_id": str(driver_id).upper().strip(),
                "full_name": str(getattr(row, "FullName", "") or ""),
                "team": str(getattr(row, "TeamName", "") or ""),
                "nationality": str(getattr(row, "CountryCode", "") or ""),
                "position": position,
                "grid_position": grid_position,
                "points": points,
                "status": str(getattr(row, "Status", "") or ""),
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "_bronze_written_at": datetime.now(timezone.utc).isoformat(),
                **_BACKFILL_LINEAGE,
            }

            f.write(json.dumps(record) + "\n")
            written += 1

    print(f"[backfill][race_results] Wrote {written} records for {year} R{round_num} {session_code}")
    return written


def _write_bronze_weather(session_obj, year, round_num, session_code, bronze_base):
    weather_dir = os.path.join(bronze_base, "weather", _session_key(year, round_num, session_code))
    os.makedirs(weather_dir, exist_ok=True)

    weather = session_obj.weather_data
    if weather is None or weather.empty:
        print(f"[backfill][weather] No weather data for {year} R{round_num} {session_code}")
        return 0

    file_name = "weather.ndjson"
    file_path = os.path.join(weather_dir, file_name)
    written = 0

    with open(file_path, "a", encoding="utf-8") as f:
        for idx, row in enumerate(weather.itertuples(index=False)):
            rainfall = getattr(row, "Rainfall", None)
            try:
                is_raining = bool(rainfall) if rainfall == rainfall else None
            except (TypeError, ValueError):
                is_raining = None

            record = {
                "record_id": _make_record_id(year, round_num, session_code, idx),
                "race_year": year,
                "race_round": round_num,
                "session": session_code,
                "snapshot_index": idx,
                "time_offset_ms": _timedelta_to_ms(getattr(row, "Time", None)),
                "air_temp_c": _to_maybe_float(getattr(row, "AirTemp", None)),
                "track_temp_c": _to_maybe_float(getattr(row, "TrackTemp", None)),
                "humidity_pct": _to_maybe_float(getattr(row, "Humidity", None)),
                "pressure_mbar": _to_maybe_float(getattr(row, "Pressure", None)),
                "wind_speed_ms": _to_maybe_float(getattr(row, "WindSpeed", None)),
                "is_raining": is_raining,
                "event_ts": datetime.now(timezone.utc).isoformat(),
                "_bronze_written_at": datetime.now(timezone.utc).isoformat(),
                **_BACKFILL_LINEAGE,
            }
            f.write(json.dumps(record) + "\n")
            written += 1

    print(f"[backfill][weather] Wrote {written} records for {year} R{round_num} {session_code}")
    return written


# ── Main ──────────────────────────────────────────────────────────────────────

def run_backfill():
    completed = _load_manifest() if not BACKFILL_FORCE else set()
    sessions = _build_session_list()

    print(f"[backfill] {len(sessions)} sessions to consider | force={BACKFILL_FORCE} | data_types={BACKFILL_DATA_TYPES}")

    for year, round_num, session_code in sessions:
        key = (year, round_num, session_code)

        if key in completed:
            print(f"[backfill] Skipping {year} R{round_num} {session_code} (already in manifest)")
            continue

        print(f"\n[backfill] Processing {year} R{round_num} {session_code} ...")
        counts = {"laps_written": 0, "results_written": 0, "weather_written": 0}
        status = "ok"
        error_msg = None

        try:
            session_obj = fastf1.get_session(year, round_num, session_code)
            session_obj.load(laps=True, weather=True, messages=False, telemetry=False)

            if "laps" in BACKFILL_DATA_TYPES:
                counts["laps_written"] = _write_bronze_laps(session_obj, year, round_num, session_code, BRONZE_BASE_DIR)
            if "race_results" in BACKFILL_DATA_TYPES:
                counts["results_written"] = _write_bronze_results(session_obj, year, round_num, session_code, BRONZE_BASE_DIR)
            if "weather" in BACKFILL_DATA_TYPES:
                counts["weather_written"] = _write_bronze_weather(session_obj, year, round_num, session_code, BRONZE_BASE_DIR)

        except Exception as e:
            status = "error"
            error_msg = str(e)
            print(f"[backfill] ERROR for {year} R{round_num} {session_code}: {e}")

        _append_manifest({
            "race_year": year,
            "race_round": round_num,
            "session": session_code,
            "data_types": sorted(BACKFILL_DATA_TYPES),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "error": error_msg,
            **counts,
        })

        if BACKFILL_SLEEP_SECONDS > 0 and status == "ok":
            time.sleep(BACKFILL_SLEEP_SECONDS)

    print("\n[backfill] Done.")


if __name__ == "__main__":
    run_backfill()
