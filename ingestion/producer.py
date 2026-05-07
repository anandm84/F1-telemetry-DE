import hashlib
import json
import os
import time
from datetime import datetime, timezone

import fastf1
from kafka import KafkaProducer

fastf1.Cache.enable_cache("cache")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_LAPS = os.getenv("KAFKA_TOPIC", "f1_lap_times")
KAFKA_TOPIC_RACE_RESULTS = os.getenv("KAFKA_TOPIC_RACE_RESULTS", "f1_race_results")
KAFKA_TOPIC_WEATHER = os.getenv("KAFKA_TOPIC_WEATHER", "f1_weather")
RACE_YEAR = int(os.getenv("RACE_YEAR", "2024"))
RACE_ROUND = os.getenv("RACE_ROUND", "1")
RACE_SESSION = os.getenv("RACE_SESSION", "R")
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "0.02"))
DATA_TYPES = {t.strip() for t in os.getenv("DATA_TYPES", "laps,race_results,weather").split(",")}

ALL_SESSION_CODES = ["FP1", "FP2", "FP3", "SQ", "S", "Q", "R"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def _to_maybe_float(value):
    if value is None:
        return None
    try:
        f = float(value)
        return None if f != f else f  # NaN -> None
    except (TypeError, ValueError):
        return None


def _timedelta_to_ms(value):
    """Convert a pandas Timedelta to integer milliseconds, or None if null/missing."""
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


def _make_record_id(*parts):
    key = "|".join(str(p) for p in parts)
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _emit_laps(session, year, round_num, session_code):
    laps = session.laps[
        [
            "Driver",
            "LapNumber",
            "LapTime",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "Compound",
            "PitInTime",
            "PitOutTime",
        ]
    ].copy()

    laps = laps.dropna(subset=["Driver", "LapNumber", "LapTime"])
    if laps.empty:
        print(f"[laps] no rows for {year} R{round_num} {session_code}")
        return

    laps["LapSeconds"] = laps["LapTime"].dt.total_seconds()
    laps["RaceTime"] = laps.groupby("Driver")["LapSeconds"].cumsum()
    laps = laps.sort_values("RaceTime")

    prev_time = 0
    count = 0
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
        }

        if SPEED_FACTOR > 0:
            time.sleep(max(0.0, (lap["RaceTime"] - prev_time)) * SPEED_FACTOR)
        producer.send(KAFKA_TOPIC_LAPS, record)
        count += 1
        prev_time = lap["RaceTime"]

    producer.flush()
    print(f"[laps] {year} R{round_num} {session_code}: emitted {count} laps")


def _emit_race_results(session, year, round_num, session_code):
    results = session.results
    if results is None or results.empty:
        print(f"[race_results] no data for {year} R{round_num} {session_code}")
        return

    count = 0
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
        }

        producer.send(KAFKA_TOPIC_RACE_RESULTS, record)
        count += 1

    producer.flush()
    print(f"[race_results] {year} R{round_num} {session_code}: emitted {count} rows")


def _emit_weather(session, year, round_num, session_code):
    weather = session.weather_data
    if weather is None or weather.empty:
        print(f"[weather] no data for {year} R{round_num} {session_code}")
        return

    count = 0
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
        }
        producer.send(KAFKA_TOPIC_WEATHER, record)
        count += 1

    producer.flush()
    print(f"[weather] {year} R{round_num} {session_code}: emitted {count} snapshots")


def _process_session(year, round_num, session_code):
    print(f"\n=== {year} R{round_num} {session_code} ===")
    try:
        session = fastf1.get_session(year, round_num, session_code)
        session.load(laps=True, weather=True, messages=False, telemetry=False)
    except Exception as e:
        print(f"[skip] {year} R{round_num} {session_code}: {e}")
        return

    if "laps" in DATA_TYPES:
        try:
            _emit_laps(session, year, round_num, session_code)
        except Exception as e:
            print(f"[laps][error] {year} R{round_num} {session_code}: {e}")

    if "race_results" in DATA_TYPES:
        try:
            _emit_race_results(session, year, round_num, session_code)
        except Exception as e:
            print(f"[race_results][error] {year} R{round_num} {session_code}: {e}")

    if "weather" in DATA_TYPES:
        try:
            _emit_weather(session, year, round_num, session_code)
        except Exception as e:
            print(f"[weather][error] {year} R{round_num} {session_code}: {e}")


def _resolve_rounds(year):
    if RACE_ROUND.strip().lower() == "all":
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        return sorted(int(r) for r in schedule["RoundNumber"].dropna().astype(int).tolist())
    return [int(r.strip()) for r in RACE_ROUND.split(",") if r.strip()]


def _resolve_sessions():
    if RACE_SESSION.strip().lower() == "all":
        return list(ALL_SESSION_CODES)
    return [s.strip() for s in RACE_SESSION.split(",") if s.strip()]


def stream_data():
    rounds = _resolve_rounds(RACE_YEAR)
    sessions = _resolve_sessions()
    print(f"Producer plan: year={RACE_YEAR} rounds={rounds} sessions={sessions} data_types={sorted(DATA_TYPES)}")

    for round_num in rounds:
        for session_code in sessions:
            _process_session(RACE_YEAR, round_num, session_code)


if __name__ == "__main__":
    stream_data()
