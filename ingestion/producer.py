import hashlib
import json
import os
import time
from datetime import datetime, timezone

import fastf1
from kafka import KafkaProducer

fastf1.Cache.enable_cache("cache")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1_lap_times")
RACE_YEAR = int(os.getenv("RACE_YEAR", "2026"))
RACE_ROUND = int(os.getenv("RACE_ROUND", "1"))
RACE_SESSION = os.getenv("RACE_SESSION", "R")
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "1"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def _to_maybe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_timedelta_hhmmssmmmm(value):
    if value is None or not hasattr(value, "total_seconds"):
        return None

    total_seconds = float(value.total_seconds())
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


def _build_record(lap):
    driver = lap.get("Driver")
    lap_number = _to_maybe_float(lap.get("LapNumber"))
    lap_time = _format_timedelta_hhmmssmmmm(lap.get("LapTime"))
    event_ts = datetime.now(timezone.utc).isoformat()

    record_key = f"{RACE_YEAR}|{RACE_ROUND}|{RACE_SESSION}|{driver}|{lap_number}|{lap_time}"
    record_id = hashlib.sha1(record_key.encode("utf-8")).hexdigest()

    return {
        "Driver": driver,
        "LapNumber": lap_number,
        "LapTime": lap_time,
        "Sector1Time": _format_timedelta_hhmmssmmmm(lap.get("Sector1Time")),
        "Sector2Time": _format_timedelta_hhmmssmmmm(lap.get("Sector2Time")),
        "Sector3Time": _format_timedelta_hhmmssmmmm(lap.get("Sector3Time")),
        "Compound": lap.get("Compound"),
        "event_ts": event_ts,
        "race_year": RACE_YEAR,
        "race_round": RACE_ROUND,
        "session": RACE_SESSION,
        "record_id": record_id,
    }


def fetch_lap_data():
    session = fastf1.get_session(RACE_YEAR, RACE_ROUND, RACE_SESSION)
    session.load()

    laps = session.laps[
        [
            "Driver",
            "LapNumber",
            "LapTime",
            "Sector1Time",
            "Sector2Time",
            "Sector3Time",
            "Compound",
        ]
    ]

    laps = laps.dropna()
    # convert lap time to seconds
    laps["LapSeconds"] = laps["LapTime"].dt.total_seconds()

    # cumulative race time per driver
    laps["RaceTime"] = laps.groupby("Driver")["LapSeconds"].cumsum()

    # sort by race completion time
    laps = laps.sort_values("RaceTime")

    return laps.to_dict(orient="records")


def stream_data():
    prev_time = 0
    SPEED_FACTOR = 0.02  # Adjust this to speed up or slow down the replay
    laps = fetch_lap_data()

    for lap in laps:
        record = _build_record(lap)
        delay = lap["RaceTime"] - prev_time
        time.sleep(delay * SPEED_FACTOR)
        producer.send(KAFKA_TOPIC, record)
        print("Sent:", record["Driver"], record["LapNumber"], record["LapTime"])
        prev_time = lap["RaceTime"]
        
    producer.flush()


if __name__ == "__main__":
    stream_data()
