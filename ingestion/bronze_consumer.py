import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer


def _is_truthy(value):
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "f1_lap_times")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "f1-bronze-consumer-v1")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
BRONZE_DIR = os.getenv("BRONZE_DIR", "data/bronze")
BRONZE_FILE_PREFIX = os.getenv("BRONZE_FILE_PREFIX", "laps.ndjson")
BRONZE_BATCH_SIZE = int(os.getenv("BRONZE_BATCH_SIZE", "20"))
BRONZE_FLUSH_INTERVAL_SECONDS = float(os.getenv("BRONZE_FLUSH_INTERVAL_SECONDS", "3"))
BRONZE_MAX_IDLE_SECONDS = float(os.getenv("BRONZE_MAX_IDLE_SECONDS", "20"))
BRONZE_EXIT_ON_IDLE = _is_truthy(os.getenv("BRONZE_EXIT_ON_IDLE", "true"))
BRONZE_RUN_ID = os.getenv("BRONZE_RUN_ID", "").strip()
BRONZE_CLEAR_SESSION_ON_START = _is_truthy(os.getenv("BRONZE_CLEAR_SESSION_ON_START", "false"))
BRONZE_TARGET_SESSION = os.getenv("BRONZE_TARGET_SESSION", os.getenv("RACE_SESSION", "")).strip()
CONSUMER_POLL_TIMEOUT_MS = int(os.getenv("CONSUMER_POLL_TIMEOUT_MS", "1000"))
CONSUMER_MAX_POLL_RECORDS = int(os.getenv("CONSUMER_MAX_POLL_RECORDS", "500"))

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    enable_auto_commit=False,
)

os.makedirs(BRONZE_DIR, exist_ok=True)

buffers = defaultdict(list)
last_flush_time = time.monotonic()
last_message_time = time.monotonic()


def _safe_token(value, fallback):
    if value is None:
        return fallback
    token = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value).strip())
    return token or fallback


def _driver_session_file_path(session, driver):
    session_token = _safe_token(session, "UNKNOWN_SESSION")
    driver_token = _safe_token(driver, "UNKNOWN_DRIVER")
    run_suffix = f".run-{_safe_token(BRONZE_RUN_ID, 'default')}" if BRONZE_RUN_ID else ""

    file_name = (
        f"{BRONZE_FILE_PREFIX}."
        f"session-{session_token}."
        f"driver-{driver_token}"
        f"{run_suffix}.ndjson"
    )
    return os.path.join(BRONZE_DIR, file_name)


def _clear_existing_session_files():
    if not BRONZE_CLEAR_SESSION_ON_START:
        return

    if not BRONZE_TARGET_SESSION:
        print("BRONZE_CLEAR_SESSION_ON_START is true but BRONZE_TARGET_SESSION is empty; skipping cleanup.")
        return

    session_token = _safe_token(BRONZE_TARGET_SESSION, "UNKNOWN_SESSION")
    file_prefix = f"{BRONZE_FILE_PREFIX}.session-{session_token}.driver-"
    deleted_count = 0

    for name in os.listdir(BRONZE_DIR):
        if name.startswith(file_prefix) and name.endswith(".ndjson"):
            os.remove(os.path.join(BRONZE_DIR, name))
            deleted_count += 1

    print(f"Cleared {deleted_count} existing bronze files for session={session_token}.")


def _flush_buffers():
    global last_flush_time

    total_records = 0

    for (session, driver), records in list(buffers.items()):
        if not records:
            continue

        # One append-only bronze file per driver+session.
        driver_session_path = _driver_session_file_path(session, driver)

        with open(driver_session_path, "a", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

        print(f"Appended {len(records)} records to bronze: {driver_session_path}")
        total_records += len(records)
        buffers[(session, driver)].clear()

    if total_records > 0:
        consumer.commit()

    last_flush_time = time.monotonic()


_clear_existing_session_files()


try:
    while True:
        records_by_partition = consumer.poll(
            timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
            max_records=CONSUMER_MAX_POLL_RECORDS,
        )
        received_count = 0

        for _topic_partition, records in records_by_partition.items():
            for message in records:
                received_count += 1
                raw_event = message.value
                bronze_record = {
                    **raw_event,
                    "_kafka_topic": message.topic,
                    "_kafka_partition": message.partition,
                    "_kafka_offset": message.offset,
                    "_kafka_timestamp": message.timestamp,
                    "_bronze_written_at": datetime.now(timezone.utc).isoformat(),
                }

                session_key = bronze_record.get("session", "UNKNOWN_SESSION")
                driver_key = bronze_record.get("Driver", "UNKNOWN_DRIVER")
                buffers[(session_key, driver_key)].append(bronze_record)

        now = time.monotonic()
        if received_count > 0:
            last_message_time = now

        should_flush_size = any(len(records) >= BRONZE_BATCH_SIZE for records in buffers.values())
        should_flush_time = (now - last_flush_time) >= BRONZE_FLUSH_INTERVAL_SECONDS

        if should_flush_size or should_flush_time:
            _flush_buffers()

        idle_for_seconds = now - last_message_time
        if idle_for_seconds >= BRONZE_MAX_IDLE_SECONDS:
            _flush_buffers()
            if BRONZE_EXIT_ON_IDLE:
                print(
                    f"No new records for {BRONZE_MAX_IDLE_SECONDS}s. "
                    "Flushed buffers and exiting bronze consumer."
                )
                break
except KeyboardInterrupt:
    _flush_buffers()
