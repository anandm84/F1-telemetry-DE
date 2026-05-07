with raw as (
    select * from {{ bronze_read('laps', 'laps.ndjson.driver-*.ndjson') }}
),

typed as (
    select
        record_id,
        cast(race_year as integer) as race_year,
        cast(race_round as integer) as race_round,
        session,
        try_cast(event_ts as timestamp) as event_timestamp,
        try_cast(_bronze_written_at as timestamp) as bronze_written_at,
        upper(trim(Driver)) as driver_id,
        cast(LapNumber as integer) as lap_number,
        cast({{ duration_to_ms('LapTime') }} as bigint) as lap_time_ms,
        cast({{ duration_to_ms('Sector1Time') }} as bigint) as sector1_ms,
        cast({{ duration_to_ms('Sector2Time') }} as bigint) as sector2_ms,
        cast({{ duration_to_ms('Sector3Time') }} as bigint) as sector3_ms,
        upper(trim(Compound)) as tire_compound,
        cast(PitInTime_ms as bigint) as PitInTime_ms,
        cast(PitOutTime_ms as bigint) as PitOutTime_ms,
        _kafka_topic,
        cast(_kafka_partition as integer) as _kafka_partition,
        cast(_kafka_offset as bigint) as _kafka_offset,
        cast(_kafka_timestamp as bigint) as _kafka_timestamp
    from raw
    where record_id is not null
      and event_ts is not null
      and Driver is not null
      and trim(Driver) <> ''
      and LapNumber is not null
)

select
    record_id,
    race_year,
    race_round,
    session,
    event_timestamp,
    bronze_written_at,
    driver_id,
    driver_id as driver,
    lap_number,
    lap_time_ms,
    lap_time_ms as lap_time,
    sector1_ms,
    sector2_ms,
    sector3_ms,
    tire_compound,
    PitInTime_ms,
    PitOutTime_ms,
    _kafka_topic,
    _kafka_partition,
    _kafka_offset,
    _kafka_timestamp
from typed
where lap_time_ms is not null
qualify row_number() over (partition by record_id order by event_timestamp desc) = 1
