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
        cast(PitInTime_ms as bigint) as pit_in_ms,
        cast(PitOutTime_ms as bigint) as pit_out_ms,
        upper(trim(Compound)) as tire_compound_before,
        _kafka_topic,
        cast(_kafka_partition as integer) as _kafka_partition,
        cast(_kafka_offset as bigint) as _kafka_offset,
        cast(_kafka_timestamp as bigint) as _kafka_timestamp
    from raw
    where PitInTime_ms is not null
      and record_id is not null
      and Driver is not null
      and trim(Driver) <> ''
      and LapNumber is not null
)

select *
from typed
qualify row_number() over (partition by record_id order by event_timestamp desc) = 1
