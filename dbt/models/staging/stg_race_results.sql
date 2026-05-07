with raw as (
    select * from {{ bronze_read('race_results', 'results.ndjson*') }}
)

select
    record_id,
    cast(race_year as integer) as race_year,
    cast(race_round as integer) as race_round,
    session,
    try_cast(event_ts as timestamp) as event_timestamp,
    try_cast(_bronze_written_at as timestamp) as bronze_written_at,
    upper(trim(driver_id)) as driver_id,
    full_name,
    team,
    nationality,
    cast(position as integer) as position,
    cast(grid_position as integer) as grid_position,
    cast(points as double) as points,
    status,
    _kafka_topic,
    cast(_kafka_partition as integer) as _kafka_partition,
    cast(_kafka_offset as bigint) as _kafka_offset,
    cast(_kafka_timestamp as bigint) as _kafka_timestamp
from raw
where record_id is not null
  and driver_id is not null
  and trim(driver_id) <> ''
  and race_year is not null
  and race_round is not null
qualify row_number() over (partition by record_id order by event_ts desc) = 1
