
  
    
    

    create  table
      "warehouse"."main_silver"."stg_weather__dbt_tmp"
  
    as (
      with raw as (
    select * from 
    read_json(
        '../data/bronze/weather/*/weather.ndjson*',
        format = 'newline_delimited',
        ignore_errors = true,
        maximum_object_size = 67108864
    )

)

select
    record_id,
    cast(race_year as integer) as race_year,
    cast(race_round as integer) as race_round,
    session,
    try_cast(event_ts as timestamp) as event_timestamp,
    try_cast(_bronze_written_at as timestamp) as bronze_written_at,
    cast(snapshot_index as integer) as snapshot_index,
    cast(time_offset_ms as bigint) as time_offset_ms,
    cast(air_temp_c as double) as air_temp_c,
    cast(track_temp_c as double) as track_temp_c,
    cast(humidity_pct as double) as humidity_pct,
    cast(pressure_mbar as double) as pressure_mbar,
    cast(wind_speed_ms as double) as wind_speed_ms,
    cast(is_raining as boolean) as is_raining,
    _kafka_topic,
    cast(_kafka_partition as integer) as _kafka_partition,
    cast(_kafka_offset as bigint) as _kafka_offset,
    cast(_kafka_timestamp as bigint) as _kafka_timestamp
from raw
where record_id is not null
  and race_year is not null
  and race_round is not null
qualify row_number() over (partition by record_id order by event_ts desc) = 1
    );
  
  