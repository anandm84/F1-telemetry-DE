
  
    
    

    create  table
      "warehouse"."main_silver"."stg_laps__dbt_tmp"
  
    as (
      with raw as (
    select * from 
    read_json(
        '../data/bronze/laps/*/laps.ndjson.driver-*.ndjson',
        format = 'newline_delimited',
        ignore_errors = true,
        maximum_object_size = 67108864
    )

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
        cast(
    case
        when regexp_matches(LapTime, '^\d{2}:\d{2}:\d{2}:\d{4}$') then
            (
                cast(regexp_extract(LapTime, '^(\d{2}):\d{2}:\d{2}:\d{4}$', 1) as double) * 3600000
                + cast(regexp_extract(LapTime, '^\d{2}:(\d{2}):\d{2}:\d{4}$', 1) as double) * 60000
                + cast(regexp_extract(LapTime, '^\d{2}:\d{2}:(\d{2}):\d{4}$', 1) as double) * 1000
                + cast(regexp_extract(LapTime, '^\d{2}:\d{2}:\d{2}:(\d{4})$', 1) as double) / 10.0
            )
        when regexp_matches(LapTime, '^(?:\d+\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$') then
            (
                coalesce(cast(nullif(regexp_extract(LapTime, '^(\d+)\s+days\s+', 1), '') as double), 0) * 86400000
                + cast(regexp_extract(LapTime, '(?:\d+\s+days\s+)?(\d{2}):', 1) as double) * 3600000
                + cast(regexp_extract(LapTime, '(?:\d+\s+days\s+)?\d{2}:(\d{2}):', 1) as double) * 60000
                + cast(regexp_extract(LapTime, '(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)', 1) as double) * 1000
            )
    end
 as bigint) as lap_time_ms,
        cast(
    case
        when regexp_matches(Sector1Time, '^\d{2}:\d{2}:\d{2}:\d{4}$') then
            (
                cast(regexp_extract(Sector1Time, '^(\d{2}):\d{2}:\d{2}:\d{4}$', 1) as double) * 3600000
                + cast(regexp_extract(Sector1Time, '^\d{2}:(\d{2}):\d{2}:\d{4}$', 1) as double) * 60000
                + cast(regexp_extract(Sector1Time, '^\d{2}:\d{2}:(\d{2}):\d{4}$', 1) as double) * 1000
                + cast(regexp_extract(Sector1Time, '^\d{2}:\d{2}:\d{2}:(\d{4})$', 1) as double) / 10.0
            )
        when regexp_matches(Sector1Time, '^(?:\d+\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$') then
            (
                coalesce(cast(nullif(regexp_extract(Sector1Time, '^(\d+)\s+days\s+', 1), '') as double), 0) * 86400000
                + cast(regexp_extract(Sector1Time, '(?:\d+\s+days\s+)?(\d{2}):', 1) as double) * 3600000
                + cast(regexp_extract(Sector1Time, '(?:\d+\s+days\s+)?\d{2}:(\d{2}):', 1) as double) * 60000
                + cast(regexp_extract(Sector1Time, '(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)', 1) as double) * 1000
            )
    end
 as bigint) as sector1_ms,
        cast(
    case
        when regexp_matches(Sector2Time, '^\d{2}:\d{2}:\d{2}:\d{4}$') then
            (
                cast(regexp_extract(Sector2Time, '^(\d{2}):\d{2}:\d{2}:\d{4}$', 1) as double) * 3600000
                + cast(regexp_extract(Sector2Time, '^\d{2}:(\d{2}):\d{2}:\d{4}$', 1) as double) * 60000
                + cast(regexp_extract(Sector2Time, '^\d{2}:\d{2}:(\d{2}):\d{4}$', 1) as double) * 1000
                + cast(regexp_extract(Sector2Time, '^\d{2}:\d{2}:\d{2}:(\d{4})$', 1) as double) / 10.0
            )
        when regexp_matches(Sector2Time, '^(?:\d+\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$') then
            (
                coalesce(cast(nullif(regexp_extract(Sector2Time, '^(\d+)\s+days\s+', 1), '') as double), 0) * 86400000
                + cast(regexp_extract(Sector2Time, '(?:\d+\s+days\s+)?(\d{2}):', 1) as double) * 3600000
                + cast(regexp_extract(Sector2Time, '(?:\d+\s+days\s+)?\d{2}:(\d{2}):', 1) as double) * 60000
                + cast(regexp_extract(Sector2Time, '(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)', 1) as double) * 1000
            )
    end
 as bigint) as sector2_ms,
        cast(
    case
        when regexp_matches(Sector3Time, '^\d{2}:\d{2}:\d{2}:\d{4}$') then
            (
                cast(regexp_extract(Sector3Time, '^(\d{2}):\d{2}:\d{2}:\d{4}$', 1) as double) * 3600000
                + cast(regexp_extract(Sector3Time, '^\d{2}:(\d{2}):\d{2}:\d{4}$', 1) as double) * 60000
                + cast(regexp_extract(Sector3Time, '^\d{2}:\d{2}:(\d{2}):\d{4}$', 1) as double) * 1000
                + cast(regexp_extract(Sector3Time, '^\d{2}:\d{2}:\d{2}:(\d{4})$', 1) as double) / 10.0
            )
        when regexp_matches(Sector3Time, '^(?:\d+\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$') then
            (
                coalesce(cast(nullif(regexp_extract(Sector3Time, '^(\d+)\s+days\s+', 1), '') as double), 0) * 86400000
                + cast(regexp_extract(Sector3Time, '(?:\d+\s+days\s+)?(\d{2}):', 1) as double) * 3600000
                + cast(regexp_extract(Sector3Time, '(?:\d+\s+days\s+)?\d{2}:(\d{2}):', 1) as double) * 60000
                + cast(regexp_extract(Sector3Time, '(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)', 1) as double) * 1000
            )
    end
 as bigint) as sector3_ms,
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
    );
  
  