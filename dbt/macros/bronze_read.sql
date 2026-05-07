{#
  Read NDJSON files from a bronze sub-directory. Bronze is laid out as
  data/bronze/{laps,race_results,weather}/{year}_{round}_{session}/*.ndjson
  so the glob walks one level into the per-session sub-folders.

  NOTE: union_by_name was previously enabled but caused OOM on DuckDB 1.5.x
  when scanning 1000+ NDJSON files (each schema is loaded and merged).
  Bronze writers (producer.py, backfill.py, *_consumer.py) emit a stable
  schema per data type, so union_by_name is unnecessary in practice. If a
  future schema change introduces drift, re-enable union_by_name selectively.
#}
{% macro bronze_read(subdir, glob) %}
    read_json(
        '{{ env_var("F1_BRONZE_ROOT", "../data/bronze") }}/{{ subdir }}/*/{{ glob }}',
        format = 'newline_delimited',
        ignore_errors = true,
        maximum_object_size = 67108864
    )
{% endmacro %}
