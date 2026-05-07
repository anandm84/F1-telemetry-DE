{#
  Convert a duration string to milliseconds.
  Modern producer format: 'HH:MM:SS:MMMM' (4-digit fractional encodes 0.1ms).
  Legacy pandas Timedelta:  'D days HH:MM:SS.ssssss' or 'HH:MM:SS.ssssss'.
  Returns NULL when the input matches neither shape.
#}
{% macro duration_to_ms(col) %}
    case
        when regexp_matches({{ col }}, '^\d{2}:\d{2}:\d{2}:\d{4}$') then
            (
                cast(regexp_extract({{ col }}, '^(\d{2}):\d{2}:\d{2}:\d{4}$', 1) as double) * 3600000
                + cast(regexp_extract({{ col }}, '^\d{2}:(\d{2}):\d{2}:\d{4}$', 1) as double) * 60000
                + cast(regexp_extract({{ col }}, '^\d{2}:\d{2}:(\d{2}):\d{4}$', 1) as double) * 1000
                + cast(regexp_extract({{ col }}, '^\d{2}:\d{2}:\d{2}:(\d{4})$', 1) as double) / 10.0
            )
        when regexp_matches({{ col }}, '^(?:\d+\s+days\s+)?\d{2}:\d{2}:\d+(?:\.\d+)?$') then
            (
                coalesce(cast(nullif(regexp_extract({{ col }}, '^(\d+)\s+days\s+', 1), '') as double), 0) * 86400000
                + cast(regexp_extract({{ col }}, '(?:\d+\s+days\s+)?(\d{2}):', 1) as double) * 3600000
                + cast(regexp_extract({{ col }}, '(?:\d+\s+days\s+)?\d{2}:(\d{2}):', 1) as double) * 60000
                + cast(regexp_extract({{ col }}, '(?:\d+\s+days\s+)?\d{2}:\d{2}:(\d+(?:\.\d+)?)', 1) as double) * 1000
            )
    end
{% endmacro %}
