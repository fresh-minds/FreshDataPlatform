{% macro millis_to_timestamp(column_sql) -%}
    case
        when {{ column_sql }} is null then null
        when ({{ column_sql }})::text ~ '^-?[0-9]+$'
            then to_timestamp(({{ column_sql }})::bigint / 1000.0)
        else to_timestamp(
            (
                nullif(
                    regexp_replace(({{ column_sql }})::text, '[^0-9-]', '', 'g'),
                    ''
                )::bigint
            ) / 1000.0
        )
    end
{%- endmacro %}
