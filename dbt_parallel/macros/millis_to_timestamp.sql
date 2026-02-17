{% macro millis_to_timestamp(column_sql) -%}
    to_timestamp(({{ column_sql }})::bigint / 1000.0)
{%- endmacro %}
