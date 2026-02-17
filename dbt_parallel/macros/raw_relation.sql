{% macro raw_relation(domain, table_name) -%}
  {% set seed_name = domain ~ '__' ~ table_name %}
  {% if var('use_seed_data', false) %}
    {{ ref(seed_name) }}
  {% else %}
    {{ source(domain, table_name) }}
  {% endif %}
{%- endmacro %}
