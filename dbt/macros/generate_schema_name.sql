-- generate_schema_name.sql
-- Override dbt's default schema naming behaviour.
--
-- By default dbt concatenates the profile target schema (here: "dbt") with
-- any custom +schema setting, producing "dbt_gold", "dbt_bronze", etc.
-- This macro disables that prefix so models land in clean schema names:
--   +schema: gold   → gold
--   +schema: silver → silver
--   +schema: bronze → bronze
--
-- Models without a custom schema still fall back to target.schema ("dbt"),
-- which is correct for dbt-internal tables and default model placement.
--
-- Snapshots use +target_schema (handled separately by dbt) and are unaffected.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
