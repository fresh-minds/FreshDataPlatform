{% macro hashed_key(columns) -%}
md5(
  concat_ws(
    '||',
    {% for c in columns -%}
      coalesce(cast({{ c }} as text), ''){% if not loop.last %}, {% endif %}
    {%- endfor %}
  )
)
{%- endmacro %}
