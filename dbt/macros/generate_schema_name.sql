{#- Use the model's +schema as-is (e.g. "bronze") instead of dbt's default
    "<target_schema>_<custom_schema>" prefixing, so table names stay stable
    across targets: iceberg.bronze.events. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is none -%}
    {{ target.schema }}
  {%- else -%}
    {{ custom_schema_name | trim }}
  {%- endif -%}
{%- endmacro %}
