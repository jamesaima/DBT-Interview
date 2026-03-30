{% macro build_consolidation() %}
{#
  Truncates all all_ prefixed consolidation tables in the public schema.
  Run this before a full dbt run to rebuild from scratch.

  Usage:
    dbt run-operation build_consolidation
    dbt run --select consolidation

  Or simply: make build-consolidation
#}
  {{ truncate_all_consolidation_tables() }}
{% endmacro %}