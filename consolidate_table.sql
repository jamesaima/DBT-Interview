{% macro validate_tenant_tables(table_name) %}
{#
  Run before any consolidation model executes.
  Checks every tenant-prefixed table exists in the public schema
  (e.g. public.aim_claim_claim) and that all tenants have identical
  column sets. Fails fast with a clear message if not.
#}
{%- set tenants = var('tenants') -%}
{%- if execute -%}

  {%- set canonical_cols = [] -%}
  {%- set errors = [] -%}

  {%- for tenant in tenants -%}
    {%- set prefixed_table = tenant ~ '_' ~ table_name -%}

    {%- set check_table %}
      select count(1) as cnt
      from information_schema.tables
      where table_schema = 'public'
        and table_name   = '{{ prefixed_table }}'
        and table_type   = 'BASE TABLE'
    {%- endset -%}

    {%- set result = run_query(check_table) -%}
    {%- if result.rows[0][0] == 0 -%}
      {%- do errors.append("MISSING TABLE: public." ~ prefixed_table) -%}
    {%- else -%}

      {%- set col_query %}
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name   = '{{ prefixed_table }}'
        order by ordinal_position
      {%- endset -%}

      {%- set cols = run_query(col_query).columns[0].values() -%}

      {%- if canonical_cols | length == 0 -%}
        {%- for c in cols -%}{%- do canonical_cols.append(c) -%}{%- endfor -%}
      {%- else -%}
        {%- set col_set = cols | list -%}
        {%- for c in canonical_cols -%}
          {%- if c not in col_set -%}
            {%- do errors.append("SCHEMA MISMATCH: public." ~ prefixed_table ~ " missing column=" ~ c) -%}
          {%- endif -%}
        {%- endfor -%}
      {%- endif -%}

    {%- endif -%}
  {%- endfor -%}

  {%- if errors | length > 0 -%}
    {{ exceptions.raise_compiler_error(
        "Pre-flight validation failed for table '" ~ table_name ~ "':\n" ~ errors | join("\n")
    ) }}
  {%- endif -%}

{%- endif -%}
{% endmacro %}


{% macro consolidate_table(table_name) %}
{#
  Generates a UNION ALL across all tenant-prefixed tables in the public schema
  (e.g. public.aim_claim_claim, public.armo_claim_claim) then deduplicates.

  Normal tables (have modified_on):
    - Deduplicates by (id, client_name) keeping the row with the latest modified_on
    - Incremental mode: filters to rows modified since the last run per tenant

  Junction tables (no modified_on — listed in junction_tables var):
    - Left joins the parent table to borrow its modified_on for deduplication
    - Same incremental watermark logic using parent's modified_on
    - _parent_modified_on is kept as a column (serves as watermark on next sync)

  All output includes a client_name column populated from the tenant prefix.
  Uses QUALIFY on the outer query after UNION ALL — supported in Redshift Serverless.
  Uses explicit column list (derived from first tenant) to guard against schema drift.
#}

{%- set tenants = var('tenants') -%}
{%- set junction_tables = var('junction_tables') -%}
{%- set is_junction = table_name in junction_tables -%}

{# -- Run pre-flight validation (fails fast if any tenant table is missing/mismatched) -- #}
{{ validate_tenant_tables(table_name) }}

{# -- Derive explicit column list from first tenant's prefixed table at compile time -- #}
{%- if execute -%}
  {%- set first_tenant = tenants[0] -%}
  {%- set first_prefixed = first_tenant ~ '_' ~ table_name -%}
  {%- set col_query %}
    select column_name
    from information_schema.columns
    where table_schema = 'public'
      and table_name   = '{{ first_prefixed }}'
    order by ordinal_position
  {%- endset -%}
  {%- set columns = run_query(col_query).columns[0].values() -%}
{%- else -%}
  {%- set columns = [] -%}
{%- endif -%}

{%- set quoted_cols = [] -%}
{%- for c in columns -%}{%- do quoted_cols.append('"' ~ c ~ '"') -%}{%- endfor -%}
{%- set col_list = quoted_cols | join(', ') -%}

{%- if is_junction -%}
  {%- set jt_config = junction_tables[table_name] -%}
  {%- set parent_table = jt_config['parent_table'] -%}
  {%- set parent_fk = jt_config['parent_fk'] -%}

select * from (
  select *, row_number() over (
    partition by id, client_name
    order by _parent_modified_on desc nulls last
  ) as _rn
  from (
    {% for tenant in tenants %}
    {%- set prefixed_table = tenant ~ '_' ~ table_name -%}
    {%- set prefixed_parent = tenant ~ '_' ~ parent_table -%}
    select
      {{ col_list }},
      p.modified_on as _parent_modified_on,
      '{{ tenant }}' as client_name
    from public.{{ prefixed_table }} t
    left join public.{{ prefixed_parent }} p
      on t.{{ parent_fk }} = p.id
    {% if is_incremental() %}
    where p.modified_on > (
      select coalesce(max(_parent_modified_on), '1900-01-01'::timestamp)
      from {{ this }}
      where client_name = '{{ tenant }}'
    )
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
  )
)
where _rn = 1

{%- else -%}

select * from (
  select *, row_number() over (
    partition by id, client_name
    order by modified_on desc nulls last
  ) as _rn
  from (
    {% for tenant in tenants %}
    {%- set prefixed_table = tenant ~ '_' ~ table_name -%}
    select
      {{ col_list }},
      '{{ tenant }}' as client_name
    from public.{{ prefixed_table }}
    {% if is_incremental() %}
    where modified_on > (
      select coalesce(max(modified_on), '1900-01-01'::timestamp)
      from {{ this }}
      where client_name = '{{ tenant }}'
    )
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
  )
)
where _rn = 1

{%- endif -%}
{% endmacro %}


{% macro truncate_all_consolidation_tables() %}
{#
  Truncates all tables with the all_ prefix in the public schema.
  run_query executes immediately during run-operation (not deferred).
  Semicolons must not be included — Redshift rejects them inside run_query.
#}
{%- set find_tables %}
  select table_name
  from information_schema.tables
  where table_schema = 'public'
    and table_name like 'all!_%' escape '!'
    and table_type = 'BASE TABLE'
    order by table_name
{%- endset -%}

{%- if execute -%}
  {%- set results = run_query(find_tables) -%}
  {%- for row in results.rows -%}
    {%- set tbl = row[0] -%}
    {{ log("Truncating public." ~ tbl, info=True) }}
    {%- do run_query("truncate table public." ~ tbl) -%}
  {%- endfor -%}
{%- endif -%}
{% endmacro %}
