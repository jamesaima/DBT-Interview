{{
  config(
    materialized = 'incremental' if var('sync_mode', false) else 'table',
    unique_key = ['id', 'client_name'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    dist_key = 'id',
    sort_keys = ['client_name', 'active', 'id']
  )
}}

{{ consolidate_table('claim') }}