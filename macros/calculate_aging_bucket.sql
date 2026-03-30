{%- macro calculate_aging_bucket(claim_date_column) -%}
  case
    when datediff(day, {{ claim_date_column }}, current_date) <= 30 then '0-30 days'
    when datediff(day, {{ claim_date_column }}, current_date) <= 60 then '31-60 days'
    when datediff(day, {{ claim_date_column }}, current_date) <= 90 then '61-90 days'
    when datediff(day, {{ claim_date_column }}, current_date) <= 120 then '91-120 days'
    else '120+ days'
  end as aging_bucket
{%- endmacro -%}