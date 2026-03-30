{{
  config(
    materialized='table',
    schema='gold',
    tags=['daily'],
    -- alias='claims_payment_summary'
  )
}}

select
  c.claim_id,
  c.patient_id,
  c.claim_amount,
  c.status,
  c.claim_date,
  coalesce(p.payment_amount, 0) as total_paid,
  c.claim_amount - coalesce(p.payment_amount, 0) as outstanding_balance,
  p.last_payment_date,
  {{ calculate_aging_bucket('c.claim_date') }}
from {{ ref('all_claim') }} c
left join {{ ref('all_payment') }} p
  on c.claim_id = p.claim_id
where c.deleted_on is null