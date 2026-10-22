-- Singular test: Delivered orders must have an associated payment value greater than zero
-- Fails if any rows are returned

select 
    order_id,
    order_status,
    order_total_payment
from {{ ref('stg_orders_enriched') }}
where order_status = 'delivered'
  and (order_total_payment is null or order_total_payment <= 0)
