-- Singular test: Delivery delays must fall within a reasonable logistical range
-- Negative values indicate early delivery (valid). Extreme anomalies outside [-30, 365] days fail.
-- Fails if any rows are returned

select 
    order_id,
    order_item_id,
    delivery_delay_days,
    delivered_at,
    estimated_delivery_at
from {{ ref('int_orders_enriched') }}
where order_status = 'delivered'
  and delivered_at is not null
  and estimated_delivery_at is not null
  and (delivery_delay_days < -30 or delivery_delay_days > 365)
