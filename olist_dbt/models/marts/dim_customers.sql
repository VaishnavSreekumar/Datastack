{{ config(materialized='table') }}

select 
    customer_id,
    customer_city,
    customer_state,
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_items_purchased,
    round(cast(sum(total_item_value) as numeric), 2) as total_revenue,
    min(purchased_at) as first_order_date,
    max(purchased_at) as last_order_date
from {{ ref('int_orders_enriched') }}
group by 1, 2, 3
