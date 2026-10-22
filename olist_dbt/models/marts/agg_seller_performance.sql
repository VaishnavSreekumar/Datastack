{{ config(materialized='table') }}

select 
    seller_id,
    seller_state,
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_items_sold,
    round(cast(sum(total_item_value) as numeric), 2) as total_revenue,
    round(cast(avg(delivery_delay_days) as numeric), 2) as avg_delivery_delay,
    round(cast(sum(is_late) * 100.0 / nullif(count(order_item_id), 0) as numeric), 2) as delayed_order_percentage
from {{ ref('int_orders_enriched') }}
group by 1, 2
