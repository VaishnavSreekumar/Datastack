{{ config(materialized='table') }}

select 
    order_date,
    order_year,
    order_month,
    product_category_name_english,
    count(distinct order_id) as total_orders,
    count(order_item_id) as total_items_sold,
    round(cast(sum(total_item_value) as numeric), 2) as total_revenue,
    round(cast(sum(total_item_value) / nullif(count(distinct order_id), 0) as numeric), 2) as avg_order_value
from {{ ref('int_orders_enriched') }}
group by 1, 2, 3, 4
