{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id'],
        incremental_strategy='delete+insert'
    )
}}

select 
    order_id,
    order_item_id,
    customer_id,
    order_status,
    purchased_at,
    delivered_at,
    estimated_delivery_at,
    delivery_delay_days,
    is_late,
    product_id,
    product_category_name_english,
    seller_id,
    seller_city,
    seller_state,
    price,
    freight_value,
    total_item_value,
    order_total_payment,
    primary_payment_type,
    customer_city,
    customer_state,
    cast(purchased_at as date) as order_date,
    {{ extract_year('purchased_at') }} as order_year,
    {{ extract_month('purchased_at') }} as order_month
from {{ ref('stg_orders_enriched') }}

{% if is_incremental() %}
    where purchased_at > (
        select coalesce(max(purchased_at), '1970-01-01'::timestamp) - interval '45 days' 
        from {{ this }}
    )
{% endif %}
