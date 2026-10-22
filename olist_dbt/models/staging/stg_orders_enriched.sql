select 
    order_id,
    order_item_id,
    customer_id,
    order_status,
    order_purchase_timestamp as purchased_at,
    order_delivered_customer_date as delivered_at,
    order_estimated_delivery_date as estimated_delivery_at,
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
    customer_state
from {{ source('olist', 'silver_orders_enriched') }}
where order_status != 'canceled'
