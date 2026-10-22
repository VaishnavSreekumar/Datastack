-- Singular test: Every customer record in dim_customers must reflect non-negative revenue and at least one order
-- Fails if any rows are returned

select 
    customer_id,
    total_orders,
    total_revenue
from {{ ref('dim_customers') }}
where coalesce(total_revenue, 0) <= 0 
   or coalesce(total_orders, 0) < 1
