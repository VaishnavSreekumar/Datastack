# DataStack Data Dictionary

## 1. Source Datasets (Olist Brazilian E-Commerce)

The pipeline integrates 9 relational datasets from Olist:

| Dataset | Granularity | Primary Key / Composite Key | Key Foreign Keys |
|---|---|---|---|
| `olist_orders_dataset.csv` | Order | `order_id` | `customer_id` |
| `olist_order_items_dataset.csv` | Order Item | `(order_id, order_item_id)` | `order_id`, `product_id`, `seller_id` |
| `olist_customers_dataset.csv` | Customer Identity | `customer_id` | `customer_unique_id` |
| `olist_products_dataset.csv` | Product Catalog | `product_id` | `product_category_name` |
| `olist_sellers_dataset.csv` | Merchant/Seller | `seller_id` | `seller_zip_code_prefix` |
| `olist_order_payments_dataset.csv` | Payment Installment | `(order_id, payment_sequential)` | `order_id` |
| `olist_order_reviews_dataset.csv` | Customer Review | `review_id` | `order_id` |
| `olist_geolocation_dataset.csv` | Zip Code Coordinates | N/A (Geographic Points) | `geolocation_zip_code_prefix` |
| `product_category_name_translation.csv`| Translation Lookup | `product_category_name` | N/A |

---

## 2. Table Column Schemas

### `olist_orders_dataset`
- `order_id` (string, UUID): Unique order identifier.
- `customer_id` (string): Reference to order customer token.
- `order_status` (string): Order state (`delivered`, `shipped`, `canceled`, `invoiced`, `processing`, `created`, `approved`, `unavailable`).
- `order_purchase_timestamp` (timestamp): Timestamp when order was placed.
- `order_approved_at` (timestamp): Timestamp payment was approved.
- `order_delivered_carrier_date` (timestamp): Hand-off to postal logistics carrier.
- `order_delivered_customer_date` (timestamp): Actual delivery timestamp to customer.
- `order_estimated_delivery_date` (timestamp): Delivery promise date communicated at purchase.

### `olist_order_items_dataset`
- `order_id` (string): Order identifier.
- `order_item_id` (integer): Sequential number of the item within the order (1, 2, 3...).
- `product_id` (string): Product identifier.
- `seller_id` (string): Seller identifier.
- `shipping_limit_date` (timestamp): Seller shipping deadline.
- `price` (decimal/double): Item sale price in BRL.
- `freight_value` (decimal/double): Shipping cost per item in BRL.

### `olist_order_payments_dataset`
- `order_id` (string): Order identifier.
- `payment_sequential` (integer): Sequence number of payment method.
- `payment_type` (string): `credit_card`, `boleto`, `voucher`, `debit_card`, `not_defined`.
- `payment_installments` (integer): Number of installments chosen.
- `payment_value` (decimal/double): Total amount paid per transaction slice.

### `olist_products_dataset`
- `product_id` (string): Product identifier.
- `product_category_name` (string): Portuguese category name.
- `product_name_lenght` (integer): Name character length.
- `product_description_lenght` (integer): Description length.
- `product_photos_qty` (integer): Number of published photos.
- `product_weight_g` (double): Weight in grams.
- `product_length_cm`, `product_height_cm`, `product_width_cm` (double): Physical dimensions.

### `olist_sellers_dataset`
- `seller_id` (string): Seller identifier.
- `seller_zip_code_prefix` (string): Zip prefix.
- `seller_city` (string): City name.
- `seller_state` (string): Two-letter state code.

### `olist_customers_dataset`
- `customer_id` (string): Order-specific customer identifier.
- `customer_unique_id` (string): Master identifier across multiple orders for the same person.
- `customer_zip_code_prefix` (string): Customer postal code prefix.
- `customer_city` (string): Customer city.
- `customer_state` (string): Customer state code.

---

## 3. Warehouse Target Models (Silver & Gold)

### Silver Table: `silver_orders_enriched`
Grain: One row per order item.
- `order_id`, `order_item_id` (Composite key)
- `customer_id`, `customer_city`, `customer_state`
- `order_status`
- `order_purchase_timestamp`, `order_delivered_customer_date`, `order_estimated_delivery_date`
- `delivery_delay_days` (Difference between delivered and estimated dates)
- `is_late` (1 if delivered after estimated date, else 0)
- `product_id`, `product_category_name_english`
- `seller_id`, `seller_city`, `seller_state`
- `price`, `freight_value`, `total_item_value`
- `payment_value` (Order-level total payment allocated or reference)

### Gold Mart: `agg_daily_revenue`
Grain: One row per day per product category.
- `order_date` (date)
- `order_year` (integer), `order_month` (integer)
- `product_category_name_english` (string)
- `total_orders` (integer): Distinct count of orders.
- `total_items_sold` (integer): Total units sold.
- `total_revenue` (double): Accurate gross merchandise value.
- `avg_order_value` (double): Average revenue per order.

### Gold Mart: `agg_seller_performance`
Grain: One row per seller.
- `seller_id` (string)
- `seller_state` (string)
- `total_orders` (integer): Distinct orders fulfilled.
- `total_revenue` (double): Total item revenue.
- `delayed_order_percentage` (double): Proportion of delivered orders exceeding estimated delivery date.
- `avg_delivery_delay` (double): Average delivery delay in days.

### Gold Mart: `dim_customers`
Grain: One row per customer.
- `customer_id` (string)
- `customer_city` (string), `customer_state` (string)
- `total_orders` (integer)
- `total_revenue` (double)
- `first_order_date` (timestamp)
- `last_order_date` (timestamp)
