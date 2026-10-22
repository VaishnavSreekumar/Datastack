"""Explicit schema definitions for Olist raw datasets.

Ensures deterministic typing, prevents silent inference errors, and eliminates
runtime schema drift.
"""

from typing import Dict, List

# Explicit column names and types for universal validation
DATASET_SCHEMAS: Dict[str, Dict[str, str]] = {
    "olist_orders_dataset": {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp",
    },
    "olist_order_items_dataset": {
        "order_id": "string",
        "order_item_id": "integer",
        "product_id": "string",
        "seller_id": "string",
        "shipping_limit_date": "timestamp",
        "price": "double",
        "freight_value": "double",
    },
    "olist_customers_dataset": {
        "customer_id": "string",
        "customer_unique_id": "string",
        "customer_zip_code_prefix": "string",
        "customer_city": "string",
        "customer_state": "string",
    },
    "olist_products_dataset": {
        "product_id": "string",
        "product_category_name": "string",
        "product_name_lenght": "double",
        "product_description_lenght": "double",
        "product_photos_qty": "double",
        "product_weight_g": "double",
        "product_length_cm": "double",
        "product_height_cm": "double",
        "product_width_cm": "double",
    },
    "olist_sellers_dataset": {
        "seller_id": "string",
        "seller_zip_code_prefix": "string",
        "seller_city": "string",
        "seller_state": "string",
    },
    "olist_order_payments_dataset": {
        "order_id": "string",
        "payment_sequential": "integer",
        "payment_type": "string",
        "payment_installments": "integer",
        "payment_value": "double",
    },
    "olist_order_reviews_dataset": {
        "review_id": "string",
        "order_id": "string",
        "review_score": "integer",
        "review_comment_title": "string",
        "review_comment_message": "string",
        "review_creation_date": "timestamp",
        "review_answer_timestamp": "timestamp",
    },
    "olist_geolocation_dataset": {
        "geolocation_zip_code_prefix": "string",
        "geolocation_lat": "double",
        "geolocation_lng": "double",
        "geolocation_city": "string",
        "geolocation_state": "string",
    },
    "product_category_name_translation": {
        "product_category_name": "string",
        "product_category_name_english": "string",
    },
}

REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "olist_orders_dataset": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "olist_order_items_dataset": ["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"],
    "olist_customers_dataset": ["customer_id", "customer_unique_id", "customer_city", "customer_state"],
    "olist_products_dataset": ["product_id"],
    "olist_sellers_dataset": ["seller_id"],
    "olist_order_payments_dataset": ["order_id", "payment_sequential", "payment_type", "payment_value"],
    "olist_order_reviews_dataset": ["review_id", "order_id", "review_score"],
    "olist_geolocation_dataset": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": ["product_category_name", "product_category_name_english"],
}


def get_spark_schema(dataset_name: str):
    """Dynamically return the PySpark StructType schema for a dataset."""
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        DoubleType,
        TimestampType,
    )

    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
    }

    schema_dict = DATASET_SCHEMAS.get(dataset_name)
    if not schema_dict:
        raise ValueError(f"Unknown dataset name for schema lookup: {dataset_name}")

    fields = []
    required_cols = REQUIRED_COLUMNS.get(dataset_name, [])
    for col_name, type_str in schema_dict.items():
        nullable = col_name not in required_cols
        spark_type = type_mapping.get(type_str, StringType())
        fields.append(StructField(col_name, spark_type, nullable=nullable))

    return StructType(fields)
