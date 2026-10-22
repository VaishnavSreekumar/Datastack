"""Silver layer transformation for DataStack.

Resolves relationships across orders, items, customers, products, payments, sellers,
and translations while eliminating the upstream Cartesian revenue duplication bug.
"""

import logging
from pathlib import Path
from typing import Optional, Tuple
import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

from datastack.config import settings
from datastack.warehouse.postgres import create_silver_table, initialize_warehouse_schemas

logger = logging.getLogger(__name__)


def transform_silver_duckdb(
    bronze_dir: Optional[Path] = None, silver_dir: Optional[Path] = None
) -> Tuple[pd.DataFrame, int]:
    """Executes silver layer transformations using DuckDB.

    Fixes the Cartesian join product:
    Aggregates order_payments to order grain before joining with line items,
    guaranteeing that 1:M relationships never artificially inflate row counts
    or multiply item prices/freights.
    """
    bronze_dir = bronze_dir or settings.DATA_LAKE_BRONZE_DIR
    silver_dir = silver_dir or settings.DATA_LAKE_SILVER_DIR
    silver_dir.mkdir(parents=True, exist_ok=True)

    orders_parquet = (bronze_dir / "olist_orders_dataset.parquet").as_posix()
    items_parquet = (bronze_dir / "olist_order_items_dataset.parquet").as_posix()
    customers_parquet = (bronze_dir / "olist_customers_dataset.parquet").as_posix()
    products_parquet = (bronze_dir / "olist_products_dataset.parquet").as_posix()
    payments_parquet = (bronze_dir / "olist_order_payments_dataset.parquet").as_posix()
    sellers_parquet = (bronze_dir / "olist_sellers_dataset.parquet").as_posix()
    translations_parquet = (bronze_dir / "product_category_name_translation.parquet").as_posix()

    con = duckdb.connect(database=":memory:")

    # Transformation query with financial reconciliation
    query = f"""
    WITH payments_agg AS (
        SELECT 
            order_id,
            SUM(TRY_CAST(payment_value AS DOUBLE)) AS order_total_payment,
            MIN(payment_type) AS primary_payment_type
        FROM '{payments_parquet}'
        GROUP BY order_id
    ),
    orders_clean AS (
        SELECT
            order_id,
            customer_id,
            order_status,
            TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
            TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
            ROUND(
                date_diff('second', 
                    TRY_CAST(order_estimated_delivery_date AS TIMESTAMP),
                    TRY_CAST(order_delivered_customer_date AS TIMESTAMP)
                ) / 86400.0, 2
            ) AS delivery_delay_days,
            CASE 
                WHEN TRY_CAST(order_delivered_customer_date AS TIMESTAMP) > TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) THEN 1
                ELSE 0
            END AS is_late
        FROM '{orders_parquet}'
    )
    SELECT
        oi.order_id,
        CAST(oi.order_item_id AS INTEGER) AS order_item_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        o.delivery_delay_days,
        o.is_late,
        oi.product_id,
        COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category_name_english,
        oi.seller_id,
        s.seller_city,
        s.seller_state,
        TRY_CAST(oi.price AS DOUBLE) AS price,
        TRY_CAST(oi.freight_value AS DOUBLE) AS freight_value,
        ROUND(COALESCE(TRY_CAST(oi.price AS DOUBLE), 0.0) + COALESCE(TRY_CAST(oi.freight_value AS DOUBLE), 0.0), 2) AS total_item_value,
        p_agg.order_total_payment,
        p_agg.primary_payment_type,
        c.customer_city,
        c.customer_state
    FROM '{items_parquet}' oi
    INNER JOIN orders_clean o ON oi.order_id = o.order_id
    INNER JOIN '{customers_parquet}' c ON o.customer_id = c.customer_id
    LEFT JOIN '{products_parquet}' p ON oi.product_id = p.product_id
    LEFT JOIN '{translations_parquet}' t ON p.product_category_name = t.product_category_name
    LEFT JOIN '{sellers_parquet}' s ON oi.seller_id = s.seller_id
    LEFT JOIN payments_agg p_agg ON oi.order_id = p_agg.order_id
    """

    df = con.execute(query).df()
    con.close()

    # Save to Silver Parquet
    target_parquet = silver_dir / "orders_enriched.parquet"
    df.to_parquet(target_parquet, index=False, engine="pyarrow")
    logger.info("Silver transformation complete. Saved %d rows to %s", len(df), target_parquet)

    return df, len(df)


def load_silver_to_postgres(df: pd.DataFrame, schema: str = "public") -> int:
    """Idempotently loads the transformed silver DataFrame into PostgreSQL warehouse."""
    initialize_warehouse_schemas()
    create_silver_table(schema=schema)

    engine = create_engine(settings.postgres_connection_uri)
    target_table = "silver_orders_enriched"

    # Truncate and batch load for repeatable and clean execution
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{target_table};"))

    df.to_sql(
        name=target_table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )
    logger.info("Successfully loaded %d records into %s.%s", len(df), schema, target_table)
    return len(df)


def run_silver_pipeline(schema: str = "public") -> int:
    """Full silver pipeline execution."""
    df, row_count = transform_silver_duckdb()
    load_silver_to_postgres(df, schema=schema)
    return row_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_silver_pipeline()
