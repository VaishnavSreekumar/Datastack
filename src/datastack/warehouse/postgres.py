"""PostgreSQL warehouse client and management utilities."""

import logging
import time
from typing import Optional
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text

from datastack.config import settings

logger = logging.getLogger(__name__)


def check_postgres_health(retries: int = 5, delay: float = 2.0) -> bool:
    """Verifies warehouse connectivity with retries."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                dbname=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                connect_timeout=3,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1;")
            cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info("PostgreSQL warehouse connection verified successfully.")
            return True
        except Exception as exc:
            logger.warning(
                "PostgreSQL health check attempt %d/%d failed: %s",
                attempt,
                retries,
                exc,
            )
            if attempt < retries:
                time.sleep(delay)
    return False


def initialize_warehouse_schemas():
    """Ensures dev and public schemas exist and have proper permissions."""
    engine = create_engine(settings.postgres_connection_uri)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.POSTGRES_SCHEMA_DEV};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {settings.POSTGRES_SCHEMA_PROD};"))
        logger.info(
            "Schemas '%s' and '%s' initialized.",
            settings.POSTGRES_SCHEMA_DEV,
            settings.POSTGRES_SCHEMA_PROD,
        )


def create_silver_table(schema: str = "public"):
    """Creates the silver_orders_enriched table with proper primary keys and indexes."""
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.silver_orders_enriched (
        order_id VARCHAR(50) NOT NULL,
        order_item_id INT NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        order_status VARCHAR(30) NOT NULL,
        order_purchase_timestamp TIMESTAMP NOT NULL,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP,
        delivery_delay_days DOUBLE PRECISION,
        is_late INT NOT NULL DEFAULT 0,
        product_id VARCHAR(50),
        product_category_name_english VARCHAR(100),
        seller_id VARCHAR(50),
        seller_city VARCHAR(100),
        seller_state VARCHAR(10),
        price DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        freight_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        total_item_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
        order_total_payment DOUBLE PRECISION,
        primary_payment_type VARCHAR(30),
        customer_city VARCHAR(100),
        customer_state VARCHAR(10),
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT pk_silver_orders_enriched PRIMARY KEY (order_id, order_item_id)
    );

    CREATE INDEX IF NOT EXISTS idx_silver_order_id ON {schema}.silver_orders_enriched(order_id);
    CREATE INDEX IF NOT EXISTS idx_silver_customer_id ON {schema}.silver_orders_enriched(customer_id);
    CREATE INDEX IF NOT EXISTS idx_silver_purchased_at ON {schema}.silver_orders_enriched(order_purchase_timestamp);
    CREATE INDEX IF NOT EXISTS idx_silver_seller_id ON {schema}.silver_orders_enriched(seller_id);
    """
    engine = create_engine(settings.postgres_connection_uri)
    with engine.begin() as conn:
        conn.execute(text(ddl))
        logger.info("Table '%s.silver_orders_enriched' and indexes verified.", schema)
