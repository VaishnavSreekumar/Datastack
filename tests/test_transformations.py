"""Unit tests for silver transformation metrics and business logic."""

from pathlib import Path
import pytest
import pandas as pd
from datastack.ingest.bronze import run_bronze_ingestion_duckdb
from datastack.transform.silver import transform_silver_duckdb


@pytest.fixture(scope="module")
def silver_data(tmp_path_factory):
    base_tmp = tmp_path_factory.mktemp("metrics_test")
    raw_dir = Path(__file__).resolve().parent / "fixtures"
    bronze_dir = base_tmp / "bronze"
    silver_dir = base_tmp / "silver"

    run_bronze_ingestion_duckdb(raw_dir=raw_dir, bronze_dir=bronze_dir)
    df, _ = transform_silver_duckdb(bronze_dir=bronze_dir, silver_dir=silver_dir)
    return df


def test_early_delivery_delay_calculation(silver_data):
    """order_001 was estimated 2017-10-15 and delivered 2017-10-10 (5 days early).

    delivery_delay_days must be negative (~ -4.11 to -5.0 days) and is_late must be 0.
    """
    df = silver_data
    order_1 = df[df["order_id"] == "order_001"].iloc[0]
    assert order_1["delivery_delay_days"] < 0
    assert order_1["is_late"] == 0


def test_late_delivery_delay_calculation(silver_data):
    """order_002 was estimated 2017-11-01 and delivered 2017-11-05 (4 days late).

    delivery_delay_days must be positive (~ +4.58 days) and is_late must be 1.
    """
    df = silver_data
    order_2 = df[df["order_id"] == "order_002"].iloc[0]
    assert order_2["delivery_delay_days"] > 0
    assert order_2["is_late"] == 1


def test_in_flight_orders_have_null_delay(silver_data):
    """order_003 is shipped but not delivered yet.

    delivery_delay_days should be NaN/None and is_late should default to 0.
    """
    df = silver_data
    order_3 = df[df["order_id"] == "order_003"].iloc[0]
    assert pd.isna(order_3["delivery_delay_days"])
    assert order_3["is_late"] == 0


def test_category_translation_mapping(silver_data):
    """prod_001 category 'telefonia' must be translated to 'telephony'."""
    df = silver_data
    prod_1 = df[df["product_id"] == "prod_001"].iloc[0]
    assert prod_1["product_category_name_english"] == "telephony"
