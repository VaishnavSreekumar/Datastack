"""Tests for join cardinality and financial integrity.

Crucially validates that multi-item (1:N) and multi-payment (1:M) orders
do NOT result in a Cartesian product inflating revenue or row counts.
"""

from pathlib import Path
import pytest
from datastack.ingest.bronze import run_bronze_ingestion_duckdb
from datastack.transform.silver import transform_silver_duckdb


@pytest.fixture(scope="module")
def populated_lake(tmp_path_factory):
    """Ingests fixtures into a temporary bronze lake and transforms to silver."""
    base_tmp = tmp_path_factory.mktemp("lake_test")
    raw_dir = Path(__file__).resolve().parent / "fixtures"
    bronze_dir = base_tmp / "bronze"
    silver_dir = base_tmp / "silver"

    run_bronze_ingestion_duckdb(raw_dir=raw_dir, bronze_dir=bronze_dir)
    df, row_count = transform_silver_duckdb(bronze_dir=bronze_dir, silver_dir=silver_dir)
    return df


def test_no_cartesian_join_multiplication(populated_lake):
    """order_001 has 2 items and 2 payments.

    In the upstream flawed pipeline, this generated 2x2 = 4 rows.
    In DataStack, it MUST generate exactly 2 rows (1 row per order item).
    """
    df = populated_lake
    order_1_rows = df[df["order_id"] == "order_001"]
    assert len(order_1_rows) == 2, f"Expected 2 rows for order_001, got {len(order_1_rows)} (Cartesian bug!)"


def test_revenue_not_duplicated_across_payments(populated_lake):
    """order_001 item prices are 100.0 and 50.0; freights are 10.0 and 5.0.

    Total merchandise value = 165.0.
    In the flawed upstream pipeline, payment duplication doubled this to 330.0.
    """
    df = populated_lake
    order_1_rows = df[df["order_id"] == "order_001"]
    
    total_price = order_1_rows["price"].sum()
    total_freight = order_1_rows["freight_value"].sum()
    total_item_val = order_1_rows["total_item_value"].sum()

    assert total_price == 150.0
    assert total_freight == 15.0
    assert total_item_val == 165.0

    # Order-level total payment should reflect 165.0 (65.0 voucher + 100.0 credit card)
    assert order_1_rows["order_total_payment"].iloc[0] == 165.0


def test_total_silver_row_count_matches_order_items(populated_lake):
    """The total number of silver rows must exactly equal the number of items for valid orders."""
    df = populated_lake
    # In our fixtures, order_001 has 2 items, order_002 has 1 item, order_003 has 1 item, order_004 has 1 item
    # Total items = 5
    assert len(df) == 5
