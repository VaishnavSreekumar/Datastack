"""Tests for pipeline idempotency and recovery across repeated executions."""

from pathlib import Path
from datastack.ingest.bronze import run_bronze_ingestion_duckdb
from datastack.transform.silver import transform_silver_duckdb


def test_bronze_ingestion_idempotent_runs(tmp_path: Path):
    """Running bronze ingestion multiple times must yield identical row counts."""
    raw_dir = Path(__file__).resolve().parent / "fixtures"
    bronze_dir = tmp_path / "bronze"

    run_1_counts = run_bronze_ingestion_duckdb(raw_dir=raw_dir, bronze_dir=bronze_dir)
    run_2_counts = run_bronze_ingestion_duckdb(raw_dir=raw_dir, bronze_dir=bronze_dir)

    assert run_1_counts == run_2_counts
    # Verify deduplication: olist_orders_dataset has 5 rows in fixture (1 duplicate), so dedup count is 4
    assert run_1_counts["olist_orders_dataset"] == 4


def test_silver_transformation_idempotency(tmp_path: Path):
    """Running silver transformation repeatedly must yield stable output without row inflation."""
    raw_dir = Path(__file__).resolve().parent / "fixtures"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    run_bronze_ingestion_duckdb(raw_dir=raw_dir, bronze_dir=bronze_dir)
    
    df1, count1 = transform_silver_duckdb(bronze_dir=bronze_dir, silver_dir=silver_dir)
    df2, count2 = transform_silver_duckdb(bronze_dir=bronze_dir, silver_dir=silver_dir)

    assert count1 == count2
    assert df1["total_item_value"].sum() == df2["total_item_value"].sum()
