"""Unit tests for schema contracts and input validation."""

import pytest
from pathlib import Path
from datastack.schemas import DATASET_SCHEMAS, REQUIRED_COLUMNS, get_spark_schema
from datastack.ingest.bronze import validate_source_files, IngestionError


def test_schema_definitions_exist():
    """Verify that all 9 Olist tables have registered schemas."""
    expected_tables = {
        "olist_orders_dataset",
        "olist_order_items_dataset",
        "olist_customers_dataset",
        "olist_products_dataset",
        "olist_sellers_dataset",
        "olist_order_payments_dataset",
        "olist_order_reviews_dataset",
        "olist_geolocation_dataset",
        "product_category_name_translation",
    }
    assert set(DATASET_SCHEMAS.keys()) == expected_tables
    assert set(REQUIRED_COLUMNS.keys()) == expected_tables


def test_required_columns_are_present_in_schemas():
    """Verify all required columns are defined in dataset schemas."""
    for table_name, req_cols in REQUIRED_COLUMNS.items():
        table_schema = DATASET_SCHEMAS[table_name]
        for col in req_cols:
            assert col in table_schema, f"Required column '{col}' missing from schema for {table_name}"


def test_missing_source_directory_raises_actionable_error(tmp_path: Path):
    """Verify that a non-existent raw directory raises an explicit IngestionError."""
    fake_path = tmp_path / "non_existent_raw_dir"
    with pytest.raises(IngestionError, match="Source raw data directory not found"):
        validate_source_files(fake_path)


def test_missing_files_raises_actionable_error(tmp_path: Path):
    """Verify that a directory missing required datasets fails immediately."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    # Create only one file
    (raw_dir / "olist_orders_dataset.csv").write_text("order_id\n123")
    
    with pytest.raises(IngestionError, match="Missing required source datasets"):
        validate_source_files(raw_dir)


def test_all_fixtures_present_and_valid():
    """Verify that tests/fixtures contains all 9 required datasets."""
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    found = validate_source_files(fixtures_dir)
    assert len(found) == 9
