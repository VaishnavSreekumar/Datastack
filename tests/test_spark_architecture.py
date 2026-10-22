"""Unit tests for Phase 2 Spark execution architecture and query plan analysis."""

import os
import pytest
from datastack.spark_lab.execution_analysis import (
    CapturedPlans,
    SparkExecutionLab,
    get_spark_session,
    run_architecture_experiments,
)


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture."""
    session = get_spark_session("Test_Spark_Architecture")
    yield session
    session.stop()


@pytest.fixture
def fixtures_dir():
    """Return absolute path to test fixtures."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))


@pytest.fixture
def lab(spark):
    """Instantiate SparkExecutionLab."""
    return SparkExecutionLab(spark)


def test_lazy_evaluation(lab, fixtures_dir):
    """Verify transformations do not trigger job execution until an action is called."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")
    orders_df, _ = lab.load_datasets(orders_path, items_path)

    transformed_df, was_lazy, row_count = lab.demonstrate_lazy_evaluation(orders_df)

    assert was_lazy is True
    assert row_count == 3  # delivered orders in fixture


def test_narrow_pipeline_no_shuffle(lab, fixtures_dir):
    """Verify that narrow transformations (select, filter, withColumn) have no shuffle exchange."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")
    orders_df, _ = lab.load_datasets(orders_path, items_path)

    _, plans = lab.demonstrate_narrow_pipeline(orders_df)

    assert isinstance(plans, CapturedPlans)
    assert plans.has_shuffle_exchange is False
    assert len(plans.parsed_plan) > 0
    assert len(plans.analyzed_plan) > 0
    assert len(plans.optimized_plan) > 0
    assert len(plans.physical_plan) > 0


def test_wide_pipeline_shuffle_exchange(lab, fixtures_dir):
    """Verify that wide transformations (join, groupBy) introduce shuffle Exchange operators."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")
    orders_df, items_df = lab.load_datasets(orders_path, items_path)

    _, plans = lab.demonstrate_wide_pipeline(orders_df, items_df)

    assert isinstance(plans, CapturedPlans)
    assert plans.has_shuffle_exchange is True
    assert "Exchange" in plans.physical_plan
    assert "HashAggregate" in plans.physical_plan


def test_repartition_vs_coalesce(lab, fixtures_dir):
    """Verify that repartition triggers shuffle exchange whereas coalesce reduces partitions."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")
    orders_df, _ = lab.load_datasets(orders_path, items_path)

    comparison = lab.demonstrate_repartition_and_coalesce(orders_df)

    orig_count, _, _ = comparison["original"]
    rep_count, rep_has_exchange, rep_plan = comparison["repartition"]
    coa_count, _, coa_plan = comparison["coalesce"]

    assert orig_count == 1
    assert rep_count == 4
    assert rep_has_exchange is True
    assert "Exchange" in rep_plan

    assert coa_count == 2
    assert "Coalesce" in coa_plan


def test_run_architecture_experiments():
    """Verify end-to-end architecture experiment function returns complete metrics."""
    results = run_architecture_experiments()
    assert results["lazy_evaluation_verified"] is True
    assert results["narrow_has_shuffle"] is False
    assert results["wide_has_shuffle"] is True
    assert "partition_comparison" in results
