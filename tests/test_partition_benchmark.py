"""Unit tests for Phase 3 Spark partitioning benchmark."""

import os
import pytest
from datastack.spark_lab.partition_benchmark import (
    PartitionBenchmark,
    PartitionBenchmarkMetric,
    get_spark_session,
    run_partition_experiment,
)


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture."""
    session = get_spark_session("Test_Partition_Benchmark", master="local[2]")
    yield session
    session.stop()


@pytest.fixture
def fixtures_dir():
    """Return absolute path to test fixtures."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))


@pytest.fixture
def lab(spark):
    """Instantiate PartitionBenchmark."""
    return PartitionBenchmark(spark)


def test_scaled_workload_generation(lab, fixtures_dir):
    """Verify workload generator scales rows correctly while preserving schema."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")

    scaled_df = lab.generate_scaled_workload(orders_path, items_path, scale_factor=10)
    assert scaled_df.count() == 70  # 7 joined items in fixture * 10 multiplier
    assert "price" in scaled_df.columns
    assert "order_status" in scaled_df.columns


def test_partition_strategies_correctness(lab, fixtures_dir):
    """Verify all 5 partition configurations return valid partition counts and mathematical results."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")

    results = lab.run_full_benchmark(orders_path, items_path, scale_factor=20)
    assert len(results) == 5

    strategy_names = [r.strategy_name for r in results]
    assert strategy_names == [
        "default",
        "repartition_4",
        "repartition_8",
        "coalesce_2",
        "partition_by_status",
    ]

    # Verify partition counts
    part_map = {r.strategy_name: r.partition_count for r in results}
    assert part_map["repartition_4"] == 4
    assert part_map["repartition_8"] == 8
    assert part_map["coalesce_2"] == 2
    assert part_map["partition_by_status"] == 4

    # Verify data correctness across all strategies
    baseline_rev = results[0].total_revenue_computed
    baseline_cnt = results[0].item_count_computed
    for r in results:
        assert abs(r.total_revenue_computed - baseline_rev) < 0.01
        assert r.item_count_computed == baseline_cnt
        assert r.avg_runtime_ms > 0.0


def test_run_partition_experiment():
    """Verify top-level partition experiment returns complete metrics."""
    metrics = run_partition_experiment()
    assert len(metrics) == 5
    for m in metrics:
        assert isinstance(m, PartitionBenchmarkMetric)
        assert m.input_row_count > 0
