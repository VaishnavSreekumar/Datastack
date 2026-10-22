"""Unit tests for Phase 1 RDD vs DataFrame analytics."""

import os
import pytest
from datastack.spark_lab.rdd_analytics import (
    AggregatedRevenueResult,
    DataFrameOrderAnalytics,
    FunctionalRDD,
    RDDOrderAnalytics,
    get_spark_session,
    run_comparison_experiment,
    verify_equivalence,
)


@pytest.fixture(scope="module")
def spark():
    """Shared SparkSession fixture for testing."""
    session = get_spark_session("Test_RDD_Analytics")
    yield session
    session.stop()


@pytest.fixture
def fixtures_dir():
    """Return absolute path to test fixtures."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures"))


def test_functional_rdd_primitives():
    """Test core RDD functional transformations and actions."""
    data = [1, 2, 3, 4, 5, 5]
    rdd = FunctionalRDD(data, partitions=2)

    # count & take
    assert rdd.count() == 6
    assert rdd.take(2) == [1, 2]

    # map
    doubled = rdd.map(lambda x: x * 2).collect()
    assert doubled == [2, 4, 6, 8, 10, 10]

    # filter
    evens = rdd.filter(lambda x: x % 2 == 0).collect()
    assert evens == [2, 4]

    # distinct
    unique = rdd.distinct().collect()
    assert sorted(unique) == [1, 2, 3, 4, 5]

    # flatMap
    words = FunctionalRDD(["order:1,item:A", "order:2,item:B"])
    tokens = words.flatMap(lambda s: s.split(",")).collect()
    assert tokens == ["order:1", "item:A", "order:2", "item:B"]

    # reduceByKey
    pairs = FunctionalRDD([("A", 10), ("B", 20), ("A", 30)])
    reduced = dict(pairs.reduceByKey(lambda x, y: x + y).collect())
    assert reduced == {"A": 40, "B": 20}


def test_unsafe_collect_guard():
    """Verify that collecting an RDD exceeding driver limit raises MemoryError."""
    large_rdd = FunctionalRDD(list(range(50)))
    with pytest.raises(MemoryError, match="Unsafe collect prevented"):
        large_rdd.collect(max_limit=10)


def test_dataframe_order_aggregation(spark, fixtures_dir):
    """Verify DataFrame analytics pipeline calculates valid metrics."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")

    df_engine = DataFrameOrderAnalytics(spark)
    results = df_engine.run_dataframe_aggregation(orders_path, items_path)

    assert len(results) >= 1
    delivered = next(r for r in results if r.group_key == "delivered")
    assert delivered.total_revenue == 380.0
    assert delivered.order_count == 5


def test_rdd_and_dataframe_equivalence(spark, fixtures_dir):
    """Verify that RDD and DataFrame pipelines yield identical mathematical results."""
    orders_path = os.path.join(fixtures_dir, "olist_orders_dataset.csv")
    items_path = os.path.join(fixtures_dir, "olist_order_items_dataset.csv")

    rdd_engine = RDDOrderAnalytics(spark)
    df_engine = DataFrameOrderAnalytics(spark)

    rdd_results = rdd_engine.run_rdd_aggregation(orders_path, items_path)
    df_results = df_engine.run_dataframe_aggregation(orders_path, items_path)

    assert verify_equivalence(rdd_results, df_results) is True
    assert len(rdd_results) == len(df_results)
    for r, d in zip(rdd_results, df_results):
        assert r.group_key == d.group_key
        assert abs(r.total_revenue - d.total_revenue) < 0.01
        assert r.order_count == d.order_count


def test_verify_equivalence_mismatch():
    """Verify equivalence detector catches value differences."""
    r1 = [AggregatedRevenueResult("delivered", 100.0, 5)]
    r2 = [AggregatedRevenueResult("delivered", 100.5, 5)]
    r3 = [AggregatedRevenueResult("shipped", 100.0, 5)]

    assert verify_equivalence(r1, r2) is False
    assert verify_equivalence(r1, r3) is False
