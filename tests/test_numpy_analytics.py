"""Unit tests for Phase 4 NumPy analytics and Pandas interoperability."""

import numpy as np
import pandas as pd
import pytest
from datastack.analytics.numpy_metrics import (
    NumericalSummary,
    calculate_vectorized_total_cost,
    compute_delivery_delay_metrics,
    compute_numerical_summary,
    filter_orders_by_revenue_threshold,
    inspect_array_metadata,
    series_to_clean_numpy,
)
from datastack.analytics.pandas_numpy_comparison import (
    benchmark_loop_vs_vectorized,
    calculate_total_cost_loop,
    calculate_total_cost_vectorized,
    demonstrate_null_handling_differences,
    demonstrate_pandas_to_numpy_interop,
)


def test_inspect_array_metadata():
    """Verify array metadata extraction."""
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    meta = inspect_array_metadata(arr)
    assert meta["shape"] == (3,)
    assert meta["ndim"] == 1
    assert meta["dtype"] == "float64"
    assert meta["total_nbytes"] == 24
    assert meta["is_c_contiguous"] is True


def test_series_to_clean_numpy():
    """Verify Series conversion with NaN counting and filling."""
    s = pd.Series([10.0, np.nan, 30.0])
    cleaned, meta = series_to_clean_numpy(s, fill_value=0.0)

    assert meta["null_count"] == 1
    assert cleaned.shape == (3,)
    assert cleaned[1] == 0.0
    assert not np.isnan(cleaned).any()


def test_calculate_vectorized_total_cost():
    """Verify vectorized price + freight addition."""
    prices = np.array([100.0, 200.0, np.nan])
    freights = np.array([15.0, 25.0, 10.0])

    totals = calculate_vectorized_total_cost(prices, freights)
    assert totals.shape == (3,)
    assert totals[0] == 115.0
    assert totals[1] == 225.0
    assert totals[2] == 10.0  # nan price filled with 0 + 10 freight


def test_calculate_vectorized_shape_mismatch():
    """Verify shape mismatch raises ValueError."""
    p = np.array([10.0, 20.0])
    f = np.array([5.0])
    with pytest.raises(ValueError, match="Shape mismatch"):
        calculate_vectorized_total_cost(p, f)


def test_compute_numerical_summary():
    """Verify summary metrics calculation."""
    arr = np.array([10.0, 20.0, 30.0, 40.0, 50.0, np.nan])
    summary = compute_numerical_summary(arr)

    assert isinstance(summary, NumericalSummary)
    assert summary.count == 6
    assert summary.valid_count == 5
    assert summary.null_count == 1
    assert summary.mean == 30.0
    assert summary.median == 30.0
    assert summary.min_val == 10.0
    assert summary.max_val == 50.0
    assert summary.p50 == 30.0


def test_compute_numerical_summary_empty_and_nan():
    """Verify error handling on empty or all-NaN arrays."""
    with pytest.raises(ValueError, match="empty array"):
        compute_numerical_summary(np.array([]))

    with pytest.raises(ValueError, match="exclusively NaNs"):
        compute_numerical_summary(np.array([np.nan, np.nan]))


def test_compute_delivery_delay_metrics():
    """Verify delivery delay days and late indicator calculation."""
    actual = pd.Series(["2023-01-10", "2023-01-20", None])
    estimated = pd.Series(["2023-01-05", "2023-01-25", "2023-01-15"])

    metrics = compute_delivery_delay_metrics(actual, estimated)
    assert metrics["valid_record_count"] == 2
    assert metrics["late_order_count"] == 1  # 5 days late
    assert metrics["severe_late_count"] == 0
    assert metrics["late_order_percentage"] == 50.0
    assert isinstance(metrics["delay_summary"], NumericalSummary)


def test_filter_orders_by_revenue_threshold():
    """Verify boolean masking on arrays."""
    orders = np.array(["ord_1", "ord_2", "ord_3"])
    costs = np.array([50.0, 150.0, 250.0])

    filtered_ids, filtered_costs = filter_orders_by_revenue_threshold(orders, costs, threshold=100.0)
    assert list(filtered_ids) == ["ord_2", "ord_3"]
    assert list(filtered_costs) == [150.0, 250.0]


def test_demonstrate_pandas_to_numpy_interop():
    """Verify Pandas to NumPy roundtrip conversion."""
    s = pd.Series([1.5, 2.5, 3.5], name="price")
    res = demonstrate_pandas_to_numpy_interop(s)
    assert res["roundtrip_equal"] is True
    assert res["numpy_shape"] == (3,)
    assert res["pandas_null_count"] == 0


def test_demonstrate_null_handling_differences():
    """Verify IEEE 754 NaN properties and Pandas nullable support."""
    res = demonstrate_null_handling_differences()
    assert res["numpy_nan_equals_self"] is False
    assert res["numpy_isnan_correct"] is True
    assert res["numpy_int_can_hold_nan"] is False
    assert res["pandas_nullable_supports_na"] is True


def test_benchmark_loop_vs_vectorized():
    """Verify loop and vectorized implementations produce identical results."""
    res = benchmark_loop_vs_vectorized(dataset_size=5_000, runs=3)
    assert res.results_match is True
    assert res.speedup_ratio > 1.0
    assert res.vectorized_avg_ms > 0.0
