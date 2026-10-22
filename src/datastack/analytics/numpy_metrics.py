"""NumPy numerical analytics and vectorized operations for e-commerce metrics.

This module demonstrates low-level numerical computation using NumPy arrays on bounded
e-commerce datasets (price, freight value, delivery delay, payment value).

Core Concepts Demonstrated:
---------------------------
1. NumPy Arrays & Inspection:
   - Creating ndarrays, inspecting shape, ndim, dtype, itemsize, and nbytes.
   - Explicit handling of IEEE 754 floating-point NaNs and missing values.
   - Conversion of Pandas Series to contiguous, typed NumPy buffers.
2. Vectorization:
   - Element-wise arithmetic (total_cost = price + freight_value) executed in compiled C loops.
   - SIMD (Single Instruction, Multiple Data) execution without Python bytecode interpretation.
   - Elimination of slow scalar Python for-loops.
3. Numerical Operations:
   - Statistical aggregates: mean, median, standard deviation, percentiles (p25, p50, p75, p90, p99).
   - Extremes: minimum, maximum.
   - Conditional branch vectorization using np.where().
   - Boolean masking: Filtering sub-arrays based on boolean predicates.
4. Bounded Local Analytics Principle:
   - NumPy arrays reside entirely in driver process virtual memory (RAM).
   - Never collect an unbounded multi-terabyte Spark DataFrame to driver memory to use NumPy.
   - Use PySpark for distributed aggregation, then use NumPy/Pandas on aggregated summary results.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class NumericalSummary:
    """Statistical summary metrics calculated from a numerical NumPy array."""

    count: int
    valid_count: int
    null_count: int
    mean: float
    median: float
    std: float
    min_val: float
    max_val: float
    p25: float
    p50: float
    p75: float
    p90: float
    p99: float


def inspect_array_metadata(arr: np.ndarray) -> Dict[str, Any]:
    """Inspect memory, dimensionality, and type metadata of a NumPy array."""
    return {
        "shape": arr.shape,
        "ndim": arr.ndim,
        "dtype": str(arr.dtype),
        "itemsize_bytes": arr.itemsize,
        "total_nbytes": arr.nbytes,
        "is_c_contiguous": arr.flags["C_CONTIGUOUS"],
    }


def series_to_clean_numpy(
    series: pd.Series, fill_value: Optional[float] = None
) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Convert a Pandas Series into a clean 1D float64 NumPy array with metadata."""
    raw_arr = series.to_numpy(dtype=np.float64, copy=True)
    meta_before = inspect_array_metadata(raw_arr)

    null_mask = np.isnan(raw_arr)
    meta_before["null_count"] = int(np.sum(null_mask))

    if fill_value is not None and meta_before["null_count"] > 0:
        cleaned_arr = np.nan_to_num(raw_arr, nan=fill_value)
    else:
        cleaned_arr = raw_arr

    return cleaned_arr, meta_before


def calculate_vectorized_total_cost(
    prices: np.ndarray, freights: np.ndarray, fill_null_freight: float = 0.0
) -> np.ndarray:
    """Vectorized calculation of total item cost: total_cost = price + freight_value.

    Avoids Python scalar loops by leveraging NumPy SIMD vectorized addition.
    """
    if prices.shape != freights.shape:
        raise ValueError(
            f"Shape mismatch: prices shape {prices.shape} != freights shape {freights.shape}"
        )

    clean_prices = np.nan_to_num(prices, nan=0.0)
    clean_freights = np.nan_to_num(freights, nan=fill_null_freight)

    # Vectorized element-wise addition
    total_costs = clean_prices + clean_freights
    return total_costs


def compute_numerical_summary(arr: np.ndarray) -> NumericalSummary:
    """Compute comprehensive statistical metrics using vectorized NumPy routines."""
    total_count = arr.size
    if total_count == 0:
        raise ValueError("Cannot compute numerical summary on an empty array.")

    null_mask = np.isnan(arr)
    null_count = int(np.sum(null_mask))
    valid_count = total_count - null_count

    if valid_count == 0:
        raise ValueError("Cannot compute statistics on array containing exclusively NaNs.")

    valid_data = arr[~null_mask]

    return NumericalSummary(
        count=total_count,
        valid_count=valid_count,
        null_count=null_count,
        mean=round(float(np.mean(valid_data)), 4),
        median=round(float(np.median(valid_data)), 4),
        std=round(float(np.std(valid_data)), 4),
        min_val=round(float(np.min(valid_data)), 4),
        max_val=round(float(np.max(valid_data)), 4),
        p25=round(float(np.percentile(valid_data, 25)), 4),
        p50=round(float(np.percentile(valid_data, 50)), 4),
        p75=round(float(np.percentile(valid_data, 75)), 4),
        p90=round(float(np.percentile(valid_data, 90)), 4),
        p99=round(float(np.percentile(valid_data, 99)), 4),
    )


def compute_delivery_delay_metrics(
    actual_delivery_timestamps: pd.Series,
    estimated_delivery_timestamps: pd.Series,
) -> Dict[str, Any]:
    """Compute delivery delay days and late delivery indicators using NumPy vectorization.

    - Delay Days = (actual_date - estimated_date) in days.
    - Late Indicator = 1 if delay > 0 else 0 (via np.where).
    - Severe Late Indicator = delay > 7.0 days (via boolean masking).
    """
    # Convert timestamps to pandas datetime
    actual_dt = pd.to_datetime(actual_delivery_timestamps, errors="coerce")
    estimated_dt = pd.to_datetime(estimated_delivery_timestamps, errors="coerce")

    # Use pandas notna to properly flag NaT sentinel values
    valid_mask = actual_dt.notna().to_numpy() & estimated_dt.notna().to_numpy()

    if not np.any(valid_mask):
        return {
            "valid_record_count": 0,
            "late_order_count": 0,
            "late_order_percentage": 0.0,
            "delay_summary": None,
        }

    # Extract nanosecond deltas as float64 numpy arrays
    actual_ns = actual_dt.values.astype("float64")
    estimated_ns = estimated_dt.values.astype("float64")

    # One day in nanoseconds: 86,400 * 1,000,000,000
    ns_per_day = 86_400_000_000_000.0

    # Vectorized subtraction and unit conversion to days
    delays_days = (actual_ns[valid_mask] - estimated_ns[valid_mask]) / ns_per_day

    # Conditional operations using np.where()
    is_late_indicator = np.where(delays_days > 0.0, 1, 0)
    is_severe_late = np.where(delays_days > 7.0, 1, 0)

    late_count = int(np.sum(is_late_indicator))
    severe_late_count = int(np.sum(is_severe_late))
    total_valid = int(delays_days.size)

    delay_summary = compute_numerical_summary(delays_days)

    return {
        "valid_record_count": total_valid,
        "late_order_count": late_count,
        "late_order_percentage": round((late_count / total_valid) * 100.0, 2),
        "severe_late_count": severe_late_count,
        "delays_array": delays_days,
        "is_late_array": is_late_indicator,
        "delay_summary": delay_summary,
    }


def filter_orders_by_revenue_threshold(
    order_ids: np.ndarray, total_costs: np.ndarray, threshold: float
) -> Tuple[np.ndarray, np.ndarray]:
    """Filter orders using boolean masking: returns (high_value_order_ids, high_value_costs)."""
    if order_ids.shape != total_costs.shape:
        raise ValueError("order_ids and total_costs must have identical shapes.")

    mask = total_costs >= threshold
    return order_ids[mask], total_costs[mask]
