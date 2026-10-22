# NumPy Numerical Analytics & Vectorization Fundamentals

A comprehensive technical reference for low-level numerical computing, array mechanics, vectorization, and memory boundaries in enterprise data engineering.

---

## 1. NumPy Arrays and Data Types (`dtypes`)

### Core Architecture of an `ndarray`
A NumPy `ndarray` is a contiguous block of homogeneous memory managed by a metadata header:
- **`data` Pointer:** Points to the first byte in contiguous memory (C-contiguous or Fortran-contiguous).
- **`shape`:** A tuple describing the dimensions of the array (e.g. `(10000, 4)`).
- **`dtype`:** The uniform descriptor indicating how bytes are interpreted (e.g., `float64`, `int32`, `bool_`).
- **`strides`:** A tuple of byte offsets required to step to the next element in each dimension.

### Why Data Types Matter in Data Pipelines
- **Homogeneous Memory:** Unlike Python lists (which store pointers to individual PyObject wrappers on the heap), NumPy arrays store raw binary data contiguously in memory.
- **Memory Footprint:** A Python list of 1,000,000 floats consumes ~8 MB for the pointers plus ~28 MB for float objects (~36 MB total). A NumPy `float64` array of 1,000,000 floats consumes exactly 8 MB.
- **Cache Locality:** Contiguous layout allows the CPU prefetcher to load sequential values into high-speed L1/L2 caches, eliminating memory latency.

---

## 2. Vectorization: Beyond Python Bytecode

### How Vectorization Operates
In a standard Python `for`-loop:
1. The Python interpreter evaluates bytecode instructions for each iteration.
2. Dynamic type-checking occurs on every element.
3. PyObject reference counts are incremented and decremented.

In **NumPy Vectorization**:
- Iteration is pushed down into pre-compiled C/Fortran/Cython loops.
- Employs **SIMD (Single Instruction, Multiple Data)** hardware instructions (AVX-512, AVX2, SSE).
- A single CPU instruction processes 4 to 8 floating-point numbers simultaneously in hardware registers.

```python
# Python loop: ~64.0 ms for 50,000 elements
total = [prices[i] + freights[i] for i in range(len(prices))]

# Vectorized: ~0.44 ms (140x+ speedup)
total = prices + freights
```

---

## 3. Broadcasting Rules

Broadcasting allows arithmetic operations between arrays of different shapes without copying data.

### The Broadcasting Rule:
Starting from trailing (rightmost) dimensions, two dimensions are compatible if:
1. They are **equal**, or
2. One of them is **1**.

```python
# Example: Adding a 1D tax rate array to a 2D pricing matrix
prices = np.zeros((100, 4))   # Shape: (100, 4)
rates  = np.array([0.05, 0.08, 0.10, 0.12]) # Shape: (4,) -> broadcast to (1, 4) -> (100, 4)

adjusted_prices = prices * (1.0 + rates) # Valid broadcasting
```

---

## 4. Boolean Masking and Conditional Operations

### Boolean Masking
Filtering sub-arrays using boolean indexing expressions:
```python
# Create a boolean predicate array
mask = total_costs > 200.0   # Shape (N,), dtype bool

# Filter values using the mask
high_value_orders = order_ids[mask]
```

### Conditional Branching with `np.where()`
Instead of writing `if/else` statements inside Python loops, `np.where(condition, x, y)` vectorizes conditional evaluation across entire arrays:
```python
# Vectorized delivery delay flagging
is_late = np.where(delivery_delay_days > 0.0, 1, 0)
```

---

## 5. Statistical Aggregations & Missing Value Handling

### Aggregation Functions
- Central tendency: `np.mean()`, `np.median()`.
- Dispersion: `np.std()`, `np.var()`.
- Percentiles: `np.percentile(arr, [25, 50, 75, 90, 99])`.
- Extremes: `np.min()`, `np.max()`.

### IEEE 754 Float NaN Mechanics
In standard IEEE 754 floating-point specification:
- `np.nan == np.nan` evaluates to **`False`**.
- To identify nulls, always use `np.isnan(arr)`.
- Use `np.nanmean()`, `np.nanstd()`, or explicit masking to ignore NaNs during aggregation:
```python
valid_mask = ~np.isnan(arr)
clean_mean = np.mean(arr[valid_mask])
```

---

## 6. Pandas and NumPy Interoperability

| Aspect | Pandas | NumPy |
|---|---|---|
| **Primary Data Structures** | `DataFrame`, `Series`. | `ndarray`. |
| **Indexing** | Labeled index (strings, datetimes, hierarchical). | 0-indexed integer coordinates. |
| **Missing Values** | `pd.NA`, `np.nan`, `None`. | `np.nan` (floats only; integer arrays cannot natively store `np.nan`). |
| **Data Types** | Extended arrow/nullable dtypes (`Int64`, `boolean`, `category`). | Native machine types (`float64`, `int64`, `int32`). |
| **Conversion** | `series.to_numpy()` or `series.values`. | `pd.Series(arr)`. |

### Zero-Copy Views vs Memory Copies
- Calling `series.to_numpy()` on a homogeneous numeric column can return a zero-copy **view** sharing the underlying memory buffer (`arr.base is not None`).
- If dtype casting or null conversion is required, NumPy allocates a new contiguous memory buffer (`copy=True`).

---

## 7. Technology Comparison: NumPy vs Pandas vs PySpark

| Metric | NumPy | Pandas | Apache Spark (PySpark) |
|---|---|---|---|
| **Execution Environment** | Single machine, in-memory (RAM). | Single machine, in-memory (RAM). | Distributed cluster or local JVM multi-threading. |
| **Dataset Scale** | Bounded (Megabytes to low Gigabytes). | Bounded (Megabytes to ~10 Gigabytes). | Terabytes to Petabytes. |
| **Hardware Utilization** | Multi-threaded BLAS/LAPACK, CPU SIMD. | Single-core by default (some vectorized C ops). | Multi-core, multi-node horizontal scaling. |
| **Data Model** | N-dimensional homogeneous matrices. | 2D labeled tabular data. | Distributed partitioned datasets with Catalyst optimization. |
| **Latency** | Sub-millisecond (Microseconds). | Milliseconds. | Seconds to Minutes (due to job/stage scheduling). |

---

## 8. Memory Boundaries & Anti-Patterns

### Critical Anti-Pattern: Unbounded Driver Collection
> [!CAUTION]
> **Never collect an entire multi-terabyte Spark dataset into the driver JVM merely to perform numerical analytics in NumPy or Pandas.**

```python
# ANTI-PATTERN: Crashes driver with OutOfMemoryError on big data!
large_df = spark.read.parquet("s3://huge-bucket/events/")
huge_numpy_array = large_df.toPandas().to_numpy() # OOM CRASH

# ENTERPRISE PATTERN: Distributed reduction in Spark, bounded metrics in NumPy
# 1. Perform high-volume distributed group-by & aggregation inside Spark cluster
summary_df = large_df.groupBy("category").agg(
    F.sum("price").alias("rev"),
    F.count("order_id").alias("cnt")
)
# 2. Collect only bounded aggregate summary rows (< 10,000 rows) to driver
bounded_sample = summary_df.collect()
# 3. Apply NumPy / SciPy for high-precision local matrix modeling
local_revs = np.array([r["rev"] for r in bounded_sample])
stats = np.percentile(local_revs, [25, 50, 75, 99])
```
