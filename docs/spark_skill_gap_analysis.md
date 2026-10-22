# DataStack Spark & Python Skill Gap Analysis

## Executive Summary
This gap analysis benchmarks the existing **DataStack** e-commerce pipeline against technical competencies required for senior enterprise data engineering roles (specifically targeting the Deloitte Spark/Python practice).

DataStack already possesses a production-grade data engineering foundation: typed medallion bronze/silver ingestion, dimensional staging and intermediate models in dbt/PostgreSQL, Airflow DAG orchestration, and idempotency regression tests. 

This analysis evaluates where existing coverage aligns with enterprise requirements and details the implementation plan to close all identified architectural, distributed computing, and numerical analytics gaps.

---

## 1. Skill Matrix & Gap Assessment

| Competency Area | Target Deloitte Requirement | Current Repository State | Identified Gap | Action Plan |
|---|---|---|---|---|
| **RDD Fundamentals** | Low-level distributed transformations (`map`, `filter`, `flatMap`, `reduceByKey`, `distinct`, `take`, `count`, bounded `collect`) | Pipeline uses high-level PySpark DataFrame API exclusively in `ingest/bronze.py` and `transform/silver.py`. | No explicit RDD manipulation or demonstration of low-level resilient distributed datasets. | Implement `src/datastack/spark_lab/rdd_analytics.py` with end-to-end RDD vs DataFrame comparison on order revenue aggregations. |
| **DataFrame Optimization** | Structured query optimization, Catalyst expressions, schema enforcement | Explicit StructType schemas in `schemas.py`; joins in `silver.py`. | Missing execution plan introspection (`explain()`), physical plan analysis, and query optimization metrics. | Implement `src/datastack/spark_lab/execution_analysis.py` capturing Parsed, Analyzed, Optimized, and Physical execution plans. |
| **Spark Architecture** | Driver vs Executor responsibilities, DAG decomposition, Jobs, Stages, Tasks, Shuffle boundaries | Code runs locally via `SparkSession.builder.master("local[*]")`. | Architectural lifecycle (narrow vs wide transformations, shuffle write/read) is implicit rather than empirically analyzed. | Build execution architecture experiments profiling DAG scheduling and shuffle boundaries. |
| **Partitioning & Shuffles** | Data distribution strategies (`repartition` vs `coalesce`), partition skew, shuffle costs | Default partitioning inherited from file reader and `spark.sql.shuffle.partitions`. | No empirical benchmarks comparing repartitioning overhead against coalescing or custom partitioning keys. | Implement `src/datastack/spark_lab/partition_benchmark.py` measuring runtime, plan changes, and shuffle exchange counts. |
| **NumPy Numerical Analytics** | Vectorized computation, ndarrays, dtypes, broadcasting, statistical aggregations (`mean`, `median`, `std`, percentiles, `where`) | Pipeline relies on SQL/PySpark aggregate functions for metrics. | Lack of dedicated numerical array processing module for bounded downstream analytics. | Implement `src/datastack/analytics/numpy_metrics.py` performing vectorized price, delivery delay, and freight calculations. |
| **Pandas & NumPy Interoperability** | Type conversions, missing value semantics (`NaN` vs `None` vs `pd.NA`), memory boundaries | Pandas used transiently during DB ingestion in `warehouse/postgres.py`. | Absence of formal type boundary comparisons and vectorized-vs-loop benchmarking. | Implement `src/datastack/analytics/pandas_numpy_comparison.py` with automated correctness tests and execution benchmarks. |

---

## 2. Environment & Runtime Context
- **Operating System:** Windows 11 (x86_64)
- **Python Runtime:** Python 3.12.15 (Conda environment: `datastack-env`)
- **PySpark Runtime:** Apache Spark 3.5.1 / Py4J 0.10.9.7
- **JVM Runtime:** Eclipse Temurin OpenJDK 11.0.28 (pre-configured to prevent Java 25 `Subject.getSubject` deprecation incompatibilities)
- **Hadoop Binaries:** Winutils & Hadoop 3.3.6 native libraries integrated for Windows file handling
- **Numerical Libraries:** NumPy 2.5.4, Pandas 3.0.6
- **Database / Warehouse:** Native PostgreSQL 17 + DuckDB 2.0.0 + dbt-core 1.11.2

### Platform Specific Consideration: Spark Workers on Windows
On Windows with Python 3.12+, upstream Apache Spark issue **SPARK-53759** produces a Python worker crash (`java.io.EOFException`) during socket finalization in `pyspark.worker`.
- **Supported Native Environments:** Python 3.11 (tested and 100% passing in `test-py311` conda environment) and Linux / WSL2 / Docker runtimes.
- **Audited Execution Policy:** The codebase strictly executes genuine PySpark RDD operations on `pyspark.rdd.RDD` without simulation fallbacks. If executed in an unsupported runtime (Windows Python 3.12), it explicitly reports the outcome as `UNVERIFIED (SPARK-53759)` and skips tests with actionable diagnostic messages rather than masking failures.
- **DataFrame Engine:** Spark DataFrame and Catalyst execution operates natively on the JVM and is fully functional across all Python and OS versions.

---

## 3. Implementation Roadmap
1. **Phase 1: RDD Fundamentals & Equivalence** (`src/datastack/spark_lab/rdd_analytics.py`)
2. **Phase 2: Spark Execution Architecture** (`src/datastack/spark_lab/execution_analysis.py`)
3. **Phase 3: Partitioning Strategies & Benchmarks** (`src/datastack/spark_lab/partition_benchmark.py`)
4. **Phase 4: NumPy Metrics & Pandas Interoperability** (`src/datastack/analytics/numpy_metrics.py`, `src/datastack/analytics/pandas_numpy_comparison.py`)
5. **Phase 5: Pipeline Integration & Regression Suite** (Pytest suite covering all modules)
6. **Phase 6: Technical Documentation & Interview Readiness** (`docs/spark_fundamentals.md`, `docs/numpy_fundamentals.md`, `docs/interview_walkthrough.md`)
7. **Phase 7: Final Validation & Remote Synchronization**
