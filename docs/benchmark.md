# DataStack Performance Benchmarking

## 1. Benchmarking Objective

In modern data engineering, distributed engines (like Apache Spark) are often chosen by default, even when single-node vectorized analytical engines (such as DuckDB or Polars) can execute the exact same workload in a fraction of the time with significantly lower memory and compute overhead.

The goal of this benchmark is to:
1. Provide an honest, reproducible, and verifiable performance comparison between:
   - **Distributed Engine:** PySpark 3.5 (Local JVM runtime)
   - **Vectorized Engine:** DuckDB / Pandas
2. Verify output equivalence: Prove that both implementations execute identical joins, transformations, and aggregation logic, producing bitwise or numerically identical outputs.
3. Record realistic resource utilization without marketing bias or fabricated speedups.

---

## 2. Methodology & Rigor

- **Hardware & Software Baseline:**
  - OS: Windows 11 / WSL2
  - CPU & RAM: System specifications detected at runtime.
  - Python: 3.12.x
  - PySpark: 3.5.1
  - DuckDB: Latest stable
- **Workload:**
  - Ingestion: Reading CSV / Parquet source files.
  - Transformations: Joining orders, items, products, sellers, and payments; calculating timestamps, delay metrics, and customer aggregations.
- **Warm-Up & Repeated Runs:**
  - One warm-up run to eliminate cold cache/JIT variance.
  - Multiple timed runs, computing min, mean, and standard deviation.
  - Separate reporting for Spark initialization vs execution time.
- **Equivalence Verification:**
  - Automated hash/row-count check on resulting datasets to guarantee output parity.

---

## 3. Execution

```powershell
python scripts/run_benchmark.py
```
Detailed measurements and system profile are output directly to `benchmark_results.json` and formatted as a Markdown table.
