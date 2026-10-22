# Third-Party Notices & Attribution

## Upstream Project Attribution

This project, **DataStack — Olist Data Engineering Platform**, builds upon and significantly improves the architectural foundation of the open-source project:

- **Upstream Repository:** [https://github.com/abaqasif-aa/olist_data_pipeline_end](https://github.com/abaqasif-aa/olist_data_pipeline_end)
- **Original Author:** Abaq Asif
- **Upstream License:** MIT License (as specified in upstream README)

### Inherited Architectural Concepts
- Multi-layer medallion architecture (Bronze, Silver, Gold) targeting the Brazilian E-Commerce Public Dataset by Olist.
- PySpark ingestion pattern from CSV to Parquet.
- dbt transformation framework using staging, intermediate, and marts layers.
- Airflow DAG orchestration concept.

### Significant Modifications & Engineering Improvements in DataStack
1. **Robust Ingestion & Schema Enforcement:**
   - Replaced fragile schema inference (`inferSchema=True`) with explicit, strongly-typed schemas for all 9 Olist tables.
   - Added validation for missing files, corrupt records, and schema evolution.
2. **Elimination of Join Multiplication Bug:**
   - Fixed a critical data engineering flaw where one-to-many joins across `order_items` and `order_payments` produced a Cartesian product multiplying order revenues and order counts.
   - Decoupled payment aggregations and line-item aggregations to guarantee financial reconciliation accuracy.
3. **Environment Portability & Security:**
   - Removed all hardcoded absolute machine paths (e.g., `/mnt/d/Practise Projects/...` and `C:/hadoop`).
   - Removed hardcoded credentials and personal notification emails.
   - Introduced dynamic configuration via environment variables and `.env`.
   - Enabled native Windows, Docker Compose, and cross-platform support.
4. **dbt Modeling & Data Quality:**
   - Corrected incremental model unique key strategies and lookback window logic.
   - Fixed broken tests (such as valid negative delivery delay flags being misidentified as anomalies).
   - Added schema contract assertions and financial integrity singular tests.
5. **Testing, Benchmarking & Reliability:**
   - Built a comprehensive test suite with synthetic fixtures covering edge cases (zero spend, delayed delivery, multi-item orders).
   - Added reproducible performance benchmarks comparing PySpark with single-node baseline engines.
