"""Apache Airflow DAG for DataStack Olist Medallion Pipeline.

Orchestrates:
1. Bronze Ingestion (Raw CSVs -> Structured Parquet)
2. Silver Transformation (Enrichment, Join Resolution, Postgres Loading)
3. Gold Transformations (dbt staging, intermediate, and marts)
4. Data Quality Testing (dbt schema and singular tests)
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Determine project root dynamically
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Optional alerting configuration via environment variables (no hardcoded emails)
ENABLE_EMAIL = os.getenv("AIRFLOW_ENABLE_EMAIL_ALERTS", "False").lower() in ("true", "1")
ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL", "")
ALERT_EMAILS = [ALERT_EMAIL] if (ENABLE_EMAIL and ALERT_EMAIL) else []

default_args = {
    "owner": "datastack",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": ALERT_EMAILS,
    "email_on_failure": ENABLE_EMAIL,
    "email_on_retry": False,
}

with DAG(
    dag_id="datastack_olist_pipeline",
    default_args=default_args,
    description="End-to-end Medallion pipeline for Brazilian Olist E-commerce data",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datastack", "olist", "pyspark", "dbt", "ecommerce"],
) as dag:

    def execute_bronze_ingestion():
        """Executes the bronze layer ingestion."""
        # Dynamically append src to path
        src_path = str(REPO_ROOT / "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        from datastack.ingest.bronze import run_bronze_ingestion_duckdb
        row_counts = run_bronze_ingestion_duckdb()
        print(f"Bronze ingestion complete: {row_counts}")

    def execute_silver_transformation():
        """Executes the silver layer transformation and warehouse load."""
        src_path = str(REPO_ROOT / "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        from datastack.transform.silver import run_silver_pipeline
        loaded_rows = run_silver_pipeline()
        print(f"Silver pipeline complete. Loaded {loaded_rows} rows into warehouse.")

    task_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=execute_bronze_ingestion,
    )

    task_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=execute_silver_transformation,
    )

    dbt_dir = (REPO_ROOT / "olist_dbt").as_posix()

    task_dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"dbt run --project-dir {dbt_dir}",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test_quality",
        bash_command=f"dbt test --project-dir {dbt_dir}",
    )

    task_bronze >> task_silver >> task_dbt_run >> task_dbt_test
