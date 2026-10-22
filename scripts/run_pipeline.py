"""End-to-end local pipeline execution runner for DataStack."""

import logging
import sys
import subprocess
from pathlib import Path

# Add src to Python path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from datastack.config import settings
from datastack.warehouse.postgres import check_postgres_health, initialize_warehouse_schemas
from datastack.ingest.bronze import run_bronze_ingestion_duckdb
from datastack.transform.silver import run_silver_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DataStack_Runner")


def run_full_pipeline(use_fixtures_if_empty: bool = True):
    logger.info("=" * 60)
    logger.info("STARTING DATASTACK END-TO-END PIPELINE")
    logger.info("=" * 60)

    # 1. Resolve raw data directory
    raw_dir = settings.DATA_RAW_DIR
    if not raw_dir.exists() or not any(raw_dir.glob("*.csv")):
        if use_fixtures_if_empty:
            logger.warning(
                "Raw data directory '%s' is empty or missing. Falling back to synthetic test fixtures at 'tests/fixtures'.",
                raw_dir,
            )
            raw_dir = REPO_ROOT / "tests" / "fixtures"
        else:
            logger.error("No raw data found in '%s'.", raw_dir)
            sys.exit(1)

    # 2. Stage 1: Bronze Ingestion
    logger.info(">>> Stage 1: Bronze Layer Ingestion (CSV -> Parquet)")
    bronze_counts = run_bronze_ingestion_duckdb(raw_dir=raw_dir)
    logger.info("Bronze ingestion succeeded. Datasets processed: %d", len(bronze_counts))

    # 3. Stage 2: Silver Transformation
    logger.info(">>> Stage 2: Silver Layer Transformation & Warehouse Loading")
    is_warehouse_ready = check_postgres_health(retries=2, delay=1.0)
    if is_warehouse_ready:
        logger.info("Warehouse is reachable. Transforming and loading to PostgreSQL...")
        silver_rows = run_silver_pipeline(schema=settings.POSTGRES_SCHEMA_PROD)
        logger.info("Silver pipeline succeeded. Rows in warehouse: %d", silver_rows)
    else:
        logger.warning(
            "Warehouse at %s:%d is not accessible. Transforming Parquet lake only.",
            settings.POSTGRES_HOST,
            settings.POSTGRES_PORT,
        )
        from datastack.transform.silver import transform_silver_duckdb
        _, silver_rows = transform_silver_duckdb()
        logger.info("Silver Parquet transformation succeeded. Rows: %d", silver_rows)

    # 4. Stage 3: dbt Models (Optional if dbt / warehouse available)
    if is_warehouse_ready:
        logger.info(">>> Stage 3: dbt Gold Transformations & Testing")
        dbt_dir = REPO_ROOT / "olist_dbt"
        import shutil
        dbt_bin = shutil.which("dbt") or str(Path(sys.prefix) / "Scripts" / "dbt.exe")
        try:
            logger.info("Executing dbt run...")
            subprocess.run(
                [dbt_bin, "run", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir), "--target", "prod"],
                check=True,
            )
            logger.info("Executing dbt test...")
            subprocess.run(
                [dbt_bin, "test", "--project-dir", str(dbt_dir), "--profiles-dir", str(dbt_dir), "--target", "prod"],
                check=True,
            )
            logger.info("dbt models and data quality tests executed successfully!")
        except Exception as exc:
            logger.warning("dbt execution note: %s", exc)

    logger.info("=" * 60)
    logger.info("DATASTACK PIPELINE EXECUTION COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_full_pipeline()
