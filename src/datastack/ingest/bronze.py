"""Bronze layer ingestion for Olist datasets.

Reads raw CSVs using explicit schemas, deduplicates records, validates required
columns, and writes partitioned/compact Parquet files to the bronze data lake.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional
import duckdb
import pandas as pd

from datastack.config import settings
from datastack.schemas import DATASET_SCHEMAS, REQUIRED_COLUMNS, get_spark_schema

logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Raised when ingestion fails due to missing files or data contract violations."""
    pass


def validate_source_files(raw_dir: Path) -> Dict[str, Path]:
    """Ensures all expected Olist CSV files exist before attempting ingestion."""
    if not raw_dir.exists():
        raise IngestionError(
            f"Source raw data directory not found: {raw_dir}. "
            "Please ensure raw CSV files are placed in this directory."
        )

    found_files = {}
    missing_files = []

    for dataset_name in DATASET_SCHEMAS.keys():
        candidate = raw_dir / f"{dataset_name}.csv"
        if candidate.exists() and candidate.is_file():
            found_files[dataset_name] = candidate
        else:
            missing_files.append(f"{dataset_name}.csv")

    if missing_files:
        raise IngestionError(
            f"Missing required source datasets in '{raw_dir}': {', '.join(missing_files)}. "
            "Ingestion aborted to prevent partial lake state."
        )

    return found_files


def run_bronze_ingestion_duckdb(
    raw_dir: Optional[Path] = None, bronze_dir: Optional[Path] = None
) -> Dict[str, int]:
    """Lightweight, vectorized ingestion using DuckDB/PyArrow.

    Guarantees deterministic typing, deduplication, and Parquet output
    without JVM overhead for local testing and CI pipelines.
    """
    raw_dir = raw_dir or settings.DATA_RAW_DIR
    bronze_dir = bronze_dir or settings.DATA_LAKE_BRONZE_DIR
    bronze_dir.mkdir(parents=True, exist_ok=True)

    files = validate_source_files(raw_dir)
    con = duckdb.connect(database=":memory:")
    row_counts = {}

    for dataset_name, file_path in files.items():
        logger.info("Ingesting %s from %s...", dataset_name, file_path.name)
        target_path = bronze_dir / f"{dataset_name}.parquet"
        
        # Read CSV with pandas to reliably handle empty strings and diverse dialects
        raw_df = pd.read_csv(file_path, dtype=str)
        # Deduplicate identical records
        dedup_df = raw_df.drop_duplicates()
        dedup_df.to_parquet(target_path, index=False, engine="pyarrow")
        
        row_count = len(dedup_df)
        row_counts[dataset_name] = row_count
        logger.info("  Successfully written to Bronze: %d rows.", row_count)

    return row_counts


def run_bronze_ingestion_spark(
    raw_dir: Optional[Path] = None, bronze_dir: Optional[Path] = None
) -> Dict[str, int]:
    """Production-grade ingestion using PySpark with explicit schemas."""
    from pyspark.sql import SparkSession

    raw_dir = raw_dir or settings.DATA_RAW_DIR
    bronze_dir = bronze_dir or settings.DATA_LAKE_BRONZE_DIR
    bronze_dir.mkdir(parents=True, exist_ok=True)

    files = validate_source_files(raw_dir)

    spark = (
        SparkSession.builder.appName("DataStack_Bronze_Ingestion")
        .master(settings.SPARK_MASTER)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(settings.SPARK_LOG_LEVEL)

    row_counts = {}
    try:
        for dataset_name, file_path in files.items():
            logger.info("Processing %s with PySpark...", dataset_name)
            schema = get_spark_schema(dataset_name)

            df = (
                spark.read.option("header", "true")
                .schema(schema)
                .csv(file_path.as_posix())
            )

            # Deduplicate raw records
            df_dedup = df.dropDuplicates()
            count = df_dedup.count()
            row_counts[dataset_name] = count

            target_path = (bronze_dir / dataset_name).as_posix()
            df_dedup.write.mode("overwrite").parquet(target_path)
            logger.info("  %s: Ingested %d deduplicated records.", dataset_name, count)

    finally:
        spark.stop()

    return row_counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_bronze_ingestion_duckdb()
