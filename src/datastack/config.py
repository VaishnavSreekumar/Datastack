"""Configuration and environment management for DataStack."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Automatically locate and load .env from repository root if present
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(REPO_ROOT / ".env")


class Settings:
    """Application and pipeline settings."""

    # Base Paths
    REPO_ROOT: Path = REPO_ROOT
    DATA_RAW_DIR: Path = Path(os.getenv("DATA_RAW_DIR", str(REPO_ROOT / "raw_data")))
    DATA_LAKE_BRONZE_DIR: Path = Path(
        os.getenv("DATA_LAKE_BRONZE_DIR", str(REPO_ROOT / "data_lake" / "bronze"))
    )
    DATA_LAKE_SILVER_DIR: Path = Path(
        os.getenv("DATA_LAKE_SILVER_DIR", str(REPO_ROOT / "data_lake" / "silver"))
    )

    # PostgreSQL Warehouse Configuration
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "olist_dw")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "olist")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "olist_secure_dev_password")
    POSTGRES_SCHEMA_DEV: str = os.getenv("POSTGRES_SCHEMA_DEV", "dev")
    POSTGRES_SCHEMA_PROD: str = os.getenv("POSTGRES_SCHEMA_PROD", "public")

    # Spark & Java Settings
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_LOG_LEVEL: str = os.getenv("SPARK_LOG_LEVEL", "ERROR")
    POSTGRES_JDBC_JAR: str = os.getenv(
        "POSTGRES_JDBC_JAR", str(REPO_ROOT / "drivers" / "postgresql-42.7.3.jar")
    )

    @property
    def postgres_connection_uri(self) -> str:
        """SQLAlchemy connection string."""
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@"
            f"{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        """JDBC connection string for Spark."""
        return f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


settings = Settings()
