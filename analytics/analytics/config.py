"""
Configuration management for Claude Analytics Platform.

Uses Pydantic Settings for type-safe configuration with environment variable support.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoSettings(BaseSettings):
    """MongoDB source configuration."""

    model_config = SettingsConfigDict(
        env_prefix="MONGO_",
        env_file=".env.analytics",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    uri: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URI",
    )
    db: str = Field(
        default="claude_logs",
        description="Database name containing conversation logs",
    )
    collection: str = Field(
        default="conversations",
        description="Collection name with conversation entries",
    )

    @property
    def database(self) -> str:
        """Alias for db field for clarity."""
        return self.db


class DuckDBSettings(BaseSettings):
    """DuckDB target configuration."""

    model_config = SettingsConfigDict(env_prefix="DUCKDB_")

    path: Path = Field(
        default=Path("/duckdb/analytics.db"),
        description="Path to DuckDB database file",
    )
    threads: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Number of threads for DuckDB queries",
    )


class IcebergSettings(BaseSettings):
    """Apache Iceberg configuration."""

    model_config = SettingsConfigDict(env_prefix="ICEBERG_")

    warehouse_path: Path = Field(
        default=Path("/data/iceberg"),
        description="Path to Iceberg warehouse directory",
    )
    catalog_name: str = Field(
        default="default",
        description="Name of the Iceberg catalog",
    )
    catalog_type: Literal["sql", "rest"] = Field(
        default="sql",
        description="Type of catalog (sql for SQLite, rest for REST catalog)",
    )
    catalog_uri: str | None = Field(
        default=None,
        description="URI for catalog database (SQLite path or REST endpoint)",
    )
    namespace: str = Field(
        default="analytics",
        description="Iceberg namespace for tables",
    )
    table_name: str = Field(
        default="conversations",
        description="Name of the main Iceberg table",
    )

    @property
    def sqlite_catalog_path(self) -> Path:
        """Get the SQLite catalog database path."""
        if self.catalog_uri:
            return Path(self.catalog_uri.replace("sqlite:///", ""))
        return self.warehouse_path / "catalog.db"

    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.namespace}.{self.table_name}"


class DataSettings(BaseSettings):
    """Data directory configuration."""

    model_config = SettingsConfigDict(env_prefix="")

    data_dir: Path = Field(
        default=Path("/data"),
        description="Base directory for all data files",
    )
    raw_dir: Path = Field(
        default=Path("/data/raw"),
        description="Directory for raw extracted Parquet files (legacy)",
    )
    iceberg_dir: Path = Field(
        default=Path("/data/iceberg"),
        description="Directory for Iceberg warehouse",
    )
    incremental_dir: Path = Field(
        default=Path("/data/incremental"),
        description="Directory for incremental updates",
    )
    dead_letter_dir: Path = Field(
        default=Path("/data/dead_letter"),
        description="Directory for failed records",
    )

    def ensure_directories(self) -> None:
        """Create data directories if they don't exist."""
        for directory in [self.data_dir, self.raw_dir, self.iceberg_dir, self.incremental_dir, self.dead_letter_dir]:
            directory.mkdir(parents=True, exist_ok=True)


class PipelineSettings(BaseSettings):
    """Pipeline execution configuration."""

    model_config = SettingsConfigDict(env_prefix="")

    batch_size: int = Field(
        default=10000,
        ge=100,
        le=100000,
        description="Number of documents to process per batch",
    )
    backfill_all_historical: bool = Field(
        default=True,
        description="Enable full historical backfill on first run",
    )
    high_water_mark_file: Path = Field(
        default=Path("/data/.high_water_mark"),
        description="File for tracking last sync timestamp",
    )


class PrefectSettings(BaseSettings):
    """Prefect orchestration configuration."""

    model_config = SettingsConfigDict(env_prefix="PREFECT_")

    api_url: str | None = Field(
        default=None,
        description="Prefect API URL (leave empty for local execution)",
    )
    work_pool: str = Field(
        default="analytics-pool",
        description="Work pool name for Prefect workers",
    )


class DbtSettings(BaseSettings):
    """dbt configuration."""

    model_config = SettingsConfigDict(env_prefix="DBT_")

    project_dir: Path = Field(
        default=Path("/app/dbt"),
        description="dbt project directory",
    )
    profiles_dir: Path = Field(
        default=Path("/app/dbt"),
        description="dbt profiles directory",
    )
    target: Literal["dev", "prod"] = Field(
        default="dev",
        description="dbt target environment",
    )


class GreatExpectationsSettings(BaseSettings):
    """Great Expectations configuration."""

    model_config = SettingsConfigDict(env_prefix="GE_")

    project_dir: Path = Field(
        default=Path("/app/great_expectations"),
        description="Great Expectations project directory",
    )
    enabled: bool = Field(
        default=True,
        description="Enable/disable data quality checks",
    )


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    model_config = SettingsConfigDict(env_prefix="LOG_")

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Log level",
    )
    format: Literal["json", "text"] = Field(
        default="text",
        description="Log format",
    )


class AlertingSettings(BaseSettings):
    """Alerting configuration."""

    model_config = SettingsConfigDict(env_prefix="")

    slack_webhook_url: str | None = Field(
        default=None,
        description="Slack webhook URL for alerts",
    )
    alert_email: str | None = Field(
        default=None,
        description="Email address for alerts",
    )


class Settings(BaseSettings):
    """
    Main settings class that aggregates all configuration sections.

    Configuration is loaded from environment variables with optional .env file support.
    """

    model_config = SettingsConfigDict(
        env_file=".env.analytics",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Nested settings
    mongo: MongoSettings = Field(default_factory=MongoSettings)
    duckdb: DuckDBSettings = Field(default_factory=DuckDBSettings)
    iceberg: IcebergSettings = Field(default_factory=IcebergSettings)
    data: DataSettings = Field(default_factory=DataSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    prefect: PrefectSettings = Field(default_factory=PrefectSettings)
    dbt: DbtSettings = Field(default_factory=DbtSettings)
    great_expectations: GreatExpectationsSettings = Field(
        default_factory=GreatExpectationsSettings
    )
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    alerting: AlertingSettings = Field(default_factory=AlertingSettings)

    def setup(self) -> None:
        """Initialize settings: create directories, configure logging."""
        self.data.ensure_directories()


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses lru_cache to ensure settings are only loaded once.
    """
    settings = Settings()
    settings.setup()
    return settings
