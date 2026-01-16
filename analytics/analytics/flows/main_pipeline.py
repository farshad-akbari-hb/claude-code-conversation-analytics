"""
Main analytics pipeline flow for Claude Analytics.

This module defines the core ELT pipeline that orchestrates:
1. Extract: Pull data from MongoDB to Parquet files or Iceberg tables
2. Load: Load Parquet/Iceberg data into DuckDB
3. Transform: Run dbt models to build analytics layer
"""

import subprocess
from pathlib import Path
from typing import Optional

from prefect import flow, task, get_run_logger

from analytics.config import get_settings
from analytics.extractor import MongoExtractor
from analytics.loader import DuckDBLoader


# Task retry configuration
RETRY_DELAYS = [30, 60, 120]  # seconds


@task(
    name="extract-mongodb",
    description="Extract data from MongoDB to Parquet files or Iceberg table",
    retries=3,
    retry_delay_seconds=RETRY_DELAYS,
)
def extract_task(full_backfill: bool = False, use_iceberg: bool = False) -> dict:
    """
    Extract data from MongoDB and write to Parquet files or Iceberg table.

    Args:
        full_backfill: If True, extract all historical data
        use_iceberg: If True, write to Iceberg table instead of Parquet files

    Returns:
        Dictionary with extraction statistics
    """
    logger = get_run_logger()
    settings = get_settings()

    logger.info(f"Starting MongoDB extraction (format={'Iceberg' if use_iceberg else 'Parquet'})")

    if use_iceberg:
        from analytics.iceberg_extractor import IcebergExtractor
        extractor = IcebergExtractor(settings=settings)

        try:
            if full_backfill:
                logger.info("Running full backfill extraction to Iceberg")
                count = extractor.full_extract()
            else:
                logger.info("Running incremental extraction to Iceberg")
                count = extractor.incremental_extract()

            stats = {"records_written": count, "format": "iceberg"}
            logger.info(f"Extraction complete: {stats}")
            return stats

        finally:
            extractor.disconnect()
    else:
        extractor = MongoExtractor(settings=settings)

        try:
            if full_backfill:
                logger.info("Running full backfill extraction to Parquet")
                files = extractor.full_extract()
            else:
                logger.info("Running incremental extraction to Parquet")
                files = extractor.incremental_extract()

            stats = {"files_written": len(files), "format": "parquet"}
            logger.info(f"Extraction complete: {stats}")
            return stats

        finally:
            extractor.disconnect()


@task(
    name="load-duckdb",
    description="Load Parquet files or Iceberg table into DuckDB",
    retries=3,
    retry_delay_seconds=RETRY_DELAYS,
)
def load_task(
    extraction_stats: dict,
    full_refresh: bool = False,
    use_iceberg: bool = False,
) -> dict:
    """
    Load Parquet files or Iceberg table into DuckDB.

    Args:
        extraction_stats: Stats from extraction task (for dependency)
        full_refresh: If True, reload all data
        use_iceberg: If True, load from Iceberg table instead of Parquet files

    Returns:
        Dictionary with loading statistics
    """
    logger = get_run_logger()
    settings = get_settings()

    # Auto-detect format from extraction stats if available
    source_format = extraction_stats.get("format", "iceberg" if use_iceberg else "parquet")
    use_iceberg = source_format == "iceberg"

    logger.info(f"Starting DuckDB loading from {source_format}")

    loader = DuckDBLoader(settings=settings)

    try:
        # Ensure database and schema exist
        loader.create_database()

        if use_iceberg:
            iceberg_path = (
                settings.iceberg.warehouse_path
                / settings.iceberg.namespace
                / settings.iceberg.table_name
            )
            if full_refresh:
                logger.info(f"Running full load from Iceberg: {iceberg_path}")
                stats = loader.load_from_iceberg(iceberg_path, full_refresh=True)
            else:
                logger.info(f"Running incremental upsert from Iceberg: {iceberg_path}")
                stats = loader.upsert_from_iceberg(iceberg_path)
        else:
            parquet_path = Path(settings.data.raw_dir)
            if full_refresh:
                logger.info(f"Running full load from Parquet: {parquet_path}")
                stats = loader.load_from_parquet(str(parquet_path), full_refresh=True)
            else:
                logger.info(f"Running incremental upsert from Parquet: {parquet_path}")
                stats = loader.upsert_incremental(str(parquet_path))

        # Get final stats
        table_stats = loader.get_table_stats()
        logger.info(f"Load complete. Table stats: {table_stats}")

        return {
            "load_stats": stats,
            "table_stats": table_stats,
            "source_format": source_format,
        }

    finally:
        loader.disconnect()


@task(
    name="transform-dbt",
    description="Run dbt transformations",
    retries=2,
    retry_delay_seconds=RETRY_DELAYS,
)
def transform_task(
    load_stats: dict,
    full_refresh: bool = False,
    select: Optional[str] = None,
) -> dict:
    """
    Run dbt transformations.

    Args:
        load_stats: Stats from load task (for dependency)
        full_refresh: If True, run with --full-refresh
        select: Optional dbt selector (e.g., "+fct_messages")

    Returns:
        Dictionary with transformation results
    """
    logger = get_run_logger()
    settings = get_settings()

    logger.info("Starting dbt transformations")

    dbt_project_dir = Path(settings.dbt.project_dir)
    dbt_profiles_dir = Path(settings.dbt.profiles_dir)

    # Build dbt command (use 'build' to include seeds, models, and tests)
    cmd = [
        "dbt", "build",
        "--project-dir", str(dbt_project_dir),
        "--profiles-dir", str(dbt_profiles_dir),
        "--target", settings.dbt.target,
    ]

    if full_refresh:
        cmd.append("--full-refresh")

    if select:
        cmd.extend(["--select", select])

    logger.info(f"Running dbt command: {' '.join(cmd)}")

    # Run dbt build (seeds + models + tests)
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(dbt_project_dir),
    )

    if result.returncode != 0:
        logger.error(f"dbt build failed:\n{result.stdout}")
        raise RuntimeError(f"dbt build failed: {result.stdout}")

    logger.info(f"dbt build output:\n{result.stdout}")

    return {
        "build_success": result.returncode == 0,
        "output": result.stdout,
    }


@flow(
    name="claude-analytics-pipeline",
    description="Main ELT pipeline for Claude conversation analytics",
    version="1.1.0",
)
def analytics_pipeline(
    full_backfill: bool = False,
    full_refresh: bool = False,
    use_iceberg: bool = False,
    skip_extract: bool = False,
    skip_load: bool = False,
    skip_transform: bool = False,
    dbt_select: Optional[str] = None,
) -> dict:
    """
    Main analytics pipeline orchestrating extract, load, and transform.

    Args:
        full_backfill: Extract all historical data from MongoDB
        full_refresh: Reload all data and rebuild dbt models
        use_iceberg: Use Iceberg format instead of Parquet files
        skip_extract: Skip extraction step
        skip_load: Skip loading step
        skip_transform: Skip transformation step
        dbt_select: Optional dbt selector for partial runs

    Returns:
        Dictionary with pipeline results from each step
    """
    logger = get_run_logger()
    results = {}

    logger.info("Starting Claude Analytics Pipeline")
    logger.info(f"Options: backfill={full_backfill}, refresh={full_refresh}, iceberg={use_iceberg}")

    # Step 1: Extract
    if not skip_extract:
        extraction_stats = extract_task(full_backfill=full_backfill, use_iceberg=use_iceberg)
        results["extraction"] = extraction_stats
    else:
        logger.info("Skipping extraction step")
        results["extraction"] = {"skipped": True, "format": "iceberg" if use_iceberg else "parquet"}

    # Step 2: Load
    if not skip_load:
        load_stats = load_task(
            extraction_stats=results["extraction"],
            full_refresh=full_refresh,
            use_iceberg=use_iceberg,
        )
        results["load"] = load_stats
    else:
        logger.info("Skipping load step")
        results["load"] = {"skipped": True}

    # Step 3: Transform
    if not skip_transform:
        transform_stats = transform_task(
            load_stats=results["load"],
            full_refresh=full_refresh,
            select=dbt_select,
        )
        results["transform"] = transform_stats
    else:
        logger.info("Skipping transform step")
        results["transform"] = {"skipped": True}

    logger.info("Pipeline complete")
    logger.info(f"Results: {results}")

    return results


# Scheduled pipeline variant
@flow(
    name="claude-analytics-scheduled",
    description="Scheduled hourly pipeline run",
)
def scheduled_pipeline() -> dict:
    """
    Scheduled pipeline variant for hourly batch processing.
    Runs incremental extraction and loading.
    """
    return analytics_pipeline(
        full_backfill=False,
        full_refresh=False,
    )
