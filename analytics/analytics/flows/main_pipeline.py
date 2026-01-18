"""
Main analytics pipeline flow for Claude Analytics.

This module defines the core ELT pipeline that orchestrates:
1. Extract: Pull data from MongoDB to Iceberg tables
2. Load: Load Iceberg data into DuckDB
3. Transform: Run dbt models to build analytics layer

Includes automatic Metabase coordination to prevent DuckDB lock conflicts.
"""

import os
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from prefect import flow, task, get_run_logger

from analytics.config import get_settings
from analytics.loader import DuckDBLoader


# Task retry configuration
RETRY_DELAYS = [30, 60, 120]  # seconds

# Metabase control configuration
METABASE_CONTAINERS = ["metabase", "dbt-docs"]


def _run_docker_command(action: str, containers: list[str], logger) -> bool:
    """
    Run docker command to control containers.

    Args:
        action: 'stop' or 'start'
        containers: List of container names
        logger: Prefect logger instance

    Returns:
        True if successful, False otherwise
    """
    success = True

    for container in containers:
        cmd = ["docker", action, container]

        try:
            logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                logger.info(f"Successfully {action}ed container: {container}")
            else:
                # Container might not exist or already be in desired state
                logger.warning(f"docker {action} {container}: {result.stderr.strip()}")
                # Don't fail if container doesn't exist
                if "No such container" not in result.stderr:
                    success = False

        except subprocess.TimeoutExpired:
            logger.warning(f"docker {action} {container} timed out")
            success = False
        except FileNotFoundError:
            logger.warning("docker CLI not found - running outside Docker?")
            return False
        except Exception as e:
            logger.warning(f"Failed to {action} container {container}: {e}")
            success = False

    return success


def pause_metabase(logger) -> bool:
    """Stop Metabase containers to release DuckDB lock."""
    logger.info("Pausing Metabase to release DuckDB lock...")
    success = _run_docker_command("stop", METABASE_CONTAINERS, logger)
    if success:
        # Give containers time to fully stop and release locks
        time.sleep(2)
    return success


def resume_metabase(logger) -> bool:
    """Start Metabase containers after pipeline completes."""
    logger.info("Resuming Metabase...")
    return _run_docker_command("start", METABASE_CONTAINERS, logger)


@contextmanager
def metabase_paused(logger) -> Generator[None, None, None]:
    """
    Context manager to pause Metabase during DuckDB operations.

    Automatically resumes Metabase even if an exception occurs.
    """
    paused = pause_metabase(logger)
    try:
        yield
    finally:
        if paused:
            resume_metabase(logger)


@task(
    name="extract-mongodb",
    description="Extract data from MongoDB to Iceberg table",
    retries=3,
    retry_delay_seconds=RETRY_DELAYS,
)
def extract_task(full_backfill: bool = False) -> dict:
    """
    Extract data from MongoDB and write to Iceberg table.

    Args:
        full_backfill: If True, extract all historical data

    Returns:
        Dictionary with extraction statistics
    """
    from analytics.extractor import IcebergExtractor

    logger = get_run_logger()
    settings = get_settings()

    logger.info("Starting MongoDB extraction to Iceberg")

    extractor = IcebergExtractor(settings=settings)

    try:
        if full_backfill:
            logger.info("Running full backfill extraction")
            count = extractor.full_extract()
        else:
            logger.info("Running incremental extraction")
            count = extractor.incremental_extract()

        stats = {"records_written": count}
        logger.info(f"Extraction complete: {stats}")
        return stats

    finally:
        extractor.disconnect()


@task(
    name="load-duckdb",
    description="Load Iceberg table into DuckDB",
    retries=3,
    retry_delay_seconds=RETRY_DELAYS,
)
def load_task(
    extraction_stats: dict,
    full_refresh: bool = False,
) -> dict:
    """
    Load Iceberg table into DuckDB.

    Args:
        extraction_stats: Stats from extraction task (for dependency)
        full_refresh: If True, reload all data

    Returns:
        Dictionary with loading statistics
    """
    logger = get_run_logger()
    settings = get_settings()

    logger.info("Starting DuckDB loading from Iceberg")

    loader = DuckDBLoader(settings=settings)

    try:
        # Ensure database and schema exist
        loader.create_database()

        if full_refresh:
            logger.info("Running full load from Iceberg")
            rows = loader.load(full_refresh=True)
        else:
            logger.info("Running incremental upsert from Iceberg")
            rows = loader.upsert()

        # Get final stats
        table_stats = loader.get_table_stats()
        logger.info(f"Load complete. Table stats: {table_stats}")

        return {
            "rows_loaded": rows,
            "table_stats": table_stats,
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
    version="2.1.0",
)
def analytics_pipeline(
    full_backfill: bool = False,
    full_refresh: bool = False,
    skip_extract: bool = False,
    skip_load: bool = False,
    skip_transform: bool = False,
    dbt_select: Optional[str] = None,
    coordinate_metabase: bool = True,
) -> dict:
    """
    Main analytics pipeline orchestrating extract, load, and transform.

    Args:
        full_backfill: Extract all historical data from MongoDB
        full_refresh: Reload all data and rebuild dbt models
        skip_extract: Skip extraction step
        skip_load: Skip loading step
        skip_transform: Skip transformation step
        dbt_select: Optional dbt selector for partial runs
        coordinate_metabase: If True, pause Metabase during DuckDB operations
            to prevent lock conflicts (default: True)

    Returns:
        Dictionary with pipeline results from each step
    """
    logger = get_run_logger()
    results = {}

    logger.info("Starting Claude Analytics Pipeline")
    logger.info(f"Options: backfill={full_backfill}, refresh={full_refresh}, coordinate_metabase={coordinate_metabase}")

    # Step 1: Extract (doesn't need Metabase coordination - writes to Iceberg)
    if not skip_extract:
        extraction_stats = extract_task(full_backfill=full_backfill)
        results["extraction"] = extraction_stats
    else:
        logger.info("Skipping extraction step")
        results["extraction"] = {"skipped": True}

    # Steps 2 & 3: Load and Transform (need Metabase coordination for DuckDB access)
    needs_duckdb = not skip_load or not skip_transform

    if needs_duckdb and coordinate_metabase:
        logger.info("Coordinating with Metabase for DuckDB access...")
        with metabase_paused(logger):
            results = _run_duckdb_steps(
                results, skip_load, skip_transform, full_refresh, dbt_select, logger
            )
    else:
        results = _run_duckdb_steps(
            results, skip_load, skip_transform, full_refresh, dbt_select, logger
        )

    logger.info("Pipeline complete")
    logger.info(f"Results: {results}")

    return results


def _run_duckdb_steps(
    results: dict,
    skip_load: bool,
    skip_transform: bool,
    full_refresh: bool,
    dbt_select: Optional[str],
    logger,
) -> dict:
    """Run the DuckDB-dependent steps (load and transform)."""
    # Step 2: Load
    if not skip_load:
        load_stats = load_task(
            extraction_stats=results["extraction"],
            full_refresh=full_refresh,
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
