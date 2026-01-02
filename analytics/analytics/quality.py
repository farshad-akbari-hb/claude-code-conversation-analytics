"""
Data quality validation using Great Expectations.

This module provides integration with Great Expectations for validating
data quality at different pipeline stages.
"""

import logging
from pathlib import Path
from typing import Any

from analytics.config import get_settings

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Data quality validator using Great Expectations.

    Validates data at bronze and silver layers to ensure
    data integrity throughout the pipeline.
    """

    def __init__(self, ge_project_dir: Path | None = None):
        """
        Initialize the validator.

        Args:
            ge_project_dir: Path to Great Expectations project directory
        """
        self.settings = get_settings()
        self.ge_project_dir = ge_project_dir or Path(
            self.settings.great_expectations.project_dir
        )
        self._context = None

    @property
    def context(self) -> Any:
        """Get or create Great Expectations data context."""
        if self._context is None:
            try:
                import great_expectations as gx
                self._context = gx.get_context(
                    context_root_dir=str(self.ge_project_dir)
                )
            except ImportError:
                logger.warning("Great Expectations not installed")
                return None
            except Exception as e:
                logger.error(f"Failed to initialize GE context: {e}")
                return None
        return self._context

    def validate_bronze(self) -> dict[str, Any]:
        """
        Validate bronze (raw) layer data quality.

        Returns:
            Dictionary with validation results
        """
        return self._run_validation("bronze_expectations")

    def validate_silver(self) -> dict[str, Any]:
        """
        Validate silver (intermediate) layer data quality.

        Returns:
            Dictionary with validation results
        """
        return self._run_validation("silver_expectations")

    def run_checkpoint(self, checkpoint_name: str = "analytics_checkpoint") -> dict[str, Any]:
        """
        Run a named checkpoint.

        Args:
            checkpoint_name: Name of the checkpoint to run

        Returns:
            Dictionary with checkpoint results
        """
        if self.context is None:
            return {"success": False, "error": "Great Expectations not available"}

        try:
            result = self.context.run_checkpoint(checkpoint_name=checkpoint_name)

            return {
                "success": result.success,
                "run_id": str(result.run_id),
                "validation_results": [
                    {
                        "success": vr.success,
                        "statistics": vr.statistics if hasattr(vr, "statistics") else {},
                    }
                    for vr in result.run_results.values()
                ],
            }

        except Exception as e:
            logger.error(f"Checkpoint run failed: {e}")
            return {"success": False, "error": str(e)}

    def _run_validation(self, suite_name: str) -> dict[str, Any]:
        """
        Run a specific expectation suite.

        Args:
            suite_name: Name of the expectation suite

        Returns:
            Dictionary with validation results
        """
        if self.context is None:
            return {"success": False, "error": "Great Expectations not available"}

        try:
            # Get the expectation suite
            suite = self.context.get_expectation_suite(expectation_suite_name=suite_name)

            # Get datasource and batch
            datasource = self.context.get_datasource("duckdb_datasource")

            # Create batch request based on suite
            if "bronze" in suite_name:
                table_name = "raw.conversations"
            else:
                table_name = "staging.stg_conversations"

            batch_request = datasource.get_batch_list_from_batch_request(
                batch_request={
                    "data_asset_name": table_name,
                }
            )

            if not batch_request:
                return {"success": False, "error": f"No batch found for {table_name}"}

            # Validate
            batch = batch_request[0]
            validation_result = batch.validate(expectation_suite=suite)

            return {
                "success": validation_result.success,
                "statistics": validation_result.statistics,
                "results_count": len(validation_result.results),
            }

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"success": False, "error": str(e)}

    def get_data_docs_url(self) -> str | None:
        """
        Get the URL for data documentation site.

        Returns:
            URL string or None if not available
        """
        if self.context is None:
            return None

        try:
            docs_sites = self.context.get_docs_sites_urls()
            if docs_sites:
                return docs_sites[0].get("site_url")
            return None
        except Exception:
            return None

    def build_data_docs(self) -> bool:
        """
        Build data documentation.

        Returns:
            True if successful, False otherwise
        """
        if self.context is None:
            return False

        try:
            self.context.build_data_docs()
            return True
        except Exception as e:
            logger.error(f"Failed to build data docs: {e}")
            return False


def validate_pipeline_data(
    validate_bronze: bool = True,
    validate_silver: bool = True,
) -> dict[str, Any]:
    """
    Convenience function to validate pipeline data.

    Args:
        validate_bronze: Whether to validate bronze layer
        validate_silver: Whether to validate silver layer

    Returns:
        Dictionary with all validation results
    """
    validator = DataQualityValidator()
    results = {}

    if validate_bronze:
        results["bronze"] = validator.validate_bronze()

    if validate_silver:
        results["silver"] = validator.validate_silver()

    # Overall success
    results["success"] = all(
        r.get("success", False) for r in results.values()
        if isinstance(r, dict)
    )

    return results
