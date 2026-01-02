"""
Prefect flows for Claude Analytics pipeline orchestration.

This package contains Prefect flow definitions for:
- Main analytics pipeline (extract → load → transform)
- Scheduled batch processing
"""

from analytics.flows.main_pipeline import (
    analytics_pipeline,
    scheduled_pipeline,
    extract_task,
    load_task,
    transform_task,
)

__all__ = [
    "analytics_pipeline",
    "scheduled_pipeline",
    "extract_task",
    "load_task",
    "transform_task",
]
