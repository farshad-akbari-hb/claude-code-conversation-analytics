"""
Prefect deployment configuration for Claude Analytics.

This module creates and registers deployments for the analytics pipeline.
"""

from datetime import timedelta

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule, IntervalSchedule

from analytics.flows.main_pipeline import analytics_pipeline, scheduled_pipeline


def create_deployments() -> list[Deployment]:
    """
    Create Prefect deployments for the analytics pipeline.

    Returns:
        List of configured Deployment objects
    """
    deployments = []

    # Hourly scheduled pipeline
    hourly_deployment = Deployment.build_from_flow(
        flow=scheduled_pipeline,
        name="hourly-analytics",
        description="Hourly incremental analytics pipeline",
        schedule=IntervalSchedule(interval=timedelta(hours=1)),
        tags=["analytics", "scheduled", "hourly"],
        work_pool_name="analytics-pool",
        parameters={},
    )
    deployments.append(hourly_deployment)

    # Daily full refresh (at 2 AM)
    daily_refresh_deployment = Deployment.build_from_flow(
        flow=analytics_pipeline,
        name="daily-full-refresh",
        description="Daily full refresh of analytics (2 AM)",
        schedule=CronSchedule(cron="0 2 * * *"),
        tags=["analytics", "scheduled", "daily"],
        work_pool_name="analytics-pool",
        parameters={
            "full_backfill": False,
            "full_refresh": True,
        },
    )
    deployments.append(daily_refresh_deployment)

    # Manual ad-hoc pipeline
    adhoc_deployment = Deployment.build_from_flow(
        flow=analytics_pipeline,
        name="adhoc-analytics",
        description="Manual ad-hoc analytics pipeline run",
        schedule=None,  # No schedule, manual trigger only
        tags=["analytics", "manual"],
        work_pool_name="analytics-pool",
        parameters={
            "full_backfill": False,
            "full_refresh": False,
        },
    )
    deployments.append(adhoc_deployment)

    # Full backfill deployment (for initial setup or recovery)
    backfill_deployment = Deployment.build_from_flow(
        flow=analytics_pipeline,
        name="full-backfill",
        description="Full historical backfill (use for initial setup)",
        schedule=None,  # No schedule, manual trigger only
        tags=["analytics", "manual", "backfill"],
        work_pool_name="analytics-pool",
        parameters={
            "full_backfill": True,
            "full_refresh": True,
        },
    )
    deployments.append(backfill_deployment)

    return deployments


def apply_deployments() -> None:
    """Apply all deployments to Prefect server."""
    deployments = create_deployments()
    for deployment in deployments:
        deployment.apply()
        print(f"Applied deployment: {deployment.name}")


if __name__ == "__main__":
    apply_deployments()
