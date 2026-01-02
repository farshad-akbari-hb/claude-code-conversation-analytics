"""
Prefect deployment configuration for Claude Analytics.

Deployments are defined in prefect.yaml at the project root.
Use `prefect deploy --all` to deploy all flows.

This module provides a Python wrapper for convenience.
"""

import subprocess
import sys


def apply_deployments() -> int:
    """
    Deploy all flows to Prefect server using prefect.yaml.

    Returns:
        Exit code from prefect deploy command
    """
    print("Deploying flows to Prefect server...")
    result = subprocess.run(
        ["prefect", "deploy", "--all"],
        cwd="/app",  # Where prefect.yaml is located
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(apply_deployments())
