"""
Claude Analytics Platform

ELT analytics platform for Claude Code conversation logs.
Extracts from MongoDB, loads to DuckDB via Apache Iceberg, transforms with dbt.
"""

__version__ = "0.1.0"
__author__ = "Claude Analytics Team"

from analytics.config import Settings, get_settings

__all__ = [
    "__version__",
    "Settings",
    "get_settings",
]
