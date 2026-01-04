#!/usr/bin/env python3
"""
Configure Metabase DuckDB connection for read-only mode.

This script uses the Metabase API to update the DuckDB database connection
to use read-only mode, preventing lock conflicts with the ETL pipeline.

Usage:
    python scripts/configure_metabase_readonly.py

Environment variables:
    METABASE_URL: Metabase server URL (default: http://localhost:3001)
    METABASE_USER: Admin email (default: admin@example.com)
    METABASE_PASSWORD: Admin password (required)
"""

import json
import os
import sys
import time
from urllib import request, error


METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3001")
METABASE_USER = os.getenv("METABASE_USER", "admin@example.com")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD")


def make_request(
    endpoint: str,
    method: str = "GET",
    data: dict | None = None,
    token: str | None = None,
) -> dict:
    """Make an API request to Metabase."""
    url = f"{METABASE_URL}/api{endpoint}"
    headers = {"Content-Type": "application/json"}

    if token:
        headers["X-Metabase-Session"] = token

    req = request.Request(
        url,
        method=method,
        headers=headers,
        data=json.dumps(data).encode() if data else None,
    )

    try:
        with request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"HTTP Error {e.code}: {body}")
        raise


def wait_for_metabase(max_attempts: int = 30) -> bool:
    """Wait for Metabase to be ready."""
    print(f"Waiting for Metabase at {METABASE_URL}...")

    for attempt in range(max_attempts):
        try:
            req = request.Request(f"{METABASE_URL}/api/health")
            with request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                if data.get("status") == "ok":
                    print("Metabase is ready")
                    return True
        except Exception:
            pass

        time.sleep(2)
        print(f"  Attempt {attempt + 1}/{max_attempts}...")

    return False


def login() -> str:
    """Login to Metabase and get session token."""
    print(f"Logging in as {METABASE_USER}...")

    response = make_request(
        "/session",
        method="POST",
        data={"username": METABASE_USER, "password": METABASE_PASSWORD},
    )

    token = response.get("id")
    if not token:
        raise RuntimeError("Failed to get session token")

    print("Login successful")
    return token


def get_databases(token: str) -> list[dict]:
    """Get all configured databases."""
    return make_request("/database", token=token)


def find_duckdb_database(token: str) -> dict | None:
    """Find the DuckDB database configuration."""
    databases = get_databases(token)

    for db in databases.get("data", []):
        if db.get("engine") == "duckdb":
            return db

    return None


def update_database_readonly(token: str, db_id: int, db_config: dict) -> None:
    """Update database to use read-only mode."""
    print(f"Updating database ID {db_id} to read-only mode...")

    # Get current details
    details = db_config.get("details", {})

    # Add read_only option
    # The DuckDB driver supports additional JDBC options
    current_options = details.get("additional-options", "")

    if "duckdb.read_only=true" in current_options:
        print("Database is already configured for read-only mode")
        return

    # Append read_only option
    if current_options:
        new_options = f"{current_options};duckdb.read_only=true"
    else:
        new_options = "duckdb.read_only=true"

    details["additional-options"] = new_options

    # Update the database
    make_request(
        f"/database/{db_id}",
        method="PUT",
        data={"details": details},
        token=token,
    )

    print("Database updated to read-only mode")


def setup_duckdb_readonly(token: str, db_path: str = "/duckdb/analytics.db") -> None:
    """
    Set up DuckDB database with read-only configuration.

    If DuckDB database doesn't exist, creates it with read-only mode.
    If it exists, updates it to use read-only mode.
    """
    existing_db = find_duckdb_database(token)

    if existing_db:
        print(f"Found existing DuckDB database: {existing_db.get('name')}")
        update_database_readonly(token, existing_db["id"], existing_db)
    else:
        print("No DuckDB database found. Creating new connection...")

        # Create new DuckDB database with read-only mode
        make_request(
            "/database",
            method="POST",
            data={
                "engine": "duckdb",
                "name": "Claude Analytics (DuckDB)",
                "details": {
                    "database_file": db_path,
                    "additional-options": "duckdb.read_only=true",
                },
            },
            token=token,
        )

        print(f"Created DuckDB database connection to {db_path} (read-only)")


def main() -> int:
    """Main entry point."""
    if not METABASE_PASSWORD:
        print("Error: METABASE_PASSWORD environment variable is required")
        print("\nUsage:")
        print("  METABASE_PASSWORD=your_password python scripts/configure_metabase_readonly.py")
        return 1

    # Wait for Metabase to be ready
    if not wait_for_metabase():
        print("Error: Metabase is not responding")
        return 1

    try:
        # Login
        token = login()

        # Configure DuckDB for read-only
        setup_duckdb_readonly(token)

        print("\nSuccess! DuckDB is now configured for read-only access.")
        print("The ETL pipeline can now write to DuckDB without lock conflicts.")

        return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
