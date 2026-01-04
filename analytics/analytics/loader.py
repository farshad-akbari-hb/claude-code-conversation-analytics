"""
DuckDB Loader for Claude Analytics Platform.

Loads Parquet files into DuckDB database with support for both
full refresh and incremental upsert operations.
"""

import logging
import time
from pathlib import Path
from typing import Any

import duckdb

from analytics.config import Settings, get_settings

logger = logging.getLogger(__name__)

# Retry configuration for lock conflicts
LOCK_RETRY_MAX_ATTEMPTS = 5
LOCK_RETRY_BASE_DELAY = 2.0  # seconds
LOCK_RETRY_MAX_DELAY = 30.0  # seconds


# SQL statements for schema creation
CREATE_RAW_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

CREATE_CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS raw.conversations (
    -- Primary identifiers
    _id VARCHAR PRIMARY KEY,
    type VARCHAR,
    session_id VARCHAR,
    project_id VARCHAR,

    -- Timestamps
    timestamp TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE,
    extracted_at TIMESTAMP WITH TIME ZONE,

    -- Message content (flattened)
    message_role VARCHAR,
    message_content VARCHAR,
    message_raw VARCHAR,

    -- Source tracking
    source_file VARCHAR,

    -- Partitioning (derived)
    date DATE
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_conversations_project_id ON raw.conversations(project_id);",
    "CREATE INDEX IF NOT EXISTS idx_conversations_session_id ON raw.conversations(session_id);",
    "CREATE INDEX IF NOT EXISTS idx_conversations_date ON raw.conversations(date);",
    "CREATE INDEX IF NOT EXISTS idx_conversations_type ON raw.conversations(type);",
    "CREATE INDEX IF NOT EXISTS idx_conversations_timestamp ON raw.conversations(timestamp);",
]

# SQL for loading and upserting data
LOAD_FROM_PARQUET = """
INSERT INTO raw.conversations
SELECT * FROM read_parquet('{parquet_path}', hive_partitioning=true)
ON CONFLICT (_id) DO UPDATE SET
    type = EXCLUDED.type,
    session_id = EXCLUDED.session_id,
    project_id = EXCLUDED.project_id,
    timestamp = EXCLUDED.timestamp,
    ingested_at = EXCLUDED.ingested_at,
    extracted_at = EXCLUDED.extracted_at,
    message_role = EXCLUDED.message_role,
    message_content = EXCLUDED.message_content,
    message_raw = EXCLUDED.message_raw,
    source_file = EXCLUDED.source_file,
    date = EXCLUDED.date;
"""

COUNT_ROWS = "SELECT COUNT(*) FROM raw.conversations;"

GET_TABLE_INFO = """
SELECT
    table_schema,
    table_name,
    (SELECT COUNT(*) FROM raw.conversations) as row_count
FROM information_schema.tables
WHERE table_schema = 'raw' AND table_name = 'conversations';
"""


class DuckDBLoader:
    """
    Loads Parquet files into DuckDB database.

    Supports both full refresh and incremental upsert operations.
    Uses DuckDB's native Parquet reader with Hive partitioning support.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Establish connection to DuckDB database with retry on lock conflicts.

        Creates the database file if it doesn't exist.
        Uses exponential backoff when encountering lock conflicts.
        """
        if self._conn is not None:
            return self._conn

        db_path = self.settings.duckdb.path
        db_path.parent.mkdir(parents=True, exist_ok=True)

        last_error: Exception | None = None

        for attempt in range(LOCK_RETRY_MAX_ATTEMPTS):
            try:
                logger.info(f"Connecting to DuckDB: {db_path}")
                self._conn = duckdb.connect(str(db_path))

                # Configure DuckDB settings
                self._conn.execute(f"SET threads TO {self.settings.duckdb.threads};")

                logger.info("DuckDB connection established")
                return self._conn

            except duckdb.IOException as e:
                last_error = e
                error_msg = str(e).lower()

                # Check if this is a lock conflict
                if "lock" in error_msg or "conflicting" in error_msg:
                    delay = min(
                        LOCK_RETRY_BASE_DELAY * (2 ** attempt),
                        LOCK_RETRY_MAX_DELAY
                    )
                    logger.warning(
                        f"DuckDB lock conflict (attempt {attempt + 1}/{LOCK_RETRY_MAX_ATTEMPTS}). "
                        f"Another process may be using the database. Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
                else:
                    # Not a lock error, raise immediately
                    raise

        # All retries exhausted
        logger.error(
            f"Failed to connect to DuckDB after {LOCK_RETRY_MAX_ATTEMPTS} attempts. "
            "Ensure Metabase is configured with read_only=true for the DuckDB connection."
        )
        raise last_error  # type: ignore

    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("DuckDB connection closed")

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get connection, connecting if necessary."""
        if self._conn is None:
            self.connect()
        return self._conn  # type: ignore

    def create_database(self) -> None:
        """
        Initialize the DuckDB database with required schemas and tables.

        Creates:
        - raw schema
        - raw.conversations table
        - Indexes for common query patterns
        """
        logger.info("Creating database schema...")

        # Create schema
        self.conn.execute(CREATE_RAW_SCHEMA)
        logger.info("Created 'raw' schema")

        # Create conversations table
        self.conn.execute(CREATE_CONVERSATIONS_TABLE)
        logger.info("Created 'raw.conversations' table")

        # Create indexes
        for idx_sql in CREATE_INDEXES:
            self.conn.execute(idx_sql)
        logger.info(f"Created {len(CREATE_INDEXES)} indexes")

        logger.info("Database schema initialization complete")

    def load_from_parquet(
        self,
        parquet_path: Path | str,
        full_refresh: bool = False,
    ) -> int:
        """
        Load Parquet files into DuckDB.

        Args:
            parquet_path: Path to Parquet file or directory with partitions
            full_refresh: If True, truncate table before loading

        Returns:
            Number of rows loaded
        """
        parquet_path = Path(parquet_path)

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet path not found: {parquet_path}")

        # Ensure schema exists
        self.create_database()

        # Build the glob pattern for Parquet files
        if parquet_path.is_dir():
            # Directory with partitions: data/raw/**/*.parquet
            glob_pattern = str(parquet_path / "**" / "*.parquet")
        else:
            # Single file
            glob_pattern = str(parquet_path)

        # Check if any files match
        matching_files = list(parquet_path.glob("**/*.parquet")) if parquet_path.is_dir() else [parquet_path]
        if not matching_files:
            logger.warning(f"No Parquet files found in {parquet_path}")
            return 0

        logger.info(f"Found {len(matching_files)} Parquet file(s) to load")

        # Get count before
        count_before = self._get_row_count()

        if full_refresh:
            logger.info("Full refresh: truncating existing data")
            self.conn.execute("DELETE FROM raw.conversations;")

        # Load data using upsert (ON CONFLICT)
        load_sql = LOAD_FROM_PARQUET.format(parquet_path=glob_pattern)
        self.conn.execute(load_sql)

        # Get count after
        count_after = self._get_row_count()
        rows_loaded = count_after - count_before if not full_refresh else count_after

        logger.info(f"Loaded {rows_loaded} rows (total: {count_after})")
        return rows_loaded

    def upsert_incremental(self, parquet_path: Path | str) -> int:
        """
        Upsert Parquet files into DuckDB (insert or update on conflict).

        This is the same as load_from_parquet with full_refresh=False,
        but with explicit naming for clarity.

        Args:
            parquet_path: Path to Parquet file or directory

        Returns:
            Number of rows affected
        """
        return self.load_from_parquet(parquet_path, full_refresh=False)

    def _get_row_count(self) -> int:
        """Get current row count in conversations table."""
        try:
            result = self.conn.execute(COUNT_ROWS).fetchone()
            return result[0] if result else 0
        except duckdb.CatalogException:
            # Table doesn't exist yet
            return 0

    def get_table_stats(self) -> dict[str, Any]:
        """
        Get statistics about the loaded data.

        Returns:
            Dictionary with table statistics
        """
        stats: dict[str, Any] = {
            "row_count": self._get_row_count(),
            "tables": [],
        }

        try:
            # Get table info
            result = self.conn.execute(GET_TABLE_INFO).fetchall()
            for row in result:
                stats["tables"].append({
                    "schema": row[0],
                    "name": row[1],
                    "rows": row[2],
                })

            # Get date range
            date_range = self.conn.execute("""
                SELECT
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as date_count
                FROM raw.conversations
            """).fetchone()

            if date_range and date_range[0]:
                stats["date_range"] = {
                    "min": str(date_range[0]),
                    "max": str(date_range[1]),
                    "count": date_range[2],
                }

            # Get project counts
            project_counts = self.conn.execute("""
                SELECT project_id, COUNT(*) as count
                FROM raw.conversations
                GROUP BY project_id
                ORDER BY count DESC
                LIMIT 10
            """).fetchall()

            stats["top_projects"] = [
                {"project_id": row[0], "count": row[1]}
                for row in project_counts
            ]

            # Get type distribution
            type_counts = self.conn.execute("""
                SELECT type, COUNT(*) as count
                FROM raw.conversations
                GROUP BY type
                ORDER BY count DESC
            """).fetchall()

            stats["type_distribution"] = [
                {"type": row[0], "count": row[1]}
                for row in type_counts
            ]

        except duckdb.CatalogException:
            # Table doesn't exist
            pass

        return stats

    def execute_query(self, sql: str) -> list[tuple[Any, ...]]:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query to execute

        Returns:
            List of result tuples
        """
        return self.conn.execute(sql).fetchall()


def main() -> None:
    """CLI entry point for loader."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Load Parquet files into DuckDB")
    parser.add_argument(
        "parquet_path",
        type=Path,
        nargs="?",
        help="Path to Parquet file or directory",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Truncate table before loading",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show table statistics",
    )
    parser.add_argument(
        "--init-only",
        action="store_true",
        help="Only initialize database schema",
    )

    args = parser.parse_args()

    loader = DuckDBLoader()

    try:
        if args.init_only:
            loader.create_database()
            print("Database initialized successfully")
        elif args.stats:
            stats = loader.get_table_stats()
            print(f"Row count: {stats['row_count']}")
            if stats.get("date_range"):
                print(f"Date range: {stats['date_range']['min']} to {stats['date_range']['max']}")
            if stats.get("type_distribution"):
                print("Type distribution:")
                for t in stats["type_distribution"]:
                    print(f"  {t['type']}: {t['count']}")
        elif args.parquet_path:
            rows = loader.load_from_parquet(
                args.parquet_path,
                full_refresh=args.full_refresh,
            )
            print(f"Loaded {rows} rows")
        else:
            parser.print_help()
    finally:
        loader.disconnect()


if __name__ == "__main__":
    main()
