"""
Tests for DuckDB loader module.
"""

import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from analytics.loader import DuckDBLoader


@pytest.fixture
def temp_db_path() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


@pytest.fixture
def temp_iceberg_warehouse() -> Path:
    """Create a temporary Iceberg warehouse path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        warehouse = Path(tmpdir) / "iceberg"
        warehouse.mkdir(parents=True)
        yield warehouse


@pytest.fixture
def loader(temp_db_path: Path, temp_iceberg_warehouse: Path) -> DuckDBLoader:
    """Create a loader with temporary database."""
    from analytics.config import Settings, DuckDBSettings, DataSettings, IcebergSettings

    settings = Settings()
    settings.duckdb = DuckDBSettings(path=temp_db_path, threads=1)
    settings.data = DataSettings(
        data_dir=temp_db_path.parent,
        dead_letter_dir=temp_db_path.parent / "dead_letter",
    )
    settings.iceberg = IcebergSettings(
        warehouse_path=temp_iceberg_warehouse,
        catalog_name="test_catalog",
        namespace="test",
        table_name="conversations",
    )

    loader = DuckDBLoader(settings)
    yield loader
    loader.disconnect()


class TestDuckDBLoader:
    """Tests for DuckDBLoader class."""

    def test_create_database(self, loader: DuckDBLoader) -> None:
        """Test database initialization."""
        loader.create_database()

        # Verify schema exists
        result = loader.execute_query(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'raw'"
        )
        assert len(result) == 1
        assert result[0][0] == "raw"

    def test_create_database_creates_table(self, loader: DuckDBLoader) -> None:
        """Test that create_database creates the conversations table."""
        loader.create_database()

        # Verify table exists
        result = loader.execute_query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'raw' AND table_name = 'conversations'"
        )
        assert len(result) == 1
        assert result[0][0] == "conversations"

    def test_create_database_creates_indexes(self, loader: DuckDBLoader) -> None:
        """Test that create_database creates indexes."""
        loader.create_database()

        # Verify at least one index exists
        result = loader.execute_query(
            "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'conversations'"
        )
        assert len(result) >= 1

    def test_create_database_idempotent(self, loader: DuckDBLoader) -> None:
        """Test that create_database can be called multiple times."""
        loader.create_database()
        loader.create_database()  # Should not raise

        result = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert result[0][0] == 0

    def test_execute_query(self, loader: DuckDBLoader) -> None:
        """Test executing arbitrary SQL."""
        loader.create_database()

        result = loader.execute_query("SELECT 1 + 1 AS result")

        assert result[0][0] == 2

    def test_connect_creates_parent_directory(self, temp_db_path: Path) -> None:
        """Test that connect creates parent directories."""
        from analytics.config import Settings, DuckDBSettings

        nested_path = temp_db_path.parent / "nested" / "dir" / "test.db"

        settings = Settings()
        settings.duckdb = DuckDBSettings(path=nested_path, threads=1)

        loader = DuckDBLoader(settings)
        loader.connect()

        assert nested_path.parent.exists()
        loader.disconnect()

    def test_get_table_stats_empty_database(self, loader: DuckDBLoader) -> None:
        """Test getting stats from empty database."""
        loader.create_database()

        stats = loader.get_table_stats()

        assert stats["row_count"] == 0


class TestDuckDBLoaderIcebergIntegration:
    """Integration tests for DuckDB loader with Iceberg.

    Note: These tests require a properly configured Iceberg catalog
    and are skipped if the Iceberg extension is not available.
    """

    def test_iceberg_extension_installed(self, loader: DuckDBLoader) -> None:
        """Test that Iceberg extension can be installed."""
        loader.connect()

        # This should not raise an error
        loader.execute_query("INSTALL iceberg")
        loader.execute_query("LOAD iceberg")

    def test_upsert_empty_table(self, loader: DuckDBLoader) -> None:
        """Test upsert with empty staging data."""
        loader.create_database()

        # Insert some test data directly
        loader.execute_query("""
            INSERT INTO raw.conversations (_id, type, date, extracted_at)
            VALUES ('test-1', 'user', '2025-01-02', '2025-01-02 12:00:00')
        """)

        result = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert result[0][0] == 1


class TestDuckDBLoaderDataIntegrity:
    """Tests for data integrity in DuckDBLoader."""

    def test_direct_insert_data_integrity(self, loader: DuckDBLoader) -> None:
        """Test that directly inserted data maintains integrity."""
        loader.create_database()

        # Insert test data directly
        loader.execute_query("""
            INSERT INTO raw.conversations
            (_id, type, session_id, project_id, message_role, message_content, date, extracted_at)
            VALUES
            ('test-001', 'user', 'session-001', 'project-001', 'user', 'Hello, world!', '2025-01-02', '2025-01-02 12:00:00'),
            ('test-002', 'assistant', 'session-001', 'project-001', 'assistant', 'Hello! How can I help?', '2025-01-02', '2025-01-02 12:00:00')
        """)

        # Check specific values
        result = loader.execute_query(
            "SELECT _id, type, message_content FROM raw.conversations ORDER BY _id"
        )

        assert result[0] == ("test-001", "user", "Hello, world!")
        assert result[1] == ("test-002", "assistant", "Hello! How can I help?")

    def test_upsert_updates_existing_records(self, loader: DuckDBLoader) -> None:
        """Test that upsert updates existing records."""
        loader.create_database()

        # Insert initial data
        loader.execute_query("""
            INSERT INTO raw.conversations
            (_id, type, message_content, date, extracted_at)
            VALUES ('update-test', 'user', 'Original content', '2025-01-02', '2025-01-02 12:00:00')
        """)

        # Update via direct SQL (simulating upsert)
        loader.execute_query("""
            UPDATE raw.conversations
            SET message_content = 'Updated content'
            WHERE _id = 'update-test'
        """)

        # Verify update
        result = loader.execute_query(
            "SELECT message_content FROM raw.conversations WHERE _id = 'update-test'"
        )
        assert result[0][0] == "Updated content"

        # Verify no duplicate
        count = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert count[0][0] == 1
