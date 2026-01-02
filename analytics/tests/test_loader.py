"""
Tests for DuckDB loader module.
"""

import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analytics.loader import DuckDBLoader


@pytest.fixture
def temp_db_path() -> Path:
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test.db"


@pytest.fixture
def loader(temp_db_path: Path) -> DuckDBLoader:
    """Create a loader with temporary database."""
    from analytics.config import Settings, DuckDBSettings, DataSettings

    settings = Settings()
    settings.duckdb = DuckDBSettings(path=temp_db_path, threads=1)
    settings.data = DataSettings(
        data_dir=temp_db_path.parent,
        raw_dir=temp_db_path.parent / "raw",
        incremental_dir=temp_db_path.parent / "incremental",
        dead_letter_dir=temp_db_path.parent / "dead_letter",
    )

    loader = DuckDBLoader(settings)
    yield loader
    loader.disconnect()


@pytest.fixture
def sample_parquet(temp_db_path: Path) -> Path:
    """Create a sample Parquet file for testing."""
    from analytics.extractor import CONVERSATION_SCHEMA

    parquet_dir = temp_db_path.parent / "raw" / "date=2025-01-02"
    parquet_dir.mkdir(parents=True)
    parquet_path = parquet_dir / "test.parquet"

    # Create sample data
    data = [
        {
            "_id": "test-001",
            "type": "user",
            "session_id": "session-001",
            "project_id": "project-001",
            "timestamp": datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2025, 1, 2, 10, 5, 0, tzinfo=timezone.utc),
            "extracted_at": datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            "message_role": "user",
            "message_content": "Hello, world!",
            "message_raw": None,
            "source_file": "/path/to/file.jsonl",
            "date": date(2025, 1, 2),
        },
        {
            "_id": "test-002",
            "type": "assistant",
            "session_id": "session-001",
            "project_id": "project-001",
            "timestamp": datetime(2025, 1, 2, 10, 1, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2025, 1, 2, 10, 6, 0, tzinfo=timezone.utc),
            "extracted_at": datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            "message_role": "assistant",
            "message_content": "Hello! How can I help?",
            "message_raw": None,
            "source_file": "/path/to/file.jsonl",
            "date": date(2025, 1, 2),
        },
    ]

    table = pa.Table.from_pylist(data, schema=CONVERSATION_SCHEMA)
    pq.write_table(table, parquet_path)

    return temp_db_path.parent / "raw"


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

    def test_load_from_parquet(
        self, loader: DuckDBLoader, sample_parquet: Path
    ) -> None:
        """Test loading Parquet files into DuckDB."""
        rows = loader.load_from_parquet(sample_parquet)

        assert rows == 2

        # Verify data loaded correctly
        result = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert result[0][0] == 2

    def test_load_from_parquet_with_full_refresh(
        self, loader: DuckDBLoader, sample_parquet: Path
    ) -> None:
        """Test full refresh loading."""
        # Load once
        loader.load_from_parquet(sample_parquet)

        # Load again with full refresh
        rows = loader.load_from_parquet(sample_parquet, full_refresh=True)

        # Should have same count (not doubled)
        result = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert result[0][0] == 2

    def test_upsert_incremental(
        self, loader: DuckDBLoader, sample_parquet: Path
    ) -> None:
        """Test incremental upsert."""
        # Initial load
        loader.load_from_parquet(sample_parquet)

        # Upsert same data (should not duplicate)
        rows = loader.upsert_incremental(sample_parquet)

        result = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert result[0][0] == 2  # No duplicates

    def test_load_nonexistent_path_raises(self, loader: DuckDBLoader) -> None:
        """Test that loading from non-existent path raises error."""
        with pytest.raises(FileNotFoundError):
            loader.load_from_parquet(Path("/nonexistent/path"))

    def test_get_table_stats(
        self, loader: DuckDBLoader, sample_parquet: Path
    ) -> None:
        """Test getting table statistics."""
        loader.load_from_parquet(sample_parquet)

        stats = loader.get_table_stats()

        assert stats["row_count"] == 2
        assert "date_range" in stats
        assert "type_distribution" in stats
        assert "top_projects" in stats

    def test_get_table_stats_empty_database(self, loader: DuckDBLoader) -> None:
        """Test getting stats from empty database."""
        loader.create_database()

        stats = loader.get_table_stats()

        assert stats["row_count"] == 0

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


class TestDuckDBLoaderDataIntegrity:
    """Tests for data integrity in DuckDBLoader."""

    def test_loaded_data_matches_source(
        self, loader: DuckDBLoader, sample_parquet: Path
    ) -> None:
        """Test that loaded data matches source Parquet."""
        loader.load_from_parquet(sample_parquet)

        # Check specific values
        result = loader.execute_query(
            "SELECT _id, type, message_content FROM raw.conversations ORDER BY _id"
        )

        assert result[0] == ("test-001", "user", "Hello, world!")
        assert result[1] == ("test-002", "assistant", "Hello! How can I help?")

    def test_upsert_updates_existing_records(
        self, loader: DuckDBLoader, temp_db_path: Path
    ) -> None:
        """Test that upsert updates existing records."""
        from analytics.extractor import CONVERSATION_SCHEMA

        # Create initial data
        parquet_dir = temp_db_path.parent / "raw" / "date=2025-01-02"
        parquet_dir.mkdir(parents=True)

        initial_data = [{
            "_id": "update-test",
            "type": "user",
            "session_id": "s1",
            "project_id": "p1",
            "timestamp": datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            "extracted_at": datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
            "message_role": "user",
            "message_content": "Original content",
            "message_raw": None,
            "source_file": "/path",
            "date": date(2025, 1, 2),
        }]

        table = pa.Table.from_pylist(initial_data, schema=CONVERSATION_SCHEMA)
        pq.write_table(table, parquet_dir / "initial.parquet")

        # Load initial data
        loader.load_from_parquet(temp_db_path.parent / "raw")

        # Create updated data
        updated_data = [{
            "_id": "update-test",
            "type": "user",
            "session_id": "s1",
            "project_id": "p1",
            "timestamp": datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            "ingested_at": datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
            "extracted_at": datetime(2025, 1, 2, 13, 0, 0, tzinfo=timezone.utc),
            "message_role": "user",
            "message_content": "Updated content",
            "message_raw": None,
            "source_file": "/path",
            "date": date(2025, 1, 2),
        }]

        table = pa.Table.from_pylist(updated_data, schema=CONVERSATION_SCHEMA)
        pq.write_table(table, parquet_dir / "updated.parquet")

        # Upsert
        loader.upsert_incremental(temp_db_path.parent / "raw")

        # Verify update
        result = loader.execute_query(
            "SELECT message_content FROM raw.conversations WHERE _id = 'update-test'"
        )
        assert result[0][0] == "Updated content"

        # Verify no duplicate
        count = loader.execute_query("SELECT COUNT(*) FROM raw.conversations")
        assert count[0][0] == 1
