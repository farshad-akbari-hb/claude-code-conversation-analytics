"""
Tests for MongoDB extractor module.
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from analytics.extractor import DocumentTransformer, HighWaterMark


class TestDocumentTransformer:
    """Tests for DocumentTransformer class."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.transformer = DocumentTransformer()
        self.extracted_at = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

    def test_flatten_message_none(self) -> None:
        """Test flattening None message."""
        role, content, raw = self.transformer.flatten_message(None)
        assert role is None
        assert content is None
        assert raw is None

    def test_flatten_message_string(self) -> None:
        """Test flattening simple string message."""
        role, content, raw = self.transformer.flatten_message("Hello, world!")
        assert role is None
        assert content == "Hello, world!"
        assert raw is None

    def test_flatten_message_dict_with_role_and_content(self) -> None:
        """Test flattening dict with role and content."""
        message = {"role": "user", "content": "What is Python?"}
        role, content, raw = self.transformer.flatten_message(message)
        assert role == "user"
        assert content == "What is Python?"
        assert raw is not None

    def test_flatten_message_with_content_blocks(self) -> None:
        """Test flattening message with content blocks array."""
        message = {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "Here is the answer:"},
                {"type": "text", "text": "Python is a programming language."},
            ],
        }
        role, content, raw = self.transformer.flatten_message(message)
        assert role == "assistant"
        assert "Here is the answer:" in content
        assert "Python is a programming language." in content

    def test_flatten_message_with_tool_use_block(self) -> None:
        """Test flattening message with tool_use block."""
        message = {
            "role": "assistant",
            "content": [
                {"type": "tool_use", "name": "Read", "input": {"path": "/file.txt"}},
            ],
        }
        role, content, raw = self.transformer.flatten_message(message)
        assert role == "assistant"
        assert "[tool_use: Read]" in content

    def test_parse_timestamp_none(self) -> None:
        """Test parsing None timestamp."""
        result = self.transformer.parse_timestamp(None)
        assert result is None

    def test_parse_timestamp_datetime(self) -> None:
        """Test parsing datetime object."""
        dt = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
        result = self.transformer.parse_timestamp(dt)
        assert result == dt

    def test_parse_timestamp_iso_string(self) -> None:
        """Test parsing ISO format string."""
        result = self.transformer.parse_timestamp("2025-01-02T12:00:00Z")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 2

    def test_parse_timestamp_iso_string_with_offset(self) -> None:
        """Test parsing ISO format string with timezone offset."""
        result = self.transformer.parse_timestamp("2025-01-02T12:00:00+00:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_transform_full_document(self) -> None:
        """Test transforming a complete MongoDB document."""
        doc = {
            "_id": "abc123",
            "type": "user",
            "sessionId": "session-001",
            "projectId": "project-001",
            "timestamp": "2025-01-02T10:00:00Z",
            "ingestedAt": datetime(2025, 1, 2, 10, 5, 0, tzinfo=timezone.utc),
            "message": {"role": "user", "content": "Hello!"},
            "sourceFile": "/path/to/file.jsonl",
        }

        result = self.transformer.transform(doc, self.extracted_at)

        assert result["_id"] == "abc123"
        assert result["type"] == "user"
        assert result["session_id"] == "session-001"
        assert result["project_id"] == "project-001"
        assert result["message_role"] == "user"
        assert result["message_content"] == "Hello!"
        assert result["source_file"] == "/path/to/file.jsonl"
        assert result["date"] is not None

    def test_transform_minimal_document(self) -> None:
        """Test transforming a minimal MongoDB document."""
        doc = {
            "_id": "abc123",
            "type": "message",
        }

        result = self.transformer.transform(doc, self.extracted_at)

        assert result["_id"] == "abc123"
        assert result["type"] == "message"
        assert result["session_id"] is None
        assert result["project_id"] is None
        assert result["date"] == self.extracted_at.date()


class TestHighWaterMark:
    """Tests for HighWaterMark class."""

    def test_get_nonexistent_file(self) -> None:
        """Test getting high water mark when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hwm = HighWaterMark(Path(tmpdir) / "nonexistent.json")
            result = hwm.get()
            assert result is None

    def test_set_and_get(self) -> None:
        """Test setting and getting high water mark."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hwm = HighWaterMark(Path(tmpdir) / "hwm.json")
            timestamp = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

            hwm.set(timestamp)
            result = hwm.get()

            assert result is not None
            assert result.year == 2025
            assert result.month == 1
            assert result.day == 2

    def test_set_creates_parent_directory(self) -> None:
        """Test that set creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            hwm = HighWaterMark(Path(tmpdir) / "nested" / "dir" / "hwm.json")
            timestamp = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

            hwm.set(timestamp)

            assert hwm.file_path.exists()

    def test_get_invalid_json(self) -> None:
        """Test getting high water mark with invalid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "hwm.json"
            file_path.write_text("invalid json")

            hwm = HighWaterMark(file_path)
            result = hwm.get()

            assert result is None


class TestParquetSchema:
    """Tests for Parquet schema definition."""

    def test_schema_has_required_fields(self) -> None:
        """Test that schema has all required fields."""
        from analytics.extractor import CONVERSATION_SCHEMA

        field_names = [field.name for field in CONVERSATION_SCHEMA]

        assert "_id" in field_names
        assert "type" in field_names
        assert "session_id" in field_names
        assert "project_id" in field_names
        assert "timestamp" in field_names
        assert "message_role" in field_names
        assert "message_content" in field_names
        assert "date" in field_names
