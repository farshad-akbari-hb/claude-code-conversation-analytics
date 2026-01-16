"""
Apache Iceberg Extractor for Claude Analytics Platform.

Extracts conversation data from MongoDB and writes to Apache Iceberg tables
with support for ACID transactions, schema evolution, and time travel.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    NestedField,
    StringType,
    TimestamptzType,
    DateType,
)
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from analytics.config import Settings, get_settings

logger = logging.getLogger(__name__)


# Iceberg schema for conversations table
ICEBERG_SCHEMA = Schema(
    # Primary identifiers
    NestedField(field_id=1, name="_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="type", field_type=StringType(), required=False),
    NestedField(field_id=3, name="session_id", field_type=StringType(), required=False),
    NestedField(field_id=4, name="project_id", field_type=StringType(), required=False),
    # Timestamps
    NestedField(field_id=5, name="timestamp", field_type=TimestamptzType(), required=False),
    NestedField(field_id=6, name="ingested_at", field_type=TimestamptzType(), required=False),
    NestedField(field_id=7, name="extracted_at", field_type=TimestamptzType(), required=False),
    # Message content (flattened)
    NestedField(field_id=8, name="message_role", field_type=StringType(), required=False),
    NestedField(field_id=9, name="message_content", field_type=StringType(), required=False),
    NestedField(field_id=10, name="message_raw", field_type=StringType(), required=False),
    # Source tracking
    NestedField(field_id=11, name="source_file", field_type=StringType(), required=False),
    # Partitioning
    NestedField(field_id=12, name="date", field_type=DateType(), required=False),
)

# PyArrow schema matching the Iceberg schema (for data conversion)
PYARROW_SCHEMA = pa.schema([
    ("_id", pa.string()),
    ("type", pa.string()),
    ("session_id", pa.string()),
    ("project_id", pa.string()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
    ("extracted_at", pa.timestamp("us", tz="UTC")),
    ("message_role", pa.string()),
    ("message_content", pa.string()),
    ("message_raw", pa.string()),
    ("source_file", pa.string()),
    ("date", pa.date32()),
])


class DocumentTransformer:
    """
    Transforms MongoDB documents into a flat structure suitable for Iceberg.

    Handles the flexible `message` field which can be:
    - A simple string
    - An object with `role` and `content` fields
    - An array of content blocks
    - None/missing
    """

    @staticmethod
    def flatten_message(message: Any) -> tuple[str | None, str | None, str | None]:
        """
        Flatten the message field into role, content, and raw JSON.

        Returns:
            Tuple of (role, content, raw_json)
        """
        if message is None:
            return None, None, None

        if isinstance(message, str):
            return None, message, None

        if isinstance(message, dict):
            role = message.get("role")
            content = message.get("content")

            # Handle content that might be a list of blocks
            if isinstance(content, list):
                # Extract text from content blocks
                text_parts = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "tool_use":
                            text_parts.append(f"[tool_use: {block.get('name', 'unknown')}]")
                        elif block.get("type") == "tool_result":
                            text_parts.append(f"[tool_result]")
                    elif isinstance(block, str):
                        text_parts.append(block)
                content = "\n".join(text_parts) if text_parts else None
            elif not isinstance(content, str):
                content = str(content) if content is not None else None

            # Keep raw JSON for complex messages
            raw_json = json.dumps(message, default=str) if message else None
            return role, content, raw_json

        # Fallback: serialize whatever we got
        return None, None, json.dumps(message, default=str)

    @staticmethod
    def parse_timestamp(ts: Any) -> datetime | None:
        """Parse timestamp from various formats."""
        if ts is None:
            return None

        if isinstance(ts, datetime):
            return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts

        if isinstance(ts, str):
            try:
                # Try ISO format first
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
            except ValueError:
                logger.warning(f"Failed to parse timestamp: {ts}")
                return None

        return None

    def transform(self, doc: dict[str, Any], extracted_at: datetime) -> dict[str, Any]:
        """
        Transform a MongoDB document into a flat record for Iceberg.

        Args:
            doc: MongoDB document
            extracted_at: Timestamp of extraction

        Returns:
            Flattened dictionary ready for Iceberg
        """
        # Extract message fields
        message_role, message_content, message_raw = self.flatten_message(
            doc.get("message")
        )

        # Parse timestamps
        timestamp = self.parse_timestamp(doc.get("timestamp"))
        ingested_at = self.parse_timestamp(doc.get("ingestedAt"))

        # Derive date for partitioning (prefer timestamp, fallback to ingestedAt)
        partition_date = None
        if timestamp:
            partition_date = timestamp.date()
        elif ingested_at:
            partition_date = ingested_at.date()
        else:
            partition_date = extracted_at.date()

        return {
            "_id": str(doc.get("_id", "")),
            "type": doc.get("type"),
            "session_id": doc.get("sessionId"),
            "project_id": doc.get("projectId"),
            "timestamp": timestamp,
            "ingested_at": ingested_at,
            "extracted_at": extracted_at,
            "message_role": message_role,
            "message_content": message_content,
            "message_raw": message_raw,
            "source_file": doc.get("sourceFile"),
            "date": partition_date,
        }


class HighWaterMark:
    """
    Tracks the last successfully extracted timestamp for incremental extraction.

    Stores the high water mark in a simple JSON file.
    """

    def __init__(self, file_path: Path):
        self.file_path = file_path

    def get(self) -> datetime | None:
        """Get the last extraction timestamp."""
        if not self.file_path.exists():
            return None

        try:
            data = json.loads(self.file_path.read_text())
            ts = data.get("last_extracted_at")
            if ts:
                return datetime.fromisoformat(ts)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to read high water mark: {e}")

        return None

    def set(self, timestamp: datetime) -> None:
        """Set the last extraction timestamp."""
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "last_extracted_at": timestamp.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.file_path.write_text(json.dumps(data, indent=2))
        logger.info(f"Updated high water mark to {timestamp.isoformat()}")


class IcebergCatalogManager:
    """
    Manages Apache Iceberg catalog and table operations.

    Supports SQLite catalog for local development and REST catalog
    for production deployments.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._catalog: SqlCatalog | None = None

    @property
    def catalog(self) -> SqlCatalog:
        """Get or create the Iceberg catalog."""
        if self._catalog is None:
            self._catalog = self._create_catalog()
        return self._catalog

    def _create_catalog(self) -> SqlCatalog:
        """Create and configure the Iceberg catalog."""
        iceberg_settings = self.settings.iceberg
        warehouse_path = iceberg_settings.warehouse_path

        # Ensure warehouse directory exists
        warehouse_path.mkdir(parents=True, exist_ok=True)

        # Configure SQLite catalog
        catalog_path = iceberg_settings.sqlite_catalog_path
        catalog_path.parent.mkdir(parents=True, exist_ok=True)

        catalog_uri = f"sqlite:///{catalog_path}"

        logger.info(f"Creating Iceberg catalog at {catalog_uri}")
        logger.info(f"Warehouse path: {warehouse_path}")

        catalog = SqlCatalog(
            name=iceberg_settings.catalog_name,
            **{
                "uri": catalog_uri,
                "warehouse": str(warehouse_path),
            }
        )

        return catalog

    def ensure_namespace(self) -> None:
        """Create the namespace if it doesn't exist."""
        namespace = self.settings.iceberg.namespace
        try:
            self.catalog.create_namespace(namespace)
            logger.info(f"Created namespace: {namespace}")
        except NamespaceAlreadyExistsError:
            logger.debug(f"Namespace already exists: {namespace}")

    def get_or_create_table(self) -> Table:
        """Get the conversations table, creating it if necessary."""
        self.ensure_namespace()

        table_name = self.settings.iceberg.full_table_name

        try:
            table = self.catalog.load_table(table_name)
            logger.info(f"Loaded existing table: {table_name}")
            return table
        except NoSuchTableError:
            logger.info(f"Creating new table: {table_name}")

            # Create table with date partitioning
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import DayTransform

            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=12,  # date field
                    field_id=1000,
                    transform=DayTransform(),
                    name="date_day",
                )
            )

            table = self.catalog.create_table(
                identifier=table_name,
                schema=ICEBERG_SCHEMA,
                partition_spec=partition_spec,
            )

            logger.info(f"Created table: {table_name}")
            return table

    def drop_table(self) -> bool:
        """Drop the conversations table if it exists."""
        table_name = self.settings.iceberg.full_table_name
        try:
            self.catalog.drop_table(table_name)
            logger.info(f"Dropped table: {table_name}")
            return True
        except NoSuchTableError:
            logger.info(f"Table does not exist: {table_name}")
            return False


class IcebergExtractor:
    """
    Extracts conversation data from MongoDB and writes to Apache Iceberg tables.

    Supports both full historical backfill and incremental extraction
    based on high water mark tracking.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.transformer = DocumentTransformer()
        self.high_water_mark = HighWaterMark(self.settings.pipeline.high_water_mark_file)
        self.catalog_manager = IcebergCatalogManager(settings)

        self._client: MongoClient | None = None
        self._db: Database | None = None
        self._collection: Collection | None = None

    def connect(self) -> None:
        """Establish MongoDB connection."""
        if self._client is not None:
            return

        logger.info(f"Connecting to MongoDB: {self.settings.mongo.uri}")
        self._client = MongoClient(self.settings.mongo.uri)
        self._db = self._client[self.settings.mongo.db]
        self._collection = self._db[self.settings.mongo.collection]

        # Verify connection
        self._client.admin.command("ping")
        logger.info("MongoDB connection established")

    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._collection = None
            logger.info("MongoDB connection closed")

    @property
    def collection(self) -> Collection:
        """Get the MongoDB collection, connecting if necessary."""
        if self._collection is None:
            self.connect()
        return self._collection  # type: ignore

    def _fetch_documents(
        self,
        since: datetime | None = None,
        batch_size: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        Fetch documents from MongoDB with optional time filter.

        Args:
            since: Only fetch documents ingested after this timestamp
            batch_size: Number of documents per batch

        Yields:
            MongoDB documents
        """
        batch_size = batch_size or self.settings.pipeline.batch_size

        query: dict[str, Any] = {}
        if since:
            query["ingestedAt"] = {"$gt": since}

        cursor = (
            self.collection
            .find(query)
            .sort("ingestedAt", 1)  # Oldest first for consistent processing
            .batch_size(batch_size)
        )

        count = 0
        for doc in cursor:
            yield doc
            count += 1
            if count % 10000 == 0:
                logger.info(f"Fetched {count} documents...")

        logger.info(f"Total documents fetched: {count}")

    def _write_to_iceberg(
        self,
        records: list[dict[str, Any]],
        table: Table,
    ) -> int:
        """
        Write records to Iceberg table using append operation.

        Args:
            records: List of transformed records
            table: Iceberg table to write to

        Returns:
            Number of records written
        """
        if not records:
            return 0

        # Create PyArrow table from records
        arrow_table = pa.Table.from_pylist(records, schema=PYARROW_SCHEMA)

        # Append to Iceberg table
        table.append(arrow_table)

        logger.info(f"Appended {len(records)} records to Iceberg table")
        return len(records)

    def extract(
        self,
        full_backfill: bool = False,
    ) -> int:
        """
        Extract data from MongoDB and write to Iceberg table.

        Args:
            full_backfill: If True, extract all data ignoring high water mark

        Returns:
            Total number of records extracted
        """
        # Get or create the Iceberg table
        table = self.catalog_manager.get_or_create_table()

        # Determine start time
        since = None
        if not full_backfill:
            since = self.high_water_mark.get()
            if since:
                logger.info(f"Incremental extraction since {since.isoformat()}")
            else:
                logger.info("No high water mark found, performing full extraction")
        else:
            logger.info("Full backfill requested, extracting all data")

        extracted_at = datetime.now(timezone.utc)
        latest_ingested_at: datetime | None = None
        total_count = 0
        batch: list[dict[str, Any]] = []

        try:
            self.connect()

            for doc in self._fetch_documents(since=since):
                # Transform document
                record = self.transformer.transform(doc, extracted_at)
                batch.append(record)

                # Track latest ingested_at for high water mark
                if record["ingested_at"]:
                    if latest_ingested_at is None or record["ingested_at"] > latest_ingested_at:
                        latest_ingested_at = record["ingested_at"]

                total_count += 1

                # Write batch when it reaches the configured size
                if len(batch) >= self.settings.pipeline.batch_size:
                    self._write_to_iceberg(batch, table)
                    batch = []

            # Write remaining records
            if batch:
                self._write_to_iceberg(batch, table)

            # Update high water mark
            if latest_ingested_at:
                self.high_water_mark.set(latest_ingested_at)

            logger.info(f"Extraction complete: {total_count} documents to Iceberg")

        finally:
            self.disconnect()

        return total_count

    def full_extract(self) -> int:
        """
        Perform full historical extraction (ignores high water mark).

        Returns:
            Total number of records extracted
        """
        return self.extract(full_backfill=True)

    def incremental_extract(self) -> int:
        """
        Perform incremental extraction based on high water mark.

        Returns:
            Total number of records extracted
        """
        return self.extract(full_backfill=False)

    def get_table_info(self) -> dict[str, Any]:
        """Get information about the Iceberg table."""
        try:
            table = self.catalog_manager.get_or_create_table()

            # Get snapshot info
            current_snapshot = table.current_snapshot()

            info = {
                "table_name": self.settings.iceberg.full_table_name,
                "location": str(table.location()),
                "schema_fields": [field.name for field in table.schema().fields],
                "partition_spec": str(table.spec()),
            }

            if current_snapshot:
                info["current_snapshot_id"] = current_snapshot.snapshot_id
                info["snapshot_timestamp"] = current_snapshot.timestamp_ms

                # Get summary stats if available
                if current_snapshot.summary:
                    info["summary"] = dict(current_snapshot.summary)

            # Get snapshot history
            snapshots = list(table.snapshots())
            info["snapshot_count"] = len(snapshots)

            return info

        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {"error": str(e)}


def main() -> None:
    """CLI entry point for Iceberg extractor."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Extract MongoDB data to Iceberg")
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Extract all historical data",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show table information",
    )

    args = parser.parse_args()

    extractor = IcebergExtractor()

    if args.info:
        info = extractor.get_table_info()
        print("Iceberg Table Info:")
        for key, value in info.items():
            print(f"  {key}: {value}")
    else:
        count = extractor.extract(full_backfill=args.full_backfill)
        print(f"Extraction complete. {count} records written to Iceberg.")


if __name__ == "__main__":
    main()
