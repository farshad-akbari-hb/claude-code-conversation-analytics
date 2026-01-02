"""
MongoDB Extractor for Claude Analytics Platform.

Extracts conversation data from MongoDB and writes to Parquet files
with date-based partitioning for efficient downstream processing.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from analytics.config import Settings, get_settings

logger = logging.getLogger(__name__)


# Parquet schema for extracted conversations
CONVERSATION_SCHEMA = pa.schema([
    # Primary identifiers
    ("_id", pa.string()),
    ("type", pa.string()),
    ("session_id", pa.string()),
    ("project_id", pa.string()),

    # Timestamps
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
    ("extracted_at", pa.timestamp("us", tz="UTC")),

    # Message content (flattened)
    ("message_role", pa.string()),
    ("message_content", pa.string()),
    ("message_raw", pa.string()),  # Original JSON for complex messages

    # Source tracking
    ("source_file", pa.string()),

    # Partitioning
    ("date", pa.date32()),
])


class DocumentTransformer:
    """
    Transforms MongoDB documents into a flat structure suitable for Parquet.

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
        Transform a MongoDB document into a flat record for Parquet.

        Args:
            doc: MongoDB document
            extracted_at: Timestamp of extraction

        Returns:
            Flattened dictionary ready for Parquet
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


class MongoExtractor:
    """
    Extracts conversation data from MongoDB and writes to Parquet files.

    Supports both full historical backfill and incremental extraction
    based on high water mark tracking.
    """

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self.transformer = DocumentTransformer()
        self.high_water_mark = HighWaterMark(self.settings.pipeline.high_water_mark_file)

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

    def _write_partition(
        self,
        records: list[dict[str, Any]],
        partition_date: datetime,
        output_dir: Path,
    ) -> Path:
        """
        Write records to a Parquet file in date-partitioned directory.

        Args:
            records: List of transformed records
            partition_date: Date for partitioning
            output_dir: Base output directory

        Returns:
            Path to written Parquet file
        """
        # Create partition directory
        date_str = partition_date.strftime("%Y-%m-%d")
        partition_dir = output_dir / f"date={date_str}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        # Generate unique filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        file_path = partition_dir / f"conversations_{timestamp}.parquet"

        # Create PyArrow table
        table = pa.Table.from_pylist(records, schema=CONVERSATION_SCHEMA)

        # Write Parquet file with compression
        pq.write_table(
            table,
            file_path,
            compression="snappy",
            write_statistics=True,
        )

        logger.info(f"Wrote {len(records)} records to {file_path}")
        return file_path

    def extract(
        self,
        full_backfill: bool = False,
        output_dir: Path | None = None,
    ) -> list[Path]:
        """
        Extract data from MongoDB and write to Parquet files.

        Args:
            full_backfill: If True, extract all data ignoring high water mark
            output_dir: Output directory for Parquet files

        Returns:
            List of paths to written Parquet files
        """
        output_dir = output_dir or self.settings.data.raw_dir
        output_dir.mkdir(parents=True, exist_ok=True)

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
        written_files: list[Path] = []

        # Group records by date for partitioned writing
        records_by_date: dict[str, list[dict[str, Any]]] = {}
        latest_ingested_at: datetime | None = None
        total_count = 0

        try:
            self.connect()

            for doc in self._fetch_documents(since=since):
                # Transform document
                record = self.transformer.transform(doc, extracted_at)

                # Group by partition date
                date_key = record["date"].isoformat() if record["date"] else "unknown"
                if date_key not in records_by_date:
                    records_by_date[date_key] = []
                records_by_date[date_key].append(record)

                # Track latest ingested_at for high water mark
                if record["ingested_at"]:
                    if latest_ingested_at is None or record["ingested_at"] > latest_ingested_at:
                        latest_ingested_at = record["ingested_at"]

                total_count += 1

                # Write partitions when they get large enough
                for date_key in list(records_by_date.keys()):
                    if len(records_by_date[date_key]) >= self.settings.pipeline.batch_size:
                        partition_date = datetime.fromisoformat(date_key)
                        file_path = self._write_partition(
                            records_by_date[date_key],
                            partition_date,
                            output_dir,
                        )
                        written_files.append(file_path)
                        records_by_date[date_key] = []

            # Write remaining records
            for date_key, records in records_by_date.items():
                if records:
                    try:
                        partition_date = datetime.fromisoformat(date_key)
                    except ValueError:
                        partition_date = extracted_at
                    file_path = self._write_partition(records, partition_date, output_dir)
                    written_files.append(file_path)

            # Update high water mark
            if latest_ingested_at:
                self.high_water_mark.set(latest_ingested_at)

            logger.info(
                f"Extraction complete: {total_count} documents, "
                f"{len(written_files)} Parquet files"
            )

        finally:
            self.disconnect()

        return written_files

    def full_extract(self, output_dir: Path | None = None) -> list[Path]:
        """
        Perform full historical extraction (ignores high water mark).

        Args:
            output_dir: Output directory for Parquet files

        Returns:
            List of paths to written Parquet files
        """
        return self.extract(full_backfill=True, output_dir=output_dir)

    def incremental_extract(self, output_dir: Path | None = None) -> list[Path]:
        """
        Perform incremental extraction based on high water mark.

        Args:
            output_dir: Output directory for Parquet files

        Returns:
            List of paths to written Parquet files
        """
        return self.extract(full_backfill=False, output_dir=output_dir)


def main() -> None:
    """CLI entry point for extractor."""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="Extract MongoDB data to Parquet")
    parser.add_argument(
        "--full-backfill",
        action="store_true",
        help="Extract all historical data",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Output directory for Parquet files",
    )

    args = parser.parse_args()

    extractor = MongoExtractor()
    files = extractor.extract(
        full_backfill=args.full_backfill,
        output_dir=args.output_dir,
    )

    print(f"Extraction complete. Written files:")
    for f in files:
        print(f"  {f}")


if __name__ == "__main__":
    main()
