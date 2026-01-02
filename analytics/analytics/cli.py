"""
Command-line interface for Claude Analytics Platform.

Provides commands for extraction, loading, transformation, and pipeline execution.
"""

import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from analytics import __version__
from analytics.config import get_settings

app = typer.Typer(
    name="claude-analytics",
    help="Claude Analytics Platform - ELT pipeline for conversation logs",
    add_completion=False,
)
console = Console()


def setup_logging(level: str = "INFO") -> None:
    """Configure logging with rich handler."""
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit",
    ),
) -> None:
    """Claude Analytics Platform CLI."""
    if version:
        console.print(f"claude-analytics version {__version__}")
        raise typer.Exit()


@app.command()
def config() -> None:
    """Display current configuration."""
    settings = get_settings()
    console.print("[bold]Current Configuration[/bold]\n")
    console.print(f"[cyan]MongoDB URI:[/cyan] {settings.mongo.uri}")
    console.print(f"[cyan]MongoDB DB:[/cyan] {settings.mongo.db}")
    console.print(f"[cyan]MongoDB Collection:[/cyan] {settings.mongo.collection}")
    console.print(f"[cyan]DuckDB Path:[/cyan] {settings.duckdb.path}")
    console.print(f"[cyan]Data Directory:[/cyan] {settings.data.data_dir}")
    console.print(f"[cyan]Batch Size:[/cyan] {settings.pipeline.batch_size}")
    console.print(f"[cyan]Log Level:[/cyan] {settings.logging.level}")


@app.command()
def extract(
    full_backfill: bool = typer.Option(
        False,
        "--full-backfill",
        help="Extract all historical data (ignores high water mark)",
    ),
    output_dir: Path | None = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory for Parquet files (defaults to raw_dir)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Extract data from MongoDB to Parquet files."""
    from analytics.extractor import MongoExtractor

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    console.print("[bold blue]MongoDB Extraction[/bold blue]\n")
    console.print(f"  Source: {settings.mongo.uri}/{settings.mongo.db}.{settings.mongo.collection}")
    console.print(f"  Mode: {'Full Backfill' if full_backfill else 'Incremental'}")
    console.print(f"  Output: {output_dir or settings.data.raw_dir}")
    console.print()

    try:
        extractor = MongoExtractor(settings)
        files = extractor.extract(
            full_backfill=full_backfill,
            output_dir=output_dir,
        )

        if files:
            console.print(f"\n[bold green]Extraction complete![/bold green]")
            console.print(f"Written {len(files)} Parquet file(s):\n")

            table = Table(show_header=True, header_style="bold cyan")
            table.add_column("File", style="dim")
            table.add_column("Partition")

            for f in files:
                partition = f.parent.name if f.parent.name.startswith("date=") else "-"
                table.add_row(f.name, partition)

            console.print(table)
        else:
            console.print("[yellow]No new data to extract[/yellow]")

    except Exception as e:
        console.print(f"[bold red]Extraction failed:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def load(
    source_dir: str | None = typer.Option(
        None,
        "--source",
        "-s",
        help="Source directory for Parquet files (defaults to raw_dir)",
    ),
) -> None:
    """Load Parquet files into DuckDB."""
    console.print("[yellow]Load command not yet implemented[/yellow]")
    console.print(f"  source_dir: {source_dir}")
    # TODO: Implement in Phase 3


@app.command()
def transform(
    models: str | None = typer.Option(
        None,
        "--models",
        "-m",
        help="Specific dbt models to run (comma-separated)",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Force full refresh of incremental models",
    ),
) -> None:
    """Run dbt transformations."""
    console.print("[yellow]Transform command not yet implemented[/yellow]")
    console.print(f"  models: {models}")
    console.print(f"  full_refresh: {full_refresh}")
    # TODO: Implement in Phase 4


@app.command()
def pipeline(
    full_backfill: bool = typer.Option(
        False,
        "--full-backfill",
        help="Run full historical backfill",
    ),
) -> None:
    """Run the complete analytics pipeline (extract → load → transform)."""
    console.print("[yellow]Pipeline command not yet implemented[/yellow]")
    console.print(f"  full_backfill: {full_backfill}")
    # TODO: Implement in Phase 10


@app.command()
def validate() -> None:
    """Run Great Expectations data quality checks."""
    console.print("[yellow]Validate command not yet implemented[/yellow]")
    # TODO: Implement in Phase 12


if __name__ == "__main__":
    app()
