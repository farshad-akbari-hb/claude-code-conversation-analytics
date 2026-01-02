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
    source_dir: Path | None = typer.Option(
        None,
        "--source",
        "-s",
        help="Source directory for Parquet files (defaults to raw_dir)",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Truncate table before loading",
    ),
    stats: bool = typer.Option(
        False,
        "--stats",
        help="Show table statistics after loading",
    ),
    init_only: bool = typer.Option(
        False,
        "--init-only",
        help="Only initialize database schema (no data load)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Load Parquet files into DuckDB."""
    from analytics.loader import DuckDBLoader

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    source_path = source_dir or settings.data.raw_dir

    console.print("[bold blue]DuckDB Loading[/bold blue]\n")
    console.print(f"  Database: {settings.duckdb.path}")
    console.print(f"  Source: {source_path}")
    console.print(f"  Mode: {'Full Refresh' if full_refresh else 'Upsert'}")
    console.print()

    try:
        loader = DuckDBLoader(settings)

        if init_only:
            loader.create_database()
            console.print("[bold green]Database initialized successfully![/bold green]")
        else:
            rows = loader.load_from_parquet(
                source_path,
                full_refresh=full_refresh,
            )
            console.print(f"\n[bold green]Loading complete![/bold green]")
            console.print(f"Rows loaded/updated: {rows}")

        if stats:
            console.print("\n[bold]Table Statistics:[/bold]")
            table_stats = loader.get_table_stats()

            console.print(f"  Total rows: {table_stats['row_count']}")

            if table_stats.get("date_range"):
                dr = table_stats["date_range"]
                console.print(f"  Date range: {dr['min']} to {dr['max']} ({dr['count']} days)")

            if table_stats.get("type_distribution"):
                console.print("\n  Type distribution:")
                table = Table(show_header=True, header_style="bold cyan")
                table.add_column("Type")
                table.add_column("Count", justify="right")

                for t in table_stats["type_distribution"]:
                    table.add_row(str(t["type"]), str(t["count"]))

                console.print(table)

        loader.disconnect()

    except FileNotFoundError as e:
        console.print(f"[bold red]Source not found:[/bold red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[bold red]Loading failed:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def transform(
    models: str | None = typer.Option(
        None,
        "--models",
        "-m",
        help="Specific dbt models to run (comma-separated or dbt selector)",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Force full refresh of incremental models",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Run dbt transformations."""
    import subprocess

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    console.print("[bold blue]dbt Transformation[/bold blue]\n")
    console.print(f"  Project: {settings.dbt.project_dir}")
    console.print(f"  Target: {settings.dbt.target}")
    console.print(f"  Models: {models or 'all'}")
    console.print(f"  Mode: {'Full Refresh' if full_refresh else 'Incremental'}")
    console.print()

    # Build dbt run command
    cmd = [
        "dbt", "run",
        "--project-dir", str(settings.dbt.project_dir),
        "--profiles-dir", str(settings.dbt.profiles_dir),
        "--target", settings.dbt.target,
    ]

    if full_refresh:
        cmd.append("--full-refresh")

    if models:
        cmd.extend(["--select", models])

    try:
        console.print("[dim]Running dbt...[/dim]\n")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(settings.dbt.project_dir),
        )

        if result.stdout:
            console.print(result.stdout)

        if result.returncode != 0:
            console.print(f"[bold red]dbt run failed:[/bold red]")
            if result.stderr:
                console.print(result.stderr)
            raise typer.Exit(1)

        console.print("[bold green]dbt transformations complete![/bold green]")

    except FileNotFoundError:
        console.print("[bold red]Error: dbt not found. Is it installed?[/bold red]")
        raise typer.Exit(1)


@app.command()
def pipeline(
    full_backfill: bool = typer.Option(
        False,
        "--full-backfill",
        help="Run full historical backfill",
    ),
    full_refresh: bool = typer.Option(
        False,
        "--full-refresh",
        help="Rebuild all data and dbt models",
    ),
    skip_extract: bool = typer.Option(
        False,
        "--skip-extract",
        help="Skip extraction step",
    ),
    skip_load: bool = typer.Option(
        False,
        "--skip-load",
        help="Skip loading step",
    ),
    skip_transform: bool = typer.Option(
        False,
        "--skip-transform",
        help="Skip transformation step",
    ),
    use_prefect: bool = typer.Option(
        False,
        "--prefect",
        help="Run via Prefect orchestration (requires Prefect server)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Run the complete analytics pipeline (extract → load → transform)."""
    setup_logging("DEBUG" if verbose else "INFO")

    console.print("[bold blue]Claude Analytics Pipeline[/bold blue]\n")
    console.print(f"  Mode: {'Full Backfill' if full_backfill else 'Incremental'}")
    console.print(f"  Refresh: {'Full' if full_refresh else 'Incremental'}")
    console.print(f"  Steps: {'Prefect' if use_prefect else 'Direct'}")
    console.print()

    if use_prefect:
        from analytics.flows import analytics_pipeline as prefect_pipeline

        console.print("[dim]Running via Prefect...[/dim]\n")
        result = prefect_pipeline(
            full_backfill=full_backfill,
            full_refresh=full_refresh,
            skip_extract=skip_extract,
            skip_load=skip_load,
            skip_transform=skip_transform,
        )
        console.print(f"\n[bold green]Pipeline complete![/bold green]")
        console.print(f"Results: {result}")
    else:
        # Direct execution without Prefect
        from analytics.extractor import MongoExtractor
        from analytics.loader import DuckDBLoader

        settings = get_settings()

        # Step 1: Extract
        if not skip_extract:
            console.print("[bold cyan]Step 1: Extract[/bold cyan]")
            try:
                extractor = MongoExtractor(
                    mongo_uri=settings.mongo.uri,
                    mongo_db=settings.mongo.database,
                    collection_name=settings.mongo.collection,
                    output_dir=Path(settings.data.raw_dir),
                )

                if full_backfill:
                    stats = extractor.full_extract()
                else:
                    stats = extractor.incremental_extract()

                console.print(f"  [green]✓[/green] Extraction complete: {stats}")
                extractor.close()
            except Exception as e:
                console.print(f"  [red]✗[/red] Extraction failed: {e}")
                raise typer.Exit(1)
        else:
            console.print("[dim]Step 1: Extract (skipped)[/dim]")

        # Step 2: Load
        if not skip_load:
            console.print("[bold cyan]Step 2: Load[/bold cyan]")
            try:
                loader = DuckDBLoader(db_path=Path(settings.duckdb.path))
                loader.create_database()

                if full_refresh:
                    stats = loader.load_from_parquet(str(settings.data.raw_dir))
                else:
                    stats = loader.upsert_incremental(str(settings.data.raw_dir))

                console.print(f"  [green]✓[/green] Loading complete: {stats}")
                loader.close()
            except Exception as e:
                console.print(f"  [red]✗[/red] Loading failed: {e}")
                raise typer.Exit(1)
        else:
            console.print("[dim]Step 2: Load (skipped)[/dim]")

        # Step 3: Transform
        if not skip_transform:
            console.print("[bold cyan]Step 3: Transform[/bold cyan]")
            import subprocess

            cmd = [
                "dbt", "run",
                "--project-dir", str(settings.dbt.project_dir),
                "--profiles-dir", str(settings.dbt.profiles_dir),
                "--target", settings.dbt.target,
            ]

            if full_refresh:
                cmd.append("--full-refresh")

            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    cwd=str(settings.dbt.project_dir),
                )

                if result.returncode != 0:
                    console.print(f"  [red]✗[/red] dbt failed: {result.stderr}")
                    raise typer.Exit(1)

                console.print(f"  [green]✓[/green] Transformations complete")
            except FileNotFoundError:
                console.print(f"  [red]✗[/red] dbt not found")
                raise typer.Exit(1)
        else:
            console.print("[dim]Step 3: Transform (skipped)[/dim]")

        console.print("\n[bold green]Pipeline complete![/bold green]")


@app.command()
def validate() -> None:
    """Run Great Expectations data quality checks."""
    console.print("[yellow]Validate command not yet implemented[/yellow]")
    # TODO: Implement in Phase 12


if __name__ == "__main__":
    app()
