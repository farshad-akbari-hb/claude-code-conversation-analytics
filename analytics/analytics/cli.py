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
    console.print()
    console.print("[bold]Iceberg Configuration[/bold]")
    console.print(f"[cyan]Warehouse Path:[/cyan] {settings.iceberg.warehouse_path}")
    console.print(f"[cyan]Catalog Name:[/cyan] {settings.iceberg.catalog_name}")
    console.print(f"[cyan]Namespace:[/cyan] {settings.iceberg.namespace}")
    console.print(f"[cyan]Table Name:[/cyan] {settings.iceberg.table_name}")


@app.command()
def extract(
    full_backfill: bool = typer.Option(
        False,
        "--full-backfill",
        help="Extract all historical data (ignores high water mark)",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Extract data from MongoDB to Iceberg table."""
    from analytics.extractor import IcebergExtractor

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    console.print("[bold blue]MongoDB Extraction[/bold blue]\n")
    console.print(f"  Source: {settings.mongo.uri}/{settings.mongo.db}.{settings.mongo.collection}")
    console.print(f"  Mode: {'Full Backfill' if full_backfill else 'Incremental'}")
    console.print(f"  Table: {settings.iceberg.full_table_name}")
    console.print()

    try:
        extractor = IcebergExtractor(settings)
        count = extractor.extract(full_backfill=full_backfill)

        console.print(f"\n[bold green]Extraction complete![/bold green]")
        console.print(f"Written {count} records to Iceberg table")

        # Show table info
        info = extractor.get_table_info()
        if info.get("snapshot_count"):
            console.print(f"  Snapshots: {info['snapshot_count']}")
        if info.get("summary", {}).get("added-records"):
            console.print(f"  Added records: {info['summary']['added-records']}")

    except Exception as e:
        console.print(f"[bold red]Extraction failed:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def load(
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
    """Load Iceberg table into DuckDB."""
    from analytics.loader import DuckDBLoader

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    iceberg_path = (
        settings.iceberg.warehouse_path
        / settings.iceberg.namespace
        / settings.iceberg.table_name
    )

    console.print("[bold blue]DuckDB Loading[/bold blue]\n")
    console.print(f"  Database: {settings.duckdb.path}")
    console.print(f"  Source: {iceberg_path}")
    console.print(f"  Mode: {'Full Refresh' if full_refresh else 'Upsert'}")
    console.print()

    try:
        loader = DuckDBLoader(settings)

        if init_only:
            loader.create_database()
            console.print("[bold green]Database initialized successfully![/bold green]")
        else:
            rows = loader.load(full_refresh=full_refresh)
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

    # Build dbt build command (runs seeds, models, and tests in DAG order)
    cmd = [
        "dbt", "build",
        "--project-dir", str(settings.dbt.project_dir),
        "--profiles-dir", str(settings.dbt.profiles_dir),
        "--target", settings.dbt.target,
        "--exclude", "test_type:data",  # Skip data tests for speed
    ]

    if full_refresh:
        cmd.append("--full-refresh")

    if models:
        cmd.extend(["--select", models])

    try:
        console.print("[dim]Running dbt build...[/dim]\n")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(settings.dbt.project_dir),
        )

        if result.stdout:
            console.print(result.stdout)

        if result.returncode != 0:
            console.print(f"[bold red]dbt build failed:[/bold red]")
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
        from analytics.extractor import IcebergExtractor
        from analytics.loader import DuckDBLoader

        settings = get_settings()

        # Step 1: Extract
        if not skip_extract:
            console.print("[bold cyan]Step 1: Extract[/bold cyan]")
            try:
                extractor = IcebergExtractor(settings)
                count = extractor.extract(full_backfill=full_backfill)
                console.print(f"  [green]✓[/green] Extraction complete: {count} records")
            except Exception as e:
                console.print(f"  [red]✗[/red] Extraction failed: {e}")
                raise typer.Exit(1)
        else:
            console.print("[dim]Step 1: Extract (skipped)[/dim]")

        # Step 2: Load
        if not skip_load:
            console.print("[bold cyan]Step 2: Load[/bold cyan]")
            try:
                loader = DuckDBLoader(settings)
                rows = loader.load(full_refresh=full_refresh)
                console.print(f"  [green]✓[/green] Loading complete: {rows} rows")
                loader.disconnect()
            except Exception as e:
                console.print(f"  [red]✗[/red] Loading failed: {e}")
                raise typer.Exit(1)
        else:
            console.print("[dim]Step 2: Load (skipped)[/dim]")

        # Step 3: Transform
        if not skip_transform:
            console.print("[bold cyan]Step 3: Transform[/bold cyan]")
            import subprocess

            # Use 'dbt build' which runs seeds, models, and tests in DAG order
            cmd = [
                "dbt", "build",
                "--project-dir", str(settings.dbt.project_dir),
                "--profiles-dir", str(settings.dbt.profiles_dir),
                "--target", settings.dbt.target,
                "--exclude", "test_type:data",  # Skip data tests for speed
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
def validate(
    bronze: bool = typer.Option(
        True,
        "--bronze/--no-bronze",
        help="Validate bronze (raw) layer",
    ),
    silver: bool = typer.Option(
        True,
        "--silver/--no-silver",
        help="Validate silver (intermediate) layer",
    ),
    checkpoint: str | None = typer.Option(
        None,
        "--checkpoint",
        "-c",
        help="Run a specific checkpoint by name",
    ),
    build_docs: bool = typer.Option(
        False,
        "--build-docs",
        help="Build data documentation after validation",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Run Great Expectations data quality checks."""
    from analytics.quality import DataQualityValidator

    setup_logging("DEBUG" if verbose else "INFO")

    console.print("[bold blue]Data Quality Validation[/bold blue]\n")

    validator = DataQualityValidator()

    if validator.context is None:
        console.print("[bold red]Great Expectations not available[/bold red]")
        console.print("Install with: pip install great-expectations")
        raise typer.Exit(1)

    results = {}

    if checkpoint:
        console.print(f"Running checkpoint: {checkpoint}")
        results["checkpoint"] = validator.run_checkpoint(checkpoint)
    else:
        if bronze:
            console.print("Validating bronze layer...")
            results["bronze"] = validator.validate_bronze()

        if silver:
            console.print("Validating silver layer...")
            results["silver"] = validator.validate_silver()

    # Display results
    console.print("\n[bold]Validation Results:[/bold]\n")

    all_success = True
    for layer, result in results.items():
        success = result.get("success", False)
        all_success = all_success and success

        status = "[green]✓ PASSED[/green]" if success else "[red]✗ FAILED[/red]"
        console.print(f"  {layer}: {status}")

        if result.get("statistics"):
            stats = result["statistics"]
            console.print(f"    Expectations: {stats.get('evaluated_expectations', 0)}")
            console.print(f"    Successful: {stats.get('successful_expectations', 0)}")
            console.print(f"    Unsuccessful: {stats.get('unsuccessful_expectations', 0)}")

        if result.get("error"):
            console.print(f"    [red]Error: {result['error']}[/red]")

    if build_docs:
        console.print("\nBuilding data documentation...")
        if validator.build_data_docs():
            docs_url = validator.get_data_docs_url()
            console.print(f"[green]Data docs built successfully[/green]")
            if docs_url:
                console.print(f"View at: {docs_url}")
        else:
            console.print("[yellow]Failed to build data docs[/yellow]")

    console.print()
    if all_success:
        console.print("[bold green]All validations passed![/bold green]")
    else:
        console.print("[bold red]Some validations failed[/bold red]")
        raise typer.Exit(1)


@app.command()
def iceberg(
    action: str = typer.Argument(
        "info",
        help="Action to perform: info, create, drop, snapshots",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Manage Iceberg table (info, create, drop, snapshots)."""
    from analytics.extractor import IcebergExtractor, IcebergCatalogManager

    setup_logging("DEBUG" if verbose else "INFO")
    settings = get_settings()

    console.print("[bold blue]Iceberg Table Management[/bold blue]\n")
    console.print(f"  Warehouse: {settings.iceberg.warehouse_path}")
    console.print(f"  Table: {settings.iceberg.full_table_name}")
    console.print()

    catalog_manager = IcebergCatalogManager(settings)

    if action == "info":
        extractor = IcebergExtractor(settings)
        info = extractor.get_table_info()

        if info.get("error"):
            console.print(f"[yellow]Table not found or error: {info['error']}[/yellow]")
        else:
            console.print("[bold]Table Information:[/bold]")
            console.print(f"  Location: {info.get('location', 'N/A')}")
            console.print(f"  Snapshots: {info.get('snapshot_count', 0)}")
            console.print(f"  Partition Spec: {info.get('partition_spec', 'N/A')}")

            if info.get("schema_fields"):
                console.print(f"  Fields: {', '.join(info['schema_fields'])}")

            if info.get("summary"):
                console.print("\n[bold]Current Snapshot Summary:[/bold]")
                for key, value in info["summary"].items():
                    console.print(f"  {key}: {value}")

    elif action == "create":
        try:
            table = catalog_manager.get_or_create_table()
            console.print(f"[green]✓[/green] Table created/verified: {settings.iceberg.full_table_name}")
            console.print(f"  Location: {table.location()}")
        except Exception as e:
            console.print(f"[red]✗[/red] Failed to create table: {e}")
            raise typer.Exit(1)

    elif action == "drop":
        confirm = typer.confirm(
            f"Are you sure you want to drop table '{settings.iceberg.full_table_name}'?"
        )
        if confirm:
            if catalog_manager.drop_table():
                console.print(f"[green]✓[/green] Table dropped: {settings.iceberg.full_table_name}")
            else:
                console.print("[yellow]Table does not exist[/yellow]")
        else:
            console.print("[dim]Operation cancelled[/dim]")

    elif action == "snapshots":
        try:
            table = catalog_manager.get_or_create_table()
            snapshots = list(table.snapshots())

            if not snapshots:
                console.print("[yellow]No snapshots found[/yellow]")
            else:
                console.print(f"[bold]Snapshots ({len(snapshots)}):[/bold]\n")

                from rich.table import Table as RichTable
                tbl = RichTable(show_header=True, header_style="bold cyan")
                tbl.add_column("Snapshot ID")
                tbl.add_column("Timestamp")
                tbl.add_column("Operation")
                tbl.add_column("Records")

                for snapshot in snapshots[-10:]:  # Show last 10
                    from datetime import datetime
                    ts = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
                    records = snapshot.summary.get("added-records", "N/A") if snapshot.summary else "N/A"
                    op = snapshot.summary.get("operation", "N/A") if snapshot.summary else "N/A"
                    tbl.add_row(
                        str(snapshot.snapshot_id),
                        ts.strftime("%Y-%m-%d %H:%M:%S"),
                        op,
                        str(records),
                    )

                console.print(tbl)

        except Exception as e:
            console.print(f"[red]✗[/red] Failed to list snapshots: {e}")
            raise typer.Exit(1)

    else:
        console.print(f"[red]Unknown action: {action}[/red]")
        console.print("Valid actions: info, create, drop, snapshots")
        raise typer.Exit(1)


@app.command()
def deploy(
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
) -> None:
    """Deploy all flows to Prefect server.

    Creates/updates the following deployments:
    - hourly-analytics: Incremental sync every hour
    - daily-full-refresh: Full refresh at 2 AM daily
    - adhoc-analytics: Manual trigger
    - full-backfill: Manual full historical backfill
    """
    setup_logging("DEBUG" if verbose else "INFO")

    console.print("[bold blue]Deploying Flows to Prefect[/bold blue]\n")

    try:
        from analytics.flows.deployment import apply_deployments

        apply_deployments()
        console.print("\n[bold green]All deployments applied successfully![/bold green]")
        console.print("\nView deployments at: http://localhost:4200/deployments")

    except Exception as e:
        console.print(f"[bold red]Deployment failed:[/bold red] {e}")
        console.print("\nMake sure Prefect server is running:")
        console.print("  docker-compose -f docker-compose.analytics.yml up -d prefect-server")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
