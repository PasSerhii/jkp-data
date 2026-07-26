"""JKP Data CLI - Factor data generation pipeline."""

from datetime import date
from enum import StrEnum
from pathlib import Path

import typer

from . import __version__
from .database_sources import CompustatSource


class OutputFormat(StrEnum):
    """Supported output file formats."""

    parquet = "parquet"
    csv = "csv"


app = typer.Typer(
    name="jkp",
    help="JKP Factor Data generation pipeline.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


def _parse_iso_date(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise typer.BadParameter(f"{option_name} must be an ISO date like 2024-12-31") from None


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the package version and exit.",
    ),
) -> None:
    """JKP Factor Data generation pipeline."""


@app.command()
def build(
    output_dir: Path = typer.Argument(
        help="Directory for pipeline output (raw, interim, and processed data).",
    ),
    persistent_connection: bool = typer.Option(
        False,
        "--persistent-connection",
        "-p",
        help="Use one persistent PostgreSQL connection for all source downloads.",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing data in output directory without prompting.",
    ),
    reuse_raw: bool = typer.Option(
        False,
        "--reuse-raw",
        help="Validate and reuse existing raw Parquets, skipping source downloads.",
    ),
    bypass_crsp: bool | None = typer.Option(
        None,
        "--bypass-crsp/--no-bypass-crsp",
        help="Build from Compustat only, skipping all CRSP download/processing. "
        "Defaults to config.BYPASS_CRSP when not specified.",
    ),
    production_output: bool | None = typer.Option(
        None,
        "--production/--no-production",
        help="Also emit the alpha-beta production CSVs (per-country monthly + daily). "
        "Defaults to config.PRODUCTION_OUTPUT when not specified.",
    ),
    compustat_corrections: bool | None = typer.Option(
        None,
        "--compustat-corrections/--no-compustat-corrections",
        help="Opt into decimal-shift repairs and unreliable-history filtering. "
        "Disabled by default to preserve WRDS/SAS source-cell parity.",
    ),
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help="Earliest database source date to download, as YYYY-MM-DD. "
        "Defaults to config.ACCOUNTING_START_DATE.",
    ),
    end_date: str | None = typer.Option(
        None,
        "--end-date",
        help="Latest database source date to download, as YYYY-MM-DD. Defaults to config.END_DATE.",
    ),
    compustat_source: CompustatSource = typer.Option(
        CompustatSource.xpressfeed,
        "--compustat-source",
        help="Compustat source: XpressFeed RDS (default) or WRDS for regression runs.",
    ),
    metrics_interval_seconds: float = typer.Option(
        60.0,
        "--metrics-interval",
        min=1.0,
        help="Seconds between persistent CPU, memory, disk, I/O, and network samples.",
    ),
    daily_download_workers: int = typer.Option(
        2,
        "--daily-download-workers",
        min=1,
        max=4,
        help="Shared parallel workers for indexed SECD/G_SECD batch downloads.",
    ),
) -> None:
    """Run the full data generation pipeline."""
    from .config import APPLY_COMPUSTAT_CORRECTIONS, BYPASS_CRSP, PRODUCTION_OUTPUT
    from .main import run_pipeline

    if not force and output_dir.exists() and any(output_dir.iterdir()):
        typer.confirm(
            f"Output directory '{output_dir}' already contains data. Overwrite?",
            abort=True,
        )

    run_pipeline(
        persistent_connection=persistent_connection,
        output_dir=output_dir,
        bypass_crsp=BYPASS_CRSP if bypass_crsp is None else bypass_crsp,
        production_output=PRODUCTION_OUTPUT if production_output is None else production_output,
        apply_compustat_corrections=(
            APPLY_COMPUSTAT_CORRECTIONS if compustat_corrections is None else compustat_corrections
        ),
        start_date=_parse_iso_date(start_date, "--start-date"),
        end_date=_parse_iso_date(end_date, "--end-date"),
        compustat_source=compustat_source,
        metrics_interval_seconds=metrics_interval_seconds,
        reuse_raw=reuse_raw,
        daily_download_workers=daily_download_workers,
    )


@app.command()
def portfolio(
    output_dir: Path = typer.Argument(
        help="Directory containing pipeline output (must match output_dir from build).",
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.parquet,
        "--output-format",
        help="Output file format.",
    ),
    end_date: str | None = typer.Option(
        None,
        "--end-date",
        help="Latest portfolio output date, as YYYY-MM-DD. Defaults to config.END_DATE.",
    ),
) -> None:
    """Generate factor portfolios from characteristics data."""
    from .portfolio import run_portfolio

    run_portfolio(
        output_format=output_format.value,
        output_dir=output_dir,
        end_date=_parse_iso_date(end_date, "--end-date"),
    )


# This help text is the canonical description of the credential precedence
# order; the README, the wrds_credentials module docstring, and the tests point
# here rather than restating it. Update here first.
@app.command()
def connect(
    reset: bool = typer.Option(
        False,
        "--reset",
        "-r",
        help="Reset stored WRDS credentials.",
    ),
) -> None:
    """Test or configure the WRDS connection.

    Credential precedence (highest first):

      1. WRDS_USERNAME and WRDS_PASSWORD environment variables. Useful for
         containers and shared service accounts.
      2. The system keyring (Keychain on macOS, Secret Service on Linux desktop,
         Credential Vault on Windows). Default for interactive sessions.
      3. The file-backed keyring (keyrings.alt.file.PlaintextKeyring), which
         stores the password in a mode-600 file under
         ~/.local/share/python_keyring/. Selected only when
         JKP_ALLOW_PLAINTEXT_KEYRING is set to exactly "1" ("true", "yes", etc.
         are treated as not set); appropriate for headless environments (HPC
         compute nodes, minimal Docker images) where no system keyring daemon
         is available. A warning is emitted on first use (once per process) so
         the backend change is never silent.
    """
    from .wrds_credentials import get_wrds_credentials, reset_credentials

    if reset:
        reset_credentials(full_reset=True)
        typer.echo("Credentials reset.")
        return

    creds = get_wrds_credentials()
    typer.echo(f"Connected as: {creds.username}")


if __name__ == "__main__":
    app()
