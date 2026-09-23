"""JKP Data CLI - Factor data generation pipeline."""

from datetime import date
from enum import StrEnum
from pathlib import Path

import typer

from . import __version__
from .config import MAX_DAILY_COMPUSTAT_DOWNLOAD_WORKERS
from .database_sources import CompustatSource


class OutputFormat(StrEnum):
    """Supported output file formats."""

    parquet = "parquet"
    csv = "csv"


app = typer.Typer(
    name="jkp",
    help="JKP Factor Data generation pipeline.",
    no_args_is_help=True,
    # Defense in depth: never render frame locals in tracebacks. A WRDS
    # Credentials object bound in a command frame would otherwise be exposed to
    # Typer's pretty exception handler. Typer only defaulted this off in 0.23.0
    # and the project floor is typer>=0.15.0, so set it explicitly.
    pretty_exceptions_show_locals=False,
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
    start_date: str | None = typer.Option(
        None,
        "--start-date",
        help="Earliest database source date to download, as YYYY-MM-DD. "
        "Defaults to config.ROLLING_INPUT_YEARS (23) before the end date, which is "
        "the shortest window that keeps every characteristic valid. A shorter "
        "explicit window nulls the 240-month seasonality characteristics.",
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
    daily_download_workers: int | None = typer.Option(
        None,
        "--daily-download-workers",
        min=1,
        max=MAX_DAILY_COMPUSTAT_DOWNLOAD_WORKERS,
        help="Shared parallel workers for indexed SECD/G_SECD batch downloads. "
        "Defaults to config.DAILY_DOWNLOAD_WORKERS when not specified.",
    ),
    production_years: int | None = typer.Option(
        None,
        "--production-years",
        min=0,
        help="Years of history the per-country production CSVs carry, counted back "
        "from the end date. 0 emits everything, which a downstream re-seed needs. "
        "Defaults to config.PRODUCTION_OUTPUT_YEARS. Output only: characteristics "
        "are always computed over the full source window, so this changes no value.",
    ),
    full_history: bool = typer.Option(
        False,
        "--full-history",
        help="Download complete source history (config.ACCOUNTING_START_DATE) instead of "
        "the rolling window. Use when re-seeding a downstream store, or reissuing after "
        "a change that rewrites historical values. Cannot be combined with --start-date.",
    ),
    keep_interim: bool = typer.Option(
        False,
        "--keep-interim",
        help="Retain interim/ and raw/ after the run instead of deleting them. "
        "Needed to inspect or test against intermediate artefacts; costs several "
        "hundred GB of disk.",
    ),
    db_update: bool = typer.Option(
        False,
        "--db-update/--no-db-update",
        help="After the run, upload the production CSVs to the research MSSQL database "
        "(incremental delete-and-insert; target comes from the RESEARCH_UPDATE "
        "environment variable). Requires production output. Default off.",
    ),
) -> None:
    """Run the full data generation pipeline."""
    from .config import (
        BYPASS_CRSP,
        DAILY_DOWNLOAD_WORKERS,
        PRODUCTION_OUTPUT,
        PRODUCTION_OUTPUT_YEARS,
    )
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
        start_date=_parse_iso_date(start_date, "--start-date"),
        end_date=_parse_iso_date(end_date, "--end-date"),
        compustat_source=compustat_source,
        metrics_interval_seconds=metrics_interval_seconds,
        reuse_raw=reuse_raw,
        daily_download_workers=(
            DAILY_DOWNLOAD_WORKERS if daily_download_workers is None else daily_download_workers
        ),
        keep_interim=keep_interim,
        full_history=full_history,
        production_years=(
            PRODUCTION_OUTPUT_YEARS if production_years is None else production_years
        ),
        db_update=db_update,
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
    """Verify the WRDS connection, or configure/reset stored credentials.

    Opens a real WRDS connection (attaches the database read-only and runs a
    trivial query), so a successful run confirms credentials, connectivity,
    and MFA all actually work. A password entered at the prompt is verified
    before it is stored, so a typo is never persisted and simply re-prompts on
    the next run. This may trigger a WRDS Duo MFA push: WRDS trusts a
    username/IP pair for 30 days after a successful approval, so expect one on the
    first run from a new IP and again once that window expires.

    Credential precedence (highest first):

      1. WRDS_USERNAME and WRDS_PASSWORD environment variables. Useful for
         containers, CI, and shared service accounts.
      2. The system keyring (Keychain on macOS, Secret Service on Linux desktop,
         Credential Vault on Windows). Default for interactive desktop sessions.
      3. A libpq password file: $PGPASSFILE, else ~/.pgpass
         (%APPDATA%\\postgresql\\pgpass.conf on Windows). The standard
         Postgres/WRDS mechanism, ideal for headless HPC nodes — jkp omits the
         password from the connection string and libpq reads the file.

    Running `jkp connect` stores the password in the system keyring where one is
    available. On a headless login node (no keyring, but an interactive
    terminal) it writes ~/.pgpass (mode 600) instead; because $HOME is shared
    with the compute nodes, batch jobs then read it without any further setup.
    The selected source is printed to stderr on every run.
    """
    from .wrds_credentials import get_wrds_credentials, reset_credentials

    try:
        if reset:
            reset_credentials(full_reset=True)
            typer.echo("Credentials reset.")
            return

        # Imported before resolving credentials, not after: this is the process's
        # first `duckdb` import, and a broken native wheel (GLIBC mismatch on an HPC
        # node) raises ImportError, which is not in the except tuple below and so
        # escapes to Typer's pretty-exception handler. That handler prints frame
        # locals on typer < 0.23.0, and pyproject allows typer>=0.15.0 — so no
        # plaintext password may be bound in this frame when the import runs.
        from .wrds_connection import verify_wrds_connection

        # Injected rather than called on the result: on the freshly-prompted path the
        # check has to run between the prompt and the store, which is inside
        # get_wrds_credentials. It verifies every resolution path exactly once, so
        # verifying again here would open a second connection (and risk a second Duo
        # push). Passing the function also keeps the plaintext password out of this
        # frame on the prompt path.
        creds = get_wrds_credentials(verify=verify_wrds_connection)
        typer.echo(f"Connected as: {creds.username}")
    except (RuntimeError, ValueError, OSError) as exc:
        # Anticipated, actionable failures from credential resolution and connection
        # verification: RuntimeError (no/empty username, unreadable ~/.pgpass, a failed
        # WRDS attach), ValueError (e.g. a password containing a newline), and OSError
        # (e.g. an unwritable state dir when persisting the username / writing ~/.pgpass).
        # Their messages are password-free; surface the message and exit non-zero rather
        # than dumping a traceback.
        typer.echo(str(exc), err=True)
        # Still worth pointing at: a password typed at the prompt is now verified
        # before it is stored, but one that arrived from an env var, an entry
        # predating this check, or a hand-edited ~/.pgpass can still be wrong, and
        # resolution never re-prompts while a stored value exists. Kept at the CLI
        # layer because the wrds_connection messages are shared with pipeline worker
        # paths, where --reset is not the right advice.
        typer.echo(
            "If a stored password is wrong, run `jkp connect --reset` and re-enter credentials.",
            err=True,
        )
        raise typer.Exit(1) from exc


if __name__ == "__main__":
    app()
