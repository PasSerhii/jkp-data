"""Tests for the JKP CLI entry point."""

import re
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from jkp.data import __version__
from jkp.data.cli import app
from jkp.data.config import BYPASS_CRSP, DAILY_DOWNLOAD_WORKERS, PRODUCTION_OUTPUT
from jkp.data.database_sources import CompustatSource

runner = CliRunner()


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text.

    Typer's Rich output inserts color codes that split option names
    (e.g. ``--reset`` becomes ``\\x1b[1;36m-\\x1b[0m\\x1b[1;36m-reset``),
    causing plain substring checks to fail in CI where a terminal is
    not detected.
    """
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


@pytest.mark.unit
class TestCliHelp:
    """Test that --help output works for all commands."""

    def test_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "JKP Factor Data generation pipeline" in result.output

    def test_build_help(self):
        result = runner.invoke(app, ["build", "--help"])
        assert result.exit_code == 0
        assert "--persistent-co" in _strip_ansi(result.output)
        assert "--start-date" in _strip_ansi(result.output)
        assert "--end-date" in _strip_ansi(result.output)
        assert "--compustat-sou" in _strip_ansi(result.output)
        assert "--daily-downloa" in _strip_ansi(result.output)
        assert "OUTPUT_DIR" in _strip_ansi(result.output)

    def test_portfolio_help(self):
        result = runner.invoke(app, ["portfolio", "--help"])
        assert result.exit_code == 0
        assert "factor portfolios" in result.output.lower()
        assert "OUTPUT_DIR" in _strip_ansi(result.output)

    def test_connect_help(self):
        result = runner.invoke(app, ["connect", "--help"])
        assert result.exit_code == 0
        assert "--reset" in _strip_ansi(result.output)


@pytest.mark.unit
class TestVersionFlag:
    """Test that --version prints the package version and exits."""

    def test_version_prints_package_version(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output

    def test_version_does_not_invoke_subcommand(self):
        # --version should short-circuit before any subcommand runs
        with patch("jkp.data.main.run_pipeline") as mock_run:
            result = runner.invoke(app, ["--version", "build", "/tmp/whatever"])
            assert result.exit_code == 0
            mock_run.assert_not_called()

    def test_version_attribute_resolves_from_metadata(self):
        # Catches the case where pyproject.toml's name field is renamed without
        # updating version("jkp-data") in __init__.py — the existing test would
        # silently pass against the fallback string in that scenario.
        assert isinstance(__version__, str)
        assert __version__ != "0.0.0+unknown"
        assert __version__.count(".") >= 2  # X.Y.Z minimum


@pytest.mark.unit
class TestBuildCommand:
    """Test the build command routes to run_pipeline correctly."""

    @patch("jkp.data.main.run_pipeline")
    def test_build_calls_run_pipeline(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path)])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=False,
            output_dir=tmp_path,
            bypass_crsp=BYPASS_CRSP,
            production_output=PRODUCTION_OUTPUT,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_persistent_connection(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--persistent-connection"])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=True,
            output_dir=tmp_path,
            bypass_crsp=BYPASS_CRSP,
            production_output=PRODUCTION_OUTPUT,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_persistent_connection_short(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "-p"])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=True,
            output_dir=tmp_path,
            bypass_crsp=BYPASS_CRSP,
            production_output=PRODUCTION_OUTPUT,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_bypass_crsp_explicit(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--bypass-crsp"])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=False,
            output_dir=tmp_path,
            bypass_crsp=True,
            production_output=PRODUCTION_OUTPUT,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_no_bypass_crsp_explicit(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--no-bypass-crsp"])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=False,
            output_dir=tmp_path,
            bypass_crsp=False,
            production_output=PRODUCTION_OUTPUT,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_no_production_explicit(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--no-production"])
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=False,
            output_dir=tmp_path,
            bypass_crsp=BYPASS_CRSP,
            production_output=False,
            start_date=None,
            end_date=None,
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_date_filter(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(
            app,
            [
                "build",
                str(tmp_path),
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-12-31",
            ],
        )
        assert result.exit_code == 0
        mock_run_pipeline.assert_called_once_with(
            persistent_connection=False,
            output_dir=tmp_path,
            bypass_crsp=BYPASS_CRSP,
            production_output=PRODUCTION_OUTPUT,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            compustat_source=CompustatSource.xpressfeed,
            metrics_interval_seconds=60.0,
            reuse_raw=False,
            daily_download_workers=DAILY_DOWNLOAD_WORKERS,
            keep_interim=False,
        )

    @patch("jkp.data.main.run_pipeline")
    def test_build_reuses_raw_downloads(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--reuse-raw"])
        assert result.exit_code == 0
        assert mock_run_pipeline.call_args.kwargs["reuse_raw"] is True

    @patch("jkp.data.main.run_pipeline")
    def test_build_daily_download_workers(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--daily-download-workers", "4"])
        assert result.exit_code == 0
        assert mock_run_pipeline.call_args.kwargs["daily_download_workers"] == 4

    @patch("jkp.data.main.run_pipeline")
    def test_build_keep_interim(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--keep-interim"])
        assert result.exit_code == 0
        assert mock_run_pipeline.call_args.kwargs["keep_interim"] is True

    def test_build_rejects_more_than_four_daily_download_workers(self, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--daily-download-workers", "5"])
        assert result.exit_code != 0

    @patch("jkp.data.main.run_pipeline")
    def test_build_wrds_source(self, mock_run_pipeline, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--compustat-source", "wrds"])
        assert result.exit_code == 0
        assert mock_run_pipeline.call_args.kwargs["compustat_source"] is CompustatSource.wrds

    def test_build_missing_output_dir(self):
        result = runner.invoke(app, ["build"])
        assert result.exit_code != 0

    def test_build_country_option_is_not_supported(self, tmp_path):
        result = runner.invoke(app, ["build", str(tmp_path), "--country", "ISR"])
        assert result.exit_code != 0


@pytest.mark.unit
class TestPortfolioCommand:
    """Test the portfolio command routes to run_portfolio correctly."""

    @patch("jkp.data.portfolio.run_portfolio")
    def test_portfolio_calls_run_portfolio(self, mock_run_portfolio, tmp_path):
        result = runner.invoke(app, ["portfolio", str(tmp_path)])
        assert result.exit_code == 0
        mock_run_portfolio.assert_called_once_with(
            output_format="parquet", output_dir=tmp_path, end_date=None
        )

    @patch("jkp.data.portfolio.run_portfolio")
    def test_portfolio_csv_format(self, mock_run_portfolio, tmp_path):
        result = runner.invoke(app, ["portfolio", str(tmp_path), "--output-format", "csv"])
        assert result.exit_code == 0
        mock_run_portfolio.assert_called_once_with(
            output_format="csv", output_dir=tmp_path, end_date=None
        )

    @patch("jkp.data.portfolio.run_portfolio")
    def test_portfolio_end_date(self, mock_run_portfolio, tmp_path):
        result = runner.invoke(app, ["portfolio", str(tmp_path), "--end-date", "2026-07-31"])
        assert result.exit_code == 0
        assert mock_run_portfolio.call_args.kwargs["end_date"] == date(2026, 7, 31)

    def test_portfolio_missing_output_dir(self):
        result = runner.invoke(app, ["portfolio"])
        assert result.exit_code != 0

    def test_portfolio_country_option_is_not_supported(self, tmp_path):
        result = runner.invoke(app, ["portfolio", str(tmp_path), "--country", "ISR"])
        assert result.exit_code != 0


@pytest.mark.unit
class TestConnectCommand:
    """Test the connect command routes to wrds_credentials correctly."""

    @patch("jkp.data.wrds_credentials.get_wrds_credentials")
    def test_connect_shows_username(self, mock_get_creds):
        mock_get_creds.return_value = MagicMock(username="testuser")
        result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0
        assert "testuser" in result.output
        mock_get_creds.assert_called_once()

    @patch("jkp.data.wrds_credentials.reset_credentials")
    def test_connect_reset(self, mock_reset):
        result = runner.invoke(app, ["connect", "--reset"])
        assert result.exit_code == 0
        mock_reset.assert_called_once_with(full_reset=True)
        assert "reset" in result.output.lower()

    @patch("jkp.data.wrds_credentials.reset_credentials")
    def test_connect_reset_short(self, mock_reset):
        result = runner.invoke(app, ["connect", "-r"])
        assert result.exit_code == 0
        mock_reset.assert_called_once_with(full_reset=True)
