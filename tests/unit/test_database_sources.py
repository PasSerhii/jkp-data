"""Tests for XpressFeed connection resolution and source routing."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest

from jkp.data.database_sources import get_xpressfeed_connection_info


@pytest.mark.unit
def test_xpressfeed_url_is_duckdb_compatible_and_guarded(monkeypatch):
    monkeypatch.setenv(
        "COMPUSTAT",
        "postgresql+psycopg2://pipeline:secret@example.test:5432/compustat",
    )

    result = get_xpressfeed_connection_info()
    parsed = urlsplit(result)
    query = parse_qs(parsed.query)

    assert parsed.scheme == "postgresql"
    assert parsed.hostname == "example.test"
    assert parsed.path == "/compustat"
    assert query["sslmode"] == ["require"]
    assert query["connect_timeout"] == ["15"]
    assert query["application_name"] == ["jkp-data"]
    assert query["options"] == ["-c statement_timeout=300000"]


@pytest.mark.unit
def test_environment_takes_precedence_over_dotenv(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "COMPUSTAT=postgresql://file_user:file_pw@file.test/file_db\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("COMPUSTAT", "postgresql://env_user:env_pw@env.test/env_db")

    result = get_xpressfeed_connection_info(dotenv_path=dotenv)

    assert urlsplit(result).hostname == "env.test"


@pytest.mark.unit
def test_reads_repository_style_dotenv_without_mutating_environment(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        'COMPUSTAT="postgresql+psycopg2://user:pw@rds.test/compustat"\n',
        encoding="utf-8",
    )
    monkeypatch.delenv("COMPUSTAT", raising=False)

    result = get_xpressfeed_connection_info(dotenv_path=dotenv)

    assert urlsplit(result).hostname == "rds.test"
    assert "COMPUSTAT" not in __import__("os").environ


@pytest.mark.unit
def test_missing_xpressfeed_setting_has_actionable_error(monkeypatch, tmp_path):
    monkeypatch.delenv("COMPUSTAT", raising=False)

    with (
        pytest.raises(RuntimeError, match="COMPUSTAT is not set"),
        patch("jkp.data.database_sources._find_dotenv", return_value=None),
    ):
        get_xpressfeed_connection_info(dotenv_path=Path(tmp_path / "missing.env"))


@pytest.mark.unit
def test_run_pipeline_xpressfeed_does_not_request_wrds_credentials(tmp_path):
    from jkp.data.main import run_pipeline

    with (
        patch("jkp.data.main.get_xpressfeed_connection_info", return_value="postgresql://rds"),
        patch("jkp.data.main.get_wrds_credentials") as wrds_credentials,
        patch("jkp.data.main.setup_folder_structure", side_effect=RuntimeError("stop")),
        pytest.raises(RuntimeError, match="stop"),
    ):
        run_pipeline(output_dir=tmp_path, bypass_crsp=True, compustat_source="xpressfeed")

    wrds_credentials.assert_not_called()


@pytest.mark.unit
def test_run_pipeline_rejects_crsp_with_xpressfeed_before_downloading(tmp_path):
    from jkp.data.main import run_pipeline

    with (
        patch("jkp.data.main.get_xpressfeed_connection_info") as xpressfeed_connection,
        patch("jkp.data.main.download_raw_data_tables") as download,
        pytest.raises(ValueError, match="not CRSP"),
    ):
        run_pipeline(output_dir=tmp_path, bypass_crsp=False, compustat_source="xpressfeed")

    xpressfeed_connection.assert_not_called()
    download.assert_not_called()


@pytest.mark.unit
def test_download_error_does_not_expose_connection_url(test_paths):
    from jkp.data.aux_functions import download_raw_data_tables

    secret_url = "postgresql://private_user:private_password@rds.test/compustat"
    with (
        patch("jkp.data.aux_functions.duckdb") as duckdb_mock,
        patch(
            "jkp.data.aux_functions.download_wrds_table",
            side_effect=RuntimeError(f"could not connect using {secret_url}"),
        ),
        pytest.raises(RuntimeError) as error,
    ):
        duckdb_mock.connect.return_value = MagicMock()
        download_raw_data_tables(
            test_paths,
            bypass_crsp=True,
            connection_info=secret_url,
            source_label="XpressFeed RDS",
        )

    assert "private_password" not in str(error.value)
    assert secret_url not in str(error.value)
    assert "comp.exrt_dly" in str(error.value)
