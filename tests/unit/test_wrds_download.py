"""
Tests for WRDS download functionality.

This module tests the download_raw_data_tables function and its helper functions,
particularly the persistent connection feature that uses ATTACH instead of postgres_scan().
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest


class TestBuildProjection:
    """Tests for build_projection() function."""

    def test_no_special_columns(self):
        """When no special columns present, return simple wildcard."""
        from jkp.data.aux_functions import build_projection

        cols = ["date", "value", "name"]
        result = build_projection(cols)
        assert result == "*"

    def test_permno_column_cast(self):
        """permno column should be cast to BIGINT."""
        from jkp.data.aux_functions import build_projection

        cols = ["permno", "date", "ret"]
        result = build_projection(cols)
        assert "TRY_CAST(permno AS BIGINT) AS permno" in result
        assert result.startswith("* REPLACE (")

    def test_multiple_special_columns(self):
        """Multiple special columns should all be cast."""
        from jkp.data.aux_functions import build_projection

        cols = ["permno", "permco", "sic", "sich", "date"]
        result = build_projection(cols)
        assert "TRY_CAST(permno AS BIGINT) AS permno" in result
        assert "TRY_CAST(permco AS BIGINT) AS permco" in result
        assert "TRY_CAST(sic AS BIGINT) AS sic" in result
        assert "TRY_CAST(sich AS BIGINT) AS sich" in result

    def test_selected_columns_are_projected_in_contract_order(self):
        """A selected projection should omit every unused source column."""
        from jkp.data.aux_functions import build_projection

        result = build_projection(
            ["gvkey", "iid", "datadate", "unused"],
            ("gvkey", "iid", "datadate"),
        )

        assert result == '"gvkey", "iid", "datadate"'
        assert "unused" not in result

    def test_selected_columns_must_exist(self):
        """A source schema change should fail before a partial download starts."""
        from jkp.data.aux_functions import build_projection

        with pytest.raises(RuntimeError, match="missing required columns: trfd"):
            build_projection(["gvkey", "iid"], ("gvkey", "iid", "trfd"))


class TestGenWrdsConnectionInfo:
    """Tests for gen_wrds_connection_info() function."""

    def test_connection_string_format(self):
        """Connection string should have correct format."""
        from jkp.data.aux_functions import gen_wrds_connection_info

        result = gen_wrds_connection_info("testuser", "testpass")

        assert "host=wrds-pgdata.wharton.upenn.edu" in result
        assert "port=9737" in result
        assert "dbname=wrds" in result
        assert "user=testuser" in result
        assert "password=testpass" in result
        assert "sslmode=require" in result


class TestStatementTimeoutConnectionInfo:
    """Tests for applying the RDS session guardrail at connection startup."""

    def test_adds_encoded_option_to_uri(self):
        from jkp.data.aux_functions import with_pg_statement_timeout

        result = with_pg_statement_timeout("postgresql://example/db")
        assert result.endswith("?options=-c%20statement_timeout%3D300000")

    def test_preserves_existing_uri_query(self):
        from jkp.data.aux_functions import with_pg_statement_timeout

        result = with_pg_statement_timeout("postgresql://example/db?sslmode=require")
        assert "&options=-c%20statement_timeout%3D300000" in result

    def test_adds_option_to_keyword_dsn(self):
        from jkp.data.aux_functions import with_pg_statement_timeout

        result = with_pg_statement_timeout("host=example dbname=wrds")
        assert result.endswith(" options='-c statement_timeout=300000'")

    def test_does_not_replace_existing_options(self):
        from jkp.data.aux_functions import with_pg_statement_timeout

        original = "postgresql://example/db?options=-c%20statement_timeout%3D120000"
        assert with_pg_statement_timeout(original) == original


class TestDownloadRawDataTablesBranching:
    """Tests for download_raw_data_tables() branching logic.

    These tests verify that the correct download method is used based on
    the persistent_connection parameter.
    """

    @pytest.fixture
    def mock_duckdb(self):
        """Create a mock DuckDB connection."""
        from jkp.data.aux_functions import LARGE_COMPUSTAT_COLUMNS

        available_columns = sorted(
            {column for columns in LARGE_COMPUSTAT_COLUMNS.values() for column in columns}
        )
        with (
            patch("jkp.data.aux_functions.duckdb") as mock,
            patch(
                "jkp.data.aux_functions.load_security_pairs",
                return_value=[("001234", "01")],
            ),
        ):
            mock_conn = MagicMock()
            mock.connect.return_value = mock_conn
            mock_result = MagicMock()
            mock_result.description = [(column,) for column in available_columns]
            mock_conn.execute.return_value = mock_result
            yield mock, mock_conn

    def test_persistent_connection_false_uses_postgres_scan(self, mock_duckdb, test_paths):
        """When persistent_connection=False, should use postgres_scan()."""
        from jkp.data.aux_functions import download_raw_data_tables

        mock, mock_conn = mock_duckdb

        download_raw_data_tables(test_paths, "user", "pass", persistent_connection=False)

        executed_sql = [
            str(c[0][0])
            for c in mock_conn.execute.call_args_list
            if c[0] and isinstance(c[0][0], str)
        ]
        sql_joined = " ".join(executed_sql)

        assert "postgres_scan" in sql_joined
        # Ordinary tables retain postgres_scan. The compact full-history age
        # aggregate uses one temporary attached connection.
        assert "AS age_anchor_source" in sql_joined

    def test_persistent_connection_true_uses_attach(self, mock_duckdb, test_paths):
        """When persistent_connection=True, should use ATTACH."""
        from jkp.data.aux_functions import download_raw_data_tables

        mock, mock_conn = mock_duckdb

        download_raw_data_tables(test_paths, "user", "pass", persistent_connection=True)

        executed_sql = [
            str(c[0][0])
            for c in mock_conn.execute.call_args_list
            if c[0] and isinstance(c[0][0], str)
        ]
        sql_joined = " ".join(executed_sql)

        assert "ATTACH" in sql_joined
        assert "DETACH" in sql_joined
        assert "source_db." in sql_joined

    def test_persistent_connection_true_single_attach(self, mock_duckdb, test_paths):
        """Persistent connection should only ATTACH once for all tables."""
        from jkp.data.aux_functions import download_raw_data_tables

        mock, mock_conn = mock_duckdb

        download_raw_data_tables(test_paths, "user", "pass", persistent_connection=True)

        executed_sql = [
            str(c[0][0])
            for c in mock_conn.execute.call_args_list
            if c[0] and isinstance(c[0][0], str)
        ]

        attach_count = sum(1 for sql in executed_sql if "ATTACH" in sql and "DETACH" not in sql)
        detach_count = sum(1 for sql in executed_sql if "DETACH" in sql)

        assert attach_count == 1, f"Expected 1 ATTACH, got {attach_count}"
        assert detach_count == 1, f"Expected 1 DETACH, got {detach_count}"

    def test_connection_closed_after_download(self, mock_duckdb, test_paths):
        """Connection should be closed after download completes."""
        from jkp.data.aux_functions import download_raw_data_tables

        mock, mock_conn = mock_duckdb

        download_raw_data_tables(test_paths, "user", "pass", persistent_connection=False)
        mock_conn.close.assert_called_once()

        mock_conn.reset_mock()

        download_raw_data_tables(test_paths, "user", "pass", persistent_connection=True)
        mock_conn.close.assert_called_once()

    def test_persistent_connection_oom_retries_with_postgres_scan(self, mock_duckdb, test_paths):
        """Attached downloads that hit DuckDB OOM should retry with postgres_scan."""
        from jkp.data.aux_functions import download_raw_data_tables

        class OutOfMemoryException(Exception):
            pass

        attached_calls = 0

        def attached_side_effect(*args, **kwargs):
            nonlocal attached_calls
            attached_calls += 1
            if attached_calls == 1:
                raise OutOfMemoryException("allocation failure")
            return None

        with (
            patch(
                "jkp.data.aux_functions.download_wrds_table_attached",
                side_effect=attached_side_effect,
            ) as mock_attached,
            patch("jkp.data.aux_functions.download_wrds_table") as mock_postgres_scan,
        ):
            download_raw_data_tables(
                test_paths,
                "user",
                "pass",
                persistent_connection=True,
                bypass_crsp=True,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )

        assert mock_attached.called
        assert mock_postgres_scan.called
        assert mock_postgres_scan.call_args_list[0].args[2] == "comp.exrt_dly"


class TestGetColumnsAttached:
    """Tests for get_columns_attached() function."""

    def test_returns_column_names(self):
        """Should extract column names from query description."""
        from jkp.data.aux_functions import get_columns_attached

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.description = [("permno",), ("date",), ("ret",)]
        mock_conn.execute.return_value = mock_result

        result = get_columns_attached(mock_conn, "wrds", "crsp", "msf")

        assert result == ["permno", "date", "ret"]

    def test_queries_attached_database(self):
        """Should query the attached database with correct syntax."""
        from jkp.data.aux_functions import get_columns_attached

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.description = [("col1",)]
        mock_conn.execute.return_value = mock_result

        get_columns_attached(mock_conn, "mydb", "mylib", "mytable")

        call_args = mock_conn.execute.call_args[0][0]
        assert "mydb.mylib.mytable" in call_args
        assert "LIMIT 0" in call_args


class TestDownloadWrdsTableAttached:
    """Tests for download_wrds_table_attached() function."""

    def test_copies_to_parquet(self):
        """Should execute COPY TO parquet command."""
        from jkp.data.aux_functions import download_wrds_table_attached

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.description = [("col1",), ("col2",)]
        mock_conn.execute.return_value = mock_result

        download_wrds_table_attached(mock_conn, "wrds", "crsp.msf", "/tmp/test.parquet")

        copy_calls = [
            c for c in mock_conn.execute.call_args_list if c[0] and "COPY" in str(c[0][0])
        ]

        assert len(copy_calls) == 1, "Should have exactly one COPY command"
        copy_sql = copy_calls[0][0][0]
        assert "wrds.crsp.msf" in copy_sql
        assert "/tmp/test.parquet" in copy_sql
        assert "FORMAT PARQUET" in copy_sql

    def test_selected_columns_replace_wildcard(self):
        """Large-table downloads should transfer only their frozen contract."""
        from jkp.data.aux_functions import download_wrds_table_attached

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.description = [("gvkey",), ("iid",), ("datadate",), ("unused",)]
        mock_conn.execute.return_value = mock_result

        download_wrds_table_attached(
            mock_conn,
            "source",
            "comp.secm",
            "/tmp/secm.parquet",
            selected_columns=("gvkey", "iid", "datadate"),
        )

        copy_sql = [
            call.args[0] for call in mock_conn.execute.call_args_list if "COPY" in call.args[0]
        ][0]
        assert 'SELECT "gvkey", "iid", "datadate"' in copy_sql
        assert "unused" not in copy_sql


class TestDailyCompustatBatching:
    """Tests for pair-indexed daily view extraction."""

    def test_pair_clause_uses_scalar_predicates_and_dates(self):
        from jkp.data.aux_functions import _pair_where_clause

        result = _pair_where_clause(
            [("001234", "01"), ("005678", "02W")],
            "datadate",
            date(2000, 1, 1),
            date(2026, 6, 30),
        )

        assert "gvkey = '001234' AND iid = '01'" in result
        assert "gvkey = '005678' AND iid = '02W'" in result
        assert "datadate >= '2000-01-01'" in result
        assert "datadate <= '2026-06-30'" in result
        assert "IN (" not in result

    def test_download_writes_one_part_per_pair_batch(self, tmp_path):
        from jkp.data.aux_functions import _download_pair_batches

        conn = MagicMock()
        filename = str(tmp_path / "comp_secd.parquet")
        _download_pair_batches(
            conn,
            "source_db.comp.secd",
            '"gvkey", "iid", "datadate"',
            [("001234", "01"), ("005678", "02"), ("009999", "01")],
            filename,
            "datadate",
            date(2000, 1, 1),
            date(2026, 6, 30),
            batch_size=2,
        )

        copy_sql = [call.args[0] for call in conn.execute.call_args_list]
        assert len(copy_sql) == 2
        assert "part-000001.parquet" in copy_sql[0]
        assert "part-000002.parquet" in copy_sql[1]
        assert "009999" not in copy_sql[0]
        assert "009999" in copy_sql[1]
        assert (tmp_path / "comp_secd_parts").is_dir()
