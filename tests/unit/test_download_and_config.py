"""
Tests for WRDS download functions, config module, and filter functions.

These tests cover:
- download_wrds_table: WHERE clause generation for date-filtered downloads
- download_raw_data_tables: date_columns mapping passed correctly to download_wrds_table
- save_main_data: me_lag1 computation and output (no filtering, no end_date parameter)
- filter_dsf, filter_msf, filter_world: MAIN_FILTERS screening
- config: END_DATE and MAIN_FILTERS constants

Paper Reference: Jensen, Kelly, Pedersen (2023), "Is There a Replication Crisis in Finance?"
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from jkp.data.config import (
    ACCOUNTING_START_DATE,
    COLLECT_CHUNK_SIZE,
    END_DATE,
    MAIN_FILTERS,
    PORTFOLIO_BP_MIN_N,
    PORTFOLIO_CHARS,
    PORTFOLIO_PFS,
    PORTFOLIO_SETTINGS,
    REGIONAL_COUNTRIES_MIN,
    REGIONAL_COUNTRY_EXCL,
    REGIONAL_MONTHS_MIN,
    REGIONAL_STOCKS_MIN,
    ROLLING_DAILY_SPECS,
    START_DATE,
)

# =============================================================================
# Tests: config
# =============================================================================


class TestConfig:
    """Tests for the config module constants."""

    def test_end_date_is_date(self):
        """END_DATE should be a datetime.date instance."""
        assert isinstance(END_DATE, date), f"END_DATE should be datetime.date, got {type(END_DATE)}"

    def test_end_date_is_month_end(self):
        """END_DATE should fall on the last day of its month."""
        next_day = date(END_DATE.year + (END_DATE.month // 12), END_DATE.month % 12 + 1, 1)
        last_day = (next_day - __import__("datetime").timedelta(days=1)).day
        assert END_DATE.day == last_day, (
            f"END_DATE ({END_DATE}) is not the last day of its month (expected day {last_day})"
        )

    def test_main_filters_is_dict(self):
        """MAIN_FILTERS should be a dict."""
        assert isinstance(MAIN_FILTERS, dict), (
            f"MAIN_FILTERS should be a dict, got {type(MAIN_FILTERS)}"
        )

    def test_main_filters_has_expected_keys(self):
        """MAIN_FILTERS should contain the four standard screening columns."""
        expected = {"primary_sec", "common", "obs_main", "exch_main"}
        assert set(MAIN_FILTERS.keys()) == expected, (
            f"MAIN_FILTERS keys should be {expected}, got {set(MAIN_FILTERS.keys())}"
        )

    def test_main_filters_values_are_one(self):
        """All MAIN_FILTERS values should be 1 (the passing value)."""
        for k, v in MAIN_FILTERS.items():
            assert v == 1, f"MAIN_FILTERS['{k}'] should be 1, got {v}"

    def test_accounting_start_date_is_polars_expr(self):
        """ACCOUNTING_START_DATE should be a Polars expression (consumed inline)."""
        assert isinstance(ACCOUNTING_START_DATE, pl.Expr), (
            f"ACCOUNTING_START_DATE should be pl.Expr, got {type(ACCOUNTING_START_DATE)}"
        )

    def test_accounting_start_date_value(self):
        """ACCOUNTING_START_DATE should evaluate to 1949-12-31."""
        evaluated = pl.select(ACCOUNTING_START_DATE).item()
        assert evaluated.date() == date(1949, 12, 31), (
            f"ACCOUNTING_START_DATE should be 1949-12-31, got {evaluated}"
        )

    def test_start_date_matches_modified_sas_update_mode(self):
        """START_DATE should mirror the modified SAS update_mode=1 start_date."""
        assert date(2000, 1, 1) == START_DATE

    def test_collect_chunk_size_is_positive_int(self):
        """COLLECT_CHUNK_SIZE must be a positive int (used as a slice step)."""
        assert isinstance(COLLECT_CHUNK_SIZE, int) and COLLECT_CHUNK_SIZE > 0, (
            f"COLLECT_CHUNK_SIZE should be a positive int, got {COLLECT_CHUNK_SIZE!r}"
        )

    def test_portfolio_pfs_value(self):
        """PORTFOLIO_PFS pins the portfolio sort count (tertiles by default)."""
        assert PORTFOLIO_PFS == 3, f"PORTFOLIO_PFS expected 3, got {PORTFOLIO_PFS}"

    def test_portfolio_bp_min_n_value(self):
        """PORTFOLIO_BP_MIN_N pins the per-(industry, month) breakpoint min."""
        assert PORTFOLIO_BP_MIN_N == 10, f"PORTFOLIO_BP_MIN_N expected 10, got {PORTFOLIO_BP_MIN_N}"

    def test_regional_stocks_min_value(self):
        """REGIONAL_STOCKS_MIN pins the per-country-month minimum."""
        assert REGIONAL_STOCKS_MIN == 5, (
            f"REGIONAL_STOCKS_MIN expected 5, got {REGIONAL_STOCKS_MIN}"
        )

    def test_regional_months_min_value(self):
        """REGIONAL_MONTHS_MIN pins the 5-year history minimum."""
        assert REGIONAL_MONTHS_MIN == 60, (
            f"REGIONAL_MONTHS_MIN expected 60 months, got {REGIONAL_MONTHS_MIN}"
        )

    def test_regional_countries_min_value(self):
        """REGIONAL_COUNTRIES_MIN pins the regional aggregation minimum."""
        assert REGIONAL_COUNTRIES_MIN == 3, (
            f"REGIONAL_COUNTRIES_MIN expected 3, got {REGIONAL_COUNTRIES_MIN}"
        )

    def test_regional_country_excl_is_immutable(self):
        """REGIONAL_COUNTRY_EXCL must be a tuple to prevent accidental mutation."""
        assert isinstance(REGIONAL_COUNTRY_EXCL, tuple), (
            f"REGIONAL_COUNTRY_EXCL should be a tuple, got {type(REGIONAL_COUNTRY_EXCL)}"
        )

    def test_regional_country_excl_value(self):
        """REGIONAL_COUNTRY_EXCL pins the excluded ISO-3 codes."""
        assert REGIONAL_COUNTRY_EXCL == ("ZWE", "VEN"), (
            f"REGIONAL_COUNTRY_EXCL expected ('ZWE', 'VEN'), got {REGIONAL_COUNTRY_EXCL}"
        )

    def test_portfolio_chars_is_unique_nonempty_str_list(self):
        """PORTFOLIO_CHARS must be a list of unique non-empty strings."""
        assert isinstance(PORTFOLIO_CHARS, list), (
            f"PORTFOLIO_CHARS should be a list, got {type(PORTFOLIO_CHARS)}"
        )
        assert PORTFOLIO_CHARS, "PORTFOLIO_CHARS is empty"
        assert all(isinstance(c, str) and c for c in PORTFOLIO_CHARS), (
            "PORTFOLIO_CHARS entries must be non-empty strings"
        )
        assert len(PORTFOLIO_CHARS) == len(set(PORTFOLIO_CHARS)), (
            "PORTFOLIO_CHARS contains duplicates"
        )

    def test_portfolio_chars_known_anchors(self):
        """A handful of canonical characteristics must be present (typo guard)."""
        anchors = {"market_equity", "ret_12_1", "be_me", "ivol_capm_21d", "qmj"}
        missing = anchors - set(PORTFOLIO_CHARS)
        assert not missing, f"PORTFOLIO_CHARS missing anchors: {sorted(missing)}"

    def test_portfolio_settings_top_level_shape(self):
        """PORTFOLIO_SETTINGS must carry the keys `portfolios()` reads at runtime."""
        required = {
            "end_date",
            "pfs",
            "source",
            "wins_ret",
            "bps",
            "bp_min_n",
            "cmp",
            "signals",
            "regional_pfs",
            "daily_pf",
            "ind_pf",
        }
        assert isinstance(PORTFOLIO_SETTINGS, dict)
        missing = required - set(PORTFOLIO_SETTINGS)
        assert not missing, f"PORTFOLIO_SETTINGS missing keys: {sorted(missing)}"

    def test_portfolio_settings_nested_keys(self):
        """Nested `cmp`/`signals`/`regional_pfs` blocks must keep their schema."""
        assert set(PORTFOLIO_SETTINGS["cmp"]) == {"us", "int"}
        assert set(PORTFOLIO_SETTINGS["signals"]) == {"us", "int", "standardize", "weight"}
        assert set(PORTFOLIO_SETTINGS["regional_pfs"]) == {
            "country_excl",
            "country_weights",
            "stocks_min",
            "months_min",
            "countries_min",
        }
        assert PORTFOLIO_SETTINGS["regional_pfs"]["country_excl"] == list(REGIONAL_COUNTRY_EXCL)
        assert PORTFOLIO_SETTINGS["pfs"] == PORTFOLIO_PFS
        assert PORTFOLIO_SETTINGS["bp_min_n"] == PORTFOLIO_BP_MIN_N


class TestRollingDailySpecs:
    """Integrity checks for ROLLING_DAILY_SPECS."""

    KNOWN_SUFFIXES = {"_21d", "_126d", "_252d", "_1260d"}

    def test_is_nonempty_list(self):
        assert isinstance(ROLLING_DAILY_SPECS, list) and ROLLING_DAILY_SPECS

    def test_suffixes_are_known(self):
        """Every sfx must be in the set gen_aux_maps recognizes natively."""
        sfxs = [sfx for sfx, _, _ in ROLLING_DAILY_SPECS]
        unknown = set(sfxs) - self.KNOWN_SUFFIXES
        assert not unknown, f"Unknown suffixes: {sorted(unknown)}"

    def test_suffixes_are_unique(self):
        sfxs = [sfx for sfx, _, _ in ROLLING_DAILY_SPECS]
        assert len(sfxs) == len(set(sfxs)), f"Duplicate suffixes: {sfxs}"

    def test_min_obs_positive_int(self):
        for sfx, min_obs, _ in ROLLING_DAILY_SPECS:
            assert isinstance(min_obs, int) and min_obs > 0, (
                f"{sfx}: min_obs must be positive int, got {min_obs!r}"
            )

    def test_variables_are_nonempty_str_lists(self):
        for sfx, _, vars_ in ROLLING_DAILY_SPECS:
            assert isinstance(vars_, list) and vars_, f"{sfx}: variables list is empty"
            assert all(isinstance(v, str) and v for v in vars_), (
                f"{sfx}: variables must be non-empty strings, got {vars_!r}"
            )


# =============================================================================
# Tests: download_wrds_table
# =============================================================================


class TestDownloadWrdsTable:
    """Tests for download_wrds_table().

    This function downloads a WRDS table via DuckDB postgres_scan, optionally
    filtering rows by a date column. Tests mock DuckDB and get_columns to
    verify the SQL query is constructed correctly.
    """

    @pytest.fixture(autouse=True)
    def _patch_helpers(self):
        """Patch get_columns and build_projection for all tests."""
        with (
            patch("jkp.data.aux_functions.get_columns", return_value=["col_a", "col_b"]),
            patch("jkp.data.aux_functions.build_projection", return_value="*"),
        ):
            yield

    def _run(
        self,
        date_column: str | None = None,
        end_date: date | None = None,
        country_filter: tuple[str, ...] | None = None,
        table_name: str = "comp.funda",
    ) -> str:
        """Call download_wrds_table with a mock conn and return the executed SQL."""
        from jkp.data.aux_functions import download_wrds_table

        mock_conn = MagicMock()
        download_wrds_table(
            conninfo="host=test",
            duckdb_conn=mock_conn,
            table_name=table_name,
            filename="out.parquet",
            date_column=date_column,
            end_date=end_date,
            country_filter=country_filter,
        )
        return "\n".join(
            str(c.args[0]) for c in mock_conn.execute.call_args_list if c.args
        )

    def test_no_date_filter_when_params_absent(self):
        """SQL should have no WHERE clause when date_column and end_date are None."""
        sql = self._run()
        assert "WHERE" not in sql, f"Unexpected WHERE clause in SQL: {sql}"

    def test_no_date_filter_when_only_date_column(self):
        """SQL should have no WHERE clause when only date_column is provided."""
        sql = self._run(date_column="datadate")
        assert "WHERE" not in sql, f"Unexpected WHERE clause in SQL: {sql}"

    def test_no_date_filter_when_only_end_date(self):
        """SQL should have no WHERE clause when only end_date is provided."""
        sql = self._run(end_date=date(2025, 12, 31))
        assert "WHERE" not in sql, f"Unexpected WHERE clause in SQL: {sql}"

    def test_where_clause_when_both_params_provided(self):
        """SQL should contain a WHERE clause filtering on the date column."""
        sql = self._run(date_column="datadate", end_date=date(2025, 12, 31))
        assert "WHERE datadate <= '2025-12-31'" in sql, (
            f"Expected WHERE clause with date filter, got: {sql}"
        )

    def test_where_clause_uses_correct_column_name(self):
        """WHERE clause should use the provided date_column name."""
        sql = self._run(date_column="mthcaldt", end_date=date(2024, 6, 30))
        assert "WHERE mthcaldt <= '2024-06-30'" in sql, (
            f"Expected mthcaldt in WHERE clause, got: {sql}"
        )

    def test_sql_targets_correct_table(self):
        """SQL should reference the correct library and table via postgres_scan."""
        sql = self._run()
        assert "'comp'" in sql, f"Expected lib 'comp' in SQL, got: {sql}"
        assert "'funda'" in sql, f"Expected table 'funda' in SQL, got: {sql}"

    def test_sql_outputs_to_correct_filename(self):
        """SQL COPY should target the provided filename."""
        sql = self._run()
        assert "'out.parquet'" in sql, f"Expected filename in SQL, got: {sql}"

    def test_country_filter_on_direct_country_table(self):
        """Country-filtered security tables should filter directly on excntry."""
        sql = self._run(table_name="comp.security", country_filter=("USA", "CAN"))
        assert "postgres_query" in sql
        assert "FROM comp.security WHERE excntry IN (''USA'', ''CAN'')" in sql

    def test_country_filter_on_gvkey_table(self):
        """Country-filtered accounting tables should restrict gvkeys from security."""
        sql = self._run(
            table_name="comp.funda",
            date_column="datadate",
            end_date=date(2025, 12, 31),
            country_filter=("USA", "CAN"),
        )
        assert "postgres_query" in sql
        assert "WHERE datadate <= ''2025-12-31'' AND gvkey IN" in sql
        assert "FROM comp.security WHERE excntry IN (''USA'', ''CAN'')" in sql


# =============================================================================
# Tests: download_raw_data_tables
# =============================================================================


class TestDownloadRawDataTables:
    """Tests for download_raw_data_tables().

    This function orchestrates downloading multiple WRDS tables. Tests mock
    the WRDS connection and download_wrds_table to verify that date filtering
    parameters are passed correctly for each table.
    """

    @pytest.fixture()
    def captured_calls(self, test_paths):
        """Run download_raw_data_tables and capture all download_wrds_table calls."""
        with (
            patch("jkp.data.aux_functions.gen_wrds_connection_info", return_value="host=test"),
            patch("jkp.data.aux_functions.duckdb") as mock_duckdb,
            patch("jkp.data.aux_functions.download_wrds_table") as mock_download,
        ):
            mock_conn = MagicMock()
            mock_duckdb.connect.return_value = mock_conn

            from jkp.data.aux_functions import download_raw_data_tables

            download_raw_data_tables(
                test_paths,
                "user",
                "pass",
                end_date=date(2025, 12, 31),
            )
            yield mock_download.call_args_list

    def test_date_filtered_tables_get_date_column(self, captured_calls):
        """Tables with known date columns should receive the date_column kwarg."""
        expected_date_cols = {
            "crsp.msf_v2": "mthcaldt",
            "crsp.dsf_v2": "dlycaldt",
            "comp.secd": "datadate",
            "comp.g_secd": "datadate",
            "comp.secm": "datadate",
            "comp.funda": "datadate",
            "comp.fundq": "datadate",
            "comp.g_funda": "datadate",
            "comp.g_fundq": "datadate",
        }
        for c in captured_calls:
            table_name = c.args[2] if len(c.args) > 2 else c.kwargs.get("table_name")
            date_col = c.kwargs.get("date_column")
            if table_name in expected_date_cols:
                assert date_col == expected_date_cols[table_name], (
                    f"Table {table_name}: expected date_column={expected_date_cols[table_name]}, "
                    f"got {date_col}"
                )

    def test_reference_tables_get_no_date_column(self, captured_calls):
        """Reference/metadata tables should have date_column=None."""
        reference_tables = {
            "comp.exrt_dly",
            "ff.factors_monthly",
            "comp.g_security",
            "comp.security",
            "comp.r_ex_codes",
        }
        for c in captured_calls:
            table_name = c.args[2] if len(c.args) > 2 else c.kwargs.get("table_name")
            if table_name in reference_tables:
                date_col = c.kwargs.get("date_column")
                assert date_col is None, (
                    f"Reference table {table_name} should not have date_column, got {date_col}"
                )

    def test_end_date_passed_to_all_calls(self, captured_calls):
        """Every download_wrds_table call should receive the end_date."""
        for c in captured_calls:
            table_name = c.args[2] if len(c.args) > 2 else c.kwargs.get("table_name")
            end = c.kwargs.get("end_date")
            assert end == date(2025, 12, 31), (
                f"Table {table_name}: expected end_date=2025-12-31, got {end}"
            )

    def test_all_expected_tables_downloaded(self, captured_calls):
        """All tables in the canonical list should be downloaded."""
        downloaded = {
            c.args[2] if len(c.args) > 2 else c.kwargs.get("table_name") for c in captured_calls
        }
        expected_subset = {"comp.funda", "crsp.msf_v2", "crsp.dsf_v2", "comp.secd"}
        assert expected_subset <= downloaded, f"Missing tables: {expected_subset - downloaded}"

    def test_country_filter_without_usa_skips_crsp_tables(self, test_paths):
        """CRSP downloads should be skipped when selected countries exclude USA."""
        with (
            patch("jkp.data.aux_functions.gen_wrds_connection_info", return_value="host=test"),
            patch("jkp.data.aux_functions.duckdb") as mock_duckdb,
            patch("jkp.data.aux_functions.download_wrds_table") as mock_download,
        ):
            mock_duckdb.connect.return_value = MagicMock()

            from jkp.data.aux_functions import download_raw_data_tables

            download_raw_data_tables(
                test_paths,
                "user",
                "pass",
                end_date=date(2025, 12, 31),
                countries=("CAN",),
            )

        downloaded = {
            c.args[2] if len(c.args) > 2 else c.kwargs.get("table_name")
            for c in mock_download.call_args_list
        }
        assert downloaded
        assert not any(t.startswith("crsp.") for t in downloaded)
        assert "comp.funda" in downloaded
        for c in mock_download.call_args_list:
            assert c.kwargs.get("country_filter") == ("CAN",)


# =============================================================================
# Tests: save_main_data
# =============================================================================


class TestSaveMainData:
    """Tests for save_main_data().

    This function computes lagged market equity and exports country-level files.
    Filtering is now done upstream by filter_world(). Tests verify me_lag1
    computation and that all rows pass through.
    """

    def test_accepts_paths_parameter(self):
        """save_main_data should accept a paths parameter."""
        import inspect

        from jkp.data.aux_functions import save_main_data

        # measure_time wraps the function; inspect the inner function via closure
        inner_func = save_main_data.__closure__[0].cell_contents
        sig = inspect.signature(inner_func)
        assert "paths" in sig.parameters, (
            f"save_main_data should accept 'paths' parameter, got: {list(sig.parameters)}"
        )

    def _run_save_main_data(self, paths) -> None:
        """Run save_main_data with DuckDB mocked; the parquet read/write is what we test."""
        from jkp.data.aux_functions import save_main_data

        with patch("jkp.data.aux_functions.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value = MagicMock()
            save_main_data(paths)

    def test_all_rows_pass_through(self, test_paths):
        """save_main_data should not filter — all rows should appear in output."""
        world_data = pl.DataFrame(
            {
                "id": ["A", "B", "C", "D"],
                "eom": [date(2020, 1, 31)] * 4,
                "me": [100.0, 200.0, 300.0, 400.0],
                "primary_sec": [1, 0, 1, 1],
                "common": [1, 1, 0, 1],
                "obs_main": [1, 1, 1, 0],
                "exch_main": [1, 1, 1, 1],
                "excntry": ["USA"] * 4,
            }
        )
        out_path = test_paths.interim_dir / "world_data_output.parquet"
        world_data.write_parquet(out_path)
        self._run_save_main_data(test_paths)

        output = pl.read_parquet(out_path)
        assert len(output) == 4, f"Expected all 4 rows (no filtering), got {len(output)}"

    def test_no_eom_date_filter(self, test_paths):
        """All dates should pass through — there should be no eom <= end_date filter."""
        world_data = pl.DataFrame(
            {
                "id": ["A", "A"],
                "eom": [date(2020, 1, 31), date(2099, 12, 31)],
                "me": [100.0, 200.0],
                "primary_sec": [1, 1],
                "common": [1, 1],
                "obs_main": [1, 1],
                "exch_main": [1, 1],
                "excntry": ["USA"] * 2,
            }
        )
        out_path = test_paths.interim_dir / "world_data_output.parquet"
        world_data.write_parquet(out_path)
        self._run_save_main_data(test_paths)

        output = pl.read_parquet(out_path)
        assert len(output) == 2, f"Expected both rows (no date filter), got {len(output)}"

    def test_me_lag1_computed(self, test_paths):
        """save_main_data should add me_lag1 column with lagged market equity."""
        world_data = pl.DataFrame(
            {
                "id": ["A", "A", "A"],
                "eom": [date(2020, 1, 31), date(2020, 2, 29), date(2020, 3, 31)],
                "me": [100.0, 200.0, 300.0],
                "primary_sec": [1, 1, 1],
                "common": [1, 1, 1],
                "obs_main": [1, 1, 1],
                "exch_main": [1, 1, 1],
                "excntry": ["USA"] * 3,
            }
        )
        out_path = test_paths.interim_dir / "world_data_output.parquet"
        world_data.write_parquet(out_path)
        self._run_save_main_data(test_paths)

        output = pl.read_parquet(out_path).sort("eom")
        assert "me_lag1" in output.columns, "me_lag1 column should be present"
        assert output["me_lag1"][0] is None or output["me_lag1"][0] != output["me_lag1"][0]
        assert output["me_lag1"][1] == pytest.approx(100.0)
        assert output["me_lag1"][2] == pytest.approx(200.0)


class TestSaveMonthlyRet:
    """Tests for the SAS-compatible monthly return output."""

    def test_monthly_return_schema_matches_modified_sas(self, test_paths):
        from jkp.data.aux_functions import save_monthly_ret

        pl.DataFrame(
            {
                "excntry": ["USA"],
                "id": [1],
                "source_crsp": [0],
                "eom": [date(2020, 1, 31)],
                "me": [100.0],
                "ret_exc": [0.01],
                "ret": [0.02],
                "ret_local": [0.03],
                "ret_exc_wins": [0.011],
            }
        ).write_parquet(test_paths.interim_dir / "world_msf_output.parquet")

        save_monthly_ret(test_paths)

        expected = ["excntry", "id", "source_crsp", "eom", "ret_exc", "ret", "ret_local"]
        parquet_out = pl.read_parquet(
            test_paths.processed_dir / "return_data" / "world_ret_monthly.parquet"
        )
        assert parquet_out.columns == expected

        csv_out = test_paths.sas_output_dir / "world_ret_monthly.csv"
        assert csv_out.exists()
        assert [c.strip('"') for c in csv_out.read_text().splitlines()[0].split(",")] == expected


class TestSaveOutputFiles:
    """Tests for the SAS-compatible small output CSVs."""

    def test_writes_sas_output_csv_files(self, test_paths):
        from jkp.data.aux_functions import save_output_files

        sample = pl.DataFrame({"excntry": ["USA"], "date": [date(2020, 1, 31)], "ret": [0.01]})
        for name in (
            "market_returns",
            "market_returns_daily",
            "nyse_cutoffs",
            "return_cutoffs",
            "return_cutoffs_daily",
            "ap_factors_monthly",
            "ap_factors_daily",
        ):
            sample.write_parquet(test_paths.interim_dir / f"{name}.parquet")

        save_output_files(test_paths)

        for name in (
            "market_returns_daily",
            "market_returns",
            "nyse_cutoffs",
            "return_cutoffs",
            "return_cutoffs_daily",
        ):
            assert (test_paths.sas_output_dir / f"{name}.csv").exists()
            assert (test_paths.processed_dir / "other_output" / f"{name}.parquet").exists()


class TestCountryFilter:
    """Tests for optional country-only pipeline builds."""

    def test_normalize_country_filter(self):
        from jkp.data.aux_functions import normalize_country_filter

        assert normalize_country_filter(None) is None
        assert normalize_country_filter([]) is None
        assert normalize_country_filter([" usa ", "ISR", "usa"]) == ("USA", "ISR")
        with pytest.raises(ValueError, match="ISO-3"):
            normalize_country_filter(["US"])

    def test_filter_security_files_by_country(self, test_paths):
        from jkp.data.aux_functions import filter_security_files_by_country

        for filename in ("world_msf.parquet", "world_dsf.parquet"):
            pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "excntry": ["USA", "ISR", "FRA"],
                    "eom": [date(2020, 1, 31)] * 3,
                }
            ).write_parquet(test_paths.interim_dir / filename)

        filter_security_files_by_country(test_paths, ("USA", "ISR"))

        for filename in ("world_msf.parquet", "world_dsf.parquet"):
            out = pl.read_parquet(test_paths.interim_dir / filename)
            assert out["excntry"].to_list() == ["USA", "ISR"]

    def test_filter_security_files_by_country_raises_on_no_match(self, test_paths):
        from jkp.data.aux_functions import filter_security_files_by_country

        for filename in ("world_msf.parquet", "world_dsf.parquet"):
            pl.DataFrame(
                {
                    "id": [1],
                    "excntry": ["USA"],
                    "eom": [date(2020, 1, 31)],
                }
            ).write_parquet(test_paths.interim_dir / filename)

        with pytest.raises(ValueError, match="produced no rows"):
            filter_security_files_by_country(test_paths, ("ZZZ",))


# =============================================================================
# Tests: filter functions
# =============================================================================


class TestFilterFunctions:
    """Tests for filter_dsf(), filter_msf(), and filter_world().

    These functions apply MAIN_FILTERS screening to interim parquet files,
    keeping only rows where all four filter columns equal 1.
    """

    @staticmethod
    def _make_test_data() -> pl.DataFrame:
        """Create a toy DataFrame with mixed filter values."""
        return pl.DataFrame(
            {
                "id": ["A", "B", "C", "D", "E"],
                "eom": [date(2020, 1, 31)] * 5,
                "me": [100.0, 200.0, 300.0, 400.0, 500.0],
                "ret": [0.01, 0.02, 0.03, 0.04, 0.05],
                "primary_sec": [1, 0, 1, 1, 1],
                "common": [1, 1, 0, 1, 1],
                "obs_main": [1, 1, 1, 0, 1],
                "exch_main": [1, 1, 1, 1, 0],
                "excntry": ["USA"] * 5,
            }
        )

    def _run_filter(self, paths, func_name: str, source: str, output: str) -> pl.DataFrame:
        """Write test data, run a filter function, and return the result."""
        import jkp.data.aux_functions as aux_functions

        data = self._make_test_data()
        data.write_parquet(paths.interim_dir / source)

        getattr(aux_functions, func_name)(paths)

        return pl.read_parquet(paths.interim_dir / output)

    def test_filter_dsf_keeps_only_passing_rows(self, test_paths):
        """filter_dsf should keep only rows where all four filter columns are 1."""
        result = self._run_filter(
            test_paths, "filter_dsf", "world_dsf.parquet", "world_dsf_output.parquet"
        )
        assert len(result) == 1, f"Expected 1 passing row, got {len(result)}"
        assert result["id"][0] == "A"

    def test_filter_msf_keeps_only_passing_rows(self, test_paths):
        """filter_msf should keep only rows where all four filter columns are 1."""
        result = self._run_filter(
            test_paths, "filter_msf", "world_msf.parquet", "world_msf_output.parquet"
        )
        assert len(result) == 1, f"Expected 1 passing row, got {len(result)}"
        assert result["id"][0] == "A"

    def test_filter_world_keeps_only_passing_rows(self, test_paths):
        """filter_world should keep only rows where all four filter columns are 1."""
        result = self._run_filter(
            test_paths, "filter_world", "world_data.parquet", "world_data_output.parquet"
        )
        assert len(result) == 1, f"Expected 1 passing row, got {len(result)}"
        assert result["id"][0] == "A"

    def test_filter_preserves_all_columns(self, test_paths):
        """Filtered output should retain all original columns."""
        original_cols = set(self._make_test_data().columns)
        result = self._run_filter(
            test_paths, "filter_world", "world_data.parquet", "world_data_output.parquet"
        )
        assert set(result.columns) == original_cols, (
            f"Column mismatch: expected {original_cols}, got {set(result.columns)}"
        )

    def test_filter_does_not_modify_source(self, test_paths):
        """Source file should be unchanged after filtering."""
        import jkp.data.aux_functions as aux_functions

        data = self._make_test_data()
        data.write_parquet(test_paths.interim_dir / "world_data.parquet")

        aux_functions.filter_world(test_paths)

        source = pl.read_parquet(test_paths.interim_dir / "world_data.parquet")
        assert len(source) == 5, f"Source should be unchanged (5 rows), got {len(source)}"

    def test_filter_is_idempotent(self, test_paths):
        """Running filter twice should produce the same result."""
        import jkp.data.aux_functions as aux_functions

        data = self._make_test_data()
        data.write_parquet(test_paths.interim_dir / "world_data.parquet")

        aux_functions.filter_world(test_paths)
        first_run = pl.read_parquet(test_paths.interim_dir / "world_data_output.parquet")
        aux_functions.filter_world(test_paths)
        second_run = pl.read_parquet(test_paths.interim_dir / "world_data_output.parquet")

        assert first_run.equals(second_run), "Second run should produce identical output"

    def test_filter_all_pass(self, test_paths):
        """When all rows pass the filter, all should be retained."""
        import jkp.data.aux_functions as aux_functions

        data = pl.DataFrame(
            {
                "id": ["A", "B"],
                "eom": [date(2020, 1, 31)] * 2,
                "me": [100.0, 200.0],
                "ret": [0.01, 0.02],
                "primary_sec": [1, 1],
                "common": [1, 1],
                "obs_main": [1, 1],
                "exch_main": [1, 1],
                "excntry": ["USA"] * 2,
            }
        )
        data.write_parquet(test_paths.interim_dir / "world_data.parquet")

        aux_functions.filter_world(test_paths)

        result = pl.read_parquet(test_paths.interim_dir / "world_data_output.parquet")
        assert len(result) == 2, f"Expected both rows to pass, got {len(result)}"

    def test_filter_none_pass(self, test_paths):
        """When no rows pass the filter, output should be empty."""
        import jkp.data.aux_functions as aux_functions

        data = pl.DataFrame(
            {
                "id": ["A"],
                "eom": [date(2020, 1, 31)],
                "me": [100.0],
                "ret": [0.01],
                "primary_sec": [0],
                "common": [0],
                "obs_main": [0],
                "exch_main": [0],
                "excntry": ["USA"],
            }
        )
        data.write_parquet(test_paths.interim_dir / "world_data.parquet")

        aux_functions.filter_world(test_paths)

        result = pl.read_parquet(test_paths.interim_dir / "world_data_output.parquet")
        assert len(result) == 0, f"Expected 0 rows, got {len(result)}"
