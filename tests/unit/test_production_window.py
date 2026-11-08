"""The per-country production CSVs carry a bounded window of history.

An appending loader only reads rows past its own high-water mark, so emitting the
full panel writes, stores and ships roughly 87% of rows that nothing reads. The
bound is applied to the finished frame: every characteristic is still computed
over the whole source window, so trimming must not move a single value in the
rows that remain. That invariant is what most of this module checks.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from jkp.data.config import PRODUCTION_OUTPUT_YEARS
from jkp.data.paths import DataPaths
from jkp.data.production import (
    _production_lower_bound,
    save_daily_production_csv,
    save_main_production_csv,
)

pytestmark = pytest.mark.unit

END = date(2026, 5, 31)


class TestLowerBound:
    def test_counts_back_from_the_end_date(self) -> None:
        assert _production_lower_bound(END, 3) == date(2023, 5, 1)

    def test_zero_means_everything(self) -> None:
        assert _production_lower_bound(END, 0) is None

    def test_negative_is_treated_as_unbounded(self) -> None:
        assert _production_lower_bound(END, -1) is None

    def test_anchored_to_the_first_so_the_span_is_never_short(self) -> None:
        for day in (1, 15, 31):
            bound = _production_lower_bound(date(2026, 5, day), 3)
            assert bound == date(2023, 5, 1)

    def test_leap_day_end_date(self) -> None:
        assert _production_lower_bound(date(2024, 2, 29), 3) == date(2021, 2, 1)

    def test_default_matches_config(self) -> None:
        assert _production_lower_bound(END, PRODUCTION_OUTPUT_YEARS) == date(
            END.year - PRODUCTION_OUTPUT_YEARS, END.month, 1
        )


def _paths(tmp_path: Path) -> DataPaths:
    paths = DataPaths(base_dir=tmp_path)
    (paths.interim_dir / "raw_data_dfs").mkdir(parents=True, exist_ok=True)
    paths.raw_tables_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    return paths


def _write_fx(paths: DataPaths) -> None:
    pl.DataFrame(
        {
            "curcdd": ["ILS", "ILS"],
            "datadate": [date(2018, 1, 1), date(2027, 1, 1)],
            "fx": [0.25, 0.25],
        }
    ).cast({"datadate": pl.Date}).write_parquet(
        paths.interim_dir / "raw_data_dfs" / "__fx1.parquet"
    )


def _eom(year: int, month: int) -> date:
    """Last calendar day of the given month."""
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _write_panels(paths: DataPaths, eoms: list[date]) -> None:
    """A one-security monthly panel plus the daily rows behind it."""
    n = len(eoms)
    pl.DataFrame(
        {
            "id": [1] * n,
            "gvkey": ["100000"] * n,
            "iid": ["01"] * n,
            "comp_exchg": [11] * n,
            "excntry": ["BRA"] * n,
            "date": eoms,
            "eom": eoms,
            "me": [100.0 + i for i in range(n)],
            "me_company": [100.0 + i for i in range(n)],
            "me_lag1": [90.0 + i for i in range(n)],
            "ret_1_0": [0.01 * i for i in range(n)],
        }
    ).cast({"date": pl.Date, "eom": pl.Date, "comp_exchg": pl.Int64}).write_parquet(
        paths.interim_dir / "world_data_output.parquet"
    )

    daily = pl.DataFrame(
        {
            "id": [1] * n,
            "excntry": ["BRA"] * n,
            "date": eoms,
            "eom": eoms,
            "dolvol": [100.0] * n,
            "prc": [10.0] * n,
            "prc_high": [11.0] * n,
            "prc_low": [9.0] * n,
            "prc_open_lcl": [10.0] * n,
            "fx": [0.5] * n,
            "ret": [0.01] * n,
            "ret_local": [0.02] * n,
            "ret_exc": [0.005] * n,
        }
    ).cast({"date": pl.Date, "eom": pl.Date})
    daily.write_parquet(paths.interim_dir / "world_dsf.parquet")
    daily.write_parquet(paths.interim_dir / "world_dsf_output.parquet")

    ids = pl.DataFrame(
        {
            "gvkey": ["100000"] * n,
            "iid": ["01"] * n,
            "exchg": [11] * n,
            "datadate": eoms,
            "cusip": ["037833100"] * n,
            "conm": ["TEST CO"] * n,
        }
    ).cast({"datadate": pl.Date})
    ids.write_parquet(paths.raw_tables_dir / "comp_secd.parquet")
    ids.drop("cusip").with_columns(
        sedol=pl.lit("1234567"), isin=pl.lit(None, dtype=pl.Utf8)
    ).write_parquet(paths.raw_tables_dir / "comp_g_secd.parquet")


# Six years of month-ends ending at END, so a 3-year window keeps roughly half.
_EOMS = [_eom(y, m) for y in range(2020, 2027) for m in range(1, 13) if date(y, m, 1) <= END]


class TestWindowApplied:
    def test_monthly_keeps_only_the_window(self, tmp_path: Path) -> None:
        paths = _paths(tmp_path)
        _write_fx(paths)
        _write_panels(paths, _EOMS)

        save_main_production_csv(paths, end_date=END, production_years=3)
        df = pl.read_csv(paths.production_dir / "monthly" / "bra.csv")
        eom = df["eom"].cast(pl.Utf8).to_list()
        assert eom, "window must not empty the file"
        assert min(eom) >= "20230501"
        assert max(eom) <= "20260531"

    def test_daily_keeps_only_the_window(self, tmp_path: Path) -> None:
        paths = _paths(tmp_path)
        _write_fx(paths)
        _write_panels(paths, _EOMS)

        save_daily_production_csv(paths, end_date=END, production_years=3)
        df = pl.read_csv(paths.production_dir / "daily" / "bra.csv")
        dates = df["date"].cast(pl.Utf8).to_list()
        assert dates
        assert min(dates) >= "20230501"

    def test_zero_emits_the_whole_panel(self, tmp_path: Path) -> None:
        paths = _paths(tmp_path)
        _write_fx(paths)
        _write_panels(paths, _EOMS)

        save_main_production_csv(paths, end_date=END, production_years=0)
        df = pl.read_csv(paths.production_dir / "monthly" / "bra.csv")
        assert df.height == len(_EOMS)

    def test_window_is_smaller_than_the_full_panel(self, tmp_path: Path) -> None:
        """Guards against a bound that silently matches everything."""
        paths = _paths(tmp_path)
        _write_fx(paths)
        _write_panels(paths, _EOMS)

        save_main_production_csv(paths, end_date=END, production_years=0)
        full = pl.read_csv(paths.production_dir / "monthly" / "bra.csv").height
        save_main_production_csv(paths, end_date=END, production_years=3)
        sliced = pl.read_csv(paths.production_dir / "monthly" / "bra.csv").height
        assert 0 < sliced < full


class TestSlicingChangesNoValue:
    """The invariant that matters: trimming must move nothing in the kept rows."""

    def test_monthly_retained_rows_are_byte_identical(self, tmp_path: Path) -> None:
        full_paths = _paths(tmp_path / "full")
        _write_fx(full_paths)
        _write_panels(full_paths, _EOMS)
        save_main_production_csv(full_paths, end_date=END, production_years=0)
        full_lines = (full_paths.production_dir / "monthly" / "bra.csv").read_text().splitlines()

        cut_paths = _paths(tmp_path / "cut")
        _write_fx(cut_paths)
        _write_panels(cut_paths, _EOMS)
        save_main_production_csv(cut_paths, end_date=END, production_years=3)
        cut_lines = (cut_paths.production_dir / "monthly" / "bra.csv").read_text().splitlines()

        assert cut_lines[0] == full_lines[0], "header must not change"
        # Every emitted row must appear verbatim in the unsliced output.
        assert set(cut_lines[1:]).issubset(set(full_lines[1:]))
        assert len(cut_lines) > 1

    def test_daily_retained_rows_are_byte_identical(self, tmp_path: Path) -> None:
        full_paths = _paths(tmp_path / "full")
        _write_fx(full_paths)
        _write_panels(full_paths, _EOMS)
        save_daily_production_csv(full_paths, end_date=END, production_years=0)
        full_lines = (full_paths.production_dir / "daily" / "bra.csv").read_text().splitlines()

        cut_paths = _paths(tmp_path / "cut")
        _write_fx(cut_paths)
        _write_panels(cut_paths, _EOMS)
        save_daily_production_csv(cut_paths, end_date=END, production_years=3)
        cut_lines = (cut_paths.production_dir / "daily" / "bra.csv").read_text().splitlines()

        assert cut_lines[0] == full_lines[0]
        assert set(cut_lines[1:]).issubset(set(full_lines[1:]))
        assert len(cut_lines) > 1

    def test_boundary_month_is_included(self, tmp_path: Path) -> None:
        """The bound is inclusive; the first month of the window must survive."""
        paths = _paths(tmp_path)
        _write_fx(paths)
        _write_panels(paths, _EOMS)

        save_main_production_csv(paths, end_date=END, production_years=3)
        eom = pl.read_csv(paths.production_dir / "monthly" / "bra.csv")["eom"].cast(pl.Utf8)
        assert "20230531" in eom.to_list()
