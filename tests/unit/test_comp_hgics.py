"""Regression test: comp_hgics output must not depend on the wall-clock date.

Issue #131 documented that calling `date.today()` inside `comp_hgics` made the
output depend on when the pipeline ran (each currently-active GICS record gained
one extra exploded row per day that passed between runs). The fix replaced
`date.today()` with `END_DATE` from `config.py`. This test guards against future
regressions where someone re-introduces a wall-clock dependency.

We monkeypatch the `date` name imported into `jkp.data.aux_functions` so that
`date.today()` returns two wildly different values across two consecutive
invocations of `comp_hgics`, then assert the outputs are bit-identical. If the
function ever calls `date.today()` again, the two outputs will differ and this
test will fail.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

import polars as pl
import pytest

import jkp.data.aux_functions as aux_functions
from jkp.data.aux_functions import comp_hgics
from jkp.data.paths import DataPaths


def _write_minimal_hgics_fixture(raw_data_dfs: Path) -> None:
    """Write a tiny comp_hgics_na.parquet with one open-ended industry record.

    The open-ended row (`indthru` is NULL) is the trigger for the wall-clock
    fill path — without it the regression test would be trivially satisfied.
    """
    # Column is named `gics` (not `gsubind`): the upstream `comp_hgics_aux`
    # renames `gsubind` -> `gics` while building `comp_hgics_na.parquet`, so by
    # the time `comp_hgics` reads the file the column is already `gics`.
    pl.DataFrame(
        {
            "gvkey": ["001000", "001000", "001001"],
            "indfrom": [_dt.date(2010, 1, 1), _dt.date(2015, 6, 1), _dt.date(2018, 3, 1)],
            "indthru": [_dt.date(2015, 5, 31), None, None],
            "gics": [10101010, 10101020, 20202020],
        }
    ).write_parquet(raw_data_dfs / "comp_hgics_na.parquet")


def _date_subclass_returning(today_value: _dt.date) -> type:
    """Build a `date` subclass whose `today()` classmethod returns a fixed value.

    Subclassing (rather than monkeypatching the `today` method directly) keeps
    `pl.lit(date.today())` happy: Polars will still see a `datetime.date` instance.
    """

    class _FixedDate(_dt.date):
        @classmethod
        def today(cls) -> _dt.date:
            return today_value

    return _FixedDate


class TestCompHgics:
    """Tests for `comp_hgics`."""

    def test_empty_input_writes_empty_typed_output(self, test_paths: DataPaths) -> None:
        raw_data_dfs = test_paths.interim_dir / "raw_data_dfs"
        pl.DataFrame(
            schema={
                "gvkey": pl.String,
                "indfrom": pl.Date,
                "indthru": pl.Date,
                "gics": pl.Int64,
            }
        ).write_parquet(raw_data_dfs / "comp_hgics_gl.parquet")

        comp_hgics(test_paths, "global")

        out = pl.read_parquet(test_paths.interim_dir / "g_hgics.parquet")
        assert out.is_empty()
        assert out.schema == {"gvkey": pl.String, "date": pl.Date, "gics": pl.Int64}

    def test_runtime_end_date_closes_open_interval(self, test_paths: DataPaths) -> None:
        raw_data_dfs = test_paths.interim_dir / "raw_data_dfs"
        pl.DataFrame(
            {
                "gvkey": ["001000"],
                "indfrom": [_dt.date(2026, 7, 29)],
                "indthru": [None],
                "gics": [10101010],
            }
        ).write_parquet(raw_data_dfs / "comp_hgics_na.parquet")

        comp_hgics(test_paths, "national", end_date=_dt.date(2026, 7, 31))

        out = pl.read_parquet(test_paths.interim_dir / "na_hgics.parquet")
        assert out["date"].to_list() == [
            _dt.date(2026, 7, 29),
            _dt.date(2026, 7, 30),
            _dt.date(2026, 7, 31),
        ]

    def test_unsorted_history_still_closes_latest_open_interval(
        self, test_paths: DataPaths
    ) -> None:
        """Physical parquet order must not decide which GICS interval is current."""
        raw_data_dfs = test_paths.interim_dir / "raw_data_dfs"
        pl.DataFrame(
            {
                # Deliberately place the current row before the older closed row.
                "gvkey": ["001000", "001000"],
                "indfrom": [_dt.date(2020, 1, 1), _dt.date(2010, 1, 1)],
                "indthru": [None, _dt.date(2019, 12, 31)],
                "gics": [20202020, 10101010],
            }
        ).write_parquet(raw_data_dfs / "comp_hgics_na.parquet")

        comp_hgics(test_paths, "national", end_date=_dt.date(2020, 1, 3))

        out = pl.read_parquet(test_paths.interim_dir / "na_hgics.parquet")
        current = out.filter(pl.col("date") >= _dt.date(2020, 1, 1))
        assert current["date"].to_list() == [
            _dt.date(2020, 1, 1),
            _dt.date(2020, 1, 2),
            _dt.date(2020, 1, 3),
        ]
        assert current["gics"].to_list() == [20202020, 20202020, 20202020]

    def test_all_null_indfrom_does_not_crash(self, test_paths: DataPaths) -> None:
        """Non-empty input with all-null indfrom must not raise TypeError."""
        raw_data_dfs = test_paths.interim_dir / "raw_data_dfs"
        raw_data_dfs.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "gvkey": ["001000"],
                "indfrom": [None],
                "indthru": [None],
                "gics": [10101010],
            },
            schema={
                "gvkey": pl.Utf8,
                "indfrom": pl.Date,
                "indthru": pl.Date,
                "gics": pl.Int64,
            },
        ).write_parquet(raw_data_dfs / "comp_hgics_na.parquet")

        comp_hgics(test_paths, "national")

        output = pl.read_parquet(test_paths.interim_dir / "na_hgics.parquet")
        assert set(output.columns) == {"gvkey", "date", "gics"}

    @pytest.mark.regression
    def test_independent_of_wall_clock(
        self, test_paths: DataPaths, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """comp_hgics output must be identical regardless of `date.today()`.

        If a future change re-introduces a `date.today()` (or any other wall-clock
        read) into `comp_hgics`, the two runs below will produce different output
        because their patched "today" values differ by 6 years — a difference that
        would show up as ~2,200 extra exploded daily rows per open-ended record.
        """
        raw_data_dfs = test_paths.interim_dir / "raw_data_dfs"
        _write_minimal_hgics_fixture(raw_data_dfs)
        output_path = test_paths.interim_dir / "na_hgics.parquet"

        monkeypatch.setattr(aux_functions, "date", _date_subclass_returning(_dt.date(2024, 1, 15)))
        comp_hgics(test_paths, "national")
        first = pl.read_parquet(output_path)

        output_path.unlink()

        monkeypatch.setattr(aux_functions, "date", _date_subclass_returning(_dt.date(2030, 7, 15)))
        comp_hgics(test_paths, "national")
        second = pl.read_parquet(output_path)

        assert first.equals(second), (
            "comp_hgics output must not depend on the wall-clock date; "
            "two runs with different patched `date.today()` values produced "
            "different output, which means a wall-clock dependency has been "
            "re-introduced (regression of #131)."
        )
