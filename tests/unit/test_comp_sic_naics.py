"""Tests for comp_sic_naics() (Issue #155).

Covers the daily SIC/NAICS expansion from Compustat NA + Global histories:

- Full outer join between ``sic_naics_na`` and ``sic_naics_gl`` on ``(gvkey, datadate)``
- Coalesce precedence: non-null SIC wins (DuckDB ``DISTINCT ON ... ORDER BY ... sic``)
- Hard-coded filter removing ``gvkey='175650'`` / ``datadate=2005-12-31`` / null naics
- ``LPAD(gvkey, 6, '0')`` zero-padding
- Daily expansion of [datadate, next datadate) gaps with ``closed="left"``
- The newest record of each gvkey runs through ``end_date`` (the panel end); the
  per-test ``end_date`` equals that test's latest datadate where the terminal row is
  expected to stay a single day
- Dedup + sort by ``(gvkey, date)``
- A regression golden fixture locking the output bit-for-bit.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from jkp.data.aux_functions import comp_sic_naics
from jkp.data.paths import DataPaths
from tests.conftest import assert_sorted_by_keys, assert_unique_keys
from tests.golden.comp_sic_naics_inputs import empty_sic_naics_frame, sic_naics_frame

GOLDEN_DIR = Path(__file__).parent.parent / "golden" / "fixtures" / "comp_sic_naics"
# Panel end used for the golden fixture (the latest datadate among its inputs), so the
# newest record of every gvkey runs through it, as in production.
GOLDEN_END = date(2020, 6, 15)


def _write_inputs(paths: DataPaths, na: pl.DataFrame, gl: pl.DataFrame) -> None:
    """Persist NA/GL fixtures in the locations ``comp_sic_naics`` reads from."""
    raw_data_dfs = paths.interim_dir / "raw_data_dfs"
    raw_data_dfs.mkdir(parents=True, exist_ok=True)
    na.write_parquet(raw_data_dfs / "sic_naics_na.parquet")
    gl.write_parquet(raw_data_dfs / "sic_naics_gl.parquet")


class TestCompSicNaics:
    """Tests for ``comp_sic_naics``."""

    @pytest.fixture(autouse=True)
    def _setup(self, test_paths: DataPaths) -> None:
        self.paths = test_paths
        self.output_path = self.paths.interim_dir / "comp_other.parquet"

    def test_na_only_gvkey(self) -> None:
        """A gvkey present only in NA appears in the output with its NA codes."""
        na = sic_naics_frame(["001000"], [date(2020, 1, 1)], [7372], [511210])
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2020, 1, 1))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["gvkey"] == "001000"
        assert row["sic"] == 7372
        assert row["naics"] == 511210

    def test_gl_only_gvkey(self) -> None:
        """A gvkey present only in GL appears in the output with its GL codes."""
        gl = sic_naics_frame(["002000"], [date(2020, 6, 15)], [2834], [325412])
        _write_inputs(self.paths, empty_sic_naics_frame(), gl)

        comp_sic_naics(self.paths, end_date=date(2020, 6, 15))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["sic"] == 2834
        assert row["naics"] == 325412

    def test_coalesce_prefers_non_null_sic(self) -> None:
        """When NA has null SIC and GL has a real one, the non-null value wins."""
        na = sic_naics_frame(["004000"], [date(2019, 3, 1)], [None], [541110])
        gl = sic_naics_frame(["004000"], [date(2019, 3, 1)], [4813], [517110])
        _write_inputs(self.paths, na, gl)

        comp_sic_naics(self.paths, end_date=date(2019, 3, 1))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        row = result.row(0, named=True)
        # COALESCE(sica, sicb) brings the GL value in when NA is null.
        assert row["sic"] == 4813

    def test_coalesce_na_takes_precedence_over_gl(self) -> None:
        """When both NA and GL have non-null SIC, the NA value wins."""
        na = sic_naics_frame(["003000"], [date(2018, 5, 1)], [6020], [522110])
        gl = sic_naics_frame(["003000"], [date(2018, 5, 1)], [6021], [522120])
        _write_inputs(self.paths, na, gl)

        comp_sic_naics(self.paths, end_date=date(2018, 5, 1))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["sic"] == 6020, (
            "COALESCE(sica, sicb) must prefer the NA value when both are non-null"
        )

    def test_hardcoded_175650_row_dropped(self) -> None:
        """The hard-coded NA filter removes ``(175650, 2005-12-31, naics=NULL)``."""
        na = sic_naics_frame(
            ["175650", "175650"],
            [date(2005, 12, 31), date(2006, 6, 30)],
            [1311, 1311],
            [None, 211120],
        )
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2006, 6, 30))

        result = pl.read_parquet(self.output_path)
        # The 2005-12-31 row was dropped; only the 2006-06-30 row remains and
        # (since it is the only/last row per gvkey) is kept as a single-date row.
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["date"] == date(2006, 6, 30)
        assert row["naics"] == 211120

    def test_gvkey_zero_padded_to_six_chars(self) -> None:
        """Unpadded gvkey inputs are ``LPAD``-ed to width 6 in the output."""
        na = sic_naics_frame(["500"], [date(2021, 7, 15)], [7372], [511210])
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2021, 7, 15))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        assert result["gvkey"].to_list() == ["000500"]

    def test_daily_expansion_closed_left(self) -> None:
        """Two consecutive datadates fill the half-open interval [d1, d2)."""
        na = sic_naics_frame(
            ["001000", "001000"],
            [date(2020, 1, 1), date(2020, 1, 4)],
            [3711, 3713],
            [336111, 336112],
        )
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2020, 1, 4))

        result = pl.read_parquet(self.output_path).sort("date")
        # First span Jan-1 -> Jan-4 (closed="left") = Jan-1, Jan-2, Jan-3 (3 rows)
        # plus the trailing single-date row at Jan-4 = 4 rows total.
        assert result.height == 4
        assert result["date"].to_list() == [
            date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 4),
        ]
        # First three rows carry the Jan-1 codes; the Jan-4 row carries Jan-4 codes.
        first_three = result.filter(pl.col("date") < date(2020, 1, 4))
        assert first_three["sic"].unique().to_list() == [3711]
        last = result.filter(pl.col("date") == date(2020, 1, 4)).row(0, named=True)
        assert last["sic"] == 3713

    def test_distinct_on_picks_lowest_sic(self) -> None:
        """When duplicate (gvkey, datadate) rows have different SICs, the lowest wins.

        The SQL uses ``DISTINCT ON (gvkey, date) ... ORDER BY gvkey, date, sic``
        (ascending), so among rows sharing the same (gvkey, date) after
        COALESCE, the row with the smallest SIC is retained.
        """
        na = sic_naics_frame(
            ["005000", "005000"],
            [date(2020, 1, 1), date(2020, 1, 1)],
            [9000, 1000],
            [999999, 111111],
        )
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2020, 1, 1))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["sic"] == 1000, (
            "DISTINCT ON ... ORDER BY sic (ascending) must keep the lowest SIC"
        )
        assert row["naics"] == 111111

    def test_dedup_and_sort_invariants(self) -> None:
        """Output has unique ``(gvkey, date)`` keys and is sorted ascending."""
        na = sic_naics_frame(
            ["002000", "001000"],
            [date(2020, 6, 15), date(2020, 1, 1)],
            [2834, 7372],
            [325412, 511210],
        )
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2020, 6, 15))

        result = pl.read_parquet(self.output_path)
        assert_unique_keys(result, ["gvkey", "date"])
        assert_sorted_by_keys(result, "gvkey", "date")

    def test_newest_record_runs_through_panel_end(self) -> None:
        """The newest record has no successor and must run through ``end_date``.

        Ending it on its own datadate (the SAS ``if FIRST.gvkey then valid_to = date``)
        left every month after the last fiscal year-end with null sic/naics/ff49.
        """
        end = date(2026, 8, 31)
        na = sic_naics_frame(
            ["001000", "001000"],
            [date(2024, 12, 31), date(2025, 12, 31)],
            [1000, 2000],
            [111, 222],
        )
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=end)

        result = pl.read_parquet(self.output_path).sort("date")
        first = result.filter(pl.col("sic") == 1000)
        last = result.filter(pl.col("sic") == 2000)
        assert (first["date"].min(), first["date"].max()) == (
            date(2024, 12, 31),
            date(2025, 12, 30),
        )
        assert (last["date"].min(), last["date"].max()) == (date(2025, 12, 31), end)
        assert last.height == (end - date(2025, 12, 31)).days + 1
        assert_unique_keys(result, ["gvkey", "date"])

    def test_record_after_panel_end_keeps_one_day(self) -> None:
        na = sic_naics_frame(["001001"], [date(2026, 12, 31)], [3000], [333])
        _write_inputs(self.paths, na, empty_sic_naics_frame())

        comp_sic_naics(self.paths, end_date=date(2026, 8, 31))

        result = pl.read_parquet(self.output_path)
        assert result.height == 1
        assert result["date"][0] == date(2026, 12, 31)

    def test_global_only_company_is_extended_too(self) -> None:
        gl = sic_naics_frame(["300001"], [date(2025, 6, 30)], [4000], [None])
        _write_inputs(self.paths, empty_sic_naics_frame(), gl)

        comp_sic_naics(self.paths, end_date=date(2026, 8, 31))

        result = pl.read_parquet(self.output_path)
        assert (result["date"].min(), result["date"].max()) == (
            date(2025, 6, 30),
            date(2026, 8, 31),
        )
        assert result["sic"].unique().to_list() == [4000]

    @pytest.mark.regression
    def test_comp_sic_naics_golden_fixture(self) -> None:
        """Bit-identical match against the locked golden fixture."""
        from tests.golden.comp_sic_naics_inputs import build_sic_naics_inputs

        na, gl = build_sic_naics_inputs()
        _write_inputs(self.paths, na, gl)
        comp_sic_naics(self.paths, end_date=GOLDEN_END)

        result = pl.read_parquet(self.output_path)
        golden = pl.read_parquet(GOLDEN_DIR / "comp_other.parquet")
        assert_frame_equal(result, golden, check_exact=True)
