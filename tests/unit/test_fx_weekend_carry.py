"""Carrying the latest exchange rate over an undelivered weekend that ends the build window.

S&P publishes Saturday and Sunday rates as copies of Friday's but delivers them only
with Monday's. The run on Sunday 2026-11-01 therefore had no rate for Saturday
2026-10-31, the month end that accounting values are converted at.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from jkp.data.aux_functions import carry_fx_over_weekend_end, compustat_fx, gen_fx1

pytestmark = pytest.mark.unit

SCHEMA = {"curcdd": pl.String, "datadate": pl.Date, "fx": pl.Float64}


def _fx(rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=SCHEMA, orient="row")


def _added(before: pl.DataFrame, after: pl.DataFrame) -> pl.DataFrame:
    return after.join(before, on=["curcdd", "datadate"], how="anti").sort(["curcdd", "datadate"])


class TestCarryFxOverWeekendEnd:
    def test_saturday_end_gets_fridays_rate_for_every_currency(self):
        fx = _fx(
            [
                ("EUR", date(2026, 10, 29), 1.10),
                ("EUR", date(2026, 10, 30), 1.11),
                ("USD", date(2026, 10, 30), 1.0),
            ]
        )
        out = carry_fx_over_weekend_end(fx, date(2026, 10, 31))
        assert _added(fx, out).rows() == [
            ("EUR", date(2026, 10, 31), 1.11),
            ("USD", date(2026, 10, 31), 1.0),
        ]
        assert out.schema == fx.schema

    def test_sunday_end_gets_saturday_and_sunday(self):
        fx = _fx([("EUR", date(2027, 1, 29), 1.2)])
        out = carry_fx_over_weekend_end(fx, date(2027, 1, 31))
        assert _added(fx, out).rows() == [
            ("EUR", date(2027, 1, 30), 1.2),
            ("EUR", date(2027, 1, 31), 1.2),
        ]

    def test_missing_friday_is_not_filled(self):
        fx = _fx([("EUR", date(2026, 10, 29), 1.10)])
        assert carry_fx_over_weekend_end(fx, date(2026, 10, 31)).equals(fx)

    def test_weekday_end_is_not_filled(self):
        fx = _fx([("EUR", date(2026, 9, 29), 1.10)])
        assert carry_fx_over_weekend_end(fx, date(2026, 9, 30)).equals(fx)

    def test_discontinued_currency_is_not_extended(self):
        fx = _fx([("DEM", date(1998, 12, 31), 0.6), ("EUR", date(2026, 10, 30), 1.11)])
        added = _added(fx, carry_fx_over_weekend_end(fx, date(2026, 10, 31)))
        assert added["curcdd"].to_list() == ["EUR"]

    def test_rates_after_the_end_date_do_not_move_the_target(self):
        """comp.exrt_dly is downloaded without an end-date filter."""
        fx = _fx([("EUR", date(2026, 10, 30), 1.11), ("EUR", date(2026, 11, 2), 1.15)])
        added = _added(fx, carry_fx_over_weekend_end(fx, date(2026, 10, 31)))
        assert added.rows() == [("EUR", date(2026, 10, 31), 1.11)]

    def test_delivered_weekend_rates_win(self):
        fx = _fx(
            [
                ("EUR", date(2026, 10, 30), 1.11),
                ("EUR", date(2026, 10, 31), 1.12),
                ("JPY", date(2026, 10, 30), 0.0066),
            ]
        )
        out = carry_fx_over_weekend_end(fx, date(2026, 10, 31))
        assert _added(fx, out).rows() == [("JPY", date(2026, 10, 31), 0.0066)]
        eur_saturday = out.filter(
            (pl.col("curcdd") == "EUR") & (pl.col("datadate") == date(2026, 10, 31))
        )
        assert eur_saturday["fx"].to_list() == [1.12]

    @pytest.mark.parametrize("rate", [None, 0.0, float("inf"), float("nan")])
    def test_invalid_latest_rate_is_not_carried(self, rate):
        fx = _fx([("EUR", date(2026, 10, 30), rate)])
        assert carry_fx_over_weekend_end(fx, date(2026, 10, 31)).height == 1


class TestGenFx1EndDate:
    @staticmethod
    def _write_exrt(paths) -> None:
        rows = []
        for day in (date(2026, 10, 29), date(2026, 10, 30)):
            rows += [
                ("GBP", "USD", day, Decimal("1.30000000")),
                ("GBP", "EUR", day, Decimal("1.15000000")),
            ]
        pl.DataFrame(
            rows,
            schema={
                "fromcurd": pl.String,
                "tocurd": pl.String,
                "datadate": pl.Date,
                "exratd": pl.Decimal(16, 8),
            },
            orient="row",
        ).write_parquet(paths.raw_tables_dir / "comp_exrt_dly.parquet")

    def test_saturday_month_end_is_converted_at_fridays_rate(self, test_paths):
        self._write_exrt(test_paths)
        gen_fx1(test_paths, date(2026, 10, 31))
        fx = compustat_fx(test_paths)
        saturday = fx.filter(pl.col("datadate") == date(2026, 10, 31)).sort("curcdd")
        friday = fx.filter(pl.col("datadate") == date(2026, 10, 30)).sort("curcdd")
        assert saturday["curcdd"].to_list() == ["EUR", "USD"]
        assert saturday["fx"].to_list() == pytest.approx(friday["fx"].to_list())
        assert saturday["fx"].to_list() == pytest.approx([1.30 / 1.15, 1.0])

    def test_without_end_date_the_weekend_stays_empty(self, test_paths):
        self._write_exrt(test_paths)
        gen_fx1(test_paths)
        fx = compustat_fx(test_paths)
        assert fx.filter(pl.col("datadate") == date(2026, 10, 31)).is_empty()
