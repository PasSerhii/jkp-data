"""Tests for the alpha-beta production output layer (production.py)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from jkp.data.paths import DataPaths, get_production_monthly_columns
from jkp.data.production import (
    _add_isin,
    isin_check_digit,
    make_isin_from_cusip,
    market_min_price,
    market_volumes,
    save_daily_production_csv,
    save_main_production_csv,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# ISIN check digit (Luhn)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "isin",
    [
        "US0378331005",  # Apple
        "US5949181045",  # Microsoft
        "GB0002634946",  # BAE Systems
        "US38259P5089",  # Google (old)
    ],
)
def test_isin_check_digit_matches_known_isins(isin: str) -> None:
    body, expected = isin[:11], int(isin[11])
    assert isin_check_digit(body) == expected


def test_make_isin_from_cusip() -> None:
    assert make_isin_from_cusip("USA", "037833100") == "US0378331005"
    assert make_isin_from_cusip(None, "037833100") is None
    assert make_isin_from_cusip("USA", None) is None
    assert make_isin_from_cusip("USA", "short") is None  # wrong length


def test_add_isin_prefers_isin_orig() -> None:
    df = pl.DataFrame(
        {
            "excntry": ["USA", "USA"],
            "cusip": ["037833100", "594918104"],
            "isin_orig": ["US9999999999", None],
        }
    )
    out = _add_isin(df)
    # First row keeps the original ISIN; second is constructed from cusip.
    assert out["isin"].to_list() == ["US9999999999", "US5949181045"]


# ---------------------------------------------------------------------------
# Fixtures for analytics / writers
# ---------------------------------------------------------------------------


def _make_paths(tmp_path: Path) -> DataPaths:
    paths = DataPaths(base_dir=tmp_path)
    (paths.interim_dir / "raw_data_dfs").mkdir(parents=True, exist_ok=True)
    paths.raw_tables_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    return paths


def _write_fx(paths: DataPaths) -> None:
    # compustat_fx expands each (curcdd) obs forward to the next obs; provide a
    # wide ILS span so daily test dates are covered.
    pl.DataFrame(
        {
            "curcdd": ["ILS", "ILS"],
            "datadate": [date(2020, 1, 1), date(2021, 1, 1)],
            "fx": [0.25, 0.25],
        }
    ).cast({"datadate": pl.Date}).write_parquet(
        paths.interim_dir / "raw_data_dfs" / "__fx1.parquet"
    )


def _write_daily(paths: DataPaths) -> None:
    # One id, two months of daily data (Jan, Feb 2020), dolvol/prc known.
    rows = []
    for d, dv, prc in [
        (date(2020, 1, 10), 100.0, 10.0),
        (date(2020, 1, 20), 200.0, 8.0),
        (date(2020, 2, 10), 300.0, 6.0),
        (date(2020, 2, 20), 500.0, 4.0),
    ]:
        rows.append(
            {
                "id": 1,
                "excntry": "BRA",
                "date": d,
                "eom": date(d.year, d.month, 1),  # placeholder, fixed below
                "dolvol": dv,
                "prc": prc,
                "prc_high": prc + 1,
                "prc_low": prc - 1,
                "prc_open_lcl": prc,
                "fx": 0.5,
                "ret": 0.01,
                "ret_local": 0.02,
                "ret_exc": 0.005,
            }
        )
    df = pl.DataFrame(rows).cast({"date": pl.Date}).with_columns(eom=pl.col("date").dt.month_end())
    df.write_parquet(paths.interim_dir / "world_dsf.parquet")
    df.write_parquet(paths.interim_dir / "world_dsf_output.parquet")


def test_market_volumes_trailing_window(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    _write_fx(paths)
    _write_daily(paths)

    out = market_volumes(paths, 2).sort("eom")
    feb = out.filter(pl.col("eom") == date(2020, 2, 29))
    # 2-month window ending Feb covers all four daily obs.
    assert feb["avgdolvol"][0] == pytest.approx((100 + 200 + 300 + 500) / 4)
    assert feb["mediandolvol"][0] == pytest.approx(250.0)
    # ILS volume = dolvol / fx_ils (0.25).
    assert feb["avgilsvol"][0] == pytest.approx(((100 + 200 + 300 + 500) / 4) / 0.25)

    jan = out.filter(pl.col("eom") == date(2020, 1, 31))
    # Window ending Jan only sees the two January obs.
    assert jan["avgdolvol"][0] == pytest.approx(150.0)


def test_market_min_price_trailing_window(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    _write_fx(paths)
    _write_daily(paths)

    out = market_min_price(paths, 2).sort("eom")
    feb = out.filter(pl.col("eom") == date(2020, 2, 29))
    assert feb["min_prc"][0] == pytest.approx(4.0)  # min over all four days
    jan = out.filter(pl.col("eom") == date(2020, 1, 31))
    assert jan["min_prc"][0] == pytest.approx(8.0)  # min over January only


def test_trailing_analytics_use_pre_filter_daily_history(tmp_path: Path) -> None:
    """Earlier non-main days still belong to a later main issue's trailing window."""
    paths = _make_paths(tmp_path)
    _write_fx(paths)
    _write_daily(paths)

    # Model an issue becoming eligible only on the final day.  The production
    # output panel has one row, while the pre-filter panel retains its history.
    filtered = pl.read_parquet(paths.interim_dir / "world_dsf_output.parquet").tail(1)
    filtered.write_parquet(paths.interim_dir / "world_dsf_output.parquet")

    volumes = market_volumes(paths, 2).filter(pl.col("eom") == date(2020, 2, 29))
    assert volumes["avgdolvol"][0] == pytest.approx((100 + 200 + 300 + 500) / 4)
    assert volumes["mediandolvol"][0] == pytest.approx(250.0)

    prices = market_min_price(paths, 2).filter(pl.col("eom") == date(2020, 2, 29))
    assert prices["min_prc"][0] == pytest.approx(4.0)


def _write_monthly_and_ids(paths: DataPaths) -> None:
    pl.DataFrame(
        {
            "id": [1],
            "gvkey": ["100000"],
            "iid": ["01"],
            "comp_exchg": [11],
            "excntry": ["BRA"],
            "date": [date(2020, 1, 31)],
            "eom": [date(2020, 1, 31)],
            "me": [100.0],
            "me_company": [100.0],
            "me_lag1": [90.0],
            "ret_1_0": [0.05],
        }
    ).cast({"date": pl.Date, "eom": pl.Date, "comp_exchg": pl.Int64}).write_parquet(
        paths.interim_dir / "world_data_output.parquet"
    )
    # SECD / G_SECD are daily.  Jan 31 is deliberately absent so this fixture
    # also verifies the monthly writer uses the last trading-day identifiers.
    id_dates = [date(2020, 1, 10), date(2020, 1, 20)]
    n = len(id_dates)
    pl.DataFrame(
        {
            "gvkey": ["100000"] * n,
            "iid": ["01"] * n,
            "exchg": [11] * n,
            "datadate": id_dates,
            "cusip": ["037833100"] * n,
            "conm": ["TEST CO"] * n,
        }
    ).cast({"datadate": pl.Date}).write_parquet(paths.raw_tables_dir / "comp_secd.parquet")
    pl.DataFrame(
        {
            "gvkey": ["100000"] * n,
            "iid": ["01"] * n,
            "exchg": [11] * n,
            "datadate": id_dates,
            "sedol": ["1234567"] * n,
            "isin": [None] * n,
            "conm": ["TEST CO"] * n,
        }
    ).cast({"datadate": pl.Date, "isin": pl.Utf8}).write_parquet(
        paths.raw_tables_dir / "comp_g_secd.parquet"
    )


def test_save_main_production_csv_matches_column_order(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    _write_fx(paths)
    _write_daily(paths)
    _write_monthly_and_ids(paths)

    # production_years=0: this asserts column order, which the output window does
    # not touch. The fixture's only row is 2020-01, well outside the 3-year default.
    save_main_production_csv(paths, end_date=date(2026, 5, 31), production_years=0)
    out_file = paths.production_dir / "monthly" / "bra.csv"
    assert out_file.exists()
    # processed/output/ held a byte-identical second copy of every country CSV,
    # ~95 GiB per run, purely to present the SAS directory shape. It must stay gone.
    assert not (paths.processed_dir / "output").exists()
    header = out_file.read_text().splitlines()[0]
    cols = [c.strip('"') for c in header.split(",")]
    assert cols == get_production_monthly_columns()  # exact 455-col order
    # Read id columns as strings to preserve leading zeros (the CSV quotes them).
    df = pl.read_csv(
        out_file, schema_overrides={"cusip": pl.Utf8, "isin": pl.Utf8, "sedol": pl.Utf8}
    )
    # Identifiers were joined and ISIN constructed from cusip.
    assert df["conm"][0] == "TEST CO"
    assert df["cusip"][0] == "037833100"
    # excntry is BRA, so the constructed ISIN carries the BR prefix.
    assert df["isin"][0] == "BR0378331009"
    assert df["me_lag1"][0] == 90.0


def test_save_daily_production_csv_format(tmp_path: Path) -> None:
    paths = _make_paths(tmp_path)
    _write_fx(paths)
    _write_daily(paths)
    _write_monthly_and_ids(paths)

    # production_years=0 for the same reason as the monthly test above.
    save_daily_production_csv(paths, end_date=date(2026, 5, 31), production_years=0)
    out_file = paths.production_dir / "daily" / "bra.csv"
    assert out_file.exists()
    assert not (paths.processed_dir / "output").exists()
    df = pl.read_csv(
        out_file, schema_overrides={"cusip": pl.Utf8, "sedol": pl.Utf8, "isin": pl.Utf8}
    )
    assert df.columns == [
        "excntry",
        "id",
        "date",
        "ret",
        "prc_open",
        "prc_high",
        "prc_low",
        "prc_close",
        "ret_dollar",
        "ret_exc_dollar",
        "sedol",
        "cusip",
        "isin",
    ]
    # Local prices = USD / fx (fx=0.5 -> close = prc/0.5).
    row = df.sort("date").row(0, named=True)
    assert row["prc_close"] == pytest.approx(10.0 / 0.5)
    assert row["ret"] == pytest.approx(0.02)  # local return
    assert row["ret_dollar"] == pytest.approx(0.01)  # USD return
    assert row["sedol"] == "1234567"
