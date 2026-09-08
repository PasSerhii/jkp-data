"""Point-in-time resolution of security identifiers in the production export."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from jkp.data.production import _identifier_history, _identifier_panel, _resolve_identifiers_asof

pytestmark = pytest.mark.unit

# T3 Defense (gvkey 022320, iid 01) as it appears in comp.sec_id_history: the
# CUSIP effective through 2026-07-25 is what May and June must report, while the
# security header now carries the post-reverse-split value.
T3_HISTORY = pl.DataFrame(
    {
        "gvkey": ["022320"] * 4,
        "iid": ["01"] * 4,
        "item": ["CUSIP", "CUSIP", "ISIN", "ISIN"],
        "itemvalue": ["67054R203", "67054R302", "US67054R2031", "US67054R3021"],
        "efffrom": [date(2024, 10, 24), date(2026, 7, 26), date(2024, 10, 24), date(2026, 7, 26)],
        "effthru": [date(2026, 7, 25), date(2900, 1, 1), date(2026, 7, 25), date(2900, 1, 1)],
        "source": ["wrds", "feed", "wrds", "feed"],
    },
    schema_overrides={"efffrom": pl.Date, "effthru": pl.Date},
)


def _write_raw(paths, secd: pl.DataFrame, gsecd: pl.DataFrame, history: pl.DataFrame | None):
    secd.write_parquet(paths.raw_tables_dir / "comp_secd.parquet")
    gsecd.write_parquet(paths.raw_tables_dir / "comp_g_secd.parquet")
    if history is not None:
        history.write_parquet(paths.raw_tables_dir / "comp_sec_id_history.parquet")


def _secd(dates: list[date], cusip: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "gvkey": ["022320"] * len(dates),
            "iid": ["01"] * len(dates),
            "datadate": dates,
            "cusip": [cusip] * len(dates),
            "conm": ["T3 DEFENSE INC"] * len(dates),
        },
        schema_overrides={"datadate": pl.Date},
    )


def _gsecd(dates: list[date]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "gvkey": ["022320"] * len(dates),
            "iid": ["01"] * len(dates),
            "datadate": dates,
            "sedol": [None] * len(dates),
            "isin": ["US67054R3021"] * len(dates),
            "conm": ["T3 DEFENSE INC"] * len(dates),
        },
        schema_overrides={"datadate": pl.Date, "sedol": pl.Utf8},
    )


def test_resolution_returns_the_interval_covering_each_date() -> None:
    keys = pl.DataFrame(
        {
            "gvkey": ["022320"] * 3,
            "iid": ["01"] * 3,
            "date": [date(2026, 5, 29), date(2026, 6, 30), date(2026, 7, 26)],
        },
        schema_overrides={"date": pl.Date},
    ).lazy()

    out = _resolve_identifiers_asof(T3_HISTORY.lazy(), keys, "date").collect().sort("date")

    assert out["cusip"].to_list() == ["67054R203", "67054R203", "67054R302"]
    assert out["isin_orig"].to_list() == ["US67054R2031", "US67054R2031", "US67054R3021"]


def test_panel_overrides_the_current_header_with_point_in_time_values(test_paths) -> None:
    """The header stamps today's CUSIP on every row; history must win."""
    dates = [date(2026, 5, 29), date(2026, 7, 26)]
    _write_raw(test_paths, _secd(dates, "67054R302"), _gsecd(dates), T3_HISTORY)

    out = _identifier_panel(test_paths).collect().sort("date")

    assert out["cusip"].to_list() == ["67054R203", "67054R302"]
    assert out["isin_orig"].to_list() == ["US67054R2031", "US67054R3021"]
    # conm has no historical source in the feed and stays header-stamped.
    assert out["conm"].to_list() == ["T3 DEFENSE INC"] * 2


def test_panel_falls_back_to_header_when_history_is_absent(test_paths) -> None:
    """An older raw set without the table must still produce output."""
    dates = [date(2026, 5, 29)]
    _write_raw(test_paths, _secd(dates, "67054R302"), _gsecd(dates), None)

    assert _identifier_history(test_paths) is None
    out = _identifier_panel(test_paths).collect()
    assert out["cusip"].to_list() == ["67054R302"]


def test_dates_outside_every_interval_fall_back_to_the_header(test_paths) -> None:
    """Before an issue's first recorded interval, keep the header value.

    The header is current-stamped and so may be the wrong vintage, but it is a
    real identifier. Emitting null instead would be worse: `_add_isin` then
    fabricates `excntry[:2] + cusip`, which is a valid ISIN only for
    US-domiciled issuers.
    """
    dates = [date(2020, 1, 2)]
    _write_raw(test_paths, _secd(dates, "67054R302"), _gsecd(dates), T3_HISTORY)

    out = _identifier_panel(test_paths).collect()
    assert out["cusip"].to_list() == ["67054R302"]


def test_history_supplies_isin_when_gsecd_has_none(test_paths) -> None:
    """The case that motivated this: a foreign issuer listed in the US.

    `g_secd` carries no ISIN for these, so `isin_orig` was null and the export
    synthesised `US` + cusip -- an identifier that resolves nowhere for an issuer
    domiciled in Cayman, Canada or Israel. The history has the real one.
    """
    dates = [date(2026, 5, 29)]
    history = pl.DataFrame(
        {
            "gvkey": ["034482", "034482"],
            "iid": ["01", "01"],
            "item": ["CUSIP", "ISIN"],
            "itemvalue": ["G45139121", "KYG451391216"],
            "efffrom": [date(2026, 5, 29), date(2026, 5, 29)],
            "effthru": [date(2900, 1, 1), date(2900, 1, 1)],
            "source": ["wrds", "wrds"],
        },
        schema_overrides={"efffrom": pl.Date, "effthru": pl.Date},
    )
    secd = pl.DataFrame(
        {
            "gvkey": ["034482"],
            "iid": ["01"],
            "datadate": dates,
            "cusip": ["G45139139"],
            "conm": ["HITEK GLOBAL INC"],
        },
        schema_overrides={"datadate": pl.Date},
    )
    gsecd = pl.DataFrame(
        {
            "gvkey": ["034482"],
            "iid": ["01"],
            "datadate": dates,
            "sedol": [None],
            "isin": [None],  # US-listed foreign issuer: no Global ISIN
            "conm": ["HITEK GLOBAL INC"],
        },
        schema_overrides={"datadate": pl.Date, "sedol": pl.Utf8, "isin": pl.Utf8},
    )
    _write_raw(test_paths, secd, gsecd, history)

    out = _identifier_panel(test_paths).collect()

    assert out["isin_orig"].to_list() == ["KYG451391216"]
    assert out["cusip"].to_list() == ["G45139121"]


def test_real_isin_wins_over_the_synthetic_fallback(test_paths) -> None:
    """End to end: a populated isin_orig must suppress the fabricated ISIN."""
    from jkp.data.production import _add_isin

    df = pl.DataFrame(
        {
            "excntry": ["USA", "USA"],
            "cusip": ["G45139121", "00846U101"],
            "isin_orig": ["KYG451391216", None],
        }
    )
    out = _add_isin(df).sort("cusip")

    got = dict(zip(out["cusip"].to_list(), out["isin"].to_list(), strict=True))
    # Real ISIN kept; the synthetic would have been the wrong "USG45139121x".
    assert got["G45139121"] == "KYG451391216"
    # No history for this one, so the synthetic is still used -- last resort.
    assert got["00846U101"].startswith("US00846U101")
