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


def test_dates_outside_every_interval_resolve_to_null(test_paths) -> None:
    """Before an issue's first recorded interval there is no correct value."""
    dates = [date(2020, 1, 2)]
    _write_raw(test_paths, _secd(dates, "67054R302"), _gsecd(dates), T3_HISTORY)

    out = _identifier_panel(test_paths).collect()
    assert out["cusip"].to_list() == [None]
