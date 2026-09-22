"""Trailing-4-quarter sums must come from one Compustat package.

Global halves a semi-annual reporter's flows across two quarters, NA carries
the full half in fqtr 2 and 4 only. A window that mixes the two packages sums
full halves with half-halves (BHP FY2026 sales: 83,514 against 58,760), so
``cumulate_4q`` nulls it instead.
"""

from __future__ import annotations

import polars as pl
import pytest

from jkp.data.aux_functions import cumulate_4q, quarterize, resolve_dual_package_rows

pytestmark = pytest.mark.unit


def _frame(rows: list[dict]) -> pl.DataFrame:
    base = {"gvkey": "013312", "fyr": 6, "fyearq": 2026, "curcdq": "USD", "source": "GLOBAL"}
    return pl.DataFrame([{**base, **row} for row in rows]).sort(["gvkey", "fyr", "fyearq", "fqtr"])


def _sale(df: pl.DataFrame) -> list[float | None]:
    return cumulate_4q(df.lazy(), ["saleq"]).collect()["sale"].to_list()


def test_ttm_sums_four_same_source_quarters() -> None:
    df = _frame(
        [
            {"fqtr": 1, "saleq": 13951.0, "saley": 13951.0},
            {"fqtr": 2, "saleq": 13951.0, "saley": 27902.0},
            {"fqtr": 3, "saleq": 15429.0, "saley": 43331.0},
            {"fqtr": 4, "saleq": 15429.0, "saley": 58760.0},
        ]
    )
    assert _sale(df) == [None, None, None, 58760.0]


def test_ttm_is_null_when_source_alternates() -> None:
    """The BHP regression: NA full halves interleaved with Global half-halves."""
    df = _frame(
        [
            {"fyearq": 2025, "fqtr": 4, "source": "NA", "saleq": 26232.0, "saley": 51630.0},
            {"fqtr": 1, "source": "GLOBAL", "saleq": 13951.0, "saley": 13951.0},
            {"fqtr": 2, "source": "NA", "saleq": 27902.0, "saley": 27902.0},
            {"fqtr": 3, "source": "GLOBAL", "saleq": 15429.0, "saley": 43331.0},
            {"fqtr": 4, "source": "NA", "saleq": 31372.0, "saley": 59274.0},
        ]
    )
    out = _sale(df)
    assert 83514.0 not in out
    # Both fiscal-year-ends fall back to their own ytd value; the three FY2026
    # windows that mix packages are nulled instead of summed.
    assert out == [51630.0, None, None, None, 59274.0]


def test_single_flip_nulls_the_windows_that_span_it() -> None:
    df = _frame(
        [
            {"fyearq": 2025, "fqtr": q, "source": "NA", "saleq": 10.0, "saley": 10.0 * q}
            for q in (1, 2, 3, 4)
        ]
        + [{"fqtr": q, "source": "GLOBAL", "saleq": 20.0, "saley": 20.0 * q} for q in (1, 2, 3, 4)]
    )
    assert _sale(df) == [None, None, None, 40.0, None, None, None, 80.0]


def test_fqtr4_falls_back_to_ytd() -> None:
    df = _frame([{"fqtr": 4, "saleq": 7.0, "saley": 30.0}])
    assert _sale(df) == [30.0]


def test_curcdq_change_still_nulls() -> None:
    df = _frame(
        [
            {"fqtr": 1, "saleq": 1.0, "saley": 1.0},
            {"fqtr": 2, "saleq": 1.0, "saley": 2.0},
            {"fqtr": 3, "saleq": 1.0, "saley": 3.0, "curcdq": "AUD"},
            {"fqtr": 4, "saleq": 1.0, "saley": 4.0, "curcdq": "AUD"},
        ]
    )
    # The window mixes currencies; fqtr 4 still falls back to its own ytd.
    assert _sale(df) == [None, None, None, 4.0]


def test_frame_without_source_column_still_works() -> None:
    df = _frame(
        [{"fqtr": q, "saleq": 5.0, "saley": 5.0 * q} for q in (1, 2, 3, 4)]
        + [{"fyearq": 2027, "fqtr": 1, "saleq": 5.0, "saley": 5.0}]
    ).drop("source")
    assert _sale(df) == [None, None, None, 20.0, 20.0]


def test_cross_package_ytd_difference_cannot_contaminate_later_same_source_ttm() -> None:
    """Q2 must stay null when its YTD predecessor belongs to another package.

    Without the quarterize guard, Q2 becomes 20 - 100 = -80. That corrupt
    value survives the TTM source check at next year's Q1, yielding -50.
    """
    df = _frame(
        [
            {"fqtr": 1, "saley": 100.0},
            {"fqtr": 2, "source": "NA", "saley": 20.0},
            {"fqtr": 3, "source": "NA", "saley": 30.0},
            {"fqtr": 4, "source": "NA", "saley": 40.0},
            {"fyearq": 2027, "fqtr": 1, "source": "NA", "saley": 10.0},
            {"fyearq": 2027, "fqtr": 2, "source": "NA", "saley": 20.0},
        ]
    )
    quarters = quarterize(df.lazy(), ["saley"]).collect().rename({"saley_q": "saleq"})
    assert quarters["saleq"].to_list() == [100.0, None, 10.0, 10.0, 10.0, 10.0]
    # Preserve the Q4 annual fallback; a clean trailing window becomes valid again.
    assert _sale(quarters) == [None, None, None, 40.0, None, 40.0]


def test_global_preference_then_quarterization_preserves_semiannual_total() -> None:
    """Exercise selection and flow arithmetic together for the BHP-shaped case."""
    rows = []
    for fqtr, saley in ((1, 13951.0), (2, 27902.0), (3, 43331.0), (4, 58760.0)):
        rows.append({"fqtr": fqtr, "saley": saley, "capxy": None})
    for fqtr, saley in ((1, None), (2, 27902.0), (3, None), (4, 59274.0)):
        rows.append({"fqtr": fqtr, "source": "NA", "saley": saley, "capxy": 1000.0})
    selected = resolve_dual_package_rows(
        _frame(rows).lazy(), key_cols=["gvkey", "fyr", "fyearq", "fqtr"]
    )
    quarters = quarterize(selected, ["saley"]).collect().rename({"saley_q": "saleq"})
    assert quarters["source"].to_list() == ["GLOBAL"] * 4
    assert quarters["saleq"].to_list() == [13951.0, 13951.0, 15429.0, 15429.0]
    # This is the fiscal-year-end total, not an August publication-date expectation.
    assert _sale(quarters) == [None, None, None, 58760.0]
