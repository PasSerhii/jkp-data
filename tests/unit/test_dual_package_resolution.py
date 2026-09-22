"""Upstream Global-first selection preserves periods without scoring completeness."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from jkp.data.aux_functions import accounting_public_start, resolve_dual_package_rows

pytestmark = pytest.mark.unit

KEY = ["gvkey", "datadate"]


def _row(gvkey: str, source: str, **values) -> dict:
    base = {
        "gvkey": gvkey,
        "datadate": date(2025, 12, 31),
        "source": source,
        "curcd": "USD",
        "n": 0,
        "at": None,
        "act": None,
        "lct": None,
        "capx": None,
        "che": None,
    }
    base.update(values)
    return base


def _resolve(rows: list[dict]) -> pl.DataFrame:
    return resolve_dual_package_rows(pl.DataFrame(rows).lazy(), key_cols=KEY).collect()


def test_bank_keeps_global_definition_despite_richer_na_filing() -> None:
    """Extra NA INDL fields do not override a Global FS statement."""
    out = _resolve(
        [
            _row("144496", "GLOBAL", at=1565028.0),
            _row("144496", "NA", at=1565028.0, act=900.0, lct=800.0, capx=2008.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"
    assert out["capx"][0] is None


def test_sparse_global_row_is_retained_over_complete_na_row() -> None:
    """Accept upstream's coverage tradeoff instead of choosing by completeness."""
    out = _resolve(
        [
            _row("200264", "GLOBAL", at=22268.0),
            _row("200264", "NA", at=22268.0, act=2232.0, lct=2476.0, capx=1500.0, che=90.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"


def test_equally_populated_rows_still_prefer_global() -> None:
    out = _resolve(
        [
            _row("001000", "GLOBAL", at=100.0, act=50.0),
            _row("001000", "NA", at=100.0, act=50.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"


def test_richer_global_row_is_kept() -> None:
    out = _resolve(
        [
            _row("001001", "GLOBAL", at=100.0, act=50.0, capx=7.0),
            _row("001001", "NA", at=100.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"


def test_single_source_rows_pass_through_untouched() -> None:
    out = _resolve([_row("001002", "GLOBAL", at=10.0), _row("001003", "NA", at=20.0)])
    assert out.height == 2
    assert set(out["gvkey"].to_list()) == {"001002", "001003"}


def test_one_row_per_key_across_many_companies() -> None:
    rows = []
    for i in range(50):
        gv = f"{i:06d}"
        rows.append(_row(gv, "GLOBAL", at=float(i)))
        rows.append(_row(gv, "NA", at=float(i), capx=float(i) if i % 2 else None))
    out = _resolve(rows)
    assert out.height == 50
    assert out.group_by(KEY).len()["len"].max() == 1
    assert out["source"].unique().to_list() == ["GLOBAL"]


QKEY = ["gvkey", "fyr", "fyearq", "fqtr"]


def _qrow(gvkey: str, source: str, fyearq: int, fqtr: int, **values) -> dict:
    base = {
        "gvkey": gvkey,
        "fyr": 6,
        "fyearq": fyearq,
        "fqtr": fqtr,
        "source": source,
        "curcdq": "USD",
        "n": 0,
        "atq": None,
        "saley": None,
        "capxy": None,
    }
    base.update(values)
    return base


def _resolve_q(rows: list[dict]) -> pl.DataFrame:
    return (
        resolve_dual_package_rows(
            pl.DataFrame(
                rows,
                schema_overrides={"atq": pl.Float64, "saley": pl.Float64, "capxy": pl.Float64},
            ).lazy(),
            key_cols=QKEY,
        )
        .collect()
        .sort(QKEY)
    )


def test_dual_filed_quarters_keep_global_values() -> None:
    """BHP-shaped semi-annual reporter filed in both packages.

    Global carries (halved) values in all four quarters; NA carries the full
    half only in fqtr 2 and 4 but with one extra field there, so a per-quarter
    completeness vote would alternate GLOBAL/NA/GLOBAL/NA and corrupt trailing sums.
    """
    rows = []
    for fqtr, saley in ((1, 13951.0), (2, 27902.0), (3, 43331.0), (4, 58760.0)):
        rows.append(_qrow("013312", "GLOBAL", 2026, fqtr, atq=121387.0, saley=saley))
    rows.append(_qrow("013312", "NA", 2026, 1))
    rows.append(_qrow("013312", "NA", 2026, 2, atq=116012.0, saley=27902.0, capxy=5000.0))
    rows.append(_qrow("013312", "NA", 2026, 3))
    rows.append(_qrow("013312", "NA", 2026, 4, atq=121387.0, saley=59274.0, capxy=10000.0))
    out = _resolve_q(rows)
    assert out.height == 4
    assert out.group_by(QKEY).len()["len"].max() == 1
    assert out["source"].n_unique() == 1
    assert out["source"][0] == "GLOBAL"
    assert out["saley"].to_list() == [13951.0, 27902.0, 43331.0, 58760.0]


@pytest.mark.parametrize("sources", [("GLOBAL", "NA"), ("NA", "GLOBAL")])
def test_quarterly_selection_is_independent_of_input_order(sources) -> None:
    rows = [
        _qrow("001005", s, 2025, q, atq=10.0, saley=float(q)) for s in sources for q in (1, 2, 3, 4)
    ]
    out = _resolve_q(rows)
    assert out.height == 4
    assert out["source"].unique().to_list() == ["GLOBAL"]


def test_global_preference_is_unchanged_when_na_becomes_richer() -> None:
    rows = []
    for q in (1, 2, 3, 4):
        rows.append(_qrow("001006", "GLOBAL", 2024, q, atq=10.0, saley=float(q), capxy=1.0))
        rows.append(_qrow("001006", "NA", 2024, q, atq=10.0, saley=float(q)))
        rows.append(_qrow("001006", "GLOBAL", 2025, q, atq=11.0))
        rows.append(_qrow("001006", "NA", 2025, q, atq=11.0, saley=float(q), capxy=2.0))
    out = _resolve_q(rows)
    assert out.height == 8
    picked = dict(zip(out["fyearq"].to_list(), out["source"].to_list(), strict=True))
    assert picked == {2024: "GLOBAL", 2025: "GLOBAL"}
    assert out.filter(pl.col("fyearq") == 2025)["source"].n_unique() == 1


def test_quarterly_key_is_supported() -> None:
    rows = [
        {
            "gvkey": "001004",
            "fyr": 12,
            "fyearq": 2025,
            "fqtr": 4,
            "source": s,
            "curcdq": "USD",
            "atq": 10.0,
            "capxq": c,
        }
        for s, c in (("GLOBAL", None), ("NA", 3.0))
    ]
    out = resolve_dual_package_rows(
        pl.DataFrame(rows).lazy(), key_cols=["gvkey", "fyr", "fyearq", "fqtr"]
    ).collect()
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"


def test_later_quarter_does_not_change_an_earlier_public_observation() -> None:
    """An August Q2 filing must not choose a different Q1 source for July."""
    q1_metadata = {
        "fyr": 12,
        "datadate": date(2025, 3, 31),
        "availability_date": date(2025, 5, 1),
    }
    rows = [
        _qrow("001007", "GLOBAL", 2025, 1, atq=100.0, saley=10.0, **q1_metadata),
        _qrow("001007", "NA", 2025, 1, saley=20.0, **q1_metadata),
    ]
    before = _resolve_q(rows).with_columns(accounting_public_start(4))
    q2_metadata = {
        "fyr": 12,
        "datadate": date(2025, 6, 30),
        "availability_date": date(2025, 8, 1),
    }
    rows += [
        _qrow("001007", "GLOBAL", 2025, 2, **q2_metadata),
        _qrow("001007", "NA", 2025, 2, atq=200.0, saley=40.0, capxy=2.0, **q2_metadata),
    ]
    after = _resolve_q(rows).with_columns(accounting_public_start(4)).filter(pl.col("fqtr") == 1)
    assert_frame_equal(before, after)
    assert after["start_date"].to_list() == [date(2025, 7, 31)]
    assert after["source"].to_list() == ["GLOBAL"]


def test_newer_na_only_quarter_survives_richer_global_earlier_quarters() -> None:
    """Choosing Global for Q1/Q2 must not discard NA's unique Q3 balance sheet."""
    rows = [
        _qrow("001008", "GLOBAL", 2025, q, atq=100.0, saley=10.0 * q, capxy=2.0 * q) for q in (1, 2)
    ] + [_qrow("001008", "NA", 2025, q, atq=200.0, saley=20.0 * q) for q in (1, 2, 3)]
    out = _resolve_q(rows)
    assert out["fqtr"].to_list() == [1, 2, 3]
    assert out["source"].to_list() == ["GLOBAL", "GLOBAL", "NA"]
    assert out["atq"].to_list() == [100.0, 100.0, 200.0]
    assert out["saley"].to_list() == [10.0, 20.0, 60.0]


def test_annual_na_only_period_survives_global_other_year() -> None:
    out = _resolve(
        [
            _row("001009", "GLOBAL", at=100.0),
            _row("001009", "NA", at=200.0),
            _row("001009", "NA", datadate=date(2026, 12, 31), at=300.0),
        ]
    ).sort(KEY)
    assert out["source"].to_list() == ["GLOBAL", "NA"]
    assert out["at"].to_list() == [100.0, 300.0]
