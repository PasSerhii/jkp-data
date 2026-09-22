"""Choosing between a company's Global and NA Compustat filings.

The SAS leaves this undefined — both rows are concatenated and a later
`proc sort nodupkey` discards one with no ORDER BY and no stable-sort
guarantee. Preferring Global unconditionally is deterministic but reliably
picks the weaker row for banks (sparse `FS` globally, populated `INDL` in NA)
and for dual filers whose Global row is still a stub.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from jkp.data.aux_functions import resolve_dual_package_rows

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


def test_bank_keeps_the_populated_na_filing() -> None:
    """UBS-shaped: `FS` globally has no capex/working capital, `INDL` in NA does."""
    out = _resolve(
        [
            _row("144496", "GLOBAL", at=1565028.0),
            _row("144496", "NA", at=1565028.0, act=900.0, lct=800.0, capx=2008.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "NA"
    assert out["capx"][0] == 2008.0


def test_stub_global_row_loses_to_complete_na_row() -> None:
    """Sunbelt-shaped: the Global filing has not caught up for this year."""
    out = _resolve(
        [
            _row("200264", "GLOBAL", at=22268.0),
            _row("200264", "NA", at=22268.0, act=2232.0, lct=2476.0, capx=1500.0, che=90.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "NA"


def test_equally_populated_rows_still_prefer_global() -> None:
    """Unchanged behaviour where the choice does not cost data."""
    out = _resolve(
        [
            _row("001000", "GLOBAL", at=100.0, act=50.0),
            _row("001000", "NA", at=100.0, act=50.0),
        ]
    )
    assert out.height == 1
    assert out["source"][0] == "GLOBAL"


def test_richer_global_row_is_kept() -> None:
    """The preference is completeness, not a blanket switch to NA."""
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
    # Odd companies have a populated NA capex and must resolve to NA.
    picked = dict(zip(out["gvkey"].to_list(), out["source"].to_list(), strict=True))
    assert picked["000001"] == "NA"
    assert picked["000002"] == "GLOBAL"


QKEY = ["gvkey", "fyr", "fyearq", "fqtr"]
QGROUP = ["gvkey", "fyr", "fyearq"]


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
        resolve_dual_package_rows(pl.DataFrame(rows).lazy(), key_cols=QKEY, group_cols=QGROUP)
        .collect()
        .sort(QKEY)
    )


def test_quarterly_choice_is_constant_within_fiscal_year() -> None:
    """BHP-shaped semi-annual reporter filed in both packages.

    Global carries (halved) values in all four quarters; NA carries the full
    half only in fqtr 2 and 4 but with one extra field there, so a per-quarter
    choice would alternate GLOBAL/NA/GLOBAL/NA and corrupt the trailing sums.
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


def test_quarterly_group_tie_prefers_global() -> None:
    rows = [
        _qrow("001005", s, 2025, q, atq=10.0, saley=float(q))
        for s in ("GLOBAL", "NA")
        for q in (1, 2, 3, 4)
    ]
    out = _resolve_q(rows)
    assert out.height == 4
    assert out["source"].unique().to_list() == ["GLOBAL"]


def test_quarterly_choice_can_differ_across_fiscal_years() -> None:
    """A stub Global year loses to NA without dragging the earlier, richer Global year."""
    rows = []
    for q in (1, 2, 3, 4):
        rows.append(_qrow("001006", "GLOBAL", 2024, q, atq=10.0, saley=float(q), capxy=1.0))
        rows.append(_qrow("001006", "NA", 2024, q, atq=10.0, saley=float(q)))
        rows.append(_qrow("001006", "GLOBAL", 2025, q, atq=11.0))
        rows.append(_qrow("001006", "NA", 2025, q, atq=11.0, saley=float(q), capxy=2.0))
    out = _resolve_q(rows)
    assert out.height == 8
    picked = dict(zip(out["fyearq"].to_list(), out["source"].to_list(), strict=True))
    assert picked == {2024: "GLOBAL", 2025: "NA"}
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
    assert out["source"][0] == "NA"
