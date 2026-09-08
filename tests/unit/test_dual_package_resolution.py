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
