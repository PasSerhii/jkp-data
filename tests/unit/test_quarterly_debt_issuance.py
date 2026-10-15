"""Quarterly long-term debt issuance (DLTIS) through standardized_accounting_data.

The four-quarter sum of quarterly flows listed ``dsq``, which is not a Compustat item, instead of
``dltisq``, and quarterly ``dltis`` was then set to null. Net long-term debt issuance from a
quarterly statement counted repayments only. The raw tables use Compustat's real column layout
(tests/fixtures/compustat_fund_columns.json) with made-up values shaped like Meta's: a $29,906m
bond in Q4 2025, repayments every quarter.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import jkp.data.aux_functions as aux

COLUMNS = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "compustat_fund_columns.json").read_text()
)
TYPES = {"f": pl.Float64, "i": pl.Int64, "d": pl.Date, "s": pl.String}
KEYS = {"gvkey": "170617", "indfmt": "INDL", "datafmt": "STD", "popsrc": "D", "consol": "C"}


def _table(name: str, rows: list[dict]) -> pl.DataFrame:
    return pl.from_dicts(rows, schema={c: TYPES[t] for c, t in COLUMNS[name]})


def _write_raw(test_paths, quarters: list[dict], years: list[dict]) -> Path:
    raw = test_paths.raw_tables_dir
    _table("comp_fundq", quarters).write_parquet(raw / "comp_fundq.parquet")
    _table("comp_funda", years).write_parquet(raw / "comp_funda.parquet")
    _table("comp_g_fundq", []).write_parquet(raw / "comp_g_fundq.parquet")
    _table("comp_g_funda", []).write_parquet(raw / "comp_g_funda.parquet")
    me_path = test_paths.interim_dir / "world_msf.parquet"
    pl.DataFrame(
        schema={
            "gvkey": pl.String,
            "eom": pl.Date,
            "primary_sec": pl.Int64,
            "common": pl.Int64,
            "obs_main": pl.Int64,
            "me_company": pl.Float64,
        }
    ).write_parquet(me_path)
    return me_path


# (datadate, fyearq, fqtr, dltisy, dltry): year-to-date debt issued and repaid
META_QUARTERS = [
    (date(2025, 3, 31), 2025, 1, 0.0, 751.0),
    (date(2025, 6, 30), 2025, 2, 0.0, 1225.0),
    (date(2025, 9, 30), 2025, 3, 0.0, 1770.0),
    (date(2025, 12, 31), 2025, 4, 29906.0, 2524.0),
    (date(2026, 3, 31), 2026, 1, 0.0, 843.0),
]


@pytest.fixture
def standardized(test_paths) -> tuple[pl.DataFrame, pl.DataFrame]:
    quarters = [
        {
            **KEYS,
            "curcdq": "USD",
            "fyr": 12,
            "datadate": d,
            "fyearq": fy,
            "fqtr": q,
            "dltisy": iss,
            "dltry": rep,
            "atq": 395250.0,
        }
        for d, fy, q, iss, rep in META_QUARTERS
    ]
    years = [
        {
            **KEYS,
            "curcd": "USD",
            "fyr": 12,
            "fyear": 2025,
            "datadate": date(2025, 12, 31),
            "dltis": 29906.0,
            "dltr": 2524.0,
            "at": 366021.0,
        }
    ]
    me_path = _write_raw(test_paths, quarters, years)

    aux.standardized_accounting_data(test_paths, "na", 0, me_path, 1, None)

    qtr = pl.read_parquet(test_paths.interim_dir / "acc_std_qtr.parquet")
    ann = pl.read_parquet(test_paths.interim_dir / "acc_std_ann.parquet")
    return qtr, ann


def test_quarterly_debt_issuance_is_summed_over_four_quarters(standardized) -> None:
    qtr, _ = standardized
    row = qtr.filter(pl.col("datadate") == date(2026, 3, 31)).row(0, named=True)
    assert row["dltis"] == pytest.approx(29906.0)  # Q2..Q4 2025 + Q1 2026
    assert row["dltr"] == pytest.approx(2616.0)  # 474 + 545 + 754 + 843


def test_quarterly_net_debt_issuance_counts_issuance(standardized) -> None:
    qtr, _ = standardized
    row = qtr.filter(pl.col("datadate") == date(2026, 3, 31)).row(0, named=True)
    assert row["dltnetis_x"] == pytest.approx(29906.0 - 2616.0)


def test_fiscal_q4_uses_the_year_to_date_value(standardized) -> None:
    """Without four quarters of history the TTM falls back to the year-to-date value at Q4."""
    qtr, _ = standardized
    row = qtr.filter(pl.col("datadate") == date(2025, 12, 31)).row(0, named=True)
    assert row["dltis"] == pytest.approx(29906.0)
    assert row["dltr"] == pytest.approx(2524.0)


def test_annual_debt_issuance_is_unchanged(standardized) -> None:
    _, ann = standardized
    row = ann.filter(pl.col("datadate") == date(2025, 12, 31)).row(0, named=True)
    assert row["dltis"] == pytest.approx(29906.0)
    assert row["dltnetis_x"] == pytest.approx(29906.0 - 2524.0)


def test_quarterly_panel_has_no_leftover_debt_columns(standardized) -> None:
    """The placeholder ``ds`` and the unsummed ``dltisq``/``dltisy`` are gone, as for every other flow."""
    qtr, _ = standardized
    assert not {"ds", "dltisq", "dltisy"} & set(qtr.columns)
