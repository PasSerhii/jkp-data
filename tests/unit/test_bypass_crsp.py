"""Tests for the CRSP-bypass (Compustat-only) build path.

These cover the ``bypass_crsp=True`` branches added to mirror the SAS
``bypass_crsp=1`` pipeline:

* ``combine_crsp_comp_sf`` builds the world files from Compustat only (no CRSP
  CTEs / UNION ALL, CRSP parquet inputs never read).
* ``nyse_size_cutoffs`` identifies NYSE via ``comp_exchg = 11`` instead of
  ``crsp_nyse = 1``.
* ``merge_industry_to_world_msf`` takes SIC/NAICS from Compustat only.
* ``add_rf_and_exchange_data_to_temporary_sf`` uses the FF risk-free rate with a
  last-month fallback instead of the CRSP 30y T-bill.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from jkp.data.aux_functions import (
    _date_where_clause,
    add_rf_and_exchange_data_to_temporary_sf,
    combine_crsp_comp_sf,
    comp_exchanges,
    merge_industry_to_world_msf,
    nyse_size_cutoffs,
)
from jkp.data.config import BYPASS_CRSP, PORTFOLIO_SETTINGS
from jkp.data.paths import DataPaths

pytestmark = pytest.mark.unit


def _make_paths(tmp_path: Path) -> DataPaths:
    """Create the DataPaths layout under tmp_path."""
    paths = DataPaths(base_dir=tmp_path)
    paths.interim_dir.mkdir(parents=True, exist_ok=True)
    (paths.interim_dir / "raw_data_dfs").mkdir(parents=True, exist_ok=True)
    paths.raw_tables_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)
    return paths


# ---------------------------------------------------------------------------
# Minimal Compustat-only fixtures for combine_crsp_comp_sf
# ---------------------------------------------------------------------------


def _write_comp_msf(interim: Path, n_gvkeys: int = 3) -> None:
    months = pl.date_range(date(2020, 1, 31), date(2020, 4, 30), "1mo", eager=True).to_list()
    rows = []
    for i in range(n_gvkeys):
        gk = f"{200000 + i:06d}"
        for j, d in enumerate(months):
            rows.append(
                {
                    "gvkey": gk,
                    "iid": "01",
                    "excntry": "USA",
                    "exch_main": 1,
                    "tpci": "0",
                    "primary_sec": 1,
                    "prcstd": 4,
                    "exchg": 11,
                    "curcdd": "USD",
                    "fx": 1.0,
                    "datadate": d,
                    "eom": d,
                    "ajexdi": 1.0,
                    "cshoc": 1000.0,
                    "me": 100.0 + i + j,
                    "prc": 10.0,
                    "prc_local": 10.0,
                    "prc_high": 11.0,
                    "prc_low": 9.0,
                    "dolvol": 5000.0,
                    "cshtrm": 500.0,
                    "ret": 0.01 * (j + 1),
                    "ret_local": 0.01 * (j + 1),
                    "ret_exc": 0.005 * (j + 1),
                    "ret_lag_dif": 1,
                    "div_tot": 0.0,
                    "div_cash": 0.0,
                    "div_spc": 0.0,
                }
            )
    pl.DataFrame(rows).cast({"datadate": pl.Date, "eom": pl.Date}).write_parquet(
        interim / "comp_msf.parquet"
    )


def _write_comp_dsf(interim: Path, n_gvkeys: int = 3) -> None:
    days = pl.date_range(date(2020, 1, 1), date(2020, 1, 10), "1d", eager=True).to_list()
    rows = []
    for i in range(n_gvkeys):
        gk = f"{200000 + i:06d}"
        for j, d in enumerate(days):
            rows.append(
                {
                    "gvkey": gk,
                    "iid": "01",
                    "excntry": "USA",
                    "exch_main": 1,
                    "tpci": "0",
                    "primary_sec": 1,
                    "prcstd": 4,
                    "curcdd": "USD",
                    "fx": 1.0,
                    "datadate": d,
                    "ajexdi": 1.0,
                    "cshoc": 1000.0,
                    "me": 100.0 + i + j,
                    "dolvol": 5000.0,
                    "cshtrd": 500.0,
                    "prc": 10.0,
                    "prc_high": 11.0,
                    "prc_low": 9.0,
                    "prc_open_lcl": 10.0,
                    "ret_local": 0.001 * (j + 1),
                    "ret": 0.001 * (j + 1),
                    "ret_exc": 0.0005 * (j + 1),
                    "ret_intraday": 0.0004 * (j + 1),
                    "ret_overnight": 0.0006 * (j + 1),
                    "ret_intraday_local": 0.0004 * (j + 1),
                    "ret_overnight_local": 0.0006 * (j + 1),
                    "ret_lag_dif": 1,
                }
            )
    pl.DataFrame(rows).cast({"datadate": pl.Date}).write_parquet(interim / "comp_dsf.parquet")


def test_combine_bypass_is_compustat_only(tmp_path: Path) -> None:
    """With bypass, world files build from Compustat only; CRSP inputs unused."""
    paths = _make_paths(tmp_path)
    _write_comp_msf(paths.interim_dir)
    _write_comp_dsf(paths.interim_dir)

    # Deliberately do NOT create crsp_msf.parquet / crsp_dsf.parquet.
    assert not (paths.interim_dir / "crsp_msf.parquet").exists()
    assert not (paths.interim_dir / "crsp_dsf.parquet").exists()

    combine_crsp_comp_sf(paths, bypass_crsp=True)

    msf = pl.read_parquet(paths.interim_dir / "__msf_world.parquet")
    dsf = pl.read_parquet(paths.interim_dir / "world_dsf.parquet")

    # Every row is Compustat-sourced.
    assert msf.height > 0
    assert dsf.height > 0
    assert (msf["source_crsp"] == 0).all()
    assert (dsf["source_crsp"] == 0).all()

    # CRSP-only identifiers are null (Compustat CTE emits NULL for these).
    assert msf["permno"].is_null().all()
    assert msf["permco"].is_null().all()
    assert msf["primaryexch"].is_null().all()
    assert msf["conditionaltype"].is_null().all()

    # Schema parity with the non-bypass output (key columns present).
    for c in ["id", "eom", "ret_exc_lead1m", "obs_main", "comp_exchg"]:
        assert c in msf.columns


# ---------------------------------------------------------------------------
# nyse_size_cutoffs
# ---------------------------------------------------------------------------


def test_nyse_size_cutoffs_bypass_uses_comp_exchg(tmp_path: Path) -> None:
    """Under bypass, NYSE membership comes from comp_exchg==11, not crsp_exchcd."""
    paths = _make_paths(tmp_path)
    eom = date(2020, 1, 31)
    df = pl.DataFrame(
        {
            # 3 NYSE-by-Compustat rows (comp_exchg=11) with crsp_nyse zero,
            # plus 2 non-NYSE rows that crsp logic would have missed/changed.
            "eom": [eom] * 5,
            "comp_exchg": [11, 11, 11, 12, 14],
            "crsp_nyse": [0, 0, 0, 0, 0],
            "obs_main": [1] * 5,
            "exch_main": [1] * 5,
            "primary_sec": [1] * 5,
            "common": [1] * 5,
            "me": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    ).cast({"eom": pl.Date})
    data_path = paths.interim_dir / "msf_for_cutoffs.parquet"
    df.write_parquet(data_path)

    nyse_size_cutoffs(paths, data_path, bypass_crsp=True)
    out = pl.read_parquet(paths.interim_dir / "nyse_cutoffs.parquet")

    assert out.height == 1
    # Only the 3 comp_exchg==11 rows are counted.
    assert out["n"][0] == 3


# ---------------------------------------------------------------------------
# merge_industry_to_world_msf
# ---------------------------------------------------------------------------


def test_merge_industry_bypass_no_crsp_ind(tmp_path: Path) -> None:
    """Under bypass, SIC/NAICS come from Compustat and crsp_ind is never read."""
    paths = _make_paths(tmp_path)
    eom = date(2020, 1, 31)
    pl.DataFrame(
        {
            "id": [1, 2],
            "gvkey": ["200000", "200001"],
            "permco": [None, None],
            "permno": [None, None],
            "eom": [eom, eom],
        }
    ).cast({"eom": pl.Date, "permco": pl.Int64, "permno": pl.Int64}).write_parquet(
        paths.interim_dir / "__msf_world.parquet"
    )
    pl.DataFrame(
        {
            "gvkey": ["200000", "200001"],
            "date": [eom, eom],
            "gics": [101010, 202020],
            "sic": [3711, 7372],
            "naics": [336111, 511210],
        }
    ).cast({"date": pl.Date}).write_parquet(paths.interim_dir / "comp_ind.parquet")

    # No crsp_ind.parquet exists.
    assert not (paths.interim_dir / "crsp_ind.parquet").exists()

    merge_industry_to_world_msf(paths, bypass_crsp=True)
    out = pl.read_parquet(paths.interim_dir / "__msf_world2.parquet").sort("gvkey")

    assert out["sic"].to_list() == [3711, 7372]
    assert out["naics"].to_list() == [336111, 511210]
    assert out["gics"].to_list() == [101010, 202020]


# ---------------------------------------------------------------------------
# Risk-free rate
# ---------------------------------------------------------------------------


def test_rf_bypass_ff_rate_with_last_month_fallback(tmp_path: Path) -> None:
    """ret_exc uses FF rf, falling back to the last available rf for new months."""
    paths = _make_paths(tmp_path)
    rdf = paths.interim_dir / "raw_data_dfs"

    # FF rf for Jan/Feb 2020 only; last_rf = 0.002 (Feb).
    pl.DataFrame(
        {
            "date": [date(2020, 1, 31), date(2020, 2, 29)],
            "rf": [0.001, 0.002],
        }
    ).cast({"date": pl.Date}).write_parquet(rdf / "ff_factors_monthly.parquet")

    # Minimal inputs for comp_exchanges().
    pl.DataFrame({"exchg": [11], "excntry": ["USA"]}).write_parquet(rdf / "__ex_country1.parquet")
    pl.DataFrame({"exchgdesc": ["NYSE"], "exchgcd": [11]}).write_parquet(
        rdf / "comp_r_ex_codes.parquet"
    )

    temp_sf = pl.DataFrame(
        {
            # Jan: rf matched (0.001); Mar: no FF row -> fallback to last_rf (0.002).
            "datadate": [date(2020, 1, 15), date(2020, 3, 15)],
            "ret": [0.05, 0.05],
            "exchg": [11, 11],
        }
    ).cast({"datadate": pl.Date})

    out = add_rf_and_exchange_data_to_temporary_sf(paths, "m", temp_sf, bypass_crsp=True)
    out = out.sort("datadate")

    assert "t30ret" not in out.columns
    assert out["ret_exc"].to_list() == pytest.approx([0.05 - 0.001, 0.05 - 0.002])


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------


def test_config_defaults_compustat_only() -> None:
    assert BYPASS_CRSP is True
    assert PORTFOLIO_SETTINGS["source"] == ["COMPUSTAT"]


# ---------------------------------------------------------------------------
# Download date-window filter
# ---------------------------------------------------------------------------


def test_date_where_clause() -> None:
    s, e = date(2018, 1, 1), date(2026, 5, 31)
    assert _date_where_clause(None, s, e) == ""
    assert _date_where_clause("datadate", None, None) == ""
    assert _date_where_clause("datadate", None, e) == "WHERE datadate <= '2026-05-31'"
    assert _date_where_clause("datadate", s, None) == "WHERE datadate >= '2018-01-01'"
    assert (
        _date_where_clause("datadate", s, e)
        == "WHERE datadate >= '2018-01-01' AND datadate <= '2026-05-31'"
    )


# ---------------------------------------------------------------------------
# Exchange main flag — US special-exchange override
# ---------------------------------------------------------------------------


def test_comp_exchanges_usa_special_exchange_override(tmp_path: Path) -> None:
    """US stocks on exchg 15/16/17/18/21 are main; same codes elsewhere are not."""
    paths = _make_paths(tmp_path)
    rdf = paths.interim_dir / "raw_data_dfs"

    pl.DataFrame(
        {
            "exchg": [15, 21, 290, 11],
            "excntry": ["USA", "GBR", "USA", "USA"],
        }
    ).write_parquet(rdf / "__ex_country1.parquet")
    pl.DataFrame(
        {
            "exchgdesc": ["A", "B", "C", "D"],
            "exchgcd": [15, 21, 290, 11],
        }
    ).write_parquet(rdf / "comp_r_ex_codes.parquet")

    out = comp_exchanges(paths).sort("exchg")
    flags = dict(zip(out["exchg"].to_list(), out["exch_main"].to_list(), strict=True))

    assert flags[15] == 1  # US special exchange -> overridden to main
    assert flags[21] == 0  # special exchange, non-US -> excluded
    assert flags[290] == 0  # US but not in the override set -> excluded
    assert flags[11] == 1  # ordinary US exchange -> main
