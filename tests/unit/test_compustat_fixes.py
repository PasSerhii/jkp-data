"""Tests for the Compustat North America history fixes (compustat_fixes.py) through their hooks.

1) Firm-shares fallback: scale the company's share count with the security's own adjustment
   factor at the report date (Berkshire class A: company factor 1,500 on the class B basis).
2) Monthly returns across a SECM/SECD switch: use SECM prices at both ends (Dec-1983).
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta

import polars as pl
import pytest

import jkp.data.aux_functions as aux
from jkp.data.compustat_fixes import correct_source_switch_returns

REPORT = date(1985, 12, 31)


def _secm_rows(
    gvkey: str, months: list[date], prices: list[float], ajexm: list[float]
) -> pl.DataFrame:
    n = len(months)
    return pl.DataFrame(
        {
            "gvkey": [gvkey] * n,
            "iid": ["01"] * n,
            "datadate": months,
            "tpci": ["0"] * n,
            "exchg": [11] * n,
            "dvpsxm": [0.0] * n,
            "curcdm": ["USD"] * n,
            "prccm": prices,
            "prchm": prices,
            "prclm": prices,
            "ajexm": ajexm,
            "cshom": pl.Series([None] * n, dtype=pl.Float64),
            "csfsm": pl.Series([None] * n, dtype=pl.Float64),
            "cshoq": pl.Series([None] * n, dtype=pl.Float64),
            "cshtrm": [1000.0] * n,
            "curcddvm": ["USD"] * n,
            "trfm": [1.0] * n,
        }
    )


def _firm_shares(rows: list[tuple[str, float, float]], days: list[date]) -> pl.DataFrame:
    """One company report (REPORT) per gvkey, valid on every listed day, as populate_own writes it."""
    return pl.DataFrame(
        [(d, g, REPORT, csho, ajex) for g, csho, ajex in rows for d in days],
        schema=["ddate", "gvkey", "datadate", "csho_fund", "ajex_fund"],
        orient="row",
    )


def _usd_fx(monkeypatch, days: list[date]) -> None:
    monkeypatch.setattr(
        aux,
        "compustat_fx",
        lambda _paths: pl.DataFrame(
            {"datadate": days, "curcdd": ["USD"] * len(days), "fx": [1.0] * len(days)}
        ),
    )


def test_secm_fallback_uses_security_adjustment_factor(test_paths, monkeypatch) -> None:
    """Berkshire-type share basis: the company factor (1,500, class B basis) no longer inflates a
    class A security whose own factor is 1; a normal split and a missing report month are unchanged."""
    months = [REPORT, date(1986, 1, 31), date(1986, 2, 28)]
    pl.concat(
        [
            # Berkshire-type: class A factor 1, company factor 1,500
            _secm_rows("002176", months, [2430.0, 2600.0, 2700.0], [1.0, 1.0, 1.0]),
            # 2:1 split in Jan-1986, company and security factors agree
            _secm_rows("001000", months, [40.0, 21.0, 22.0], [2.0, 1.0, 1.0]),
            # no SECM row at the report month
            _secm_rows("003000", months[1:], [5.0, 5.5], [1.0, 1.0]),
        ]
    ).write_parquet(test_paths.raw_tables_dir / "comp_secm.parquet")
    _firm_shares(
        [("002176", 1.147, 1500.0), ("001000", 10.0, 2.0), ("003000", 5.0, 3.0)], months
    ).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
    _usd_fx(monkeypatch, months)

    aux.gen_secm_data(test_paths)

    shares = (
        pl.read_parquet(test_paths.interim_dir / "secm_data.parquet")
        .filter(pl.col("datadate") == date(1986, 2, 28))
        .select("gvkey", "cshoc")
        .sort("gvkey")
    )
    got = dict(shares.iter_rows())
    assert got["002176"] == pytest.approx(1.147)  # was 1.147 * 1500 = 1,720.5
    assert got["001000"] == pytest.approx(20.0)  # split since the report: 10 * 2 / 1 either way
    # no security factor at the report month: the company factor is kept
    assert got["003000"] == pytest.approx(15.0)


def test_secd_fallback_uses_security_adjustment_factor(test_paths, monkeypatch) -> None:
    """The daily fallback (SECD rows without cshoc) scales with the security's own factor too."""
    days = [date(1986, 1, 2) + timedelta(days=i) for i in range(3)]
    n = len(days)
    daily = {
        "gvkey": ["002176"] * n,
        "iid": ["01"] * n,
        "datadate": days,
        "tpci": ["0"] * n,
        "exchg": [11] * n,
        "prcstd": [4] * n,
        "curcdd": ["USD"] * n,
        "prccd": [2440.0, 2450.0, 2460.0],
        "ajexdi": [1.0] * n,
        "prchd": [2450.0] * n,
        "prcld": [2430.0] * n,
        "prcod": [2440.0] * n,
        "cshtrd": [100.0] * n,
        "cshoc": pl.Series([None] * n, dtype=pl.Float64),
        "trfd": pl.Series([None] * n, dtype=pl.Float64),
        "curcddv": ["USD"] * n,
        "div": [0.0] * n,
        "divd": [0.0] * n,
        "divsp": [0.0] * n,
    }
    pl.DataFrame(daily).write_parquet(test_paths.raw_tables_dir / "comp_secd.parquet")
    pl.DataFrame(
        {
            **daily,
            "gvkey": ["200000"] * n,
            "exchg": [104] * n,
            "cshoc": [1e6] * n,
            "qunit": [1.0] * n,
        }
    ).write_parquet(test_paths.raw_tables_dir / "comp_g_secd.parquet")
    _secm_rows("002176", [REPORT], [2430.0], [1.0]).write_parquet(
        test_paths.raw_tables_dir / "comp_secm.parquet"
    )
    _firm_shares([("002176", 1.147, 1500.0)], days).write_parquet(
        test_paths.interim_dir / "__firm_shares2.parquet"
    )
    _usd_fx(monkeypatch, days)

    aux.gen_comp_dsf(test_paths)

    na = pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet").filter(
        pl.col("gvkey") == "002176"
    )
    assert na.height == n
    assert na["cshoc"].to_list() == pytest.approx([1.147] * n)  # was 1,720.5


# --- monthly returns across a SECM/SECD switch -------------------------------------------------


def _msf_row(gvkey: str, datadate: date, prcstd: int, ri: float) -> dict:
    return {
        "gvkey": gvkey,
        "iid": "01",
        "datadate": datadate,
        "eom": date(datadate.year, datadate.month, monthrange(datadate.year, datadate.month)[1]),
        "prcstd": prcstd,
        "ri": ri,
        "ri_local": ri,
        "curcdd": "USD",
    }


def _write_switch_inputs(
    test_paths,
    msf: list[dict],
    secd_eoms: list[tuple[str, date]],
    secm: list[tuple[str, date, float]],
) -> None:
    pl.DataFrame(msf).write_parquet(test_paths.interim_dir / "__comp_msf.parquet")
    pl.DataFrame(secd_eoms, schema=["gvkey", "eom"], orient="row").with_columns(
        iid=pl.lit("01")
    ).write_parquet(test_paths.interim_dir / "secd_data.parquet")
    pl.DataFrame(secm, schema=["gvkey", "datadate", "ri"], orient="row").with_columns(
        iid=pl.lit("01"), eom=pl.col("datadate"), ri_local=pl.col("ri")
    ).write_parquet(test_paths.interim_dir / "secm_data.parquet")


def _returns(test_paths) -> dict[tuple[str, date], float | None]:
    out = aux.gen_returns_df(test_paths, "m")
    return {(g, d): r for g, d, r in out.select("gvkey", "datadate", "ret").iter_rows()}


def test_return_at_secm_to_secd_switch_uses_secm_prices(test_paths) -> None:
    """BancWest Dec-1983: SECM Nov (trfm 2.4447) -> SECD Dec (trfd empty -> 1) gave -57.8% while the
    price rose 1.25%; the switch month now uses SECM at both ends and later SECD months are unchanged."""
    nov, dec, dec_d, jan = (
        date(1983, 11, 30),
        date(1983, 12, 31),
        date(1983, 12, 30),
        date(1984, 1, 31),
    )
    _write_switch_inputs(
        test_paths,
        msf=[
            _msf_row("004708", nov, 10, 40.0 / 16 * 2.4447),  # SECM
            _msf_row("004708", dec_d, 4, 41.25 / 16),  # SECD, trfd empty
            _msf_row("004708", jan, 4, 43.5 / 16),  # SECD
        ],
        secd_eoms=[("004708", dec), ("004708", jan)],
        secm=[
            ("004708", nov, 40.0 / 16 * 2.4447),
            ("004708", dec, 40.5 / 16 * 2.4447),
            ("004708", jan, 42.5 / 16 * 2.4447),
        ],
    )

    ret = _returns(test_paths)

    assert ret[("004708", dec_d)] == pytest.approx(
        40.5 / 40.0 - 1
    )  # was 41.25 / (40 * 2.4447) - 1 = -57.8%
    assert ret[("004708", jan)] == pytest.approx(43.5 / 41.25 - 1)  # SECD -> SECD: unchanged
    assert ret[("004708", nov)] is None


def test_returns_around_a_secd_gap_use_secm_both_ways(test_paths) -> None:
    """A month missing from SECD is filled from SECM: the returns into and out of it use SECM."""
    jan, feb, mar = date(1995, 1, 31), date(1995, 2, 28), date(1995, 3, 31)
    _write_switch_inputs(
        test_paths,
        msf=[
            _msf_row("005000", jan, 4, 10.0),
            _msf_row("005000", feb, 10, 11.0 * 3),
            _msf_row("005000", mar, 4, 12.0),
        ],
        secd_eoms=[("005000", jan), ("005000", mar)],
        secm=[("005000", jan, 10.0 * 3), ("005000", feb, 11.0 * 3), ("005000", mar, 12.0 * 3)],
    )

    ret = _returns(test_paths)

    assert ret[("005000", feb)] == pytest.approx(0.1)  # was 33 / 10 - 1 = +230%
    assert ret[("005000", mar)] == pytest.approx(12.0 / 11.0 - 1)  # was 12 / 33 - 1 = -64%


def test_switch_without_both_secm_months_gives_no_return(test_paths) -> None:
    """If SECM lacks either month of a switch, the return is missing rather than a cross-file ratio."""
    nov, dec, dec_d = date(1983, 11, 30), date(1983, 12, 31), date(1983, 12, 30)
    _write_switch_inputs(
        test_paths,
        msf=[_msf_row("006000", nov, 10, 5.0), _msf_row("006000", dec_d, 4, 2.0)],
        secd_eoms=[("006000", dec)],
        secm=[("006000", nov, 5.0)],
    )

    assert _returns(test_paths)[("006000", dec_d)] is None


def test_source_switch_correction_leaves_daily_data_alone(test_paths) -> None:
    frame = pl.LazyFrame(
        {
            "gvkey": ["1"],
            "iid": ["01"],
            "datadate": [date(2020, 1, 2)],
            "ret": [0.5],
            "ret_local": [0.5],
        }
    )
    assert correct_source_switch_returns(frame, test_paths, "d").collect().equals(frame.collect())
