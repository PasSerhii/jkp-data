"""Tests for the Compustat price-history fixes (compustat_fixes.py) through their hooks.

1) Firm-shares fallback: scale the company's share count with the security's own adjustment
   factor at the report date (Berkshire class A: company factor 1,500 on the class B basis).
2) Monthly returns across a SECM/SECD switch: use SECM prices at both ends (Dec-1983).
3) Splits missing from Compustat's adjustment factor: restore them from Capital IQ's split
   history (Mullen Automotive, 1-for-100 on 21 Dec 2023).
"""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta

import ibis
import polars as pl
import pytest

import jkp.data.aux_functions as aux
from jkp.data.compustat_fixes import (
    SPLIT_EVENT_SCHEMA,
    SPLIT_EVENTS_TABLE,
    build_split_events_query,
    correct_source_switch_returns,
    download_split_events_attached,
    register_security_ajex_at_report,
)

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
        schema={
            "ddate": pl.Date,
            "gvkey": pl.String,
            "datadate": pl.Date,
            "csho_fund": pl.Float64,
            "ajex_fund": pl.Float64,
        },
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


JAN86 = date(1986, 1, 31)


def _run_daily_fallback(
    test_paths,
    monkeypatch,
    days: list[date],
    ajexdi: list[float],
    secm: list[tuple[date, float]],
    company: tuple[float, float] = (1.147, 1500.0),
) -> list[float]:
    """One security without its own daily share count (SECD rows on ``days`` with ``ajexdi``, SECM
    month-end factors ``secm``); the company reports ``company`` = (shares, company factor) at
    REPORT. Returns the daily cshoc that gen_comp_dsf infers."""
    n = len(days)
    daily = {
        "gvkey": ["002176"] * n,
        "iid": ["01"] * n,
        "datadate": days,
        "tpci": ["0"] * n,
        "exchg": [11] * n,
        "prcstd": [4] * n,
        "curcdd": ["USD"] * n,
        "prccd": [100.0] * n,
        "ajexdi": ajexdi,
        "prchd": [101.0] * n,
        "prcld": [99.0] * n,
        "prcod": [100.0] * n,
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
            "ajexdi": [1.0] * n,
            "cshoc": [1e6] * n,
            "qunit": [1.0] * n,
        }
    ).write_parquet(test_paths.raw_tables_dir / "comp_g_secd.parquet")
    _secm_rows(
        "002176", [d for d, _ in secm], [100.0] * len(secm), [f for _, f in secm]
    ).write_parquet(test_paths.raw_tables_dir / "comp_secm.parquet")
    _firm_shares([("002176", *company)], days).write_parquet(
        test_paths.interim_dir / "__firm_shares2.parquet"
    )
    _usd_fx(monkeypatch, days)

    aux.gen_comp_dsf(test_paths)

    na = (
        pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet")
        .filter(pl.col("gvkey") == "002176")
        .sort("datadate")
    )
    assert na.height == n
    return na["cshoc"].to_list()


def test_secd_fallback_uses_security_adjustment_factor(test_paths, monkeypatch) -> None:
    """The daily fallback (SECD rows without cshoc) scales with the security's own factor too."""
    days = [REPORT] + [date(1986, 1, 2) + timedelta(days=i) for i in range(3)]
    cshoc = _run_daily_fallback(
        test_paths, monkeypatch, days, [1.0] * 4, [(REPORT, 1.0), (JAN86, 1.0)]
    )

    assert cshoc == pytest.approx([1.147] * 4)  # was 1,720.5


def test_secd_fallback_never_divides_a_secm_factor_by_a_secd_one(test_paths, monkeypatch) -> None:
    """GalaGen 1998: SECM carried ajexm 1.001 while SECD's ajexdi was 0.000001. Dividing a SECM
    factor by a SECD one made its market cap a million times too large ($9 trillion)."""
    days = [REPORT] + [date(1986, 1, 2) + timedelta(days=i) for i in range(3)]
    cshoc = _run_daily_fallback(
        test_paths, monkeypatch, days, [0.000001] * 4, [(REPORT, 1.001), (JAN86, 1.001)]
    )

    assert cshoc == pytest.approx([1.147] * 4)


def test_secd_fallback_does_not_need_secd_in_the_report_month(test_paths, monkeypatch) -> None:
    """Berkshire had no SECD rows from 1985 to 1987; taking the report-month factor from SECD would
    fall back to the company factor there and bring the 1,500x back. SECM covers every month."""
    days = [date(1986, 1, 2) + timedelta(days=i) for i in range(3)]
    cshoc = _run_daily_fallback(
        test_paths, monkeypatch, days, [1.0] * 3, [(REPORT, 1.0), (JAN86, 1.0)]
    )

    assert cshoc == pytest.approx([1.147] * 3)


def test_secd_fallback_follows_a_split_within_the_month(test_paths, monkeypatch) -> None:
    """A 2:1 split on 15 Jan 1986: days before it keep the pre-split count, days after double it."""
    days = [REPORT, date(1986, 1, 2), date(1986, 1, 20), JAN86]
    cshoc = _run_daily_fallback(
        test_paths,
        monkeypatch,
        days,
        [2.0, 2.0, 1.0, 1.0],
        [(REPORT, 2.0), (JAN86, 1.0)],
        company=(1.0, 2.0),
    )

    assert cshoc == pytest.approx([1.0, 1.0, 2.0, 2.0])


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


# --- splits missing from Compustat's adjustment factor ----------------------------------------

EX = date(2023, 12, 21)


def _weekdays(start: date, end: date) -> list[date]:
    return [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ]


DAYS = _weekdays(date(2023, 11, 1), date(2024, 1, 31))


def _prices(before: float, after: float, ex: date = EX, days: list[date] = DAYS) -> list[float]:
    """A price path that wobbles by up to 4% a day and jumps from ``before`` to ``after`` at ``ex``."""
    return [(before if d < ex else after) * (1 + 0.02 * (i % 5 - 2)) for i, d in enumerate(days)]


def _secd(
    gvkey: str,
    prices: list[float],
    ajexdi: list[float],
    trfd: list[float] | None = None,
    *,
    days: list[date] = DAYS,
    global_file: bool = False,
) -> pl.DataFrame:
    n = len(days)
    data = {
        "gvkey": [gvkey] * n,
        "iid": ["01W" if global_file else "01"] * n,
        "datadate": days,
        "tpci": ["0"] * n,
        "exchg": [104 if global_file else 14] * n,
        "prcstd": [4] * n,
        "curcdd": ["USD"] * n,
        "prccd": prices,
        "ajexdi": ajexdi,
        "prchd": prices,
        "prcld": prices,
        "prcod": prices,
        "cshtrd": [1000.0] * n,
        "cshoc": [1e6] * n,
        "trfd": pl.Series(trfd if trfd is not None else [None] * n, dtype=pl.Float64),
        "curcddv": ["USD"] * n,
        "div": [0.0] * n,
        "divd": [0.0] * n,
        "divsp": [0.0] * n,
    }
    if global_file:
        data["qunit"] = [100.0] * n  # quoted per 100 units, as for pence-quoted lines
    return pl.DataFrame(data)


def _write_split_events(test_paths, events: list[tuple[str, str, date, float]]) -> None:
    pl.DataFrame(events, schema=SPLIT_EVENT_SCHEMA, orient="row").write_parquet(
        test_paths.raw_table_source(SPLIT_EVENTS_TABLE)
    )


def _daily(
    test_paths,
    monkeypatch,
    na: pl.DataFrame,
    events: list[tuple[str, str, date, float]] | None,
    gl: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Run gen_comp_dsf on one SECD and one G_SECD frame; ``events=None`` means no split history.
    Returns __comp_dsf with each row's daily return from the return index."""
    days = na["datadate"].to_list()
    if gl is None:
        gl = _secd("200000", [10.0] * len(days), [1.0] * len(days), days=days, global_file=True)
    na.write_parquet(test_paths.raw_tables_dir / "comp_secd.parquet")
    gl.write_parquet(test_paths.raw_tables_dir / "comp_g_secd.parquet")
    _firm_shares([], days).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
    _usd_fx(monkeypatch, sorted(set(days) | set(gl["datadate"].to_list())))
    split_events = test_paths.raw_table_source(SPLIT_EVENTS_TABLE)
    split_events.unlink(missing_ok=True)
    if events is not None:
        _write_split_events(test_paths, events)

    aux.gen_comp_dsf(test_paths)

    return (
        pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet")
        .sort("gvkey", "iid", "datadate")
        .with_columns(ret=pl.col("ri") / pl.col("ri").shift(1).over("gvkey", "iid") - 1)
    )


def _security(frame: pl.DataFrame, gvkey: str) -> pl.DataFrame:
    return frame.filter(pl.col("gvkey") == gvkey)


def test_split_missing_from_the_factor_is_restored(test_paths, monkeypatch) -> None:
    """Mullen Automotive, 1-for-100 on 21 Dec 2023: Compustat holds the factor at 1.6e-7 on both
    sides, so the split day returned the price ratio (about +8,900%). Restored from Capital IQ, the
    day returns its true move and no other return changes."""
    prices = _prices(0.10, 9.0)
    secd = _secd("040116", prices, [1.6e-7] * len(DAYS))

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "040116")
    fixed = _security(_daily(test_paths, monkeypatch, secd, [("040116", "01", EX, 0.01)]), "040116")

    k = DAYS.index(EX)
    assert raw["ret"][k] == pytest.approx(prices[k] / prices[k - 1] - 1)  # +8,900%
    assert fixed["ret"][k] == pytest.approx(prices[k] / (prices[k - 1] * 100) - 1)  # -12%
    others = [i for i in range(1, len(DAYS)) if i != k]
    assert fixed["ret"].gather(others).to_list() == pytest.approx(
        raw["ret"].gather(others).to_list(), rel=1e-12
    )
    # only the return index moves: the factor, share counts and market caps stay as delivered
    for column in ("ajexdi", "cshoc", "me", "prc"):
        assert fixed[column].to_list() == raw[column].to_list()


def test_split_without_capital_iq_history_keeps_compustat_factor(test_paths, monkeypatch) -> None:
    """An empty split history (a WRDS source has no Capital IQ tables) changes nothing."""
    secd = _secd("040116", _prices(0.10, 9.0), [1.6e-7] * len(DAYS))

    raw = _daily(test_paths, monkeypatch, secd, None)
    empty = _daily(test_paths, monkeypatch, secd, [])

    assert empty.select("gvkey", "datadate", "ajexdi", "ri").equals(
        raw.select("gvkey", "datadate", "ajexdi", "ri")
    )


def _shift_after(days: list[date], ex: date, trading_days: int) -> date:
    return days[days.index(ex) + trading_days]


@pytest.mark.parametrize(
    ("prices", "ajexdi", "trfd", "rate"),
    [
        pytest.param(
            _prices(0.10, 9.0),
            [1.6e-9 if d < EX else 1.6e-7 for d in DAYS],
            None,
            0.01,
            id="compustat-applied-it",
        ),
        pytest.param(
            # Warner Bros. Discovery Series A, 7 Aug 2014: one Series C share per share halves the
            # price, and Compustat books it in the total-return factor
            _prices(80.0, 40.0),
            [1.0] * len(DAYS),
            [1.0 if d < EX else 2.0 for d in DAYS],
            2.0,
            id="booked-in-trfd",
        ),
        pytest.param(
            _prices(0.10, 0.10),
            [1.0] * len(DAYS),
            None,
            0.01,
            id="not-in-compustat-prices",
        ),
        pytest.param(
            # Compustat moves the factor three trading days after Capital IQ's ex-date
            _prices(0.10, 9.0),
            [1.6e-9 if d < _shift_after(DAYS, EX, 3) else 1.6e-7 for d in DAYS],
            None,
            0.01,
            id="factor-change-dated-nearby",
        ),
        pytest.param(
            _prices(10.0, 8.0), [1.0] * len(DAYS), None, 1.25, id="too-small-to-tell-from-a-move"
        ),
        pytest.param(
            # the factor moves by 15% across the ex-date: more than rounding, so not a missed split
            _prices(0.10, 9.0),
            [1.6e-7 if d < EX else 1.84e-7 for d in DAYS],
            None,
            0.01,
            id="factor-moves-too-much",
        ),
    ],
)
def test_split_compustat_handles_is_left_alone(
    test_paths, monkeypatch, prices, ajexdi, trfd, rate
) -> None:
    secd = _secd("001000", prices, ajexdi, trfd)

    raw = _daily(test_paths, monkeypatch, secd, None)
    fixed = _daily(test_paths, monkeypatch, secd, [("001000", "01", EX, rate)])

    assert fixed["ajexdi"].to_list() == raw["ajexdi"].to_list()
    assert fixed["ri"].to_list() == raw["ri"].to_list()


def test_split_across_a_long_price_gap_is_left_alone(test_paths, monkeypatch) -> None:
    """Six weeks without prices before the ex-date: the price ratio no longer identifies the split."""
    days = [d for d in DAYS if not date(2023, 11, 10) <= d < EX]
    secd = _secd("001000", _prices(0.10, 9.0, days=days), [1.6e-7] * len(days), days=days)

    raw = _daily(test_paths, monkeypatch, secd, None)
    fixed = _daily(test_paths, monkeypatch, secd, [("001000", "01", EX, 0.01)])

    assert fixed["ri"].to_list() == raw["ri"].to_list()


def test_several_missed_splits_stack(test_paths, monkeypatch) -> None:
    """Two missed reverse splits: each stretch's factor carries every later missed split. The second
    is two weeks before the data ends, as a recent split is in a monthly run."""
    ex2 = date(2024, 1, 17)
    prices = [
        (0.10 if d < EX else 9.0 if d < ex2 else 200.0) * (1 + 0.02 * (i % 5 - 2))
        for i, d in enumerate(DAYS)
    ]
    secd = _secd("040116", prices, [1.6e-7] * len(DAYS))

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "040116")
    na = _security(
        _daily(
            test_paths,
            monkeypatch,
            secd,
            [("040116", "01", EX, 0.01), ("040116", "01", ex2, 0.04)],
        ),
        "040116",
    )

    k1, k2 = DAYS.index(EX), DAYS.index(ex2)
    assert na["ret"][k1] == pytest.approx(prices[k1] / (prices[k1 - 1] * 100) - 1)
    assert na["ret"][k2] == pytest.approx(prices[k2] / (prices[k2 - 1] * 25) - 1)
    assert (na["ri"] / raw["ri"]).to_list() == pytest.approx(
        [1 / (0.0004 if d < EX else 0.04 if d < ex2 else 1) for d in DAYS], rel=1e-12
    )


def _price_moves(prices: list[float], ex: date, ratio: float) -> list[float]:
    """Each day's true return: the price's own move, with the split taken out on its ex-date."""
    return [
        prices[i] / (prices[i - 1] * (ratio if DAYS[i] == ex else 1)) - 1
        for i in range(1, len(prices))
    ]


@pytest.mark.parametrize(
    ("factor_day", "before", "after", "rate"),
    [
        # 029024, 1-for-5 in April 1997: Compustat's factor moves on 1 Apr, the price on 30 Apr
        pytest.param(date(2023, 11, 22), 0.002, 0.01, 0.2, id="factor-booked-29-days-early"),
        # 252410, 1-for-20 on 14 Dec 2018: the factor follows on 2 Jan 2019, in the next month
        pytest.param(date(2024, 1, 9), 0.05, 1.0, 0.05, id="factor-booked-19-days-late"),
    ],
)
def test_split_whose_factor_step_is_booked_on_another_day_is_bridged(
    test_paths, monkeypatch, factor_day, before, after, rate
) -> None:
    """Compustat books the split's factor step weeks away from the price's split, outside the
    window that tells a missed split. Restoring the split alone would leave the factor's step in
    the index as a jump of the split ratio; bridged, every return is the price's own move, and
    the index outside the two days is as delivered."""
    prices = _prices(0.45, 0.45 / rate)
    secd = _secd("029024", prices, [before if d < factor_day else after for d in DAYS])

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "029024")
    fixed = _security(_daily(test_paths, monkeypatch, secd, [("029024", "01", EX, rate)]), "029024")

    assert fixed["ret"].to_list()[1:] == pytest.approx(_price_moves(prices, EX, 1 / rate))
    first, last = min(factor_day, EX), max(factor_day, EX)
    outside = [i for i, d in enumerate(DAYS) if not first <= d < last]
    assert (fixed["ri"] / raw["ri"]).gather(outside).to_list() == pytest.approx(
        [1.0] * len(outside), rel=1e-12
    )


def test_split_with_a_genuine_split_nearby_is_not_bridged(test_paths, monkeypatch) -> None:
    """The factor step 29 days earlier comes with its own price jump: Compustat applied another
    split there. Only the missed split is restored, as without a factor step."""
    early = date(2023, 11, 22)
    prices = [
        (0.10 if d < early else 10.0 if d < EX else 900.0) * (1 + 0.02 * (i % 5 - 2))
        for i, d in enumerate(DAYS)
    ]
    secd = _secd("040116", prices, [1.6e-9 if d < early else 1.6e-7 for d in DAYS])

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "040116")
    fixed = _security(_daily(test_paths, monkeypatch, secd, [("040116", "01", EX, 0.01)]), "040116")

    assert (fixed["ri"] / raw["ri"]).to_list() == pytest.approx(
        [100.0 if d < EX else 1.0 for d in DAYS], rel=1e-12
    )


def test_split_seen_only_in_the_total_return_factor_is_not_bridged(test_paths, monkeypatch) -> None:
    """294435 (Becamex IJC), Capital IQ 1-for-2 on 17 Jan 2017: the price stays put on the ex-date
    and only Compustat's total-return factor jumps; the factor steps on 20 Apr with the price again
    flat. No price split to move the factor step to, so the split is restored as without a step."""
    early = date(2023, 11, 22)
    secd = _secd(
        "294435",
        _prices(8400.0, 8400.0),
        [0.772 if d < early else 1.544 for d in DAYS],
        [1.0 if d < EX else 2.0 for d in DAYS],
    )

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "294435")
    fixed = _security(_daily(test_paths, monkeypatch, secd, [("294435", "01", EX, 0.5)]), "294435")

    assert (fixed["ri"] / raw["ri"]).to_list() == pytest.approx(
        [2.0 if d < EX else 1.0 for d in DAYS], rel=1e-12
    )


def test_factor_step_at_another_capital_iq_split_is_not_bridged(test_paths, monkeypatch) -> None:
    """A factor step on a day Capital IQ has a split of its own belongs to that split, not to the
    missed one, even when the price does not show it."""
    early = date(2023, 11, 22)
    prices = _prices(0.45, 2.25)
    secd = _secd("029024", prices, [0.002 if d < early else 0.01 for d in DAYS])

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "029024")
    fixed = _security(
        _daily(
            test_paths,
            monkeypatch,
            secd,
            [("029024", "01", EX, 0.2), ("029024", "01", early, 0.2)],
        ),
        "029024",
    )

    assert (fixed["ri"] / raw["ri"]).to_list() == pytest.approx(
        [5.0 if d < EX else 1.0 for d in DAYS], rel=1e-12
    )


EARLY = date(2023, 11, 22)


@pytest.mark.parametrize(
    ("ajexdi", "trfd", "other_splits"),
    [
        pytest.param(
            [0.002 if d < EARLY else 0.01 for d in DAYS],
            [1.0 if d < EARLY else 0.2 for d in DAYS],
            [],
            id="trfd-offsets-the-factor-step",
        ),
        pytest.param(
            [0.002 if d < EARLY else 0.008 for d in DAYS],
            None,
            [],
            id="step-does-not-fit-the-split",
        ),
        pytest.param(
            [0.002 if d < EARLY else 0.01 for d in DAYS],
            None,
            [("029024", "01", EARLY - timedelta(days=20), 0.2)],
            id="a-closer-split-fits-the-step",
        ),
    ],
)
def test_factor_step_that_may_belong_elsewhere_is_not_bridged(
    test_paths, monkeypatch, ajexdi, trfd, other_splits
) -> None:
    """A factor step that the total-return factor cancels, that does not fit the split, or that a
    Capital IQ split at least as close fits too, is left alone: the split is restored as without
    a step."""
    secd = _secd("029024", _prices(0.45, 2.25), ajexdi, trfd)

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "029024")
    fixed = _security(
        _daily(test_paths, monkeypatch, secd, [("029024", "01", EX, 0.2), *other_splits]),
        "029024",
    )

    assert (fixed["ri"] / raw["ri"]).to_list() == pytest.approx(
        [5.0 if d < EX else 1.0 for d in DAYS], rel=1e-12
    )


def test_two_splits_competing_for_two_factor_steps_are_not_bridged(test_paths, monkeypatch) -> None:
    """Two missed 1-for-5 splits (20 Nov, 21 Dec) and two factor steps of 5x (10 Jan, 25 Jan):
    the later split fits both steps and the pairing is ambiguous, so neither is bridged and both
    are restored as without a step."""
    first, step1, step2 = date(2023, 11, 20), date(2024, 1, 10), date(2024, 1, 25)
    prices = [
        (0.45 if d < first else 2.25 if d < EX else 11.25) * (1 + 0.02 * (i % 5 - 2))
        for i, d in enumerate(DAYS)
    ]
    ajexdi = [0.0004 if d < step1 else 0.002 if d < step2 else 0.01 for d in DAYS]
    secd = _secd("029024", prices, ajexdi)

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "029024")
    fixed = _security(
        _daily(
            test_paths,
            monkeypatch,
            secd,
            [("029024", "01", first, 0.2), ("029024", "01", EX, 0.2)],
        ),
        "029024",
    )

    assert (fixed["ri"] / raw["ri"]).to_list() == pytest.approx(
        [25.0 if d < first else 5.0 if d < EX else 1.0 for d in DAYS], rel=1e-12
    )


def test_bridged_split_keeps_the_factor_wobble_across_the_ex_date_out(
    test_paths, monkeypatch
) -> None:
    """The factor wobbles by 5% across the ex-date, as rounded factors do, and steps 5x 29 days
    earlier. Bridged, the ex-date still returns the price's move net of the split alone."""
    prices = _prices(0.45, 2.25)
    ajexdi = [0.002 if d < EARLY else 0.01 if d < EX else 0.0105 for d in DAYS]
    secd = _secd("029024", prices, ajexdi)

    fixed = _security(_daily(test_paths, monkeypatch, secd, [("029024", "01", EX, 0.2)]), "029024")

    assert fixed["ret"].to_list()[1:] == pytest.approx(_price_moves(prices, EX, 5.0))


def test_bridge_moves_compustats_own_step(test_paths, monkeypatch) -> None:
    """103618, 2-for-1 on 16 Dec 1996: Compustat's factor steps by 2.067, not 2, and only on
    10 Jan 1997. Its own step moves to the ex-date: the factor day returns the price's move, the
    ex-date the price's move net of 2.067, and the index outside the two days is as delivered."""
    late = date(2024, 1, 9)
    prices = _prices(8.0, 4.0)
    secd = _secd("103618", prices, [2.06712 if d < late else 1.0 for d in DAYS])

    raw = _security(_daily(test_paths, monkeypatch, secd, None), "103618")
    fixed = _security(_daily(test_paths, monkeypatch, secd, [("103618", "01", EX, 2.0)]), "103618")

    k, f = DAYS.index(EX), DAYS.index(late)
    assert fixed["ret"][f] == pytest.approx(prices[f] / prices[f - 1] - 1)
    assert fixed["ret"][k] == pytest.approx(prices[k] * 2.06712 / prices[k - 1] - 1)
    outside = [i for i, d in enumerate(DAYS) if not EX <= d < late]
    assert (fixed["ri"] / raw["ri"]).gather(outside).to_list() == pytest.approx(
        [1.0] * len(outside), rel=1e-12
    )


def test_split_hidden_by_factor_rounding_is_restored(test_paths, monkeypatch) -> None:
    """Conduit Pharmaceuticals, 1-for-100 on 27 Jan 2025: Compustat's factor wobbles from 1.32e-6
    to 1.33e-6 across the split instead of staying flat. The split is restored net of the wobble."""
    prices = _prices(0.05, 3.3)
    secd = _secd("040557", prices, [1.32e-6 if d < EX else 1.33e-6 for d in DAYS])

    fixed = _security(_daily(test_paths, monkeypatch, secd, [("040557", "01", EX, 0.01)]), "040557")

    k = DAYS.index(EX)
    assert fixed["ret"][k] == pytest.approx(prices[k] / (prices[k - 1] * 100) - 1)


def test_missed_split_is_restored_in_global_data(test_paths, monkeypatch) -> None:
    """G_SECD quotes some lines per 100 units; the split shows in price / qunit / factor."""
    prices = _prices(5.0, 75.0)
    gl = _secd("200100", prices, [3e-8] * len(DAYS), global_file=True)
    na = _secd("001000", _prices(10.0, 10.0), [1.0] * len(DAYS))

    fixed = _daily(test_paths, monkeypatch, na, [("200100", "01W", EX, 1 / 15)], gl=gl)

    k = DAYS.index(EX)
    assert _security(fixed, "200100")["ret"][k] == pytest.approx(
        prices[k] / (prices[k - 1] * 15) - 1
    )


MONTHS = [date(2023, 9, 30), date(2023, 10, 31), date(2023, 11, 30)] + [
    date(2023, 12, 31),
    date(2024, 1, 31),
    date(2024, 2, 29),
    date(2024, 3, 31),
]


def test_secm_is_left_as_delivered(test_paths, monkeypatch) -> None:
    """The monthly file is not corrected (its monthly price ratio identifies a split less
    reliably): with split history present, SECM's December 2023 return for Mullen is unchanged."""
    prices = [0.15 if m < EX else 14.29 for m in MONTHS]
    _secm_rows("040116", MONTHS, prices, [1.6e-7] * len(MONTHS)).write_parquet(
        test_paths.raw_tables_dir / "comp_secm.parquet"
    )
    _firm_shares([], MONTHS).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
    _usd_fx(monkeypatch, MONTHS)
    _write_split_events(test_paths, [("040116", "01", EX, 0.01)])

    aux.gen_secm_data(test_paths)

    ri = pl.read_parquet(test_paths.interim_dir / "secm_data.parquet").sort("datadate")["ri"]
    k = MONTHS.index(date(2023, 12, 31))
    assert ri[k] / ri[k - 1] == pytest.approx(14.29 / 0.15)


def test_share_fallback_keeps_compustat_factors(test_paths) -> None:
    """The firm-shares fallback reads SECM's factors as delivered, split history or not."""
    _secm_rows("040116", MONTHS, [0.15] * 3 + [14.29] * 4, [1.6e-7] * len(MONTHS)).write_parquet(
        test_paths.raw_tables_dir / "comp_secm.parquet"
    )
    _write_split_events(test_paths, [("040116", "01", EX, 0.01)])
    con = ibis.duckdb.connect()

    register_security_ajex_at_report(con, test_paths)

    ajex = dict(con.raw_sql("SELECT rep_eom, ajex_sec FROM __sec_ajex_rep").fetchall())
    assert list(ajex.values()) == pytest.approx([1.6e-7] * len(MONTHS))


def test_split_restoration_leaves_inferred_share_counts_alone(test_paths, monkeypatch) -> None:
    """A security without its own daily share count infers it from SECM's factor across months and
    SECD's within the month. The daily file restores Mullen's missed split, the monthly file cannot
    (no SECM price in the two months before it): the inferred counts and market caps must not move,
    while the split-day return is restored."""
    n = len(DAYS)
    prices = _prices(0.10, 9.0)
    months = [m for m in MONTHS if m not in (date(2023, 10, 31), date(2023, 11, 30))]

    def run(events: list[tuple[str, str, date, float]] | None) -> pl.DataFrame:
        _secd("040116", prices, [1.6e-7] * n).with_columns(
            cshoc=pl.lit(None, dtype=pl.Float64)
        ).write_parquet(test_paths.raw_tables_dir / "comp_secd.parquet")
        _secd("200000", [10.0] * n, [1.0] * n, global_file=True).write_parquet(
            test_paths.raw_tables_dir / "comp_g_secd.parquet"
        )
        _secm_rows(
            "040116", months, [0.15 if m < EX else 14.29 for m in months], [1.6e-7] * len(months)
        ).write_parquet(test_paths.raw_tables_dir / "comp_secm.parquet")
        pl.DataFrame(
            {
                "ddate": DAYS,
                "gvkey": ["040116"] * n,
                "datadate": [date(2023, 9, 30)] * n,
                "csho_fund": [5.0] * n,
                "ajex_fund": [1.6e-7] * n,
            }
        ).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
        _usd_fx(monkeypatch, DAYS)
        test_paths.raw_table_source(SPLIT_EVENTS_TABLE).unlink(missing_ok=True)
        if events is not None:
            _write_split_events(test_paths, events)
        aux.gen_comp_dsf(test_paths)
        return _security(
            pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet").sort("datadate"),
            "040116",
        )

    raw = run(None)
    fixed = run([("040116", "01", EX, 0.01)])

    assert raw["cshoc"].null_count() == 0
    assert fixed["cshoc"].to_list() == raw["cshoc"].to_list()
    assert fixed["me"].to_list() == raw["me"].to_list()
    k = DAYS.index(EX)
    assert fixed["ri"][k] / fixed["ri"][k - 1] == pytest.approx(prices[k] / (prices[k - 1] * 100))


def test_split_events_query_maps_capital_iq_to_compustat_securities() -> None:
    query = build_split_events_query("public")

    assert '"public"."ciqgvkeyiid"' in query and '"public"."ciqsplit"' in query
    assert "s.appliedflag = 1" in query
    assert "exp(sum(ln(" in query  # same-day events multiply
    with pytest.raises(ValueError, match="Invalid raw PostgreSQL schema"):
        build_split_events_query("public; DROP TABLE x")


class _SourceConnection:
    """Answers the Capital IQ table check and records every statement."""

    def __init__(self, available: bool) -> None:
        self.available = available
        self.statements: list[str] = []
        self._result: tuple = ()

    def execute(self, sql: str) -> _SourceConnection:
        self.statements.append(sql)
        self._result = (self.available,) if "to_regclass" in sql else (0,)
        return self

    def fetchone(self) -> tuple:
        return self._result


def test_split_events_download_copies_capital_iq_history(test_paths) -> None:
    con = _SourceConnection(available=True)

    download_split_events_attached(con, "source_db", test_paths, "public", "RDS")

    copies = [s for s in con.statements if "COPY (" in s]
    assert len(copies) == 1
    assert "ciqsplit" in copies[0] and "ciq_split_events.parquet" in copies[0]


def test_split_events_download_without_capital_iq_writes_an_empty_table(test_paths) -> None:
    con = _SourceConnection(available=False)

    download_split_events_attached(con, "source_db", test_paths, "comp", "WRDS")

    assert not [s for s in con.statements if "COPY (" in s]
    events = pl.read_parquet(test_paths.raw_table_source(SPLIT_EVENTS_TABLE))
    assert events.height == 0 and events.schema == pl.Schema(SPLIT_EVENT_SCHEMA)
