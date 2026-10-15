"""Integration seam tests for the daily Compustat assembly (gen_comp_dsf)."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

import jkp.data.aux_functions as aux


def _daily_rows(gvkey: str, exchg: int, *, global_file: bool) -> pl.DataFrame:
    dates = [date(2024, 1, 2) + timedelta(days=i) for i in range(5)]
    data = {
        "gvkey": [gvkey] * 5,
        "iid": ["01"] * 5,
        "datadate": dates,
        "tpci": ["0"] * 5,
        "exchg": [exchg] * 5,
        "prcstd": [4] * 5,
        "curcdd": ["USD"] * 5,
        "prccd": [10.0, 10.2, 10.4, 10.3, 10.5],
        "ajexdi": [1.0] * 5,
        "prchd": [10.1, 10.3, 10.5, 10.4, 10.6],
        "prcld": [9.9, 10.1, 10.3, 10.2, 10.4],
        "prcod": [9.95, 10.15, 10.35, 10.25, 10.45],
        "cshtrd": [1000.0] * 5,
        "cshoc": [1_000_000.0] * 5,
        "trfd": [1.0] * 5,
        "curcddv": ["USD"] * 5,
        "div": [0.0] * 5,
        "divd": [0.0] * 5,
        "divsp": [0.0] * 5,
    }
    if global_file:
        data["qunit"] = [1.0] * 5
    return pl.DataFrame(data)


def _write_shared_inputs(test_paths, monkeypatch) -> None:
    pl.DataFrame(
        {
            "gvkey": pl.Series([], dtype=pl.String),
            "ddate": pl.Series([], dtype=pl.Date),
            "datadate": pl.Series([], dtype=pl.Date),
            "csho_fund": pl.Series([], dtype=pl.Float64),
            "ajex_fund": pl.Series([], dtype=pl.Float64),
        }
    ).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
    monkeypatch.setattr(
        aux,
        "compustat_fx",
        lambda _paths: pl.DataFrame(
            {
                "datadate": [date(2024, 1, 2) + timedelta(days=i) for i in range(5)],
                "curcdd": ["USD"] * 5,
                "fx": [1.0] * 5,
            }
        ),
    )


def test_gen_comp_dsf_uses_production_trfd_coalesce(test_paths, monkeypatch):
    """The return index mirrors the production SAS coalesce(trfd, 1): a missing
    total-return factor never nulls the return index, even for dividend payers."""
    na = _daily_rows("001000", 11, global_file=False).with_columns(
        trfd=pl.Series("trfd", [1.0, None, 1.02, None, 1.02]),
        divd=pl.Series("divd", [0.0, 0.5, 0.0, 0.0, 0.0]),
    )
    gl = _daily_rows("200000", 104, global_file=True).with_columns(
        trfd=pl.Series("trfd", [None] * 5, dtype=pl.Float64),
        div=pl.Series("div", [0.3, 0.0, 0.0, 0.0, 0.0]),
    )
    na.write_parquet(test_paths.raw_tables_dir / "comp_secd.parquet")
    gl.write_parquet(test_paths.raw_tables_dir / "comp_g_secd.parquet")
    _write_shared_inputs(test_paths, monkeypatch)

    aux.gen_comp_dsf(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet")
    assert result.height == 10
    assert result["ri"].null_count() == 0


def test_gen_comp_dsf_supports_batched_sources_and_preserves_open_price(test_paths, monkeypatch):
    """Part-file (batched download) sources stream through the same views, and
    the open price survives to the output."""
    na_parts = test_paths.raw_tables_dir / "comp_secd_parts"
    gl_parts = test_paths.raw_tables_dir / "comp_g_secd_parts"
    na_parts.mkdir(parents=True)
    gl_parts.mkdir(parents=True)
    _daily_rows("001000", 11, global_file=False).write_parquet(na_parts / "part-000.parquet")
    _daily_rows("200000", 104, global_file=True).write_parquet(gl_parts / "part-000.parquet")
    _write_shared_inputs(test_paths, monkeypatch)

    aux.gen_comp_dsf(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet").sort(
        ["gvkey", "datadate"]
    )
    assert result["gvkey"].unique().sort().to_list() == ["001000", "200000"]
    assert result.filter(pl.col("gvkey") == "001000")["prc_open_lcl"][0] == 9.95
    assert result.filter(pl.col("gvkey") == "200000")["prc_open_lcl"][0] == 9.95


def _write_dividend_cases(test_paths, monkeypatch) -> None:
    """A non-payer (no dividend currency), a payer, a payer whose dividend currency has no FX rate,
    and a payer whose dividend sits on a row without a price or adjustment factor."""
    none = pl.Series([None] * 5, dtype=pl.Float64)
    non_payer = _daily_rows("001000", 11, global_file=False).with_columns(
        curcddv=pl.Series([None] * 5, dtype=pl.String), div=none, divd=none, divsp=none
    )
    payer = _daily_rows("002000", 11, global_file=False).with_columns(
        div=pl.Series([None, 0.5, None, None, None], dtype=pl.Float64),
        divd=pl.Series([None, 0.5, None, None, None], dtype=pl.Float64),
        divsp=none,
    )
    no_fx = _daily_rows("200000", 104, global_file=True).with_columns(
        curcddv=pl.Series(["EUR"] * 5),  # the FX table has USD only
        div=pl.Series([0.3, None, None, None, None], dtype=pl.Float64),
        divd=none,
        divsp=pl.Series([0.3, None, None, None, None], dtype=pl.Float64),
    )
    dividend_only_row = _daily_rows("003000", 11, global_file=False).with_columns(
        prccd=pl.Series([10.0, 10.2, None, 10.3, 10.5]),
        ajexdi=pl.Series([1.0, 1.0, None, 1.0, 1.0]),
        div=pl.Series([None, None, 0.2, None, None], dtype=pl.Float64),
        divd=pl.Series([None, None, 0.2, None, None], dtype=pl.Float64),
        divsp=none,
    )
    pl.concat([non_payer, payer, dividend_only_row]).write_parquet(
        test_paths.raw_tables_dir / "comp_secd.parquet"
    )
    no_fx.write_parquet(test_paths.raw_tables_dir / "comp_g_secd.parquet")
    _write_shared_inputs(test_paths, monkeypatch)


def test_gen_comp_dsf_days_without_a_dividend_are_zero(test_paths, monkeypatch):
    """A day without a dividend is 0, as the documentation sets a missing dividend: also for a
    security that never paid one and so has no dividend currency (no FX rate to multiply by).
    A real dividend in a currency without an FX rate stays null."""
    _write_dividend_cases(test_paths, monkeypatch)

    aux.gen_comp_dsf(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet").sort(
        ["gvkey", "datadate"]
    )
    divs = ["div_tot", "div_cash", "div_spc"]
    by = {
        g: result.filter(pl.col("gvkey") == g).select(divs) for g in ("001000", "002000", "200000")
    }
    assert by["001000"].to_dict(as_series=False) == {c: [0.0] * 5 for c in divs}
    assert by["002000"].to_dict(as_series=False) == {
        "div_tot": [0.0, 0.5, 0.0, 0.0, 0.0],
        "div_cash": [0.0, 0.5, 0.0, 0.0, 0.0],
        "div_spc": [0.0] * 5,
    }
    assert by["200000"].to_dict(as_series=False) == {
        "div_tot": [None, 0.0, 0.0, 0.0, 0.0],
        "div_cash": [0.0] * 5,
        "div_spc": [None, 0.0, 0.0, 0.0, 0.0],
    }


def test_gen_secm_data_month_without_a_dividend_is_zero(test_paths, monkeypatch):
    """The SECM fallback follows the daily rule: no dividend gives 0 even without a dividend
    currency, a dividend is converted, and a dividend without an FX rate stays null. SECM has
    no cash/special split: both are 0 without a dividend and null with one."""
    month = date(2024, 1, 31)
    n = 4
    pl.DataFrame(
        {
            "gvkey": ["001000", "002000", "003000", "004000"],
            "iid": ["01"] * n,
            "datadate": [month] * n,
            "tpci": ["0"] * n,
            "exchg": [11] * n,
            "dvpsxm": pl.Series([None, 0.0, 0.5, 0.4], dtype=pl.Float64),
            "curcdm": ["USD"] * n,
            "prccm": [10.0] * n,
            "prchm": [10.0] * n,
            "prclm": [10.0] * n,
            "ajexm": [1.0] * n,
            "cshom": [1_000_000.0] * n,
            "csfsm": pl.Series([None] * n, dtype=pl.Float64),
            "cshoq": pl.Series([None] * n, dtype=pl.Float64),
            "cshtrm": [1000.0] * n,
            "curcddvm": pl.Series([None, None, "USD", "EUR"], dtype=pl.String),
            "trfm": [1.0] * n,
        }
    ).write_parquet(test_paths.raw_tables_dir / "comp_secm.parquet")
    pl.DataFrame(
        schema={
            "ddate": pl.Date,
            "gvkey": pl.String,
            "datadate": pl.Date,
            "csho_fund": pl.Float64,
            "ajex_fund": pl.Float64,
        }
    ).write_parquet(test_paths.interim_dir / "__firm_shares2.parquet")
    monkeypatch.setattr(
        aux,
        "compustat_fx",
        lambda _paths: pl.DataFrame({"datadate": [month], "curcdd": ["USD"], "fx": [1.0]}),
    )

    aux.gen_secm_data(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "secm_data.parquet").sort("gvkey")
    assert result["div_tot"].to_list() == [0.0, 0.0, 0.5, None]
    assert result["div_cash"].to_list() == [0.0, 0.0, None, None]
    assert result["div_spc"].to_list() == [0.0, 0.0, None, None]


def test_gen_secd_data_month_with_an_unconvertible_dividend_is_unknown(test_paths, monkeypatch):
    """Monthly dividends sum the daily ones: 0 for a non-payer, the dividend for a payer, and
    null for a month whose dividend could not be converted or adjusted (not 0 from the other days)."""
    _write_dividend_cases(test_paths, monkeypatch)
    aux.gen_comp_dsf(test_paths)

    aux.gen_secd_data(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "secd_data.parquet").sort("gvkey")
    assert result["gvkey"].to_list() == ["001000", "002000", "003000", "200000"]
    assert result["div_tot"].to_list() == [0.0, 0.5, None, None]
    assert result["div_cash"].to_list() == [0.0, 0.5, None, 0.0]
    assert result["div_spc"].to_list() == [0.0, 0.0, 0.0, None]
