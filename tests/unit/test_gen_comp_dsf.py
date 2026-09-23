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
