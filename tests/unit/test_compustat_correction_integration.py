"""Integration seam tests for correction-aware Compustat daily assembly."""

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


def test_gen_comp_dsf_correction_supports_batched_sources_and_preserves_open_price(
    test_paths, monkeypatch
):
    na_parts = test_paths.raw_tables_dir / "comp_secd_parts"
    gl_parts = test_paths.raw_tables_dir / "comp_g_secd_parts"
    na_parts.mkdir(parents=True)
    gl_parts.mkdir(parents=True)
    _daily_rows("001000", 11, global_file=False).write_parquet(na_parts / "part-000.parquet")
    _daily_rows("200000", 104, global_file=True).write_parquet(gl_parts / "part-000.parquet")

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
    monkeypatch.setattr(
        aux,
        "comp_exchanges",
        lambda _paths: pl.DataFrame({"exchg": [11, 104], "excntry": ["USA", "ISR"]}),
    )

    corrected_sources: list[str] = []

    def correction_spy(frame, **_kwargs):
        corrected_sources.append(str(frame.explain()))
        return frame

    monkeypatch.setattr(aux, "correct_decimal_errors", correction_spy)
    monkeypatch.setattr(aux, "drop_unreliable_observations", lambda frame, **_kwargs: frame)

    aux.gen_comp_dsf(test_paths, apply_correction=True)

    result = pl.read_parquet(test_paths.interim_dir / "__comp_dsf.parquet").sort(
        ["gvkey", "datadate"]
    )
    assert result["gvkey"].unique().sort().to_list() == ["001000", "200000"]
    assert result.filter(pl.col("gvkey") == "001000")["prc_open_lcl"][0] == 9.95
    assert result.filter(pl.col("gvkey") == "200000")["prc_open_lcl"][0] == 9.95
    assert "excntry" not in result.columns
    assert len(corrected_sources) == 2
    assert any("comp_secd_parts" in source for source in corrected_sources)
    assert any("comp_g_secd_parts" in source for source in corrected_sources)
