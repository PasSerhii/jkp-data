"""Regression tests for WRDS/SAS parity fixes."""

import json
from datetime import date

import ibis
import polars as pl
import pytest

import jkp.data.aux_functions as aux
from jkp.data.aux_functions import (
    _record_ff_snapshot,
    _register_historical_exchange_view,
)
from jkp.data.paths import DataPaths

pytestmark = pytest.mark.unit


def test_exchange_is_resolved_from_history_for_observation_date(tmp_path) -> None:
    source_path = tmp_path / "prices.parquet"
    history_path = tmp_path / "history.parquet"
    pl.DataFrame(
        {
            "gvkey": ["036883", "036883"],
            "iid": ["03", "03"],
            "datadate": [date(2026, 5, 29), date(2026, 7, 15)],
            "exchg": [19, 19],
            "prccd": [1.0, 1.0],
        }
    ).write_parquet(source_path)
    pl.DataFrame(
        {
            "gvkey": ["036883"],
            "iid": ["03"],
            "historical_exchg": [12],
            "effdate": [date(2020, 1, 1)],
            "thrudate": [date(2026, 6, 30)],
        }
    ).write_parquet(history_path)

    con = ibis.duckdb.connect()
    _register_historical_exchange_view(
        con,
        view_name="prices",
        source_path=source_path,
        history_path=history_path,
    )
    result = pl.from_arrow(
        con.raw_sql("SELECT datadate, exchg FROM prices ORDER BY datadate").arrow()
    )
    con.disconnect()

    assert result["exchg"].to_list() == [12, 19]


def _write_sec_history_tables(paths: DataPaths, na_rows: list[dict], g_rows: list[dict]) -> None:
    schema = {
        "gvkey": pl.Utf8,
        "iid": pl.Utf8,
        "item": pl.Utf8,
        "itemvalue": pl.Utf8,
        "effdate": pl.Date,
        "thrudate": pl.Date,
    }
    pl.DataFrame(na_rows, schema=schema).write_parquet(
        paths.raw_tables_dir / "comp_sec_history.parquet"
    )
    pl.DataFrame(g_rows, schema=schema).write_parquet(
        paths.raw_tables_dir / "comp_g_sec_history.parquet"
    )


def test_exchg_history_parses_decimal_formatted_codes(test_paths) -> None:
    """EXCHG itemvalues stored as '11.0000' must still resolve to integers."""
    _write_sec_history_tables(
        test_paths,
        na_rows=[
            {
                "gvkey": "000001",
                "iid": "01",
                "item": "EXCHG",
                "itemvalue": "11.0000",
                "effdate": date(2020, 1, 1),
                "thrudate": None,
            }
        ],
        g_rows=[
            {
                "gvkey": "200000",
                "iid": "01W",
                "item": "EXCHG",
                "itemvalue": "104",
                "effdate": date(2020, 1, 1),
                "thrudate": None,
            }
        ],
    )

    aux.gen_prihist_files(test_paths)

    history = pl.read_parquet(test_paths.interim_dir / "raw_data_dfs" / "__exchg_history.parquet")
    assert history["historical_exchg"].sort().to_list() == [11, 104]
    assert history["historical_exchg"].dtype == pl.Int32


def test_exchg_history_fails_when_no_itemvalue_parses(test_paths) -> None:
    """Unparseable EXCHG codes must fail the build, not silently disable the fix."""
    _write_sec_history_tables(
        test_paths,
        na_rows=[
            {
                "gvkey": "000001",
                "iid": "01",
                "item": "EXCHG",
                "itemvalue": "N/A",
                "effdate": date(2020, 1, 1),
                "thrudate": None,
            }
        ],
        g_rows=[],
    )

    with pytest.raises(RuntimeError, match="parsed as an integer exchange code"):
        aux.gen_prihist_files(test_paths)


def test_exchg_history_fails_when_source_has_no_exchg_rows(test_paths) -> None:
    """A sec_history source without EXCHG items must fail the build."""
    _write_sec_history_tables(
        test_paths,
        na_rows=[
            {
                "gvkey": "000001",
                "iid": "01",
                "item": "PRIHISTUSA",
                "itemvalue": "01",
                "effdate": date(2020, 1, 1),
                "thrudate": None,
            }
        ],
        g_rows=[],
    )

    with pytest.raises(RuntimeError, match="no EXCHG rows"):
        aux.gen_prihist_files(test_paths)


def test_secm_return_index_uses_production_trfm_coalesce(test_paths, monkeypatch) -> None:
    """Monthly SECM mirrors the production SAS coalesce(trfm, 1): a missing
    total-return factor never nulls the monthly return index."""
    pl.DataFrame(
        {
            "gvkey": ["001000", "001000"],
            "iid": ["01", "01"],
            "datadate": [date(2026, 4, 30), date(2026, 5, 29)],
            "tpci": ["0", "0"],
            "exchg": [11, 11],
            "dvpsxm": [0.0, 0.5],
            "curcdm": ["USD", "USD"],
            "prccm": [10.0, 11.0],
            "prchm": [10.5, 11.5],
            "prclm": [9.5, 10.5],
            "ajexm": [1.0, 1.0],
            "cshom": [1_000_000.0, 1_000_000.0],
            "csfsm": pl.Series([None, None], dtype=pl.Float64),
            "cshoq": pl.Series([None, None], dtype=pl.Float64),
            "cshtrm": [1000.0, 1000.0],
            "curcddvm": ["USD", "USD"],
            "trfm": pl.Series([None, None], dtype=pl.Float64),
        }
    ).write_parquet(test_paths.raw_tables_dir / "comp_secm.parquet")
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
            {"datadate": [date(2026, 5, 29)], "curcdd": ["USD"], "fx": [1.0]}
        ),
    )

    aux.gen_secm_data(test_paths)

    result = pl.read_parquet(test_paths.interim_dir / "secm_data.parquet")
    assert result.height == 2
    assert result["ri"].null_count() == 0


def test_ff_snapshot_manifest_records_input_identity(tmp_path) -> None:
    paths = DataPaths(base_dir=tmp_path)
    paths.raw_tables_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [date(2026, 5, 31), date(2026, 6, 30)],
            "rf": [0.0029, 0.00315],
        }
    ).write_parquet(paths.raw_tables_dir / "ff_factors_monthly.parquet")

    _record_ff_snapshot(paths)
    manifest = json.loads((tmp_path / "source_snapshot_manifest.json").read_text())

    assert manifest["row_count"] == 2
    assert manifest["max_date"] == "2026-06-30"
    assert manifest["latest_rf"] == pytest.approx(0.00315)
    assert len(manifest["sha256"]) == 64


def test_accounting_expansion_uses_plain_four_month_lag(test_paths, monkeypatch) -> None:
    """Each statement covers month_end(datadate + 4 months) until the month before the
    next statement's start, or 18 months after the period end, as in the production SAS.

    Two statements in the same month leave one of them with end < start: SAS's %expand
    writes no rows for it, and so must the pipeline (not a null public_date row).
    """

    def fake_persistence(paths, data_path, n_years, n_min):
        pl.DataFrame(
            schema={
                "gvkey": pl.Utf8,
                "curcd": pl.Utf8,
                "datadate": pl.Date,
                "ni_ar1": pl.Float64,
                "ni_ivol": pl.Float64,
            }
        ).write_parquet(paths.interim_dir / "ni_ar_res.parquet")

    monkeypatch.setattr(aux, "compute_earnings_persistence", fake_persistence)

    records = pl.LazyFrame(
        {
            "gvkey": ["051423"] * 4,
            "curcd": ["USD"] * 4,
            "datadate": [
                date(2025, 6, 30),
                date(2025, 9, 30),
                date(2025, 12, 31),
                date(2025, 12, 31),
            ],
            "data_available": [1, 1, 1, 1],
            "at_x": [194.853, None, 92.836, 92.836],
        }
    )

    expanded = aux.add_earnings_persistence_and_expand(
        test_paths, records, data_path=None, lag_to_pub=4, max_lag=18
    ).collect()
    windows = (
        expanded.group_by("datadate")
        .agg(
            start=pl.col("public_date").min(),
            end=pl.col("public_date").max(),
            months=pl.len(),
        )
        .sort("datadate")
    )

    assert expanded["public_date"].null_count() == 0
    assert expanded.height == expanded["public_date"].n_unique()
    assert windows.rows() == [
        (date(2025, 6, 30), date(2025, 10, 31), date(2025, 12, 31), 3),
        (date(2025, 9, 30), date(2026, 1, 31), date(2026, 3, 31), 3),
        (date(2025, 12, 31), date(2026, 4, 30), date(2027, 6, 30), 15),
    ]


def test_combine_ann_qtr_chars_keeps_quarterly_only_coverage(test_paths) -> None:
    """A security-month covered only by the quarterly panel must survive the merge.

    Vivanta Industries (gvkey 343490) in the 2026-07-27 run: its FY2024 annual
    record expires (18-month cap) in 2025-09, and FY2025 isn't public until
    2026-06, leaving an annual-panel gap that covers May 2026. Its Q4-2025
    quarterly record was public by 2026-02, so the quarterly panel has no such
    gap. The old `ann LEFT JOIN qtr` dropped May 2026 entirely for this gvkey
    because it only exists on the annual side by construction; the row and its
    quarterly-sourced values must now come through.
    """
    ann_df_path = test_paths.interim_dir / "ann.parquet"
    qtr_df_path = test_paths.interim_dir / "qtr.parquet"

    # gvkey "1": both panels cover May; quarterly is newer and must win (unchanged).
    # gvkey "2": only annual covers May; annual value must be kept (unchanged).
    # gvkey "3": only quarterly covers May; this row was silently dropped before the fix.
    pl.DataFrame(
        {
            "source": ["NA", "NA"],
            "gvkey": ["1", "2"],
            "public_date": [date(2026, 5, 31), date(2026, 5, 31)],
            "datadate": [date(2025, 1, 31), date(2025, 1, 31)],
            "at": [10.0, 5.0],
        }
    ).write_parquet(ann_df_path)
    pl.DataFrame(
        {
            "source": ["NA"],
            "gvkey": ["1"],
            "public_date": [date(2026, 5, 31)],
            "datadate": [date(2025, 6, 30)],
            "at_qitem": [20.0],
        }
    ).write_parquet(qtr_df_path)

    aux.combine_ann_qtr_chars(test_paths, ann_df_path, qtr_df_path, ["at"], "_qitem")
    combined = pl.read_parquet(test_paths.interim_dir / "acc_chars_world.parquet").sort("gvkey")

    assert combined["gvkey"].to_list() == ["1", "2"]
    assert combined["at"].to_list() == [20.0, 5.0]  # gvkey 1 prefers the newer quarterly value

    # Now add gvkey "3": quarterly-only coverage of May 2026.
    pl.DataFrame(
        {
            "source": ["NA", "GLOBAL"],
            "gvkey": ["1", "3"],
            "public_date": [date(2026, 5, 31), date(2026, 5, 31)],
            "datadate": [date(2025, 6, 30), date(2025, 12, 31)],
            "at_qitem": [20.0, 30.0],
        }
    ).write_parquet(qtr_df_path)

    aux.combine_ann_qtr_chars(test_paths, ann_df_path, qtr_df_path, ["at"], "_qitem")
    combined = pl.read_parquet(test_paths.interim_dir / "acc_chars_world.parquet").sort("gvkey")

    assert combined["gvkey"].to_list() == ["1", "2", "3"]
    assert combined["at"].to_list() == [20.0, 5.0, 30.0]
    row3 = combined.filter(pl.col("gvkey") == "3")
    assert row3["public_date"].to_list() == [date(2026, 5, 31)]
    assert row3["source"].to_list() == ["GLOBAL"]
