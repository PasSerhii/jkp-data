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
    accounting_public_start,
    load_raw_fund_table_and_filter,
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


def test_accounting_public_start_respects_actual_availability() -> None:
    result = (
        pl.DataFrame(
            {
                "datadate": [date(2025, 12, 31)] * 3,
                "availability_date": [date(2026, 3, 15), date(2026, 7, 1), None],
            },
            schema={"datadate": pl.Date, "availability_date": pl.Date},
        )
        .with_columns(accounting_public_start(4))
        .get_column("start_date")
        .to_list()
    )

    assert result == [date(2026, 4, 30), date(2026, 7, 31), date(2026, 4, 30)]


def test_accounting_loader_uses_earliest_publication_field(tmp_path) -> None:
    """Availability is the first public release, not the final filing.

    Row 1 mirrors Driven Brands FY2025: released 2026-05-19, finalized
    2026-06-03. Using the later date would withhold it from the May panel that
    the production SAS includes. Row 2 mirrors Akanda FY2025, which has no
    pdate, so fdate is the only evidence of when the data existed.
    """
    source_path = tmp_path / "funda.parquet"
    pl.DataFrame(
        {
            "gvkey": ["037751", "040703"],
            "datadate": [date(2025, 12, 31), date(2025, 12, 31)],
            "indfmt": ["INDL", "INDL"],
            "datafmt": ["STD", "STD"],
            "popsrc": ["D", "D"],
            "consol": ["C", "C"],
            "pdate": [date(2026, 5, 19), None],
            "fdate": [date(2026, 6, 3), date(2026, 7, 1)],
        },
        schema_overrides={"pdate": pl.Date, "fdate": pl.Date},
    ).write_parquet(source_path)

    result = load_raw_fund_table_and_filter(source_path, None, "NA", 2).collect()

    assert result["availability_date"].to_list() == [date(2026, 5, 19), date(2026, 7, 1)]


def test_publication_guard_matches_production_lag_on_known_records(tmp_path) -> None:
    """End-to-end on the four securities audited against the RDS.

    The guard must only ever delay a statement past the plain four-month lag
    when the data genuinely was not public yet.
    """
    source_path = tmp_path / "funda.parquet"
    pl.DataFrame(
        {
            "gvkey": ["037751", "319310", "009818", "040703"],
            "datadate": [
                date(2025, 12, 31),  # Driven Brands FY2025
                date(2026, 1, 31),  # OVS FY2025
                date(2026, 3, 31),  # Sony FY2025
                date(2025, 12, 31),  # Akanda FY2025
            ],
            "indfmt": ["INDL"] * 4,
            "datafmt": ["STD"] * 4,
            "popsrc": ["D"] * 4,
            "consol": ["C"] * 4,
            "pdate": [date(2026, 5, 19), date(2026, 4, 21), date(2026, 5, 8), None],
            "fdate": [date(2026, 6, 3), date(2026, 6, 5), date(2026, 6, 23), date(2026, 7, 1)],
        },
        schema_overrides={"pdate": pl.Date, "fdate": pl.Date},
    ).write_parquet(source_path)

    starts = (
        load_raw_fund_table_and_filter(source_path, None, "NA", 2)
        .with_columns(accounting_public_start(4))
        .collect()
        .get_column("start_date")
        .to_list()
    )

    assert starts == [
        date(2026, 5, 31),  # Driven Brands: public 05-19, in the May panel
        date(2026, 5, 31),  # OVS: public 04-21, four-month lag binds
        date(2026, 7, 31),  # Sony annual FY2025: four-month lag binds
        date(2026, 7, 31),  # Akanda: no pdate, fdate 07-01 blocks the look-ahead
    ]


def test_quarterly_guard_uses_report_date_not_final_filing(tmp_path) -> None:
    """Sony Q3 FY2025 is the record the May panel actually resolves to.

    Its results were reported 2026-02-05 but the filing was only finalised
    2026-06-15. Keying availability off the final date pushed it out of May, so
    the panel fell back to Q2 and carried a pre-spinoff balance sheet
    (atq 244,127) instead of the correct post-spinoff one (atq 101,315).
    """
    source_path = tmp_path / "fundq.parquet"
    pl.DataFrame(
        {
            "gvkey": ["009818"],
            "datadate": [date(2025, 12, 31)],
            "indfmt": ["INDL"],
            "datafmt": ["STD"],
            "popsrc": ["D"],
            "consol": ["C"],
            "rdq": [date(2026, 2, 5)],
            "pdateq": [None],
            "fdateq": [date(2026, 6, 15)],
        },
        schema_overrides={"rdq": pl.Date, "pdateq": pl.Date, "fdateq": pl.Date},
    ).write_parquet(source_path)

    result = (
        load_raw_fund_table_and_filter(source_path, None, "NA", 2)
        .with_columns(accounting_public_start(4))
        .collect()
    )

    assert result["availability_date"][0] == date(2026, 2, 5)
    assert result["start_date"][0] == date(2026, 4, 30)


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


def test_expansion_is_not_extended_by_out_of_order_publication(test_paths, monkeypatch) -> None:
    """A late-published fiscal period must not extend the preceding stale record.

    BGIN Blockchain (gvkey 051423) in the 2026-07-27 run: its 2025-09-30 quarter
    was published 2026-06-03, after the 2025-12-31 quarter (2026-04-24). Chaining
    end_date off only the next row's start let the stale 2025-06-30 record cover
    April and May 2026, overriding the fresher FY2025Q4 data that was already
    public. Each record must instead end before the earliest start of any later
    record.
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
            "gvkey": ["051423"] * 3,
            "curcd": ["USD"] * 3,
            "datadate": [date(2025, 6, 30), date(2025, 9, 30), date(2025, 12, 31)],
            "availability_date": [date(2025, 11, 14), date(2026, 6, 3), date(2026, 4, 24)],
            "data_available": [1, 1, 1],
            "at_x": [194.853, None, 92.836],
        }
    )

    expanded = (
        aux.add_earnings_persistence_and_expand(
            test_paths, records, data_path=None, lag_to_pub=4, max_lag=18
        )
        .collect()
        .sort("public_date")
    )
    by_month = dict(
        zip(expanded["public_date"].to_list(), expanded["datadate"].to_list(), strict=True)
    )

    # Exactly one record may cover any month, and superseded ranges must vanish
    # instead of exploding into null public_date rows.
    assert expanded["public_date"].null_count() == 0
    assert expanded.height == expanded["public_date"].n_unique()
    # The stale 2025-06-30 record ends before FY2025Q4 becomes available...
    assert by_month[date(2026, 3, 31)] == date(2025, 6, 30)
    # ...and April/May 2026 carry the fresh record that was public since April.
    assert by_month[date(2026, 4, 30)] == date(2025, 12, 31)
    assert by_month[date(2026, 5, 31)] == date(2025, 12, 31)
    # The late-published 2025-09-30 report is fully superseded before it starts.
    assert date(2025, 9, 30) not in set(expanded["datadate"].to_list())
