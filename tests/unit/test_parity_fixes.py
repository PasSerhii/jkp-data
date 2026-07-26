"""Regression tests for WRDS/SAS parity fixes."""

import json
from datetime import date

import ibis
import polars as pl
import pytest

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


def test_accounting_loader_uses_latest_publication_field(tmp_path) -> None:
    source_path = tmp_path / "funda.parquet"
    pl.DataFrame(
        {
            "gvkey": ["040703"],
            "datadate": [date(2025, 12, 31)],
            "indfmt": ["INDL"],
            "datafmt": ["STD"],
            "popsrc": ["D"],
            "consol": ["C"],
            "pdate": [date(2026, 6, 20)],
            "fdate": [date(2026, 7, 1)],
        }
    ).write_parquet(source_path)

    result = load_raw_fund_table_and_filter(source_path, None, "NA", 2).collect()

    assert result["availability_date"][0] == date(2026, 7, 1)


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
