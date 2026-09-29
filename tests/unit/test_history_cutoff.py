"""Regression tests for bounded-history downloads and lifetime age anchors."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from jkp.data.aux_functions import (
    build_compustat_age_anchor_query,
    download_compustat_age_anchor_attached,
    firm_age,
)


def test_age_anchor_query_uses_indexed_first_row_probes_not_daily_aggregate() -> None:
    sql = build_compustat_age_anchor_query("public")

    assert 'FROM "public"."security" s' in sql
    assert 'FROM "public"."co_adesind"' in sql
    for table in (
        "sec_dprc",
        "sec_divid",
        "sec_dtrt",
        "sec_split",
        "sec_mth",
        "sec_mthprc",
        "sec_mthtrt",
    ):
        assert f'FROM "public"."{table}" x' in sql
    assert sql.count("ORDER BY x.datadate LIMIT 1") == 8
    assert "FROM comp.g_secd" not in sql
    assert "FROM comp.secd" not in sql
    assert "MIN(x.datadate)" not in sql
    # security rows: the first daily price the monthly panel would keep
    assert "x.prcstd IN (3, 4, 10)" in sql
    assert "first_daily::date" in sql
    assert "AS comp_dprc_first" in sql


def test_age_anchor_query_rejects_untrusted_schema() -> None:
    with pytest.raises(ValueError, match="Invalid raw PostgreSQL schema"):
        build_compustat_age_anchor_query('public"; DROP SCHEMA public; --')


def test_age_anchor_download_runs_remotely_and_writes_parquet() -> None:
    conn = MagicMock()

    download_compustat_age_anchor_attached(conn, "source_db", "age.parquet", raw_schema="public")

    sql = conn.execute.call_args.args[0]
    assert "postgres_query('source_db'" in sql
    assert "age.parquet" in sql
    assert 'FROM "public"."security" s' in sql


def _write_age_anchor(test_paths, rows: list[tuple]) -> None:
    """Rows of (gvkey, iid, comp_ret_first, comp_acc_first, comp_dprc_first)."""
    pl.DataFrame(
        rows,
        schema={
            "gvkey": pl.String,
            "iid": pl.String,
            "comp_ret_first": pl.Date,
            "comp_acc_first": pl.Date,
            "comp_dprc_first": pl.Date,
        },
        orient="row",
    ).write_parquet(test_paths.raw_tables_dir / "comp_age_anchor.parquet")


def _write_panel(test_paths, rows: list[tuple]) -> None:
    """Rows of (gvkey, id, iid, eom) for world_msf."""
    pl.DataFrame(
        [(g, None, i, iid, e) for g, i, iid, e in rows],
        schema={
            "gvkey": pl.String,
            "permco": pl.Int64,
            "id": pl.Int64,
            "iid": pl.String,
            "eom": pl.Date,
        },
        orient="row",
    ).write_parquet(test_paths.interim_dir / "world_msf.parquet")


def test_firm_age_bypass_uses_full_anchor_without_crsp_file(test_paths) -> None:
    _write_panel(
        test_paths,
        [
            ("001000", 1, "01", date(2026, 7, 31)),
            ("002000", 2, "01", date(2026, 6, 30)),
            ("002000", 2, "01", date(2026, 7, 31)),
        ],
    )
    _write_age_anchor(test_paths, [("001000", None, date(1990, 4, 30), date(1980, 6, 30), None)])

    firm_age(test_paths, test_paths.interim_dir / "world_msf.parquet", bypass_crsp=True)

    result = pl.read_parquet(test_paths.interim_dir / "firm_age.parquet").sort(["id", "eom"])
    assert result.filter(pl.col("id") == 1)["age"].item() == 559
    assert result.filter(pl.col("id") == 2)["age"].to_list() == [0, 1]
    assert not (test_paths.interim_dir / "raw_data_dfs" / "crsp_msf_v2_aug.parquet").exists()


def test_firm_age_is_the_same_for_a_full_history_and_a_rolling_window(test_paths) -> None:
    """CEL-SCI: daily prices from 1983-12-30, first annual report 1985-09, first monthly
    price 1986-06. A full-history panel starts in Dec-1983; a 23-year panel starts in 2003.
    With the security's first daily price in the anchor both give 512 months in Aug-2026
    (the company anchor alone, 31-Dec-1984, gave 500). A second line of the company that
    started trading later keeps the company anchor."""
    _write_age_anchor(
        test_paths,
        [
            ("012711", None, date(1986, 6, 30), date(1985, 9, 30), None),
            ("012711", "01", None, None, date(1983, 12, 30)),
            ("012711", "02", None, None, date(2013, 12, 18)),
        ],
    )
    ages = {}
    for window, first in (("full", date(1983, 12, 31)), ("rolling", date(2003, 9, 30))):
        _write_panel(
            test_paths,
            [
                ("012711", 101271101, "01", first),
                ("012711", 101271101, "01", date(2026, 8, 31)),
                ("012711", 101271102, "02", date(2013, 12, 31)),
                ("012711", 101271102, "02", date(2026, 8, 31)),
            ],
        )
        firm_age(test_paths, test_paths.interim_dir / "world_msf.parquet", bypass_crsp=True)
        ages[window] = dict(
            pl.read_parquet(test_paths.interim_dir / "firm_age.parquet")
            .filter(pl.col("eom") == date(2026, 8, 31))
            .select("id", "age")
            .iter_rows()
        )

    assert ages["full"] == ages["rolling"] == {101271101: 512, 101271102: 500}
