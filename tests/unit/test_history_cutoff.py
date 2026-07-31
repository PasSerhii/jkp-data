"""Regression tests for bounded-history downloads and lifetime age anchors."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from jkp.data.aux_functions import (
    build_compustat_age_anchor_query,
    download_compustat_age_anchor_attached,
    firm_age,
    gen_aux_maps,
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
    assert sql.count("ORDER BY x.datadate LIMIT 1") == 7
    assert "FROM comp.g_secd" not in sql
    assert "FROM comp.secd" not in sql
    assert "MIN(x.datadate)" not in sql


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


def test_firm_age_bypass_uses_full_anchor_without_crsp_file(test_paths) -> None:
    pl.DataFrame(
        {
            "gvkey": ["001000", "002000", "002000"],
            "permco": [None, None, None],
            "id": [1, 2, 2],
            "eom": [date(2026, 7, 31), date(2026, 6, 30), date(2026, 7, 31)],
        },
        schema={"gvkey": pl.String, "permco": pl.Int64, "id": pl.Int64, "eom": pl.Date},
    ).write_parquet(test_paths.interim_dir / "world_msf.parquet")
    pl.DataFrame(
        {
            "gvkey": ["001000"],
            "comp_ret_first": [date(1990, 4, 30)],
            "comp_acc_first": [date(1980, 6, 30)],
        }
    ).write_parquet(test_paths.raw_tables_dir / "comp_age_anchor.parquet")

    firm_age(test_paths, test_paths.interim_dir / "world_msf.parquet", bypass_crsp=True)

    result = pl.read_parquet(test_paths.interim_dir / "firm_age.parquet").sort(["id", "eom"])
    assert result.filter(pl.col("id") == 1)["age"].item() == 559
    assert result.filter(pl.col("id") == 2)["age"].to_list() == [0, 1]
    assert not (test_paths.interim_dir / "raw_data_dfs" / "crsp_msf_v2_aug.parquet").exists()


def test_gen_aux_maps_uses_runtime_end_date() -> None:
    with patch("jkp.data.aux_functions.group_mapping_dfs", side_effect=lambda dates, k: dates):
        dates = gen_aux_maps("_21d", end_date=date(2026, 7, 31))

    assert dates[-1] == 2026 * 12 + 7
