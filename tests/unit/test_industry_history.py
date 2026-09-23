"""Missing industry recovery must not rewrite populated values or leak future codes."""

from datetime import date

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from jkp.data.aux_functions import ff_ind_class, merge_industry_to_world_msf
from jkp.data.industry import (
    build_industry_history_query,
    fill_missing_industry_codes,
    fill_missing_sic,
    resolve_industry_history,
    resolve_sic_history,
)


def history_frame(rows):
    return pl.DataFrame(
        rows,
        schema={
            "gvkey": pl.String,
            "datadate": pl.Date,
            "sich": pl.Int64,
            "naicsh": pl.String,
            "popsrc": pl.String,
            "consol": pl.String,
        },
        orient="row",
    )


def test_history_respects_changes_partial_null_updates_and_unknown_codes():
    history = history_frame(
        [
            ("001000", date(1990, 1, 1), 4813, "517", "D", "C"),
            ("001000", date(2020, 3, 1), 7372, "511", "D", "C"),
            ("001000", date(2020, 4, 1), None, "522", "D", "C"),
            ("001000", date(2020, 5, 1), 0, None, "D", "C"),
            ("001000", date(2020, 6, 1), 6020, None, "D", "C"),
            # An entirely blank row supplies no industry classification.
            ("001000", date(2020, 7, 1), None, None, "D", "C"),
        ]
    )
    months = [date(1989, 12, 31)] + [date(2020, month, 28) for month in range(2, 8)]
    keys = pl.DataFrame({"gvkey": ["001000"] * len(months), "eom": months})
    resolved = resolve_sic_history(keys.lazy(), history.lazy()).collect().sort("eom")
    assert resolved["sic_history"].to_list() == [None, 4813, 7372, None, None, 6020, 6020]
    assert resolved.filter(pl.col("datadate") > pl.col("eom")).is_empty()


def test_ties_prefer_na_then_consolidated_and_do_not_guess_conflicts():
    stamp = date(2002, 1, 1)
    history = history_frame(
        [
            ("001000", stamp, 4813, None, "I", "C"),
            ("001000", stamp, 6020, None, "D", "N"),
            ("001000", stamp, 7372, None, "D", "C"),
            ("001000", stamp, 7372, None, "D", "C"),
            ("002000", stamp, 4813, None, "I", "N"),
            ("003000", stamp, 6020, None, "D", "C"),
            ("003000", stamp, 7372, None, "D", "C"),
        ]
    )
    keys = pl.DataFrame({"gvkey": ["001000", "002000", "003000"], "eom": [date(2026, 8, 31)] * 3})
    result = resolve_sic_history(keys.lazy(), history.lazy()).collect().sort("gvkey")
    assert result["sic_history"].to_list() == [7372, 4813, None]
    assert result.height == 3


def test_fill_preserves_rows_order_populated_sic_and_every_other_column():
    stamp = date(2026, 8, 31)
    data = pl.DataFrame(
        {
            "id": [9, 1, 7, 3],
            "gvkey": ["001000", "002000", "001000", "003000"],
            "eom": [stamp] * 4,
            "sic": [None, 6020, None, None],
            "naics": [None, 522110, None, 333],
            "me": [2.5, 4.3, 7.0, 6.0],
            "ret_exc": [0.04, -0.02, None, 0.01],
        }
    )
    history = history_frame(
        [
            ("001000", date(2000, 1, 1), 4813, "517", "D", "C"),
            ("002000", date(2000, 1, 1), 7372, "511", "D", "C"),
            ("003000", date(2026, 9, 1), 6020, "522", "D", "C"),
        ]
    )
    resolved = resolve_sic_history(data.lazy(), history.lazy())
    fixed = fill_missing_sic(data.lazy(), resolved).collect()
    assert fixed["sic"].to_list() == [4813, 6020, 4813, None]
    assert_frame_equal(data.drop("sic"), fixed.drop("sic"), check_exact=True)
    # A malformed resolver result must fail rather than multiply security rows.
    with pytest.raises(pl.exceptions.ComputeError, match="m:1"):
        fill_missing_sic(data.lazy(), pl.concat([resolved, resolved])).collect()


@pytest.mark.parametrize("schema", ["comp", "public"])
def test_download_retains_pre_window_anchor_ties_and_excludes_future(schema):
    rows = history_frame(
        [
            ("001000", date(1980, 1, 1), 1111, None, "D", "C"),
            ("001000", date(1990, 1, 1), 4813, None, "D", "C"),
            ("001000", date(1990, 1, 1), 7372, None, "I", "C"),
            ("001000", date(2020, 1, 1), 6020, None, "D", "C"),
            ("001000", date(2026, 9, 1), 2000, None, "D", "C"),
            ("002000", date(1980, 1, 1), 4813, None, "I", "N"),
            ("002000", date(2000, 1, 1), None, None, "I", "C"),
        ]
    )
    with duckdb.connect() as con:
        con.execute(f'CREATE SCHEMA "{schema}"')
        con.register("rows", rows)
        for table, predicate in (
            [("co_industry", "popsrc='D'"), ("g_co_industry", "popsrc='I'")]
            if schema == "comp"
            else [("co_industry", "TRUE")]
        ):
            con.execute(
                f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM rows WHERE {predicate}'
            )
        query = build_industry_history_query(schema, date(2005, 1, 1), date(2026, 8, 31))
        compact = con.sql(query).pl()
        full = con.sql(build_industry_history_query(schema, None, date(2026, 8, 31))).pl()
    assert compact.height == 4
    assert full.height == 5
    assert compact.filter(pl.col("datadate") == date(1990, 1, 1)).height == 2
    keys = pl.DataFrame({"gvkey": ["001000", "002000"], "eom": [date(2006, 1, 31)] * 2})
    assert_frame_equal(
        resolve_sic_history(keys.lazy(), compact.lazy()).collect().sort("gvkey"),
        resolve_sic_history(keys.lazy(), full.lazy()).collect().sort("gvkey"),
    )


def test_download_rejects_invalid_schema():
    with pytest.raises(ValueError, match="Invalid raw PostgreSQL schema"):
        build_industry_history_query('public"; SELECT 1; --', None, None)


@pytest.mark.parametrize("bypass_crsp", [True, False])
def test_pipeline_merge_and_ff49_keep_existing_source_precedence(test_paths, bypass_crsp):
    stamp = date(2026, 8, 31)
    paths = test_paths
    world = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "gvkey": ["001000", "002000", "003000"],
            "eom": [stamp] * 3,
            "permco": [1, 2, 3],
            "permno": [1, 2, 3],
            "me": [100.0, 200.0, 300.0],
        }
    )
    world.write_parquet(paths.interim_dir / "__msf_world.parquet")
    pl.DataFrame(
        {
            "gvkey": world["gvkey"],
            "date": [stamp] * 3,
            "sic": [None, 7372, None],
            "naics": [None, 511210, None],
            "gics": [None, 45103010, None],
        }
    ).write_parquet(paths.interim_dir / "comp_ind.parquet")
    history_frame(
        [(key, date(2000, 1, 1), 4813, "517", "D", "C") for key in world["gvkey"]]
    ).write_parquet(paths.raw_tables_dir / "comp_industry_history.parquet")
    if not bypass_crsp:
        pl.DataFrame(
            {"permco": [3], "permno": [3], "date": [stamp], "sic": [6020], "naics": [522110]}
        ).write_parquet(paths.interim_dir / "crsp_ind.parquet")
    merge_industry_to_world_msf(paths, bypass_crsp=bypass_crsp)
    ff_ind_class(paths, paths.interim_dir / "__msf_world2.parquet")
    result = pl.read_parquet(paths.interim_dir / "__msf_world3.parquet").sort("id")
    assert result["sic"].to_list() == [4813, 7372, 4813 if bypass_crsp else 6020]
    assert result["naics"].to_list() == [517, 511210, 517 if bypass_crsp else 522110]
    assert result["ff49"].to_list() == [32, 36, 32 if bypass_crsp else 45]
    assert_frame_equal(result.select(world.columns), world, check_exact=True)
    assert pl.read_parquet(paths.interim_dir / "sic_history_fallback.parquet")["datadate"].to_list()


def test_naics_latest_partial_update_blocks_older_code_and_never_uses_future():
    history = history_frame(
        [
            ("001000", date(2014, 12, 31), 6799, "523999", "D", "C"),
            ("001000", date(2025, 12, 31), 6799, None, "D", "C"),
            ("001000", date(2026, 9, 1), 7011, "721110", "D", "C"),
            ("002000", date(2002, 1, 1), 1000, "2122", "I", "N"),
        ]
    )
    data = pl.DataFrame(
        {
            "gvkey": ["001000", "001000", "002000"],
            "eom": [date(2024, 8, 31), date(2026, 8, 31), date(2026, 8, 31)],
            "sic": [6799, 6799, None],
            "naics": [None, None, None],
        }
    )
    out = fill_missing_industry_codes(
        data.lazy(), resolve_industry_history(data.lazy(), history.lazy())
    ).collect()
    assert out["naics"].to_list() == [523999, None, 2122]
    assert out["sic"].to_list() == [6799, 6799, 1000]


def test_naics_conflicts_and_invalid_codes_do_not_invalidate_unambiguous_sic():
    stamp = date(2000, 1, 1)
    history = history_frame(
        [
            ("001000", stamp, 4813, "517", "D", "C"),
            ("001000", stamp, 4813, "518", "D", "C"),
            ("002000", stamp, 4813, "523999.5", "D", "C"),
            ("003000", stamp, 4813, "522", "D", "C"),
            ("003000", stamp, 4813, None, "D", "C"),
            ("004000", stamp, 4813, "00", "D", "C"),
        ]
    )
    keys = pl.DataFrame(
        {"gvkey": ["001000", "002000", "003000", "004000"], "eom": [date(2026, 8, 31)] * 4}
    )
    out = resolve_industry_history(keys.lazy(), history.lazy()).collect().sort("gvkey")
    assert out["naics_history"].to_list() == [None, None, None, None]
    assert out["sic_history"].to_list() == [4813, 4813, 4813, 4813]


def test_naics_fill_preserves_populated_values_row_order_types_and_other_fields():
    data = pl.DataFrame(
        {
            "id": [4, 1, 9],
            "gvkey": ["001000"] * 3,
            "eom": [date(2026, 8, 31)] * 3,
            "sic": [7372] * 3,
            "naics": [None, 511210, None],
            "ret": [0.1, None, -0.2],
        },
        schema_overrides={"naics": pl.Int32},
    )
    history = history_frame([("001000", date(2000, 1, 1), 4813, "517", "D", "C")])
    resolved = resolve_industry_history(data.lazy(), history.lazy())
    out = fill_missing_industry_codes(data.lazy(), resolved).collect()
    assert out["naics"].to_list() == [517, 511210, 517]
    assert out.schema["naics"] == pl.Int32
    assert_frame_equal(data.drop("naics"), out.drop("naics"), check_exact=True)
    with pytest.raises(pl.exceptions.ComputeError, match="m:1"):
        fill_missing_industry_codes(data.lazy(), pl.concat([resolved, resolved])).collect()
