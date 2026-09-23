"""Static regressions for the checked-in WRDS compatibility contract."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VIEW_DIR = ROOT / "sql" / "xpressfeed_views"


EXPECTED_COLUMN_COUNTS = {
    "funda": 949,
    "g_funda": 444,
    "fundq": 648,
    "g_fundq": 381,
    "secd": 43,
    "g_secd": 54,
    "secm": 45,
    "security": 16,
    "g_security": 15,
    "company": 40,
    "g_company": 39,
    "sec_history": 6,
    "g_sec_history": 6,
    "co_hgic": 8,
    "g_co_hgic": 8,
    "exrt_dly": 5,
    "r_ex_codes": 2,
}


def sql(name: str) -> str:
    return (VIEW_DIR / name).read_text(encoding="utf-8")


def test_checked_in_wrds_schema_contract_is_complete() -> None:
    contract = json.loads((VIEW_DIR / "wrds_schema_contract.json").read_text(encoding="utf-8"))
    assert set(contract["objects"]) == set(EXPECTED_COLUMN_COUNTS)
    for name, expected in EXPECTED_COLUMN_COUNTS.items():
        columns = contract["objects"][name]
        assert len(columns) == expected
        assert len({column for column, _ in columns}) == expected


def test_daily_views_use_complete_event_spines_and_exact_key_types() -> None:
    secd = sql("secd.sql")
    g_secd = sql("g_secd.sql")
    for ddl in (secd, g_secd):
        assert 'b."gvkey"::varchar(7) AS "gvkey"' in ddl
        assert 'b."iid"::varchar(4) AS "iid"' in ddl
        assert "SELECT gvkey, iid, datadate FROM public.sec_dprc" in ddl
        assert "SELECT gvkey, iid, datadate FROM public.sec_divid" in ddl
        assert "SELECT gvkey, iid, datadate FROM public.sec_dtrt" in ddl
        assert "LEFT JOIN LATERAL" in ddl
        assert "ORDER BY rt.datadate DESC" in ddl
    assert "NOT EXISTS (SELECT 1 FROM public.sec_dtrt x" not in secd
    assert "COALESCE(t.trfd" not in secd
    assert "NOT EXISTS (SELECT 1 FROM public.sec_dtrt x" in g_secd
    assert "THEN 1.0 END" in g_secd
    assert "COALESCE(t.trfd" in g_secd
    assert "public.sec_split" not in secd
    assert "SELECT gvkey, iid, datadate FROM public.sec_split" in g_secd
    assert "te.gvkey IS NULL" in g_secd
    assert "MAX(x.datadate) OVER" in g_secd


def test_accounting_edge_case_rules_are_generated() -> None:
    funda = sql("funda.sql")
    g_funda = sql("g_funda.sql")
    fundq = sql("fundq.sql")
    g_fundq = sql("g_fundq.sql")
    for ddl in (funda, g_funda, fundq, g_fundq):
        assert "adjex IS NOT NULL" in ddl
        assert "ORDER BY effdate DESC, thrudate DESC, adjex DESC" in ddl
    assert "CASE des.curcd WHEN 'CAD' THEN ph1.itemvalue" in funda
    assert "WHEN 'USD' THEN ph0.itemvalue END" in funda
    assert "CASE des.curcdq WHEN 'CAD' THEN ph1.itemvalue" in fundq
    assert "WHEN 'USD' THEN ph0.itemvalue END" in fundq
    for ddl in (funda, fundq):
        assert "COALESCE(ph1.itemvalue, ph0.itemvalue" not in ddl
        assert "item='PRIHISTROW'" not in ddl
        assert "co.fic='CAN'" not in ddl
    for ddl in (g_funda, g_fundq):
        assert "item='PRIHISTROW'" in ddl
        assert "item='PRIHISTUSA'" not in ddl
        assert "item='PRIHISTCAN'" not in ddl
    assert 'mkf."year"=0' in funda
    assert funda.index("LEFT JOIN public.co_amkt mkf") < funda.index("LEFT JOIN public.co_amkt mkc")
    assert "THEN EXTRACT(YEAR FROM des.datadate)" in funda
    assert 'WHEN mkf.gvkey IS NOT NULL THEN mkf."year"' in funda
    assert "SELECT 1 AS has_fiscal_year" in funda
    assert "WHERE mkf.gvkey IS NULL AND mkc.datadate=des.datadate" in funda
    assert "mky.has_fiscal_year IS NULL" in funda
    assert (
        'CASE WHEN (mkf.gvkey IS NOT NULL AND mkf."year" IS NOT NULL) OR '
        "(mkf.gvkey IS NULL AND mkc.datadate=des.datadate "
        'AND mky.has_fiscal_year IS NULL) THEN mkc."prcc" END' in funda
    )
    assert (
        "CASE WHEN mkc.gvkey IS NOT NULL AND ((mkf.gvkey IS NOT NULL AND "
        'mkf."year" IS NOT NULL) OR (mkf.gvkey IS NULL AND '
        "mkc.datadate=des.datadate AND mky.has_fiscal_year IS NULL)) "
        "THEN afc.adjex END" in funda
    )
    assert "effdate <= mkc.datadate" in funda
    assert "CASE WHEN mkf.gvkey IS NOT NULL THEN aff.adjex END" in funda
    assert "CASE WHEN mkf.gvkey IS NOT NULL THEN aff.adjex END" in fundq


def test_monthly_view_sources_spind_and_gates_quarterly_shares() -> None:
    ddl = sql("secm.sql")
    for column in (
        "sph100",
        "sphcusip",
        "sphiid",
        "sphmid",
        "sphname",
        "sphsec",
        "sphtic",
        "sphvg",
    ):
        assert f'spind."{column}"' in ddl
    assert "LEFT JOIN public.sec_spind spind" in ddl
    assert "LEFT JOIN LATERAL" in ddl
    assert "SELECT MAX(q.cshoq) AS cshoq" in ddl
    assert "b.iid=COALESCE(co.priusa, co.prican, co.prirow)" in ddl


def test_population_is_derived_from_live_company_and_security_data() -> None:
    ddl = sql("_population.sql")
    assert "comp.wrds_population" not in ddl
    assert "public.sec_history" not in ddl
    assert "prirow" not in ddl
    assert "priusa" not in ddl
    assert "prican" not in ddl
    assert "CREATE OR REPLACE VIEW comp._gl_gvkeys" in ddl
    assert ddl.count("SELECT co.gvkey::varchar AS gvkey") == 2
    assert "co.fic IS DISTINCT FROM 'USA'" in ddl
    assert "co.fic IS DISTINCT FROM 'CAN'" in ddl
    assert "s.iid LIKE '%W'" in ddl
    assert "CREATE OR REPLACE VIEW comp._na_gvkeys" in ddl
    assert "s.iid NOT LIKE '%W'" in ddl


def test_sp500_flag_is_an_intentional_typed_null_placeholder() -> None:
    for name in ("company.sql", "security.sql"):
        ddl = sql(name)
        assert 'NULL::float8 AS "curr_sp500_flag"' in ddl
        assert "comp.wrds_sp500" not in ddl


def test_installer_has_no_wrds_population_or_sp500_snapshot_dependency() -> None:
    installer = sql("apply_views.py")
    loader = sql("load_ff_factors.py")
    assert "load_wrds_population" not in installer
    assert "wrds_population" not in installer
    assert "wrds_sp500" not in installer
    assert "--skip-snapshot-refresh" not in installer
    assert "--allow-stale-snapshot" not in installer
    assert "--skip-ff-refresh" not in installer
    assert not (VIEW_DIR / "load_wrds_population.py").exists()

    assert "_verify_current_ff_factors(cur, ff_refresh)" in installer
    for source in (installer, loader):
        assert "compatibility_snapshot_history" not in source
        assert "compatibility_view_build_history" not in source


def test_ff_schema_verification_precedes_mutation_and_is_part_of_build_gate() -> None:
    loader = sql("load_ff_factors.py")
    transaction = loader.index('"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"')
    timeout = loader.index("wc.execute(\"SET LOCAL statement_timeout = '300s'\")")
    source_as_of = loader.index('wc.execute("select current_timestamp")')
    source_read = loader.index("wc.execute(FF_FACTORS_MONTHLY_SELECT)")
    create_target = loader.index('"""CREATE TABLE IF NOT EXISTS ff.factors_monthly (')
    verify_target = loader.index("_verify_ff_factors_schema(rc)")
    create_stage = loader.index('"""CREATE TEMP TABLE ff_factors_monthly_stage (')
    mutate_target = loader.index(
        'rc.execute("LOCK TABLE ff.factors_monthly IN ACCESS EXCLUSIVE MODE")'
    )
    assert transaction < timeout < source_as_of < source_read
    assert create_target < verify_target < create_stage < mutate_target

    installer = sql("apply_views.py")
    verify_function = installer.index("def _verify_targets(")
    verify_ff = installer.index("_verify_ff_factors_schema(cur)", verify_function)
    next_function = installer.index("def main(", verify_function)
    assert verify_function < verify_ff < next_function


def test_fama_french_snapshot_has_exact_checked_in_types() -> None:
    ddl = sql("ff_factors_monthly.sql")
    assert "mktrf numeric(8,6)" in ddl
    assert "rf numeric(7,5)" in ddl
    assert "umd numeric(8,6)" in ddl
    assert "factors_monthly_date_idx" in ddl


def test_current_exchange_description_typmod_and_safe_sample_ordering() -> None:
    assert '::varchar(101) AS "exchgdesc"' in sql("r_ex_codes.sql")
    validator = sql("validate_post_fable_fresh.py")
    assert "SELECT {selected} FROM (SELECT DISTINCT {selected}" in validator
    assert "FROM {spec.sql_name}{where}) matched_keys" in validator
