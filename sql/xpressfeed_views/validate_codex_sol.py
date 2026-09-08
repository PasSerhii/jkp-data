"""Independent, reproducible WRDS/RDS validation helpers.

This script intentionally does not read any repository validation report.  It
loads database credentials from the repository .env, applies the required RDS
statement timeout, and writes only aggregate comparison evidence/examples.

Run with:
    uv run --with "psycopg[binary]" python \
        sql/xpressfeed_views/validate_codex_sol.py all \
        --result-file sql/xpressfeed_views/VALIDATION_EVIDENCE_CODEX_Sol_POSTFIX.json \
        --force
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import math
import os
import time
from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import groupby, zip_longest
from pathlib import Path
from typing import Any

import psycopg

ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / ".env"
DEFAULT_RESULT_FILE = ROOT / "sql" / "xpressfeed_views" / "VALIDATION_EVIDENCE_CODEX_Sol.json"
CUTOFF = dt.date(2026, 6, 30)
SEED = "codex-sol-2026-07-18"
REL_TOL = 1e-9
ABS_TOL = 1e-12

# Catalog-name fields are deliberately excluded: the two databases naturally
# have different catalog names.  These are the information_schema attributes
# that define the ordered column contract and its complete type metadata.
SCHEMA_METADATA_FIELDS = (
    "ordinal_position",
    "column_name",
    "is_nullable",
    "data_type",
    "character_maximum_length",
    "character_octet_length",
    "numeric_precision",
    "numeric_precision_radix",
    "numeric_scale",
    "datetime_precision",
    "interval_type",
    "interval_precision",
    "character_set_schema",
    "character_set_name",
    "collation_schema",
    "collation_name",
    "domain_schema",
    "domain_name",
    "udt_schema",
    "udt_name",
    "maximum_cardinality",
)


@dataclass(frozen=True)
class Obj:
    schema: str
    name: str
    key: tuple[str, ...]
    cutoff_col: str | None = None

    @property
    def fq(self) -> str:
        return f'{qi(self.schema)}.{qi(self.name)}'

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.name}"


OBJECTS = {
    o.label: o
    for o in (
        Obj("comp", "funda", ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc"), "datadate"),
        Obj("comp", "g_funda", ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc"), "datadate"),
        Obj("comp", "fundq", ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "fyr"), "datadate"),
        Obj("comp", "g_fundq", ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "fyr"), "datadate"),
        Obj("comp", "secd", ("gvkey", "iid", "datadate"), "datadate"),
        Obj("comp", "g_secd", ("gvkey", "iid", "datadate"), "datadate"),
        Obj("comp", "secm", ("gvkey", "iid", "datadate"), "datadate"),
        Obj("comp", "security", ("gvkey", "iid")),
        Obj("comp", "g_security", ("gvkey", "iid")),
        Obj("comp", "company", ("gvkey",)),
        Obj("comp", "g_company", ("gvkey",)),
        Obj("comp", "sec_history", ("gvkey", "iid", "item", "effdate")),
        Obj("comp", "g_sec_history", ("gvkey", "iid", "item", "effdate")),
        Obj("comp", "co_hgic", ("gvkey", "indtype", "indfrom")),
        Obj("comp", "g_co_hgic", ("gvkey", "indtype", "indfrom")),
        Obj("comp", "exrt_dly", ("tocurd", "datadate"), "datadate"),
        Obj("comp", "r_ex_codes", ("exchgcd",)),
        Obj("ff", "factors_monthly", ("date",), "date"),
    )
}

FULL_VALUE_OBJECTS = (
    "comp.security",
    "comp.g_security",
    "comp.company",
    "comp.g_company",
    "comp.sec_history",
    "comp.g_sec_history",
    "comp.co_hgic",
    "comp.g_co_hgic",
    "comp.r_ex_codes",
    "comp.exrt_dly",
    "ff.factors_monthly",
)

# Frozen from the pipeline audit in VALIDATION_REPORT_CODEX_Sol.md.  Keep this
# contract in code: validation must not depend on parsing a prose report.
PIPELINE_COLUMNS: dict[str, tuple[str, ...]] = {
    "comp.funda": ("aco", "act", "ajex", "ao", "ap", "at", "capx", "ceq", "che", "cogs", "consol", "csho", "curcd", "datadate", "datafmt", "dlc", "dlcch", "dltis", "dltr", "dltt", "do", "dp", "dpc", "dv", "dvc", "dvt", "ebit", "ebitda", "emp", "fiao", "fincf", "gdwl", "gp", "gvkey", "ib", "ibc", "icapt", "indfmt", "intan", "invt", "itcb", "ivao", "ivst", "lco", "lct", "lo", "lt", "mib", "mii", "naicsh", "ni", "nopi", "oancf", "oiadp", "oibdp", "pi", "popsrc", "ppegt", "ppent", "prstkc", "pstk", "pstkl", "pstkrv", "re", "rect", "revt", "sale", "seq", "sich", "spi", "sstk", "txbcof", "txdb", "txditc", "txp", "txt", "xad", "xi", "xido", "xidoc", "xint", "xlr", "xopr", "xrd", "xsga"),
    "comp.g_funda": ("aco", "act", "ao", "ap", "at", "capx", "ceq", "che", "cogs", "consol", "curcd", "datadate", "datafmt", "dlc", "dlcch", "dltis", "dltr", "dltt", "do", "dp", "dpc", "dv", "dvc", "dvt", "ebit", "ebitda", "emp", "fiao", "fincf", "gdwl", "gvkey", "ib", "ibc", "icapt", "indfmt", "intan", "invt", "ivao", "ivst", "lco", "lct", "lo", "lt", "ltdch", "mib", "mii", "naicsh", "nopi", "oancf", "oiadp", "oibdp", "pi", "popsrc", "ppegt", "ppent", "prstkc", "pstk", "purtshr", "re", "rect", "revt", "sale", "seq", "sich", "spi", "sstk", "txdb", "txditc", "txp", "txt", "wcapt", "xi", "xido", "xidoc", "xint", "xlr", "xopr", "xrd", "xsga"),
    "comp.fundq": ("acoq", "actq", "ajexq", "aoq", "apq", "atq", "capxy", "ceqq", "cheq", "cogsq", "cogsy", "consol", "cshoq", "curcdq", "datadate", "datafmt", "dlcchy", "dlcq", "dltisy", "dltry", "dlttq", "doq", "doy", "dpcy", "dpq", "dpy", "dvy", "fiaoy", "fincfy", "fqtr", "fyearq", "fyr", "gdwlq", "gvkey", "ibcy", "ibq", "iby", "icaptq", "indfmt", "intanq", "invtq", "ivaoq", "ivstq", "lcoq", "lctq", "loq", "ltq", "mibq", "miiq", "miiy", "niq", "niy", "nopiq", "nopiy", "oancfy", "oiadpq", "oiadpy", "oibdpq", "oibdpy", "piq", "piy", "popsrc", "ppegtq", "ppentq", "prstkcy", "pstkq", "rectq", "req", "revtq", "revty", "saleq", "saley", "seqq", "spiq", "spiy", "sstky", "txbcofy", "txdbq", "txditcq", "txpq", "txtq", "txty", "xidocy", "xidoq", "xidoy", "xintq", "xinty", "xiq", "xiy", "xoprq", "xopry", "xrdq", "xrdy", "xsgaq", "xsgay"),
    "comp.g_fundq": ("acoq", "actq", "aoq", "apq", "atq", "capxy", "ceqq", "cheq", "cogsq", "cogsy", "consol", "curcdq", "datadate", "datafmt", "dlcchy", "dlcq", "dltisy", "dltry", "dlttq", "dpactq", "dpcy", "dpq", "dpy", "dvtq", "dvty", "dvy", "fiaoy", "fincfy", "fqtr", "fyearq", "fyr", "gdwlq", "gpq", "gpy", "gvkey", "ibcy", "ibq", "iby", "indfmt", "intanq", "invtq", "ivaoq", "ivstq", "lcoq", "lctq", "loq", "ltdchy", "ltq", "mibq", "miiq", "miiy", "nopiq", "nopiy", "oancfy", "oiadpq", "oiadpy", "oibdpq", "oibdpy", "piq", "piy", "popsrc", "ppentq", "prstkcy", "pstkq", "purtshry", "rectq", "req", "revtq", "revty", "saleq", "saley", "seqq", "spiq", "spiy", "sstky", "txdbq", "txtq", "txty", "wcapty", "xidocy", "xintq", "xinty", "xiq", "xiy", "xoprq", "xopry", "xsgaq", "xsgay"),
    "comp.secd": ("ajexdi", "conm", "cshoc", "cshtrd", "curcdd", "curcddv", "cusip", "datadate", "div", "divd", "divsp", "exchg", "gvkey", "iid", "prccd", "prchd", "prcld", "prcod", "prcstd", "tpci", "trfd"),
    "comp.g_secd": ("ajexdi", "conm", "cshoc", "cshtrd", "curcdd", "curcddv", "datadate", "div", "divd", "divsp", "exchg", "gvkey", "iid", "isin", "monthend", "prccd", "prchd", "prcld", "prcod", "prcstd", "qunit", "sedol", "tpci", "trfd"),
    "comp.secm": ("ajexm", "csfsm", "cshom", "cshoq", "cshtrm", "curcddvm", "curcdm", "datadate", "dvpsxm", "exchg", "gvkey", "iid", "prccm", "prchm", "prclm", "tpci", "trfm"),
    "comp.security": ("gvkey", "iid", "secstat", "dlrsni", "exchg", "excntry"),
    "comp.g_security": ("gvkey", "iid", "secstat", "dlrsni", "exchg", "excntry"),
    "comp.company": ("gvkey", "prirow", "priusa", "prican"),
    "comp.g_company": ("gvkey", "prirow", "priusa", "prican"),
    "comp.sec_history": ("gvkey", "item", "itemvalue", "effdate", "thrudate"),
    "comp.g_sec_history": ("gvkey", "item", "itemvalue", "effdate", "thrudate"),
    "comp.co_hgic": ("gvkey", "indfrom", "indthru", "gsubind"),
    "comp.g_co_hgic": ("gvkey", "indfrom", "indthru", "gsubind"),
    "comp.exrt_dly": ("tocurd", "datadate", "exratd", "fromcurd"),
    "comp.r_ex_codes": ("exchgcd", "exchgdesc"),
    "ff.factors_monthly": ("date", "rf"),
}

EXPECTED_PIPELINE_COLUMN_COUNTS = {
    "comp.funda": 85,
    "comp.g_funda": 79,
    "comp.fundq": 95,
    "comp.g_fundq": 88,
    "comp.secd": 21,
    "comp.g_secd": 24,
    "comp.secm": 17,
    "comp.security": 6,
    "comp.g_security": 6,
    "comp.company": 4,
    "comp.g_company": 4,
    "comp.sec_history": 5,
    "comp.g_sec_history": 5,
    "comp.co_hgic": 4,
    "comp.g_co_hgic": 4,
    "comp.exrt_dly": 4,
    "comp.r_ex_codes": 2,
    "ff.factors_monthly": 2,
}

if set(PIPELINE_COLUMNS) != set(OBJECTS) or {
    label: len(names) for label, names in PIPELINE_COLUMNS.items()
} != EXPECTED_PIPELINE_COLUMN_COUNTS:
    raise RuntimeError("The encoded pipeline-column contract is incomplete or has wrong counts")


def qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def load_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def connect() -> tuple[psycopg.Connection[Any], psycopg.Connection[Any]]:
    env = load_env()
    rds_dsn = env["COMPUSTAT"].replace("postgresql+psycopg2://", "postgresql://")
    rds = psycopg.connect(rds_dsn, connect_timeout=30, application_name="codex_sol_validation")
    wrds = psycopg.connect(
        host="wrds-pgdata.wharton.upenn.edu",
        port=9737,
        dbname="wrds",
        user=env["ENV_USERNAME"],
        password=env["ENV_PASSWORD"],
        sslmode="require",
        connect_timeout=30,
        application_name="codex_sol_validation",
    )
    for conn in (rds, wrds):
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")
        conn.commit()
    return rds, wrds


def columns(conn: psycopg.Connection[Any], obj: Obj) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_schema=%s AND table_name=%s
               ORDER BY ordinal_position""",
            (obj.schema, obj.name),
        )
        return [r[0] for r in cur.fetchall()]


def schema_metadata(conn: psycopg.Connection[Any], obj: Obj) -> list[dict[str, Any]]:
    selected = ", ".join(qi(field) for field in SCHEMA_METADATA_FIELDS)
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT {selected}
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position""",
            (obj.schema, obj.name),
        )
        return [dict(zip(SCHEMA_METADATA_FIELDS, row, strict=False)) for row in cur.fetchall()]


def compare_schema_object(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], obj: Obj
) -> dict[str, Any]:
    rds_columns = schema_metadata(rds, obj)
    wrds_columns = schema_metadata(wrds, obj)
    mismatches: list[dict[str, Any]] = []
    mismatches_by_field: defaultdict[str, int] = defaultdict(int)

    for rds_column, wrds_column in zip_longest(rds_columns, wrds_columns):
        differences: dict[str, dict[str, Any]] = {}
        if rds_column is None or wrds_column is None:
            differences["__missing_column__"] = {
                "rds": rds_column,
                "wrds": wrds_column,
            }
            mismatches_by_field["__missing_column__"] += 1
        else:
            for field in SCHEMA_METADATA_FIELDS:
                if rds_column[field] != wrds_column[field]:
                    differences[field] = {
                        "rds": rds_column[field],
                        "wrds": wrds_column[field],
                    }
                    mismatches_by_field[field] += 1
        if differences:
            mismatches.append(
                {
                    "ordinal_position": (
                        rds_column or wrds_column or {}
                    ).get("ordinal_position"),
                    "rds_column_name": None if rds_column is None else rds_column["column_name"],
                    "wrds_column_name": None if wrds_column is None else wrds_column["column_name"],
                    "differences": differences,
                }
            )

    rds_order = [(c["ordinal_position"], c["column_name"]) for c in rds_columns]
    wrds_order = [(c["ordinal_position"], c["column_name"]) for c in wrds_columns]
    return {
        "rds_column_count": len(rds_columns),
        "wrds_column_count": len(wrds_columns),
        "column_names_and_order_identical": rds_order == wrds_order,
        "type_metadata_identical": not any(
            field not in {"ordinal_position", "column_name"}
            for field in mismatches_by_field
        ),
        "identical": not mismatches,
        "mismatch_columns": len(mismatches),
        "mismatch_fields": sum(mismatches_by_field.values()),
        "mismatches_by_field": dict(sorted(mismatches_by_field.items())),
        "mismatches": mismatches,
        "rds_columns": rds_columns,
        "wrds_columns": wrds_columns,
    }


def schema_summary(comparisons: dict[str, Any]) -> dict[str, Any]:
    return {
        "objects_compared": len(comparisons),
        "objects_identical": sum(bool(item.get("identical")) for item in comparisons.values()),
        "rds_columns_compared": sum(int(item.get("rds_column_count", 0)) for item in comparisons.values()),
        "wrds_columns_compared": sum(int(item.get("wrds_column_count", 0)) for item in comparisons.values()),
        "mismatch_columns": sum(int(item.get("mismatch_columns", 0)) for item in comparisons.values()),
        "mismatch_fields": sum(int(item.get("mismatch_fields", 0)) for item in comparisons.values()),
        "mismatch_objects": [
            label for label, item in comparisons.items() if not item.get("identical")
        ],
    }


def load_results(result_file: Path) -> dict[str, Any]:
    if result_file.exists():
        return json.loads(result_file.read_text(encoding="utf-8"))
    return {
        "metadata": {
            "cutoff": CUTOFF.isoformat(),
            "seed": SEED,
            "relative_tolerance": REL_TOL,
            "absolute_tolerance": ABS_TOL,
        }
    }


def save_results(results: dict[str, Any], result_file: Path) -> None:
    result_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = result_file.with_name(result_file.name + ".tmp")
    tmp.write_text(json.dumps(results, indent=2, sort_keys=True, default=json_default), encoding="utf-8")
    os.replace(tmp, result_file)


def json_default(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def run_schema(result_file: Path, force: bool = False) -> None:
    results = load_results(result_file)
    comparisons: dict[str, Any] = results.get("schema_comparisons", {})
    if force:
        comparisons = {}
        results["schema_comparisons"] = comparisons
        results.pop("schema_summary", None)
        save_results(results, result_file)

    rds, wrds = connect()
    try:
        for label, obj in OBJECTS.items():
            if label in comparisons:
                print(f"schema reuse {label}", flush=True)
                continue
            print(f"schema start {label}", flush=True)
            comparisons[label] = compare_schema_object(rds, wrds, obj)
            results["schema_comparisons"] = comparisons
            results["schema_summary"] = schema_summary(comparisons)
            save_results(results, result_file)
            print(
                f"schema done {label} identical={comparisons[label]['identical']} "
                f"mismatch_fields={comparisons[label]['mismatch_fields']}",
                flush=True,
            )
        results["schema_summary"] = schema_summary(comparisons)
        save_results(results, result_file)
    finally:
        rds.close()
        wrds.close()


def scalar(conn: psycopg.Connection[Any], query: str, params: Sequence[Any] = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


def full_counts(rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    excluded = {"comp.secd", "comp.g_secd"}
    for label, obj in OBJECTS.items():
        if label in excluded:
            continue
        item: dict[str, Any] = {}
        print(f"count start {label}", flush=True)
        for side, conn in (("rds", rds), ("wrds", wrds)):
            started = time.monotonic()
            try:
                item[side] = int(scalar(conn, f"SELECT count(*) FROM {obj.fq}"))
                item[f"{side}_seconds"] = round(time.monotonic() - started, 3)
            except Exception as exc:  # captured so one timeout does not erase completed evidence
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SET statement_timeout = '300s'")
                conn.commit()
                item[f"{side}_error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
        if "rds" in item and "wrds" in item:
            item["difference"] = item["rds"] - item["wrds"]
        out[label] = item
        print(f"count done {label} rds={item.get('rds')} wrds={item.get('wrds')}", flush=True)
    return out


def deterministic_header_candidates(
    conn: psycopg.Connection[Any], header: str, limit: int = 600
) -> list[tuple[str, str]]:
    query = f"""
        SELECT gvkey, iid
        FROM comp.{qi(header)}
        ORDER BY md5(gvkey || '|' || iid || %s)
        LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (SEED, limit))
        return [(r[0], r[1]) for r in cur.fetchall()]


def pair_stats(
    conn: psycopg.Connection[Any], obj: Obj, pair: tuple[str, str]
) -> tuple[int, dt.date | None, dt.date | None]:
    query = f"""
        SELECT count(*), min(datadate), max(datadate)
        FROM {obj.fq}
        WHERE gvkey=%s AND iid=%s AND datadate <= %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (pair[0], pair[1], CUTOFF))
        n, lo, hi = cur.fetchone()
        return int(n), lo, hi


def pair_stats_batch(
    conn: psycopg.Connection[Any], obj: Obj, pairs: Sequence[tuple[str, str]]
) -> dict[tuple[str, str], tuple[int, dt.date | None, dt.date | None]]:
    predicate, params = pair_filter(pairs)
    query = f"""
        SELECT gvkey, iid, count(*), min(datadate), max(datadate)
        FROM {obj.fq}
        WHERE {predicate} AND datadate <= %s
        GROUP BY gvkey, iid
    """
    with conn.cursor() as cur:
        cur.execute(query, [*params, CUTOFF])
        return {(r[0], r[1]): (int(r[2]), r[3], r[4]) for r in cur.fetchall()}


def raw_daily_presence_batch(
    conn: psycopg.Connection[Any], schema: str, table: str, pairs: Sequence[tuple[str, str]]
) -> set[tuple[str, str]]:
    predicate, params = pair_filter(pairs)
    query = f"""
        SELECT gvkey, iid
        FROM {qi(schema)}.{qi(table)}
        WHERE {predicate} AND datadate <= %s
        GROUP BY gvkey, iid
    """
    with conn.cursor() as cur:
        cur.execute(query, [*params, CUTOFF])
        return {(r[0], r[1]) for r in cur.fetchall()}


def sampled_daily_counts(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], label: str, target: int = 110
) -> dict[str, Any]:
    obj = OBJECTS[label]
    header = "g_security" if label == "comp.g_secd" else "security"
    wrds_candidates = deterministic_header_candidates(wrds, header)
    rds_candidates = set(deterministic_header_candidates(rds, header, 1200))
    candidates = [p for p in wrds_candidates if p in rds_candidates]
    # Header rows often have no daily history.  Pre-filter through the indexed
    # raw spine on each system so expensive compatibility-view batches contain
    # only common, nonempty histories.  These raw probes are selection only;
    # reported counts/ranges below still come from comp.secd/comp.g_secd.
    rds_nonempty: set[tuple[str, str]] = set()
    wrds_nonempty: set[tuple[str, str]] = set()
    for start in range(0, len(candidates), 200):
        raw_batch = candidates[start : start + 200]
        rds_nonempty.update(raw_daily_presence_batch(rds, "public", "sec_dprc", raw_batch))
        wrds_raw = "g_sec_dprc" if label == "comp.g_secd" else "sec_dprc"
        wrds_nonempty.update(raw_daily_presence_batch(wrds, "comp", wrds_raw, raw_batch))
    candidates = [p for p in candidates if p in rds_nonempty and p in wrds_nonempty]
    rows: list[dict[str, Any]] = []
    checked = 0
    # Each query is still explicitly restricted to named pairs, but batching
    # avoids hundreds of WRDS network round trips and lets Postgres use BitmapOr
    # index paths for the raw daily spine.
    # Moderate explicit batches amortize helper-view work but stay below the
    # 300-second cap observed to be too small for a 200-header batch.
    batch_size = 55
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        rs_all = pair_stats_batch(rds, obj, batch)
        ws_all = pair_stats_batch(wrds, obj, batch)
        checked += len(batch)
        for pair in batch:
            rs = rs_all.get(pair, (0, None, None))
            ws = ws_all.get(pair, (0, None, None))
            if not rs[0] or not ws[0]:
                continue
            rows.append(
                {
                    "gvkey": pair[0],
                    "iid": pair[1],
                    "rds_count": rs[0],
                    "wrds_count": ws[0],
                    "rds_min": rs[1],
                    "wrds_min": ws[1],
                    "rds_max": rs[2],
                    "wrds_max": ws[2],
                    "count_difference": rs[0] - ws[0],
                    "range_match": rs[1:] == ws[1:],
                }
            )
            if len(rows) >= target:
                break
        print(f"pair count {label} retained={len(rows)} checked={checked}", flush=True)
        if len(rows) >= target:
            break
    if len(rows) < 100:
        raise RuntimeError(f"Only {len(rows)} common nonempty pairs found for {label}")
    return {
        "method": "deterministic MD5 ordering of common header pairs; only pair-indexed history queries",
        "checked_candidates": checked,
        "sample_size": len(rows),
        "mismatch_pairs": sum(
            r["count_difference"] != 0 or not r["range_match"] for r in rows
        ),
        "rows": rows,
    }


def run_full_counts(result_file: Path, force: bool = False) -> None:
    results = load_results(result_file)
    expected = set(OBJECTS) - {"comp.secd", "comp.g_secd"}
    existing: dict[str, Any] = results.get("counts", {})
    if not force and expected.issubset(existing):
        print("full counts reuse complete section", flush=True)
        return
    if force:
        results.pop("counts", None)
        save_results(results, result_file)

    rds, wrds = connect()
    try:
        results["counts"] = full_counts(rds, wrds)
        save_results(results, result_file)
    finally:
        rds.close()
        wrds.close()


def run_daily_counts(result_file: Path, force: bool = False) -> None:
    results = load_results(result_file)
    if force:
        results["daily_sample_counts"] = {}
        save_results(results, result_file)

    rds, wrds = connect()
    try:
        daily: dict[str, Any] = results.get("daily_sample_counts", {})
        for label in ("comp.secd", "comp.g_secd"):
            if daily.get(label, {}).get("sample_size", 0) >= 100:
                print(f"sampled daily counts reuse {label}", flush=True)
                continue
            print(f"sampled daily counts start {label}", flush=True)
            daily[label] = sampled_daily_counts(rds, wrds, label)
            results["daily_sample_counts"] = daily
            save_results(results, result_file)
            print(
                f"sampled daily counts done {label} mismatches={daily[label]['mismatch_pairs']}",
                flush=True,
            )
    finally:
        rds.close()
        wrds.close()


def fund_candidates(
    conn: psycopg.Connection[Any], global_: bool, limit: int = 500
) -> list[str]:
    header = "g_company" if global_ else "company"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT gvkey FROM comp.{qi(header)} ORDER BY md5(gvkey || %s) LIMIT %s",
            (SEED, limit),
        )
        return [r[0] for r in cur.fetchall()]


def exists_for_gvkey(
    conn: psycopg.Connection[Any], obj: Obj, gvkey: str
) -> bool:
    return bool(
        scalar(
            conn,
            f"SELECT EXISTS (SELECT 1 FROM {obj.fq} WHERE gvkey=%s AND datadate<=%s)",
            (gvkey, CUTOFF),
        )
    )


def choose_fund_gvkeys(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], obj: Obj, target: int = 18
) -> list[str]:
    global_ = obj.name.startswith("g_")
    candidates = fund_candidates(wrds, global_)
    rds_header = set(fund_candidates(rds, global_, 1000))
    chosen: list[str] = []
    for gvkey in candidates:
        if gvkey not in rds_header:
            continue
        if exists_for_gvkey(rds, obj, gvkey) and exists_for_gvkey(wrds, obj, gvkey):
            chosen.append(gvkey)
            if len(chosen) >= target:
                return chosen
    raise RuntimeError(f"Only {len(chosen)} common gvkeys found for {obj.label}")


def pair_filter(pairs: Sequence[tuple[str, str]]) -> tuple[str, list[Any]]:
    # Explicit scalar predicates avoid psycopg/Postgres anonymous-composite issues.
    clauses: list[str] = []
    params: list[Any] = []
    for gvkey, iid in pairs:
        clauses.append("(gvkey=%s AND iid=%s)")
        params.extend((gvkey, iid))
    return "(" + " OR ".join(clauses) + ")", params


def explicit_key_filter(
    key_columns: Sequence[str], keys: Sequence[Sequence[Any]]
) -> tuple[str, list[Any]]:
    """Expand composite keys into index-friendly scalar predicates."""
    if not keys:
        raise ValueError("At least one key is required")
    row_clauses: list[str] = []
    params: list[Any] = []
    for key in keys:
        if len(key) != len(key_columns):
            raise ValueError("Key width does not match the object's natural key")
        column_clauses: list[str] = []
        for column, value in zip(key_columns, key, strict=False):
            if value is None:
                column_clauses.append(f"{qi(column)} IS NULL")
            else:
                column_clauses.append(f"{qi(column)}=%s")
                params.append(value)
        row_clauses.append("(" + " AND ".join(column_clauses) + ")")
    return "(" + " OR ".join(row_clauses) + ")", params


def natural_key_candidates(
    conn: psycopg.Connection[Any], obj: Obj, limit: int
) -> list[tuple[Any, ...]]:
    selected = ", ".join(qi(column) for column in obj.key)
    ordering = ", ".join(f"{qi(column)} NULLS FIRST" for column in obj.key)
    where = ""
    params: list[Any] = []
    if obj.cutoff_col:
        where = f" WHERE {qi(obj.cutoff_col)}<=%s"
        params.append(CUTOFF)
    params.append(limit)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {selected} FROM {obj.fq}{where} ORDER BY {ordering} LIMIT %s",
            params,
        )
        return [tuple(row) for row in cur.fetchall()]


def choose_matched_natural_keys(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    obj: Obj,
    target: int = 100,
) -> list[tuple[Any, ...]]:
    wrds_candidates = natural_key_candidates(wrds, obj, max(1000, target * 10))
    rds_candidates = set(natural_key_candidates(rds, obj, max(2000, target * 20)))
    chosen = [key for key in wrds_candidates if key in rds_candidates][:target]
    if len(chosen) < target:
        raise RuntimeError(f"Only {len(chosen)} matched natural keys found for {obj.label}")
    return chosen


def choose_daily_population_pairs(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    label: str,
    target: int,
) -> list[tuple[str, str]]:
    header = "g_security" if label == "comp.g_secd" else "security"
    wrds_candidates = deterministic_header_candidates(wrds, header, 600)
    rds_candidates = set(deterministic_header_candidates(rds, header, 1200))
    candidates = [pair for pair in wrds_candidates if pair in rds_candidates]
    wrds_raw = "g_sec_dprc" if label == "comp.g_secd" else "sec_dprc"
    chosen: list[tuple[str, str]] = []
    for start in range(0, len(candidates), 200):
        batch = candidates[start : start + 200]
        # raw_daily_presence_batch itself expands every pair into scalar
        # predicates; neither daily compatibility view is scanned here.
        rds_present = raw_daily_presence_batch(rds, "public", "sec_dprc", batch)
        wrds_present = raw_daily_presence_batch(wrds, "comp", wrds_raw, batch)
        chosen.extend(
            pair for pair in batch if pair in rds_present and pair in wrds_present
        )
        if len(chosen) >= target:
            return chosen[:target]
    raise RuntimeError(f"Only {len(chosen)} common daily histories found for {label}")


def choose_secm_pairs(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], target: int = 15
) -> list[tuple[str, str]]:
    wrds_candidates = deterministic_header_candidates(wrds, "security", 400)
    rds_candidates = set(deterministic_header_candidates(rds, "security", 800))
    chosen: list[tuple[str, str]] = []
    for pair in wrds_candidates:
        if pair not in rds_candidates:
            continue
        # Do not probe comp.secm one pair at a time: its cshoq subquery groups
        # co_ifndq globally and makes every point probe expensive.  Presence in
        # the indexed monthly spine is sufficient for choosing random keys;
        # the actual all-column comparison remains against comp.secm itself.
        rs = scalar(
            rds,
            "SELECT EXISTS (SELECT 1 FROM public.sec_mth WHERE gvkey=%s AND iid=%s AND datadate<=%s)",
            (pair[0], pair[1], CUTOFF),
        )
        ws = scalar(
            wrds,
            "SELECT EXISTS (SELECT 1 FROM comp.sec_mth WHERE gvkey=%s AND iid=%s AND datadate<=%s)",
            (pair[0], pair[1], CUTOFF),
        )
        if rs and ws:
            chosen.append(pair)
            if len(chosen) >= target:
                return chosen
    raise RuntimeError(f"Only {len(chosen)} common secm pairs found")


def value_equal(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    numeric = (int, float, decimal.Decimal)
    if isinstance(a, numeric) and isinstance(b, numeric) and not isinstance(a, bool) and not isinstance(b, bool):
        if isinstance(a, float) and math.isnan(a):
            return isinstance(b, float) and math.isnan(b)
        try:
            af = float(a)
            bf = float(b)
            return math.isclose(af, bf, rel_tol=REL_TOL, abs_tol=ABS_TOL)
        except (OverflowError, ValueError):
            aa = decimal.Decimal(str(a))
            bb = decimal.Decimal(str(b))
            scale = max(abs(aa), abs(bb), decimal.Decimal(1))
            return abs(aa - bb) <= decimal.Decimal(str(REL_TOL)) * scale
    return a == b


def short(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (dt.date, dt.datetime, dt.time)):
        return value.isoformat()
    if isinstance(value, str) and len(value) > 160:
        return value[:157] + "..."
    return value


def key_token(key: tuple[Any, ...]) -> tuple[tuple[int, Any], ...]:
    return tuple((0, "") if value is None else (1, value) for value in key)


def ordered_query(obj: Obj, cols: Sequence[str], where: str | None) -> str:
    selected = ", ".join(qi(c) for c in cols)
    # Bytewise ordering makes merge order independent of server locale/collation.
    order = ", ".join(
        f"({qi(k)} IS NOT NULL), encode(convert_to(COALESCE({qi(k)}::text,''),'UTF8'),'hex')"
        for k in obj.key
    )
    query = f"SELECT {selected} FROM {obj.fq}"
    if where:
        query += " WHERE " + where
    return query + " ORDER BY " + order


def cursor_groups(
    conn: psycopg.Connection[Any],
    name: str,
    query: str,
    params: Sequence[Any],
    key_indices: Sequence[int],
) -> tuple[psycopg.ServerCursor[Any], Iterator[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]]:
    cur = conn.cursor(name=name)
    cur.itersize = 1000
    cur.execute(query, params)

    def iterator() -> Iterator[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]:
        for key, rows in groupby(cur, key=lambda row: tuple(row[i] for i in key_indices)):
            yield key, [tuple(r) for r in rows]

    return cur, iterator()


def mismatch_positions(a: Sequence[Any], b: Sequence[Any]) -> list[int]:
    return [i for i, (x, y) in enumerate(zip(a, b, strict=False)) if not value_equal(x, y)]


def compare_groups(
    left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]
) -> tuple[list[tuple[tuple[Any, ...], tuple[Any, ...], list[int]]], int, int]:
    # Natural keys should normally be unique.  For any duplicate key, greedily
    # pair the most similar rows so a single difference is not inflated by an
    # arbitrary database row order.
    remaining = list(right)
    pairs: list[tuple[tuple[Any, ...], tuple[Any, ...], list[int]]] = []
    for lrow in left:
        if not remaining:
            break
        scored = [(len(mismatch_positions(lrow, rrow)), i, rrow) for i, rrow in enumerate(remaining)]
        _, index, rrow = min(scored, key=lambda x: (x[0], x[1]))
        remaining.pop(index)
        pairs.append((lrow, rrow, mismatch_positions(lrow, rrow)))
    return pairs, max(0, len(left) - len(right)), max(0, len(right) - len(left))


def compare_object(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    obj: Obj,
    where: str | None = None,
    params: Sequence[Any] = (),
) -> dict[str, Any]:
    cols = columns(rds, obj)
    wrds_cols = columns(wrds, obj)
    if cols != wrds_cols:
        raise RuntimeError(f"Column order differs for {obj.label}; value comparison is unsafe")
    key_indices = [cols.index(k) for k in obj.key]
    query = ordered_query(obj, cols, where)
    rc, rg = cursor_groups(rds, "codex_rds_" + obj.name, query, params, key_indices)
    wc, wg = cursor_groups(wrds, "codex_wrds_" + obj.name, query, params, key_indices)
    mismatches: defaultdict[str, int] = defaultdict(int)
    examples: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    cells = matched_rows = rds_only = wrds_only = duplicate_groups = 0
    ritem = next(rg, None)
    witem = next(wg, None)
    last_progress = time.monotonic()
    try:
        while ritem is not None or witem is not None:
            if time.monotonic() - last_progress >= 30:
                print(
                    f"values progress {obj.label} matched={matched_rows} rds_only={rds_only} wrds_only={wrds_only}",
                    flush=True,
                )
                last_progress = time.monotonic()
            if witem is None or (ritem is not None and key_token(ritem[0]) < key_token(witem[0])):
                rds_only += len(ritem[1])
                if len(examples["__rds_only_row__"]) < 3:
                    examples["__rds_only_row__"].append({"key": [short(v) for v in ritem[0]]})
                ritem = next(rg, None)
                continue
            if ritem is None or key_token(witem[0]) < key_token(ritem[0]):
                wrds_only += len(witem[1])
                if len(examples["__wrds_only_row__"]) < 3:
                    examples["__wrds_only_row__"].append({"key": [short(v) for v in witem[0]]})
                witem = next(wg, None)
                continue
            rkey, rrows = ritem
            _, wrows = witem
            if len(rrows) > 1 or len(wrows) > 1:
                duplicate_groups += 1
            row_pairs, extra_r, extra_w = compare_groups(rrows, wrows)
            rds_only += extra_r
            wrds_only += extra_w
            for rrow, wrow, bad in row_pairs:
                matched_rows += 1
                cells += len(cols)
                for index in bad:
                    col = cols[index]
                    mismatches[col] += 1
                    if len(examples[col]) < 3:
                        examples[col].append(
                            {
                                "key": [short(v) for v in rkey],
                                "rds": short(rrow[index]),
                                "wrds": short(wrow[index]),
                            }
                        )
            ritem = next(rg, None)
            witem = next(wg, None)
    finally:
        rc.close()
        wc.close()
        rds.rollback()
        wrds.rollback()
    return {
        "columns": len(cols),
        "matched_rows": matched_rows,
        "cells_compared": cells,
        "cell_mismatches": sum(mismatches.values()),
        "mismatches_by_column": dict(sorted(mismatches.items())),
        "rds_only_rows": rds_only,
        "wrds_only_rows": wrds_only,
        "duplicate_key_groups": duplicate_groups,
        "examples": dict(examples),
    }


def compare_population_object(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    obj: Obj,
    consumed: Sequence[str],
    where: str,
    params: Sequence[Any],
    sample_kind: str,
    sample_keys: Sequence[Any],
) -> dict[str, Any]:
    rds_available = set(columns(rds, obj))
    wrds_available = set(columns(wrds, obj))
    missing_rds = [column for column in consumed if column not in rds_available]
    missing_wrds = [column for column in consumed if column not in wrds_available]
    missing_key_rds = [column for column in obj.key if column not in rds_available]
    missing_key_wrds = [column for column in obj.key if column not in wrds_available]
    common_consumed = [
        column
        for column in consumed
        if column in rds_available and column in wrds_available
    ]
    result: dict[str, Any] = {
        "consumed_columns": list(consumed),
        "consumed_column_count": len(consumed),
        "missing_on_rds": missing_rds,
        "missing_on_wrds": missing_wrds,
        "missing_key_columns_on_rds": missing_key_rds,
        "missing_key_columns_on_wrds": missing_key_wrds,
        "all_consumed_columns_exist": not missing_rds and not missing_wrds,
        "sample_kind": sample_kind,
        "sample_key_count": len(sample_keys),
        "sample_keys": list(sample_keys),
    }
    if missing_key_rds or missing_key_wrds:
        result.update(
            {
                "matched_rows": 0,
                "rds_only_rows": 0,
                "wrds_only_rows": 0,
                "duplicate_key_groups": 0,
                "non_null_by_column": {},
                "all_non_null_counts_identical": False,
            }
        )
        return result

    selected = list(dict.fromkeys((*obj.key, *common_consumed)))
    key_indices = [selected.index(column) for column in obj.key]
    rds_cursor, rds_groups = cursor_groups(
        rds,
        "codex_population_rds_" + obj.name,
        ordered_query(obj, selected, where),
        params,
        key_indices,
    )
    wrds_cursor, wrds_groups = cursor_groups(
        wrds,
        "codex_population_wrds_" + obj.name,
        ordered_query(obj, selected, where),
        params,
        key_indices,
    )
    non_null = {column: [0, 0] for column in common_consumed}
    matched_rows = rds_only = wrds_only = duplicate_groups = 0
    rds_item = next(rds_groups, None)
    wrds_item = next(wrds_groups, None)
    try:
        while rds_item is not None or wrds_item is not None:
            if wrds_item is None or (
                rds_item is not None and key_token(rds_item[0]) < key_token(wrds_item[0])
            ):
                rds_only += len(rds_item[1])
                rds_item = next(rds_groups, None)
                continue
            if rds_item is None or key_token(wrds_item[0]) < key_token(rds_item[0]):
                wrds_only += len(wrds_item[1])
                wrds_item = next(wrds_groups, None)
                continue
            rds_rows, wrds_rows = rds_item[1], wrds_item[1]
            if len(rds_rows) > 1 or len(wrds_rows) > 1:
                duplicate_groups += 1
            row_pairs, extra_rds, extra_wrds = compare_groups(rds_rows, wrds_rows)
            rds_only += extra_rds
            wrds_only += extra_wrds
            for rds_row, wrds_row, _ in row_pairs:
                matched_rows += 1
                for column in common_consumed:
                    index = selected.index(column)
                    non_null[column][0] += rds_row[index] is not None
                    non_null[column][1] += wrds_row[index] is not None
            rds_item = next(rds_groups, None)
            wrds_item = next(wrds_groups, None)
    finally:
        rds_cursor.close()
        wrds_cursor.close()
        rds.rollback()
        wrds.rollback()

    column_evidence: dict[str, Any] = {}
    mismatch_columns: list[str] = []
    for column in consumed:
        if column not in non_null:
            column_evidence[column] = {
                "rds_non_null": None,
                "wrds_non_null": None,
                "rds_rate": None,
                "wrds_rate": None,
            }
            mismatch_columns.append(column)
            continue
        rds_count, wrds_count = non_null[column]
        rds_rate = None if not matched_rows else rds_count / matched_rows
        wrds_rate = None if not matched_rows else wrds_count / matched_rows
        column_evidence[column] = {
            "rds_non_null": rds_count,
            "wrds_non_null": wrds_count,
            "count_difference": rds_count - wrds_count,
            "rds_rate": rds_rate,
            "wrds_rate": wrds_rate,
            "rate_difference": None if rds_rate is None else rds_rate - wrds_rate,
            "identical": rds_count == wrds_count,
        }
        if rds_count != wrds_count:
            mismatch_columns.append(column)
    result.update(
        {
            "matched_rows": matched_rows,
            "rds_only_rows": rds_only,
            "wrds_only_rows": wrds_only,
            "duplicate_key_groups": duplicate_groups,
            "non_null_by_column": column_evidence,
            "non_null_mismatch_columns": mismatch_columns,
            "all_non_null_counts_identical": (
                result["all_consumed_columns_exist"]
                and matched_rows > 0
                and not mismatch_columns
            ),
        }
    )
    return result


def population_summary(objects: dict[str, Any]) -> dict[str, Any]:
    return {
        "objects_compared": len(objects),
        "objects_with_all_columns": sum(
            bool(item.get("all_consumed_columns_exist")) for item in objects.values()
        ),
        "objects_with_identical_non_null_counts": sum(
            bool(item.get("all_non_null_counts_identical")) for item in objects.values()
        ),
        "matched_rows": sum(int(item.get("matched_rows", 0)) for item in objects.values()),
        "column_existence_failures": [
            label
            for label, item in objects.items()
            if not item.get("all_consumed_columns_exist")
        ],
        "population_mismatch_objects": [
            label
            for label, item in objects.items()
            if not item.get("all_non_null_counts_identical")
        ],
    }


def run_population(result_file: Path, force: bool = False) -> None:
    results = load_results(result_file)
    if force:
        section: dict[str, Any] = {"objects": {}}
        sample_keys: dict[str, Any] = {}
        results["pipeline_population"] = section
        results["sample_keys"] = sample_keys
        save_results(results, result_file)
    else:
        section = results.get("pipeline_population", {"objects": {}})
        sample_keys = results.get("sample_keys", {})
    objects: dict[str, Any] = section.setdefault("objects", {})

    rds, wrds = connect()
    try:
        for label, obj in OBJECTS.items():
            if label in objects:
                sample_keys[label] = objects[label].get(
                    "sample_keys", sample_keys.get(label, [])
                )
                print(f"population reuse {label}", flush=True)
                continue
            if label in {"comp.funda", "comp.g_funda", "comp.fundq", "comp.g_fundq"}:
                keys: Sequence[Any] = choose_fund_gvkeys(rds, wrds, obj, 18)
                placeholders = ",".join(["%s"] * len(keys))
                where = f"gvkey IN ({placeholders}) AND datadate<=%s"
                params: list[Any] = [*keys, CUTOFF]
                sample_kind = "18 deterministic common gvkeys; all matched rows through cutoff"
            elif label in {"comp.secd", "comp.g_secd"}:
                target = 10 if label == "comp.secd" else 8
                keys = choose_daily_population_pairs(rds, wrds, label, target)
                predicate, pair_params = pair_filter(keys)
                where = predicate + " AND datadate<=%s"
                params = [*pair_params, CUTOFF]
                sample_kind = f"{target} deterministic common full daily histories through cutoff"
            elif label == "comp.secm":
                keys = choose_secm_pairs(rds, wrds, 15)
                predicate, pair_params = pair_filter(keys)
                where = predicate + " AND datadate<=%s"
                params = [*pair_params, CUTOFF]
                sample_kind = "15 deterministic common monthly histories through cutoff"
            else:
                keys = choose_matched_natural_keys(rds, wrds, obj, 100)
                where, params = explicit_key_filter(obj.key, keys)
                sample_kind = "100 deterministic matched natural keys"
            print(f"population start {label} sample={len(keys)}", flush=True)
            sample_keys[label] = list(keys)
            objects[label] = compare_population_object(
                rds,
                wrds,
                obj,
                PIPELINE_COLUMNS[label],
                where,
                params,
                sample_kind,
                keys,
            )
            section["summary"] = population_summary(objects)
            section["method"] = (
                "Non-null counts and rates use matched natural-key rows only; "
                "daily queries use expanded scalar pair predicates."
            )
            results["pipeline_population"] = section
            results["sample_keys"] = sample_keys
            save_results(results, result_file)
            print(
                f"population done {label} matched={objects[label]['matched_rows']} "
                f"rate_match={objects[label]['all_non_null_counts_identical']}",
                flush=True,
            )
        section["summary"] = population_summary(objects)
        results["pipeline_population"] = section
        results["sample_keys"] = sample_keys
        save_results(results, result_file)
    finally:
        rds.close()
        wrds.close()


def run_values(result_file: Path, force: bool = False) -> None:
    results = load_results(result_file)
    if force:
        results["value_comparisons"] = {}
        save_results(results, result_file)

    rds, wrds = connect()
    try:
        sample_keys: dict[str, Any] = results.get("sample_keys", {})
        comparisons: dict[str, Any] = results.get("value_comparisons", {})

        for label in ("comp.funda", "comp.g_funda", "comp.fundq", "comp.g_fundq"):
            if label in comparisons:
                print(f"values reuse {label}", flush=True)
                continue
            obj = OBJECTS[label]
            gvkeys = sample_keys.get(label, [])
            if len(gvkeys) < 18:
                gvkeys = choose_fund_gvkeys(rds, wrds, obj)
            sample_keys[label] = gvkeys
            placeholders = ",".join(["%s"] * len(gvkeys))
            where = f"gvkey IN ({placeholders}) AND datadate<=%s"
            print(f"values start {label} gvkeys={len(gvkeys)}", flush=True)
            comparisons[label] = compare_object(rds, wrds, obj, where, [*gvkeys, CUTOFF])
            print(
                f"values done {label} cells={comparisons[label]['cells_compared']} mismatches={comparisons[label]['cell_mismatches']}",
                flush=True,
            )
            results["sample_keys"] = sample_keys
            results["value_comparisons"] = comparisons
            save_results(results, result_file)

        daily_evidence = results.get("daily_sample_counts", {})
        for label in ("comp.secd", "comp.g_secd"):
            if label in comparisons:
                print(f"values reuse {label}", flush=True)
                continue
            required = 10 if label == "comp.secd" else 8
            pairs = sample_keys.get(label, [])
            if len(pairs) < required:
                rows = daily_evidence.get(label, {}).get("rows", [])
                if len(rows) < required:
                    raise RuntimeError(
                        f"Run population or counts first; no >={required}-pair sample available for {label}"
                    )
                pairs = [(r["gvkey"], r["iid"]) for r in rows[:required]]
            else:
                pairs = pairs[:required]
            sample_keys[label] = pairs
            predicate, pair_params = pair_filter(pairs)
            where = predicate + " AND datadate<=%s"
            print(f"values start {label} pairs={len(pairs)}", flush=True)
            comparisons[label] = compare_object(rds, wrds, OBJECTS[label], where, [*pair_params, CUTOFF])
            print(
                f"values done {label} cells={comparisons[label]['cells_compared']} mismatches={comparisons[label]['cell_mismatches']}",
                flush=True,
            )
            results["sample_keys"] = sample_keys
            results["value_comparisons"] = comparisons
            save_results(results, result_file)

        if "comp.secm" not in comparisons:
            secm_pairs = sample_keys.get("comp.secm", [])
            if len(secm_pairs) < 12:
                secm_pairs = choose_secm_pairs(rds, wrds)
            sample_keys["comp.secm"] = secm_pairs
            predicate, pair_params = pair_filter(secm_pairs)
            print(f"values start comp.secm pairs={len(secm_pairs)}", flush=True)
            comparisons["comp.secm"] = compare_object(
                rds,
                wrds,
                OBJECTS["comp.secm"],
                predicate + " AND datadate<=%s",
                [*pair_params, CUTOFF],
            )
            print(
                "values done comp.secm "
                f"cells={comparisons['comp.secm']['cells_compared']} "
                f"mismatches={comparisons['comp.secm']['cell_mismatches']}",
                flush=True,
            )
            results["sample_keys"] = sample_keys
            results["value_comparisons"] = comparisons
            save_results(results, result_file)
        else:
            print("values reuse comp.secm", flush=True)

        for label in FULL_VALUE_OBJECTS:
            if label in comparisons:
                print(f"values reuse {label}", flush=True)
                continue
            obj = OBJECTS[label]
            where = None
            params: list[Any] = []
            if obj.cutoff_col:
                where = f"{qi(obj.cutoff_col)}<=%s"
                params = [CUTOFF]
            print(f"values start {label} full", flush=True)
            comparisons[label] = compare_object(rds, wrds, obj, where, params)
            print(
                f"values done {label} cells={comparisons[label]['cells_compared']} mismatches={comparisons[label]['cell_mismatches']}",
                flush=True,
            )
            results["value_comparisons"] = comparisons
            save_results(results, result_file)
    finally:
        rds.close()
        wrds.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the deployed RDS compatibility objects with WRDS. "
            "Daily objects are always restricted to explicit security samples."
        )
    )
    parser.add_argument(
        "phase",
        choices=(
            "schema",
            "population",
            "counts",
            "counts-full",
            "counts-daily",
            "values",
            "all",
        ),
    )
    parser.add_argument(
        "--result-file",
        type=Path,
        default=DEFAULT_RESULT_FILE,
        help=(
            "JSON evidence path (default: %(default)s). Use a new path for "
            "an isolated post-fix run."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute the selected phase instead of reusing completed JSON sections.",
    )
    args = parser.parse_args()
    result_file = args.result_file.expanduser().resolve()
    print(f"result file {result_file}", flush=True)
    if args.phase == "schema":
        run_schema(result_file, args.force)
    elif args.phase == "population":
        run_population(result_file, args.force)
    elif args.phase == "counts":
        run_full_counts(result_file, args.force)
        run_daily_counts(result_file, args.force)
    elif args.phase == "counts-full":
        run_full_counts(result_file, args.force)
    elif args.phase == "counts-daily":
        run_daily_counts(result_file, args.force)
    elif args.phase == "values":
        run_values(result_file, args.force)
    elif args.phase == "all":
        run_schema(result_file, args.force)
        run_population(result_file, args.force)
        run_full_counts(result_file, args.force)
        run_daily_counts(result_file, args.force)
        run_values(result_file, args.force)


if __name__ == "__main__":
    main()
