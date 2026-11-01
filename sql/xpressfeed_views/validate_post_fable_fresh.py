"""Independent post-Fable differential validation of WRDS compatibility views.

This harness is deliberately separate from ``validate_codex_sol.py`` and the
Fable v01-v12 scripts.  It executes the original checks in their requested
order, checkpoints evidence after every object, and never performs an
unfiltered query against the RDS daily compatibility views.

Run all phases (schema -> pipeline -> counts -> values -> root evidence):

    uv run --with "psycopg[binary]" python \
        sql/xpressfeed_views/validate_post_fable_fresh.py all

Individual phases may be resumed by naming the phase.  Add ``--force`` to
replace that phase's prior evidence.  Credentials are read from ``.env`` and
are never printed or written to evidence.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import hashlib
import itertools
import json
import re
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
ENV_FILE = REPO / ".env"
EVIDENCE_FILE = HERE / "VALIDATION_EVIDENCE_POST_FABLE_FRESH.json"
CLASSIFICATIONS_FILE = HERE / "VALIDATION_CLASSIFICATIONS_POST_FABLE.json"
CUTOFF = dt.date(2026, 6, 30)
SEED = "post-fable-fresh-v1-20260719"
REL_TOL = Decimal("1e-9")
ABS_TOL = Decimal("1e-12")


@dataclass(frozen=True)
class ObjectSpec:
    schema: str
    name: str
    key: tuple[str, ...]
    time_column: str | None = None

    @property
    def label(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def sql_name(self) -> str:
        return f'"{self.schema}"."{self.name}"'


SPECS = {
    spec.label: spec
    for spec in (
        ObjectSpec(
            "comp",
            "funda",
            ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "curcd"),
            "datadate",
        ),
        ObjectSpec(
            "comp",
            "g_funda",
            ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "curcd"),
            "datadate",
        ),
        ObjectSpec(
            "comp",
            "fundq",
            ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "fyr", "curcdq"),
            "datadate",
        ),
        ObjectSpec(
            "comp",
            "g_fundq",
            ("gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc", "fyr", "curcdq"),
            "datadate",
        ),
        ObjectSpec("comp", "secd", ("gvkey", "iid", "datadate"), "datadate"),
        ObjectSpec("comp", "g_secd", ("gvkey", "iid", "datadate"), "datadate"),
        ObjectSpec("comp", "secm", ("gvkey", "iid", "datadate"), "datadate"),
        ObjectSpec("comp", "security", ("gvkey", "iid")),
        ObjectSpec("comp", "g_security", ("gvkey", "iid")),
        ObjectSpec("comp", "company", ("gvkey",)),
        ObjectSpec("comp", "g_company", ("gvkey",)),
        ObjectSpec("comp", "sec_history", ("gvkey", "iid", "item", "effdate")),
        ObjectSpec("comp", "g_sec_history", ("gvkey", "iid", "item", "effdate")),
        ObjectSpec("comp", "co_hgic", ("gvkey", "indtype", "indfrom")),
        ObjectSpec("comp", "g_co_hgic", ("gvkey", "indtype", "indfrom")),
        ObjectSpec("comp", "exrt_dly", ("fromcurd", "tocurd", "datadate"), "datadate"),
        ObjectSpec("comp", "r_ex_codes", ("exchgcd",)),
        ObjectSpec("ff", "factors_monthly", ("date",), "date"),
    )
}

ACCOUNTING = ("comp.funda", "comp.g_funda", "comp.fundq", "comp.g_fundq")
DAILY = ("comp.secd", "comp.g_secd")
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

# Re-audited directly against src/jkp/data on 2026-07-19.  ``n`` and
# ``source`` in the accounting transforms are locally created fields and are
# intentionally absent.  The source-reference scan stored in the evidence
# records every source line that opens each parquet.
PIPELINE_COLUMNS: dict[str, tuple[str, ...]] = {
    "comp.funda": (
        "aco",
        "act",
        "ajex",
        "ao",
        "ap",
        "at",
        "capx",
        "ceq",
        "che",
        "cogs",
        "consol",
        "csho",
        "curcd",
        "datadate",
        "datafmt",
        "dlc",
        "dlcch",
        "dltis",
        "dltr",
        "dltt",
        "do",
        "dp",
        "dpc",
        "dv",
        "dvc",
        "dvt",
        "ebit",
        "ebitda",
        "emp",
        "fiao",
        "fincf",
        "gdwl",
        "gp",
        "gvkey",
        "ib",
        "ibc",
        "icapt",
        "indfmt",
        "intan",
        "invt",
        "itcb",
        "ivao",
        "ivst",
        "lco",
        "lct",
        "lo",
        "lt",
        "mib",
        "mii",
        "naicsh",
        "ni",
        "nopi",
        "oancf",
        "oiadp",
        "oibdp",
        "pi",
        "popsrc",
        "ppegt",
        "ppent",
        "prstkc",
        "pstk",
        "pstkl",
        "pstkrv",
        "re",
        "rect",
        "revt",
        "sale",
        "seq",
        "sich",
        "spi",
        "sstk",
        "txbcof",
        "txdb",
        "txditc",
        "txp",
        "txt",
        "xad",
        "xi",
        "xido",
        "xidoc",
        "xint",
        "xlr",
        "xopr",
        "xrd",
        "xsga",
    ),
    "comp.g_funda": (
        "aco",
        "act",
        "ao",
        "ap",
        "at",
        "capx",
        "ceq",
        "che",
        "cogs",
        "consol",
        "curcd",
        "datadate",
        "datafmt",
        "dlc",
        "dlcch",
        "dltis",
        "dltr",
        "dltt",
        "do",
        "dp",
        "dpc",
        "dv",
        "dvc",
        "dvt",
        "ebit",
        "ebitda",
        "emp",
        "fiao",
        "fincf",
        "gdwl",
        "gvkey",
        "ib",
        "ibc",
        "icapt",
        "indfmt",
        "intan",
        "invt",
        "ivao",
        "ivst",
        "lco",
        "lct",
        "lo",
        "lt",
        "ltdch",
        "mib",
        "mii",
        "naicsh",
        "nopi",
        "oancf",
        "oiadp",
        "oibdp",
        "pi",
        "popsrc",
        "ppegt",
        "ppent",
        "prstkc",
        "pstk",
        "purtshr",
        "re",
        "rect",
        "revt",
        "sale",
        "seq",
        "sich",
        "spi",
        "sstk",
        "txdb",
        "txditc",
        "txp",
        "txt",
        "wcapt",
        "xi",
        "xido",
        "xidoc",
        "xint",
        "xlr",
        "xopr",
        "xrd",
        "xsga",
    ),
    "comp.fundq": (
        "acoq",
        "actq",
        "ajexq",
        "aoq",
        "apq",
        "atq",
        "capxy",
        "ceqq",
        "cheq",
        "cogsq",
        "cogsy",
        "consol",
        "cshoq",
        "curcdq",
        "datadate",
        "datafmt",
        "dlcchy",
        "dlcq",
        "dltisy",
        "dltry",
        "dlttq",
        "doq",
        "doy",
        "dpcy",
        "dpq",
        "dpy",
        "dvy",
        "fiaoy",
        "fincfy",
        "fqtr",
        "fyearq",
        "fyr",
        "gdwlq",
        "gvkey",
        "ibcy",
        "ibq",
        "iby",
        "icaptq",
        "indfmt",
        "intanq",
        "invtq",
        "ivaoq",
        "ivstq",
        "lcoq",
        "lctq",
        "loq",
        "ltq",
        "mibq",
        "miiq",
        "miiy",
        "niq",
        "niy",
        "nopiq",
        "nopiy",
        "oancfy",
        "oiadpq",
        "oiadpy",
        "oibdpq",
        "oibdpy",
        "piq",
        "piy",
        "popsrc",
        "ppegtq",
        "ppentq",
        "prstkcy",
        "pstkq",
        "rectq",
        "req",
        "revtq",
        "revty",
        "saleq",
        "saley",
        "seqq",
        "spiq",
        "spiy",
        "sstky",
        "txbcofy",
        "txdbq",
        "txditcq",
        "txpq",
        "txtq",
        "txty",
        "xidocy",
        "xidoq",
        "xidoy",
        "xintq",
        "xinty",
        "xiq",
        "xiy",
        "xoprq",
        "xopry",
        "xrdq",
        "xrdy",
        "xsgaq",
        "xsgay",
    ),
    "comp.g_fundq": (
        "acoq",
        "actq",
        "aoq",
        "apq",
        "atq",
        "capxy",
        "ceqq",
        "cheq",
        "cogsq",
        "cogsy",
        "consol",
        "curcdq",
        "datadate",
        "datafmt",
        "dlcchy",
        "dlcq",
        "dltisy",
        "dltry",
        "dlttq",
        "dpactq",
        "dpcy",
        "dpq",
        "dpy",
        "dvtq",
        "dvty",
        "dvy",
        "fiaoy",
        "fincfy",
        "fqtr",
        "fyearq",
        "fyr",
        "gdwlq",
        "gpq",
        "gpy",
        "gvkey",
        "ibcy",
        "ibq",
        "iby",
        "indfmt",
        "intanq",
        "invtq",
        "ivaoq",
        "ivstq",
        "lcoq",
        "lctq",
        "loq",
        "ltdchy",
        "ltq",
        "mibq",
        "miiq",
        "miiy",
        "nopiq",
        "nopiy",
        "oancfy",
        "oiadpq",
        "oiadpy",
        "oibdpq",
        "oibdpy",
        "piq",
        "piy",
        "popsrc",
        "ppentq",
        "prstkcy",
        "pstkq",
        "purtshry",
        "rectq",
        "req",
        "revtq",
        "revty",
        "saleq",
        "saley",
        "seqq",
        "spiq",
        "spiy",
        "sstky",
        "txdbq",
        "txtq",
        "txty",
        "wcapty",
        "xidocy",
        "xintq",
        "xinty",
        "xiq",
        "xiy",
        "xoprq",
        "xopry",
        "xsgaq",
        "xsgay",
    ),
    "comp.secd": (
        "ajexdi",
        "conm",
        "cshoc",
        "cshtrd",
        "curcdd",
        "curcddv",
        "cusip",
        "datadate",
        "div",
        "divd",
        "divsp",
        "exchg",
        "gvkey",
        "iid",
        "prccd",
        "prchd",
        "prcld",
        "prcod",
        "prcstd",
        "tpci",
        "trfd",
    ),
    "comp.g_secd": (
        "ajexdi",
        "conm",
        "cshoc",
        "cshtrd",
        "curcdd",
        "curcddv",
        "datadate",
        "div",
        "divd",
        "divsp",
        "exchg",
        "gvkey",
        "iid",
        "isin",
        "monthend",
        "prccd",
        "prchd",
        "prcld",
        "prcod",
        "prcstd",
        "qunit",
        "sedol",
        "tpci",
        "trfd",
    ),
    "comp.secm": (
        "ajexm",
        "csfsm",
        "cshom",
        "cshoq",
        "cshtrm",
        "curcddvm",
        "curcdm",
        "datadate",
        "dvpsxm",
        "exchg",
        "gvkey",
        "iid",
        "prccm",
        "prchm",
        "prclm",
        "tpci",
        "trfm",
    ),
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

if set(PIPELINE_COLUMNS) != set(SPECS):
    raise RuntimeError("pipeline contract must enumerate every object")


def ascii_print(message: str) -> None:
    print(message.encode("ascii", "replace").decode("ascii"), flush=True)


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if isinstance(value, (dt.date, dt.datetime, dt.time, Decimal)):
        return str(value)
    return value


def current_script_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def load_evidence() -> dict[str, Any]:
    if not EVIDENCE_FILE.exists():
        return {
            "metadata": {
                "harness": Path(__file__).name,
                "seed": SEED,
                "cutoff": CUTOFF.isoformat(),
                "relative_tolerance": str(REL_TOL),
                "absolute_tolerance": str(ABS_TOL),
                "rds_statement_timeout_seconds": 300,
                "guardrail": "No unfiltered RDS comp.secd/comp.g_secd query",
                "created_at": dt.datetime.now(dt.UTC).isoformat(),
                "script_sha256": current_script_sha256(),
                "script_sha256_at_validation_start": current_script_sha256(),
            }
        }
    return json.loads(EVIDENCE_FILE.read_text(encoding="utf-8"))


def save_evidence(evidence: dict[str, Any]) -> None:
    evidence["metadata"]["updated_at"] = dt.datetime.now(dt.UTC).isoformat()
    EVIDENCE_FILE.write_text(
        json.dumps(jsonable(evidence), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def read_env() -> dict[str, str]:
    result: dict[str, str] = {}
    for raw in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        result[key.strip()] = value
    return result


def connect() -> tuple[psycopg.Connection[Any], psycopg.Connection[Any]]:
    env = read_env()
    rds_dsn = env["COMPUSTAT"].replace("+psycopg2", "")
    rds = psycopg.connect(
        rds_dsn,
        connect_timeout=30,
        application_name="jkp_post_fable_fresh_validation",
    )
    wrds = psycopg.connect(
        host="wrds-pgdata.wharton.upenn.edu",
        port=9737,
        dbname="wrds",
        user=env["ENV_USERNAME"],
        password=env["ENV_PASSWORD"],
        sslmode="require",
        connect_timeout=60,
        application_name="jkp_post_fable_fresh_validation",
    )
    for conn in (rds, wrds):
        with conn.cursor() as cursor:
            cursor.execute("SET statement_timeout = '300s'")
        conn.commit()
    return rds, wrds


def fetch_all(
    conn: psycopg.Connection[Any], query: str, params: Sequence[Any] = ()
) -> tuple[list[str], list[tuple[Any, ...]], float]:
    started = time.monotonic()
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [item.name for item in cursor.description]
    conn.rollback()
    return columns, rows, time.monotonic() - started


def scalar(conn: psycopg.Connection[Any], query: str, params: Sequence[Any] = ()) -> Any:
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        value = cursor.fetchone()[0]
    conn.rollback()
    return value


def placeholders(count: int) -> str:
    return ",".join("%s" for _ in range(count))


def pair_predicate(pairs: Sequence[tuple[str, str]]) -> tuple[str, list[str]]:
    if not pairs:
        raise ValueError("at least one pair is required")
    return " OR ".join("(gvkey=%s AND iid=%s)" for _ in pairs), [
        value for pair in pairs for value in pair
    ]


def explicit_key_predicate(
    key_columns: Sequence[str], keys: Sequence[Sequence[Any]]
) -> tuple[str, list[Any]]:
    if not keys:
        raise ValueError("at least one key is required")
    clauses: list[str] = []
    params: list[Any] = []
    for key in keys:
        terms: list[str] = []
        for column, value in zip(key_columns, key, strict=True):
            if value is None:
                terms.append(f'"{column}" IS NULL')
            else:
                terms.append(f'"{column}"=%s')
                params.append(value)
        clauses.append("(" + " AND ".join(terms) + ")")
    return " OR ".join(clauses), params


def stable_rank(*parts: Any) -> str:
    text = "|".join(str(part) for part in (SEED, *parts))
    return hashlib.md5(text.encode(), usedforsecurity=False).hexdigest()


def decimal_value(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation:
        return None
    return result


def cells_equal(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    left_num = decimal_value(left)
    right_num = decimal_value(right)
    if left_num is not None and right_num is not None:
        if left_num.is_nan() or right_num.is_nan():
            return left_num.is_nan() and right_num.is_nan()
        if left_num.is_infinite() or right_num.is_infinite():
            return left_num == right_num
        difference = abs(left_num - right_num)
        scale = max(abs(left_num), abs(right_num))
        return difference <= max(ABS_TOL, REL_TOL * scale)
    if isinstance(left, dt.datetime):
        left = left.date()
    if isinstance(right, dt.datetime):
        right = right.date()
    return left == right


TEXT_DATA_TYPES = {"character", "character varying", "text"}


def sortable(value: Any) -> tuple[bool, Any]:
    """Match the explicit PostgreSQL key ordering used by ``select_query``.

    Every compared object has already passed an exact schema comparison, so
    corresponding non-null key values have the same Python-comparable type.
    NULL sorts last in both PostgreSQL and this tuple key.  Text keys use the
    PostgreSQL ``C`` collation (see ``key_ordering``), whose byte/code-point
    ordering agrees with Python for these strings.
    """
    return value is None, value


def key_ordering(key_columns: Sequence[str], data_types: dict[str, str]) -> str:
    expressions: list[str] = []
    for column in key_columns:
        expression = f'"{column}"'
        if data_types[column] in TEXT_DATA_TYPES:
            expression += ' COLLATE "C"'
        expressions.append(f"{expression} ASC NULLS LAST")
    return ", ".join(expressions)


def assert_merge_ordering_contract() -> None:
    """Small deterministic guard against reintroducing the stream-merge bug."""
    numeric = [None, 10, 2, 1]
    if sorted(numeric, key=sortable) != [1, 2, 10, None]:
        raise RuntimeError("numeric/NULL merge ordering self-test failed")
    text = [None, "b", "a", "A"]
    if sorted(text, key=sortable) != ["A", "a", "b", None]:
        raise RuntimeError("C-collated text/NULL merge ordering self-test failed")


def row_distance(left: Sequence[Any], right: Sequence[Any]) -> int:
    return sum(not cells_equal(a, b) for a, b in zip(left, right, strict=True))


def pair_duplicate_groups(
    left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]
) -> tuple[list[tuple[tuple[Any, ...], tuple[Any, ...]]], int, int]:
    """Match duplicate-key groups without hiding value differences."""
    pairs: list[tuple[tuple[Any, ...], tuple[Any, ...]]] = []
    remaining_right = list(right)
    remaining_left: list[tuple[Any, ...]] = []
    for lrow in left:
        exact = next(
            (
                index
                for index, rrow in enumerate(remaining_right)
                if all(cells_equal(a, b) for a, b in zip(lrow, rrow, strict=True))
            ),
            None,
        )
        if exact is None:
            remaining_left.append(lrow)
        else:
            pairs.append((lrow, remaining_right.pop(exact)))
    while remaining_left and remaining_right:
        lrow = remaining_left.pop(0)
        index = min(
            range(len(remaining_right)),
            key=lambda i: (row_distance(lrow, remaining_right[i]), str(remaining_right[i])),
        )
        pairs.append((lrow, remaining_right.pop(index)))
    return pairs, len(remaining_left), len(remaining_right)


def grouped_rows(
    rows: Iterable[tuple[Any, ...]], key_indexes: Sequence[int]
) -> Iterator[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]:
    def key(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(row[index] for index in key_indexes)

    for group_key, group in itertools.groupby(rows, key=key):
        yield group_key, list(group)


def cursor_groups(
    conn: psycopg.Connection[Any],
    cursor_name: str,
    query: str,
    params: Sequence[Any],
    key_indexes: Sequence[int],
) -> tuple[psycopg.Cursor[Any], Iterator[tuple[tuple[Any, ...], list[tuple[Any, ...]]]]]:
    cursor = conn.cursor(name=cursor_name)
    cursor.itersize = 5000
    cursor.execute(query, params)
    return cursor, grouped_rows(cursor, key_indexes)


def compare_queries(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    columns: Sequence[str],
    key_columns: Sequence[str],
    query: str,
    params: Sequence[Any],
    *,
    example_limit: int = 4,
) -> dict[str, Any]:
    key_indexes = [columns.index(column) for column in key_columns]
    rcur, rgroups = cursor_groups(rds, "fresh_rds", query, params, key_indexes)
    wcur, wgroups = cursor_groups(wrds, "fresh_wrds", query, params, key_indexes)
    ritem = next(rgroups, None)
    witem = next(wgroups, None)
    matched_rows = rds_only = wrds_only = duplicate_groups = 0
    mismatch_counts: Counter[str] = Counter()
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)
    started = time.monotonic()
    try:
        while ritem is not None or witem is not None:
            if witem is None or (
                ritem is not None
                and tuple(map(sortable, ritem[0])) < tuple(map(sortable, witem[0]))
            ):
                rds_only += len(ritem[1])
                if len(examples["__rds_only__"]) < example_limit:
                    examples["__rds_only__"].append({"key": list(ritem[0]), "rows": len(ritem[1])})
                ritem = next(rgroups, None)
                continue
            if ritem is None or tuple(map(sortable, witem[0])) < tuple(map(sortable, ritem[0])):
                wrds_only += len(witem[1])
                if len(examples["__wrds_only__"]) < example_limit:
                    examples["__wrds_only__"].append({"key": list(witem[0]), "rows": len(witem[1])})
                witem = next(wgroups, None)
                continue
            if len(ritem[1]) > 1 or len(witem[1]) > 1:
                duplicate_groups += 1
            row_pairs, extra_rds, extra_wrds = pair_duplicate_groups(ritem[1], witem[1])
            rds_only += extra_rds
            wrds_only += extra_wrds
            for rrow, wrow in row_pairs:
                matched_rows += 1
                for index, column in enumerate(columns):
                    if not cells_equal(rrow[index], wrow[index]):
                        mismatch_counts[column] += 1
                        if len(examples[column]) < example_limit:
                            examples[column].append(
                                {
                                    "key": list(ritem[0]),
                                    "rds": rrow[index],
                                    "wrds": wrow[index],
                                }
                            )
            ritem = next(rgroups, None)
            witem = next(wgroups, None)
    finally:
        rcur.close()
        wcur.close()
        rds.rollback()
        wrds.rollback()
    return {
        "matched_rows": matched_rows,
        "cells_compared_including_keys": matched_rows * len(columns),
        "rds_only_rows": rds_only,
        "wrds_only_rows": wrds_only,
        "duplicate_key_groups": duplicate_groups,
        "mismatches_by_column": dict(sorted(mismatch_counts.items())),
        "cell_mismatches": sum(mismatch_counts.values()),
        "examples": dict(examples),
        "seconds": round(time.monotonic() - started, 3),
    }


def object_columns(conn: psycopg.Connection[Any], spec: ObjectSpec) -> list[str]:
    _, rows, _ = fetch_all(
        conn,
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
        (spec.schema, spec.name),
    )
    return [row[0] for row in rows]


def object_data_types(conn: psycopg.Connection[Any], spec: ObjectSpec) -> dict[str, str]:
    _, rows, _ = fetch_all(
        conn,
        """SELECT column_name,data_type FROM information_schema.columns
           WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
        (spec.schema, spec.name),
    )
    return {str(column): str(data_type) for column, data_type in rows}


def select_query(
    spec: ObjectSpec,
    columns: Sequence[str],
    data_types: dict[str, str],
    where: str | None = None,
) -> str:
    selected = ", ".join(f'"{column}"' for column in columns)
    ordering = key_ordering(spec.key, data_types)
    suffix = f" WHERE {where}" if where else ""
    return f"SELECT {selected} FROM {spec.sql_name}{suffix} ORDER BY {ordering}"


def source_references() -> dict[str, list[dict[str, Any]]]:
    roots = [REPO / "src" / "jkp" / "data"]
    references: dict[str, list[dict[str, Any]]] = {}
    for label, spec in SPECS.items():
        filename = (
            "ff_factors_monthly.parquet"
            if label == "ff.factors_monthly"
            else f"comp_{spec.name}.parquet"
        )
        hits: list[dict[str, Any]] = []
        for root in roots:
            for path in root.rglob("*.py"):
                for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                    if filename in line:
                        hits.append(
                            {
                                "file": path.relative_to(REPO).as_posix(),
                                "line": number,
                                "text": line.strip(),
                            }
                        )
        references[label] = hits
    return references


def _literal_string_list(function: ast.FunctionDef, variable: str) -> tuple[list[str], int]:
    for node in ast.walk(function):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == variable for target in node.targets
        ):
            continue
        value = ast.literal_eval(node.value)
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise RuntimeError(f"{variable} is no longer a literal string list")
        return value, node.lineno
    raise RuntimeError(f"could not derive {variable} from standardized_accounting_data")


def derive_accounting_columns_from_source() -> tuple[dict[str, tuple[str, ...]], dict[str, Any]]:
    """Reconstruct accounting reads from current source plus current schemas.

    This is intentionally data-flow based: it does not read either earlier
    validator or any validation report.  The quarterly list is generated by
    the same suffix rule visible in ``standardized_accounting_data`` over the
    checked-in WRDS schema contract.
    """
    source_path = REPO / "src" / "jkp" / "data" / "aux_functions.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "standardized_accounting_data"
    )
    extracted: dict[str, list[str]] = {}
    line_numbers: dict[str, int] = {}
    for name in ("avars_inc", "avars_cf", "avars_bs", "avars_other"):
        extracted[name], line_numbers[name] = _literal_string_list(function, name)
    annual = extracted["avars_inc"] + extracted["avars_cf"] + extracted["avars_bs"]
    annual_other = extracted["avars_other"]

    contract = json.loads((HERE / "wrds_schema_contract.json").read_text(encoding="utf-8"))
    contract_objects = contract["objects"]
    contract_names = {
        name: [column for column, _type in contract_objects[name]] for name in ("fundq", "g_fundq")
    }
    combined = set(contract_names["fundq"]) | set(contract_names["g_fundq"])
    qvars = sorted(
        column
        for column in combined
        if len(column) > 1 and column[-1] in {"q", "y"} and column[:-1].lower() in annual
    )

    common_annual = {"gvkey", "datadate", "indfmt", "datafmt", "popsrc", "consol", "curcd"}
    common_quarter = {
        "gvkey",
        "datadate",
        "indfmt",
        "datafmt",
        "popsrc",
        "consol",
        "fyr",
        "fyearq",
        "fqtr",
        "curcdq",
    }
    na_annual = common_annual | (set(annual + annual_other) - {"wcapt", "ltdch", "purtshr"})
    na_annual |= {"csho", "ajex", "sich", "naicsh"}
    gl_annual = common_annual | (
        set(annual + annual_other) - {"gp", "pstkrv", "pstkl", "itcb", "xad", "txbcof", "ni"}
    )
    gl_annual |= {"ib", "xi", "do", "sich", "naicsh"}
    na_quarter = common_quarter | (
        set(qvars) - {"dvtq", "gpq", "dvty", "gpy", "ltdchy", "purtshry", "wcapty"}
    )
    na_quarter |= {"cshoq", "ajexq"}
    gl_quarter = common_quarter | (
        set(qvars)
        - {
            "icaptq",
            "niy",
            "txditcq",
            "txpq",
            "xidoq",
            "xidoy",
            "xrdq",
            "xrdy",
            "txbcofy",
            "niq",
            "ppegtq",
            "doq",
            "doy",
        }
    )
    gl_quarter |= {"ibq", "xiq", "ppentq", "dpactq"}
    result = {
        "comp.funda": tuple(sorted(na_annual)),
        "comp.g_funda": tuple(sorted(gl_annual)),
        "comp.fundq": tuple(sorted(na_quarter)),
        "comp.g_fundq": tuple(sorted(gl_quarter)),
    }
    provenance = {
        "method": (
            "AST extraction of avars_inc/cf/bs/other, source-visible exclusion/computed-input "
            "rules, auxiliary firm-share/SIC-NAICS reads, and q/y suffix expansion against "
            "the checked-in WRDS schema contract"
        ),
        "file": source_path.relative_to(REPO).as_posix(),
        "function_line": function.lineno,
        "list_assignment_lines": line_numbers,
        "quarterly_schema_columns_examined": {
            name: len(columns) for name, columns in contract_names.items()
        },
    }
    return result, provenance


DIRECT_PIPELINE_PROVENANCE: dict[str, list[tuple[str, int, int]]] = {
    "comp.secd": [
        ("src/jkp/data/aux_functions.py", 1423, 1472),
        ("src/jkp/data/production.py", 199, 209),
    ],
    "comp.g_secd": [
        ("src/jkp/data/aux_functions.py", 1423, 1450),
        ("src/jkp/data/aux_functions.py", 6734, 6738),
        ("src/jkp/data/production.py", 211, 222),
    ],
    "comp.secm": [
        ("src/jkp/data/aux_functions.py", 1614, 1660),
        ("src/jkp/data/aux_functions.py", 6730, 6733),
    ],
    "comp.security": [
        ("src/jkp/data/aux_functions.py", 240, 264),
        ("src/jkp/data/aux_functions.py", 533, 564),
    ],
    "comp.g_security": [
        ("src/jkp/data/aux_functions.py", 240, 264),
        ("src/jkp/data/aux_functions.py", 533, 564),
    ],
    "comp.company": [
        ("src/jkp/data/aux_functions.py", 273, 294),
        ("src/jkp/data/aux_functions.py", 567, 570),
    ],
    "comp.g_company": [
        ("src/jkp/data/aux_functions.py", 273, 294),
        ("src/jkp/data/aux_functions.py", 567, 570),
    ],
    "comp.sec_history": [("src/jkp/data/aux_functions.py", 296, 414)],
    "comp.g_sec_history": [("src/jkp/data/aux_functions.py", 296, 414)],
    "comp.co_hgic": [
        ("src/jkp/data/aux_functions.py", 218, 238),
        ("src/jkp/data/aux_functions.py", 500, 503),
    ],
    "comp.g_co_hgic": [
        ("src/jkp/data/aux_functions.py", 218, 238),
        ("src/jkp/data/aux_functions.py", 500, 503),
    ],
    "comp.exrt_dly": [("src/jkp/data/aux_functions.py", 419, 451)],
    "comp.r_ex_codes": [("src/jkp/data/aux_functions.py", 553, 559)],
    "ff.factors_monthly": [
        ("src/jkp/data/aux_functions.py", 548, 552),
        ("src/jkp/data/aux_functions.py", 1966, 1970),
    ],
}


def pipeline_derivation_evidence() -> dict[str, Any]:
    derived_accounting, accounting_provenance = derive_accounting_columns_from_source()
    mismatches = {
        label: {
            "derived_only": sorted(set(derived) - set(PIPELINE_COLUMNS[label])),
            "encoded_only": sorted(set(PIPELINE_COLUMNS[label]) - set(derived)),
        }
        for label, derived in derived_accounting.items()
        if set(derived) != set(PIPELINE_COLUMNS[label])
    }
    if mismatches:
        raise RuntimeError(f"source-derived accounting contract disagrees: {mismatches}")

    direct: dict[str, Any] = {}
    for label, ranges in DIRECT_PIPELINE_PROVENANCE.items():
        excerpts: list[dict[str, Any]] = []
        combined = ""
        for relative, start, end in ranges:
            lines = (REPO / relative).read_text(encoding="utf-8").splitlines()
            selected = lines[start - 1 : end]
            combined += "\n" + "\n".join(selected)
            excerpts.append({"file": relative, "start_line": start, "end_line": end})
        # Column names can appear as SQL/Polars identifiers or be supplied via
        # a helper in the same cited range.  Record tokens not literally visible
        # for manual review rather than silently treating the manifest as proof.
        absent_tokens = [
            column
            for column in PIPELINE_COLUMNS[label]
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(column)}(?![A-Za-z0-9_])", combined) is None
        ]
        direct[label] = {
            "method": "manual data-flow audit of cited DuckDB SQL / Polars selections",
            "source_ranges": excerpts,
            "columns": list(PIPELINE_COLUMNS[label]),
            "tokens_not_literal_in_cited_ranges": absent_tokens,
        }
    return {
        "accounting": {
            **accounting_provenance,
            "derived_columns": {key: list(value) for key, value in derived_accounting.items()},
            "matches_encoded_contract": not mismatches,
        },
        "direct_objects": direct,
        "parquet_open_references": source_references(),
    }


SCHEMA_FIELDS = (
    "ordinal_position",
    "column_name",
    "data_type",
    "character_maximum_length",
    "character_octet_length",
    "numeric_precision",
    "numeric_precision_radix",
    "numeric_scale",
    "datetime_precision",
    "interval_type",
    "interval_precision",
    "udt_schema",
    "udt_name",
    "domain_schema",
    "domain_name",
    "is_nullable",
    "collation_schema",
    "collation_name",
)


def schema_rows(conn: psycopg.Connection[Any], spec: ObjectSpec) -> list[tuple[Any, ...]]:
    fields = ", ".join(SCHEMA_FIELDS)
    _, rows, _ = fetch_all(
        conn,
        f"""SELECT {fields} FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""",
        (spec.schema, spec.name),
    )
    return rows


def run_state(conn: psycopg.Connection[Any]) -> dict[str, Any]:
    return current_state(conn)


def run_schema(evidence: dict[str, Any], *, force: bool) -> None:
    if force:
        for name in (
            "schema",
            "pipeline",
            "samples",
            "counts",
            "values",
            "root_evidence",
            "acceptance",
        ):
            evidence.pop(name, None)
        evidence["metadata"].pop("validation_start_state", None)
        save_evidence(evidence)
    section = evidence.setdefault("schema", {"objects": {}})
    rds, wrds = connect()
    try:
        if "validation_start_state" not in evidence["metadata"]:
            evidence["metadata"]["validation_start_state"] = run_state(rds)
            save_evidence(evidence)
        for label, spec in SPECS.items():
            if label in section["objects"]:
                continue
            rrows = schema_rows(rds, spec)
            wrows = schema_rows(wrds, spec)
            differences: list[dict[str, Any]] = []
            for ordinal in range(max(len(rrows), len(wrows))):
                rrow = rrows[ordinal] if ordinal < len(rrows) else None
                wrow = wrows[ordinal] if ordinal < len(wrows) else None
                if rrow == wrow:
                    continue
                fields = [
                    field
                    for index, field in enumerate(SCHEMA_FIELDS)
                    if rrow is None or wrow is None or rrow[index] != wrow[index]
                ]
                differences.append(
                    {"ordinal": ordinal + 1, "fields": fields, "rds": rrow, "wrds": wrow}
                )
            section["objects"][label] = {
                "rds_columns": len(rrows),
                "wrds_columns": len(wrows),
                "identical": not differences,
                "differences": differences,
            }
            save_evidence(evidence)
            ascii_print(f"schema {label}: {len(wrows)} columns, differences={len(differences)}")
        section["summary"] = {
            "objects": len(section["objects"]),
            "identical_objects": sum(item["identical"] for item in section["objects"].values()),
            "wrds_columns": sum(item["wrds_columns"] for item in section["objects"].values()),
            "difference_entries": sum(
                len(item["differences"]) for item in section["objects"].values()
            ),
        }
        save_evidence(evidence)
    finally:
        rds.close()
        wrds.close()


def common_existing_gvkeys(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    label: str,
    candidates: Sequence[str],
) -> list[str]:
    if not candidates:
        return []
    spec = SPECS[label]
    candidate_list = list(dict.fromkeys(candidates))
    query = (
        f"SELECT DISTINCT gvkey FROM {spec.sql_name} "
        f"WHERE gvkey IN ({placeholders(len(candidate_list))}) AND datadate<=%s"
    )
    rset = {row[0] for row in fetch_all(rds, query, [*candidate_list, CUTOFF])[1]}
    wset = {row[0] for row in fetch_all(wrds, query, [*candidate_list, CUTOFF])[1]}
    return [gvkey for gvkey in candidate_list if gvkey in rset and gvkey in wset]


def random_accounting_candidates(
    wrds: psycopg.Connection[Any], label: str, limit: int = 250
) -> list[str]:
    header = "g_company" if label.startswith("comp.g_") else "company"
    _, rows, _ = fetch_all(
        wrds,
        f"""SELECT gvkey FROM comp.{header}
            ORDER BY md5(gvkey || %s) LIMIT %s""",
        (SEED + ":" + label, limit),
    )
    return [row[0] for row in rows]


def wrds_risk_gvkeys(wrds: psycopg.Connection[Any]) -> dict[str, list[str]]:
    risks: dict[str, list[str]] = {}
    queries = {
        "calendar_no_anchor_nonlookahead": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND prcc_c IS NOT NULL AND prcc_f IS NULL""",
        "calendar_anchor": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND prcc_c IS NOT NULL AND prcc_f IS NOT NULL""",
        "calendar_non_december": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND fyr<>12 AND prcc_c IS NOT NULL""",
        "annual_currency_usd": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND curcd='USD'""",
        "annual_currency_cad": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND curcd='CAD'""",
        "annual_currency_other": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND curcd IS NOT NULL AND curcd NOT IN ('USD','CAD')""",
        "annual_currency_null": """SELECT gvkey FROM comp.funda
            WHERE datadate<=%s AND curcd IS NULL""",
        "quarter_currency_usd": """SELECT gvkey FROM comp.fundq
            WHERE datadate<=%s AND curcdq='USD'""",
        "quarter_currency_cad": """SELECT gvkey FROM comp.fundq
            WHERE datadate<=%s AND curcdq='CAD'""",
        "quarter_currency_other": """SELECT gvkey FROM comp.fundq
            WHERE datadate<=%s AND curcdq IS NOT NULL AND curcdq NOT IN ('USD','CAD')""",
        "quarter_currency_null": """SELECT gvkey FROM comp.fundq
            WHERE datadate<=%s AND curcdq IS NULL""",
        "global_annual_currency_usd": """SELECT gvkey FROM comp.g_funda
            WHERE datadate<=%s AND curcd='USD'""",
        "global_annual_currency_cad": """SELECT gvkey FROM comp.g_funda
            WHERE datadate<=%s AND curcd='CAD'""",
        "global_annual_currency_other": """SELECT gvkey FROM comp.g_funda
            WHERE datadate<=%s AND curcd IS NOT NULL AND curcd NOT IN ('USD','CAD')""",
        "global_annual_currency_null": """SELECT gvkey FROM comp.g_funda
            WHERE datadate<=%s AND curcd IS NULL""",
        "global_quarter_currency_usd": """SELECT gvkey FROM comp.g_fundq
            WHERE datadate<=%s AND curcdq='USD'""",
        "global_quarter_currency_cad": """SELECT gvkey FROM comp.g_fundq
            WHERE datadate<=%s AND curcdq='CAD'""",
        "global_quarter_currency_other": """SELECT gvkey FROM comp.g_fundq
            WHERE datadate<=%s AND curcdq IS NOT NULL AND curcdq NOT IN ('USD','CAD')""",
        "global_quarter_currency_null": """SELECT gvkey FROM comp.g_fundq
            WHERE datadate<=%s AND curcdq IS NULL""",
    }
    for name, query in queries.items():
        query += " GROUP BY gvkey ORDER BY md5(gvkey || %s) LIMIT 12"
        _, rows, _ = fetch_all(wrds, query, (CUTOFF, SEED + ":" + name))
        risks[name] = [row[0] for row in rows]

    _, rows, _ = fetch_all(
        wrds,
        """SELECT des.gvkey
           FROM comp.co_adesind des
           JOIN comp.co_amkt mkc
             ON mkc.gvkey=des.gvkey AND mkc."year"=des.fyear
            AND mkc.popsrc=des.popsrc AND mkc.cfflag='C' AND mkc.curcd=des.curcd
           LEFT JOIN comp.co_amkt mkf
             ON mkf.gvkey=des.gvkey AND mkf.datadate=des.datadate
            AND mkf.popsrc=des.popsrc AND mkf.cfflag='F' AND mkf.curcd=des.curcd
           WHERE des.popsrc='D' AND des.datadate<=%s
             AND mkf.gvkey IS NULL AND mkc.datadate>des.datadate
           GROUP BY des.gvkey ORDER BY md5(des.gvkey || %s) LIMIT 12""",
        (CUTOFF, SEED + ":calendar-no-anchor-lookahead"),
    )
    risks["calendar_no_anchor_lookahead"] = [row[0] for row in rows]

    _, rows, _ = fetch_all(
        wrds,
        """SELECT c.gvkey
           FROM comp.company c
           WHERE c.fic='CAN'
             AND EXISTS (SELECT 1 FROM comp.sec_history h
                         WHERE h.gvkey=c.gvkey AND h.item='PRIHISTUSA')
             AND EXISTS (SELECT 1 FROM comp.sec_history h
                         WHERE h.gvkey=c.gvkey AND h.item='PRIHISTCAN')
           GROUP BY c.gvkey ORDER BY md5(c.gvkey || %s) LIMIT 24""",
        (SEED + ":dual-prihist",),
    )
    risks["dual_prihist_can"] = [row[0] for row in rows]
    _, rows, _ = fetch_all(
        wrds,
        """SELECT gvkey FROM comp.g_sec_history
           WHERE item='PRIHISTROW' GROUP BY gvkey
           ORDER BY md5(gvkey || %s) LIMIT 20""",
        (SEED + ":global-prihistrow",),
    )
    risks["global_prihistrow"] = [row[0] for row in rows]
    return risks


ACCOUNTING_REGRESSIONS: dict[str, list[str]] = {
    "comp.funda": [
        "163937",
        "174355",
        "006875",
        "145414",
        "156735",
        "178535",
        "064683",
        "020701",
        "338735",
        "010277",
        "018513",
        "184740",
        "006233",
        "009102",
        "011632",
    ],
    "comp.g_funda": ["100794", "216030"],
    "comp.fundq": ["184740", "002137", "001096", "018513", "006233", "009102"],
    "comp.g_fundq": ["100794", "216030"],
}


def choose_accounting_samples(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any]
) -> dict[str, dict[str, Any]]:
    risks = wrds_risk_gvkeys(wrds)
    risk_names = {
        "comp.funda": [
            "calendar_anchor",
            "calendar_no_anchor_nonlookahead",
            "calendar_no_anchor_lookahead",
            "calendar_non_december",
            "annual_currency_usd",
            "annual_currency_cad",
            "annual_currency_other",
            "annual_currency_null",
            "dual_prihist_can",
        ],
        "comp.fundq": [
            "quarter_currency_usd",
            "quarter_currency_cad",
            "quarter_currency_other",
            "quarter_currency_null",
            "dual_prihist_can",
        ],
        "comp.g_funda": [
            "global_annual_currency_usd",
            "global_annual_currency_cad",
            "global_annual_currency_other",
            "global_annual_currency_null",
            "global_prihistrow",
        ],
        "comp.g_fundq": [
            "global_quarter_currency_usd",
            "global_quarter_currency_cad",
            "global_quarter_currency_other",
            "global_quarter_currency_null",
            "global_prihistrow",
        ],
    }
    targets = {"comp.funda": 36, "comp.fundq": 28, "comp.g_funda": 22, "comp.g_fundq": 22}
    output: dict[str, dict[str, Any]] = {}
    for label in ACCOUNTING:
        tagged: dict[str, set[str]] = defaultdict(set)
        ordered: list[str] = []
        for gvkey in ACCOUNTING_REGRESSIONS[label]:
            tagged[gvkey].add("regression")
            if gvkey not in ordered:
                ordered.append(gvkey)
        # Round-robin one candidate per risk before taking a second from any
        # stratum, preventing the large USD stratum from swallowing the sample.
        for round_index in range(4):
            for risk_name in risk_names[label]:
                values = risks[risk_name]
                if round_index < len(values):
                    gvkey = values[round_index]
                    tagged[gvkey].add(risk_name)
                    if gvkey not in ordered:
                        ordered.append(gvkey)
        randoms = random_accounting_candidates(wrds, label)
        for gvkey in randoms:
            tagged[gvkey].add("fresh_hash_sample")
            if gvkey not in ordered:
                ordered.append(gvkey)
            if len(ordered) >= targets[label] * 3:
                break
        common_candidates = common_existing_gvkeys(rds, wrds, label, ordered)
        random_common = [
            gvkey
            for gvkey in common_candidates
            if "fresh_hash_sample" in tagged[gvkey] and "regression" not in tagged[gvkey]
        ]
        non_random_common = [gvkey for gvkey in common_candidates if gvkey not in random_common]
        reserved_random = 15
        common = list(
            dict.fromkeys(
                [
                    *non_random_common[: targets[label] - reserved_random],
                    *random_common[:reserved_random],
                ]
            )
        )
        if len(common) < targets[label]:
            common.extend(gvkey for gvkey in random_common[reserved_random:] if gvkey not in common)
            common = common[: targets[label]]
        if len(common) < 15:
            raise RuntimeError(f"only {len(common)} common accounting gvkeys for {label}")
        fresh_hash_count = sum(
            "fresh_hash_sample" in tagged[gvkey] and "regression" not in tagged[gvkey]
            for gvkey in common
        )
        if fresh_hash_count < 15:
            raise RuntimeError(f"only {fresh_hash_count} fresh hash accounting gvkeys for {label}")
        output[label] = {
            "gvkeys": common,
            "tags": {gvkey: sorted(tagged[gvkey]) for gvkey in common},
            "target": targets[label],
            "fresh_hash_sample_count": fresh_hash_count,
        }
    output["_risk_candidate_counts"] = {name: len(values) for name, values in risks.items()}
    return output


def header_rows(
    conn: psycopg.Connection[Any], header: str
) -> dict[tuple[str, str], tuple[Any, ...]]:
    columns, rows, _ = fetch_all(
        conn,
        f"SELECT gvkey, iid, secstat, excntry, exchg FROM comp.{header}",
    )
    del columns
    return {(row[0], row[1]): row[2:] for row in rows}


def raw_nonempty_pairs(
    conn: psycopg.Connection[Any], schema: str, table: str, pairs: Sequence[tuple[str, str]]
) -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for start in range(0, len(pairs), 80):
        chunk = pairs[start : start + 80]
        predicate, params = pair_predicate(chunk)
        _, rows, _ = fetch_all(
            conn,
            f"""SELECT gvkey, iid FROM {schema}.{table}
                WHERE ({predicate}) AND datadate<=%s GROUP BY gvkey, iid""",
            [*params, CUTOFF],
        )
        found.update((row[0], row[1]) for row in rows)
    return found


DAILY_REGRESSIONS = {
    "comp.secd": [("020727", "01"), ("035213", "02")],
    "comp.g_secd": [
        ("216030", "01W"),
        ("214759", "90W"),
        ("362268", "02W"),
        ("246156", "02W"),
        ("313819", "04W"),
        ("370366", "01W"),
        ("314232", "01W"),
    ],
}


def choose_daily_candidates(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], label: str
) -> tuple[list[tuple[str, str]], dict[str, list[str]]]:
    header = "g_security" if label == "comp.g_secd" else "security"
    rrows = header_rows(rds, header)
    wrows = header_rows(wrds, header)
    common = set(rrows) & set(wrows)
    tags: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pair in common:
        iid = pair[1]
        secstat, excntry, _exchg = wrows[pair]
        tags[pair].add("active" if secstat == "A" else "inactive_or_other")
        if iid.endswith("C"):
            tags[pair].add("canadian_iid")
        if iid.endswith("W"):
            tags[pair].add("world_iid")
        if excntry == "CAN":
            tags[pair].add("canadian_exchange_country")
        elif excntry == "USA":
            tags[pair].add("usa_exchange_country")
        else:
            tags[pair].add("other_exchange_country")
    for pair in DAILY_REGRESSIONS[label]:
        if pair in common:
            tags[pair].add("regression")

    strata = [
        "regression",
        "canadian_iid",
        "canadian_exchange_country",
        "inactive_or_other",
        "other_exchange_country",
        "active",
        "usa_exchange_country",
        "world_iid",
    ]
    selected: list[tuple[str, str]] = []
    ordered_by_stratum = {
        tag: sorted(
            (pair for pair in common if tag in tags[pair]),
            key=lambda pair: stable_rank(label, tag, *pair),
        )
        for tag in strata
    }
    for round_index in range(100):
        for tag in strata:
            values = ordered_by_stratum[tag]
            if round_index < len(values) and values[round_index] not in selected:
                selected.append(values[round_index])
        if len(selected) >= 520:
            break
    r_nonempty = raw_nonempty_pairs(rds, "public", "sec_dprc", selected)
    wrds_table = "g_sec_dprc" if label == "comp.g_secd" else "sec_dprc"
    w_nonempty = raw_nonempty_pairs(wrds, "comp", wrds_table, selected)
    retained = [pair for pair in selected if pair in r_nonempty and pair in w_nonempty]
    return retained, {f"{a}/{b}": sorted(tags[(a, b)]) for a, b in retained}


def secm_risk_pairs(wrds: psycopg.Connection[Any]) -> list[tuple[str, str]]:
    risks = [("029397", "01"), ("029397", "02"), ("033008", "01C")]
    _, rows, _ = fetch_all(
        wrds,
        """SELECT DISTINCT gvkey, iid FROM comp.secm
           WHERE sph100 IS NOT NULL OR sphcusip IS NOT NULL OR sphiid IS NOT NULL
           ORDER BY gvkey, iid LIMIT 12""",
    )
    risks.extend((row[0], row[1]) for row in rows)
    return list(dict.fromkeys(risks))


def choose_secm_pairs(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], target: int = 30
) -> tuple[list[tuple[str, str]], dict[str, list[str]]]:
    rheaders = header_rows(rds, "security")
    wheaders = header_rows(wrds, "security")
    common = set(rheaders) & set(wheaders)
    tagged: dict[tuple[str, str], set[str]] = defaultdict(set)
    for pair in secm_risk_pairs(wrds):
        if pair in common:
            tagged[pair].add("secm_regression")
    for pair in sorted(common, key=lambda value: stable_rank("secm", *value))[:500]:
        tagged[pair].add("fresh_hash_sample")
        iid = pair[1]
        secstat, excntry, _ = wheaders[pair]
        if iid.endswith("C") or excntry == "CAN":
            tagged[pair].add("canadian_precision_risk")
        if secstat != "A":
            tagged[pair].add("inactive_or_other")
    candidates = sorted(
        tagged,
        key=lambda pair: ("secm_regression" not in tagged[pair], stable_rank("secm-final", *pair)),
    )
    retained: list[tuple[str, str]] = []
    for start in range(0, len(candidates), 80):
        chunk = candidates[start : start + 80]
        pred, params = pair_predicate(chunk)
        query = f"SELECT DISTINCT gvkey,iid FROM comp.secm WHERE ({pred}) AND datadate<=%s"
        rset = {(a, b) for a, b in fetch_all(rds, query, [*params, CUTOFF])[1]}
        wset = {(a, b) for a, b in fetch_all(wrds, query, [*params, CUTOFF])[1]}
        retained.extend(pair for pair in chunk if pair in rset and pair in wset)
        if len(retained) >= target:
            break
    retained = retained[:target]
    if len(retained) < 12:
        raise RuntimeError(f"only {len(retained)} common secm pairs")
    fresh_count = sum(
        "fresh_hash_sample" in tagged[pair] and "secm_regression" not in tagged[pair]
        for pair in retained
    )
    if fresh_count < 12:
        raise RuntimeError(f"only {fresh_count} fresh hash secm pairs")
    return retained, {f"{a}/{b}": sorted(tagged[(a, b)]) for a, b in retained}


def small_matched_keys(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    spec: ObjectSpec,
    target: int = 100,
) -> list[tuple[Any, ...]]:
    selected = ",".join(f'"{column}"' for column in spec.key)
    ordering = key_ordering(spec.key, object_data_types(rds, spec))
    where = f' WHERE "{spec.time_column}"<=%s' if spec.time_column else ""
    params: Sequence[Any] = (CUTOFF,) if spec.time_column else ()
    # Apply the explicit cross-database collation after DISTINCT. PostgreSQL
    # otherwise requires every collated ORDER BY expression to appear in the
    # DISTINCT select list even though it represents the same key column.
    query = (
        f"SELECT {selected} FROM (SELECT DISTINCT {selected} "
        f"FROM {spec.sql_name}{where}) matched_keys "
        f"ORDER BY {ordering} LIMIT 800"
    )
    rset = set(fetch_all(rds, query, params)[1])
    wrows = fetch_all(wrds, query, params)[1]
    common = [row for row in wrows if row in rset]
    if len(common) < min(target, len(wrows)):
        raise RuntimeError(f"insufficient matched keys for {spec.label}: {len(common)}")
    return common[:target]


def build_samples(rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any]) -> dict[str, Any]:
    accounting = choose_accounting_samples(rds, wrds)
    daily: dict[str, Any] = {}
    for label in DAILY:
        candidates, tags = choose_daily_candidates(rds, wrds, label)
        if len(candidates) < 110:
            raise RuntimeError(
                f"only {len(candidates)} common nonempty daily candidates for {label}"
            )
        candidates = candidates[:180]
        daily[label] = {
            "candidate_pairs": candidates,
            "tags": {f"{a}/{b}": tags[f"{a}/{b}"] for a, b in candidates},
            "population_pairs": candidates[:12],
        }
    secm_pairs, secm_tags = choose_secm_pairs(rds, wrds)
    small: dict[str, Any] = {}
    for label, spec in SPECS.items():
        if label in ACCOUNTING or label in DAILY or label == "comp.secm":
            continue
        small[label] = small_matched_keys(rds, wrds, spec)
    return {
        "method": (
            "new deterministic hash seed; accounting stratified by currency, calendar anchor/"
            "lookahead, and primary-history risk; daily stratified by status/country/iid and "
            "pre-filtered only through pair-indexed raw price queries; secm includes primary, "
            "Canadian precision, and S&P-history regressions"
        ),
        "accounting": accounting,
        "daily": daily,
        "secm": {
            "pairs": secm_pairs,
            "tags": secm_tags,
            "fresh_hash_sample_count": sum(
                "fresh_hash_sample" in tags and "secm_regression" not in tags
                for tags in secm_tags.values()
            ),
        },
        "small": small,
    }


def population_rows(
    conn: psycopg.Connection[Any],
    spec: ObjectSpec,
    selected: Sequence[str],
    data_types: dict[str, str],
    where: str,
    params: Sequence[Any],
) -> list[tuple[Any, ...]]:
    query = select_query(spec, selected, data_types, where)
    return fetch_all(conn, query, params)[1]


def common_non_null_presence(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    spec: ObjectSpec,
    column: str,
    data_types: dict[str, str],
    *,
    candidate_limit: int = 256,
) -> dict[str, Any]:
    """Find one common populated cell without ever scanning an RDS daily view.

    WRDS returns only a small candidate-key set.  The RDS lookup is then
    restricted by the complete natural keys (including date for daily data),
    so ``secd``/``g_secd`` remain index-driven.  This is a documented source
    expansion when a risk sample happens not to contain a sparse consumed
    field; it does not alter the base-sample non-null rates.
    """
    selected = ", ".join([*(f'"{name}"' for name in spec.key), f'"{column}"'])
    conditions = [f'"{column}" IS NOT NULL']
    params: list[Any] = []
    if spec.time_column:
        conditions.append(f'"{spec.time_column}"<=%s')
        params.append(CUTOFF)
    wrds_query = (
        f"SELECT {selected} FROM {spec.sql_name} WHERE {' AND '.join(conditions)} "
        f"ORDER BY {key_ordering(spec.key, data_types)} LIMIT %s"
    )
    _, candidates, seconds = fetch_all(wrds, wrds_query, [*params, candidate_limit])
    if not candidates:
        return {
            "status": "no_wrds_non_null_candidate",
            "wrds_candidate_rows": 0,
            "wrds_seconds": round(seconds, 3),
            "rds_query_scope": "not run",
        }
    candidate_keys = [tuple(row[: len(spec.key)]) for row in candidates]
    rds_values: dict[tuple[Any, ...], Any] = {}
    for start in range(0, len(candidate_keys), 64):
        chunk = candidate_keys[start : start + 64]
        predicate, key_params = explicit_key_predicate(spec.key, chunk)
        _, rows, _ = fetch_all(
            rds,
            f"SELECT {selected} FROM {spec.sql_name} "
            f'WHERE ({predicate}) AND "{column}" IS NOT NULL',
            key_params,
        )
        rds_values.update((tuple(row[: len(spec.key)]), row[-1]) for row in rows)
    for row in candidates:
        key = tuple(row[: len(spec.key)])
        if key in rds_values:
            return {
                "status": "common_non_null_proved",
                "key": key,
                "rds": rds_values[key],
                "wrds": row[-1],
                "values_equal": cells_equal(rds_values[key], row[-1]),
                "wrds_candidate_rows": len(candidates),
                "wrds_seconds": round(seconds, 3),
                "rds_query_scope": "complete explicit natural keys in batches of 64",
            }
    return {
        "status": "no_common_non_null_in_candidate_keys",
        "wrds_candidate_rows": len(candidates),
        "wrds_seconds": round(seconds, 3),
        "rds_query_scope": "complete explicit natural keys in batches of 64",
    }


def population_result(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    spec: ObjectSpec,
    consumed: Sequence[str],
    where: str,
    params: Sequence[Any],
) -> dict[str, Any]:
    r_columns = object_columns(rds, spec)
    w_columns = object_columns(wrds, spec)
    missing_rds = sorted(set(consumed) - set(r_columns))
    missing_wrds = sorted(set(consumed) - set(w_columns))
    common_consumed = [column for column in consumed if column in r_columns and column in w_columns]
    selected = list(dict.fromkeys((*spec.key, *common_consumed)))
    r_data_types = object_data_types(rds, spec)
    w_data_types = object_data_types(wrds, spec)
    if r_data_types != w_data_types:
        raise RuntimeError(f"schema types changed before pipeline comparison for {spec.label}")
    rrows = population_rows(rds, spec, selected, r_data_types, where, params)
    wrows = population_rows(wrds, spec, selected, w_data_types, where, params)
    key_indexes = [selected.index(column) for column in spec.key]

    def row_map(rows: Sequence[tuple[Any, ...]]) -> dict[tuple[Any, ...], list[tuple[Any, ...]]]:
        result: dict[tuple[Any, ...], list[tuple[Any, ...]]] = defaultdict(list)
        for row in rows:
            result[tuple(row[index] for index in key_indexes)].append(row)
        return result

    rmap, wmap = row_map(rrows), row_map(wrows)
    non_null = {column: [0, 0] for column in common_consumed}
    matched = rds_only = wrds_only = duplicate_groups = 0
    for key in set(rmap) | set(wmap):
        left, right = rmap.get(key, []), wmap.get(key, [])
        if not left:
            wrds_only += len(right)
            continue
        if not right:
            rds_only += len(left)
            continue
        if len(left) > 1 or len(right) > 1:
            duplicate_groups += 1
        pairs, extra_left, extra_right = pair_duplicate_groups(left, right)
        rds_only += extra_left
        wrds_only += extra_right
        for lrow, rrow in pairs:
            matched += 1
            for column in common_consumed:
                index = selected.index(column)
                non_null[column][0] += lrow[index] is not None
                non_null[column][1] += rrow[index] is not None
    rates = {
        column: {
            "rds_non_null": counts[0],
            "wrds_non_null": counts[1],
            "rds_rate": counts[0] / matched if matched else None,
            "wrds_rate": counts[1] / matched if matched else None,
            "rate_difference": (counts[0] - counts[1]) / matched if matched else None,
            "identical_count": counts[0] == counts[1],
        }
        for column, counts in non_null.items()
    }
    base_unpopulated = [
        column
        for column, item in rates.items()
        if item["rds_non_null"] == 0 or item["wrds_non_null"] == 0
    ]
    presence_expansion = {
        column: common_non_null_presence(rds, wrds, spec, column, r_data_types)
        for column in base_unpopulated
    }
    unpopulated_after_expansion = [
        column
        for column, item in presence_expansion.items()
        if item.get("status") != "common_non_null_proved"
    ]
    return {
        "consumed_columns": list(consumed),
        "consumed_column_count": len(consumed),
        "missing_on_rds": missing_rds,
        "missing_on_wrds": missing_wrds,
        "rds_rows": len(rrows),
        "wrds_rows": len(wrows),
        "matched_rows": matched,
        "rds_only_rows": rds_only,
        "wrds_only_rows": wrds_only,
        "duplicate_key_groups": duplicate_groups,
        "non_null_by_column": rates,
        "non_null_mismatch_columns": [
            column for column, item in rates.items() if not item["identical_count"]
        ],
        "unpopulated_in_base_sample": base_unpopulated,
        "presence_source_expansion": presence_expansion,
        "unpopulated_after_source_expansion": unpopulated_after_expansion,
        "all_columns_exist": not missing_rds and not missing_wrds,
        "all_consumed_columns_proved_populated": not unpopulated_after_expansion,
    }


def sample_filter(label: str, samples: dict[str, Any]) -> tuple[str, list[Any]]:
    spec = SPECS[label]
    if label in ACCOUNTING:
        gvkeys = samples["accounting"][label]["gvkeys"]
        return f"gvkey IN ({placeholders(len(gvkeys))}) AND datadate<=%s", [*gvkeys, CUTOFF]
    if label in DAILY:
        pairs = [tuple(pair) for pair in samples["daily"][label]["population_pairs"]]
        predicate, params = pair_predicate(pairs)
        return f"({predicate}) AND datadate<=%s", [*params, CUTOFF]
    if label == "comp.secm":
        pairs = [tuple(pair) for pair in samples["secm"]["pairs"]]
        predicate, params = pair_predicate(pairs)
        return f"({predicate}) AND datadate<=%s", [*params, CUTOFF]
    keys = [tuple(key) for key in samples["small"][label]]
    predicate, params = explicit_key_predicate(spec.key, keys)
    return f"({predicate})", params


def run_pipeline(evidence: dict[str, Any], *, force: bool) -> None:
    # A schema mismatch is itself validation evidence, not a reason to skip all
    # later required checks.  Enforce ordering/completeness here; the final
    # acceptance gate still fails unless all 18 schemas are exact.
    if evidence.get("schema", {}).get("summary", {}).get("objects") != len(SPECS):
        raise RuntimeError("schema phase must complete before pipeline phase")
    if force:
        for name in ("pipeline", "samples", "counts", "values", "root_evidence", "acceptance"):
            evidence.pop(name, None)
        save_evidence(evidence)
    section = evidence.setdefault("pipeline", {"objects": {}})
    if "derivation" not in section:
        section["derivation"] = pipeline_derivation_evidence()
        save_evidence(evidence)
    rds, wrds = connect()
    try:
        if "samples" not in evidence:
            ascii_print("choosing fresh risk-stratified samples")
            evidence["samples"] = build_samples(rds, wrds)
            save_evidence(evidence)
        samples = evidence["samples"]
        for label, spec in SPECS.items():
            if label in section["objects"]:
                continue
            where, params = sample_filter(label, samples)
            ascii_print(f"pipeline population {label} start")
            result = population_result(rds, wrds, spec, PIPELINE_COLUMNS[label], where, params)
            section["objects"][label] = result
            save_evidence(evidence)
            ascii_print(
                f"pipeline population {label}: matched={result['matched_rows']} "
                f"missing={len(result['missing_on_rds'])} rate-diffs={len(result['non_null_mismatch_columns'])}"
            )
        section["summary"] = {
            "objects": len(section["objects"]),
            "all_columns_present_objects": sum(
                item["all_columns_exist"] for item in section["objects"].values()
            ),
            "all_consumed_columns_proved_populated_objects": sum(
                item["all_consumed_columns_proved_populated"]
                for item in section["objects"].values()
            ),
            "unpopulated_after_source_expansion": {
                label: item["unpopulated_after_source_expansion"]
                for label, item in section["objects"].items()
                if item["unpopulated_after_source_expansion"]
            },
            "identical_population_objects": sum(
                not item["non_null_mismatch_columns"] for item in section["objects"].values()
            ),
            "matched_rows": sum(item["matched_rows"] for item in section["objects"].values()),
        }
        save_evidence(evidence)
    finally:
        rds.close()
        wrds.close()


def daily_pair_stats_batch(
    conn: psycopg.Connection[Any], spec: ObjectSpec, pairs: Sequence[tuple[str, str]]
) -> dict[tuple[str, str], tuple[int, Any, Any]]:
    predicate, params = pair_predicate(pairs)
    _, rows, _ = fetch_all(
        conn,
        f"""SELECT gvkey, iid, count(*), min(datadate), max(datadate)
            FROM {spec.sql_name}
            WHERE ({predicate}) AND datadate<=%s
            GROUP BY gvkey, iid""",
        [*params, CUTOFF],
    )
    return {(row[0], row[1]): (int(row[2]), row[3], row[4]) for row in rows}


def daily_counts_result(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    label: str,
    pairs: Sequence[tuple[str, str]],
    tags: dict[str, list[str]],
) -> dict[str, Any]:
    spec = SPECS[label]
    rows: list[dict[str, Any]] = []
    for start in range(0, len(pairs), 30):
        chunk = pairs[start : start + 30]
        rstats = daily_pair_stats_batch(rds, spec, chunk)
        wstats = daily_pair_stats_batch(wrds, spec, chunk)
        for pair in chunk:
            rvalue = rstats.get(pair, (0, None, None))
            wvalue = wstats.get(pair, (0, None, None))
            rows.append(
                {
                    "gvkey": pair[0],
                    "iid": pair[1],
                    "tags": tags.get(f"{pair[0]}/{pair[1]}", []),
                    "rds": rvalue,
                    "wrds": wvalue,
                    "identical": rvalue == wvalue,
                }
            )
        ascii_print(f"daily counts {label}: {len(rows)}/{len(pairs)}")
    return {
        "sample_size": len(rows),
        "method": "explicit scalar pair predicates in batches of 30; cutoff applied",
        "mismatch_pairs": sum(not row["identical"] for row in rows),
        "rows": rows,
    }


def run_counts(evidence: dict[str, Any], *, force: bool) -> None:
    if evidence.get("pipeline", {}).get("summary", {}).get("objects") != len(SPECS):
        raise RuntimeError("pipeline phase must complete before counts phase")
    if force:
        for name in ("counts", "values", "root_evidence", "acceptance"):
            evidence.pop(name, None)
        save_evidence(evidence)
    section = evidence.setdefault("counts", {"full": {}, "daily_samples": {}})
    rds, wrds = connect()
    try:
        for label, spec in SPECS.items():
            if label in DAILY or label in section["full"]:
                continue
            ascii_print(f"full count {label} start")
            started = time.monotonic()
            rcount = int(scalar(rds, f"SELECT count(*) FROM {spec.sql_name}"))
            rseconds = time.monotonic() - started
            started = time.monotonic()
            wcount = int(scalar(wrds, f"SELECT count(*) FROM {spec.sql_name}"))
            wseconds = time.monotonic() - started
            section["full"][label] = {
                "rds": rcount,
                "wrds": wcount,
                "delta": rcount - wcount,
                "rds_seconds": round(rseconds, 3),
                "wrds_seconds": round(wseconds, 3),
            }
            save_evidence(evidence)
            ascii_print(f"full count {label}: RDS={rcount} WRDS={wcount} delta={rcount - wcount}")
        for label in DAILY:
            if label in section["daily_samples"]:
                continue
            sample = evidence["samples"]["daily"][label]
            pairs = [tuple(pair) for pair in sample["candidate_pairs"][:110]]
            if len(pairs) < 100:
                raise RuntimeError(f"daily sample for {label} has only {len(pairs)} pairs")
            section["daily_samples"][label] = daily_counts_result(
                rds, wrds, label, pairs, sample["tags"]
            )
            save_evidence(evidence)
        section["summary"] = {
            "full_objects": len(section["full"]),
            "daily_objects": len(section["daily_samples"]),
            "daily_pairs": {
                label: item["sample_size"] for label, item in section["daily_samples"].items()
            },
            "daily_mismatch_pairs": {
                label: item["mismatch_pairs"] for label, item in section["daily_samples"].items()
            },
        }
        save_evidence(evidence)
    finally:
        rds.close()
        wrds.close()


def value_filter(label: str, samples: dict[str, Any]) -> tuple[str | None, list[Any], Any]:
    if label in ACCOUNTING:
        gvkeys = samples["accounting"][label]["gvkeys"]
        return (
            f"gvkey IN ({placeholders(len(gvkeys))}) AND datadate<=%s",
            [*gvkeys, CUTOFF],
            {"gvkeys": gvkeys, "tags": samples["accounting"][label]["tags"]},
        )
    if label in DAILY:
        candidates = [tuple(pair) for pair in samples["daily"][label]["candidate_pairs"]]
        regressions = [pair for pair in DAILY_REGRESSIONS[label] if pair in candidates]
        regression_set = set(regressions)
        fresh_pairs = [pair for pair in candidates if pair not in regression_set][:8]
        if len(fresh_pairs) < 8:
            raise RuntimeError(f"only {len(fresh_pairs)} fresh daily value pairs for {label}")
        pairs = list(dict.fromkeys([*regressions, *fresh_pairs]))
        predicate, params = pair_predicate(pairs)
        return (
            f"({predicate}) AND datadate<=%s",
            [*params, CUTOFF],
            {
                "pairs": pairs,
                "regression_pairs": regressions,
                "fresh_pairs": fresh_pairs,
                "fresh_pair_count": len(fresh_pairs),
                "tags": {
                    f"{a}/{b}": samples["daily"][label]["tags"].get(f"{a}/{b}", [])
                    for a, b in pairs
                },
            },
        )
    if label == "comp.secm":
        pairs = [tuple(pair) for pair in samples["secm"]["pairs"]]
        predicate, params = pair_predicate(pairs)
        return (
            f"({predicate}) AND datadate<=%s",
            [*params, CUTOFF],
            {"pairs": pairs, "tags": samples["secm"]["tags"]},
        )
    spec = SPECS[label]
    if spec.time_column:
        return f'"{spec.time_column}"<=%s', [CUTOFF], {"scope": "full through cutoff"}
    return None, [], {"scope": "full table"}


def run_values(evidence: dict[str, Any], *, force: bool) -> None:
    counts_summary = evidence.get("counts", {}).get("summary", {})
    if counts_summary.get("full_objects") != len(SPECS) - len(DAILY):
        raise RuntimeError("counts phase must complete before values phase")
    if force:
        for name in ("values", "root_evidence", "acceptance"):
            evidence.pop(name, None)
        save_evidence(evidence)
    section = evidence.setdefault("values", {"objects": {}})
    ordered_labels = (*ACCOUNTING, *DAILY, "comp.secm", *FULL_VALUE_OBJECTS)
    rds, wrds = connect()
    try:
        for label in ordered_labels:
            if label in section["objects"]:
                continue
            spec = SPECS[label]
            columns = object_columns(rds, spec)
            wrds_columns = object_columns(wrds, spec)
            if columns != wrds_columns:
                raise RuntimeError(f"schema changed before value comparison for {label}")
            data_types = object_data_types(rds, spec)
            if data_types != object_data_types(wrds, spec):
                raise RuntimeError(f"schema types changed before value comparison for {label}")
            where, params, scope = value_filter(label, evidence["samples"])
            query = select_query(spec, columns, data_types, where)
            ascii_print(f"values {label} start ({len(columns)} columns)")
            result = compare_queries(rds, wrds, columns, spec.key, query, params)
            result["scope"] = scope
            result["column_count"] = len(columns)
            section["objects"][label] = result
            save_evidence(evidence)
            ascii_print(
                f"values {label}: matched={result['matched_rows']} cells={result['cells_compared_including_keys']} "
                f"mismatches={result['cell_mismatches']} R-only={result['rds_only_rows']} "
                f"W-only={result['wrds_only_rows']}"
            )
        section["summary"] = {
            "objects": len(section["objects"]),
            "matched_rows": sum(item["matched_rows"] for item in section["objects"].values()),
            "cells_compared_including_keys": sum(
                item["cells_compared_including_keys"] for item in section["objects"].values()
            ),
            "cell_mismatches": sum(item["cell_mismatches"] for item in section["objects"].values()),
            "rds_only_rows": sum(item["rds_only_rows"] for item in section["objects"].values()),
            "wrds_only_rows": sum(item["wrds_only_rows"] for item in section["objects"].values()),
            "duplicate_key_groups": sum(
                item["duplicate_key_groups"] for item in section["objects"].values()
            ),
        }
        save_evidence(evidence)
    finally:
        rds.close()
        wrds.close()


def query_both(
    rds: psycopg.Connection[Any],
    wrds: psycopg.Connection[Any],
    rds_query: str,
    wrds_query: str,
    params: Sequence[Any] = (),
) -> dict[str, Any]:
    rcols, rrows, rseconds = fetch_all(rds, rds_query, params)
    wcols, wrows, wseconds = fetch_all(wrds, wrds_query, params)
    rpayload = json.dumps(jsonable([rcols, rrows]), separators=(",", ":"), sort_keys=True)
    wpayload = json.dumps(jsonable([wcols, wrows]), separators=(",", ":"), sort_keys=True)
    return {
        "rds": {"columns": rcols, "rows": rrows, "seconds": round(rseconds, 3)},
        "wrds": {"columns": wcols, "rows": wrows, "seconds": round(wseconds, 3)},
        "normalized_parity": {
            "rds_rows": len(rrows),
            "wrds_rows": len(wrows),
            "rds_sha256": hashlib.sha256(rpayload.encode("utf-8")).hexdigest(),
            "wrds_sha256": hashlib.sha256(wpayload.encode("utf-8")).hexdigest(),
            "identical": rpayload == wpayload,
        },
    }


def known_root_evidence(
    rds: psycopg.Connection[Any], wrds: psycopg.Connection[Any], evidence: dict[str, Any]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    calendar_keys = [
        "163937",
        "174355",
        "006875",
        "145414",
        "156735",
        "178535",
        "064683",
    ]
    ph = placeholders(len(calendar_keys))
    result["calendar_view_regressions"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,datadate,fyear,fyr,curcd,prcc_f,prcc_c,prch_c,prcl_c,
                   cshtr_c,dvpsp_c,dvpsx_c,adjex_f,adjex_c
              FROM comp.funda WHERE gvkey IN ({ph}) AND datadate<=%s
             ORDER BY gvkey,datadate""",
        f"""SELECT gvkey,datadate,fyear,fyr,curcd,prcc_f,prcc_c,prch_c,prcl_c,
                   cshtr_c,dvpsp_c,dvpsx_c,adjex_f,adjex_c
              FROM comp.funda WHERE gvkey IN ({ph}) AND datadate<=%s
             ORDER BY gvkey,datadate""",
        [*calendar_keys, CUTOFF],
    )
    result["calendar_raw_co_amkt"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,datadate,"year",cfflag,curcd,popsrc,prcc,prch,prcl,cshtr,dvpsp,dvpsx
              FROM public.co_amkt WHERE gvkey IN ({ph}) ORDER BY gvkey,cfflag,datadate,curcd""",
        f"""SELECT gvkey,datadate,"year",cfflag,curcd,popsrc,prcc,prch,prcl,cshtr,dvpsp,dvpsx
              FROM comp.co_amkt WHERE gvkey IN ({ph}) ORDER BY gvkey,cfflag,datadate,curcd""",
        calendar_keys,
    )
    result["calendar_raw_co_adjfact"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,effdate,thrudate,adjex
              FROM public.co_adjfact WHERE gvkey IN ({ph})
             ORDER BY gvkey,effdate,thrudate,adjex""",
        f"""SELECT gvkey,effdate,thrudate,adjex
              FROM comp.co_adjfact WHERE gvkey IN ({ph})
             ORDER BY gvkey,effdate,thrudate,adjex""",
        calendar_keys,
    )

    hiid_keys = ["184740", "002137", "001096", "018513", "006233", "009102", "011632"]
    ph = placeholders(len(hiid_keys))
    result["fundq_hiid_regressions"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,datadate,hiid,fyr,curcdq FROM comp.fundq
              WHERE gvkey IN ({ph}) AND datadate<=%s ORDER BY gvkey,datadate,fyr,curcdq""",
        f"""SELECT gvkey,datadate,hiid,fyr,curcdq FROM comp.fundq
              WHERE gvkey IN ({ph}) AND datadate<=%s ORDER BY gvkey,datadate,fyr,curcdq""",
        [*hiid_keys, CUTOFF],
    )
    result["funda_hiid_regressions"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,datadate,hiid,curcd FROM comp.funda
              WHERE gvkey IN ({ph}) AND datadate<=%s ORDER BY gvkey,datadate,curcd""",
        f"""SELECT gvkey,datadate,hiid,curcd FROM comp.funda
              WHERE gvkey IN ({ph}) AND datadate<=%s ORDER BY gvkey,datadate,curcd""",
        [*hiid_keys, CUTOFF],
    )
    result["hiid_raw_company"] = query_both(
        rds,
        wrds,
        f"SELECT gvkey,fic,priusa,prican,prirow FROM public.company WHERE gvkey IN ({ph}) ORDER BY gvkey",
        f"SELECT gvkey,fic,priusa,prican,prirow FROM comp.company WHERE gvkey IN ({ph}) ORDER BY gvkey",
        hiid_keys,
    )
    result["hiid_raw_history"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,iid,item,itemvalue,effdate,thrudate FROM public.sec_history
              WHERE gvkey IN ({ph}) AND item LIKE 'PRIHIST%%'
              ORDER BY gvkey,item,effdate,itemvalue""",
        f"""SELECT gvkey,iid,item,itemvalue,effdate,thrudate FROM comp.sec_history
              WHERE gvkey IN ({ph}) AND item LIKE 'PRIHIST%%'
              ORDER BY gvkey,item,effdate,itemvalue""",
        hiid_keys,
    )

    trfd_pairs = list(dict.fromkeys(DAILY_REGRESSIONS["comp.g_secd"]))
    pair_where, pair_params = pair_predicate(trfd_pairs)
    result["global_trfd_raw"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,iid,count(*) AS rows,min(datadate),max(datadate),min(trfd),max(trfd)
              FROM public.sec_dtrt WHERE ({pair_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        f"""SELECT gvkey,iid,count(*) AS rows,min(datadate),max(datadate),min(trfd),max(trfd)
              FROM comp.g_sec_dtrt WHERE ({pair_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        [*pair_params, CUTOFF],
    )
    result["global_trfd_views"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,iid,count(*),min(trfd),max(trfd)
              FROM comp.g_secd WHERE ({pair_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        f"""SELECT gvkey,iid,count(*),min(trfd),max(trfd)
              FROM comp.g_secd WHERE ({pair_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        [*pair_params, CUTOFF],
    )

    na_trfd_pairs = [("035213", "02")]
    na_where, na_params = pair_predicate(na_trfd_pairs)
    result["north_american_trfd_raw"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,iid,count(*) AS rows,min(datadate),max(datadate),min(trfd),max(trfd)
              FROM public.sec_dtrt WHERE ({na_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        f"""SELECT gvkey,iid,count(*) AS rows,min(datadate),max(datadate),min(trfd),max(trfd)
              FROM comp.sec_dtrt WHERE ({na_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        [*na_params, CUTOFF],
    )
    result["north_american_trfd_views"] = query_both(
        rds,
        wrds,
        f"""SELECT gvkey,iid,count(*),count(trfd),min(trfd),max(trfd)
              FROM comp.secd WHERE ({na_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        f"""SELECT gvkey,iid,count(*),count(trfd),min(trfd),max(trfd)
              FROM comp.secd WHERE ({na_where}) AND datadate<=%s
              GROUP BY gvkey,iid ORDER BY gvkey,iid""",
        [*na_params, CUTOFF],
    )

    result["retained_funda_rank_raw"] = query_both(
        rds,
        wrds,
        """SELECT gvkey,datadate,indfmt,datafmt,consol,popsrc,rank
             FROM public.co_aaudit WHERE gvkey='011632' AND datadate='2023-09-30'""",
        """SELECT gvkey,datadate,indfmt,datafmt,consol,popsrc,rank
             FROM comp.co_aaudit WHERE gvkey='011632' AND datadate='2023-09-30'""",
    )
    result["raw_r_ex_codes"] = query_both(
        rds,
        wrds,
        "SELECT * FROM public.r_ex_codes ORDER BY exchgcd",
        "SELECT * FROM comp.r_ex_codes ORDER BY exchgcd",
    )

    secm_examples: list[tuple[str, str, str]] = []
    secm_values = evidence.get("values", {}).get("objects", {}).get("comp.secm", {})
    for column_examples in secm_values.get("examples", {}).values():
        for example in column_examples:
            key = example.get("key", [])
            if len(key) >= 3:
                candidate = (str(key[0]), str(key[1]), str(key[2])[:10])
                if candidate not in secm_examples:
                    secm_examples.append(candidate)
            if len(secm_examples) >= 12:
                break
    if secm_examples:
        clauses = " OR ".join("(gvkey=%s AND iid=%s AND datadate=%s)" for _ in secm_examples)
        params = [value for key in secm_examples for value in key]
        result["secm_raw_mismatch_dates"] = {}
        for table in ("sec_mthprc", "sec_mthdiv", "sec_mthtrt"):
            result["secm_raw_mismatch_dates"][table] = query_both(
                rds,
                wrds,
                f"SELECT * FROM public.{table} WHERE {clauses} ORDER BY gvkey,iid,datadate",
                f"SELECT * FROM comp.{table} WHERE {clauses} ORDER BY gvkey,iid,datadate",
                params,
            )
    return result


def current_state(rds: psycopg.Connection[Any]) -> dict[str, Any]:
    """Fingerprint current SQL inputs, database views, and FF rows without history tables."""

    sql_files = [
        "_population.sql",
        "_security.sql",
        "company.sql",
        "g_company.sql",
        "security.sql",
        "g_security.sql",
        "sec_history.sql",
        "g_sec_history.sql",
        "co_hgic.sql",
        "g_co_hgic.sql",
        "funda.sql",
        "g_funda.sql",
        "fundq.sql",
        "g_fundq.sql",
        "secm.sql",
        "secd.sql",
        "g_secd.sql",
        "exrt_dly.sql",
        "r_ex_codes.sql",
    ]
    digest = hashlib.sha256()
    file_hashes: dict[str, str] = {}
    for filename in sql_files:
        raw = (HERE / filename).read_bytes()
        file_hashes[filename] = hashlib.sha256(raw).hexdigest()
        digest.update(filename.encode("utf-8"))
        digest.update(b"\0")
        digest.update(raw)
        digest.update(b"\0")
    local_hash = digest.hexdigest()
    target_names = [spec.name for spec in SPECS.values() if spec.schema == "comp"]
    _, view_rows, _ = fetch_all(
        rds,
        """SELECT c.relname, pg_get_viewdef(c.oid, true)
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='comp' AND c.relkind='v' AND c.relname=ANY(%s)
            ORDER BY c.relname""",
        (target_names,),
    )
    view_digest = hashlib.sha256(
        json.dumps(jsonable(view_rows), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    _, ff_rows, _ = fetch_all(
        rds,
        """SELECT date,mktrf,smb,hml,rf,year,month,umd,dateff
             FROM ff.factors_monthly ORDER BY date""",
    )
    ff_digest = hashlib.sha256(
        json.dumps(jsonable(ff_rows), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "local_sql_content_sha256": local_hash,
        "local_file_sha256": file_hashes,
        "database_view_count": len(view_rows),
        "database_view_definition_sha256": view_digest,
        "ff_row_count": len(ff_rows),
        "ff_content_sha256": ff_digest,
    }


def difference_inventory(evidence: dict[str, Any]) -> dict[str, Any]:
    counts = evidence.get("counts", {})
    values = evidence.get("values", {}).get("objects", {})
    return {
        "count_deltas": {
            label: item["delta"]
            for label, item in counts.get("full", {}).items()
            if item.get("delta")
        },
        "daily_count_mismatch_pairs": {
            label: item["mismatch_pairs"]
            for label, item in counts.get("daily_samples", {}).items()
            if item.get("mismatch_pairs")
        },
        "value_differences": {
            label: {
                "mismatches_by_column": item["mismatches_by_column"],
                "rds_only_rows": item["rds_only_rows"],
                "wrds_only_rows": item["wrds_only_rows"],
            }
            for label, item in values.items()
            if item.get("cell_mismatches")
            or item.get("rds_only_rows")
            or item.get("wrds_only_rows")
        },
    }


def unclassified_difference_inventory(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    """Make every observed difference class explicit until root-cause review."""
    pending: list[dict[str, Any]] = []
    for label, delta in inventory.get("count_deltas", {}).items():
        pending.append(
            {
                "difference_id": f"count_delta:{label}",
                "evidence_path": f"root_evidence.difference_inventory.count_deltas.{label}",
                "observed": delta,
                "classes": [],
                "status": "unclassified_pending_report_review",
            }
        )
    for label, mismatch_pairs in inventory.get("daily_count_mismatch_pairs", {}).items():
        pending.append(
            {
                "difference_id": f"daily_count_mismatch:{label}",
                "evidence_path": (
                    f"root_evidence.difference_inventory.daily_count_mismatch_pairs.{label}"
                ),
                "observed": mismatch_pairs,
                "classes": [],
                "status": "unclassified_pending_report_review",
            }
        )
    for label, details in inventory.get("value_differences", {}).items():
        pending.append(
            {
                "difference_id": f"value_difference:{label}",
                "evidence_path": f"root_evidence.difference_inventory.value_differences.{label}",
                "observed": details,
                "classes": [],
                "status": "unclassified_pending_report_review",
            }
        )
    return pending


def apply_classification_overlay(evidence: dict[str, Any]) -> None:
    """Merge reviewed classifications without mutating the validator itself.

    The overlay is a JSON object keyed by the exact dynamic ``difference_id``.
    Each value must contain ``classification`` as either one class or a
    nonempty unique list of classes (a/b/c/d), nonempty ``root_cause`` text,
    and nonempty ``evidence`` text.  It is normalized to ``classes`` in the
    evidence.  Partial overlays are allowed but cannot pass the final gate;
    stale/unknown IDs are rejected.
    """
    root = evidence.get("root_evidence")
    if not root:
        return
    inventory = root.get("classification_inventory", [])
    by_id: dict[str, dict[str, Any]] = {}
    for item in inventory:
        difference_id = item.get("difference_id")
        if not isinstance(difference_id, str) or not difference_id:
            raise RuntimeError("classification inventory contains an invalid difference_id")
        if difference_id in by_id:
            raise RuntimeError(f"duplicate difference_id in inventory: {difference_id}")
        by_id[difference_id] = item
        item["classes"] = []
        item.pop("classification", None)
        item["status"] = "unclassified_pending_report_review"
        item.pop("root_cause", None)
        item.pop("evidence", None)

    root.pop("classification_overlay", None)
    evidence.get("metadata", {}).pop("classification_overlay", None)
    if not CLASSIFICATIONS_FILE.exists():
        return

    raw_bytes = CLASSIFICATIONS_FILE.read_bytes()
    try:
        overlay = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid classification overlay JSON: {exc}") from exc
    if not isinstance(overlay, dict):
        raise RuntimeError("classification overlay must be a JSON object keyed by difference_id")
    unknown_ids = sorted(set(overlay) - set(by_id))
    if unknown_ids:
        raise RuntimeError(f"classification overlay contains unknown difference IDs: {unknown_ids}")

    for difference_id, review in overlay.items():
        if not isinstance(review, dict):
            raise RuntimeError(f"classification for {difference_id} must be an object")
        classification = review.get("classification")
        root_cause = review.get("root_cause")
        supporting_evidence = review.get("evidence")
        if isinstance(classification, str):
            classes = [classification]
        elif isinstance(classification, list):
            classes = classification
        else:
            classes = []
        if (
            not classes
            or any(value not in {"a", "b", "c", "d"} for value in classes)
            or len(classes) != len(set(classes))
        ):
            raise RuntimeError(
                f"classification for {difference_id} must be one class or a "
                "nonempty unique list drawn from a/b/c/d"
            )
        if not isinstance(root_cause, str) or not root_cause.strip():
            raise RuntimeError(f"classification for {difference_id} needs root_cause text")
        if not isinstance(supporting_evidence, str) or not supporting_evidence.strip():
            raise RuntimeError(f"classification for {difference_id} needs evidence text")
        item = by_id[difference_id]
        item.update(
            {
                "classes": sorted(classes, key="abcd".index),
                "root_cause": root_cause.strip(),
                "evidence": supporting_evidence.strip(),
                "status": "classified_by_reviewed_overlay",
            }
        )

    overlay_state = {
        "file": CLASSIFICATIONS_FILE.name,
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "classified_difference_ids": sorted(overlay),
        "classified_count": len(overlay),
        "inventory_count": len(inventory),
    }
    root["classification_overlay"] = overlay_state
    evidence["metadata"]["classification_overlay"] = overlay_state


def run_roots(evidence: dict[str, Any], *, force: bool) -> None:
    if evidence.get("values", {}).get("summary", {}).get("objects") != len(SPECS):
        raise RuntimeError("values phase must complete before root-evidence phase")
    if force:
        evidence.pop("root_evidence", None)
        evidence.pop("acceptance", None)
        save_evidence(evidence)
    if "root_evidence" in evidence:
        return
    rds, wrds = connect()
    try:
        inventory = difference_inventory(evidence)
        section = {
            "difference_inventory": inventory,
            "classification_inventory": unclassified_difference_inventory(inventory),
            "current_state": current_state(rds),
            "known_difference_raw_checks": known_root_evidence(rds, wrds, evidence),
            "classification_note": (
                "This section stores raw facts. Every nonempty inventory entry must be assigned "
                "class a/b/c/d in the accompanying report; a new class-a entry fails the gate."
            ),
        }
        evidence["root_evidence"] = section
        save_evidence(evidence)
    finally:
        rds.close()
        wrds.close()


def acceptance_summary(evidence: dict[str, Any]) -> dict[str, Any]:
    schema = evidence.get("schema", {}).get("summary", {})
    pipeline = evidence.get("pipeline", {}).get("summary", {})
    count_section = evidence.get("counts", {})
    counts = count_section.get("summary", {})
    values = evidence.get("values", {}).get("objects", {})
    accounting_sizes = {
        label: len(
            evidence.get("samples", {}).get("accounting", {}).get(label, {}).get("gvkeys", [])
        )
        for label in ACCOUNTING
    }
    daily_value_sizes = {
        label: len(values.get(label, {}).get("scope", {}).get("pairs", [])) for label in DAILY
    }
    daily_value_fresh_sizes = {
        label: int(values.get(label, {}).get("scope", {}).get("fresh_pair_count", 0))
        for label in DAILY
    }
    accounting_fresh_sizes = {
        label: int(
            evidence.get("samples", {})
            .get("accounting", {})
            .get(label, {})
            .get("fresh_hash_sample_count", 0)
        )
        for label in ACCOUNTING
    }
    daily_count_fresh_sizes = {
        label: sum(
            "regression" not in row.get("tags", [])
            for row in count_section.get("daily_samples", {}).get(label, {}).get("rows", [])
        )
        for label in DAILY
    }
    secm_size = len(values.get("comp.secm", {}).get("scope", {}).get("pairs", []))
    secm_fresh_size = int(
        evidence.get("samples", {}).get("secm", {}).get("fresh_hash_sample_count", 0)
    )
    start_state = evidence.get("metadata", {}).get("validation_start_state")
    end_state = evidence.get("root_evidence", {}).get("current_state")
    same_state = bool(start_state and end_state and start_state == end_state)
    metadata = evidence.get("metadata", {})
    script_hash_stable = bool(
        metadata.get("script_sha256_at_validation_start")
        and metadata.get("script_sha256_at_validation_start")
        == metadata.get("script_sha256_at_validation_end")
        == metadata.get("script_sha256")
    )
    classification_inventory = evidence.get("root_evidence", {}).get("classification_inventory", [])
    unclassified_differences = sum(not item.get("classes") for item in classification_inventory)
    class_a_differences = sum("a" in item.get("classes", []) for item in classification_inventory)
    checks = {
        "schema_18_of_18_exact": schema.get("identical_objects") == len(SPECS),
        "pipeline_18_objects_checked": pipeline.get("objects") == len(SPECS),
        "pipeline_all_consumed_columns_exist": pipeline.get("all_columns_present_objects")
        == len(SPECS),
        "pipeline_all_consumed_columns_proved_populated": pipeline.get(
            "all_consumed_columns_proved_populated_objects"
        )
        == len(SPECS),
        "full_counts_16_objects": counts.get("full_objects") == len(SPECS) - len(DAILY),
        "daily_counts_at_least_100_each": all(
            counts.get("daily_pairs", {}).get(label, 0) >= 100 for label in DAILY
        ),
        "daily_counts_at_least_100_fresh_each": all(
            size >= 100 for size in daily_count_fresh_sizes.values()
        ),
        "accounting_at_least_15_each": all(size >= 15 for size in accounting_sizes.values()),
        "accounting_at_least_15_fresh_each": all(
            size >= 15 for size in accounting_fresh_sizes.values()
        ),
        "daily_values_at_least_8_each": all(size >= 8 for size in daily_value_sizes.values()),
        "daily_values_at_least_8_fresh_each": all(
            size >= 8 for size in daily_value_fresh_sizes.values()
        ),
        "secm_values_at_least_12": secm_size >= 12,
        "secm_values_at_least_12_fresh": secm_fresh_size >= 12,
        "all_18_value_objects": len(values) == len(SPECS),
        "no_duplicate_key_groups": sum(
            item.get("duplicate_key_groups", 0) for item in values.values()
        )
        == 0,
        "root_evidence_present": "root_evidence" in evidence,
        "view_and_ff_state_unchanged_during_validation": same_state,
        "validator_script_hash_stable": script_hash_stable,
        "all_observed_difference_classes_classified": unclassified_differences == 0,
        "no_residual_class_a_construction_differences": class_a_differences == 0,
    }
    return {
        "checks": checks,
        "mechanical_checks_passed": all(
            value
            for name, value in checks.items()
            if name
            not in {
                "all_observed_difference_classes_classified",
                "no_residual_class_a_construction_differences",
            }
        ),
        "passed": all(checks.values()),
        "unclassified_difference_count": unclassified_differences,
        "class_a_difference_count": class_a_differences,
        "accounting_sample_sizes": accounting_sizes,
        "accounting_fresh_sample_sizes": accounting_fresh_sizes,
        "daily_count_fresh_sample_sizes": daily_count_fresh_sizes,
        "daily_value_sample_sizes": daily_value_sizes,
        "daily_value_fresh_sample_sizes": daily_value_fresh_sizes,
        "secm_value_sample_size": secm_size,
        "secm_value_fresh_sample_size": secm_fresh_size,
    }


def assert_no_credentials(evidence: dict[str, Any]) -> None:
    rendered = json.dumps(jsonable(evidence), sort_keys=True)
    env = read_env()
    for key in ("COMPUSTAT", "ENV_USERNAME", "ENV_PASSWORD"):
        value = env.get(key)
        if value and value in rendered:
            raise RuntimeError(f"credential value for {key} was embedded in evidence")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "phase",
        choices=("schema", "pipeline", "counts", "values", "roots", "all", "summary"),
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    assert_merge_ordering_contract()
    evidence = load_evidence()
    invocation_hash = current_script_sha256()
    stored_hash = evidence["metadata"].get("script_sha256")
    if args.force and args.phase in {"schema", "all"}:
        evidence["metadata"]["script_sha256"] = invocation_hash
        evidence["metadata"]["script_sha256_at_validation_start"] = invocation_hash
        evidence["metadata"]["validation_run_started_at"] = dt.datetime.now(dt.UTC).isoformat()
    elif stored_hash and stored_hash != invocation_hash:
        raise RuntimeError(
            "validator changed since evidence creation; restart with `all --force` "
            "or `schema --force`"
        )
    evidence["metadata"]["script_sha256_at_invocation"] = invocation_hash
    evidence["metadata"]["merge_ordering_self_test"] = "passed: numeric 1/2/10/NULL and C text"
    phases = (
        ("schema", run_schema),
        ("pipeline", run_pipeline),
        ("counts", run_counts),
        ("values", run_values),
        ("roots", run_roots),
    )
    if args.phase != "summary":
        for name, function in phases:
            if args.phase in {name, "all"}:
                function(evidence, force=args.force)
    apply_classification_overlay(evidence)
    evidence["metadata"]["script_sha256_at_validation_end"] = current_script_sha256()
    evidence["acceptance"] = acceptance_summary(evidence)
    assert_no_credentials(evidence)
    save_evidence(evidence)
    ascii_print(json.dumps(evidence["acceptance"], indent=2, sort_keys=True))
    ascii_print(f"evidence: {EVIDENCE_FILE}")


if __name__ == "__main__":
    main()
