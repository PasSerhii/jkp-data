"""Generate WRDS-compatible view DDL for a native XpressFeed database.

Description:
    Connects to WRDS and the XpressFeed RDS, reads the exact column layout
    (names, order, types) of each WRDS Compustat view the jkp pipeline
    downloads, resolves every column to its source in the native XpressFeed
    raw tables, and writes one CREATE VIEW file per view into this directory.
Steps:
    1) Fetch WRDS information_schema for the 17 comp.* objects.
    2) Fetch raw-table column sets from the XpressFeed DB.
    3) Resolve each WRDS column via per-view alias priority + special rules
       (co_amkt cfflag C/F pivot, y-suffixed YTD items, header joins).
    4) Emit DDL with every column cast to the WRDS type; unresolved columns
       become typed NULLs and are listed in the console report.
Output:
    <view>.sql files in sql/xpressfeed_views/ and a resolution report on stdout.

Run:  uv run --with "psycopg[binary]" python sql/xpressfeed_views/gen_views.py
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path

import psycopg

OUT_DIR = Path(__file__).parent
ENV = Path(__file__).parents[2] / ".env"
CONTRACT_FILE = OUT_DIR / "wrds_schema_contract.json"

VIEW_NAMES = (
    "funda", "g_funda", "fundq", "g_fundq", "secd", "g_secd", "secm",
    "security", "g_security", "company", "g_company", "sec_history",
    "g_sec_history", "co_hgic", "g_co_hgic", "exrt_dly", "r_ex_codes",
)

TYPE_MAP = {
    "character varying": "varchar",
    "character": "char",
    "numeric": "numeric",
    "double precision": "float8",
    "real": "float4",
    "integer": "int4",
    "bigint": "int8",
    "smallint": "int2",
    "date": "date",
    "timestamp without time zone": "timestamp",
    "text": "text",
}


# The credentials these scripts need. The process environment may supply them
# instead of the .env file, which is how the container form works: the runtime
# image ships no .env and `docker run --env-file` is the only channel.
ENV_KEYS = ("COMPUSTAT", "ENV_USERNAME", "ENV_PASSWORD")


def load_env() -> dict[str, str]:
    """Read credentials from the repo .env, then let the environment override.

    Environment wins so a container started with ``--env-file`` works with no
    .env on disk at all. On a developer machine none of ``ENV_KEYS`` is normally
    exported, so the file still decides.
    """
    env: dict[str, str] = {}
    if ENV.is_file():
        for line in ENV.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                value = v.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                    value = value[1:-1]
                env[k.strip()] = value
    env.update({k: os.environ[k] for k in ENV_KEYS if os.environ.get(k)})
    return env


def wrds_columns(cur, table: str) -> list[tuple[str, str]]:
    cur.execute(
        """select column_name, data_type, numeric_precision, numeric_scale, character_maximum_length
           from information_schema.columns
           where table_schema='comp' and table_name=%s order by ordinal_position""",
        (table,),
    )
    out = []
    for name, dtype, prec, scale, charlen in cur.fetchall():
        typ = TYPE_MAP[dtype]
        # Constrained numerics must keep the WRDS precision/scale so values are
        # rounded identically (e.g. secm.trfm is numeric(*,4) on WRDS while the
        # raw monthly tables carry full precision). Same for varchar lengths so
        # information_schema matches WRDS exactly.
        if typ == "numeric" and prec is not None and scale is not None:
            typ = f"numeric({prec},{scale})"
        if typ == "varchar" and charlen is not None:
            typ = f"varchar({charlen})"
        out.append((name, typ))
    return out


def write_schema_contract(cur) -> dict[str, list[tuple[str, str]]]:
    objects = {name: wrds_columns(cur, name) for name in VIEW_NAMES}
    missing = [name for name, cols in objects.items() if not cols]
    if missing:
        raise RuntimeError(f"WRDS objects missing from schema contract: {missing}")
    payload = {
        "source": "WRDS comp information_schema",
        "refreshed_at_utc": dt.datetime.now(dt.UTC).isoformat(),
        "objects": {name: [[col, typ] for col, typ in cols] for name, cols in objects.items()},
    }
    tmp = CONTRACT_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, CONTRACT_FILE)
    return objects


def load_schema_contract() -> dict[str, list[tuple[str, str]]]:
    if not CONTRACT_FILE.exists():
        raise RuntimeError(
            f"Missing {CONTRACT_FILE.name}; run gen_views.py --refresh-contract "
            "once with WRDS access and review the checked-in contract"
        )
    payload = json.loads(CONTRACT_FILE.read_text(encoding="utf-8"))
    raw_objects = payload.get("objects", {})
    missing = [name for name in VIEW_NAMES if name not in raw_objects]
    extra = sorted(set(raw_objects) - set(VIEW_NAMES))
    if missing or extra:
        raise RuntimeError(f"Invalid schema contract; missing={missing}, extra={extra}")
    objects: dict[str, list[tuple[str, str]]] = {}
    for name in VIEW_NAMES:
        cols = raw_objects[name]
        if not cols or any(not isinstance(row, list) or len(row) != 2 for row in cols):
            raise RuntimeError(f"Invalid column contract for comp.{name}")
        objects[name] = [(str(col), str(typ)) for col, typ in cols]
    return objects


def raw_columns(cur, table: str) -> set[str]:
    cur.execute(
        """select column_name from information_schema.columns
           where table_schema='public' and table_name=%s""",
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


def q(col: str) -> str:
    return f'"{col}"'


def build_select(
    wrds_cols: list[tuple[str, str]],
    resolve,  # (col) -> expression or None
    no_cast: frozenset[str] = frozenset(),
    no_normalize: frozenset[str] = frozenset(),
) -> tuple[str, list[str]]:
    # no_cast columns are emitted as bare references. Needed where a cast would
    # stop Postgres pushing outer quals below a window function (g_secd.monthend
    # partitions by gvkey/iid; a ::varchar relabel on them forces the window to
    # run over the whole table for point queries).
    lines, unresolved = [], []
    for col, typ in wrds_cols:
        expr = resolve(col)
        if expr is None:
            unresolved.append(col)
            expr = "NULL"
        # WRDS loads via SAS, which strips trailing blanks and stores empty
        # strings as NULL; the raw feed keeps trailing spaces. Normalize text
        # outputs the same way (except no_cast key columns, which must stay
        # bare for window-function qual pushdown).
        if (
            col not in no_cast
            and col not in no_normalize
            and expr != "NULL"
            and (typ.startswith("varchar") or typ in ("text", "char"))
        ):
            expr = f"NULLIF(RTRIM({expr}), '')"
        cast = "" if col in no_cast else f"::{typ}"
        lines.append(f"  {expr}{cast} AS {q(col)}")
    return ",\n".join(lines), unresolved


def fund_view(
    name: str,
    wrds_cols: list[tuple[str, str]],
    raw: dict[str, set[str]],
    *,
    quarterly: bool,
    popsrc: str,
) -> tuple[str, list[str]]:
    if quarterly:
        item1, item2 = "co_ifndq", "co_ifndytd"
        des, aud, mkt = "co_idesind", "co_iaudit", "co_imkt"
    else:
        item1, item2 = "co_afnd1", "co_afnd2"
        des, aud, mkt = "co_adesind", "co_aaudit", "co_amkt"

    # Primary-iid rules per the official WRDS build docs ("Generation of funda
    # File" diagrams): NA = PRIUSA else PRICAN else PRIROW; Global = PRIROW,
    # else the company's g_security iid but only when it has exactly one W
    # security, else missing.
    prim = (
        "COALESCE(co.priusa, co.prican, co.prirow)"
        if popsrc == "D"
        else "COALESCE(co.prirow, sing.iid)"
    )
    sing_join = (
        ""
        if popsrc == "D"
        else (
            "\nLEFT JOIN (SELECT gvkey, MIN(iid) AS iid FROM public.security"
            " WHERE iid LIKE '%W' GROUP BY gvkey HAVING COUNT(*) = 1) sing"
            " ON sing.gvkey=des.gvkey"
        )
    )
    # The view spine is the descriptive table (co_adesind / co_idesind): its
    # popsrc-filtered row count equals the WRDS view exactly; item tables are
    # LEFT-joined (rows without accounting items exist on WRDS with null items).
    # Quarterly tables carry fyr in their primary key, so quarterly joins must
    # include it to stay 1:1. Market rows are matched on the reporting currency
    # (co_amkt/co_imkt hold one row per trading currency). Annual calendar
    # (cfflag='C') market rows are linked through the fiscal market row's year
    # with the documented first-observation/calendar-only edge handling below;
    # quarterly rows align on datadate. Audit rows are taken at rank=1
    # (co-auditor rows would duplicate the key otherwise).
    keys = {"gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc"}
    key_cols = ["gvkey", "datadate", "indfmt", "datafmt", "consol", "popsrc"] + (["fyr"] if quarterly else [])
    c_join = "mkc.\"year\"=des.fyear" if not quarterly else "des.datadate=mkc.datadate"
    q_mkt = " AND mkf.yem=des.fyr" if quarterly else ""
    curcd = "curcdq" if quarterly else "curcd"
    order = [("des", raw[des]), ("a1", raw[item1]), ("a2", raw[item2]), ("aud", raw[aud]), ("ind", raw["co_industry"])]

    # hiid = the primary issue valid AT datadate.  The North-American build
    # selects the historical-primary package from the row's reporting
    # currency: CAD -> PRIHISTCAN and USD -> PRIHISTUSA.  Other/null currencies
    # stay NULL, and it deliberately does not fall back across packages when
    # the selected history is absent.  The Global build uses PRIHISTROW only.
    if popsrc == "D":
        hi_items = ["PRIHISTUSA", "PRIHISTCAN"]
        hi_currency = "curcdq" if quarterly else "curcd"
        hiid_expr = (
            f"CASE des.{hi_currency} WHEN 'CAD' THEN ph1.itemvalue "
            "WHEN 'USD' THEN ph0.itemvalue END"
        )
    else:
        hi_items = ["PRIHISTROW"]
        hiid_expr = "ph0.itemvalue"
    # Interval tables can share boundary dates (thrudate of one row equals the
    # next effdate), so plain range joins duplicate the spine; LATERAL with
    # ORDER BY effdate DESC LIMIT 1 picks the latest applicable row exactly once.
    ph_joins = "".join(
        f"\nLEFT JOIN LATERAL (SELECT itemvalue FROM public.sec_history"
        f" WHERE gvkey=des.gvkey AND item='{it}' AND effdate <= des.datadate"
        f" AND COALESCE(thrudate, 'infinity'::timestamp) >= des.datadate"
        f" ORDER BY effdate DESC LIMIT 1) ph{n} ON true"
        for n, it in enumerate(hi_items)
    )
    # adjex* = cumulative adjustment factor from co_adjfact (company-level,
    # interval table): fiscal snapshot at datadate; calendar snapshot at the
    # selected co_amkt calendar row's date (annual only).
    adj_joins = (
        "\nLEFT JOIN LATERAL (SELECT adjex FROM public.co_adjfact"
        " WHERE gvkey=des.gvkey AND adjex IS NOT NULL"
        " AND effdate <= des.datadate"
        " AND COALESCE(thrudate, 'infinity'::timestamp) >= des.datadate"
        " ORDER BY effdate DESC, thrudate DESC, adjex DESC LIMIT 1) aff ON true"
    )
    iss_joins = ""
    if not quarterly:
        adj_joins += (
            "\nLEFT JOIN LATERAL (SELECT adjex FROM public.co_adjfact"
            " WHERE gvkey=des.gvkey AND adjex IS NOT NULL"
            " AND mkc.datadate IS NOT NULL"
            " AND effdate <= mkc.datadate"
            " AND COALESCE(thrudate, 'infinity'::timestamp) >= mkc.datadate"
            " ORDER BY effdate DESC, thrudate DESC, adjex DESC LIMIT 1) afc ON true"
        )
    if not quarterly and popsrc == "I":
        # Global issue-level annual items (nicon, epsexcon, ...) live in the
        # security-annual tables at the primary issue, keyed issue-level:
        # indfmt='ISSUE', consol='I'. LATERAL guards against fyr multiplicity.
        for al, t in (("sad", "sec_adesind"), ("saf", "sec_afnd")):
            iss_joins += (
                f"\nLEFT JOIN LATERAL (SELECT * FROM public.{t}"
                f" WHERE gvkey=des.gvkey AND iid={prim} AND datadate=des.datadate"
                f" AND indfmt='ISSUE' AND consol='I'"
                f" AND datafmt=des.datafmt AND popsrc=des.popsrc"
                f" ORDER BY fyr DESC LIMIT 1) {al} ON true"
            )

    # Annual calendar market rows are linked through the fiscal market row's
    # year, not blindly through descriptor fyear.  A zero fiscal year is the
    # first-observation sentinel and maps to datadate's calendar year.  A
    # calendar-only row is emitted only on the same descriptor date and only
    # when no other fiscal row anchors that market year.  An empty fiscal row
    # (year IS NULL) suppresses calendar output.
    calendar_attach = (
        "(mkf.gvkey IS NOT NULL AND mkf.\"year\" IS NOT NULL) OR "
        "(mkf.gvkey IS NULL AND mkc.datadate=des.datadate "
        "AND mky.has_fiscal_year IS NULL)"
    )

    def resolve(col: str) -> str | None:
        if col in keys:
            return f"des.{q(col)}"
        for alias, cols in order:
            if col in cols:
                return f"{alias}.{q(col)}"
        if col.endswith("_c") and col[:-2] in raw[mkt]:
            return f"CASE WHEN {calendar_attach} THEN mkc.{q(col[:-2])} END"
        if col.endswith("_f") and col[:-2] in raw[mkt]:
            return f"mkf.{q(col[:-2])}"
        if col in raw[mkt]:  # unsuffixed market fields (mkvalt) come from the fiscal row
            return f"mkf.{q(col)}"
        if col == "iid":
            return prim
        if col == "hiid":
            return hiid_expr
        if col == "adjex_f" and not quarterly:
            return "CASE WHEN mkf.gvkey IS NOT NULL THEN aff.adjex END"
        if col == "adjex_c" and not quarterly:
            return (
                f"CASE WHEN mkc.gvkey IS NOT NULL AND ({calendar_attach}) "
                "THEN afc.adjex END"
            )
        if col == "adjex" and quarterly:
            return "CASE WHEN mkf.gvkey IS NOT NULL THEN aff.adjex END"
        if not quarterly and col in raw["sec_adesind"]:
            return f"sad.{q(col)}"
        if not quarterly and col in raw["sec_afnd"]:
            return f"saf.{q(col)}"
        if col in raw["company"]:
            return f"co.{q(col)}"
        if col in raw["security"]:
            return f"sec.{q(col)}"
        return None

    select, unresolved = build_select(wrds_cols, resolve)
    k = " AND ".join(f"des.{c}={{a}}.{c}" for c in key_cols)
    if not quarterly and any(
        col.endswith("_c") or col == "adjex_c" for col, _ in wrds_cols
    ):
        market_joins = f"""
LEFT JOIN public.{mkt} mkf
  ON des.gvkey=mkf.gvkey AND des.datadate=mkf.datadate AND des.popsrc=mkf.popsrc AND mkf.cfflag='F' AND mkf.curcd=des.{curcd}
LEFT JOIN public.{mkt} mkc
  ON des.gvkey=mkc.gvkey
 AND mkc."year"=(CASE WHEN mkf.gvkey IS NOT NULL AND mkf."year"=0
                      THEN EXTRACT(YEAR FROM des.datadate)
                      WHEN mkf.gvkey IS NOT NULL THEN mkf."year"
                      ELSE des.fyear END)
 AND des.popsrc=mkc.popsrc AND mkc.cfflag='C' AND mkc.curcd=des.{curcd}
LEFT JOIN LATERAL (
  SELECT 1 AS has_fiscal_year FROM public.{mkt} z
  WHERE mkf.gvkey IS NULL AND mkc.datadate=des.datadate
    AND z.gvkey=des.gvkey AND z.popsrc=des.popsrc AND z.curcd=des.{curcd}
    AND z.cfflag='F' AND z."year"=mkc."year"
  LIMIT 1
) mky ON true"""
    else:
        market_joins = f"""
LEFT JOIN public.{mkt} mkc
  ON des.gvkey=mkc.gvkey AND {c_join} AND des.popsrc=mkc.popsrc AND mkc.cfflag='C' AND mkc.curcd=des.{curcd}
LEFT JOIN public.{mkt} mkf
  ON des.gvkey=mkf.gvkey AND des.datadate=mkf.datadate AND des.popsrc=mkf.popsrc AND mkf.cfflag='F' AND mkf.curcd=des.{curcd}{q_mkt}"""

    ddl = f"""CREATE OR REPLACE VIEW comp.{name} AS
SELECT
{select}
FROM public.{des} des
LEFT JOIN public.{item1} a1 ON {k.format(a="a1")}
LEFT JOIN public.{item2} a2 ON {k.format(a="a2")}
LEFT JOIN public.{aud} aud ON {k.format(a="aud")} AND aud.rank=1
LEFT JOIN public.co_industry ind
  ON des.gvkey=ind.gvkey AND des.datadate=ind.datadate AND des.consol=ind.consol AND des.popsrc=ind.popsrc
{market_joins}
LEFT JOIN public.company co ON co.gvkey=des.gvkey{sing_join}
LEFT JOIN comp._security sec ON sec.gvkey=des.gvkey AND sec.iid={prim}{ph_joins}{adj_joins}{iss_joins}
WHERE des.popsrc='{popsrc}';
"""
    return ddl, unresolved


# WRDS package membership (company level), derived entirely from the current
# XpressFeed company/security population.  These are independent memberships:
# a foreign company with both a W issue and a North-American issue belongs to
# both packages.  NULL fic is deliberately eligible for Global membership.
POPULATION_VIEWS = """CREATE OR REPLACE VIEW comp._gl_gvkeys AS
SELECT co.gvkey::varchar AS gvkey
FROM public.company co
WHERE co.fic IS DISTINCT FROM 'USA'
  AND co.fic IS DISTINCT FROM 'CAN'
  AND EXISTS (
    SELECT 1
    FROM public.security s
    WHERE s.gvkey = co.gvkey AND s.iid LIKE '%W'
  );

CREATE OR REPLACE VIEW comp._na_gvkeys AS
SELECT co.gvkey::varchar AS gvkey
FROM public.company co
WHERE EXISTS (
  SELECT 1
  FROM public.security s
  WHERE s.gvkey = co.gvkey AND s.iid NOT LIKE '%W'
);
"""

SECURITY_ENH = """CREATE OR REPLACE VIEW comp._security AS
SELECT
  s.gvkey, s.iid, s.dldtei, s.dlrsni, s.dsci, s.epf, s.exchg, s.excntry, s.ibtic, s.secstat, s.tpci,
  COALESCE(s.cusip, i.cusip) AS cusip,
  COALESCE(s.isin,  i.isin)  AS isin,
  COALESCE(s.sedol, i.sedol) AS sedol,
  COALESCE(s.tic,   i.tic)   AS tic
FROM public.security s
LEFT JOIN (
  SELECT gvkey, iid,
    MAX(itemvalue) FILTER (WHERE item='CUSIP') AS cusip,
    MAX(itemvalue) FILTER (WHERE item='ISIN')  AS isin,
    MAX(itemvalue) FILTER (WHERE item='SEDOL') AS sedol,
    MAX(itemvalue) FILTER (WHERE item='TIC')   AS tic
  FROM public.sec_idcurrent
  GROUP BY gvkey, iid
) i ON i.gvkey=s.gvkey AND i.iid=s.iid;
"""


def secd_view(name: str, wrds_cols: list[tuple[str, str]], raw: dict[str, set[str]], *, global_: bool) -> tuple[str, list[str]]:
    iid_filter = (
        "LIKE '%W' AND b.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys)"
        if global_
        else "NOT LIKE '%W'"
    )

    if global_:
        if not wrds_cols or wrds_cols[-1][0] != "monthend":
            raise RuntimeError("comp.g_secd contract must end with monthend")
        projected_cols = wrds_cols[:-1]
    else:
        projected_cols = wrds_cols

    def resolve(col: str) -> str | None:
        if col in ("gvkey", "iid", "datadate"):
            return f"b.{q(col)}"
        if col == "trfd":
            if not global_:
                # North America has no stable WRDS fallback for securities
                # whose current raw factor history is empty.  Most such WRDS
                # rows are NULL and the remaining levels are stale, varying
                # constants that cannot be reconstructed from either feed.
                return "t.trfd"
            no_history_fallback = (
                "CASE WHEN p.gvkey IS NOT NULL AND NOT EXISTS "
                "(SELECT 1 FROM public.sec_dtrt x "
                "WHERE x.gvkey=b.gvkey AND x.iid=b.iid) THEN 1.0 END"
            )
            # Pure split-only Global rows have NULL trfd on WRDS even where an
            # older interval covers the split date.  The accepted 1.0 fallback
            # applies only to price rows for interval-less securities.
            return (
                "CASE WHEN p.gvkey IS NULL AND dv.gvkey IS NULL AND te.gvkey IS NULL THEN NULL "
                f"ELSE COALESCE(t.trfd, {no_history_fallback}) END"
            )
        if col in ("split", "splitf"):
            return f"sp.{q(col)}"
        if col in raw["sec_dprc"]:
            return f"p.{q(col)}"
        if col in raw["sec_divid"]:
            return f"dv.{q(col)}"
        if col in raw["security"]:
            return f"hdr.{q(col)}"
        if col in raw["company"]:
            return f"co.{q(col)}"
        return None

    # Cast the keys to their exact WRDS typmods, but avoid RTRIM wrappers on
    # them.  PostgreSQL can then push outer pair predicates through the UNION
    # and (for Global) below the WindowAgg into each raw primary-key index.
    select, unresolved = build_select(
        projected_cols,
        resolve,
        no_normalize=frozenset({"gvkey", "iid"}),
    )
    event_sources = ["sec_dprc", "sec_divid", "sec_dtrt"]
    if global_:
        event_sources.append("sec_split")
    spine = "\n  UNION\n".join(
        f"  SELECT gvkey, iid, datadate FROM public.{source}"
        for source in event_sources
    )
    split_join = (
        "\nLEFT JOIN public.sec_split sp"
        "\n  ON sp.gvkey=b.gvkey AND sp.iid=b.iid AND sp.datadate=b.datadate"
        if global_
        else ""
    )
    factor_event_join = (
        "\nLEFT JOIN public.sec_dtrt te"
        "\n  ON te.gvkey=b.gvkey AND te.iid=b.iid AND te.datadate=b.datadate"
        if global_
        else ""
    )
    body = f"""SELECT
{select}
-- WRDS emits event rows even when there is no price. UNION deduplicates the
-- price/dividend/factor-start date spine; Global additionally emits split-only
-- dates, while North-American secd deliberately does not.
FROM (
{spine}
) b
LEFT JOIN public.sec_dprc p
  ON p.gvkey=b.gvkey AND p.iid=b.iid AND p.datadate=b.datadate
LEFT JOIN public.sec_divid dv
  ON dv.gvkey=b.gvkey AND dv.iid=b.iid AND dv.datadate=b.datadate{split_join}{factor_event_join}
LEFT JOIN LATERAL (
  SELECT rt.trfd
  FROM public.sec_dtrt rt
  WHERE rt.gvkey=b.gvkey AND rt.iid=b.iid
    AND b.datadate >= rt.datadate
    AND b.datadate <= COALESCE(rt.thrudate, 'infinity'::timestamp)
  ORDER BY rt.datadate DESC
  LIMIT 1
) t ON true
JOIN comp._security hdr ON hdr.gvkey=b.gvkey AND hdr.iid=b.iid
LEFT JOIN public.company co ON co.gvkey=b.gvkey
WHERE hdr.iid {iid_filter}"""
    if global_:
        ddl = f"""CREATE OR REPLACE VIEW comp.{name} AS
SELECT
  x.*,
  (CASE WHEN x.datadate = MAX(x.datadate) OVER
    (PARTITION BY x.gvkey, x.iid, date_trunc('month', x.datadate::timestamp))
    THEN 1 ELSE 0 END)::float8 AS "monthend"
FROM (
{body}
) x;
"""
    else:
        ddl = f"""CREATE OR REPLACE VIEW comp.{name} AS
{body};
"""
    return ddl, unresolved


def secm_view(wrds_cols: list[tuple[str, str]], raw: dict[str, set[str]]) -> tuple[str, list[str]]:
    order = [
        ("m", raw["sec_mth"]),
        ("mp", raw["sec_mthprc"]),
        ("md", raw["sec_mthdiv"]),
        ("mt", raw["sec_mthtrt"]),
        ("ms", raw["sec_mshare"]),
        ("mst", raw["sec_mthspt"]),
    ]
    keys = ("gvkey", "iid", "datadate")
    inner_cols: dict[str, str] = {}  # col -> alias, first table in priority order wins
    for alias, cols in order:
        for col in cols:
            if col not in keys and col != "pacvertofeedpop":
                inner_cols.setdefault(col, alias)
    inner_select = ", ".join(["gvkey", "iid", "datadate"] + [f"{a}.{q(c)} AS {q(c)}" for c, a in sorted(inner_cols.items())])

    def resolve(col: str) -> str | None:
        if col in keys or col in inner_cols:
            return f"b.{q(col)}"
        if col == "cshoq":
            return "fq.cshoq"
        if col in raw["sec_spind"]:
            return f"spind.{q(col)}"
        if col in raw["security"]:
            return f"hdr.{q(col)}"
        if col in raw["company"]:
            return f"co.{q(col)}"
        return None

    select, unresolved = build_select(wrds_cols, resolve)
    ddl = f"""CREATE OR REPLACE VIEW comp.secm AS
SELECT
{select}
FROM (
  SELECT {inner_select}
  FROM public.sec_mth m
  FULL JOIN public.sec_mthprc mp USING (gvkey, iid, datadate)
  FULL JOIN public.sec_mthtrt mt USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mthdiv md USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mshare ms USING (gvkey, iid, datadate)
  LEFT JOIN public.sec_mthspt mst USING (gvkey, iid, datadate)
) b
JOIN comp._security hdr ON hdr.gvkey=b.gvkey AND hdr.iid=b.iid
LEFT JOIN public.company co ON co.gvkey=b.gvkey
LEFT JOIN public.sec_spind spind
  ON spind.gvkey=b.gvkey AND spind.iid=b.iid AND spind.datadate=b.datadate
LEFT JOIN LATERAL (
  -- co_ifndq carries fyr siblings.  Aggregate the indexed, parameterized
  -- lookup so it cannot multiply monthly rows.  WRDS attaches cshoq only to
  -- the company's current NA primary issue, not every historical issue.
  SELECT MAX(q.cshoq) AS cshoq
  FROM public.co_ifndq q
  WHERE b.iid=COALESCE(co.priusa, co.prican, co.prirow)
    AND q.gvkey=b.gvkey AND q.datadate=b.datadate
    AND q.indfmt='INDL' AND q.datafmt='STD'
    AND q.popsrc='D' AND q.consol='C'
) fq ON true
WHERE b.iid NOT LIKE '%W';
"""
    return ddl, unresolved


def passthrough_view(
    name: str, wrds_cols: list[tuple[str, str]], src: str, raw: dict[str, set[str]], where: str = "",
    src_schema: str = "public", src_name: str | None = None,
    extra: dict[str, str] | None = None,
) -> tuple[str, list[str]]:
    def resolve(col: str) -> str | None:
        if extra and col in extra:
            return extra[col]
        return f"s.{q(col)}" if col in raw[src] else None

    select, unresolved = build_select(wrds_cols, resolve)
    ddl = f"CREATE OR REPLACE VIEW comp.{name} AS\nSELECT\n{select}\nFROM {src_schema}.{src_name or src} s{where};\n"
    return ddl, unresolved


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh-contract",
        action="store_true",
        help="refresh the reviewed WRDS schema contract before generating DDL",
    )
    args = parser.parse_args()
    env = load_env()
    w = None
    if args.refresh_contract:
        w = psycopg.connect(
            host="wrds-pgdata.wharton.upenn.edu", port=9737, dbname="wrds",
            user=env["ENV_USERNAME"], password=env["ENV_PASSWORD"],
            sslmode="require", connect_timeout=30,
        )
        with w.cursor() as wc:
            wc.execute("SET statement_timeout = '300s'")
            schema_contract = write_schema_contract(wc)
        w.commit()
        print(f"schema contract refreshed: {CONTRACT_FILE}")
    else:
        schema_contract = load_schema_contract()
        print(f"schema contract loaded: {CONTRACT_FILE}")
    conn_str = env["COMPUSTAT"].replace("postgresql+psycopg2://", "postgresql://")
    c = psycopg.connect(conn_str, connect_timeout=30)
    cc = c.cursor()
    cc.execute("SET statement_timeout = '300s'")

    raw_tables = [
        "co_afnd1", "co_afnd2", "co_adesind", "co_aaudit", "co_industry", "co_amkt",
        "co_ifndq", "co_ifndytd", "co_idesind", "co_iaudit", "co_imkt",
        "co_adjfact", "sec_adesind", "sec_afnd",
        "sec_dprc", "sec_dtrt", "sec_divid", "sec_split",
        "sec_mth", "sec_mthprc", "sec_mthdiv", "sec_mthtrt", "sec_mshare", "sec_mthspt",
        "sec_spind",
        "security", "company", "sec_history", "co_hgic", "exrt_dly", "r_ex_codes",
    ]
    raw = {t: raw_columns(cc, t) for t in raw_tables}
    for t, cols in raw.items():
        if not cols:
            raise SystemExit(f"raw table missing in XpressFeed DB: {t}")

    na_company = " WHERE s.gvkey IN (SELECT gvkey FROM comp._na_gvkeys)"
    gl_company = " WHERE s.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys)"
    gl_wiid = " WHERE s.iid LIKE '%W' AND s.gvkey IN (SELECT gvkey FROM comp._gl_gvkeys)"

    # The pipeline does not consume current S&P 500 membership and the raw
    # delivery does not provide the WRDS constituent source.  Preserve the
    # WRDS-shaped schema without retaining a WRDS snapshot dependency.
    sec_flag = co_flag = {"curr_sp500_flag": "NULL"}

    views: dict[str, tuple[str, list[str]]] = {}
    views["funda"] = fund_view("funda", schema_contract["funda"], raw, quarterly=False, popsrc="D")
    views["g_funda"] = fund_view("g_funda", schema_contract["g_funda"], raw, quarterly=False, popsrc="I")
    views["fundq"] = fund_view("fundq", schema_contract["fundq"], raw, quarterly=True, popsrc="D")
    views["g_fundq"] = fund_view("g_fundq", schema_contract["g_fundq"], raw, quarterly=True, popsrc="I")
    views["secd"] = secd_view("secd", schema_contract["secd"], raw, global_=False)
    views["g_secd"] = secd_view("g_secd", schema_contract["g_secd"], raw, global_=True)
    views["secm"] = secm_view(schema_contract["secm"], raw)
    views["security"] = passthrough_view("security", schema_contract["security"], "security", raw, " WHERE s.iid NOT LIKE '%W'", src_schema="comp", src_name="_security", extra=sec_flag)
    views["g_security"] = passthrough_view("g_security", schema_contract["g_security"], "security", raw, gl_wiid, src_schema="comp", src_name="_security")
    views["sec_history"] = passthrough_view("sec_history", schema_contract["sec_history"], "sec_history", raw, " WHERE s.iid NOT LIKE '%W'")
    views["g_sec_history"] = passthrough_view("g_sec_history", schema_contract["g_sec_history"], "sec_history", raw, gl_wiid)
    views["company"] = passthrough_view("company", schema_contract["company"], "company", raw, na_company, extra=co_flag)
    views["g_company"] = passthrough_view("g_company", schema_contract["g_company"], "company", raw, gl_company)
    views["co_hgic"] = passthrough_view("co_hgic", schema_contract["co_hgic"], "co_hgic", raw, na_company)
    views["g_co_hgic"] = passthrough_view("g_co_hgic", schema_contract["g_co_hgic"], "co_hgic", raw, gl_company)
    views["exrt_dly"] = passthrough_view("exrt_dly", schema_contract["exrt_dly"], "exrt_dly", raw)
    views["r_ex_codes"] = passthrough_view("r_ex_codes", schema_contract["r_ex_codes"], "r_ex_codes", raw)

    (OUT_DIR / "_population.sql").write_text(POPULATION_VIEWS, encoding="utf-8")
    print("_population: membership views written")
    (OUT_DIR / "_security.sql").write_text(SECURITY_ENH, encoding="utf-8")
    print("_security: helper view written")

    total_unresolved = 0
    for name, (ddl, unresolved) in views.items():
        (OUT_DIR / f"{name}.sql").write_text(ddl, encoding="utf-8")
        if unresolved:
            total_unresolved += len(unresolved)
            print(f"{name}: {len(unresolved)} unresolved -> NULL: {unresolved}")
        else:
            print(f"{name}: fully resolved")
    print(f"\n{len(views)} views written to {OUT_DIR}; {total_unresolved} NULL columns total")
    if w is not None:
        w.close()
    c.close()


if __name__ == "__main__":
    main()
