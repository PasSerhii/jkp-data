"""Corrections for Compustat price-history defects inherited from the production SAS.

The first two only bite on long histories (almost entirely before 1999), so they are
invisible in the rolling 23-year production run but distort a full-history build:

* The firm-shares fallback mixed the company's adjustment factor with the security's,
  which made Berkshire Hathaway's pre-1998 market cap 1,500x too large (10-90% of the
  US value-weighted market from 1969 to 1998).
* Monthly returns were computed across a switch between the SECM and SECD files, whose
  total-return factors are on different bases; this put the Dec-1983 US value-weighted
  market at -20% (Ken French: -1.8%).

The third affects every run, North America and Global alike:

* Some splits are missing from Compustat's cumulative adjustment factor, so the split
  day carries the split ratio as a return (Mullen Automotive, 1-for-100 in Dec-2023:
  +9,301% for the month instead of -6%). S&P stores the factor with 8 decimals; once
  later reverse splits would push it below that, it drops the older split events and
  holds the factor flat, and some splits it never recorded. Capital IQ's split
  history in the same feed still has them, and they are put back into the daily
  return index (SECD and G_SECD).

They live in their own module so that ``aux_functions.py`` only carries one-line hooks.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .paths import DataPaths

if TYPE_CHECKING:
    import duckdb
    from ibis.backends.duckdb import Backend

SECURITY_AJEX_VIEW = "__sec_ajex_rep"
DAILY_AJEX_EOM_VIEW = "__secd_ajex_eom"

SPLIT_EVENTS_TABLE = "ciq.split_events"
SPLIT_EVENT_SCHEMA = {"gvkey": pl.String, "iid": pl.String, "exdate": pl.Date, "rate": pl.Float64}
# Smallest split ratio (or inverse ratio) that is restored: below it, the price jump of a
# split cannot be told apart from an ordinary daily move.
MIN_SPLIT_RATIO = 1.5
# Largest move of Compustat's factor around an ex-date for which the split still counts as
# missed. Factors held below the 8-decimal precision keep 1-2 significant digits and wobble
# by a few percent; a split Compustat applies moves the factor by at least MIN_SPLIT_RATIO.
MAX_FACTOR_DRIFT = 1.1
# A factor change this many calendar days from an ex-date means Compustat applied the split,
# perhaps on another day.
SPLIT_WINDOW_DAYS = 15
# Longest gap between the prices either side of an ex-date for which their ratio still shows
# the split.
SPLIT_MAX_GAP_DAYS = 31
# Furthest a factor step can lie from a missed split's ex-date and still be that split, booked
# on the wrong day (029024: factor 5x on 1 Apr 1997, price 5x on 30 Apr; seen up to 93 days).
SPLIT_BRIDGE_DAYS = 120
# The daily price files and their quote-unit column (G_SECD quotes some lines per 100 units).
_DAILY_QUOTE_UNIT = {"comp_secd": None, "comp_g_secd": "qunit"}


def _sql_path(value: Path | str) -> str:
    return str(value).replace("\\", "/").replace("'", "''")


def register_security_ajex_at_report(
    con: Backend, paths: DataPaths, *, daily: bool = False
) -> None:
    """
    Description:
        Register DuckDB views with each North American security's own cumulative
        adjustment factor, for the firm-shares fallback.
    Steps:
        1) ``__sec_ajex_rep``: SECM ``ajexm`` per {gvkey, iid, month-end} (latest
           ``datadate``, positive factors). Without a comp.secm download it is empty, so
           the fallback keeps the company factor.
        2) With ``daily=True`` also ``__secd_ajex_eom``: SECD ``ajexdi`` on each month's
           last trading day, from the ``comp_secd_current`` view already on ``con``.
    Output:
        ``__sec_ajex_rep`` {gvkey, iid, rep_eom, ajex_sec}; with ``daily=True`` also
        ``__secd_ajex_eom`` {gvkey, iid, eom, ajex_eom}.
    Note:
        When a security's own share count is missing, SECD/SECM infer it from the
        company's last report as ``csho_fund * ajex_fund / ajex(t)``. ``ajex_fund`` follows
        the company's reference share class, so for Berkshire class A it is on the class B
        basis (1,500) while the class A factor is 1: the inferred count was 1,500x too large
        until SECD began reporting class A shares in April 1998. The fallback now adjusts
        with the security's own SECM factors, ``ajexm(report month) / ajexm(month of t)``.
        On the daily file that ratio is carried to the day with SECD's own step
        ``ajexdi(month-end) / ajexdi(t)``: a factor from one file is never divided by one
        from the other, because SECM and SECD can carry a security on different scales
        (GalaGen 1998: ajexm 1.001, ajexdi 0.000001, which made its market cap $9 trillion),
        and SECD can miss the report month (Berkshire had no SECD rows in 1986).
        Checked on 1.17M fallback months against the next reported share count and on
        the US value-weighted market against Ken French (0.996-0.999 in every decade).
    """
    source = paths.raw_table_source("comp.secm")
    if isinstance(source, Path) and not source.exists():
        con.raw_sql(f"""
        CREATE OR REPLACE VIEW {SECURITY_AJEX_VIEW} AS
            SELECT NULL::VARCHAR AS gvkey, NULL::VARCHAR AS iid,
                   NULL::DATE AS rep_eom, NULL::DOUBLE AS ajex_sec
            WHERE FALSE;
        """)
    else:
        sql_source = str(source).replace("\\", "/").replace("'", "''")
        con.raw_sql(f"""
        CREATE OR REPLACE VIEW {SECURITY_AJEX_VIEW} AS
            SELECT gvkey, iid, last_day(datadate) AS rep_eom, CAST(ajexm AS DOUBLE) AS ajex_sec
            FROM read_parquet('{sql_source}')
            WHERE ajexm > 0
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY gvkey, iid, last_day(datadate) ORDER BY datadate DESC
            ) = 1;
        """)
    if daily:
        con.raw_sql(f"""
        CREATE OR REPLACE VIEW {DAILY_AJEX_EOM_VIEW} AS
            SELECT gvkey, iid, last_day(datadate) AS eom,
                   arg_max(CAST(ajexdi AS DOUBLE), datadate) AS ajex_eom
            FROM comp_secd_current
            WHERE ajexdi > 0
            GROUP BY gvkey, iid, last_day(datadate);
        """)


def correct_source_switch_returns(frame: pl.LazyFrame, paths: DataPaths, freq: str) -> pl.LazyFrame:
    """
    Description:
        Recompute monthly returns in the months where the merged Compustat monthly file
        switches between SECM and SECD rows, using SECM prices at both ends.
    Steps:
        1) Flag a monthly row as SECD-sourced when secd_data has its {gvkey, iid, eom}
           (gen_comp_msf keeps the SECD row whenever both files have the month).
        2) Attach SECM's USD and local return indexes for the same month from secm_data.
        3) Where a row's source differs from the previous row's, set ret/ret_local to the
           SECM return between the two months; null when SECM lacks either month.
    Output:
        The frame with ret/ret_local corrected at source switches; all other rows and
        daily data are unchanged.
    Note:
        SECM's total-return factor (trfm) and SECD's (trfd) are cumulative on different
        bases, and trfd is empty on SECD's first day (1983-12-30) for 97% of securities, so
        a ratio across the two files is not a return: BancWest's Dec-1983 return came out
        -57.8% while its price rose 3%. Besides Dec-1983 this covers ~7,800 later months
        in which SECD has a gap and SECM fills in. The frames written by gen_comp_msf are
        required; without them (unit tests of gen_returns_df alone) nothing changes.
    """
    secd_path = paths.interim_dir / "secd_data.parquet"
    secm_path = paths.interim_dir / "secm_data.parquet"
    if freq != "m" or not (secd_path.exists() and secm_path.exists()):
        return frame

    keys = ["gvkey", "iid", "eom"]
    by = ["gvkey", "iid"]
    secd_months = (
        pl.scan_parquet(secd_path).select(keys).unique().with_columns(_from_secd=pl.lit(True))
    )
    secm_ri = (
        pl.scan_parquet(secm_path)
        .select(*keys, "datadate", _ri_secm=pl.col("ri"), _ri_local_secm=pl.col("ri_local"))
        .sort([*keys, "datadate"])
        .unique(keys, keep="last")
        .drop("datadate")
    )
    switch = pl.col("_from_secd") != pl.col("_from_secd").shift(1).over(by)

    def secm_return(ri: str) -> pl.Expr:
        prev = pl.col(ri).shift(1).over(by)
        return pl.when(prev > 0).then(pl.col(ri) / prev - 1)

    return (
        frame.join(secd_months, on=keys, how="left")
        .join(secm_ri, on=keys, how="left")
        .with_columns(pl.col("_from_secd").fill_null(False))
        .sort([*by, "datadate"])
        .with_columns(
            ret=pl.when(switch).then(secm_return("_ri_secm")).otherwise(pl.col("ret")),
            ret_local=pl.when(switch)
            .then(secm_return("_ri_local_secm"))
            .otherwise(pl.col("ret_local")),
        )
        .drop(["_from_secd", "_ri_secm", "_ri_local_secm"])
    )


def build_split_events_query(raw_schema: str) -> str:
    """
    Description:
        PostgreSQL query for Capital IQ's split history, keyed by Compustat security.
    Steps:
        1) Map each Capital IQ trading item to its Compustat {gvkey, iid} through
           ``ciqgvkeyiid``, within the dates the mapping is valid.
        2) Keep the splits Capital IQ applied (``appliedflag = 1``) with a positive rate:
           stock splits, consolidations, subdivisions and stock dividends alike.
        3) Combine events on the same day by multiplying their rates (odd-lot schemes
           such as 1-for-5,000 then 5,000-for-1 net out to 1).
    Output:
        SQL returning {gvkey, iid, exdate, rate}; ``rate`` is new shares per old share,
        as in Compustat's split factor (0.01 for a 1-for-100 reverse split).
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw_schema):
        raise ValueError(f"Invalid raw PostgreSQL schema: {raw_schema!r}")
    return f"""
SELECT g.gvkey::varchar AS gvkey, g.iid::varchar AS iid, s.exdate::date AS exdate,
       exp(sum(ln(s.rate::double precision))) AS rate
FROM "{raw_schema}"."ciqgvkeyiid" AS g
JOIN "{raw_schema}"."ciqsplit" AS s
  ON s.tradingitemid = g.objectid
 AND s.exdate >= coalesce(g.symbolstartdate, DATE '1900-01-01')
 AND s.exdate <= coalesce(g.symbolenddate, DATE '2900-01-01')
WHERE s.appliedflag = 1 AND s.rate > 0
GROUP BY 1, 2, 3
""".strip()


def download_split_events_attached(
    duckdb_conn: duckdb.DuckDBPyConnection,
    db_alias: str,
    paths: DataPaths,
    raw_schema: str,
    source_label: str,
) -> None:
    """
    Description:
        Download Capital IQ's split history for ``restore_missed_splits``.
    Steps:
        1) Check that the source has the Capital IQ tables (the XpressFeed RDS does,
           WRDS's ``comp`` schema does not).
        2) If so, run ``build_split_events_query``; otherwise write an empty table, so
           Compustat's split factors are used as delivered.
    Output:
        raw_tables/ciq_split_events.parquet {gvkey, iid, exdate, rate}.
    """
    # aux_functions imports this module, so its helpers are imported when first needed
    from .aux_functions import (
        _is_timeout_error,
        _raise_redacted_source_error,
        _record_single_table_download,
        _record_table_download_failure,
        _report_progress,
        _sql_literal,
        download_postgres_query_attached,
    )

    query = build_split_events_query(raw_schema)
    filename = str(paths.raw_table_source(SPLIT_EVENTS_TABLE))
    started_at = datetime.now(UTC).isoformat()
    started = time.monotonic()
    _report_progress(f"Downloading {source_label} Capital IQ split history")
    check = (
        f"SELECT to_regclass('{raw_schema}.ciqsplit') IS NOT NULL "
        f"AND to_regclass('{raw_schema}.ciqgvkeyiid') IS NOT NULL AS available"
    )
    try:
        available = duckdb_conn.execute(
            f"SELECT * FROM postgres_query({_sql_literal(db_alias)}, {_sql_literal(check)})"
        ).fetchone()[0]
        if available:
            download_postgres_query_attached(duckdb_conn, db_alias, query, filename)
        else:
            _report_progress(
                f"{source_label} has no Capital IQ split history; "
                "Compustat split factors are used as delivered"
            )
            pl.DataFrame(schema=SPLIT_EVENT_SCHEMA).write_parquet(filename)
    except Exception as exc:
        _record_table_download_failure(
            SPLIT_EVENTS_TABLE,
            started_at_utc=started_at,
            started_monotonic=started,
            error=exc,
            timeouts=int(_is_timeout_error(exc)),
        )
        _raise_redacted_source_error(source_label, "split-history download", exc)
    _record_single_table_download(
        duckdb_conn,
        SPLIT_EVENTS_TABLE,
        filename,
        started_at_utc=started_at,
        started_monotonic=started,
    )


def restore_missed_splits(
    con: Backend, paths: DataPaths, view_name: str, source_path: Path | str
) -> None:
    """
    Description:
        Put the splits missing from a daily Compustat price file's adjustment factor back
        into its return index, by redefining the ``{view_name}_current`` view that reads it.
    Steps:
        1) Find the missed splits of the file (``_missed_split_table``).
        2) Divide the total-return factor ``trfd`` of every row before a missed split by
           the split's restored step; rows before several missed splits divide by the product.
    Output:
        ``{view_name}_current`` with ``trfd`` corrected (as DOUBLE, a missing factor counted
        as 1, as in the return index) on the rows before a missed split; all other columns
        and rows unchanged. Without a split-history download nothing changes.
    Note:
        ``view_name`` is ``comp_secd`` or ``comp_g_secd``, the views
        ``_register_historical_exchange_view`` registers, which read ``{view_name}_current``.
        The return index is ``prccd / ajexdi * coalesce(trfd, 1)``, and ``trfd`` is used
        nowhere else, so the correction reaches the returns and nothing else: ``ajexdi``, and
        with it share counts, the firm-shares fallback, adjusted prices and ``adjfct``, stay
        exactly as Compustat delivers them. A missed split returns the split ratio on its
        ex-date (Mullen Automotive's 1-for-100 on 21 Dec 2023: +17,700% that day); within
        each stretch between missed splits the index is scaled by a constant, so that one
        return is the only one that changes. Where Compustat books the split's factor step on
        another day, the returns of those two days change and no other return.
        The monthly file (SECM) is not corrected: its monthly price ratio cannot tell a split
        from a month's move as well, and FactSet confirmed only 82-84% of the splits it would
        restore where the daily file has no matching jump or no rows. SECM months enter the
        panel only before 1984, in SECD gaps and at SECM/SECD switches.
    """
    if _split_events(paths) is not None:
        restored = _restored_source(con, paths, view_name, source_path)
        con.raw_sql(f"CREATE OR REPLACE VIEW {view_name}_current AS {restored};")


def _split_events(paths: DataPaths) -> Path | None:
    events = paths.raw_table_source(SPLIT_EVENTS_TABLE)
    return events if isinstance(events, Path) and events.is_file() else None


def _restored_source(
    con: Backend, paths: DataPaths, view_name: str, source_path: Path | str
) -> str:
    """SELECT over a daily price file with its missed splits restored in ``trfd``."""
    events = _split_events(paths)
    assert events is not None
    source = _sql_path(source_path)
    table = _missed_split_table(con, events, view_name, source)
    return f"""
        SELECT s.* REPLACE (
            CASE WHEN m.multiplier IS NULL THEN CAST(s.trfd AS DOUBLE)
                 ELSE COALESCE(CAST(s.trfd AS DOUBLE), 1) / m.multiplier
            END AS trfd
        )
        FROM read_parquet('{source}') AS s
        LEFT JOIN {table} AS m
          ON s.gvkey = m.gvkey AND s.iid = m.iid
         AND s.datadate >= m.valid_from AND s.datadate < m.valid_to
    """


def _missed_split_table(con: Backend, events: Path, view_name: str, source: str) -> str:
    """
    Description:
        Find the splits a daily Compustat price file's adjustment factor misses, and the
        return index multiplier each stretch between them needs.
    Steps:
        1) Capital IQ splits with a ratio of at least MIN_SPLIT_RATIO either way.
        2) The file's own rows around each ex-date: the last before it, the first on or
           after it, and all rows within SPLIT_WINDOW_DAYS of it.
        3) A split is missed when (a) ``ajexdi`` moves by less than MAX_FACTOR_DRIFT over all
           those rows, (b) the return index ``prccd / quote unit / ajexdi * trfd`` jumps
           between the two rows either side of the ex-date by the split's log ratio, give or
           take half of it, and (c) those two rows are at most SPLIT_MAX_GAP_DAYS apart.
        4) The restored step is ``rate * ajexdi after / ajexdi before``: the split, net of the
           factor's own drift across the ex-date (none when it is flat).
        5) Compustat may book a missed split's factor step on another day, up to
           SPLIT_BRIDGE_DAYS away. That step is moved to the ex-date ("bridged") when the
           split shows in the price itself (its jump across the ex-date is within half the
           log ratio, not only in the return index), and on the other day, between two rows
           at most SPLIT_MAX_GAP_DAYS apart, the factor moves by the split within
           MAX_FACTOR_DRIFT, while the price moves by less than half the split's log ratio
           and less than MIN_SPLIT_RATIO, and ``trfd`` by less than MAX_FACTOR_DRIFT. No other
           Capital IQ split may be within SPLIT_WINDOW_DAYS of that day, nor one the step fits
           that is at least as close to it, and the split and the day must pair only with each
           other; otherwise the split is restored as in 4). That day gets the factor's own step
           as a counter-step, and the ex-date the factor's drift across it divided by that
           step, instead of the restored step.
        6) For each security, stretches end at each missed ex-date and counter-step day; the
           rows of a stretch need the product of the steps from its end onwards.
    Output:
        DuckDB table ``__missed_splits_{view_name}`` {gvkey, iid, valid_from, valid_to,
        multiplier}: rows with valid_from <= datadate < valid_to get their return index
        divided by ``multiplier``. The first stretch starts at 0001-01-01: a NULL start
        tested with OR made DuckDB join the whole price file by nested loop (75x slower).
    Note:
        Checked on the full XpressFeed daily history on 7 October 2026. Of 83,408 Capital
        IQ splits of at least 1.5x, Compustat applies 64,501 on the ex-date; 1,375 have a
        flat factor and pass (b) and (c) (744 North American, 631 Global, 357 with factors
        below 1e-4), and 62 more have a factor that drifts by 1-7% instead (Conduit
        Pharmaceuticals, 27 Jan 2025: 1.32e-6 to 1.33e-6 across a 1-for-100). FactSet's
        split-adjusted prices agree with the restored return for 1,035 of the 1,071 of
        these it prices; the other 36 are splits FactSet's history lacks too (Tyler
        Technologies' 3-for-1 of 15 May 1990, Bryn Mawr Bank's 3-for-1 of 2 Jan 1987).
        Condition (b) is on the return index, not the price: with the price alone, splits
        that Compustat's prices do not show would also count, and so would those Compustat
        books in ``trfd`` (Warner Bros. Discovery Series A, 7 Aug 2014: one Series C share
        per share, a halved price and a doubled trfd). The window in (a) skips splits whose
        factor change Compustat dates a few days away from Capital IQ's ex-date.
        Step 5 handles a factor change dated further away. Restoring such a split alone would
        count it twice: the factor's own step would stay in the index as a jump of the split
        ratio (029024, 1-for-5: April 1997 at -82% instead of -12%). Moving Compustat's own
        step to the ex-date changes only two daily returns: the other day's loses the
        factor's jump, and the ex-date's takes Compustat's step (net of any wobble across the
        ex-date) instead of Capital IQ's ratio. Where the two differ, Compustat's step is the
        one that keeps the return over both days as delivered (103618, 2-for-1 in December
        1996, a step of 2.067: FactSet confirms the return over the two months).
        Checked on 14 October 2026 around all 1,422 restored splits in the daily files: 12 are
        bridged, 18 daily returns change and nothing else. FactSet's split-adjusted prices match the return over
        the bridged stretch for 10; FactSet lacks one of the splits (201438, Jordan, 1997), and
        the last differs on a day the change does not touch. A split seen only in ``trfd``
        (Becamex IJC, 17 Jan 2017, flat price) is not bridged.
    """
    table = f"__missed_splits_{view_name}"
    quote_unit = _DAILY_QUOTE_UNIT[view_name]
    qunit = f"CAST(s.{quote_unit} AS DOUBLE)" if quote_unit else "1"
    reach = SPLIT_WINDOW_DAYS + SPLIT_MAX_GAP_DAYS
    bridge_reach = SPLIT_BRIDGE_DAYS + SPLIT_MAX_GAP_DAYS
    con.raw_sql(f"""
    CREATE TABLE IF NOT EXISTS {table} AS
    WITH all_events AS (
        SELECT gvkey, iid, CAST(exdate AS DATE) AS exdate, CAST(rate AS DOUBLE) AS rate
        FROM read_parquet('{_sql_path(events)}')
        WHERE rate > 0
    ),
    events AS (
        SELECT gvkey, iid, exdate, rate,
               exdate - {SPLIT_WINDOW_DAYS} AS window_start,
               exdate + {SPLIT_WINDOW_DAYS} AS window_end
        FROM all_events
        WHERE abs(ln(rate)) >= ln({MIN_SPLIT_RATIO})
    ),
    prices AS (
        SELECT s.gvkey, s.iid, CAST(s.datadate AS DATE) AS datadate,
               CAST(s.ajexdi AS DOUBLE) AS factor,
               CAST(s.prccd AS DOUBLE) / {qunit} AS price,
               CAST(s.prccd AS DOUBLE) / {qunit} / CAST(s.ajexdi AS DOUBLE)
                   * COALESCE(CAST(s.trfd AS DOUBLE), 1) AS ri
        FROM read_parquet('{source}') AS s
        WHERE s.prccd > 0 AND s.ajexdi > 0
          AND COALESCE(s.trfd, 1) > 0
          AND {qunit} > 0
          AND EXISTS (
              SELECT 1 FROM events AS e
              WHERE e.gvkey = s.gvkey AND e.iid = s.iid
                AND CAST(s.datadate AS DATE) BETWEEN e.exdate - {reach} AND e.exdate + {reach}
          )
    ),
    around AS (
        SELECT e.gvkey, e.iid, e.exdate, e.rate,
               b.datadate AS before_date, b.factor AS before_factor, b.ri AS before_ri,
               b.price AS before_price,
               a.datadate AS after_date, a.factor AS after_factor, a.ri AS after_ri,
               a.price AS after_price
        FROM events AS e
        ASOF JOIN prices AS b
            ON e.gvkey = b.gvkey AND e.iid = b.iid AND e.exdate > b.datadate
        ASOF JOIN prices AS a
            ON e.gvkey = a.gvkey AND e.iid = a.iid AND e.exdate <= a.datadate
    ),
    window_factors AS (
        SELECT e.gvkey, e.iid, e.exdate,
               min(p.factor) AS window_min, max(p.factor) AS window_max
        FROM events AS e
        JOIN prices AS p
          ON e.gvkey = p.gvkey AND e.iid = p.iid
         AND p.datadate BETWEEN e.window_start AND e.window_end
        GROUP BY ALL
    ),
    missed AS (
        SELECT gvkey, iid, exdate, rate, rate * after_factor / before_factor AS step,
               abs(ln(after_price / before_price) + ln(rate)) < abs(ln(rate)) / 2 AS price_split
        FROM around
        LEFT JOIN window_factors USING (gvkey, iid, exdate)
        WHERE greatest(before_factor, after_factor, window_max)
                / least(before_factor, after_factor, window_min) < {MAX_FACTOR_DRIFT}
          AND after_date - before_date <= {SPLIT_MAX_GAP_DAYS}
          AND abs(ln(after_ri / before_ri) + ln(rate)) < abs(ln(rate)) / 2
    ),
    near_missed AS (
        SELECT s.gvkey, s.iid, CAST(s.datadate AS DATE) AS datadate,
               CAST(s.ajexdi AS DOUBLE) AS factor,
               CAST(s.prccd AS DOUBLE) / {qunit} AS price,
               COALESCE(CAST(s.trfd AS DOUBLE), 1) AS trfd
        FROM read_parquet('{source}') AS s
        SEMI JOIN (SELECT DISTINCT gvkey, iid FROM missed WHERE price_split) AS k
          ON s.gvkey = k.gvkey AND s.iid = k.iid
        WHERE s.prccd > 0 AND s.ajexdi > 0
          AND COALESCE(s.trfd, 1) > 0
          AND {qunit} > 0
          AND EXISTS (
              SELECT 1 FROM missed AS m
              WHERE m.price_split AND m.gvkey = s.gvkey AND m.iid = s.iid
                AND CAST(s.datadate AS DATE)
                    BETWEEN m.exdate - {bridge_reach} AND m.exdate + {bridge_reach}
          )
    ),
    factor_steps AS (
        SELECT gvkey, iid, datadate, factor / prev_factor AS step,
               price / prev_price AS price_move, trfd / prev_trfd AS trfd_move
        FROM (
            SELECT *,
                   lag(datadate) OVER w AS prev_date,
                   lag(factor) OVER w AS prev_factor,
                   lag(price) OVER w AS prev_price,
                   lag(trfd) OVER w AS prev_trfd
            FROM near_missed
            WINDOW w AS (PARTITION BY gvkey, iid ORDER BY datadate)
        )
        WHERE factor <> prev_factor AND datadate - prev_date <= {SPLIT_MAX_GAP_DAYS}
    ),
    candidates AS (
        SELECT m.gvkey, m.iid, m.exdate, f.datadate AS factor_day, f.step
        FROM missed AS m
        JOIN factor_steps AS f
          ON f.gvkey = m.gvkey AND f.iid = m.iid
         AND f.datadate BETWEEN m.exdate - {SPLIT_BRIDGE_DAYS} AND m.exdate + {SPLIT_BRIDGE_DAYS}
        WHERE m.price_split
          AND abs(ln(f.step * m.rate)) < ln({MAX_FACTOR_DRIFT})
          AND abs(ln(f.price_move)) < least(abs(ln(m.rate)) / 2, ln({MIN_SPLIT_RATIO}))
          AND abs(ln(f.trfd_move)) < ln({MAX_FACTOR_DRIFT})
          AND NOT EXISTS (
              SELECT 1 FROM all_events AS e
              WHERE e.gvkey = f.gvkey AND e.iid = f.iid AND e.exdate <> m.exdate
                AND (
                    e.exdate BETWEEN f.datadate - {SPLIT_WINDOW_DAYS}
                                 AND f.datadate + {SPLIT_WINDOW_DAYS}
                    OR (abs(ln(f.step * e.rate)) < ln({MAX_FACTOR_DRIFT})
                        AND abs(e.exdate - f.datadate) <= abs(m.exdate - f.datadate))
                )
          )
    ),
    bridges AS (
        SELECT * FROM candidates
        QUALIFY count(*) OVER (PARTITION BY gvkey, iid, exdate) = 1
            AND count(*) OVER (PARTITION BY gvkey, iid, factor_day) = 1
    ),
    steps AS (
        SELECT m.gvkey, m.iid, m.exdate, COALESCE(m.step / m.rate / b.step, m.step) AS step
        FROM missed AS m
        LEFT JOIN bridges AS b USING (gvkey, iid, exdate)
        UNION ALL
        SELECT gvkey, iid, factor_day AS exdate, step FROM bridges
    )
    SELECT gvkey, iid,
           lag(exdate, 1, DATE '0001-01-01') OVER (PARTITION BY gvkey, iid ORDER BY exdate)
               AS valid_from,
           exdate AS valid_to,
           exp(sum(ln(step)) OVER (
               PARTITION BY gvkey, iid ORDER BY exdate DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )) AS multiplier
    FROM steps;
    """)
    return table
