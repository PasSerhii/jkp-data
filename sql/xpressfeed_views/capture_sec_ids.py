"""Accumulate point-in-time security identifiers from the XpressFeed feed.

Run once per monthly build, before the pipeline downloads its raw tables:

    uv run --with "psycopg[binary]" python sql/xpressfeed_views/capture_sec_ids.py

On first run the table is seeded from the `comp.sec_idhist` snapshot copied from
WRDS, which supplies the historical backfill. Every run then diffs
`public.sec_idcurrent` against the open intervals and records changes, so the
history maintains itself from the native feed with no further WRDS dependency.

Idempotent: re-running on the same day is a no-op, because a value that already
matches its open interval produces no change.

Known limitation: capture granularity is the run cadence. A value that changes
mid-month is detected at the next capture, so `efffrom` is the capture date
rather than the true change date, and the intervening days keep the previous
value. That is the conservative direction — it reports what was known, never a
future identifier — but it is not exact, and the seeded `wrds` rows remain the
only source of exact intervals before the first capture.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

import psycopg
from gen_views import load_env

ITEMS = ("CUSIP", "ISIN", "SEDOL", "TIC")
OPEN_SENTINEL = date(2900, 1, 1)


def _seed_from_wrds_snapshot(cur: psycopg.Cursor) -> int:
    """Load the historical backfill once; later runs leave these rows alone."""
    cur.execute("SELECT count(*) FROM comp.sec_id_history WHERE source = 'wrds'")
    if cur.fetchone()[0]:
        return 0
    cur.execute(
        """INSERT INTO comp.sec_id_history
               (gvkey, iid, item, itemvalue, efffrom, effthru, source)
           SELECT gvkey, iid, item, itemvalue, efffrom,
                  COALESCE(effthru, DATE '2900-01-01'), 'wrds'
             FROM comp.sec_idhist
            WHERE item = ANY(%s)
              AND itemvalue IS NOT NULL
              AND btrim(itemvalue) <> ''
              AND efffrom IS NOT NULL
              AND COALESCE(effthru, DATE '2900-01-01') >= efffrom""",
        (list(ITEMS),),
    )
    return cur.rowcount


def _capture(cur: psycopg.Cursor, captured_on: date) -> tuple[int, int]:
    """Close intervals whose value changed and open one for the new value."""
    cur.execute(
        """CREATE TEMP TABLE current_ids ON COMMIT DROP AS
           SELECT btrim(gvkey) AS gvkey, btrim(iid) AS iid, item,
                  btrim(itemvalue) AS itemvalue
             FROM public.sec_idcurrent
            WHERE item = ANY(%s)
              AND itemvalue IS NOT NULL
              AND btrim(itemvalue) <> ''""",
        (list(ITEMS),),
    )
    cur.execute("CREATE INDEX ON current_ids (gvkey, iid, item)")

    # An issue-item is "changed" when the feed disagrees with its open interval,
    # and "new" when it has no open interval at all. Both need a fresh interval;
    # only the former needs the previous one closed.
    cur.execute(
        """CREATE TEMP TABLE changed ON COMMIT DROP AS
           SELECT c.gvkey, c.iid, c.item, c.itemvalue
             FROM current_ids c
             LEFT JOIN comp.sec_id_history h
                    ON h.gvkey = c.gvkey AND h.iid = c.iid AND h.item = c.item
                   AND h.effthru = DATE '2900-01-01'
            WHERE h.gvkey IS NULL OR h.itemvalue IS DISTINCT FROM c.itemvalue"""
    )

    # Guard against a capture dated on or before an interval's start, which
    # would otherwise create effthru < efffrom and trip the range check.
    cur.execute(
        """UPDATE comp.sec_id_history h
              SET effthru = GREATEST(%s::date, h.efffrom)
             FROM changed ch
            WHERE h.gvkey = ch.gvkey AND h.iid = ch.iid AND h.item = ch.item
              AND h.effthru = DATE '2900-01-01'""",
        (captured_on - timedelta(days=1),),
    )
    closed = cur.rowcount

    cur.execute(
        """INSERT INTO comp.sec_id_history
               (gvkey, iid, item, itemvalue, efffrom, effthru, source)
           SELECT gvkey, iid, item, itemvalue, %s, DATE '2900-01-01', 'feed'
             FROM changed""",
        (captured_on,),
    )
    return closed, cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--captured-on",
        type=date.fromisoformat,
        default=date.today(),
        help="Capture date recorded as efffrom for new intervals (default: today).",
    )
    args = parser.parse_args()

    dsn = load_env()["COMPUSTAT"].replace("postgresql+psycopg2://", "postgresql://")
    with psycopg.connect(dsn, connect_timeout=30) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = '900s'")
        seeded = _seed_from_wrds_snapshot(cur)
        closed, opened = _capture(cur, args.captured_on)
        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE source = 'feed') FROM comp.sec_id_history"
        )
        total, from_feed = cur.fetchone()
        conn.commit()

    if seeded:
        print(f"seeded {seeded:,} historical rows from the WRDS sec_idhist snapshot")
    print(
        f"capture {args.captured_on}: closed {closed:,} intervals, opened {opened:,}; "
        f"table holds {total:,} rows ({from_feed:,} accumulated from the feed)"
    )


if __name__ == "__main__":
    main()
