"""Reconcile dated WRDS identifier history, then capture feed-only identifiers.

Run before each monthly download. Requires WRDS access: the native feed has no
identifier effective dates. Failure leaves the existing history intact and
stops the build rather than treating capture dates as exact vendor dates.
Only comp.sec_id_history is updated; no Fama-French tables are read or refreshed.

Use --dry-run to validate the staged result without publishing it. Existing
ENV_USERNAME/ENV_PASSWORD credentials are supported, as are jkp's standard
WRDS credentials. The old comp.sec_idhist seed is no longer used.
"""

from __future__ import annotations

import argparse
from datetime import date

import polars as pl
import psycopg
from gen_views import load_env
from psycopg import sql

from jkp.data.identifier_history import HISTORY_SCHEMA, ITEMS, reconcile_identifier_history
from jkp.data.wrds_connection import gen_wrds_connection_info
from jkp.data.wrds_credentials import get_wrds_credentials


def read_authoritative_history(env: dict[str, str]) -> pl.DataFrame:
    """Read one complete, current WRDS snapshot before opening a write transaction."""
    if env.get("ENV_USERNAME") and env.get("ENV_PASSWORD"):
        username, password = env["ENV_USERNAME"], env["ENV_PASSWORD"]
    else:
        credentials = get_wrds_credentials()
        username, password = credentials.username, credentials.password
    dsn = gen_wrds_connection_info(username, password)
    with psycopg.connect(dsn, connect_timeout=30) as conn, conn.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cur.execute("SET LOCAL statement_timeout = '300s'")
        cur.execute(
            """SELECT gvkey, iid, item, itemvalue, efffrom, effthru
                 FROM comp.sec_idhist WHERE item = ANY(%s)""",
            (list(ITEMS),),
        )
        schema = {k: v for k, v in HISTORY_SCHEMA.items() if k != "source"}
        return pl.DataFrame(cur.fetchall(), schema=schema, orient="row")


def stage_history(cur: psycopg.Cursor, history: pl.DataFrame) -> None:
    """Check database types/constraints before changing any persistent rows."""
    cur.execute(
        """CREATE TEMP TABLE identifier_history_stage
           (LIKE comp.sec_id_history INCLUDING ALL) ON COMMIT DROP"""
    )
    with cur.copy(
        "COPY identifier_history_stage (gvkey,iid,item,itemvalue,efffrom,effthru,source) FROM STDIN"
    ) as copy:
        for row in history.select(list(HISTORY_SCHEMA)).iter_rows():
            copy.write_row(row)
    cur.execute("ANALYZE identifier_history_stage")


def publish_history(cur: psycopg.Cursor) -> tuple[int, int]:
    """Apply only changed rows; the caller owns the lock and transaction."""
    columns = sql.SQL(", ").join(map(sql.Identifier, HISTORY_SCHEMA))
    same_row = sql.SQL(" AND ").join(
        sql.SQL("h.{0} = s.{0}").format(sql.Identifier(c)) for c in HISTORY_SCHEMA
    )
    cur.execute(
        sql.SQL("""DELETE FROM comp.sec_id_history h
                   WHERE NOT EXISTS (SELECT 1 FROM identifier_history_stage s WHERE {})""").format(
            same_row
        )
    )
    removed = cur.rowcount
    cur.execute(
        sql.SQL("""INSERT INTO comp.sec_id_history ({0})
                   SELECT {0} FROM identifier_history_stage
                   EXCEPT SELECT {0} FROM comp.sec_id_history""").format(columns)
    )
    inserted = cur.rowcount
    cur.execute(
        """SELECT (SELECT count(*) FROM comp.sec_id_history)
                  = (SELECT count(*) FROM identifier_history_stage)"""
    )
    if not cur.fetchone()[0]:
        raise RuntimeError("Identifier history row-count verification failed")
    return removed, inserted


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--captured-on", type=date.fromisoformat, default=date.today())
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = load_env()
    try:
        authoritative = read_authoritative_history(env)
        dsn = env["COMPUSTAT"].replace("postgresql+psycopg2://", "postgresql://")
        with psycopg.connect(dsn, connect_timeout=30) as conn, conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '900s'")
            # Serialize captures/reconciliations while retaining concurrent readers.
            cur.execute("LOCK TABLE comp.sec_id_history IN SHARE ROW EXCLUSIVE MODE")
            cur.execute(
                "SELECT gvkey,iid,item,itemvalue,efffrom,effthru,source FROM comp.sec_id_history"
            )
            previous = pl.DataFrame(cur.fetchall(), schema=HISTORY_SCHEMA, orient="row")
            cur.execute(
                """SELECT gvkey,iid,item,itemvalue FROM public.sec_idcurrent
                   WHERE item = ANY(%s)""",
                (list(ITEMS),),
            )
            current = pl.DataFrame(
                cur.fetchall(),
                schema=dict.fromkeys(list(HISTORY_SCHEMA)[:4], pl.String),
                orient="row",
            )
            history = reconcile_identifier_history(
                authoritative, previous, current, args.captured_on
            )
            stage_history(cur, history)
            if args.dry_run:
                conn.rollback()
                print(f"Dry run: validated {history.height:,} reconciled identifier intervals")
            else:
                removed, inserted = publish_history(cur)
                conn.commit()
                print(
                    f"Identifiers reconciled: replaced {removed:,} rows, inserted {inserted:,} rows"
                )
    except psycopg.Error:
        raise SystemExit(
            "Identifier reconciliation failed; no changes committed; credentials omitted"
        ) from None


if __name__ == "__main__":
    main()
