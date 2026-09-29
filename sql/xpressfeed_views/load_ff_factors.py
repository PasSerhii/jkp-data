"""Atomically refresh ff.factors_monthly from WRDS.

The table is a small required input rather than an XpressFeed source. Rows are
staged and validated before a transactional replacement, so readers never
observe a partial monthly update. No refresh history is stored in the database.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime

import psycopg
from gen_views import load_env

FF_FACTORS_MONTHLY_SCHEMA: tuple[tuple[str, str, bool], ...] = (
    ("date", "date", False),
    ("mktrf", "numeric(8,6)", False),
    ("smb", "numeric(8,6)", False),
    ("hml", "numeric(8,6)", False),
    ("rf", "numeric(7,5)", False),
    ("year", "double precision", False),
    ("month", "double precision", False),
    ("umd", "numeric(8,6)", False),
    ("dateff", "date", False),
)

FF_FACTORS_MONTHLY_SELECT = """SELECT date, mktrf, smb, hml, rf, year, month, umd, dateff
FROM ff.factors_monthly
ORDER BY date"""


@dataclass(frozen=True)
class FfRefreshResult:
    """Current WRDS rows used to verify the just-refreshed RDS table."""

    source_as_of: datetime
    row_count: int
    content_sha256: str
    minimum_date: date
    maximum_date: date


def _ff_rows_content_sha256(rows: list[tuple[object, ...]]) -> str:
    """Return the canonical digest used for both loading and deployment checks."""

    payload = "".join(
        "|".join("" if value is None else str(value) for value in row) + "\n" for row in rows
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _verify_ff_factors_schema(cur: psycopg.Cursor[tuple[object, ...]]) -> None:
    """Reject a missing, non-table, or column-drifted FF target."""

    cur.execute(
        """SELECT a.attname,
                  pg_catalog.format_type(a.atttypid, a.atttypmod),
                  a.attnotnull
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
             JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid
            WHERE n.nspname='ff'
              AND c.relname='factors_monthly'
              AND c.relkind='r'
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum"""
    )
    actual = tuple(
        (str(name), str(type_name), bool(not_null)) for name, type_name, not_null in cur.fetchall()
    )
    if actual != FF_FACTORS_MONTHLY_SCHEMA:
        raise RuntimeError(
            "ff.factors_monthly schema mismatch; "
            f"expected={FF_FACTORS_MONTHLY_SCHEMA!r}, actual={actual!r}"
        )


def main() -> FfRefreshResult:
    env = load_env()
    wrds = psycopg.connect(
        host="wrds-pgdata.wharton.upenn.edu",
        port=9737,
        dbname="wrds",
        user=env["ENV_USERNAME"],
        password=env["ENV_PASSWORD"],
        sslmode="require",
        connect_timeout=30,
    )
    rds_dsn = env["COMPUSTAT"].replace("postgresql+psycopg2://", "postgresql://")
    rds = psycopg.connect(rds_dsn, connect_timeout=30)
    wc, rc = wrds.cursor(), rds.cursor()
    # Keep the timestamp and factor rows on one current WRDS snapshot.
    wc.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    wc.execute("SET LOCAL statement_timeout = '300s'")
    rc.execute("SET statement_timeout = '300s'")

    wc.execute("select current_timestamp")
    source_as_of = wc.fetchone()[0]
    wc.execute(FF_FACTORS_MONTHLY_SELECT)
    rows = wc.fetchall()
    if not rows:
        raise RuntimeError("WRDS ff.factors_monthly returned no rows")
    if len({row[0] for row in rows}) != len(rows):
        raise RuntimeError("WRDS ff.factors_monthly contains duplicate dates")
    content_hash = _ff_rows_content_sha256(rows)

    try:
        rc.execute("CREATE SCHEMA IF NOT EXISTS ff")
        rc.execute(
            """CREATE TABLE IF NOT EXISTS ff.factors_monthly (
                   date date,
                   mktrf numeric(8,6),
                   smb numeric(8,6),
                   hml numeric(8,6),
                   rf numeric(7,5),
                   year float8,
                   month float8,
                   umd numeric(8,6),
                   dateff date
               )"""
        )
        _verify_ff_factors_schema(rc)
        rc.execute(
            """CREATE TEMP TABLE ff_factors_monthly_stage (
                   date date,
                   mktrf numeric(8,6),
                   smb numeric(8,6),
                   hml numeric(8,6),
                   rf numeric(7,5),
                   year float8,
                   month float8,
                   umd numeric(8,6),
                   dateff date
               ) ON COMMIT DROP"""
        )
        rc.executemany(
            "INSERT INTO ff_factors_monthly_stage VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows,
        )
        rc.execute("SELECT count(*), count(DISTINCT date) FROM ff_factors_monthly_stage")
        staged, distinct_dates = rc.fetchone()
        if staged != len(rows) or distinct_dates != len(rows):
            raise RuntimeError("Fama-French staging validation failed")

        rc.execute("LOCK TABLE ff.factors_monthly IN ACCESS EXCLUSIVE MODE")
        rc.execute("TRUNCATE ff.factors_monthly")
        rc.execute("INSERT INTO ff.factors_monthly SELECT * FROM ff_factors_monthly_stage")
        rc.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS factors_monthly_date_idx "
            "ON ff.factors_monthly (date)"
        )
        rds.commit()
    except Exception:
        rds.rollback()
        raise
    finally:
        wrds.close()
        rds.close()

    print(
        f"ff.factors_monthly refreshed atomically: {len(rows):,} rows "
        f"through {rows[-1][0]} (WRDS as-of {source_as_of.isoformat()})"
    )
    return FfRefreshResult(
        source_as_of=source_as_of,
        row_count=len(rows),
        content_sha256=content_hash,
        minimum_date=rows[0][0],
        maximum_date=rows[-1][0],
    )


if __name__ == "__main__":
    main()
