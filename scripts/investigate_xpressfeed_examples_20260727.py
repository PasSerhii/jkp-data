"""Collect read-only XpressFeed evidence for the 2026-07-27 rerun analysis.

The comparison itself is against the two approved Research tables.  These
queries are only for tracing representative causes in the upstream source.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import psycopg


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from jkp.data.database_sources import get_xpressfeed_connection_info  # noqa: E402


OUTPUT = ROOT / "report_codex_27-07-2026_run_analysis_artifacts" / "tables"

# One-sided universe examples plus securities used in score/root-cause traces.
UNIVERSE_GVKEYS = (
    "032949", "075930", "175621", "363680", "204047", "014155",
    "350876", "238736", "363353", "372634", "372649", "372667",
    "372687", "005073", "008549",
)
ACCOUNTING_GVKEYS = (
    "226824",  # Plenum
    "282615",  # Ethero
    "288567",  # Halo Minerals
    "321134",  # NewCelX
)
DOMESTIC_ACCOUNTING_GVKEYS = ("051423", "051863")  # BGIN, GrowHub
DOMESTIC_UNIVERSE_GVKEYS = (
    "005073", "008549", "032949", "075930", "175621", "363680"
)
GLOBAL_TRADING_KEYS = (
    ("014155", "01W"), ("204047", "01W"), ("238736", "01W"),
    ("350876", "01W"), ("363353", "01W"), ("372634", "01W"),
    ("372649", "01W"), ("372667", "01W"), ("372687", "01W"),
)
DOMESTIC_TRADING_KEYS = (
    ("005073", "19C"), ("008549", "01C"), ("032949", "01C"),
    ("075930", "01C"), ("175621", "01C"), ("363680", "01C"),
)


def read_frame(conn: psycopg.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        names = [item.name for item in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=names)


def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    conn = psycopg.connect(get_xpressfeed_connection_info(dotenv_path=ROOT / ".env"))
    try:
        placeholders = ",".join(["%s"] * len(UNIVERSE_GVKEYS))
        universe = read_frame(
            conn,
            f"""
            select s.gvkey, s.iid, s.excntry, s.exchg, s.secstat, s.dldtei,
                   s.cusip, s.isin, s.sedol
            from comp.g_security s
            where s.gvkey in ({placeholders})
            order by s.gvkey, s.iid
            """,
            UNIVERSE_GVKEYS,
        )
        universe.to_csv(OUTPUT / "source_universe_examples.csv", index=False)

        domestic_universe_placeholders = ",".join(
            ["%s"] * len(DOMESTIC_UNIVERSE_GVKEYS)
        )
        domestic_universe = read_frame(
            conn,
            f"""
            select gvkey, iid, tic, excntry, exchg, secstat, dldtei, tpci,
                   cusip, isin, sedol
            from comp.security
            where gvkey in ({domestic_universe_placeholders})
            order by gvkey, iid
            """,
            DOMESTIC_UNIVERSE_GVKEYS,
        )
        domestic_universe.to_csv(
            OUTPUT / "source_universe_domestic_examples.csv", index=False
        )

        trading_counts = []
        for source_table, keys in (
            ("g_secd", GLOBAL_TRADING_KEYS), ("secd", DOMESTIC_TRADING_KEYS)
        ):
            for gvkey, iid in keys:
                count = read_frame(
                    conn,
                    f"""
                    select %s as source_table, %s as gvkey, %s as iid,
                           count(*) as source_trading_rows_may_june,
                           min(datadate) as first_source_date,
                           max(datadate) as last_source_date
                    from comp.{source_table}
                    where gvkey=%s and iid=%s
                      and datadate between date '2026-05-01' and date '2026-06-30'
                    """,
                    (source_table, gvkey, iid, gvkey, iid),
                )
                trading_counts.append(count)
        pd.concat(trading_counts, ignore_index=True).to_csv(
            OUTPUT / "source_universe_trading_counts.csv", index=False
        )

        history = read_frame(
            conn,
            f"""
            select gvkey, iid, item, itemvalue, effdate, thrudate
            from comp.sec_history
            where gvkey in ({placeholders})
              and item in ('EXCHG', 'EXCNTRY', 'PRIHIST', 'SECSTAT', 'TPCI')
            order by gvkey, iid, item, effdate
            """,
            UNIVERSE_GVKEYS,
        )
        history.to_csv(OUTPUT / "source_universe_security_history.csv", index=False)

        # ELCID Investments: the largest return-horizon outlier.
        elcid_security = read_frame(
            conn,
            """
            select gvkey, iid, excntry, exchg, secstat, dldtei, cusip, isin, sedol
            from comp.g_security where gvkey='341068' order by iid
            """,
        )
        elcid_security.to_csv(OUTPUT / "source_elcid_security.csv", index=False)

        elcid = read_frame(
            conn,
            """
            select gvkey, iid, datadate, prccd, ajexdi, qunit, trfd,
                   prccd/nullif(qunit,0)/nullif(ajexdi,0) as split_adjusted_price,
                   prccd/nullif(qunit,0)/nullif(ajexdi,0)*coalesce(trfd,1)
                       as total_return_index
            from comp.g_secd
            where gvkey='341068'
              and datadate between date '2024-09-01' and date '2024-12-31'
            order by iid, datadate
            """,
        )
        elcid.to_csv(OUTPUT / "source_elcid_price_history.csv", index=False)

        acc_placeholders = ",".join(["%s"] * len(ACCOUNTING_GVKEYS))
        annual = read_frame(
            conn,
            f"""
            select gvkey, iid, datadate, fdate, pdate, fyr, indfmt, datafmt,
                   consol, popsrc, curcd, at, ceq, seq, sale, revt, ib, nio, nit
            from comp.g_funda
            where gvkey in ({acc_placeholders})
              and datadate <= date '2026-06-30'
            order by gvkey, datadate desc, fdate desc nulls last
            """,
            ACCOUNTING_GVKEYS,
        )
        annual.groupby("gvkey", group_keys=False).head(5).to_csv(
            OUTPUT / "source_accounting_annual_examples.csv", index=False
        )

        domestic_placeholders = ",".join(["%s"] * len(DOMESTIC_ACCOUNTING_GVKEYS))
        domestic_annual = read_frame(
            conn,
            f"""
            select gvkey, iid, datadate, fdate, pdate, fyr, indfmt, datafmt,
                   consol, popsrc, curcd, at, ceq, seq, sale, revt, ib, ni, niadj
            from comp.funda
            where gvkey in ({domestic_placeholders})
              and datadate <= date '2026-06-30'
            order by gvkey, datadate desc, fdate desc nulls last
            """,
            DOMESTIC_ACCOUNTING_GVKEYS,
        )
        domestic_annual.groupby("gvkey", group_keys=False).head(5).to_csv(
            OUTPUT / "source_accounting_domestic_annual_examples.csv", index=False
        )

        quarterly = read_frame(
            conn,
            f"""
            select gvkey, iid, datadate, fyr, indfmt, datafmt, consol, popsrc,
                   atq, ceqq, seqq, saleq, revtq, ibq, nitq, nity
            from comp.g_fundq
            where gvkey in ({acc_placeholders})
              and datadate <= date '2026-06-30'
            order by gvkey, datadate desc
            """,
            ACCOUNTING_GVKEYS,
        )
        quarterly.groupby("gvkey", group_keys=False).head(8).to_csv(
            OUTPUT / "source_accounting_quarterly_examples.csv", index=False
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
