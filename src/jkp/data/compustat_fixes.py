"""Corrections for two Compustat North America history defects inherited from the production SAS.

Both defects only bite on long histories (almost entirely before 1999), so they are
invisible in the rolling 23-year production run but distort a full-history build:

* The firm-shares fallback mixed the company's adjustment factor with the security's,
  which made Berkshire Hathaway's pre-1998 market cap 1,500x too large (10-90% of the
  US value-weighted market from 1969 to 1998).
* Monthly returns were computed across a switch between the SECM and SECD files, whose
  total-return factors are on different bases; this put the Dec-1983 US value-weighted
  market at -20% (Ken French: -1.8%).

They live in their own module so that ``aux_functions.py`` only carries one-line hooks.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .paths import DataPaths

if TYPE_CHECKING:
    from ibis.backends.duckdb import Backend

SECURITY_AJEX_VIEW = "__sec_ajex_rep"


def register_security_ajex_at_report(con: Backend, paths: DataPaths) -> None:
    """
    Description:
        Register a DuckDB view with each North American security's own cumulative
        adjustment factor (SECM ``ajexm``) by month-end, for the firm-shares fallback.
    Steps:
        1) Read the downloaded comp.secm and keep rows with a positive ``ajexm``.
        2) Keep one row per {gvkey, iid, month-end}, the latest ``datadate``.
        3) Without a comp.secm download, register an empty view so the fallback keeps
           the company factor.
    Output:
        DuckDB view ``__sec_ajex_rep`` with {gvkey, iid, rep_eom, ajex_sec}.
    Note:
        When a security's own share count is missing, SECD/SECM infer it as
        ``csho_fund * ajex_fund / ajex(t)`` from the company's last report. ``ajex_fund``
        follows the company's reference share class, so for Berkshire class A it is on the
        class B basis (1,500) while the class A factor is 1: the inferred count was
        1,500x too large until SECD began reporting class A shares in April 1998.
        Using the security's own factor at the report date, ``csho_fund * ajex_sec /
        ajex(t)``, only adjusts for this security's splits between the report and ``t``.
        Validated against the security's next reported share count on 673k fallback months
        of NYSE/AMEX/NASDAQ common stock: 2,705 months move closer to it, 94 move further
        away (mostly two-class companies, where a company-wide count cannot be right for
        either class).
    """
    source = paths.raw_table_source("comp.secm")
    if isinstance(source, Path) and not source.exists():
        con.raw_sql(f"""
        CREATE OR REPLACE VIEW {SECURITY_AJEX_VIEW} AS
            SELECT NULL::VARCHAR AS gvkey, NULL::VARCHAR AS iid,
                   NULL::DATE AS rep_eom, NULL::DOUBLE AS ajex_sec
            WHERE FALSE;
        """)
        return
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
