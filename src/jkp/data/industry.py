"""Dated Compustat industry history used to recover missing SIC/NAICS codes."""

from __future__ import annotations

import re
from datetime import date

import polars as pl


def build_industry_history_query(
    raw_schema: str, start_date: date | None, end_date: date | None
) -> str:
    """Download the window plus its last preceding industry row, including ties.

    WRDS's ``comp`` schema separates NA and Global industry tables. Native
    XpressFeed (``public`` in production) combines them using ``popsrc``.
    Industry history is independent of financial-statement eligibility.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw_schema):
        raise ValueError(f"Invalid raw PostgreSQL schema: {raw_schema!r}")
    tables = ("co_industry", "g_co_industry") if raw_schema == "comp" else ("co_industry",)
    upper = f"AND datadate <= DATE '{end_date.isoformat()}'" if end_date else ""
    sources = "\nUNION ALL\n".join(
        f"""SELECT RTRIM(gvkey)::varchar AS gvkey, datadate::date AS datadate,
                   sich::integer AS sich, naicsh::varchar AS naicsh,
                   RTRIM(popsrc)::varchar AS popsrc, RTRIM(consol)::varchar AS consol
            FROM "{raw_schema}"."{table}"
            WHERE gvkey IS NOT NULL AND datadate IS NOT NULL
              AND popsrc IN ('D', 'I')
              AND (sich IS NOT NULL OR naicsh IS NOT NULL) {upper}"""
        for table in tables
    )
    if start_date is None:
        selection = "SELECT * FROM history"
    else:
        lower = f"DATE '{start_date.isoformat()}'"
        selection = f"""
SELECT h.* FROM history h
LEFT JOIN (
    SELECT gvkey, MAX(datadate) AS anchor_date FROM history
    WHERE datadate < {lower} GROUP BY gvkey
) a ON a.gvkey = h.gvkey
WHERE h.datadate >= {lower} OR h.datadate = a.anchor_date"""
    return f"WITH history AS MATERIALIZED ({sources})\n{selection}"


def resolve_industry_history(keys: pl.LazyFrame, history: pl.LazyFrame) -> pl.LazyFrame:
    """Resolve one dated candidate per (gvkey, eom), retaining its provenance.

    Latest industry-bearing ROW wins, rather than independently carrying old
    fields through a newer partial classification. At the same date prefer NA
    over Global, then consolidated over unconsolidated. Conflicting values
    within that priority are left unresolved per code. Never use a future row.
    """
    history = (
        history.with_columns(
            pl.col("gvkey").cast(pl.String).str.strip_chars().str.pad_start(6, "0"),
            pl.col("datadate").cast(pl.Date),
            pl.col("sich").cast(pl.Int64, strict=False),
            pl.col("naicsh").cast(pl.String).str.strip_chars(),
            pl.col("popsrc", "consol").cast(pl.String).str.strip_chars(),
        )
        .filter(
            pl.col("gvkey").is_not_null()
            & pl.col("datadate").is_not_null()
            & pl.col("popsrc").is_in(["D", "I"])
            & pl.any_horizontal(pl.col("sich", "naicsh").is_not_null())
        )
        .with_columns(
            pl.when(pl.col("sich").is_between(1, 9999))
            .then(pl.col("sich"))
            .otherwise(None)
            .alias("sic_history"),
            pl.when(pl.col("naicsh").str.contains(r"^[1-9]\d{1,5}$"))
            .then(pl.col("naicsh").cast(pl.Int64, strict=False))
            .otherwise(None)
            .alias("naics_history"),
        )
        .group_by("gvkey", "datadate", "popsrc", "consol")
        .agg(
            pl.when(pl.col(code).n_unique() == 1)
            .then(pl.col(code).first())
            .otherwise(None)
            .alias(code)
            for code in ("sic_history", "naics_history")
        )
        .with_columns((pl.col("consol") != "C").fill_null(True).alias("_not_consolidated"))
        .sort("gvkey", "datadate", "popsrc", "_not_consolidated", "consol", nulls_last=True)
        .unique(["gvkey", "datadate"], keep="first", maintain_order=True)
        .drop("_not_consolidated")
        .sort("datadate")
    )
    return (
        keys.select("gvkey", "eom")
        .unique()
        .sort("eom")
        .join_asof(
            history,
            left_on="eom",
            right_on="datadate",
            by="gvkey",
            strategy="backward",
            check_sortedness=False,  # both sides explicitly sorted by their date
        )
    )


def resolve_sic_history(keys: pl.LazyFrame, history: pl.LazyFrame) -> pl.LazyFrame:
    """SIC-only projection for callers that do not request NAICS recovery."""
    return resolve_industry_history(keys, history).drop("naics_history")


def _fill_missing_codes(
    data: pl.LazyFrame, resolved: pl.LazyFrame, codes: tuple[str, ...]
) -> pl.LazyFrame:
    schema = data.collect_schema()
    return (
        data.join(
            resolved.select("gvkey", "eom", *[f"{code}_history" for code in codes]),
            on=["gvkey", "eom"],
            how="left",
            validate="m:1",
            maintain_order="left",
        )
        .with_columns(
            pl.coalesce(code, f"{code}_history")
            .cast(pl.Int64 if schema[code] == pl.Null else schema[code])
            .alias(code)
            for code in codes
        )
        .drop([f"{code}_history" for code in codes])
    )


def fill_missing_sic(data: pl.LazyFrame, resolved: pl.LazyFrame) -> pl.LazyFrame:
    """Fill only null SIC; preserve all rows, their order and every other column."""
    return _fill_missing_codes(data, resolved, ("sic",))


def fill_missing_industry_codes(data: pl.LazyFrame, resolved: pl.LazyFrame) -> pl.LazyFrame:
    """Fill only null SIC/NAICS after the existing Compustat/CRSP selections."""
    return _fill_missing_codes(data, resolved, ("sic", "naics"))
