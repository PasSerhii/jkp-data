"""Alpha-beta production output layer.

Reproduces the SAS `*_production_*` macros: per-country monthly characteristics
CSVs (e.g. usa.csv, 455 columns) and daily return CSVs (e.g. bra.csv, 13
columns), including the production-only fields the academic jkp output omits:
company name, security identifiers (sedol/cusip/isin), trailing-window turnover
(USD + ILS), trailing 3-month minimum price, and ILS market value.

These run after the main pipeline (before interim cleanup), reading the filtered
interim outputs `world_data_output.parquet` and `world_dsf_output.parquet`.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from .aux_functions import compustat_fx, measure_time
from .config import END_DATE
from .paths import DataPaths, get_production_monthly_columns

# ---------------------------------------------------------------------------
# ISIN check digit (Luhn over the alphanumeric body); mirrors %ISINVERIFICATION
# ---------------------------------------------------------------------------


def isin_check_digit(body: str) -> int:
    """Compute the ISIN check digit for an 11-character body (country + cusip).

    Letters map A->10 .. Z->35, digits map to themselves; the resulting digit
    string is scored with the Luhn algorithm (doubling every second digit,
    anchored so the rightmost body digit is doubled), and the check digit is
    ``(10 - total % 10) % 10``.
    """
    digits = ""
    for ch in body:
        o = ord(ch.upper())
        if 65 <= o <= 95:  # A-Z (mirrors the SAS 65..95 range)
            digits += str(o - 55)
        elif 48 <= o <= 57:  # 0-9
            digits += str(o - 48)
        else:
            return -1  # unexpected character -> invalid
    n = len(digits)
    total = 0
    for i, d in enumerate(digits):
        j = i + 1  # 1-based, matching the SAS loop
        y = int(d)
        double = (j % 2 == 0) if (n % 2 == 0) else (j % 2 != 0)
        if double:
            y *= 2
        total += y // 10 + y % 10
    return (10 - total % 10) % 10


def make_isin_from_cusip(excntry: str | None, cusip: str | None) -> str | None:
    """Build a 12-character ISIN from country prefix + cusip + computed check digit.

    Returns None when country or cusip is missing. The body is the 2-letter
    country code followed by the 9-character cusip (11 chars); the check digit is
    appended.
    """
    if not excntry or not cusip:
        return None
    body = (excntry[:2] + cusip).upper()
    if len(body) != 11:
        return None
    cd = isin_check_digit(body)
    if cd < 0:
        return None
    return body + str(cd)


def _add_isin(df: pl.DataFrame) -> pl.DataFrame:
    """Add the final ``isin`` column = coalesce(isin_orig, country+cusip+checkdigit).

    The check digit is computed once per distinct (excntry, cusip) pair to avoid
    recomputing over millions of rows.
    """
    pairs = (
        df.select("excntry", "cusip")
        .unique()
        .filter(pl.col("cusip").is_not_null() & pl.col("excntry").is_not_null())
    )
    if pairs.height > 0:
        pairs = pairs.with_columns(
            isin_temp=pl.struct("excntry", "cusip").map_elements(
                lambda s: make_isin_from_cusip(s["excntry"], s["cusip"]),
                return_dtype=pl.Utf8,
            )
        )
        df = df.join(pairs, on=["excntry", "cusip"], how="left")
    else:
        df = df.with_columns(isin_temp=pl.lit(None, dtype=pl.Utf8))
    return df.with_columns(isin=pl.coalesce(["isin_orig", "isin_temp"])).drop("isin_temp")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _yyyymmdd(colname: str) -> pl.Expr:
    """Format a Date column as an integer YYYYMMDD (matching SAS YYMMDDN8.).

    Build via strftime to avoid Int8 overflow from dt.month()/dt.day() arithmetic.
    """
    return pl.col(colname).dt.strftime("%Y%m%d").cast(pl.Int64).alias(colname)


def _ils_fx(paths: DataPaths) -> pl.DataFrame:
    """Daily ILS FX series: columns [date, fx_ils] (USD per 1 ILS)."""
    return (
        compustat_fx(paths)
        .filter(pl.col("curcdd") == "ILS")
        .select(date=pl.col("datadate"), fx_ils=pl.col("fx"))
        .unique("date")
    )


def _month_index(colname: str) -> pl.Expr:
    """0-based month index (year*12 + month - 1) for a Date column."""
    c = pl.col(colname)
    return (c.dt.year() * 12 + c.dt.month() - 1).cast(pl.Int64)


def _eom_from_month_index(colname: str) -> pl.Expr:
    """Convert a 0-based month index back to the month-end Date."""
    m = pl.col(colname)
    return pl.date(m // 12, (m % 12) + 1, 1).dt.month_end().alias("eom")


# ---------------------------------------------------------------------------
# Trailing-window analytics (mirror %market_volumes / %market_min_price)
# ---------------------------------------------------------------------------


@measure_time
def market_volumes(paths: DataPaths, n: int) -> pl.DataFrame:
    """Per (id, eom) trailing ``n``-month mean/median daily dollar volume (USD & ILS).

    Mirrors %market_volumes: for each target month-end T, aggregate the daily
    ``dolvol`` over the window (T-n months, T]. A daily observation in month m
    contributes to targets m..m+n-1.  Use the pre-filter daily panel: historical
    observations must remain available when an issue only becomes the company's
    primary/main security at the target month-end.
    """
    fx = _ils_fx(paths).lazy()
    daily = (
        pl.scan_parquet(paths.interim_dir / "world_dsf.parquet")
        .select("id", "date", "eom", "dolvol")
        .join(fx, on="date", how="left")
        .with_columns(
            ilsvol=pl.col("dolvol") / pl.col("fx_ils"),
            _m=_month_index("eom"),
        )
        .with_columns(_t=pl.int_ranges(pl.col("_m"), pl.col("_m") + n))
        .explode("_t")
        .group_by("id", "_t")
        .agg(
            avgdolvol=pl.col("dolvol").mean(),
            mediandolvol=pl.col("dolvol").median(),
            avgilsvol=pl.col("ilsvol").mean(),
            medianilsvol=pl.col("ilsvol").median(),
        )
        .with_columns(_eom_from_month_index("_t"))
        .drop("_t")
    )
    return daily.collect(engine="streaming")


@measure_time
def market_min_price(paths: DataPaths, n: int) -> pl.DataFrame:
    """Per (id, eom) trailing ``n``-month minimum daily price (USD).

    Mirrors %market_min_price and deliberately uses the pre-filter daily panel.
    Filtering to main/primary observations first can discard valid earlier days
    in the trailing window, especially around listings and primary-issue changes.
    """
    daily = (
        pl.scan_parquet(paths.interim_dir / "world_dsf.parquet")
        .select("id", "eom", "prc")
        .with_columns(_m=_month_index("eom"))
        .with_columns(_t=pl.int_ranges(pl.col("_m"), pl.col("_m") + n))
        .explode("_t")
        .group_by("id", "_t")
        .agg(min_prc=pl.col("prc").min())
        .with_columns(_eom_from_month_index("_t"))
        .drop("_t")
    )
    return daily.collect(engine="streaming")


# ---------------------------------------------------------------------------
# Security identifiers (conm / sedol / cusip / isin_orig) from SECD / G_SECD
# ---------------------------------------------------------------------------


_ID_HISTORY_ITEMS = {"cusip": "CUSIP", "isin_orig": "ISIN", "sedol": "SEDOL"}


def _identifier_history(paths: DataPaths) -> pl.LazyFrame | None:
    """Point-in-time identifier intervals, or None when unavailable.

    ``comp.sec_id_history`` is maintained by sql/xpressfeed_views/capture_sec_ids.py:
    a one-time backfill from WRDS plus a monthly diff of the feed's current
    identifiers. Returns None when the table has not been downloaded, so a run
    against an older raw set still produces output rather than failing.
    """
    source = paths.raw_table_source("comp.sec_id_history")
    # A glob string means the parts directory already exists; a bare Path may not.
    if isinstance(source, Path) and not source.is_file():
        return None
    return (
        pl.scan_parquet(source)
        .filter(pl.col("item").is_in(list(_ID_HISTORY_ITEMS.values())))
        .select(
            "gvkey",
            "iid",
            "item",
            itemvalue=pl.col("itemvalue").cast(pl.Utf8),
            efffrom=pl.col("efffrom").cast(pl.Date),
            effthru=pl.col("effthru").cast(pl.Date),
        )
    )


def _resolve_identifiers_asof(
    history: pl.LazyFrame, keys: pl.LazyFrame, date_col: str
) -> pl.LazyFrame:
    """Attach cusip/isin_orig/sedol effective on each row's observation date.

    Interval containment rather than a plain join: an issue can carry several
    values for one item over its life, and only the interval covering the
    observation date is correct for that row.
    """
    resolved = keys
    for column, item in _ID_HISTORY_ITEMS.items():
        matched = (
            keys.join(history.filter(pl.col("item") == item), on=["gvkey", "iid"], how="inner")
            .filter(
                (pl.col(date_col) >= pl.col("efffrom")) & (pl.col(date_col) <= pl.col("effthru"))
            )
            # Overlapping intervals are possible where the backfill and the feed
            # disagree; prefer the latest-starting one.
            .sort("efffrom")
            .group_by(["gvkey", "iid", date_col])
            .agg(pl.col("itemvalue").last().alias(column))
        )
        resolved = resolved.join(matched, on=["gvkey", "iid", date_col], how="left")
    return resolved


def _identifier_panel(paths: DataPaths) -> pl.LazyFrame:
    """Daily identifier panel keyed by the stable issue key and trading date.

    cusip + conm come from comp.secd; sedol + isin_orig (+ conm fallback) from
    comp.g_secd.  Exchange is intentionally not a join key: it changes over an
    issue's life and current headers can differ from the historical value.

    These columns come from the security header, which holds only the value in
    force today, so they are stamped on every historical row.  When
    ``comp.sec_id_history`` is present the caller replaces cusip/isin_orig/sedol
    with the point-in-time values; ``conm`` has no historical source in the feed
    and stays current-stamped.
    """
    secd = (
        pl.scan_parquet(paths.raw_table_source("comp.secd"))
        .select(
            "gvkey",
            "iid",
            date=pl.col("datadate"),
            cusip=pl.col("cusip"),
            conm_secd=pl.col("conm"),
        )
        .unique(["gvkey", "iid", "date"])
    )
    gsecd = (
        pl.scan_parquet(paths.raw_table_source("comp.g_secd"))
        .select(
            "gvkey",
            "iid",
            date=pl.col("datadate"),
            sedol=pl.col("sedol"),
            isin_orig=pl.col("isin"),
            conm_g=pl.col("conm"),
        )
        .unique(["gvkey", "iid", "date"])
    )
    panel = secd.join(gsecd, on=["gvkey", "iid", "date"], how="full", coalesce=True).with_columns(
        conm=pl.coalesce(["conm_secd", "conm_g"])
    )

    history = _identifier_history(paths)
    if history is None:
        return panel
    keys = panel.select("gvkey", "iid", "date")
    pit = _resolve_identifiers_asof(history, keys, "date")
    # Point-in-time value wins; the security header is the fallback, not a
    # replacement. Dropping the header outright would null the column wherever
    # the history has no interval -- and for `isin_orig` a null is worse than a
    # stale value, because _add_isin then fabricates `excntry[:2] + cusip`. That
    # synthetic is a real ISIN only for US-domiciled issuers; for the ~24% of the
    # US universe domiciled in Cayman, Canada, Israel and elsewhere it invents an
    # identifier that resolves nowhere. Coalescing keeps the synthetic as a true
    # last resort.
    return (
        panel.join(pit, on=["gvkey", "iid", "date"], how="left", suffix="_pit")
        .with_columns(
            [
                pl.coalesce([pl.col(f"{name}_pit"), pl.col(name)]).alias(name)
                for name in _ID_HISTORY_ITEMS
            ]
        )
        .drop([f"{name}_pit" for name in _ID_HISTORY_ITEMS])
    )


def _monthly_identifier_panel(paths: DataPaths) -> pl.LazyFrame:
    """Last available identifiers in each issue-month.

    Production observations use calendar month-end dates, which can be weekends
    or holidays while SECD identifiers exist only on trading days.  Carrying the
    last non-null identifier within the same month mirrors the SAS month-end
    merge and prevents spurious missing names/CUSIPs/ISINs.
    """
    return (
        _identifier_panel(paths)
        .with_columns(eom=pl.col("date").dt.month_end())
        .sort(["gvkey", "iid", "date"])
        .group_by(["gvkey", "iid", "eom"])
        .agg(
            [
                pl.col(name).drop_nulls().last().alias(name)
                for name in ["conm", "sedol", "cusip", "isin_orig"]
            ]
        )
    )


# ---------------------------------------------------------------------------
# Daily production CSVs (bra.csv format)
# ---------------------------------------------------------------------------


@measure_time
def save_daily_production_csv(paths: DataPaths, end_date: date = END_DATE) -> None:
    """Write per-country daily return CSVs in the SAS production (bra.csv) format.

    Columns: excntry, id, date, ret (local), prc_open (local), prc_high/low/close
    (local = USD/fx), ret_dollar (USD), ret_exc_dollar, sedol, cusip, isin.
    """
    out_dir = paths.production_dir / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    daily_cutoff = min(end_date, date.today())

    # id -> stable Compustat issue key map from the monthly file (daily lacks it).
    id_map = (
        pl.scan_parquet(paths.interim_dir / "world_data_output.parquet")
        .select("id", "gvkey", "iid")
        .unique("id")
    )
    ids = _identifier_panel(paths).select("gvkey", "iid", "date", "sedol", "cusip", "isin_orig")

    daily = (
        pl.scan_parquet(paths.interim_dir / "world_dsf_output.parquet")
        .filter(pl.col("date") <= pl.lit(daily_cutoff))
        .select(
            "excntry",
            "id",
            "date",
            "fx",
            ret=pl.col("ret_local"),
            prc_open=pl.col("prc_open_lcl"),
            prc_high=pl.col("prc_high") / pl.col("fx"),
            prc_low=pl.col("prc_low") / pl.col("fx"),
            prc_close=pl.col("prc") / pl.col("fx"),
            ret_dollar=pl.col("ret"),
            ret_exc_dollar=pl.col("ret_exc"),
        )
        .join(id_map, on="id", how="left")
        .join(ids, on=["gvkey", "iid", "date"], how="left")
        .select(
            "excntry",
            "id",
            _yyyymmdd("date"),
            "ret",
            "prc_open",
            "prc_high",
            "prc_low",
            "prc_close",
            "ret_dollar",
            "ret_exc_dollar",
            "sedol",
            "cusip",
            pl.col("isin_orig").alias("isin"),
        )
    )
    daily_df = daily.collect(engine="streaming")
    _write_country_csvs(daily_df, out_dir)


# ---------------------------------------------------------------------------
# Monthly production CSVs (usa.csv format, 455 columns)
# ---------------------------------------------------------------------------


@measure_time
def save_main_production_csv(paths: DataPaths, end_date: date = END_DATE) -> None:
    """Write per-country monthly characteristics CSVs in the SAS production format."""
    out_dir = paths.production_dir / "monthly"
    out_dir.mkdir(parents=True, exist_ok=True)

    vol3 = market_volumes(paths, 3).rename(
        {
            "avgdolvol": "avg_turnover_3m_usd",
            "mediandolvol": "median_turnover_3m_usd",
            "avgilsvol": "avg_turnover_3m_ils",
            "medianilsvol": "median_turnover_3m_ils",
        }
    )
    vol6 = market_volumes(paths, 6).rename(
        {
            "avgdolvol": "avg_turnover_6m_usd",
            "mediandolvol": "median_turnover_6m_usd",
            "avgilsvol": "avg_turnover_6m_ils",
            "medianilsvol": "median_turnover_6m_ils",
        }
    )
    minp = market_min_price(paths, 3).rename({"min_prc": "min_price_last_3m_usd"})
    fx_ils = _ils_fx(paths)
    ids = _monthly_identifier_panel(paths)

    data = (
        pl.scan_parquet(paths.interim_dir / "world_data_output.parquet")
        # Identifiers are supplied by the panel join; drop any pre-existing
        # columns of the same name to avoid '<col>_right' collisions.
        .drop("conm", "sedol", "cusip", "isin_orig", strict=False)
        .filter(pl.col("eom") <= pl.lit(end_date))
        .join(ids, on=["gvkey", "iid", "eom"], how="left")
        .join(vol3.lazy(), on=["id", "eom"], how="left")
        .join(vol6.lazy(), on=["id", "eom"], how="left")
        .join(minp.lazy(), on=["id", "eom"], how="left")
        .join(fx_ils.lazy(), on="date", how="left")
        .with_columns(market_value_ils=pl.col("me_company") / pl.col("fx_ils"))
        .collect(engine="streaming")
    )
    data = _add_isin(data)
    data = data.with_columns(_yyyymmdd("date"), _yyyymmdd("eom"))

    # Reindex to the exact SAS column order; any column not produced is emitted
    # as null so the schema matches byte-for-byte.
    ordered = get_production_monthly_columns()
    existing = set(data.columns)
    data = data.with_columns([pl.lit(None).alias(c) for c in ordered if c not in existing]).select(
        ordered
    )

    _write_country_csvs(data, out_dir)


# ---------------------------------------------------------------------------
# Shared per-country CSV writer + orchestrator
# ---------------------------------------------------------------------------


def _write_country_csvs(lf: pl.DataFrame | pl.LazyFrame, out_dir) -> None:
    """Split a frame by excntry and write one lowercase-named CSV per country."""
    df = lf.collect(engine="streaming") if isinstance(lf, pl.LazyFrame) else lf
    countries = df.select("excntry").drop_nulls().unique().to_series().to_list()
    for country in countries:
        (
            df.filter(pl.col("excntry") == country).write_csv(
                out_dir / f"{str(country).lower()}.csv",
                quote_style="non_numeric",
                null_value="",
            )
        )


@measure_time
def export_production(paths: DataPaths, end_date: date = END_DATE) -> None:
    """Write both production outputs (monthly characteristics + daily returns)."""
    save_main_production_csv(paths, end_date)
    save_daily_production_csv(paths, end_date)
