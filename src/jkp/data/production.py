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


def _identifier_panel(paths: DataPaths) -> pl.LazyFrame:
    """Daily identifier panel keyed (gvkey, iid, comp_exchg, date).

    cusip + conm come from comp.secd; sedol + isin_orig (+ conm fallback) from
    comp.g_secd. Mirrors the joins in the SAS production macros.
    """
    secd = (
        pl.scan_parquet(paths.raw_table_source("comp.secd"))
        .select(
            "gvkey",
            "iid",
            comp_exchg=pl.col("exchg").cast(pl.Int64),
            date=pl.col("datadate"),
            cusip=pl.col("cusip"),
            conm_secd=pl.col("conm"),
        )
        .unique(["gvkey", "iid", "comp_exchg", "date"])
    )
    gsecd = (
        pl.scan_parquet(paths.raw_table_source("comp.g_secd"))
        .select(
            "gvkey",
            "iid",
            comp_exchg=pl.col("exchg").cast(pl.Int64),
            date=pl.col("datadate"),
            sedol=pl.col("sedol"),
            isin_orig=pl.col("isin"),
            conm_g=pl.col("conm"),
        )
        .unique(["gvkey", "iid", "comp_exchg", "date"])
    )
    return secd.join(
        gsecd, on=["gvkey", "iid", "comp_exchg", "date"], how="full", coalesce=True
    ).with_columns(conm=pl.coalesce(["conm_secd", "conm_g"]))


# ---------------------------------------------------------------------------
# Daily production CSVs (bra.csv format)
# ---------------------------------------------------------------------------


@measure_time
def save_daily_production_csv(paths: DataPaths, end_date: date = END_DATE) -> None:
    """Write per-country daily return CSVs in the SAS production (bra.csv) format.

    Columns: excntry, id, date, ret (local), prc_open (local), prc_high/low/close
    (local = USD/fx), ret_dollar (USD), ret_exc_dollar, sedol, cusip, isin.
    """
    out_dir = paths.processed_dir / "production" / "daily"
    sas_out_dir = paths.sas_output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    sas_out_dir.mkdir(parents=True, exist_ok=True)
    daily_cutoff = min(end_date, date.today())

    # id -> (gvkey, iid, comp_exchg) map from the monthly file (daily lacks them).
    id_map = (
        pl.scan_parquet(paths.interim_dir / "world_data_output.parquet")
        .select("id", "gvkey", "iid", "comp_exchg")
        .unique("id")
    )
    ids = _identifier_panel(paths).select(
        "gvkey", "iid", "comp_exchg", "date", "sedol", "cusip", "isin_orig"
    )

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
        .join(ids, on=["gvkey", "iid", "comp_exchg", "date"], how="left")
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
    _write_country_csvs(daily_df, sas_out_dir)


# ---------------------------------------------------------------------------
# Monthly production CSVs (usa.csv format, 455 columns)
# ---------------------------------------------------------------------------


@measure_time
def save_main_production_csv(paths: DataPaths, end_date: date = END_DATE) -> None:
    """Write per-country monthly characteristics CSVs in the SAS production format."""
    out_dir = paths.processed_dir / "production" / "monthly"
    sas_out_dir = paths.sas_output_dir / "CharacteristicsProduction"
    out_dir.mkdir(parents=True, exist_ok=True)
    sas_out_dir.mkdir(parents=True, exist_ok=True)

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
    ids = _identifier_panel(paths).select(
        "gvkey", "iid", "comp_exchg", "date", "conm", "sedol", "cusip", "isin_orig"
    )

    data = (
        pl.scan_parquet(paths.interim_dir / "world_data_output.parquet")
        # Identifiers are supplied by the panel join; drop any pre-existing
        # columns of the same name to avoid '<col>_right' collisions.
        .drop("conm", "sedol", "cusip", "isin_orig", strict=False)
        .filter(pl.col("eom") <= pl.lit(end_date))
        .join(ids, on=["gvkey", "iid", "comp_exchg", "date"], how="left")
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
    _write_country_csvs(data, sas_out_dir)


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
