"""Reconcile authoritative identifier dates with observations from a current-only feed."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

import polars as pl

ITEMS = ("CUSIP", "ISIN", "SEDOL", "TIC")
KEY = ["gvkey", "iid", "item"]
OPEN_END = date(2900, 1, 1)
HISTORY_SCHEMA = {
    "gvkey": pl.String,
    "iid": pl.String,
    "item": pl.String,
    "itemvalue": pl.String,
    "efffrom": pl.Date,
    "effthru": pl.Date,
    "source": pl.String,
}


def _normalize(frame: pl.DataFrame, *, history: bool) -> pl.DataFrame:
    frame = frame.with_columns(
        pl.col(*KEY, "itemvalue").cast(pl.String).str.strip_chars(),
    )
    if history:
        frame = frame.with_columns(
            pl.col("efffrom").cast(pl.Date),
            pl.col("effthru").cast(pl.Date).fill_null(OPEN_END),
        )
    invalid = (
        pl.any_horizontal(pl.col(*KEY, "itemvalue").is_null())
        | pl.any_horizontal(pl.col(*KEY, "itemvalue") == "")
        | ~pl.col("item").is_in(ITEMS)
    )
    if history:
        invalid |= pl.col("efffrom").is_null() | (pl.col("effthru") < pl.col("efffrom"))
    if frame.filter(invalid).height:
        raise ValueError("Invalid identifier keys, values or date intervals")
    return frame.unique()


def _validate_intervals(history: pl.DataFrame) -> None:
    ordered = history.sort(*KEY, "efffrom", "effthru").with_columns(
        _previous_end=pl.col("effthru").cum_max().shift(1).over(KEY)
    )
    if ordered.filter(pl.col("efffrom") <= pl.col("_previous_end")).height:
        raise ValueError("Conflicting or overlapping identifier intervals")


def reconcile_identifier_history(
    authoritative: pl.DataFrame,
    previous: pl.DataFrame,
    current: pl.DataFrame,
    observed_on: date,
) -> pl.DataFrame:
    """Replace stale seeded dates without turning capture dates into vendor dates.

    WRDS intervals supersede old WRDS rows, including removed/reassigned keys.
    Feed observations for values absent from WRDS remain useful in uncovered
    periods. A still-current, not-yet-known-to-WRDS value can extend the latest
    authoritative interval from its first observed date. A header carrying an
    older *known* WRDS value must not undo a newer authoritative change.

    Only feed observations are closed when a current value disappears; an
    absent header alone cannot supply an earlier effective deletion date.
    The caller stages/validates the complete result before an atomic DB write.
    """
    if authoritative.is_empty():
        raise ValueError("Refusing an empty authoritative identifier snapshot")
    wrds = _normalize(authoritative, history=True).with_columns(source=pl.lit("wrds"))
    wrds = wrds.select(list(HISTORY_SCHEMA))
    _validate_intervals(wrds)
    previous = _normalize(previous, history=True).select(list(HISTORY_SCHEMA))
    if previous.filter(~pl.col("source").is_in(["wrds", "feed"])).height:
        raise ValueError("Unknown identifier history source")
    feed = previous.filter(pl.col("source") == "feed")
    if feed.filter(pl.col("efffrom") > observed_on).height:
        raise ValueError("Cannot reconcile before an existing feed observation")

    # Null/blank current values mean absence, not an interval with an empty ID.
    current = current.filter(
        pl.col("itemvalue").is_not_null()
        & (pl.col("itemvalue").cast(pl.String).str.strip_chars() != "")
    )
    current = _normalize(current, history=False).select(*KEY, "itemvalue").unique()
    if current.group_by(KEY).len().filter(pl.col("len") > 1).height:
        raise ValueError("Conflicting current identifiers for the same issue/item")
    if current.is_empty():
        raise ValueError("Refusing an empty current identifier snapshot")

    known = wrds.select(*KEY, "itemvalue").unique()
    # Once WRDS supplies a value's dates, discard its approximate capture dates.
    feed = feed.join(known, on=[*KEY, "itemvalue"], how="anti")
    feed = (
        feed.join(current.rename({"itemvalue": "_current"}), on=KEY, how="left")
        .with_columns(
            effthru=pl.when(
                (pl.col("effthru") >= observed_on)
                & ~pl.col("itemvalue").eq_missing(pl.col("_current"))
            )
            .then(pl.lit(observed_on - timedelta(days=1)))
            .when(
                (pl.col("effthru") >= observed_on)
                & pl.col("itemvalue").eq_missing(pl.col("_current"))
            )
            .then(pl.lit(OPEN_END))
            .otherwise(pl.col("effthru"))
        )
        .drop("_current")
        .filter(pl.col("efffrom") <= pl.col("effthru"))
    )
    new_values = current.join(known, on=[*KEY, "itemvalue"], how="anti").join(
        feed.filter(pl.col("effthru") == OPEN_END).select(*KEY, "itemvalue"),
        on=[*KEY, "itemvalue"],
        how="anti",
    )
    feed = pl.concat(
        [
            feed,
            new_values.with_columns(
                efffrom=pl.lit(observed_on), effthru=pl.lit(OPEN_END), source=pl.lit("feed")
            ).select(list(HISTORY_SCHEMA)),
        ]
    )
    # Preserve an unconfirmed current tail's first observation across reruns,
    # unless a newer authoritative boundary supersedes that observation.
    latest = (
        wrds.filter(pl.col("efffrom") <= observed_on)
        .group_by(KEY)
        .agg(pl.col("efffrom").max().alias("_latest"))
    )
    feed = (
        feed.join(latest, on=KEY, how="left")
        .with_columns(
            efffrom=pl.when(
                (pl.col("effthru") == OPEN_END) & (pl.col("efffrom") <= pl.col("_latest"))
            )
            .then(pl.lit(observed_on))
            .otherwise(pl.col("efffrom"))
        )
        .drop("_latest")
    )
    _validate_intervals(feed)
    tails = feed.filter(pl.col("effthru") == OPEN_END).select(
        *KEY, pl.col("efffrom").alias("_tail_start")
    )
    wrds = (
        wrds.join(tails, on=KEY, how="left")
        .with_columns(
            effthru=pl.when(pl.col("efffrom") <= pl.col("_tail_start"))
            .then(pl.min_horizontal("effthru", pl.col("_tail_start") - pl.duration(days=1)))
            .otherwise(pl.col("effthru"))
        )
        .drop("_tail_start")
        .filter(pl.col("efffrom") <= pl.col("effthru"))
    )

    # Subtract authoritative coverage from the few feed intervals. This keeps
    # useful feed-only coverage without overlapping/back-dating exact history.
    coverage = defaultdict(list)
    for gvkey, iid, item, start, end in (
        wrds.join(feed.select(KEY).unique(), on=KEY, how="semi")
        .sort("efffrom")
        .select(*KEY, "efffrom", "effthru")
        .iter_rows()
    ):
        coverage[(gvkey, iid, item)].append((start, end))
    rows = []
    for gvkey, iid, item, value, start, end, source in feed.iter_rows():
        for lo, hi in coverage[(gvkey, iid, item)]:
            if hi < start:
                continue
            if lo > end:
                break
            if lo > start:
                rows.append((gvkey, iid, item, value, start, lo - timedelta(days=1), source))
            start = max(start, hi + timedelta(days=1))
            if start > end:
                break
        if start <= end:
            rows.append((gvkey, iid, item, value, start, end, source))
    result = pl.concat([wrds, pl.DataFrame(rows, schema=HISTORY_SCHEMA, orient="row")]).sort(
        *KEY, "efffrom"
    )
    _validate_intervals(result)
    return result
