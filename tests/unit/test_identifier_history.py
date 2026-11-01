"""Exact vendor intervals supersede capture estimates without losing feed-only coverage."""

from datetime import date

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from jkp.data.identifier_history import (
    HISTORY_SCHEMA,
    OPEN_END,
    reconcile_identifier_history,
)
from jkp.data.production import _resolve_identifiers_asof

TODAY = date(2026, 9, 23)


def hist(rows):
    return pl.DataFrame(rows, schema=HISTORY_SCHEMA, orient="row")


def current(rows):
    return pl.DataFrame(
        rows, schema=dict.fromkeys(list(HISTORY_SCHEMA)[:4], pl.String), orient="row"
    )


def row(value, start, end=OPEN_END, source="wrds", iid="01", gvkey="001000"):
    return (gvkey, iid, "CUSIP", value, start, end, source)


def test_vendor_dates_replace_capture_dates_and_use_actual_observation_day():
    wrds = hist([row("OLD", date(2020, 1, 1), date(2026, 8, 17)), row("NEW", date(2026, 8, 18))])
    old = hist(
        [
            row("OLD", date(2020, 1, 1), date(2026, 8, 31)),
            row("NEW", date(2026, 9, 1), source="feed"),
        ]
    )
    now = current([("001000", "01", "CUSIP", "NEW")])
    fixed = reconcile_identifier_history(wrds, old, now, TODAY)
    assert_frame_equal(fixed, wrds)
    keys = pl.DataFrame(
        {
            "gvkey": ["001000"] * 3,
            "iid": ["01"] * 3,
            "date": [date(2026, 8, 17), date(2026, 8, 18), date(2026, 8, 31)],
        }
    )
    assert _resolve_identifiers_asof(fixed.lazy(), keys.lazy(), "date").collect()[
        "cusip"
    ].to_list() == ["OLD", "NEW", "NEW"]
    assert_frame_equal(fixed, reconcile_identifier_history(wrds, fixed, now, TODAY))


def test_removed_or_reassigned_seed_is_not_kept_on_the_old_issue():
    old = hist([row("REASSIGNED", date(2025, 3, 4))])
    wrds = hist([row("REASSIGNED", date(2025, 3, 4), iid="02")])
    now = current([("001000", "02", "CUSIP", "REASSIGNED")])
    fixed = reconcile_identifier_history(wrds, old, now, TODAY)
    assert_frame_equal(fixed, wrds)


def test_unknown_current_tail_keeps_first_observation_until_wrds_dates_arrive():
    wrds = hist([row("OLD", date(2020, 1, 1))])
    old = hist(
        [
            row("OLD", date(2020, 1, 1), date(2026, 8, 31)),
            row("NEW", date(2026, 9, 1), source="feed"),
        ]
    )
    now = current([("001000", "01", "CUSIP", "NEW")])
    fixed = reconcile_identifier_history(wrds, old, now, TODAY)
    assert_frame_equal(fixed, old)
    assert_frame_equal(fixed, reconcile_identifier_history(wrds, fixed, now, date(2026, 10, 1)))
    exact = hist([row("OLD", date(2020, 1, 1), date(2026, 8, 14)), row("NEW", date(2026, 8, 15))])
    assert_frame_equal(reconcile_identifier_history(exact, fixed, now, TODAY), exact)


def test_stale_native_header_does_not_undo_a_known_authoritative_change():
    wrds = hist([row("OLD", date(2020, 1, 1), date(2026, 8, 14)), row("NEW", date(2026, 8, 15))])
    now = current([("001000", "01", "CUSIP", "OLD")])
    assert_frame_equal(reconcile_identifier_history(wrds, wrds, now, TODAY), wrds)


def test_feed_only_coverage_is_preserved_and_disappearance_closes_it():
    wrds = hist([row("BASE", date(2020, 1, 1))])
    old = pl.concat([wrds, hist([row("FEED", date(2026, 8, 1), source="feed", iid="02")])])
    now = current([("001000", "01", "CUSIP", "BASE"), ("001000", "02", "CUSIP", None)])
    fixed = reconcile_identifier_history(wrds, old, now, TODAY)
    assert fixed.filter(pl.col("iid") == "02")["effthru"].item() == date(2026, 9, 22)
    assert_frame_equal(fixed, reconcile_identifier_history(wrds, fixed, now, TODAY))


def test_same_day_native_change_does_not_create_overlapping_zero_length_intervals():
    wrds = hist([row("BASE", date(2020, 1, 1))])
    old = pl.concat([wrds, hist([row("FEED", TODAY, source="feed", iid="02")])])
    now = current([("001000", "01", "CUSIP", "BASE"), ("001000", "02", "CUSIP", "NEW")])
    fixed = reconcile_identifier_history(wrds, old, now, TODAY)
    assert fixed.filter(pl.col("iid") == "02")["itemvalue"].to_list() == ["NEW"]
    assert fixed.filter(pl.col("iid") == "02")["efffrom"].item() == TODAY


def test_future_authoritative_change_is_preserved_after_unconfirmed_native_tail():
    wrds = hist([row("OLD", date(2020, 1, 1), date(2026, 9, 30)), row("FUTURE", date(2026, 10, 1))])
    now = current([("001000", "01", "CUSIP", "CURRENT")])
    out = reconcile_identifier_history(wrds, wrds, now, TODAY)
    assert out["itemvalue"].to_list() == ["OLD", "CURRENT", "FUTURE"]
    assert out["effthru"].to_list() == [date(2026, 9, 22), date(2026, 9, 30), OPEN_END]
    assert_frame_equal(out, reconcile_identifier_history(wrds, out, now, TODAY))


@pytest.mark.parametrize(
    "problem",
    [
        "empty_wrds",
        "overlap",
        "reverse",
        "empty_current",
        "conflicting_current",
        "backdated_capture",
    ],
)
def test_invalid_sources_are_rejected_before_publication(problem):
    wrds = hist([row("BASE", date(2020, 1, 1))])
    previous = wrds
    now = current([("001000", "01", "CUSIP", "BASE")])
    if problem == "empty_wrds":
        wrds = wrds.clear()
    if problem == "overlap":
        wrds = pl.concat([wrds, hist([row("OTHER", date(2026, 1, 1))])])
    if problem == "reverse":
        wrds = hist([row("BASE", TODAY, date(2020, 1, 1))])
    if problem == "empty_current":
        now = now.clear()
    if problem == "conflicting_current":
        now = pl.concat([now, current([("001000", "01", "CUSIP", "OTHER")])])
    if problem == "backdated_capture":
        previous = pl.concat([wrds, hist([row("NEW", date(2026, 10, 1), source="feed")])])
    with pytest.raises(ValueError):
        reconcile_identifier_history(wrds, previous, now, TODAY)
