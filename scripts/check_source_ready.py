"""Decide whether the Compustat feed is complete enough to run a monthly build.

    uv run python scripts/check_source_ready.py                 # previous month end
    uv run python scripts/check_source_ready.py 2026-07-31      # explicit target
    uv run python scripts/check_source_ready.py --wait-minutes 60

Exits 0 when every gate passes, 1 otherwise, so it can gate an automated run.

Why not just look at ``max(datadate)``: the monthly table ``sec_mth`` stamps rows
with the month-end date as they arrive, so it reports the *current* month end
from the first day of that month while holding only a fraction of the universe.
Trusting it would silently build a month with most securities missing.

So the daily side is measured directly instead: find the month's last real
trading day and check it carries a full cross-section. An earlier version asked
whether the feed's frontier had moved *past* the month end, which sounds
equivalent and is not -- the last trading day generally *is* the month end, so
the frontier cannot pass it until the next month starts delivering. That test
failed every month on the 1st, which is precisely when the monthly build runs.

``--wait-minutes`` turns the single verdict into a bounded poll, for the
unattended monthly run that starts at a fixed hour and cannot ask a human to
retry. It defaults to 0, so every existing caller keeps the single-shot
behaviour.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from datetime import date, timedelta

import duckdb

from jkp.data.database_sources import get_xpressfeed_connection_info

# A completed month must hold at least this share of the previous month's rows /
# trading dates, and its last trading day this share of the month's typical
# cross-section. Real drift is well under 5%.
COVERAGE_MIN = 0.95
# How far the last full trading day may sit before the calendar month end.
# Covers a Saturday or Sunday month end plus a global holiday.
DAILY_TAIL_DAYS = 4
# Separates real trading days from thin ones. Sundays carry ~1.3% of a weekday's
# rows (Middle-East markets only) and must not drag the median down.
FULL_DAY_SHARE = 0.5

Gate = tuple[str, bool, str]
DayCounts = list[tuple[date, int]]

DAILY_GATE = "daily prices through month end"


def _previous_month_end(today: date) -> date:
    return today.replace(day=1) - timedelta(days=1)


def assess_daily_month(counts: DayCounts, target: date) -> Gate:
    """Is the month's final trading day present and carrying a full cross-section?

    ``counts`` is (datadate, row count) for every date in the target month.
    Pure so it can be tested without a database.
    """
    if not counts:
        return (DAILY_GATE, False, "no daily rows in the target month")
    median = statistics.median(n for _, n in counts)
    full = [(d, n) for d, n in counts if n >= FULL_DAY_SHARE * median]
    if not full:
        return (DAILY_GATE, False, f"no full trading day (median {median:,})")
    last_day, last_n = max(full)
    lag = (target - last_day).days
    share = last_n / median if median else 0.0
    ok = lag <= DAILY_TAIL_DAYS and share >= COVERAGE_MIN
    return (
        DAILY_GATE,
        ok,
        f"last full day {last_day} ({lag}d before month end), "
        f"{last_n:,} rows = {share:.0%} of median",
    )


def evaluate(target: date) -> tuple[list[Gate], str]:
    """Measure every readiness gate for ``target``, plus the FF fallback note.

    Opens its own connection so a polling caller re-reads the feed rather than
    a snapshot taken before the delivery landed.
    """
    prev = _previous_month_end(target.replace(day=1))
    month_start = target.replace(day=1)
    con = duckdb.connect()
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(
            f"ATTACH '{get_xpressfeed_connection_info()}' AS src (TYPE postgres, READ_ONLY)"
        )

        def scalar(sql: str):
            return con.sql(f"SELECT * FROM postgres_query('src', $q${sql}$q$)").fetchone()[0]

        def rows(sql: str):
            return con.sql(f"SELECT * FROM postgres_query('src', $q${sql}$q$)").fetchall()

        def as_date(value):
            return value.date() if hasattr(value, "date") else value

        daily_counts: DayCounts = [
            (as_date(d), int(n))
            for d, n in rows(
                "SELECT datadate, count(*) FROM public.sec_dprc "
                f"WHERE datadate BETWEEN date '{month_start}' AND date '{target}' "
                "GROUP BY 1"
            )
        ]
        fx_counts: DayCounts = [
            (as_date(d), int(n))
            for d, n in rows(
                "SELECT datadate, count(*) FROM public.exrt_dly "
                f"WHERE datadate BETWEEN date '{month_start}' AND date '{target}' "
                "GROUP BY 1"
            )
        ]
        ff_max = scalar("SELECT max(date) FROM ff.factors_monthly")
        rows_target = scalar(
            f"SELECT count(*) FROM public.sec_mth WHERE datadate = date '{target}'"
        )
        rows_prev = scalar(f"SELECT count(*) FROM public.sec_mth WHERE datadate = date '{prev}'")
        days_prev = scalar(
            "SELECT count(DISTINCT datadate) FROM public.sec_dprc "
            f"WHERE datadate BETWEEN date '{prev.replace(day=1)}' AND date '{prev}'"
        )
    finally:
        con.close()

    monthly_cov = rows_target / rows_prev if rows_prev else 0.0
    daily_cov = len(daily_counts) / days_prev if days_prev else 0.0

    # FX publishes on every calendar day, weekends included, so the month end
    # itself must be present rather than the last trading day before it.
    fx_by_date = dict(fx_counts)
    fx_median = statistics.median(n for _, n in fx_counts) if fx_counts else 0
    fx_target = fx_by_date.get(target, 0)

    gates: list[Gate] = [
        assess_daily_month(daily_counts, target),
        (
            "FX through month end",
            bool(fx_median) and fx_target >= COVERAGE_MIN * fx_median,
            f"{fx_target:,} rows on {target} vs median {fx_median:,} (gates USD conversion)",
        ),
        (
            "monthly universe complete",
            monthly_cov >= COVERAGE_MIN,
            f"{rows_target:,} rows vs {rows_prev:,} prior ({monthly_cov:.1%})",
        ),
        (
            "daily trading days complete",
            daily_cov >= COVERAGE_MIN,
            f"{len(daily_counts)} dates vs {days_prev} prior ({daily_cov:.1%})",
        ),
    ]
    # RF lags roughly a month by design; the build falls back to the last
    # available rate and records it in source_snapshot_manifest.json.
    return gates, f"ff.factors_monthly through {ff_max} (fallback used beyond this)"


def _report(target: date, gates: list[Gate], ff_note: str) -> None:
    print(f"Compustat readiness for month end {target}\n")
    for name, ok, detail in gates:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:32s} {detail}")
    print(f"  [info] {ff_note}")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "target",
        nargs="?",
        type=date.fromisoformat,
        default=None,
        help="month end to check (default: the previous month end)",
    )
    parser.add_argument(
        "--wait-minutes",
        type=int,
        default=0,
        help="keep polling until ready, giving up after this many minutes (default: 0, one check)",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        help="minutes between polls while waiting (default: 10)",
    )
    args = parser.parse_args(argv)
    if args.target is None:
        args.target = _previous_month_end(date.today())
    if args.wait_minutes < 0:
        parser.error("--wait-minutes cannot be negative")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")
    return args


def main(argv: list[str] | None = None, *, sleep=time.sleep) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    # A deadline rather than an attempt count, so a slow query eats into the
    # budget instead of extending it past the hour the caller allowed for.
    deadline = time.monotonic() + args.wait_minutes * 60

    while True:
        gates, ff_note = evaluate(args.target)
        _report(args.target, gates, ff_note)
        if all(ok for _, ok, _ in gates):
            print("\nREADY - safe to launch")
            return 0
        if time.monotonic() + args.poll_interval * 60 > deadline:
            break
        print(f"\nNOT READY - retrying in {args.poll_interval} min\n")
        sleep(args.poll_interval * 60)

    print("\nNOT READY - do not launch")
    if args.wait_minutes:
        print(f"Gave up after waiting {args.wait_minutes} minutes.")
    print("The feed usually completes a month 1-3 business days after the last")
    print("trading day. Re-run this check before starting the build.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
