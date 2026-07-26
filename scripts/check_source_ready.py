"""Decide whether the Compustat feed is complete enough to run a monthly build.

    uv run python scripts/check_source_ready.py                 # previous month end
    uv run python scripts/check_source_ready.py 2026-07-31      # explicit target

Exits 0 when every gate passes, 1 otherwise, so it can gate an automated run.

Why not just look at ``max(datadate)``: the monthly table ``sec_mth`` stamps rows
with the month-end date as they arrive, so it reports the *current* month end
from the first day of that month while holding only a fraction of the universe.
Trusting it would silently build a month with most securities missing. The
decisive signal is the securities count at the target month end measured against
the month before, combined with the daily frontier having reached the month's
last trading day.
"""

from __future__ import annotations

import sys
from datetime import date

import duckdb

from jkp.data.database_sources import get_xpressfeed_connection_info

# A completed month must hold at least this share of the previous month's rows /
# trading dates. Real month-over-month drift is well under 5%.
COVERAGE_MIN = 0.95


def _previous_month_end(today: date) -> date:
    return today.replace(day=1) - __import__("datetime").timedelta(days=1)


def main() -> int:
    target = (
        date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else _previous_month_end(date.today())
    )
    prev = _previous_month_end(target.replace(day=1))
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{get_xpressfeed_connection_info()}' AS src (TYPE postgres, READ_ONLY)")

    def scalar(sql: str):
        return con.sql(f"SELECT * FROM postgres_query('src', $q${sql}$q$)").fetchone()[0]

    daily_max = scalar("SELECT max(datadate) FROM public.sec_dprc")
    fx_max = scalar("SELECT max(datadate) FROM public.exrt_dly")
    ff_max = scalar("SELECT max(date) FROM ff.factors_monthly")
    rows_target = scalar(f"SELECT count(*) FROM public.sec_mth WHERE datadate = date '{target}'")
    rows_prev = scalar(f"SELECT count(*) FROM public.sec_mth WHERE datadate = date '{prev}'")
    days_target = scalar(
        "SELECT count(DISTINCT datadate) FROM public.sec_dprc "
        f"WHERE datadate BETWEEN date '{target.replace(day=1)}' AND date '{target}'"
    )
    days_prev = scalar(
        "SELECT count(DISTINCT datadate) FROM public.sec_dprc "
        f"WHERE datadate BETWEEN date '{prev.replace(day=1)}' AND date '{prev}'"
    )

    daily_d = daily_max.date() if hasattr(daily_max, "date") else daily_max
    fx_d = fx_max.date() if hasattr(fx_max, "date") else fx_max
    monthly_cov = rows_target / rows_prev if rows_prev else 0.0
    daily_cov = days_target / days_prev if days_prev else 0.0

    gates = [
        # The feed must have moved past the target month end; that is the only
        # proof the month's final trading day has been delivered without needing
        # an exchange calendar.
        ("daily prices past month end", daily_d > target, f"max {daily_d} vs target {target}"),
        ("FX past month end", fx_d > target, f"max {fx_d} (gates USD conversion)"),
        (
            "monthly universe complete",
            monthly_cov >= COVERAGE_MIN,
            f"{rows_target:,} rows vs {rows_prev:,} prior ({monthly_cov:.1%})",
        ),
        (
            "daily trading days complete",
            daily_cov >= COVERAGE_MIN,
            f"{days_target} dates vs {days_prev} prior ({daily_cov:.1%})",
        ),
    ]

    print(f"Compustat readiness for month end {target}\n")
    for name, ok, detail in gates:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:32s} {detail}")
    # RF lags roughly a month by design; the build falls back to the last
    # available rate and records it in source_snapshot_manifest.json.
    print(f"  [info] ff.factors_monthly through {ff_max} (fallback used beyond this)")

    ready = all(ok for _, ok, _ in gates)
    print(f"\n{'READY - safe to launch' if ready else 'NOT READY - do not launch'}")
    if not ready:
        print("The feed usually completes a month 1-3 business days after the last")
        print("trading day. Re-run this check before starting the build.")
    return 0 if ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
