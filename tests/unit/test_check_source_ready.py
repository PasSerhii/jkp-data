"""Bounded feed-readiness polling for the unattended monthly run.

The attended path asks once and lets a human retry. The unattended run starts at
a fixed hour with nobody watching, so it polls -- but a poll with no ceiling
turns a late delivery into an instance idling until someone notices the bill.
These tests pin the ceiling, the attempt cadence, and the guarantee that the
default stays single-shot for every existing caller.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_source_ready.py"


def _load_module():
    """Import the script by path; scripts/ is not a package."""
    spec = importlib.util.spec_from_file_location("check_source_ready", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def mod():
    return _load_module()


@pytest.fixture
def clock(mod, monkeypatch):
    """A monotonic clock that only advances when the code sleeps."""
    now = {"t": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: now["t"])
    return now


def _stub_evaluate(mod, monkeypatch, verdicts):
    """Feed ``evaluate`` a scripted sequence of ready/not-ready results."""
    calls: list[date] = []
    remaining = list(verdicts)

    def fake(target):
        calls.append(target)
        ready = remaining.pop(0) if remaining else remaining_default
        return [("daily prices past month end", ready, "stub")], "ff stub"

    remaining_default = verdicts[-1]
    monkeypatch.setattr(mod, "evaluate", fake)
    return calls


class TestSingleShot:
    def test_default_asks_once_and_never_sleeps(self, mod, monkeypatch, clock):
        calls = _stub_evaluate(mod, monkeypatch, [False])
        slept: list[float] = []

        rc = mod.main(["2026-07-31"], sleep=slept.append)

        assert rc == 1
        assert len(calls) == 1
        assert slept == []

    def test_ready_feed_exits_zero(self, mod, monkeypatch, clock):
        _stub_evaluate(mod, monkeypatch, [True])
        assert mod.main(["2026-07-31"], sleep=lambda _: None) == 0

    def test_explicit_target_is_used_verbatim(self, mod, monkeypatch, clock):
        calls = _stub_evaluate(mod, monkeypatch, [True])
        mod.main(["2026-02-28"], sleep=lambda _: None)
        assert calls == [date(2026, 2, 28)]

    def test_no_target_defaults_to_previous_month_end(self, mod, monkeypatch, clock):
        calls = _stub_evaluate(mod, monkeypatch, [True])
        mod.main([], sleep=lambda _: None)
        assert calls == [mod._previous_month_end(date.today())]


class TestBoundedPoll:
    def test_stops_as_soon_as_the_feed_lands(self, mod, monkeypatch, clock):
        calls = _stub_evaluate(mod, monkeypatch, [False, False, True])
        slept: list[float] = []

        def sleep(seconds):
            slept.append(seconds)
            clock["t"] += seconds

        rc = mod.main(["2026-07-31", "--wait-minutes", "60"], sleep=sleep)

        assert rc == 0
        assert len(calls) == 3
        assert slept == [600.0, 600.0]

    def test_gives_up_inside_the_budget(self, mod, monkeypatch, clock):
        """60 minutes at 10-minute steps: checks at 0..60, then stops."""
        calls = _stub_evaluate(mod, monkeypatch, [False])
        start = clock["t"]

        def sleep(seconds):
            clock["t"] += seconds

        rc = mod.main(["2026-07-31", "--wait-minutes", "60"], sleep=sleep)

        assert rc == 1
        assert len(calls) == 7
        # Never overruns the ceiling the caller paid for.
        assert clock["t"] - start == 3600.0

    def test_never_sleeps_past_the_deadline(self, mod, monkeypatch, clock):
        """A budget that is not a whole multiple of the interval still stops early."""
        _stub_evaluate(mod, monkeypatch, [False])
        start = clock["t"]

        def sleep(seconds):
            clock["t"] += seconds

        mod.main(["2026-07-31", "--wait-minutes", "25", "--poll-interval", "10"], sleep=sleep)

        assert clock["t"] - start <= 25 * 60

    def test_slow_query_eats_the_budget_rather_than_extending_it(self, mod, monkeypatch, clock):
        """Each evaluate() costs 8 minutes, so far fewer polls fit the same hour."""
        calls: list[date] = []

        def slow(target):
            calls.append(target)
            clock["t"] += 8 * 60
            return [("gate", False, "stub")], "ff stub"

        monkeypatch.setattr(mod, "evaluate", slow)

        def sleep(seconds):
            clock["t"] += seconds

        start = clock["t"]
        mod.main(["2026-07-31", "--wait-minutes", "60"], sleep=sleep)

        assert len(calls) == 4
        assert clock["t"] - start <= 3600 + 8 * 60


class TestDailyCompleteness:
    """The month's last real trading day must be present and carry a full cross-section.

    The predecessor asked whether the feed's frontier had moved *past* the month
    end. It cannot: the last trading day generally *is* the month end, so that
    test failed every month on the 1st -- exactly when the monthly build runs.
    July 2026 was the case that exposed it: 105,367 rows on 2026-07-31, the
    highest of the fortnight, and the gate still said NOT READY.
    """

    @staticmethod
    def _month(mod, days, rows=105_000, sundays=()):
        """Per-date counts for July 2026; Sundays carry ~1.3% of a weekday."""
        out = []
        for d in days:
            day = date(2026, 7, d)
            out.append((day, 1_400 if d in sundays else rows))
        return out

    def test_month_ending_on_a_trading_day_is_ready(self, mod):
        counts = self._month(mod, [27, 28, 29, 30, 31], sundays=(26,))
        name, ok, detail = mod.assess_daily_month(counts, date(2026, 7, 31))
        assert ok, detail

    def test_the_july_2026_case_that_was_wrongly_rejected(self, mod):
        counts = self._month(mod, [24, 26, 27, 28, 29, 30, 31], sundays=(26,))
        counts[-1] = (date(2026, 7, 31), 105_367)
        _, ok, _ = mod.assess_daily_month(counts, date(2026, 7, 31))
        assert ok

    def test_sundays_do_not_drag_the_median_down(self, mod):
        """A thin Sunday must not become the reference for a "full" day."""
        counts = self._month(mod, [19, 20, 21, 26, 27, 28], sundays=(19, 26))
        _, ok, detail = mod.assess_daily_month(counts, date(2026, 7, 28))
        assert ok, detail

    def test_month_ending_at_a_weekend_uses_the_last_trading_day(self, mod):
        """2026-08-31 is a Monday; construct a Saturday month end instead."""
        counts = [(date(2026, 7, d), 105_000) for d in (28, 29, 30, 31)]
        # pretend the month ends Sunday 2026-08-02, two days after the last full day
        _, ok, detail = mod.assess_daily_month(counts, date(2026, 8, 2))
        assert ok, detail

    def test_feed_stalled_mid_month_is_rejected(self, mod):
        counts = self._month(mod, [20, 21, 22, 23, 24])
        _, ok, detail = mod.assess_daily_month(counts, date(2026, 7, 31))
        assert not ok
        assert "7d before month end" in detail

    def test_partially_delivered_final_day_is_rejected(self, mod):
        """The date exists but only a slice of the universe has landed."""
        counts = self._month(mod, [28, 29, 30])
        counts.append((date(2026, 7, 31), 60_000))  # 57% of median
        _, ok, detail = mod.assess_daily_month(counts, date(2026, 7, 31))
        assert not ok
        assert "57% of median" in detail

    def test_empty_month_is_rejected(self, mod):
        _, ok, detail = mod.assess_daily_month([], date(2026, 7, 31))
        assert not ok
        assert "no daily rows" in detail


class TestArguments:
    @pytest.mark.parametrize(
        "argv",
        [
            ["--wait-minutes", "-1"],
            ["--poll-interval", "0"],
            ["--poll-interval", "-5"],
            ["not-a-date"],
        ],
    )
    def test_rejected(self, mod, argv):
        with pytest.raises(SystemExit) as excinfo:
            mod._parse_args(argv)
        assert excinfo.value.code == 2

    def test_defaults_are_single_shot_ten_minute(self, mod):
        args = mod._parse_args([])
        assert args.wait_minutes == 0
        assert args.poll_interval == 10
