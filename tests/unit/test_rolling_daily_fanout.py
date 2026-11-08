"""Regression tests for the concurrent roll_apply_daily fan-out."""

from __future__ import annotations

import csv
import threading
from datetime import date

import pytest

import jkp.data.main as main
from jkp.data.config import ROLLING_DAILY_SPECS

pytestmark = pytest.mark.unit


def _expected_jobs() -> set[tuple[str, str, int]]:
    return {(var, sfx, min_obs) for sfx, min_obs, vars_ in ROLLING_DAILY_SPECS for var in vars_}


@pytest.mark.parametrize("workers", [1, 4])
def test_every_window_variable_pair_runs_exactly_once(monkeypatch, workers) -> None:
    """Sequential and concurrent paths must cover the same job set."""
    seen: list[tuple[str, str, int]] = []
    labels: list[str] = []
    lock = threading.Lock()

    # _step_name mirrors the real roll_apply_daily, whose @measure_time wrapper
    # consumes the kwarg; a double without it would not stand in for the original.
    def fake(paths, var, sfx, min_obs, end_date, _step_name=None):  # noqa: ARG001
        with lock:
            seen.append((var, sfx, min_obs))
            labels.append(_step_name)

    monkeypatch.setattr(main, "roll_apply_daily", fake)
    monkeypatch.setattr(main, "ROLLING_DAILY_WORKERS", workers)

    main._run_rolling_daily(object(), date(2026, 6, 30))

    assert len(seen) == len(_expected_jobs())
    assert set(seen) == _expected_jobs()
    # Both paths label every job, and no two jobs share a label.
    assert len(set(labels)) == len(_expected_jobs())
    assert all(label.startswith("roll_apply_daily[") for label in labels)


def test_output_paths_are_unique_per_job() -> None:
    """The fan-out is only safe because no two jobs share an output file."""
    names = [f"__roll{sfx}_{var}.parquet" for sfx, _, vars_ in ROLLING_DAILY_SPECS for var in vars_]
    assert len(names) == len(set(names))


def test_failure_propagates_after_all_jobs_settle(monkeypatch) -> None:
    """One failing window must surface, and must not cancel the siblings.

    Letting every future settle first keeps the merge step from ever seeing a
    partially written window set.
    """
    completed: list[str] = []
    lock = threading.Lock()

    def fake(paths, var, sfx, min_obs, end_date, _step_name=None):  # noqa: ARG001
        if var == "rvol" and sfx == "_21d":
            raise RuntimeError("boom")
        with lock:
            completed.append(f"{sfx}_{var}")

    monkeypatch.setattr(main, "roll_apply_daily", fake)
    monkeypatch.setattr(main, "ROLLING_DAILY_WORKERS", 4)

    with pytest.raises(RuntimeError, match="boom"):
        main._run_rolling_daily(object(), date(2026, 6, 30))

    assert len(completed) == len(_expected_jobs()) - 1


def test_concurrent_step_rows_are_not_interleaved(tmp_path) -> None:
    """Concurrent step_finished calls produce complete, well-formed CSV rows.

    step_timings.csv is where the run timings are read from, so parallel
    steps must not lose or split rows. Note this asserts the invariant rather
    than proving the lock in _append_step_row is what upholds it: short rows
    append atomically even unlocked, so the test still passes without it. The
    lock guards the case this cannot cheaply reproduce — a failed step whose
    serialized error message is large enough to be split mid-write.
    """
    from jkp.data.runtime_monitor import PipelineRunMonitor

    monitor = PipelineRunMonitor(tmp_path)
    monitor.start()
    try:

        def worker(index: int) -> None:
            for _ in range(25):
                token = monitor.step_started(f"step_{index}")
                monitor.step_finished(token)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        monitor.stop()

    with monitor.steps_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    header, body = rows[0], rows[1:]
    assert len(body) == 8 * 25
    assert all(len(row) == len(header) for row in body)
