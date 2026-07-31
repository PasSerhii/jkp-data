"""Step nesting, parallel fan-out instrumentation, and memory reporting.

Covers the defects found in the 2026-07-29 run: the 19 rolling-daily jobs ran
on ThreadPoolExecutor threads where the monitor ContextVar did not propagate, so
none of them was recorded, while nested steps under ``export_production`` were
counted twice. The two errors nearly cancelled, so the totals looked healthy.
"""

from __future__ import annotations

import contextvars
import csv
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest

from jkp.data.aux_functions import measure_time
from jkp.data.config import ROLLING_DAILY_SPECS, ROLLING_DAILY_WORKERS
from jkp.data.main import _roll_label
from jkp.data.runtime_monitor import _ACTIVE_MONITOR, PipelineRunMonitor

pytestmark = pytest.mark.unit


def _rows(monitor: PipelineRunMonitor) -> list[dict[str, str]]:
    with monitor.steps_path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _started(tmp_path) -> PipelineRunMonitor:
    monitor = PipelineRunMonitor(tmp_path, sample_interval_seconds=3600)
    monitor.start()
    return monitor


class TestStepHierarchy:
    def test_nested_steps_record_parent_and_depth(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        outer = monitor.step_started("export_production")
        inner = monitor.step_started("save_main_production_csv")
        monitor.step_finished(inner)
        monitor.step_finished(outer)
        monitor.stop()

        by_name = {row["step"]: row for row in _rows(monitor)}
        assert by_name["export_production"]["step_depth"] == "0"
        assert by_name["export_production"]["parent_step_token"] == ""
        assert by_name["save_main_production_csv"]["step_depth"] == "1"
        assert by_name["save_main_production_csv"]["parent_step_token"] == str(outer)

    def test_top_level_totals_match_phase_duration(self, tmp_path) -> None:
        """The bug this exists to catch: summing every row double-counts children."""
        monitor = _started(tmp_path)
        monitor.set_phase("final_outputs")
        outer = monitor.step_started("export_production")
        for _ in range(3):
            inner = monitor.step_started("child")
            time.sleep(0.01)
            monitor.step_finished(inner)
        monitor.step_finished(outer)
        monitor.stop()

        rows = _rows(monitor)
        every_row = sum(float(row["duration_seconds"]) for row in rows)
        top_level = sum(float(r["duration_seconds"]) for r in rows if r["step_depth"] == "0")
        phase = json.loads(monitor.summary_path.read_text(encoding="utf-8"))[
            "phase_timings_seconds"
        ]["final_outputs"]

        assert top_level == pytest.approx(phase, abs=0.2)
        # Summing indiscriminately roughly doubles it, which is the old behaviour.
        assert every_row > top_level * 1.5

    def test_sibling_steps_are_both_top_level(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        for name in ("first", "second"):
            token = monitor.step_started(name)
            monitor.step_finished(token)
        monitor.stop()
        assert [row["step_depth"] for row in _rows(monitor)] == ["0", "0"]

    def test_failed_child_does_not_strand_the_stack(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        outer = monitor.step_started("parent")
        inner = monitor.step_started("child")
        monitor.step_finished(inner, RuntimeError("boom"))
        after = monitor.step_started("sibling")
        monitor.step_finished(after)
        monitor.step_finished(outer)
        monitor.stop()

        by_name = {row["step"]: row for row in _rows(monitor)}
        assert by_name["child"]["status"] == "failed"
        # The sibling must nest under parent, not under the failed child.
        assert by_name["sibling"]["parent_step_token"] == str(outer)
        assert by_name["sibling"]["step_depth"] == "1"

    def test_existing_columns_keep_their_positions(self, tmp_path) -> None:
        """status.sh and the comparison scripts read these by index."""
        monitor = _started(tmp_path)
        monitor.stop()
        with monitor.steps_path.open(encoding="utf-8", newline="") as handle:
            header = next(csv.reader(handle))
        assert header[:9] == [
            "step_token",
            "phase",
            "step",
            "started_at_utc",
            "finished_at_utc",
            "duration_seconds",
            "status",
            "error_type",
            "error_message",
        ]
        assert header[9:] == ["parent_step_token", "step_depth"]


class TestRollingFanout:
    def test_every_rolling_job_has_a_unique_label(self) -> None:
        jobs = [(v, s, m) for s, m, vs in ROLLING_DAILY_SPECS for v in vs]
        labels = [_roll_label(*job) for job in jobs]
        assert len(jobs) == 19
        assert len(set(labels)) == 19

    def test_repeated_variable_across_windows_stays_distinct(self) -> None:
        """zero_trades runs over 21d, 126d and 252d; the bare name cannot tell them apart."""
        jobs = [(v, s, m) for s, m, vs in ROLLING_DAILY_SPECS for v in vs if v == "zero_trades"]
        assert len(jobs) == 3
        assert len({_roll_label(*job) for job in jobs}) == 3

    def test_labels_contain_no_comma(self) -> None:
        """awk -F, in status.sh splits on every comma regardless of CSV quoting."""
        for spec in ROLLING_DAILY_SPECS:
            for var in spec[2]:
                assert "," not in _roll_label(var, spec[0], spec[1])

    def test_workers_record_steps_under_the_fanout_parent(self, tmp_path) -> None:
        """Without copy_context the ContextVar is unset in workers and nothing is recorded."""
        monitor = _started(tmp_path)
        monitor.set_phase("daily_characteristics")
        token = _ACTIVE_MONITOR.set(monitor)

        @measure_time
        def job(_n: int) -> None:
            time.sleep(0.01)

        parent = monitor.step_started("rolling_daily_fanout")
        with ThreadPoolExecutor(max_workers=ROLLING_DAILY_WORKERS) as pool:
            futures = [
                pool.submit(
                    contextvars.copy_context().run,
                    partial(job, n, _step_name=f"job[n={n}]"),
                )
                for n in range(19)
            ]
            for future in futures:
                future.result()
        monitor.step_finished(parent)
        _ACTIVE_MONITOR.reset(token)
        monitor.stop()

        rows = _rows(monitor)
        children = [row for row in rows if row["step"].startswith("job[")]
        assert len(children) == 19
        assert len({row["step"] for row in children}) == 19
        assert all(row["parent_step_token"] == str(parent) for row in children)
        assert all(row["step_depth"] == "1" for row in children)

        top_level = [row for row in rows if row["step_depth"] == "0"]
        assert [row["step"] for row in top_level] == ["rolling_daily_fanout"]

    def test_worker_failure_recorded_while_siblings_settle(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        token = _ACTIVE_MONITOR.set(monitor)

        @measure_time
        def job(n: int) -> None:
            if n == 2:
                raise ValueError("worker 2 failed")

        parent = monitor.step_started("rolling_daily_fanout")
        with ThreadPoolExecutor(max_workers=ROLLING_DAILY_WORKERS) as pool:
            futures = [
                pool.submit(
                    contextvars.copy_context().run,
                    partial(job, n, _step_name=f"job[n={n}]"),
                )
                for n in range(4)
            ]
            errors = [future.exception() for future in futures]
        first = next(item for item in errors if item is not None)
        monitor.step_finished(parent, first)
        _ACTIVE_MONITOR.reset(token)
        monitor.stop()

        by_name = {row["step"]: row for row in _rows(monitor)}
        assert by_name["job[n=2]"]["status"] == "failed"
        assert by_name["job[n=2]"]["error_type"] == "ValueError"
        # Siblings still complete, and the parent records the failure too.
        assert all(by_name[f"job[n={n}]"]["status"] == "completed" for n in (0, 1, 3))
        assert by_name["rolling_daily_fanout"]["status"] == "failed"

    def test_concurrent_log_lines_are_never_interleaved(self, tmp_path, capfd) -> None:
        monitor = _started(tmp_path)
        token = _ACTIVE_MONITOR.set(monitor)

        @measure_time
        def job(_n: int) -> None:
            time.sleep(0.005)

        with ThreadPoolExecutor(max_workers=ROLLING_DAILY_WORKERS) as pool:
            futures = [
                pool.submit(
                    contextvars.copy_context().run,
                    partial(job, n, _step_name=f"job[n={n}]"),
                )
                for n in range(19)
            ]
            for future in futures:
                future.result()
        _ACTIVE_MONITOR.reset(token)
        monitor.stop()
        capfd.readouterr()

        text = monitor.pipeline_log_path.read_text(encoding="utf-8")
        starts = re.findall(r"^.*STEP START (job\[n=\d+\]) .*$", text, flags=re.MULTILINE)
        done = re.findall(r"^.*STEP COMPLETED (job\[n=\d+\]) .*$", text, flags=re.MULTILINE)
        assert len(starts) == 19
        assert len(done) == 19
        # Every line is whole: no line carries two records spliced together.
        for line in text.splitlines():
            assert line.count("STEP START") <= 1
            assert line.count("STEP COMPLETED") <= 1


class TestMeasureTime:
    def test_preserves_function_metadata(self) -> None:
        @measure_time
        def documented_function() -> None:
            """Original docstring."""

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "Original docstring."

    def test_step_name_kwarg_is_not_passed_through(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        token = _ACTIVE_MONITOR.set(monitor)
        seen: dict[str, object] = {}

        @measure_time
        def job(**kwargs) -> None:
            seen.update(kwargs)

        job(real_kwarg=1, _step_name="renamed")
        _ACTIVE_MONITOR.reset(token)
        monitor.stop()

        assert seen == {"real_kwarg": 1}
        assert [row["step"] for row in _rows(monitor)] == ["renamed"]

    def test_defaults_to_function_name(self, tmp_path) -> None:
        monitor = _started(tmp_path)
        token = _ACTIVE_MONITOR.set(monitor)

        @measure_time
        def some_step() -> None:
            pass

        some_step()
        _ACTIVE_MONITOR.reset(token)
        monitor.stop()
        assert [row["step"] for row in _rows(monitor)] == ["some_step"]

    def test_unmonitored_output_is_one_line_per_event(self, capsys) -> None:
        @measure_time
        def job() -> None:
            pass

        job(_step_name="labelled")
        out = capsys.readouterr().out.strip().splitlines()
        assert len(out) == 2
        assert out[0].startswith("START labelled at ")
        assert out[1].startswith("END labelled in ")

    def test_unmonitored_failure_is_reported(self, capsys) -> None:
        @measure_time
        def job() -> None:
            raise ValueError("nope")

        with pytest.raises(ValueError):
            job(_step_name="doomed")
        out = capsys.readouterr().out
        assert "FAILED doomed" in out
        assert "type=ValueError" in out


class TestMemoryReporting:
    def test_metrics_carry_raw_rss_and_cgroup_columns(self, tmp_path) -> None:
        monitor = PipelineRunMonitor(tmp_path, sample_interval_seconds=0.01)
        monitor.start()
        time.sleep(0.05)
        monitor.stop()

        with monitor.metrics_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert rows
        header = rows[0].keys()
        assert "process_rss_raw_gib" in header
        assert "children_rss_raw_gib" in header
        assert "cgroup_memory_current_gib" in header
        assert "cgroup_memory_limit_gib" in header
        # The misleading name must be gone, not merely supplemented.
        assert "process_rss_gib" not in header

    def test_summary_reports_available_ram_floor(self, tmp_path) -> None:
        monitor = PipelineRunMonitor(tmp_path, sample_interval_seconds=0.01)
        monitor.start()
        time.sleep(0.05)
        monitor.stop()

        peaks = json.loads(monitor.summary_path.read_text(encoding="utf-8"))["peaks"]
        assert peaks["ram_available_gib_min"] > 0
        assert "process_rss_raw_gib" in peaks
        assert "process_rss_gib" not in peaks

    def test_cgroup_reader_tolerates_absence(self) -> None:
        """Off cgroup v2 (Windows, macOS, cgroup v1) both values are simply unknown."""
        from jkp.data.runtime_monitor import _cgroup_field, _read_cgroup_memory

        current, limit = _read_cgroup_memory()
        assert current is None or current >= 0
        assert limit is None or limit > 0
        assert _cgroup_field(None, None) == ""
        assert _cgroup_field(1.5, None) == " cgroup_mem=1.5GiB"
        assert _cgroup_field(1.5, 8.0) == " cgroup_mem=1.5GiB/8.0GiB"
