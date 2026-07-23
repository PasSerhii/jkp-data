from __future__ import annotations

import csv
import json
import time

import pytest

from jkp.data.runtime_monitor import PIPELINE_PHASES, PipelineRunMonitor, monitor_pipeline


def _config() -> dict[str, object]:
    return {
        "start_date": "2000-01-01",
        "end_date": "2026-06-30",
        "bypass_crsp": True,
        "production_output": True,
        "compustat_source": "xpressfeed",
    }


def test_monitor_persists_successful_run_metrics_steps_and_history(tmp_path) -> None:
    monitor = PipelineRunMonitor(tmp_path, sample_interval_seconds=0.01)
    monitor.start()
    monitor.configure(**_config())
    for phase in PIPELINE_PHASES:
        monitor.set_phase(phase)
        token = monitor.step_started(f"work_{phase}")
        time.sleep(0.002)
        monitor.step_finished(token)
    monitor.stop()

    summary = json.loads(monitor.summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "succeeded"
    assert summary["config"]["start_date"] == "2000-01-01"
    assert set(summary["phase_timings_seconds"]) == set(PIPELINE_PHASES)
    assert summary["host"]["logical_cpu_count"] >= 1

    with monitor.metrics_path.open(encoding="utf-8", newline="") as handle:
        metric_rows = list(csv.DictReader(handle))
    assert metric_rows
    assert {
        "system_cpu_percent",
        "cpu_iowait_percent",
        "process_cpu_cores",
        "ram_used_gib",
        "process_rss_gib",
        "disk_used_delta_gib",
        "system_disk_write_gib",
        "network_recv_gib",
    }.issubset(metric_rows[0])

    with monitor.steps_path.open(encoding="utf-8", newline="") as handle:
        step_rows = list(csv.DictReader(handle))
    assert len(step_rows) == len(PIPELINE_PHASES)
    assert all(row["status"] == "completed" for row in step_rows)

    monitor.record_download(
        event_type="batch",
        table="comp.secd",
        status="completed",
        started_at_utc="2026-07-23T00:00:00+00:00",
        duration_seconds=2.0,
        batch_number=1,
        total_batches=4,
        worker_id=2,
        pair_count=250,
        row_count=1_000,
        bytes_written=2 * 1024 * 1024,
        retries=1,
        timeouts=1,
        completed_batches=1,
        table_completion_percent=25.0,
        overall_completion_percent=12.5,
    )
    with monitor.downloads_path.open(encoding="utf-8", newline="") as handle:
        download_rows = list(csv.DictReader(handle))
    assert download_rows == [
        {
            "timestamp_utc": download_rows[0]["timestamp_utc"],
            "event_type": "batch",
            "table": "comp.secd",
            "status": "completed",
            "started_at_utc": "2026-07-23T00:00:00+00:00",
            "duration_seconds": "2.0",
            "batch_number": "1",
            "total_batches": "4",
            "worker_id": "2",
            "pair_count": "250",
            "row_count": "1000",
            "bytes_written": str(2 * 1024 * 1024),
            "mib_written": "2.0",
            "mib_per_second": "1.0",
            "rows_per_second": "500.0",
            "retries": "1",
            "timeouts": "1",
            "completed_batches": "1",
            "table_completion_percent": "25.0",
            "overall_completion_percent": "12.5",
            "error_type": "",
        }
    ]

    history = json.loads(monitor.history_path.read_text(encoding="utf-8"))
    assert set(history["phases"]) == set(PIPELINE_PHASES)
    assert monitor.latest_path.read_text(encoding="ascii").strip() == monitor.run_id
    assert "RUN SUCCEEDED" in monitor.pipeline_log_path.read_text(encoding="utf-8")


def test_monitor_pipeline_records_uncaught_failure(tmp_path) -> None:
    @monitor_pipeline
    def failing_pipeline(*, output_dir, metrics_interval_seconds=60.0) -> None:
        del output_dir, metrics_interval_seconds
        raise RuntimeError("intentional failure")

    with pytest.raises(RuntimeError, match="intentional failure"):
        failing_pipeline(output_dir=tmp_path, metrics_interval_seconds=0.01)

    run_id = (tmp_path / "run_logs" / "latest_run.txt").read_text(encoding="ascii").strip()
    summary = json.loads(
        (tmp_path / "run_logs" / run_id / "run_summary.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "failed"
    assert summary["error"] == {
        "type": "RuntimeError",
        "message": "intentional failure",
    }
    assert not (tmp_path / "run_logs" / "timing_history.json").exists()
