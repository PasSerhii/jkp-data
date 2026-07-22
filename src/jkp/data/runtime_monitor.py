"""Persistent progress and resource monitoring for long pipeline runs."""

from __future__ import annotations

import contextvars
import csv
import functools
import json
import os
import platform
import socket
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar

import psutil

PIPELINE_PHASES = (
    "initialization",
    "source_download",
    "security_panels",
    "market_returns",
    "accounting_characteristics",
    "factor_models",
    "daily_characteristics",
    "final_outputs",
)

_F = TypeVar("_F", bound=Callable[..., Any])
_ACTIVE_MONITOR: contextvars.ContextVar[PipelineRunMonitor | None] = contextvars.ContextVar(
    "jkp_active_pipeline_monitor", default=None
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "unavailable"
    seconds = max(0, round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes:d}m {secs:02d}s"
    return f"{secs:d}s"


def _gib(value: int | float) -> float:
    return float(value) / (1024**3)


@dataclass
class _Step:
    token: int
    name: str
    phase: str | None
    started_monotonic: float
    started_at: str


class PipelineRunMonitor:
    """Write durable progress, timing, and host/process resource telemetry."""

    def __init__(self, output_dir: Path, sample_interval_seconds: float = 60.0) -> None:
        if sample_interval_seconds <= 0:
            raise ValueError("sample_interval_seconds must be positive")

        self.output_dir = output_dir.resolve()
        self.logs_root = self.output_dir / "run_logs"
        run_stamp = _utc_now().strftime("%Y%m%dT%H%M%S%fZ")
        self.run_id = f"{run_stamp}-pid{os.getpid()}"
        self.run_dir = self.logs_root / self.run_id
        self.pipeline_log_path = self.run_dir / "pipeline.log"
        self.metrics_path = self.run_dir / "resource_metrics.csv"
        self.steps_path = self.run_dir / "step_timings.csv"
        self.summary_path = self.run_dir / "run_summary.json"
        self.history_path = self.logs_root / "timing_history.json"
        self.latest_path = self.logs_root / "latest_run.txt"
        self.sample_interval_seconds = sample_interval_seconds

        self._started_at = _utc_now()
        self._started_monotonic = time.monotonic()
        self._process = psutil.Process()
        self._stop_event = threading.Event()
        self._sampler: threading.Thread | None = None
        self._lock = threading.RLock()
        self._phase: str | None = None
        self._phase_started_monotonic: float | None = None
        self._phase_timings: dict[str, float] = {}
        self._steps: list[_Step] = []
        self._step_counter = 0
        self._config: dict[str, Any] = {}
        self._history = self._read_history()
        self._history_compatible = False
        self._peaks: dict[str, float] = {}
        self._status = "running"
        self._error: dict[str, str] | None = None
        self._baseline_disk_used = 0
        self._baseline_network_recv = 0
        self._baseline_network_sent = 0
        self._baseline_disk_read = 0
        self._baseline_disk_write = 0

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._started_monotonic

    def start(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self.latest_path.write_text(f"{self.run_id}\n", encoding="ascii")
        self._baseline_disk_used = psutil.disk_usage(str(self.output_dir)).used
        initial_network = psutil.net_io_counters()
        self._baseline_network_recv = initial_network.bytes_recv
        self._baseline_network_sent = initial_network.bytes_sent
        initial_disk_io = psutil.disk_io_counters()
        if initial_disk_io is not None:
            self._baseline_disk_read = initial_disk_io.read_bytes
            self._baseline_disk_write = initial_disk_io.write_bytes
        self._initialize_csv_files()
        self._process.cpu_percent(None)
        psutil.cpu_percent(None)
        psutil.cpu_times_percent(None)
        self._log(
            "RUN START "
            f"id={self.run_id} output={self.output_dir} "
            f"metrics_interval={self.sample_interval_seconds:g}s"
        )
        self._write_summary()
        self._sample_resources()
        self._sampler = threading.Thread(
            target=self._sampling_loop,
            name="jkp-resource-monitor",
            daemon=True,
        )
        self._sampler.start()

    def configure(self, **config: Any) -> None:
        serializable = {
            key: value.isoformat() if hasattr(value, "isoformat") else value
            for key, value in config.items()
        }
        with self._lock:
            self._config.update(serializable)
            history_config = self._history.get("config", {})
            compatibility_keys = (
                "start_date",
                "bypass_crsp",
                "production_output",
                "compustat_source",
                "reuse_raw",
            )
            self._history_compatible = bool(history_config) and all(
                history_config.get(key) == self._config.get(key) for key in compatibility_keys
            )
        self._log(
            "RUN CONFIG "
            + " ".join(f"{key}={value}" for key, value in serializable.items())
        )
        if not self._history_compatible:
            self._log("ETA unavailable until a comparable run completes successfully")
        self._write_summary()

    def set_phase(self, phase: str) -> None:
        if phase not in PIPELINE_PHASES:
            raise ValueError(f"Unknown pipeline phase: {phase}")
        with self._lock:
            self._finish_current_phase_locked()
            self._phase = phase
            self._phase_started_monotonic = time.monotonic()
            phase_number = PIPELINE_PHASES.index(phase) + 1
            eta = self._estimated_remaining_locked()
        self._log(
            f"PHASE {phase_number}/{len(PIPELINE_PHASES)} START {phase} "
            f"elapsed={_format_duration(self.elapsed_seconds)} "
            f"estimated_remaining={_format_duration(eta)}"
        )
        self._write_summary()

    def note(self, message: str) -> None:
        """Persist a human-readable progress event and mirror it to stdout."""
        self._log(f"PROGRESS {message}")

    def step_started(self, name: str) -> int:
        with self._lock:
            self._step_counter += 1
            step = _Step(
                token=self._step_counter,
                name=name,
                phase=self._phase,
                started_monotonic=time.monotonic(),
                started_at=_utc_now().isoformat(),
            )
            self._steps.append(step)
        self._log(f"STEP START {name} phase={step.phase or 'unassigned'}")
        return step.token

    def step_finished(self, token: int, error: BaseException | None = None) -> None:
        with self._lock:
            step = next((item for item in reversed(self._steps) if item.token == token), None)
            if step is None:
                return
            self._steps.remove(step)
            duration = time.monotonic() - step.started_monotonic
        status = "failed" if error is not None else "completed"
        self._append_step_row(step, duration, status, error)
        self._log(
            f"STEP {status.upper()} {step.name} duration={_format_duration(duration)} "
            f"overall_elapsed={_format_duration(self.elapsed_seconds)}"
        )

    def stop(self, error: BaseException | None = None) -> None:
        self._stop_event.set()
        if self._sampler is not None:
            self._sampler.join(timeout=min(5.0, self.sample_interval_seconds + 1.0))
        self._sample_resources()
        with self._lock:
            self._finish_current_phase_locked()
            if error is None:
                self._status = "succeeded"
            else:
                self._status = "failed"
                self._error = {"type": type(error).__name__, "message": str(error)}
        self._write_summary()
        if error is None:
            self._write_history()
        self._log(
            f"RUN {self._status.upper()} elapsed={_format_duration(self.elapsed_seconds)} "
            f"summary={self.summary_path}"
        )

    def _log(self, message: str) -> None:
        timestamp = _utc_now().isoformat(timespec="seconds")
        line = f"{timestamp} elapsed={_format_duration(self.elapsed_seconds)} {message}"
        with self._lock, self.pipeline_log_path.open(
            "a", encoding="utf-8", newline=""
        ) as handle:
            handle.write(line + "\n")
            handle.flush()
        print(line, flush=True)

    def _initialize_csv_files(self) -> None:
        with self.metrics_path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerow(
                (
                    "timestamp_utc",
                    "elapsed_seconds",
                    "phase",
                    "step",
                    "system_cpu_percent",
                    "cpu_iowait_percent",
                    "process_cpu_percent",
                    "process_cpu_cores",
                    "process_threads",
                    "load_1m",
                    "load_5m",
                    "load_15m",
                    "ram_total_gib",
                    "ram_used_gib",
                    "ram_available_gib",
                    "ram_percent",
                    "process_rss_gib",
                    "children_rss_gib",
                    "swap_used_gib",
                    "disk_total_gib",
                    "disk_used_gib",
                    "disk_used_delta_gib",
                    "disk_free_gib",
                    "process_read_gib",
                    "process_write_gib",
                    "system_disk_read_gib",
                    "system_disk_write_gib",
                    "network_recv_gib",
                    "network_sent_gib",
                )
            )
        with self.steps_path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerow(
                (
                    "step_token",
                    "phase",
                    "step",
                    "started_at_utc",
                    "finished_at_utc",
                    "duration_seconds",
                    "status",
                    "error_type",
                    "error_message",
                )
            )

    def _sampling_loop(self) -> None:
        while not self._stop_event.wait(self.sample_interval_seconds):
            self._sample_resources()

    def _sample_resources(self) -> None:
        try:
            with self._lock:
                phase = self._phase or ""
                step = self._steps[-1].name if self._steps else ""
            system_cpu = psutil.cpu_percent(None)
            cpu_times = psutil.cpu_times_percent(None)
            cpu_iowait = float(getattr(cpu_times, "iowait", 0.0))
            process_cpu = self._process.cpu_percent(None)
            process_threads = self._process.num_threads()
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            process_rss = self._process.memory_info().rss
            children_rss = 0
            for child in self._process.children(recursive=True):
                try:
                    children_rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            disk = psutil.disk_usage(str(self.output_dir))
            io = self._process.io_counters()
            disk_io = psutil.disk_io_counters()
            network = psutil.net_io_counters()
            try:
                load_1m, load_5m, load_15m = os.getloadavg()
            except (AttributeError, OSError):
                load_1m = load_5m = load_15m = 0.0

            row = (
                _utc_now().isoformat(),
                round(self.elapsed_seconds, 3),
                phase,
                step,
                round(system_cpu, 3),
                round(cpu_iowait, 3),
                round(process_cpu, 3),
                round(process_cpu / 100.0, 3),
                process_threads,
                round(load_1m, 3),
                round(load_5m, 3),
                round(load_15m, 3),
                round(_gib(memory.total), 3),
                round(_gib(memory.used), 3),
                round(_gib(memory.available), 3),
                round(memory.percent, 3),
                round(_gib(process_rss), 3),
                round(_gib(children_rss), 3),
                round(_gib(swap.used), 3),
                round(_gib(disk.total), 3),
                round(_gib(disk.used), 3),
                round(_gib(max(0, disk.used - self._baseline_disk_used)), 3),
                round(_gib(disk.free), 3),
                round(_gib(io.read_bytes), 3),
                round(_gib(io.write_bytes), 3),
                round(
                    _gib(max(0, disk_io.read_bytes - self._baseline_disk_read))
                    if disk_io is not None
                    else 0.0,
                    3,
                ),
                round(
                    _gib(max(0, disk_io.write_bytes - self._baseline_disk_write))
                    if disk_io is not None
                    else 0.0,
                    3,
                ),
                round(_gib(max(0, network.bytes_recv - self._baseline_network_recv)), 3),
                round(_gib(max(0, network.bytes_sent - self._baseline_network_sent)), 3),
            )
            with self.metrics_path.open("a", encoding="utf-8", newline="") as handle:
                csv.writer(handle).writerow(row)
                handle.flush()
            with self._lock:
                self._update_peak("system_cpu_percent", system_cpu)
                self._update_peak("cpu_iowait_percent", cpu_iowait)
                self._update_peak("process_cpu_percent", process_cpu)
                self._update_peak("ram_used_gib", _gib(memory.used))
                self._update_peak("process_rss_gib", _gib(process_rss + children_rss))
                self._update_peak("swap_used_gib", _gib(swap.used))
                self._update_peak("disk_used_gib", _gib(disk.used))
                eta = self._estimated_remaining_locked()
            self._write_summary()
            self._log(
                f"HEARTBEAT phase={phase or 'unassigned'} step={step or 'none'} "
                f"cpu={system_cpu:.1f}% iowait={cpu_iowait:.1f}% "
                f"process_cpu_cores={process_cpu / 100.0:.1f} "
                f"ram_used={_gib(memory.used):.1f}/{_gib(memory.total):.1f}GiB "
                f"process_rss={_gib(process_rss + children_rss):.1f}GiB "
                f"disk_free={_gib(disk.free):.1f}GiB "
                f"estimated_remaining={_format_duration(eta)}"
            )
        except (OSError, psutil.Error) as error:
            self._log(f"METRICS SAMPLE FAILED type={type(error).__name__} message={error}")

    def _update_peak(self, name: str, value: float) -> None:
        self._peaks[name] = max(value, self._peaks.get(name, value))

    def _append_step_row(
        self,
        step: _Step,
        duration: float,
        status: str,
        error: BaseException | None,
    ) -> None:
        with self.steps_path.open("a", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerow(
                (
                    step.token,
                    step.phase or "",
                    step.name,
                    step.started_at,
                    _utc_now().isoformat(),
                    round(duration, 3),
                    status,
                    type(error).__name__ if error is not None else "",
                    str(error) if error is not None else "",
                )
            )
            handle.flush()

    def _finish_current_phase_locked(self) -> None:
        if self._phase is None or self._phase_started_monotonic is None:
            return
        phase = self._phase
        duration = time.monotonic() - self._phase_started_monotonic
        self._phase_timings[phase] = duration
        phase_number = PIPELINE_PHASES.index(phase) + 1
        self._phase = None
        self._phase_started_monotonic = None
        self._log(
            f"PHASE {phase_number}/{len(PIPELINE_PHASES)} END {phase} "
            f"duration={_format_duration(duration)}"
        )

    def _estimated_remaining_locked(self) -> float | None:
        if not self._history_compatible or self._phase is None:
            return None
        historical_phases = self._history.get("phases", {})
        current_index = PIPELINE_PHASES.index(self._phase)
        remaining = 0.0
        for phase in PIPELINE_PHASES[current_index:]:
            if phase not in historical_phases:
                return None
            duration = float(historical_phases[phase])
            if phase == self._phase and self._phase_started_monotonic is not None:
                duration = max(0.0, duration - (time.monotonic() - self._phase_started_monotonic))
            remaining += duration
        return remaining

    def _read_history(self) -> dict[str, Any]:
        try:
            return json.loads(self.history_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}

    def _summary_data(self) -> dict[str, Any]:
        with self._lock:
            current_step = self._steps[-1].name if self._steps else None
            return {
                "run_id": self.run_id,
                "status": self._status,
                "started_at_utc": self._started_at.isoformat(),
                "updated_at_utc": _utc_now().isoformat(),
                "elapsed_seconds": round(self.elapsed_seconds, 3),
                "current_phase": self._phase,
                "current_step": current_step,
                "config": self._config,
                "phase_timings_seconds": {
                    key: round(value, 3) for key, value in self._phase_timings.items()
                },
                "peaks": {key: round(value, 3) for key, value in self._peaks.items()},
                "error": self._error,
                "host": {
                    "hostname": socket.gethostname(),
                    "platform": platform.platform(),
                    "python": sys.version.split()[0],
                    "logical_cpu_count": psutil.cpu_count(logical=True),
                    "physical_cpu_count": psutil.cpu_count(logical=False),
                    "ram_total_gib": round(_gib(psutil.virtual_memory().total), 3),
                },
                "files": {
                    "pipeline_log": str(self.pipeline_log_path),
                    "resource_metrics": str(self.metrics_path),
                    "step_timings": str(self.steps_path),
                },
            }

    def _write_summary(self) -> None:
        if not self.run_dir.exists():
            return
        self.summary_path.write_text(
            json.dumps(self._summary_data(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _write_history(self) -> None:
        history = {
            "run_id": self.run_id,
            "completed_at_utc": _utc_now().isoformat(),
            "config": self._config,
            "phases": {key: round(value, 3) for key, value in self._phase_timings.items()},
            "total_seconds": round(self.elapsed_seconds, 3),
        }
        temporary_path = self.history_path.with_suffix(".json.tmp")
        temporary_path.write_text(
            json.dumps(history, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary_path, self.history_path)


def get_active_monitor() -> PipelineRunMonitor | None:
    return _ACTIVE_MONITOR.get()


def monitor_pipeline(func: _F) -> _F:
    """Create a monitor around a pipeline entry point with an ``output_dir`` argument."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        output_dir = kwargs.get("output_dir")
        if output_dir is None:
            raise TypeError("monitored pipeline calls must provide output_dir as a keyword argument")
        interval = float(kwargs.get("metrics_interval_seconds", 60.0))
        monitor = PipelineRunMonitor(Path(output_dir), interval)
        monitor.start()
        context_token = _ACTIVE_MONITOR.set(monitor)
        try:
            result = func(*args, **kwargs)
        except BaseException as error:
            monitor.stop(error)
            raise
        else:
            monitor.stop()
            return result
        finally:
            _ACTIVE_MONITOR.reset(context_token)

    return wrapper  # type: ignore[return-value]
