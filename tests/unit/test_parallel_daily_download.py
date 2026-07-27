from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

import jkp.data.aux_functions as aux
from jkp.data.paths import DataPaths

TABLES = ("comp.secd", "comp.g_secd")
DATE_COLUMNS = dict.fromkeys(TABLES, "datadate")
PAIRS = [("001", "01"), ("002", "02"), ("003", "03")]

# Orchestration constants for download_raw_data_tables with bypass_crsp=True:
# the first table processed after both pair headers, and the last sequential one.
FIRST_POST_HEADER_TABLE = "comp.r_ex_codes"
LAST_SEQUENTIAL_TABLE = "comp.g_fundq"
SECRET_CONNINFO = "host=test password=hunter2"


class _FakeConnection:
    def __init__(self, worker_id: int) -> None:
        self.worker_id = worker_id
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeMonitor:
    def __init__(self) -> None:
        self.downloads: list[dict[str, object]] = []
        self.notes: list[str] = []

    def record_download(self, **values: object) -> None:
        self.downloads.append(values)

    def note(self, message: str) -> None:
        self.notes.append(message)

    def step_started(self, name: str) -> str:
        return name

    def step_finished(self, token: str, error: BaseException | None = None) -> None:
        pass


def _paths(root: Path) -> DataPaths:
    paths = DataPaths(base_dir=root)
    paths.raw_tables_dir.mkdir(parents=True)
    return paths


def _install_planning_fakes(monkeypatch) -> None:
    all_columns = sorted(
        {column for table in TABLES for column in aux.LARGE_COMPUSTAT_COLUMNS[table]}
    )
    monkeypatch.setattr(aux, "get_columns", lambda *args: all_columns)
    monkeypatch.setattr(aux, "load_security_pairs", lambda *args: list(PAIRS))
    monkeypatch.setattr(aux, "DAILY_COMPUSTAT_RETRY_BACKOFF_SECONDS", (0.0, 0.0, 0.0))


def _logical_rows(paths: DataPaths) -> list[tuple[str, int, str, str]]:
    rows: list[tuple[str, int, str, str]] = []
    for table in TABLES:
        parts = paths.raw_tables_dir / f"{table.replace('.', '_')}_parts"
        for part in sorted(parts.glob("part-*.parquet")):
            rows.extend(pl.read_parquet(part).rows())
    return sorted(rows)


def test_one_and_two_workers_produce_identical_parts_and_two_are_concurrent(
    tmp_path, monkeypatch
) -> None:
    _install_planning_fakes(monkeypatch)
    state_lock = threading.Lock()
    active = 0
    max_active = 0
    opened: list[_FakeConnection] = []

    def open_connection(conninfo: str, worker_id: int):
        del conninfo
        connection = _FakeConnection(worker_id)
        opened.append(connection)
        return connection, f"worker_{worker_id}"

    def download_once(
        connection,
        relation,
        task,
        worker_id,
        retry_number,
        prior_timeouts,
    ):
        nonlocal active, max_active
        del connection, relation
        started_at = datetime.now(UTC).isoformat()
        started = time.monotonic()
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        pl.DataFrame(
            {
                "table": [task.table_name] * len(task.pairs),
                "batch": [task.batch_number] * len(task.pairs),
                "gvkey": [pair[0] for pair in task.pairs],
                "iid": [pair[1] for pair in task.pairs],
            }
        ).write_parquet(task.part_file)
        with state_lock:
            active -= 1
        result = aux._DailyBatchResult(
            task=task,
            status="completed",
            started_at_utc=started_at,
            duration_seconds=time.monotonic() - started,
            worker_id=worker_id,
            row_count=len(task.pairs),
            bytes_written=task.part_file.stat().st_size,
            retries=retry_number,
            timeouts=prior_timeouts,
        )
        aux._write_batch_manifest(task, result)
        return result

    monkeypatch.setattr(aux, "_open_daily_worker_connection", open_connection)
    monkeypatch.setattr(aux, "_download_daily_batch_once", download_once)
    monkeypatch.setattr(aux, "get_active_monitor", lambda: None)

    serial_paths = _paths(tmp_path / "serial")
    aux.download_wrds_daily_tables_parallel(
        "private",
        object(),
        serial_paths,
        TABLES,
        DATE_COLUMNS,
        start_date=date(2000, 1, 1),
        end_date=date(2026, 6, 30),
        worker_count=1,
        batch_size=1,
    )
    serial_rows = _logical_rows(serial_paths)

    active = 0
    max_active = 0
    opened.clear()
    parallel_paths = _paths(tmp_path / "parallel")
    aux.download_wrds_daily_tables_parallel(
        "private",
        object(),
        parallel_paths,
        TABLES,
        DATE_COLUMNS,
        start_date=date(2000, 1, 1),
        end_date=date(2026, 6, 30),
        worker_count=2,
        batch_size=1,
    )

    assert _logical_rows(parallel_paths) == serial_rows
    assert max_active == 2
    assert len(opened) == 2
    assert len({id(connection) for connection in opened}) == 2
    assert all(connection.closed for connection in opened)
    for table in TABLES:
        parts = parallel_paths.raw_tables_dir / f"{table.replace('.', '_')}_parts"
        assert [path.name for path in sorted(parts.glob("part-*.parquet"))] == [
            "part-000001.parquet",
            "part-000002.parquet",
            "part-000003.parquet",
        ]


def test_matching_parts_resume_but_a_changed_cutoff_rebuilds(tmp_path, monkeypatch) -> None:
    _install_planning_fakes(monkeypatch)
    paths = _paths(tmp_path)
    calls = 0

    def open_connection(conninfo: str, worker_id: int):
        del conninfo
        return _FakeConnection(worker_id), f"worker_{worker_id}"

    def download_once(
        connection,
        relation,
        task,
        worker_id,
        retry_number,
        prior_timeouts,
    ):
        nonlocal calls
        del connection, relation
        calls += 1
        pl.DataFrame({"gvkey": [task.pairs[0][0]]}).write_parquet(task.part_file)
        result = aux._DailyBatchResult(
            task=task,
            status="completed",
            started_at_utc=datetime.now(UTC).isoformat(),
            duration_seconds=0.01,
            worker_id=worker_id,
            row_count=1,
            bytes_written=task.part_file.stat().st_size,
            retries=retry_number,
            timeouts=prior_timeouts,
        )
        aux._write_batch_manifest(task, result)
        return result

    monitor = _FakeMonitor()
    monkeypatch.setattr(aux, "_open_daily_worker_connection", open_connection)
    monkeypatch.setattr(aux, "_download_daily_batch_once", download_once)
    monkeypatch.setattr(aux, "get_active_monitor", lambda: monitor)

    def run(end_date: date) -> None:
        aux.download_wrds_daily_tables_parallel(
            "private",
            object(),
            paths,
            TABLES,
            DATE_COLUMNS,
            start_date=date(2000, 1, 1),
            end_date=end_date,
            worker_count=2,
            batch_size=2,
        )

    run(date(2026, 6, 30))
    assert calls == 4
    run(date(2026, 6, 30))
    assert calls == 4
    assert len([row for row in monitor.downloads if row.get("status") == "reused"]) == 4

    damaged_part = paths.raw_tables_dir / "comp_secd_parts" / "part-000001.parquet"
    damaged_part.write_bytes(b"PAR1damagedPAR1")
    run(date(2026, 6, 30))
    assert calls == 5

    run(date(2026, 7, 31))
    assert calls == 9


def test_timeout_retries_with_new_connection_and_records_telemetry(tmp_path, monkeypatch) -> None:
    _install_planning_fakes(monkeypatch)
    paths = _paths(tmp_path)
    monitor = _FakeMonitor()
    opened: list[_FakeConnection] = []
    failures_remaining = 3

    def open_connection(conninfo: str, worker_id: int):
        del conninfo
        connection = _FakeConnection(worker_id)
        opened.append(connection)
        return connection, f"worker_{worker_id}"

    def download_once(
        connection,
        relation,
        task,
        worker_id,
        retry_number,
        prior_timeouts,
    ):
        nonlocal failures_remaining
        del connection, relation
        if failures_remaining:
            failures_remaining -= 1
            raise TimeoutError("statement timed out")
        pl.DataFrame({"gvkey": [task.pairs[0][0]]}).write_parquet(task.part_file)
        result = aux._DailyBatchResult(
            task=task,
            status="completed",
            started_at_utc=datetime.now(UTC).isoformat(),
            duration_seconds=0.02,
            worker_id=worker_id,
            row_count=1,
            bytes_written=task.part_file.stat().st_size,
            retries=retry_number,
            timeouts=prior_timeouts,
        )
        aux._write_batch_manifest(task, result)
        return result

    monkeypatch.setattr(aux, "_open_daily_worker_connection", open_connection)
    monkeypatch.setattr(aux, "_download_daily_batch_once", download_once)
    monkeypatch.setattr(aux, "get_active_monitor", lambda: monitor)

    aux.download_wrds_daily_tables_parallel(
        "private",
        object(),
        paths,
        ("comp.secd",),
        DATE_COLUMNS,
        start_date=date(2000, 1, 1),
        end_date=date(2026, 6, 30),
        worker_count=1,
        batch_size=10,
    )

    completed = [
        row
        for row in monitor.downloads
        if row.get("event_type") == "batch" and row.get("status") == "completed"
    ]
    assert len(opened) == 4
    assert all(connection.closed for connection in opened)
    assert completed[0]["worker_id"] == 1
    assert completed[0]["retries"] == 3
    assert completed[0]["timeouts"] == 3
    assert completed[0]["table_completion_percent"] == 100.0
    assert completed[0]["overall_completion_percent"] == 100.0
    retry_notes = [note for note in monitor.notes if "retrying comp.secd batch" in note]
    assert len(retry_notes) == 3


def _install_orchestration_fakes(monkeypatch, *, table_stub, parallel_stub) -> None:
    monkeypatch.setattr(aux, "duckdb", MagicMock())
    monkeypatch.setattr(aux, "download_wrds_table", table_stub)
    monkeypatch.setattr(aux, "download_wrds_daily_tables_parallel", parallel_stub)


def _download_raw(paths: DataPaths, workers: int = 2) -> None:
    aux.download_raw_data_tables(
        paths,
        connection_info=SECRET_CONNINFO,
        bypass_crsp=True,
        daily_download_workers=workers,
    )


def test_daily_queue_overlaps_remaining_sequential_tables(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    queue_started = threading.Event()
    release_queue = threading.Event()
    queue_kwargs: dict[str, object] = {}

    def parallel_stub(*args, **kwargs) -> None:
        queue_kwargs.update(kwargs)
        queue_started.set()
        assert release_queue.wait(5), "sequential loop never finished while queue ran"

    def table_stub(conninfo, connection, table, filename, **kwargs) -> None:
        if table == FIRST_POST_HEADER_TABLE:
            assert queue_started.wait(5), "daily queue did not start right after the headers"
        if table == LAST_SEQUENTIAL_TABLE:
            release_queue.set()

    _install_orchestration_fakes(monkeypatch, table_stub=table_stub, parallel_stub=parallel_stub)
    _download_raw(paths)

    assert queue_kwargs["worker_count"] == 2
    assert queue_kwargs.get("planning_db_alias") is None
    assert isinstance(queue_kwargs["cancel_event"], threading.Event)


def test_background_queue_daily_error_propagates_unwrapped(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)

    def parallel_stub(*args, **kwargs) -> None:
        raise aux._DailyBatchDownloadError("boom")

    _install_orchestration_fakes(
        monkeypatch, table_stub=lambda *args, **kwargs: None, parallel_stub=parallel_stub
    )
    with pytest.raises(aux._DailyBatchDownloadError, match="boom"):
        _download_raw(paths)


def test_background_queue_error_is_redacted(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)

    def parallel_stub(*args, **kwargs) -> None:
        raise ValueError(f"connection failed: {SECRET_CONNINFO}")

    _install_orchestration_fakes(
        monkeypatch, table_stub=lambda *args, **kwargs: None, parallel_stub=parallel_stub
    )
    with pytest.raises(RuntimeError) as excinfo:
        _download_raw(paths)

    assert "parallel daily download" in str(excinfo.value)
    assert "hunter2" not in str(excinfo.value)


def test_sequential_failure_cancels_background_queue(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    queue_cancelled = threading.Event()

    def parallel_stub(*args, cancel_event=None, **kwargs) -> None:
        if cancel_event.wait(5):
            queue_cancelled.set()

    def table_stub(conninfo, connection, table, filename, **kwargs) -> None:
        if table == "comp.company":
            raise ValueError(f"lost connection: {SECRET_CONNINFO}")

    _install_orchestration_fakes(monkeypatch, table_stub=table_stub, parallel_stub=parallel_stub)
    with pytest.raises(RuntimeError) as excinfo:
        _download_raw(paths)

    assert "download of comp.company" in str(excinfo.value)
    assert "hunter2" not in str(excinfo.value)
    assert queue_cancelled.is_set(), "main-loop failure did not cancel the background queue"


def test_workers_one_downloads_daily_tables_inline(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    batched_tables: list[str] = []

    def parallel_stub(*args, **kwargs) -> None:
        raise AssertionError("the parallel queue must not run at workers=1")

    def batched_stub(conninfo, connection, table, *args, **kwargs) -> None:
        batched_tables.append(table)

    _install_orchestration_fakes(
        monkeypatch, table_stub=lambda *args, **kwargs: None, parallel_stub=parallel_stub
    )
    monkeypatch.setattr(aux, "download_wrds_daily_table_batched", batched_stub)
    monkeypatch.setattr(aux, "load_security_pairs", lambda *args: list(PAIRS))
    _download_raw(paths, workers=1)

    assert batched_tables == ["comp.secd", "comp.g_secd"]


def test_sequential_timeout_retries_once_and_records_telemetry(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    monitor = _FakeMonitor()
    attempts: list[str] = []

    def table_stub(conninfo, connection, table, filename, **kwargs) -> None:
        attempts.append(table)
        if table == "comp.funda" and attempts.count("comp.funda") == 1:
            raise TimeoutError("canceling statement due to statement timeout")
        Path(filename).write_bytes(b"stub")

    _install_orchestration_fakes(
        monkeypatch, table_stub=table_stub, parallel_stub=lambda *args, **kwargs: None
    )
    monkeypatch.setattr(aux, "get_active_monitor", lambda: monitor)
    _download_raw(paths)

    assert attempts.count("comp.funda") == 2
    funda_rows = [
        row
        for row in monitor.downloads
        if row.get("table") == "comp.funda" and row.get("event_type") == "table"
    ]
    assert funda_rows[0]["retries"] == 1
    assert funda_rows[0]["timeouts"] == 1


def test_sequential_non_timeout_error_fails_immediately(tmp_path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    attempts: list[str] = []

    def table_stub(conninfo, connection, table, filename, **kwargs) -> None:
        attempts.append(table)
        if table == "comp.funda":
            raise ValueError("relation does not exist")

    _install_orchestration_fakes(
        monkeypatch, table_stub=table_stub, parallel_stub=lambda *args, **kwargs: None
    )
    with pytest.raises(RuntimeError, match="download of comp.funda"):
        _download_raw(paths)

    assert attempts.count("comp.funda") == 1
