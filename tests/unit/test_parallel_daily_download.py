from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

import jkp.data.aux_functions as aux
from jkp.data.paths import DataPaths

TABLES = ("comp.secd", "comp.g_secd")
DATE_COLUMNS = dict.fromkeys(TABLES, "datadate")
PAIRS = [("001", "01"), ("002", "02"), ("003", "03")]


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
