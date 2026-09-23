"""Tests for the incremental research-database upload (dataupdate module)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from jkp.data.database_sources import get_research_update_connection_info
from jkp.data.dataupdate import (
    CHARACTERISTICS_TABLE,
    DAILY_RETURNS_TABLE,
    INSERT_CHUNK_ROWS,
    RETURN_CUTOFFS_DAILY_TABLE,
    STANDALONE_TABLE_SPECS,
    DataUpdateError,
    _insert_rows,
    _update_characteristics_production,
    _update_daily_returns_production,
    _update_return_cutoffs_daily,
    _update_standalone_table,
    _validate_identifier,
    update_research_db,
)

FAKE_URL = "mssql+pyodbc://user:secret_password@dbhost.test/research_test?driver=Fake+Driver"


# ---------------------------------------------------------------------------
# Recording fake engine: captures every statement and its parameters, serves
# programmable results for the read queries, and mimics engine.begin()'s
# commit-on-success / rollback-on-error contract.
# ---------------------------------------------------------------------------


class _FakeResult:
    def __init__(self, *, rows=None, scalar=None, first=None, rowcount=0):
        self._rows = rows or []
        self._scalar = scalar
        self._first = first
        self.rowcount = rowcount

    def fetchall(self):
        return self._rows

    def scalar(self):
        return self._scalar

    def first(self):
        return self._first


_TABLE_IN_SQL = re.compile(r"\bdbo\.(\w+)")


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, parameters=None):
        sql = str(statement)
        table = match.group(1) if (match := _TABLE_IN_SQL.search(sql)) else None
        self.engine.executed.append((sql, parameters))
        if "INFORMATION_SCHEMA" in sql:
            rows = list(self.engine.columns.get(parameters["table"], {}).items())
            return _FakeResult(rows=rows)
        if "MAX([month])" in sql:
            return _FakeResult(first=self.engine.period_row)
        if "SELECT MAX(" in sql:
            excntry = (parameters or {}).get("excntry")
            key = (table, excntry) if excntry is not None else table
            return _FakeResult(scalar=self.engine.max_dates.get(key))
        if "SELECT TOP 1 1" in sql:
            return _FakeResult(first=(1,) if self.engine.has_rows else None)
        if sql.startswith("DELETE"):
            self.engine.deletes.append((sql, parameters))
            return _FakeResult(rowcount=self.engine.delete_rowcount)
        if sql.startswith("INSERT"):
            if table in self.engine.fail_insert_tables:
                raise RuntimeError(f"simulated insert failure on {table}")
            self.engine.inserts.append((sql, parameters))
            return _FakeResult()
        raise AssertionError(f"unexpected SQL in test: {sql}")


class _Transaction:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return FakeConnection(self.engine)

    def __exit__(self, exc_type, exc, tb):
        self.engine.transactions.append("rollback" if exc_type else "commit")
        return False


class _PlainConnect:
    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        return FakeConnection(self.engine)

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeEngine:
    def __init__(
        self,
        columns: dict[str, dict[str, str]],
        max_dates: dict | None = None,
        *,
        has_rows: bool = True,
        period_row: tuple | None = None,
        fail_insert_tables: frozenset[str] = frozenset(),
    ):
        self.columns = columns
        self.max_dates = max_dates or {}
        self.has_rows = has_rows
        self.period_row = period_row
        self.fail_insert_tables = fail_insert_tables
        self.delete_rowcount = 1
        self.executed: list[tuple[str, object]] = []
        self.deletes: list[tuple[str, object]] = []
        self.inserts: list[tuple[str, object]] = []
        self.transactions: list[str] = []
        self.disposed = False

    def connect(self):
        return _PlainConnect(self)

    def begin(self):
        return _Transaction(self)

    def dispose(self):
        self.disposed = True


def _inserted_rows(engine: FakeEngine) -> list[dict]:
    rows: list[dict] = []
    for _sql, params in engine.inserts:
        rows.extend(params)
    return rows


# ---------------------------------------------------------------------------
# RESEARCH_UPDATE resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_research_update_env_takes_precedence_over_dotenv(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "RESEARCH_UPDATE=mssql+pyodbc://u:p@file.test/file_db?driver=X\n", encoding="utf-8"
    )
    monkeypatch.setenv("RESEARCH_UPDATE", "mssql+pyodbc://u:p@env.test/env_db?driver=X")

    result = get_research_update_connection_info(dotenv_path=dotenv)

    assert "env.test" in result


@pytest.mark.unit
def test_research_update_read_from_dotenv_without_mutating_environment(monkeypatch, tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        'RESEARCH_UPDATE="mssql+pyodbc://u:p@rds.test/research_test?driver=X"\n', encoding="utf-8"
    )
    monkeypatch.delenv("RESEARCH_UPDATE", raising=False)

    result = get_research_update_connection_info(dotenv_path=dotenv)

    assert "rds.test" in result
    assert "RESEARCH_UPDATE" not in __import__("os").environ


@pytest.mark.unit
def test_missing_research_update_has_actionable_error(monkeypatch, tmp_path):
    monkeypatch.delenv("RESEARCH_UPDATE", raising=False)

    with (
        pytest.raises(RuntimeError, match="RESEARCH_UPDATE is not set"),
        patch("jkp.data.database_sources._find_dotenv", return_value=None),
    ):
        get_research_update_connection_info(dotenv_path=Path(tmp_path / "missing.env"))


@pytest.mark.unit
def test_research_update_rejects_non_mssql_scheme(monkeypatch):
    monkeypatch.setenv("RESEARCH_UPDATE", "postgresql://u:p@host/research_test")

    with pytest.raises(ValueError, match=r"mssql\+pyodbc"):
        get_research_update_connection_info()


# ---------------------------------------------------------------------------
# Spec registry and identifier validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_standalone_specs_cover_expected_tables_and_date_columns():
    assert [(s.table, s.csv_name, s.date_column) for s in STANDALONE_TABLE_SPECS] == [
        ("market_returns", "market_returns.csv", "eom"),
        ("market_returns_daily", "market_returns_daily.csv", "date"),
        ("nyse_cutoffs", "nyse_cutoffs.csv", "eom"),
        ("return_cutoffs", "return_cutoffs.csv", "eom"),
        ("world_ret_monthly", "world_ret_monthly.csv", "eom"),
    ]
    # fx is uploaded by sasWrds but not produced by this pipeline.
    assert "fx" not in {s.table for s in STANDALONE_TABLE_SPECS}
    # The new pipeline metric absent from the SAS-era tables is dropped on upload.
    assert {s.table: s.drop_columns for s in STANDALONE_TABLE_SPECS if s.drop_columns} == {
        "market_returns": ("mkt_vw_cap_exc",),
        "market_returns_daily": ("mkt_vw_cap_exc",),
    }


@pytest.mark.unit
@pytest.mark.parametrize("name", ["eom; DROP TABLE x", "a b", "", "1abc", "col-name"])
def test_identifier_validation_rejects_hostile_names(name):
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        _validate_identifier(name)


# ---------------------------------------------------------------------------
# Standalone table semantics
# ---------------------------------------------------------------------------


def _write_market_returns_csv(path: Path, eoms: list[str]) -> None:
    pl.DataFrame({"eom": eoms, "mkt_vw_exc": [0.01] * len(eoms)}).write_csv(path)


_MARKET_RETURNS_COLUMNS = {"market_returns": {"eom": "date", "mkt_vw_exc": "float"}}


@pytest.mark.unit
@pytest.mark.parametrize(
    ("eom_db_type", "expected_cutoff"),
    [("date", date(2026, 5, 31)), ("varchar", "2026-05-31")],
)
def test_standalone_update_deletes_equal_cutoff_and_inserts_tail(
    tmp_path, eom_db_type, expected_cutoff
):
    csv_path = tmp_path / "market_returns.csv"
    _write_market_returns_csv(csv_path, ["2026-04-30", "2026-05-31", "2026-06-30"])
    engine = FakeEngine(
        {"market_returns": {"eom": eom_db_type, "mkt_vw_exc": "float"}},
        {"market_returns": date(2026, 5, 31)},
    )

    _update_standalone_table(engine, STANDALONE_TABLE_SPECS[0], csv_path)

    (delete_sql, delete_params), *rest = engine.deletes
    assert not rest
    assert "DELETE FROM dbo.market_returns WHERE [eom] = :cutoff" in delete_sql
    assert delete_params == {"cutoff": expected_cutoff}
    inserted = _inserted_rows(engine)
    assert len(inserted) == 2  # 2026-05-31 replaced, 2026-06-30 appended
    assert engine.transactions == ["commit"]


@pytest.mark.unit
def test_standalone_update_empty_table_raises_bootstrap_error(tmp_path):
    csv_path = tmp_path / "market_returns.csv"
    _write_market_returns_csv(csv_path, ["2026-05-31"])
    engine = FakeEngine(_MARKET_RETURNS_COLUMNS, {"market_returns": None})

    with pytest.raises(RuntimeError, match="empty.*full_upload"):
        _update_standalone_table(engine, STANDALONE_TABLE_SPECS[0], csv_path)

    assert engine.deletes == []
    assert engine.inserts == []


@pytest.mark.unit
def test_standalone_overlap_missing_refuses_to_write(tmp_path):
    csv_path = tmp_path / "market_returns.csv"
    _write_market_returns_csv(csv_path, ["2026-06-30", "2026-07-31"])
    engine = FakeEngine(_MARKET_RETURNS_COLUMNS, {"market_returns": date(2026, 4, 30)})

    with pytest.raises(RuntimeError, match="refusing to write a gap"):
        _update_standalone_table(engine, STANDALONE_TABLE_SPECS[0], csv_path)

    assert engine.deletes == []
    assert engine.inserts == []


@pytest.mark.unit
def test_column_preflight_rejects_csv_column_missing_from_table(tmp_path):
    csv_path = tmp_path / "market_returns.csv"
    _write_market_returns_csv(csv_path, ["2026-05-31"])
    engine = FakeEngine({"market_returns": {"eom": "date"}}, {"market_returns": date(2026, 5, 31)})

    with pytest.raises(RuntimeError, match=r"not present in dbo\.market_returns.*mkt_vw_exc"):
        _update_standalone_table(engine, STANDALONE_TABLE_SPECS[0], csv_path)

    assert engine.deletes == []
    assert engine.inserts == []


# ---------------------------------------------------------------------------
# Per-country tables
# ---------------------------------------------------------------------------


_CHARACTERISTICS_COLUMNS = {
    CHARACTERISTICS_TABLE: {"excntry": "varchar", "eom": "date", "date": "date", "id": "varchar"}
}


def _write_monthly_csv(path: Path, eoms: list[int], excntry: str = "USA") -> None:
    pl.DataFrame(
        {"excntry": [excntry] * len(eoms), "eom": eoms, "date": eoms, "id": ["x1"] * len(eoms)}
    ).write_csv(path)


@pytest.mark.unit
def test_characteristics_scopes_by_country_and_parses_yyyymmdd(tmp_path):
    csv_path = tmp_path / "usa.csv"
    _write_monthly_csv(csv_path, [20260430, 20260531, 20260630])
    engine = FakeEngine(
        _CHARACTERISTICS_COLUMNS, {(CHARACTERISTICS_TABLE, "USA"): date(2026, 5, 31)}
    )

    _update_characteristics_production(engine, csv_path, skipped := [])

    max_queries = [(sql, p) for sql, p in engine.executed if "SELECT MAX(" in sql]
    assert max_queries[0][1] == {"excntry": "USA"}
    (delete_sql, delete_params), *rest = engine.deletes
    assert not rest
    assert "[eom] = :cutoff AND [excntry] = :excntry" in delete_sql
    assert delete_params == {"cutoff": date(2026, 5, 31), "excntry": "USA"}
    assert len(_inserted_rows(engine)) == 2
    assert skipped == []


@pytest.mark.unit
def test_characteristics_unknown_country_skipped_when_table_nonempty(tmp_path):
    csv_path = tmp_path / "xyz.csv"
    _write_monthly_csv(csv_path, [20260531], excntry="XYZ")
    engine = FakeEngine(_CHARACTERISTICS_COLUMNS, {}, has_rows=True)

    _update_characteristics_production(engine, csv_path, skipped := [])

    assert skipped == [f"{CHARACTERISTICS_TABLE}/XYZ"]
    assert engine.deletes == []
    assert engine.inserts == []


@pytest.mark.unit
def test_characteristics_empty_table_raises_bootstrap_error(tmp_path):
    csv_path = tmp_path / "usa.csv"
    _write_monthly_csv(csv_path, [20260531])
    engine = FakeEngine(_CHARACTERISTICS_COLUMNS, {}, has_rows=False)

    with pytest.raises(RuntimeError, match="empty.*full_upload"):
        _update_characteristics_production(engine, csv_path, [])

    assert engine.deletes == []


_DAILY_COLUMNS = {
    DAILY_RETURNS_TABLE: {"excntry": "varchar", "date": "date", "id": "varchar", "ret": "float"}
}


def _write_daily_csv(path: Path, dates: list[int], excntry: str = "USA") -> None:
    pl.DataFrame(
        {
            "excntry": [excntry] * len(dates),
            "date": dates,
            "id": ["x1"] * len(dates),
            "ret": [0.01] * len(dates),
        }
    ).write_csv(path)


@pytest.mark.unit
def test_daily_returns_rewinds_14_days(tmp_path):
    csv_path = tmp_path / "usa.csv"
    _write_daily_csv(csv_path, [20260810, 20260820, 20260828])
    engine = FakeEngine(_DAILY_COLUMNS, {(DAILY_RETURNS_TABLE, "USA"): date(2026, 8, 25)})

    _update_daily_returns_production(engine, csv_path, [])

    (delete_sql, delete_params), *rest = engine.deletes
    assert not rest
    assert "[date] >= :cutoff AND [excntry] = :excntry" in delete_sql
    assert delete_params == {"cutoff": date(2026, 8, 11), "excntry": "USA"}
    # 2026-08-10 predates the rewind window and must not be re-inserted.
    assert len(_inserted_rows(engine)) == 2


@pytest.mark.unit
def test_daily_returns_skips_delete_when_no_tail(tmp_path):
    csv_path = tmp_path / "usa.csv"
    _write_daily_csv(csv_path, [20260701, 20260702])
    engine = FakeEngine(_DAILY_COLUMNS, {(DAILY_RETURNS_TABLE, "USA"): date(2026, 8, 25)})

    _update_daily_returns_production(engine, csv_path, [])

    assert engine.deletes == []
    assert engine.inserts == []


# ---------------------------------------------------------------------------
# return_cutoffs_daily (period-keyed, extra eom column dropped)
# ---------------------------------------------------------------------------


_CUTOFFS_DAILY_COLUMNS = {RETURN_CUTOFFS_DAILY_TABLE: {"year": "int", "month": "int", "n": "int"}}


def _write_cutoffs_daily_csv(path: Path, periods: list[tuple[int, int]]) -> None:
    pl.DataFrame(
        {
            "eom": [f"{y}-{m:02d}-28" for y, m in periods],
            "year": [y for y, _ in periods],
            "month": [m for _, m in periods],
            "n": [100] * len(periods),
        }
    ).write_csv(path)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("csv_periods", "expects_write"),
    [
        ([(2026, 6), (2026, 7)], False),  # csv max < db max
        ([(2026, 7), (2026, 8)], False),  # csv max == db max: not refreshed (sasWrds parity)
        ([(2026, 7), (2026, 8), (2026, 9)], True),
    ],
)
def test_return_cutoffs_daily_period_key_and_early_return(tmp_path, csv_periods, expects_write):
    csv_path = tmp_path / "return_cutoffs_daily.csv"
    _write_cutoffs_daily_csv(csv_path, csv_periods)
    engine = FakeEngine(_CUTOFFS_DAILY_COLUMNS, period_row=(2026, 8))

    _update_return_cutoffs_daily(engine, csv_path)

    if not expects_write:
        assert engine.deletes == []
        assert engine.inserts == []
        return
    (delete_sql, delete_params), *rest = engine.deletes
    assert not rest
    assert "[year] * 100 + [month] >= :max_period" in delete_sql
    assert delete_params == {"max_period": 202608}
    inserted = _inserted_rows(engine)
    assert len(inserted) == 2  # periods 2026-08 and 2026-09
    assert all("eom" not in row for row in inserted)


# ---------------------------------------------------------------------------
# Insert batching
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_insert_batches_rows_in_chunks(tmp_path):
    total = 2 * INSERT_CHUNK_ROWS + 1
    df = pl.DataFrame({"eom": ["2026-05-31"] * total, "value": [1.0] * total})
    engine = FakeEngine({})
    conn = FakeConnection(engine)

    inserted = _insert_rows(conn, "market_returns", df, {"eom": "varchar", "value": "float"})

    assert inserted == total
    chunk_sizes = [len(params) for _sql, params in engine.inserts]
    assert chunk_sizes == [INSERT_CHUNK_ROWS, INSERT_CHUNK_ROWS, 1]


# ---------------------------------------------------------------------------
# Orchestration: isolation, rollback, target announcement
# ---------------------------------------------------------------------------


def _write_all_production_fixtures(production_dir: Path) -> None:
    production_dir.mkdir(parents=True, exist_ok=True)
    (production_dir / "monthly").mkdir(exist_ok=True)
    (production_dir / "daily").mkdir(exist_ok=True)
    for name in (
        "market_returns.csv",
        "nyse_cutoffs.csv",
        "return_cutoffs.csv",
        "world_ret_monthly.csv",
    ):
        _write_market_returns_csv(production_dir / name, ["2026-05-31", "2026-06-30"])
    pl.DataFrame({"date": ["2026-05-31", "2026-06-30"], "mkt_vw_exc": [0.01, 0.01]}).write_csv(
        production_dir / "market_returns_daily.csv"
    )
    _write_cutoffs_daily_csv(production_dir / "return_cutoffs_daily.csv", [(2026, 8), (2026, 9)])
    _write_monthly_csv(production_dir / "monthly" / "usa.csv", [20260531, 20260630])
    _write_daily_csv(production_dir / "daily" / "usa.csv", [20260820, 20260828])


def _full_fake_engine(**kwargs) -> FakeEngine:
    eom_keyed = {"eom": "date", "mkt_vw_exc": "float"}
    columns = {
        "market_returns": eom_keyed,
        "market_returns_daily": {"date": "date", "mkt_vw_exc": "float"},
        "nyse_cutoffs": eom_keyed,
        "return_cutoffs": eom_keyed,
        "world_ret_monthly": eom_keyed,
        **_CUTOFFS_DAILY_COLUMNS,
        **_CHARACTERISTICS_COLUMNS,
        **_DAILY_COLUMNS,
    }
    max_dates = {
        table: date(2026, 5, 31)
        for table in (
            "market_returns",
            "market_returns_daily",
            "nyse_cutoffs",
            "return_cutoffs",
            "world_ret_monthly",
        )
    }
    max_dates[(CHARACTERISTICS_TABLE, "USA")] = date(2026, 5, 31)
    max_dates[(DAILY_RETURNS_TABLE, "USA")] = date(2026, 8, 25)
    return FakeEngine(columns, max_dates, period_row=(2026, 8), **kwargs)


@pytest.mark.unit
def test_update_research_db_uploads_every_element_and_disposes_engine(test_paths):
    _write_all_production_fixtures(test_paths.production_dir)
    engine = _full_fake_engine()

    with patch("jkp.data.dataupdate.sa.create_engine", return_value=engine) as create_engine:
        update_research_db(test_paths, connection_url=FAKE_URL)

    create_engine.assert_called_once()
    assert create_engine.call_args.args == (FAKE_URL,)
    # 5 standalone + return_cutoffs_daily + 1 monthly country + 1 daily country
    assert len(engine.deletes) == 8
    assert engine.transactions == ["commit"] * 8
    assert engine.disposed


@pytest.mark.unit
def test_failure_rolls_back_and_isolation_continues_to_next_element(test_paths):
    _write_all_production_fixtures(test_paths.production_dir)
    engine = _full_fake_engine(fail_insert_tables=frozenset({"market_returns"}))

    with (
        patch("jkp.data.dataupdate.sa.create_engine", return_value=engine),
        pytest.raises(DataUpdateError, match="1 failed element") as error,
    ):
        update_research_db(test_paths, connection_url=FAKE_URL)

    assert "simulated insert failure" in str(error.value)
    assert engine.transactions[0] == "rollback"
    # Every other element still committed after the first one failed.
    assert engine.transactions[1:] == ["commit"] * 7
    assert engine.disposed


@pytest.mark.unit
def test_target_announcement_prints_host_and_database_never_password(test_paths, capsys):
    _write_all_production_fixtures(test_paths.production_dir)
    engine = _full_fake_engine()

    with patch("jkp.data.dataupdate.sa.create_engine", return_value=engine):
        update_research_db(test_paths, connection_url=FAKE_URL)

    out = capsys.readouterr().out
    assert "host=dbhost.test" in out
    assert "database=research_test" in out
    assert "secret_password" not in out


@pytest.mark.unit
def test_missing_production_dir_raises_before_any_connection(test_paths):
    with (
        patch("jkp.data.dataupdate.sa.create_engine") as create_engine,
        pytest.raises(RuntimeError, match="production directory"),
    ):
        update_research_db(test_paths, connection_url=FAKE_URL)

    create_engine.assert_not_called()


# ---------------------------------------------------------------------------
# Pipeline wiring
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_run_pipeline_db_update_resolves_url_before_running(tmp_path):
    from jkp.data.main import run_pipeline

    with (
        patch("jkp.data.main.get_research_update_connection_info", return_value=FAKE_URL) as getter,
        patch("jkp.data.main.get_xpressfeed_connection_info", return_value="postgresql://rds"),
        patch("jkp.data.main.setup_folder_structure", side_effect=RuntimeError("stop")),
        pytest.raises(RuntimeError, match="stop"),
    ):
        run_pipeline(output_dir=tmp_path, bypass_crsp=True, db_update=True, production_output=True)

    getter.assert_called_once()


@pytest.mark.unit
def test_run_pipeline_db_update_off_never_touches_research_config(tmp_path):
    from jkp.data.main import run_pipeline

    with (
        patch("jkp.data.main.get_research_update_connection_info") as getter,
        patch("jkp.data.main.update_research_db") as update,
        patch("jkp.data.main.get_xpressfeed_connection_info", return_value="postgresql://rds"),
        patch("jkp.data.main.setup_folder_structure", side_effect=RuntimeError("stop")),
        pytest.raises(RuntimeError, match="stop"),
    ):
        run_pipeline(output_dir=tmp_path, bypass_crsp=True)

    getter.assert_not_called()
    update.assert_not_called()


@pytest.mark.unit
def test_run_pipeline_db_update_requires_production_output(tmp_path):
    from jkp.data.main import run_pipeline

    with (
        patch("jkp.data.main.get_research_update_connection_info") as getter,
        pytest.raises(ValueError, match="production_output is off"),
    ):
        run_pipeline(output_dir=tmp_path, bypass_crsp=True, db_update=True, production_output=False)

    getter.assert_not_called()
