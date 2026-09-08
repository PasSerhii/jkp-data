"""Incremental upload of the production CSVs to the research MSSQL database.

Replaces the legacy sasWrds uploader (AlphaJobs/sasWrds) for the Python
pipeline, keeping its business semantics: for each table, the rows at or after
the table's current maximum date are deleted and re-inserted from the CSVs
inside one transaction per element, so the last stored period absorbs upstream
revisions and later periods are appended. Incremental only -- an empty target
table is an error, not a bootstrap; seed it once with the legacy sasWrds
full_upload before pointing this phase at it.

The target database is chosen solely by the connection URL (resolved from the
``RESEARCH_UPDATE`` environment variable by ``database_sources``).
All SQL values are bound parameters; table and
column names are internal constants validated against an identifier pattern.

Known, deliberate parity with sasWrds: dailyreturnsproduction is reloaded over
a fixed 14-day rewind with no overlap assertion (a publishing gap longer than
the rewind would insert without detection), and return_cutoffs_daily is not
refreshed when the CSV's latest year/month period equals the database's.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import sqlalchemy as sa

from .aux_functions import measure_time
from .paths import DataPaths
from .runtime_monitor import get_active_monitor

SCHEMA = "dbo"
# Bounds the buffer pyodbc's fast_executemany preallocates (widest column x
# rows); an unchunked insert of the wide characteristics table exhausts memory.
INSERT_CHUNK_ROWS = 10_000
# Recent daily returns are re-loaded over this window so upstream revisions to
# already-stored days overwrite the stale values (sasWrds TIMEDELTA).
DAILY_OVERLAP_DAYS = 14

CHARACTERISTICS_TABLE = "characteristicsproduction"
DAILY_RETURNS_TABLE = "dailyreturnsproduction"
RETURN_CUTOFFS_DAILY_TABLE = "return_cutoffs_daily"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CHARACTER_DB_TYPES = frozenset({"char", "nchar", "varchar", "nvarchar"})


class DataUpdateError(RuntimeError):
    """Raised at the end of a run in which one or more elements failed."""


@dataclass(frozen=True)
class StandaloneTableSpec:
    """One cross-country CSV that loads into one table keyed by a date column."""

    table: str
    csv_name: str
    date_column: str
    drop_columns: tuple[str, ...] = ()


# fx is deliberately absent: sasWrds uploaded it, but this pipeline does not
# produce fx.csv. return_cutoffs_daily is handled separately because its
# incremental key is (year, month), not a date column. mkt_vw_cap_exc is a new
# pipeline metric the SAS-era tables do not have; it stays out of the database
# so the schema is unchanged.
STANDALONE_TABLE_SPECS: tuple[StandaloneTableSpec, ...] = (
    StandaloneTableSpec("market_returns", "market_returns.csv", "eom", ("mkt_vw_cap_exc",)),
    StandaloneTableSpec(
        "market_returns_daily", "market_returns_daily.csv", "date", ("mkt_vw_cap_exc",)
    ),
    StandaloneTableSpec("nyse_cutoffs", "nyse_cutoffs.csv", "eom"),
    StandaloneTableSpec("return_cutoffs", "return_cutoffs.csv", "eom"),
    StandaloneTableSpec("world_ret_monthly", "world_ret_monthly.csv", "eom"),
)


def _log(message: str) -> None:
    monitor = get_active_monitor()
    if monitor is not None:
        monitor.note(message)
    else:
        print(message, flush=True)


def _validate_identifier(name: str) -> str:
    """Refuse any table or column name that is not a plain SQL identifier.

    Names are interpolated into SQL text (values never are), so this is the
    gate that keeps a malformed CSV header from becoming SQL.
    """
    if not _IDENTIFIER.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def _qualified(table: str) -> str:
    return f"{SCHEMA}.{_validate_identifier(table)}"


def _table_columns(conn: sa.Connection, table: str) -> dict[str, str]:
    """Return {column_name: data_type} for one table, from INFORMATION_SCHEMA."""
    rows = conn.execute(
        sa.text(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
        ),
        {"schema": SCHEMA, "table": table},
    ).fetchall()
    columns = {str(name): str(dtype).lower() for name, dtype in rows}
    if not columns:
        raise RuntimeError(f"table {SCHEMA}.{table} does not exist in the target database")
    return columns


def _check_columns(table: str, csv_columns: list[str], db_columns: dict[str, str]) -> None:
    """Fail before any DELETE when the CSV has columns the table lacks.

    The tables have no in-repo DDL (they were created by pandas ``to_sql`` from
    the SAS CSVs), so column drift is only detectable against the live schema.
    Table columns absent from the CSV are tolerated -- they receive NULL, which
    is what ``to_sql`` append did.
    """
    missing = [column for column in csv_columns if column not in db_columns]
    if missing:
        raise RuntimeError(
            f"CSV columns not present in {SCHEMA}.{table}: {missing}. "
            "The table schema must be extended before these rows can be loaded."
        )
    unfilled = sorted(set(db_columns) - set(csv_columns))
    if unfilled:
        _log(f"WARNING {SCHEMA}.{table}: columns {unfilled} absent from the CSV; inserting NULL")


def _read_production_csv(
    path: Path,
    *,
    yyyymmdd_columns: tuple[str, ...] = (),
    iso_date_columns: tuple[str, ...] = (),
) -> pl.DataFrame:
    """Read one production CSV, parsing its date columns to ``pl.Date``.

    The per-country monthly/daily CSVs encode dates as YYYYMMDD integers; the
    cross-country files carry ISO ``YYYY-MM-DD`` strings.
    """
    df = pl.read_csv(path, infer_schema_length=None)
    casts = [
        pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
        for column in yyyymmdd_columns
        if column in df.columns
    ] + [
        pl.col(column).cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d")
        for column in iso_date_columns
        if column in df.columns
    ]
    if casts:
        df = df.with_columns(casts)
    if df.is_empty():
        raise RuntimeError(f"{path.name} is empty; refusing to update from it")
    return df


def _as_date(value: object) -> date:
    """Normalise a MAX(date_column) result to ``datetime.date``.

    pyodbc returns ``datetime`` for DATETIME columns, ``date`` for DATE
    columns, and ``str`` when the column is character-typed (the tables were
    created by pandas type inference, so both shapes exist).
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise TypeError(f"cannot interpret {value!r} as a date")


def _bind_value_for(column_type: str, value: date) -> date | str:
    """Bind dates as ISO strings when the target column is character-typed."""
    if column_type in _CHARACTER_DB_TYPES:
        return value.isoformat()
    return value


def _prepare_for_insert(df: pl.DataFrame, db_columns: dict[str, str]) -> pl.DataFrame:
    """Adapt the frame's values to the table's storage types.

    Date columns whose database column is character-typed become ISO strings
    (replicating sasWrds's ``eom.astype(str)``); float NaN becomes null so it
    lands as SQL NULL instead of failing the bind.
    """
    casts = []
    for column, dtype in df.schema.items():
        if dtype == pl.Date and db_columns.get(column) in _CHARACTER_DB_TYPES:
            casts.append(pl.col(column).dt.strftime("%Y-%m-%d"))
        elif dtype in (pl.Float32, pl.Float64):
            casts.append(pl.col(column).fill_nan(None))
    return df.with_columns(casts) if casts else df


def _insert_rows(
    conn: sa.Connection, table: str, df: pl.DataFrame, db_columns: dict[str, str]
) -> int:
    """Append every row of ``df`` to ``dbo.<table>`` in bounded executemany chunks."""
    columns = [_validate_identifier(column) for column in df.columns]
    statement = sa.text(
        f"INSERT INTO {_qualified(table)} ({', '.join(f'[{column}]' for column in columns)}) "
        f"VALUES ({', '.join(f':{column}' for column in columns)})"
    )
    prepared = _prepare_for_insert(df, db_columns)
    for chunk in prepared.iter_slices(INSERT_CHUNK_ROWS):
        conn.execute(statement, chunk.to_dicts())
    return prepared.height


def _delete_and_insert(
    engine: sa.Engine,
    table: str,
    delete_sql: str,
    delete_params: dict[str, object],
    df: pl.DataFrame,
    db_columns: dict[str, str],
) -> None:
    """Run the DELETE and the full insert in one transaction.

    ``engine.begin()`` commits on success and rolls back and re-raises on
    failure, so the table is never left with the window deleted but not
    repopulated.
    """
    with engine.begin() as conn:
        deleted = conn.execute(sa.text(delete_sql), delete_params).rowcount
        inserted = _insert_rows(conn, table, df, db_columns)
    _log(f"{SCHEMA}.{table}: deleted {deleted} row(s), inserted {inserted} row(s)")


def _max_date(
    conn: sa.Connection, table: str, date_column: str, excntry: str | None = None
) -> object | None:
    column = _validate_identifier(date_column)
    sql = f"SELECT MAX([{column}]) FROM {_qualified(table)}"
    params: dict[str, object] = {}
    if excntry is not None:
        sql += " WHERE [excntry] = :excntry"
        params["excntry"] = excntry
    return conn.execute(sa.text(sql), params).scalar()


def _table_has_rows(conn: sa.Connection, table: str) -> bool:
    return conn.execute(sa.text(f"SELECT TOP 1 1 FROM {_qualified(table)}")).first() is not None


def _assert_overlap(element: str, cutoff: date, csv_dates: pl.Series) -> None:
    """Refuse to write when the database's max date is absent from the CSV.

    Without the overlap, deleting the last stored period and inserting the
    CSV's tail would silently punch a gap between the two histories.
    """
    if cutoff not in set(csv_dates.to_list()):
        raise RuntimeError(
            f"{element}: database max date {cutoff} is not present in the CSV "
            f"(covers {csv_dates.min()}..{csv_dates.max()}); refusing to write a gap"
        )


def _bootstrap_error(table: str) -> RuntimeError:
    return RuntimeError(
        f"table {SCHEMA}.{table} is empty; the incremental update cannot bootstrap it. "
        "Seed it once with the legacy sasWrds full_upload, then re-run."
    )


def _update_standalone_table(engine: sa.Engine, spec: StandaloneTableSpec, csv_path: Path) -> None:
    """Delete the table's last stored period and re-insert it plus newer rows."""
    df = _read_production_csv(csv_path, iso_date_columns=(spec.date_column,)).drop(
        spec.drop_columns, strict=False
    )
    with engine.connect() as conn:
        db_columns = _table_columns(conn, spec.table)
        _check_columns(spec.table, df.columns, db_columns)
        raw_max = _max_date(conn, spec.table, spec.date_column)
    if raw_max is None:
        raise _bootstrap_error(spec.table)
    cutoff = _as_date(raw_max)
    _assert_overlap(spec.table, cutoff, df[spec.date_column])
    tail = df.filter(pl.col(spec.date_column) >= cutoff)
    if tail.is_empty():
        _log(f"{SCHEMA}.{spec.table}: already up to date (max {cutoff})")
        return
    date_column = _validate_identifier(spec.date_column)
    _delete_and_insert(
        engine,
        spec.table,
        f"DELETE FROM {_qualified(spec.table)} WHERE [{date_column}] = :cutoff",
        {"cutoff": _bind_value_for(db_columns[spec.date_column], cutoff)},
        tail,
        db_columns,
    )


def _country_frame(csv_path: Path, date_column: str) -> tuple[pl.DataFrame, str]:
    """Read one per-country CSV and return it with its single country code."""
    df = _read_production_csv(csv_path, yyyymmdd_columns=("eom", "date"))
    for required in ("excntry", date_column):
        if required not in df.columns:
            raise RuntimeError(f"{csv_path.name}: required column {required!r} is missing")
    countries = df["excntry"].drop_nulls().unique().to_list()
    if len(countries) != 1:
        raise RuntimeError(f"{csv_path.name}: expected one excntry value, found {countries}")
    return df, str(countries[0])


def _resolve_country_cutoff(
    conn: sa.Connection, table: str, date_column: str, country: str, skipped: list[str]
) -> date | None:
    """Country-scoped max date; None means the country was skipped with a warning.

    A country absent from a populated table needs a one-time full load and must
    not fail the whole run (sasWrds parity); an entirely empty table is an
    error like everywhere else.
    """
    raw_max = _max_date(conn, table, date_column, excntry=country)
    if raw_max is None:
        if not _table_has_rows(conn, table):
            raise _bootstrap_error(table)
        skipped.append(f"{table}/{country}")
        _log(
            f"WARNING {country}: not present in {SCHEMA}.{table}; skipped "
            "(a new country needs a one-time full load)"
        )
        return None
    return _as_date(raw_max)


def _update_characteristics_production(
    engine: sa.Engine, csv_path: Path, skipped: list[str]
) -> None:
    """Replace one country's last stored month and append the newer months."""
    df, country = _country_frame(csv_path, "eom")
    with engine.connect() as conn:
        db_columns = _table_columns(conn, CHARACTERISTICS_TABLE)
        _check_columns(CHARACTERISTICS_TABLE, df.columns, db_columns)
        cutoff = _resolve_country_cutoff(conn, CHARACTERISTICS_TABLE, "eom", country, skipped)
    if cutoff is None:
        return
    _assert_overlap(f"{CHARACTERISTICS_TABLE}/{country}", cutoff, df["eom"])
    tail = df.filter(pl.col("eom") >= cutoff)
    if tail.is_empty():
        _log(f"{SCHEMA}.{CHARACTERISTICS_TABLE}/{country}: already up to date (max {cutoff})")
        return
    _delete_and_insert(
        engine,
        CHARACTERISTICS_TABLE,
        f"DELETE FROM {_qualified(CHARACTERISTICS_TABLE)} "
        "WHERE [eom] = :cutoff AND [excntry] = :excntry",
        {"cutoff": _bind_value_for(db_columns["eom"], cutoff), "excntry": country},
        tail,
        db_columns,
    )


def _update_daily_returns_production(engine: sa.Engine, csv_path: Path, skipped: list[str]) -> None:
    """Reload one country's trailing 14 days and append the newer days."""
    df, country = _country_frame(csv_path, "date")
    with engine.connect() as conn:
        db_columns = _table_columns(conn, DAILY_RETURNS_TABLE)
        _check_columns(DAILY_RETURNS_TABLE, df.columns, db_columns)
        max_date = _resolve_country_cutoff(conn, DAILY_RETURNS_TABLE, "date", country, skipped)
    if max_date is None:
        return
    cutoff = max_date - timedelta(days=DAILY_OVERLAP_DAYS)
    tail = df.filter(pl.col("date") >= cutoff)
    if tail.is_empty():
        # Delete only alongside an insert; an empty tail must not erase the
        # trailing window (sasWrds deletes inside the non-empty branch only).
        _log(f"{SCHEMA}.{DAILY_RETURNS_TABLE}/{country}: no rows on or after {cutoff}; skipped")
        return
    _delete_and_insert(
        engine,
        DAILY_RETURNS_TABLE,
        f"DELETE FROM {_qualified(DAILY_RETURNS_TABLE)} "
        "WHERE [date] >= :cutoff AND [excntry] = :excntry",
        {"cutoff": _bind_value_for(db_columns["date"], cutoff), "excntry": country},
        tail,
        db_columns,
    )


def _update_return_cutoffs_daily(engine: sa.Engine, csv_path: Path) -> None:
    """Reload the table's latest year/month period and append the newer ones.

    The table has no date column; the incremental key is ``year * 100 + month``.
    The CSV's extra ``eom`` column (absent from the SAS-era table) is dropped.
    """
    table = RETURN_CUTOFFS_DAILY_TABLE
    df = _read_production_csv(csv_path).drop("eom", strict=False)
    with engine.connect() as conn:
        db_columns = _table_columns(conn, table)
        _check_columns(table, df.columns, db_columns)
        row = conn.execute(
            sa.text(
                f"SELECT [year], MAX([month]) FROM {_qualified(table)} "
                f"WHERE [year] = (SELECT MAX([year]) FROM {_qualified(table)}) GROUP BY [year]"
            )
        ).first()
    if row is None:
        raise _bootstrap_error(table)
    max_period = int(row[0]) * 100 + int(row[1])
    csv_max_period = (df["year"] * 100 + df["month"]).max()
    if not isinstance(csv_max_period, int):
        raise RuntimeError(f"{csv_path.name}: year/month columns are not integer periods")
    if csv_max_period <= max_period:
        _log(f"{SCHEMA}.{table}: already up to date (max period {max_period})")
        return
    tail = df.filter((pl.col("year") * 100 + pl.col("month")) >= max_period)
    _delete_and_insert(
        engine,
        table,
        f"DELETE FROM {_qualified(table)} WHERE [year] * 100 + [month] >= :max_period",
        {"max_period": max_period},
        tail,
        db_columns,
    )


def _preflight_production_files(paths: DataPaths) -> tuple[list[Path], list[Path], list[str]]:
    """Locate every expected source file before any database work.

    A completely missing production directory means the run cannot do anything
    and raises; individually missing files are recorded so the remaining
    elements still upload (per-element isolation, as in sasWrds).
    """
    production_dir = paths.production_dir
    if not production_dir.is_dir():
        raise RuntimeError(
            f"production directory {production_dir} does not exist; "
            "run the pipeline with production output enabled first"
        )
    errors = [
        f"{spec.csv_name}: missing from {production_dir}"
        for spec in STANDALONE_TABLE_SPECS
        if not (production_dir / spec.csv_name).is_file()
    ]
    if not (production_dir / "return_cutoffs_daily.csv").is_file():
        errors.append(f"return_cutoffs_daily.csv: missing from {production_dir}")
    monthly = sorted((production_dir / "monthly").glob("*.csv"))
    daily = sorted((production_dir / "daily").glob("*.csv"))
    if not monthly:
        errors.append(f"no monthly country CSVs found under {production_dir / 'monthly'}")
    if not daily:
        errors.append(f"no daily country CSVs found under {production_dir / 'daily'}")
    return monthly, daily, errors


@measure_time
def update_research_db(paths: DataPaths, *, connection_url: str) -> None:
    """Upload the production CSVs to the research database incrementally.

    Steps:
        1) Announce the effective target (host and database, never credentials)
           and locate every expected file under ``paths.production_dir``.
        2) For each cross-country table, each monthly country CSV
           (characteristicsproduction) and each daily country CSV
           (dailyreturnsproduction): delete the trailing window and re-insert
           it from the CSV in one transaction. One element's failure is
           recorded and the loop continues.
    Output:
        Rows appended/replaced in the database named by ``connection_url``.
        Raises DataUpdateError listing every failed element, if any.
    """
    url = sa.engine.make_url(connection_url)
    _log(f"Database update target: host={url.host} database={url.database}")
    monthly, daily, errors = _preflight_production_files(paths)
    skipped: list[str] = []

    def run_element(element: str, action: Callable[..., None], *args: object) -> None:
        try:
            action(*args)
        except Exception as exc:  # noqa: BLE001 - recorded and re-raised in summary
            # repr(), not str(): a bare AssertionError would otherwise record
            # an empty message.
            errors.append(f"{element}: {exc!r}")
            _log(f"FAILED {element}: {exc!r}")

    engine = sa.create_engine(connection_url, fast_executemany=True, connect_args={"timeout": 10})
    try:
        for spec in STANDALONE_TABLE_SPECS:
            csv_path = paths.production_dir / spec.csv_name
            if csv_path.is_file():
                run_element(spec.table, _update_standalone_table, engine, spec, csv_path)
        cutoffs_daily_csv = paths.production_dir / "return_cutoffs_daily.csv"
        if cutoffs_daily_csv.is_file():
            run_element(
                RETURN_CUTOFFS_DAILY_TABLE, _update_return_cutoffs_daily, engine, cutoffs_daily_csv
            )
        for csv_path in monthly:
            run_element(
                f"{CHARACTERISTICS_TABLE}/{csv_path.stem}",
                _update_characteristics_production,
                engine,
                csv_path,
                skipped,
            )
        for csv_path in daily:
            run_element(
                f"{DAILY_RETURNS_TABLE}/{csv_path.stem}",
                _update_daily_returns_production,
                engine,
                csv_path,
                skipped,
            )
    finally:
        engine.dispose()

    if skipped:
        _log(f"Skipped (not present in the database, need a one-time full load): {skipped}")
    if errors:
        raise DataUpdateError(
            f"database update finished with {len(errors)} failed element(s):\n" + "\n".join(errors)
        )
    _log("Database update completed successfully")
