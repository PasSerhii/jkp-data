"""Behavioral tests for XpressFeed compatibility deployment guards."""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("psycopg")

ROOT = Path(__file__).resolve().parents[2]
VIEW_DIR = ROOT / "sql" / "xpressfeed_views"
sys.path.insert(0, str(VIEW_DIR))

from apply_views import _verify_current_ff_factors  # noqa: E402
from load_ff_factors import (  # noqa: E402
    FF_FACTORS_MONTHLY_SCHEMA,
    FfRefreshResult,
    _ff_rows_content_sha256,
    _verify_ff_factors_schema,
)


class SchemaCursor:
    def __init__(self, rows: tuple[tuple[str, str, bool], ...]) -> None:
        self.rows = rows
        self.query = ""

    def execute(self, query: str) -> None:
        self.query = query

    def fetchall(self) -> list[tuple[str, str, bool]]:
        return list(self.rows)


def test_ff_schema_guard_accepts_only_the_exact_contract() -> None:
    cursor = SchemaCursor(FF_FACTORS_MONTHLY_SCHEMA)

    _verify_ff_factors_schema(cursor)  # type: ignore[arg-type]

    assert "pg_catalog.format_type" in cursor.query
    assert "c.relkind='r'" in cursor.query
    assert "ORDER BY a.attnum" in cursor.query


def ff_schema_drifts() -> list[tuple[tuple[str, str, bool], ...]]:
    exact = list(FF_FACTORS_MONTHLY_SCHEMA)
    missing = exact[:-1]
    extra = [*exact, ("unexpected", "text", False)]
    reordered = [exact[1], exact[0], *exact[2:]]
    renamed = [*exact]
    renamed[0] = ("month_date", "date", False)
    wrong_typmod = [*exact]
    wrong_typmod[4] = ("rf", "numeric(8,6)", False)
    wrong_nullability = [*exact]
    wrong_nullability[0] = ("date", "date", True)
    return [
        tuple(missing),
        tuple(extra),
        tuple(reordered),
        tuple(renamed),
        tuple(wrong_typmod),
        tuple(wrong_nullability),
    ]


@pytest.mark.parametrize("drifted_schema", ff_schema_drifts())
def test_ff_schema_guard_rejects_every_tested_drift(
    drifted_schema: tuple[tuple[str, str, bool], ...],
) -> None:
    cursor = SchemaCursor(drifted_schema)

    with pytest.raises(RuntimeError, match=r"ff\.factors_monthly schema mismatch"):
        _verify_ff_factors_schema(cursor)  # type: ignore[arg-type]


class CurrentFfCursor:
    def __init__(
        self,
        *,
        target_exists: bool = True,
        live_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.target_exists = target_exists
        self.live_rows = live_rows or [
            (
                date(2026, 5, 1),
                Decimal("0.001000"),
                Decimal("0.002000"),
                Decimal("0.003000"),
                Decimal("0.00010"),
                2026.0,
                5.0,
                Decimal("0.004000"),
                date(2026, 5, 31),
            )
        ]
        self.query = ""

    def execute(self, query: str) -> None:
        self.query = query

    def fetchone(self) -> tuple[object, ...] | None:
        if "to_regclass" in self.query:
            return ("ff.factors_monthly" if self.target_exists else None,)
        raise AssertionError(f"unexpected fetchone query: {self.query}")

    def fetchall(self) -> list[tuple[str, str, bool]]:
        if "pg_catalog.format_type" in self.query:
            return list(FF_FACTORS_MONTHLY_SCHEMA)
        if "FROM ff.factors_monthly" in self.query:
            return self.live_rows  # type: ignore[return-value]
        raise AssertionError(f"unexpected fetchall query: {self.query}")


def expected_refresh(rows: list[tuple[object, ...]]) -> FfRefreshResult:
    return FfRefreshResult(
        source_as_of=datetime(2026, 7, 20, 12, tzinfo=UTC),
        row_count=len(rows),
        content_sha256=_ff_rows_content_sha256(rows),
        minimum_date=rows[0][0],  # type: ignore[arg-type]
        maximum_date=rows[-1][0],  # type: ignore[arg-type]
    )


def test_current_ff_gate_accepts_exact_just_refreshed_rows() -> None:
    cursor = CurrentFfCursor()
    expected = expected_refresh(cursor.live_rows)

    _verify_current_ff_factors(cursor, expected)  # type: ignore[arg-type]

    assert "FROM ff.factors_monthly" in cursor.query


def test_current_ff_gate_rejects_row_count_drift() -> None:
    cursor = CurrentFfCursor()
    expected = expected_refresh(cursor.live_rows)
    expected = FfRefreshResult(
        source_as_of=expected.source_as_of,
        row_count=expected.row_count + 1,
        content_sha256=expected.content_sha256,
        minimum_date=expected.minimum_date,
        maximum_date=expected.maximum_date,
    )

    with pytest.raises(RuntimeError, match="row count changed"):
        _verify_current_ff_factors(cursor, expected)  # type: ignore[arg-type]


def test_current_ff_gate_rejects_missing_table() -> None:
    cursor = CurrentFfCursor(target_exists=False)
    expected = expected_refresh(cursor.live_rows)

    with pytest.raises(RuntimeError, match="missing after refresh"):
        _verify_current_ff_factors(cursor, expected)  # type: ignore[arg-type]


def test_current_ff_gate_rejects_same_count_content_drift() -> None:
    cursor = CurrentFfCursor()
    expected = expected_refresh(cursor.live_rows)
    expected = FfRefreshResult(
        source_as_of=expected.source_as_of,
        row_count=expected.row_count,
        content_sha256="a" * 64,
        minimum_date=expected.minimum_date,
        maximum_date=expected.maximum_date,
    )

    with pytest.raises(RuntimeError, match="content changed"):
        _verify_current_ff_factors(cursor, expected)  # type: ignore[arg-type]
