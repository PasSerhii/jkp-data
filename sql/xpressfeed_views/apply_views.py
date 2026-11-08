"""Transactionally install the checked-in WRDS compatibility views.

This first runs ``load_ff_factors`` so the pipeline-used Fama-French table is
current. The view application then runs under one RDS transaction and one
transaction-scoped advisory lock.  Checked-in DROP
statements are validated but never executed: ordinary updates use
CREATE OR REPLACE VIEW, and the three known typmod migrations use a guarded
DROP VIEW (never CASCADE) only when PostgreSQL reports that replacement cannot
change a view-column type.

Run:
    uv run --with "psycopg[binary]" python sql/xpressfeed_views/apply_views.py
"""

from __future__ import annotations

import argparse
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import psycopg
from gen_views import load_env, load_schema_contract
from load_ff_factors import (
    FF_FACTORS_MONTHLY_SELECT,
    FfRefreshResult,
    _ff_rows_content_sha256,
    _verify_ff_factors_schema,
)
from load_ff_factors import (
    main as refresh_ff_factors,
)
from psycopg import errors, sql

HERE = Path(__file__).resolve().parent
ADVISORY_LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"jkp-data:comp-xpressfeed-view-build").digest()[:8],
    byteorder="big",
    signed=True,
)

# Helpers precede every consumer.  The targets are intentionally listed rather
# than discovered from the directory, so an unrelated or newly generated SQL
# file can never become part of a production build by accident.
SQL_DEPENDENCY_ORDER: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("_population.sql", ("_gl_gvkeys", "_na_gvkeys")),
    ("_security.sql", ("_security",)),
    ("company.sql", ("company",)),
    ("g_company.sql", ("g_company",)),
    ("security.sql", ("security",)),
    ("g_security.sql", ("g_security",)),
    ("sec_history.sql", ("sec_history",)),
    ("g_sec_history.sql", ("g_sec_history",)),
    ("co_hgic.sql", ("co_hgic",)),
    ("g_co_hgic.sql", ("g_co_hgic",)),
    ("funda.sql", ("funda",)),
    ("g_funda.sql", ("g_funda",)),
    ("fundq.sql", ("fundq",)),
    ("g_fundq.sql", ("g_fundq",)),
    ("secm.sql", ("secm",)),
    ("secd.sql", ("secd",)),
    ("g_secd.sql", ("g_secd",)),
    ("exrt_dly.sql", ("exrt_dly",)),
    ("r_ex_codes.sql", ("r_ex_codes",)),
)

TARGET_VIEWS: tuple[str, ...] = (
    "funda",
    "g_funda",
    "fundq",
    "g_fundq",
    "secd",
    "g_secd",
    "secm",
    "security",
    "g_security",
    "company",
    "g_company",
    "sec_history",
    "g_sec_history",
    "co_hgic",
    "g_co_hgic",
    "exrt_dly",
    "r_ex_codes",
)

# PostgreSQL cannot CREATE OR REPLACE a view when an existing output typmod
# changes.  These are the confirmed one-time migrations.  No other view is
# eligible for an automatic drop/recreate fallback.
TYPE_RECREATE_VIEWS = frozenset({"secd", "g_secd", "r_ex_codes"})
_SCHEMA_CONTRACT = load_schema_contract()
_TYPE_MIGRATION_COLUMNS = (
    ("secd", "gvkey"),
    ("secd", "iid"),
    ("g_secd", "gvkey"),
    ("g_secd", "iid"),
    ("r_ex_codes", "exchgdesc"),
)


def _contract_varchar_length(view: str, column: str) -> int:
    types = dict(_SCHEMA_CONTRACT[view])
    match = re.fullmatch(r"varchar\((\d+)\)", types[column])
    if match is None:
        raise RuntimeError(f"schema contract no longer defines {view}.{column} as varchar(n)")
    return int(match.group(1))


EXPECTED_CHARACTER_LENGTHS: dict[tuple[str, str], int] = {
    key: _contract_varchar_length(*key) for key in _TYPE_MIGRATION_COLUMNS
}

_LEADING_TRIVIA = re.compile(r"\A(?:\s+|--[^\r\n]*(?:\r?\n|\Z)|/\*.*?\*/)*", re.DOTALL)
_DROP_VIEW = re.compile(
    r"\ADROP\s+VIEW\s+IF\s+EXISTS\s+comp\.([a-z_][a-z0-9_]*)"
    r"(?:\s+(?:CASCADE|RESTRICT))?\s*\Z",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_VIEW = re.compile(
    r"\ACREATE(?:\s+OR\s+REPLACE)?\s+VIEW\s+"
    r"comp\.([a-z_][a-z0-9_]*)\s+AS\b",
    re.IGNORECASE | re.DOTALL,
)
_VIEW_OPTION = re.compile(r"\A[a-z_][a-z0-9_]*\Z")
_ALLOWED_PRIVILEGES = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN",
    }
)


@dataclass(frozen=True)
class ViewDefinition:
    name: str
    filename: str
    create_sql: str
    replace_sql: str


@dataclass(frozen=True)
class Grant:
    grantee: str | None  # None means PUBLIC.
    privilege: str
    grantable: bool
    column: str | None = None


@dataclass(frozen=True)
class ViewMetadata:
    owner: str
    comment: str | None
    column_comments: tuple[tuple[str, str], ...]
    reloptions: tuple[str, ...]
    grants: tuple[Grant, ...]


def _split_sql_statements(text: str) -> list[str]:
    """Split checked-in PostgreSQL without treating quoted semicolons as ends."""

    statements: list[str] = []
    start = 0
    i = 0
    state = "normal"
    block_depth = 0
    dollar_tag: str | None = None

    while i < len(text):
        if state == "line_comment":
            if text[i] in "\r\n":
                state = "normal"
            i += 1
            continue

        if state == "block_comment":
            if text.startswith("/*", i):
                block_depth += 1
                i += 2
            elif text.startswith("*/", i):
                block_depth -= 1
                i += 2
                if block_depth == 0:
                    state = "normal"
            else:
                i += 1
            continue

        if state == "single_quote":
            if text[i] == "\\" and i + 1 < len(text):
                i += 2
            elif text[i] == "'":
                if i + 1 < len(text) and text[i + 1] == "'":
                    i += 2
                else:
                    state = "normal"
                    i += 1
            else:
                i += 1
            continue

        if state == "double_quote":
            if text[i] == '"':
                if i + 1 < len(text) and text[i + 1] == '"':
                    i += 2
                else:
                    state = "normal"
                    i += 1
            else:
                i += 1
            continue

        if state == "dollar_quote":
            assert dollar_tag is not None
            if text.startswith(dollar_tag, i):
                i += len(dollar_tag)
                state = "normal"
                dollar_tag = None
            else:
                i += 1
            continue

        if text.startswith("--", i):
            state = "line_comment"
            i += 2
        elif text.startswith("/*", i):
            state = "block_comment"
            block_depth = 1
            i += 2
        elif text[i] == "'":
            state = "single_quote"
            i += 1
        elif text[i] == '"':
            state = "double_quote"
            i += 1
        elif text[i] == "$":
            match = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", text[i:])
            if match:
                dollar_tag = match.group(0)
                state = "dollar_quote"
                i += len(dollar_tag)
            else:
                i += 1
        elif text[i] == ";":
            statement = text[start:i].strip()
            if statement:
                statements.append(statement)
            start = i + 1
            i += 1
        else:
            i += 1

    if state not in {"normal", "line_comment"}:
        raise ValueError("unterminated SQL quote or comment")
    tail = text[start:].strip()
    if tail:
        statements.append(tail)
    return statements


def _without_leading_trivia(statement: str) -> tuple[str, str]:
    match = _LEADING_TRIVIA.match(statement)
    assert match is not None
    return statement[: match.end()], statement[match.end() :]


def _load_definitions() -> tuple[list[ViewDefinition], str]:
    definitions: list[ViewDefinition] = []
    build_hash = hashlib.sha256()

    for filename, expected_names in SQL_DEPENDENCY_ORDER:
        path = HERE / filename
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        build_hash.update(filename.encode("utf-8"))
        build_hash.update(b"\0")
        build_hash.update(raw)
        build_hash.update(b"\0")

        created: list[str] = []
        dropped: list[str] = []
        for statement in _split_sql_statements(text):
            prefix, core = _without_leading_trivia(statement)
            drop_match = _DROP_VIEW.match(core)
            if drop_match:
                dropped.append(drop_match.group(1))
                continue

            create_match = _CREATE_VIEW.match(core)
            if not create_match:
                raise ValueError(f"{filename} contains unsupported SQL")
            name = create_match.group(1)
            created.append(name)
            replacement = re.sub(
                r"\ACREATE(?:\s+OR\s+REPLACE)?\s+VIEW\b",
                "CREATE OR REPLACE VIEW",
                core,
                count=1,
                flags=re.IGNORECASE,
            )
            definitions.append(
                ViewDefinition(
                    name=name,
                    filename=filename,
                    create_sql=prefix + core,
                    replace_sql=prefix + replacement,
                )
            )

        if tuple(created) != expected_names:
            raise ValueError(f"{filename} creates {created!r}; expected {list(expected_names)!r}")
        if not set(dropped).issubset(expected_names) or len(dropped) != len(set(dropped)):
            raise ValueError(f"{filename} has invalid or duplicate DROP declarations")

    if tuple(d.name for d in definitions if d.name in TARGET_VIEWS) != tuple(
        name for _, names in SQL_DEPENDENCY_ORDER for name in names if name in TARGET_VIEWS
    ):
        raise AssertionError("target view dependency order is inconsistent")

    return definitions, build_hash.hexdigest()


def _preflight_required_typmods(definitions: list[ViewDefinition]) -> None:
    by_name = {definition.name: definition.create_sql for definition in definitions}
    required_fragments = {
        view: tuple(
            f'::varchar({length}) AS "{column}"'
            for (candidate, column), length in EXPECTED_CHARACTER_LENGTHS.items()
            if candidate == view
        )
        for view in TYPE_RECREATE_VIEWS
    }
    for name, fragments in required_fragments.items():
        missing = [fragment for fragment in fragments if fragment not in by_name[name]]
        if missing:
            raise ValueError(
                f"{name}.sql does not contain the validated target typmod(s): " + ", ".join(missing)
            )


def _rds_dsn() -> str:
    env = load_env()
    try:
        value = env["COMPUSTAT"].strip().strip('"').strip("'")
    except KeyError as exc:
        raise RuntimeError("COMPUSTAT is missing from .env") from exc
    if not value:
        raise RuntimeError("COMPUSTAT is empty in .env")
    return value.replace("+psycopg2", "")


def _verify_current_ff_factors(
    cur: psycopg.Cursor[tuple[object, ...]], expected: FfRefreshResult
) -> None:
    """Verify RDS still contains exactly the WRDS rows refreshed this run."""

    cur.execute("SELECT to_regclass('ff.factors_monthly')")
    if cur.fetchone()[0] is None:
        raise RuntimeError("ff.factors_monthly is missing after refresh")
    _verify_ff_factors_schema(cur)
    # Hold the small table stable through this check and view application.
    cur.execute("LOCK TABLE ff.factors_monthly IN SHARE MODE")
    cur.execute(FF_FACTORS_MONTHLY_SELECT)
    live_rows = cur.fetchall()
    if len(live_rows) != expected.row_count:
        raise RuntimeError("ff.factors_monthly row count changed after refresh")
    if not live_rows:
        raise RuntimeError("ff.factors_monthly must be nonempty")
    if live_rows[0][0] != expected.minimum_date or live_rows[-1][0] != expected.maximum_date:
        raise RuntimeError("ff.factors_monthly date range changed after refresh")
    if _ff_rows_content_sha256(live_rows) != expected.content_sha256:
        raise RuntimeError("ff.factors_monthly content changed after refresh")


def _capture_view_metadata(cur: psycopg.Cursor[tuple[object, ...]], name: str) -> ViewMetadata:
    cur.execute(
        """SELECT pg_get_userbyid(c.relowner),
                  obj_description(c.oid, 'pg_class'),
                  COALESCE(c.reloptions, ARRAY[]::text[])
             FROM pg_class c
             JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='comp' AND c.relname=%s AND c.relkind='v'""",
        (name,),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"comp.{name} disappeared during type migration")
    owner, comment, reloptions = row

    cur.execute(
        """SELECT a.attname, col_description(c.oid, a.attnum)
             FROM pg_class c
             JOIN pg_namespace n ON n.oid=c.relnamespace
             JOIN pg_attribute a ON a.attrelid=c.oid
            WHERE n.nspname='comp' AND c.relname=%s
              AND a.attnum > 0 AND NOT a.attisdropped
              AND col_description(c.oid, a.attnum) IS NOT NULL
            ORDER BY a.attnum""",
        (name,),
    )
    column_comments = tuple((str(col), str(value)) for col, value in cur.fetchall())

    cur.execute(
        """SELECT CASE WHEN acl.grantee=0
                            THEN NULL
                            ELSE pg_get_userbyid(acl.grantee)
                       END,
                  acl.privilege_type, acl.is_grantable
             FROM pg_class c
             JOIN pg_namespace n ON n.oid=c.relnamespace
             CROSS JOIN LATERAL aclexplode(c.relacl) acl
            WHERE n.nspname='comp' AND c.relname=%s
              AND acl.grantee <> c.relowner""",
        (name,),
    )
    grants = [
        Grant(
            grantee=None if grantee is None else str(grantee),
            privilege=str(privilege).upper(),
            grantable=bool(grantable),
        )
        for grantee, privilege, grantable in cur.fetchall()
    ]

    cur.execute(
        """SELECT a.attname,
                  CASE WHEN acl.grantee=0
                            THEN NULL
                            ELSE pg_get_userbyid(acl.grantee)
                       END,
                  acl.privilege_type, acl.is_grantable
             FROM pg_class c
             JOIN pg_namespace n ON n.oid=c.relnamespace
             JOIN pg_attribute a ON a.attrelid=c.oid
             CROSS JOIN LATERAL aclexplode(a.attacl) acl
            WHERE n.nspname='comp' AND c.relname=%s
              AND a.attnum > 0 AND NOT a.attisdropped
              AND acl.grantee <> c.relowner""",
        (name,),
    )
    grants.extend(
        Grant(
            grantee=None if grantee is None else str(grantee),
            privilege=str(privilege).upper(),
            grantable=bool(grantable),
            column=str(column),
        )
        for column, grantee, privilege, grantable in cur.fetchall()
    )
    return ViewMetadata(
        owner=str(owner),
        comment=None if comment is None else str(comment),
        column_comments=column_comments,
        reloptions=tuple(str(option) for option in reloptions),
        grants=tuple(grants),
    )


def _qualified_view(name: str) -> sql.Composed:
    return sql.SQL("{}.{}").format(sql.Identifier("comp"), sql.Identifier(name))


def _restore_view_metadata(
    cur: psycopg.Cursor[tuple[object, ...]], name: str, metadata: ViewMetadata
) -> None:
    qualified = _qualified_view(name)
    for option in metadata.reloptions:
        key, separator, value = option.partition("=")
        if not separator or not _VIEW_OPTION.fullmatch(key):
            raise RuntimeError(f"cannot safely restore view option on comp.{name}")
        cur.execute(
            sql.SQL("ALTER VIEW {} SET ({} = {})").format(
                qualified, sql.Identifier(key), sql.Literal(value)
            )
        )

    if metadata.comment is not None:
        cur.execute(
            sql.SQL("COMMENT ON VIEW {} IS %s").format(qualified),
            (metadata.comment,),
        )
    for column, comment in metadata.column_comments:
        cur.execute(
            sql.SQL("COMMENT ON COLUMN {}.{} IS %s").format(qualified, sql.Identifier(column)),
            (comment,),
        )

    for grant in metadata.grants:
        if grant.privilege not in _ALLOWED_PRIVILEGES:
            raise RuntimeError(f"cannot safely restore {grant.privilege!r} on comp.{name}")
        grantee = sql.SQL("PUBLIC") if grant.grantee is None else sql.Identifier(grant.grantee)
        privilege = sql.SQL(grant.privilege)
        grant_option = sql.SQL(" WITH GRANT OPTION") if grant.grantable else sql.SQL("")
        if grant.column is None:
            command = sql.SQL("GRANT {} ON TABLE {} TO {}{}").format(
                privilege, qualified, grantee, grant_option
            )
        else:
            command = sql.SQL("GRANT {} ({}) ON TABLE {} TO {}{}").format(
                privilege,
                sql.Identifier(grant.column),
                qualified,
                grantee,
                grant_option,
            )
        cur.execute(command)

    cur.execute(
        sql.SQL("ALTER VIEW {} OWNER TO {}").format(qualified, sql.Identifier(metadata.owner))
    )


def _apply_definition(
    conn: psycopg.Connection[tuple[object, ...]],
    cur: psycopg.Cursor[tuple[object, ...]],
    definition: ViewDefinition,
) -> str:
    try:
        # The nested transaction is a savepoint because the caller owns the
        # build transaction.  It lets us recover from the expected typmod
        # error without ever committing a partial build.
        with conn.transaction():
            cur.execute(definition.replace_sql)
        return "create_or_replace"
    except errors.InvalidTableDefinition as exc:
        primary = (exc.diag.message_primary or "").lower()
        if (
            definition.name not in TYPE_RECREATE_VIEWS
            or "cannot change data type of view column" not in primary
        ):
            raise

    metadata = _capture_view_metadata(cur, definition.name)
    try:
        with conn.transaction():
            # Deliberately omit CASCADE.  PostgreSQL will refuse the migration
            # if any downstream object would be destroyed, and the savepoint
            # plus outer transaction leave the old view intact.
            cur.execute(sql.SQL("DROP VIEW {}").format(_qualified_view(definition.name)))
            cur.execute(definition.create_sql)
            _restore_view_metadata(cur, definition.name, metadata)
    except errors.DependentObjectsStillExist as exc:
        raise RuntimeError(
            f"comp.{definition.name} needs its one-time type migration but has "
            "dependent objects; rebuild those explicitly (CASCADE is disabled)"
        ) from exc
    return "drop_recreate_for_type_change"


def _verify_targets(cur: psycopg.Cursor[tuple[object, ...]]) -> None:
    cur.execute(
        """SELECT c.relname, c.relkind
             FROM pg_class c
             JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='comp' AND c.relname = ANY(%s)""",
        (list(TARGET_VIEWS),),
    )
    found = {str(name): str(kind) for name, kind in cur.fetchall()}
    missing = sorted(set(TARGET_VIEWS) - set(found))
    wrong_kind = sorted(name for name, kind in found.items() if kind != "v")
    if missing or wrong_kind:
        raise RuntimeError(f"target verification failed; missing={missing}, not_views={wrong_kind}")

    for (view, column), expected_length in EXPECTED_CHARACTER_LENGTHS.items():
        cur.execute(
            """SELECT data_type, character_maximum_length
                 FROM information_schema.columns
                WHERE table_schema='comp' AND table_name=%s AND column_name=%s""",
            (view, column),
        )
        row = cur.fetchone()
        if row != ("character varying", expected_length):
            raise RuntimeError(
                f"comp.{view}.{column} has {row!r}; expected varchar({expected_length})"
            )

    _verify_ff_factors_schema(cur)


def _parse_args() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()


def main() -> None:
    _parse_args()

    # Validate and hash every local artifact before refreshing any external
    # state.  In particular, a stale generated SQL file cannot trigger a
    # snapshot refresh and only then fail the view build.
    definitions, content_hash = _load_definitions()
    _preflight_required_typmods(definitions)

    try:
        ff_refresh = refresh_ff_factors()
    except psycopg.Error:
        raise SystemExit(
            "Fama-French refresh failed; connection credentials were not displayed"
        ) from None

    try:
        conn = psycopg.connect(_rds_dsn(), connect_timeout=30, autocommit=True)
    except psycopg.Error:
        raise SystemExit(
            "RDS connection failed; connection credentials were not displayed"
        ) from None

    try:
        with conn, conn.transaction(), conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '300s'")
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (ADVISORY_LOCK_KEY,),
            )
            _verify_current_ff_factors(cur, ff_refresh)
            for definition in definitions:
                _apply_definition(conn, cur, definition)
            _verify_targets(cur)
    except psycopg.OperationalError:
        raise SystemExit(
            "RDS transport failed; connection credentials were not displayed"
        ) from None

    print(
        f"Applied and verified {len(TARGET_VIEWS)} comp views "
        f"(content {content_hash}); ff.factors_monthly has "
        f"{ff_refresh.row_count:,} rows through {ff_refresh.maximum_date}"
    )


if __name__ == "__main__":
    main()
