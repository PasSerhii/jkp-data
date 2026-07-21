"""Database-source configuration for pipeline downloads.

Connection secrets are resolved at runtime and are never logged.  The
XpressFeed URL can be supplied directly in the ``COMPUSTAT`` environment
variable or in a ``.env`` file in the current directory (or one of its
parents).  Environment variables take precedence over the file.
"""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit


class CompustatSource(StrEnum):
    """Supported sources for WRDS-shaped Compustat tables."""

    xpressfeed = "xpressfeed"
    wrds = "wrds"


COMPUSTAT_ENV_VAR = "COMPUSTAT"
RDS_STATEMENT_TIMEOUT_MS = "300000"


def _find_dotenv(start: Path | None = None) -> Path | None:
    """Find the nearest ``.env`` without changing ``os.environ``."""
    current = (start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent
    for directory in (current, *current.parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate

    # This also supports invoking an editable checkout from another directory.
    checkout_candidate = Path(__file__).resolve().parents[3] / ".env"
    if checkout_candidate.is_file():
        return checkout_candidate
    return None


def _read_dotenv_value(path: Path, name: str) -> str | None:
    """Read one simple dotenv assignment without importing or exporting it."""
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != name:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        return value or None
    return None


def _normalise_xpressfeed_url(value: str) -> str:
    """Convert a SQLAlchemy PostgreSQL URL into a DuckDB/libpq URL.

    The RDS safety settings are added here so every connection created by the
    DuckDB PostgreSQL extension inherits the five-minute statement timeout.
    """
    if value.startswith("postgresql+psycopg2://"):
        value = "postgresql://" + value.removeprefix("postgresql+psycopg2://")
    elif value.startswith("postgres+psycopg2://"):
        value = "postgresql://" + value.removeprefix("postgres+psycopg2://")

    parsed = urlsplit(value)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise ValueError(
            f"{COMPUSTAT_ENV_VAR} must be a PostgreSQL URL; got scheme "
            f"{parsed.scheme or '<missing>'!r}."
        )
    if not parsed.hostname or not parsed.path.strip("/"):
        raise ValueError(f"{COMPUSTAT_ENV_VAR} must include a host and database name.")

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.setdefault("sslmode", "require")
    query.setdefault("connect_timeout", "15")
    query.setdefault("application_name", "jkp-data")
    query.setdefault("options", f"-c statement_timeout={RDS_STATEMENT_TIMEOUT_MS}")
    encoded_query = urlencode(query, doseq=True, quote_via=quote)
    return urlunsplit(("postgresql", parsed.netloc, parsed.path, encoded_query, parsed.fragment))


def get_xpressfeed_connection_info(*, dotenv_path: Path | None = None) -> str:
    """Resolve and normalize the private XpressFeed RDS connection URL."""
    value = os.environ.get(COMPUSTAT_ENV_VAR)
    if not value:
        env_file = dotenv_path or _find_dotenv()
        if env_file is not None and env_file.is_file():
            value = _read_dotenv_value(env_file, COMPUSTAT_ENV_VAR)
    if not value:
        raise RuntimeError(
            f"XpressFeed source selected but {COMPUSTAT_ENV_VAR} is not set. "
            "Set it in the environment or in the repository .env file."
        )
    return _normalise_xpressfeed_url(value)
