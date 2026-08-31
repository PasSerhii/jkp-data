# syntax=docker/dockerfile:1.7

ARG UV_VERSION=0.7.19
# Both stages must share this base: the copied virtualenv hardcodes the
# interpreter path and its script shebangs.
ARG PYTHON_TAG=3.11-slim-bookworm

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# ---------------------------------------------------------------------------
# Builder: resolve and install the virtualenv. Only /app/.venv is carried into
# the runtime image, so uv, the lockfile, and src/ stay out of the final layers.
# ---------------------------------------------------------------------------
FROM python:${PYTHON_TAG} AS builder

COPY --from=uv /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Dependencies resolve from the lockfile alone, so this layer stays cached until
# pyproject.toml or uv.lock changes.
COPY pyproject.toml uv.lock README.md .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# --no-editable installs the package itself into the virtualenv (including the
# resources/ package data), so /app/src is only a build input.
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM python:${PYTHON_TAG} AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/app/.venv/bin:$PATH \
    HOME=/home/jkp

# msodbcsql18: the Microsoft ODBC driver pyodbc needs for the --db-update
# phase (research MSSQL upload). Driver 17 is not published for bookworm, so
# the RESEARCH_UPDATE connection URL used in this container must name
# "ODBC Driver 18 for SQL Server". curl/gnupg are only needed to add the
# Microsoft apt repository and are purged again in the same layer.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl gnupg libgomp1 tini \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --yes --no-install-recommends msodbcsql18 \
    && apt-get purge --yes curl gnupg \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin jkp \
    && mkdir --parents /data /work \
    && chown --recursive jkp:jkp /data /work /home/jkp

# One copy of the finished environment, already byte-compiled by the builder.
COPY --from=builder --chown=jkp:jkp /app/.venv /app/.venv

# The monthly run's operational scripts: the readiness gate, the Fama-French
# refresh, the identifier capture, and the runner that sequences them. Baked in
# rather than cloned onto the host at boot, so the scripts that gate a build
# cannot be a different commit from the build they gate -- the image tag is the
# only version there is, and there is no git, no PyPI and no network to GitHub
# in the path. production-run.sh needs bash/aws/docker, so the host lifts it out
# with `docker cp`; the Python entry points run inside the container.
#
# Only the operational scripts, not all of scripts/ -- the rest are one-off
# analyses that would drag unrelated churn into every image rebuild.
#
# Left root-owned: the container runs as jkp, which needs to read and execute
# these but must not be able to rewrite the scripts it is about to run.
COPY sql /opt/jkp/sql
COPY scripts/check_source_ready.py scripts/production-run.sh /opt/jkp/scripts/
# The repo is developed on Windows, where git stores no exec bit.
RUN chmod 0755 /opt/jkp/scripts/production-run.sh

USER jkp

# Bake the extension into the image so an EC2 job does not need access to the
# DuckDB extension repository when it starts.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL postgres'); c.close()"

WORKDIR /work
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/jkp"]
CMD ["--help"]
