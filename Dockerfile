# syntax=docker/dockerfile:1.7

ARG UV_VERSION=0.7.19
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.11-slim-bookworm AS runtime

COPY --from=uv /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    HOME=/home/jkp

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgomp1 tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache third-party dependencies separately from frequently changing source.
COPY pyproject.toml uv.lock README.md .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin jkp \
    && mkdir --parents /data /work \
    && chown --recursive jkp:jkp /data /work /home/jkp

USER jkp

# Bake the extension into the image so an EC2 job does not need access to the
# DuckDB extension repository when it starts.
RUN /app/.venv/bin/python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL postgres'); c.close()"

WORKDIR /work
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/jkp"]
CMD ["--help"]
