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

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgomp1 tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin jkp \
    && mkdir --parents /data /work \
    && chown --recursive jkp:jkp /data /work /home/jkp

# One copy of the finished environment, already byte-compiled by the builder.
COPY --from=builder --chown=jkp:jkp /app/.venv /app/.venv

USER jkp

# Bake the extension into the image so an EC2 job does not need access to the
# DuckDB extension repository when it starts.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL postgres'); c.close()"

WORKDIR /work
VOLUME ["/data"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/.venv/bin/jkp"]
CMD ["--help"]

# Provenance. .dockerignore excludes .git, so the revision cannot be read during
# the build and must be passed in:
#   docker build --build-arg GIT_REVISION=$(git rev-parse HEAD) ...
# Declared last because these values change on every commit; anything below this
# point would be rebuilt each time. Labels are metadata only and add no bytes.
ARG GIT_REVISION=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="jkp-data" \
      org.opencontainers.image.description="Global Factor Data pipeline (Jensen, Kelly and Pedersen)" \
      org.opencontainers.image.source="https://github.com/PasSerhii/jkp-data" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.created="${BUILD_DATE}"
