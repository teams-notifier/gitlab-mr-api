FROM python:3.12-slim-bookworm AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

COPY requirements.txt .

RUN uv venv /app/.venv && \
    uv pip install -r requirements.txt && \
    uv run opentelemetry-bootstrap -a requirements | uv pip install --requirement -

FROM python:3.12-slim-bookworm

ARG VERSION
ARG SEMVER_CORE
ARG COMMIT_SHA
ARG GITHUB_REPO
ARG BUILD_DATE

LABEL org.opencontainers.image.source=${GITHUB_REPO}
LABEL org.opencontainers.image.created=${BUILD_DATE}
LABEL org.opencontainers.image.version=${VERSION}
LABEL org.opencontainers.image.revision=${COMMIT_SHA}

RUN useradd -ms /bin/bash -d /app app

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv

COPY --chown=app:app . /app/

USER app

ENV VERSION=${VERSION} \
    SEMVER_CORE=${SEMVER_CORE} \
    COMMIT_SHA=${COMMIT_SHA} \
    BUILD_DATE=${BUILD_DATE} \
    GITHUB_REPO=${GITHUB_REPO}

CMD ["/app/run.sh"]
