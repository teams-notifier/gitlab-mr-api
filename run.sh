#!/bin/bash

export OTEL_PYTHON_EXCLUDED_URLS=${OTEL_PYTHON_EXCLUDED_URLS:-healthz}

exec /app/.venv/bin/opentelemetry-instrument \
    --traces_exporter otlp \
    --metrics_exporter otlp \
    --logs_exporter otlp \
    --service_name notifier-gitlab-mr-api \
    /app/.venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
