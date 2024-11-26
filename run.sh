#!/bin/bash

export OTEL_PYTHON_EXCLUDED_URLS=${OTEL_PYTHON_EXCLUDED_URLS:-healthz}

opentelemetry-instrument \
    --traces_exporter otlp,console \
    --metrics_exporter otlp \
    --logs_exporter otlp,console \
    --service_name notifier-gitlab-mr-api \
    fastapi run
